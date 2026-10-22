package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DoneIndexTTL is the retention of terminal task IDs (§4.3); within
// the window the index deduplicates late reports and repeated claims.
const DoneIndexTTL = 24 * time.Hour

// ErrTerminalImmutable rejects a write that would move a terminal entry back
// to a non-terminal state (§4.2 invariant: terminal is irreversible).
var ErrTerminalImmutable = errors.New("terminal entry cannot become non-terminal")

// StateStore is the shared state face stage one persists through
// (ADR-046 决策 0, 02-contracts.md §4.3/§6.1): the live mapping rows, the
// settled terminal records and the token revocation set live in a shared,
// persistent store instead of process memory alone (F11). The interface is
// satisfied by internal/state; a no-op implementation keeps the pre-stage-one
// behaviour (in-memory index + K8s rebuild) for deployments without the
// shared state face.
type StateStore interface {
	// PutEntry stores the live row's snapshot. The registry writes it after
	// every transition; the snapshot never carries the memory-only secrets
	// (Payload, DecidedReport, TokenHash — the store's FromEntry drops them).
	PutEntry(ctx context.Context, ws, taskID string, e TaskEntry) error
	// GetEntry reads the live row of one task. Reserved for stage two's
	// partitioned lookups; stage one restores through ListEntries and then
	// reads its in-memory index.
	GetEntry(ctx context.Context, ws, taskID string) (TaskEntry, bool, error)
	// ListEntries lists every live snapshot (the startup-restore scan).
	ListEntries(ctx context.Context) ([]TaskEntry, error)
	// DeleteEntry removes one live row.
	DeleteEntry(ctx context.Context, ws, taskID string) error

	// PutSettled stores the settled terminal record of one task.
	PutSettled(ctx context.Context, ws, taskID string, e TaskEntry) error
	// GetSettled reads the settled terminal record of one task. Reserved for
	// stage two's partitioned lookups; stage one restores the whole settled
	// set through ListSettled.
	GetSettled(ctx context.Context, ws, taskID string) (TaskEntry, bool, error)
	// ListSettled lists every settled terminal record (the startup-restore
	// scan of the post-reclaim answer face).
	ListSettled(ctx context.Context) ([]TaskEntry, error)

	// Revoke records a revoked token digest until validUntil (§6.1).
	Revoke(ctx context.Context, ws, hash string, validUntil time.Time) error
	// IsRevoked answers the revocation check for one digest. Reserved for
	// stage two's cross-instance read; stage one's in-process set is seeded at
	// startup and written through, so the hot path needs no store read.
	IsRevoked(ctx context.Context, ws, hash string) (bool, error)
	// ListRevoked lists the live revocations (digest → valid-until) so a
	// restart can seed the in-process positive cache.
	ListRevoked(ctx context.Context) (map[string]time.Time, error)

	// Ping verifies the store is reachable.
	Ping(ctx context.Context) error
}

// WithStateStore wires the shared state face. Without it the registry is the
// pre-stage-one in-memory index (the K8s rebuild owns restart recovery).
func WithStateStore(s StateStore) Option {
	return func(r *Registry) { r.state = s }
}

// WithStateErrorHook observes shared-state failures (§9.1: the shared state
// face's unavailability is an event, never silent). The hook is optional:
// tests and the no-op store run without one.
func WithStateErrorHook(h func(op, taskID string, err error)) Option {
	return func(r *Registry) { r.stateErr = h }
}

// Registry is the in-memory mapping index. The K8s Job objects are the
// durable truth (ADR-002). A configured shared state face persists every
// transition (ADR-046 阶段一) so a restart restores the rows instead of
// rebuilding them from K8s; without one, nothing is persisted and the K8s
// rebuild owns restart recovery. Entries stay in the live index until
// Delete — including terminal entries in their drain window, whose credential
// is still valid — so Inflight counts only non-terminal rows.
type Registry struct {
	mu   sync.RWMutex
	now  func() time.Time
	live map[string]TaskEntry // task_id → entry (until Delete)

	// drainTimeout is FOREMAN_JOB_DRAIN_TIMEOUT: the upper bound written once
	// at the alive → draining transition (ADR-017).
	drainTimeout time.Duration

	// sessionTTL is the shared soft-affinity row's retention
	// (FOREMAN_AGENT_STATE_TTL, §4.5). Zero keeps the store default.
	sessionTTL time.Duration

	// deliveryConfirm is FOREMAN_DELIVERY_CONFIRM_TIMEOUT: the S6 delivery
	// confirmation window stamped at each accepted delivery commit (ADR-032).
	// Zero leaves the window unarmed — the delivery is then committed once
	// and the re-delivery exit is off.
	deliveryConfirm time.Duration

	byJob    map[string]string // job_name → task_id
	byDaemon map[string]string // daemon_id → task_id

	// done is the settled index: terminal task_id → the entry's terminal
	// projection (§4.3). It backs the late-replay dedup, the drain
	// duration and — after the credential reclaim deletes the live row — the
	// retained terminal record the ops endpoint still answers with.
	done *ttlMap[TaskEntry]
	// session_key → node_name is the soft-affinity index and its one key
	// (ADR-015): written only by LearnSession, evicted by the reconcile round
	// once FOREMAN_AGENT_STATE_TTL passed. There is no issue_id → node index
	// — an issue key would pin every agent of an issue to one node while only
	// the resuming agent gains anything from it.
	sessionNode map[string]sessionNodeEntry
	// Saturation markers of the per-node soft cap (§8.6 饱和抑制,
	// ADR-006 节流): replaced wholesale by the scheduler's reconcile round,
	// so the set never outgrows the live node count.
	saturated map[string]bool

	runtimes        map[string]JobRuntime // job_runtime_id → runtime
	runtimeByDaemon map[string]string     // daemon_id → job_runtime_id

	// state is the shared state face (ADR-046 阶段一). Nil keeps the
	// pre-stage-one behaviour: the registry is the in-memory index and the
	// K8s rebuild owns restart recovery.
	state    StateStore
	stateErr func(op, taskID string, err error)
	// shared is the stage-two coordination face the configured store exposes
	// (capability detection): when set, the decisive transitions run against
	// the shared row atomically and lookups fall back to it, so any instance
	// serves any Job (ADR-046 决策 1-2; 02-contracts.md §2.1 任实例服务).
	shared SharedStore
	// stateCh is the ordered write queue; nil when no shared state face is
	// wired. statePending counts the writes queued but not yet applied.
	// stateStop ends the writer goroutine (Close); stateDone closes once it
	// has returned, so Close can wait for it. stateMu makes "refuse or
	// enqueue" atomic against Close: without it a write that read
	// stateClosed=false before Close could still be sent after the writer
	// returned, and would never be applied. The writer never takes it.
	stateCh      chan stateOp
	statePending atomic.Int64
	stateStop    chan struct{}
	stateDone    chan struct{}
	stateMu      sync.Mutex
	stateClosed  bool
	stateClose   sync.Once
	// stateCloseErr is Close's first outcome: a budget that expired before
	// the writer drained.
	stateCloseErr error
}

// Option customizes a Registry.
type Option func(*Registry)

// WithDrainTimeout sets the credential-reclaim upper bound written at the
// alive → draining transition (FOREMAN_JOB_DRAIN_TIMEOUT, ADR-017). Zero
// means "do not wait": the reclaim still lands on the next observation point.
func WithDrainTimeout(d time.Duration) Option {
	return func(r *Registry) {
		if d > 0 {
			r.drainTimeout = d
		}
	}
}

// WithSessionTTL sets the shared soft-affinity row's retention
// (FOREMAN_AGENT_STATE_TTL): the index is shared in stage two, so the store
// row must live as long as the configured retention (§4.5).
func WithSessionTTL(d time.Duration) Option {
	return func(r *Registry) {
		if d > 0 {
			r.sessionTTL = d
		}
	}
}

// WithDeliveryConfirmTimeout sets the S6 delivery confirmation window
// (FOREMAN_DELIVERY_CONFIRM_TIMEOUT, ADR-032): the span each accepted delivery
// commit withholds the payload for. Passing it without a start fact releases
// the delivery for one more attempt on the daemon's next claim, so the window
// bounds the re-delivery and the boot upper bound stays the entry's terminal
// bound. Zero (the default) leaves the window unarmed: the delivery is
// committed exactly once and no re-delivery happens.
func WithDeliveryConfirmTimeout(d time.Duration) Option {
	return func(r *Registry) {
		if d > 0 {
			r.deliveryConfirm = d
		}
	}
}

// New returns an empty Registry. now defaults to time.Now (tests inject a
// fixed clock).
func New(now func() time.Time, opts ...Option) *Registry {
	if now == nil {
		now = time.Now
	}
	r := &Registry{
		now:             now,
		live:            make(map[string]TaskEntry),
		byJob:           make(map[string]string),
		byDaemon:        make(map[string]string),
		done:            newTTLMap[TaskEntry](DoneIndexTTL, now),
		sessionNode:     make(map[string]sessionNodeEntry),
		saturated:       make(map[string]bool),
		runtimes:        make(map[string]JobRuntime),
		runtimeByDaemon: make(map[string]string),
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.state != nil {
		if shared, ok := r.state.(SharedStore); ok && shared.Shared() {
			r.shared = shared
		}
		r.stateCh = make(chan stateOp, stateQueueSize)
		r.stateStop = make(chan struct{})
		r.stateDone = make(chan struct{})
		go r.runStateWriter()
	}
	return r
}

// Put inserts or replaces e, keyed by TaskID, and maintains the secondary
// indexes. A terminal entry can only be overwritten by a terminal one.
func (r *Registry) Put(e TaskEntry) error {
	if e.TaskID == "" {
		return errors.New("task_id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.live[e.TaskID]; ok {
		if old.State == StateTerminal && e.State != StateTerminal {
			return fmt.Errorf("%w: task %s", ErrTerminalImmutable, e.TaskID)
		}
		r.dropIndexesLocked(old)
	}
	r.live[e.TaskID] = e
	r.indexLocked(e)
	if r.shared != nil {
		return r.writeShared(e)
	}
	r.persist(e.TaskID, e)
	return nil
}

// PutIfAbsent inserts e only when no entry with its TaskID is live. It reports
// whether the insert happened; a present entry — terminal or not — is never
// replaced. The check and the write are one locked step, so a caller racing a
// claim cannot clobber the entry the claim just wrote.
func (r *Registry) PutIfAbsent(e TaskEntry) (bool, error) {
	if e.TaskID == "" {
		return false, errors.New("task_id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.live[e.TaskID]; ok {
		return false, nil
	}
	r.live[e.TaskID] = e
	r.indexLocked(e)
	// No write-through: PutIfAbsent's callers are the shared-state restore
	// (the row is already in the store) and the K8s gap-fill. The gap-fill's
	// projection carries no payload and no claim-time deadlines, so writing
	// it would overwrite a healthier store row after a transient restore
	// failure — the row's next transition (or the terminal write) persists
	// the complete snapshot instead.
	return true, nil
}

// stateWriteTimeout bounds one shared-state write, so a slow or unreachable
// store can never stall the writer for its dial timeout.
const stateWriteTimeout = 2 * time.Second

// stateQueueSize bounds the write queue. A burst past it is dropped with an
// event: the K8s Job remains the durable truth, so a dropped write only means
// the next restart rebuilds that row from K8s (§4.3).
const stateQueueSize = 4096

// stateOpKind discriminates one queued shared-state write.
type stateOpKind int

const (
	stateOpPut stateOpKind = iota
	stateOpDelete
	stateOpPutSettled
	stateOpMerge
)

// stateOp is one queued write. The snapshot is taken under the registry lock
// (so it matches the row it belongs to) and applied by the writer in queue
// order, outside the lock: the daemon face never waits on store I/O (C13
// every 5s per entry, C4 per renewal would otherwise serialize it).
type stateOp struct {
	kind   stateOpKind
	ws     string
	taskID string
	e      TaskEntry
	patch  EntryPatch
}

// persist enqueues the live row's snapshot. The memory index is the runtime
// authority (stage one keeps the single-instance form); the shared store is
// the persistence face a restart restores from, so a failed write must never
// disturb the in-memory transition — the miss only means the restart rebuilds
// that row from K8s (§4.3). Called with r.mu held.
func (r *Registry) persist(taskID string, e TaskEntry) {
	if r.shared != nil {
		// Stage two writes through the field-level merge/CAS scripts; a full
		// snapshot would clobber a concurrent gate write.
		return
	}
	r.enqueueState(stateOp{kind: stateOpPut, ws: e.WorkspaceID, taskID: taskID, e: e})
}

// persistDelete enqueues the removal of the live row's snapshot.
func (r *Registry) persistDelete(taskID string, e TaskEntry) {
	if r.shared != nil {
		return
	}
	r.enqueueState(stateOp{kind: stateOpDelete, ws: e.WorkspaceID, taskID: taskID})
}

// persistSettled enqueues the settled terminal record (the row a restart
// needs to keep answering the post-reclaim single-task endpoint; §4.3,
// ADR-032).
func (r *Registry) persistSettled(taskID string, e TaskEntry) {
	if r.shared != nil {
		// CommitTerminal writes the settled record in the same atomic script
		// as the terminal verdict.
		return
	}
	r.enqueueState(stateOp{kind: stateOpPutSettled, ws: e.WorkspaceID, taskID: taskID, e: e})
}

// enqueueState hands one write to the ordered writer. It is a non-blocking
// channel send plus a counter bump — no store I/O ever runs under r.mu.
func (r *Registry) enqueueState(op stateOp) {
	if r.stateCh == nil {
		return
	}
	// The closed check and the send are one critical section: Close cannot
	// slip between them, so an accepted write always reaches a live writer.
	r.stateMu.Lock()
	if r.stateClosed {
		r.stateMu.Unlock()
		// No writer is left to apply it; a silent drop would hide the lost
		// transition, so it goes through the same failure hook.
		r.stateFailure("closed", op.taskID, errRegistryClosed)
		return
	}
	r.statePending.Add(1)
	select {
	case r.stateCh <- op:
		r.stateMu.Unlock()
	default:
		r.statePending.Add(-1)
		r.stateMu.Unlock()
		r.stateFailure("queue_full", op.taskID, errStateQueueFull)
	}
}

// errStateQueueFull reports a write the bounded queue refused.
var errStateQueueFull = errors.New("shared state write queue is full")

// errRegistryClosed reports a write attempted after Close stopped the writer.
var errRegistryClosed = errors.New("registry is closed")

// errStateDrainTimeout reports a close budget that ran out with writes still
// queued: those writes are refused through the failure hook rather than
// waited out, so shutdown stays inside the pod's grace period.
var errStateDrainTimeout = errors.New("shared state writer did not drain within the close budget")

// runStateWriter applies the queued writes in order. A failed write is
// reported through the hook; the next write of the same row repairs it (every
// op carries a full snapshot or a delete).
func (r *Registry) runStateWriter() {
	defer close(r.stateDone)
	for {
		select {
		case op := <-r.stateCh:
			r.applyStateOp(op)
			r.statePending.Add(-1)
		case <-r.stateStop:
			// Close stops new writes landing under stateMu before it signals
			// here, so this drain sees a finite set: apply what was already
			// accepted and leave nothing counted but unapplied.
			for {
				select {
				case op := <-r.stateCh:
					r.applyStateOp(op)
					r.statePending.Add(-1)
				default:
					return
				}
			}
		}
	}
}

// Close stops the shared-state writer and waits up to ctx for it to drain.
// Writes already accepted are applied while the budget lasts; when it runs
// out, the rest are counted off and refused through the failure hook (op
// "closed"), and Close returns errStateDrainTimeout instead of waiting out a
// store that stopped answering. A write attempted after Close is refused the
// same way. Safe to call more than once and without a state store; every call
// reports the first call's outcome.
func (r *Registry) Close(ctx context.Context) error {
	r.stateClose.Do(func() {
		if r.stateStop == nil {
			return
		}
		// Under stateMu no send is in flight and none can start once the flag
		// is set; the writer does not take the lock, so this cannot deadlock.
		r.stateMu.Lock()
		r.stateClosed = true
		close(r.stateStop)
		r.stateMu.Unlock()
		select {
		case <-r.stateDone:
			return
		case <-ctx.Done():
			r.stateCloseErr = fmt.Errorf("%w: %w", errStateDrainTimeout, ctx.Err())
		}
		// The writer is still inside a store call. Take what is queued so
		// every accepted write is either applied or reported, and nothing is
		// left counted but unapplied.
		for {
			select {
			case op := <-r.stateCh:
				r.statePending.Add(-1)
				r.stateFailure("closed", op.taskID, errStateDrainTimeout)
			default:
				return
			}
		}
	})
	return r.stateCloseErr
}

func (r *Registry) applyStateOp(op stateOp) {
	ctx, cancel := context.WithTimeout(context.Background(), stateWriteTimeout)
	defer cancel()
	var err error
	switch op.kind {
	case stateOpPut:
		err = r.state.PutEntry(ctx, op.ws, op.taskID, op.e)
	case stateOpDelete:
		err = r.state.DeleteEntry(ctx, op.ws, op.taskID)
	case stateOpPutSettled:
		err = r.state.PutSettled(ctx, op.ws, op.taskID, op.e)
	case stateOpMerge:
		err = r.shared.MergeEntry(ctx, op.ws, op.taskID, op.patch)
	}
	if err != nil {
		opName := "put_entry"
		switch op.kind {
		case stateOpDelete:
			opName = "delete_entry"
		case stateOpPutSettled:
			opName = "put_settled"
		case stateOpMerge:
			opName = "merge_entry"
		}
		r.stateFailure(opName, op.taskID, err)
	}
}

// FlushState blocks until every queued write has been applied (or ctx ends).
// It is the test seam and the graceful-shutdown drain: callers must not rely
// on a total order with writes enqueued after the call.
func (r *Registry) FlushState(ctx context.Context) error {
	if r.stateCh == nil {
		return nil
	}
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		if r.statePending.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// stateFailure reports one shared-state failure through the hook; a store
// failure never rolls back the in-memory transition (the K8s rebuild is the
// backstop, §4.3).
func (r *Registry) stateFailure(op, taskID string, err error) {
	if r.stateErr != nil {
		r.stateErr(op, taskID, err)
	}
}

// Restore rebuilds the in-memory index from the shared state face instead of
// from K8s (§4.3: 实例重启只重新接入共享状态，已存在的条目不再从 K8s 全量重建).
// It returns the entries restored and whether the store answered at all: a
// store outage (or an unconfigured one) answers false and the caller falls
// back to the K8s rebuild.
func (r *Registry) Restore(ctx context.Context) ([]TaskEntry, bool, error) {
	if r.state == nil {
		return nil, false, nil
	}
	if err := r.state.Ping(ctx); err != nil {
		return nil, false, err
	}
	// The restore scan is per workspace; the workspace id is not known to the
	// registry, so the store lists its whole entry namespace. The stage-one
	// single workspace keeps this a single scan.
	entries, err := r.state.ListEntries(ctx)
	if err != nil {
		return nil, false, err
	}
	restored := 0
	for _, e := range entries {
		if e.TaskID == "" {
			continue
		}
		inserted, err := r.PutIfAbsent(e)
		if err != nil {
			return entries, true, err
		}
		if !inserted {
			continue
		}
		// Restore the secondary indexes the snapshot cannot carry: the
		// job runtime row is what the daemon's heartbeat and the fake
		// server's claim lookup need.
		if e.JobRuntimeID != "" {
			r.PutRuntime(JobRuntime{
				JobRuntimeID:    e.JobRuntimeID,
				DaemonID:        e.DaemonID,
				TaskID:          e.TaskID,
				Provider:        "omp",
				Name:            "foreman-job",
				Online:          true,
				LastHeartbeatAt: r.now(),
			})
		}
		restored++
	}
	// Restore the settled records into the done index: the terminal row
	// deleted from the live set at reclaim still answers the post-reclaim
	// single-task endpoint (§4.3, ADR-032). These rows are independent of
	// the live set — a task can be fully reclaimed (live row gone) while
	// its retained record is still answerable. A listing failure is an
	// error, never a silent empty set (§9.1): the caller logs it and the
	// K8s fill-gaps continue, but the terminal face must not read as
	// "nothing settled".
	settled, err := r.state.ListSettled(ctx)
	if err != nil {
		return entries, true, err
	}
	for _, rec := range settled {
		if rec.TaskID == "" {
			continue
		}
		at := rec.CompletedAt
		if at.IsZero() {
			at = rec.DrainDeadline
		}
		r.done.put(rec.TaskID, at, settledRecord(rec))
	}
	return entries, true, nil
}

// indexLocked writes e's secondary indexes. The caller holds r.mu.
func (r *Registry) indexLocked(e TaskEntry) {
	if e.JobName != "" {
		r.byJob[e.JobName] = e.TaskID
	}
	if e.DaemonID != "" {
		r.byDaemon[e.DaemonID] = e.TaskID
	}
}

// Get returns the entry for taskID.
func (r *Registry) Get(taskID string) (TaskEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.live[taskID]
	return e, ok
}

// ByJob returns the entry whose Job is jobName.
func (r *Registry) ByJob(jobName string) (TaskEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.get(r.byJob[jobName])
}

// ByDaemon returns the entry served by daemonID.
func (r *Registry) ByDaemon(daemonID string) (TaskEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.get(r.byDaemon[daemonID])
}

func (r *Registry) get(taskID string) (TaskEntry, bool) {
	e, ok := r.live[taskID]
	return e, ok
}

// Delete removes taskID from the live index together with its job runtime
// rows (DeleteRuntime), so no index grows with process lifetime. The
// terminal record stays in the done index until its TTL expires.
func (r *Registry) Delete(taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.live[taskID]; ok {
		r.dropIndexesLocked(e)
		delete(r.live, taskID)
		r.dropRuntimeLocked(e.JobRuntimeID, e.DaemonID)
		if r.shared != nil {
			ctx, cancel := r.sharedCtx()
			defer cancel()
			if err := r.shared.DeleteEntry(ctx, e.WorkspaceID, taskID); err != nil {
				return err
			}
			if e.State != StateTerminal {
				// A row deleted before its terminal transition (a discarded
				// pre-delivery attempt) still held its in-flight slot.
				r.releaseSlotsShared(e.WorkspaceID)
			}
			return nil
		}
		r.persistDelete(taskID, e)
	}
	return nil
}

// SetLastStatusSeen records the latest C13 answer for a task in place,
// without touching any other field: callers holding a pre-network snapshot
// must never Put it back wholesale (that would clobber concurrent
// transitions such as MarkDelivering/MarkStarted).
func (r *Registry) SetLastStatusSeen(taskID, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.live[taskID]; ok {
		e.LastStatusSeen = status
		r.live[taskID] = e
		if r.shared != nil {
			r.mergeShared(taskID, e, EntryPatch{LastStatusSeen: &status})
			return
		}
		r.persist(taskID, e)
	}
}

// SetLeaseKeepUntil records the task's current prepare-lease deadline
// (§10 lease_keep_until): the last successful C4 renewal plus the
// server's 45s lease TTL. The value is a local upper bound of the server's
// own prepare_lease_expires_at — it is projected with Foreman's clock and
// taken after the renewal response returns (local round trip/2 + clock
// drift), so it never trails the server's expiry. In-place, like
// SetLastStatusSeen: callers holding a pre-network snapshot must never Put
// it back wholesale.
func (r *Registry) SetLeaseKeepUntil(taskID string, until time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.live[taskID]; ok {
		e.LeaseKeepUntil = until
		r.live[taskID] = e
		if r.shared != nil {
			r.mergeShared(taskID, e, EntryPatch{LeaseKeepUntil: &until})
			return
		}
		r.persist(taskID, e)
	}
}

// List returns a snapshot of all live entries, including terminal ones
// whose K8s cleanup has not finished yet.
func (r *Registry) List() []TaskEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TaskEntry, 0, len(r.live))
	for _, e := range r.live {
		out = append(out, e)
	}
	return out
}

// Inflight counts non-terminal entries (§4.2).
func (r *Registry) Inflight() int {
	if r.shared != nil {
		ctx, cancel := r.sharedCtx()
		if n, err := r.shared.Slots(ctx, ""); err == nil {
			cancel()
			return int(n)
		}
		cancel()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, e := range r.live {
		if e.State != StateTerminal {
			n++
		}
	}
	return n
}

// InflightFor counts the non-terminal entries of one workspace (§10 两级预算).
func (r *Registry) InflightFor(workspaceID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, e := range r.live {
		if e.State != StateTerminal && e.WorkspaceID == workspaceID {
			n++
		}
	}
	return n
}

// IsDone reports whether (workspaceID, taskID) reached terminal state within
// the retention window; it backs the late-replay idempotent path and claim
// dedup. The workspace is part of the key, so a terminal task of one
// workspace never suppresses the same task id in another.
func (r *Registry) IsDone(workspaceID, taskID string) bool {
	e, _, ok := r.done.get(taskID)
	return ok && e.WorkspaceID == workspaceID
}

// IsDoneAny reports whether taskID reached terminal state in any workspace.
// It is the malformed-claim path's conservative check: when the claim body
// cannot carry a workspace, a settled task under that id must still not be
// re-executed.
func (r *Registry) IsDoneAny(taskID string) bool {
	return r.done.has(taskID)
}

// DoneAt returns the moment the task reached its verdict, as recorded by the
// single alive → draining transition. The credential reclaim measures the
// drain duration from it (§9.2 foreman_job_drain_seconds).
func (r *Registry) DoneAt(taskID string) (time.Time, bool) {
	_, at, ok := r.done.get(taskID)
	return at, ok
}

// Settled returns the retained terminal record of taskID (§4.3):
// the entry's terminal projection, still answerable after the credential
// reclaim deleted the live row, until the retention window expires. It backs
// the ops endpoint's post-hoc query face (ADR-032); the record never carries
// the payload body or the token digest.
func (r *Registry) Settled(taskID string) (TaskEntry, bool) {
	e, _, ok := r.done.get(taskID)
	return e, ok
}

// Now is the registry's own clock. The S6 claim path reads it to classify an
// empty batch (empty_unconfirmed vs empty); the delivery commit instant and its
// confirmation deadline come from the caller's stamp instead — the scheduler
// passes its own clock into MarkDelivering. The two sources must therefore be
// the same one: the process wires both to time.Now and a test injects one clock
// into both. They are not interchangeable — a second source would let the
// deadline test and the stamp disagree at the boundary.
func (r *Registry) Now() time.Time { return r.now() }

// MarkTerminal is the single write path into the terminal state (§4.2). The
// first call writes state, result, attribution, the drain bound and the
// settled record atomically and reports that transition. Terminal is
// irreversible: a repeated report (daemon retry, queue replay, late replay)
// only gets the settled entry back (first=false), with its verdict, drain
// bound and settled record untouched. reason is the attribution of a failure
// Foreman judged itself; the server's own verdicts carry none (§4.1).
func (r *Registry) MarkTerminal(taskID string, result Result, reason string, completedAt time.Time) (TaskEntry, bool, error) {
	if completedAt.IsZero() {
		completedAt = r.now()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live[taskID]
	if !ok {
		return TaskEntry{}, false, fmt.Errorf("unknown task %s", taskID)
	}
	if e.State == StateTerminal {
		return e, false, nil
	}
	if r.shared != nil {
		drainDeadline := r.now().Add(r.drainTimeout)
		r.mu.Unlock()
		ctx, cancel := r.sharedCtx()
		entry, first, err := r.shared.CommitTerminal(ctx, e.WorkspaceID, taskID, result, reason, completedAt, drainDeadline)
		cancel()
		r.mu.Lock()
		if err != nil {
			r.stateFailure("terminal", taskID, err)
			return TaskEntry{}, false, err
		}
		if entry.TaskID == "" {
			return TaskEntry{}, false, fmt.Errorf("unknown task %s", taskID)
		}
		r.cacheEntryLocked(entry)
		if entry.State == StateTerminal {
			at := entry.CompletedAt
			if at.IsZero() {
				at = entry.DrainDeadline
			}
			r.done.put(taskID, at, settledRecord(entry))
		}
		if first {
			r.releaseSlotsShared(entry.WorkspaceID)
		}
		return entry, first, nil
	}
	e.State = StateTerminal
	e.JobPhase = JobPhaseDraining
	e.DrainDeadline = r.now().Add(r.drainTimeout)
	e.CompletedAt = completedAt
	e.Result = result
	if reason != "" {
		e.FailureReason = reason
	}
	// The settled record is the projection at the verdict instant: it is
	// what the ops endpoint keeps answering with once the credential
	// reclaim deletes the live row (ADR-032). Both faces persist: the live
	// row (drain keeps the credential valid) and the settled record (the
	// post-reclaim single-task answer, §4.3).
	r.done.put(taskID, e.CompletedAt, settledRecord(e))
	r.live[taskID] = e
	r.persist(taskID, e)
	r.persistSettled(taskID, settledRecord(e))
	return e, true, nil
}

// SettleNoResult applies §4.4's in-place decision to the report that was still
// being decided when the server answered 404: the task no longer exists, so
// nothing is writable and the verdict carries no result. expected is the
// result that report wrote; a mismatch means another path already decided the
// entry, and the settled verdict stands (§4.2).
func (r *Registry) SettleNoResult(taskID string, expected Result) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live[taskID]
	if !ok || e.State != StateTerminal || e.Result != expected {
		return e, false
	}
	if r.shared != nil {
		r.mu.Unlock()
		ctx, cancel := r.sharedCtx()
		entry, first, err := r.shared.SettleNoResult(ctx, e.WorkspaceID, taskID, expected)
		cancel()
		r.mu.Lock()
		if err != nil {
			r.stateFailure("settle_no_result", taskID, err)
			return e, false
		}
		if !first {
			return entry, false
		}
		if _, at, ok := r.done.get(taskID); ok {
			r.done.put(taskID, at, settledRecord(entry))
		}
		r.cacheEntryLocked(entry)
		r.mu.Unlock()
		return entry, true
	}
	e.Result = ""
	e.FailureReason = ""
	r.live[taskID] = e
	r.persist(taskID, e)
	if _, at, ok := r.done.get(taskID); ok {
		r.done.put(taskID, at, settledRecord(e))
		r.persistSettled(taskID, settledRecord(e))
	}
	return e, true
}

// settledRecord strips the memory-only secrets from the entry the settled
// index retains: the retention window must never hold a payload body (F3), a
// token digest (ADR-017) or a decided terminal body (the durable queue keeps
// its own copy).
func settledRecord(e TaskEntry) TaskEntry {
	e.Payload = nil
	e.TokenHash = ""
	e.DecidedReport = nil
	return e
}

// DecidedReport returns the terminal body registered for the instance. Its
// presence is the "already decided" criterion of the attribution gate
// (§4.4, ADR-031).
func (r *Registry) DecidedReport(taskID string) ([]byte, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.live[taskID]
	if !ok || len(e.DecidedReport) == 0 {
		return nil, false
	}
	return e.DecidedReport, true
}

// RegisterDecidedReport records body as the instance's decided terminal body
// unless one is already registered — the first decision wins, so a replay, a
// compensation or a late report can never displace the body its verdict was
// forwarded with (§4.4, ADR-031). A missing entry leaves the registry
// untouched. It returns the entry as it stands after the call.
func (r *Registry) RegisterDecidedReport(taskID string, body []byte) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live[taskID]
	if !ok {
		return TaskEntry{}, false
	}
	if len(e.DecidedReport) == 0 && len(body) > 0 {
		e.DecidedReport = body
		r.live[taskID] = e
		// The decided body is part of the mapping record (ADR-046 决策 0).
		// Both call paths register it right after MarkTerminal, whose write
		// predates this field, so this write-through is what persists it.
		if r.shared != nil {
			r.mu.Unlock()
			ctx, cancel := r.sharedCtx()
			entry, first, err := r.shared.CommitDecidedReport(ctx, e.WorkspaceID, taskID, body)
			cancel()
			r.mu.Lock()
			if err != nil {
				r.stateFailure("decided_report", taskID, err)
			} else if !first {
				// Another instance decided first; adopt its body.
				e = entry
				r.cacheEntryLocked(entry)
			}
			return e, true
		}
		r.persist(taskID, e)
	}
	return e, true
}

// SetTokenHash records sha256(token) of the Job's credential. OnClaim is the
// only writer, right after the builder produced the Secret (ADR-017): the
// reclaim path feeds it to the revocation set so a re-dispatched task cannot
// be claimed by its predecessor's pod. A rebuilt entry has no hash — it never
// leaves memory — and the reclaim degrades visibly instead.
func (r *Registry) SetTokenHash(taskID, tokenHash string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live[taskID]
	if !ok {
		return
	}
	e.TokenHash = tokenHash
	r.live[taskID] = e
	if r.shared != nil {
		r.mergeNow(taskID, e, EntryPatch{TokenHash: &tokenHash})
	}
}

// MarkJobCreated advances pending → job_created and records the apiserver's
// creationTimestamp as job_created_at (§4.1: the pod instance
// filter's basis, never a local clock reading). It writes in place — the
// claim's snapshot predates the Job create, which can wait up to jobNameWait
// on a name still held by a terminating predecessor, so re-Putting that
// snapshot would revert a prepare-lease renewal the LeaseKeeper landed in
// the window (lost update). Returns false once the entry is gone or settled.
func (r *Registry) MarkJobCreated(taskID string, at time.Time) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live[taskID]
	if !ok || e.State == StateTerminal {
		return TaskEntry{}, false
	}
	if r.shared != nil {
		r.mu.Unlock()
		ctx, cancel := r.sharedCtx()
		entry, first, err := r.shared.CommitJobCreated(ctx, e.WorkspaceID, taskID, at)
		cancel()
		r.mu.Lock()
		if err != nil {
			r.stateFailure("job_created", taskID, err)
			return TaskEntry{}, false
		}
		if !first {
			r.cacheEntryLocked(entry)
			return TaskEntry{}, false
		}
		r.cacheEntryLocked(entry)
		return entry, true
	}
	if e.State == StatePending {
		e.State = StateJobCreated
	}
	e.JobCreatedAt = at
	r.live[taskID] = e
	r.persist(taskID, e)
	return e, true
}

// MarkDaemonRegistered advances job_created → daemon_registered on S4
// (idempotent: re-registration refreshes the timestamp without regressing
// delivering/running entries).
func (r *Registry) MarkDaemonRegistered(daemonID string, at time.Time) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.get(r.byDaemon[daemonID])
	if !ok || e.State == StateTerminal || e.State == StatePending {
		// pending has no Job yet, so no daemon can exist for it.
		return TaskEntry{}, false
	}
	if r.shared != nil {
		r.mu.Unlock()
		ctx, cancel := r.sharedCtx()
		entry, first, err := r.shared.CommitDaemonRegistered(ctx, e.WorkspaceID, e.TaskID, at)
		cancel()
		r.mu.Lock()
		if err != nil {
			r.stateFailure("daemon_registered", e.TaskID, err)
			return TaskEntry{}, false
		}
		if !first {
			return TaskEntry{}, false
		}
		r.cacheEntryLocked(entry)
		return entry, true
	}
	if e.State == StateJobCreated || e.State == StateDaemonRegistered {
		e.State = StateDaemonRegistered
	}
	e.DaemonRegisteredAt = at
	r.live[e.TaskID] = e
	r.persist(e.TaskID, e)
	return e, true
}

// MarkDelivering atomically performs the S6 delivery check and records the
// delivered body as the entry's payload, so the ops preview serves exactly
// what the daemon received (§9.3). Only a claim gate the
// entry passes transitions (deliverable); without the single-lock check, two
// concurrent claims could both pass the test before either writes. It also
// (re-)arms the delivery confirmation window (ADR-032): the entry's delivery
// deadline is the commit instant plus FOREMAN_DELIVERY_CONFIRM_TIMEOUT, and
// each accepted delivery stamps its own window.
func (r *Registry) MarkDelivering(daemonID string, at time.Time, delivered json.RawMessage) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.get(r.byDaemon[daemonID])
	if !ok || !Deliverable(e, at) {
		return TaskEntry{}, false
	}
	if r.shared != nil {
		// The shared row is the delivery authority (stage two delivers through
		// the queue pickup; this path persists the accounting when a caller
		// commits directly).
		r.mu.Unlock()
		ctx, cancel := r.sharedCtx()
		entry, first, err := r.shared.CommitDelivering(ctx, e.WorkspaceID, e.TaskID, at, delivered, r.deliveryConfirm)
		cancel()
		r.mu.Lock()
		if err != nil {
			r.stateFailure("delivering", e.TaskID, err)
			return TaskEntry{}, false
		}
		if !first {
			return TaskEntry{}, false
		}
		r.cacheEntryLocked(entry)
		return entry, true
	}
	e.State = StateDelivering
	e.DeliveredAt = at
	e.Deliveries++
	// §4.1: the claim-after-delivery count is scoped to the replaced commit,
	// so a new commit starts from zero.
	e.ClaimsAfterDelivery = 0
	e.Payload = delivered
	if r.deliveryConfirm > 0 {
		e.DeliveryDeadline = at.Add(r.deliveryConfirm)
	}
	r.live[e.TaskID] = e
	r.persist(e.TaskID, e)
	return e, true
}

// Deliverable is the S6 gate: the entry's first delivery, or the re-delivery
// an expired confirmation window released. A started, terminal or draining
// entry is never deliverable, and so is a delivered entry whose window is
// still open — the commit the daemon has not answered yet (ADR-032). It is
// exported so the shared-row commit applies the identical predicate.
func Deliverable(e TaskEntry, at time.Time) bool {
	if e.State == StateDaemonRegistered && e.DeliveredAt.IsZero() {
		return true
	}
	return e.DeliveryRearmable(at)
}

// NoteClaimAfterDelivery records one S6 claim answered with an empty batch
// while the entry's delivery was committed and still unconfirmed
// (§4.1). The count is the per-task discriminator between "the request never
// arrived" and "the response was lost": the daemon keeps asking only when it
// never got the payload.
func (r *Registry) NoteClaimAfterDelivery(taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shared != nil {
		// The shared pickup counts the empty claim in the same atomic step
		// that decides it; counting again here would double it.
		return
	}
	if e, ok := r.live[taskID]; ok {
		e.ClaimsAfterDelivery++
		r.live[taskID] = e
		r.persist(taskID, e)
	}
}

// MarkStarted records S8 under the registry lock so a concurrent terminal
// write always wins over a late start. started_at is set once; the state
// advances to running from any non-terminal state (a rebuilt job_created
// entry must accept its running daemon's reports). This is the report path
// only: the C13 adoption paths record the fact with MarkStartFact and leave
// the state untouched.
func (r *Registry) MarkStarted(taskID string, at time.Time) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shared != nil {
		e, ok := r.live[taskID]
		if !ok {
			return TaskEntry{}, false
		}
		r.mu.Unlock()
		ctx, cancel := r.sharedCtx()
		entry, _, err := r.shared.ConfirmStart(ctx, e.WorkspaceID, taskID, at, true)
		cancel()
		r.mu.Lock()
		if err != nil {
			r.stateFailure("started", taskID, err)
			return TaskEntry{}, false
		}
		if entry.TaskID == "" || entry.State == StateTerminal {
			return TaskEntry{}, false
		}
		r.cacheEntryLocked(entry)
		return entry, true
	}
	e, ok := r.startFactLocked(taskID, at)
	if !ok {
		return TaskEntry{}, false
	}
	e.State = StateRunning
	r.live[taskID] = e
	r.persist(taskID, e)
	return e, true
}

// MarkStartFact records the start fact without advancing the state
// (ADR-026 决策结果 1): adoption observes a post-delivery server status, it is
// not a report, and the projection must keep showing the rebuild's initial
// phase. started_at is written once; a terminal entry is never reopened.
func (r *Registry) MarkStartFact(taskID string, at time.Time) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shared != nil {
		e, ok := r.live[taskID]
		if !ok {
			return TaskEntry{}, false
		}
		r.mu.Unlock()
		ctx, cancel := r.sharedCtx()
		entry, _, err := r.shared.ConfirmStart(ctx, e.WorkspaceID, taskID, at, false)
		cancel()
		r.mu.Lock()
		if err != nil {
			r.stateFailure("start_fact", taskID, err)
			return TaskEntry{}, false
		}
		if entry.TaskID == "" || entry.State == StateTerminal {
			return TaskEntry{}, false
		}
		r.cacheEntryLocked(entry)
		return entry, true
	}
	e, ok := r.startFactLocked(taskID, at)
	if ok {
		r.persist(taskID, e)
	}
	return e, ok
}

// startFactLocked applies the once-only started_at write; the caller holds
// r.mu and decides whether the state moves.
func (r *Registry) startFactLocked(taskID string, at time.Time) (TaskEntry, bool) {
	e, ok := r.live[taskID]
	if !ok || e.State == StateTerminal {
		return TaskEntry{}, false
	}
	if e.StartedAt.IsZero() {
		e.StartedAt = at
	}
	r.live[taskID] = e
	return e, true
}

// SetNode records the node a task's pod landed on. It is the placement fact
// only: the per-node soft cap counts it, and the entry's terminal report is
// what feeds the session index (LearnSession) — placement itself never
// learns, because a Job is built before its pod is scheduled.
func (r *Registry) SetNode(taskID, nodeName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live[taskID]
	if !ok {
		return
	}
	e.NodeName = nodeName
	r.live[taskID] = e
	if r.shared != nil {
		// §4.1/§4.5: node_name is read back through the shared row by the
		// daemon report path (session learning) and the ops projection, so it
		// must not stay process-local in stage two.
		r.mergeShared(taskID, e, EntryPatch{NodeName: &nodeName})
	}
}

// ActiveJobsByNode counts the non-terminal entries placed on each node
// (§4.2: Inflight semantics). Entries whose pod has not landed yet
// carry no node_name and are not counted, so the load appears one reconcile
// round after placement. It is the load input of the per-node soft cap
// (ADR-006 §决策结果 3).
func (r *Registry) ActiveJobsByNode() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	byNode := make(map[string]int)
	for _, e := range r.live {
		if e.State == StateTerminal || e.NodeName == "" {
			continue
		}
		byNode[e.NodeName]++
	}
	return byNode
}

// SetSaturatedNodes replaces the saturation markers (§8.6 饱和抑制:
// Reconcile marks the nodes at or over FOREMAN_MAX_JOBS_PER_NODE). The whole
// set is replaced because a node that dropped below the cap must lose its
// marker again; replacement also prunes nodes that left the index.
func (r *Registry) SetSaturatedNodes(nodes []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		if n != "" {
			next[n] = true
		}
	}
	r.saturated = next
}

// SaturatedNodes returns the current saturation markers, sorted.
func (r *Registry) SaturatedNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.saturated))
	for n := range r.saturated {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Saturated implements jobbuilder.NodeIndex: the node currently sits at or
// over the per-node soft cap, so its reuse affinity must be dropped (soft
// constraint — the Job is still created, no admission control).
func (r *Registry) Saturated(nodeName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.saturated[nodeName]
}

func (r *Registry) dropIndexesLocked(e TaskEntry) {
	delete(r.byJob, e.JobName)
	delete(r.byDaemon, e.DaemonID)
}

// PutRuntime upserts the fake runtime of a Job's daemon (S4 registration,
// §2.3).
func (r *Registry) PutRuntime(rt JobRuntime) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runtimes[rt.JobRuntimeID] = rt
	if rt.DaemonID != "" {
		r.runtimeByDaemon[rt.DaemonID] = rt.JobRuntimeID
	}
}

// RuntimeByID returns the job runtime registered under jobRuntimeID.
func (r *Registry) RuntimeByID(jobRuntimeID string) (JobRuntime, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rt, ok := r.runtimes[jobRuntimeID]
	return rt, ok
}

// RuntimeByDaemon returns the job runtime of daemonID.
func (r *Registry) RuntimeByDaemon(daemonID string) (JobRuntime, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rt, ok := r.runtimes[r.runtimeByDaemon[daemonID]]
	return rt, ok
}

// TouchHeartbeat refreshes the liveness timestamp of a job runtime; S5
// heartbeats and WS heartbeat acks share this single clock (scenario #6).
func (r *Registry) TouchHeartbeat(jobRuntimeID string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rt, ok := r.runtimes[jobRuntimeID]
	if !ok {
		return
	}
	rt.LastHeartbeatAt = at
	rt.Online = true
	r.runtimes[jobRuntimeID] = rt
}

// SetRuntimeDeliveredTask records the task the runtime's last delivery handed
// over, without touching the heartbeat fields. A read-modify-write from a
// snapshot would write back a stale LastHeartbeatAt/Online and lose a
// concurrent heartbeat; every field write on a runtime row goes through a
// lock-scoped setter like this one.
func (r *Registry) SetRuntimeDeliveredTask(jobRuntimeID, taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rt, ok := r.runtimes[jobRuntimeID]
	if !ok {
		return
	}
	rt.DeliveredTaskID = taskID
	r.runtimes[jobRuntimeID] = rt
}

// SetRuntimeOffline marks the runtime row offline (S19), preserving every
// other field under the same lock.
func (r *Registry) SetRuntimeOffline(jobRuntimeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rt, ok := r.runtimes[jobRuntimeID]
	if !ok {
		return
	}
	rt.Online = false
	r.runtimes[jobRuntimeID] = rt
}

// DeleteRuntime removes a job runtime and its daemon reverse index.
func (r *Registry) DeleteRuntime(jobRuntimeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dropRuntimeLocked(jobRuntimeID, "")
}

// dropRuntimeLocked removes the runtime row; daemonID may be empty (looked
// up from the row). Callers hold the lock.
func (r *Registry) dropRuntimeLocked(jobRuntimeID, daemonID string) {
	if jobRuntimeID == "" {
		return
	}
	rt, ok := r.runtimes[jobRuntimeID]
	if !ok {
		return
	}
	if daemonID == "" {
		daemonID = rt.DaemonID
	}
	delete(r.runtimes, jobRuntimeID)
	delete(r.runtimeByDaemon, daemonID)
}
