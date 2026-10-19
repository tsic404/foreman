package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// DoneIndexTTL is the retention of terminal task IDs (§4.3); within
// the window the index deduplicates late reports and repeated claims.
const DoneIndexTTL = 24 * time.Hour

// ErrTerminalImmutable rejects a write that would move a terminal entry back
// to a non-terminal state (§4.2 invariant: terminal is irreversible).
var ErrTerminalImmutable = errors.New("terminal entry cannot become non-terminal")

// Registry is the in-memory mapping index. The K8s Job objects are the
// durable truth (ADR-002); nothing here is persisted. Entries stay in the
// live index until Delete — including terminal entries in their drain window,
// whose credential is still valid — so Inflight counts only non-terminal
// rows.
type Registry struct {
	mu   sync.RWMutex
	now  func() time.Time
	live map[string]TaskEntry // task_id → entry (until Delete)

	// drainTimeout is FOREMAN_JOB_DRAIN_TIMEOUT: the upper bound written once
	// at the alive → draining transition (ADR-017).
	drainTimeout time.Duration

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
	if e.JobName != "" {
		r.byJob[e.JobName] = e.TaskID
	}
	if e.DaemonID != "" {
		r.byDaemon[e.DaemonID] = e.TaskID
	}
	return nil
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

// IsDone reports whether taskID reached terminal state within the retention
// window; it backs the late-replay idempotent path and claim dedup.
func (r *Registry) IsDone(taskID string) bool {
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

// MarkTerminal is the single write path into the terminal state
// (§4.2 markTerminal): it sets state+result atomically with the settled
// record. completedAt defaults to now. reason is the
// externally observable attribution of a failure Foreman judged itself; an
// empty one leaves any recorded attribution in place, and the server's own
// verdicts carry none (§4.1).
//
// It is also the drain entry point (ADR-017): the alive → draining transition
// writes drain_deadline, the completion time and the settled record exactly
// once, so a repeated terminal report (daemon retries, pending-report
// replays) only overwrites the result and can neither reset the upper bound
// nor restart the drain duration.
func (r *Registry) MarkTerminal(taskID string, result Result, reason string, completedAt time.Time) (TaskEntry, error) {
	if completedAt.IsZero() {
		completedAt = r.now()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live[taskID]
	if !ok {
		return TaskEntry{}, fmt.Errorf("unknown task %s", taskID)
	}
	first := e.State != StateTerminal
	if first {
		e.JobPhase = JobPhaseDraining
		e.DrainDeadline = r.now().Add(r.drainTimeout)
		e.CompletedAt = completedAt
	}
	e.State = StateTerminal
	e.Result = result
	if reason != "" {
		e.FailureReason = reason
	}
	if first {
		// The settled record is the projection at the verdict instant: it is
		// what the ops endpoint keeps answering with once the credential
		// reclaim deletes the live row (ADR-032).
		r.done.put(taskID, e.CompletedAt, settledRecord(e))
	}
	r.live[taskID] = e
	return e, nil
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
	if e.State == StatePending {
		e.State = StateJobCreated
	}
	e.JobCreatedAt = at
	r.live[taskID] = e
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
	if e.State == StateJobCreated || e.State == StateDaemonRegistered {
		e.State = StateDaemonRegistered
	}
	e.DaemonRegisteredAt = at
	r.live[e.TaskID] = e
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
	if !ok || !deliverable(e, at) {
		return TaskEntry{}, false
	}
	e.State = StateDelivering
	e.DeliveredAt = at
	e.Deliveries++
	e.Payload = delivered
	if r.deliveryConfirm > 0 {
		e.DeliveryDeadline = at.Add(r.deliveryConfirm)
	}
	r.live[e.TaskID] = e
	return e, true
}

// deliverable is the S6 gate: the entry's first delivery, or the re-delivery
// an expired confirmation window released. A started, terminal or draining
// entry is never deliverable, and so is a delivered entry whose window is
// still open — the commit the daemon has not answered yet (ADR-032).
func deliverable(e TaskEntry, at time.Time) bool {
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
	if e, ok := r.live[taskID]; ok {
		e.ClaimsAfterDelivery++
		r.live[taskID] = e
	}
}

// MarkStarted records S8 under the registry lock so a concurrent terminal
// write always wins over a late start. started_at is set once; the state
// advances to running from any non-terminal state (a rebuilt job_created
// entry must accept its running daemon's reports).
func (r *Registry) MarkStarted(taskID string, at time.Time) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live[taskID]
	if !ok || e.State == StateTerminal {
		return TaskEntry{}, false
	}
	if e.StartedAt.IsZero() {
		e.StartedAt = at
	}
	e.State = StateRunning
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
