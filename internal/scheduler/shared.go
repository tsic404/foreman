package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
)

// ErrNoSlot reports a claim refused because the shared two-level counters had
// no free slot. The task stays with the server (it is re-dispatched after the
// claim's lease lapses); Foreman never pre-fetches (AC-02, AC-76).
var ErrNoSlot = errors.New("no in-flight slot available")

// ErrStoreUnavailable reports a decision that needs the shared state while the
// store cannot answer. The daemon face fails closed on it (§10 共享状态不可用).
var ErrStoreUnavailable = errors.New("shared state unavailable")

// Scheduler-side constants of the stage-two faces.
const (
	sharedCallTimeout  = 2 * time.Second
	defaultTaskLockTTL = 120 * time.Second
)

func (s *Scheduler) sharedCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return context.WithTimeout(context.Background(), sharedCallTimeout)
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, sharedCallTimeout)
}

// lockTaskFor takes the write lock of a task this process already knows; the
// workspace comes from the local row.
func (s *Scheduler) lockTaskFor(ctx context.Context, taskID string) (func(), error) {
	ws := ""
	if e, ok := s.reg.Get(taskID); ok {
		ws = e.WorkspaceID
	}
	return s.lockTaskIn(ctx, ws, taskID)
}

// lockTaskIn takes the per-task write lock: the local keyed mutex serializes
// this process's paths, the shared lease serializes the same task across
// instances (key contains the workspace; ADR-046 决策 6, 02-contracts.md
// §4.2 跨实例单写). A store failure fails the action closed.
func (s *Scheduler) lockTaskIn(ctx context.Context, ws, taskID string) (func(), error) {
	if s.shared == nil {
		return s.taskMu.Lock(taskID), nil
	}
	owner := s.instanceID
	ttl := s.taskLockTTL
	if ttl <= 0 {
		ttl = defaultTaskLockTTL
	}
	started := time.Now()
	for {
		lockCtx, cancel := s.sharedCtx(ctx)
		ok, err := s.shared.LockTask(lockCtx, ws, taskID, owner, ttl)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("%w: task lock: %v", ErrStoreUnavailable, err)
		}
		if ok {
			break
		}
		if ctx != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if waited := time.Since(started); waited > time.Millisecond {
		s.metrics.TaskLockWait(waited.Seconds())
	}
	unlockLocal := s.taskMu.Lock(taskID)
	return func() {
		unlockLocal()
		if s.shared != nil {
			releaseCtx, cancel := sharedDetachedCtx()
			defer cancel()
			_ = s.shared.UnlockTask(releaseCtx, ws, taskID, owner)
		}
	}, nil
}

func sharedDetachedCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), sharedCallTimeout)
}

// reserveSlots takes one slot on both levels (AC-76). A refused workspace
// reservation returns the global one it already took.
func (s *Scheduler) reserveSlots(ctx context.Context, ws string) error {
	if s.shared == nil {
		return nil
	}
	callCtx, cancel := s.sharedCtx(ctx)
	defer cancel()
	ok, err := s.shared.ReserveSlot(callCtx, "", s.cfg.MaxInflightJobs)
	if err != nil {
		s.metrics.StateError("reserve_slot")
		return fmt.Errorf("%w: reserve global slot: %v", ErrStoreUnavailable, err)
	}
	if !ok {
		return ErrNoSlot
	}
	if s.cfg.MaxInflightJobsPerWorkspace > 0 && ws != "" {
		ok, err = s.shared.ReserveSlot(callCtx, ws, s.cfg.MaxInflightJobsPerWorkspace)
		if err != nil {
			s.releaseGlobalReservation()
			return fmt.Errorf("%w: reserve workspace slot: %v", ErrStoreUnavailable, err)
		}
		if !ok {
			s.releaseGlobalReservation()
			return ErrNoSlot
		}
	}
	return nil
}

// releaseGlobalReservation returns the global slot whose workspace half was
// refused. The write is detached: the refusal usually means the call budget is
// spent, so inheriting that context would make the compensation itself fail
// and leak the reservation until the TTL/reconcile edge.
func (s *Scheduler) releaseGlobalReservation() {
	callCtx, cancel := sharedDetachedCtx()
	defer cancel()
	_ = s.shared.ReleaseSlot(callCtx, "")
}

// consumeSlots turns one reservation into entry ownership on both levels. The
// release-style writes use a detached context: a cancelled claim loop must
// still return its slots.
//
// A failed consume is reported but never retried. Popping a reservation is not
// idempotent: a response lost after the pop was applied would make the retry
// pop a different pending claim's reservation at the same level, which is a
// slot that claim still owns. The unapplied case heals on its own — the
// member is still pending, so the expiry purge lowers the counter for it, and
// the periodic reconcile raises a counter that fell below the live rows.
func (s *Scheduler) consumeSlots(ctx context.Context, ws string) {
	if s.shared == nil {
		return
	}
	if s.cfg.MaxInflightJobsPerWorkspace > 0 && ws != "" {
		s.consumeSlotLevel(ws)
	}
	s.consumeSlotLevel("")
}

// consumeSlotLevel pops one reservation on one level, single attempt: a retry
// would consume another claim's pending reservation (see consumeSlots). The
// failure is still visible (§9.1 共享状态错误; §10 两级在途上限).
func (s *Scheduler) consumeSlotLevel(ws string) {
	callCtx, cancel := sharedDetachedCtx()
	defer cancel()
	err := s.shared.ConsumeSlotReservation(callCtx, ws)
	if err != nil {
		s.metrics.StateError("consume_slot")
		s.log.Warn("coordination.consume_slot_failed",
			"workspace_id", ws, "err", err.Error())
	}
}

// releaseSlots abandons one reservation on both levels. The write is
// detached from the caller's context: a claim loop cancelled mid-batch (or a
// shutting-down process) must not leak its reservation.
func (s *Scheduler) releaseSlots(ctx context.Context, ws string) {
	if s.shared == nil {
		return
	}
	callCtx, cancel := sharedDetachedCtx()
	defer cancel()
	if s.cfg.MaxInflightJobsPerWorkspace > 0 && ws != "" {
		_ = s.shared.ReleaseSlot(callCtx, ws)
	}
	_ = s.shared.ReleaseSlot(callCtx, "")
}

// ReserveClaimSlots reserves up to want slots before a C3 call (§10: claim 前
// 必须持有槽位预留). It returns how many were taken; a reached cap returns
// fewer (0 when full), which the caller uses as the claim batch size.
func (s *Scheduler) ReserveClaimSlots(ctx context.Context, ws string, want int) (int, error) {
	if s.shared == nil {
		return want, nil
	}
	if want <= 0 {
		return 0, nil
	}
	got := 0
	for range want {
		if err := s.reserveSlots(ctx, ws); err != nil {
			if errors.Is(err, ErrNoSlot) {
				break
			}
			s.ReleaseClaimSlots(ctx, ws, got)
			return 0, err
		}
		got++
	}
	return got, nil
}

// ReleaseClaimSlots returns n reservations the C3 call did not fill.
func (s *Scheduler) ReleaseClaimSlots(ctx context.Context, ws string, n int) {
	for range n {
		s.releaseSlots(ctx, ws)
	}
}

// The raw Scheduler also serves the single-workspace claim loop directly (the
// harness and the legacy wiring): the workspace is the legacy configured one.
func (s *Scheduler) ReserveSlots(ctx context.Context, want int) (int, error) {
	return s.ReserveClaimSlots(ctx, s.cfg.WorkspaceID, want)
}

// ReleaseSlots returns reservations the single-workspace loop did not fill.
func (s *Scheduler) ReleaseSlots(ctx context.Context, n int) {
	s.ReleaseClaimSlots(ctx, s.cfg.WorkspaceID, n)
}

// OnClaimReserved registers a claimed task for the legacy single workspace.
func (s *Scheduler) OnClaimReserved(ctx context.Context, task json.RawMessage) error {
	return s.OnClaimReservedFor(ctx, "", task)
}

// ReconcileSlots is the leader's periodic two-level reconciliation (§10 计数按
// 条目周期对账): it only lowers a counter, because a claim's reservation is
// taken before its row lands and must never be read as a leak.
func (s *Scheduler) ReconcileSlots(ctx context.Context) error {
	return s.reconcileSlots(ctx, false)
}

// Delivery is the outcome of one S-6 delivery attempt (§5).
type Delivery struct {
	Tasks     []json.RawMessage
	Delivered bool
	Held      bool
	// Deliveries is the delivery count carried by the entry after the take
	// (1 on the first delivery, >1 on a re-delivery).
	Deliveries int
}

// Deliver hands the Job's task to its daemon (S6). In stage two the take is
// the shared queue's atomic pickup: any instance serves the Job, and the
// confirmation window/lease and re-delivery live in the shared row. Without a
// shared store the stage-one in-memory gate applies.
func (s *Scheduler) Deliver(ctx context.Context, workspaceID, daemonID, runtimeID string, maxTasks int) (Delivery, error) {
	if maxTasks < 1 {
		return Delivery{}, nil
	}
	e, ok := s.reg.ByDaemonInWorkspace(workspaceID, daemonID)
	if !ok {
		return Delivery{}, fmt.Errorf("%w: %s", ErrUnknownDaemon, daemonID)
	}
	if runtimeID != e.JobRuntimeID {
		return Delivery{}, fmt.Errorf("%w: %s", ErrRuntimeMismatch, runtimeID)
	}
	if s.shared != nil {
		return s.deliverShared(ctx, e)
	}
	if len(e.Payload) == 0 {
		s.log.Warn("task.payload_missing",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID)
		return Delivery{}, nil
	}
	out, err := s.rewriteRuntime(e.Payload, e.JobRuntimeID)
	if err != nil {
		return Delivery{}, fmt.Errorf("parse stored payload for task %s: %w", e.TaskID, err)
	}
	prevDelivered := e.DeliveredAt
	prevClaims := e.ClaimsAfterDelivery
	now := s.now()
	e, ok = s.reg.MarkDelivering(daemonID, now, out)
	if !ok {
		return Delivery{}, nil
	}
	s.reg.SetRuntimeDeliveredTask(e.JobRuntimeID, e.TaskID)
	s.logDelivery(e, prevDelivered, prevClaims, now)
	return Delivery{Tasks: []json.RawMessage{out}, Delivered: true, Deliveries: e.Deliveries}, nil
}

// deliverShared performs the atomic shared pickup and mirrors the result into
// the local cache. The pickup itself decides first-delivery vs re-delivery vs
// empty; the payload comes from the shared row, because the row is the
// authority on this instance's copy.
func (s *Scheduler) deliverShared(ctx context.Context, e registry.TaskEntry) (Delivery, error) {
	callCtx, cancel := s.sharedCtx(ctx)
	defer cancel()
	pickup, err := s.shared.PickupDelivery(callCtx, e.WorkspaceID, e.TaskID, s.cfg.DeliveryConfirmTimeout)
	if err != nil {
		return Delivery{}, fmt.Errorf("%w: delivery pickup: %v", ErrStoreUnavailable, err)
	}
	if pickup.Delivered {
		payload := pickup.Entry.Payload
		out, err := s.rewriteRuntime(payload, pickup.Entry.JobRuntimeID)
		if err != nil {
			// The stored body is unparsable: the row is kept and the boot
			// bound converges it; nothing is handed over.
			return Delivery{}, fmt.Errorf("parse stored payload for task %s: %w", e.TaskID, err)
		}
		e = pickup.Entry
		s.reg.Cache(e)
		s.reg.SetRuntimeDeliveredTask(e.JobRuntimeID, e.TaskID)
		if e.Deliveries > 1 {
			s.metrics.QueueOp("redelivered")
		} else {
			s.metrics.QueueOp("taken")
		}
		s.syncQueueDepth(ctx)
		// The rearm event reports the commit this take released, not the new
		// one (§4.1 交付后的 claim 计数).
		s.logDelivery(e, pickup.PrevDeliveredAt, pickup.PrevClaims, s.now())
		return Delivery{Tasks: []json.RawMessage{out}, Delivered: true, Deliveries: e.Deliveries}, nil
	}
	if pickup.Held {
		s.reg.Cache(pickup.Entry)
	}
	return Delivery{Held: pickup.Held}, nil
}

// rewriteRuntime applies the one field S-6 may change (the runtime id), keeping
// every other field's value (§2.5).
func (s *Scheduler) rewriteRuntime(payload json.RawMessage, jobRuntimeID string) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, err
	}
	runtimeRaw, err := json.Marshal(jobRuntimeID)
	if err != nil {
		return nil, err
	}
	fields["runtime_id"] = runtimeRaw
	return json.Marshal(fields)
}

func (s *Scheduler) logDelivery(e registry.TaskEntry, prevDelivered time.Time, prevClaims int, now time.Time) {
	s.log.Info("task.delivered",
		"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
		"workspace_id", e.WorkspaceID,
		"deliver_latency_ms", now.Sub(e.ClaimedAt).Milliseconds())
	if e.Deliveries > 1 {
		s.log.Warn("task.delivery_rearmed",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
			"delivered_at", registry.ProjectedTime(prevDelivered),
			"window_seconds", e.DeliveryDeadline.Sub(e.DeliveredAt).Seconds(),
			"claims_after_delivery", prevClaims,
			"deliveries", e.Deliveries)
	}
}

// enqueueShared puts a freshly claimed task into the shared queue (§5 单次入队).
func (s *Scheduler) enqueueShared(ctx context.Context, e registry.TaskEntry) error {
	if s.shared == nil {
		return nil
	}
	callCtx, cancel := s.sharedCtx(ctx)
	defer cancel()
	created, err := s.shared.EnqueueEntry(callCtx, e.WorkspaceID, e.TaskID, e)
	if err != nil {
		s.metrics.StateError("enqueue")
		return fmt.Errorf("%w: enqueue: %v", ErrStoreUnavailable, err)
	}
	if created {
		s.metrics.QueueOp("enqueued")
	} else {
		s.metrics.QueueOp("idempotent")
	}
	s.syncQueueDepth(ctx)
	if !created {
		s.log.Info("task.enqueue_idempotent",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
			"workspace_id", e.WorkspaceID)
	}
	return nil
}

// OnTakeover reconciles the shared state before the new control plane serves
// (AC-77): the local index is replaced by the shared rows, the two-level
// counters are rebuilt from them, and pre-delivery rows missing a queue marker
// are re-enqueued. It runs on the leader after the takeover window.
func (s *Scheduler) OnTakeover(ctx context.Context) error {
	if s.shared == nil {
		return nil
	}
	started := time.Now()
	result := "ok"
	defer func() {
		s.metrics.ReconcileResult(result, time.Since(started).Seconds())
	}()
	if err := s.reg.Resync(ctx); err != nil {
		result = "failed"
		return fmt.Errorf("resync shared entries: %w", err)
	}
	// §4.3 阶段二重建职责: the takeover fills the rows the store no longer
	// holds from the K8s objects (idempotent; a row restored from the store
	// is never replaced by its payload-less projection).
	if err := s.rebuildFromK8s(ctx); err != nil {
		result = "failed"
		return fmt.Errorf("rebuild from k8s on takeover: %w", err)
	}
	if err := s.pendingReports.Restore(ctx); err != nil {
		s.log.ErrorContext(ctx, "pending.restore_failed", "err", err)
	}
	// The takeover is the exact-write point: the previous holder is gone and
	// this instance is not serving yet, so the counters must match the rows
	// in both directions (a data loss leaves them at zero while the rebuilt
	// rows are live — AC-76/AC-78).
	if err := s.reconcileSlots(ctx, true); err != nil {
		return err
	}
	for _, e := range s.reg.List() {
		if e.IsTerminal() || e.IsDraining() || !e.StartedAt.IsZero() {
			continue
		}
		if err := s.enqueueShared(ctx, e); err != nil {
			result = "failed"
			return err
		}
	}
	s.syncQueueDepth(ctx)
	s.log.InfoContext(ctx, "coordination.takeover_reconciled", "entries", len(s.reg.List()))
	return nil
}

// syncQueueDepth publishes the live queue-row gauge (§9.2 条目数). It is
// called on the lifecycle edges (enqueue, take, takeover), not per request.
func (s *Scheduler) syncQueueDepth(ctx context.Context) {
	if s.shared == nil {
		return
	}
	callCtx, cancel := s.sharedCtx(ctx)
	defer cancel()
	if depth, err := s.shared.QueueDepth(callCtx); err == nil {
		s.metrics.QueueDepth(int(depth))
	}
}

// reconcileSlots rebuilds the shared two-level counters from the live rows
// plus the live reservations.
//
// exact=true writes the truth in both directions: the takeover / startup path
// uses it after a data loss (the store's counters are gone while the K8s rows
// are rebuilt), where no concurrent claim can exist because the caller is not
// yet the serving holder. exact=false is the periodic leader-round repair:
//
//   - a counter above the truth is a leak and is lowered;
//   - a counter below the live-row count is a lost decrement (a failed
//     reservation consume) and is raised to the row lower bound — a live row
//     always owns a slot, so the rows alone are a safe floor;
//   - the write is a compare-and-set against the atomic counter snapshot, so
//     a claim reservation taken between the read and the write makes the CAS
//     decline that round instead of losing its +1.
//
// Every level's snapshot is taken atomically (purge + counter + reservations)
// before the single rows traversal, so a reservation converted in between is
// counted twice — the periodic repair only needs that direction to be safe.
// Every configured workspace is reconciled, including one with no live row: a
// reservation left by a dead holder must not survive as a permanent budget
// loss (AC-76). An empty workspace id is not a level — such a row counts in
// the global total only, never as a workspace key that could overwrite it.
func (s *Scheduler) reconcileSlots(ctx context.Context, exact bool) error {
	if s.shared == nil {
		return nil
	}
	callCtx, cancel := s.sharedCtx(ctx)
	defer cancel()

	levels := []string{""}
	for _, ws := range s.WorkspaceIDs() {
		if ws != "" {
			levels = append(levels, ws)
		}
	}

	type levelSnapshot struct {
		count        int64
		reservations int64
	}
	snapshots := make(map[string]levelSnapshot, len(levels))
	for _, ws := range levels {
		if exact {
			// The previous holder is gone: its pending reservations cannot
			// become rows, so they are reclaimed now (AC-76).
			if err := s.shared.ClearSlotReservations(callCtx, ws); err != nil {
				s.metrics.StateError("clear_slot_reservations")
				return fmt.Errorf("%w: clear slot reservations: %v", ErrStoreUnavailable, err)
			}
		}
		count, reservations, err := s.shared.SlotSnapshot(callCtx, ws)
		if err != nil {
			s.metrics.StateError("read_slots")
			return fmt.Errorf("%w: read slot snapshot: %v", ErrStoreUnavailable, err)
		}
		snapshots[ws] = levelSnapshot{count: count, reservations: reservations}
		s.metrics.SharedSlots(ws, int(count))
	}

	// One traversal: rows are read after every level's snapshot (see above).
	total := int64(0)
	perWS := make(map[string]int64, len(levels))
	for _, e := range s.reg.List() {
		if e.State == registry.StateTerminal {
			continue
		}
		total++
		if e.WorkspaceID != "" {
			perWS[e.WorkspaceID]++
		}
	}

	reconcileLevel := func(ws string, rows int64) error {
		snap := snapshots[ws]
		next := int64(0)
		write := false
		switch {
		case exact:
			next = rows + snap.reservations
			write = true
		case snap.count > rows+snap.reservations:
			// A leak: lower to the row + live-reservation truth.
			next = rows + snap.reservations
			write = true
		case snap.count < rows:
			// A lost decrement (e.g. a failed reservation consume): a live
			// row always owns a slot, so the row count is a safe floor.
			next = rows
			write = true
		}
		if !write {
			return nil
		}
		if exact {
			if err := s.shared.SetSlots(callCtx, ws, next); err != nil {
				s.metrics.StateError("reconcile_slots")
				return fmt.Errorf("%w: reconcile slots: %v", ErrStoreUnavailable, err)
			}
		} else {
			swapped, err := s.shared.CompareAndSetSlots(callCtx, ws, snap.count, next)
			if err != nil {
				s.metrics.StateError("reconcile_slots")
				return fmt.Errorf("%w: reconcile slots: %v", ErrStoreUnavailable, err)
			}
			if !swapped {
				// A claim moved the counter after the snapshot: this round has
				// no safe value to write (the next round recomputes).
				return nil
			}
		}
		fields := []any{"level", "workspace", "workspace_id", ws, "before", snap.count, "after", next}
		if ws == "" {
			fields = []any{"level", "global", "before", snap.count, "after", next}
		}
		s.log.WarnContext(ctx, "coordination.slots_reconciled", fields...)
		s.metrics.SharedSlots(ws, int(next))
		return nil
	}

	if err := reconcileLevel("", total); err != nil {
		return err
	}
	for _, ws := range levels[1:] {
		if err := reconcileLevel(ws, perWS[ws]); err != nil {
			return err
		}
	}
	return nil
}

// AttachShared attaches a follower instance to the shared state: it restores
// the live rows into the local index so it can serve any Job's daemon face,
// but it does not rebuild from K8s and does not converge — those are the
// leader's actions (§4.3 阶段二重建职责).
func (s *Scheduler) AttachShared(ctx context.Context) error {
	s.lifecycleMu.Lock()
	s.lifecycle = ctx
	s.lifecycleMu.Unlock()
	if s.shared == nil {
		return nil
	}
	if _, ok, err := s.reg.Restore(ctx); err != nil {
		return fmt.Errorf("restore shared entries: %w", err)
	} else if !ok {
		return errors.New("attach: the shared state face is not configured")
	}
	return nil
}

// ContinueClaim continues a claim whose Job was never created: the entry and
// its payload are in the shared row, the object is absent, and the
// convergence round recreates both idempotently (§4.3 未完成创建的接续). It
// returns nil when there is nothing to continue or the continuation landed;
// an error keeps the entry for the next round.
func (s *Scheduler) ContinueClaim(ctx context.Context, taskID string) error {
	unlock, err := s.lockTaskFor(ctx, taskID)
	if err != nil {
		return err
	}
	defer unlock()
	e, ok := s.reg.Get(taskID)
	if !ok || e.IsTerminal() || e.IsDraining() || !e.StartedAt.IsZero() || !e.DeliveredAt.IsZero() || len(e.Payload) == 0 {
		return nil
	}
	if e.State != registry.StatePending {
		// job_created/daemon_registered say the object exists: the
		// continuation's window is between the row's persistence and that
		// transition. A Job that vanished afterwards belongs to the
		// missing-Job compensation, not here.
		return nil
	}
	job, err := s.jobs.GetJob(ctx, e.JobName)
	if err != nil {
		return fmt.Errorf("continue claim: get job %s: %w", e.JobName, err)
	}
	if job != nil {
		// The object exists: the claim's crash landed before the transition.
		if _, ok := s.reg.MarkJobCreated(taskID, job.CreationTimestamp.Time); !ok {
			s.log.WarnContext(ctx, "task.claim_continue_skipped", "task_id", taskID, "reason", "entry changed")
		}
		return nil
	}
	s.log.InfoContext(ctx, "task.claim_continued",
		"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
		"workspace_id", e.WorkspaceID)
	built, secret, tokenHash, err := s.builder.Build(jobbuilderEntry(e))
	if err != nil {
		s.failClaim(ctx, e, err)
		return err
	}
	if err := s.reg.SetTokenHash(e.TaskID, tokenHash); err != nil {
		// The row cannot carry the digest, so issuing the credential would
		// leave no instance able to adjudicate or revoke it (ADR-046 决策 7).
		// Nothing has been created yet; the next round retries the continuation.
		return fmt.Errorf("%w: record token digest: %v", ErrStoreUnavailable, err)
	}
	_ = s.jobs.DeleteSecret(ctx, jobbuilder.CredentialSecretName(e.DaemonID))
	if err := s.jobs.CreateSecret(ctx, secret); err != nil {
		s.failClaim(ctx, e, fmt.Errorf("create credential secret: %w", err))
		return err
	}
	created, err := s.createJob(ctx, e, built)
	if err != nil {
		_ = s.jobs.DeleteSecret(ctx, jobbuilder.CredentialSecretName(e.DaemonID))
		if errors.Is(err, errJobNameHeld) {
			// A terminating predecessor still holds the name: the next round
			// continues, never a failure (the claim-deferral face).
			return err
		}
		s.failClaim(ctx, e, fmt.Errorf("create job: %w", err))
		return err
	}
	if _, ok := s.reg.MarkJobCreated(e.TaskID, created.CreationTimestamp.Time); !ok {
		return s.deleteObjects(ctx, e)
	}
	return nil
}
