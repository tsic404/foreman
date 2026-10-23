// Package recovery implements the failure-compensation and recovery side of
// Foreman: the restart/failure reconciler over the Job/Pod facts and the
// server's C13 truth, the K8s object watcher, the heartbeat-suspect signal
// and the cancel-ack timeout. The durable terminal-report queue is the
// scheduler's own (ADR-031) and is drained through the settlement surface.
// Design: 02-contracts.md §4.2, §10.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// Timing defaults of the failure-recovery paths. They are design
// constants (only the interval and the recheck window are configurable); the
// reconciler carries them as fields so a caller or its own tests can shrink
// them, and every timer reads the real clock so a shrunk window is honoured
// by the loop.
const (
	// defaultRecheckWindow gives a daemon that just lost its container the
	// chance to land its terminal report before Foreman synthesizes one
	// (§4.6).
	defaultRecheckWindow = 10 * time.Second
	// defaultCancelAckTimeout is how long a server-side cancelled task waits
	// for its daemon's cancel-ack before its objects are removed (§4.6).
	defaultCancelAckTimeout = 60 * time.Second
	// defaultSuspectThreshold is the two-channel heartbeat silence after
	// which a task is flagged suspect — signalled, never acted on (§4.6).
	defaultSuspectThreshold = 60 * time.Second
	// defaultJobGoneWindow is how long a Job must be observed missing before
	// the missing-Job compensation engages. Measured from the first missing
	// observation, never from the claim: a long-lived Job that briefly
	// disappears from the API or the informer view must not be failed on the
	// spot.
	defaultJobGoneWindow = 60 * time.Second
	// cleanupRetryLimit bounds the object-deletion retries of a terminal
	// entry (顺序与幂等规则 2: 每轮重试删除，最多 10 轮). When the budget is
	// spent the Job is left to its ttlSecondsAfterFinished and the entry
	// stays as the handle a restart re-attempts with.
	cleanupRetryLimit = 10
)

// C13 statuses the convergence switches on (§1.2).
const (
	statusRunning               = "running"
	statusQueued                = "queued"
	statusDispatched            = "dispatched"
	statusWaitingLocalDirectory = "waiting_local_directory"
	statusCancelled             = "cancelled"
)

// StatusClient is the recovery module's view of the fake client's C13
// query (proxy.Client; a task the server deleted maps to
// scheduler.ErrTaskNotFound).
type StatusClient interface {
	TaskStatus(ctx context.Context, taskID string) (string, error)
}

// Watcher triggers a convergence round on Job/Pod changes so the loop does
// not wait for the next tick (the Job watcher). The
// reconciler re-reads the object state itself, so a missed or stale event
// costs latency, never correctness.
type Watcher interface {
	Watch(ctx context.Context, notify func()) error
}

// Metrics is the recovery module's observability seam (§9.2).
// The observability module provides the Prometheus implementation; nil
// degrades to a no-op.
type Metrics interface {
	// HeartbeatSuspect sets how many tasks currently fail the heartbeat
	// freshness check (foreman_heartbeat_suspect).
	HeartbeatSuspect(n int)
}

type noopMetrics struct{}

func (noopMetrics) HeartbeatSuspect(int) {}

// Reconciler is the convergence driver:
// every round settles each live entry against the Job/Pod facts, the
// server's C13 truth and the daemon's own reports, drains the durable
// terminal-report queue and refreshes the heartbeat-suspect signal.
type Reconciler struct {
	cfg     Config
	reg     *registry.Registry
	objects ObjectClient
	server  StatusClient
	settler Settler
	comp    *Compensator
	metrics Metrics
	log     *slog.Logger

	// Timing of the deferred settlements. The recheck window is the one the
	// config carries (§4.6, 10s unless a caller injects one); the rest are
	// design constants. All stay fields so the package tests can shrink them,
	// and every timer below runs on the real clock, so a shrunk value is what
	// the loop actually waits.
	recheckWindow    time.Duration
	cancelAckTimeout time.Duration
	suspectThreshold time.Duration
	jobGoneWindow    time.Duration

	watcher Watcher

	// mu serializes rounds: reconcileOne is reentrant-safe because the
	// watcher and the timer only ever ask for another round, never settle
	// an entry themselves.
	mu     sync.Mutex
	wakeCh chan struct{}
	// leader gates the rounds: convergence and sweep run only on the
	// control-plane holder (§1.1 发出者口径). Nil keeps the gate open.
	leader func() bool

	// ended records when a Job was first observed ended, so the daemon's
	// report gets recheckWindow to arrive before the synthesized fail
	// (§4.6). missing records the first observation of a Job object that
	// is gone (jobGoneWindow, cleared as soon as it is visible again).
	// cancels records the same for a server-side cancellation (§4.6).
	// cleanups counts the failed deletion rounds of a terminal entry
	// releasing remembers a release this process started but
	// could not finish, so a later round completes it instead of reading the
	// missing Job as an unexpected death. suspects tracks the tasks
	// currently flagged (transition events and gauge only).
	ended     map[string]time.Time
	missing   map[string]time.Time
	cancels   map[string]time.Time
	cleanups  map[string]int
	releasing map[string]bool
	suspects  map[string]bool
}

// Option customizes a Reconciler.
type Option func(*Reconciler)

// WithWatcher wires the K8s object watcher that triggers immediate rounds.
func WithWatcher(w Watcher) Option {
	return func(r *Reconciler) { r.watcher = w }
}

// WithMetrics wires the observability seam.
func WithMetrics(m Metrics) Option {
	return func(r *Reconciler) {
		if m != nil {
			r.metrics = m
		}
	}
}

// WithLogger overrides the logger (tests).
func WithLogger(l *slog.Logger) Option {
	return func(r *Reconciler) { r.log = l.With("component", "reconciler") }
}

// New builds a Reconciler. cfg, reg, objects, server and settler are
// required; settler is satisfied by *scheduler.Scheduler.
func New(cfg Config, reg *registry.Registry, objects ObjectClient, server StatusClient, settler Settler, opts ...Option) (*Reconciler, error) {
	if reg == nil || objects == nil || server == nil || settler == nil {
		return nil, errors.New("registry, object client, status client and settler are required")
	}
	if cfg.ReconcileInterval <= 0 {
		cfg.ReconcileInterval = DefaultReconcileInterval
	}
	if cfg.RecheckWindow <= 0 {
		cfg.RecheckWindow = defaultRecheckWindow
	}
	r := &Reconciler{
		cfg:     cfg,
		reg:     reg,
		objects: objects,
		server:  server,
		settler: settler,
		metrics: noopMetrics{},
		log:     slog.Default().With("component", "reconciler"),

		recheckWindow:    cfg.RecheckWindow,
		cancelAckTimeout: defaultCancelAckTimeout,
		suspectThreshold: defaultSuspectThreshold,
		jobGoneWindow:    defaultJobGoneWindow,

		wakeCh:    make(chan struct{}, 1),
		ended:     make(map[string]time.Time),
		missing:   make(map[string]time.Time),
		cancels:   make(map[string]time.Time),
		cleanups:  make(map[string]int),
		releasing: make(map[string]bool),
		suspects:  make(map[string]bool),
	}
	for _, opt := range opts {
		opt(r)
	}
	r.comp = NewCompensator(settler, r.log)
	return r, nil
}

// Run converges every cfg.ReconcileInterval until ctx ends. The watcher
// (when wired) asks for a round as soon as a Job or Pod changes, so a
// failure is compensated without waiting for the next tick.
func (r *Reconciler) Run(ctx context.Context) error {
	if r.watcher != nil {
		go func() {
			if err := r.watcher.Watch(ctx, r.wake); err != nil && ctx.Err() == nil {
				r.log.ErrorContext(ctx, "job.watcher_failed", "err", err)
			}
		}()
	}
	go r.refreshInflight(ctx)
	for {
		if r.leader != nil && !r.leader() {
			// Only the control-plane holder converges and sweeps (§1.1
			// 发出者口径, AC-77); a parked instance keeps its watcher running.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		if err := r.Reconcile(ctx); err != nil {
			r.log.WarnContext(ctx, "reconcile round finished with failures", "err", err)
		}
		if !r.wait(ctx) {
			return nil
		}
	}
}

// refreshInflight re-derives foreman_inflight_jobs on its own cadence. The
// round refreshes it too, but a round spends minutes inside its own I/O (a
// terminal report's 4s→64s retry budget, object deletes), and a value that
// waits for the round to finish would freeze for exactly that long. This
// ticker is independent of the round, so the gauge cannot lag the live index
// by more than one interval whatever the round is doing (§9.2,
// AC-10).
func (r *Reconciler) refreshInflight(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.settler.SyncInflight()
		}
	}
}

// wait blocks until the next round is due: the interval, a watcher nudge,
// or the moment a deferred settlement (recheck window, cancel-ack timeout)
// falls due. Deadlines are stamped by the same clock that fires the timer,
// so a due settlement never has to wait out the whole interval.
func (r *Reconciler) wait(ctx context.Context) bool {
	d := r.cfg.ReconcileInterval
	if next, ok := r.nextDeadline(); ok {
		if until := time.Until(next); until < d {
			d = until
		}
	}
	if d < 0 {
		d = 0
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	case <-r.wakeCh:
		return true
	}
}

// wake asks the loop for a round now (the watcher's notification). It never
// blocks: a pending nudge is enough.
func (r *Reconciler) wake() {
	select {
	case r.wakeCh <- struct{}{}:
	default:
	}
}

// nextDeadline is the earliest moment a deferred settlement falls due: the
// daemon's recheck window, the cancel-ack timeout, or a draining entry's
// credential upper bound (ADR-017 — the reclaim must not wait out a whole
// interval past FOREMAN_JOB_DRAIN_TIMEOUT).
func (r *Reconciler) nextDeadline() (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var next time.Time
	for _, seen := range r.ended {
		if d := seen.Add(r.recheckWindow); next.IsZero() || d.Before(next) {
			next = d
		}
	}
	for _, seen := range r.cancels {
		if d := seen.Add(r.cancelAckTimeout); next.IsZero() || d.Before(next) {
			next = d
		}
	}
	now := time.Now()
	for _, e := range r.reg.List() {
		// A deadline that has already passed is not a wait source: it would
		// arm a zero-delay timer and spin the loop (and the K8s API) for as
		// long as a reclaim keeps failing, since rule 7 retries it without
		// bound. Such a round rides the regular interval instead.
		if !e.IsDraining() || !e.DrainDeadline.After(now) {
			continue
		}
		if next.IsZero() || e.DrainDeadline.Before(next) {
			next = e.DrainDeadline
		}
	}
	return next, !next.IsZero()
}

// Reconcile runs one convergence round: the durable queue is drained, the
// suspect signal refreshed, every live entry settled, then the deferred
// state of settled tasks forgotten. It is the seam the scheduler calls
// (scheduler.Reconciler) and what Run repeats.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	// The inflight gauge is derived state of the live index: re-derive it on
	// entry so the settlements below start from a fresh value, and let the
	// independent ticker bound any straggler (§9.2).
	r.settler.SyncInflight()
	r.mu.Lock()
	defer r.mu.Unlock()
	// §4.3: the carrier list is the premise of "this task has no pending
	// report". Until it completes, no entry is settled — a failed list must
	// never read as an empty queue (and must never emit a synthetic
	// terminal). Known queue entries still replay: that is delivery, not a
	// settlement decision.
	if err := r.settler.RestorePendingReports(ctx); err != nil {
		r.log.WarnContext(ctx, "pending.restore_failed", "err", err)
		r.drainPending(ctx)
		return nil
	}
	r.drainPending(ctx)
	r.reportSuspects(ctx)
	err := r.round(ctx)
	r.prune()
	return err
}

// round settles every live entry, collecting the first error so one broken
// task does not hide the rest of the round. The orphan-Secret sweep runs once
// per round, before the entries (§6.5): it covers the
// credentials a reclaim left behind when this process restarted.
func (r *Reconciler) round(ctx context.Context) error {
	// The session index expires on this loop alone (ADR-015): its entries are
	// learned from terminal reports, so this sweep sits ahead of the round's
	// live-entry check — index-only state must converge too.
	if dropped := r.reg.PruneExpiredSessions(r.cfg.AgentStateTTL); dropped > 0 {
		r.log.InfoContext(ctx, "session_index.pruned", "count", dropped,
			"ttl", r.cfg.AgentStateTTL.String())
	}
	if err := r.settler.SweepOrphanSecrets(ctx); err != nil {
		r.log.WarnContext(ctx, "cred.orphan_sweep_failed", "err", err)
	}
	entries := r.reg.List()
	if len(entries) == 0 {
		return nil
	}
	// One pod read per round, grouped by owning Job, instead of one call
	// per entry (the judgement reads both objects). A failed read must not
	// freeze the paths that never consume pods: terminal Job deletion and
	// drain closure move on, only the end-reason judgement of live entries is
	// deferred (§4.4: a read failure is not a teardown sign).
	pods, listErr := r.objects.ListPods(ctx)
	if listErr != nil {
		listErr = fmt.Errorf("list foreman pods: %w", listErr)
		r.log.WarnContext(ctx, "reconcile.pods_unavailable", "err", listErr)
	}
	byJob := indexPods(pods)

	var firstErr error
	for _, e := range entries {
		if ctx.Err() != nil {
			break
		}
		if listErr != nil && !e.IsTerminal() && !r.releasing[e.TaskID] {
			continue
		}
		// A re-run reuses the Job name (fm-<task_id>), so the list face can
		// still carry a predecessor's corpse pod; both consumers of the
		// round's pod facts read only the current instance (ADR-017).
		current := instancePods(byJob[e.JobName], e.JobCreatedAt)
		// Node placement is the fact the per-node soft cap counts
		// (FOREMAN_MAX_JOBS_PER_NODE, contract §3: Pod 读 spec.nodeName),
		// and the node_name the terminal report's learning face reads
		// (ADR-015). The pods are already listed for this round; the current
		// observation wins, so a pod rebuilt onto another node (eviction,
		// drain) moves the entry — and both consumers — with it.
		if node := podNodeName(current); node != "" && node != e.NodeName {
			r.reg.SetNode(e.TaskID, node)
		}
		if err := r.reconcileOne(ctx, e, current); err != nil {
			r.log.WarnContext(ctx, "reconcile entry failed",
				"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if firstErr == nil {
		firstErr = listErr
	}
	// §10 两级在途上限: the leader round repairs a leaked counter (a dead
	// reserver's unexplained increment) within one round. Downward-only on the
	// live path; the absolute write happens on takeover.
	if serr := r.settler.ReconcileSlots(ctx); serr != nil {
		r.log.WarnContext(ctx, "coordination.slots_reconcile_failed", "err", serr)
	}
	return firstErr
}

// reconcileOne converges a single task:
//
//  0. locally terminal → retry the queued report, delete the Job, then close
//     the drain window once the Job terminated or its upper bound expired
//     (ADR-017)
//  1. Job object gone  → the missing-Job compensation (grace + C13)
//  2. Job object ended → recheck window, then the failure compensation
//  3. otherwise the server's C13 status decides
//
// "Job 状态判定是否结束，C13 判定该报什么，daemon 上报判定具体结果；三者都不具备
// 时保持等待" is the 判据模型 this ladder encodes.
func (r *Reconciler) reconcileOne(ctx context.Context, e registry.TaskEntry, pods []corev1.Pod) error {
	if e.IsTerminal() {
		// Locally terminal: only a queued report and the Job deletion may
		// still linger (§4.2: the state was already written), and the
		// credential waits for the Job to terminate or the drain upper bound
		// (ADR-017). Both are attempted: the drain must not stall behind a
		// deletion that keeps failing.
		delete(r.ended, e.JobName)
		delete(r.missing, e.JobName)
		err := r.settleTerminalEntry(ctx, e)
		if rerr := r.reclaimIfDue(ctx, e); rerr != nil && err == nil {
			err = rerr
		}
		return err
	}
	if r.releasing[e.TaskID] {
		// An earlier round began a report-free release but could not finish
		// it. The Job may be gone by now, so re-deriving the intent from the
		// objects would read it as an unexpectedly missing Job and fail a
		// task that is on its way back to the server queue — finish the
		// release instead (the deletion is idempotent).
		return r.release(ctx, e.TaskID)
	}
	job, err := r.objects.GetJob(ctx, e.JobName)
	if err != nil {
		return fmt.Errorf("get job %s: %w", e.JobName, err)
	}
	if job == nil {
		// §4.3 未完成创建的接续: a pre-delivery entry whose payload is in the
		// shared row and whose Job was never created is continued, not
		// failed — the task never ran and the claim is still ours.
		if !e.IsTerminal() && !e.IsDraining() && e.State == registry.StatePending &&
			e.StartedAt.IsZero() && e.DeliveredAt.IsZero() && len(e.Payload) != 0 {
			if err := r.settler.ContinueClaim(ctx, e.TaskID); err == nil {
				delete(r.missing, e.JobName)
				return nil
			} else if errors.Is(err, scheduler.ErrStoreUnavailable) {
				// The row cannot carry the token digest, so the continuation
				// created nothing (ADR-046 决策 7). That is not a Job-missing
				// observation: the compensation below would settle a task that
				// never ran as job_missing. Keep the entry; the next round
				// retries once the shared state answers.
				delete(r.missing, e.JobName)
				return err
			}
		}
		// §4.3/§4.6: the Job vanished (deleted, TTL-collected, or never
		// created). The compensation waits out jobGoneWindow measured from
		// this first missing observation — a briefly invisible long-lived
		// Job must not be failed on the spot — and C13 then decides what to
		// settle inside OnJobGone.
		delete(r.ended, e.JobName)
		first, ok := r.missing[e.JobName]
		if !ok {
			r.missing[e.JobName] = time.Now()
			return nil
		}
		if time.Since(first) < r.jobGoneWindow {
			return nil
		}
		delete(r.missing, e.JobName)
		return r.settler.OnJobGone(ctx, e.JobName)
	}
	delete(r.missing, e.JobName)
	if reason, ended := jobEndReason(job, pods, e.JobCreatedAt); ended {
		return r.settleEnded(ctx, e, reason)
	}
	delete(r.ended, e.JobName)
	return r.settleFromServer(ctx, e)
}

// release discards the objects and mapping of a task that will be re-run,
// without any terminal report and without entering drain (§4.3). A failed
// attempt is remembered: the entry is the retry handle,
// and the next round must finish the release instead of treating the
// already-deleted Job as an unexpected loss.
//
// An adopted entry left the disposal table permanently (ADR-026 决策 5), so
// the start fact is re-read from the live entry before the call. That read is
// an optimization only: the authoritative check sits at the deletion surface
// (DiscardTask re-reads the entry under the task lock), so an S8 transition
// landing right after this read still wins.
func (r *Reconciler) release(ctx context.Context, taskID string) error {
	if e, ok := r.reg.Get(taskID); ok && !e.StartedAt.IsZero() {
		delete(r.releasing, taskID)
		return nil
	}
	if err := r.settler.DiscardTask(ctx, taskID); err != nil {
		r.releasing[taskID] = true
		return err
	}
	delete(r.releasing, taskID)
	return nil
}

// settleTerminalEntry retries the Job deletion of a terminal entry, bounded
// per entry (顺序与幂等规则 2). A failed round is counted; once the budget is
// spent the deletion is not attempted again by this process — the Job is left
// to its ttlSecondsAfterFinished and the entry stays as the handle a restart
// picks the retries up with. A pending report that has not landed yet is not
// a deletion round, so it does not consume the budget.
func (r *Reconciler) settleTerminalEntry(ctx context.Context, e registry.TaskEntry) error {
	if r.cleanups[e.TaskID] >= cleanupRetryLimit {
		return nil
	}
	if err := r.settler.SettleTerminal(ctx, e.TaskID, string(e.Result)); err != nil {
		r.cleanups[e.TaskID]++
		if r.cleanups[e.TaskID] >= cleanupRetryLimit {
			r.log.ErrorContext(ctx, "task.cleanup_abandoned",
				"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
				"attempts", r.cleanups[e.TaskID], "err", err)
		}
		return err
	}
	delete(r.cleanups, e.TaskID)
	return nil
}

// reclaimIfDue closes the drain window of a settled entry (ADR-017): the
// credential is reclaimed once the Job's pods have all terminated
// (job_terminated) or the drain upper bound has expired (drain_timeout),
// whichever comes first. The termination judgement is the scheduler's single
// evidence surface; the upper bound closes the window unconditionally, which
// is what covers an unreachable node, a pod stuck Terminating and a lost
// informer event.
//
// The reclaim's retries are unbounded (顺序与幂等规则 7): the upper bound
// constrains the credential's lifetime, never the deletion of the objects it
// leaves behind.
func (r *Reconciler) reclaimIfDue(ctx context.Context, e registry.TaskEntry) error {
	if !e.IsDraining() {
		return nil
	}
	if e.FailureReason == scheduler.FailureReasonInvalidTaskID {
		// No Job or Secret ever existed for this instance (an invalid task id,
		// or a malformed claim dead letter). Its identity-derived names may be
		// illegal selectors, and there is no termination fact to read: reclaim
		// now instead of issuing reads the apiserver rejects every round.
		return r.settler.OnJobTerminated(ctx, e.JobName, scheduler.JobTerminationReasonNoObjects, time.Time{}, 0)
	}
	since := e.JobCreatedAt
	// The Job's termination is the primary fact and is read every round: a
	// Job that already terminated must be reclaimed as job_terminated even
	// when the upper bound has also expired (the reason must follow the
	// fact, not the timer).
	terminated, err := r.settler.JobTerminated(ctx, e.JobName, since)
	if err != nil {
		// No fact this round: it is not a verdict, so the empty result is
		// never read as "terminated"; the upper bound still closes the
		// window when it expires.
		r.log.WarnContext(ctx, "job.termination_evidence_failed",
			"task_id", e.TaskID, "job_name", e.JobName, "err", err)
	}
	due := !e.DrainDeadline.IsZero() && !time.Now().Before(e.DrainDeadline)
	if !terminated && !due {
		return nil
	}
	reason := scheduler.JobTerminationReasonDrainTimeout
	var podTerminatedAt time.Time
	if terminated {
		reason = scheduler.JobTerminationReasonJobTerminated
		at, err := r.settler.LastPodTerminatedAt(ctx, e.JobName, since)
		if err != nil {
			r.log.WarnContext(ctx, "job.termination_evidence_failed",
				"task_id", e.TaskID, "job_name", e.JobName, "err", err)
		}
		podTerminatedAt = at
	}
	orphans, err := r.settler.AlivePods(ctx, e.JobName, since)
	if err != nil {
		r.log.WarnContext(ctx, "job.termination_evidence_failed",
			"task_id", e.TaskID, "job_name", e.JobName, "err", err)
	}
	return r.settler.OnJobTerminated(ctx, e.JobName, reason, podTerminatedAt, orphans)
}

// settleEnded compensates a Job whose container ended (§4.6).
// A deadline kill is immediate; every other end first gives the daemon
// recheckWindow to land its own terminal report — the daemon's report wins
// whenever it arrives (判据模型), which the C13 recheck inside the
// compensation confirms.
func (r *Reconciler) settleEnded(ctx context.Context, e registry.TaskEntry, reason string) error {
	if reason != scheduler.FailureReasonJobDeadline {
		first, ok := r.ended[e.JobName]
		if !ok {
			// Stamp and time.Since both ride the process monotonic clock:
			// a wall-clock step must never shorten the window — an early
			// settle would beat the daemon's own report (判据模型). The
			// price is that a suspend freezes this clock, so the window
			// stretches by the suspend duration: a late retry, never an
			// early one. K8s controllers accept that drift.
			r.ended[e.JobName] = time.Now()
			return nil
		}
		if time.Since(first) < r.recheckWindow {
			return nil
		}
	}
	delete(r.ended, e.JobName)
	return r.comp.Fail(ctx, e, reason)
}

// settleFromServer applies the C13 facts. A healthy
// in-flight task is left alone: the lease keeper and the daemon's own
// reports own it.
func (r *Reconciler) settleFromServer(ctx context.Context, e registry.TaskEntry) error {
	st, err := r.server.TaskStatus(ctx, e.TaskID)
	switch {
	case errors.Is(err, scheduler.ErrTaskNotFound):
		// §4.6: the server deleted the task — settle it terminal with an
		// empty result (the only source of result="") and delete the Job, but
		// send no report for a task it never knew. The credential stays until
		// the Job is gone (ADR-017).
		delete(r.cancels, e.TaskID)
		return r.settler.OnTaskVanished(ctx, e.TaskID)
	case err != nil:
		// C13 failed (5xx/network): truth unknown, change nothing this
		// round (错误处理表).
		return fmt.Errorf("task status %s: %w", e.TaskID, err)
	}
	r.reg.SetLastStatusSeen(e.TaskID, st)

	// The cancel confirmation window is measured over a continuous cancelled
	// observation (§4.6). Any other answer ends that stretch: keeping its
	// timestamp would let nextDeadline() return a past instant (the loop's
	// wait goes to zero and spins) and would make the next cancelled
	// observation settle on the old window instead of starting a fresh one.
	if st != statusCancelled {
		delete(r.cancels, e.TaskID)
	}

	switch {
	case st == statusCancelled:
		return r.settleCancelled(ctx, e)
	case registry.IsTerminalResult(st):
		return r.settler.SettleTerminal(ctx, e.TaskID, st)
	case st == statusRunning:
		// §4.3: the task is executing — keep the Job, stop lease
		// renewals and accept the daemon's reports (a rebuilt entry has no
		// delivery to replay).
		delete(r.cancels, e.TaskID)
		return r.settler.AdoptRunning(ctx, e.TaskID, st)
	case st == statusQueued || st == statusDispatched:
		// §4.3 / ADR-026 决策 3/5: the discard row needs the payload to be
		// unavailable and the start fact to be absent. Payload availability
		// is stable for a live entry; the start fact is re-read from the live
		// entry at the deletion surface (DiscardTask holds the task lock), so
		// an S8 start that landed during this C13 round trip keeps the Job
		// even though the round snapshot still reads started_at == 0.
		if len(e.Payload) != 0 {
			// In-flight pre-start task: it still holds its payload; the
			// delivery and boot faces own it.
			return nil
		}
		delete(r.cancels, e.TaskID)
		return r.release(ctx, e.TaskID)
	case st == statusWaitingLocalDirectory:
		// §4.3: the daemon holds the task, so a rebuilt entry adopts (start
		// fact recorded, no renewals). An in-flight entry still holds its
		// payload and is simply waiting for the daemon to start it.
		if len(e.Payload) == 0 {
			delete(r.cancels, e.TaskID)
			return r.settler.AdoptRunning(ctx, e.TaskID, st)
		}
		return nil
	default:
		// Outside the C13 domain: keep the entry and report it — a value
		// extension is a design change, not a guess (ADR-026 决策结果 3).
		r.log.ErrorContext(ctx, "task.status_unclassified",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID, "status", st)
		return nil
	}
}

// settleCancelled implements §4.6: the server cancelled the task while
// its daemon may still be polling C13 (S9, every 5s) to notice. Foreman
// waits cancelAckTimeout for the daemon's cancel-ack (forwarded as C11 and
// settled by the normal report path), then settles the entry as cancelled
// without a fail report — the verdict is already server-side — and deletes
// the Job, leaving the credential to the drain (ADR-017 路径 ④).
func (r *Reconciler) settleCancelled(ctx context.Context, e registry.TaskEntry) error {
	first, ok := r.cancels[e.TaskID]
	if !ok {
		// Same monotonic stamp as the recheck window: immune to wall-clock
		// steps, stretched — never shortened — by a process suspend.
		r.cancels[e.TaskID] = time.Now()
		r.log.InfoContext(ctx, "task.cancel_seen",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID)
		return nil
	}
	if time.Since(first) < r.cancelAckTimeout {
		return nil
	}
	r.log.WarnContext(ctx, "task.cancel_ack_timeout",
		"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
		"cancel_ack_timeout_seconds", r.cancelAckTimeout.Seconds())
	delete(r.cancels, e.TaskID)
	// cancelled is already terminal server-side: no fail report, but the
	// verdict still starts the drain (ADR-017 路径 ④).
	return r.settler.SettleTerminal(ctx, e.TaskID, string(registry.ResultCancelled))
}

// reportSuspects flags every live task whose daemon has been silent on both
// heartbeat channels (S5 and the S30 WS frame share last_heartbeat_at) for
// suspectThreshold, and refreshes the gauge (§4.6). It never acts: the
// daemon may still be working behind a partition, so the Job is kept and
// only the pod's end (§4.6) may settle the task.
func (r *Reconciler) reportSuspects(ctx context.Context) {
	suspects := 0
	for _, e := range r.reg.List() {
		if e.IsTerminal() {
			continue
		}
		rt, ok := r.reg.RuntimeByID(e.JobRuntimeID)
		if !ok || rt.LastHeartbeatAt.IsZero() {
			// The daemon never registered: the boot deadline owns it.
			continue
		}
		age := time.Since(rt.LastHeartbeatAt)
		if age <= r.suspectThreshold {
			delete(r.suspects, e.TaskID)
			continue
		}
		suspects++
		if r.suspects[e.TaskID] {
			continue
		}
		r.suspects[e.TaskID] = true
		r.log.WarnContext(ctx, "task.heartbeat_suspect",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
			"last_heartbeat_age_seconds", age.Seconds())
	}
	r.metrics.HeartbeatSuspect(suspects)
}

// drainPending re-delivers the durable terminal-report queue every round
// (§4.6/§10): the contract bounds a restored Foreman's catch-up to
// 2×FOREMAN_RECONCILE_INTERVAL (§10 可靠性). The queue is the scheduler's
// (ADR-031), so the drain is a call on the settlement surface — the existing
// recovery → scheduler direction.
func (r *Reconciler) drainPending(ctx context.Context) {
	if n, err := r.settler.DrainPendingReports(ctx); err != nil {
		r.log.WarnContext(ctx, "pending.drain_failed", "queued", n, "err", err)
	}
}

// prune forgets the deferred state of tasks that are no longer live, so the
// bookkeeping stays proportional to the in-flight set.
func (r *Reconciler) prune() {
	liveTasks := make(map[string]bool)
	liveJobs := make(map[string]bool)
	for _, e := range r.reg.List() {
		liveTasks[e.TaskID] = true
		liveJobs[e.JobName] = true
	}
	for name := range r.ended {
		if !liveJobs[name] {
			delete(r.ended, name)
		}
	}
	for name := range r.missing {
		if !liveJobs[name] {
			delete(r.missing, name)
		}
	}
	for id := range r.cancels {
		if !liveTasks[id] {
			delete(r.cancels, id)
		}
	}
	for id := range r.cleanups {
		if !liveTasks[id] {
			delete(r.cleanups, id)
		}
	}
	for id := range r.releasing {
		if !liveTasks[id] {
			delete(r.releasing, id)
		}
	}
	for id := range r.suspects {
		if !liveTasks[id] {
			delete(r.suspects, id)
		}
	}
}

// SetLeaderGate wires the control-plane ownership check.
func (r *Reconciler) SetLeaderGate(g func() bool) { r.leader = g }
