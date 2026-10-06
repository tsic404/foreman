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

// Timing defaults of the failure-handling 场景矩阵. They are design
// constants (§5.1 makes only the interval configurable); the reconciler
// carries them as fields so its own tests can shrink them, and every timer
// reads the real clock so a shrunk window is honoured by the loop.
const (
	// defaultRecheckWindow gives a daemon that just lost its container the
	// chance to land its terminal report before Foreman synthesizes one
	// (场景 #3).
	defaultRecheckWindow = 10 * time.Second
	// defaultCancelAckTimeout is how long a server-side cancelled task waits
	// for its daemon's cancel-ack before its objects are removed (场景 #8).
	defaultCancelAckTimeout = 60 * time.Second
	// defaultSuspectThreshold is the two-channel heartbeat silence after
	// which a task is flagged suspect — signalled, never acted on (场景 #6).
	defaultSuspectThreshold = 60 * time.Second
	// defaultJobGoneWindow is how long a Job must be observed missing before
	// the missing-Job compensation engages. Measured from the first missing
	// observation, never from the claim: a long-lived Job that briefly
	// disappears from the API or the informer view must not be failed on the
	// spot (reconcileOne step 1).
	defaultJobGoneWindow = 60 * time.Second
	// cleanupRetryLimit bounds the object-deletion retries of a terminal
	// entry (顺序与幂等规则 2: 每轮重试删除，最多 10 轮). When the budget is
	// spent the Job is left to its ttlSecondsAfterFinished and the entry
	// stays as the handle a restart re-attempts with.
	cleanupRetryLimit = 10
)

// C13 statuses the convergence switches on (contract §1.1).
const (
	statusRunning    = "running"
	statusQueued     = "queued"
	statusDispatched = "dispatched"
	statusCancelled  = "cancelled"
)

// StatusClient is the recovery module's view of the fake client's C13
// query (proxy.Client; a task the server deleted maps to
// scheduler.ErrTaskNotFound).
type StatusClient interface {
	TaskStatus(ctx context.Context, taskID string) (string, error)
}

// Watcher triggers a convergence round on Job/Pod changes so the loop does
// not wait for the next tick (failure-handling 内部结构 JobWatcher). The
// reconciler re-reads the object state itself, so a missed or stale event
// costs latency, never correctness.
type Watcher interface {
	Watch(ctx context.Context, notify func()) error
}

// Metrics is the recovery module's observability seam (observability.md).
// The observability module provides the Prometheus implementation; nil
// degrades to a no-op.
type Metrics interface {
	// HeartbeatSuspect sets how many tasks currently fail the heartbeat
	// freshness check (foreman_heartbeat_suspect).
	HeartbeatSuspect(n int)
}

type noopMetrics struct{}

func (noopMetrics) HeartbeatSuspect(int) {}

// Reconciler is the convergence driver (failure-handling 内部结构):
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

	// Timing of the deferred settlements (design constants, §5.1). Kept as
	// fields so the package tests can shrink them; every timer below runs on
	// the real clock, so the shrunk value is what the loop actually waits.
	recheckWindow    time.Duration
	cancelAckTimeout time.Duration
	suspectThreshold time.Duration
	jobGoneWindow    time.Duration

	watcher Watcher
	pending *PendingReports
	sender  ReportSender

	// mu serializes rounds: reconcileOne is reentrant-safe because the
	// watcher and the timer only ever ask for another round, never settle
	// an entry themselves.
	mu     sync.Mutex
	wakeCh chan struct{}

	// ended records when a Job was first observed ended, so the daemon's
	// report gets recheckWindow to arrive before the synthesized fail
	// (场景 #3). missing records the first observation of a Job object that
	// is gone (jobGoneWindow, cleared as soon as it is visible again).
	// cancels records the same for a server-side cancellation (场景 #8).
	// cleanups counts the failed deletion rounds of a terminal entry
	// (顺序规则 2). releasing remembers a release this process started but
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

// WithPendingReports wires the durable terminal-report queue and the sender
// that re-delivers its entries (contract §4).
func WithPendingReports(p *PendingReports, sender ReportSender) Option {
	return func(r *Reconciler) {
		r.pending = p
		r.sender = sender
	}
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
	r := &Reconciler{
		cfg:     cfg,
		reg:     reg,
		objects: objects,
		server:  server,
		settler: settler,
		metrics: noopMetrics{},
		log:     slog.Default().With("component", "reconciler"),

		recheckWindow:    defaultRecheckWindow,
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
	for {
		if err := r.Reconcile(ctx); err != nil {
			r.log.WarnContext(ctx, "reconcile round finished with failures", "err", err)
		}
		if !r.wait(ctx) {
			return nil
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

// nextDeadline is the earliest moment a deferred settlement falls due.
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
	return next, !next.IsZero()
}

// Reconcile runs one convergence round: the durable queue is drained, the
// suspect signal refreshed, every live entry settled, then the deferred
// state of settled tasks forgotten. It is the seam the scheduler calls
// (scheduler.Reconciler) and what Run repeats.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drainPending(ctx)
	r.reportSuspects(ctx)
	err := r.round(ctx)
	r.prune()
	return err
}

// round settles every live entry, collecting the first error so one broken
// task does not hide the rest of the round.
func (r *Reconciler) round(ctx context.Context) error {
	entries := r.reg.List()
	if len(entries) == 0 {
		return nil
	}
	// One pod read per round, grouped by owning Job, instead of one call
	// per entry (failure-handling 判据模型 reads both objects).
	pods, err := r.objects.ListPods(ctx)
	if err != nil {
		return fmt.Errorf("list foreman pods: %w", err)
	}
	byJob := indexPods(pods)

	var firstErr error
	for _, e := range entries {
		if ctx.Err() != nil {
			break
		}
		// Node placement is the fact the per-node soft cap counts
		// (FOREMAN_MAX_JOBS_PER_NODE) and the node-reuse affinity keys on
		// (contract §1.3: Pod 读 spec.nodeName). The pods are already listed
		// for this round; record the node once it is known.
		if e.NodeName == "" {
			if node := podNodeName(byJob[e.JobName]); node != "" {
				r.reg.SetNode(e.TaskID, node)
			}
		}
		if err := r.reconcileOne(ctx, e, byJob[e.JobName]); err != nil {
			r.log.WarnContext(ctx, "reconcile entry failed",
				"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// reconcileOne converges a single task (failure-handling 收敛算法):
//
//  0. locally terminal → retry the queued report, then clean the objects up
//  1. Job object gone  → the missing-Job compensation (grace + C13)
//  2. Job object ended → recheck window, then the failure compensation
//  3. otherwise the server's C13 status decides
//
// "Job 状态判定是否结束，C13 判定该报什么，daemon 上报判定具体结果；三者都不具备
// 时保持等待" is the 判据模型 this ladder encodes.
func (r *Reconciler) reconcileOne(ctx context.Context, e registry.TaskEntry, pods []corev1.Pod) error {
	if e.IsTerminal() {
		// Locally terminal: only a queued report and the objects may still
		// linger (顺序规则 1: the state was already written).
		delete(r.ended, e.JobName)
		return r.settleTerminalEntry(ctx, e)
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
		// 场景 #1/#9: the Job vanished (deleted, TTL-collected, or never
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
	if reason, ended := jobEndReason(job, pods); ended {
		return r.settleEnded(ctx, e, reason)
	}
	delete(r.ended, e.JobName)
	return r.settleFromServer(ctx, e)
}

// release removes the task's objects and mapping without any terminal report
// (failure-handling 场景 #2/#8/#9). A failed attempt is remembered: the entry
// is the retry handle, and the next round must finish the release instead of
// treating the already-deleted Job as an unexpected loss.
func (r *Reconciler) release(ctx context.Context, taskID string) error {
	if err := r.settler.ReleaseTask(ctx, taskID); err != nil {
		r.releasing[taskID] = true
		return err
	}
	delete(r.releasing, taskID)
	return nil
}

// settleTerminalEntry retries the object deletion of a terminal entry,
// bounded per entry (顺序与幂等规则 2). A failed round is counted; once the
// budget is spent the deletion is not attempted again by this process — the
// Job is left to its ttlSecondsAfterFinished and the entry stays as the
// handle a restart picks the retries up with. A pending report that has not
// landed yet is not a deletion round, so it does not consume the budget.
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

// settleEnded compensates a Job whose container ended (场景 #3/#5/#11).
// A deadline kill is immediate; every other end first gives the daemon
// recheckWindow to land its own terminal report — the daemon's report wins
// whenever it arrives (判据模型), which the C13 recheck inside the
// compensation confirms.
func (r *Reconciler) settleEnded(ctx context.Context, e registry.TaskEntry, reason string) error {
	if reason != scheduler.FailureReasonJobDeadline {
		first, ok := r.ended[e.JobName]
		if !ok {
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

// settleFromServer applies the C13 facts (收敛算法 step 3). A healthy
// in-flight task is left alone: the lease keeper and the daemon's own
// reports own it.
func (r *Reconciler) settleFromServer(ctx context.Context, e registry.TaskEntry) error {
	st, err := r.server.TaskStatus(ctx, e.TaskID)
	switch {
	case errors.Is(err, scheduler.ErrTaskNotFound):
		// 场景 #9: the server deleted the task — remove the objects and
		// drop the mapping, no terminal report for a task it never knew.
		delete(r.cancels, e.TaskID)
		return r.release(ctx, e.TaskID)
	case err != nil:
		// C13 failed (5xx/network): truth unknown, change nothing this
		// round (错误处理表).
		return fmt.Errorf("task status %s: %w", e.TaskID, err)
	}
	r.reg.SetLastStatusSeen(e.TaskID, st)

	switch {
	case st == statusCancelled:
		return r.settleCancelled(ctx, e)
	case registry.IsTerminalResult(st):
		delete(r.cancels, e.TaskID)
		return r.settler.SettleTerminal(ctx, e.TaskID, st)
	case st == statusRunning:
		// 场景 #1: the task is executing — keep the Job, stop lease
		// renewals and accept the daemon's reports (a rebuilt entry has no
		// delivery to replay).
		delete(r.cancels, e.TaskID)
		return r.settler.AdoptRunning(ctx, e.TaskID)
	case st == statusQueued || st == statusDispatched:
		// 场景 #2: only a rebuilt entry (payload lost with the restart,
		// F3) has nothing to deliver — release the Job so the server
		// re-dispatches after its reclaim window. An in-flight entry still
		// holds its payload and is simply pre-start: leave it alone.
		if len(e.Payload) == 0 {
			delete(r.cancels, e.TaskID)
			return r.release(ctx, e.TaskID)
		}
		return nil
	default:
		// waiting_local_directory and friends: no decisive fact yet.
		return nil
	}
}

// settleCancelled implements 场景 #8: the server cancelled the task while
// its daemon may still be polling C13 (S9, every 5s) to notice. Foreman
// waits cancelAckTimeout for the daemon's cancel-ack (forwarded as C11 and
// cleaned up by the normal report path), then removes the objects without a
// fail report — cancelled is already terminal server-side.
func (r *Reconciler) settleCancelled(ctx context.Context, e registry.TaskEntry) error {
	first, ok := r.cancels[e.TaskID]
	if !ok {
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
	return r.release(ctx, e.TaskID)
}

// reportSuspects flags every live task whose daemon has been silent on both
// heartbeat channels (S5 and the S30 WS frame share last_heartbeat_at) for
// suspectThreshold, and refreshes the gauge (场景 #6). It never acts: the
// daemon may still be working behind a partition, so the Job is kept and
// only the pod's end (#3) may settle the task.
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
// (场景 #7/#10): the contract bounds a restored Foreman's catch-up to
// 2×FOREMAN_RECONCILE_INTERVAL (§7 可靠性).
func (r *Reconciler) drainPending(ctx context.Context) {
	if r.pending == nil || r.sender == nil {
		return
	}
	if n, err := r.pending.Drain(ctx, r.sender); err != nil {
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
