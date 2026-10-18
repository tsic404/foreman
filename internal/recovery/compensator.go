package recovery

import (
	"context"
	"log/slog"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// Settler is the scheduler's settlement surface.
// Every method owns the invariants the recovery loop must not re-implement:
// the state is written before the K8s objects go (§4.2), a terminal
// callback is never dropped (contract §10), and C13 is consulted before any
// compensation (§4.6).
type Settler interface {
	// OnJobGone compensates a Job object that disappeared (§4.6); the
	// freshly-created grace period is included.
	OnJobGone(ctx context.Context, jobName string) error
	// FailJob reports a synthesized failure for a Job whose container ended
	// without a daemon terminal report (§4.6). reason is the reported
	// failure_reason.
	FailJob(ctx context.Context, jobName, reason string) error
	// SettleTerminal converges an entry the server holds as terminal: any
	// queued report lands first, then the Job is deleted. The credential and
	// the entry stay for the drain (ADR-017).
	SettleTerminal(ctx context.Context, taskID, result string) error
	// AdoptRunning keeps the Job of a task the server reports as running
	// and stops lease renewals (§4.3).
	AdoptRunning(ctx context.Context, taskID string) error
	// DiscardTask removes the objects and the mapping of a task that will be
	// re-run (§4.3 redispatch), without a terminal report and without entering
	// drain; the credential is revoked when its digest is known.
	DiscardTask(ctx context.Context, taskID string) error
	// OnTaskVanished settles a task the server deleted (§4.6): terminal with an
	// empty result, Job deleted, entry into drain.
	OnTaskVanished(ctx context.Context, taskID string) error
	// JobTerminated reports whether every pod of the Job instance has
	// terminated (the pod objects are gone, or their phase is
	// Succeeded/Failed). An error means no fact — never a verdict.
	JobTerminated(ctx context.Context, jobName string, since time.Time) (bool, error)
	// LastPodTerminatedAt is the kubelet-reported termination fact of the
	// Job's last pod; zero when no pod carries one.
	LastPodTerminatedAt(ctx context.Context, jobName string, since time.Time) (time.Time, error)
	// AlivePods counts the Job's pods still alive at reclaim time
	// (orphan_pods).
	AlivePods(ctx context.Context, jobName string, since time.Time) (int, error)
	// OnJobTerminated is the credential-reclaim entry point (ADR-017):
	// revoke → delete Secret → drop the entry.
	OnJobTerminated(ctx context.Context, jobName, reason string, podTerminatedAt time.Time, orphanPods int) error
	// SweepOrphanSecrets deletes credential Secrets whose task has no
	// registry entry (any state) and no Job object (§6.5).
	SweepOrphanSecrets(ctx context.Context) error
	// SyncInflight re-derives the inflight gauge from the live index. The
	// round calls it on entry and a ticker repeats it every interval, so a
	// settlement path that missed its own refresh cannot keep a stale count.
	SyncInflight()
}

// Compensator reports a terminal state on the daemon's behalf
// (Fail(e, reason)).
type Compensator struct {
	settler Settler
	log     *slog.Logger
}

// NewCompensator builds the compensator over the scheduler's settlement
// primitives.
func NewCompensator(settler Settler, log *slog.Logger) *Compensator {
	if log == nil {
		log = slog.Default()
	}
	return &Compensator{settler: settler, log: log.With("component", "compensator")}
}

// Fail settles e as failed and reports it on the daemon's behalf. It is
// idempotent (收敛算法 Fail): the scheduler re-reads the entry state and
// C13, so a second call for an already settled task is a no-op, and a C10
// answer of 404/409 also counts as delivered.
func (c *Compensator) Fail(ctx context.Context, e registry.TaskEntry, reason string) error {
	if e.IsTerminal() {
		// Already settled locally: only the K8s objects may still linger.
		return c.settler.SettleTerminal(ctx, e.TaskID, string(e.Result))
	}
	if err := c.settler.FailJob(ctx, e.JobName, reason); err != nil {
		c.log.ErrorContext(ctx, "compensation deferred",
			"task_id", e.TaskID, "job_name", e.JobName, "failure_reason", reason, "err", err)
		return err
	}
	return nil
}
