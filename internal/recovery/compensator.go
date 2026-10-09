package recovery

import (
	"context"
	"log/slog"

	"github.com/tsic404/foreman/internal/registry"
)

// Settler is the scheduler's settlement surface (task-mapping 内部结构).
// Every method owns the invariants the recovery loop must not re-implement:
// the state is written before the K8s objects go (顺序规则 1), a terminal
// callback is never dropped (contract §4), and C13 is consulted before any
// compensation (顺序规则 3).
type Settler interface {
	// OnJobGone compensates a Job object that disappeared (#1/#9/#11); the
	// freshly-created grace period is included.
	OnJobGone(ctx context.Context, jobName string) error
	// FailJob reports a synthesized failure for a Job whose container ended
	// without a daemon terminal report (#3/#5/#11). reason is the reported
	// failure_reason.
	FailJob(ctx context.Context, jobName, reason string) error
	// SettleTerminal converges an entry the server holds as terminal: any
	// queued report lands first, then the objects go.
	SettleTerminal(ctx context.Context, taskID, result string) error
	// AdoptRunning keeps the Job of a task the server reports as running
	// and stops lease renewals (#1).
	AdoptRunning(ctx context.Context, taskID string) error
	// ReleaseTask removes the Job/Secret and drops the mapping without a
	// terminal report (#2 redispatch, #8 cancel timeout, #9 gone).
	ReleaseTask(ctx context.Context, taskID string) error
	// SyncInflight re-derives the inflight gauge from the live index. The
	// round calls it on entry and a ticker repeats it every interval, so a
	// settlement path that missed its own refresh cannot keep a stale count.
	SyncInflight()
}

// Compensator reports a terminal state on the daemon's behalf
// (failure-handling 内部结构: Fail(e, reason)).
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
