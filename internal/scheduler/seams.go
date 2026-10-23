package scheduler

import (
	"context"
	"errors"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// ErrTaskNotFound is the C13 signal that the server no longer knows the
// task (deleted server-side; §4.6).
var ErrTaskNotFound = errors.New("task not found on server")

// ErrUnknownDaemon rejects a claim from a daemon with no mapping entry
// (proxy maps it to 403).
var ErrUnknownDaemon = errors.New("unknown daemon")

// ErrRuntimeMismatch rejects a claim whose runtime ID is not the one bound
// to the daemon's Job (proxy maps it to 403).
var ErrRuntimeMismatch = errors.New("runtime does not belong to this job")

// ServerClient is the scheduler's view of the proxy's fake client: C13
// status truth checks and lifecycle forwarding. The proxy implementation
// owns the terminal retry budget (4s→64s) and header/credential details.
type ServerClient interface {
	// TaskStatus returns the server-side status (C13): queued, dispatched,
	// running, waiting_local_directory, completed, failed, cancelled; or
	// ErrTaskNotFound.
	TaskStatus(ctx context.Context, taskID string) (string, error)
	// Forward posts a lifecycle callback body to the server and returns its
	// status code and response body.
	Forward(ctx context.Context, ep Endpoint, taskID string, body []byte) (int, []byte, error)
	// ForwardInWorkspace posts a callback for a task the index no longer
	// holds, using workspaceID's credential: a replayed report can outlive
	// the entry that carried the workspace, and in a multi-workspace
	// deployment there is no single client to fall back to (§10, ADR-047).
	ForwardInWorkspace(ctx context.Context, ep Endpoint, taskID, workspaceID string, body []byte) (int, []byte, error)
}

// JobBuilder renders the Job and credential Secret for an entry; satisfied
// by *jobbuilder.Builder. The task payload is not part of the contract: it
// never reaches a K8s object (F3/ADR-007). tokenHash is the digest of the
// token inside the Secret — OnClaim records it as the entry's token_hash
// (ADR-017), the only handle the reclaim path has on the credential.
type JobBuilder interface {
	Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, string, error)
	// ResolveName composes the display name the template expresses for one
	// task (ADR-034): the same function Build runs internally, exposed so the
	// claim can index the entry under the final name before any object exists.
	// A failure refuses the claim (invalid_task_id).
	ResolveName(e jobbuilder.TaskEntry) (string, error)
	// RequiresProjectIdentity reports whether any of the task's declared axis
	// tiers reads the project side (project / projectWithAgent). It is what
	// keeps the read-only reverse lookup from happening when no axis needs it
	// (ADR-048 decision 4).
	RequiresProjectIdentity(e jobbuilder.TaskEntry) bool
	// BuildCatalogProbe renders a catalog-probe Job and its credential Secret
	// (ADR-019): shape = the template copy plus the probe write face, budget
	// into the probe's activeDeadlineSeconds, token TTL = budget + drain
	// timeout.
	BuildCatalogProbe(requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, error)
}

// Revoker feeds a revoked credential digest to the Job Token authority
// (satisfied by *auth.Issuer). The reclaim path calls it before the Secret
// and the entry go; a missing revoker is a wiring error, not a mode, because
// a silent no-op would leave the predecessor's token valid on the
// same-name-reuse face (ADR-017). The workspace partitions the revocation
// set (ADR-046 阶段一, §6.1): one workspace's revocations must never
// adjudicate another's.
type Revoker interface {
	Revoke(tokenHash, workspaceID string)
}

// Metrics is the observability seam. The observability module provides the
// Prometheus implementation; nil degrades to no-ops.
type Metrics interface {
	TaskClaimed()
	TaskTerminal(result string)
	JobBootSeconds(seconds float64)
	JobCreateSeconds(seconds float64)
	InflightJobs(n int)
	DuplicateDispatch()
	// CredReclaimed counts one credential reclaim by reason
	// (job_terminated|drain_timeout) and observes the drain duration
	// (§9.2, ADR-017).
	CredReclaimed(reason string, drainSeconds float64)
	// DeliveryUnconfirmed counts one S6 delivery the confirmation window
	// judged undelivered (§9.2, foreman_delivery_unconfirmed_total, ADR-032).
	DeliveryUnconfirmed()
	// ClaimError counts a claim Foreman could not map, by reason. A claim
	// with no readable task id has no other convergence than this signal and
	// the server-side lease expiry.
	ClaimError(reason string)
	// JobDeleteFailed counts a Job deletion that failed with no retry handle
	// left, by stage (currently catch_up: the entry was already dropped).
	JobDeleteFailed(stage string)
	// TokenUsage accumulates one usage report entry by provider and model
	// (§9.2 / ADR-009 decision 4: Foreman adds the usage while forwarding
	// the report, without a new persistence face). The four counters are the
	// S-12 entry's input/output and its two prompt-cache directions.
	TokenUsage(provider, model string, input, output, cacheRead, cacheWrite int64)
	// QueueOp counts one shared-queue operation by result
	// (enqueued|idempotent|taken|redelivered) (§9.2 共享队列).
	QueueOp(result string)
	// QueueDepth sets the live queue-row gauge (§9.2 条目数).
	QueueDepth(n int)
	// SharedSlots sets the two-level in-flight counter gauge; an empty
	// workspaceID is the cluster level (§9.2 两级在途).
	SharedSlots(workspaceID string, n int)
	// TaskLockWait observes one shared task-lock acquisition's wait.
	TaskLockWait(seconds float64)
	// ReconcileResult counts one takeover reconciliation by result and
	// observes its duration (§9.2 重建与对账结果).
	ReconcileResult(result string, seconds float64)
	// StateError counts one shared-state operation failure by operation
	// (§9.2 共享状态错误).
	StateError(op string)
}

// Reconciler is the recovery module's convergence driver. When wired,
// Scheduler.Reconcile delegates to it; until then the scheduler runs its
// own startup convergence over the rebuilt index.
type Reconciler interface {
	Reconcile(ctx context.Context) error
}

// PendingReportQueue is the durable terminal-report queue seam
// (§10: terminal callbacks are never dropped). It is the scheduler's own
// queue (ADR-031): every terminal forward writes its body ahead of the
// attempt, the direct answer drops it, and the recovery loop drains what is
// left through the Settler seam. A queued report also holds back the object
// cleanup of its task: a deleted Job must never stand in for a delivered
// report.
type PendingReportQueue interface {
	Enqueue(ctx context.Context, taskID, jobName, daemonID, workspaceID string, ep Endpoint, body []byte, failureReason string) error
	// Queued reports whether taskID's terminal report is still undelivered.
	// It is conservative: while the carrier list has not completed every task
	// reads as queued (§4.3 — an uncompleted rebuild is not an empty queue).
	Queued(taskID string) bool
	// Rebuilt reports whether the carrier list completed, so the queue's
	// state is authoritative and a settlement may act on it.
	Rebuilt() bool
	// HasQueuedEntry reports the queue membership the process actually
	// holds; the write-ahead decision uses it so an unknown list never
	// suppresses a report write.
	HasQueuedEntry(taskID string) bool
	// Drop forgets taskID's report after its direct forward settled it.
	Drop(ctx context.Context, taskID string)
	// SweepCarrier deletes one listed carrier object when no queue entry
	// holds its task. The check and the delete are one step under that
	// task's carrier lock, so an enqueue landing in between keeps its object
	// (§6.5).
	SweepCarrier(ctx context.Context, taskID, name string) (bool, error)
	// Drain re-delivers every queued report once through sender and returns
	// how many are still undelivered.
	Drain(ctx context.Context, sender ReportSender) (int, error)
	// Restore rebuilds the queue from its cluster carriers (§4.3). An error
	// means the list did not complete — not that the queue is empty.
	Restore(ctx context.Context) error
}

// noopMetrics keeps every Metrics call site nil-safe.
type noopMetrics struct{}

func (noopMetrics) TaskClaimed()                                          {}
func (noopMetrics) TaskTerminal(string)                                   {}
func (noopMetrics) JobBootSeconds(float64)                                {}
func (noopMetrics) JobCreateSeconds(float64)                              {}
func (noopMetrics) InflightJobs(int)                                      {}
func (noopMetrics) DuplicateDispatch()                                    {}
func (noopMetrics) CredReclaimed(string, float64)                         {}
func (noopMetrics) DeliveryUnconfirmed()                                  {}
func (noopMetrics) ClaimError(string)                                     {}
func (noopMetrics) JobDeleteFailed(string)                                {}
func (noopMetrics) TokenUsage(string, string, int64, int64, int64, int64) {}
func (noopMetrics) QueueOp(string)                                        {}
func (noopMetrics) QueueDepth(int)                                        {}
func (noopMetrics) SharedSlots(string, int)                               {}
func (noopMetrics) TaskLockWait(float64)                                  {}
func (noopMetrics) ReconcileResult(string, float64)                       {}
func (noopMetrics) StateError(string)                                     {}

func orNoopMetrics(m Metrics) Metrics {
	if m == nil {
		return noopMetrics{}
	}
	return m
}
