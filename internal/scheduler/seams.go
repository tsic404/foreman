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
// task (deleted server-side; failure-handling scenario #9).
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
}

// JobBuilder renders the Job and credential Secret for an entry; satisfied
// by *jobbuilder.Builder. The task payload is not part of the contract: it
// never reaches a K8s object (F3/ADR-007). tokenHash is the digest of the
// token inside the Secret — OnClaim records it as the entry's token_hash
// (ADR-017), the only handle the reclaim path has on the credential.
type JobBuilder interface {
	Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, string, error)
	// BuildCatalogProbe renders a catalog-probe Job and its credential Secret
	// (ADR-019): shape source by CM window, budget into the default template's
	// activeDeadlineSeconds, token TTL = budget + drain timeout.
	BuildCatalogProbe(requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, error)
}

// Revoker feeds a revoked credential digest to the Job Token authority
// (satisfied by *auth.Issuer). The reclaim path calls it before the Secret
// and the entry go; a missing revoker is a wiring error, not a mode, because
// a silent no-op would leave the predecessor's token valid on the
// same-name-reuse face (ADR-017).
type Revoker interface {
	Revoke(tokenHash string)
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
	// (observability.md §指标, ADR-017).
	CredReclaimed(reason string, drainSeconds float64)
}

// Reconciler is the recovery module's convergence driver. When wired,
// Scheduler.Reconcile delegates to it; until then the scheduler runs its
// own startup convergence over the rebuilt index.
type Reconciler interface {
	Reconcile(ctx context.Context) error
}

// PendingReports is the recovery module's durable terminal-report queue
// (contract §4: terminal callbacks are never dropped). The scheduler hands
// it every terminal report whose forward exhausted the proxy's retry budget
// or failed outright; recovery drains the queue until the server accepts
// them. A queued report also holds back the object cleanup of its task:
// a deleted Job must never stand in for a delivered report.
type PendingReports interface {
	Enqueue(taskID string, ep Endpoint, body []byte) error
	// Queued reports whether taskID's terminal report is still undelivered.
	Queued(taskID string) bool
}

// noopMetrics keeps every Metrics call site nil-safe.
type noopMetrics struct{}

func (noopMetrics) TaskClaimed()                  {}
func (noopMetrics) TaskTerminal(string)           {}
func (noopMetrics) JobBootSeconds(float64)        {}
func (noopMetrics) JobCreateSeconds(float64)      {}
func (noopMetrics) InflightJobs(int)              {}
func (noopMetrics) DuplicateDispatch()            {}
func (noopMetrics) CredReclaimed(string, float64) {}

func orNoopMetrics(m Metrics) Metrics {
	if m == nil {
		return noopMetrics{}
	}
	return m
}
