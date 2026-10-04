package scheduler

import (
	"context"
	"encoding/json"
	"errors"

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
// by *jobbuilder.Builder.
type JobBuilder interface {
	Build(e jobbuilder.TaskEntry, payload json.RawMessage) (*batchv1.Job, *corev1.Secret, error)
}

// Metrics is the observability seam. The observability module provides the
// Prometheus implementation; nil degrades to no-ops.
type Metrics interface {
	TaskClaimed()
	TaskTerminal(result string)
	JobBootSeconds(seconds float64)
	InflightJobs(n int)
	DuplicateDispatch()
}

// Reconciler is the recovery module's convergence driver. When wired,
// Scheduler.Reconcile delegates to it; until then the scheduler runs its
// own startup convergence over the rebuilt index.
type Reconciler interface {
	Reconcile(ctx context.Context) error
}

// noopMetrics keeps every Metrics call site nil-safe.
type noopMetrics struct{}

func (noopMetrics) TaskClaimed()           {}
func (noopMetrics) TaskTerminal(string)    {}
func (noopMetrics) JobBootSeconds(float64) {}
func (noopMetrics) InflightJobs(int)       {}
func (noopMetrics) DuplicateDispatch()     {}

func orNoopMetrics(m Metrics) Metrics {
	if m == nil {
		return noopMetrics{}
	}
	return m
}
