package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/scheduler"
)

// These tests drive the composition root in the production order — claim,
// failed Job, recovery round, compensation, cleanup — and read the gauge off
// the exposition the scrape endpoint serves. Asserting the value the
// scheduler's own metrics seam received cannot catch a refresh that never
// runs on this path: the field failure was a gauge frozen at the count the
// claim had written.

func claimFor(t *testing.T, sched *scheduler.Scheduler, taskID string) {
	t.Helper()
	claim := json.RawMessage(`{"id":"` + taskID + `","agent_id":"a1","issue_id":"i1","issue_identifier":"TSI-1","workspace_id":"w1"}`)
	if err := sched.OnClaim(context.Background(), claim); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
}

// AC-10 / TC-tech-observability-01 step 4 on the job_failed path: after the
// compensation converges, the gauge, the index, the task snapshot and the K8s
// objects all read zero.
func TestIntegrationJobFailedCompensationResetsInflightGauge(t *testing.T) {
	server := newFakeServerClient(map[string]string{"t1": "running"})
	metrics := observability.NewMetrics()
	reg, jobs, sched, _, newReconciler := setupIntegrationWithMetrics(t, server, "", metrics)
	ctx := context.Background()

	claimFor(t, sched, "t1")
	if got := scrapeGauge(t, metrics.Handler(), "foreman_inflight_jobs"); got != 1 {
		t.Fatalf("foreman_inflight_jobs = %v while the task is live, want 1", got)
	}

	// The Job's container ended without a daemon report (场景 #3): the
	// reconciler compensates with failure_reason=job_failed.
	rec := newReconciler(&fakeObjects{
		job: failedJob(fakeJobName("t1"), "BackoffLimitExceeded"),
		pods: []corev1.Pod{podFor(fakeJobName("t1"), func(p *corev1.Pod) {
			p.Status.Phase = corev1.PodFailed
		})},
	})
	if err := rec.Reconcile(ctx); err != nil { // sighting round
		t.Fatalf("Reconcile: %v", err)
	}
	rec.recheckWindow = 0
	if err := rec.Reconcile(ctx); err != nil { // compensation round
		t.Fatalf("Reconcile: %v", err)
	}
	if fails := server.forwarded(scheduler.EPFail); len(fails) != 1 || fails[0].reason != scheduler.FailureReasonJobFailed {
		t.Fatalf("fail forwards = %+v, want one job_failed report", fails)
	}

	if got := scrapeGauge(t, metrics.Handler(), "foreman_inflight_jobs"); got != 0 {
		t.Fatalf("foreman_inflight_jobs = %v after the job_failed compensation, want 0", got)
	}
	if got := reg.Inflight(); got != 0 {
		t.Fatalf("Registry.Inflight() = %d, want 0", got)
	}
	if entries := reg.List(); len(entries) != 0 {
		t.Fatalf("task snapshot = %+v, want empty", entries)
	}
	if jobs.has(fakeJobName("t1")) || jobs.hasSecret(fakeJobName("t1")+"-cred") {
		t.Fatal("the Job and its Secret must be gone once the gauge reads zero")
	}
}

// The round re-derives the gauge from the index, so a settlement path that
// missed its own refresh cannot freeze a stale count past one interval: the
// field failure required a process restart to clear it.
func TestIntegrationReconcileRoundHealsStaleInflightGauge(t *testing.T) {
	server := newFakeServerClient(map[string]string{"t1": "running"})
	metrics := observability.NewMetrics()
	reg, _, sched, _, newReconciler := setupIntegrationWithMetrics(t, server, "", metrics)
	ctx := context.Background()

	claimFor(t, sched, "t1")
	// A removal that never reached the gauge: the index is empty while
	// /metrics still serves the count the claim wrote.
	if err := reg.Delete("t1"); err != nil {
		t.Fatalf("reg.Delete: %v", err)
	}
	if got := scrapeGauge(t, metrics.Handler(), "foreman_inflight_jobs"); got != 1 {
		t.Fatalf("foreman_inflight_jobs = %v, want the stale 1 this test starts from", got)
	}

	rec := newReconciler(&fakeObjects{})
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := scrapeGauge(t, metrics.Handler(), "foreman_inflight_jobs"); got != 0 {
		t.Fatalf("foreman_inflight_jobs = %v after one round, want 0 — the round did not re-derive it", got)
	}
	if got := reg.Inflight(); got != 0 {
		t.Fatalf("Registry.Inflight() = %d, want 0", got)
	}
}

// AC-10 on a terminal report whose upstream is unreachable: the terminal
// write lands, the report goes to the durable queue (contract §4) and the
// gauge must follow the index right there — no round runs on this path, so a
// refresh registered after the forward would leave the pre-terminal count
// standing (observability.md §指标: 四源一致).
func TestIntegrationTerminalReportTransportErrorKeepsInflightGaugeInSync(t *testing.T) {
	server := newFakeServerClient(map[string]string{"t1": "running"})
	metrics := observability.NewMetrics()
	reg, _, sched, pending, _ := setupIntegrationWithMetrics(t, server, "", metrics)
	ctx := context.Background()

	claimFor(t, sched, "t1")
	entry, ok := reg.Get("t1")
	if !ok {
		t.Fatal("the claim did not register the task")
	}

	server.setForwardErr(errors.New("dial tcp 10.0.0.1:443: connect: connection refused"))
	code, _, err := sched.OnReport(ctx, scheduler.EPFail, entry, []byte(`{"error":"boom"}`))
	if err != nil {
		t.Fatalf("OnReport: %v", err)
	}
	if code != http.StatusBadGateway {
		t.Fatalf("OnReport status = %d, want 502 (upstream unavailable)", code)
	}
	if got := pending.Len(); got != 1 {
		t.Fatalf("queued reports = %d, want 1", got)
	}
	if got := reg.Inflight(); got != 0 {
		t.Fatalf("Registry.Inflight() = %d, want 0 after the terminal write", got)
	}
	if got := scrapeGauge(t, metrics.Handler(), "foreman_inflight_jobs"); got != 0 {
		t.Fatalf("foreman_inflight_jobs = %v with Registry.Inflight() = 0 — the gauge kept the pre-terminal count", got)
	}
}

// A round spends minutes inside its own I/O (a terminal report's 4s→64s
// retry budget, object deletes), so the round's own re-derivation cannot
// bound the gauge: the periodic one must run on its own cadence. AC-10's
// budget is two intervals; this parks a round inside the queue drain and
// requires the gauge back in sync inside that budget.
func TestIntegrationInflightGaugeConvergesWhileARoundIsBusy(t *testing.T) {
	server := newFakeServerClient(map[string]string{"t1": "running"})
	metrics := observability.NewMetrics()
	reg, _, sched, pending, newReconciler := setupIntegrationWithMetrics(t, server, "", metrics)
	const interval = 20 * time.Millisecond

	claimFor(t, sched, "t1")
	if err := pending.Enqueue("t1", scheduler.EPFail, []byte(`{"error":"boom"}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	busy := &stallingSender{entered: make(chan struct{}, 1), release: make(chan struct{})}
	rec := newReconciler(&fakeObjects{job: runningJob(fakeJobName("t1"))})
	WithPendingReports(pending, busy)(rec)
	rec.cfg.ReconcileInterval = interval

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer close(busy.release)
	go func() { _ = rec.Run(ctx) }()

	select {
	case <-busy.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the round never reached the drain")
	}
	// A settlement that missed its own refresh — the class the periodic
	// re-derivation exists for: the index empties, the gauge does not.
	if err := reg.Delete("t1"); err != nil {
		t.Fatalf("reg.Delete: %v", err)
	}

	deadline := time.Now().Add(2 * interval)
	for time.Now().Before(deadline) {
		if scrapeGauge(t, metrics.Handler(), "foreman_inflight_jobs") == 0 {
			return
		}
		time.Sleep(interval / 4)
	}
	t.Fatalf("foreman_inflight_jobs = %v with Registry.Inflight() = %d after 2 intervals — the busy round left the stale count standing",
		scrapeGauge(t, metrics.Handler(), "foreman_inflight_jobs"), reg.Inflight())
}

// stallingSender parks the queue drain of a round until it is released.
type stallingSender struct {
	entered chan struct{}
	release chan struct{}
}

func (s *stallingSender) Forward(ctx context.Context, _ scheduler.Endpoint, _ string, _ []byte) (int, []byte, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return 200, nil, nil
}

// The boot-timeout path is the control the field report held up as working:
// the two settlement paths must converge the same four sources to zero.
func TestIntegrationBootTimeoutResetsInflightGauge(t *testing.T) {
	server := newFakeServerClient(map[string]string{"t1": "running"})
	metrics := observability.NewMetrics()
	reg, jobs, sched, _, _ := setupIntegrationWithMetrics(t, server, "", metrics)
	ctx := context.Background()

	claimFor(t, sched, "t1")
	if got := scrapeGauge(t, metrics.Handler(), "foreman_inflight_jobs"); got != 1 {
		t.Fatalf("foreman_inflight_jobs = %v while the task is live, want 1", got)
	}

	// The daemon never reported a start before the boot deadline.
	if err := sched.OnBootTimeout(ctx, "t1"); err != nil {
		t.Fatalf("OnBootTimeout: %v", err)
	}
	if got := scrapeGauge(t, metrics.Handler(), "foreman_inflight_jobs"); got != 0 {
		t.Fatalf("foreman_inflight_jobs = %v after the boot timeout, want 0", got)
	}
	if got := reg.Inflight(); got != 0 {
		t.Fatalf("Registry.Inflight() = %d, want 0", got)
	}
	if entries := reg.List(); len(entries) != 0 {
		t.Fatalf("task snapshot = %+v, want empty", entries)
	}
	if jobs.has(fakeJobName("t1")) || jobs.hasSecret(fakeJobName("t1")+"-cred") {
		t.Fatal("the Job and its Secret must be gone once the gauge reads zero")
	}
}
