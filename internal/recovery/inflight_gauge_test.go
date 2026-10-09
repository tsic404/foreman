package recovery

import (
	"context"
	"encoding/json"
	"testing"

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
