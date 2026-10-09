package scheduler

import (
	"context"
	"errors"
	"testing"

	batchv1 "k8s.io/api/batch/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// inflightGauge is what a /metrics scrape reads: the last value written to
// the gauge (TC-tech-observability-01 step 4).
func (f *fixture) inflightGauge(t *testing.T) int {
	t.Helper()
	if len(f.metrics.inflight) == 0 {
		t.Fatal("foreman_inflight_jobs was never set")
	}
	return f.metrics.inflight[len(f.metrics.inflight)-1]
}

// assertSettled checks that the surfaces AC-10 requires to agree do agree
// once a task has settled: the gauge, Registry.Inflight(), the /foreman/tasks
// snapshot and the Job.
func (f *fixture) assertSettled(t *testing.T, taskID string) {
	t.Helper()
	if n := f.reg.Inflight(); n != 0 {
		t.Fatalf("Registry.Inflight() = %d, want 0 after settlement", n)
	}
	if got := f.inflightGauge(t); got != 0 {
		t.Fatalf("foreman_inflight_jobs = %d, want 0 — the gauge froze at its pre-settlement value", got)
	}
	if n := len(f.reg.List()); n != 0 {
		t.Fatalf("registry entries = %d, want 0", n)
	}
	if f.hasJob("fm-" + taskID) {
		t.Fatalf("Job fm-%s survives the settlement", taskID)
	}
}

// FailJob compensates a Job that ended without a daemon report: the task
// turns terminal, so the gauge must fall with Registry.Inflight()
// (observability.md §指标, AC-10).
func TestFailJobResetsInflightGauge(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "running"

	if err := f.sched.FailJob(context.Background(), "fm-task-1", FailureReasonJobMissing); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	f.assertSettled(t, "task-1")
}

// A terminal entry whose fail report cannot be forwarded stays in the index
// as the retry handle (contract §4), but it is no longer inflight: the gauge
// must read 0 although the Job is still there.
func TestFailJobResetsInflightGaugeWhenTheReportIsRetained(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "running"
	f.server.forwardErr = errors.New("transport down")

	if err := f.sched.FailJob(context.Background(), "fm-task-1", FailureReasonJobMissing); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	e, ok := f.reg.Get("task-1")
	if !ok || !e.IsTerminal() {
		t.Fatalf("entry = %+v, %v, want the retained terminal entry", e, ok)
	}
	if got := f.inflightGauge(t); got != 0 {
		t.Fatalf("foreman_inflight_jobs = %d, want 0 with Registry.Inflight() = %d", got, f.reg.Inflight())
	}
	if !f.hasJob("fm-task-1") {
		t.Fatal("the Job must survive as the retry handle until the report lands")
	}
}

// SettleTerminal converges an entry the server already holds terminal: the
// same gauge requirement applies (AC-10).
func TestSettleTerminalResetsInflightGauge(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")

	if err := f.sched.SettleTerminal(context.Background(), "task-1", string(registry.ResultCancelled)); err != nil {
		t.Fatalf("SettleTerminal: %v", err)
	}
	f.assertSettled(t, "task-1")
}

// A cleanup the apiserver rejects keeps the entry as the retry handle
// (顺序与幂等规则 2) while the gauge still follows the terminal transition.
func TestSettleTerminalResetsInflightGaugeWhenCleanupIsRetried(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.jobs.deleteJobErr = errors.New("apiserver unavailable")

	if err := f.sched.SettleTerminal(context.Background(), "task-1", string(registry.ResultCompleted)); err == nil {
		t.Fatal("SettleTerminal: want the deletion error surfaced")
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the entry must stay as the retry handle")
	}
	if got := f.inflightGauge(t); got != 0 {
		t.Fatalf("foreman_inflight_jobs = %d, want 0 with Registry.Inflight() = %d", got, f.reg.Inflight())
	}
}

// A restart rebuilds the index from the surviving Jobs while the gauge is
// still at its boot value: the reconcile round re-derives it, so a stale
// gauge never outlives one round (要求 3: 2 个 reconcile 周期内自愈).
func TestReconcileResetsInflightGaugeAfterRebuild(t *testing.T) {
	f := newFixture(t)
	f.jobs.jobs = []batchv1.Job{jobObject("task-1")}
	f.server.status = "running"

	if err := f.sched.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.sched.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := f.reg.Inflight(); want != 1 {
		t.Fatalf("Registry.Inflight() = %d, want the rebuilt running task", want)
	}
	if got := f.inflightGauge(t); got != 1 {
		t.Fatalf("foreman_inflight_jobs = %d, want 1 after the reconcile round", got)
	}
}

// The third settlement path — a lease refusal the server answers with a
// terminal status — shares the guarantee: a cleanup the apiserver rejects
// must not leave the gauge at the non-terminal count.
func TestConvergeLeaseRefusedResetsInflightGaugeWhenCleanupIsRetried(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "completed"
	f.jobs.deleteJobErr = errors.New("apiserver unavailable")

	if err := f.sched.ConvergeLeaseRefused(context.Background(), "task-1"); err == nil {
		t.Fatal("ConvergeLeaseRefused: want the deletion error surfaced")
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the entry must stay as the retry handle")
	}
	if got := f.inflightGauge(t); got != 0 {
		t.Fatalf("foreman_inflight_jobs = %d, want 0 with Registry.Inflight() = %d", got, f.reg.Inflight())
	}
}

// Dropping the entry of a task the server already deleted must take the
// gauge down with it: no survivor of the discarded claim refreshes it.
func TestDuplicateClaimDiscardResetsInflightGauge(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.statusErr = ErrTaskNotFound

	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("the entry of a task deleted server-side must be dropped")
	}
	if got := f.inflightGauge(t); got != 0 {
		t.Fatalf("foreman_inflight_jobs = %d, want 0 with Registry.Inflight() = %d", got, f.reg.Inflight())
	}
}
