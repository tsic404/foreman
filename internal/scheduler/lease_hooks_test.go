package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

// fakePendingReports records enqueued terminal reports.
type fakePendingReports struct {
	reports []pendingCall
}

type pendingCall struct {
	taskID string
	ep     Endpoint
	body   string
}

func (f *fakePendingReports) Enqueue(taskID string, ep Endpoint, body []byte) error {
	f.reports = append(f.reports, pendingCall{taskID, ep, string(body)})
	return nil
}

func (f *fixture) withPendingReports() *fakePendingReports {
	p := &fakePendingReports{}
	WithPendingReports(p)(f.sched)
	return p
}

func (f *fixture) hasJob(name string) bool {
	for _, j := range f.jobs.createdJobs {
		if j.Name == name {
			for _, d := range f.jobs.deletedJobs {
				if d == name {
					return false
				}
			}
			return true
		}
	}
	return false
}

func TestOnBootTimeoutFailsAndCleansUp(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "t-boot")

	if err := f.sched.OnBootTimeout(context.Background(), "t-boot"); err != nil {
		t.Fatalf("OnBootTimeout: %v", err)
	}
	if _, ok := f.reg.Get("t-boot"); ok {
		t.Error("entry must be gone after the boot-timeout settlement")
	}
	if f.hasJob("fm-t-boot") {
		t.Error("Job must be deleted after the boot-timeout settlement")
	}
	if !f.reg.IsDone("t-boot") {
		t.Error("done index must record the terminal task")
	}
	// The synthetic C10 carries failure_reason=job_boot_timeout.
	if len(f.server.forwards) != 1 || f.server.forwards[0].ep != EPFail {
		t.Fatalf("forwards = %+v", f.server.forwards)
	}
	if !strings.Contains(f.server.forwards[0].body, "job_boot_timeout") {
		t.Errorf("fail report body = %s", f.server.forwards[0].body)
	}
}

func TestOnBootTimeoutKeepsEntryWhenReportFails(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "t-boot-fail")
	f.server.forwardErr = errors.New("transport down")

	if err := f.sched.OnBootTimeout(context.Background(), "t-boot-fail"); err != nil {
		t.Fatalf("OnBootTimeout: %v", err)
	}
	// Contract §4: the terminal callback is never dropped — the entry stays
	// and a reconcile round retries the report before cleaning up.
	e, ok := f.reg.Get("t-boot-fail")
	if !ok || !e.IsTerminal() {
		t.Fatalf("entry = %+v, want retained terminal", e)
	}
	if f.hasJob("fm-t-boot-fail") == false {
		t.Error("Job must survive until the fail report lands")
	}

	// The server recovers; reconcile delivers the report and cleans up.
	f.server.forwardErr = nil
	if err := f.sched.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := f.reg.Get("t-boot-fail"); ok {
		t.Error("entry must be gone once the report landed")
	}
	if f.hasJob("fm-t-boot-fail") {
		t.Error("Job must be deleted once the report landed")
	}
}

func TestOnBootTimeoutIgnoresStartedOrTerminal(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "t-started")
	f.reg.MarkStarted("t-started", f.now)

	if err := f.sched.OnBootTimeout(context.Background(), "t-started"); err != nil {
		t.Fatalf("OnBootTimeout: %v", err)
	}
	if len(f.server.forwards) != 0 {
		t.Errorf("a started task must not be failed, forwards = %+v", f.server.forwards)
	}
	if !f.hasJob("fm-t-started") {
		t.Error("a started task's Job must survive")
	}
}

func TestOnTaskVanished(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "t-vanish")

	if err := f.sched.OnTaskVanished(context.Background(), "t-vanish"); err != nil {
		t.Fatalf("OnTaskVanished: %v", err)
	}
	if _, ok := f.reg.Get("t-vanish"); ok {
		t.Error("entry must be dropped")
	}
	if f.hasJob("fm-t-vanish") {
		t.Error("Job must be deleted")
	}
	// Scenario #9: no terminal report for a task the server deleted.
	if len(f.server.forwards) != 0 {
		t.Errorf("no report may be sent, forwards = %+v", f.server.forwards)
	}
}

func TestConvergeLeaseRefused(t *testing.T) {
	t.Run("running marks started", func(t *testing.T) {
		f := newFixture(t)
		f.claim(t, "t-lr")
		f.server.status = "running"

		if err := f.sched.ConvergeLeaseRefused(context.Background(), "t-lr"); err != nil {
			t.Fatalf("ConvergeLeaseRefused: %v", err)
		}
		e, _ := f.reg.Get("t-lr")
		if e.State != registry.StateRunning || e.StartedAt.IsZero() {
			t.Errorf("entry = %+v, want running with started_at", e)
		}
		if !f.hasJob("fm-t-lr") {
			t.Error("running task's Job must survive")
		}
	})

	t.Run("terminal settles and cleans up", func(t *testing.T) {
		f := newFixture(t)
		f.claim(t, "t-lt")
		f.server.status = "completed"

		if err := f.sched.ConvergeLeaseRefused(context.Background(), "t-lt"); err != nil {
			t.Fatalf("ConvergeLeaseRefused: %v", err)
		}
		if _, ok := f.reg.Get("t-lt"); ok {
			t.Error("terminal entry must be cleaned up")
		}
		if f.hasJob("fm-t-lt") {
			t.Error("terminal task's Job must be deleted")
		}
	})

	t.Run("gone vanishes", func(t *testing.T) {
		f := newFixture(t)
		f.claim(t, "t-lg")
		f.server.statusErr = ErrTaskNotFound

		if err := f.sched.ConvergeLeaseRefused(context.Background(), "t-lg"); err != nil {
			t.Fatalf("ConvergeLeaseRefused: %v", err)
		}
		if _, ok := f.reg.Get("t-lg"); ok {
			t.Error("gone task must be dropped")
		}
	})

	t.Run("dispatched keeps waiting", func(t *testing.T) {
		f := newFixture(t)
		f.claim(t, "t-ld")
		f.server.status = "dispatched"

		if err := f.sched.ConvergeLeaseRefused(context.Background(), "t-ld"); err != nil {
			t.Fatalf("ConvergeLeaseRefused: %v", err)
		}
		e, ok := f.reg.Get("t-ld")
		if !ok || e.IsTerminal() {
			t.Errorf("entry = %+v, want retained non-terminal", e)
		}
		if !f.hasJob("fm-t-ld") {
			t.Error("a dispatched task's Job must survive (server will redispatch)")
		}
	})

	t.Run("status error defers", func(t *testing.T) {
		f := newFixture(t)
		f.claim(t, "t-le")
		f.server.statusErr = errors.New("5xx")

		if err := f.sched.ConvergeLeaseRefused(context.Background(), "t-le"); err == nil {
			t.Error("a status failure must surface so the next round retries")
		}
		if _, ok := f.reg.Get("t-le"); !ok {
			t.Error("entry must survive a transient status failure")
		}
	})
}

func TestOnReportTerminalFailureEnqueuesPendingReport(t *testing.T) {
	f := newFixture(t)
	p := f.withPendingReports()
	e := f.claim(t, "t-pend")
	f.server.forwardCode = 503

	code, resp, err := f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{"result":"ok"}`))
	if err != nil {
		t.Fatalf("OnReport: %v", err)
	}
	if code != 502 || !strings.Contains(string(resp), "upstream unavailable") {
		t.Fatalf("OnReport = %d %s, want 502 upstream unavailable", code, resp)
	}
	if len(p.reports) != 1 || p.reports[0].taskID != "t-pend" || p.reports[0].ep != EPComplete {
		t.Fatalf("pending reports = %+v", p.reports)
	}
	if p.reports[0].body != `{"result":"ok"}` {
		t.Errorf("queued body = %s, want the daemon's original", p.reports[0].body)
	}
	// The entry stays terminal; the Job survives until the report lands.
	entry, ok := f.reg.Get("t-pend")
	if !ok || !entry.IsTerminal() {
		t.Errorf("entry = %+v, want retained terminal", entry)
	}
	if !f.hasJob("fm-t-pend") {
		t.Error("Job must survive until the report lands")
	}
}

func TestOnReportTerminalPermanent4xxNotEnqueued(t *testing.T) {
	f := newFixture(t)
	p := f.withPendingReports()
	e := f.claim(t, "t-perm")
	f.server.forwardCode = 400

	code, _, err := f.sched.OnReport(context.Background(), EPFail, e, []byte(`{"error":"x"}`))
	if err != nil || code != 400 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if len(p.reports) != 0 {
		t.Errorf("4xx is permanent: no queue entry expected, got %+v", p.reports)
	}
	if !f.hasJob("fm-t-perm") {
		t.Error("Job must stay for inspection on a permanent failure")
	}
}

func TestOnReportTerminal404IsIdempotentSuccess(t *testing.T) {
	f := newFixture(t)
	p := f.withPendingReports()
	e := f.claim(t, "t-404")
	f.server.forwardCode = 404

	code, _, err := f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{}`))
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v (terminal 404 must map to 200)", code, err)
	}
	if len(p.reports) != 0 {
		t.Errorf("404 terminal is moot, not queued: %+v", p.reports)
	}
	if f.hasJob("fm-t-404") {
		t.Error("Job must be cleaned up on a terminal 404")
	}
}

func TestOnReportNonTerminal404Vanishes(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "t-prog404")
	f.server.forwardCode = 404

	code, _, err := f.sched.OnReport(context.Background(), EPProgress, e, []byte(`{}`))
	if err != nil || code != 404 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if _, ok := f.reg.Get("t-prog404"); ok {
		t.Error("entry must be dropped when the server deleted the task")
	}
	if f.hasJob("fm-t-prog404") {
		t.Error("Job must be deleted when the server deleted the task")
	}
	// The vanish settlement refreshes the inflight gauge: the entry count
	// changed, so the gauge must not wait for the next claim event.
	if n := len(f.metrics.inflight); n == 0 || f.metrics.inflight[n-1] != 0 {
		t.Errorf("inflight gauge not refreshed on vanish: %v", f.metrics.inflight)
	}
}

func TestOnReportNonTerminalTransientMaps502(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "t-prog500")
	f.server.forwardCode = 500

	// proxy.md 转发 switch: upstream 5xx/408/429 on a non-terminal endpoint
	// answers the daemon 502 {"error":"upstream unavailable"}.
	code, resp, err := f.sched.OnReport(context.Background(), EPProgress, e, []byte(`{}`))
	if err != nil {
		t.Fatalf("OnReport: %v", err)
	}
	if code != 502 || !strings.Contains(string(resp), "upstream unavailable") {
		t.Fatalf("OnReport = %d %s, want 502 upstream unavailable", code, resp)
	}
}
