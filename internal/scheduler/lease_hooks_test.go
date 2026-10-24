package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

// fakePendingReports records enqueued terminal reports and models the queue
// membership the scheduler gates its object cleanup on.
type fakePendingReports struct {
	reports []pendingCall
	queued  map[string]bool
	err     error // Enqueue's durable-write failure
	// notRebuilt models an outstanding carrier list: Queued answers true for
	// every task (unknown is not empty) and Rebuilt answers false.
	notRebuilt bool
	// deleteCarrier is the cluster delete the queue performs for a settled
	// carrier; the fixture wires it to the job-client fake.
	deleteCarrier func(context.Context, string) error
}

type pendingCall struct {
	taskID        string
	jobName       string
	daemonID      string
	workspaceID   string
	ep            Endpoint
	body          string
	failureReason string
}

func (f *fakePendingReports) Enqueue(_ context.Context, taskID, jobName, daemonID, workspaceID string, ep Endpoint, body []byte, failureReason string) error {
	f.reports = append(f.reports, pendingCall{taskID, jobName, daemonID, workspaceID, ep, string(body), failureReason})
	if f.err != nil {
		return f.err
	}
	if f.queued == nil {
		f.queued = map[string]bool{}
	}
	f.queued[taskID] = true
	return nil
}

// Queued reports whether the report is still undelivered. An outstanding
// carrier list reads as "may be queued" (§4.3).
func (f *fakePendingReports) Queued(taskID string) bool {
	if f.notRebuilt {
		return true
	}
	return f.queued[taskID]
}

// Rebuilt reports whether the carrier list completed.
func (f *fakePendingReports) Rebuilt() bool { return !f.notRebuilt }

// HasQueuedEntry reports the known membership, ignoring the uncompleted-list
// conservatism.
func (f *fakePendingReports) HasQueuedEntry(taskID string) bool { return f.queued[taskID] }

// Drop forgets the report the direct forward settled.
func (f *fakePendingReports) Drop(_ context.Context, taskID string) { delete(f.queued, taskID) }

// DropIfUnchanged forgets the report only while the recorded snapshot is the
// one the caller wrote: a later Enqueue for the same task keeps its entry.
func (f *fakePendingReports) DropIfUnchanged(_ context.Context, taskID string, ep Endpoint, body []byte) {
	for i := len(f.reports) - 1; i >= 0; i-- {
		rep := f.reports[i]
		if rep.taskID != taskID {
			continue
		}
		if rep.ep == ep && rep.body == string(body) {
			delete(f.queued, taskID)
		}
		return
	}
}

// SweepCarrier models the queue's one-step settled check and delete: an
// unfinished rebuild or a live entry keeps the object.
func (f *fakePendingReports) SweepCarrier(ctx context.Context, taskID, name string) (bool, error) {
	if f.Queued(taskID) {
		return false, nil
	}
	if f.deleteCarrier == nil {
		return false, nil
	}
	if err := f.deleteCarrier(ctx, name); err != nil {
		return false, err
	}
	return true, nil
}

// Restore is the carrier rebuild: the fake has no carriers.
func (f *fakePendingReports) Restore(context.Context) error { return nil }

// Drain models the recovery loop's round: every queued report is re-delivered
// once through sender and the queue is reported empty.
func (f *fakePendingReports) Drain(ctx context.Context, sender ReportSender) (int, error) {
	for taskID := range f.queued {
		if _, _, err := sender.DeliverQueued(ctx, EPFail, taskID, "", nil, ""); err != nil {
			return len(f.queued), err
		}
		delete(f.queued, taskID)
	}
	return 0, nil
}

// deliver models the recovery loop's drain landing the report.
func (f *fakePendingReports) deliver(taskID string) { delete(f.queued, taskID) }

func (f *fixture) withPendingReports() *fakePendingReports {
	// The fixture wires a queue for every test (New requires one); this
	// resets it so a test sees only the reports it causes.
	f.pending = &fakePendingReports{}
	// The sweep's carrier delete goes through the queue seam; this helper
	// rebuilds the fake, so it restores the wiring newFixtureUnder applies.
	f.pending.deleteCarrier = f.jobs.DeleteSecret
	WithPendingReports(f.pending)(f.sched)
	return f.pending
}

// withDurableReports swaps the fixture's recording fake for the real queue,
// so a test can observe the durable file and reopen it as a new process.
func (f *fixture) withDurableReports(t *testing.T) *PendingReports {
	t.Helper()
	p, err := NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
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
	if e, ok := f.reg.Get("t-boot"); !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Errorf("entry = %+v, %v; want a retained draining entry", e, ok)
	}
	if f.hasJob("fm-t-boot") {
		t.Error("Job must be deleted after the boot-timeout settlement")
	}
	if !f.reg.IsDone("ws-1", "t-boot") {
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

func TestOnBootTimeoutQueuesReportAndKeepsObjects(t *testing.T) {
	f := newFixture(t)
	p := f.withPendingReports()
	f.claim(t, "t-boot-fail")
	f.server.forwardErr = errors.New("transport down")

	if err := f.sched.OnBootTimeout(context.Background(), "t-boot-fail"); err != nil {
		t.Fatalf("OnBootTimeout: %v", err)
	}
	// §10: the terminal callback is not dropped — it lands in the
	// durable queue and the objects stay until a drain delivers it.
	e, ok := f.reg.Get("t-boot-fail")
	if !ok || !e.IsTerminal() {
		t.Fatalf("entry = %+v, want retained terminal", e)
	}
	if len(p.reports) != 1 || p.reports[0].taskID != "t-boot-fail" || p.reports[0].ep != EPFail {
		t.Fatalf("pending reports = %+v, want the boot-timeout fail report", p.reports)
	}
	if !strings.Contains(p.reports[0].body, "job_boot_timeout") {
		t.Errorf("queued body = %s, want the C10 fail body", p.reports[0].body)
	}
	if !f.hasJob("fm-t-boot-fail") {
		t.Error("Job must survive until the queued report lands")
	}

	// A drain delivered the report: the next round cleans up. The scheduler
	// itself never forwards it a second time.
	p.deliver("t-boot-fail")
	if err := f.sched.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if e, ok := f.reg.Get("t-boot-fail"); !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Errorf("entry = %+v, %v; want a retained draining entry", e, ok)
	}
	if f.hasJob("fm-t-boot-fail") {
		t.Error("Job must be deleted once the report landed")
	}
	if n := len(f.server.forwards); n != 1 {
		t.Errorf("forwards = %d, want 1 (the queue owns the redelivery)", n)
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
	f.server.statusErr = ErrTaskNotFound

	if err := f.sched.OnTaskVanished(context.Background(), "t-vanish"); err != nil {
		t.Fatalf("OnTaskVanished: %v", err)
	}
	if e, ok := f.reg.Get("t-vanish"); !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Errorf("entry = %+v, %v; want a retained draining entry", e, ok)
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
	t.Run("running records the start fact only", func(t *testing.T) {
		f := newFixture(t)
		f.claim(t, "t-lr")
		f.server.status = "running"

		if err := f.sched.ConvergeLeaseRefused(context.Background(), "t-lr"); err != nil {
			t.Fatalf("ConvergeLeaseRefused: %v", err)
		}
		e, _ := f.reg.Get("t-lr")
		// Adoption leaves the state alone; only the report path advances it
		// (ADR-026 后果).
		if e.State != registry.StateJobCreated || e.StartedAt.IsZero() {
			t.Errorf("entry = %+v, want job_created with started_at", e)
		}
		if !f.hasJob("fm-t-lr") {
			t.Error("running task's Job must survive")
		}
	})

	t.Run("waiting_local_directory records the start fact", func(t *testing.T) {
		f := newFixture(t)
		f.claim(t, "t-lw")
		f.server.status = "waiting_local_directory"

		if err := f.sched.ConvergeLeaseRefused(context.Background(), "t-lw"); err != nil {
			t.Fatalf("ConvergeLeaseRefused: %v", err)
		}
		e, _ := f.reg.Get("t-lw")
		if e.State != registry.StateJobCreated || e.StartedAt.IsZero() {
			t.Errorf("entry = %+v, want job_created with started_at", e)
		}
		if !f.hasJob("fm-t-lw") {
			t.Error("a daemon-held task's Job must survive")
		}
	})

	t.Run("unclassified status is reported and kept", func(t *testing.T) {
		f := newFixture(t)
		buf := newLogScheduler(t, f, slog.LevelInfo)
		f.claim(t, "t-lu")
		f.server.status = "paused"

		if err := f.sched.ConvergeLeaseRefused(context.Background(), "t-lu"); err != nil {
			t.Fatalf("ConvergeLeaseRefused: %v", err)
		}
		record := findLogRecord(t, buf.Bytes(), "task.status_unclassified")
		assertErrorLevel(t, record)
		assertTaskIdentity(t, record, "t-lu", "fm-t-lu", "fm-t-lu")
		if got := record["status"]; got != "paused" {
			t.Errorf("status = %v, want paused", got)
		}
		if e, ok := f.reg.Get("t-lu"); !ok || e.IsTerminal() {
			t.Errorf("entry = %+v, %v; want retained non-terminal", e, ok)
		}
		if !f.hasJob("fm-t-lu") {
			t.Error("an unclassified status must keep the Job")
		}
	})

	t.Run("terminal settles and cleans up", func(t *testing.T) {
		f := newFixture(t)
		f.claim(t, "t-lt")
		f.server.status = "completed"

		if err := f.sched.ConvergeLeaseRefused(context.Background(), "t-lt"); err != nil {
			t.Fatalf("ConvergeLeaseRefused: %v", err)
		}
		if e, ok := f.reg.Get("t-lt"); !ok || !e.IsTerminal() || !e.IsDraining() {
			t.Errorf("entry = %+v, %v; want a retained draining entry", e, ok)
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
		if e, ok := f.reg.Get("t-lg"); !ok || !e.IsTerminal() || !e.IsDraining() {
			t.Errorf("entry = %+v, %v; want a retained draining entry", e, ok)
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

// TestConvergeLeaseRefusedTerminalRecordsTheVerdict (AC-10,
// TC-tech-observability-01): C13 is the only writer of a verdict it converges
// on, so that write must count foreman_tasks_total by result and emit
// task.terminal with the task identity — a settle observed on neither face
// leaves the result invisible (§9.1, §9.2).
func TestConvergeLeaseRefusedTerminalRecordsTheVerdict(t *testing.T) {
	for _, result := range []registry.Result{registry.ResultCompleted, registry.ResultFailed} {
		t.Run(string(result), func(t *testing.T) {
			f := newFixture(t)
			buf := newLogScheduler(t, f, slog.LevelInfo)
			e := f.claim(t, "t-lc")
			f.server.status = string(result)

			if err := f.sched.ConvergeLeaseRefused(context.Background(), "t-lc"); err != nil {
				t.Fatalf("ConvergeLeaseRefused: %v", err)
			}
			if got := f.metrics.terminal; len(got) != 1 || got[0] != string(result) {
				t.Fatalf("terminal metric = %v, want [%s]", got, result)
			}
			records := collectLogRecords(t, buf.Bytes(), "task.terminal")
			if len(records) != 1 {
				t.Fatalf("task.terminal events = %d, want 1", len(records))
			}
			assertTaskIdentity(t, records[0], "t-lc", e.JobName, e.DaemonID)
			if got := records[0]["result"]; got != string(result) {
				t.Errorf("task.terminal result = %v, want %s", got, result)
			}
			if got := records[0]["workspace_id"]; got != "ws-1" {
				t.Errorf("task.terminal workspace_id = %v, want ws-1", got)
			}

			// The settled entry makes a second convergence a no-op: the
			// verdict is observed once, never once per round.
			if err := f.sched.ConvergeLeaseRefused(context.Background(), "t-lc"); err != nil {
				t.Fatalf("second ConvergeLeaseRefused: %v", err)
			}
			if got := f.metrics.terminal; len(got) != 1 {
				t.Errorf("terminal metric after reconvergence = %v, want one count", got)
			}
			if records := collectLogRecords(t, buf.Bytes(), "task.terminal"); len(records) != 1 {
				t.Errorf("task.terminal events after reconvergence = %d, want 1", len(records))
			}
		})
	}
}

// TestSettleReplayedTerminalRecordsTheVerdict (AC-10,
// TC-tech-observability-02): the recovery drain is a terminal writer of its
// own — a rebuilt entry carries no verdict, so the replayed report is the only
// source of one. That settle must reach both observation faces like every
// other terminal write; counted but unlogged, the replayed result sits off the
// single-task log chain (§9.1).
func TestSettleReplayedTerminalRecordsTheVerdict(t *testing.T) {
	for _, ep := range []Endpoint{EPComplete, EPFail, EPCancelAck} {
		t.Run(string(ep), func(t *testing.T) {
			f := newFixture(t)
			buf := newLogScheduler(t, f, slog.LevelInfo)
			e := f.claim(t, "t-rp")

			// The rebuilt-entry shape: no verdict and no decided body, so the
			// queue entry is what settles the task after the restart.
			code, _, err := f.sched.DeliverQueued(context.Background(), ep, "t-rp", e.WorkspaceID, []byte(`{}`), "job_failed")
			if err != nil || code != 200 {
				t.Fatalf("DeliverQueued = %d, %v; want 200", code, err)
			}
			if got := f.metrics.terminal; len(got) != 1 || got[0] != ep.TerminalResult() {
				t.Fatalf("terminal metric = %v, want [%s]", got, ep.TerminalResult())
			}
			records := collectLogRecords(t, buf.Bytes(), "task.terminal")
			if len(records) != 1 {
				t.Fatalf("task.terminal events = %d, want 1", len(records))
			}
			assertTaskIdentity(t, records[0], "t-rp", e.JobName, e.DaemonID)
			if got := records[0]["workspace_id"]; got != "ws-1" {
				t.Errorf("task.terminal workspace_id = %v, want ws-1", got)
			}
			if got := records[0]["result"]; got != ep.TerminalResult() {
				t.Errorf("task.terminal result = %v, want %s", got, ep.TerminalResult())
			}

			// A second drain finds the entry settled: the verdict is observed
			// once, never once per replay.
			if _, _, err := f.sched.DeliverQueued(context.Background(), ep, "t-rp", e.WorkspaceID, []byte(`{}`), "job_failed"); err != nil {
				t.Fatalf("second DeliverQueued: %v", err)
			}
			if got := f.metrics.terminal; len(got) != 1 {
				t.Errorf("terminal metric after a second replay = %v, want one count", got)
			}
			if records := collectLogRecords(t, buf.Bytes(), "task.terminal"); len(records) != 1 {
				t.Errorf("task.terminal events after a second replay = %d, want 1", len(records))
			}
		})
	}
}

func TestOnReportTerminalFailureEnqueuesPendingReport(t *testing.T) {
	f := newFixture(t)
	p := f.withPendingReports()
	e := f.claim(t, "t-pend")
	f.server.forwardCode = 503

	code, resp, err := f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{"result":"ok"}`), "")
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

func TestOnReportTerminalPermanent4xxNotRetried(t *testing.T) {
	f := newFixture(t)
	p := f.withPendingReports()
	e := f.claim(t, "t-perm")
	f.server.forwardCode = 400

	code, _, err := f.sched.OnReport(context.Background(), EPFail, e, []byte(`{"error":"x"}`), "")
	if err != nil || code != 400 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	// The report was written ahead of the attempt; a permanent refusal is
	// never replayed, so the queue entry is dropped again.
	if p.Queued("t-perm") {
		t.Errorf("4xx is permanent: the write-ahead entry must be dropped, got %+v", p.reports)
	}
	// The verdict is written, so the Job is torn down like every other
	// terminal path — the same conclusion 2xx/404/409 reach.
	if f.hasJob("fm-t-perm") {
		t.Error("Job must be deleted on a permanent failure")
	}
}

func TestOnReportTerminal409IsMootSuccess(t *testing.T) {
	f := newFixture(t)
	p := f.withPendingReports()
	e := f.claim(t, "t-409")
	f.server.forwardCode = 409

	code, _, err := f.sched.OnReport(context.Background(), EPFail, e, []byte(`{"error":"x"}`), "")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v (terminal 409 must map to 200)", code, err)
	}
	// The write-ahead entry is queued before the forward and dropped when the
	// server answers with a verdict that is already written.
	if p.Queued("t-409") {
		t.Errorf("409 terminal is moot: the write-ahead entry must be dropped, got %+v", p.reports)
	}
	if f.hasJob("fm-t-409") {
		t.Error("Job must be cleaned up on a terminal 409")
	}
}

func TestOnReportTerminal404IsIdempotentSuccess(t *testing.T) {
	f := newFixture(t)
	p := f.withPendingReports()
	e := f.claim(t, "t-404")
	f.server.forwardCode = 404

	code, _, err := f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{}`), "")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v (terminal 404 must map to 200)", code, err)
	}
	if p.Queued("t-404") {
		t.Errorf("404 terminal is moot: the write-ahead entry must be dropped, got %+v", p.reports)
	}
	if f.hasJob("fm-t-404") {
		t.Error("Job must be cleaned up on a terminal 404")
	}
}

func TestOnReportNonTerminal404Vanishes(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "t-prog404")
	f.server.forwardCode = 404
	f.server.statusErr = ErrTaskNotFound

	code, _, err := f.sched.OnReport(context.Background(), EPProgress, e, []byte(`{}`), "")
	if err != nil || code != 404 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if e, ok := f.reg.Get("t-prog404"); !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Errorf("entry = %+v, %v; want a retained draining entry", e, ok)
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

	// Transient upstream failure: upstream 5xx/408/429 on a non-terminal
	// endpoint answers the daemon 502 {"error":"upstream unavailable"}.
	code, resp, err := f.sched.OnReport(context.Background(), EPProgress, e, []byte(`{}`), "")
	if err != nil {
		t.Fatalf("OnReport: %v", err)
	}
	if code != 502 || !strings.Contains(string(resp), "upstream unavailable") {
		t.Fatalf("OnReport = %d %s, want 502 upstream unavailable", code, resp)
	}
}

// TestReportFailDropsPermanent4xxAndKeepsTransient pins the compensation
// forward's queue face under the write-ahead design: a permanent 4xx must drop
// the already-queued report instead of leaving it for a replay that can only be
// refused again; a transient failure leaves it for the drain.
func TestReportFailDropsPermanent4xxAndKeepsTransient(t *testing.T) {
	cases := []struct {
		name       string
		code       int
		wantErr    bool
		wantQueued bool
	}{
		{"400 permanent", 400, true, false},
		{"403 permanent", 403, true, false},
		{"409 already settled", 409, false, false},
		{"408 retryable", 408, true, true},
		{"429 retryable", 429, true, true},
		{"503 transient", 503, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			p := f.withPendingReports()
			e := f.claim(t, "t-rf")
			// The verdict path queued the report ahead of the forward; this
			// test models that state directly.
			if err := p.Enqueue(context.Background(), e.TaskID, e.JobName, e.DaemonID, "", EPFail, []byte(`{}`), ""); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			f.server.forwardCode = tc.code

			err := f.sched.reportFail(t.Context(), e, []byte(`{}`))
			if tc.wantErr != (err != nil) {
				t.Fatalf("reportFail(%d) err = %v, wantErr %v", tc.code, err, tc.wantErr)
			}
			if got := p.Queued(e.TaskID); got != tc.wantQueued {
				t.Errorf("Queued after %d = %v, want %v (queue %+v)", tc.code, got, tc.wantQueued, p.reports)
			}
		})
	}
}

// TestDeliverQueuedSettlesPermanent4xx pins the queue replay's §1.5 face: a
// permanent 4xx leaves the queue (no drain replay to the TTL) and deletes the
// Job, while 408/429 stay queued for the retry budget.
func TestDeliverQueuedSettlesPermanent4xx(t *testing.T) {
	cases := []struct {
		name       string
		code       int
		wantQueued bool
	}{
		{"400 permanent", 400, false},
		{"403 permanent", 403, false},
		{"408 retryable", 408, true},
		{"429 retryable", 429, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			p := f.withDurableReports(t)
			e := f.claim(t, "t-dq")
			if err := p.Enqueue(context.Background(), e.TaskID, e.JobName, e.DaemonID, "", EPFail, []byte(`{}`), ""); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			f.server.forwardCode = tc.code

			code, _, err := f.sched.DeliverQueued(t.Context(), EPFail, e.TaskID, "", []byte(`{}`), "")
			if err != nil || code != tc.code {
				t.Fatalf("DeliverQueued = %d, %v; want the upstream code %d", code, err, tc.code)
			}
			if got := p.Queued(e.TaskID); got != tc.wantQueued {
				t.Fatalf("Queued after %d = %v, want %v", tc.code, got, tc.wantQueued)
			}
			if !tc.wantQueued && f.hasJob(e.JobName) {
				t.Error("a permanent rejection must delete the Job")
			}
			if tc.wantQueued && !f.hasJob(e.JobName) {
				t.Error("a retryable failure must keep the Job")
			}
		})
	}
}

// TestDeliverQueuedWithoutEntrySettlesPermanent4xx pins the replay face once
// the credential is reclaimed: the entry is gone, so no Job can be deleted,
// but a permanent 4xx must still leave the queue instead of being replayed to
// the TTL. 404/409 are the delivered faces, not rejections: they leave through
// the drain's delivered branch and log no permanent drop.
func TestDeliverQueuedWithoutEntrySettlesPermanent4xx(t *testing.T) {
	cases := []struct {
		name        string
		code        int
		wantQueued  bool
		wantDropped bool
		wantDrained bool
	}{
		{"400 permanent", 400, false, true, false},
		{"403 permanent", 403, false, true, false},
		{"408 retryable", 408, true, false, false},
		{"429 retryable", 429, true, false, false},
		{"404 delivered", 404, true, false, true},
		{"409 delivered", 409, true, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			p := f.withDurableReports(t)
			e := f.claim(t, "t-dq-gone")
			if err := p.Enqueue(context.Background(), e.TaskID, e.JobName, e.DaemonID, "", EPFail, []byte(`{}`), ""); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			// The credential was reclaimed before the replay landed: the
			// entry is gone and there is no Job handle left to delete.
			if err := f.reg.Delete(e.TaskID); err != nil {
				t.Fatalf("reg.Delete: %v", err)
			}
			logs := &strings.Builder{}
			f.sched.log = slog.New(slog.NewJSONHandler(logs, nil))
			f.server.forwardCode = tc.code

			code, _, err := f.sched.DeliverQueued(t.Context(), EPFail, e.TaskID, "", []byte(`{}`), "")
			if err != nil || code != tc.code {
				t.Fatalf("DeliverQueued = %d, %v; want the upstream code %d", code, err, tc.code)
			}
			if got := strings.Contains(logs.String(), `"reason":"permanent_4xx"`); got != tc.wantDropped {
				t.Errorf("permanent_4xx drop log = %v, want %v; logs:\n%s", got, tc.wantDropped, logs)
			}
			if got := p.Queued(e.TaskID); got != tc.wantQueued {
				t.Fatalf("Queued after %d = %v, want %v", tc.code, got, tc.wantQueued)
			}
			if len(f.jobs.deletedJobs) != 0 {
				t.Errorf("entry-gone replay deleted Jobs: %v", f.jobs.deletedJobs)
			}
			if !tc.wantDrained {
				return
			}
			// 404/409 still leave the queue: the drain's delivered branch
			// drops the snapshot after the replay lands.
			remaining, err := p.Drain(t.Context(), f.sched)
			if err != nil || remaining != 0 {
				t.Fatalf("Drain = %d, %v; want the delivered report dropped", remaining, err)
			}
		})
	}
}

// TestDeliverQueuedPermanent4xxDropsOnlyTheReplayedSnapshot pins the queue
// face of the permanent-rejection exit: what leaves is the snapshot the drain
// handed over, never the task id. A late report enqueued while the replay was
// in flight is its own forward's write-ahead entry (§10); deleting it with
// the refused snapshot would leave that report without a queue home.
func TestDeliverQueuedPermanent4xxDropsOnlyTheReplayedSnapshot(t *testing.T) {
	cases := []struct {
		name      string
		entryGone bool
	}{
		{"entry present", false},
		{"entry gone", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			p := f.withDurableReports(t)
			e := f.claim(t, "t-dq-snap")
			sent := []byte(`{"result":"ok"}`)
			newer := []byte(`{"result":"failed","failure_reason":"job_failed"}`)
			if err := p.Enqueue(context.Background(), e.TaskID, e.JobName, e.DaemonID, "", EPComplete, sent, ""); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			// The concurrent enqueue landed after the drain snapshotted
			// `sent`, so the queued entry now belongs to another forward.
			if err := p.Enqueue(context.Background(), e.TaskID, e.JobName, e.DaemonID, "", EPFail, newer, "job_failed"); err != nil {
				t.Fatalf("re-Enqueue: %v", err)
			}
			if tc.entryGone {
				if err := f.reg.Delete(e.TaskID); err != nil {
					t.Fatalf("reg.Delete: %v", err)
				}
			}
			f.server.forwardCode = 400

			if code, _, err := f.sched.DeliverQueued(t.Context(), EPComplete, e.TaskID, "", sent, ""); err != nil || code != 400 {
				t.Fatalf("DeliverQueued = %d, %v; want the upstream code 400", code, err)
			}
			if !p.Queued(e.TaskID) {
				t.Fatal("the newer enqueue owns its own write-ahead entry; the refused snapshot must not drop it by task id")
			}
			// The survivor is the newer snapshot, and the next drain replays
			// exactly that report.
			sender := &fakeSender{code: 200}
			if remaining, err := p.Drain(context.Background(), sender); err != nil || remaining != 0 {
				t.Fatalf("Drain = %d, %v; want the newer report delivered", remaining, err)
			}
			if len(sender.sent) != 1 || !strings.HasPrefix(sender.sent[0], e.TaskID+":"+string(EPFail)+":") {
				t.Fatalf("replayed = %v, want the newer failure report", sender.sent)
			}
		})
	}
}

// TestDeliverQueuedPermanent4xxDropsTheQueuedSnapshotAfterAReplacedBody: the
// replay forwards the instance's decided body, so the identity of the entry
// being dropped is the queue's own snapshot — comparing the body actually sent
// would leave the refused entry queued until the TTL.
func TestDeliverQueuedPermanent4xxDropsTheQueuedSnapshotAfterAReplacedBody(t *testing.T) {
	f := newFixture(t)
	p := f.withDurableReports(t)
	e := f.claim(t, "t-dq-decided")
	queued := []byte(`{"result":"failed","failure_reason":"job_create_failed"}`)
	decided := []byte(`{"result":"failed","failure_reason":"job_failed"}`)
	if err := p.Enqueue(context.Background(), e.TaskID, e.JobName, e.DaemonID, "", EPFail, queued, "job_create_failed"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, ok := f.reg.RegisterDecidedReport(e.TaskID, decided); !ok {
		t.Fatal("RegisterDecidedReport: entry missing")
	}
	f.server.forwardCode = 400

	if code, _, err := f.sched.DeliverQueued(t.Context(), EPFail, e.TaskID, "", queued, "job_create_failed"); err != nil || code != 400 {
		t.Fatalf("DeliverQueued = %d, %v; want the upstream code 400", code, err)
	}
	if len(f.server.forwards) != 1 || f.server.forwards[0].body != string(decided) {
		t.Fatalf("forwarded = %+v, want the instance's decided body", f.server.forwards)
	}
	if p.Queued(e.TaskID) {
		t.Fatal("the snapshot the replay sent was permanently refused; its queue entry must leave")
	}
}
