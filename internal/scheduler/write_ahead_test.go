package scheduler

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
)

// TestOnReportTerminalWritesAheadOfTheForward pins the ordering §10 needs:
// the report is durable before the forward is attempted, so a crash in the
// forward window replays it instead of losing the verdict with the process.
// The successful direct answer then drops the entry again.
func TestOnReportTerminalWritesAheadOfTheForward(t *testing.T) {
	f := newFixture(t)
	p := f.withDurableReports(t)
	e := f.claim(t, "task-1")
	e.State = registry.StateRunning
	f.reg.Put(e)

	entered := make(chan struct{})
	release := make(chan struct{})
	f.server.onForward = func() {
		close(entered)
		<-release
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{"result":"ok"}`), "")
		done <- err
	}()
	<-entered
	if _, err := os.Stat(filepath.Join(p.dir, "task-1.json")); err != nil {
		t.Fatalf("queue file missing while the forward is in flight: %v", err)
	}
	if !p.Queued("task-1") {
		t.Fatal("the write-ahead report must count as undelivered")
	}
	if entry, ok := f.reg.Get("task-1"); !ok || !entry.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want the verdict written before the forward", entry, ok)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("OnReport: %v", err)
	}
	if p.Queued("task-1") || p.Len() != 0 {
		t.Fatalf("the delivered report must leave the queue: Queued=%v Len=%d", p.Queued("task-1"), p.Len())
	}
	if !f.jobs.deleted("fm-task-1") {
		t.Fatal("the Job must be deleted once the report landed")
	}
}

// TestWriteAheadReportReplaysInANewProcess: a process that died before the
// forward completed leaves the report on disk, and a queue opened over the
// same state root delivers it — the crash window the old mark-then-enqueue
// order lost.
func TestWriteAheadReportReplaysInANewProcess(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	p, err := NewPendingReports(dir)
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	WithPendingReports(p)(f.sched)
	e := f.claim(t, "task-1")
	e.State = registry.StateRunning
	f.reg.Put(e)

	entered := make(chan struct{})
	release := make(chan struct{})
	f.server.onForward = func() {
		close(entered)
		<-release
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{"result":"ok"}`), "")
	}()
	<-entered

	// The process is gone: a fresh queue over the same directory is all a
	// restart has, and the drain must find the report.
	restarted, err := NewPendingReports(dir)
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	sender := &fakeSender{code: 200}
	remaining, err := restarted.Drain(context.Background(), sender)
	if err != nil || remaining != 0 {
		t.Fatalf("Drain after restart = %d, %v", remaining, err)
	}
	if len(sender.sent) != 1 || !strings.Contains(sender.sent[0], "task-1:complete:") {
		t.Fatalf("replayed = %v, want the write-ahead complete report", sender.sent)
	}

	close(release)
	<-done
}

// TestCompensatedFailReportIsDurableBeforeTheForward pins the same ordering on
// the compensation path: the synthesized failure verdict writes its report
// ahead of the verdict and the forward, and the accepted answer drops it.
func TestCompensatedFailReportIsDurableBeforeTheForward(t *testing.T) {
	f := newFixture(t)
	p := f.withDurableReports(t)
	e := f.claim(t, "task-1")
	e.State = registry.StateRunning
	f.reg.Put(e)
	f.now = testNow.Add(2 * time.Minute)
	f.server.status = "running"

	entered := make(chan struct{})
	release := make(chan struct{})
	f.server.onForward = func() {
		close(entered)
		<-release
	}
	done := make(chan error, 1)
	go func() {
		done <- f.sched.OnJobGone(context.Background(), "fm-task-1")
	}()
	<-entered
	if !p.Queued("task-1") {
		t.Fatal("the compensation report must be durable before its forward")
	}
	if entry, ok := f.reg.Get("task-1"); !ok || !entry.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want the failure verdict written", entry, ok)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("OnJobGone: %v", err)
	}
	if p.Queued("task-1") {
		t.Fatal("the delivered compensation report must leave the queue")
	}
	if len(f.server.forwards) != 1 || f.server.forwards[0].ep != EPFail {
		t.Fatalf("forwards = %+v, want exactly one fail report", f.server.forwards)
	}
}

// TestTerminalReportDroppedAtTheRetentionBound: with the queue at its bound, an
// inbound terminal report has no write-ahead landing. The queue refuses it with
// its own error row — the alarm face (§4.6, ADR-046 decision 6) — and the
// scheduler does not log the refusal a second time. The verdict is still
// written and the forward still goes out: the queue is a fallback, not the
// delivery path.
func TestTerminalReportDroppedAtTheRetentionBound(t *testing.T) {
	f := newFixture(t)
	queueLogs := &strings.Builder{}
	p, err := NewPendingReports(t.TempDir(),
		WithPendingReportsMaxEntries(1),
		WithPendingReportsLogger(slog.New(slog.NewJSONHandler(queueLogs, nil))))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	// The scheduler's own face is captured too: the refusal must not grow a
	// second, differently-worded line in enqueuePending.
	schedLogs := &strings.Builder{}
	cfg, err := LoadConfig(testEnv(nil))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	sched, err := New(cfg, f.reg, f.jobs, f.builder, f.server, f.metrics,
		WithClock(func() time.Time { return f.now }),
		WithPendingReports(p),
		WithRevoker(f.revoker),
		WithProbeRegistry(registry.NewProbeRegistry()),
		WithLogger(observability.NewLogger(schedLogs, slog.LevelInfo, "")))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	f.sched = sched
	f.server.forwardCode = 503

	first := f.claim(t, "task-1")
	if code, _, err := f.sched.OnReport(context.Background(), EPComplete, first, []byte(`{"result":"ok"}`), ""); err != nil || code != 502 {
		t.Fatalf("OnReport task-1 = %d, %v; want 502 with the upstream down", code, err)
	}
	if got := p.Len(); got != 1 {
		t.Fatalf("pending reports = %d, want the first report queued", got)
	}

	second := f.claim(t, "task-2")
	if code, _, err := f.sched.OnReport(context.Background(), EPComplete, second, []byte(`{"result":"ok"}`), ""); err != nil || code != 502 {
		t.Fatalf("OnReport task-2 = %d, %v; want the same forward", code, err)
	}
	if got := p.Len(); got != 1 {
		t.Fatalf("pending reports = %d, want the bound held", got)
	}
	if p.Queued("task-2") {
		t.Fatal("the refused report must not gate task-2's objects")
	}
	if entry, ok := f.reg.Get("task-2"); !ok || !entry.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want the verdict written despite the refusal", entry, ok)
	}
	// The refused report was still forwarded: the queue is a fallback, not the
	// delivery path.
	forwarded := false
	for _, call := range f.server.forwards {
		if call.ep == EPComplete && call.taskID == "task-2" {
			forwarded = true
		}
	}
	if !forwarded {
		t.Fatalf("forwards = %+v, want task-2's complete report attempted", f.server.forwards)
	}
	// One drop row for the one refused report, with the task-face identity.
	record := pendingLogRecord(t, queueLogs.String(), "task.forward_dropped", "task-2")
	assertPendingLogIdentity(t, record, "fm-task-2", "fm-task-2")
	if got := strings.Count(queueLogs.String(), `"msg":"task.forward_dropped"`); got != 1 {
		t.Fatalf("drop rows = %d, want exactly the queue's alarm row", got)
	}
	// The scheduler must not repeat the queue's drop as an undelivered line.
	if got := strings.Count(schedLogs.String(), `"msg":"task.forward_undelivered"`); got != 0 {
		t.Fatalf("scheduler undelivered rows = %d, want none for the bound's drop", got)
	}
}
