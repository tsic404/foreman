package scheduler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// terminalWriteFailingStore is the shared face of one terminal write: the
// verdict commit answers the programmed error. The embedded interface keeps
// the fixture to the methods the registry's stage-one interface and its
// promotion check reach.
type terminalWriteFailingStore struct {
	registry.SharedStore

	commitErr error
	commits   int
	mu        sync.Mutex
	// entered and release park the first commit mid-flight, so a test can
	// land another settlement for the same task in that window: entered is
	// closed when the commit starts, release ends the park. Later commits
	// land, the way another instance's verdict would.
	entered chan struct{}
	release chan struct{}
}

func (f *terminalWriteFailingStore) Shared() bool { return true }

func (f *terminalWriteFailingStore) CommitTerminal(_ context.Context, ws, taskID string, result registry.Result, reason string, completedAt, drainDeadline time.Time) (registry.TaskEntry, bool, error) {
	f.mu.Lock()
	f.commits++
	n := f.commits
	f.mu.Unlock()
	if n == 1 {
		if f.entered != nil {
			close(f.entered)
			<-f.release
		}
		return registry.TaskEntry{}, false, f.commitErr
	}
	// The row already carries a verdict: this write is the no-op the shared
	// CAS answers with first=false.
	return registry.TaskEntry{
		TaskID:        taskID,
		WorkspaceID:   ws,
		JobName:       "fm-" + taskID,
		DaemonID:      "fm-" + taskID,
		State:         registry.StateTerminal,
		Result:        result,
		FailureReason: reason,
		CompletedAt:   completedAt,
		DrainDeadline: drainDeadline,
	}, false, nil
}

func (f *terminalWriteFailingStore) SetEntry(context.Context, string, string, registry.TaskEntry) error {
	return nil
}
func (f *terminalWriteFailingStore) PutEntry(context.Context, string, string, registry.TaskEntry) error {
	return nil
}
func (f *terminalWriteFailingStore) GetEntry(context.Context, string, string) (registry.TaskEntry, bool, error) {
	return registry.TaskEntry{}, false, nil
}
func (f *terminalWriteFailingStore) ListEntries(context.Context) ([]registry.TaskEntry, error) {
	return nil, nil
}
func (f *terminalWriteFailingStore) DeleteEntry(context.Context, string, string) error { return nil }
func (f *terminalWriteFailingStore) PutSettled(context.Context, string, string, registry.TaskEntry) error {
	return nil
}
func (f *terminalWriteFailingStore) GetSettled(context.Context, string, string) (registry.TaskEntry, bool, error) {
	return registry.TaskEntry{}, false, nil
}
func (f *terminalWriteFailingStore) ListSettled(context.Context) ([]registry.TaskEntry, error) {
	return nil, nil
}
func (f *terminalWriteFailingStore) Revoke(context.Context, string, string, time.Time) error {
	return nil
}
func (f *terminalWriteFailingStore) IsRevoked(context.Context, string, string) (bool, error) {
	return false, nil
}
func (f *terminalWriteFailingStore) ListRevoked(context.Context) (map[string]map[string]time.Time, error) {
	return nil, nil
}
func (f *terminalWriteFailingStore) Ping(context.Context) error { return nil }

// Slots is the shared in-flight counter the registry's gauge reads; zero
// keeps the fixture counting nothing on the shared face.
func (f *terminalWriteFailingStore) Slots(context.Context, string) (int64, error) {
	return 0, nil
}

// runningEntry is the pre-report entry shape the rollback tests start from.
func runningEntry(taskID string) registry.TaskEntry {
	return registry.TaskEntry{
		TaskID:      taskID,
		JobName:     "fm-" + taskID,
		DaemonID:    "fm-" + taskID,
		WorkspaceID: "ws-1",
		State:       registry.StateRunning,
		ClaimedAt:   testNow,
	}
}

// TestOnReportVerdictFailureWithdrawsTheQueuedReport pins the second half of
// §10's write-ahead order: the entry is written ahead of the verdict, so a
// verdict write that fails takes the entry with it. A report this instance
// never decided has no queue home — the drain must not forward it, or the
// server would hold a terminal verdict the local index contradicts while the
// task is still running (the daemon's retry carries the report instead).
func TestOnReportVerdictFailureWithdrawsTheQueuedReport(t *testing.T) {
	storeErr := errors.New("shared state unavailable")
	store := &terminalWriteFailingStore{commitErr: storeErr}
	f := newFixtureWith(t, registry.WithStateStore(store))
	carriers := newMemCarrierStore()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := p.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	WithPendingReports(p)(f.sched)
	e := runningEntry("task-1")
	if err := f.reg.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}

	code, _, err := f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{"result":"ok"}`), "")
	if !errors.Is(err, storeErr) {
		t.Fatalf("OnReport = %d, %v; want the verdict write's error to reach the caller", code, err)
	}
	if store.commits != 1 {
		t.Fatalf("terminal commits = %d, want exactly the one that failed", store.commits)
	}
	if got, ok := f.reg.Get("task-1"); !ok || got.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want the pre-report non-terminal entry", got, ok)
	}
	if p.HasQueuedEntry("task-1") || p.Len() != 0 {
		t.Fatalf("queue entry survived the failed verdict write: held=%v len=%d",
			p.HasQueuedEntry("task-1"), p.Len())
	}
	if got := carriers.count(); got != 0 {
		t.Fatalf("carrier entries = %d, want the write-ahead carrier withdrawn with the entry", got)
	}
	if _, statErr := os.Stat(filepath.Join(p.dir, "task-1.json")); !os.IsNotExist(statErr) {
		t.Fatalf("queue file still on disk: %v", statErr)
	}

	// The regression: the recovery drain must not replay an undecided report.
	// Before the rollback it forwarded it, so the server ended terminal while
	// this instance still held the task as running.
	if remaining, err := p.Drain(context.Background(), f.sched); err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v; want an empty queue", remaining, err)
	}
	if len(f.server.forwards) != 0 {
		t.Fatalf("drain forwarded the undecided report: %+v", f.server.forwards)
	}

	// The carrier is the only cross-process face, so a restarted queue must
	// rebuild nothing for this task either.
	restarted, err := NewPendingReports(p.dir, WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := restarted.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if restarted.HasQueuedEntry("task-1") || restarted.Len() != 0 {
		t.Fatalf("restarted queue replayed the undecided report: held=%v len=%d",
			restarted.HasQueuedEntry("task-1"), restarted.Len())
	}
}

// TestSettleFailureVerdictFailureWithdrawsTheQueuedReport is the same order on
// the compensation face: Foreman's own failure verdict is written ahead of the
// report too, and a write that fails leaves no verdict for the report to
// represent. The convergence round re-derives the failure from the entry's
// facts, so withdrawing the entry loses nothing.
func TestSettleFailureVerdictFailureWithdrawsTheQueuedReport(t *testing.T) {
	storeErr := errors.New("shared state unavailable")
	f := newFixtureWith(t, registry.WithStateStore(&terminalWriteFailingStore{commitErr: storeErr}))
	carriers := newMemCarrierStore()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := p.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	WithPendingReports(p)(f.sched)
	e := runningEntry("task-1")
	if err := f.reg.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}

	_, _, err = f.sched.settleFailure(context.Background(), e, failReport{
		reason:  FailureReasonJobBootTimeout,
		message: "job daemon did not start before the boot deadline",
	})
	if !errors.Is(err, storeErr) {
		t.Fatalf("settleFailure = %v; want the verdict write's error", err)
	}
	if p.HasQueuedEntry("task-1") || p.Len() != 0 {
		t.Fatalf("queue entry survived the failed compensation write: held=%v len=%d",
			p.HasQueuedEntry("task-1"), p.Len())
	}
	if got := carriers.count(); got != 0 {
		t.Fatalf("carrier entries = %d, want the write-ahead carrier withdrawn", got)
	}
}

// TestVerdictFailureKeepsAConcurrentReportsQueueEntry pins the snapshot face
// of the rollback: a verdict write that fails withdraws the report it wrote
// ahead of, not whatever the queue holds under that task id. The compensation
// settlement below does not take the task lock, so it lands its own entry
// while the inbound terminal write is still in flight; the failed write's
// rollback must leave that entry — and its carrier — in place, or the
// compensation's forward would go out with no write-ahead entry (§10).
func TestVerdictFailureKeepsAConcurrentReportsQueueEntry(t *testing.T) {
	storeErr := errors.New("shared state unavailable")
	store := &terminalWriteFailingStore{
		commitErr: storeErr,
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	f := newFixtureWith(t, registry.WithStateStore(store))
	carriers := newMemCarrierStore()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := p.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	WithPendingReports(p)(f.sched)
	e := runningEntry("task-1")
	if err := f.reg.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}

	reported := make(chan error, 1)
	go func() {
		_, _, err := f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{"result":"ok"}`), "")
		reported <- err
	}()
	<-store.entered // the inbound verdict write is parked, task lock held

	settled, settledBody, err := f.sched.settleFailure(context.Background(), e, failReport{
		reason:  FailureReasonJobDeadline,
		message: "task terminated by the platform before the daemon's report",
	})
	if err != nil {
		t.Fatalf("settleFailure: %v", err)
	}
	close(store.release)
	if err := <-reported; !errors.Is(err, storeErr) {
		t.Fatalf("OnReport = %v; want the verdict write's error", err)
	}

	if !p.HasQueuedEntry("task-1") {
		t.Fatal("the failed verdict withdrew a concurrent report's queue entry")
	}
	if got := carriers.count(); got != 1 {
		t.Fatalf("carrier entries = %d, want the concurrent report's carrier kept", got)
	}

	// The concurrent settlement's caller forwards next; §10 needs the entry
	// to still be there at that instant.
	queuedAtForward := false
	f.server.onForward = func() { queuedAtForward = p.HasQueuedEntry("task-1") }
	if err := f.sched.reportFail(context.Background(), settled, settledBody); err != nil {
		t.Fatalf("reportFail: %v", err)
	}
	if !queuedAtForward {
		t.Fatal("the compensation's report went out with no write-ahead entry")
	}
	if len(f.server.forwards) != 1 || f.server.forwards[0].body != string(settledBody) {
		t.Fatalf("forwards = %+v, want the compensation's body", f.server.forwards)
	}
}

// TestVerdictFailureWithdrawsAReportBodyTheQueueReEncodes pins the snapshot
// identity across the queue's own encoding. The disk face stores a body
// through json.Marshal, which compacts it and escapes <, > and &, so the bytes
// that come back are not the bytes the caller passed. The rollback compares
// both sides in that encoded form; comparing raw caller bytes against the
// record would leave the entry — and its carrier — behind on any report
// carrying whitespace or those characters, and the drain would replay a
// verdict this instance never decided.
func TestVerdictFailureWithdrawsAReportBodyTheQueueReEncodes(t *testing.T) {
	storeErr := errors.New("shared state unavailable")
	f := newFixtureWith(t, registry.WithStateStore(&terminalWriteFailingStore{commitErr: storeErr}))
	carriers := newMemCarrierStore()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := p.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	WithPendingReports(p)(f.sched)
	e := runningEntry("task-1")
	if err := f.reg.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}

	body := []byte(`{ "result" : "ok", "note" : "boot < 30s & retry" }`)
	if _, _, err := f.sched.OnReport(context.Background(), EPComplete, e, body, ""); !errors.Is(err, storeErr) {
		t.Fatalf("OnReport = %v; want the verdict write's error", err)
	}
	if p.HasQueuedEntry("task-1") || p.Len() != 0 {
		t.Fatalf("queue entry survived the failed verdict write: held=%v len=%d",
			p.HasQueuedEntry("task-1"), p.Len())
	}
	if got := carriers.count(); got != 0 {
		t.Fatalf("carrier entries = %d, want the write-ahead carrier withdrawn", got)
	}
	if remaining, err := p.Drain(context.Background(), f.sched); err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v; want an empty queue", remaining, err)
	}
	if len(f.server.forwards) != 0 {
		t.Fatalf("drain forwarded the undecided report: %+v", f.server.forwards)
	}
}

// TestRollbackKeepsAConcurrentEnqueuesCarrier pins the carrier half of the
// rollback's snapshot rule. A carrier is create-once, so a create that lands
// inside the delete's window is answered as an already-landed write; an
// unconditional delete then removes the object the new entry depends on, and
// §6.5 cannot repair it because the entry is still queued. The queue's carrier
// lock orders the two: the create waits for the delete, then lands its own
// object.
func TestRollbackKeepsAConcurrentEnqueuesCarrier(t *testing.T) {
	carriers := newMemCarrierStore()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	restoreForServe(t, p)
	withdrawn := []byte(`{"result":"ok"}`)
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", "", EPComplete, withdrawn, ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	deleteEntered := make(chan struct{})
	deleteRelease := make(chan struct{})
	createInWindow := make(chan struct{})
	var once sync.Once
	carriers.onDelete = func(string) {
		close(deleteEntered)
		<-deleteRelease
	}
	carriers.onPut = func(Carrier) { once.Do(func() { close(createInWindow) }) }

	rolledBack := make(chan struct{})
	go func() {
		defer close(rolledBack)
		p.DropIfUnchanged(context.Background(), "task-1", EPComplete, withdrawn)
	}()
	<-deleteEntered // the delete is past the queue's guard, parked before it runs

	newBody := []byte(`{"reason":"job boot timeout"}`)
	enqueued := make(chan error, 1)
	go func() {
		enqueued <- p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", "",
			EPFail, newBody, FailureReasonJobBootTimeout)
	}()
	select {
	case <-createInWindow:
		t.Fatal("the carrier create landed inside the delete window: the delete would remove the entry's cross-process face")
	case <-time.After(250 * time.Millisecond):
	}
	close(deleteRelease)
	<-rolledBack
	if err := <-enqueued; err != nil {
		t.Fatalf("concurrent Enqueue: %v", err)
	}

	if !p.HasQueuedEntry("task-1") {
		t.Fatal("the rollback withdrew the concurrent enqueue's entry")
	}
	if got := carriers.count(); got != 1 {
		t.Fatalf("carrier entries = %d, want the concurrent entry's carrier kept", got)
	}
	if got := carriers.carriers["task-1"].Body; !bytes.Equal(got, newBody) {
		t.Fatalf("carrier body = %s, want the concurrent entry's %s", got, newBody)
	}
}

// TestFailClaimVerdictFailureDoesNotForward pins the caller contract the
// rollback depends on: a verdict write that fails returns before any forward.
// failClaim used to log that error and report anyway, announcing a failure
// this instance never recorded while the write-ahead entry was already gone —
// the local/server divergence §10 forbids.
func TestFailClaimVerdictFailureDoesNotForward(t *testing.T) {
	storeErr := errors.New("shared state unavailable")
	f := newFixtureWith(t, registry.WithStateStore(&terminalWriteFailingStore{commitErr: storeErr}))
	p, err := NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := p.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	WithPendingReports(p)(f.sched)
	e := runningEntry("task-1")
	if err := f.reg.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}

	f.sched.failClaim(context.Background(), e, errors.New("create job: quota exceeded"))

	if len(f.server.forwards) != 0 {
		t.Fatalf("forwards = %+v; want none without a verdict", f.server.forwards)
	}
	if p.HasQueuedEntry("task-1") || p.Len() != 0 {
		t.Fatalf("queue entry survived the failed claim verdict: held=%v len=%d",
			p.HasQueuedEntry("task-1"), p.Len())
	}
	if got, ok := f.reg.Get("task-1"); !ok || got.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want the pre-verdict entry kept", got, ok)
	}
}
