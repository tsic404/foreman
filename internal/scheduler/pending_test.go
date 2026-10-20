package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSender programs Drain outcomes per task.
type fakeSender struct {
	code    int
	err     error
	sent    []string
	reasons []string
}

func (f *fakeSender) DeliverQueued(_ context.Context, ep Endpoint, taskID string, body []byte, failureReason string) (int, []byte, error) {
	f.sent = append(f.sent, taskID+":"+string(ep)+":"+string(body))
	f.reasons = append(f.reasons, failureReason)
	if f.err != nil {
		return 0, nil, f.err
	}
	return f.code, nil, nil
}

func TestEnqueueWritesDurableFile(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPendingReports(dir)
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPComplete, []byte(`{"result":"ok"}`), ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if p.Len() != 1 {
		t.Fatalf("Len = %d", p.Len())
	}
	raw, err := os.ReadFile(filepath.Join(dir, "task-1.json"))
	if err != nil {
		t.Fatalf("queue file: %v", err)
	}
	var rep pendingReport
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rep.TaskID != "task-1" || rep.Endpoint != "complete" || string(rep.Body) != `{"result":"ok"}` {
		t.Errorf("report = %+v", rep)
	}
	if rep.JobName != "fm-task-1" || rep.DaemonID != "fm-task-1" {
		t.Errorf("identity = job %q daemon %q, want the task's Job identity", rep.JobName, rep.DaemonID)
	}

	// A reopened queue sees the report (survives process restart).
	p2, err := NewPendingReports(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if p2.Len() != 1 {
		t.Errorf("reopened Len = %d, want 1", p2.Len())
	}
}

func TestEnqueueRejectsUnsafeNames(t *testing.T) {
	p, err := NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := p.Enqueue(context.Background(), "../evil", "", "", EPFail, []byte(`{}`), ""); err == nil {
		t.Error("path-unsafe task ids must be rejected")
	}
}

func TestDrainDeliversAndRemoves(t *testing.T) {
	p, err := NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	_ = p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPComplete, []byte(`{"a":1}`), "")
	sender := &fakeSender{code: 200}

	remaining, err := p.Drain(context.Background(), sender)
	if err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v", remaining, err)
	}
	if len(sender.sent) != 1 || sender.sent[0] != `task-1:complete:{"a":1}` {
		t.Fatalf("sent = %v", sender.sent)
	}
	if p.Len() != 0 {
		t.Errorf("Len = %d after successful drain", p.Len())
	}
}

func TestDrainTreats404And409AsDelivered(t *testing.T) {
	for _, code := range []int{404, 409} {
		p, _ := NewPendingReports(t.TempDir())
		_ = p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPCancelAck, []byte(`{}`), "")
		remaining, err := p.Drain(context.Background(), &fakeSender{code: code})
		if err != nil || remaining != 0 {
			t.Errorf("code %d: Drain = %d, %v (a settled/gone task makes the report moot)", code, remaining, err)
		}
	}
}

func TestDrainKeepsFailuresAndCountsAttempts(t *testing.T) {
	p, err := NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	_ = p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{}`), "")
	sender := &fakeSender{code: 503}

	remaining, err := p.Drain(context.Background(), sender)
	if err == nil || remaining != 1 {
		t.Fatalf("Drain = %d, %v", remaining, err)
	}
	// The attempt counter is durable.
	p2, _ := NewPendingReports(p.dir)
	sender.code = 200
	if _, err := p2.Drain(context.Background(), sender); err != nil {
		t.Fatalf("second Drain: %v", err)
	}
	if p2.Len() != 0 {
		t.Errorf("Len = %d after recovery", p2.Len())
	}
}

func TestDrainDropsExpiredReports(t *testing.T) {
	now := time.Now()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	_ = p.Enqueue(context.Background(), "task-old", "fm-task-old", "fm-task-old", EPFail, []byte(`{}`), "")

	// 25h later the report exceeds the TTL and is dropped with an error log
	// (§10).
	later := now.Add(25 * time.Hour)
	p2, _ := NewPendingReports(p.dir, WithPendingReportsClock(func() time.Time { return later }))
	sender := &fakeSender{code: 200}
	remaining, err := p2.Drain(context.Background(), sender)
	if err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v", remaining, err)
	}
	if len(sender.sent) != 0 {
		t.Errorf("an expired report must not be sent, sent = %v", sender.sent)
	}
}

func TestEnqueuePreservesOriginalEnqueueTime(t *testing.T) {
	now := time.Now()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	_ = p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{"v":1}`), "")

	later := now.Add(time.Hour)
	p2, _ := NewPendingReports(p.dir, WithPendingReportsClock(func() time.Time { return later }))
	if err := p2.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{"v":2}`), ""); err != nil {
		t.Fatalf("re-Enqueue: %v", err)
	}
	raw, _ := os.ReadFile(filepath.Join(p.dir, "task-1.json"))
	var rep pendingReport
	_ = json.Unmarshal(raw, &rep)
	if !rep.EnqueuedAt.Equal(now) {
		t.Errorf("EnqueuedAt = %s, want the original %s", rep.EnqueuedAt, now)
	}
	if string(rep.Body) != `{"v":2}` {
		t.Errorf("Body = %s, want the latest", rep.Body)
	}
}

func TestDrainSurfacesTransportErrors(t *testing.T) {
	p, _ := NewPendingReports(t.TempDir())
	_ = p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPComplete, []byte(`{}`), "")
	remaining, err := p.Drain(context.Background(), &fakeSender{err: errors.New("dial refused")})
	if err == nil || remaining != 1 {
		t.Fatalf("Drain = %d, %v", remaining, err)
	}
}

func TestMalformedFileQuarantined(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "junk.json"), []byte(`{not json`), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := NewPendingReports(dir)
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	remaining, err := p.Drain(context.Background(), &fakeSender{code: 200})
	if err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v", remaining, err)
	}
	// Quarantined once: out of the .json rotation, not re-read, not counted.
	if _, err := os.Stat(filepath.Join(dir, "junk.json.bad")); err != nil {
		t.Errorf("quarantined file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "junk.json")); !os.IsNotExist(err) {
		t.Errorf("malformed file still in the queue")
	}
	if p.Len() != 0 {
		t.Errorf("Len = %d, want 0", p.Len())
	}
}

// TestDrainDoesNotBlockEnqueue: the queue lock is never held across network
// I/O — an Enqueue landing mid-Forward returns immediately.
func TestDrainDoesNotBlockEnqueue(t *testing.T) {
	p, err := NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	_ = p.Enqueue(context.Background(), "task-slow", "fm-task-slow", "fm-task-slow", EPComplete, []byte(`{}`), "")

	release := make(chan struct{})
	blocking := newBlockingSender(release)
	drainDone := make(chan struct{})
	go func() {
		_, _ = p.Drain(context.Background(), blocking)
		close(drainDone)
	}()

	// Wait until Drain is inside Forward (network I/O in flight).
	<-blocking.entered
	enqueued := make(chan error, 1)
	go func() {
		enqueued <- p.Enqueue(context.Background(), "task-fast", "fm-task-fast", "fm-task-fast", EPFail, []byte(`{}`), "")
	}()
	select {
	case err := <-enqueued:
		if err != nil {
			t.Fatalf("Enqueue during drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue blocked behind an in-flight Forward (lock held across I/O)")
	}
	close(release)
	<-drainDone
	// task-slow failed (503) and task-fast arrived mid-drain: both remain.
	if p.Len() != 2 {
		t.Errorf("Len = %d, want 2 (both reports still queued)", p.Len())
	}
}

// blockingSender parks inside Forward until released.
type blockingSender struct {
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func newBlockingSender(release chan struct{}) *blockingSender {
	return &blockingSender{release: release, entered: make(chan struct{})}
}

func (s *blockingSender) DeliverQueued(context.Context, Endpoint, string, []byte, string) (int, []byte, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return 503, nil, nil
}

// makeQueueUnwritable leaves the queue directory listable but refuses writes:
// the shape the 错误处理表's staging row covers (a known depth, a failed
// write). A directory that cannot be listed is the other face — unknown
// capacity — which the retention bound refuses instead of admitting.
func makeQueueUnwritable(t *testing.T, dir string) {
	t.Helper()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if err := os.WriteFile(filepath.Join(dir, "probe"), []byte("x"), 0o600); err == nil {
		_ = os.Remove(filepath.Join(dir, "probe"))
		t.Skip("the queue directory is still writable (privileged test user)")
	}
}

// A failed write must not drop the report: it stays staged, counts as
// undelivered for Queued, never grows the gauge, and the next drain that can
// write lands it (错误处理表「待发队列目录不可写 → 记 error + gauge 不增长；
// 退避后重试」).
func TestEnqueueFailureStagesReportUntilWritable(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "pending-reports")
	var gauges []int
	p, err := NewPendingReports(dir, WithPendingReportsGauge(func(n int) { gauges = append(gauges, n) }))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}

	// The directory stays listable (the depth is known) but refuses writes.
	makeQueueUnwritable(t, dir)
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{"failure_reason":"job_failed"}`), ""); err == nil {
		t.Fatal("Enqueue against an unwritable queue: want the write error")
	}
	if p.Len() != 0 {
		t.Fatalf("Len = %d, want 0 (nothing is durable yet)", p.Len())
	}
	if !p.Queued("task-1") {
		t.Fatal("a staged report must count as undelivered")
	}
	for _, n := range gauges {
		if n != 0 {
			t.Fatalf("foreman_pending_reports grew to %d on a failed write", n)
		}
	}

	// The directory is writable again: the retry lands the report and one
	// delivery takes it off the queue.
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("restore dir: %v", err)
	}
	sender := &fakeSender{code: 200}
	remaining, err := p.Drain(context.Background(), sender)
	if err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v", remaining, err)
	}
	if len(sender.sent) != 1 || sender.sent[0] != `task-1:fail:{"failure_reason":"job_failed"}` {
		t.Fatalf("sent = %v, want the staged report delivered exactly once", sender.sent)
	}
	if p.Queued("task-1") || p.Len() != 0 {
		t.Fatalf("report still queued after delivery: Queued=%v Len=%d", p.Queued("task-1"), p.Len())
	}
}

// An id that cannot name a queue file is never a durable entry: Queued
// answers false without touching the filesystem, and the drain delivers the
// staged report straight from memory so the terminal callback is not lost.
func TestDrainDeliversUnstorableTaskIDFromStaging(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "pending-reports")
	p, err := NewPendingReports(dir)
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if p.Queued("../escape") {
		t.Fatal("Queued must not claim an unsafe task id is a queue entry")
	}
	if err := p.Enqueue(context.Background(), "../escape", "", "", EPFail, []byte(`{"failure_reason":"invalid_task_id"}`), ""); err == nil {
		t.Fatal("Enqueue with an unsafe task id: want an error")
	}
	if p.Len() != 0 {
		t.Fatalf("Len = %d, want 0", p.Len())
	}

	sender := &fakeSender{code: 200}
	remaining, err := p.Drain(context.Background(), sender)
	if err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v", remaining, err)
	}
	if len(sender.sent) != 1 || sender.sent[0] != `../escape:fail:{"failure_reason":"invalid_task_id"}` {
		t.Fatalf("sent = %v, want the staged report delivered from memory", sender.sent)
	}
	if _, err := os.Stat(filepath.Join(root, "escape.json")); !os.IsNotExist(err) {
		t.Fatalf("queue wrote outside its directory: %v", err)
	}
	// Nothing left to retry.
	if remaining, err := p.Drain(context.Background(), sender); err != nil || remaining != 0 || len(sender.sent) != 1 {
		t.Fatalf("second Drain = %d, %v, sent = %v", remaining, err, sender.sent)
	}
}

// TestReportEventsCarryTaskIdentity (02-contracts.md §9.1, AC-10): every pending
// queue event row carries task_id, job_name and daemon_id. The identity is on
// the durable file, so a restarted queue still logs it; a report whose
// identity is unknown keeps the keys with empty values.
func TestReportEventsCarryTaskIdentity(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "pending-reports")
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	now := time.Now()
	clock := func() time.Time { return now }

	p, err := NewPendingReports(dir, WithPendingReportsLogger(logger), WithPendingReportsClock(clock))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	// A failed write stages the report; the retry's queue line is the second
	// forward_queued emission (the first is the direct write below).
	makeQueueUnwritable(t, dir)
	if err := p.Enqueue(context.Background(), "task-staged", "fm-task-staged", "fm-daemon-staged", EPFail, []byte(`{}`), ""); err == nil {
		t.Fatal("Enqueue against an unwritable queue: want the write error")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("restore dir: %v", err)
	}
	if _, err := p.Drain(context.Background(), &fakeSender{code: 200}); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// The durable file carries the identity across a reopened queue.
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-daemon-1", EPComplete, []byte(`{}`), ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	p2, err := NewPendingReports(dir, WithPendingReportsLogger(logger), WithPendingReportsClock(clock))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := p2.Drain(context.Background(), &fakeSender{code: 200}); err != nil {
		t.Fatalf("Drain reopened queue: %v", err)
	}

	// An unknown identity keeps both keys with empty values, on the queue and
	// on the TTL drop.
	if err := p2.Enqueue(context.Background(), "task-2", "", "", EPFail, []byte(`{}`), ""); err != nil {
		t.Fatalf("Enqueue without identity: %v", err)
	}
	now = now.Add(PendingReportTTL + time.Hour)
	if _, err := p2.Drain(context.Background(), &fakeSender{code: 200}); err != nil {
		t.Fatalf("Drain expired queue: %v", err)
	}

	retried := pendingLogRecord(t, buf.String(), "task.forward_queued", "task-staged")
	assertPendingLogIdentity(t, retried, "fm-task-staged", "fm-daemon-staged")
	if retried["retried_write"] != true {
		t.Errorf("retried_write = %v, want true", retried["retried_write"])
	}
	delivered := pendingLogRecord(t, buf.String(), "task.forward_delivered", "task-1")
	assertPendingLogIdentity(t, delivered, "fm-task-1", "fm-daemon-1")
	dropped := pendingLogRecord(t, buf.String(), "task.forward_dropped", "task-2")
	assertPendingLogIdentity(t, dropped, "", "")
}

// pendingLogRecord returns the first queue log record for msg and taskID.
func pendingLogRecord(t *testing.T, raw, msg, taskID string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record["msg"] == msg && record["task_id"] == taskID {
			return record
		}
	}
	t.Fatalf("no %s record for %s in logs:\n%s", msg, taskID, raw)
	return nil
}

// assertPendingLogIdentity checks the three identity keys, with presence for
// daemon_id so an unknown identity is an empty value rather than a missing key.
func assertPendingLogIdentity(t *testing.T, record map[string]any, jobName, daemonID string) {
	t.Helper()
	if got := record["job_name"]; got != jobName {
		t.Errorf("job_name = %v, want %q", got, jobName)
	}
	got, ok := record["daemon_id"]
	if !ok {
		t.Errorf("daemon_id key missing: %v", record)
	} else if got != daemonID {
		t.Errorf("daemon_id = %v, want %q", got, daemonID)
	}
}

// TestPendingReportsDirFollowsStateRoot pins the queue directory's resolution
// (§10: FOREMAN_STATE_ROOT/pending-reports).
func TestPendingReportsDirFollowsStateRoot(t *testing.T) {
	got := PendingReportsDir(func(key string) string {
		if key == EnvStateRoot {
			return "/srv/foreman/"
		}
		return ""
	})
	if got != "/srv/foreman/pending-reports" {
		t.Fatalf("PendingReportsDir = %q", got)
	}
	if got := PendingReportsDir(func(string) string { return "" }); got != DefaultStateRoot+"/pending-reports" {
		t.Fatalf("PendingReportsDir default = %q", got)
	}
}

// TestDrainKeepsAReportReplacedMidFlight pins §10's no-loss face under the
// snapshot race: the delivered body is dropped only while the queue still holds
// it; a body a concurrent Enqueue wrote mid-flight stays queued.
func TestDrainKeepsAReportReplacedMidFlight(t *testing.T) {
	p, err := NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{"old":1}`), ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	replaced := false
	sender := &senderFunc{send: func(_ string, _ []byte) (int, []byte, error) {
		if !replaced {
			replaced = true
			if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{"new":2}`), ""); err != nil {
				t.Errorf("mid-flight Enqueue: %v", err)
			}
		}
		return 200, nil, nil
	}}

	remaining, err := p.Drain(context.Background(), sender)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("remaining = %d, want the replaced report still queued", remaining)
	}
	cur, ok := p.readLocked("task-1")
	if !ok || string(cur.Body) != `{"new":2}` {
		t.Fatalf("queued report = %+v (ok=%v), want the mid-flight body", cur, ok)
	}
}

// senderFunc adapts a closure to ReportSender.
type senderFunc struct {
	send func(taskID string, body []byte) (int, []byte, error)
}

func (f *senderFunc) DeliverQueued(_ context.Context, _ Endpoint, taskID string, body []byte, _ string) (int, []byte, error) {
	return f.send(taskID, body)
}

// TestMalformedFileQuarantineFailureIsLoggedOnce pins the retry bound: a
// quarantine rename that keeps failing must not repeat both error lines on
// every Drain/Len call.
func TestMalformedFileQuarantineFailureIsLoggedOnce(t *testing.T) {
	dir := t.TempDir()
	// The .bad target is a directory, so the rename can never succeed.
	if err := os.Mkdir(filepath.Join(dir, "t.json.bad"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "t.json"), []byte(`not json`), 0o600); err != nil {
		t.Fatalf("write malformed file: %v", err)
	}
	logs := &strings.Builder{}
	p, err := NewPendingReports(dir, WithPendingReportsLogger(
		slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	_ = p.Len()
	_ = p.Len()
	_ = p.Queued("t")
	if got := strings.Count(logs.String(), "quarantining malformed pending report"); got != 1 {
		t.Fatalf("quarantine log lines = %d, want one across repeated reads", got)
	}
	if got := strings.Count(logs.String(), "quarantine failed"); got != 1 {
		t.Fatalf("quarantine-failure log lines = %d, want one", got)
	}
}

// TestEnqueueRefusesNewEntriesAtTheRetentionBound pins §4.6 / ADR-046 decision
// 6: once the queue holds its bound, a report for a task it does not already
// hold is refused — dropped with an error row, the alarm face — while a task's
// own replacement is still admitted and no queued entry is removed.
func TestEnqueueRefusesNewEntriesAtTheRetentionBound(t *testing.T) {
	store := newMemCarrierStore()
	logs := &strings.Builder{}
	p, err := NewPendingReports(t.TempDir(),
		WithPendingReportsMaxEntries(2),
		WithPendingReportsCarrier(store),
		WithPendingReportsLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	// The startup rebuild completes before reports arrive (§4.3); an
	// unrestored list reads every task as possibly queued.
	if err := p.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	for _, taskID := range []string{"task-1", "task-2"} {
		if err := p.Enqueue(context.Background(), taskID, "fm-"+taskID, "fm-"+taskID, EPFail, []byte(`{}`), ""); err != nil {
			t.Fatalf("Enqueue %s: %v", taskID, err)
		}
	}
	if got := p.Len(); got != 2 {
		t.Fatalf("Len = %d, want the bound", got)
	}
	if got := store.count(); got != 2 {
		t.Fatalf("carriers = %d, want 2", got)
	}

	// A new entry is refused: nothing written, nothing staged, nothing
	// carried, and the drop is the alarm face's error row.
	err = p.Enqueue(context.Background(), "task-3", "fm-task-3", "fm-task-3", EPComplete, []byte(`{"result":"ok"}`), "")
	if !errors.Is(err, ErrPendingQueueFull) {
		t.Fatalf("Enqueue at the bound = %v, want ErrPendingQueueFull", err)
	}
	if got := p.Len(); got != 2 {
		t.Fatalf("Len after the refusal = %d, want the bound untouched", got)
	}
	if p.Queued("task-3") {
		t.Fatal("the refused report must not be held: a queued entry holds its Job back")
	}
	if got := store.count(); got != 2 {
		t.Fatalf("carriers after the refusal = %d, want the queued two only", got)
	}
	if _, ok := p.readLocked("task-3"); ok {
		t.Fatal("the refused report must not name a queue file")
	}
	record := pendingLogRecord(t, logs.String(), "task.forward_dropped", "task-3")
	assertPendingLogIdentity(t, record, "fm-task-3", "fm-task-3")
	if got := record["reason"]; got != "pending report queue bound reached" {
		t.Errorf("reason = %v, want the bound's drop reason", got)
	}

	// A task the queue already holds is a replacement, not a new entry: it is
	// admitted at the bound and never grows the queue.
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{"v":2}`), ""); err != nil {
		t.Fatalf("re-enqueue at the bound: %v", err)
	}
	if got := p.Len(); got != 2 {
		t.Fatalf("Len after the replacement = %d, want the bound", got)
	}
	if rep, ok := p.readLocked("task-1"); !ok || string(rep.Body) != `{"v":2}` {
		t.Fatalf("replaced report = %+v (ok=%v), want the newer body", rep, ok)
	}

	// Settlement frees capacity: the same task is admitted once the queue is
	// below the bound again.
	p.Drop(context.Background(), "task-2")
	if got := p.Len(); got != 1 {
		t.Fatalf("Len after settlement = %d, want 1", got)
	}
	if err := p.Enqueue(context.Background(), "task-3", "fm-task-3", "fm-task-3", EPComplete, []byte(`{"result":"ok"}`), ""); err != nil {
		t.Fatalf("Enqueue below the bound: %v", err)
	}
}

// TestRetentionBoundCountsCarrierBackedEntries: a carrier-restored backlog
// occupies the bound (nothing is deleted to make room, and none of it is
// trimmed), so the queue refuses new entries until the backlog drains.
func TestRetentionBoundCountsCarrierBackedEntries(t *testing.T) {
	store := newMemCarrierStore()
	for _, taskID := range []string{"task-1", "task-2"} {
		if err := store.PutCarrier(context.Background(), Carrier{
			TaskID: taskID, Endpoint: string(EPFail), Body: []byte(`{}`), EnqueuedAt: time.Now(),
		}); err != nil {
			t.Fatalf("PutCarrier: %v", err)
		}
	}
	p, err := NewPendingReports(t.TempDir(),
		WithPendingReportsMaxEntries(2),
		WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := p.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := p.Len(); got != 2 {
		t.Fatalf("rebuilt depth = %d, want the carrier count", got)
	}
	// The backlog is already enqueued: the bound never trims it.
	if err := p.Enqueue(context.Background(), "task-3", "", "", EPFail, []byte(`{}`), ""); !errors.Is(err, ErrPendingQueueFull) {
		t.Fatalf("Enqueue over a restored backlog = %v, want ErrPendingQueueFull", err)
	}
	if got := store.count(); got != 2 {
		t.Fatalf("carriers after the refusal = %d, want the restored two kept", got)
	}
	// Both restored reports are still delivered — dropping them for the bound
	// would pass a mere depth-zero check but lose two terminal callbacks.
	sender := &fakeSender{code: 200}
	remaining, err := p.Drain(context.Background(), sender)
	if err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v", remaining, err)
	}
	delivered := make(map[string]bool, len(sender.sent))
	for _, sent := range sender.sent {
		taskID, _, _ := strings.Cut(sent, ":")
		delivered[taskID] = true
	}
	if len(sender.sent) != 2 || !delivered["task-1"] || !delivered["task-2"] {
		t.Fatalf("replayed = %v, want both restored reports delivered", sender.sent)
	}
	if got := store.count(); got != 0 {
		t.Fatalf("carriers after settlement = %d, want 0", got)
	}
}

// TestRetentionBoundCountsStagedReports: a report a failed write left staged
// is held by the queue too, so it occupies the bound — the memory face cannot
// grow past it while the disk is unwritable.
func TestRetentionBoundCountsStagedReports(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pending-reports")
	p, err := NewPendingReports(dir, WithPendingReportsMaxEntries(1))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	makeQueueUnwritable(t, dir)
	if err := p.Enqueue(context.Background(), "task-1", "", "", EPFail, []byte(`{}`), ""); err == nil {
		t.Fatal("Enqueue against an unwritable queue: want the write error")
	}
	if !p.Queued("task-1") {
		t.Fatal("the staged report must be held")
	}
	if err := p.Enqueue(context.Background(), "task-2", "", "", EPFail, []byte(`{}`), ""); !errors.Is(err, ErrPendingQueueFull) {
		t.Fatalf("Enqueue over a staged report = %v, want ErrPendingQueueFull", err)
	}

	// The queue below the bound admits again once the staged report left.
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("restore dir: %v", err)
	}
	if remaining, err := p.Drain(context.Background(), &fakeSender{code: 200}); err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v", remaining, err)
	}
	if err := p.Enqueue(context.Background(), "task-2", "", "", EPFail, []byte(`{}`), ""); err != nil {
		t.Fatalf("Enqueue after the queue drained: %v", err)
	}
}

// TestUnlistableQueueRefusesNewEntries: a queue directory that can be written
// but not listed hides the entries it already holds. The admission gate reads
// that unknown capacity as reached — the same discipline Queued follows
// (§4.3) — and refuses the new entry through the same drop row, instead of
// reading the depth as zero and letting the queue grow.
func TestUnlistableQueueRefusesNewEntries(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pending-reports")
	logs := &strings.Builder{}
	p, err := NewPendingReports(dir,
		WithPendingReportsMaxEntries(8),
		WithPendingReportsLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{}`), ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Writable but not listable: creating and renaming files still works, the
	// directory read does not.
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if _, err := os.ReadDir(dir); err == nil {
		t.Skip("the queue directory is still listable (privileged test user)")
	}

	// A task already queued stays a replacement even with the listing gone:
	// its own file is still probeable by name, so the re-enqueue is admitted
	// and draws no depth-unknown drop.
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{"v":2}`), ""); err != nil {
		t.Fatalf("re-enqueue of a held task with an unlistable queue: %v", err)
	}
	if got := strings.Count(logs.String(), `"msg":"task.forward_dropped"`); got != 0 {
		t.Fatalf("drop rows after a held replacement = %d, want none", got)
	}
	if rep, ok := p.readLocked("task-1"); !ok || string(rep.Body) != `{"v":2}` {
		t.Fatalf("replaced report = %+v (ok=%v), want the newer body", rep, ok)
	}

	err = p.Enqueue(context.Background(), "task-2", "fm-task-2", "fm-task-2", EPComplete, []byte(`{"result":"ok"}`), "")
	if !errors.Is(err, ErrPendingQueueFull) {
		t.Fatalf("Enqueue with an unlistable queue = %v, want ErrPendingQueueFull", err)
	}
	record := pendingLogRecord(t, logs.String(), "task.forward_dropped", "task-2")
	assertPendingLogIdentity(t, record, "fm-task-2", "fm-task-2")
	if got := record["reason"]; got != "pending report queue depth unknown: queue directory cannot be listed" {
		t.Errorf("reason = %v, want the unknown-depth drop", got)
	}
	if _, ok := record["err"]; !ok {
		t.Error("the unknown-depth drop must carry the listing error")
	}
	if _, err := os.Stat(filepath.Join(dir, "task-2.json")); !os.IsNotExist(err) {
		t.Fatalf("the refused report left a queue file: %v", err)
	}
	if !p.Queued("task-1") {
		t.Fatal("the held entry must survive the refused admission")
	}

	// The listing is back: the same report is admitted.
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("restore mode: %v", err)
	}
	if err := p.Enqueue(context.Background(), "task-2", "fm-task-2", "fm-task-2", EPComplete, []byte(`{"result":"ok"}`), ""); err != nil {
		t.Fatalf("Enqueue after the listing recovered: %v", err)
	}
	if got := p.Len(); got != 2 {
		t.Fatalf("depth = %d, want both entries", got)
	}
}

// TestUnrebuiltCarrierBacklogRefusesNewEntries: before the carrier list lands,
// the previous process's backlog is invisible, so capacity is unknown and new
// entries are refused. A task the queue already holds (visible on disk) stays a
// replacement; the rebuild restores admission without dropping anything.
func TestUnrebuiltCarrierBacklogRefusesNewEntries(t *testing.T) {
	dir := t.TempDir()
	held, err := json.Marshal(pendingReport{
		TaskID: "held", Endpoint: string(EPFail), Body: json.RawMessage(`{}`), EnqueuedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal held report: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "held.json"), held, 0o600); err != nil {
		t.Fatalf("write held report: %v", err)
	}
	store := newMemCarrierStore()
	logs := &strings.Builder{}
	p, err := NewPendingReports(dir,
		WithPendingReportsMaxEntries(8),
		WithPendingReportsCarrier(store),
		WithPendingReportsLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}

	// A held task is a replacement: capacity is not judged for it.
	if err := p.Enqueue(context.Background(), "held", "fm-held", "fm-held", EPFail, []byte(`{"v":2}`), ""); err != nil {
		t.Fatalf("re-enqueue while the backlog is unknown: %v", err)
	}
	// A new task is refused: the backlog could already fill the bound.
	err = p.Enqueue(context.Background(), "task-new", "fm-task-new", "fm-task-new", EPComplete, []byte(`{}`), "")
	if !errors.Is(err, ErrPendingQueueFull) {
		t.Fatalf("Enqueue while the backlog is unknown = %v, want ErrPendingQueueFull", err)
	}
	record := pendingLogRecord(t, logs.String(), "task.forward_dropped", "task-new")
	if got := record["reason"]; got != "pending report queue depth unknown: carrier backlog not rebuilt" {
		t.Errorf("reason = %v, want the unrebuilt-backlog drop", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "task-new.json")); !os.IsNotExist(err) {
		t.Fatalf("the refused report left a queue file: %v", err)
	}

	// The rebuild lands: admission resumes and nothing was trimmed.
	if err := p.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := p.Len(); got != 1 {
		t.Fatalf("depth after the rebuild = %d, want the held entry only", got)
	}
	if err := p.Enqueue(context.Background(), "task-new", "fm-task-new", "fm-task-new", EPComplete, []byte(`{}`), ""); err != nil {
		t.Fatalf("Enqueue after the rebuild: %v", err)
	}
}
