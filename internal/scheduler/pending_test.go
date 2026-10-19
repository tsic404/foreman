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
	code int
	err  error
	sent []string
}

func (f *fakeSender) DeliverQueued(_ context.Context, ep Endpoint, taskID string, body []byte) (int, []byte, error) {
	f.sent = append(f.sent, taskID+":"+string(ep)+":"+string(body))
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
	if err := p.Enqueue("task-1", "fm-task-1", "fm-task-1", EPComplete, []byte(`{"result":"ok"}`)); err != nil {
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
	if err := p.Enqueue("../evil", "", "", EPFail, []byte(`{}`)); err == nil {
		t.Error("path-unsafe task ids must be rejected")
	}
}

func TestDrainDeliversAndRemoves(t *testing.T) {
	p, err := NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	_ = p.Enqueue("task-1", "fm-task-1", "fm-task-1", EPComplete, []byte(`{"a":1}`))
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
		_ = p.Enqueue("task-1", "fm-task-1", "fm-task-1", EPCancelAck, []byte(`{}`))
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
	_ = p.Enqueue("task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{}`))
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
	_ = p.Enqueue("task-old", "fm-task-old", "fm-task-old", EPFail, []byte(`{}`))

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
	_ = p.Enqueue("task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{"v":1}`))

	later := now.Add(time.Hour)
	p2, _ := NewPendingReports(p.dir, WithPendingReportsClock(func() time.Time { return later }))
	if err := p2.Enqueue("task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{"v":2}`)); err != nil {
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
	_ = p.Enqueue("task-1", "fm-task-1", "fm-task-1", EPComplete, []byte(`{}`))
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
	_ = p.Enqueue("task-slow", "fm-task-slow", "fm-task-slow", EPComplete, []byte(`{}`))

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
		enqueued <- p.Enqueue("task-fast", "fm-task-fast", "fm-task-fast", EPFail, []byte(`{}`))
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

func (s *blockingSender) DeliverQueued(context.Context, Endpoint, string, []byte) (int, []byte, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return 503, nil, nil
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

	// Break the queue directory: the write cannot land.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	if err := os.WriteFile(dir, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("break dir: %v", err)
	}
	if err := p.Enqueue("task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{"failure_reason":"job_failed"}`)); err == nil {
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
	if err := os.Remove(dir); err != nil {
		t.Fatalf("remove blocker: %v", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
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
	if err := p.Enqueue("../escape", "", "", EPFail, []byte(`{"failure_reason":"invalid_task_id"}`)); err == nil {
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
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	if err := os.WriteFile(dir, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("break dir: %v", err)
	}
	if err := p.Enqueue("task-staged", "fm-task-staged", "fm-daemon-staged", EPFail, []byte(`{}`)); err == nil {
		t.Fatal("Enqueue against an unwritable queue: want the write error")
	}
	if err := os.Remove(dir); err != nil {
		t.Fatalf("remove blocker: %v", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("restore dir: %v", err)
	}
	if _, err := p.Drain(context.Background(), &fakeSender{code: 200}); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// The durable file carries the identity across a reopened queue.
	if err := p.Enqueue("task-1", "fm-task-1", "fm-daemon-1", EPComplete, []byte(`{}`)); err != nil {
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
	if err := p2.Enqueue("task-2", "", "", EPFail, []byte(`{}`)); err != nil {
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
