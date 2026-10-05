package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/scheduler"
)

// fakeSender programs Drain outcomes per task.
type fakeSender struct {
	code int
	err  error
	sent []string
}

func (f *fakeSender) Forward(_ context.Context, ep scheduler.Endpoint, taskID string, body []byte) (int, []byte, error) {
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
	if err := p.Enqueue("task-1", scheduler.EPComplete, []byte(`{"result":"ok"}`)); err != nil {
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
	if err := p.Enqueue("../evil", scheduler.EPFail, []byte(`{}`)); err == nil {
		t.Error("path-unsafe task ids must be rejected")
	}
}

func TestDrainDeliversAndRemoves(t *testing.T) {
	p, err := NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	_ = p.Enqueue("task-1", scheduler.EPComplete, []byte(`{"a":1}`))
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
		_ = p.Enqueue("task-1", scheduler.EPCancelAck, []byte(`{}`))
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
	_ = p.Enqueue("task-1", scheduler.EPFail, []byte(`{}`))
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
	_ = p.Enqueue("task-old", scheduler.EPFail, []byte(`{}`))

	// 25h later the report exceeds the TTL and is dropped with an error log
	// (failure-handling 场景 #10).
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
	_ = p.Enqueue("task-1", scheduler.EPFail, []byte(`{"v":1}`))

	later := now.Add(time.Hour)
	p2, _ := NewPendingReports(p.dir, WithPendingReportsClock(func() time.Time { return later }))
	if err := p2.Enqueue("task-1", scheduler.EPFail, []byte(`{"v":2}`)); err != nil {
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
	_ = p.Enqueue("task-1", scheduler.EPComplete, []byte(`{}`))
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
	_ = p.Enqueue("task-slow", scheduler.EPComplete, []byte(`{}`))

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
	go func() { enqueued <- p.Enqueue("task-fast", scheduler.EPFail, []byte(`{}`)) }()
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

func (s *blockingSender) Forward(context.Context, scheduler.Endpoint, string, []byte) (int, []byte, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return 503, nil, nil
}
