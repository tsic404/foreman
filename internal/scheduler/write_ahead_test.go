package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
