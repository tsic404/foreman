package scheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

// A failed deletion must keep the entry live: it is the only retry handle
// for the Job and its credential Secret, and dropping it would leave a
// zombie Job or leak the Secret (failure-handling 顺序与幂等规则 2).
func TestReleaseTaskKeepsEntryUntilObjectsAreGone(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")

	secretsBefore := len(f.jobs.deletedSecrets)
	f.jobs.deleteJobErr = errors.New("apiserver unavailable")
	if err := f.sched.ReleaseTask(context.Background(), "task-1"); err == nil {
		t.Fatal("ReleaseTask: want the deletion error surfaced for the next round")
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the entry must stay live as the retry handle")
	}
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatalf("deletedJobs = %v, want none while the API rejects the delete", f.jobs.deletedJobs)
	}
	// The Secret delete is still attempted: one failure must not leak the
	// other object, and the entry keeps the Job for the retry.
	if got := f.jobs.deletedSecrets[secretsBefore:]; len(got) != 1 || got[0] != "fm-task-1-cred" {
		t.Fatalf("secret deletes since release = %v, want [fm-task-1-cred]", got)
	}

	// The retry succeeds: only then does the mapping go.
	f.jobs.deleteJobErr = nil
	if err := f.sched.ReleaseTask(context.Background(), "task-1"); err != nil {
		t.Fatalf("ReleaseTask retry: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("the entry must be dropped once every object is gone")
	}
	if len(f.jobs.deletedJobs) != 1 || f.jobs.deletedJobs[0] != "fm-task-1" {
		t.Fatalf("deletedJobs = %v, want [fm-task-1]", f.jobs.deletedJobs)
	}
}

// A terminal entry whose cleanup keeps failing must keep its mapping too:
// the same retry handle backs SettleTerminal (顺序与幂等规则 1/2).
func TestSettleTerminalKeepsEntryWhenCleanupFails(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	if _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}

	f.jobs.deleteJobErr = errors.New("apiserver unavailable")
	if err := f.sched.SettleTerminal(context.Background(), "task-1", "completed"); err == nil {
		t.Fatal("SettleTerminal: want the deletion error surfaced")
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("a terminal entry with lingering objects must stay as the retry handle")
	}

	f.jobs.deleteJobErr = nil
	if err := f.sched.SettleTerminal(context.Background(), "task-1", "completed"); err != nil {
		t.Fatalf("SettleTerminal retry: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("the entry must be dropped once the objects are gone")
	}
}
