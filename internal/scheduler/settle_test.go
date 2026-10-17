package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// A failed discard must keep the entry live: it is the only retry handle for
// the Job and its credential Secret, and dropping it would leave a zombie Job
// or leak the Secret (failure-handling 顺序与幂等规则 2).
func TestDiscardTaskKeepsEntryUntilObjectsAreGone(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	wantJob := e.JobName
	wantCred := registry.CredName(wantJob)

	secretsBefore := len(f.jobs.deletedSecrets)
	f.jobs.deleteJobErr = errors.New("apiserver unavailable")
	if err := f.sched.DiscardTask(context.Background(), "task-1"); err == nil {
		t.Fatal("DiscardTask: want the deletion error surfaced for the next round")
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the entry must stay live as the retry handle")
	}
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatalf("deletedJobs = %v, want none while the API rejects the delete", f.jobs.deletedJobs)
	}
	// The Secret delete is still attempted: one failure must not leak the
	// other object, and the entry keeps the Job for the retry.
	if got := f.jobs.deletedSecrets[secretsBefore:]; len(got) != 1 || got[0] != wantCred {
		t.Fatalf("secret deletes since discard = %v, want [%s]", got, wantCred)
	}

	// The retry succeeds: only then does the mapping go.
	f.jobs.deleteJobErr = nil
	if err := f.sched.DiscardTask(context.Background(), "task-1"); err != nil {
		t.Fatalf("DiscardTask retry: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("the entry must be dropped once every object is gone")
	}
	if len(f.jobs.deletedJobs) != 1 || f.jobs.deletedJobs[0] != wantJob {
		t.Fatalf("deletedJobs = %v, want [%s]", f.jobs.deletedJobs, wantJob)
	}
}

// The discard path is the "task will be re-run" one: the successor Job
// reuses the name, so the predecessor's digest must be revoked (ADR-017).
// A rebuilt entry has no digest and says so.
func TestDiscardTaskRevokesTheCredential(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.jobs.deletedSecrets = nil

	if err := f.sched.DiscardTask(context.Background(), "task-1"); err != nil {
		t.Fatalf("DiscardTask: %v", err)
	}
	if len(f.revoker.revoked) != 1 || f.revoker.revoked[0] != "hash-of-task-1" {
		t.Fatalf("revoked = %v, want the entry's digest", f.revoker.revoked)
	}

	// A rebuilt entry (no digest in memory) degrades visibly instead of
	// silently leaving the old token valid.
	rebuilt := registry.TaskEntry{
		TaskID: "task-2", JobName: "fm-task-2", DaemonID: "fm-task-2",
		State: registry.StateJobCreated,
	}
	if err := f.reg.Put(rebuilt); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := f.sched.DiscardTask(context.Background(), "task-2"); err != nil {
		t.Fatalf("DiscardTask(rebuilt): %v", err)
	}
	if len(f.revoker.revoked) != 1 {
		t.Fatalf("revoked = %v, want no digest for a rebuilt entry", f.revoker.revoked)
	}
}

// A terminal entry whose report is still queued keeps its Job: the deletion
// waits for the drain that lands the report (contract §4) — a deleted Job
// must never stand in for a delivered report.
func TestSettleTerminalKeepsJobWhileReportIsQueued(t *testing.T) {
	f := newFixture(t)
	p := f.withPendingReports()
	f.claim(t, "task-1")
	if _, err := f.reg.MarkTerminal("task-1", registry.ResultFailed, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if err := p.Enqueue("task-1", EPFail, []byte(`{"failure_reason":"job_missing"}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := f.sched.SettleTerminal(context.Background(), "task-1", "failed"); err != nil {
		t.Fatalf("SettleTerminal: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the entry must stay while its report is queued")
	}
	if !f.hasJob(derivedJobName("task-1")) {
		t.Fatal("the Job must survive while its report is queued")
	}
	if len(f.jobs.deletedSecrets) != 1 { // only the claim-time stale sweep
		t.Fatalf("secret deletes = %v, want only the claim-time sweep", f.jobs.deletedSecrets)
	}

	// The drain delivered it: the verdict-time deletion runs, and the
	// credential stays for the Job's own drain window (ADR-017).
	p.deliver("task-1")
	if err := f.sched.SettleTerminal(context.Background(), "task-1", "failed"); err != nil {
		t.Fatalf("SettleTerminal after delivery: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the entry stays until the credential reclaim")
	}
	if f.hasJob(derivedJobName("task-1")) {
		t.Fatal("the Job must be deleted once the report landed")
	}
	if len(f.jobs.deletedSecrets) != 1 {
		t.Fatalf("secret deletes = %v, want the Secret kept until the reclaim", f.jobs.deletedSecrets)
	}
}

// A Job deletion that keeps failing must keep the mapping too: the same
// retry handle backs SettleTerminal (顺序与幂等规则 1/2).
func TestSettleTerminalKeepsEntryWhenJobDeletionFails(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	if _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}

	f.jobs.deleteJobErr = errors.New("apiserver unavailable")
	if err := f.sched.SettleTerminal(context.Background(), "task-1", "completed"); err == nil {
		t.Fatal("SettleTerminal: want the deletion error surfaced")
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("a terminal entry with a lingering Job must stay as the retry handle")
	}

	f.jobs.deleteJobErr = nil
	if err := f.sched.SettleTerminal(context.Background(), "task-1", "completed"); err != nil {
		t.Fatalf("SettleTerminal retry: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the entry stays until the credential reclaim")
	}
	if len(f.jobs.deletedJobs) != 1 || f.jobs.deletedJobs[0] != derivedJobName("task-1") {
		t.Fatalf("deletedJobs = %v, want [%s]", f.jobs.deletedJobs, derivedJobName("task-1"))
	}
}

// A task the server deleted settles terminal with an empty result and enters
// drain: no report is sent, the Job goes, the credential stays (ADR-017
// path ③).
func TestOnTaskVanishedEntersDrain(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.jobs.deletedSecrets = nil

	if err := f.sched.OnTaskVanished(context.Background(), "task-1"); err != nil {
		t.Fatalf("OnTaskVanished: %v", err)
	}
	e, ok := f.reg.Get("task-1")
	if !ok || !e.IsTerminal() || e.Result != "" || !e.IsDraining() {
		t.Fatalf("entry = %+v, %v; want terminal with an empty result in drain", e, ok)
	}
	if len(f.server.forwards) != 0 {
		t.Fatalf("forwards = %+v, want none for a task the server no longer knows", f.server.forwards)
	}
	if f.hasJob(derivedJobName("task-1")) {
		t.Fatal("the Job must be deleted at the verdict")
	}
	if len(f.jobs.deletedSecrets) != 0 {
		t.Fatalf("secret deletes = %v, want the Secret kept until the reclaim", f.jobs.deletedSecrets)
	}
	if len(f.metrics.terminal) != 0 {
		t.Fatalf("terminal metric = %v, want none for the empty-result path", f.metrics.terminal)
	}
}

// The drain upper bound: a task whose Job never terminates is reclaimed once
// FOREMAN_JOB_DRAIN_TIMEOUT has passed, with the still-live pod reported as
// an orphan (ADR-017 兜底上界).
func TestOnJobTerminatedByDrainTimeoutReportsOrphans(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	f.jobs.deletedSecrets = nil
	if _, err := f.reg.MarkTerminal("task-1", registry.ResultFailed, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	f.jobs.pods = []corev1.Pod{testPod(e.JobName, corev1.PodRunning)}

	if err := f.sched.OnJobTerminated(context.Background(), e.JobName,
		JobTerminationReasonDrainTimeout, time.Time{}, 1); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("the reclaimed entry must leave the live index")
	}
	if len(f.jobs.deletedSecrets) != 1 || f.jobs.deletedSecrets[0] != registry.CredName(e.JobName) {
		t.Fatalf("secret deletes = %v, want the credential Secret", f.jobs.deletedSecrets)
	}
	if len(f.revoker.revoked) != 1 || f.revoker.revoked[0] != "hash-of-task-1" {
		t.Fatalf("revoked = %v, want the entry's digest", f.revoker.revoked)
	}
	if len(f.metrics.reclaim) != 1 || f.metrics.reclaim[0] != JobTerminationReasonDrainTimeout {
		t.Fatalf("cred reclaim metric = %v, want [drain_timeout]", f.metrics.reclaim)
	}
}

// A Secret deletion that fails keeps the entry in drain for the next round:
// the entry is the only retry handle (顺序与幂等规则 7).
func TestOnJobTerminatedKeepsEntryWhenSecretDeletionFails(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.jobs.deletedSecrets = nil
	if _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	f.jobs.deleteSecretErr = errors.New("apiserver unavailable")

	if err := f.sched.OnJobTerminated(context.Background(), derivedJobName("task-1"),
		JobTerminationReasonJobTerminated, f.now, 0); err == nil {
		t.Fatal("OnJobTerminated: want the Secret deletion error surfaced")
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the entry must stay as the reclaim retry handle")
	}
	if len(f.metrics.reclaim) != 0 {
		t.Fatalf("cred reclaim metric = %v, want none before the reclaim completed", f.metrics.reclaim)
	}

	f.jobs.deleteSecretErr = nil
	if err := f.sched.OnJobTerminated(context.Background(), derivedJobName("task-1"),
		JobTerminationReasonJobTerminated, f.now, 0); err != nil {
		t.Fatalf("OnJobTerminated retry: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("the entry must be dropped once the reclaim completed")
	}
	if len(f.metrics.reclaim) != 1 || f.metrics.reclaim[0] != JobTerminationReasonJobTerminated {
		t.Fatalf("cred reclaim metric = %v, want [job_terminated]", f.metrics.reclaim)
	}
}

// The reclaim is a no-op for an entry that is not draining: it is reachable
// from the Job watcher, which may fire for any Job.
func TestOnJobTerminatedIgnoresLiveEntries(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.jobs.deletedSecrets = nil

	if err := f.sched.OnJobTerminated(context.Background(), derivedJobName("task-1"),
		JobTerminationReasonJobTerminated, f.now, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("a live entry must survive a stray termination report")
	}
	if len(f.jobs.deletedSecrets) != 0 || len(f.revoker.revoked) != 0 {
		t.Fatalf("secrets=%v revoked=%v, want no action for a live entry",
			f.jobs.deletedSecrets, f.revoker.revoked)
	}
}
