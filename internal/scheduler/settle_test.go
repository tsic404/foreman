package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// rebuiltEntry puts the entry a restart rebuilds: identity and Job name, no
// payload and no token digest. It is the only shape the discard row is for
// (§4.3: 判别式 = 载荷可得性).
func (f *fixture) rebuiltEntry(t *testing.T, taskID string) registry.TaskEntry {
	t.Helper()
	e := registry.TaskEntry{
		TaskID: taskID, JobName: "fm-" + taskID, DaemonID: "fm-" + taskID,
		JobNamespace: "multica-agents", State: registry.StateJobCreated,
		ClaimedAt: f.now,
	}
	if err := f.reg.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	return e
}

// A failed discard must keep the entry live: it is the only retry handle for
// the Job and its credential Secret, and dropping it would leave a zombie Job
// or leak the Secret (§6.4: the entry is the only retry handle).
func TestDiscardTaskKeepsEntryUntilObjectsAreGone(t *testing.T) {
	f := newFixture(t)
	f.rebuiltEntry(t, "task-1")
	f.server.status = "queued"

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
	if got := f.jobs.deletedSecrets[secretsBefore:]; len(got) != 1 || got[0] != "fm-task-1-cred" {
		t.Fatalf("secret deletes since discard = %v, want [fm-task-1-cred]", got)
	}

	// The retry succeeds: only then does the mapping go.
	f.jobs.deleteJobErr = nil
	if err := f.sched.DiscardTask(context.Background(), "task-1"); err != nil {
		t.Fatalf("DiscardTask retry: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("the entry must be dropped once every object is gone")
	}
	if len(f.jobs.deletedJobs) != 1 || f.jobs.deletedJobs[0] != "fm-task-1" {
		t.Fatalf("deletedJobs = %v, want [fm-task-1]", f.jobs.deletedJobs)
	}
}

// The pre-start replacement row is the one whose predecessor carries a
// digest: the successor Job reuses the name, so the old pod must never be
// able to claim the re-run task (ADR-017 replace_pre_start). A rebuilt entry
// has no digest in memory (TaskEntryFromJob restores none) and degrades
// visibly instead.
func TestPreStartReplacementRevokesThePredecessorDigest(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "dispatched"

	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if len(f.revoker.revoked) != 1 || f.revoker.revoked[0] != "hash-of-task-1" {
		t.Fatalf("revoked = %v, want the predecessor's digest", f.revoker.revoked)
	}

	// A rebuilt entry (no digest in memory) degrades visibly instead of
	// silently leaving the old token valid.
	f.rebuiltEntry(t, "task-2")
	f.server.status = "queued"
	if err := f.sched.DiscardTask(context.Background(), "task-2"); err != nil {
		t.Fatalf("DiscardTask(rebuilt): %v", err)
	}
	if len(f.revoker.revoked) != 1 {
		t.Fatalf("revoked = %v, want no digest for a rebuilt entry", f.revoker.revoked)
	}
}

// ADR-026 决策 5: the discard table's 成立面 is started_at == 0 and it is
// re-read from the live entry under the task lock. A start fact that landed
// after the caller's round snapshot (S8 during a C13 round trip) keeps both
// the Job and the entry.
func TestDiscardTaskKeepsStartedEntry(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	if _, ok := f.reg.MarkStarted("task-1", f.now); !ok {
		t.Fatal("MarkStarted: entry missing")
	}

	if err := f.sched.DiscardTask(context.Background(), "task-1"); err != nil {
		t.Fatalf("DiscardTask: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the adopted entry was discarded")
	}
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatalf("deletedJobs = %v, want none for an adopted entry", f.jobs.deletedJobs)
	}
	if len(f.revoker.revoked) != 0 {
		t.Fatalf("revoked = %v, want no revocation for an adopted entry", f.revoker.revoked)
	}
}

// The replacement race the discard row must lose: a claim the server
// re-dispatched replaced the rebuilt entry (payload present, Secret and Job
// of the successor live under the same names). The round snapshot still says
// "rebuilt", but the live entry is a claim's — deleting it would revoke the
// successor's token and remove its Secret and Job.
func TestDiscardTaskKeepsAReplacementEntry(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "queued"
	f.jobs.deletedJobs = nil
	f.jobs.deletedSecrets = nil

	if err := f.sched.DiscardTask(context.Background(), "task-1"); err != nil {
		t.Fatalf("DiscardTask: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the replacement entry was discarded")
	}
	if len(f.jobs.deletedJobs) != 0 || len(f.jobs.deletedSecrets) != 0 {
		t.Fatalf("deleted jobs/secrets = %v/%v, want the successor's objects kept",
			f.jobs.deletedJobs, f.jobs.deletedSecrets)
	}
	if len(f.revoker.revoked) != 0 {
		t.Fatalf("revoked = %v, want the successor's token kept", f.revoker.revoked)
	}
}

// A terminal verdict is irreversible: the discard row must not delete the
// objects of a settled entry (its drain owns them, ADR-017).
func TestDiscardTaskKeepsTerminalEntry(t *testing.T) {
	f := newFixture(t)
	f.rebuiltEntry(t, "task-1")
	f.server.status = "queued"
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultFailed, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}

	if err := f.sched.DiscardTask(context.Background(), "task-1"); err != nil {
		t.Fatalf("DiscardTask: %v", err)
	}
	if e, ok := f.reg.Get("task-1"); !ok || !e.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want the settled entry kept", e, ok)
	}
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatalf("deletedJobs = %v, want the drain's Job lifecycle untouched", f.jobs.deletedJobs)
	}
}

// The deletion re-reads C13 under the task lock: a stale "pre-delivery"
// round snapshot must not delete a Job whose task the server now holds.
func TestDiscardTaskAdoptsWhenC13IsPostDelivery(t *testing.T) {
	f := newFixture(t)
	f.rebuiltEntry(t, "task-1")
	f.server.status = "running"

	if err := f.sched.DiscardTask(context.Background(), "task-1"); err != nil {
		t.Fatalf("DiscardTask: %v", err)
	}
	e, ok := f.reg.Get("task-1")
	if !ok || e.StartedAt.IsZero() {
		t.Fatalf("entry = %+v, %v; want the adopted entry with a start fact", e, ok)
	}
	if len(f.jobs.deletedJobs) != 0 || len(f.revoker.revoked) != 0 {
		t.Fatalf("deletedJobs = %v, revoked = %v; want the Job and credential kept",
			f.jobs.deletedJobs, f.revoker.revoked)
	}
}

// C13 terminal at the deletion surface settles the entry instead of
// discarding it: the verdict is the server's, and the drain owns the objects.
func TestDiscardTaskSettlesWhenC13IsTerminal(t *testing.T) {
	f := newFixture(t)
	f.rebuiltEntry(t, "task-1")
	f.server.status = "completed"

	if err := f.sched.DiscardTask(context.Background(), "task-1"); err != nil {
		t.Fatalf("DiscardTask: %v", err)
	}
	e, ok := f.reg.Get("task-1")
	if !ok || !e.IsTerminal() || e.Result != registry.ResultCompleted {
		t.Fatalf("entry = %+v, %v; want the server's verdict", e, ok)
	}
	if f.hasJob("fm-task-1") {
		t.Fatal("the Job must be deleted at the verdict")
	}
	if len(f.jobs.deletedSecrets) != 0 {
		t.Fatalf("secret deletes = %v, want the credential kept for the drain", f.jobs.deletedSecrets)
	}
}

// C13 gone at the deletion surface: the task will not be re-run, so it
// settles with an empty result instead of being discarded.
func TestDiscardTaskSettlesWhenC13SaysGone(t *testing.T) {
	f := newFixture(t)
	f.rebuiltEntry(t, "task-1")
	f.server.statusErr = ErrTaskNotFound

	if err := f.sched.DiscardTask(context.Background(), "task-1"); err != nil {
		t.Fatalf("DiscardTask: %v", err)
	}
	e, ok := f.reg.Get("task-1")
	if !ok || !e.IsTerminal() || e.Result != "" {
		t.Fatalf("entry = %+v, %v; want terminal with an empty result", e, ok)
	}
	if f.hasJob("fm-task-1") {
		t.Fatal("the Job must be deleted at the verdict")
	}
}

// A failed C13 re-read is not a verdict: the entry stays as the retry handle.
func TestDiscardTaskKeepsEntryWhenC13Fails(t *testing.T) {
	f := newFixture(t)
	f.rebuiltEntry(t, "task-1")
	f.server.statusErr = errors.New("status lookup unavailable")

	if err := f.sched.DiscardTask(context.Background(), "task-1"); err == nil {
		t.Fatal("DiscardTask: want the status error surfaced for the next round")
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the entry must stay as the retry handle")
	}
	if len(f.jobs.deletedJobs) != 0 || len(f.jobs.deletedSecrets) != 0 {
		t.Fatalf("deleted jobs/secrets = %v/%v, want nothing deleted on an unknown status",
			f.jobs.deletedJobs, f.jobs.deletedSecrets)
	}
}

// An outstanding carrier list is not an empty queue: no settlement may act on
// it (§4.3). FailJob is reachable outside the recovery round (the proxy's
// refused-registration path), so the gate must sit at the settlement entry
// points, not only in the round.
func TestSettlementWaitsForTheQueueRebuild(t *testing.T) {
	f := newFixture(t)
	f.pending.notRebuilt = true
	f.claim(t, "task-1")

	if err := f.sched.FailJob(context.Background(), "fm-task-1", FailureReasonDaemonNotRegistered); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if err := f.sched.SettleTerminal(context.Background(), "task-1", "completed"); err != nil {
		t.Fatalf("SettleTerminal: %v", err)
	}
	if e, ok := f.reg.Get("task-1"); !ok || e.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want it unsettled while the queue is not rebuilt", e, ok)
	}
	if len(f.server.forwards) != 0 {
		t.Fatalf("forwards = %+v, want none while the queue is not rebuilt", f.server.forwards)
	}
	if !f.hasJob("fm-task-1") {
		t.Fatal("the Job must survive while the queue is not rebuilt")
	}

	// The list completes: the same call settles normally.
	f.pending.notRebuilt = false
	if err := f.sched.SettleTerminal(context.Background(), "task-1", "completed"); err != nil {
		t.Fatalf("SettleTerminal after the rebuild: %v", err)
	}
	e, ok := f.reg.Get("task-1")
	if !ok || !e.IsTerminal() || e.Result != registry.ResultCompleted {
		t.Fatalf("entry = %+v, %v; want the completed verdict after the rebuild", e, ok)
	}
	if f.hasJob("fm-task-1") {
		t.Fatal("the Job must be deleted once the settlement is allowed")
	}
}

// The write-ahead decision is not a settlement decision: an uncompleted
// carrier list must not suppress the queue write of a late duplicate report.
func TestOnReportQueuesADuplicateWhileTheQueueIsNotRebuilt(t *testing.T) {
	f := newFixture(t)
	p := f.withPendingReports()
	p.notRebuilt = true
	e := f.claim(t, "task-1")
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}

	if _, _, err := f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{"result":"ok"}`), ""); err != nil {
		t.Fatalf("OnReport: %v", err)
	}
	if len(p.reports) != 1 {
		t.Fatalf("enqueued reports = %d, want the duplicate's write-ahead entry", len(p.reports))
	}
}

// A terminal entry whose report is still queued keeps its Job: the deletion
// waits for the drain that lands the report (§10) — a deleted Job
// must never stand in for a delivered report.
func TestSettleTerminalKeepsJobWhileReportIsQueued(t *testing.T) {
	f := newFixture(t)
	p := f.withPendingReports()
	f.claim(t, "task-1")
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultFailed, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", "", EPFail, []byte(`{"failure_reason":"job_missing"}`), ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := f.sched.SettleTerminal(context.Background(), "task-1", "failed"); err != nil {
		t.Fatalf("SettleTerminal: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the entry must stay while its report is queued")
	}
	if !f.hasJob("fm-task-1") {
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
	if f.hasJob("fm-task-1") {
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
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, "", f.now); err != nil {
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
	if len(f.jobs.deletedJobs) != 1 || f.jobs.deletedJobs[0] != "fm-task-1" {
		t.Fatalf("deletedJobs = %v, want [fm-task-1]", f.jobs.deletedJobs)
	}
}

// A task the server deleted settles terminal with an empty result and enters
// drain: no report is sent, the Job goes, the credential stays (ADR-017
// path ③).
func TestOnTaskVanishedEntersDrain(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.jobs.deletedSecrets = nil
	f.server.statusErr = ErrTaskNotFound

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
	if f.hasJob("fm-task-1") {
		t.Fatal("the Job must be deleted at the verdict")
	}
	if len(f.jobs.deletedSecrets) != 0 {
		t.Fatalf("secret deletes = %v, want the Secret kept until the reclaim", f.jobs.deletedSecrets)
	}
	if len(f.metrics.terminal) != 0 {
		t.Fatalf("terminal metric = %v, want none for the empty-result path", f.metrics.terminal)
	}
}

// The 404 is confirmed under the task lock before anything is deleted: a
// stale vanish observation must not settle a task the server still holds.
func TestOnTaskVanishedKeepsTheEntryWhenTheTaskStillExists(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "running"

	if err := f.sched.OnTaskVanished(context.Background(), "task-1"); err != nil {
		t.Fatalf("OnTaskVanished: %v", err)
	}
	if e, ok := f.reg.Get("task-1"); !ok || e.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want the live entry kept", e, ok)
	}
	if !f.hasJob("fm-task-1") {
		t.Fatal("the Job must survive a stale vanish observation")
	}
}

// A failed confirmation is not a verdict: the entry stays and the next round
// retries (错误处理表: truth unknown changes nothing).
func TestOnTaskVanishedKeepsTheEntryWhenTheStatusReadFails(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.statusErr = errors.New("status lookup unavailable")

	if err := f.sched.OnTaskVanished(context.Background(), "task-1"); err == nil {
		t.Fatal("OnTaskVanished: want the status error surfaced")
	}
	if e, ok := f.reg.Get("task-1"); !ok || e.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want the live entry kept", e, ok)
	}
	if !f.hasJob("fm-task-1") {
		t.Fatal("the Job must survive an unknown status")
	}
}

// The drain upper bound: a task whose Job never terminates is reclaimed once
// FOREMAN_JOB_DRAIN_TIMEOUT has passed, with the still-live pod reported as
// an orphan (ADR-017 兜底上界).
func TestOnJobTerminatedByDrainTimeoutReportsOrphans(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.jobs.deletedSecrets = nil
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultFailed, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	f.jobs.pods = []corev1.Pod{testPod("fm-task-1", corev1.PodRunning)}

	if err := f.sched.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonDrainTimeout, time.Time{}, 1); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("the reclaimed entry must leave the live index")
	}
	if len(f.jobs.deletedSecrets) != 1 || f.jobs.deletedSecrets[0] != "fm-task-1-cred" {
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
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	f.jobs.deleteSecretErr = errors.New("apiserver unavailable")

	if err := f.sched.OnJobTerminated(context.Background(), "fm-task-1",
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
	if err := f.sched.OnJobTerminated(context.Background(), "fm-task-1",
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

	if err := f.sched.OnJobTerminated(context.Background(), "fm-task-1",
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

// The drain entry is resolved by Job name, and a re-dispatched task may have
// replaced it under the same name between that read and the lock. The
// re-read under the lock is what keeps the successor's Secret, entry and
// token: acting on the predecessor's termination evidence would reclaim
// objects the successor now owns. The seam lands the successor exactly in
// that window.
func TestOnJobTerminatedKeepsTheSuccessorReplacingTheEntry(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	f.jobs.deletedSecrets = nil

	// The successor is a live claim: same task and Job name, a payload and a
	// digest of its own.
	successor := registry.TaskEntry{
		TaskID: "task-1", JobName: "fm-task-1", DaemonID: "fm-task-1",
		JobNamespace: "multica-agents", State: registry.StateJobCreated,
		ClaimedAt: f.now, Payload: []byte(`{"id":"task-1"}`),
		TokenHash: "hash-of-the-successor",
	}
	f.sched.beforeJobLock = func() {
		if err := f.reg.Delete("task-1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if err := f.reg.Put(successor); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	if err := f.sched.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonJobTerminated, f.now, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	e, ok := f.reg.Get("task-1")
	if !ok || e.IsDraining() || e.TokenHash != "hash-of-the-successor" {
		t.Fatalf("entry = %+v, %v; want the successor kept", e, ok)
	}
	if len(f.jobs.deletedSecrets) != 0 {
		t.Fatalf("secret deletes = %v, want the successor's Secret kept", f.jobs.deletedSecrets)
	}
	if len(f.revoker.revoked) != 0 {
		t.Fatalf("revoked = %v, want the successor's token kept", f.revoker.revoked)
	}
	if len(f.metrics.reclaim) != 0 {
		t.Fatalf("cred reclaim metric = %v, want none", f.metrics.reclaim)
	}
}
