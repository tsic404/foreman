// TC-tech-recovery-04 (AC-08): a same-name Job on the claim face is this
// task's predecessor, never its instance. The claim deletes it and retries
// inside the Job-name wait; when the deletion keeps failing the claim is
// deferred — no adoption, no delivery, no failure report, no terminal write,
// no lease renewal, and a default-level claim-deferred event. Once the name is
// free the server's re-dispatch creates the new Job and the projection follows
// it. The predecessor's pods are a separate face: a corpse left on the list
// face must not name the healthy re-run (§4.1, §4.3, TC-tech-recovery-04 ①②③).
package acceptance

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// TestRecovery04ClaimDefersWhileThePredecessorNameIsHeld is the claim-time
// face: the same-name predecessor is owned by this task, its deletion fails
// for the whole name wait, and the claim is deferred instead of settling the
// task. The re-dispatch after the release creates the new Job.
func TestRecovery04ClaimDefersWhileThePredecessorNameIsHeld(t *testing.T) {
	h := newDeliveryHarness(t)
	// The name wait is 45s of wall clock; the stepped clock lets the scenario
	// exhaust it instead of sleeping.
	clock := &steppedClock{}
	h.schedClock = clock.now
	h.rebuildScheduler(t, func(c *scheduler.Config) { c.JobBootTimeout = 300 * time.Second })

	predecessor := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "fm-task-1",
		Labels: map[string]string{
			registry.LabelManagedBy: registry.ManagedByForeman,
			registry.LabelTaskID:    "task-1",
		},
		CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
	}}
	h.jobs.programGetJob(predecessor)
	h.jobs.setNameHeld("fm-task-1", true)
	h.jobs.setDeleteJobError(errors.New("apiserver unavailable"))
	// Each name probe steps the clock past the claim's wait bound. The probe
	// runs inside jobClient.GetJob's lock, so the hook only reads the clock.
	h.jobs.setOnGetJob(func() { clock.advance(46 * time.Second) })

	err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1"))
	if err == nil || !strings.Contains(err.Error(), "job name still held") {
		t.Fatalf("OnClaim error = %v, want the deferred-claim cause", err)
	}

	// ② The claim is deferred: no entry (so no lease renewal), no failure
	// report, no terminal write, and the default-level warn event names it.
	if _, ok := h.reg.Get("task-1"); ok {
		t.Fatal("the deferred claim kept the entry: its lease would be renewed, never re-dispatched")
	}
	if fails := h.server.forwarded(scheduler.EPFail); len(fails) != 0 {
		t.Fatalf("deferred claim reported a failure: %+v", fails)
	}
	if n := h.upstream.prepareLeaseCalls(); n != 0 {
		t.Fatalf("prepare-lease calls = %d, want 0 (a deferred claim is not renewed)", n)
	}
	deferred := h.events("task.claim_deferred")
	if len(deferred) != 1 {
		t.Fatalf("task.claim_deferred events = %d, want 1", len(deferred))
	}
	if got := deferred[0]["task_id"]; got != "task-1" {
		t.Errorf("claim_deferred task_id = %v, want task-1", got)
	}
	if got := deferred[0]["job_name"]; got != "fm-task-1" {
		t.Errorf("claim_deferred job_name = %v, want fm-task-1", got)
	}
	if got := fmt.Sprint(deferred[0]["err"]); !strings.Contains(got, "job name still held") {
		t.Errorf("claim_deferred err = %q, want the held-name cause", got)
	}

	// ① The predecessor is not adopted: no new Job exists under the name, its
	// credential Secret was rolled back (so its daemon cannot even authenticate
	// to be delivered to), and no start fact or terminal record was written.
	if h.jobs.jobCreated("fm-task-1") {
		t.Fatal("the claim created a Job while the predecessor held the name")
	}
	if h.jobs.hasSecret("fm-task-1-cred") {
		t.Fatal("the deferred claim left its credential Secret behind")
	}
	if h.jobs.deletedCount("fm-task-1") != 0 {
		t.Fatalf("recorded deletions = %d, want 0 (the deletion failed, the name stayed held)", h.jobs.deletedCount("fm-task-1"))
	}

	// ③ The re-dispatch after the predecessor is released creates the new Job
	// and the entry projects it, with the new Job's creation instant — not the
	// predecessor's.
	h.jobs.setDeleteJobError(nil)
	h.jobs.setNameHeld("fm-task-1", false)
	h.jobs.setOnGetJob(nil)
	h.jobs.mu.Lock()
	delete(h.jobs.existingJobs, "fm-task-1")
	h.jobs.mu.Unlock()

	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("re-dispatch OnClaim: %v", err)
	}
	entry, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("the re-dispatched claim produced no entry")
	}
	if entry.State != registry.StateJobCreated {
		t.Fatalf("entry state = %v, want job_created", entry.State)
	}
	if entry.JobName != "fm-task-1" {
		t.Fatalf("job_name = %q, want fm-task-1", entry.JobName)
	}
	h.jobs.mu.Lock()
	var createdTime time.Time
	for _, job := range h.jobs.jobs {
		if job.Name == "fm-task-1" {
			createdTime = job.CreationTimestamp.Time
		}
	}
	h.jobs.mu.Unlock()
	if createdTime.IsZero() {
		t.Fatal("the new Job is not on the list face")
	}
	if !entry.JobCreatedAt.Equal(createdTime) {
		t.Fatalf("job_created_at = %v, want the new Job's %v (not the predecessor's %v)",
			entry.JobCreatedAt, createdTime, predecessor.CreationTimestamp.Time)
	}
	if fails := h.server.forwarded(scheduler.EPFail); len(fails) != 0 {
		t.Fatalf("re-dispatch reported a failure: %+v", fails)
	}
}

// TestRecovery04SameNamePredecessorPodsDoNotSettleTheTask is the pod face of
// ①: a re-run reuses the Job name (fm-<task_id>); a predecessor's evicted pod
// that GC lag left on the list face must not name the healthy re-run evicted.
// Past the recheck window the task is still running, the Job is kept and no
// fail report was sent.
func TestRecovery04SameNamePredecessorPodsDoNotSettleTheTask(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	claimTask(t, h, "task-1", token)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != 200 {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	entry, ok := h.reg.Get("task-1")
	if !ok || entry.JobCreatedAt.IsZero() {
		t.Fatalf("entry = %+v (%v), want a job_created_at for the instance filter", entry, ok)
	}
	h.server.setStatus("task-1", "running")

	// The corpse pod comes first on the list face, so an unfiltered read
	// would take its node and its eviction as this instance's facts.
	corpse := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "fm-task-1-predecessor",
			Labels:            map[string]string{"job-name": "fm-task-1"},
			CreationTimestamp: metav1.NewTime(entry.JobCreatedAt.Add(-time.Minute)),
		},
		Spec:   corev1.PodSpec{NodeName: "company-02"},
		Status: corev1.PodStatus{Phase: corev1.PodUnknown},
	}
	current := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "fm-task-1-current",
			Labels:            map[string]string{"job-name": "fm-task-1"},
			CreationTimestamp: metav1.NewTime(entry.JobCreatedAt),
		},
		Spec:   corev1.PodSpec{NodeName: "company-01"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	h.jobs.mu.Lock()
	h.jobs.pods = append(h.jobs.pods, corpse, current)
	h.jobs.mu.Unlock()

	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		h.reg, queueObjects{jobs: h.jobs}, h.server, h.sched,
		recovery.WithLogger(slog.New(slog.NewJSONHandler(h.logs, nil))),
	)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile round 1: %v", err)
	}
	got, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("entry task-1 vanished")
	}
	if got.NodeName != "company-01" {
		t.Fatalf("node_name = %q, want company-01 (the predecessor's node must not be recorded)", got.NodeName)
	}

	// Past the 10s recheck window: the corpse pod must not have settled the
	// task as evicted, and the healthy Job must still be there.
	time.Sleep(11 * time.Second)
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile round 2: %v", err)
	}
	got, ok = h.reg.Get("task-1")
	if !ok || got.IsTerminal() {
		t.Fatalf("entry = %+v (%v), want a live running entry", got, ok)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the healthy re-run's Job was deleted")
	}
	if fails := h.server.forwarded(scheduler.EPFail); len(fails) != 0 {
		t.Fatalf("fail reports = %+v, want none", fails)
	}
	if events := h.events("task.failed_compensated"); len(events) != 0 {
		t.Fatalf("task.failed_compensated = %+v, want none", events)
	}
}
