// TC-tech-recovery-01 (AC-08): a Job whose container ended without any daemon
// terminal report is compensated. The reconciler's recheck window first gives
// the daemon its chance; on the next round the task settles failed with the
// job_failed attribution, the synthesized report reaches the server, and the
// Job is deleted.
package acceptance

import (
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"log/slog"

	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// tcRecovery01FailJob marks the recorded Job as ended: the Job controller's
// Failed condition is the fact the reconciliation ladder reads.
func tcRecovery01FailJob(t *testing.T, h *harness, jobName string) {
	t.Helper()
	h.jobs.mu.Lock()
	defer h.jobs.mu.Unlock()
	for i := range h.jobs.jobs {
		if h.jobs.jobs[i].Name != jobName {
			continue
		}
		h.jobs.jobs[i].Status.Conditions = []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded",
		}}
		return
	}
	t.Fatalf("Job %s is not on the list face", jobName)
}

func TestRecovery01JobEndedWithoutAReportIsCompensated(t *testing.T) {
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
	tcRecovery01FailJob(t, h, "fm-task-1")
	h.server.setStatus("task-1", "running")

	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		h.reg, queueObjects{jobs: h.jobs}, h.server, h.sched,
		recovery.WithLogger(slog.New(slog.NewJSONHandler(h.logs, nil))),
	)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}

	// First round: the daemon's recheck window starts, nothing settles.
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile round 1: %v", err)
	}
	if e, ok := h.reg.Get("task-1"); !ok || e.IsTerminal() {
		t.Fatalf("entry = %+v (%v) after the first round, want a live entry inside the recheck window", e, ok)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job was deleted before the recheck window closed")
	}

	// Second round after the window: the compensation settles the task.
	time.Sleep(11 * time.Second)
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile round 2: %v", err)
	}
	entry := waitTerminal(t, h, "task-1")
	if entry.Result != registry.ResultFailed || entry.FailureReason != scheduler.FailureReasonJobFailed {
		t.Errorf("entry = %+v, want failed/job_failed", entry)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job was not deleted after the compensation")
	}
	fails := h.server.forwarded(scheduler.EPFail)
	if len(fails) != 1 || !strings.Contains(string(fails[0].Body), scheduler.FailureReasonJobFailed) {
		t.Errorf("compensation reports = %+v, want one job_failed body", fails)
	}
	events := h.events("task.failed_compensated")
	if len(events) != 1 || events[0]["failure_reason"] != scheduler.FailureReasonJobFailed {
		t.Errorf("task.failed_compensated = %+v, want one job_failed event", events)
	}
}

// TC-tech-recovery-01 (AC-08, 同名重跑面): a re-run reuses the Job name
// (fm-<task_id>). A predecessor's evicted pod that GC lag left on the list
// face must not name the healthy re-run evicted: past the recheck window the
// task is still running, the Job is kept and no fail report was sent.
func TestRecovery01SameNameRerunIgnoresThePredecessorPods(t *testing.T) {
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
