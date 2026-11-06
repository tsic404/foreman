// TC-tech-lifecycle-03 (AC-11, P1): cancellation propagates. The daemon's
// status poll forwards the server's cancelled answer, its cancel-ack is
// forwarded, the mapping settles as cancelled and the Job is deleted inside
// the 10s budget — the Job deletion is what terminates the agent process.
package acceptance

import (
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestLifecycle03CancelPropagation(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	claimTask(t, h, "task-1", token)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}

	// The server cancels; the daemon's status poll sees it verbatim.
	h.upstream.setTaskStatus("task-1", "cancelled")
	cancelledAt := time.Now()
	code, statusBody := h.call("GET", "/api/daemon/tasks/task-1/status", token, nil)
	if code != http.StatusOK {
		t.Fatalf("S9 = %d (%s), want the upstream's 200", code, statusBody)
	}
	if !strings.Contains(string(statusBody), `"cancelled"`) {
		t.Fatalf("S9 body = %s, want the server's cancelled answer verbatim", statusBody)
	}

	// The daemon's cancel-ack is forwarded and settles the mapping.
	ack := []byte(`{"status":"cancelled","reason":"user_cancel"}`)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/cancel-ack", token, ack); code != http.StatusOK {
		t.Fatalf("S15 = %d (%s), want 200", code, body)
	}
	forwards := h.server.forwarded(scheduler.EPCancelAck)
	if len(forwards) != 1 || string(forwards[0].Body) != string(ack) {
		t.Errorf("cancel-ack forwards = %+v, want one verbatim body", forwards)
	}
	if got := h.server.acceptedFor(scheduler.EPCancelAck); got != 1 {
		t.Errorf("accepted cancel-ack writes = %d, want 1", got)
	}

	entry := waitTerminal(t, h, "task-1")
	if entry.Result != registry.ResultCancelled {
		t.Errorf("result = %s, want cancelled", entry.Result)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job was not deleted after the cancel-ack")
	}
	if elapsed := time.Since(cancelledAt); elapsed > 10*time.Second {
		t.Errorf("cancellation took %s, want within 10s", elapsed)
	}
}

// lifecycle03Pod is one pod of the task's Job instance: the creationTimestamp
// keeps the instance filter, and the phase is the termination fact the
// confirmation hangs on.
func lifecycle03Pod(jobName string, since time.Time, phase corev1.PodPhase) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              jobName + "-pod",
			Labels:            map[string]string{"job-name": jobName},
			CreationTimestamp: metav1.NewTime(since),
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// lifecycle03CancelledTask claims, delivers and starts one task, then settles
// its cancellation the way an expired confirmation window does: the verdict is
// server-side, the Job goes, and the confirmation the daemon never sent becomes
// Foreman's debt.
func lifecycle03CancelledTask(t *testing.T, h *harness) registry.TaskEntry {
	t.Helper()
	if err := h.sched.RestorePendingReports(t.Context()); err != nil {
		t.Fatalf("RestorePendingReports: %v", err)
	}
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	claimTask(t, h, "task-1", token)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}

	h.upstream.setTaskStatus("task-1", "cancelled")
	if err := h.sched.SettleCancelled(t.Context(), "task-1"); err != nil {
		t.Fatalf("SettleCancelled: %v", err)
	}
	entry, ok := h.reg.Get("task-1")
	if !ok || entry.Result != registry.ResultCancelled || !entry.CancelAckOwed {
		t.Fatalf("entry = %+v, %v; want a cancelled verdict carrying the confirmation debt", entry, ok)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job was not deleted when the confirmation window expired")
	}
	if got := len(h.server.forwarded(scheduler.EPCancelAck)); got != 0 {
		t.Fatalf("cancel-ack forwards before any termination fact = %d, want 0", got)
	}
	// The fixture's registry carries no drain bound, so the verdict's deadline
	// sits at the verdict instant: pin a live window so each round judges the
	// termination evidence instead of the already-expired timer.
	entry.DrainDeadline = time.Now().Add(time.Minute)
	if err := h.reg.Put(entry); err != nil {
		t.Fatalf("Put: %v", err)
	}
	return entry
}

func lifecycle03Reconciler(t *testing.T, h *harness) *recovery.Reconciler {
	t.Helper()
	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		h.reg, queueObjects{jobs: h.jobs}, h.server, h.sched)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	return rec
}

// The daemon never confirmed the cancellation and its Job is already gone: the
// confirmation is Foreman's, and it follows the instance's termination fact —
// never the timer. It is sent once, with the endpoint's minimal body.
func TestLifecycle03ConfirmationFollowsTheTerminationFact(t *testing.T) {
	h := newDeliveryHarness(t)
	entry := lifecycle03CancelledTask(t, h)
	rec := lifecycle03Reconciler(t, h)

	// The pods are still running: the round has no termination fact and sends
	// nothing, however long the cancel window has been over.
	h.jobs.pods = []corev1.Pod{lifecycle03Pod("fm-task-1", entry.JobCreatedAt, corev1.PodRunning)}
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile with a live pod: %v", err)
	}
	if got := len(h.server.forwarded(scheduler.EPCancelAck)); got != 0 {
		t.Fatalf("cancel-ack forwards with a live pod = %d, want 0", got)
	}

	// The pods terminate: the confirmation goes out, byte for byte.
	h.jobs.pods = []corev1.Pod{lifecycle03Pod("fm-task-1", entry.JobCreatedAt, corev1.PodFailed)}
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile after the pods terminated: %v", err)
	}
	forwards := h.server.forwarded(scheduler.EPCancelAck)
	if len(forwards) != 1 {
		t.Fatalf("cancel-ack forwards = %d, want exactly 1", len(forwards))
	}
	if got := string(forwards[0].Body); got != `{"process_cleanup_confirmed":true}` {
		t.Fatalf("cancel-ack body = %s, want the endpoint's minimal confirmation", got)
	}
	if got := h.server.acceptedFor(scheduler.EPCancelAck); got != 1 {
		t.Fatalf("accepted cancel-ack writes = %d, want 1", got)
	}
	if events := h.events("task.cancel_ack_synthesized"); len(events) != 1 {
		t.Fatalf("task.cancel_ack_synthesized events = %d, want 1", len(events))
	}
	if _, ok := h.reg.Get("task-1"); ok {
		t.Fatal("the entry survived its reclaim")
	}

	// Neither a repeated round nor a repeated reclaim sends it again.
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("repeated Reconcile: %v", err)
	}
	if got := len(h.server.forwarded(scheduler.EPCancelAck)); got != 1 {
		t.Fatalf("cancel-ack forwards after another round = %d, want 1", got)
	}
}

// The drain upper bound closes the credential window with the pod still alive:
// that is a timer, not a termination fact, and no confirmation may be sent.
func TestLifecycle03ConfirmationWithheldWhenTheDrainBoundCloses(t *testing.T) {
	h := newDeliveryHarness(t)
	entry := lifecycle03CancelledTask(t, h)
	rec := lifecycle03Reconciler(t, h)

	h.jobs.pods = []corev1.Pod{lifecycle03Pod("fm-task-1", entry.JobCreatedAt, corev1.PodRunning)}
	expired := entry
	expired.DrainDeadline = time.Now().Add(-time.Second)
	if err := h.reg.Put(expired); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile past the drain bound: %v", err)
	}
	if got := len(h.server.forwarded(scheduler.EPCancelAck)); got != 0 {
		t.Fatalf("cancel-ack forwards = %d, want none: the bound is not a termination fact", got)
	}
	if _, ok := h.reg.Get("task-1"); ok {
		t.Fatal("the drain bound did not close the entry")
	}
}

// A daemon that does confirm keeps its own body: the compensation never
// replaces or repeats it.
func TestLifecycle03DaemonConfirmationIsNotReplaced(t *testing.T) {
	h := newDeliveryHarness(t)
	entry := lifecycle03CancelledTask(t, h)
	rec := lifecycle03Reconciler(t, h)

	// The daemon's own confirmation lands late, after the settle, carrying the
	// body the server already accepted.
	late := []byte(`{"status":"cancelled"}`)
	if code, body, err := h.sched.OnReport(t.Context(), scheduler.EPCancelAck, entry, late, ""); err != nil || code != http.StatusOK {
		t.Fatalf("late cancel-ack = %d (%s), %v; want 200", code, body, err)
	}
	forwards := h.server.forwarded(scheduler.EPCancelAck)
	if len(forwards) != 1 || string(forwards[0].Body) != string(late) {
		t.Fatalf("cancel-ack forwards = %+v, want the daemon's body verbatim", forwards)
	}

	h.jobs.pods = []corev1.Pod{lifecycle03Pod("fm-task-1", entry.JobCreatedAt, corev1.PodSucceeded)}
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile after the daemon confirmed: %v", err)
	}
	forwards = h.server.forwarded(scheduler.EPCancelAck)
	if len(forwards) != 1 || string(forwards[0].Body) != string(late) {
		t.Fatalf("cancel-ack forwards after the reclaim = %+v, want still the daemon's one", forwards)
	}
}

// A cancellation whose claim never produced an object is the "no pod ever
// existed" shape: the confirmation is still owed, and the objectless reclaim
// settles it instead of dropping the debt with the entry.
func TestLifecycle03ObjectlessCancellationStillConfirms(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.RestorePendingReports(t.Context()); err != nil {
		t.Fatalf("RestorePendingReports: %v", err)
	}
	// The Job name never composed, so no object was ever created for this
	// task: the entry is the only record, and it is the shape the objectless
	// reclaim owns.
	if err := h.reg.Put(registry.TaskEntry{
		TaskID: "task-1", JobName: "fm-task-1", DaemonID: "fm-task-1",
		WorkspaceID: "ws-1", State: registry.StateJobCreated,
		ClaimedAt: time.Now(), FailureReason: scheduler.FailureReasonInvalidTaskID,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := h.sched.SettleCancelled(t.Context(), "task-1"); err != nil {
		t.Fatalf("SettleCancelled: %v", err)
	}
	entry, ok := h.reg.Get("task-1")
	if !ok || entry.Result != registry.ResultCancelled || !entry.CancelAckOwed {
		t.Fatalf("entry = %+v, %v; want a cancelled verdict carrying the confirmation debt", entry, ok)
	}
	entry.DrainDeadline = time.Now().Add(time.Minute)
	if err := h.reg.Put(entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rec := lifecycle03Reconciler(t, h)
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	forwards := h.server.forwarded(scheduler.EPCancelAck)
	if len(forwards) != 1 || string(forwards[0].Body) != `{"process_cleanup_confirmed":true}` {
		t.Fatalf("cancel-ack forwards = %+v, want the confirmation verbatim", forwards)
	}
	if _, ok := h.reg.Get("task-1"); ok {
		t.Fatal("the objectless entry survived its reclaim")
	}
}
