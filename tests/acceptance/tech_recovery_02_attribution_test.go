// TC-tech-recovery-02 (AC-08) drives the platform-forced termination
// attribution end to end with the production components: a task whose
// activeDeadlineSeconds expired is killed by the platform, and the daemon
// self-reports its own failure inside the termination grace window. The
// platform fact is the authority for the failure attribution, so the
// daemon's body is replaced by the compensation body before it is forwarded —
// result=failed with failure_reason=job_deadline_exceeded on the server side,
// the compensation and attribution events on Foreman's side — where the
// ungated form would have let the daemon's self-report decide.
//
// The negative form is the other scenario: with no platform fact and no wait
// signal the self-report is forwarded verbatim at once, so the gate never
// delays (or rewrites) an ordinary failure.
package acceptance

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// TestRecovery02PlatformDeadlineAttributionWinsOverTheDaemonSelfReport is
// step 4a's executable form: the gate path's conjunction — server-side
// result=failed ∧ failure_reason=job_deadline_exceeded, Foreman-side
// task.failed_compensated and task.attribution_decided{decision=platform},
// with no degraded branch.
func TestRecovery02PlatformDeadlineAttributionWinsOverTheDaemonSelfReport(t *testing.T) {
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
	if !ok || entry.State != registry.StateRunning {
		t.Fatalf("entry = %+v, want a running task before the kill", entry)
	}

	// The platform kill is visible at the report instant: the Job's Failed
	// condition carries the deadline reason (the k8s Job controller stamps it
	// when activeDeadlineSeconds terminated the Job), and the server still
	// holds the task as running (C13 non-terminal).
	killed := h.createdJob(t, entry.JobName).DeepCopy()
	killed.Status.Conditions = []batchv1.JobCondition{{
		Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded",
	}}
	h.jobs.programGetJob(killed)
	h.server.setStatus("task-1", "running")

	// The daemon self-reports its own failure inside the grace window.
	code, body := h.call("POST", "/api/daemon/tasks/task-1/fail", token,
		[]byte(`{"error":"agent died","failure_reason":"agent_error","session_id":"sess-42"}`))
	if code != 200 {
		t.Fatalf("S14 = %d (%s), want 200", code, body)
	}

	// Server side: exactly one fail forward, and it is the compensation body —
	// the platform attribution with the learned session value carried back.
	forwards := h.server.forwarded(scheduler.EPFail)
	if len(forwards) != 1 {
		t.Fatalf("fail forwards = %d, want 1", len(forwards))
	}
	var forwarded map[string]string
	if err := json.Unmarshal(forwards[0].Body, &forwarded); err != nil {
		t.Fatalf("forward body: %v", err)
	}
	if forwarded["failure_reason"] != scheduler.FailureReasonJobDeadline {
		t.Fatalf("failure_reason = %q, want %q (the platform fact must suppress the self-report)",
			forwarded["failure_reason"], scheduler.FailureReasonJobDeadline)
	}
	if forwarded["session_id"] != "sess-42" {
		t.Fatalf("session_id = %q, want the self-report's session value carried back", forwarded["session_id"])
	}

	// Foreman side: the verdict and the two event faces of the conjunction.
	settled, ok := h.reg.Get("task-1")
	if !ok || !settled.IsTerminal() || settled.Result != registry.ResultFailed {
		t.Fatalf("entry = %+v, want a terminal failed verdict", settled)
	}
	if settled.FailureReason != scheduler.FailureReasonJobDeadline {
		t.Fatalf("failure_reason = %q, want %q", settled.FailureReason, scheduler.FailureReasonJobDeadline)
	}
	if !h.jobs.jobDeleted(entry.JobName) {
		t.Fatal("the Job must be deleted once the compensation body landed")
	}
	if events := h.events("task.failed_compensated"); len(events) != 1 ||
		events[0]["failure_reason"] != scheduler.FailureReasonJobDeadline {
		t.Fatalf("task.failed_compensated = %+v, want one event with the deadline reason", events)
	}
	decided := h.events("task.attribution_decided")
	if len(decided) != 1 {
		t.Fatalf("task.attribution_decided = %+v, want exactly one decision", decided)
	}
	if decided[0]["decision"] != scheduler.DecisionPlatform || decided[0]["fact_source"] != scheduler.FactSourceJob {
		t.Fatalf("task.attribution_decided = %+v, want decision=platform fact_source=job", decided[0])
	}
	for _, event := range decided {
		if event["decision"] == scheduler.DecisionDegraded {
			t.Fatalf("degraded branch taken on the gate path: %+v", event)
		}
	}
}

// TestRecovery02DaemonSelfReportIsForwardedAtOnceWithoutAPlatformFact is the
// gate's negative boundary: no platform fact, no wait signal, so the
// self-report decides the task — verbatim, without waiting out
// attributionWait.
func TestRecovery02DaemonSelfReportIsForwardedAtOnceWithoutAPlatformFact(t *testing.T) {
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
	// No Job object carries a fact (GetJob answers nil) and no pod carries a
	// deletion trace: neither wait signal holds.
	h.server.setStatus("task-1", "running")

	daemonBody := []byte(`{"error":"agent died","failure_reason":"agent_error"}`)
	start := time.Now()
	code, body := h.call("POST", "/api/daemon/tasks/task-1/fail", token, daemonBody)
	if code != 200 {
		t.Fatalf("S14 = %d (%s), want 200", code, body)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("S14 took %s with no wait signal; the gate must not wait out attributionWait here", elapsed)
	}
	forwards := h.server.forwarded(scheduler.EPFail)
	if len(forwards) != 1 || string(forwards[0].Body) != string(daemonBody) {
		t.Fatalf("fail forwards = %+v, want the daemon's body verbatim", forwards)
	}
	settled, _ := h.reg.Get("task-1")
	if settled.State != registry.StateTerminal || settled.Result != registry.ResultFailed || settled.FailureReason != "" {
		t.Fatalf("entry = %+v, want the daemon's own failed verdict", settled)
	}
	decided := h.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != scheduler.DecisionDaemonSelfReport {
		t.Fatalf("task.attribution_decided = %+v, want decision=daemon_self_report", decided)
	}
}

// tcRecovery02FailJob marks the recorded Job as terminated by the platform:
// the Job controller's Failed condition is the fact both the convergence
// ladder and the attribution gate read.
func tcRecovery02FailJob(t *testing.T, h *harness, jobName string) {
	t.Helper()
	h.jobs.mu.Lock()
	defer h.jobs.mu.Unlock()
	for i := range h.jobs.jobs {
		if h.jobs.jobs[i].Name != jobName {
			continue
		}
		h.jobs.jobs[i].Status.Conditions = []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded",
		}}
		return
	}
	t.Fatalf("Job %s is not on the list face", jobName)
}

// TestRecovery02ConvergenceCompensationLeavesTheAttributionUndecided is step
// 4b's executable form (the fallback path): a task the platform killed at its
// deadline never self-reports, so the convergence loop compensates it —
// Reconciler round → Compensator.Fail → Scheduler.FailJob, the real entry,
// with no stub in between. The failure is attributed by the Job fact, but the
// attribution gate never runs: task.attribution_decided must be absent.
func TestRecovery02ConvergenceCompensationLeavesTheAttributionUndecided(t *testing.T) {
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
	tcRecovery02FailJob(t, h, "fm-task-1")
	// The server still holds the task as running: the convergence path, not
	// the server's own verdict, settles it.
	h.server.setStatus("task-1", "running")

	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		h.reg, queueObjects{jobs: h.jobs}, h.server, h.sched,
		recovery.WithLogger(slog.New(slog.NewJSONHandler(h.logs, nil))),
	)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	// A deadline kill is compensated immediately: no recheck window.
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	settled := waitTerminal(t, h, "task-1")
	if settled.Result != registry.ResultFailed || settled.FailureReason != scheduler.FailureReasonJobDeadline {
		t.Fatalf("entry = %+v, want failed/%s", settled, scheduler.FailureReasonJobDeadline)
	}
	// ① server side: the accepted terminal write is the failure carrying the
	// platform's reason, so the server records result=failed with it.
	writes := h.server.appliedTerminalWrites("task-1")
	if len(writes) != 1 || writes[0].Method != string(scheduler.EPFail) {
		t.Fatalf("server terminal writes = %+v, want exactly one fail", writes)
	}
	var applied map[string]string
	if err := json.Unmarshal(writes[0].Body, &applied); err != nil {
		t.Fatalf("server write body: %v", err)
	}
	if applied["failure_reason"] != scheduler.FailureReasonJobDeadline {
		t.Fatalf("server failure_reason = %q, want %q", applied["failure_reason"], scheduler.FailureReasonJobDeadline)
	}
	// ② Foreman side: the compensation event is the path's event face.
	if events := h.events("task.failed_compensated"); len(events) != 1 ||
		events[0]["failure_reason"] != scheduler.FailureReasonJobDeadline {
		t.Fatalf("task.failed_compensated = %+v, want one deadline event", events)
	}
	// 4b's judgement: the self-report never arrived, so the gate never judged
	// this instance. Any decision event would name a decision nobody made.
	if events := h.events("task.attribution_decided"); len(events) != 0 {
		t.Fatalf("task.attribution_decided = %+v, want none on the fallback path", events)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job was not deleted after the compensation")
	}
}

// TestRecovery05ExpiredWaitReleasesTheSelfReportWithVisibleDegradation is
// TC-tech-recovery-05's executable form (AC-108, ADR-031's wait bound, §10):
// a pod carries a platform-initiated deletion while the platform fact never
// appears. On expiry the gate must release the daemon's self-report verbatim
// and leave the degradation visible: exactly one decision=degraded event
// with no fact source, no compensation body or event. The scheduler clock is
// injected because the bound belongs to it; reading wall time instead would
// spend the full 15s and trip the elapsed check below.
func TestRecovery05ExpiredWaitReleasesTheSelfReportWithVisibleDegradation(t *testing.T) {
	clock := &steppedClock{}
	h := buildHarnessWith(t, false, deliveryConfirmWindow, nil, false, false, harnessOptions{clock: clock.now})
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

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
	if !ok || entry.State != registry.StateRunning {
		t.Fatalf("entry = %+v, want a running task before the report", entry)
	}

	// The wait signal holds: the instance's pod already carries a
	// platform-initiated deletion. No object carries the deadline fact, so the
	// gate waits — and the fact stays absent.
	deletedAt := metav1.Now()
	h.jobs.mu.Lock()
	h.jobs.pods = []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{
			Name:              entry.JobName + "-0",
			Labels:            map[string]string{"job-name": entry.JobName},
			CreationTimestamp: metav1.NewTime(time.Now()),
			DeletionTimestamp: &deletedAt,
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}}
	h.jobs.mu.Unlock()
	// Every evidence read steps the scheduler clock past the gate's bound, so
	// the second read lands beyond it and the wait ends after one poll.
	h.jobs.setOnGetJob(func() { clock.advance(30 * time.Second) })
	h.server.setStatus("task-1", "running")

	daemonBody := []byte(`{"error":"agent died","failure_reason":"agent_error"}`)
	start := time.Now()
	code, body := h.call("POST", "/api/daemon/tasks/task-1/fail", token, daemonBody)
	if code != 200 {
		t.Fatalf("S14 = %d (%s), want 200", code, body)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("S14 took %s: the expired wait must read the scheduler clock, not spend the bound in wall time", elapsed)
	}

	// The released self-report is the daemon's own body: no compensation.
	forwards := h.server.forwarded(scheduler.EPFail)
	if len(forwards) != 1 || string(forwards[0].Body) != string(daemonBody) {
		t.Fatalf("fail forwards = %+v, want the daemon's body verbatim", forwards)
	}
	settled, ok := h.reg.Get("task-1")
	if !ok || !settled.IsTerminal() || settled.Result != registry.ResultFailed {
		t.Fatalf("entry = %+v, want a terminal failed verdict", settled)
	}
	if settled.FailureReason != "" {
		t.Fatalf("failure_reason = %q, want the daemon's own verdict without a platform attribution", settled.FailureReason)
	}
	if events := h.events("task.failed_compensated"); len(events) != 0 {
		t.Fatalf("task.failed_compensated = %+v, want none on the degraded branch", events)
	}
	decided := h.events("task.attribution_decided")
	if len(decided) != 1 {
		t.Fatalf("task.attribution_decided = %+v, want exactly one decision", decided)
	}
	if decided[0]["decision"] != scheduler.DecisionDegraded {
		t.Fatalf("task.attribution_decided = %+v, want decision=degraded", decided[0])
	}
	if source, present := decided[0]["fact_source"]; present && source != "" {
		t.Fatalf("task.attribution_decided fact_source = %v, want none on the degraded branch", source)
	}
}
