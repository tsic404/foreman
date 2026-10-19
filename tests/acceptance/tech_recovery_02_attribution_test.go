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
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

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
