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

	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
	"github.com/tsic404/foreman/internal/state"
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
	if decided[0]["decision"] != "platform" || decided[0]["fact_source"] != "job" {
		t.Fatalf("task.attribution_decided = %+v, want decision=platform fact_source=job", decided[0])
	}
	for _, event := range decided {
		if event["decision"] == "degraded" {
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
	if len(decided) != 1 || decided[0]["decision"] != "daemon_self_report" {
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

// newSharedDeliveryHarness is newDeliveryHarness on the deployment's shared
// store: the task lease the gate's terminal write takes is the real one, so a
// scenario judging the gate path's budget exercises the lease's deadline
// instead of the local mutex a shared-less fixture answers with.
func newSharedDeliveryHarness(t *testing.T) *harness {
	t.Helper()
	store, _ := multi08Store(t)
	coord, ok := store.(state.CoordStore)
	if !ok {
		t.Fatal("the shared store does not expose the coordination face")
	}
	h := buildHarnessWith(t, false, deliveryConfirmWindow, nil, false, false, harnessOptions{
		store: store, coord: coord, instanceID: "instance-1",
	})
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	return h
}

// TestRecovery02GatePathStaysInsideTheAttributionBudget is AC-08's latency
// clause in executable form: the platform fact is visible and the server's C13
// answer never arrives, so the gate's remaining budget — not a fresh
// control-plane timeout stacked behind the wait — is what ends the S14 round.
// The round stays inside the start point plus the attribution wait bound and
// takes the undecidable branch, forwarding the daemon's body verbatim. The
// shared store is the deployment's lease shape: the terminal write has to land
// on the reserve the gate left, not fail closed on an expired deadline.
func TestRecovery02GatePathStaysInsideTheAttributionBudget(t *testing.T) {
	h := newSharedDeliveryHarness(t)
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
	killed := h.createdJob(t, entry.JobName).DeepCopy()
	killed.Status.Conditions = []batchv1.JobCondition{{
		Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded",
	}}
	h.jobs.programGetJob(killed)
	h.server.setStatus("task-1", "running")
	h.server.setStatusHangs(true)

	daemonBody := []byte(`{"error":"agent died","failure_reason":"agent_error"}`)
	start := time.Now()
	// The client bound only keeps a missing gate bound from hanging the
	// suite; the judged bound is the gate path's own.
	code, body := h.callWithin(40*time.Second, "POST", "/api/daemon/tasks/task-1/fail", token, daemonBody)
	elapsed := time.Since(start)
	if code != 200 {
		t.Fatalf("S14 = %d (%s), want 200", code, body)
	}
	// §10 门路径不超过起算点加归因等待上界: the bound is the implementation
	// constant (15s) plus a small margin for the round's own bookkeeping — a
	// path that merely fits the daemon's 30s timeout is not enough.
	if elapsed >= 17*time.Second {
		t.Fatalf("S14 took %s, want it inside the start point plus the attribution bound", elapsed)
	}
	decided := h.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != "read_error" || decided[0]["error_source"] != "c13" {
		t.Fatalf("task.attribution_decided = %+v, want decision=read_error error_source=c13", decided)
	}
	forwards := h.server.forwarded(scheduler.EPFail)
	if len(forwards) != 1 || string(forwards[0].Body) != string(daemonBody) {
		t.Fatalf("fail forwards = %+v, want the daemon's body verbatim", forwards)
	}
	settled, ok := h.reg.Get("task-1")
	if !ok || !settled.IsTerminal() || settled.Result != registry.ResultFailed || settled.FailureReason != "" {
		t.Fatalf("entry = %+v, want the daemon's own failed verdict", settled)
	}
}
