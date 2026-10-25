// TC-tech-recovery-05 (AC-108) drives the attribution gate's wait bound to
// expiry: a running task's pod already carries a platform-initiated deletion
// while the platform termination fact never appears. The gate must release the
// daemon's self-report verbatim — the terminal verdict comes from it, with no
// compensation body or event — and make the degradation visible as exactly one
// decision=degraded attribution event without a fact source. Two forms are
// judged: the expiry past the bound on an injected clock, and the production
// bound's exact span.
package acceptance

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

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
	// The gate's wait bound runs on the scheduler clock: arming the step makes
	// the second reading land past the bound, so the wait expires without
	// spending it in wall time.
	clock.stepEach(30 * time.Second)
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
	if decided[0]["decision"] != "degraded" {
		t.Fatalf("task.attribution_decided = %+v, want decision=degraded", decided[0])
	}
	if source, present := decided[0]["fact_source"]; present && source != "" {
		t.Fatalf("task.attribution_decided fact_source = %v, want none on the degraded branch", source)
	}
}

// TestRecovery05GateExpiresAtTheProductionWaitBound is the wait bound's
// production form: no short wait is injected, one wait signal holds (the
// instance pod carries a platform deletion) and the fact never appears, so the
// gate expires inside its bound — the daemon's self-report is released
// verbatim and the degradation is visible. The fixed clock does not follow
// wall time and every evidence read steps it 1s, so the observed span is
// exactly the gate's share of the bound — a drifted bound fails here too; the
// exact 15s value is pinned by the scheduler's constant assertion.
func TestRecovery05GateExpiresAtTheProductionWaitBound(t *testing.T) {
	clock := newFixedClock()
	h := buildHarnessWith(t, false, deliveryConfirmWindow, nil, false, false,
		harnessOptions{clock: clock.now})
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
	if !ok || entry.JobCreatedAt.IsZero() {
		t.Fatalf("entry = %+v (%v), want a live instance", entry, ok)
	}
	h.server.setStatus("task-1", "running")

	deletion := metav1.Now()
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              entry.JobName + "-current",
			Labels:            map[string]string{"job-name": entry.JobName},
			CreationTimestamp: metav1.NewTime(entry.JobCreatedAt),
			DeletionTimestamp: &deletion,
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	h.jobs.mu.Lock()
	h.jobs.pods = append(h.jobs.pods, pod)
	h.jobs.mu.Unlock()
	// Each evidence read steps the fixed clock 1s: the gate re-reads until
	// its bound is crossed instead of sleeping it out, and the exit lands on
	// an exact clock value rather than on wall time.
	h.jobs.setOnGetJob(func() { clock.advance(time.Second) })

	daemonBody := []byte(`{"error":"agent died","failure_reason":"agent_error"}`)
	start := clock.now()
	code, body := h.call("POST", "/api/daemon/tasks/task-1/fail", token, daemonBody)
	if code != 200 {
		t.Fatalf("S14 = %d (%s), want 200", code, body)
	}
	// The round's budget starts before the first evidence read and the gate
	// stops one reserve short of it, so the exit lands at the 15s production
	// bound less the 2s the terminal write keeps: the first read is inside the
	// span, and a drifted bound (say the old 45s) still fails here.
	const wantGateSpan = 13 * time.Second
	if span := clock.now().Sub(start); span != wantGateSpan {
		t.Fatalf("gate released after %s of scheduler time, want exactly %s (the 15s bound less the terminal write's reserve)", span, wantGateSpan)
	}

	// Expiry releases the daemon's own report verbatim: no compensation body.
	forwards := h.server.forwarded(scheduler.EPFail)
	if len(forwards) != 1 || string(forwards[0].Body) != string(daemonBody) {
		t.Fatalf("fail forwards = %+v, want the daemon's body verbatim", forwards)
	}
	settled, _ := h.reg.Get("task-1")
	if settled.State != registry.StateTerminal || settled.Result != registry.ResultFailed || settled.FailureReason != "" {
		t.Fatalf("entry = %+v, want the daemon's own failed verdict", settled)
	}
	if events := h.events("task.failed_compensated"); len(events) != 0 {
		t.Fatalf("task.failed_compensated = %+v, want none on the expired wait", events)
	}
	decided := h.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != "degraded" {
		t.Fatalf("task.attribution_decided = %+v, want decision=degraded", decided)
	}
}
