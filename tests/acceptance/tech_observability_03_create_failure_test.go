// TC-tech-observability-03 (AC-10) is the executable form of the claim-chain
// failure path: both failure phases (secret / job), the settlement event next
// to the failure event, the task's failed verdict and attribution, the rollback
// face (no object residue), the step trace's level discipline, and the field /
// leak face. ADR-027's coexist reading is what the two events assert.
package acceptance

import (
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// observability03FailureFields is the event table's field set for
// job.create_failed: identity, phase and the cause summary — no payload, no
// credential, no runtime detail.
var observability03FailureFields = map[string]bool{
	"time": true, "level": true, "msg": true, "component": true,
	"task_id": true, "job_name": true, "phase": true, "err": true,
}

// assertCreateFailure judges one create-failure path end to end. secretCreated
// says whether the credential Secret existed before the failure (phase=job
// creates it, phase=secret never does).
func assertCreateFailure(t *testing.T, h *harness, taskID, phase, cause string, secretCreated bool) {
	t.Helper()
	jobName := "fm-" + taskID
	credentialSecret := jobbuilder.CredentialSecretName(jobbuilder.DaemonIDFor(taskID))

	// ① The failure event carries the phase and the summary, and the
	// settlement event lands at the same point (ADR-027: the two coexist).
	failed := h.events("job.create_failed")
	if len(failed) != 1 {
		t.Fatalf("job.create_failed events = %d, want 1: %v", len(failed), failed)
	}
	event := failed[0]
	if event["level"] != "ERROR" {
		t.Errorf("job.create_failed level = %v, want ERROR", event["level"])
	}
	for field := range observability03FailureFields {
		if _, ok := event[field]; !ok {
			t.Errorf("job.create_failed misses %q: %v", field, event)
		}
	}
	for field := range event {
		if !observability03FailureFields[field] {
			t.Errorf("job.create_failed carries %q: %v", field, event)
		}
	}
	if event["task_id"] != taskID || event["job_name"] != jobName {
		t.Errorf("job.create_failed identity = %v, want task %s / job %s", event, taskID, jobName)
	}
	if event["phase"] != phase {
		t.Errorf("job.create_failed phase = %v, want %q", event["phase"], phase)
	}
	summary, _ := event["err"].(string)
	if !strings.Contains(summary, cause) {
		t.Errorf("job.create_failed err = %v, want the cause summary %q", event["err"], cause)
	}
	if strings.Contains(summary, "fmj_") {
		t.Errorf("job.create_failed err carries a token: %v", event["err"])
	}

	settled := h.events("task.failed_compensated")
	if len(settled) != 1 {
		t.Fatalf("task.failed_compensated events = %d, want 1: %v", len(settled), settled)
	}
	if settled[0]["task_id"] != taskID {
		t.Errorf("task.failed_compensated task_id = %v, want %s", settled[0]["task_id"], taskID)
	}
	if settled[0]["failure_reason"] != scheduler.FailureReasonJobCreateFailed {
		t.Errorf("task.failed_compensated failure_reason = %v, want %q",
			settled[0]["failure_reason"], scheduler.FailureReasonJobCreateFailed)
	}

	// ② The task is judged failed: the server applies a failed terminal write
	// with the create-failure attribution, and the registry verdict agrees.
	writes := h.server.appliedTerminalWrites(taskID)
	if len(writes) != 1 {
		t.Fatalf("terminal writes for %s = %d, want 1", taskID, len(writes))
	}
	if writes[0].Method != string(scheduler.EPFail) {
		t.Errorf("terminal write endpoint = %q, want %q", writes[0].Method, scheduler.EPFail)
	}
	var body map[string]string
	if err := json.Unmarshal(writes[0].Body, &body); err != nil {
		t.Fatalf("terminal body is not JSON: %v (%s)", err, writes[0].Body)
	}
	if body["failure_reason"] != scheduler.FailureReasonJobCreateFailed {
		t.Errorf("terminal body failure_reason = %q, want %q", body["failure_reason"], scheduler.FailureReasonJobCreateFailed)
	}
	if !strings.Contains(body["error"], cause) {
		t.Errorf("terminal body error = %q, want the cause summary %q", body["error"], cause)
	}
	entry, ok := h.reg.Get(taskID)
	if !ok {
		t.Fatalf("entry %s missing after the failure", taskID)
	}
	if !entry.IsTerminal() || entry.Result != registry.ResultFailed ||
		entry.FailureReason != scheduler.FailureReasonJobCreateFailed {
		t.Errorf("entry verdict = state %s result %s reason %q, want terminal/failed/%q",
			entry.State, entry.Result, entry.FailureReason, scheduler.FailureReasonJobCreateFailed)
	}

	// ③ The rollback face: no Job, no credential Secret, no pending-report
	// carrier left behind.
	if h.jobs.jobCreated(jobName) {
		t.Errorf("Job %s exists after the create failure", jobName)
	}
	if h.jobs.hasSecret(credentialSecret) {
		t.Errorf("credential Secret %s exists after the failure", credentialSecret)
	}
	createdCredential := false
	for _, secret := range h.jobs.createdSecrets {
		if secret.Name == credentialSecret {
			createdCredential = true
		}
	}
	if createdCredential != secretCreated {
		t.Errorf("credential Secret created = %v, want %v (the path's object face)", createdCredential, secretCreated)
	}
	if carriers := h.jobs.carrierSecrets(); len(carriers) != 0 {
		t.Errorf("pending-report carriers left behind: %d", len(carriers))
	}

	// ④ The step trace stays out of the default level, on the failure path too.
	if steps := h.events("job.create_step"); len(steps) != 0 {
		t.Errorf("job.create_step events at info = %d, want 0: %v", len(steps), steps)
	}

	// ⑤ No credential content reaches the failure face.
	if strings.Contains(h.logs.String(), "fmj_") {
		t.Error("a token reached the scheduler log")
	}
	if strings.Contains(string(writes[0].Body), "fmj_") {
		t.Error("a token reached the terminal body")
	}
}

// TestObservability03CreateFailurePhaseSecret drives the CreateSecret branch:
// the credential Secret was never created, and the failure is attributed to the
// secret phase.
func TestObservability03CreateFailurePhaseSecret(t *testing.T) {
	h := newHarness(t, false)
	h.jobs.createSecretErr = errors.New("rbac denied")

	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-secret")); err == nil {
		t.Fatal("OnClaim swallowed the CreateSecret failure")
	}
	assertCreateFailure(t, h, "task-secret", "secret", "rbac denied", false)
}

// TestObservability03CreateFailurePhaseJob drives the CreateJob branch: the
// credential Secret was created and must be rolled back, and the failure is
// attributed to the Job phase.
func TestObservability03CreateFailurePhaseJob(t *testing.T) {
	h := newHarness(t, false)
	h.jobs.createJobErr = errors.New("quota exceeded")

	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-job")); err == nil {
		t.Fatal("OnClaim swallowed the CreateJob failure")
	}
	assertCreateFailure(t, h, "task-job", "job", "quota exceeded", true)
}

// TestObservability03StepTraceLevels pins the step event's two levels: absent
// at the default info level, complete at debug for a claim that runs through,
// and stopping at the failed step on the failure path.
func TestObservability03StepTraceLevels(t *testing.T) {
	wantSteps := []string{"build", "clear_stale_secret", "create_secret", "create_job", "job_created"}

	t.Run("info keeps the step trace out", func(t *testing.T) {
		h := newHarness(t, false)
		if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-steps-info")); err != nil {
			t.Fatalf("OnClaim: %v", err)
		}
		if steps := h.events("job.create_step"); len(steps) != 0 {
			t.Fatalf("job.create_step at info = %d, want 0: %v", len(steps), steps)
		}
		if got := len(h.events("job.created")); got != 1 {
			t.Fatalf("job.created = %d, want the positive anchor", got)
		}
	})

	t.Run("debug traces the five steps", func(t *testing.T) {
		h := newHarness(t, false)
		h.rebuildSchedulerWithLevel(t, slog.LevelDebug, func(*scheduler.Config) {})
		if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-steps-debug")); err != nil {
			t.Fatalf("OnClaim: %v", err)
		}
		steps := h.events("job.create_step")
		if len(steps) != len(wantSteps) {
			t.Fatalf("job.create_step = %d, want the five steps %v: %v", len(steps), wantSteps, steps)
		}
		for i, event := range steps {
			if event["step"] != wantSteps[i] {
				t.Errorf("step[%d] = %v, want %q", i, event["step"], wantSteps[i])
			}
			if _, ok := event["duration_ms"]; !ok {
				t.Errorf("step %v carries no duration_ms: %v", event["step"], event)
			}
			if _, repeated := event["err"]; repeated {
				t.Errorf("step %v repeats the failure's error field: %v", event["step"], event)
			}
			if event["task_id"] != "task-steps-debug" || event["job_name"] != "fm-task-steps-debug" {
				t.Errorf("step %v identity = %v", event["step"], event)
			}
		}
	})

	t.Run("failure path stops at the failed step", func(t *testing.T) {
		h := newHarness(t, false)
		h.rebuildSchedulerWithLevel(t, slog.LevelDebug, func(*scheduler.Config) {})
		h.jobs.createSecretErr = errors.New("rbac denied")
		if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-steps-fail")); err == nil {
			t.Fatal("OnClaim swallowed the CreateSecret failure")
		}
		steps := h.events("job.create_step")
		want := wantSteps[:3]
		if len(steps) != len(want) {
			t.Fatalf("job.create_step = %d, want %v: %v", len(steps), want, steps)
		}
		for i, event := range steps {
			if event["step"] != want[i] {
				t.Errorf("step[%d] = %v, want %q", i, event["step"], want[i])
			}
		}
	})
}
