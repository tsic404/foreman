// TC-tech-restart-01 (AC-09) / ADR-026 决策结果 1 证据面②、决策结果 3:
// waiting_local_directory is a post-delivery status, so every convergence face
// that observes it through C13 adopts the entry — the start fact is recorded,
// lease renewals stop and the Job is kept — and adoption never advances the
// lifecycle state (only reports do). The four faces are convergeEntry,
// ConvergeLeaseRefused, OnStartRejected and settleFromServer.
package acceptance

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/recovery"
)

// restart01Waiting is the C13 status every face in this file adopts on: the
// daemon holds the task, so the server side is past the delivery window.
const restart01Waiting = "waiting_local_directory"

// newRestartOps mounts the read face the adoption is judged on.
func newRestartOps(t *testing.T, h *harness) *httptest.Server {
	t.Helper()
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)
	return ops
}

// assertWaitingAdoption is the shared observable criterion: the entry carries
// the start fact, the lifecycle state stayed where the face found it, the Job
// survives and the projection shows the same two facts.
func assertWaitingAdoption(t *testing.T, h *harness, ops *httptest.Server, taskID, wantState string) {
	t.Helper()
	e, ok := h.reg.Get(taskID)
	if !ok {
		t.Fatalf("entry %s disappeared during adoption", taskID)
	}
	if e.StartedAt.IsZero() {
		t.Fatalf("started_at = zero, want the C13 observation instant")
	}
	if e.IsTerminal() {
		t.Fatalf("entry settled %s/%s, want the adopted non-terminal entry", e.State, e.Result)
	}
	if string(e.State) != wantState {
		t.Errorf("state = %s, want %s (adoption must not advance it)", e.State, wantState)
	}
	if h.jobs.jobDeleted(e.JobName) {
		t.Errorf("Job %s deleted by an adoption", e.JobName)
	}
	assertProjectedTime(t, ops.URL, taskID, "started_at", true)
	if got := projectedEntry(t, ops.URL, taskID)["state"]; got != wantState {
		t.Errorf("projected state = %v, want %s", got, wantState)
	}
}

// restartRebuiltWaiting rebuilds the index after a restart and leaves C13
// answering waiting_local_directory for the rebuilt entry.
func restartRebuiltWaiting(t *testing.T, h *harness) {
	t.Helper()
	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if err := h.reg.Delete("task-1"); err != nil {
		t.Fatalf("drop entry: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	h.server.setStatus("task-1", restart01Waiting)
}

// Face 1 — convergeEntry, driven by the scheduler's own convergence round.
func TestRestart01WaitingLocalDirectoryRebuildAdopts(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	ops := newRestartOps(t, h)
	restartRebuiltWaiting(t, h)

	// A rebuilt entry has no start fact: the projection keeps the key empty
	// until a post-delivery observation adopts it (§4.1).
	assertProjectedTime(t, ops.URL, "task-1", "started_at", false)

	if err := h.sched.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertWaitingAdoption(t, h, ops, "task-1", "job_created")
}

// Face 2 — ConvergeLeaseRefused (C4 400): the refused renewal is settled by
// the same post-delivery criterion instead of waiting out the lease.
func TestRestart01WaitingLocalDirectoryLeaseRefusedAdopts(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	ops := newRestartOps(t, h)

	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	h.server.setStatus("task-1", restart01Waiting)
	if err := h.sched.ConvergeLeaseRefused(t.Context(), "task-1"); err != nil {
		t.Fatalf("ConvergeLeaseRefused: %v", err)
	}
	assertWaitingAdoption(t, h, ops, "task-1", "job_created")
}

// Face 3 — OnStartRejected (S8 409): the daemon's start was refused, but C13
// says the task is past delivery. The entry is adopted, not failed and not
// left for the daemon's reports (the state stays delivering).
func TestRestart01WaitingLocalDirectoryStartRejectedAdopts(t *testing.T) {
	h := newDeliveryHarness(t)
	ops := newRestartOps(t, h)

	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	claimTask(t, h, "task-1", token)
	if e, ok := h.reg.Get("task-1"); !ok || e.State != "delivering" {
		t.Fatalf("entry = %+v, %v; want the delivered state", e, ok)
	}

	h.server.setForwardCode(http.StatusConflict)
	h.server.setStatus("task-1", restart01Waiting)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != http.StatusConflict {
		t.Fatalf("S8 = %d (%s), want the server's 409 passed through", code, body)
	}

	if n := len(h.server.forwarded("fail")); n != 0 {
		t.Fatalf("synthesized fail reports = %d, want none (the task is post-delivery)", n)
	}
	assertWaitingAdoption(t, h, ops, "task-1", "delivering")
}

// Face 4 — settleFromServer, driven by the recovery reconciler's round.
func TestRestart01WaitingLocalDirectorySettleFromServerAdopts(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	ops := newRestartOps(t, h)
	restartRebuiltWaiting(t, h)

	rec, err := recovery.New(recovery.Config{ReconcileInterval: time.Minute},
		h.reg, recoveryObjects{jobs: h.jobs}, h.server, h.sched)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertWaitingAdoption(t, h, ops, "task-1", "job_created")
}
