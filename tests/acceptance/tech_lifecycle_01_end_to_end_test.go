// TC-tech-lifecycle-01 (AC-03, AC-15): the end-to-end closed loop. A claim
// creates the Job inside the 5s budget, the Job's daemon registers and claims
// the same task identifier, start moves the mapping to running, and complete
// settles the server side and deletes the Job. The agent's task-level call
// travels through the same Foreman process and reaches the real server with
// its own credential.
package acceptance

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestLifecycle01EndToEndClosedLoop(t *testing.T) {
	h := newDeliveryHarness(t)
	payload := json.RawMessage(`{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-zz", "issue_id": "issue-1", "issue_identifier": "TSI-1",
		"agent": {"model": "k3"}
	}`)

	claimedAt := time.Now()
	if err := h.sched.OnClaim(t.Context(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if elapsed := time.Since(claimedAt); elapsed > 5*time.Second {
		t.Errorf("Job creation took %s, want within the 5s budget", elapsed)
	}
	if !h.jobs.jobCreated("fm-task-1") {
		t.Fatal("the claim produced no Job")
	}
	entry, ok := h.reg.Get("task-1")
	if !ok || entry.State != registry.StateJobCreated {
		t.Fatalf("entry = %+v (%v), want job_created", entry, ok)
	}

	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	delivered := claimTask(t, h, "task-1", token)
	var answer struct {
		Tasks []json.RawMessage `json:"tasks"`
	}
	if err := json.Unmarshal(delivered, &answer); err != nil || len(answer.Tasks) != 1 {
		t.Fatalf("S6 = %s (%v), want one task", delivered, err)
	}
	var deliveredHead struct {
		ID        string `json:"id"`
		RuntimeID string `json:"runtime_id"`
	}
	if err := json.Unmarshal(answer.Tasks[0], &deliveredHead); err != nil {
		t.Fatalf("decode delivered payload: %v", err)
	}
	if deliveredHead.ID != "task-1" {
		t.Errorf("delivered id = %q, want the claimed task identifier", deliveredHead.ID)
	}
	if deliveredHead.RuntimeID != entry.JobRuntimeID {
		t.Errorf("delivered runtime_id = %q, want the Job runtime %q", deliveredHead.RuntimeID, entry.JobRuntimeID)
	}

	// The agent's own API call, during the task, reaches the real server with
	// the agent credential (not the Job Token).
	if code, body := h.call("GET", "/api/daemon/workspaces", "mat_agent_task", nil); code != http.StatusOK {
		t.Errorf("agent passthrough = %d (%s), want 200", code, body)
	}
	agentCalls := h.upstream.calls(http.MethodGet, "/api/daemon/workspaces")
	if len(agentCalls) != 1 || agentCalls[0].Header.Get("Authorization") != "Bearer mat_agent_task" {
		t.Errorf("agent call upstream view = %+v, want one call with the agent credential", agentCalls)
	}

	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	running, _ := h.reg.Get("task-1")
	if running.State != registry.StateRunning || running.StartedAt.IsZero() {
		t.Fatalf("entry = %+v, want running with a start fact", running)
	}

	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token,
		[]byte(`{"exit_code":0,"result":"ok"}`)); code != http.StatusOK {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}
	settled := waitTerminal(t, h, "task-1")
	if settled.Result != registry.ResultCompleted {
		t.Errorf("result = %s, want completed", settled.Result)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job was not deleted at the verdict")
	}
	if got := h.server.acceptedFor(scheduler.EPComplete); got != 1 {
		t.Errorf("server-side complete writes = %d, want 1", got)
	}
}
