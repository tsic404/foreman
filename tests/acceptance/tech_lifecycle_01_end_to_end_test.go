// TC-tech-lifecycle-01 (AC-03, AC-15): the end-to-end closed loop. A claim
// creates the Job inside the 5s budget, the Job's daemon registers and claims
// the same task identifier, start moves the mapping to running, and complete
// settles the server side and deletes the Job. The agent's task-level call
// travels through the same Foreman process and reaches the real server with
// its own credential, and the agent-api forward counter rises inside the
// window that claim opened — read from the exposition a scrape returns.
package acceptance

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestLifecycle01EndToEndClosedLoop(t *testing.T) {
	forwards := observability.NewMetrics()
	h := newDeliveryHarnessWithForwardMetrics(t, forwards)
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

	// The counting face of AC-15 opens with the task: the agent-api series
	// reads its baseline before the agent has called anything.
	codesBefore, successBefore, linesBefore := tcLifecycle01AgentAPIForwards(t, scrapeMetrics(t, forwards))
	for _, line := range linesBefore {
		t.Logf("agent-api forward counter before the agent's call: %s", line)
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

	// Still inside the window: the successful passthrough is one agent-api
	// forward, and only the success series moved.
	if settled, _ := h.reg.Get("task-1"); settled.IsTerminal() {
		t.Fatalf("the task settled before the counting read: %s/%s", settled.State, settled.Result)
	}
	codesAfter, successAfter, linesAfter := tcLifecycle01AgentAPIForwards(t, scrapeMetrics(t, forwards))
	for _, line := range linesAfter {
		t.Logf("agent-api forward counter after the agent's call: %s", line)
	}
	if delta := successAfter - successBefore; delta != 1 {
		t.Errorf("agent-api 2xx forwards rose by %g across the task window, want the agent's one call (before %g, after %g)",
			delta, successBefore, successAfter)
	}
	if delta := codesAfter["200"] - codesBefore["200"]; delta != 1 {
		t.Errorf(`foreman_forward_total{code="200",endpoint="agent-api"} rose by %g, want 1 (before %g, after %g)`,
			delta, codesBefore["200"], codesAfter["200"])
	}
	for code, value := range codesAfter {
		if value != codesBefore[code] && !tcLifecycle01IsSuccessCode(code) {
			t.Errorf(`foreman_forward_total{code=%q,endpoint="agent-api"} moved from %g to %g, want the success series only`,
				code, codesBefore[code], value)
		}
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

// tcLifecycle01IsSuccessCode reports whether code is in the 2xx band the
// counter's success condition covers.
func tcLifecycle01IsSuccessCode(code string) bool {
	return len(code) == 3 && code[0] == '2'
}

// tcLifecycle01AgentAPIForwards reads one exposition's agent-api forward
// series: the value per status code, the 2xx sum AC-15 judges, and the raw
// lines themselves (the scrape output a reviewer replays).
func tcLifecycle01AgentAPIForwards(t *testing.T, exposition string) (map[string]float64, float64, []string) {
	t.Helper()
	const family = `foreman_forward_total{`
	codes := map[string]float64{}
	var success float64
	var lines []string
	for _, line := range strings.Split(exposition, "\n") {
		if !strings.HasPrefix(line, family) || !strings.Contains(line, `endpoint="agent-api"`) {
			continue
		}
		lines = append(lines, line)
		open := strings.Index(line, `code="`)
		if open < 0 {
			t.Fatalf("agent-api series without a code label: %s", line)
		}
		rest := line[open+len(`code="`):]
		end := strings.Index(rest, `"`)
		if end < 0 {
			t.Fatalf("unterminated code label: %s", line)
		}
		code := rest[:end]
		fields := strings.Fields(line)
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			t.Fatalf("value of %s: %v", line, err)
		}
		codes[code] = value
		if tcLifecycle01IsSuccessCode(code) {
			success += value
		}
	}
	if len(lines) == 0 {
		t.Fatalf("the exposition carries no foreman_forward_total{endpoint=\"agent-api\"} series:\n%s", exposition)
	}
	return codes, success, lines
}
