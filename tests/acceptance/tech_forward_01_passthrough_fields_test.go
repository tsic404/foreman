// TC-tech-forward-01 (AC-07, AC-15): the claim payload and the lifecycle
// reports travel field for field. The delivered body keeps every claim key
// (the four constant keys, the known-value comparison, and the conditional
// keys whose presence implies their shape), adds no top-level prompt or model
// key the payload did not carry, and rewrites only runtime_id. Report bodies
// reach the upstream byte for byte, and a transient upstream failure is the
// fixed 502 body.
package acceptance

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/tsic404/foreman/internal/scheduler"
)

// tcForward01Payload is the claim body: constant keys, the known-value inputs,
// conditional keys with their shapes, and a key this build does not model.
const tcForward01Payload = `{
	"id": "task-1",
	"agent_id": "agent-1",
	"workspace_id": "ws-1",
	"runtime_id": "rid-zz",
	"issue_id": "issue-1",
	"issue_identifier": "TSI-1",
	"prior_session_id": "sess-1",
	"project": {"id": "p-1", "name": "foreman"},
	"agent": {"model": "k3", "name": "omp"},
	"repos": [{"url": "https://example.invalid/r", "ref": "main"}],
	"x_future_field": {"nested": [1, 2, {"deep": true}], "null": null}
}`

// tcForward01Keys returns the top-level key set of one JSON object.
func tcForward01Keys(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode object: %v (%s)", err, raw)
	}
	return fields
}

func TestForward01ClaimAndReportPassthrough(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), json.RawMessage(tcForward01Payload)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	entry, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("the claimed task is not in the registry")
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	deliveredRaw := claimTask(t, h, "task-1", token)
	var answer struct {
		Tasks []json.RawMessage `json:"tasks"`
	}
	if err := json.Unmarshal(deliveredRaw, &answer); err != nil || len(answer.Tasks) != 1 {
		t.Fatalf("S6 answer = %s (%v), want one task", deliveredRaw, err)
	}
	delivered := answer.Tasks[0]

	// ① Constant-key gate: every key the payload carries is present and keeps
	// its type; no key is added or dropped.
	wantKeys := tcForward01Keys(t, []byte(tcForward01Payload))
	gotKeys := tcForward01Keys(t, delivered)
	if len(gotKeys) != len(wantKeys) {
		t.Errorf("delivered top-level keys = %d, want the payload's %d", len(gotKeys), len(wantKeys))
	}
	for key := range wantKeys {
		if _, ok := gotKeys[key]; !ok {
			t.Errorf("delivered body lost top-level key %q", key)
		}
	}
	for key := range gotKeys {
		if _, ok := wantKeys[key]; !ok {
			t.Errorf("delivered body invented top-level key %q", key)
		}
	}
	for _, absent := range []string{"prompt", "model", "thinking", "system_prompt"} {
		if _, ok := gotKeys[absent]; ok {
			t.Errorf("delivered body carries top-level %q, which the payload does not", absent)
		}
	}

	// ② Known-value comparison: runtime identity is the Job's own (the same id
	// the registration answer names), workspace is the configured value, and
	// the issue path keeps a non-empty issue id.
	var runtimeID string
	if err := json.Unmarshal(gotKeys["runtime_id"], &runtimeID); err != nil {
		t.Fatalf("runtime_id: %v", err)
	}
	if runtimeID != entry.JobRuntimeID {
		t.Errorf("runtime_id = %q, want the Job runtime identity %q", runtimeID, entry.JobRuntimeID)
	}
	var workspaceID string
	if err := json.Unmarshal(gotKeys["workspace_id"], &workspaceID); err != nil {
		t.Fatalf("workspace_id: %v", err)
	}
	if workspaceID != h.cfg.WorkspaceID {
		t.Errorf("workspace_id = %q, want the configured %q", workspaceID, h.cfg.WorkspaceID)
	}
	var issueID string
	if err := json.Unmarshal(gotKeys["issue_id"], &issueID); err != nil || issueID == "" {
		t.Errorf("issue_id = %q (%v), want the non-empty issue trigger value", issueID, err)
	}

	// ③ Conditional keys: present ⇒ correct shape; absent is not a failure.
	var issueIdentifier, priorSession string
	if err := json.Unmarshal(gotKeys["issue_identifier"], &issueIdentifier); err != nil || issueIdentifier != "TSI-1" {
		t.Errorf("issue_identifier = %q (%v), want TSI-1", issueIdentifier, err)
	}
	if err := json.Unmarshal(gotKeys["prior_session_id"], &priorSession); err != nil || priorSession != "sess-1" {
		t.Errorf("prior_session_id = %q (%v), want sess-1", priorSession, err)
	}
	var project map[string]string
	if err := json.Unmarshal(gotKeys["project"], &project); err != nil || project["name"] != "foreman" {
		t.Errorf("project = %v (%v), want the structured object", project, err)
	}
	var agent struct {
		Model string `json:"model"`
		Name  string `json:"name"`
	}
	if err := json.Unmarshal(gotKeys["agent"], &agent); err != nil || agent.Model != "k3" || agent.Name != "omp" {
		t.Errorf("agent = %+v (%v), want model k3 / name omp", agent, err)
	}
	var future map[string]json.RawMessage
	if err := json.Unmarshal(gotKeys["x_future_field"], &future); err != nil {
		t.Errorf("x_future_field = %s (%v), want the unmodelled object preserved", gotKeys["x_future_field"], err)
	}

	// ④ Reports: every non-terminal lifecycle body reaches the upstream
	// verbatim. The start body is translated by the real client (ADR-025), so
	// the passthrough face is judged on the remaining reports.
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	for _, report := range []struct {
		endpoint scheduler.Endpoint
		path     string
		body     string
	}{
		{scheduler.EPProgress, "/api/daemon/tasks/task-1/progress", `{"percent":42,"note":"still working"}`},
		{scheduler.EPMessages, "/api/daemon/tasks/task-1/messages", `{"messages":[{"role":"assistant","body":"hi"}]}`},
		{scheduler.EPUsage, "/api/daemon/tasks/task-1/usage", `{"input_tokens":10,"output_tokens":20}`},
	} {
		code, answerBody := h.call("POST", report.path, token, []byte(report.body))
		if code != http.StatusOK {
			t.Fatalf("%s = %d (%s), want 200", report.path, code, answerBody)
		}
		forwards := h.server.forwarded(report.endpoint)
		if len(forwards) != 1 || string(forwards[0].Body) != report.body {
			t.Errorf("%s forward = %+v, want the body verbatim", report.endpoint, forwards)
		}
	}

	// A transient upstream failure is the fixed 502 body, not the upstream's
	// own status or body.
	h.server.setForwardCode(http.StatusServiceUnavailable)
	code, body := h.call("POST", "/api/daemon/tasks/task-1/progress", token, []byte(`{"percent":50}`))
	if code != http.StatusBadGateway || string(body) != `{"error":"upstream unavailable"}` {
		t.Errorf("transient failure = %d %s, want 502 with the fixed body", code, body)
	}
	h.server.setForwardCode(0)

	// The terminal report body is forwarded as given and the verdict is the
	// terminal result.
	completeBody := `{"exit_code":0,"result":"ok","session_id":"sess-1"}`
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, []byte(completeBody)); code != http.StatusOK {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}
	forwards := h.server.forwarded(scheduler.EPComplete)
	if len(forwards) != 1 || string(forwards[0].Body) != completeBody {
		t.Errorf("S13 forward = %+v, want the daemon's body verbatim", forwards)
	}
}
