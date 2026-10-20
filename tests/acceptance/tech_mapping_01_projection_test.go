// TC-tech-mapping-01 (AC-04): the mapping projection is queryable and agrees
// with the Job annotations, located by the task-id label rather than the
// name. The four trigger-context keys are always present in the projection
// (empty strings when absent); the Job side keeps the conditional-block rule
// (an empty context writes no annotation). A no-issue-context task is served
// the same way, and an unknown task is the plain 404.
package acceptance

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	batchv1 "k8s.io/api/batch/v1"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
)

// tcMapping01DaemonLabel is the pod-template label MULTICA_DAEMON_ID's
// fieldRef reads (the identity name's carrier).
const tcMapping01DaemonLabel = "foreman.tsic.top/daemon-id"

// tcMapping01Projection reads one task from the ops endpoint.
func tcMapping01Projection(t *testing.T, base, taskID string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(base + "/foreman/tasks/" + taskID)
	if err != nil {
		t.Fatalf("GET /foreman/tasks/%s: %v", taskID, err)
	}
	defer resp.Body.Close()
	var fields map[string]any
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&fields); err != nil {
			t.Fatalf("decode projection: %v", err)
		}
	}
	return resp.StatusCode, fields
}

// tcMapping01JobByLabel finds the Job through the task-id label — the
// design's locating face.
func tcMapping01JobByLabel(t *testing.T, h *harness, taskID string) *batchv1.Job {
	t.Helper()
	h.jobs.mu.Lock()
	defer h.jobs.mu.Unlock()
	for _, job := range h.jobs.createdJobs {
		if job.Labels[registry.LabelTaskID] == taskID {
			return job
		}
	}
	t.Fatalf("no Job carries the task-id label %q", taskID)
	return nil
}

func TestMapping01ProjectionMatchesJobAnnotations(t *testing.T) {
	h := newDeliveryHarness(t)
	h.useRealBuilder(t)
	issuePayload := json.RawMessage(`{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-zz", "issue_id": "issue-1", "issue_identifier": "TSI-1",
		"agent": {"model": "k3"}
	}`)
	noIssuePayload := json.RawMessage(`{
		"id": "task-2", "agent_id": "agent-2", "workspace_id": "ws-1",
		"runtime_id": "rid-zz", "chat_session_id": "chat-1",
		"agent": {"model": "k3"}
	}`)
	for _, payload := range []json.RawMessage{issuePayload, noIssuePayload} {
		if err := h.sched.OnClaim(t.Context(), payload); err != nil {
			t.Fatalf("OnClaim: %v", err)
		}
	}
	h.reg.SetNode("task-1", "node-a")

	ops := observability.NewOpsHandler(observability.NewMetrics(), h.reg, nil, observability.OpsConfig{})
	server := httptest.NewServer(ops)
	t.Cleanup(server.Close)

	code, projection := tcMapping01Projection(t, server.URL, "task-1")
	if code != http.StatusOK {
		t.Fatalf("task-1 projection = %d, want 200", code)
	}
	job := tcMapping01JobByLabel(t, h, "task-1")

	// Identity and state agree with the Job's own labels/annotations.
	for field, want := range map[string]string{
		"task_id":        "task-1",
		"job_name":       job.Name,
		"node_name":      "node-a",
		"daemon_id":      job.Annotations[registry.AnnoDaemonID],
		"job_runtime_id": job.Annotations[registry.AnnoJobRuntimeID],
		"state":          string(registry.StateJobCreated),
		"issue_id":       job.Annotations[registry.AnnoIssueID],
	} {
		if got, _ := projection[field].(string); got != want {
			t.Errorf("projection %s = %q, want %q", field, got, want)
		}
	}
	if got := job.Spec.Template.Labels[tcMapping01DaemonLabel]; got != projection["daemon_id"] {
		t.Errorf("pod daemon-id label = %q, want the projected identity %v", got, projection["daemon_id"])
	}
	if got := projection["issue_identifier"]; got != job.Annotations[registry.AnnoIssueIdentifier] {
		t.Errorf("projection issue_identifier = %v, want the annotation %q", got, job.Annotations[registry.AnnoIssueIdentifier])
	}
	for _, key := range []string{"issue_id", "issue_identifier", "autopilot_run_id", "chat_session_id"} {
		if _, ok := projection[key]; !ok {
			t.Errorf("projection is missing the always-present context key %q", key)
		}
	}
	for _, key := range []string{registry.AnnoAutopilotRunID, registry.AnnoChatSessionID} {
		if value, present := job.Annotations[key]; present {
			t.Errorf("issue task carries annotation %q = %q, want absent", key, value)
		}
	}

	// The no-issue-context task is served the same way: the projection keeps
	// the four keys (empty strings where absent), the Job carries no issue
	// annotation, and the chat context is the only conditional block.
	code, projection = tcMapping01Projection(t, server.URL, "task-2")
	if code != http.StatusOK {
		t.Fatalf("task-2 projection = %d, want 200 (no 404/500 for a context-less task)", code)
	}
	noIssueJob := tcMapping01JobByLabel(t, h, "task-2")
	if got := projection["issue_id"]; got != "" {
		t.Errorf("task-2 issue_id = %v, want the empty string", got)
	}
	if got := projection["chat_session_id"]; got != "chat-1" {
		t.Errorf("task-2 chat_session_id = %v, want chat-1", got)
	}
	for _, key := range []string{registry.AnnoIssueID, registry.AnnoIssueIdentifier, registry.AnnoAutopilotRunID} {
		if value, present := noIssueJob.Annotations[key]; present {
			t.Errorf("context-less task carries annotation %q = %q, want absent", key, value)
		}
	}
	if got := noIssueJob.Annotations[registry.AnnoChatSessionID]; got != "chat-1" {
		t.Errorf("chat-session annotation = %q, want chat-1", got)
	}

	// An unknown task is the plain 404.
	if code, _ := tcMapping01Projection(t, server.URL, "task-missing"); code != http.StatusNotFound {
		t.Errorf("unknown task projection = %d, want 404", code)
	}
}
