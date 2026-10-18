// TC-tech-restart-01 (AC-09) covers the projection face of the restart
// judgement: after the index is rebuilt, /foreman/tasks must show "no start
// fact yet" as an empty string with the key present, so the step-3 poll keeps
// polling — the RFC3339 zero satisfied `!= ""` at once and made the adoption
// face of step 4 undecidable — and the adopted entry of step 4 must show
// started_at non-empty while its Job survives.
package acceptance

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestRestart01AbsentStartFactsProjectEmptyThenAdopt(t *testing.T) {
	h := newHarness(t, false)
	// The rebuild reads the Job objects back, so the scenario needs the
	// production Job (labels + annotations), not the canned one.
	h.useRealBuilder(t)
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	// 步骤 1: one claimed task, its Job placed in the cluster.
	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	// This process claimed the task, so the boot upper bound exists and
	// projects RFC3339; no start fact exists yet (02-contracts.md §4.1).
	assertProjectedTime(t, ops.URL, "task-1", "boot_deadline", true)
	assertProjectedTime(t, ops.URL, "task-1", "started_at", false)

	// 步骤 2: restart — the in-memory index is gone, the Jobs are not.
	if err := h.reg.Delete("task-1"); err != nil {
		t.Fatalf("drop entry: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	// 步骤 3: the rebuilt entry has neither a start fact nor a boot upper
	// bound (this process never claimed it) — both project empty with their
	// key present, which is what the poll samples.
	assertProjectedTime(t, ops.URL, "task-1", "started_at", false)
	assertProjectedTime(t, ops.URL, "task-1", "boot_deadline", false)

	// 步骤 4: C13 reports the task running → adopt: started_at carries the
	// observation instant, the Job is kept, the entry stays non-terminal.
	h.server.setStatus("task-1", "running")
	if err := h.sched.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertProjectedTime(t, ops.URL, "task-1", "started_at", true)
	projected := projectedEntry(t, ops.URL, "task-1")
	if state := projected["state"]; state == "terminal" {
		t.Errorf("state = %v, want the adopted task kept", state)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Error("the adopted task's Job was deleted")
	}
}

// TestRestart01UnboundedBootProjectsEmpty (02-contracts.md §4.1 / §4.2): with
// FOREMAN_JOB_BOOT_TIMEOUT=0 the claim writes no upper bound at all, so the
// projection keeps the key and carries the empty string — the same form a
// rebuilt entry has, and never a degenerate claimed_at that a consumer's
// `!= ""` test would read as a real instant.
func TestRestart01UnboundedBootProjectsEmpty(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	h.rebuildScheduler(t, func(c *scheduler.Config) { c.JobBootTimeout = 0 })
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	assertProjectedTime(t, ops.URL, "task-1", "boot_deadline", false)
	assertProjectedTime(t, ops.URL, "task-1", "started_at", false)
	if state := projectedEntry(t, ops.URL, "task-1")["state"]; state == "terminal" {
		t.Errorf("state = %v, want the unbounded claim still live", state)
	}
}

// TestRestart01DeletedJobLeavesNoGhostEntry guards the discard-then-restart
// combination of TC-tech-restart-01 步骤 2/5: a Job removed from the cluster
// must not reappear as an entry when the index is rebuilt from the list face —
// the fake cluster has to drop it from ListJobs the way the apiserver does.
func TestRestart01DeletedJobLeavesNoGhostEntry(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	// The pre-start discard (failure-handling 场景 #2) removes the objects and
	// the entry; the restart that follows must find nothing to rebuild.
	if err := h.sched.DiscardTask(t.Context(), "task-1"); err != nil {
		t.Fatalf("DiscardTask: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if entries := projectedList(t, ops.URL); len(entries) != 0 {
		t.Fatalf("a discarded Job came back as a ghost entry: %v", entries)
	}
}

// claimPayload is a claim as the server hands it over (S-6).
func claimPayload(taskID string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"id":%q,"agent_id":"agent-1","workspace_id":"ws-1","runtime_id":"rt-1"}`, taskID))
}

// projectedList reads the whole task list back through the ops endpoint.
func projectedList(t *testing.T, base string) []map[string]any {
	t.Helper()
	resp, err := http.Get(base + "/foreman/tasks")
	if err != nil {
		t.Fatalf("GET /foreman/tasks: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /foreman/tasks: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /foreman/tasks = %d: %s", resp.StatusCode, raw)
	}
	var entries []map[string]any
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("decode /foreman/tasks: %v", err)
	}
	return entries
}

// projectedEntry reads one task back through the ops endpoint.
func projectedEntry(t *testing.T, base, taskID string) map[string]any {
	t.Helper()
	resp, err := http.Get(base + "/foreman/tasks/" + taskID)
	if err != nil {
		t.Fatalf("GET /foreman/tasks/%s: %v", taskID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /foreman/tasks/%s: %v", taskID, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /foreman/tasks/%s = %d: %s", taskID, resp.StatusCode, raw)
	}
	var projected map[string]any
	if err := json.Unmarshal(raw, &projected); err != nil {
		t.Fatalf("decode /foreman/tasks/%s: %v", taskID, err)
	}
	return projected
}

// assertProjectedTime asserts the field's presence and its two legal forms:
// RFC3339 when the fact exists, the empty string while it does not.
func assertProjectedTime(t *testing.T, base, taskID, field string, wantValue bool) {
	t.Helper()
	got, ok := projectedEntry(t, base, taskID)[field]
	if !ok {
		t.Fatalf("%s is missing from the projection of %s", field, taskID)
	}
	if !wantValue {
		if got != "" {
			t.Errorf("%s = %v, want the empty string while the fact is absent", field, got)
		}
		return
	}
	value, ok := got.(string)
	if !ok || value == "" {
		t.Fatalf("%s = %v, want an RFC3339 instant", field, got)
	}
	if _, err := time.Parse(time.RFC3339, value); err != nil {
		t.Errorf("%s = %q is not RFC3339: %v", field, value, err)
	}
}
