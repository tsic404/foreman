// TC-tech-mapping-01 (AC-04) + §4.2: the task projection is the mapping's
// observable face, and a terminal classification once written is irreversible.
// A daemon retry crossing the first answer and a C13 convergence answer that
// disagrees must both leave the projection's result untouched, while the
// metric and event faces count the verdict exactly once.
package acceptance

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
)

func TestMapping01TerminalClassificationIsIrreversible(t *testing.T) {
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

	// The first terminal report decides completed.
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, []byte(`{"result":"ok"}`)); code != 200 {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}

	// A daemon retry crossing the first answer reports the opposite
	// classification. It is forwarded (the server dedups) but must not move
	// the local verdict (§4.2).
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/fail", token, []byte(`{"failure_reason":"job_failed"}`)); code != 200 {
		t.Fatalf("replayed S14 = %d (%s), want 200", code, body)
	}

	// The convergence face sees C13 as failed and settles: the first write
	// stands.
	if err := h.sched.SettleTerminal(t.Context(), "task-1", "failed"); err != nil {
		t.Fatalf("SettleTerminal: %v", err)
	}

	// The live mapping and its projection agree on the first verdict.
	entry, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("entry missing")
	}
	if entry.State != registry.StateTerminal || entry.Result != registry.ResultCompleted || entry.FailureReason != "" {
		t.Fatalf("entry = %s/%q reason=%q, want terminal/completed", entry.State, entry.Result, entry.FailureReason)
	}

	ops := httptest.NewServer(observability.NewOpsHandler(observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)
	code, raw := httpGet(t, ops.URL+"/foreman/tasks/task-1")
	if code != http.StatusOK {
		t.Fatalf("GET /foreman/tasks/task-1 = %d (%s), want 200", code, raw)
	}
	var projected map[string]any
	if err := json.Unmarshal(raw, &projected); err != nil {
		t.Fatalf("decode projection: %v (%s)", err, raw)
	}
	if projected["state"] != "terminal" || projected["result"] != "completed" {
		t.Fatalf("projection state/result = %v/%v, want terminal/completed", projected["state"], projected["result"])
	}
	if projected["task_id"] != "task-1" || projected["job_name"] != entry.JobName {
		t.Fatalf("projection identity = %v/%v, want task-1/%s", projected["task_id"], projected["job_name"], entry.JobName)
	}
	if reason, present := projected["failure_reason"]; !present || reason != "" {
		t.Fatalf("projection failure_reason = %v (present %v), want the empty key", reason, present)
	}

	// The verdict is counted and logged exactly once, with the first result.
	if n := h.metrics.terminalCount("completed"); n != 1 {
		t.Errorf("task terminal metric completed = %d, want 1", n)
	}
	if n := h.metrics.terminalCount("failed"); n != 0 {
		t.Errorf("task terminal metric failed = %d, want 0", n)
	}
	events := h.events("task.terminal")
	if len(events) != 1 {
		t.Fatalf("task.terminal events = %d, want exactly 1", len(events))
	}
	if events[0]["result"] != "completed" {
		t.Errorf("task.terminal result = %v, want completed", events[0]["result"])
	}

	// The convergence round trip is the second race face: another settlement
	// path decides the entry while C13 is in flight. The C13 answer that
	// arrives afterwards must not reclassify it (§4.2).
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-2")); err != nil {
		t.Fatalf("OnClaim(task-2): %v", err)
	}
	h.server.setStatus("task-2", "failed")
	h.server.setStatusHook(func() {
		if _, _, err := h.reg.MarkTerminal("task-2", registry.ResultCompleted, "", time.Now()); err != nil {
			t.Errorf("MarkTerminal(task-2): %v", err)
		}
	})
	if err := h.sched.ConvergeLeaseRefused(t.Context(), "task-2"); err != nil {
		t.Fatalf("ConvergeLeaseRefused(task-2): %v", err)
	}
	h.server.setStatusHook(nil)
	raced, ok := h.reg.Get("task-2")
	if !ok || raced.State != registry.StateTerminal || raced.Result != registry.ResultCompleted {
		t.Fatalf("task-2 = %+v, %v; want the verdict decided during the round trip", raced, ok)
	}
	code, raw = httpGet(t, ops.URL+"/foreman/tasks/task-2")
	if code != http.StatusOK {
		t.Fatalf("GET /foreman/tasks/task-2 = %d (%s), want 200", code, raw)
	}
	projected = nil
	if err := json.Unmarshal(raw, &projected); err != nil {
		t.Fatalf("decode task-2 projection: %v (%s)", err, raw)
	}
	if projected["state"] != "terminal" || projected["result"] != "completed" {
		t.Fatalf("task-2 projection state/result = %v/%v, want terminal/completed", projected["state"], projected["result"])
	}
}
