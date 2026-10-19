// TC-tech-lifecycle-03 (AC-11, P1): cancellation propagates. The daemon's
// status poll forwards the server's cancelled answer, its cancel-ack is
// forwarded, the mapping settles as cancelled and the Job is deleted inside
// the 10s budget — the Job deletion is what terminates the agent process.
package acceptance

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestLifecycle03CancelPropagation(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	claimTask(t, h, "task-1", token)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}

	// The server cancels; the daemon's status poll sees it verbatim.
	h.upstream.setTaskStatus("task-1", "cancelled")
	cancelledAt := time.Now()
	code, statusBody := h.call("GET", "/api/daemon/tasks/task-1/status", token, nil)
	if code != http.StatusOK {
		t.Fatalf("S9 = %d (%s), want the upstream's 200", code, statusBody)
	}
	if !strings.Contains(string(statusBody), `"cancelled"`) {
		t.Fatalf("S9 body = %s, want the server's cancelled answer verbatim", statusBody)
	}

	// The daemon's cancel-ack is forwarded and settles the mapping.
	ack := []byte(`{"status":"cancelled","reason":"user_cancel"}`)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/cancel-ack", token, ack); code != http.StatusOK {
		t.Fatalf("S15 = %d (%s), want 200", code, body)
	}
	forwards := h.server.forwarded(scheduler.EPCancelAck)
	if len(forwards) != 1 || string(forwards[0].Body) != string(ack) {
		t.Errorf("cancel-ack forwards = %+v, want one verbatim body", forwards)
	}
	if got := h.server.acceptedFor(scheduler.EPCancelAck); got != 1 {
		t.Errorf("accepted cancel-ack writes = %d, want 1", got)
	}

	entry := waitTerminal(t, h, "task-1")
	if entry.Result != registry.ResultCancelled {
		t.Errorf("result = %s, want cancelled", entry.Result)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job was not deleted after the cancel-ack")
	}
	if elapsed := time.Since(cancelledAt); elapsed > 10*time.Second {
		t.Errorf("cancellation took %s, want within 10s", elapsed)
	}
}
