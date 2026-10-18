// TC-tech-claim-01 (AC-56) covers the batch claim route's closed answer set and
// the legacy per-runtime claim fallback. The fallback path must stay a 404 with
// no delivery face: its only observable effect is one warn event per entry
// instance, the signal that a daemon switched off the batch path. The batch
// route's ownership judgement answers 403 for every non-own runtime id and for
// a daemon id that is not the token's job — never a 404, which the daemon would
// read as "batch claim unsupported".
package acceptance

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

func TestClaim01ClosedSetAndLegacyFallbackEvent(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-a")); err != nil {
		t.Fatalf("OnClaim(task-a): %v", err)
	}
	tokenA := issueJobToken(t, h, "task-a")
	registerTaskDaemon(t, h, "task-a", tokenA)
	entryA, ok := h.reg.Get("task-a")
	if !ok {
		t.Fatal("task-a entry missing")
	}
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-b")); err != nil {
		t.Fatalf("OnClaim(task-b): %v", err)
	}
	tokenB := issueJobToken(t, h, "task-b")
	registerTaskDaemon(t, h, "task-b", tokenB)
	entryB, ok := h.reg.Get("task-b")
	if !ok {
		t.Fatal("task-b entry missing")
	}

	// A contract-shaped batch claim: the payload is delivered and the fallback
	// event stays absent across a normal claim.
	claimBody := func(daemonID string, runtimeIDs []string) []byte {
		t.Helper()
		raw, err := json.Marshal(map[string]any{
			"daemon_id":   daemonID,
			"runtime_ids": runtimeIDs,
			"max_tasks":   4,
		})
		if err != nil {
			t.Fatalf("marshal claim body: %v", err)
		}
		return raw
	}
	observed := []int{}
	code, raw := h.call("POST", "/api/daemon/tasks/claim", tokenA, claimBody("fm-task-a", []string{entryA.JobRuntimeID}))
	observed = append(observed, code)
	if code != 200 || !bytes.Contains(raw, []byte(`"id":"task-a"`)) {
		t.Fatalf("contract-shaped S6 claim = %d %s, want 200 with the payload", code, raw)
	}
	if n := len(h.handlerEvents("task.legacy_claim_request")); n != 0 {
		t.Fatalf("legacy claim events after a batch claim = %d, want 0", n)
	}

	// A full claim-to-terminal loop on a second Job also leaves the event
	// absent: the fallback path is not part of the normal delivery flow.
	code, raw = h.call("POST", "/api/daemon/tasks/claim", tokenB, claimBody("fm-task-b", []string{entryB.JobRuntimeID}))
	observed = append(observed, code)
	if code != 200 || !bytes.Contains(raw, []byte(`"id":"task-b"`)) {
		t.Fatalf("contract-shaped S6 claim (task-b) = %d %s, want 200 with the payload", code, raw)
	}
	if code, body := h.call("POST", "/api/daemon/tasks/task-b/start", tokenB, []byte(`{}`)); code != 200 {
		t.Fatalf("S8 (task-b) = %d (%s), want 200", code, body)
	}
	if code, body := h.call("POST", "/api/daemon/tasks/task-b/complete", tokenB, []byte(`{"exit_code":0}`)); code != 200 {
		t.Fatalf("S13 (task-b) = %d (%s), want 200", code, body)
	}
	if settled := waitTerminal(t, h, "task-b"); settled.Result != registry.ResultCompleted {
		t.Fatalf("task-b verdict = %s, want completed", settled.Result)
	}
	if n := len(h.handlerEvents("task.legacy_claim_request")); n != 0 {
		t.Fatalf("legacy claim events across the closed loop = %d, want 0", n)
	}

	// Non-own runtime ids and a foreign daemon id are 403 on the Job Token
	// face. None of them may answer 404 (the daemon's switch trigger).
	for _, tc := range []struct {
		name    string
		token   string
		daemon  string
		runtime []string
	}{
		{"unknown runtime", tokenA, "fm-task-a", []string{"rid-unknown"}},
		{"another job's runtime", tokenA, "fm-task-a", []string{entryB.JobRuntimeID}},
		{"foreign runtime beside the own one", tokenA, "fm-task-a", []string{entryA.JobRuntimeID, entryB.JobRuntimeID}},
		{"daemon id is not the token's job", tokenA, "fm-task-b", []string{entryA.JobRuntimeID}},
	} {
		code, body := h.call("POST", "/api/daemon/tasks/claim", tc.token, claimBody(tc.daemon, tc.runtime))
		observed = append(observed, code)
		if code != 403 {
			t.Errorf("%s: S6 = %d (%s), want 403", tc.name, code, body)
		}
	}
	// An invalid Job Token takes the 401 face, never a delivery answer.
	code, _ = h.call("POST", "/api/daemon/tasks/claim", "fmj_invalid.token.value", claimBody("fm-task-a", []string{entryA.JobRuntimeID}))
	observed = append(observed, code)
	if code != 401 {
		t.Errorf("unauthenticated S6 = %d, want 401", code)
	}
	for _, code := range observed {
		switch code {
		case 200, 401, 403, 500:
		default:
			t.Errorf("S6 answered %d, outside the closed set {200, 401, 403, 500}", code)
		}
	}
	if n := len(h.handlerEvents("task.legacy_claim_request")); n != 0 {
		t.Fatalf("legacy claim events after the negative claims = %d, want 0", n)
	}

	// The legacy fallback: 404 with no delivery face, one event per entry.
	before, _ := h.reg.Get("task-a")
	legacyPath := func(entry registry.TaskEntry) string {
		return "/api/daemon/runtimes/" + entry.JobRuntimeID + "/tasks/claim"
	}
	for i := range 3 {
		code, body := h.call("POST", legacyPath(before), tokenA, nil)
		if code != 404 {
			t.Fatalf("legacy claim #%d = %d (%s), want 404", i+1, code, body)
		}
		if bytes.Contains(body, []byte(`"id":"task-a"`)) || bytes.Contains(body, []byte(`"tasks"`)) {
			t.Fatalf("legacy claim #%d answered with a task payload: %s", i+1, body)
		}
	}
	events := h.handlerEvents("task.legacy_claim_request")
	if len(events) != 1 {
		t.Fatalf("legacy claim events = %d, want exactly 1 for the entry", len(events))
	}
	event := events[0]
	if level, _ := event["level"].(string); level != "WARN" {
		t.Errorf("legacy claim event level = %q, want WARN", level)
	}
	for _, field := range []string{"task_id", "job_name", "daemon_id", "job_runtime_id"} {
		if _, ok := event[field]; !ok {
			t.Errorf("legacy claim event missing field %q: %v", field, event)
		}
	}
	if event["task_id"] != "task-a" || event["daemon_id"] != "fm-task-a" || event["job_name"] != before.JobName {
		t.Errorf("legacy claim event = %v, want the entry's task, daemon and Job", event)
	}
	after, _ := h.reg.Get("task-a")
	if after.State != before.State || !after.DeliveredAt.Equal(before.DeliveredAt) ||
		after.Deliveries != before.Deliveries || after.ClaimsAfterDelivery != before.ClaimsAfterDelivery ||
		after.Result != before.Result || after.FailureReason != before.FailureReason {
		t.Errorf("legacy claim changed the entry's delivery state: before %+v, after %+v", before, after)
	}

	// No Job identity: 401 and no event.
	for _, token := range []string{"", "fmj_invalid.token.value"} {
		code, _ := h.call("POST", legacyPath(before), token, nil)
		if code != 401 {
			t.Errorf("legacy claim with token %q = %d, want 401", token, code)
		}
	}
	if n := len(h.handlerEvents("task.legacy_claim_request")); n != 1 {
		t.Fatalf("legacy claim events after unauthenticated requests = %d, want 1", n)
	}

	// A re-dispatched task starts a new entry instance: the same daemon
	// identity, a new claim stamp, and one more event.
	if err := h.reg.Delete("task-a"); err != nil {
		t.Fatalf("Delete(task-a): %v", err)
	}
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-a")); err != nil {
		t.Fatalf("re-dispatch OnClaim(task-a): %v", err)
	}
	redelivered, ok := h.reg.Get("task-a")
	if !ok {
		t.Fatal("re-dispatched task-a entry missing")
	}
	if redelivered.ClaimedAt.Equal(before.ClaimedAt) {
		t.Fatalf("re-dispatched entry reused the claim stamp %v", redelivered.ClaimedAt)
	}
	tokenA = issueJobToken(t, h, "task-a")
	if code, body := h.call("POST", legacyPath(redelivered), tokenA, nil); code != 404 {
		t.Fatalf("legacy claim on the new entry = %d (%s), want 404", code, body)
	}
	if n := len(h.handlerEvents("task.legacy_claim_request")); n != 2 {
		t.Fatalf("legacy claim events after re-dispatch = %d, want 2 (one per entry instance)", n)
	}
}
