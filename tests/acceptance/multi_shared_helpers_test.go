// Shared helpers of the stage-two acceptance cases (TC-tech-multi-01…07).
// The fixtures live in harness_test.go; this file holds the two-instance
// driving steps those cases share.
package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/coordination"
	"github.com/tsic404/foreman/internal/registry"
)

// multiSharedWindowPad is the wait added past a confirmation window before the
// re-delivery assertion (the boundary itself is inclusive).
const multiSharedWindowPad = 80 * time.Millisecond

// declareAll runs the C1 declaration for both instances against the shared
// upstream stub (the harness equivalent of each process's startup).
// stateEventLines returns the recorded availability transition lines.
func (tw *twoInstances) stateEventLines() []string {
	var out []string
	for _, line := range strings.Split(tw.stateEvents.String(), "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func (tw *twoInstances) declareAll(t *testing.T) {
	t.Helper()
	tw.a.declare(declaredRuntimes())
	tw.b.declare(declaredRuntimes())
}

// startClaims runs the production claim loop on both instances. The
// scheduler's leader gate keeps the follower from claiming; the check is the
// scenario's own assertion.
func (tw *twoInstances) startClaims(t *testing.T) {
	t.Helper()
	startConcurrency01ClaimLoop(t, tw.a)
	startConcurrency01ClaimLoop(t, tw.b)
}

// awaitClaimedJob waits until the shared fake cluster holds the task's Job.
func (tw *twoInstances) awaitClaimedJob(t *testing.T, taskID string) {
	t.Helper()
	name := "fm-" + taskID
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if tw.a.jobs.jobCreated(name) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	slots, _ := tw.coord.Slots(context.Background(), "")
	t.Fatalf("Job %s was never created; slots=%d server_queue=%d queue_rows=%d", name, slots, tw.a.upstream.queuedCount(), tw.a.jobs.liveJobs())
}

// awaitNoClaimedJob asserts no Job appears for the task inside the window.
func (tw *twoInstances) awaitNoClaimedJob(t *testing.T, taskID string, window time.Duration) {
	t.Helper()
	name := "fm-" + taskID
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if tw.a.jobs.jobCreated(name) {
			t.Fatalf("Job %s appeared though no claim was allowed", name)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// entry reads one task's shared row (nil error when the row is gone).
func (tw *twoInstances) entry(t *testing.T, taskID string) (registry.TaskEntry, bool) {
	t.Helper()
	e, ok, err := tw.store.GetEntry(context.Background(), "ws-1", taskID)
	if err != nil {
		t.Fatalf("GetEntry(%s): %v", taskID, err)
	}
	return e, ok
}

// token issues a Job Token for the task through one instance's issuer.
func (tw *twoInstances) token(t *testing.T, h *harness, taskID string) string {
	t.Helper()
	return issueJobToken(t, h, taskID)
}

// claimVia performs one S-6 delivery request through h's fake server and
// returns the delivered bodies.
func (tw *twoInstances) claimVia(t *testing.T, h *harness, taskID, token string) []json.RawMessage {
	t.Helper()
	e, ok := h.reg.GetInWorkspace("ws-1", taskID)
	if !ok {
		t.Fatalf("entry %s missing", taskID)
	}
	body, err := json.Marshal(map[string]any{
		"daemon_id":   "fm-" + taskID,
		"runtime_ids": []string{e.JobRuntimeID},
		"max_tasks":   4,
	})
	if err != nil {
		t.Fatalf("marshal claim body: %v", err)
	}
	code, raw := h.call("POST", "/api/daemon/tasks/claim", token, body)
	if code != 200 {
		t.Fatalf("S6(%s) = %d (%s), want 200", taskID, code, raw)
	}
	var resp struct {
		Tasks []json.RawMessage `json:"tasks"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("S6 response: %v", err)
	}
	return resp.Tasks
}

// claimCode performs one S-6 delivery request through h's fake server and
// returns the status code without asserting it (the fail-closed faces).
func (tw *twoInstances) claimCode(t *testing.T, h *harness, taskID, token string) (int, []byte) {
	t.Helper()
	e, ok := h.reg.GetInWorkspace("ws-1", taskID)
	if !ok {
		t.Fatalf("entry %s missing", taskID)
	}
	body, err := json.Marshal(map[string]any{
		"daemon_id":   "fm-" + taskID,
		"runtime_ids": []string{e.JobRuntimeID},
		"max_tasks":   1,
	})
	if err != nil {
		t.Fatalf("marshal claim body: %v", err)
	}
	return h.call("POST", "/api/daemon/tasks/claim", token, body)
}

// registerVia performs S-4 through h's fake server (the daemon reaches the
// instance set, not the instance that created the Job).
func (tw *twoInstances) registerVia(t *testing.T, h *harness, taskID, token string) {
	t.Helper()
	registerTaskDaemon(t, h, taskID, token)
}

// startVia performs S-8 through h's fake server.
func (tw *twoInstances) startVia(t *testing.T, h *harness, taskID, token string) {
	t.Helper()
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/start", token, []byte(`{}`)); code != 200 {
		t.Fatalf("S8(%s) = %d (%s), want 200", taskID, code, body)
	}
}

// completeVia performs S-13 through h's fake server.
func (tw *twoInstances) completeVia(t *testing.T, h *harness, taskID, token string) {
	t.Helper()
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/complete", token, []byte(`{"result":"completed"}`)); code != 200 {
		t.Fatalf("S13(%s) = %d (%s), want 200", taskID, code, body)
	}
}

// statusVia performs S-9 through h's fake server.
func (tw *twoInstances) statusVia(t *testing.T, h *harness, taskID, token string) int {
	t.Helper()
	code, _ := h.call("GET", "/api/daemon/tasks/"+taskID+"/status", token, nil)
	return code
}

// projection renders one entry's external projection (the ops JSON).
func multiSharedProjection(t *testing.T, e registry.TaskEntry) map[string]any {
	t.Helper()
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal entry: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal projection: %v", err)
	}
	return out
}

// assertProjectionsEqual compares the two instances' views of one task field
// by field after both read through the shared row.
func (tw *twoInstances) assertProjectionsEqual(t *testing.T, taskID string) {
	t.Helper()
	a, okA := tw.a.reg.GetInWorkspace("ws-1", taskID)
	b, okB := tw.b.reg.GetInWorkspace("ws-1", taskID)
	if !okA || !okB {
		t.Fatalf("projection missing (a=%v b=%v)", okA, okB)
	}
	pa := multiSharedProjection(t, a)
	pb := multiSharedProjection(t, b)
	if len(pa) != len(pb) {
		t.Fatalf("projection widths differ: %d vs %d", len(pa), len(pb))
	}
	for key, want := range pa {
		if got := pb[key]; fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("projection field %s = %v on B, want %v on A", key, got, want)
		}
	}
}

// deliveryBodyComparable strips the runtime id rewrite so two deliveries can be
// compared byte for byte (§2.5: only that field may change).
func deliveryBodyComparable(t *testing.T, body json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal delivered body: %v", err)
	}
	delete(m, "runtime_id")
	return m
}

// smallLeaderCfg is the two-instance lease timing: short enough for a bounded
// takeover wait, long enough that a live holder renews well inside its lease.
func smallLeaderCfg(leaseTTL time.Duration) coordination.Config {
	return coordination.Config{
		LeaseTTL:       leaseTTL,
		TakeoverWindow: 50 * time.Millisecond,
		InstanceTTL:    5 * time.Second,
		TaskLockTTL:    time.Second,
	}
}
