// TC-tech-delivery-01 (AC-46) drives the S6 lost-response form end to end with
// the production components: a task Job's daemon registers and claims its
// payload, the response never reaches it, and the lease keeper judges the
// committed delivery undelivered once the confirmation window expires. The
// form settles under its own attribution — never as a boot timeout — and the
// payload is never handed over twice. The two boundaries of the criterion are
// separate scenarios: a start fact inside the window keeps the task alive, and
// a task that never got its payload still converges through the boot bound.
package acceptance

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestDelivery01LostResponseSettlesAsDeliveryUnconfirmed(t *testing.T) {
	h := newDeliveryHarness(t)
	// The keeper runs throughout: the boot bound is the production one (300s),
	// so only the window can fire here.
	h.startLeaseKeeper(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if _, ok := h.reg.Get("task-1"); !ok {
		t.Fatal("entry missing right after the claim")
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)

	// The payload is handed over: the delivery commits and the daemon's
	// response is lost — it never reports S8.
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
		t.Fatalf("first S6 claim = %s, want the task payload", got)
	}
	delivered, _ := h.reg.Get("task-1")
	if delivered.State != registry.StateDelivering || delivered.DeliveredAt.IsZero() {
		t.Fatalf("entry after the delivery = %s (delivered_at %v), want delivering",
			delivered.State, delivered.DeliveredAt)
	}

	// The daemon keeps asking because it never got the payload: every later
	// claim is an empty batch, and none of them delivers a second time.
	for i := 2; i <= 4; i++ {
		if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"tasks":[]`)) {
			t.Fatalf("S6 claim %d returned %s, want an empty batch (at most one delivery)", i, got)
		}
	}

	// The confirmation window expires with no start fact: the entry settles
	// under the transport attribution, long before the boot bound.
	settled := waitTerminal(t, h, "task-1")
	if settled.Result != registry.ResultFailed || settled.FailureReason != scheduler.FailureReasonDeliveryUnconfirmed {
		t.Fatalf("verdict = state %s result %s reason %q, want terminal/failed/%s",
			settled.State, settled.Result, settled.FailureReason, scheduler.FailureReasonDeliveryUnconfirmed)
	}
	if age := settled.CompletedAt.Sub(settled.DeliveredAt); age > deliveryConfirmWindow+2*leaseKeeperInterval {
		t.Errorf("settled %s after the delivery, want within the confirmation window (%s)",
			age, deliveryConfirmWindow)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job must be deleted with the verdict")
	}

	// The convergence never borrows the boot verdict, on either face.
	compensated := h.events("task.failed_compensated")
	if len(compensated) != 1 {
		t.Fatalf("task.failed_compensated events = %d, want 1", len(compensated))
	}
	if reason, _ := compensated[0]["failure_reason"].(string); reason != scheduler.FailureReasonDeliveryUnconfirmed {
		t.Errorf("compensated reason = %q, want %q (never %q)",
			reason, scheduler.FailureReasonDeliveryUnconfirmed, scheduler.FailureReasonJobBootTimeout)
	}
	fails := h.server.forwarded(scheduler.EPFail)
	if len(fails) != 1 {
		t.Fatalf("C10 reports = %d, want 1", len(fails))
	}
	if !bytes.Contains(fails[0].Body, []byte(scheduler.FailureReasonDeliveryUnconfirmed)) {
		t.Errorf("C10 body = %s, want the delivery_unconfirmed attribution", fails[0].Body)
	}

	// The delivery face: one hand-over, the empty batches after it as the
	// lost-response signature, and one convergence.
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimDelivered); n != 1 {
		t.Errorf("delivered claim count = %d, want 1", n)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimEmptyUnconfirmed); n != 3 {
		t.Errorf("empty_unconfirmed claim count = %d, want 3 (the daemon kept asking after the commit)", n)
	}
	if n := h.metrics.deliveryUnconfirmedCount(); n != 1 {
		t.Errorf("delivery_unconfirmed counter = %d, want 1", n)
	}
	events := h.events("task.delivery_unconfirmed")
	if len(events) != 1 {
		t.Fatalf("task.delivery_unconfirmed events = %d, want 1", len(events))
	}
	if level, _ := events[0]["level"].(string); level != "WARN" {
		t.Errorf("task.delivery_unconfirmed level = %q, want WARN", level)
	}
	if claims, _ := events[0]["claims_after_delivery"].(float64); claims <= 0 {
		t.Errorf("claims_after_delivery = %v, want > 0 (the requests arrived, the response did not)",
			events[0]["claims_after_delivery"])
	}
}

// TestDelivery01StartWithinWindowKeepsTheTaskAlive covers the first boundary:
// a delivery that is confirmed inside the window (the daemon reports S8) must
// never be judged undelivered.
func TestDelivery01StartWithinWindowKeepsTheTaskAlive(t *testing.T) {
	h := newDeliveryHarness(t)
	// The keeper runs from the start: the start fact has to land inside the
	// window while the keeper is judging it.
	h.startLeaseKeeper(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
		t.Fatalf("first S6 claim = %s, want the task payload", got)
	}

	// The daemon got the payload and reports start, inside the window.
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != 200 {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}

	// Outlive the window (plus a couple of lease rounds) and check that the
	// undelivered verdict never fired.
	time.Sleep(deliveryConfirmWindow + 4*leaseKeeperInterval)
	entry, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("the entry must still be visible")
	}
	if entry.State != registry.StateRunning || entry.StartedAt.IsZero() {
		t.Fatalf("entry = %s (started_at %v), want running with a start fact", entry.State, entry.StartedAt)
	}
	if entry.FailureReason != "" || entry.IsTerminal() {
		t.Fatalf("a confirmed delivery must stay non-terminal: %s/%s %q",
			entry.State, entry.Result, entry.FailureReason)
	}
	if n := h.metrics.deliveryUnconfirmedCount(); n != 0 {
		t.Errorf("delivery_unconfirmed counter = %d, want 0", n)
	}
	if n := len(h.server.forwarded(scheduler.EPFail)); n != 0 {
		t.Errorf("C10 reports = %d, want 0", n)
	}
}

// TestDelivery01UndeliveredTaskStillBootTimesOut covers the second boundary:
// a task whose daemon never claimed its payload is not the lost-response form,
// so the boot bound still owns it.
func TestDelivery01UndeliveredTaskStillBootTimesOut(t *testing.T) {
	// The boot bound is the only bound a never-delivered entry has; shorten it
	// so the scenario stays finite (the window stays below it).
	h := newDeliveryHarnessWithBootBound(t, 800*time.Millisecond)
	// No delivery is ever committed here, so no window is armed and the boot
	// branch is the only candidate from the first round on.
	h.startLeaseKeeper(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-2")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}

	settled := waitTerminal(t, h, "task-2")
	if settled.Result != registry.ResultFailed || settled.FailureReason != scheduler.FailureReasonJobBootTimeout {
		t.Fatalf("verdict = state %s result %s reason %q, want terminal/failed/%s",
			settled.State, settled.Result, settled.FailureReason, scheduler.FailureReasonJobBootTimeout)
	}
	if n := h.metrics.deliveryUnconfirmedCount(); n != 0 {
		t.Errorf("delivery_unconfirmed counter = %d, want 0 (no delivery was ever committed)", n)
	}
}

// TestDelivery01BootBoundFirstKeepsTheDeliveryAttribution covers the ordering
// the config relation cannot promise: the window is delivery-relative and the
// boot bound is claim-relative, so an entry can reach its boot bound while its
// confirmation window is still open (a delivery late in the claim window, or
// here a boot bound shortened past the window by construction). The delivered
// entry must keep its own attribution — the boot branch owns only entries
// whose window was never armed — so the boot verdict must never appear.
//
// The ordering is constructed, not raced: the delivery commits before the
// keeper starts, so every round from then on meets an armed window alongside
// an expired boot bound. Starting the keeper earlier would let a round land in
// the claim → delivery gap, where the boot branch legitimately owns the entry
// — the same verdict this case is about, which would make a real regression
// indistinguishable from a slow machine.
func TestDelivery01BootBoundFirstKeepsTheDeliveryAttribution(t *testing.T) {
	h := newDeliveryHarnessWithBootBound(t, 100*time.Millisecond)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
		t.Fatalf("first S6 claim = %s, want the task payload", got)
	}
	h.startLeaseKeeper(t)

	settled := waitTerminal(t, h, "task-1")
	if settled.Result != registry.ResultFailed || settled.FailureReason != scheduler.FailureReasonDeliveryUnconfirmed {
		t.Fatalf("verdict = state %s result %s reason %q, want terminal/failed/%s (never %s)",
			settled.State, settled.Result, settled.FailureReason,
			scheduler.FailureReasonDeliveryUnconfirmed, scheduler.FailureReasonJobBootTimeout)
	}
	compensated := h.events("task.failed_compensated")
	if len(compensated) != 1 {
		t.Fatalf("task.failed_compensated events = %d, want 1", len(compensated))
	}
	if reason, _ := compensated[0]["failure_reason"].(string); reason != scheduler.FailureReasonDeliveryUnconfirmed {
		t.Errorf("compensated reason = %q, want %q (the boot branch stole the attribution)",
			reason, scheduler.FailureReasonDeliveryUnconfirmed)
	}
}

// newDeliveryHarness is the shared fixture: the production components, two
// declared runtimes and the catalog route off. The lease keeper is NOT running
// yet — a scenario starts it (startLeaseKeeper) at the point its own ordering
// requires, so no round can land before the state the scenario constructs.
func newDeliveryHarness(t *testing.T) *harness {
	t.Helper()
	return newDeliveryHarnessWithBootBound(t, 300*time.Second)
}

// newDeliveryHarnessWithBootBound is newDeliveryHarness with the boot bound
// shortened, so a scenario can outlive it. The scheduler is rebuilt before any
// keeper starts, because the keeper is built from it.
func newDeliveryHarnessWithBootBound(t *testing.T, boot time.Duration) *harness {
	t.Helper()
	h := newHarness(t, false)
	h.rebuildScheduler(t, func(c *scheduler.Config) { c.JobBootTimeout = boot })
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	return h
}

// deliveryPayload is the claim payload the scenarios dispatch: the runtime id
// picks the provider, so the Job's daemon has to declare that provider's type.
func deliveryPayload(taskID string) json.RawMessage {
	return json.RawMessage(`{
		"id": "` + taskID + `", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-zz", "agent": {"model": "k3"}
	}`)
}

// issueJobToken issues the Job Token the task's daemon authenticates with.
func issueJobToken(t *testing.T, h *harness, taskID string) string {
	t.Helper()
	token, err := h.issuer.Issue("fm-"+taskID, taskID, "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue(fm-%s): %v", taskID, err)
	}
	return token
}

// registerTaskDaemon performs S4 for the task's Job, declaring the provider
// its claim was dispatched under.
func registerTaskDaemon(t *testing.T, h *harness, taskID, token string) {
	t.Helper()
	code, body := h.call("POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-`+taskID+`",
		"runtimes": [{"name": "foreman-zz", "type": "zz", "version": "3.1.4"}]
	}`))
	if code != 200 {
		t.Fatalf("S4 = %d (%s), want 200", code, body)
	}
}

// claimTask performs one S6 claim for the task's Job runtime and returns the
// response body.
func claimTask(t *testing.T, h *harness, taskID, token string) []byte {
	t.Helper()
	entry, ok := h.reg.Get(taskID)
	if !ok {
		t.Fatalf("entry %s missing", taskID)
	}
	body, err := json.Marshal(map[string]any{
		"daemon_id":   "fm-" + taskID,
		"runtime_ids": []string{entry.JobRuntimeID},
		"max_tasks":   4,
	})
	if err != nil {
		t.Fatalf("marshal claim body: %v", err)
	}
	code, raw := h.call("POST", "/api/daemon/tasks/claim", token, body)
	if code != 200 {
		t.Fatalf("S6 = %d (%s), want 200", code, raw)
	}
	return raw
}

// waitTerminal blocks until the entry reaches a terminal state.
func waitTerminal(t *testing.T, h *harness, taskID string) registry.TaskEntry {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if e, ok := h.reg.Get(taskID); ok && e.IsTerminal() {
			return e
		}
		time.Sleep(5 * time.Millisecond)
	}
	e, _ := h.reg.Get(taskID)
	t.Fatalf("entry %s never reached a terminal state: state %s result %s", taskID, e.State, e.Result)
	return registry.TaskEntry{}
}
