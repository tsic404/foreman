// TC-tech-delivery-01 (AC-46) drives the S6 lost-response form end to end with
// the production components: a task Job's daemon registers and claims its
// payload, the response never reaches it, and the daemon keeps asking. The
// confirmation window bounds the re-delivery rather than the entry: passing it
// with no start fact releases the payload, and the daemon's next claim gets it
// hand-over again — so the task runs instead of dying on a transient transport
// loss. The boot bound stays the form's only terminal bound, and there its
// attribution is the delivery one, never a boot timeout.
//
// The boundaries are separate scenarios: a start fact inside the window keeps
// the task alive and settles the delivery for good, a task that never got its
// payload still converges through the boot bound, and a delivery no attempt
// ever confirms ends at the boot bound under the delivery attribution.
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

func TestDelivery01LostResponseIsRedeliveredToTheDaemon(t *testing.T) {
	h := newDeliveryHarness(t)
	// The keeper runs throughout: it must never settle the entry on the
	// window — the boot bound is far away, so a verdict here is the defect
	// this scenario guards.
	h.startLeaseKeeper(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)

	// The payload is handed over and the response is lost: the daemon never
	// reports S8, and its next claims inside the window answer empty.
	first := claimTask(t, h, "task-1", token)
	if !bytes.Contains(first, []byte(`"id":"task-1"`)) {
		t.Fatalf("first S6 claim = %s, want the task payload", first)
	}
	delivered, _ := h.reg.Get("task-1")
	if delivered.State != registry.StateDelivering || delivered.DeliveredAt.IsZero() || delivered.Deliveries != 1 {
		t.Fatalf("entry after the delivery = %s (delivered_at %v, deliveries %d), want one committed delivery",
			delivered.State, delivered.DeliveredAt, delivered.Deliveries)
	}
	for i := 2; i <= 3; i++ {
		if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"tasks":[]`)) {
			t.Fatalf("S6 claim %d inside the window returned %s, want an empty batch", i, got)
		}
	}
	if entry, _ := h.reg.Get("task-1"); entry.IsTerminal() {
		t.Fatalf("an open window must not settle the entry: %s/%s %q",
			entry.State, entry.Result, entry.FailureReason)
	}

	// The window expires with no start fact; the daemon's next claim is the
	// re-delivery. Same payload, same single path, one more commit.
	time.Sleep(deliveryConfirmWindow + 2*leaseKeeperInterval)
	second := claimTask(t, h, "task-1", token)
	if !bytes.Equal(first, second) {
		t.Fatalf("re-delivered body = %s, want the first delivery's body verbatim %s", second, first)
	}
	rearmed, _ := h.reg.Get("task-1")
	if rearmed.Deliveries != 2 {
		t.Fatalf("deliveries = %d, want 2 (the expired window released the payload once)", rearmed.Deliveries)
	}
	if !rearmed.DeliveredAt.After(delivered.DeliveredAt) {
		t.Errorf("delivered_at = %v, want the re-delivery's commit after %v",
			rearmed.DeliveredAt, delivered.DeliveredAt)
	}
	if rearmed.IsTerminal() {
		t.Fatalf("the re-delivery must leave the entry live: %s/%s %q",
			rearmed.State, rearmed.Result, rearmed.FailureReason)
	}
	// The re-delivery armed its own window: the very next claim is empty again.
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"tasks":[]`)) {
		t.Fatalf("claim after the re-delivery = %s, want an empty batch (fresh window)", got)
	}

	// The daemon now has the payload and starts: the task lives on.
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != 200 {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	started, _ := h.reg.Get("task-1")
	if started.State != registry.StateRunning || started.StartedAt.IsZero() {
		t.Fatalf("entry = %s (started_at %v), want running with a start fact", started.State, started.StartedAt)
	}
	if started.FailureReason != "" {
		t.Errorf("failure_reason = %q, want none (the re-delivery recovered the task)", started.FailureReason)
	}
	// A started entry is settled for the delivery face: no later claim may
	// hand the payload over again, whatever the window says.
	time.Sleep(deliveryConfirmWindow + 2*leaseKeeperInterval)
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"tasks":[]`)) {
		t.Fatalf("claim after the start = %s, want an empty batch (no second delivery path)", got)
	}
	if final, _ := h.reg.Get("task-1"); final.Deliveries != 2 {
		t.Errorf("deliveries = %d after the start, want 2 (never handed over a third time)", final.Deliveries)
	}

	// The task closes normally: the form's whole point.
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, []byte(`{"exit_code":0}`)); code != 200 {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}
	settled := waitTerminal(t, h, "task-1")
	if settled.Result != registry.ResultCompleted {
		t.Fatalf("verdict = %s/%s, want completed (the lost response must not fail the task)",
			settled.State, settled.Result)
	}
	if n := h.metrics.deliveryUnconfirmedCount(); n != 0 {
		t.Errorf("delivery_unconfirmed counter = %d, want 0", n)
	}
	if n := len(h.server.forwarded(scheduler.EPFail)); n != 0 {
		t.Errorf("C10 reports = %d, want 0", n)
	}

	// The claim face: one first hand-over, one recovered one, and the empty
	// batches inside each window as the lost-response signature.
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimDelivered); n != 1 {
		t.Errorf("delivered claim count = %d, want 1", n)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimRedelivered); n != 1 {
		t.Errorf("redelivered claim count = %d, want 1", n)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimEmptyUnconfirmed); n != 3 {
		t.Errorf("empty_unconfirmed claim count = %d, want 3 (two in the first window, one in the second)", n)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimEmpty); n != 1 {
		t.Errorf("plain empty claim count = %d, want 1 (the started entry's claim)", n)
	}

	// The anomaly is witnessed once, with the commit it recovered and the
	// count this commit made.
	events := h.events("task.delivery_rearmed")
	if len(events) != 1 {
		t.Fatalf("task.delivery_rearmed events = %d, want 1", len(events))
	}
	if level, _ := events[0]["level"].(string); level != "WARN" {
		t.Errorf("task.delivery_rearmed level = %q, want WARN", level)
	}
	for _, field := range []string{"task_id", "job_name", "daemon_id", "delivered_at", "window_seconds", "claims_after_delivery", "deliveries"} {
		if _, ok := events[0][field]; !ok {
			t.Errorf("task.delivery_rearmed is missing field %q: %v", field, events[0])
		}
	}
	if claims, _ := events[0]["claims_after_delivery"].(float64); claims != 2 {
		t.Errorf("claims_after_delivery = %v, want 2 (the daemon kept asking inside the window)",
			events[0]["claims_after_delivery"])
	}
	if n, _ := events[0]["deliveries"].(float64); n != 2 {
		t.Errorf("deliveries = %v, want 2 (this commit is the second)", events[0]["deliveries"])
	}
	if events := h.events("task.delivery_unconfirmed"); len(events) != 0 {
		t.Errorf("task.delivery_unconfirmed events = %d, want 0", len(events))
	}
}

// TestDelivery01StartWithinWindowKeepsTheTaskAlive covers the first boundary:
// a delivery that is confirmed inside the window (the daemon reports S8) must
// never be judged undelivered, and the window must never release a payload the
// daemon already took.
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
	// undelivered verdict never fired and no expired window released the
	// payload of a task the daemon already holds.
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
	if entry.Deliveries != 1 {
		t.Errorf("deliveries = %d, want 1 (a confirmed delivery is never repeated)", entry.Deliveries)
	}
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"tasks":[]`)) {
		t.Fatalf("post-start claim = %s, want an empty batch", got)
	}
	if n := h.metrics.deliveryUnconfirmedCount(); n != 0 {
		t.Errorf("delivery_unconfirmed counter = %d, want 0", n)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimRedelivered); n != 0 {
		t.Errorf("redelivered claim count = %d, want 0", n)
	}
	if n := len(h.server.forwarded(scheduler.EPFail)); n != 0 {
		t.Errorf("C10 reports = %d, want 0", n)
	}
}

// TestDelivery01UndeliveredTaskStillBootTimesOut covers the second boundary:
// a task whose daemon never claimed its payload is not the lost-response form,
// so the boot bound still owns it under the boot attribution.
func TestDelivery01UndeliveredTaskStillBootTimesOut(t *testing.T) {
	// The boot bound is the only bound a never-delivered entry has; shorten it
	// so the scenario stays finite (the window stays below it).
	h := newDeliveryHarnessWithBootBound(t, 800*time.Millisecond)
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

// TestDelivery01DeliveredEntryKeepsItsAttributionAtTheBootBound covers the
// terminal face of the form: a committed delivery that no attempt confirmed
// runs into the boot bound (here shortened, so the scenario stays finite) and
// settles under the delivery attribution. The window is not a bound along the
// way — the entry stays live until the boot bound, and then never borrows the
// boot verdict.
func TestDelivery01DeliveredEntryKeepsItsAttributionAtTheBootBound(t *testing.T) {
	h := newDeliveryHarnessWithBootBound(t, 800*time.Millisecond)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
		t.Fatalf("first S6 claim = %s, want the task payload", got)
	}
	// The window expires without a start fact and without another claim: the
	// keeper must leave the entry alone until the boot bound.
	h.startLeaseKeeper(t)
	time.Sleep(deliveryConfirmWindow + 4*leaseKeeperInterval)
	if entry, ok := h.reg.Get("task-1"); !ok || entry.IsTerminal() {
		t.Fatalf("entry before the boot bound = %+v, want a live entry (the window is not a bound)", entry)
	}

	settled := waitTerminal(t, h, "task-1")
	if settled.Result != registry.ResultFailed || settled.FailureReason != scheduler.FailureReasonDeliveryUnconfirmed {
		t.Fatalf("verdict = state %s result %s reason %q, want terminal/failed/%s (never %s)",
			settled.State, settled.Result, settled.FailureReason,
			scheduler.FailureReasonDeliveryUnconfirmed, scheduler.FailureReasonJobBootTimeout)
	}
	// The verdict write and the compensation event are the same path, but the
	// event lands just after the state is readable: read the log on a bound
	// instead of sampling it at the instant the poll saw the terminal state —
	// a loaded -race run can schedule the writer between the two.
	waitFor(t, "the failed-compensation event of task-1", 2*time.Second, func() bool {
		return len(h.events("task.failed_compensated")) >= 1
	})
	compensated := h.events("task.failed_compensated")
	if len(compensated) != 1 {
		t.Fatalf("task.failed_compensated events = %d, want 1", len(compensated))
	}
	if reason, _ := compensated[0]["failure_reason"].(string); reason != scheduler.FailureReasonDeliveryUnconfirmed {
		t.Errorf("compensated reason = %q, want %q (the boot branch stole the attribution)",
			reason, scheduler.FailureReasonDeliveryUnconfirmed)
	}
	// The verdict write precedes the fail report and only a landed report
	// deletes the Job: poll the deletion instead of sampling it at the write.
	waitFor(t, "the Job fm-task-1 deletion after the delivery-unconfirmed verdict", 5*time.Second, func() bool {
		return h.jobs.jobDeleted("fm-task-1")
	})
}

// TestDelivery01ClosedWindowDeliversExactlyOnce covers the TC's negative form:
// with FOREMAN_DELIVERY_CONFIRM_TIMEOUT=0 the re-delivery exit is off. The
// payload is handed over exactly once, every later claim stays empty and keeps
// counting as the lost-response signature, no rearm event ever appears, and the
// boot bound settles the form under the delivery attribution.
func TestDelivery01ClosedWindowDeliversExactlyOnce(t *testing.T) {
	h := newDeliveryHarnessWith(t, 900*time.Millisecond, 0)
	h.startLeaseKeeper(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)

	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
		t.Fatalf("first S6 claim = %s, want the task payload", got)
	}
	delivered, _ := h.reg.Get("task-1")
	if delivered.Deliveries != 1 || !delivered.DeliveryDeadline.IsZero() {
		t.Fatalf("entry after the delivery = deliveries %d, deadline %v; want one commit and no window",
			delivered.Deliveries, delivered.DeliveryDeadline)
	}

	// Past the interval a window would have covered, the daemon still gets
	// nothing back: the exit is off, not merely closed.
	for i := range 2 {
		time.Sleep(deliveryConfirmWindow / 2)
		if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"tasks":[]`)) {
			t.Fatalf("claim %d after the delivery = %s, want an empty batch (delivered exactly once)", i+1, got)
		}
	}

	// No attempt is ever confirmed: the boot bound owns the attribution.
	settled := waitTerminal(t, h, "task-1")
	if settled.Result != registry.ResultFailed || settled.FailureReason != scheduler.FailureReasonDeliveryUnconfirmed {
		t.Fatalf("verdict = state %s result %s reason %q, want terminal/failed/%s",
			settled.State, settled.Result, settled.FailureReason, scheduler.FailureReasonDeliveryUnconfirmed)
	}
	if settled.Deliveries != 1 {
		t.Errorf("deliveries = %d, want 1 (the exit being off means no re-delivery)", settled.Deliveries)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimDelivered); n != 1 {
		t.Errorf("delivered claim count = %d, want 1", n)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimRedelivered); n != 0 {
		t.Errorf("redelivered claim count = %d, want 0", n)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimEmptyUnconfirmed); n != 2 {
		t.Errorf("empty_unconfirmed claim count = %d, want 2 (both claims kept asking)", n)
	}
	if n := h.metrics.deliveryUnconfirmedCount(); n != 1 {
		t.Errorf("delivery_unconfirmed counter = %d, want 1", n)
	}
	if events := h.events("task.delivery_rearmed"); len(events) != 0 {
		t.Errorf("task.delivery_rearmed events = %d, want 0 (no window ever released the payload)", len(events))
	}
	fails := h.server.forwarded(scheduler.EPFail)
	if len(fails) != 1 || !bytes.Contains(fails[0].Body, []byte(scheduler.FailureReasonDeliveryUnconfirmed)) {
		t.Errorf("C10 reports = %d (%s), want one delivery_unconfirmed verdict", len(fails), fails)
	}
}

// newDeliveryHarness is the shared fixture: the production components, two
// declared runtimes and the catalog route off. The lease keeper is NOT running
// yet — a scenario starts it (startLeaseKeeper) at the point its own ordering
// requires, so no round can land before the state the scenario constructs.
func newDeliveryHarness(t *testing.T) *harness {
	t.Helper()
	return newDeliveryHarnessWith(t, 300*time.Second, deliveryConfirmWindow)
}

// newDeliveryHarnessWithBootBound is newDeliveryHarness with the boot bound
// shortened, so a scenario can outlive it.
func newDeliveryHarnessWithBootBound(t *testing.T, boot time.Duration) *harness {
	t.Helper()
	return newDeliveryHarnessWith(t, boot, deliveryConfirmWindow)
}

// newDeliveryHarnessWith is the fixture with both pre-start knobs: the boot
// bound and the confirmation window (0 = the re-delivery exit is off). The
// scheduler is rebuilt before any keeper starts, because the keeper is built
// from it.
func newDeliveryHarnessWith(t *testing.T, boot, window time.Duration) *harness {
	t.Helper()
	h := newHarnessWithWindow(t, false, window)
	h.rebuildScheduler(t, func(c *scheduler.Config) {
		c.JobBootTimeout = boot
		c.DeliveryConfirmTimeout = window
	})
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	return h
}

// newDeliveryHarnessWithForwardMetrics is newDeliveryHarness with m receiving
// the proxy client's forward calls: the scenario reads the counter from the
// exposition a scrape of m returns instead of the fixture's recorder.
func newDeliveryHarnessWithForwardMetrics(t *testing.T, m proxy.Metrics) *harness {
	t.Helper()
	h := buildHarnessWith(t, false, deliveryConfirmWindow, nil, false, false,
		harnessOptions{forwardMetrics: m})
	h.rebuildScheduler(t, func(c *scheduler.Config) {
		c.JobBootTimeout = 300 * time.Second
		c.DeliveryConfirmTimeout = deliveryConfirmWindow
	})
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
