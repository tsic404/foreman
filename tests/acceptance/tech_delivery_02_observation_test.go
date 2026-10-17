// TC-tech-delivery-02 (AC-47) covers the observation face of the S6
// lost-response form: the four delivery claim results with their label
// semantics (the recovered re-delivery among them), the two diagnostic events
// and their fields, and the terminal record that still answers after the
// credential reclaim deleted the live entry — the convergence instant is no
// longer the end of the evidence window.
package acceptance

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestDelivery02LostResponseLeavesMetricsEventsAndRetainedRecord(t *testing.T) {
	// The boot bound (shortened, so the scenario stays finite) is the form's
	// only terminal bound: the window only releases the payload.
	h := newDeliveryHarnessWithBootBound(t, 900*time.Millisecond)
	h.startLeaseKeeper(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
		t.Fatalf("first S6 claim = %s, want the task payload", got)
	}
	for i := range 2 {
		if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"tasks":[]`)) {
			t.Fatalf("in-window claim %d = %s, want an empty batch", i+1, got)
		}
	}

	// The window expires and the daemon's next claim recovers the payload.
	time.Sleep(deliveryConfirmWindow + 2*leaseKeeperInterval)
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
		t.Fatalf("re-delivery claim = %s, want the payload again", got)
	}

	// No attempt is ever confirmed: the boot bound settles the form under the
	// delivery attribution.
	settled := waitTerminal(t, h, "task-1")
	if settled.FailureReason != scheduler.FailureReasonDeliveryUnconfirmed {
		t.Fatalf("reason = %q, want %q", settled.FailureReason, scheduler.FailureReasonDeliveryUnconfirmed)
	}
	if settled.Deliveries != 2 {
		t.Fatalf("deliveries = %d, want 2", settled.Deliveries)
	}

	// A decided entry's empty batch is the designed one — drain never delivers
	// — so it must stay a plain empty and never feed the lost-response
	// signature the alerting reads.
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"tasks":[]`)) {
		t.Fatalf("drain claim = %s, want an empty batch", got)
	}

	// ① The metric face: one hand-over, the recovered one, the empty batches
	// inside the first window as the lost-response signature, the decided
	// entry's empty as a plain empty, and one convergence counted.
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimDelivered); n != 1 {
		t.Errorf("delivered claim count = %d, want 1", n)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimRedelivered); n != 1 {
		t.Errorf("redelivered claim count = %d, want 1", n)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimEmptyUnconfirmed); n != 2 {
		t.Errorf("empty_unconfirmed claim count = %d, want 2", n)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimEmpty); n != 1 {
		t.Errorf("plain empty claim count = %d, want 1 (the decided entry's drain empty)", n)
	}
	if n := h.metrics.deliveryUnconfirmedCount(); n != 1 {
		t.Errorf("delivery_unconfirmed counter = %d, want 1", n)
	}

	// ② The event faces: the recovery and the verdict, each once, with the
	// per-task discriminator and the commit count.
	rearmed := h.events("task.delivery_rearmed")
	if len(rearmed) != 1 {
		t.Fatalf("task.delivery_rearmed events = %d, want 1", len(rearmed))
	}
	for _, field := range []string{"task_id", "job_name", "daemon_id", "delivered_at", "window_seconds", "claims_after_delivery", "deliveries"} {
		if _, ok := rearmed[0][field]; !ok {
			t.Errorf("task.delivery_rearmed is missing field %q: %v", field, rearmed[0])
		}
	}
	if n, _ := rearmed[0]["claims_after_delivery"].(float64); n != 2 {
		t.Errorf("rearmed claims_after_delivery = %v, want 2 (the daemon kept asking inside the window)", rearmed[0]["claims_after_delivery"])
	}
	if n, _ := rearmed[0]["deliveries"].(float64); n != 2 {
		t.Errorf("rearmed deliveries = %v, want 2", rearmed[0]["deliveries"])
	}

	events := h.events("task.delivery_unconfirmed")
	if len(events) != 1 {
		t.Fatalf("task.delivery_unconfirmed events = %d, want 1", len(events))
	}
	for _, field := range []string{"task_id", "job_name", "daemon_id", "delivered_at", "window_seconds", "claims_after_delivery", "deliveries"} {
		if _, ok := events[0][field]; !ok {
			t.Errorf("task.delivery_unconfirmed is missing field %q: %v", field, events[0])
		}
	}
	if n, _ := events[0]["deliveries"].(float64); n != 2 {
		t.Errorf("verdict deliveries = %v, want 2 (no attempt was ever confirmed)", events[0]["deliveries"])
	}

	// ③ The retention face: the credential reclaim deletes the live entry and
	// leaves the terminal record behind, the delivery counts included.
	if err := h.sched.OnJobTerminated(t.Context(), "fm-task-1", scheduler.JobTerminationReasonJobTerminated, time.Now(), 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if _, ok := h.reg.Get("task-1"); ok {
		t.Fatal("the reclaim must delete the live entry")
	}
	record, ok := h.reg.Settled("task-1")
	if !ok {
		t.Fatal("the reclaim must leave the terminal record")
	}
	if record.FailureReason != scheduler.FailureReasonDeliveryUnconfirmed || record.ClaimsAfterDelivery != 2 || record.Deliveries != 2 {
		t.Errorf("retained record = %+v, want the verdict with its delivery diagnostic", record)
	}
	if !record.StartedAt.IsZero() {
		t.Errorf("retained record started_at = %v, want no start fact (none ever arrived)", record.StartedAt)
	}
	if record.Payload != nil || record.TokenHash != "" {
		t.Errorf("the retained record must carry neither the payload nor the token digest: %+v", record)
	}

	ops := httptest.NewServer(observability.NewOpsHandler(observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	code, body := httpGet(t, ops.URL+"/foreman/tasks/task-1")
	if code != http.StatusOK {
		t.Fatalf("GET /foreman/tasks/task-1 = %d (%s), want 200 from the retained record", code, body)
	}
	var projected map[string]any
	if err := json.Unmarshal(body, &projected); err != nil {
		t.Fatalf("decode projection: %v (%s)", err, body)
	}
	if projected["state"] != "terminal" || projected["failure_reason"] != scheduler.FailureReasonDeliveryUnconfirmed {
		t.Errorf("projection = %v, want a terminal record with the delivery attribution", projected)
	}
	if claims, _ := projected["claims_after_delivery"].(float64); claims != 2 {
		t.Errorf("projected claims_after_delivery = %v, want 2", projected["claims_after_delivery"])
	}
	if deliveries, _ := projected["deliveries"].(float64); deliveries != 2 {
		t.Errorf("projected deliveries = %v, want 2", projected["deliveries"])
	}
	if _, ok := projected["started_at"]; !ok {
		t.Errorf("the projection keeps every key (contract §3.1): %v", projected)
	}

	// The list endpoint is the live index: a reclaimed task is not in it.
	if code, list := httpGet(t, ops.URL+"/foreman/tasks"); code != http.StatusOK || bytes.Contains(list, []byte(`"task-1"`)) {
		t.Errorf("GET /foreman/tasks = %d %s, want the live index without the reclaimed task", code, list)
	}
	// The payload is not part of the retained record, so the preview stays 404.
	if code, _ := httpGet(t, ops.URL+"/foreman/tasks/task-1/payload-preview"); code != http.StatusNotFound {
		t.Errorf("payload-preview = %d, want 404 (the record drops the body)", code)
	}
	// A task that never existed has no record either.
	if code, _ := httpGet(t, ops.URL+"/foreman/tasks/nope"); code != http.StatusNotFound {
		t.Errorf("unknown task = %d, want 404", code)
	}
}

// TestDelivery02ClaimsAfterDeliveryAccumulateAcrossRearms drives the form the
// cumulative reading of the counter is the only distinguishing one for: three
// commits with an empty-batch window between them. A per-window reading would
// report the second rearm with the claims of its own window alone, so after a
// later commit the entry would read as a daemon that stopped asking — the
// other branch of the diagnostic (contract §3.1, AC-47 ②).
func TestDelivery02ClaimsAfterDeliveryAccumulateAcrossRearms(t *testing.T) {
	// Two full windows have to fit under the boot bound, the form's only
	// terminal bound; the window keeps its scenario cadence.
	h := newDeliveryHarnessWith(t, 6*deliveryConfirmWindow, deliveryConfirmWindow)
	h.startLeaseKeeper(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)

	// Window 1: one hand-over, then two empty batches answered inside it.
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
		t.Fatalf("first S6 claim = %s, want the task payload", got)
	}
	for i := range 2 {
		if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"tasks":[]`)) {
			t.Fatalf("in-window claim %d = %s, want an empty batch", i+1, got)
		}
	}
	// Window 2: the expired window releases the payload, one empty claim
	// answers that commit.
	time.Sleep(deliveryConfirmWindow + 2*leaseKeeperInterval)
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
		t.Fatalf("re-delivery claim = %s, want the payload again", got)
	}
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"tasks":[]`)) {
		t.Fatalf("in-window claim = %s, want an empty batch", got)
	}
	// Window 3: the second release commits the payload a third time, and two
	// more empty batches follow before the boot bound closes the form.
	time.Sleep(deliveryConfirmWindow + 2*leaseKeeperInterval)
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
		t.Fatalf("second re-delivery claim = %s, want the payload again", got)
	}
	for i := range 2 {
		if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"tasks":[]`)) {
			t.Fatalf("post-rearm claim %d = %s, want an empty batch", i+1, got)
		}
	}

	// The event face: one rearm per released commit, and the second one's
	// counter carries the running total — the two claims of the first window
	// plus the one of the second. A per-window reading reports the second
	// window alone (1), which is what the post-hoc query face must never see.
	rearmed := h.events("task.delivery_rearmed")
	if len(rearmed) != 2 {
		t.Fatalf("task.delivery_rearmed events = %d, want 2 (one per released commit)", len(rearmed))
	}
	if n, _ := rearmed[0]["claims_after_delivery"].(float64); n != 2 {
		t.Errorf("first rearm claims_after_delivery = %v, want 2", rearmed[0]["claims_after_delivery"])
	}
	if n, _ := rearmed[1]["claims_after_delivery"].(float64); n != 3 {
		t.Errorf("second rearm claims_after_delivery = %v, want 3 (both windows' empty claims)",
			rearmed[1]["claims_after_delivery"])
	}
	if n, _ := rearmed[1]["deliveries"].(float64); n != 3 {
		t.Errorf("second rearm deliveries = %v, want 3", rearmed[1]["deliveries"])
	}

	// The boot bound settles the form; the live entry keeps the whole count.
	settled := waitTerminal(t, h, "task-1")
	if settled.FailureReason != scheduler.FailureReasonDeliveryUnconfirmed {
		t.Fatalf("reason = %q, want %q", settled.FailureReason, scheduler.FailureReasonDeliveryUnconfirmed)
	}
	if settled.Deliveries != 3 || settled.ClaimsAfterDelivery != 5 {
		t.Fatalf("settled entry = deliveries %d, claims_after_delivery %d; want 3 and 5 (2+1+2 empty batches)",
			settled.Deliveries, settled.ClaimsAfterDelivery)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimEmptyUnconfirmed); n != 5 {
		t.Errorf("empty_unconfirmed claim count = %d, want 5 (every empty batch kept asking)", n)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimRedelivered); n != 2 {
		t.Errorf("redelivered claim count = %d, want 2", n)
	}
}

func httpGet(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return resp.StatusCode, body
}
