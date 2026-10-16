// TC-tech-delivery-02 (AC-47) covers the observation face of the S6
// lost-response form: the three delivery metrics with their label semantics,
// the diagnostic event's fields, and the terminal record that still answers
// after the credential reclaim deleted the live entry — the convergence
// instant is no longer the end of the evidence window.
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

func TestDelivery02LostResponseLeavesMetricsEventAndRetainedRecord(t *testing.T) {
	h := newDeliveryHarness(t)
	// The keeper runs throughout: the boot bound is the production one, so the
	// window is the only bound that can settle the entry.
	h.startLeaseKeeper(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
		t.Fatalf("first S6 claim = %s, want the task payload", got)
	}
	for range 2 {
		claimTask(t, h, "task-1", token)
	}
	settled := waitTerminal(t, h, "task-1")
	if settled.FailureReason != scheduler.FailureReasonDeliveryUnconfirmed {
		t.Fatalf("reason = %q, want %q", settled.FailureReason, scheduler.FailureReasonDeliveryUnconfirmed)
	}

	// A decided entry's empty batch is the designed one — drain never delivers
	// — so it must stay a plain empty and never feed the lost-response
	// signature the alerting reads.
	if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"tasks":[]`)) {
		t.Fatalf("drain claim = %s, want an empty batch", got)
	}

	// ① The metric face: one hand-over, the empty batches after the commit
	// classified as the lost-response signature, the decided entry's empty as a
	// plain empty, and one convergence counted.
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimDelivered); n != 1 {
		t.Errorf("delivered claim count = %d, want 1", n)
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

	// ② The event face, including the per-task discriminator.
	events := h.events("task.delivery_unconfirmed")
	if len(events) != 1 {
		t.Fatalf("task.delivery_unconfirmed events = %d, want 1", len(events))
	}
	for _, field := range []string{"task_id", "job_name", "daemon_id", "delivered_at", "window_seconds", "claims_after_delivery"} {
		if _, ok := events[0][field]; !ok {
			t.Errorf("task.delivery_unconfirmed is missing field %q: %v", field, events[0])
		}
	}
	if claims, _ := events[0]["claims_after_delivery"].(float64); claims != 2 {
		t.Errorf("claims_after_delivery = %v, want 2 (the daemon kept asking after the commit)",
			events[0]["claims_after_delivery"])
	}

	// ③ The retention face: the credential reclaim deletes the live entry and
	// leaves the terminal record behind.
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
	if record.FailureReason != scheduler.FailureReasonDeliveryUnconfirmed || record.ClaimsAfterDelivery != 2 {
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
