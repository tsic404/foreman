package proxy

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/scheduler"
)

// interleavedDelivery runs a hook once, inside the first delivery attempt it
// sees. The hook is the concurrent party — another claim committing a
// delivery, or a start report settling one — whose effect lands between the
// claiming request's delivery-face read and the registry's gate.
type interleavedDelivery struct {
	TaskScheduler

	interleave func(ctx context.Context, workspaceID, daemonID, runtimeID string, maxTasks int) error

	once sync.Once
	mu   sync.Mutex
	err  error
}

func (c *interleavedDelivery) Deliver(ctx context.Context, workspaceID, daemonID, runtimeID string, maxTasks int) (scheduler.Delivery, error) {
	c.once.Do(func() {
		err := c.interleave(ctx, workspaceID, daemonID, runtimeID, maxTasks)
		c.mu.Lock()
		c.err = err
		c.mu.Unlock()
	})
	return c.TaskScheduler.Deliver(ctx, workspaceID, daemonID, runtimeID, maxTasks)
}

// interleaveErr reports the hook's own outcome.
func (c *interleavedDelivery) interleaveErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// claimOnce performs one S6 claim and returns the response body.
func claimOnce(t *testing.T, rig *testRig, taskID, token, jobRID string) []byte {
	t.Helper()
	code, body := rig.call(t, "POST", "/api/daemon/tasks/claim", token,
		[]byte(`{"daemon_id":"fm-`+taskID+`","runtime_ids":["`+jobRID+`"],"max_tasks":1}`))
	if code != 200 {
		t.Fatalf("S6 = %d: %s", code, body)
	}
	if !bytes.Contains(body, []byte(`"tasks":[]`)) {
		t.Fatalf("claim = %s, want an empty batch", body)
	}
	return body
}

// TestClaimClassifiesEmptyBatchAtTheCommitInstant pins the empty batch's kind
// to the delivery gate's instant, not to the delivery-face snapshot the
// request read before the attempt (§4.1 交付后的 claim 计数). A concurrent
// claim commits the delivery in between; the contested request is then refused
// by the gate and must still answer the lost-response signature — an
// unconfirmed empty — and count it on the entry.
func TestClaimClassifiesEmptyBatchAtTheCommitInstant(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-contested")
	tok := rig.token(t, "t-contested")
	jobRID := rig.registerDaemon(t, "t-contested", tok)

	interleaved := &interleavedDelivery{TaskScheduler: rig.sched}
	interleaved.interleave = func(ctx context.Context, workspaceID, daemonID, runtimeID string, maxTasks int) error {
		// Another claim on the same daemon commits the delivery this request's
		// snapshot predates.
		_, err := rig.sched.Deliver(ctx, workspaceID, daemonID, runtimeID, maxTasks)
		return err
	}
	rig.handler.(*FakeServer).sched = interleaved

	claimOnce(t, rig, "t-contested", tok, jobRID)
	if err := interleaved.interleaveErr(); err != nil {
		t.Fatalf("concurrent claim: %v", err)
	}

	entry, ok := rig.reg.Get("t-contested")
	if !ok || entry.Deliveries != 1 || entry.DeliveredAt.IsZero() {
		t.Fatalf("entry = %s (deliveries %d, delivered_at %v), want the concurrent claim's single commit",
			entry.State, entry.Deliveries, entry.DeliveredAt)
	}
	if n := rig.metrics.deliveryClaimCount(DeliveryClaimEmptyUnconfirmed); n != 1 {
		t.Errorf("empty_unconfirmed claim count = %d, want 1 (the claim arrived after the commit and inside its window)", n)
	}
	if n := rig.metrics.deliveryClaimCount(DeliveryClaimEmpty); n != 0 {
		t.Errorf("plain empty claim count = %d, want 0", n)
	}
	if entry.ClaimsAfterDelivery != 1 {
		t.Errorf("claims_after_delivery = %d, want 1 (the empty batch answers a committed, unconfirmed delivery)",
			entry.ClaimsAfterDelivery)
	}
}

// TestClaimAfterAConcurrentStartIsPlainEmpty is the same instant read the other
// way: the request's snapshot still shows an unconfirmed delivery, but a start
// report confirms it before the gate refuses the claim. The empty batch is the
// designed one — the delivery was confirmed — so it is neither counted as the
// lost-response signature nor noted on the entry.
func TestClaimAfterAConcurrentStartIsPlainEmpty(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-confirmed")
	tok := rig.token(t, "t-confirmed")
	jobRID := rig.registerDaemon(t, "t-confirmed", tok)

	if _, err := rig.sched.Deliver(context.Background(), "ws-1", "fm-t-confirmed", jobRID, 1); err != nil {
		t.Fatalf("first delivery: %v", err)
	}

	interleaved := &interleavedDelivery{TaskScheduler: rig.sched}
	interleaved.interleave = func(context.Context, string, string, string, int) error {
		// The daemon's start report lands before this claim reaches the gate.
		_, ok := rig.reg.MarkStarted("t-confirmed", time.Now())
		if !ok {
			t.Error("MarkStarted refused the delivery's start fact")
		}
		return nil
	}
	rig.handler.(*FakeServer).sched = interleaved

	claimOnce(t, rig, "t-confirmed", tok, jobRID)
	if err := interleaved.interleaveErr(); err != nil {
		t.Fatalf("concurrent start: %v", err)
	}

	if n := rig.metrics.deliveryClaimCount(DeliveryClaimEmptyUnconfirmed); n != 0 {
		t.Errorf("empty_unconfirmed claim count = %d, want 0 (the delivery was confirmed before the gate)", n)
	}
	if n := rig.metrics.deliveryClaimCount(DeliveryClaimEmpty); n != 1 {
		t.Errorf("plain empty claim count = %d, want 1", n)
	}
	entry, _ := rig.reg.Get("t-confirmed")
	if entry.ClaimsAfterDelivery != 0 {
		t.Errorf("claims_after_delivery = %d, want 0 (nothing was left unconfirmed)", entry.ClaimsAfterDelivery)
	}
}
