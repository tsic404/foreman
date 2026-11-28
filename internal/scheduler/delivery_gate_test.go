package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

// TestDeliveryGateHoldsAFixedPayloadUntilStarted pins the S-6 fence: a fixed
// attempt's payload is not handed to the daemon before the supervisor commits
// "started" (which it does only after verifying the registration). Until then
// the daemon receives an empty batch and keeps polling.
func TestDeliveryGateHoldsAFixedPayloadUntilStarted(t *testing.T) {
	f := newFixture(t)
	var started atomic.Bool
	var gated atomic.Int32
	f.sched.cfg.DeliveryGate = func(_ context.Context, entry registry.TaskEntry) (bool, error) {
		if len(entry.FixedAttempt) == 0 {
			return true, nil
		}
		gated.Add(1)
		return started.Load(), nil
	}

	entry := f.claim(t, "task-1")
	entry.FixedAttempt = json.RawMessage(`{"attempt_id":"task-1-1","environment_id":"env-1","slot_id":"slot-1"}`)
	f.reg.Cache(entry)
	if _, ok := f.reg.MarkDaemonRegistered(entry.DaemonID, testNow, "0.11.1", "19.0.1"); !ok {
		t.Fatal("MarkDaemonRegistered refused the entry")
	}

	delivery, err := f.sched.DeliverInSource(context.Background(), "", "ws-1", entry.DaemonID, entry.JobRuntimeID, 1)
	if err != nil {
		t.Fatalf("DeliverInSource before started: %v", err)
	}
	if delivery.Delivered || len(delivery.Tasks) != 0 {
		t.Fatalf("delivery = %+v, want an empty batch before the slot started", delivery)
	}
	if gated.Load() == 0 {
		t.Fatal("the delivery gate never saw the fixed entry")
	}
	if e, ok := f.reg.Get("task-1"); !ok || !e.DeliveredAt.IsZero() {
		t.Fatalf("entry after the held claim = %+v, want no delivery commit", e)
	}

	started.Store(true)
	delivery, err = f.sched.DeliverInSource(context.Background(), "", "ws-1", entry.DaemonID, entry.JobRuntimeID, 1)
	if err != nil {
		t.Fatalf("DeliverInSource after started: %v", err)
	}
	if !delivery.Delivered || len(delivery.Tasks) != 1 {
		t.Fatalf("delivery = %+v, want the payload after the slot started", delivery)
	}
}

// TestDeliveryGateFailsClosedWhenTheLedgerIsUnreadable pins the error side: an
// unreadable gate is not "no reservation", and no payload may be handed over.
func TestDeliveryGateFailsClosedWhenTheLedgerIsUnreadable(t *testing.T) {
	f := newFixture(t)
	f.sched.cfg.DeliveryGate = func(context.Context, registry.TaskEntry) (bool, error) {
		return false, errors.New("ledger is down")
	}
	entry := f.claim(t, "task-1")
	entry.FixedAttempt = json.RawMessage(`{"attempt_id":"task-1-1","environment_id":"env-1","slot_id":"slot-1"}`)
	f.reg.Cache(entry)
	if _, ok := f.reg.MarkDaemonRegistered(entry.DaemonID, testNow, "0.11.1", "19.0.1"); !ok {
		t.Fatal("MarkDaemonRegistered refused the entry")
	}

	delivery, err := f.sched.DeliverInSource(context.Background(), "", "ws-1", entry.DaemonID, entry.JobRuntimeID, 1)
	if !errors.Is(err, ErrStoreUnavailable) {
		t.Fatalf("err = %v, want ErrStoreUnavailable", err)
	}
	if delivery.Delivered || len(delivery.Tasks) != 0 {
		t.Fatalf("delivery = %+v, want nothing handed over", delivery)
	}
	if e, ok := f.reg.Get("task-1"); !ok || !e.DeliveredAt.IsZero() {
		t.Fatalf("entry after the failed gate = %+v, want no delivery commit", e)
	}
}
