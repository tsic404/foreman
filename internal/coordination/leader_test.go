package coordination

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/tsic404/foreman/internal/state"
)

func leaderTestStore(t *testing.T) (state.CoordStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	st, err := state.New(state.Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    time.Hour,
	})
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	return st.(state.CoordStore), mr
}

func leaseTestConfig() Config {
	return Config{
		LeaseTTL:       200 * time.Millisecond,
		TakeoverWindow: 50 * time.Millisecond,
		InstanceTTL:    time.Second,
		TaskLockTTL:    time.Second,
	}
}

// TestLeaderRenewsDuringReconcile covers the grant rule: the takeover's
// destructive writes run under a live renewal, so a long reconcile does not
// let another instance take the lease while this one is still writing
// (§10 控制面接管有界/单持有).
func TestLeaderRenewsDuringReconcile(t *testing.T) {
	store, _ := leaderTestStore(t)
	reconciling := make(chan struct{})
	release := make(chan struct{})
	a := New(store, "instance-a", leaseTestConfig(), WithOnAcquire(func(ctx context.Context) error {
		close(reconciling)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()
	select {
	case <-reconciling:
	case <-time.After(5 * time.Second):
		t.Fatal("A never started its takeover reconcile")
	}

	// B competes for the lease while A reconciles; the renewal keeps A's
	// lease alive, so B must stay a follower.
	b := New(store, "instance-b", leaseTestConfig())
	bctx, bcancel := context.WithCancel(ctx)
	defer bcancel()
	go func() { _ = b.Run(bctx) }()
	time.Sleep(3 * leaseTestConfig().LeaseTTL)
	if b.Held() {
		t.Fatal("B acquired the lease while A was reconciling under a live renewal")
	}
	if holder, err := store.LeadershipHolder(ctx); err != nil || holder != "instance-a" {
		t.Fatalf("holder = %q (err %v), want instance-a", holder, err)
	}

	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for !a.Held() {
		if time.Now().After(deadline) {
			t.Fatal("A was not granted after completing its reconcile")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if b.Held() {
		t.Fatal("B held the lease beside A's grant")
	}
}

// TestLeaderAbortsReconcileOnLeaseLoss covers the failure branch: a renewal
// failure cancels the reconcile context (its writes fail closed) and the
// candidate never serves (§10 单持有).
func TestLeaderAbortsReconcileOnLeaseLoss(t *testing.T) {
	store, mr := leaderTestStore(t)
	reconciling := make(chan struct{})
	cancelled := make(chan struct{})
	a := New(store, "instance-a", leaseTestConfig(), WithOnAcquire(func(ctx context.Context) error {
		close(reconciling)
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()
	select {
	case <-reconciling:
	case <-time.After(5 * time.Second):
		t.Fatal("A never started its takeover reconcile")
	}

	// The store goes away: renewals fail, the reconcile context is cancelled.
	mr.SetError("shared state outage")
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the lease loss did not cancel the reconcile context")
	}
	time.Sleep(100 * time.Millisecond)
	if a.Held() {
		t.Fatal("A was granted after its lease lapsed during the reconcile")
	}
	mr.SetError("")
}

// TestLastInstanceAfterShutdownCancellation pins the deregistration gate: the
// shutdown path cancels the loop contexts, and an in-flight store call then
// returns context.Canceled. That is not a store outage, so the last-instance
// verdict (and with it the deregistration) still holds (§4.6 优雅关闭).
func TestLastInstanceAfterShutdownCancellation(t *testing.T) {
	store, _ := leaderTestStore(t)
	ctx := context.Background()
	if err := store.HeartbeatInstance(ctx, "instance-a", time.Minute); err != nil {
		t.Fatalf("HeartbeatInstance: %v", err)
	}

	shutdown, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.HeartbeatInstance(shutdown, "instance-a", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("heartbeat on the cancelled shutdown context = %v, want context.Canceled", err)
	}

	last, err := New(store, "instance-a", leaseTestConfig()).LastInstance(ctx)
	if err != nil || !last {
		t.Fatalf("LastInstance after a cancelled op = %v, %v; want true, nil", last, err)
	}
}
