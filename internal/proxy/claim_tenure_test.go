package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/coordination"
	"github.com/tsic404/foreman/internal/scheduler"
)

// dispatchFenceScheduler blocks inside OnClaimReserved, the point where a
// claimed task becomes a Job. It records whether the dispatch arrived with a
// live tenure: a cancelled context means the fence prevented the side effect.
type dispatchFenceScheduler struct {
	fakeClaimScheduler
	entered    chan struct{}
	completed  chan struct{}
	release    chan struct{}
	enterOnce  sync.Once
	finishOnce sync.Once
	dispatched atomic.Int32
	cancelled  atomic.Bool
}

func newDispatchFenceScheduler() *dispatchFenceScheduler {
	return &dispatchFenceScheduler{
		fakeClaimScheduler: fakeClaimScheduler{budget: 1},
		entered:            make(chan struct{}),
		completed:          make(chan struct{}),
		release:            make(chan struct{}),
	}
}

func (s *dispatchFenceScheduler) OnClaimReserved(ctx context.Context, _ json.RawMessage, _ scheduler.SlotReservation, _ scheduler.ClaimScope) error {
	s.enterOnce.Do(func() { close(s.entered) })
	select {
	case <-ctx.Done():
	case <-s.release:
	}
	if ctx.Err() != nil {
		s.cancelled.Store(true)
	} else {
		// The real scheduler creates the Job here; a live tenure is what
		// authorizes that write.
		s.dispatched.Add(1)
	}
	s.finishOnce.Do(func() { close(s.completed) })
	return ctx.Err()
}

// TestClaimDispatchIsCancelledWhenTheTenureEnds pins the claim-dispatch fence:
// a holder that loses the lease while a claim answer is being dispatched
// cannot create the Job for it.
func TestClaimDispatchIsCancelledWhenTheTenureEnds(t *testing.T) {
	store, mr := tenureTestStore(t)
	leaderCfg := coordination.Config{
		LeaseTTL:       300 * time.Millisecond,
		TakeoverWindow: 100 * time.Millisecond,
		InstanceTTL:    time.Second,
		TaskLockTTL:    time.Second,
	}
	leader := coordination.New(store, "instance-1", leaderCfg)
	leaderCtx, stopLeader := context.WithCancel(context.Background())
	defer stopLeader()
	go func() { _ = leader.Run(leaderCtx) }()
	waitForTenure(t, leader, true)

	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/tokens/current/renew", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{})
	})
	stub.on("POST", "/api/daemon/tasks/claim", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{
			"tasks": []json.RawMessage{json.RawMessage(`{"id":"task-1"}`)},
		})
	})

	cfg := testConfig(srv.URL)
	client := NewClient(cfg, "mdt_test")
	registerDeclared(t, client)
	sched := newDispatchFenceScheduler()
	loop := NewClaimLoop(cfg, client, sched)
	loop.SetLeaderGate(leader.Held)

	loopCtx, stopLoop := context.WithCancel(context.Background())
	defer stopLoop()
	go func() { _ = loop.Run(loopCtx) }()

	select {
	case <-sched.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the claim dispatch never started")
	}

	// The holder is inside the dispatch now. End its tenure.
	mr.SetError("shared state outage")
	waitForTenure(t, leader, false)

	select {
	case <-sched.completed:
	case <-time.After(3 * leaderPollInterval):
		// A missing fence leaves the dispatch parked on the loop context.
		// Release it so the failure reads as a Job creation, not a hang.
		close(sched.release)
		select {
		case <-sched.completed:
		case <-time.After(2 * time.Second):
			t.Fatal("the claim dispatch never returned")
		}
	}

	if !sched.cancelled.Load() {
		t.Fatal("the in-flight claim dispatch kept a live context after the tenure ended")
	}
	if got := sched.dispatched.Load(); got != 0 {
		t.Fatalf("the scheduler created %d Job(s) after the tenure ended, want none", got)
	}
}
