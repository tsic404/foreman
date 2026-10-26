package state

// The availability face orders evidence by the instant its operation was
// issued (§10 判定; AC-106 事件零噪声): one outage is one event pair no matter
// how the concurrent results happen to come back.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// heldResult makes one command's result wait after the store has answered it,
// so its outcome reaches the availability face after a later command's result.
type heldResult struct {
	mu       sync.Mutex
	taken    bool
	answered chan struct{}
	release  chan struct{}
}

func newHeldResult() *heldResult {
	return &heldResult{answered: make(chan struct{}), release: make(chan struct{})}
}

func (h *heldResult) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *heldResult) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *heldResult) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		h.mu.Lock()
		hold := !h.taken && cmd.Name() == "del"
		h.taken = h.taken || hold
		h.mu.Unlock()
		if hold {
			close(h.answered)
			<-h.release
		}
		return err
	}
}

// TestCoordLateSuccessDoesNotCloseTheOutage pins the ordering: a call the
// store had already answered before the failure opened the interval cannot
// close it, however late its goroutine reports the success.
func TestCoordLateSuccessDoesNotCloseTheOutage(t *testing.T) {
	store, mr, events := outageStore(t)
	held := newHeldResult()
	store.(*redisStore).rdb.AddHook(held)

	early := make(chan error, 1)
	go func() { early <- store.DropQueue(context.Background(), "ws-1", "task-early") }()
	<-held.answered

	mr.SetError("outage")
	if err := store.DropQueue(context.Background(), "ws-1", "task-fail"); err == nil {
		t.Fatal("store failure was not reported")
	}
	close(held.release)
	if err := <-early; err != nil {
		t.Fatalf("call issued before the outage = %v, want nil", err)
	}
	awaitDetached(t, store)
	if store.Available() {
		t.Fatal("a success issued before the outage closed it")
	}
	if got := *events; len(got) != 1 || got[0] != "unavailable" {
		t.Fatalf("events = %v, want only the unavailable transition", got)
	}

	// A call issued after the outage still closes it exactly once.
	mr.SetError("")
	if err := store.DropQueue(context.Background(), "ws-1", "task-fail"); err != nil {
		t.Fatalf("recovered store: %v", err)
	}
	if !store.Available() {
		t.Fatal("a success issued after the outage did not close it")
	}
	if got := *events; len(got) != 2 || got[1] != "recovered" {
		t.Fatalf("events = %v, want unavailable then one recovered", got)
	}
}

// TestCoordLateFailureDoesNotReopenTheOutage pins the other direction: a
// failure whose operation was issued before the recovery cannot open a second
// interval after it.
func TestCoordLateFailureDoesNotReopenTheOutage(t *testing.T) {
	store, mr, events := outageStore(t)
	mr.SetError("outage")
	if err := store.DropQueue(context.Background(), "ws-1", "task-fail"); err == nil {
		t.Fatal("store failure was not reported")
	}

	held := newHeldResult()
	store.(*redisStore).rdb.AddHook(held)
	late := make(chan error, 1)
	go func() { late <- store.DropQueue(context.Background(), "ws-1", "task-late") }()
	<-held.answered

	mr.SetError("")
	if err := store.DropQueue(context.Background(), "ws-1", "task-fail"); err != nil {
		t.Fatalf("recovered store: %v", err)
	}
	close(held.release)
	if err := <-late; err == nil {
		t.Fatal("the failure of a call issued during the outage was not reported")
	}
	awaitDetached(t, store)
	if !store.Available() || store.DownFor() != 0 {
		t.Fatal("a failure issued before the recovery reopened the outage")
	}
	if got := *events; len(got) != 2 || got[0] != "unavailable" || got[1] != "recovered" {
		t.Fatalf("events = %v, want one unavailable then one recovered", got)
	}
}

// windowClock blocks the next read it is armed for, so a test can press
// another result into the middle of a transition's publication.
type windowClock struct {
	real    func() time.Time
	mu      sync.Mutex
	armed   bool
	entered chan struct{}
	release chan struct{}
}

func newWindowClock() *windowClock {
	return &windowClock{real: time.Now, entered: make(chan struct{}), release: make(chan struct{})}
}

func (c *windowClock) now() time.Time {
	c.mu.Lock()
	if c.armed {
		c.armed = false
		c.mu.Unlock()
		close(c.entered)
		<-c.release
		return c.real()
	}
	c.mu.Unlock()
	return c.real()
}

func (c *windowClock) arm() {
	c.mu.Lock()
	c.armed = true
	c.mu.Unlock()
}

// armOnAnswer arms a clock once the store has answered a successful command,
// which is the point just before the availability face publishes it.
type armOnAnswer struct {
	clock *windowClock
	once  sync.Once
}

func (h *armOnAnswer) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *armOnAnswer) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *armOnAnswer) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == "del" && err == nil {
			h.once.Do(func() { h.clock.arm() })
		}
		return err
	}
}

// outageStoreWithClock is outageStore with a test clock, which is what the
// publication window below needs.
func outageStoreWithClock(t *testing.T, now func() time.Time) (CoordStore, *miniredis.Miniredis, *[]string) {
	t.Helper()
	mr := miniredis.RunT(t)
	var events []string
	s, err := New(Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    time.Hour,
	}, WithClock(now), WithEventHook(func(event string, _ time.Duration, _ error) {
		events = append(events, event)
	}))
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	return s.(CoordStore), mr, &events
}

// TestCoordStaleFailureInTheRecoveryWindowIsNotPublished pins that a
// transition is published as one step: while the recovery is stopped inside
// it, a failure issued before the recovery neither concludes nor opens a
// second interval.
func TestCoordStaleFailureInTheRecoveryWindowIsNotPublished(t *testing.T) {
	clock := newWindowClock()
	store, mr, events := outageStoreWithClock(t, clock.now)

	mr.SetError("outage")
	if err := store.DropQueue(context.Background(), "ws-1", "task-fail"); err == nil {
		t.Fatal("store failure was not reported")
	}

	// A failing call issued during the outage: its reply is in hand, its
	// outcome not yet published.
	held := newHeldResult()
	store.(*redisStore).rdb.AddHook(held)
	stale := make(chan error, 1)
	go func() { stale <- store.DropQueue(context.Background(), "ws-1", "task-stale") }()
	<-held.answered

	// The recovery publishes: the hook arms the clock once the store has
	// answered the recovering call, so mark stops at its single clock read —
	// past its ordering gate, with neither the interval nor the watermark
	// written yet.
	store.(*redisStore).rdb.AddHook(&armOnAnswer{clock: clock})
	mr.SetError("")
	recovered := make(chan error, 1)
	go func() { recovered <- store.DropQueue(context.Background(), "ws-1", "task-recover") }()
	<-clock.entered

	// Press the stale failure into the stopped transition. Publication is one
	// critical section, so the stale verdict cannot conclude against the
	// half-applied state: it must still be waiting when the wait below ends.
	close(held.release)
	select {
	case <-stale:
		close(clock.release)
		<-recovered
		t.Fatal("the stale failure concluded while the recovery was still publishing")
	case <-time.After(2 * time.Second):
	}
	close(clock.release)
	if err := <-stale; err == nil {
		t.Fatal("the stale failure did not reach the store")
	}
	if err := <-recovered; err != nil {
		t.Fatalf("recovering call = %v, want nil", err)
	}
	awaitDetached(t, store)
	if got := *events; len(got) != 2 || got[0] != "unavailable" || got[1] != "recovered" {
		t.Fatalf("events = %v, want one unavailable then one recovered", got)
	}
	if !store.Available() || store.DownFor() != 0 {
		t.Fatal("the store stayed unavailable after the recovery was published")
	}
}
