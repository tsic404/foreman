package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/tsic404/foreman/internal/coordination"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/state"
)

// toggleLease is a manually driven tenure for the single-process assertions.
type toggleLease struct {
	held atomic.Bool
}

func (l *toggleLease) Held() bool { return l.held.Load() }
func (l *toggleLease) set(v bool) { l.held.Store(v) }

// scriptedProber fails once (to enter the retry path) and then blocks until
// its context is cancelled, so an in-flight probe is observable.
type scriptedProber struct {
	mu        sync.Mutex
	calls     int
	found     map[string]registry.Discovery
	failFirst bool
	block     bool
	blocked   chan struct{}
	cancelled chan struct{}
}

func (p *scriptedProber) RunStartupProbeInSource(ctx context.Context, _ string) (map[string]registry.Discovery, error) {
	p.mu.Lock()
	p.calls++
	n := p.calls
	p.mu.Unlock()
	if p.failFirst && n == 1 {
		return nil, errors.New("server is unreachable")
	}
	if !p.block {
		return p.found, nil
	}
	select {
	case p.blocked <- struct{}{}:
	default:
	}
	<-ctx.Done()
	select {
	case p.cancelled <- struct{}{}:
	default:
	}
	return nil, ctx.Err()
}

func (p *scriptedProber) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// controlledProber fails until switched to success, so the takeover test can
// make "would the old holder attach if it probed again?" observable.
type controlledProber struct {
	mu      sync.Mutex
	calls   int
	succeed atomic.Bool
	found   map[string]registry.Discovery
	err     error
}

func (p *controlledProber) RunStartupProbeInSource(_ context.Context, _ string) (map[string]registry.Discovery, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if !p.succeed.Load() {
		return nil, p.err
	}
	return p.found, nil
}

func (p *controlledProber) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// TestRecoveryProbeIsCancelledWhenTheLeaseLapses pins R6's first half: an
// in-flight recovery probe is cancelled the moment the tenure ends, and the
// loop stops probing and never attaches while this instance is a follower.
func TestRecoveryProbeIsCancelledWhenTheLeaseLapses(t *testing.T) {
	lease := &toggleLease{}
	lease.set(true)
	prober := &scriptedProber{
		failFirst: true,
		block:     true,
		blocked:   make(chan struct{}, 1),
		cancelled: make(chan struct{}, 1),
	}
	clients := []*proxy.Client{
		proxy.NewClient(proxy.Config{SourceID: "src-b", WorkspaceID: "ws-shared", DaemonID: "foreman-b", Version: "0.1.0"}, "token-b"),
	}
	var attached atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RetryFailedSources(ctx, prober, clients, []string{"src-b"}, lease.Held,
			func(int) time.Duration { return time.Millisecond },
			slog.New(slog.NewJSONHandler(io.Discard, nil)),
			func(string, map[string]registry.Discovery) bool {
				attached.Add(1)
				return true
			})
	}()

	// The first attempt fails; the second one is the in-flight probe.
	select {
	case <-prober.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the recovery probe never started")
	}
	lease.set(false)
	select {
	case <-prober.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("losing the lease did not cancel the in-flight probe")
	}

	// The follower parks: no further probes, and no attach.
	time.Sleep(2*sourceLeaderPoll + 200*time.Millisecond)
	if got := prober.callCount(); got != 2 {
		t.Fatalf("probe calls while follower = %d, want 2 (the loop must park, not retry)", got)
	}
	if got := attached.Load(); got != 0 {
		t.Fatalf("attach calls while follower = %d, want none", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the recovery loop did not stop with its context")
	}
}

// recoveryTestStore is the shared coordination store for the takeover test.
func recoveryTestStore(t *testing.T) (state.CoordStore, *miniredis.Miniredis) {
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

func waitForTenure(t *testing.T, leader *coordination.Leader, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for leader.Held() != want {
		if time.Now().After(deadline) {
			t.Fatalf("leader tenure never became %v", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestOnlyTheNewHolderRecoversAfterTakeover pins R6's takeover case: the old
// holder loses the lease mid-probe, its probe is cancelled and it never
// attaches; the new holder alone re-probes and attaches the source.
func TestOnlyTheNewHolderRecoversAfterTakeover(t *testing.T) {
	store, mr := recoveryTestStore(t)
	cfg := coordination.Config{
		LeaseTTL:       300 * time.Millisecond,
		TakeoverWindow: 100 * time.Millisecond,
		InstanceTTL:    time.Second,
		TaskLockTTL:    time.Second,
	}
	oldHolder := coordination.New(store, "instance-a", cfg)
	newHolder := coordination.New(store, "instance-b", cfg)
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	oldCtx, stopOld := context.WithCancel(context.Background())
	defer stopOld()
	oldRun := make(chan struct{})
	go func() { defer close(oldRun); _ = oldHolder.Run(oldCtx) }()
	waitForTenure(t, oldHolder, true)

	clientsOf := func(sourceID string) []*proxy.Client {
		return []*proxy.Client{
			proxy.NewClient(proxy.Config{SourceID: sourceID, WorkspaceID: "ws-shared", DaemonID: "foreman-" + sourceID, Version: "0.1.0"}, "token-"+sourceID),
		}
	}
	oldProber := &controlledProber{
		found: map[string]registry.Discovery{"ws-shared": {CLIVersion: "probe-old"}},
		err:   errors.New("server is unreachable"),
	}
	newProber := &scriptedProber{
		found: map[string]registry.Discovery{"ws-shared": {CLIVersion: "probe-b"}},
	}
	var oldAttached, newAttached atomic.Int32
	recoveryCtx, stopRecovery := context.WithCancel(context.Background())
	defer stopRecovery()
	oldRecovery := make(chan struct{})
	go func() {
		defer close(oldRecovery)
		RetryFailedSources(recoveryCtx, oldProber, clientsOf("src-b"), []string{"src-b"}, oldHolder.Held,
			func(int) time.Duration { return time.Millisecond }, log,
			func(string, map[string]registry.Discovery) bool {
				oldAttached.Add(1)
				return true
			})
	}()

	deadline := time.Now().Add(5 * time.Second)
	for oldProber.callCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the old holder never entered the recovery retry path")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The coordination store fails: the old holder's renewal fails and it
	// loses the tenure. Its leader loop is stopped so it cannot re-acquire;
	// the recovery loop stays alive and must park on its own.
	mr.SetError("shared state outage")
	waitForTenure(t, oldHolder, false)
	stopOld()
	select {
	case <-oldRun:
	case <-time.After(5 * time.Second):
		t.Fatal("the old holder's leader loop did not stop")
	}
	// A probe from now on would succeed: if the old holder kept the loop, it
	// would attach. The recovery must have parked instead.
	oldProber.succeed.Store(true)
	time.Sleep(200 * time.Millisecond)
	callsAfterLoss := oldProber.callCount()
	time.Sleep(time.Second)
	if got := oldProber.callCount(); got != callsAfterLoss {
		t.Fatalf("old holder probes after losing the lease: %d -> %d, want none", callsAfterLoss, got)
	}
	if got := oldAttached.Load(); got != 0 {
		t.Fatalf("the old holder attached %d time(s) after losing the lease, want none", got)
	}

	// The lease row is gone with the outage; the new holder acquires it.
	mr.Del("foreman:leader")
	mr.SetError("")
	newCtx, stopNew := context.WithCancel(context.Background())
	defer stopNew()
	go func() { _ = newHolder.Run(newCtx) }()
	waitForTenure(t, newHolder, true)

	newRecovery := make(chan struct{})
	go func() {
		defer close(newRecovery)
		RetryFailedSources(recoveryCtx, newProber, clientsOf("src-b"), []string{"src-b"}, newHolder.Held,
			func(int) time.Duration { return time.Millisecond }, log,
			func(string, map[string]registry.Discovery) bool {
				newAttached.Add(1)
				return true
			})
	}()
	select {
	case <-newRecovery:
	case <-time.After(5 * time.Second):
		t.Fatal("the new holder never recovered the source")
	}

	if got := newAttached.Load(); got != 1 {
		t.Fatalf("new holder attaches = %d, want exactly 1", got)
	}
	if got := newProber.callCount(); got != 1 {
		t.Fatalf("new holder probe calls = %d, want 1", got)
	}
}
