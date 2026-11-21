package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/tsic404/foreman/internal/coordination"
	"github.com/tsic404/foreman/internal/state"
)

// tenureTestStore is the shared coordination store for the takeover test.
func tenureTestStore(t *testing.T) (state.CoordStore, *miniredis.Miniredis) {
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

// registerUpstream answers C1 for every daemon, but the named daemon's C1
// blocks until its request context ends or the test releases it. That makes
// "already inside Preflight when the tenure ends" observable.
func registerUpstream(stub *stubUpstream, blockedDaemon string, entered, cancelled chan<- struct{}, release <-chan struct{}) {
	var enteredOnce, cancelledOnce sync.Once
	stub.on("POST", "/api/tokens/current/renew", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{})
	})
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, r *http.Request, body []byte) {
		var req struct {
			DaemonID string `json:"daemon_id"`
		}
		_ = json.Unmarshal(body, &req)
		if req.DaemonID == blockedDaemon {
			enteredOnce.Do(func() { close(entered) })
			select {
			case <-r.Context().Done():
				cancelledOnce.Do(func() { close(cancelled) })
				return
			case <-release:
			}
			if r.Context().Err() != nil {
				cancelledOnce.Do(func() { close(cancelled) })
				return
			}
		}
		writeJSONStub(w, 200, map[string]any{
			"runtimes": []map[string]string{{
				"id": "rid-1", "name": "foreman-omp", "provider": "omp", "status": "online",
			}},
			"repos":         []map[string]string{{"url": "https://github.com/tsic404/foreman"}},
			"repos_version": "abc123",
			"settings":      nil,
		})
	})
}

// TestRegistrationTakeoverCancelsTheOldHoldersC1 pins R7: the recovered
// plane's preflight runs under the tenure. When the old holder is already
// inside a blocked C1 and loses the lease, its request is cancelled, it never
// publishes a declaration, and only the new holder registers and publishes
// that source.
func TestRegistrationTakeoverCancelsTheOldHoldersC1(t *testing.T) {
	store, mr := tenureTestStore(t)
	leaderCfg := coordination.Config{
		LeaseTTL:       300 * time.Millisecond,
		TakeoverWindow: 100 * time.Millisecond,
		InstanceTTL:    time.Second,
		TaskLockTTL:    time.Second,
	}
	oldLeader := coordination.New(store, "instance-old", leaderCfg)
	newLeader := coordination.New(store, "instance-new", leaderCfg)

	oldCtx, stopOld := context.WithCancel(context.Background())
	defer stopOld()
	oldRun := make(chan struct{})
	go func() { defer close(oldRun); _ = oldLeader.Run(oldCtx) }()
	waitForTenure(t, oldLeader, true)

	srv, stub := newStubUpstream(t)
	entered := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	release := make(chan struct{})
	registerUpstream(stub, "foreman-old", entered, cancelled, release)

	oldCfg := testConfig(srv.URL)
	oldCfg.DaemonID = "foreman-old"
	oldCfg.SourceID = "src-b"
	oldClient := NewClient(oldCfg, "token-old")
	oldClient.Declare(testDiscovery())
	oldPlane := NewClaimLoop(oldCfg, oldClient, &fakeClaimScheduler{})
	oldPlane.SetLeaderGate(oldLeader.Held)
	var oldPublished atomic.Int32
	oldPlane.SetRegisteredHook(func(context.Context, *Client) error {
		oldPublished.Add(1)
		return nil
	})
	oldLoopCtx, stopOldLoop := context.WithCancel(context.Background())
	defer stopOldLoop()
	go func() { _ = oldPlane.Run(oldLoopCtx) }()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the old holder never entered the C1 preflight")
	}

	// The old holder is inside Register now. Break the shared store so its
	// renewal fails and the tenure ends; stop its leader loop so it cannot
	// re-acquire while the test drives the new holder.
	mr.SetError("shared state outage")
	waitForTenure(t, oldLeader, false)
	stopOld()
	select {
	case <-oldRun:
	case <-time.After(5 * time.Second):
		t.Fatal("the old holder's leader loop did not stop")
	}

	aborted := false
	select {
	case <-cancelled:
		aborted = true
	case <-time.After(3 * time.Second):
	}
	// Release any request the missing fence would leave blocked, so the
	// failure is observable as a publish rather than a wedged test.
	close(release)

	// The new holder acquires the lease row the outage removed.
	mr.Del("foreman:leader")
	mr.SetError("")
	newCtx, stopNew := context.WithCancel(context.Background())
	defer stopNew()
	go func() { _ = newLeader.Run(newCtx) }()
	waitForTenure(t, newLeader, true)

	newCfg := testConfig(srv.URL)
	newCfg.DaemonID = "foreman-new"
	newCfg.SourceID = "src-b"
	newClient := NewClient(newCfg, "token-new")
	newClient.Declare(testDiscovery())
	newPlane := NewClaimLoop(newCfg, newClient, &fakeClaimScheduler{})
	newPlane.SetLeaderGate(newLeader.Held)
	var newPublished atomic.Int32
	published := make(chan string, 1)
	newPlane.SetRegisteredHook(func(_ context.Context, cl *Client) error {
		newPublished.Add(1)
		select {
		case published <- cl.ScopeKey():
		default:
		}
		return nil
	})
	newLoopCtx, stopNewLoop := context.WithCancel(context.Background())
	defer stopNewLoop()
	go func() { _ = newPlane.Run(newLoopCtx) }()

	select {
	case scopeKey := <-published:
		if scopeKey != newClient.ScopeKey() {
			t.Fatalf("new holder published scope %q, want %q", scopeKey, newClient.ScopeKey())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the new holder never published the recovery declaration")
	}

	// Give a reverted loop time to finish its uncancelled C1 and publish.
	time.Sleep(2 * leaderPollInterval)
	if got := oldPublished.Load(); got != 0 {
		t.Fatalf("the old holder published %d declaration(s) after losing the lease, want none", got)
	}
	if got := len(oldClient.RuntimeIDs()); got != 0 {
		t.Fatalf("the old holder completed registration with %d runtime(s), want none", got)
	}
	if !aborted {
		t.Fatal("losing the lease did not cancel the in-flight C1")
	}
	if got := len(newClient.RuntimeIDs()); got != 1 {
		t.Fatalf("the new holder registered %d runtime(s), want 1", got)
	}
	if got := newPublished.Load(); got != 1 {
		t.Fatalf("the new holder published %d declaration(s), want exactly 1", got)
	}
}
