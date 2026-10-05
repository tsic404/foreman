package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

// fakeClaimScheduler records claimed tasks and reports a fixed budget.
type fakeClaimScheduler struct {
	mu      sync.Mutex
	budget  int
	claimed []json.RawMessage
}

func (f *fakeClaimScheduler) ClaimBudget() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.budget
}

func (f *fakeClaimScheduler) OnClaim(_ context.Context, task json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimed = append(f.claimed, task)
	return nil
}

func (f *fakeClaimScheduler) claimedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.claimed)
}

func TestBackoffProgression(t *testing.T) {
	b := newBackoff()
	// §4: exponential 1s→30s ceiling for the control-plane loops.
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for i, w := range want {
		if got := b.next(); got != w {
			t.Errorf("step %d = %s, want %s", i, got, w)
		}
	}
	b.reset()
	if got := b.next(); got != time.Second {
		t.Errorf("after reset = %s, want 1s", got)
	}
}

func TestClaimLoopIterateClaimsUpToBudget(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/daemon/tasks/claim", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{"tasks": []json.RawMessage{
			json.RawMessage(`{"id":"t1"}`), json.RawMessage(`{"id":"t2"}`),
		}})
	})
	sched := &fakeClaimScheduler{budget: 2}
	loop := NewClaimLoop(testConfig(srv.URL), NewClient(testConfig(srv.URL), "mdt_test"), sched)

	loop.iterate(t.Context())
	if sched.claimedCount() != 2 {
		t.Fatalf("claimed = %d, want 2", sched.claimedCount())
	}
	calls := stub.calls("POST", "/api/daemon/tasks/claim")
	var body map[string]any
	_ = json.Unmarshal(calls[0].Body, &body)
	if body["max_tasks"] != float64(2) {
		t.Errorf("max_tasks = %v, want the budget", body["max_tasks"])
	}
}

func TestClaimLoopIterateSkipsWhenNoBudget(t *testing.T) {
	srv, stub := newStubUpstream(t)
	sched := &fakeClaimScheduler{budget: 0}
	loop := NewClaimLoop(testConfig(srv.URL), NewClient(testConfig(srv.URL), "mdt_test"), sched)

	loop.iterate(t.Context())
	if got := len(stub.calls("POST", "/api/daemon/tasks/claim")); got != 0 {
		t.Errorf("no claim call allowed at budget 0, got %d", got)
	}
}

func TestClaimLoopWakeCoalesces(t *testing.T) {
	loop := NewClaimLoop(Config{}, nil, nil)
	loop.Wake()
	loop.Wake()
	// The cap-1 signal coalesces: one pending wakeup, the second is a no-op.
	select {
	case <-loop.wake:
	default:
		t.Fatal("wake signal lost")
	}
	select {
	case <-loop.wake:
		t.Fatal("wake signal must coalesce to one")
	default:
	}
}

func TestClaimLoopWakeTriggersImmediateIteration(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("GET", "/api/daemon/workspaces", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, []map[string]string{{"id": "ws-1", "name": "ws"}})
	})
	stub.on("POST", "/api/tokens/current/renew", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{"renewed": true})
	})
	stub.on("POST", "/api/daemon/tasks/claim", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{"tasks": []json.RawMessage{json.RawMessage(`{"id":"t-wake"}`)}})
	})
	sched := &fakeClaimScheduler{budget: 1}
	cfg := testConfig(srv.URL)
	cfg.ClaimInterval = time.Hour // polling alone must not fire within the test
	cfg.ClaimIntervalIdle = time.Hour
	loop := NewClaimLoop(cfg, NewClient(cfg, "mdt_test"), sched)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = loop.Run(ctx); close(done) }()

	// The startup iteration claims once; the wake claims again without
	// waiting for the ticker.
	deadline := time.Now().Add(5 * time.Second)
	for sched.claimedCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if sched.claimedCount() != 1 {
		t.Fatalf("startup claim count = %d", sched.claimedCount())
	}
	loop.Wake()
	deadline = time.Now().Add(5 * time.Second)
	for sched.claimedCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if sched.claimedCount() != 2 {
		t.Fatalf("wake claim count = %d, want 2", sched.claimedCount())
	}
	cancel()
	<-done
}

func TestClaimLoopIntervalSelection(t *testing.T) {
	cfg := testConfig("http://unused")
	cfg.ClaimInterval = 5 * time.Second
	cfg.ClaimIntervalIdle = 30 * time.Second
	sched := &fakeClaimScheduler{budget: 0}
	loop := NewClaimLoop(cfg, nil, sched)

	if got := loop.interval(); got != 30*time.Second {
		t.Errorf("no free slot: interval = %s, want idle 30s", got)
	}
	sched.budget = 3
	if got := loop.interval(); got != 5*time.Second {
		t.Errorf("free slots: interval = %s, want active 5s", got)
	}
}

func TestClaimLoopStopsOn403(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(403)
		_, _ = w.Write([]byte(`{"error":"token does not match daemon_id"}`))
	})
	sched := &fakeClaimScheduler{budget: 1}
	loop := NewClaimLoop(testConfig(srv.URL), NewClient(testConfig(srv.URL), "mdt_test"), sched)

	// §1.1 failure table: 403 is a config error — no blind retry; the loop
	// exits (CrashLoopBackOff is the alarm surface).
	done := make(chan error, 1)
	go func() { done <- loop.Run(t.Context()) }()
	select {
	case err := <-done:
		var se *StatusError
		if !errors.As(err, &se) || se.Code != 403 {
			t.Fatalf("Run error = %v, want 403 StatusError", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run must stop on 403 instead of retrying")
	}
	if got := len(stub.calls("POST", "/api/daemon/register")); got != 1 {
		t.Errorf("register calls = %d, want exactly 1 (no blind retry)", got)
	}
}

func TestHeartbeatLoopStopsOn403(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/daemon/heartbeat", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(403)
		_, _ = w.Write([]byte(`{"error":"forbidden"}`))
	})
	loop := NewHeartbeatLoop(NewClient(testConfig(srv.URL), "mdt_test"))

	done := make(chan error, 1)
	go func() { done <- loop.Run(t.Context()) }()
	select {
	case err := <-done:
		var se *StatusError
		if !errors.As(err, &se) || se.Code != 403 {
			t.Fatalf("Run error = %v, want 403 StatusError", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("heartbeat loop must stop on 403")
	}
}

func TestHeartbeatLoopHeartbeatsImmediately(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/daemon/heartbeat", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "ok"})
	})
	loop := NewHeartbeatLoop(NewClient(testConfig(srv.URL), "mdt_test"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = loop.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for len(stub.calls("POST", "/api/daemon/heartbeat")) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(stub.calls("POST", "/api/daemon/heartbeat")); got == 0 {
		t.Fatal("no heartbeat observed")
	}
}
