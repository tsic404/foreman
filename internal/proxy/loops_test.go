package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/scheduler"
)

// fakeClaimScheduler records claimed tasks and reports a fixed budget.
type fakeClaimScheduler struct {
	mu         sync.Mutex
	budget     int
	claimed    []json.RawMessage
	onClaimErr error
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
	return f.onClaimErr
}

func (f *fakeClaimScheduler) claimedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.claimed)
}

func TestBackoffProgression(t *testing.T) {
	b := newBackoff()
	// §1.5: exponential 1s→30s ceiling for the control-plane loops.
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

func TestClaimLoopDoesNotLogUnnameableClaims(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/daemon/tasks/claim", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{"tasks": []json.RawMessage{
			json.RawMessage(`"unreadable"`), json.RawMessage(`[1]`),
		}})
	})
	cfg := testConfig(srv.URL)
	client := NewClient(cfg, "mdt_test")
	registerDeclared(t, client)
	loop := NewClaimLoop(cfg, client, &fakeClaimScheduler{
		budget:     2,
		onClaimErr: fmt.Errorf("%w: unreadable", scheduler.ErrUnnameableClaim),
	})
	logs := &lockedBuffer{}
	loop.log = slog.New(slog.NewJSONHandler(logs, nil))

	if err := loop.iterate(t.Context()); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if got := strings.Count(logs.String(), "on-claim failed"); got != 0 {
		t.Fatalf("on-claim failed lines = %d, want none for the sentinel (the scheduler logs at a bounded rate): %s", got, logs.String())
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
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	loop := NewClaimLoop(testConfig(srv.URL), client, sched)

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
	client := NewClient(cfg, "mdt_test")
	registerDeclared(t, client)
	loop := NewClaimLoop(cfg, client, sched)

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
	client := NewClient(testConfig(srv.URL), "mdt_test")
	// The startup probe declared the runtime set before the loops start; the
	// loop's preflight therefore re-declares it (and meets the 403).
	client.disc = testDiscovery()
	client.refs = declaredRefs(testDiscovery())
	loop := NewClaimLoop(testConfig(srv.URL), client, sched)

	// §1.5: 403 is a config error — no blind retry; the loop
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
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	loop := NewHeartbeatLoop(client, nil)

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
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	loop := NewHeartbeatLoop(client, nil)

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

// TestClaimLoopRetriesTransientPreflight pins the startup C1's §1.5 behavior: a
// transient failure backs off and retries instead of exiting the process.
func TestClaimLoopRetriesTransientPreflight(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 503, map[string]string{"error": "boom"})
	})
	client := NewClient(testConfig(srv.URL), "mdt_test")
	client.disc = testDiscovery()
	client.refs = declaredRefs(testDiscovery())
	client.terminalWaits = []time.Duration{0}
	loop := NewClaimLoop(testConfig(srv.URL), client, &fakeClaimScheduler{})
	loop.backoff = &backoff{min: time.Millisecond, max: time.Millisecond}

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if err := loop.Run(ctx); err != nil {
		t.Fatalf("Run = %v, want the loop to keep retrying until ctx ends", err)
	}
	if got := len(stub.calls("POST", "/api/daemon/register")); got < 2 {
		t.Fatalf("C1 attempts = %d, want the preflight to retry a 5xx", got)
	}
}

// TestHeartbeatAckConsumesUnderTheStreamsRuntime pins the production failure:
// the slot key and the C21 {rid} are the heartbeat stream's runtime. The ack
// body's runtime_id is advisory — the real server's ack does not reliably echo
// it — so the stream runtime's cached slot must answer the command whatever the
// body says, and no probe may be started.
func TestHeartbeatAckConsumesUnderTheStreamsRuntime(t *testing.T) {
	cases := []struct {
		name  string
		echo  string
		shape func(w http.ResponseWriter)
	}{
		{"echoed runtime_id", "rid-1", func(w http.ResponseWriter) {
			writeJSONStub(w, 200, map[string]any{
				"status": "ok", "runtime_id": "rid-1",
				"pending_model_list": map[string]string{"id": "req-cmd"},
			})
		}},
		{"absent runtime_id", "", func(w http.ResponseWriter) {
			writeJSONStub(w, 200, map[string]any{
				"status":             "ok",
				"pending_model_list": map[string]string{"id": "req-cmd"},
			})
		}},
		{"foreign runtime_id", "rid-other", func(w http.ResponseWriter) {
			writeJSONStub(w, 200, map[string]any{
				"status": "ok", "runtime_id": "rid-other",
				"pending_model_list": map[string]string{"id": "req-cmd"},
			})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, stub := newStubUpstream(t)
			stubRegister(stub, "rid-1")
			stub.on("POST", "/api/daemon/heartbeat", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
				tc.shape(w)
			})
			body := []byte(`{"status":"completed","models":[{"id":"m1"}]}`)
			stub.on("POST", "/api/daemon/runtimes/rid-1/models/req-cmd/result", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
				writeJSONStub(w, 200, map[string]string{"status": "ok"})
			})
			cfg := testConfig(srv.URL)
			client := NewClient(cfg, "mdt_test")
			registerDeclared(t, client)
			slots := NewCatalogSlots()
			slots.Put("rid-1", body, time.Now())
			probes := &fakeProbe{}
			reporter := NewModelCatalogReporter(cfg, client, probes, slots, &fakeMetrics{})

			// The mismatch line is a registered event (§1.3/
			// §9.2 model.catalog_ack_mismatch): exactly one when
			// the ack echoes a foreign runtime id, none otherwise.
			logs := &lockedBuffer{}
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
			t.Cleanup(func() { slog.SetDefault(prev) })

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			go func() { _ = NewHeartbeatLoop(client, reporter).Run(ctx) }()

			const path = "/api/daemon/runtimes/rid-1/models/req-cmd/result"
			deadline := time.Now().Add(5 * time.Second)
			for len(stub.calls("POST", path)) == 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			relayed := stub.calls("POST", path)
			if len(relayed) != 1 {
				t.Fatalf("C21 calls = %d, want the stream runtime's cached body replayed once", len(relayed))
			}
			if string(relayed[0].Body) != string(body) {
				t.Errorf("C21 body = %s, want the cached bytes verbatim", relayed[0].Body)
			}
			if probes.startCount() != 0 {
				t.Error("a cached slot must never start a probe")
			}
			// The mismatch line is a registered event (§1.3/
			// §9.2 model.catalog_ack_mismatch): one when the echo is
			// missing, empty or foreign (§1.3's three forms), none when it
			// agrees.
			records := logRecords(t, []byte(logs.String()), "model.catalog_ack_mismatch")
			if tc.echo == "rid-1" {
				if len(records) != 0 {
					t.Errorf("model.catalog_ack_mismatch records = %d, want none when the echo agrees", len(records))
				}
				return
			}
			if len(records) != 1 {
				t.Fatalf("model.catalog_ack_mismatch records = %d, want one for a missing/foreign echo", len(records))
			}
			for field, want := range map[string]string{"request_id": "req-cmd", "runtime_id": "rid-1", "ack_runtime_id": tc.echo} {
				if got := records[0][field]; got != want {
					t.Errorf("model.catalog_ack_mismatch %s = %v, want %q", field, got, want)
				}
			}
		})
	}
}
