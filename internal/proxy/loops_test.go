package proxy_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
)

// recordingClaimer records what the loop hands it.
type recordingClaimer struct {
	got  chan json.RawMessage
	fail error
}

func (r *recordingClaimer) OnClaim(_ context.Context, _ string, task json.RawMessage) error {
	if r.got != nil {
		select {
		case r.got <- task:
		default:
		}
	}
	return r.fail
}

// claimServer answers C-3 (claim) and C-1 (register) plus the lease/renew
// endpoints the preflight touches, so a ClaimLoop can run against it.
func claimServer(t *testing.T, tasks []map[string]any, claims *int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/daemon/register":
			// C1 answers one row per declared runtime; the loop then claims
			// against the assigned id.
			io.WriteString(w, `{"runtimes":[{"id":"srv-rt-1","name":"daemon-1","provider":"omp","status":"online"}]}`)
		case "/api/tokens/current/renew", "/api/daemon/heartbeat":
			io.WriteString(w, `{}`)
		case "/api/daemon/tasks/claim":
			n := atomic.AddInt32(claims, 1)
			if n == 1 && len(tasks) > 0 {
				_ = json.NewEncoder(w).Encode(map[string]any{"tasks": tasks})
				return
			}
			io.WriteString(w, `{"tasks":[]}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
}

var loopCfg = proxy.Config{
	WorkspaceID:          "ws-1",
	DaemonID:             "daemon-1",
	Version:              "test",
	ClaimInterval:        10 * time.Millisecond,
	ClaimIntervalIdle:    10 * time.Millisecond,
	LeaseRefreshInterval: time.Second,
}

// newLoopClient builds a client whose declaration is already set, so the
// preflight registers once and the loop then claims.
func newLoopClient(t *testing.T, url string) *proxy.Client {
	t.Helper()
	cfg := loopCfg
	cfg.ServerURL = url
	c := proxy.NewClient(cfg, "tok-test")
	c.SetDeclared([]proxy.DeclaredRuntime{{Name: "daemon-1", Type: "omp", Version: "1.0.0", RuntimeKey: "rt-1"}})
	return c
}

// TestClaimLoopDeliversClaimedTasks is the loop's whole job: a claimed task
// must reach the dispatcher. A regression here stops all work silently.
func TestClaimLoopDeliversClaimedTasks(t *testing.T) {
	var claims int32
	srv := claimServer(t, []map[string]any{{"id": "task-a"}, {"id": "task-b"}}, &claims)
	defer srv.Close()

	c := newLoopClient(t, srv.URL)
	rec := &recordingClaimer{got: make(chan json.RawMessage, 4)}

	loop := proxy.NewClaimLoop(loopCfg, c, rec, func() int { return 4 })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go loop.Run(ctx)

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case raw := <-rec.got:
			var task struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(raw, &task); err != nil {
				t.Fatalf("unmarshal task: %v", err)
			}
			seen[task.ID] = true
		case <-ctx.Done():
			t.Fatalf("claimed tasks not delivered to the dispatcher; saw %v", seen)
		}
	}
}

// TestClaimLoopIdlesAtZeroCapacity: no free slot must mean no claim request at
// all, not a claim answered with nothing.
func TestClaimLoopIdlesAtZeroCapacity(t *testing.T) {
	var claims int32
	srv := claimServer(t, nil, &claims)
	defer srv.Close()

	c := newLoopClient(t, srv.URL)
	loop := proxy.NewClaimLoop(loopCfg, c, &recordingClaimer{}, func() int { return 0 })

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	loop.Run(ctx)

	if n := atomic.LoadInt32(&claims); n != 0 {
		t.Fatalf("claim requests = %d at zero capacity, want 0", n)
	}
}

// TestClaimLoopRenewsTokenOnceWithinInterval covers the throttle: preflight
// runs every round, but the token renewal must not follow that cadence.
func TestClaimLoopRenewsTokenOnceWithinInterval(t *testing.T) {
	var renews int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/tokens/current/renew" {
			atomic.AddInt32(&renews, 1)
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/daemon/register" {
			io.WriteString(w, `{"runtimes":[{"id":"srv-rt-1","name":"daemon-1","provider":"omp","status":"online"}]}`)
			return
		}
		io.WriteString(w, `{"tasks":[]}`)
	}))
	defer srv.Close()

	c := newLoopClient(t, srv.URL)
	loop := proxy.NewClaimLoop(loopCfg, c, &recordingClaimer{}, func() int { return 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	loop.Run(ctx)

	if n := atomic.LoadInt32(&renews); n > 1 {
		t.Fatalf("token renewed %d times in one run; the 5-minute interval must hold it to 1", n)
	}
}
