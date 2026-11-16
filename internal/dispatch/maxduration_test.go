package dispatch_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// maxDurationServer records how many times C-13 (task status) is read.
type maxDurationServer struct {
	mu     sync.Mutex
	reads  int
	status string
}

func (s *maxDurationServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/daemon/tasks/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/status") {
			http.NotFound(w, r)
			return
		}
		s.mu.Lock()
		s.reads++
		st := s.status
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": st})
	})
	return mux
}

func maxDurationRig(t *testing.T, srvURL string, max time.Duration) (*dispatch.Dispatcher, *registry.Registry) {
	t.Helper()
	reg := registry.New()
	client := proxy.NewClient(proxy.Config{
		ServerURL: srvURL, WorkspaceID: "ws-1", DaemonID: "foreman-ws-1", Version: "test",
	}, "tok")
	disp := dispatch.New(dispatch.Config{TaskMaxDuration: max}, reg, client, nil)

	reg.PutRuntime(registry.Runtime{
		RuntimeID: "worker-1", DaemonID: "worker-1", WorkspaceID: "ws-1",
		Provider: "omp", Name: "worker-1", Online: true,
	})
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-m", WorkspaceID: "ws-1", DaemonID: "worker-1",
		DaemonRuntimeID: "worker-1", RuntimeID: "srv-rt-1",
		State: registry.StateRunning, ClaimedAt: reg.Now(), StartedAt: reg.Now().Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	return disp, reg
}

// TestTaskMaxDurationTriggersServerStatusRead is the wiring proof: a running
// task past its max duration makes the reconcile round consult the server.
// Before the env key existed this branch was unreachable — the config field
// was read but nothing could ever set it.
func TestTaskMaxDurationTriggersServerStatusRead(t *testing.T) {
	stub := &maxDurationServer{status: "running"}
	server := httptest.NewServer(stub.handler())
	defer server.Close()

	disp, _ := maxDurationRig(t, server.URL, time.Minute)
	if err := disp.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.reads == 0 {
		t.Fatal("a task past its max duration never triggered a server status read")
	}
}

// TestTaskMaxDurationUnsetSkipsStatusRead: the default (no bound) must not
// turn every reconcile round into a status poll for healthy tasks.
func TestTaskMaxDurationUnsetSkipsStatusRead(t *testing.T) {
	stub := &maxDurationServer{status: "running"}
	server := httptest.NewServer(stub.handler())
	defer server.Close()

	disp, _ := maxDurationRig(t, server.URL, 0)
	if err := disp.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.reads != 0 {
		t.Fatalf("status reads = %d with no bound, want 0", stub.reads)
	}
}

// TestTaskMaxDurationYoungTaskIsNotPolled: the bound is measured from start;
// a task inside its window must not be consulted either.
func TestTaskMaxDurationYoungTaskIsNotPolled(t *testing.T) {
	stub := &maxDurationServer{status: "running"}
	server := httptest.NewServer(stub.handler())
	defer server.Close()

	reg := registry.New()
	client := proxy.NewClient(proxy.Config{
		ServerURL: server.URL, WorkspaceID: "ws-1", DaemonID: "foreman-ws-1", Version: "test",
	}, "tok")
	disp := dispatch.New(dispatch.Config{TaskMaxDuration: time.Hour}, reg, client, nil)
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "worker-1", DaemonID: "worker-1", WorkspaceID: "ws-1",
		Provider: "omp", Name: "worker-1", Online: true,
	})
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-young", WorkspaceID: "ws-1", DaemonID: "worker-1",
		DaemonRuntimeID: "worker-1", RuntimeID: "srv-rt-1",
		State: registry.StateRunning, ClaimedAt: reg.Now(), StartedAt: reg.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	if err := disp.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.reads != 0 {
		t.Fatalf("status reads = %d for a young task, want 0", stub.reads)
	}
}
