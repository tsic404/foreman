package dispatch_test

import (
	"github.com/tsic404/foreman/internal/dispatch"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// stubServer stands in for the multica server: it answers C17/C1/C3 and records
// every forwarded lifecycle report.
type stubServer struct {
	mu      sync.Mutex
	claims  int
	reports []recordedReport
}

type recordedReport struct {
	Endpoint string
	TaskID   string
	Body     string
}

func (s *stubServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/daemon/workspaces", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, []map[string]string{{"id": "ws-1", "name": "default"}})
	})
	mux.HandleFunc("/api/daemon/register", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Runtimes []struct {
				Name string `json:"name"`
				Type string `json:"type"`
			} `json:"runtimes"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		rts := make([]map[string]any, 0, len(req.Runtimes))
		for _, rt := range req.Runtimes {
			rts = append(rts, map[string]any{
				"id": rt.Type + "-real", "name": rt.Name, "provider": rt.Type,
				"status": "online", "profile_id": nil,
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"runtimes": rts, "repos": []any{}, "repos_version": ""})
	})
	mux.HandleFunc("/api/daemon/tasks/claim", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.claims++
		first := s.claims == 1
		s.mu.Unlock()
		tasks := []map[string]any{}
		if first {
			tasks = append(tasks, map[string]any{
				"id": "task-1", "workspace_id": "ws-1",
				"agent": map[string]any{"name": "agent-a"},
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
	})
	// Report endpoints: /api/daemon/tasks/{tid}/{endpoint}
	mux.HandleFunc("/api/daemon/tasks/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/daemon/tasks/"), "/")
		if len(parts) != 2 {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.reports = append(s.reports, recordedReport{TaskID: parts[0], Endpoint: parts[1], Body: string(body)})
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("/api/tokens/current/renew", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	return mux
}

// TestEndToEndChain drives the v2 flow: Foreman registers to the server,
// claims a task, hands it to a registered daemon, and the daemon's terminal
// report is forwarded back to the server.
func TestEndToEndChain(t *testing.T) {
	stub := &stubServer{}
	server := httptest.NewServer(stub.handler())
	defer server.Close()

	reg := registry.New()
	cfg := proxy.Config{
		ServerURL:   server.URL,
		WorkspaceID: "ws-1",
		DaemonID:    "foreman-ws-1",
		Version:     "test",
	}
	client := proxy.NewClient(cfg, "mul_test")
	// Foreman declares the runtime of the daemon registered to it.
	client.SetDeclared([]proxy.DeclaredRuntime{{Name: "worker-1", Type: "omp"}})
	if err := client.Register(context.Background()); err != nil {
		t.Fatalf("client register: %v", err)
	}

	// The daemon is registered to Foreman (in production it dials the fake
	// server; the row is what the dispatcher reads).
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "worker-1", DaemonID: "worker-1", Provider: "omp", Name: "worker-1",
		WorkspaceID: "ws-1", Online: true, RegisteredAt: reg.Now(),
	})

	disp := dispatch.New(dispatch.Config{}, reg, client, nil)
	ctx := context.Background()

	// One claim round: capacity 1 → claim task-1.
	tasks, err := client.ClaimTasks(ctx, disp.IdleDaemons())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 claimed task, got %d", len(tasks))
	}
	if err := disp.OnClaim(ctx, "ws-1", tasks[0]); err != nil {
		t.Fatalf("on-claim: %v", err)
	}

	// The task is assigned to worker-1.
	entry, ok := reg.ByDaemonInWorkspace("ws-1", "worker-1")
	if !ok {
		t.Fatalf("task not assigned to worker-1")
	}

	// The daemon's S6 claim picks the payload up.
	delivery, err := disp.Deliver(ctx, "ws-1", "worker-1", "worker-1", 1)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !delivery.Delivered || len(delivery.Tasks) != 1 {
		t.Fatalf("expected a delivered task, got %+v", delivery)
	}

	// The daemon starts, then completes the task.
	if _, _, err := disp.OnReport(ctx, dispatch.EPStart, entry, delivery.Tasks[0], ""); err != nil {
		t.Fatalf("start report: %v", err)
	}
	body, _ := json.Marshal(map[string]any{"status": "completed", "session_id": "sess-1"})
	if _, _, err := disp.OnReport(ctx, dispatch.EPComplete, entry, body, "sess-1"); err != nil {
		t.Fatalf("complete report: %v", err)
	}

	// Both reports reached the server, in order.
	stub.mu.Lock()
	reports := append([]recordedReport(nil), stub.reports...)
	stub.mu.Unlock()
	if len(reports) != 2 {
		t.Fatalf("expected 2 forwarded reports, got %d: %+v", len(reports), reports)
	}
	if reports[0].Endpoint != "start" {
		t.Fatalf("expected start first, got %q", reports[0].Endpoint)
	}
	if reports[1].Endpoint != "complete" {
		t.Fatalf("expected complete second, got %q", reports[1].Endpoint)
	}
	if !strings.Contains(reports[1].Body, "sess-1") {
		t.Fatalf("session id missing from forwarded body: %s", reports[1].Body)
	}

	// The task settled and was reclaimed: no live entry remains, but the
	// settled index remembers it so a re-dispatch is caught.
	if _, live := reg.Get("task-1"); live {
		t.Fatalf("task-1 should have been reclaimed after terminal")
	}
	if !reg.IsDone("ws-1", "task-1") {
		t.Fatalf("task-1 should be remembered as done")
	}

	// The daemon learned the session for affinity.
	rt, _ := reg.RuntimeByID("worker-1")
	if !rt.HoldsSession("sess-1") {
		t.Fatalf("daemon should have learned session sess-1")
	}
	_ = time.Now
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}