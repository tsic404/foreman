package dispatch_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/daemonauth"
	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// stubServer stands in for the multica server: it answers C17/C1/C3 and records
// every forwarded lifecycle report.
type stubServer struct {
	mu       sync.Mutex
	claims   int
	register int
	reports  []recordedReport
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
		s.mu.Lock()
		s.register++
		s.mu.Unlock()
		rts := make([]map[string]any, 0, len(req.Runtimes))
		for _, rt := range req.Runtimes {
			// The server assigns a real runtime id per declared identity.
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

func creds(t *testing.T) *daemonauth.Store {
	t.Helper()
	store, err := daemonauth.Load(func(k string) string {
		switch k {
		case daemonauth.EnvDaemonTokens:
			return "worker-1=tok-1"
		case daemonauth.EnvDaemonTokenWorkspaces:
			return "worker-1=ws-1"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("load daemon creds: %v", err)
	}
	return store
}

// TestEndToEndChain drives the whole v2 flow through the real faces: a daemon
// registers to the fake server, the change hook re-derives Foreman's C1
// declaration, Foreman registers to the server and claims a task, the daemon's
// S6 claim collects it, and the terminal report is forwarded back.
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
	disp := dispatch.New(dispatch.Config{}, reg, client, nil)

	// The daemon face, with the change hook that re-derives the declaration.
	redeclare := func() {
		rts := reg.RuntimesForWorkspace("ws-1")
		declared := make([]proxy.DeclaredRuntime, 0, len(rts))
		for _, rt := range rts {
			declared = append(declared, proxy.DeclaredRuntime{Name: rt.Name, Type: rt.Provider, ProfileID: rt.ProfileID})
		}
		client.SetDeclared(declared)
	}
	fakeServer := proxy.NewFakeServer(creds(t), reg, dispatchAdapter{disp}, proxy.WithDaemonChangeHook(redeclare))
	daemonHTTP := httptest.NewServer(fakeServer)
	defer daemonHTTP.Close()

	// The daemon registers to Foreman.
	registerBody, _ := json.Marshal(map[string]any{
		"daemon_id": "worker-1", "workspace_id": "ws-1",
		"runtimes": []map[string]string{{"name": "worker-1", "type": "omp", "status": "online"}},
	})
	req, _ := http.NewRequest(http.MethodPost, daemonHTTP.URL+"/api/daemon/register", bytes.NewReader(registerBody))
	req.Header.Set("Authorization", "Bearer tok-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("daemon register: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("daemon register status %d", resp.StatusCode)
	}

	// The hook marked the declaration dirty; the claim loop's preflight would
	// register. Do it explicitly so the test drives one round.
	if !client.NeedsRegistration() {
		t.Fatal("daemon registration should dirty the declaration")
	}
	if err := client.Register(context.Background()); err != nil {
		t.Fatalf("client register: %v", err)
	}
	if client.NeedsRegistration() {
		t.Fatal("successful C1 should clear the dirty flag")
	}
	stub.mu.Lock()
	registers := stub.register
	stub.mu.Unlock()
	if registers != 1 {
		t.Fatalf("expected exactly one C1, got %d", registers)
	}

	ctx := context.Background()

	// One claim round: capacity 1 → claim task-1.
	if disp.IdleDaemons() != 1 {
		t.Fatalf("expected 1 idle daemon, got %d", disp.IdleDaemons())
	}
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

	entry, ok := reg.ByDaemonInWorkspace("ws-1", "worker-1")
	if !ok {
		t.Fatalf("task not assigned to worker-1")
	}

	// The daemon's S6 claim picks the payload up.
	delivery, err := disp.Deliver(ctx, "ws-1", "worker-1", []string{"worker-1"}, 1)
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

	stub.mu.Lock()
	reports := append([]recordedReport(nil), stub.reports...)
	stub.mu.Unlock()
	if len(reports) != 2 {
		t.Fatalf("expected 2 forwarded reports, got %d: %+v", len(reports), reports)
	}
	if reports[0].Endpoint != "start" || reports[1].Endpoint != "complete" {
		t.Fatalf("expected start then complete, got %q, %q", reports[0].Endpoint, reports[1].Endpoint)
	}
	if !strings.Contains(reports[1].Body, "sess-1") {
		t.Fatalf("session id missing from forwarded body: %s", reports[1].Body)
	}

	if _, live := reg.Get("task-1"); live {
		t.Fatalf("task-1 should have been reclaimed after terminal")
	}
	if !reg.IsDone("ws-1", "task-1") {
		t.Fatalf("task-1 should be remembered as done")
	}
	rt, _ := reg.RuntimeByID("worker-1")
	if !rt.HoldsSession("sess-1") {
		t.Fatalf("daemon should have learned session sess-1")
	}
	_ = time.Now
}

// dispatchAdapter routes the fake server's single Dispatcher seam to one
// dispatcher.
type dispatchAdapter struct{ d *dispatch.Dispatcher }

func (a dispatchAdapter) Deliver(ctx context.Context, ws, daemonID string, runtimeIDs []string, max int) (dispatch.Delivery, error) {
	return a.d.Deliver(ctx, ws, daemonID, runtimeIDs, max)
}
func (a dispatchAdapter) OnReport(ctx context.Context, ep dispatch.Endpoint, e registry.TaskEntry, body []byte, sessionID string) (int, []byte, error) {
	return a.d.OnReport(ctx, ep, e, body, sessionID)
}
func (a dispatchAdapter) OnTaskVanished(ctx context.Context, taskID string) error { return nil }
func (a dispatchAdapter) OnBootTimeout(ctx context.Context, taskID string) error  { return nil }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
