package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// TestWorkspaceServerForwardInWorkspace pins the production replay router
// directly: the persisted workspace selects that workspace's credential, and
// an unknown workspace is an error — never a fallback to another (or the only)
// client (§1.4, §10, AC-86).
func TestWorkspaceServerForwardInWorkspace(t *testing.T) {
	var mu sync.Mutex
	var auths []string
	var paths []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		auths = append(auths, r.Header.Get("Authorization"))
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	t.Cleanup(upstream.Close)

	client := func(workspaceID string) *proxy.Client {
		return proxy.NewClient(proxy.Config{
			ServerURL:   upstream.URL,
			WorkspaceID: workspaceID,
			DaemonID:    "foreman-" + workspaceID,
			Version:     "0.1.0",
		}, "mdt_"+workspaceID)
	}
	set, err := proxy.NewClientSet(client("ws-a"), client("ws-b"))
	if err != nil {
		t.Fatalf("NewClientSet: %v", err)
	}
	server := &workspaceServer{reg: registry.New(time.Now), clients: set}

	code, _, err := server.ForwardInWorkspace(context.Background(), scheduler.EPFail, "task-1", "ws-b", []byte(`{}`))
	if err != nil || code/100 != 2 {
		t.Fatalf("ForwardInWorkspace(ws-b) = %d, %v; want 2xx", code, err)
	}
	mu.Lock()
	if len(auths) != 1 || auths[0] != "Bearer mdt_ws-b" {
		t.Fatalf("requests = %v, want exactly ws-b's credential", auths)
	}
	if paths[0] != "/api/daemon/tasks/task-1/fail" {
		t.Fatalf("path = %q, want the terminal path", paths[0])
	}
	mu.Unlock()

	// An unknown workspace never borrows another configured credential.
	mu.Lock()
	before := len(auths)
	mu.Unlock()
	if _, _, err := server.ForwardInWorkspace(context.Background(), scheduler.EPFail, "task-2", "ws-z", []byte(`{}`)); err == nil {
		t.Fatal("unknown workspace: want an error, got a routed replay")
	}
	mu.Lock()
	if len(auths) != before {
		t.Fatalf("unknown workspace sent %d request(s); want none", len(auths)-before)
	}
	mu.Unlock()

	// Nor does it fall back to the only client when the deployment has one.
	single, err := proxy.NewClientSet(client("ws-a"))
	if err != nil {
		t.Fatalf("NewClientSet(single): %v", err)
	}
	singleServer := &workspaceServer{reg: registry.New(time.Now), clients: single}
	mu.Lock()
	before = len(auths)
	mu.Unlock()
	if _, _, err := singleServer.ForwardInWorkspace(context.Background(), scheduler.EPFail, "task-3", "ws-z", []byte(`{}`)); err == nil {
		t.Fatal("single-client fallback: want an error, got a routed replay")
	}
	mu.Lock()
	if len(auths) != before {
		t.Fatalf("single-client fallback sent %d request(s); want none", len(auths)-before)
	}
	mu.Unlock()
}
