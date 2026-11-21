package catalog_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/catalog/catalogtest"
)

// scopedAgentServer models the anchored upstream's workspace-scoped agent
// routes: without the workspace identifier every call answers 400, exactly as
// `server/internal/middleware/workspace.go` does.
type scopedAgentServer struct {
	mu       sync.Mutex
	reads    int
	writes   int
	lastBody map[string]string
}

func (s *scopedAgentServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	scoped := r.URL.Path == "/api/agents" || strings.HasPrefix(r.URL.Path, "/api/agents/")
	if scoped && (r.Header.Get("X-Workspace-ID") != "ws-1" || r.URL.Query().Get("workspace_id") != "ws-1") {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"workspace_id or workspace_slug is required"}`))
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/api/daemon/workspaces":
		_, _ = w.Write([]byte(`[{"id":"ws-1","name":"core"}]`))
	case r.Method == http.MethodGet && r.URL.Path == "/api/projects":
		_, _ = w.Write([]byte(`{"projects":[],"total":0}`))
	case r.Method == http.MethodGet && r.URL.Path == "/api/agents":
		_, _ = w.Write([]byte(`[]`))
	case r.Method == http.MethodGet && r.URL.Path == "/api/runtimes":
		_, _ = w.Write([]byte(`[]`))
	case r.Method == http.MethodGet && r.URL.Path == "/api/agents/a1":
		s.mu.Lock()
		s.reads++
		s.mu.Unlock()
		_, _ = w.Write([]byte(`{"id":"a1","workspace_id":"ws-1","runtime_id":"rt-old","runtime_bound":true}`))
	case r.Method == http.MethodPut && r.URL.Path == "/api/agents/a1":
		s.mu.Lock()
		s.writes++
		s.lastBody = map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&s.lastBody)
		s.mu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// TestHTTPUpstreamCarriesTheWorkspaceOnEveryAgentCall pins the production
// failure this fix closes: both the comparison read, the write and the
// read-back name the workspace, and an answer naming another agent or
// workspace is refused instead of projected.
func TestHTTPUpstreamCarriesTheWorkspaceOnEveryAgentCall(t *testing.T) {
	fixture := &scopedAgentServer{}
	server := httptest.NewServer(fixture)
	defer server.Close()

	upstream := catalog.NewHTTPUpstream(catalogtest.Credentials{})
	src := catalog.Source{ID: "atlas", URL: server.URL, CredentialRef: "atlas"}

	agent, found, err := upstream.ReadAgent(context.Background(), src, "ws-1", "a1")
	if err != nil || !found {
		t.Fatalf("ReadAgent = %+v, %v, %v", agent, found, err)
	}
	if agent.ID != "a1" || agent.WorkspaceID != "ws-1" || agent.RuntimeID != "rt-old" {
		t.Fatalf("agent = %+v", agent)
	}
	if err := upstream.SetAgentRuntime(context.Background(), src, "ws-1", "a1", "rt-old", "rt-new"); err != nil {
		t.Fatalf("SetAgentRuntime: %v", err)
	}
	fixture.mu.Lock()
	reads, writes, body := fixture.reads, fixture.writes, fixture.lastBody
	fixture.mu.Unlock()
	if reads != 1 || writes != 1 {
		t.Fatalf("reads/writes = %d/%d, want 1/1", reads, writes)
	}
	if body["runtime_id"] != "rt-new" {
		t.Fatalf("write body = %v", body)
	}

	// The wrong workspace is refused by the upstream and never becomes an
	// agent row.
	if _, _, err := upstream.ReadAgent(context.Background(), src, "ws-9", "a1"); err == nil {
		t.Fatal("ReadAgent accepted a workspace the upstream refused")
	}
	if supported, reason := upstream.BindingWriteSupported(src); supported || reason == "" {
		t.Fatalf("BindingWriteSupported = %v %q, want false with a reason", supported, reason)
	}
}

// TestBindingStaysReadOnlyWithoutAConditionalWrite pins the contract's
// fallback: the pinned upstream offers no conditional agent update, so the
// management surface refuses the write with a reason and sends none — a
// second read can never stand in for an atomic comparison.
func TestBindingStaysReadOnlyWithoutAConditionalWrite(t *testing.T) {
	fixture := &scopedAgentServer{}
	server := httptest.NewServer(fixture)
	defer server.Close()

	upstream := catalog.NewHTTPUpstream(catalogtest.Credentials{})
	service, err := catalog.New(catalog.Config{
		Upstream:    upstream,
		Credentials: catalogtest.Credentials{},
		Clock:       timeAt,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	view, err := service.Register(context.Background(), catalog.Registration{
		ServerID:      "atlas",
		URL:           server.URL,
		CredentialRef: "atlas",
	}, "test")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if view.WriteBack.Enabled || view.WriteBack.Reason == "" {
		t.Fatalf("write back = %+v, want disabled with a reason", view.WriteBack)
	}
	if _, err := service.Sync(context.Background(), "atlas"); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-1", AgentID: "a1",
		ExpectedRuntimeID: "rt-old", RuntimeID: "rt-new",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingUnsupported || outcome.Detail == "" {
		t.Fatalf("outcome = %+v, want unsupported with the capability reason", outcome)
	}
	fixture.mu.Lock()
	writes := fixture.writes
	fixture.mu.Unlock()
	if writes != 0 {
		t.Fatalf("writes = %d, want zero: an unconditional write would risk a competing edit", writes)
	}
}
