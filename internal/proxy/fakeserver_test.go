package proxy_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/daemonauth"
	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

const testToken = "tok-test"

func newServer(t *testing.T, opts ...proxy.ServerOption) (*proxy.FakeServer, *registry.Registry) {
	t.Helper()
	store, err := daemonauth.Load(func(k string) string {
		switch k {
		case daemonauth.EnvDaemonTokens:
			return "worker-1=" + testToken
		case daemonauth.EnvDaemonTokenWorkspaces:
			return "worker-1=ws-1"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("creds: %v", err)
	}
	reg := registry.New()
	return proxy.NewFakeServer(store, reg, stubDispatcher{}, opts...), reg
}

// stubDispatcher accepts everything so the HTTP layer can be tested alone.
type stubDispatcher struct{}

func (stubDispatcher) Deliver(context.Context, string, string, []string, int) (dispatch.Delivery, error) {
	return dispatch.Delivery{}, nil
}
func (stubDispatcher) OnReport(context.Context, dispatch.Endpoint, registry.TaskEntry, []byte, string) (int, []byte, error) {
	return http.StatusOK, []byte(`{}`), nil
}
func (stubDispatcher) OnTaskVanished(context.Context, string) error { return nil }
func (stubDispatcher) OnBootTimeout(context.Context, string) error  { return nil }

func do(t *testing.T, s *proxy.FakeServer, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

// Every route on the daemon face must fail closed without a valid credential.
func TestDaemonFaceFailsClosed(t *testing.T) {
	s, _ := newServer(t)
	paths := []struct{ method, path string }{
		{http.MethodGet, "/api/daemon/workspaces"},
		{http.MethodGet, "/api/workspaces"},
		{http.MethodPost, "/api/daemon/register"},
		{http.MethodPost, "/api/daemon/heartbeat"},
		{http.MethodPost, "/api/daemon/tasks/claim"},
		{http.MethodGet, "/api/daemon/tasks/t1/status"},
		{http.MethodPost, "/api/daemon/tasks/t1/start"},
		{http.MethodPost, "/api/daemon/tasks/t1/complete"},
		{http.MethodPost, "/api/daemon/runtimes/r1/tasks/t1/prepare-lease"},
		{http.MethodGet, "/api/daemon/workspaces/ws-1/repos"},
	}
	for _, p := range paths {
		if w := do(t, s, p.method, p.path, "", ""); w.Code != http.StatusUnauthorized {
			t.Errorf("%s %s without credential = %d, want 401", p.method, p.path, w.Code)
		}
		if w := do(t, s, p.method, p.path, "wrong", ""); w.Code != http.StatusUnauthorized {
			t.Errorf("%s %s with bad credential = %d, want 401", p.method, p.path, w.Code)
		}
	}
}

// A report for a task bound to another daemon must be refused, not forwarded.
func TestReportForForeignDaemonIsRefused(t *testing.T) {
	s, reg := newServer(t)
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "worker-1", DaemonID: "worker-1", WorkspaceID: "ws-1", Provider: "omp",
		Online: true, RegisteredAt: reg.Now(),
	})
	if err := reg.Put(registry.TaskEntry{
		TaskID: "t-foreign", WorkspaceID: "ws-1", DaemonID: "someone-else",
		DaemonRuntimeID: "someone-else", State: registry.StateRunning, ClaimedAt: reg.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if w := do(t, s, http.MethodPost, "/api/daemon/tasks/t-foreign/complete", testToken, "{}"); w.Code != http.StatusNotFound {
		t.Fatalf("foreign report = %d, want 404", w.Code)
	}
}

// A report for a task Foreman has no row for still forwards: a Foreman restart
// empties the registry while daemons keep running, and dropping those reports
// strands the task on the server forever.
func TestReportForUnboundTaskIsForwarded(t *testing.T) {
	s, _ := newServer(t)
	if w := do(t, s, http.MethodPost, "/api/daemon/tasks/t-unseen/complete", testToken, "{}"); w.Code != http.StatusOK {
		t.Fatalf("unbound report = %d, want 200", w.Code)
	}
}

// Without a resolver seam S-26 answers the static empty set: the daemon then
// falls back to its own skill cache. That is a deliberate v2 shape, not a gap.
func TestSkillBundlesWithoutSeamAnswersEmptySet(t *testing.T) {
	s, _ := newServer(t)
	w := do(t, s, http.MethodPost, "/api/daemon/runtimes/r1/tasks/t1/skill-bundles/resolve", testToken, "{}")
	if w.Code != http.StatusOK {
		t.Fatalf("skill bundle = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"skills":[]`) {
		t.Fatalf("body = %s, want an empty skill set", w.Body.String())
	}
}

// With a resolver seam, an unbound task must answer 404 (nothing to resolve
// against), not 502 — a 502 reads as a gateway failure and invites retries for
// a task that no longer exists.
func TestSkillBundlesUnboundWithSeamIsNotFound(t *testing.T) {
	s, _ := newServer(t, proxy.WithSkillResolver(func(context.Context, string, []byte) (int, []byte, error) {
		return 0, nil, fmt.Errorf("%w: t-unseen", proxy.ErrUnboundTask)
	}))
	if w := do(t, s, http.MethodPost, "/api/daemon/runtimes/r1/tasks/t-unseen/skill-bundles/resolve", testToken, "{}"); w.Code != http.StatusNotFound {
		t.Fatalf("unbound skill bundle = %d, want 404", w.Code)
	}
}

// The static endpoints answer without any upstream seam.
func TestStaticEndpoints(t *testing.T) {
	s, _ := newServer(t)
	cases := []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/api/daemon/workspaces", http.StatusOK},
		{http.MethodGet, "/api/workspaces", http.StatusOK},
		{http.MethodGet, "/api/daemon/workspaces/ws-1/repos", http.StatusOK},
		{http.MethodGet, "/api/daemon/workspaces/ws-1/runtime-profiles", http.StatusOK},
		{http.MethodPost, "/api/tokens/current/renew", http.StatusOK},
		{http.MethodPost, "/api/daemon/runtimes/r1/recover-orphans", http.StatusOK},
		{http.MethodGet, "/not-api", http.StatusNotFound},
	}
	for _, c := range cases {
		if w := do(t, s, c.method, c.path, testToken, ""); w.Code != c.want {
			t.Errorf("%s %s = %d, want %d", c.method, c.path, w.Code, c.want)
		}
	}
}

// Register answers one row per declared runtime and keeps the keys URL-safe:
// the daemon embeds them in runtime-scoped paths.
func TestRegisterReturnsOneRowPerRuntime(t *testing.T) {
	s, _ := newServer(t)
	body, _ := json.Marshal(map[string]any{
		"daemon_id": "worker-1", "workspace_id": "ws-1", "device_name": "host",
		"runtimes": []map[string]string{
			{"name": "a", "type": "omp", "status": "online"},
			{"name": "b", "type": "omp", "status": "online", "profile_id": "prof-1"},
		},
	})
	w := do(t, s, http.MethodPost, "/api/daemon/register", testToken, string(body))
	if w.Code != http.StatusOK {
		t.Fatalf("register = %d", w.Code)
	}
	var resp struct {
		Runtimes []struct {
			ID string `json:"id"`
		} `json:"runtimes"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Runtimes) != 2 {
		t.Fatalf("runtimes = %d, want 2", len(resp.Runtimes))
	}
	for _, rt := range resp.Runtimes {
		if strings.Contains(rt.ID, "/") {
			t.Fatalf("runtime key %q contains a slash; it would split runtime-scoped paths", rt.ID)
		}
	}
}
