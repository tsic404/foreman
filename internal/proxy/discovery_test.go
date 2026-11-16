package proxy_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/proxy"
)

func discoveryClient(t *testing.T, url string) *proxy.Client {
	t.Helper()
	cfg := loopCfg
	cfg.ServerURL = url
	return proxy.NewClient(cfg, "tok-test")
}

func discoveryServer(t *testing.T, body string, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
}

// TestDiscoverWorkspacesAppliesManifest: with no manifest every visible
// workspace is served; with one, only the listed ones are, and the rest are
// reported as excluded rather than silently dropped.
func TestDiscoverWorkspacesAppliesManifest(t *testing.T) {
	srv := discoveryServer(t, `[{"id":"ws-1","name":"One"},{"id":"ws-2","name":"Two"}]`, http.StatusOK)
	defer srv.Close()
	c := discoveryClient(t, srv.URL)

	all, err := c.DiscoverWorkspaces(context.Background(), nil)
	if err != nil {
		t.Fatalf("no manifest: %v", err)
	}
	if len(all.Set) != 2 || len(all.Excluded) != 0 {
		t.Fatalf("no manifest: set=%v excluded=%v", all.Set, all.Excluded)
	}

	one, err := c.DiscoverWorkspaces(context.Background(), []string{"ws-2"})
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(one.Set) != 1 || one.Set[0].ID != "ws-2" {
		t.Fatalf("manifest set = %v, want just ws-2", one.Set)
	}
	if len(one.Excluded) != 1 || one.Excluded[0].ID != "ws-1" {
		t.Fatalf("excluded = %v, want ws-1", one.Excluded)
	}
}

// TestDiscoverWorkspacesRejectsBadAnswers: each malformed C17 answer must be an
// error, not a partially-populated discovery — startup would otherwise serve
// an incomplete workspace set.
func TestDiscoverWorkspacesRejectsBadAnswers(t *testing.T) {
	cases := map[string]struct {
		body   string
		status int
	}{
		"empty list":   {`[]`, http.StatusOK},
		"missing name": {`[{"id":"ws-1"}]`, http.StatusOK},
		"missing id":   {`[{"name":"One"}]`, http.StatusOK},
		"not json":     {`{`, http.StatusOK},
		"unauthorized": {`{"error":"nope"}`, http.StatusUnauthorized},
		"server error": {`{}`, http.StatusInternalServerError},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := discoveryServer(t, tc.body, tc.status)
			defer srv.Close()
			if _, err := discoveryClient(t, srv.URL).DiscoverWorkspaces(context.Background(), nil); err == nil {
				t.Fatalf("accepted a bad C17 answer (%s)", name)
			}
		})
	}
}

// TestDiscoverWorkspacesRejectsUnservableManifest: a manifest naming a
// workspace the credential cannot see is a deployment mistake and must refuse
// startup rather than start with a smaller set.
func TestDiscoverWorkspacesRejectsUnservableManifest(t *testing.T) {
	srv := discoveryServer(t, `[{"id":"ws-1","name":"One"}]`, http.StatusOK)
	defer srv.Close()

	_, err := discoveryClient(t, srv.URL).DiscoverWorkspaces(context.Background(), []string{"ws-9"})
	if err == nil {
		t.Fatal("accepted a manifest workspace the credential cannot see")
	}
	if !strings.Contains(err.Error(), "ws-9") {
		t.Fatalf("error does not name the offending workspace: %v", err)
	}
}

// TestStatusErrorCarriesServerBody: the refusal reason only exists in the
// upstream body, so it must reach the log line.
func TestStatusErrorCarriesServerBody(t *testing.T) {
	srv := discoveryServer(t, `{"error":"daemon_id does not belong to this credential"}`, http.StatusForbidden)
	defer srv.Close()

	_, err := discoveryClient(t, srv.URL).DiscoverWorkspaces(context.Background(), nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("error does not name the status: %v", err)
	}
	if !strings.Contains(err.Error(), "does not belong") {
		t.Fatalf("error drops the upstream body: %v", err)
	}
}

// TestClientSetRejectsDuplicates: two clients for one workspace would shadow
// each other, and which one served a request would be arbitrary.
func TestClientSetRejectsDuplicates(t *testing.T) {
	mk := func(ws string) *proxy.Client {
		cfg := loopCfg
		cfg.WorkspaceID = ws
		cfg.ServerURL = "http://127.0.0.1:1"
		return proxy.NewClient(cfg, "tok")
	}

	if _, err := proxy.NewClientSet(mk("ws-1"), mk("ws-1")); err == nil {
		t.Fatal("accepted two clients for the same workspace")
	}
	if _, err := proxy.NewClientSet(); err == nil {
		t.Fatal("accepted an empty client set")
	}
	cfg := loopCfg
	cfg.WorkspaceID = ""
	cfg.ServerURL = "http://127.0.0.1:1"
	if _, err := proxy.NewClientSet(proxy.NewClient(cfg, "tok")); err == nil {
		t.Fatal("accepted a client with no workspace id")
	}
}

// TestClientSetRoutesByWorkspace: For must return the client serving that
// workspace and nothing else — a wrong route would act on another tenant.
func TestClientSetRoutesByWorkspace(t *testing.T) {
	mk := func(ws string) *proxy.Client {
		cfg := loopCfg
		cfg.WorkspaceID = ws
		cfg.ServerURL = "http://127.0.0.1:1"
		return proxy.NewClient(cfg, "tok")
	}
	set, err := proxy.NewClientSet(mk("ws-1"), mk("ws-2"))
	if err != nil {
		t.Fatalf("build set: %v", err)
	}
	c, ok := set.For("ws-2")
	if !ok || c.WorkspaceID() != "ws-2" {
		t.Fatalf("For(ws-2) = %v,%v", c, ok)
	}
	if _, ok := set.For("ws-3"); ok {
		t.Fatal("For returned a client for an unserved workspace")
	}
	if got := set.IDs(); len(got) != 2 {
		t.Fatalf("IDs = %v, want 2", got)
	}
	if !set.Has("ws-1") {
		t.Fatal("Has(ws-1) = false")
	}
}
