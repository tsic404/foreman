// TC-tech-multiws-06 implements AC-86: the credential face is per workspace —
// configuration errors refuse startup, a runtime credential failure is
// confined to its workspace, and no request borrows another workspace's
// credential.
package acceptance

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/workspace"
)

func TestTechMultiWS06CredentialFace(t *testing.T) {
	t.Run("configuration errors refuse startup", func(t *testing.T) {
		env := map[string]string{
			"FOREMAN_WORKSPACES": `[
				{"workspace_id":"ws-a","daemon_id":"foreman-a","token":"mdt_a"},
				{"workspace_id":"ws-b","daemon_id":"foreman-b","token_env":"TOKEN_B"}
			]`,
			"TOKEN_B": "mdt_b",
		}
		set, err := workspace.Load(func(key string) string { return env[key] })
		if err != nil {
			t.Fatalf("valid set refused: %v", err)
		}
		if set.Len() != 2 || set.IDs()[0] != "ws-a" || set.IDs()[1] != "ws-b" {
			t.Fatalf("set = %v, want ws-a then ws-b", set.IDs())
		}
		identity, ok := set.Get("ws-b")
		if !ok || identity.Token != "mdt_b" || identity.DaemonID != "foreman-b" {
			t.Fatalf("ws-b identity = %+v, want the token_env credential", identity)
		}

		for name, raw := range map[string]string{
			"duplicate workspace":       `[{"workspace_id":"ws","daemon_id":"a","token":"mdt_a"},{"workspace_id":"ws","daemon_id":"b","token":"mdt_b"}]`,
			"shared daemon identity":    `[{"workspace_id":"a","daemon_id":"same","token":"mdt_a"},{"workspace_id":"b","daemon_id":"same","token":"mdt_b"}]`,
			"missing credential":        `[{"workspace_id":"a","daemon_id":"a","token":""}]`,
			"malformed credential form": `[{"workspace_id":"a","daemon_id":"a","token":"nope"}]`,
		} {
			if _, err := workspace.Load(func(key string) string {
				if key == "FOREMAN_WORKSPACES" {
					return raw
				}
				return ""
			}); err == nil {
				t.Errorf("%s: Load accepted an invalid set", name)
			}
		}
	})

	t.Run("one workspace's credential failure stays there", func(t *testing.T) {
		var mu sync.Mutex
		var authHeaders []string
		failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			authHeaders = append(authHeaders, r.Header.Get("Authorization"))
			mu.Unlock()
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		}))
		t.Cleanup(failing.Close)

		m := newMultiWSWithURLs(t, []string{"ws-a", "ws-b"}, map[string]string{"ws-a": failing.URL})

		errA := m.clients["ws-a"].Workspaces(context.Background())
		var statusErr *proxy.StatusError
		if !errors.As(errA, &statusErr) || statusErr.Code != http.StatusForbidden {
			t.Fatalf("A C-17 error = %v, want a 403 StatusError", errA)
		}
		if !proxy.IsCredentialConfigError(errA) {
			t.Error("a 403 credential rejection must classify as a startup configuration error")
		}
		// B's control plane is untouched: its own C-17 succeeds and its
		// registration (C-1) still works.
		if err := m.clients["ws-b"].Workspaces(context.Background()); err != nil {
			t.Errorf("B C-17 failed while A's credential was rejected: %v", err)
		}
		m.upstream["ws-b"].setRuntimeID("omp", "", "rid-b")
		if err := m.clients["ws-b"].RegisterWithDiscovery(context.Background(), registry.Discovery{
			CLIVersion: "0.6.1",
			Runtimes:   []registry.DiscoveredRuntime{{Name: "foreman-b", Type: "omp", Version: "0.6.1"}},
		}); err != nil {
			t.Errorf("B C-1 failed while A's credential was rejected: %v", err)
		}

		mu.Lock()
		defer mu.Unlock()
		if len(authHeaders) == 0 {
			t.Fatal("the failing upstream saw no request")
		}
		for _, header := range authHeaders {
			if header != "Bearer mdt_ws-a" {
				t.Errorf("A's request carried %q, want its own credential", header)
			}
			if header == "Bearer mdt_ws-b" {
				t.Errorf("A silently used B's credential")
			}
		}
		// B's stub saw only B's credential.
		for _, req := range m.upstream["ws-b"].all() {
			if got := req.Header.Get("Authorization"); got != "Bearer mdt_ws-b" {
				t.Errorf("B request %s carried %q, want B's credential", req.Path, got)
			}
		}
	})

	t.Run("workspace mismatch is a configuration error", func(t *testing.T) {
		mismatch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/daemon/workspaces" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"ws-b","name":"B"}]`))
		}))
		t.Cleanup(mismatch.Close)

		m := newMultiWSWithURLs(t, []string{"ws-a", "ws-b"}, map[string]string{"ws-a": mismatch.URL})
		err := m.clients["ws-a"].Workspaces(context.Background())
		var mismatchErr *proxy.WorkspaceMismatchError
		if !errors.As(err, &mismatchErr) {
			t.Fatalf("A C-17 error = %v, want a WorkspaceMismatchError", err)
		}
		if mismatchErr.Configured != "ws-a" || len(mismatchErr.Visible) != 1 || mismatchErr.Visible[0] != "ws-b" {
			t.Fatalf("mismatch = %+v, want configured ws-a / visible [ws-b]", mismatchErr)
		}
		var statusErr *proxy.StatusError
		if errors.As(err, &statusErr) {
			t.Error("a 200 C-17 answer naming another workspace must not look like a StatusError")
		}
		if !proxy.IsCredentialConfigError(err) {
			t.Error("the workspace mismatch must be classified as a startup configuration error")
		}
		// The mismatch is confined to the identity it was observed on: B's
		// own C-17 still succeeds.
		if err := m.clients["ws-b"].Workspaces(context.Background()); err != nil {
			t.Errorf("B C-17 failed while A's credential was mismatched: %v", err)
		}
	})
}
