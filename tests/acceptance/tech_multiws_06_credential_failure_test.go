// TC-tech-multiws-06 implements AC-86: the credential failure face follows the
// credential form — workspace-scoped failures stay in their workspace,
// user-scoped failures are deployment-wide, configuration errors refuse
// startup, and no request borrows a credential that does not cover its target
// workspace.
package acceptance

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

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
		dep, err := workspace.LoadDeployment(func(key string) string { return env[key] })
		if err != nil {
			t.Fatalf("valid set refused: %v", err)
		}
		if dep.Scope != workspace.ScopeWorkspace {
			t.Fatalf("scope = %v, want the workspace-scoped form", dep.Scope)
		}
		set := dep.Set
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
			if _, err := workspace.LoadDeployment(func(key string) string {
				if key == "FOREMAN_WORKSPACES" {
					return raw
				}
				return ""
			}); err == nil {
				t.Errorf("%s: Load accepted an invalid set", name)
			}
		}

		// ADR-049 decision 1: the workspace-scoped credential form accepts no
		// manifest, and a present-but-empty manifest is a configuration error
		// in the user-scoped form.
		if _, err := workspace.LoadDeployment(func(key string) string {
			switch key {
			case "FOREMAN_WORKSPACES":
				return `[{"workspace_id":"ws-a","daemon_id":"foreman-a","token":"mdt_a"}]`
			case "FOREMAN_WORKSPACES_MANIFEST":
				return `["ws-a"]`
			}
			return ""
		}); err == nil {
			t.Error("a manifest with the workspace-scoped credential form must refuse startup")
		}
		if _, err := workspace.LoadDeployment(func(key string) string {
			switch key {
			case "MULTICA_TOKEN":
				return "mul_user"
			case "FOREMAN_WORKSPACES_MANIFEST":
				return `[]`
			}
			return ""
		}); err == nil {
			t.Error("an empty manifest must refuse startup")
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

		errA := m.clients["ws-a"].ValidateWorkspaceScope(context.Background())
		var statusErr *proxy.StatusError
		if !errors.As(errA, &statusErr) || statusErr.Code != http.StatusForbidden {
			t.Fatalf("A C-17 error = %v, want a 403 StatusError", errA)
		}
		if !proxy.IsCredentialConfigError(errA) {
			t.Error("a 403 credential rejection must classify as a startup configuration error")
		}
		// B's control plane is untouched: its own C-17 succeeds and its
		// registration (C-1) still works.
		if err := m.clients["ws-b"].ValidateWorkspaceScope(context.Background()); err != nil {
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
		answer := func(body string) *httptest.Server {
			return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/daemon/workspaces" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(body))
			}))
		}

		for name, body := range map[string]string{
			"another workspace": `[{"id":"ws-b","name":"B"}]`,
			// ADR-049: the answer must EQUAL the configured workspace — a
			// superset means the credential is not workspace-scoped.
			"superset": `[{"id":"ws-a","name":"A"},{"id":"ws-b","name":"B"}]`,
			"empty":    `[]`,
		} {
			srv := answer(body)
			t.Cleanup(srv.Close)
			m := newMultiWSWithURLs(t, []string{"ws-a", "ws-b"}, map[string]string{"ws-a": srv.URL})
			err := m.clients["ws-a"].ValidateWorkspaceScope(context.Background())
			var mismatchErr *proxy.WorkspaceMismatchError
			if !errors.As(err, &mismatchErr) {
				t.Fatalf("%s: A C-17 error = %v, want a WorkspaceMismatchError", name, err)
			}
			if mismatchErr.Configured != "ws-a" {
				t.Fatalf("%s: mismatch = %+v, want configured ws-a", name, mismatchErr)
			}
			var statusErr *proxy.StatusError
			if errors.As(err, &statusErr) {
				t.Errorf("%s: a 200 C-17 answer naming another workspace must not look like a StatusError", name)
			}
			if !proxy.IsCredentialConfigError(err) {
				t.Errorf("%s: the workspace mismatch must be classified as a startup configuration error", name)
			}
		}

		// The mismatch is confined to the identity it was observed on: B's
		// own C-17 still succeeds.
		m := newMultiWS(t, "ws-a", "ws-b")
		if err := m.clients["ws-b"].ValidateWorkspaceScope(context.Background()); err != nil {
			t.Errorf("B C-17 failed while A's credential was mismatched: %v", err)
		}
	})

	t.Run("user-scoped credential failure backs off every workspace", func(t *testing.T) {
		// ADR-049 decision 4: with one user-scoped credential, a rejected
		// credential is the deployment-level failure face — every workspace's
		// control-plane loop backs off on the same credential and no request
		// keeps using it. Both loops must keep retrying (never exit) and every
		// request must carry the single credential.
		stub := newDiscoveryStub(t, []workspace.Ref{{ID: "ws-a", Name: "A"}, {ID: "ws-b", Name: "B"}})
		m, _ := newUserScopedWS(t, stub, nil)
		for _, workspaceID := range []string{"ws-a", "ws-b"} {
			m.upstream[workspaceID].setRegisterStatus(http.StatusUnauthorized, `{"error":"unauthorized"}`)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 2)
		for _, workspaceID := range []string{"ws-a", "ws-b"} {
			client := m.clients[workspaceID]
			client.Declare(registry.Discovery{
				CLIVersion: "0.6.1",
				Runtimes:   []registry.DiscoveredRuntime{{Name: "foreman-" + workspaceID, Type: "omp", Version: "0.6.1"}},
			})
			loopCfg := proxy.Config{
				ServerURL:            m.upstream[workspaceID].server.URL,
				WorkspaceID:          workspaceID,
				DaemonID:             "foreman-" + workspaceID,
				Version:              "0.1.0",
				ClaimInterval:        time.Second,
				ClaimIntervalIdle:    time.Second,
				LeaseRefreshInterval: time.Second,
			}
			loop := proxy.NewClaimLoop(loopCfg, client, m.sched)
			go func() { done <- loop.Run(ctx) }()
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if len(m.upstream["ws-a"].calls("POST", "/api/daemon/register")) >= 2 &&
				len(m.upstream["ws-b"].calls("POST", "/api/daemon/register")) >= 2 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		for _, workspaceID := range []string{"ws-a", "ws-b"} {
			if got := len(m.upstream[workspaceID].calls("POST", "/api/daemon/register")); got < 2 {
				t.Errorf("%s register attempts = %d, want >= 2 (the loop must keep retrying)", workspaceID, got)
			}
			for _, req := range m.upstream[workspaceID].all() {
				if got := req.Header.Get("Authorization"); got != "Bearer mul_user" {
					t.Errorf("%s request %s carried %q, want the single user-scoped credential", workspaceID, req.Path, got)
				}
			}
		}
		select {
		case err := <-done:
			t.Fatalf("a workspace loop exited on the shared credential: %v", err)
		default:
		}
		cancel()
		for i := 0; i < 2; i++ {
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("a workspace loop did not stop after cancel")
			}
		}
	})

	t.Run("user-scoped credential derives one identity per workspace", func(t *testing.T) {
		// ADR-049 decision 3: the discovery form derives each workspace's
		// deployment identity from the base identity; the single user-scoped
		// credential covers all of them, and no identity is shared.
		dep := workspace.Deployment{
			Scope:        workspace.ScopeUser,
			UserToken:    "mul_user",
			BaseDaemonID: "foreman",
		}
		identities := dep.Identities([]workspace.Ref{{ID: "ws-a"}, {ID: "ws-b"}})
		if len(identities) != 2 {
			t.Fatalf("identities = %d, want 2", len(identities))
		}
		if identities[0].DaemonID == identities[1].DaemonID {
			t.Fatalf("both workspaces share daemon identity %q (F2)", identities[0].DaemonID)
		}
		if identities[0].DaemonID != "foreman-ws-a" || identities[1].DaemonID != "foreman-ws-b" {
			t.Errorf("derived identities = %q, %q; want foreman-<workspace>", identities[0].DaemonID, identities[1].DaemonID)
		}
		for _, identity := range identities {
			if identity.Token != "mul_user" {
				t.Errorf("workspace %s token = %q, want the single user-scoped credential", identity.WorkspaceID, identity.Token)
			}
		}
		// Determinism across restarts: same base + same workspace ⇒ same id.
		again := dep.Identities([]workspace.Ref{{ID: "ws-a"}})
		if again[0].DaemonID != identities[0].DaemonID {
			t.Errorf("derivation is not stable: %q then %q", identities[0].DaemonID, again[0].DaemonID)
		}
	})
}
