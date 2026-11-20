package catalog_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/catalog"
)

// adminSurface is a management handler over a synced source, with the grant
// the test's caller presents.
type adminSurface struct {
	admitted bool
	grant    catalog.Grant
	handler  http.Handler
}

// newAdminSurface builds the surface and keeps the caller's grant mutable, so
// one test can change what the caller may see.
func newAdminSurface(t *testing.T, service *catalog.Service) *adminSurface {
	t.Helper()
	surface := &adminSurface{}
	surface.admitted = true
	handler, err := catalog.NewAdminHandler(catalog.AdminConfig{
		Catalog: service,
		Grant:   func(*http.Request) (catalog.Grant, bool) { return surface.grant, surface.admitted },
		Actor:   func(*http.Request) string { return "ops-credential" },
	})
	if err != nil {
		t.Fatalf("NewAdminHandler: %v", err)
	}
	surface.handler = handler
	return surface
}

// do runs one request against the surface.
func (s *adminSurface) do(t *testing.T, method, target string, body any) (int, []byte) {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request := httptest.NewRequest(method, target, reader)
	recorder := httptest.NewRecorder()
	s.handler.ServeHTTP(recorder, request)
	return recorder.Code, recorder.Body.Bytes()
}

// TestAdminSurfaceRoutesOnlyAnswerNonSuccessOnFailure pins the binding route's
// status mapping: only applied is 200.
func TestAdminSurfaceRoutesOnlyAnswerNonSuccessOnFailure(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	surface := newAdminSurface(t, service)

	code, body := surface.do(t, http.MethodPut,
		"/admin/servers/atlas/workspaces/ws-core/agents/agent-outside/binding",
		map[string]string{"runtime_id": "rt-foreman", "expected_runtime_id": "rt-outside"})
	if code != http.StatusOK {
		t.Fatalf("applied binding = %d (%s), want 200", code, body)
	}
	var applied catalog.BindingOutcome
	if err := json.Unmarshal(body, &applied); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if applied.Result != catalog.BindingApplied {
		t.Fatalf("result = %q, want applied", applied.Result)
	}

	upstream.WriteErr = &catalog.UpstreamError{Op: "put agent", Status: 422, Kind: catalog.UpstreamRejected, Message: "refused"}
	code, body = surface.do(t, http.MethodPut,
		"/admin/servers/atlas/workspaces/ws-core/agents/agent-outside/binding",
		map[string]string{"runtime_id": "rt-outside", "expected_runtime_id": "rt-foreman"})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("rejected binding = %d (%s), want 422", code, body)
	}

	upstream.WriteErr = &catalog.UpstreamError{Op: "put agent", Kind: catalog.UpstreamTransient, Message: "request failed"}
	upstream.ReadErr = &catalog.UpstreamError{Op: "get agent", Kind: catalog.UpstreamTransient, Message: "request failed"}
	upstream.ReadErrAfter = 1
	code, body = surface.do(t, http.MethodPut,
		"/admin/servers/atlas/workspaces/ws-core/agents/agent-outside/binding",
		map[string]string{"runtime_id": "rt-outside", "expected_runtime_id": "rt-foreman"})
	if code != http.StatusGatewayTimeout {
		t.Fatalf("unconfirmed binding = %d (%s), want 504", code, body)
	}
}

// TestAdminSurfaceCropsToTheCallersGrant keeps a workspace-bound credential
// from reading or writing outside its grant, and a query parameter from
// widening it.
func TestAdminSurfaceCropsToTheCallersGrant(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	// A second source whose directory the ws-dev grant never reaches.
	register(t, service, "harbor", false)
	upstream.Workspaces = []catalog.Workspace{{ID: "ws-harbor", Name: "harbor"}}
	if _, err := service.Sync(context.Background(), "harbor"); err != nil {
		t.Fatalf("harbor Sync: %v", err)
	}
	surface := newAdminSurface(t, service)
	surface.grant = catalog.Grant{Scopes: []catalog.Scope{{ServerID: "atlas", WorkspaceID: "ws-dev"}}}

	code, body := surface.do(t, http.MethodGet, "/admin/servers/atlas/directory", nil)
	if code != http.StatusOK {
		t.Fatalf("directory = %d (%s)", code, body)
	}
	var directory catalog.DirectoryView
	if err := json.Unmarshal(body, &directory); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(directory.Workspaces) != 1 || directory.Workspaces[0].WorkspaceID != "ws-dev" {
		t.Fatalf("directory workspaces = %+v, want only ws-dev", directory.Workspaces)
	}
	for _, agent := range directory.Agents {
		if agent.WorkspaceID != "ws-dev" {
			t.Fatalf("directory leaked an agent of %q", agent.WorkspaceID)
		}
	}

	// A query naming a workspace outside the grant answers like one that does
	// not exist, never with its rows.
	code, body = surface.do(t, http.MethodGet, "/admin/servers/atlas/directory?workspace_id=ws-core", nil)
	if code != http.StatusNotFound {
		t.Fatalf("cross-grant directory = %d (%s), want 404", code, body)
	}
	// The source itself is reachable through ws-dev, but its ws-core rows are
	// not; ask for a source the grant does not reach at all and the answer is
	// the same 404.
	code, body = surface.do(t, http.MethodGet, "/admin/servers/harbor/directory", nil)
	if code != http.StatusNotFound {
		t.Fatalf("unreachable source directory = %d (%s), want 404", code, body)
	}
	code, body = surface.do(t, http.MethodPut,
		"/admin/servers/atlas/workspaces/ws-core/agents/agent-managed/binding",
		map[string]string{"runtime_id": "rt-outside", "expected_runtime_id": "rt-foreman"})
	if code != http.StatusNotFound {
		t.Fatalf("cross-grant binding = %d (%s), want 404", code, body)
	}
	if upstream.WriteHappened {
		t.Fatal("a cross-grant binding reached the upstream")
	}

	// The overview numbers follow the same grant.
	code, body = surface.do(t, http.MethodGet, "/admin/overview", nil)
	if code != http.StatusOK {
		t.Fatalf("overview = %d (%s)", code, body)
	}
	var overview catalog.OverviewView
	if err := json.Unmarshal(body, &overview); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// The one registered source holds both workspaces, so the ws-dev grant
	// reaches one workspace and its one managed agent, never a ws-core row.
	if overview.Counts.Workspaces != 1 || overview.Counts.WorkspacesWithForemanAgents != 1 {
		t.Fatalf("overview counts = %+v, want the ws-dev row alone", overview.Counts)
	}
	if overview.Counts.Agents != 1 {
		t.Fatalf("overview agents = %d, want the one ws-dev managed agent", overview.Counts.Agents)
	}

	// Source-level actions stay with the deployment-wide credential.
	code, body = surface.do(t, http.MethodPost, "/admin/servers", map[string]string{
		"server_id": "harbor", "url": "https://harbor.example.test", "credential_ref": "harbor",
	})
	if code != http.StatusForbidden {
		t.Fatalf("workspace-bound registration = %d (%s), want 403", code, body)
	}
	code, body = surface.do(t, http.MethodPost, "/admin/servers/atlas/sync", nil)
	if code != http.StatusForbidden {
		t.Fatalf("workspace-bound sync = %d (%s), want 403", code, body)
	}
}

// TestAdminSurfaceAnswersTheEmptyDirectoryBeforeSync is SYNC-02 through the
// wire: an unsynced source answers an empty generation, not a 404.
func TestAdminSurfaceAnswersTheEmptyDirectoryBeforeSync(t *testing.T) {
	upstream := seededUpstream()
	service, _ := newService(t, upstream, false)
	surface := newAdminSurface(t, service)

	code, body := surface.do(t, http.MethodGet, "/admin/servers/atlas/directory", nil)
	if code != http.StatusOK {
		t.Fatalf("directory = %d (%s), want 200", code, body)
	}
	var directory catalog.DirectoryView
	if err := json.Unmarshal(body, &directory); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if directory.Generation != 0 || len(directory.Agents) != 0 {
		t.Fatalf("unsynced directory = %+v, want an empty generation", directory)
	}

	code, body = surface.do(t, http.MethodPost, "/admin/servers/atlas/sync", nil)
	if code != http.StatusOK {
		t.Fatalf("manual sync = %d (%s), want 200", code, body)
	}
	var report catalog.SyncReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if report.Result != "ok" || report.Generation != 1 || report.Agents != 4 {
		t.Fatalf("sync report = %+v, want an ok first generation", report)
	}

	// A failing sync answers the kept generation with a non-2xx status, so a
	// client cannot read it as success.
	upstream.ListErr = &catalog.UpstreamError{Op: "list workspaces", Kind: catalog.UpstreamTransient, Message: "down"}
	code, body = surface.do(t, http.MethodPost, "/admin/servers/atlas/sync", nil)
	if code == http.StatusOK {
		t.Fatalf("failed sync answered 200 (%s)", body)
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if report.Result != "failed" || report.KeptGeneration != 1 {
		t.Fatalf("failed report = %+v, want the kept generation named", report)
	}
}

// TestAdminSurfaceRefusesUnknownFields keeps a misspelled setting an error
// rather than a silently accepted registration.
func TestAdminSurfaceRefusesUnknownFields(t *testing.T) {
	upstream := seededUpstream()
	service, _ := newService(t, upstream, false)
	surface := newAdminSurface(t, service)

	request := httptest.NewRequest(http.MethodPost, "/admin/servers",
		strings.NewReader(`{"server_id":"harbor","url":"https://harbor.example.test","credential_ref":"harbor","auto_synch":true}`))
	recorder := httptest.NewRecorder()
	surface.handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("registration with an unknown field = %d (%s), want 400", recorder.Code, recorder.Body.String())
	}
	if _, ok, _ := service.Get(context.Background(), "harbor"); ok {
		t.Fatal("a refused registration was stored anyway")
	}
}

// TestAdminSurfaceIsNotMountedOnTheOpsRouteSet guards the separation: the
// management routes belong to their own table, so the census-pinned ops route
// set is unchanged.
func TestAdminSurfaceIsNotMountedOnTheOpsRouteSet(t *testing.T) {
	upstream := seededUpstream()
	service, _ := newService(t, upstream, false)
	surface := newAdminSurface(t, service)

	// The surface answers its own paths.
	if code, body := surface.do(t, http.MethodGet, "/admin/servers", nil); code != http.StatusOK {
		t.Fatalf("GET /admin/servers = %d (%s), want 200", code, body)
	}
	// And it does not answer ops paths: those are the other table's.
	if code, _ := surface.do(t, http.MethodGet, "/foreman/tasks", nil); code == http.StatusOK {
		t.Fatal("the management table answers an ops path")
	}
}

// TestAdminSurfaceNeedsAGrantResolver refuses a surface that would answer every
// workspace because no grant was wired.
func TestAdminSurfaceNeedsAGrantResolver(t *testing.T) {
	upstream := seededUpstream()
	service, _ := newService(t, upstream, false)
	if _, err := catalog.NewAdminHandler(catalog.AdminConfig{Catalog: service}); err == nil {
		t.Fatal("NewAdminHandler accepted a surface with no grant resolver")
	}
	if _, err := catalog.NewAdminHandler(catalog.AdminConfig{}); err == nil {
		t.Fatal("NewAdminHandler accepted a surface with no catalog")
	}
}

// TestAdminSurfaceFailsClosedWithoutACredential is the §8 gate: a request the
// credential face did not admit is refused instead of read as deployment-wide.
func TestAdminSurfaceFailsClosedWithoutACredential(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	surface := newAdminSurface(t, service)
	surface.admitted = false

	for _, target := range []string{"/admin/servers", "/admin/overview", "/admin/audit"} {
		code, body := surface.do(t, http.MethodGet, target, nil)
		if code != http.StatusUnauthorized {
			t.Fatalf("GET %s without an admitted credential = %d (%s), want 401", target, code, body)
		}
	}
	code, body := surface.do(t, http.MethodPut,
		"/admin/servers/atlas/workspaces/ws-core/agents/agent-outside/binding",
		map[string]string{"runtime_id": "rt-foreman", "expected_runtime_id": "rt-outside"})
	if code != http.StatusUnauthorized {
		t.Fatalf("unadmitted binding write = %d (%s), want 401", code, body)
	}
	if upstream.WriteHappened {
		t.Fatal("an unadmitted request reached the upstream")
	}
}

// TestWorkspaceGrantDoesNotCrossSources is the source-scope half of §8: the
// same workspace id under another server is another object. A credential bound
// to Atlas/ws-core must not read or write Harbor/ws-core, even though the id
// matches, and the workspace-only form refuses to guess when two sources hold
// it.
func TestWorkspaceGrantDoesNotCrossSources(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	register(t, service, "harbor", false)
	if _, err := service.Sync(context.Background(), "harbor"); err != nil {
		t.Fatalf("harbor Sync: %v", err)
	}
	// The stub answers the same workspace set for every source: Harbor holds
	// ws-core too, which is exactly the ambiguity the grant must not widen.
	surface := newAdminSurface(t, service)
	surface.grant = catalog.Grant{Scopes: []catalog.Scope{{ServerID: "atlas", WorkspaceID: "ws-core"}}}

	for _, target := range []string{
		"/admin/servers/harbor/directory",
		"/admin/servers/harbor/directory?workspace_id=ws-core",
		"/admin/servers/harbor/agents?workspace_id=ws-core",
		"/admin/servers/harbor/audit",
	} {
		code, body := surface.do(t, http.MethodGet, target, nil)
		if code != http.StatusNotFound {
			t.Fatalf("GET %s = %d (%s), want 404: the same workspace id under another server", target, code, body)
		}
	}
	code, body := surface.do(t, http.MethodPut,
		"/admin/servers/harbor/workspaces/ws-core/agents/agent-managed/binding",
		map[string]string{"runtime_id": "rt-outside", "expected_runtime_id": "rt-foreman"})
	if code != http.StatusNotFound {
		t.Fatalf("cross-server binding = %d (%s), want 404", code, body)
	}
	if upstream.WriteHappened {
		t.Fatal("a cross-server binding reached the upstream")
	}

	// The Atlas pair is still readable, and a query can only narrow within it.
	code, body = surface.do(t, http.MethodGet, "/admin/servers/atlas/directory?workspace_id=ws-core", nil)
	if code != http.StatusOK {
		t.Fatalf("atlas directory = %d (%s)", code, body)
	}
	if code, body = surface.do(t, http.MethodGet, "/admin/servers/atlas/directory?workspace_id=ws-dev", nil); code != http.StatusNotFound {
		t.Fatalf("atlas ws-dev with a ws-core grant = %d (%s), want 404", code, body)
	}

	// An agent that lives in another workspace cannot be smuggled through the
	// authorized workspace path: it is rejected before any upstream call.
	code, body = surface.do(t, http.MethodPut,
		"/admin/servers/atlas/workspaces/ws-core/agents/agent-managed-dev/binding",
		map[string]string{"runtime_id": "rt-foreman-dev", "expected_runtime_id": "rt-foreman-dev"})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("foreign-workspace agent = %d (%s), want 422", code, body)
	}
	if upstream.WriteHappened {
		t.Fatal("a foreign-workspace agent reached the upstream")
	}

	// A workspace-only credential refuses to guess once two sources hold the
	// id; the server-qualified form still resolves.
	ambiguous := service.GrantForScopes(context.Background(), []catalog.Scope{{WorkspaceID: "ws-core"}})
	if len(ambiguous.Scopes) != 0 {
		t.Fatalf("ambiguous workspace-only grant = %+v, want no scope", ambiguous.Scopes)
	}
	exact := service.GrantForScopes(context.Background(), []catalog.Scope{{ServerID: "atlas", WorkspaceID: "ws-core"}})
	if len(exact.Scopes) != 1 || exact.Scopes[0].ServerID != "atlas" {
		t.Fatalf("server-qualified grant = %+v", exact.Scopes)
	}
}

// TestWorkspaceOnlyGrantResolvesOneSource keeps the legacy deployment shape:
// while exactly one registered source holds the workspace, an unqualified
// workspace credential reaches it.
func TestWorkspaceOnlyGrantResolvesOneSource(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	grant := service.GrantForScopes(context.Background(), []catalog.Scope{{WorkspaceID: "ws-core"}})
	if len(grant.Scopes) != 1 || grant.Scopes[0].ServerID != "atlas" || grant.Scopes[0].WorkspaceID != "ws-core" {
		t.Fatalf("resolved grant = %+v", grant.Scopes)
	}
}
