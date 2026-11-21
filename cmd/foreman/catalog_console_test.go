package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/catalog/catalogtest"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// timeAtRun is the fixed instant a run-attribution fixture uses.
var timeAtRun = time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)

// TestOpsRootServesTheConsoleShellAndGatesTheApiBesideIt pins the split the
// console depends on: the shell is a browser navigation and is answered
// without a credential, while every /admin/ API route beside it still answers
// 401 without one. The shell carries no data, so the two answers are
// compatible — an operator opens the page, then types the credential into it.
func TestOpsRootServesTheConsoleShellAndGatesTheApiBesideIt(t *testing.T) {
	const token = "ops-token-console"
	api := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	})
	ops := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("a /admin/ request reached the ops table")
	})
	root := newOpsRoot(ops, api, []observability.OpsCredential{{Token: token}})

	for _, target := range []string{"/admin/", "/admin/assets/app.mjs", "/admin/assets/console.css"} {
		rec := httptest.NewRecorder()
		root.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s without a credential = %d, want the shell (200)", target, rec.Code)
		}
		if rec.Body.Len() == 0 {
			t.Errorf("GET %s answered no body", target)
		}
	}
	body := httptest.NewRecorder()
	root.ServeHTTP(body, httptest.NewRequest(http.MethodGet, "/admin/", nil))
	if strings.Contains(body.Body.String(), token) {
		t.Error("the served shell carries the ops credential")
	}

	for _, target := range []string{"/admin/servers", "/admin/overview", "/admin/agents", "/admin/audit"} {
		rec := httptest.NewRecorder()
		root.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("GET %s without a credential = %d, want 401", target, rec.Code)
		}
		admitted := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		root.ServeHTTP(admitted, req)
		if admitted.Code != http.StatusOK {
			t.Errorf("GET %s with the credential = %d, want the API answer (200)", target, admitted.Code)
		}
	}
}

// TestOpsRootKeepsTheOpsTableReachableWithoutAManagementSurface pins the
// degraded wiring: with no management handler the console paths still answer
// and the ops table keeps every other route.
func TestOpsRootKeepsTheOpsTableReachableWithoutAManagementSurface(t *testing.T) {
	ops := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ops"))
	})
	root := newOpsRoot(ops, nil, nil)

	rec := httptest.NewRecorder()
	root.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ops" {
		t.Errorf("GET /metrics = %d %q, want the ops table", rec.Code, rec.Body.String())
	}
	page := httptest.NewRecorder()
	root.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/admin/", nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "Foreman") {
		t.Errorf("GET /admin/ = %d, want the console shell", page.Code)
	}
}

// TestRunSourceAttributionStaysOnTheClaimingClient is the old-attempt clause:
// the overview attributes a run to the source that claimed the workspace's
// fixed URL. Registering a same-workspace source cannot move it, and removing
// that source cannot blank it or re-attribute it through the live directory.
func TestRunSourceAttributionStaysOnTheClaimingClient(t *testing.T) {
	ctx := context.Background()
	upstream := catalogtest.NewUpstream()
	upstream.Workspaces = []catalog.Workspace{{ID: "ws-core", Name: "core"}}
	service, err := catalog.New(catalog.Config{
		Upstream:    upstream,
		Credentials: catalogtest.Credentials{},
		Clock:       func() time.Time { return time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	atlasURL := "https://atlas.example.test"
	if _, err := service.Register(ctx, catalog.Registration{
		ServerID: "atlas", URL: atlasURL, CredentialRef: "atlas",
	}, "test"); err != nil {
		t.Fatalf("Register atlas: %v", err)
	}
	client := proxy.NewClient(proxy.Config{
		ServerURL: atlasURL, WorkspaceID: "ws-core", DaemonID: "mdt_test", Version: "test",
	}, "token")
	clientSet, err := proxy.NewClientSet(client)
	if err != nil {
		t.Fatalf("NewClientSet: %v", err)
	}
	attributor := runSourceResolver(service, clientSet)

	if source, ok := attributor(ctx, "ws-core"); !ok || source != "atlas" {
		t.Fatalf("source = %q, %v, want atlas", source, ok)
	}

	// A same-workspace source is a different server. It is a directory row,
	// but the claiming client's URL still wins.
	harborURL := "https://harbor.example.test"
	if _, err := service.Register(ctx, catalog.Registration{
		ServerID: "harbor", URL: harborURL, CredentialRef: "harbor",
	}, "test"); err != nil {
		t.Fatalf("Register harbor: %v", err)
	}
	if source, ok := attributor(ctx, "ws-core"); !ok || source != "atlas" {
		t.Fatalf("source after harbor = %q, %v, want atlas", source, ok)
	}

	// Both sources synced while ws-core existed: a directory-based answer
	// would be ambiguous. The URL still decides, and repeated reads agree.
	if _, err := service.Sync(ctx, "atlas"); err != nil {
		t.Fatalf("atlas Sync: %v", err)
	}
	if _, err := service.Sync(ctx, "harbor"); err != nil {
		t.Fatalf("harbor Sync: %v", err)
	}

	// The workspace disappears from the directory upstream. An in-flight
	// attempt keeps its server; it is not blanked and not moved to harbor.
	upstream.Workspaces = []catalog.Workspace{{ID: "ws-other", Name: "other"}}
	upstream.Catalogs = map[string]catalogtest.Catalog{}
	if _, err := service.Sync(ctx, "atlas"); err != nil {
		t.Fatalf("atlas Sync after the workspace dropped: %v", err)
	}
	for i := 0; i < 3; i++ {
		if source, ok := attributor(ctx, "ws-core"); !ok || source != "atlas" {
			t.Fatalf("source after the workspace dropped = %q, %v, want atlas", source, ok)
		}
	}
}

// TestDuplicateSourceURLIsRefusedAndLeavesRunningAttemptsAttributed is the
// registry→overview negative case: a live Atlas attempt must keep its Server,
// Workspace and Issue while the deployment tries to register a second id for
// the same upstream URL. The registration is refused, and repeated overview
// reads — deployment-wide and restricted to Atlas' own grant — keep the row.
func TestDuplicateSourceURLIsRefusedAndLeavesRunningAttemptsAttributed(t *testing.T) {
	ctx := context.Background()
	upstream := catalogtest.NewUpstream()
	upstream.Workspaces = []catalog.Workspace{{ID: "ws-core", Name: "core"}}
	upstream.Catalogs["ws-core"] = catalogtest.Catalog{}
	reg := registry.New(func() time.Time { return timeAtRun })
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-1", AgentID: "agent-1", AgentName: "dev",
		IssueID: "issue-1", IssueIdentifier: "TSI-1",
		WorkspaceID: "ws-core", State: registry.StateRunning, StartedAt: timeAtRun,
	}); err != nil {
		t.Fatalf("registry Put: %v", err)
	}

	atlasURL := "https://atlas.example.test"
	client := proxy.NewClient(proxy.Config{
		ServerURL: atlasURL, WorkspaceID: "ws-core", DaemonID: "mdt_test", Version: "test",
	}, "token")
	clientSet, err := proxy.NewClientSet(client)
	if err != nil {
		t.Fatalf("NewClientSet: %v", err)
	}
	runIndex := &catalogRunIndex{registry: reg}
	service, err := catalog.New(catalog.Config{
		Upstream:    upstream,
		Credentials: catalogtest.Credentials{},
		Clock:       func() time.Time { return timeAtRun },
		RunIndex:    runIndex,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runIndex.sourceID = runSourceResolver(service, clientSet)
	if _, err := service.Register(ctx, catalog.Registration{
		ServerID: "atlas", URL: atlasURL, CredentialRef: "atlas",
	}, "test"); err != nil {
		t.Fatalf("Register atlas: %v", err)
	}

	assertAttempt := func(label string, grant catalog.Grant, wantRows int) {
		t.Helper()
		view, err := service.Overview(ctx, grant)
		if err != nil {
			t.Fatalf("%s Overview: %v", label, err)
		}
		if view.InProgress.Attempts != wantRows {
			t.Fatalf("%s in-progress attempts = %d, want %d", label, view.InProgress.Attempts, wantRows)
		}
		if wantRows == 0 {
			return
		}
		row := view.InProgress.Rows[0]
		if row.ServerID != "atlas" || row.WorkspaceID != "ws-core" || row.IssueID != "issue-1" {
			t.Fatalf("%s row = %+v, want the Atlas attempt unchanged", label, row)
		}
	}
	wide := catalog.Grant{}
	scoped := catalog.Grant{Scopes: []catalog.Scope{{ServerID: "atlas", WorkspaceID: "ws-core"}}}
	assertAttempt("before", wide, 1)
	assertAttempt("before (scoped)", scoped, 1)

	// A second id for the same canonical URL would make the claiming client's
	// URL ambiguous — the exact hole that blanks a running attempt.
	_, err = service.Register(ctx, catalog.Registration{
		ServerID: "atlas-alias", URL: atlasURL + "/", CredentialRef: "alias",
	}, "test")
	if !errors.Is(err, catalog.ErrSourceURLExists) {
		t.Fatalf("duplicate registration err = %v, want ErrSourceURLExists", err)
	}
	if _, ok, err := service.Get(ctx, "atlas-alias"); err != nil || ok {
		t.Fatalf("a refused registration left a source behind: ok=%v err=%v", ok, err)
	}

	for i := 0; i < 3; i++ {
		assertAttempt("after", wide, 1)
		assertAttempt("after (scoped)", scoped, 1)
	}
}
