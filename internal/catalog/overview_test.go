package catalog_test

import (
	"context"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/catalog/catalogtest"
)

// timeAt is the fixed instant the tests start their clock at.
func timeAt() time.Time { return time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC) }

// staticRuns is a coordinator run-fact stub.
type staticRuns struct {
	rows []catalog.RunAttempt
}

// RunningAttempts answers the stub's rows.
func (s *staticRuns) RunningAttempts() []catalog.RunAttempt {
	return append([]catalog.RunAttempt(nil), s.rows...)
}

// TestOverviewCountsDistinctObjectsAcrossSources is UI-01's arithmetic: the
// four numbers are set cardinalities, and a same-named agent in two sources is
// two agents.
func TestOverviewCountsDistinctObjectsAcrossSources(t *testing.T) {
	upstream := seededUpstream()
	clock := &fixedClock{now: timeAt()}
	environments := 3
	service, err := catalog.New(catalog.Config{
		Upstream:          upstream,
		Credentials:       catalogtest.Credentials{},
		Clock:             clock.Now,
		ManagedRuntimeIDs: managedRuntimes,
		EnvironmentCount:  func() int { return environments },
		RunIndex:          &staticRuns{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	register(t, service, "atlas", false)
	register(t, service, "harbor", false)

	// Before any sync, only the connection faces exist: no directory object
	// may be counted.
	empty, err := service.Overview(context.Background(), catalog.Grant{})
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}
	if empty.Counts.Servers != 2 {
		t.Fatalf("servers = %d, want both handshakes counted", empty.Counts.Servers)
	}
	if empty.Counts.Workspaces != 0 || empty.Counts.Agents != 0 {
		t.Fatalf("unsynced sources contributed directory objects: %+v", empty.Counts)
	}

	for _, id := range []string{"atlas", "harbor"} {
		if _, err := service.Sync(context.Background(), catalog.ServerID(id)); err != nil {
			t.Fatalf("Sync %s: %v", id, err)
		}
	}
	view, err := service.Overview(context.Background(), catalog.Grant{})
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}
	// atlas: 2 workspaces, harbor: 2 workspaces — the same ids under two
	// sources are four (serverId, workspaceId) pairs.
	if view.Counts.Workspaces != 4 {
		t.Fatalf("workspaces = %d, want 4 (two per source)", view.Counts.Workspaces)
	}
	if view.Counts.WorkspacesWithForemanAgents != 4 {
		t.Fatalf("workspaces with foreman agents = %d, want 4", view.Counts.WorkspacesWithForemanAgents)
	}
	// atlas: 2 managed; harbor: the same two agent ids under another source
	// are two more objects, never a merge.
	if view.Counts.Agents != 4 {
		t.Fatalf("agents = %d, want 4 distinct (server, workspace, agent) triples", view.Counts.Agents)
	}
	if view.Counts.Environments != 3 {
		t.Fatalf("environments = %d, want 3", view.Counts.Environments)
	}
	if !view.InProgress.Available {
		t.Fatal("in-progress facts read as unavailable with a run index wired")
	}

	// A narrowed grant crops the numbers instead of computing them globally.
	narrowed, err := service.Overview(context.Background(), catalog.Grant{Scopes: []catalog.Scope{
		{ServerID: "atlas", WorkspaceID: "ws-core"},
		{ServerID: "harbor", WorkspaceID: "ws-core"},
	}})
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}
	if narrowed.Counts.Workspaces != 2 {
		t.Fatalf("narrowed workspaces = %d, want the two ws-core rows", narrowed.Counts.Workspaces)
	}
	if narrowed.Counts.Agents != 2 {
		t.Fatalf("narrowed agents = %d, want the two ws-core managed agents", narrowed.Counts.Agents)
	}
}

// TestOverviewInProgressDedupesAgentsNotAttempts is UI-01's list rule: one
// agent with two tasks is one agent and two rows, and a finished attempt is
// gone.
func TestOverviewInProgressDedupesAgentsNotAttempts(t *testing.T) {
	upstream := seededUpstream()
	clock := &fixedClock{now: timeAt()}
	started := timeAt().Add(-5 * time.Minute)
	attempts := &staticRuns{rows: []catalog.RunAttempt{
		{
			ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-managed", AgentName: "dev",
			IssueID: "issue-1", IssueIdentifier: "TSI-1", Environment: "env-1", Status: "running", StartedAt: started,
		},
		{
			ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-managed", AgentName: "dev",
			IssueID: "issue-2", IssueIdentifier: "TSI-2", Environment: "env-2", Status: "running", StartedAt: started.Add(time.Minute),
		},
	}}
	service, err := catalog.New(catalog.Config{
		Upstream:          upstream,
		Credentials:       catalogtest.Credentials{},
		Clock:             clock.Now,
		ManagedRuntimeIDs: managedRuntimes,
		RunIndex:          attempts,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	register(t, service, "atlas", false)
	if _, err := service.Sync(context.Background(), "atlas"); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	view, err := service.Overview(context.Background(), catalog.Grant{})
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}
	if view.InProgress.Agents != 1 {
		t.Fatalf("in-progress agents = %d, want 1 for one agent with two tasks", view.InProgress.Agents)
	}
	if view.InProgress.Attempts != 2 || len(view.InProgress.Rows) != 2 {
		t.Fatalf("attempts = %d/%d, want two rows", view.InProgress.Attempts, len(view.InProgress.Rows))
	}
	for _, row := range view.InProgress.Rows {
		if row.ServerID != "atlas" || row.WorkspaceID != "ws-core" || row.IssueID == "" || row.Environment == "" {
			t.Fatalf("row is missing its hover identity: %+v", row)
		}
		if row.StartedAt == "" {
			t.Fatalf("row has no start time: %+v", row)
		}
	}
	// The ordering is an implementation detail the design leaves open; it is
	// pinned here so the list cannot shuffle between reads. Oldest first means
	// the longest-running row leads, which is the one an operator looks for.
	if view.InProgress.Rows[0].IssueID != "issue-1" || view.InProgress.Rows[1].IssueID != "issue-2" {
		t.Fatalf("row order = %s, %s; want the earlier start first",
			view.InProgress.Rows[0].IssueID, view.InProgress.Rows[1].IssueID)
	}

	// A finished attempt leaves the list: the coordinator's facts, not the
	// catalog, decide that.
	attempts.rows = attempts.rows[:1]
	view, err = service.Overview(context.Background(), catalog.Grant{})
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}
	if view.InProgress.Attempts != 1 || view.InProgress.Agents != 1 {
		t.Fatalf("after the verdict: %+v, want one row and one agent", view.InProgress)
	}
}

// TestOverviewWithoutRunFactsIsUnavailable covers the unavailable class: no run
// index means the list is unknown, not empty.
func TestOverviewWithoutRunFactsIsUnavailable(t *testing.T) {
	upstream := seededUpstream()
	service, _ := newService(t, upstream, false)
	view, err := service.Overview(context.Background(), catalog.Grant{})
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}
	if view.InProgress.Available {
		t.Fatal("in-progress reads as available without a run index")
	}
	if len(view.InProgress.Rows) != 0 {
		t.Fatalf("rows = %+v, want none", view.InProgress.Rows)
	}
}

// TestOverviewCountsOnlyValidConnections keeps a failed handshake out of the
// server number.
func TestOverviewCountsOnlyValidConnections(t *testing.T) {
	upstream := seededUpstream()
	clock := &fixedClock{now: timeAt()}
	service, err := catalog.New(catalog.Config{
		Upstream:    upstream,
		Credentials: catalogtest.Credentials{},
		Clock:       clock.Now,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	register(t, service, "atlas", false)

	upstream.ListErr = &catalog.UpstreamError{Op: "list workspaces", Kind: catalog.UpstreamTransient, Message: "connection refused"}
	autoSync, period := false, 60
	if _, err := service.Register(context.Background(), catalog.Registration{
		ServerID:          "harbor",
		URL:               "https://harbor.example.test",
		CredentialRef:     "harbor-credential",
		AutoSync:          &autoSync,
		SyncPeriodSeconds: &period,
	}, "test"); err != nil {
		t.Fatalf("Register harbor: %v", err)
	}

	view, err := service.Overview(context.Background(), catalog.Grant{})
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}
	if view.Counts.Servers != 1 {
		t.Fatalf("servers = %d, want only the connected source counted", view.Counts.Servers)
	}
}

// TestSourceListIsCroppedToTheGrant keeps a workspace-bound caller from
// learning about a source it cannot reach, and from reading another
// workspace's counts through one it can.
func TestSourceListIsCroppedToTheGrant(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	register(t, service, "harbor", false)
	upstream.Workspaces = []catalog.Workspace{{ID: "ws-harbor", Name: "harbor"}}
	if _, err := service.Sync(context.Background(), "harbor"); err != nil {
		t.Fatalf("harbor Sync: %v", err)
	}

	all, err := service.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("deployment-wide list = %d sources, want 2", len(all))
	}
	cropped, err := service.ListForGrant(context.Background(), catalog.Grant{Scopes: []catalog.Scope{
		{ServerID: "atlas", WorkspaceID: "ws-core"},
	}})
	if err != nil {
		t.Fatalf("ListForGrant: %v", err)
	}
	if len(cropped) != 1 || cropped[0].ServerID != "atlas" {
		t.Fatalf("ws-core grant sees %+v, want only atlas", cropped)
	}
	if cropped[0].Workspaces != 1 || cropped[0].Agents != 3 {
		t.Fatalf("cropped counts = %+v, want the ws-core rows alone", cropped[0])
	}
	if !cropped[0].WriteBack.Enabled {
		t.Fatalf("a synced source with a valid credential closed its editor: %+v", cropped[0].WriteBack)
	}
}
