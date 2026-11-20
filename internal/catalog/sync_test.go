package catalog_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/catalog/catalogtest"
)

// fixedClock is the service's clock seam: a test advances it instead of
// sleeping, so a due-sync assertion is deterministic.
type fixedClock struct {
	now time.Time
}

func (c *fixedClock) Now() time.Time { return c.now }

// Advance moves the clock forward.
func (c *fixedClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

// newService builds a service over the stub upstream with one registered
// source. autoSync states whether the source syncs on its own.
func newService(t *testing.T, upstream *catalogtest.Upstream, autoSync bool) (*catalog.Service, *fixedClock) {
	t.Helper()
	clock := &fixedClock{now: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)}
	service, err := catalog.New(catalog.Config{
		Upstream:    upstream,
		Credentials: catalogtest.Credentials{},
		Clock:       clock.Now,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	register(t, service, "atlas", autoSync)
	return service, clock
}

// register stores one source through the service's own validation path.
func register(t *testing.T, service *catalog.Service, serverID string, autoSync bool) catalog.SourceView {
	t.Helper()
	period := 60
	view, err := service.Register(context.Background(), catalog.Registration{
		ServerID:          serverID,
		URL:               "https://" + serverID + ".example.test",
		CredentialRef:     serverID + "-credential",
		AutoSync:          &autoSync,
		SyncPeriodSeconds: &period,
	}, "test")
	if err != nil {
		t.Fatalf("Register %s: %v", serverID, err)
	}
	return view
}

// seededUpstream is one source with two workspaces, one project, two runtimes
// and three agents: one on a Foreman runtime, one on an outside runtime and
// one unbound.
func seededUpstream() *catalogtest.Upstream {
	upstream := catalogtest.NewUpstream()
	// The full binding protocol (conditional write + read-back) is what these
	// scenarios drive; the HTTP adapter's lack of a conditional write has its
	// own test.
	upstream.ConditionalWrite = true
	upstream.RequireWorkspace = true
	upstream.Workspaces = []catalog.Workspace{
		{ID: "ws-core", Name: "core"},
		{ID: "ws-dev", Name: "dev"},
	}
	upstream.Catalogs["ws-core"] = catalogtest.Catalog{
		Projects: []catalog.Project{{WorkspaceID: "ws-core", ID: "proj-1", Title: "foreman"}},
		Runtimes: []catalog.Runtime{
			{WorkspaceID: "ws-core", ID: "rt-foreman", Name: "foreman", Provider: "multica"},
			{WorkspaceID: "ws-core", ID: "rt-outside", Name: "outside", Provider: "multica"},
		},
		Agents: []catalogtest.Agent{
			{WorkspaceID: "ws-core", ID: "agent-managed", Name: "dev", RuntimeID: "rt-foreman", Bound: true},
			{WorkspaceID: "ws-core", ID: "agent-outside", Name: "dev", RuntimeID: "rt-outside", Bound: true},
			{WorkspaceID: "ws-core", ID: "agent-unbound", Name: "dev"},
		},
	}
	upstream.Catalogs["ws-dev"] = catalogtest.Catalog{
		Runtimes: []catalog.Runtime{{WorkspaceID: "ws-dev", ID: "rt-foreman-dev", Name: "foreman", Provider: "multica"}},
		Agents: []catalogtest.Agent{
			{WorkspaceID: "ws-dev", ID: "agent-managed-dev", Name: "dev", RuntimeID: "rt-foreman-dev", Bound: true},
		},
	}
	for _, entry := range upstream.Catalogs {
		for _, agent := range entry.Agents {
			upstream.Agents[agent.ID] = agent
		}
	}
	return upstream
}

// managedRuntimes answers the Foreman runtime set the stub registers.
func managedRuntimes(_ catalog.Source, workspaceID string) []string {
	switch workspaceID {
	case "ws-core":
		return []string{"rt-foreman"}
	case "ws-dev":
		return []string{"rt-foreman-dev"}
	}
	return nil
}

// TestSyncBeforeManualRunLeavesTheSourceEmpty is SYNC-02's first clause: a
// source with automatic sync off contributes no object until a sync runs.
func TestSyncBeforeManualRunLeavesTheSourceEmpty(t *testing.T) {
	upstream := seededUpstream()
	service, clock := newService(t, upstream, false)

	view, ok, err := service.Get(context.Background(), "atlas")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if view.Generation != 0 || view.Workspaces != 0 || view.Agents != 0 {
		t.Fatalf("a source that never synced reports a directory: %+v", view)
	}
	if view.Connection != string(catalog.ConnectionOK) {
		t.Fatalf("connection = %q, want the handshake to have succeeded", view.Connection)
	}
	directory, err := service.Directory(context.Background(), "atlas", "", catalog.Grant{})
	if err != nil {
		t.Fatalf("Directory: %v", err)
	}
	if len(directory.Workspaces) != 0 || len(directory.Agents) != 0 || len(directory.Projects) != 0 || len(directory.Runtimes) != 0 {
		t.Fatalf("an unsynced source answers directory rows: %+v", directory)
	}
	if directory.Generation != 0 {
		t.Fatalf("generation = %d, want 0 before the first sync", directory.Generation)
	}

	// The automatic loop must not sync a source that opted out, even when the
	// clock has advanced far beyond the period.
	clock.Advance(24 * time.Hour)
	service.SyncDue(context.Background())
	directory, err = service.Directory(context.Background(), "atlas", "", catalog.Grant{})
	if err != nil {
		t.Fatalf("Directory: %v", err)
	}
	if directory.Generation != 0 {
		t.Fatalf("automatic sync ran on a source with auto_sync off: generation %d", directory.Generation)
	}
}

// TestManualSyncPublishesTheCompleteDirectory is SYNC-02's second clause: one
// manual sync yields the workspaces, projects, agents and runtimes.
func TestManualSyncPublishesTheCompleteDirectory(t *testing.T) {
	upstream := seededUpstream()
	service, _ := newService(t, upstream, false)

	report, err := service.Sync(context.Background(), "atlas")
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if report.Result != "ok" || report.Generation != 1 {
		t.Fatalf("report = %+v, want an ok first generation", report)
	}
	if report.Workspaces != 2 || report.Projects != 1 || report.Agents != 4 || report.Runtimes != 3 {
		t.Fatalf("report counts = %+v, want 2/1/4/3", report)
	}

	directory, err := service.Directory(context.Background(), "atlas", "", catalog.Grant{})
	if err != nil {
		t.Fatalf("Directory: %v", err)
	}
	if len(directory.Workspaces) != 2 || len(directory.Projects) != 1 || len(directory.Agents) != 4 || len(directory.Runtimes) != 3 {
		t.Fatalf("directory rows = %d/%d/%d/%d, want 2/1/4/3",
			len(directory.Workspaces), len(directory.Projects), len(directory.Agents), len(directory.Runtimes))
	}
	if directory.Sync != string(catalog.SyncOK) || directory.Stale {
		t.Fatalf("directory face = sync %q stale %v, want a fresh ok face", directory.Sync, directory.Stale)
	}
	if directory.Generation != 1 {
		t.Fatalf("generation = %d, want 1", directory.Generation)
	}
}

// TestFailedSyncKeepsThePreviousGenerationAndSparesOtherSources is SYNC-02's
// failure clause: the failed source keeps its last complete snapshot, and a
// second source is untouched.
func TestFailedSyncKeepsThePreviousGenerationAndSparesOtherSources(t *testing.T) {
	upstream := seededUpstream()
	service, _ := newService(t, upstream, false)
	register(t, service, "harbor", false)

	if _, err := service.Sync(context.Background(), "atlas"); err != nil {
		t.Fatalf("first Sync: %v", err)
	}
	if _, err := service.Sync(context.Background(), "harbor"); err != nil {
		t.Fatalf("harbor Sync: %v", err)
	}
	before, err := service.Directory(context.Background(), "atlas", "", catalog.Grant{})
	if err != nil {
		t.Fatalf("Directory: %v", err)
	}
	harborBefore, err := service.Directory(context.Background(), "harbor", "", catalog.Grant{})
	if err != nil {
		t.Fatalf("harbor Directory: %v", err)
	}

	// One workspace read fails mid-sync: the generation must not be replaced
	// by a partial directory.
	upstream.CatalogErr["ws-dev"] = &catalog.UpstreamError{
		Op: "list agents", Kind: catalog.UpstreamTransient, Message: "connection reset",
	}
	report, err := service.Sync(context.Background(), "atlas")
	if err == nil {
		t.Fatal("Sync answered success for a truncated read")
	}
	if report.Result != "failed" {
		t.Fatalf("report result = %q, want failed", report.Result)
	}
	if report.KeptGeneration != before.Generation || report.Generation != before.Generation {
		t.Fatalf("report names generation %d/%d, want the kept %d",
			report.Generation, report.KeptGeneration, before.Generation)
	}

	after, err := service.Directory(context.Background(), "atlas", "", catalog.Grant{})
	if err != nil {
		t.Fatalf("Directory: %v", err)
	}
	if after.Generation != before.Generation {
		t.Fatalf("generation moved on a failed sync: %d -> %d", before.Generation, after.Generation)
	}
	if len(after.Agents) != len(before.Agents) || len(after.Workspaces) != len(before.Workspaces) ||
		len(after.Projects) != len(before.Projects) || len(after.Runtimes) != len(before.Runtimes) {
		t.Fatalf("the previous complete snapshot changed on a failed sync: before %+v after %+v", before, after)
	}
	if after.Sync != string(catalog.SyncFailed) || after.LastError == "" {
		t.Fatalf("failed sync face = sync %q error %q, want failed with a reason", after.Sync, after.LastError)
	}
	if after.Stale != true {
		t.Fatal("a failed sync must read as stale")
	}

	// The other source kept its own generation and stayed healthy.
	harborAfter, err := service.Directory(context.Background(), "harbor", "", catalog.Grant{})
	if err != nil {
		t.Fatalf("harbor Directory: %v", err)
	}
	if harborAfter.Generation != harborBefore.Generation || harborAfter.Sync != string(catalog.SyncOK) {
		t.Fatalf("the second source changed with the first source's failure: %+v", harborAfter)
	}

	// Recovery publishes the next generation and clears the failure face.
	delete(upstream.CatalogErr, "ws-dev")
	report, err = service.Sync(context.Background(), "atlas")
	if err != nil {
		t.Fatalf("recovery Sync: %v", err)
	}
	if report.Generation != before.Generation+1 {
		t.Fatalf("recovery generation = %d, want %d", report.Generation, before.Generation+1)
	}
}

// TestRevokedCredentialKeepsTheSnapshotAndBlocksUse is the revocation clause:
// an unauthorized answer is a connection state, not an empty directory.
func TestRevokedCredentialKeepsTheSnapshotAndBlocksUse(t *testing.T) {
	upstream := seededUpstream()
	service, _ := newService(t, upstream, false)
	if _, err := service.Sync(context.Background(), "atlas"); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	upstream.ListErr = &catalog.UpstreamError{Op: "list workspaces", Status: 401, Kind: catalog.UpstreamUnauthorized, Message: "revoked"}
	if _, err := service.Sync(context.Background(), "atlas"); err == nil {
		t.Fatal("Sync answered success for a revoked credential")
	}

	view, ok, err := service.Get(context.Background(), "atlas")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if view.Connection != string(catalog.ConnectionUnauthorized) {
		t.Fatalf("connection = %q, want unauthorized", view.Connection)
	}
	if view.Agents != 4 || view.Workspaces != 2 {
		t.Fatalf("revocation wiped the cached directory: %+v", view)
	}
	if view.WriteBack.Enabled {
		t.Fatal("a revoked source still offers binding writes")
	}
}

// TestScopedWorkspaceMissingIsRevocation guards the scope check: a configured
// workspace the credential no longer answers for fails the sync instead of
// silently narrowing the directory.
func TestScopedWorkspaceMissingIsRevocation(t *testing.T) {
	upstream := seededUpstream()
	upstream.Workspaces = []catalog.Workspace{{ID: "ws-core", Name: "core"}}
	clock := &fixedClock{now: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)}
	service, err := catalog.New(catalog.Config{
		Upstream:    upstream,
		Credentials: catalogtest.Credentials{},
		Clock:       clock.Now,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	autoSync, period := false, 60
	if _, err := service.Register(context.Background(), catalog.Registration{
		ServerID:          "atlas",
		URL:               "https://atlas.example.test",
		CredentialRef:     "atlas-credential",
		AllowedWorkspaces: []string{"ws-core", "ws-dev"},
		AutoSync:          &autoSync,
		SyncPeriodSeconds: &period,
	}, "test"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := service.Sync(context.Background(), "atlas"); err == nil {
		t.Fatal("Sync accepted a scope the credential no longer answers for")
	}
	view, _, err := service.Get(context.Background(), "atlas")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if view.Connection != string(catalog.ConnectionUnauthorized) {
		t.Fatalf("connection = %q, want unauthorized for a narrowed scope", view.Connection)
	}
}

// TestAuditAndViewsCarryNoCredential keeps the Secret out of every face: the
// registration refuses a URL with userinfo, and no answer or audit row carries
// the token.
func TestAuditAndViewsCarryNoCredential(t *testing.T) {
	upstream := seededUpstream()
	service, _ := newService(t, upstream, false)

	if _, err := service.Register(context.Background(), catalog.Registration{
		ServerID:      "leaky",
		URL:           "https://admin:" + catalogtest.RefToken + "@leaky.example.test",
		CredentialRef: "leaky-credential",
	}, "test"); err == nil {
		t.Fatal("registration accepted a URL carrying a credential")
	}
	if _, err := service.Sync(context.Background(), "atlas"); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside", RuntimeID: "rt-foreman",
	}, "test"); err != nil {
		t.Fatalf("SetBinding: %v", err)
	}

	sources, err := service.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	audit, err := service.Audit(context.Background(), "", catalog.Grant{})
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	directory, err := service.Directory(context.Background(), "atlas", "", catalog.Grant{})
	if err != nil {
		t.Fatalf("Directory: %v", err)
	}
	encoded, err := json.Marshal(struct {
		Sources   []catalog.SourceView
		Audit     []catalog.AuditEntry
		Directory catalog.DirectoryView
	}{sources, audit, directory})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(encoded), catalogtest.RefToken) {
		t.Fatal("a response carries the source credential")
	}
	for _, source := range sources {
		if strings.Contains(source.URL, "@") {
			t.Fatalf("a source URL carries userinfo: %q", source.URL)
		}
	}
}

// TestSyncInFlightDoesNotUndoAConfirmedBinding is the read-back contract's
// ordering clause: a sync that read the old value before a binding was
// confirmed must not publish that stale value over the confirmation. The
// directory keeps the confirmed binding, still advances its generation, and
// leaves another source untouched.
func TestSyncInFlightDoesNotUndoAConfirmedBinding(t *testing.T) {
	upstream := seededUpstream()
	service, err := catalog.New(catalog.Config{
		Upstream:          upstream,
		Credentials:       catalogtest.Credentials{},
		Clock:             timeAt,
		ManagedRuntimeIDs: managedRuntimes,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	register(t, service, "atlas", false)
	register(t, service, "harbor", false)

	if _, err := service.Sync(context.Background(), "atlas"); err != nil {
		t.Fatalf("first Sync: %v", err)
	}
	if _, err := service.Sync(context.Background(), "harbor"); err != nil {
		t.Fatalf("harbor Sync: %v", err)
	}
	harborBefore, err := service.Directory(context.Background(), "harbor", "", catalog.Grant{})
	if err != nil {
		t.Fatalf("harbor Directory: %v", err)
	}
	managersBefore := managedCount(t, service)
	if managersBefore != 4 {
		t.Fatalf("managed agents before the confirmation = %d, want 4 (two per source)", managersBefore)
	}

	// Park the next sync inside its workspace read, after it has already
	// captured the pre-binding epoch and will read the old binding value.
	gate := make(chan struct{})
	reached := make(chan struct{}, 1)
	upstream.CatalogGate = gate
	upstream.CatalogGateAfter = upstream.CatalogCalls
	upstream.CatalogGateReached = reached
	synced := make(chan error, 1)
	go func() { _, err := service.Sync(context.Background(), "atlas"); synced <- err }()
	<-reached

	// Another administrator binds the agent while that sync is parked; the
	// read-back is what makes it true locally.
	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingApplied {
		t.Fatalf("binding result = %q (%s), want applied", outcome.Result, outcome.Detail)
	}
	managersAfter := managedCount(t, service)
	if managersAfter != managersBefore+1 {
		t.Fatalf("managed agents after the confirmation = %d, want %d", managersAfter, managersBefore+1)
	}

	upstream.CatalogGate = nil
	upstream.CatalogGateReached = nil
	close(gate)
	if err := <-synced; err != nil {
		t.Fatalf("parked Sync: %v", err)
	}

	directory, err := service.Directory(context.Background(), "atlas", "", catalog.Grant{})
	if err != nil {
		t.Fatalf("Directory: %v", err)
	}
	if directory.Generation != 2 {
		t.Fatalf("generation = %d, want the parked sync to publish generation 2", directory.Generation)
	}
	bound := agentRow(t, directory, "ws-core", "agent-outside")
	if bound.RuntimeID != "rt-foreman" || bound.Management != string(catalog.ManagedForeman) {
		t.Fatalf("agent row = %+v, want the read-back-confirmed managed binding", bound)
	}
	if got := managedCount(t, service); got != managersAfter {
		t.Fatalf("managed agents after the late sync = %d, want the confirmed %d", got, managersAfter)
	}

	harborAfter, err := service.Directory(context.Background(), "harbor", "", catalog.Grant{})
	if err != nil {
		t.Fatalf("harbor Directory: %v", err)
	}
	if harborAfter.Generation != harborBefore.Generation {
		t.Fatalf("harbor generation moved to %d while atlas synced", harborAfter.Generation)
	}
	if got := agentRow(t, harborAfter, "ws-core", "agent-outside"); got.RuntimeID != "rt-outside" {
		t.Fatalf("harbor agent row = %+v, want its own binding untouched", got)
	}
}

// agentRow finds one agent row in a directory answer.
func agentRow(t *testing.T, directory catalog.DirectoryView, workspaceID, agentID string) catalog.AgentView {
	t.Helper()
	for _, agent := range directory.Agents {
		if agent.WorkspaceID == workspaceID && agent.AgentID == agentID {
			return agent
		}
	}
	t.Fatalf("directory carries no %s/%s row", workspaceID, agentID)
	return catalog.AgentView{}
}
