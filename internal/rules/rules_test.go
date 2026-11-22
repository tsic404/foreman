package rules

import (
	"context"
	"errors"
	"testing"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/release"
)

type fakeCatalog struct {
	view catalog.DirectoryView
	err  error
}

func (f *fakeCatalog) Directory(context.Context, catalog.ServerID, string, catalog.Grant) (catalog.DirectoryView, error) {
	return f.view, f.err
}

type fakeEnvironments struct {
	views    []environment.EnvironmentView
	acquires int
	releases int
	slots    map[string]string
}

func newFakeEnvironments(views ...environment.EnvironmentView) *fakeEnvironments {
	return &fakeEnvironments{views: views, slots: map[string]string{}}
}

func (f *fakeEnvironments) List(context.Context, environment.Filter, environment.Grant) ([]environment.EnvironmentView, error) {
	return append([]environment.EnvironmentView{}, f.views...), nil
}

func (f *fakeEnvironments) AcquireSlot(_ context.Context, id environment.ID, slotID string) (environment.Slot, error) {
	f.acquires++
	for _, view := range f.views {
		if view.ID != string(id) {
			continue
		}
		if view.FreeSlots <= 0 {
			return environment.Slot{}, environment.ErrFull
		}
		f.slots[string(id)] = slotID
		return environment.Slot{ID: slotID, State: environment.SlotActive}, nil
	}
	return environment.Slot{}, environment.ErrNotFound
}

func (f *fakeEnvironments) ReleaseSlot(_ context.Context, id environment.ID, slotID string, _ bool) (environment.Slot, error) {
	f.releases++
	delete(f.slots, string(id))
	return environment.Slot{ID: slotID}, nil
}

type fakeReleases struct {
	digests map[string]string
}

func (f fakeReleases) CurrentRelease(_ context.Context, _ string, kind release.Kind, adapter string) (string, map[string]string, bool) {
	if kind == release.KindAgent && adapter != "codex" {
		return "", nil, false
	}
	return "1.0.0", f.digests, true
}

type fakeConfigs struct {
	revision agentconfig.Revision
	found    bool
	err      error
}

func (f fakeConfigs) Resolve(context.Context, agentconfig.Scope) (agentconfig.Revision, bool, error) {
	return f.revision, f.found, f.err
}

type fakeIssues struct{}

func (fakeIssues) ByIssue(context.Context, string, string) (string, error) {
	return "mobile", nil
}

func testDirectory() catalog.DirectoryView {
	return catalog.DirectoryView{
		ServerID: "atlas", SyncedAt: "2026-01-01T00:00:00Z", Sync: "ok", Connection: "ok",
		Projects: []catalog.ProjectView{{ServerID: "atlas", WorkspaceID: "core", ProjectID: "mobile", Title: "mobile"}},
		Agents: []catalog.AgentView{{
			ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", Name: "dev",
			RuntimeID: "rt-1", RuntimeBound: true, Management: string(catalog.ManagedForeman),
		}},
		Runtimes: []catalog.RuntimeView{{
			ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-1", Provider: "codex",
		}},
	}
}

func newTestService(t *testing.T, envs *fakeEnvironments, directory catalog.DirectoryView) *Service {
	t.Helper()
	return newTestServiceWithConfig(t, Config{
		Environments: envs,
		Catalog:      &fakeCatalog{view: directory},
		Issues:       fakeIssues{},
		Releases:     fakeReleases{digests: map[string]string{"linux/amd64": "sha256:cli"}},
		Configs:      fakeConfigs{},
		Sessions:     NewMemorySessionIndex(16),
		Platform:     "linux/amd64",
		JobNamespace: "foreman",
	})
}

func newTestServiceWithConfig(t *testing.T, cfg Config) *Service {
	t.Helper()
	service, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return service
}

func envView(id string, free int, capabilities ...string) environment.EnvironmentView {
	return environment.EnvironmentView{
		ID: id, Endpoint: "rsbxd://" + id, TrustDomain: "build",
		Capabilities: capabilities, Concurrency: free + 1, FreeSlots: free,
		Phase: string(environment.PhaseActive), Schedulable: true,
		Verification: environment.VerificationView{State: string(environment.VerificationOK)},
	}
}

func combinedRule() Rule {
	return Rule{
		ID: "a5", ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", ProjectID: "mobile",
		EnvironmentID: "build-a", Enabled: true,
	}
}

func agentRule() Rule {
	return Rule{ID: "a3", ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", EnvironmentID: "build-b", Enabled: true}
}

func projectRule() Rule {
	return Rule{ID: "a4", ServerID: "atlas", WorkspaceID: "core", ProjectID: "mobile", EnvironmentID: "build-c", Enabled: true}
}

func draftAndPublish(t *testing.T, service *Service, rules []Rule, base uint64) Revision {
	t.Helper()
	ctx := context.Background()
	if _, err := service.Draft(ctx, base, rules, catalog.Grant{}, "test"); err != nil {
		t.Fatalf("Draft: %v", err)
	}
	revision, err := service.Publish(ctx, base, catalog.Grant{}, "test")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return revision
}

func TestPublishRejectsSameLevelAgentProjectConflict(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 1), envView("build-b", 1), envView("build-c", 1))
	service := newTestService(t, envs, testDirectory())
	ctx := context.Background()
	if _, err := service.Draft(ctx, 0, []Rule{agentRule(), projectRule()}, catalog.Grant{}, "test"); err != nil {
		t.Fatalf("Draft: %v", err)
	}
	_, err := service.Publish(ctx, 0, catalog.Grant{}, "test")
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("Publish err = %v, want a validation refusal", err)
	}
	if validation.Findings[0].Code != "rule_conflict" {
		t.Fatalf("first finding = %+v, want rule_conflict", validation.Findings[0])
	}
	state, err := service.State(ctx)
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if state.Revision != 0 || len(state.Published) != 0 {
		t.Fatalf("a refused publish moved the published set: revision=%d rules=%d", state.Revision, len(state.Published))
	}
}

func TestACombinedRuleForAnotherPairDoesNotMaskAConflict(t *testing.T) {
	directory := testDirectory()
	directory.Projects = append(directory.Projects, catalog.ProjectView{
		ServerID: "atlas", WorkspaceID: "core", ProjectID: "web", Title: "web",
	})
	envs := newFakeEnvironments(envView("build-a", 1), envView("build-b", 1), envView("build-c", 1))
	service := newTestService(t, envs, directory)
	ctx := context.Background()
	webRule := Rule{
		ID: "a6", ServerID: "atlas", WorkspaceID: "core", ProjectID: "web",
		EnvironmentID: "build-c", Enabled: true,
	}
	if _, err := service.Draft(ctx, 0, []Rule{combinedRule(), agentRule(), webRule}, catalog.Grant{}, "test"); err != nil {
		t.Fatalf("Draft: %v", err)
	}
	_, err := service.Publish(ctx, 0, catalog.Grant{}, "test")
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("Publish err = %v, want a validation refusal", err)
	}
	if validation.Findings[0].Code != "rule_conflict" {
		t.Fatalf("first finding = %+v, want rule_conflict", validation.Findings[0])
	}
}

func TestStaleSourceIsUnknown(t *testing.T) {
	directory := testDirectory()
	directory.Stale = true
	service := newTestService(t, newFakeEnvironments(envView("build-a", 1)), directory)
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)
	preview, err := service.Preview(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if preview.Outcome != OutcomeUnknown {
		t.Fatalf("outcome = %q, want unknown", preview.Outcome)
	}
	if len(preview.Findings) == 0 || preview.Findings[0].Code != "source_stale" {
		t.Fatalf("findings = %+v, want source_stale", preview.Findings)
	}
}

func TestNewRebuildsRuleReferencesFromTheStore(t *testing.T) {
	store := NewMemoryStore()
	references := environment.NewMemoryRuleReferences()
	cfg := Config{
		Store:        store,
		Environments: newFakeEnvironments(envView("build-a", 1)),
		Catalog:      &fakeCatalog{view: testDirectory()},
		References:   references,
	}
	service, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)

	restarted := environment.NewMemoryRuleReferences()
	cfg.References = restarted
	if _, err := New(cfg); err != nil {
		t.Fatalf("New after restart: %v", err)
	}
	rules, err := restarted.ReferencingRules(context.Background(), environment.ID("build-a"))
	if err != nil {
		t.Fatalf("ReferencingRules: %v", err)
	}
	if len(rules) != 1 || rules[0] != "a5" {
		t.Fatalf("restarted references = %v, want [a5]", rules)
	}
}

func TestCombinedRuleWinsAndDisablingItExposesTheConflict(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 1), envView("build-b", 1), envView("build-c", 1))
	service := newTestService(t, envs, testDirectory())
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule(), agentRule(), projectRule()}, 0)

	preview, err := service.Preview(ctx, Request{ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1"}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if preview.RuleID != "a5" || preview.Environment == nil || preview.Environment.ID != "build-a" {
		t.Fatalf("preview = rule %q env %+v, want a5/build-a", preview.RuleID, preview.Environment)
	}

	// Disabling the combined rule leaves the single-dimension pair, which now
	// conflicts: the draft saves, the publish is refused.
	if _, err := service.Draft(ctx, 1, []Rule{agentRule(), projectRule()}, catalog.Grant{}, "test"); err != nil {
		t.Fatalf("Draft: %v", err)
	}
	_, err = service.Publish(ctx, 1, catalog.Grant{}, "test")
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("Publish err = %v, want a validation refusal", err)
	}
	still, err := service.Preview(ctx, Request{ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1"}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if still.RuleID != "a5" {
		t.Fatalf("published rules stopped working after a refused publish: rule=%q", still.RuleID)
	}
}

func TestPublishRefusesAStaleBaseRevision(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 1))
	service := newTestService(t, envs, testDirectory())
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)
	if _, err := service.Draft(ctx, 0, []Rule{combinedRule()}, catalog.Grant{}, "test"); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("Draft err = %v, want ErrRevisionConflict", err)
	}
	if _, err := service.Publish(ctx, 0, catalog.Grant{}, "test"); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("Publish err = %v, want ErrRevisionConflict", err)
	}
}

func TestPreviewIsReadOnly(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 1))
	service := newTestService(t, envs, testDirectory())
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)
	before := len(envs.slots)
	preview, err := service.Preview(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1", PriorSessionID: "session-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if preview.Outcome != OutcomeReady {
		t.Fatalf("outcome = %q, want ready (%+v)", preview.Outcome, preview.Findings)
	}
	if envs.acquires != 0 || envs.releases != 0 || len(envs.slots) != before {
		t.Fatalf("preview touched the slot ledger: acquires=%d releases=%d slots=%d", envs.acquires, envs.releases, len(envs.slots))
	}
	if _, ok := service.cfg.Sessions.EnvironmentForSession(ctx, "core", "session-1"); ok {
		t.Fatal("preview recorded a session binding")
	}
}

func TestBindReservesTheSameDecisionPreviewReports(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 1))
	service := newTestService(t, envs, testDirectory())
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)
	request := Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
		PriorSessionID: "session-1", AttemptID: "attempt-1",
	}
	preview, err := service.Preview(ctx, request, catalog.Grant{})
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	binding, err := service.Bind(ctx, request)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if binding.EnvironmentID != preview.Environment.ID || binding.RuleID != preview.RuleID {
		t.Fatalf("bind = %+v, preview = %+v: the two callers disagreed", binding, preview)
	}
	if binding.SlotID != "attempt-1" || envs.acquires != 1 {
		t.Fatalf("bind did not reserve exactly one slot: slot=%q acquires=%d", binding.SlotID, envs.acquires)
	}
	if err := service.Release(ctx, binding); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if envs.releases != 1 {
		t.Fatalf("releases = %d, want 1", envs.releases)
	}
}

func TestContinuationWaitsForItsOriginalEnvironment(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 0), envView("build-b", 1))
	service := newTestService(t, envs, testDirectory())
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{{
		ID: "workspace", ServerID: "atlas", WorkspaceID: "core", TrustDomain: "build", Enabled: true,
	}}, 0)
	service.cfg.Sessions.RecordSession(ctx, "core", "session-1", "build-a")
	_, err := service.Bind(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
		PriorSessionID: "session-1", AttemptID: "attempt-1",
	})
	if !errors.Is(err, ErrEnvironmentSaturated) {
		t.Fatalf("Bind err = %v, want ErrEnvironmentSaturated", err)
	}
	if envs.acquires != 0 {
		t.Fatalf("a continuation fell back to another endpoint: acquires=%d", envs.acquires)
	}
}

func TestFirstRunPrefersCapabilitiesThenFreeSlots(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 0, "nix"), envView("build-b", 1, "nix"), envView("build-c", 2))
	service := newTestService(t, envs, testDirectory())
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{{
		ID: "workspace", ServerID: "atlas", WorkspaceID: "core",
		PreferCapabilities: []string{"nix"}, Enabled: true,
	}}, 0)
	binding, err := service.Bind(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1", AttemptID: "attempt-1",
	})
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if binding.EnvironmentID != "build-b" {
		t.Fatalf("environment = %q, want build-b (preference beats raw free slots)", binding.EnvironmentID)
	}
}
