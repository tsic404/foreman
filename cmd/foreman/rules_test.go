package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/release"
	"github.com/tsic404/foreman/internal/rules"
	"github.com/tsic404/foreman/internal/scheduler"
)

// twoSourceCatalog answers one directory per server. Both directories describe
// the same workspace id, which is the ambiguity the binder must not resolve by
// guesswork.
type twoSourceCatalog map[catalog.ServerID]catalog.DirectoryView

func (c twoSourceCatalog) Directory(_ context.Context, id catalog.ServerID, workspaceID string, _ catalog.Grant) (catalog.DirectoryView, error) {
	view, ok := c[id]
	if !ok || (workspaceID != "" && workspaceID != "ws-shared") {
		return catalog.DirectoryView{}, catalog.ErrSourceNotFound
	}
	return view, nil
}

// sourceScopedEnvironments applies the resolver's source grant the way the
// registry does: an environment allowed for atlas is invisible to harbor.
type sourceScopedEnvironments struct {
	views []environment.EnvironmentView
}

func (e *sourceScopedEnvironments) List(_ context.Context, _ environment.Filter, grant environment.Grant) ([]environment.EnvironmentView, error) {
	out := make([]environment.EnvironmentView, 0, len(e.views))
	for _, view := range e.views {
		if grant.DeploymentWide {
			out = append(out, view)
			continue
		}
		allowed := false
		for _, source := range view.AllowedSources {
			for _, granted := range grant.Sources {
				if source == granted {
					allowed = true
				}
			}
		}
		if allowed {
			out = append(out, view)
		}
	}
	return out, nil
}

func (e *sourceScopedEnvironments) AcquireSlot(_ context.Context, _ environment.ID, slotID string) (environment.Slot, error) {
	return environment.Slot{ID: slotID, State: environment.SlotActive}, nil
}

func (e *sourceScopedEnvironments) ReleaseSlot(_ context.Context, _ environment.ID, slotID string, _ bool) (environment.Slot, error) {
	return environment.Slot{ID: slotID}, nil
}

func (e *sourceScopedEnvironments) ReleaseSlotOnPodClosed(_ context.Context, _ environment.ID, slotID string) (environment.Slot, error) {
	return environment.Slot{ID: slotID}, nil
}

func (e *sourceScopedEnvironments) IsolateSlot(_ context.Context, _ environment.ID, slotID string) (environment.Slot, error) {
	return environment.Slot{ID: slotID, State: environment.SlotIsolated}, nil
}

type sharedWorkspaceIssues struct{}

func (sharedWorkspaceIssues) ByIssue(context.Context, string, string) (string, error) {
	return "mobile", nil
}

type publishedCLIs struct{}

func (publishedCLIs) CurrentRelease(context.Context, string, release.Kind, string) (string, map[string]string, bool) {
	return "1.0.0", map[string]string{"linux/amd64": "sha256:cli"}, true
}

func rulesBinderTestView(serverID string) catalog.DirectoryView {
	return catalog.DirectoryView{
		ServerID: serverID, SyncedAt: "2026-01-01T00:00:00Z",
		Sync: string(catalog.SyncOK), Connection: string(catalog.ConnectionOK),
		Projects: []catalog.ProjectView{{
			ServerID: serverID, WorkspaceID: "ws-shared", ProjectID: "mobile", Title: "mobile",
		}},
		Agents: []catalog.AgentView{{
			ServerID: serverID, WorkspaceID: "ws-shared", AgentID: "agent-1", Name: "agent",
			RuntimeID: "rt-1", RuntimeBound: true, Management: string(catalog.ManagedForeman),
		}},
		Runtimes: []catalog.RuntimeView{{
			ServerID: serverID, WorkspaceID: "ws-shared", RuntimeID: "rt-1", Provider: "codex",
		}},
	}
}

func rulesBinderTestEnvironment(id, source string) environment.EnvironmentView {
	return environment.EnvironmentView{
		ID: id, Endpoint: "rsbxd://" + id, TrustDomain: "build",
		AllowedSources: []string{source}, Capabilities: []string{"nix"},
		Concurrency: 2, FreeSlots: 2,
		Phase: string(environment.PhaseActive), Schedulable: true,
		Verification: environment.VerificationView{State: string(environment.VerificationOK)},
	}
}

// TestRulesBinderKeepsTwoServersSharingAWorkspaceApart pins the claim's source
// identity all the way into the resolver. The workspace-only lookup is wired
// to fail exactly as ClientSet.For does when two servers expose the same
// workspace id; each claim must still select its own source's rule and
// environment.
func TestRulesBinderKeepsTwoServersSharingAWorkspaceApart(t *testing.T) {
	ctx := context.Background()
	environments := &sourceScopedEnvironments{views: []environment.EnvironmentView{
		rulesBinderTestEnvironment("build-a", "atlas"),
		rulesBinderTestEnvironment("build-b", "harbor"),
	}}
	service, err := rules.New(rules.Config{
		Environments: environments,
		Catalog: twoSourceCatalog{
			"atlas":  rulesBinderTestView("atlas"),
			"harbor": rulesBinderTestView("harbor"),
		},
		Issues:   sharedWorkspaceIssues{},
		Releases: publishedCLIs{},
		// The workspace-only fallback answers "ambiguous", as the production
		// ClientSet.For does for two clients on one workspace id.
		SourceID: func(context.Context, string) (string, bool) { return "", false },
		Sessions: rules.NewMemorySessionIndex(8),
		Platform: "linux/amd64", JobNamespace: "foreman",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("rules.New: %v", err)
	}
	if _, err := service.Draft(ctx, 0, []rules.Rule{
		{ID: "atlas-agent", ServerID: "atlas", WorkspaceID: "ws-shared", AgentID: "agent-1", EnvironmentID: "build-a", Enabled: true},
		{ID: "harbor-agent", ServerID: "harbor", WorkspaceID: "ws-shared", AgentID: "agent-1", EnvironmentID: "build-b", Enabled: true},
	}, catalog.Grant{}, "test"); err != nil {
		t.Fatalf("Draft: %v", err)
	}
	if _, err := service.Publish(ctx, 0, catalog.Grant{}, "test"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	binder := rulesBinder{service: service}
	for _, tc := range []struct{ source, environment, rule string }{
		{source: "atlas", environment: "build-a", rule: "atlas-agent"},
		{source: "harbor", environment: "build-b", rule: "harbor-agent"},
	} {
		binding, err := binder.Bind(ctx, scheduler.EnvironmentRequest{
			SourceID: tc.source, WorkspaceID: "ws-shared", AgentID: "agent-1",
			IssueID: "issue-1", AttemptID: "attempt-" + tc.source,
		})
		if err != nil {
			t.Fatalf("Bind(%s): %v; a claim naming its source must not be refused as %v",
				tc.source, err, scheduler.ErrEnvironmentBlocked)
		}
		if binding.EnvironmentID != tc.environment || binding.RuleID != tc.rule {
			t.Fatalf("Bind(%s) = environment %q rule %q, want %q/%q",
				tc.source, binding.EnvironmentID, binding.RuleID, tc.environment, tc.rule)
		}
	}
}
