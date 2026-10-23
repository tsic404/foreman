package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/workspace"
)

// policyWorkspaceRefs is the partition-key resolution set for the policy
// gate: one ref per workspace with the display name the startup already knows
// (user-scoped form = the discovery answer; workspace-scoped form = the C-17
// self-check answer). A workspace-scoped client whose C-17 never seeded the
// name (transient failure, already warned) contributes id only — its name
// keys must not read as absent and refuse startup (§1.1, AC-86). No upstream
// call: the set is whatever the startup already knows.
func policyWorkspaceRefs(set *proxy.ClientSet, workspaces workspace.Set) []jobbuilder.WorkspaceRef {
	refs := make([]jobbuilder.WorkspaceRef, 0, workspaces.Len())
	for _, identity := range workspaces.All() {
		c, ok := set.For(identity.WorkspaceID)
		if !ok {
			continue
		}
		ref := jobbuilder.WorkspaceRef{ID: c.WorkspaceID()}
		if name, seeded := c.WorkspaceName(); seeded {
			ref.Name = name
		}
		refs = append(refs, ref)
	}
	return refs
}

// discoverUserScope resolves the user-scoped credential form into its
// workspace set (ADR-049 decision 1): C-17 once, the manifest rule applied,
// then one derived (credential, daemon identity) pair per discovered
// workspace. Discovery unavailable (network, auth rejection, empty set)
// returns an error — the caller returns it, so the process exits non-zero and
// never serves a half-configured set (§1.1, AC-96). The failure event carries
// the reason and the credential scope; the success event carries the source,
// the set and the manifest-excluded workspaces (§9.1).
func discoverUserScope(ctx context.Context, deployment workspace.Deployment, cfg proxy.Config, metrics proxy.Metrics, log *slog.Logger) (workspace.Set, *proxy.Discovery, error) {
	discoveryClient := proxy.NewClient(cfg, deployment.UserToken,
		proxy.WithMetrics(metrics),
		proxy.WithLogger(log))
	discovered, err := discoveryClient.DiscoverWorkspaces(ctx, deployment.Manifest)
	if err != nil {
		proxy.LogDiscoveryFailure(log, workspace.ScopeUser.String(), err)
		return workspace.Set{}, nil, fmt.Errorf("workspace discovery: %w", err)
	}
	proxy.LogDiscoveryEvent(log, discovered)
	set, err := workspace.NewSet(deployment.Identities(discovered.Set))
	if err != nil {
		return workspace.Set{}, nil, fmt.Errorf("derive workspace identities: %w", err)
	}
	return set, &discovered, nil
}

// runStartupSelfCheck runs the startup credential checks (§1.1):
//
//   - C-18 profile snapshot per workspace — never fatal (S-3 answers 404/503
//     and the probe continues with the builtin runtime, §1.6).
//   - C-17 check for the workspace-scoped credential form: the answer must
//     equal the configured workspace, and every entry must carry a non-empty
//     display name (whitespace-only counts as empty).
//
// Three findings refuse startup:
//   - the C-17 set is not exactly the configured workspace;
//   - the credential is rejected outright (401/403);
//   - a C-17 answer entry carries no display name.
//
// The first two log stage=workspace_credential; an unnamed answer logs
// stage=workspace_discovery, so the event names which face failed.
//
// A transient upstream failure (network, 5xx) is none of them — it is logged
// as workspace.credential_check_failed and the workspace's control loops
// converge with the §1.5 backoff, exactly as they do at runtime (AC-86).
// Making it fatal would turn a server blip into CrashLoopBackOff for every
// existing single-workspace deployment.
//
// The workspace-scoped form's §9.1 discovery event is emitted here, after the
// checks seeded each display name.
func runStartupSelfCheck(ctx context.Context, scope workspace.CredentialScope, clients []*proxy.Client, log *slog.Logger) error {
	for _, c := range clients {
		if err := c.RuntimeProfiles(ctx); err != nil {
			log.Warn("runtime.profiles_snapshot_failed", "workspace_id", c.WorkspaceID(), "err", err.Error())
		}
		if scope != workspace.ScopeWorkspace {
			continue
		}
		if err := c.ValidateWorkspaceScope(ctx); err != nil {
			if !proxy.IsStartupFatal(err) {
				// Transient: warn and let the workspace's loops retry (§1.5).
				log.Warn("workspace.credential_check_failed", "workspace_id", c.WorkspaceID(), "err", err.Error())
				continue
			}
			stage := "workspace_credential"
			var mismatch *proxy.WorkspaceMismatchError
			var answer *proxy.DiscoveryAnswerError
			if errors.As(err, &answer) {
				stage = "workspace_discovery"
			}
			fields := []any{"stage", stage, "workspace_id", c.WorkspaceID(), "err", err.Error()}
			if errors.As(err, &mismatch) {
				fields = append(fields, "visible_workspaces", mismatch.Visible)
			}
			log.Error("foreman.startup_aborted", fields...)
			return fmt.Errorf("workspace %s startup self-check failed: %w", c.WorkspaceID(), err)
		}
	}
	if scope == workspace.ScopeWorkspace {
		set := make([]workspace.Ref, 0, len(clients))
		for _, c := range clients {
			id, name := c.Workspace()
			set = append(set, workspace.Ref{ID: id, Name: name})
		}
		proxy.LogDiscoveryEvent(log, proxy.Discovery{
			Source: proxy.DiscoverySourceWorkspaceScope,
			Scope:  workspace.ScopeWorkspace.String(),
			Set:    set,
		})
	}
	return nil
}
