package main

import (
	"context"
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
