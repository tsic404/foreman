package main

import (
	"context"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// releaseClaimFacts freezes the configuration revision a claim resolves and
// binds the attempt to it. The CLI versions stay unknown: the Job does not
// receive the management pointers, so claiming them as executed facts would
// mislabel an older image.
type releaseClaimFacts struct {
	configs  *agentconfig.Service
	sourceID func(context.Context, string) (catalog.ServerID, bool)
}

// Facts implements scheduler.ClaimFactsFunc.
func (f releaseClaimFacts) Facts(ctx context.Context, workspaceID, _ string, runtimeIdentity, attemptID string) scheduler.ClaimFacts {
	var facts scheduler.ClaimFacts
	if f.configs == nil || f.sourceID == nil {
		return facts
	}
	serverID, ok := f.sourceID(ctx, workspaceID)
	if !ok {
		return facts
	}
	provider, profile, ok := registry.SplitRuntimeIdentity(runtimeIdentity)
	if !ok {
		return facts
	}
	scope := agentconfig.Scope{
		ServerID: string(serverID), WorkspaceID: workspaceID,
		Backend: provider, Profile: profile,
	}
	revision, found, err := f.configs.Resolve(ctx, scope)
	switch {
	case err != nil:
		// A lookup failure is unknown, never a known "no configuration".
		facts.ConfigKnown = false
	case !found:
		// The scope resolved and no document was written: zero is known.
		facts.ConfigKnown = true
		facts.ConfigRevision = 0
	default:
		if _, bindErr := f.configs.Bind(ctx, scope, attemptID, revision.Revision); bindErr != nil {
			facts.ConfigKnown = false
			break
		}
		facts.ConfigRevision = revision.Revision
		facts.ConfigKnown = true
	}
	return facts
}
