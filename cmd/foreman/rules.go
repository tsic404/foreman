package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"runtime"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/platform"
	"github.com/tsic404/foreman/internal/release"
	"github.com/tsic404/foreman/internal/rules"
	"github.com/tsic404/foreman/internal/scheduler"
)

const (
	// EnvRulesStateFile names the rule document. It holds published rules,
	// the draft and the revision history, so a restart keeps what the operator
	// published.
	EnvRulesStateFile = "FOREMAN_RULES_STATE_FILE"

	defaultRulesStateFile = "/var/lib/foreman/rules.json"
)

// newRulesSurface builds the rule document, the shared resolver and the
// management routes. The resolver is wired to the same environment registry,
// catalog, release pointers, Agent configuration revisions and issue lookup
// the claim path uses, so preview and real dispatch answer from one code path.
func newRulesSurface(
	getenv func(string) string,
	environmentService *environment.Service,
	catalogService *catalog.Service,
	releaseService *release.Service,
	configService *agentconfig.Service,
	projects *platform.ProjectQueries,
	sourceID func(context.Context, string) (string, bool),
	jobNamespace string,
	log *slog.Logger,
) (*rules.Service, http.Handler, error) {
	store, err := rules.NewFileStore(statePath(getenv, EnvRulesStateFile, defaultRulesStateFile))
	if err != nil {
		return nil, nil, err
	}
	service, err := rules.New(rules.Config{
		Store:        store,
		Environments: environmentService,
		Catalog:      catalogService,
		Issues:       projects,
		Releases:     releaseService,
		Configs:      configService,
		SourceID:     sourceID,
		Sessions:     rules.NewMemorySessionIndex(1024),
		References:   environmentService.RuleReferenceIndex(),
		Platform:     runtime.GOOS + "/" + runtime.GOARCH,
		JobNamespace: jobNamespace,
		Logger:       log,
	})
	if err != nil {
		return nil, nil, err
	}
	admin, err := rules.NewAdminHandler(rules.AdminConfig{
		Service: service,
		Grant: func(r *http.Request) (catalog.Grant, bool) {
			scopes, wide, ok := observability.CallerScopes(r.Context())
			if !ok {
				return catalog.Grant{}, false
			}
			if wide {
				return catalog.Grant{}, true
			}
			converted := make([]catalog.Scope, 0, len(scopes))
			for _, scope := range scopes {
				converted = append(converted, catalog.Scope{
					ServerID: catalog.ServerID(scope.ServerID), WorkspaceID: scope.WorkspaceID,
				})
			}
			return catalogService.GrantForScopes(r.Context(), converted), true
		},
		Actor: func(*http.Request) string { return "ops-credential" },
	})
	if err != nil {
		return nil, nil, err
	}
	log.Info("rules.configured", "platform", runtime.GOOS+"/"+runtime.GOARCH)
	return service, admin, nil
}

// rulesBinder adapts the rule service to the scheduler's environment seam.
// The claim's source is the server the published rules are scoped by; the
// workspace alone is ambiguous when two servers expose the same id.
type rulesBinder struct {
	service *rules.Service
}

// Bind reserves the environment for one claim.
func (b rulesBinder) Bind(ctx context.Context, request scheduler.EnvironmentRequest) (scheduler.EnvironmentBinding, error) {
	binding, err := b.service.Bind(ctx, rules.Request{
		ServerID:    request.SourceID,
		WorkspaceID: request.WorkspaceID, AgentID: request.AgentID,
		IssueID: request.IssueID, ProjectID: request.ProjectID,
		PriorSessionID: request.PriorSessionID, AttemptID: request.AttemptID,
	})
	if err != nil {
		if errors.Is(err, rules.ErrEnvironmentSaturated) {
			return scheduler.EnvironmentBinding{}, scheduler.ErrEnvironmentSaturated
		}
		return scheduler.EnvironmentBinding{}, errors.Join(scheduler.ErrEnvironmentBlocked, err)
	}
	out := scheduler.EnvironmentBinding{
		EnvironmentID: binding.EnvironmentID, SlotID: binding.SlotID, RuleID: binding.RuleID,
		Platform:        binding.Resolution.CLIs.Platform,
		AgentBackend:    binding.Resolution.Provider,
		MulticaVersion:  binding.Resolution.CLIs.MulticaVersion,
		MulticaDigest:   binding.Resolution.CLIs.MulticaDigest,
		AgentVersion:    binding.Resolution.CLIs.AgentVersion,
		AgentDigest:     binding.Resolution.CLIs.AgentDigest,
		ConfigRevision:  binding.Resolution.Config.Revision,
		ConfigSecretRef: binding.Resolution.Config.SecretRef,
		ConfigFileName:  binding.Resolution.Config.FileName,
		ConfigDigest:    binding.Resolution.Config.Digest,
	}
	if choice := binding.Resolution.Environment; choice != nil {
		out.Endpoint = choice.Endpoint
		out.TLSRef = choice.TLSRef
		out.StateRoot = choice.StateRoot
		out.EnvironmentGen = choice.Generation
		out.EnvironmentIdentity = choice.Identity
	}
	return out, nil
}

// Release returns one claim's reservation.
func (b rulesBinder) Release(ctx context.Context, binding scheduler.EnvironmentBinding) error {
	return b.service.Release(ctx, rules.Binding{
		EnvironmentID: binding.EnvironmentID, SlotID: binding.SlotID, RuleID: binding.RuleID,
	})
}

// Isolate keeps the reservation of an attempt whose Job may still exist. It is
// the release for a terminated Job: a drain bound proves no Pod closure.
func (b rulesBinder) Isolate(ctx context.Context, binding scheduler.EnvironmentBinding) error {
	return b.service.Isolate(ctx, rules.Binding{
		EnvironmentID: binding.EnvironmentID, SlotID: binding.SlotID, RuleID: binding.RuleID,
	})
}

// ReleaseAfterPodClosed completes the release once the bound Job's pods are
// gone. The remote close was recorded by the Job-side release route; this is
// the second half of the two-sided gate.
func (b rulesBinder) ReleaseAfterPodClosed(ctx context.Context, binding scheduler.EnvironmentBinding) error {
	return b.service.ReleaseAfterPodClosed(ctx, rules.Binding{
		EnvironmentID: binding.EnvironmentID, SlotID: binding.SlotID, RuleID: binding.RuleID,
	})
}
