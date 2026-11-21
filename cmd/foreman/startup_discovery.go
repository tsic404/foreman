package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/bootstrap"
	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/coordination"
	"github.com/tsic404/foreman/internal/discovery"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/release"
	"github.com/tsic404/foreman/internal/scope"
	"github.com/tsic404/foreman/internal/state"
	"github.com/tsic404/foreman/internal/tenure"
)

// startupDiscoverer is the cold-start declaration source. The production
// composition wires only this seam: there is no scheduler or Job client on
// the path, so a cold start cannot create a probe Job.
type startupDiscoverer interface {
	Discover(context.Context) (map[string]registry.Discovery, error)
}

// resolveStartupDiscoveries is the production startup assembly. The holder
// merges its in-process declaration with any published one; a follower adopts
// the published declaration; a process without coordination reads the
// control-plane declaration directly.
func resolveStartupDiscoveries(
	ctx context.Context,
	leader *coordination.Leader,
	coord state.CoordStore,
	discoverer startupDiscoverer,
	clients []*proxy.Client,
	window time.Duration,
	log *slog.Logger,
) (map[string]registry.Discovery, error) {
	if leader != nil {
		return bootstrap.StartupDiscoveries(ctx, leader, coord, discoverer.Discover, clients, window, log)
	}
	return discoverer.Discover(ctx)
}

// controlPlaneDiscovery builds the cold-start declaration from verified CLI
// definitions and their registered configuration scopes. It never creates a
// probe Job: a source with no verified runtime is left unavailable.
type controlPlaneDiscovery struct {
	catalog   *catalog.Service
	releases  *release.Service
	discovery *discovery.Service
	configs   *agentconfig.Service
	clients   *proxy.ClientSet
	log       *slog.Logger
	gate      tenure.Gate
}

func newControlPlaneDiscovery(
	catalogService *catalog.Service,
	releaseService *release.Service,
	discoveryService *discovery.Service,
	configService *agentconfig.Service,
	clientSet *proxy.ClientSet,
	log *slog.Logger,
	gate tenure.Gate,
) *controlPlaneDiscovery {
	return &controlPlaneDiscovery{
		catalog: catalogService, releases: releaseService,
		discovery: discoveryService, configs: configService,
		clients: clientSet, log: log, gate: gate,
	}
}

// Discover implements the bootstrap cold-start seam.
func (d *controlPlaneDiscovery) Discover(ctx context.Context) (map[string]registry.Discovery, error) {
	configs, err := d.configs.List(ctx, "", "")
	if err != nil {
		return nil, err
	}
	trusted := d.trustedAdapters()
	out := map[string]registry.Discovery{}
	for _, sourceID := range d.sourceIDs() {
		// Between sources: a tenure lost after the previous source must not
		// register the next one under the new holder.
		if err := tenure.Check(d.gate); err != nil {
			d.log.Warn("runtime.control_plane_discovery_deferred", "reason", "leader_lease_lost")
			return nil, err
		}
		found, derr := d.discoverSource(ctx, sourceID, configs, trusted)
		if derr != nil {
			if errors.Is(derr, tenure.ErrLost) {
				d.log.Warn("runtime.control_plane_discovery_deferred", "reason", "leader_lease_lost")
				return nil, derr
			}
			var registration *sourceRegistrationError
			if errors.As(derr, &registration) {
				// Registration is a source-scoped network action: one
				// source's failure must not stop the others from declaring.
				d.log.Error("runtime.control_plane_registration_failed",
					"server_id", sourceID, "err", registration.Error())
				continue
			}
			// A declaration-store read failure is global: fail fast instead
			// of reporting a false "nothing declared".
			return nil, derr
		}
		for workspaceID, declaration := range found {
			out[scope.Key(sourceID, workspaceID)] = declaration
		}
	}
	// After the last source: only a holder that still holds the lease may
	// return this declaration set.
	if err := tenure.Check(d.gate); err != nil {
		d.log.Warn("runtime.control_plane_discovery_deferred", "reason", "leader_lease_lost")
		return nil, err
	}
	return out, nil
}

// DiscoverSource resolves one source's declarations, keyed by workspace id.
// It is the per-source seam the recovery loop retries without touching the
// healthy sources; a source with no verified declaration returns an empty
// map and no error.
func (d *controlPlaneDiscovery) DiscoverSource(ctx context.Context, sourceID string) (map[string]registry.Discovery, error) {
	if err := tenure.Check(d.gate); err != nil {
		return nil, err
	}
	configs, err := d.configs.List(ctx, "", "")
	if err != nil {
		return nil, err
	}
	return d.discoverSource(ctx, sourceID, configs, d.trustedAdapters())
}

// RunStartupProbeInSource implements the bootstrap per-source discovery seam.
func (d *controlPlaneDiscovery) RunStartupProbeInSource(ctx context.Context, sourceID string) (map[string]registry.Discovery, error) {
	return d.DiscoverSource(ctx, sourceID)
}

func (d *controlPlaneDiscovery) trustedAdapters() map[string]bool {
	trusted := map[string]bool{}
	for _, status := range d.discovery.Adapters() {
		trusted[status.Name] = status.Trusted
	}
	return trusted
}

func (d *controlPlaneDiscovery) sourceIDs() []string {
	var out []string
	seen := map[string]bool{}
	for _, client := range d.clients.All() {
		sourceID := client.SourceID()
		if seen[sourceID] {
			continue
		}
		seen[sourceID] = true
		out = append(out, sourceID)
	}
	return out
}

// sourceRegistrationError marks a C1 failure for one source: it is retried by
// the recovery loop, while a declaration-store failure stays global.
type sourceRegistrationError struct {
	sourceID string
	err      error
}

func (e *sourceRegistrationError) Error() string {
	return fmt.Sprintf("register %s: %v", e.sourceID, e.err)
}

func (e *sourceRegistrationError) Unwrap() error { return e.err }

func (d *controlPlaneDiscovery) discoverSource(
	ctx context.Context,
	sourceID string,
	configs []agentconfig.View,
	trusted map[string]bool,
) (map[string]registry.Discovery, error) {
	out := map[string]registry.Discovery{}
	for _, client := range d.clients.All() {
		if client.SourceID() != sourceID {
			continue
		}
		declaration, err := d.resolveClient(ctx, client, configs, trusted)
		if err != nil {
			return nil, err
		}
		if len(declaration.Runtimes) == 0 {
			continue
		}
		// C1 creates the daemon identity upstream. Resolution reads published
		// facts with no external side effect, but the registration is
		// holder-only: bind it to the tenure so a holder that loses the lease
		// cannot finish this source's C1 for the next holder.
		if err := tenure.Check(d.gate); err != nil {
			return nil, err
		}
		registerCtx, stopRegister, registerLost := tenure.Bound(ctx, d.gate)
		err = client.RegisterWithDiscovery(registerCtx, declaration)
		lost := registerLost()
		stopRegister()
		if lost {
			return nil, tenure.ErrLost
		}
		if err != nil {
			return nil, &sourceRegistrationError{sourceID: sourceID, err: err}
		}
		// After registration: only the holder that still holds the lease may
		// hand the declaration on to the caller.
		if err := tenure.Check(d.gate); err != nil {
			return nil, err
		}
		out[client.WorkspaceID()] = declaration
	}
	return out, nil
}

// resolveClient derives one client's declaration from the published CLI and
// configuration facts. It performs no upstream write: the caller decides
// whether this instance may still register.
func (d *controlPlaneDiscovery) resolveClient(
	ctx context.Context,
	client *proxy.Client,
	configs []agentconfig.View,
	trusted map[string]bool,
) (registry.Discovery, error) {
	sourceID, ok := d.catalog.SourceIDForURL(ctx, client.ServerURL())
	if !ok {
		return registry.Discovery{}, nil
	}
	declaration := registry.Discovery{}
	if version, ok := d.releases.CurrentVersion(ctx, string(sourceID), release.KindMultica, ""); ok {
		declaration.CLIVersion = version
	}
	verified, err := d.releases.VerifiedAgentReleases(ctx, string(sourceID))
	if err != nil {
		return registry.Discovery{}, err
	}
	seen := map[string]bool{}
	for _, candidate := range verified {
		if !trusted[candidate.Adapter] {
			continue
		}
		if err := d.verifyBound(ctx, string(sourceID), candidate); err != nil {
			d.log.Warn("runtime.bound_verification_failed",
				"workspace_id", client.WorkspaceID(),
				"server_id", string(sourceID),
				"cli_id", candidate.CLIID,
				"version", candidate.Version,
				"err", err.Error())
			continue
		}
		profiles := configuredProfiles(configs, string(sourceID), client.WorkspaceID(), candidate.Adapter)
		if len(profiles) == 0 {
			d.log.Warn("runtime.configuration_unavailable",
				"workspace_id", client.WorkspaceID(),
				"server_id", string(sourceID),
				"adapter", candidate.Adapter,
				"detail", "no registered Agent configuration scope is available; the runtime is not declared")
			continue
		}
		for _, profile := range profiles {
			key := registry.RuntimeIdentityKey(candidate.Adapter, profile)
			if seen[key] {
				continue
			}
			seen[key] = true
			declaration.Runtimes = append(declaration.Runtimes, registry.DiscoveredRuntime{
				Name: candidate.Adapter, Type: candidate.Adapter,
				Version: candidate.Version, ProfileID: profile,
			})
		}
	}
	if len(declaration.Runtimes) == 0 {
		d.log.Warn("runtime.declaration_unavailable",
			"workspace_id", client.WorkspaceID(),
			"server_id", string(sourceID),
			"detail", "no verified Agent CLI definition is available; the workspace is not served")
		return declaration, nil
	}
	return declaration, nil
}

// verifyBound probes the exact published payload on the platform the
// restricted adapter executes on. A global adapter probe cannot authorize a
// different release's digest.
func (d *controlPlaneDiscovery) verifyBound(ctx context.Context, serverID string, candidate release.VerifiedAgentRelease) error {
	platform := runtime.GOOS + "/" + runtime.GOARCH
	artifact, ok := release.ArtifactFor(candidate.Artifacts, platform)
	if !ok {
		return fmt.Errorf("no verified artifact for %s", platform)
	}
	body, err := d.releases.VerifiedArtifactByDomain(ctx, serverID, artifact.Digest)
	if err != nil {
		return fmt.Errorf("read the verified artifact: %w", err)
	}
	dir, err := os.MkdirTemp("", "foreman-runtime-probe-")
	if err != nil {
		return fmt.Errorf("create the runtime probe directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	path := filepath.Join(dir, "agent-cli")
	if err := os.WriteFile(path, body, 0o500); err != nil {
		return fmt.Errorf("materialize the verified artifact: %w", err)
	}
	return d.discovery.VerifyBoundAdapter(ctx, candidate.Adapter, path, candidate.Version)
}

// configuredProfiles lists the scopes that actually have a registered
// configuration revision. A missing scope is not declared as a runtime: the
// model/capability surface has no bound input for it.
func configuredProfiles(configs []agentconfig.View, serverID, workspaceID, adapter string) []string {
	seen := map[string]bool{}
	out := make([]string, 0)
	for _, config := range configs {
		if config.ServerID != serverID || config.WorkspaceID != workspaceID ||
			!strings.EqualFold(config.Backend, adapter) {
			continue
		}
		if seen[config.Profile] {
			continue
		}
		seen[config.Profile] = true
		out = append(out, config.Profile)
	}
	return out
}
