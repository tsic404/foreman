package main

import (
	"context"
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
	"github.com/tsic404/foreman/internal/state"
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
}

func newControlPlaneDiscovery(
	catalogService *catalog.Service,
	releaseService *release.Service,
	discoveryService *discovery.Service,
	configService *agentconfig.Service,
	clientSet *proxy.ClientSet,
	log *slog.Logger,
) *controlPlaneDiscovery {
	return &controlPlaneDiscovery{
		catalog: catalogService, releases: releaseService,
		discovery: discoveryService, configs: configService,
		clients: clientSet, log: log,
	}
}

// Discover implements the bootstrap cold-start seam.
func (d *controlPlaneDiscovery) Discover(ctx context.Context) (map[string]registry.Discovery, error) {
	configs, err := d.configs.List(ctx, "", "")
	if err != nil {
		return nil, err
	}
	trusted := map[string]bool{}
	for _, status := range d.discovery.Adapters() {
		trusted[status.Name] = status.Trusted
	}
	out := map[string]registry.Discovery{}
	for _, client := range d.clients.All() {
		sourceID, ok := d.catalog.SourceIDForURL(ctx, client.ServerURL())
		if !ok {
			continue
		}
		declaration := registry.Discovery{}
		if version, ok := d.releases.CurrentVersion(ctx, string(sourceID), release.KindMultica, ""); ok {
			declaration.CLIVersion = version
		}
		verified, err := d.releases.VerifiedAgentReleases(ctx, string(sourceID))
		if err != nil {
			return nil, err
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
			continue
		}
		if err := client.RegisterWithDiscovery(ctx, declaration); err != nil {
			d.log.Warn("runtime.control_plane_declaration_failed",
				"workspace_id", client.WorkspaceID(),
				"server_id", string(sourceID), "err", err.Error())
			continue
		}
		out[client.WorkspaceID()] = declaration
	}
	return out, nil
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
