package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
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
	out := map[string]registry.Discovery{}
	for _, sourceID := range d.sourceIDs() {
		// Between sources: a tenure lost after the previous source must not
		// register the next one under the new holder.
		if err := tenure.Check(d.gate); err != nil {
			d.log.Warn("runtime.control_plane_discovery_deferred", "reason", "leader_lease_lost")
			return nil, err
		}
		found, derr := d.discoverSource(ctx, sourceID, configs)
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
	return d.discoverSource(ctx, sourceID, configs)
}

// RunStartupProbeInSource implements the bootstrap per-source discovery seam.
func (d *controlPlaneDiscovery) RunStartupProbeInSource(ctx context.Context, sourceID string) (map[string]registry.Discovery, error) {
	return d.DiscoverSource(ctx, sourceID)
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

// errDeclarationStale reports that the published release set changed between
// the probe that built a declaration and its registration commit. The attempt
// is abandoned rather than overwriting the newer declaration; the caller
// re-resolves with the facts it just read.
var errDeclarationStale = errors.New("the published declaration changed before its registration commit")

func (d *controlPlaneDiscovery) discoverSource(
	ctx context.Context,
	sourceID string,
	configs []agentconfig.View,
) (map[string]registry.Discovery, error) {
	out := map[string]registry.Discovery{}
	for _, client := range d.clients.All() {
		if client.SourceID() != sourceID {
			continue
		}
		declaration, verified, serverID, err := d.resolveClient(ctx, client, configs)
		if err != nil {
			return nil, err
		}
		if len(declaration.Runtimes) == 0 {
			continue
		}
		// A client that already completed C1 holds this process's live
		// identity. A second C1 would duplicate the registration, and an
		// older declaration read before a newer publication could replace it
		// (AC-77: a served declaration is never re-registered).
		if len(client.RuntimeIDs()) > 0 {
			out[client.WorkspaceID()] = declaration
			continue
		}
		// The probe may have run for seconds. Re-read the published set at
		// the commit point so a publication that landed in between is not
		// overwritten by the declaration this attempt resolved earlier.
		if err := d.recheckRelease(ctx, serverID, verified); err != nil {
			return nil, err
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

func (d *controlPlaneDiscovery) recheckRelease(ctx context.Context, serverID string, used []release.VerifiedAgentRelease) error {
	fresh, err := d.releases.VerifiedAgentReleases(ctx, serverID)
	if err != nil {
		return err
	}
	if !sameVerifiedReleases(used, fresh) {
		return errDeclarationStale
	}
	return nil
}

// sameVerifiedReleases compares the release identity a declaration was built
// from, independent of the store's order.
func sameVerifiedReleases(left, right []release.VerifiedAgentRelease) bool {
	keys := func(releases []release.VerifiedAgentRelease) []string {
		out := make([]string, 0, len(releases))
		for _, candidate := range releases {
			artifacts := make([]string, 0, len(candidate.Artifacts))
			for _, artifact := range candidate.Artifacts {
				artifacts = append(artifacts, artifact.Platform.Key()+":"+
					strings.ToLower(artifact.Digest)+":"+strconv.FormatInt(artifact.Size, 10))
			}
			sort.Strings(artifacts)
			out = append(out, candidate.CLIID+"\x00"+candidate.Adapter+"\x00"+
				candidate.Version+"\x00"+strings.Join(artifacts, ","))
		}
		sort.Strings(out)
		return out
	}
	return slices.Equal(keys(left), keys(right))
}

// resolveClient derives one client's declaration from the published CLI and
// configuration facts. It performs no upstream write: the caller decides
// whether this instance may still register.
func (d *controlPlaneDiscovery) resolveClient(
	ctx context.Context,
	client *proxy.Client,
	configs []agentconfig.View,
) (registry.Discovery, []release.VerifiedAgentRelease, string, error) {
	catalogID, ok := d.catalog.SourceIDForURL(ctx, client.ServerURL())
	if !ok {
		return registry.Discovery{}, nil, "", nil
	}
	serverID := string(catalogID)
	declaration := registry.Discovery{}
	if version, ok := d.releases.CurrentVersion(ctx, serverID, release.KindMultica, ""); ok {
		declaration.CLIVersion = version
	}
	verified, err := d.releases.VerifiedAgentReleases(ctx, serverID)
	if err != nil {
		return registry.Discovery{}, nil, "", err
	}
	seen := map[string]bool{}
	for _, candidate := range verified {
		// The bound probe runs the exact published bytes with the fixed
		// commands the parser depends on, so passing it is what makes the
		// adapter trusted for this release. A deployment that installs no
		// probe binary beside the server is not therefore untrusted: the
		// verified payload cannot drift from what the runtime executes, and
		// it is the stronger check.
		if err := d.verifyBound(ctx, serverID, candidate); err != nil {
			d.log.Warn("runtime.bound_verification_failed",
				"workspace_id", client.WorkspaceID(),
				"server_id", serverID,
				"cli_id", candidate.CLIID,
				"version", candidate.Version,
				"err", err.Error())
			continue
		}
		d.discovery.TrustPublished(candidate.Adapter, candidate.Version)
		profiles := configuredProfiles(configs, serverID, client.WorkspaceID(), candidate.Adapter)
		if len(profiles) == 0 {
			d.log.Warn("runtime.configuration_unavailable",
				"workspace_id", client.WorkspaceID(),
				"server_id", serverID,
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
			"server_id", serverID,
			"detail", "no verified Agent CLI definition is available; the workspace is not served")
		return declaration, verified, serverID, nil
	}
	return declaration, verified, serverID, nil
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
