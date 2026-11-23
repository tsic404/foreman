package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/discovery"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/release"
)

// Environment keys of the CLI, configuration and discovery state documents.
// They live on the same writable emptyDir as the catalog registry.
const (
	EnvCLIStateFile         = "FOREMAN_CLI_STATE_FILE"
	EnvAgentConfigStateFile = "FOREMAN_AGENT_CONFIG_STATE_FILE"
	EnvDiscoveryStateFile   = "FOREMAN_DISCOVERY_STATE_FILE"
	EnvOMPBinary            = "FOREMAN_OMP_BINARY"
	EnvCLIArtifactCacheDir  = "FOREMAN_CLI_ARTIFACT_CACHE_DIR"

	defaultCLIStateFile         = "/var/lib/foreman/cli-releases.json"
	defaultAgentConfigStateFile = "/var/lib/foreman/agent-configs.json"
	defaultDiscoveryStateFile   = "/var/lib/foreman/discovery.json"
	defaultCLIArtifactCacheDir  = "/var/lib/foreman/cli-cache"
)

// newCLISurface builds the release, Agent configuration and discovery
// services and returns the management router they share.
func newCLISurface(
	getenv func(string) string,
	catalogService *catalog.Service,
	catalogAdmin http.Handler,
	clientSet *proxy.ClientSet,
	reg *registry.Registry,
	log *slog.Logger,
) (http.Handler, *release.Service, *discovery.Service, *agentconfig.Service, error) {
	releaseStore, err := release.NewFileStore(statePath(getenv, EnvCLIStateFile, defaultCLIStateFile))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	artifactCache, err := release.NewFileArtifactCache(statePath(getenv, EnvCLIArtifactCacheDir, defaultCLIArtifactCacheDir))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	releaseService, err := release.New(release.Config{
		Store:     releaseStore,
		Verifier:  release.Verifier{Fetcher: release.NewHTTPFetcher(), Cache: artifactCache},
		Artifacts: artifactCache,
		Directory: releaseDirectory{catalog: catalogService},
		Runs:      releaseRunIndex{registry: reg, sourceID: runSourceResolver(catalogService, clientSet)},
		Logger:    log,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	releaseAdmin, err := release.NewAdminHandler(release.AdminConfig{
		Service:   releaseService,
		Authorize: adminAuthorize,
		Actor:     func(*http.Request) string { return "ops-credential" },
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	configStore, err := agentconfig.NewFileStore(statePath(getenv, EnvAgentConfigStateFile, defaultAgentConfigStateFile))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	configService, err := agentconfig.New(agentconfig.Config{Store: configStore, Logger: log})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	configAdmin, err := agentconfig.NewAdminHandler(agentconfig.AdminConfig{
		Service:   configService,
		Authorize: adminAuthorize,
		Actor:     func(*http.Request) string { return "ops-credential" },
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	discoveryStore, err := discovery.NewFileStore(statePath(getenv, EnvDiscoveryStateFile, defaultDiscoveryStateFile))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	ompBinary := strings.TrimSpace(getenv(EnvOMPBinary))
	ompReason := "no global omp probe binary is configured; trust is established from a verified published artifact"
	if ompBinary != "" {
		ompReason = "omp command surface is not verified"
	}
	discoveryService, err := discovery.New(discovery.Config{
		Store:     discoveryStore,
		Artifacts: releaseService,
		Configs:   noMaterializedConfig{},
		Adapters: []discovery.AdapterRegistration{
			{Adapter: discovery.NewOMPAdapter(ompBinary), Reason: ompReason},
			{Adapter: discovery.NewUnsupportedAdapter("codex", "codex app-server protocol is not verified for this build"), Reason: "codex app-server protocol is not verified for this build"},
			{Adapter: discovery.NewUnsupportedAdapter("claude", "no non-interactive model enumeration command is verified"), Reason: "no non-interactive model enumeration command is verified"},
		},
		Logger: log,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	verifyCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	for _, status := range discoveryService.VerifyAll(verifyCtx) {
		state := "untrusted"
		if status.Trusted {
			state = "trusted"
		}
		log.Info("discovery.adapter", "name", status.Name, "status", state, "reason", status.Reason)
	}
	cancel()
	discoveryAdmin, err := discovery.NewAdminHandler(discovery.AdminConfig{
		Service:   discoveryService,
		CLIs:      releaseService,
		Configs:   configService,
		Bindings:  runtimeBindings{catalog: catalogService},
		Authorize: adminAuthorize,
		Actor:     func(*http.Request) string { return "ops-credential" },
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return newManagementRoot(catalogAdmin, releaseAdmin, configAdmin, discoveryAdmin), releaseService, discoveryService, configService, nil
}

// noMaterializedConfig keeps the prototype's boundary: the configuration
// registry stores a revision, digest and Secret reference but never the
// document body. A bound adapter therefore has no readable configuration and
// stays unsupported/needs configuration instead of falling back to a global
// binary or an invented file.
type noMaterializedConfig struct{}

func (noMaterializedConfig) ProjectFile(context.Context, string, string, string, string, string, string) (string, uint64, bool, error) {
	return "", 0, false, nil
}

// discoveryCatalogSource answers a pending_model_list command from the
// restricted control-plane adapter surface (04-architecture.md §7). The
// adapter runs in-process with a private directory; no probe Job is created
// and no business slot is taken.
type discoveryCatalogSource struct {
	discovery *discovery.Service
	catalog   *catalog.Service
	releases  *release.Service
	configs   *agentconfig.Service
	client    *proxy.Client
}

func newDiscoveryCatalogSource(
	discoveryService *discovery.Service,
	catalogService *catalog.Service,
	releaseService *release.Service,
	configService *agentconfig.Service,
	client *proxy.Client,
) *discoveryCatalogSource {
	return &discoveryCatalogSource{
		discovery: discoveryService, catalog: catalogService,
		releases: releaseService, configs: configService,
		client: client,
	}
}

// Catalog implements proxy.CatalogSource.
func (s *discoveryCatalogSource) Catalog(ctx context.Context, request proxy.CatalogRequest) (proxy.CatalogAnswer, error) {
	serverID, ok := s.catalog.SourceIDForURL(ctx, s.client.ServerURL())
	if !ok {
		return proxy.CatalogAnswer{}, errors.New("the source is not registered")
	}
	ref, ok := s.runtimeRef(request.RuntimeID)
	if !ok {
		return proxy.CatalogAnswer{}, errors.New("the runtime is not declared")
	}
	revision, _ := s.configs.Revision(ctx, string(serverID), s.client.WorkspaceID(), ref.Provider, ref.ProfileID)
	platform := runtime.GOOS + "/" + runtime.GOARCH
	result, err := s.discovery.Submit(ctx, discovery.SubmitInput{
		UpstreamRequestID: request.RequestID,
		ServerID:          string(serverID),
		WorkspaceID:       s.client.WorkspaceID(),
		RuntimeID:         request.RuntimeID,
		Profile:           ref.ProfileID,
		Kind:              discovery.KindModels,
		Adapter:           ref.Provider,
		CLIDigest:         s.cliDigest(ctx, string(serverID), ref.Provider, platform),
		Platform:          platform,
		ConfigRevision:    revision,
		Deadline:          request.Deadline,
	})
	if err != nil {
		return proxy.CatalogAnswer{}, err
	}
	return catalogAnswerFor(result.Result)
}

// catalogAnswerFor maps one persisted discovery result onto the C21 answer.
// Only the two success classes carry a model list; unsupported and failed are
// errors, so the reporter never relays them as an empty success.
func catalogAnswerFor(result *discovery.Result) (proxy.CatalogAnswer, error) {
	if result == nil {
		return proxy.CatalogAnswer{}, errors.New("discovery completed without a result")
	}
	switch result.Class {
	case discovery.ClassNonEmpty, discovery.ClassEmpty:
		models := result.Models
		if models == nil {
			models = []discovery.Model{}
		}
		body, err := json.Marshal(struct {
			Provider string            `json:"provider,omitempty"`
			Models   []discovery.Model `json:"models"`
		}{Provider: result.Provider, Models: models})
		if err != nil {
			return proxy.CatalogAnswer{}, err
		}
		return proxy.CatalogAnswer{Body: body}, nil
	case discovery.ClassUnsupported:
		return proxy.CatalogAnswer{}, errors.New("model discovery is unsupported: " + result.Detail)
	default:
		return proxy.CatalogAnswer{}, errors.New("model discovery failed: " + result.Detail)
	}
}

// MinimumBudget implements proxy's catalogBudgetSource: a cached discovery
// answer needs no Job build or delivery window, so any positive budget works.
func (s *discoveryCatalogSource) MinimumBudget(time.Duration) time.Duration { return 0 }

// runtimeRef resolves the declared identity a real runtime id names.
func (s *discoveryCatalogSource) runtimeRef(runtimeID string) (proxy.RuntimeRef, bool) {
	for _, ref := range s.client.RuntimeRefs() {
		if ref.ID == runtimeID {
			return ref, true
		}
	}
	return proxy.RuntimeRef{}, false
}

// cliDigest finds the Agent CLI definition bound to this adapter and answers
// the digest the discovery cache key must include. A definition that is not
// registered leaves the digest empty rather than guessing one.
func (s *discoveryCatalogSource) cliDigest(ctx context.Context, serverID, adapter, platform string) string {
	definitions, err := s.releases.List(ctx)
	if err != nil {
		return ""
	}
	for _, definition := range definitions {
		if definition.ServerID != serverID || definition.Kind != string(release.KindAgent) ||
			!strings.EqualFold(definition.Adapter, adapter) {
			continue
		}
		if _, digest, ok := s.releases.AdapterForPlatform(ctx, serverID, definition.CLIID, platform); ok {
			return digest
		}
	}
	return ""
}

// statePath answers one state document's path.
func statePath(getenv func(string) string, key, fallback string) string {
	path := strings.TrimSpace(getenv(key))
	if path == "" {
		path = fallback
	}
	return path
}

// adminAuthorize resolves the ops credential face into the deployment-wide
// bit the release surface requires. A workspace-bound credential is admitted
// but cannot act on a release domain.
func adminAuthorize(r *http.Request) (bool, bool) {
	_, wide, ok := observability.CallerScopes(r.Context())
	return wide, ok
}

// newManagementRoot dispatches the management paths to the surface that owns
// them. Each surface keeps its own route table and credential check.
func newManagementRoot(catalogAdmin, releaseAdmin, configAdmin, discoveryAdmin http.Handler) http.Handler {
	mux := http.NewServeMux()
	mount := func(handler http.Handler, prefixes ...string) {
		if handler == nil {
			return
		}
		for _, prefix := range prefixes {
			mux.Handle(prefix, handler)
		}
	}
	mount(catalogAdmin, "/admin/servers", "/admin/servers/", "/admin/agents", "/admin/agents/", "/admin/overview", "/admin/audit")
	mount(releaseAdmin, "/admin/cli", "/admin/cli/", "/admin/cli-audit")
	mount(configAdmin, "/admin/agent-configs", "/admin/agent-configs/")
	mount(discoveryAdmin, "/admin/discovery", "/admin/discovery/")
	return mux
}

// releaseDirectory projects the synced catalog onto the impact preview's
// input. It never reads the live upstream: a stale or unsynced directory is
// handed to the preview as stale, so the preview can answer unknown.
type releaseDirectory struct {
	catalog *catalog.Service
}

// runtimeBindings resolves the adapter and execution platform a discovery
// query refers to when the console omits them. The adapter comes from the
// synced runtime row; the platform is the control-plane platform the
// restricted adapter executes on.
type runtimeBindings struct {
	catalog *catalog.Service
}

// RuntimeBinding implements discovery.BindingResolver.
func (r runtimeBindings) RuntimeBinding(ctx context.Context, serverID, workspaceID, runtimeID string) (discovery.Binding, bool) {
	if r.catalog == nil || serverID == "" || workspaceID == "" || runtimeID == "" {
		return discovery.Binding{}, false
	}
	directory, err := r.catalog.Directory(ctx, catalog.ServerID(serverID), workspaceID, catalog.Grant{})
	if err != nil {
		return discovery.Binding{}, false
	}
	for _, entry := range directory.Runtimes {
		if entry.WorkspaceID != workspaceID || entry.RuntimeID != runtimeID || entry.Provider == "" {
			continue
		}
		return discovery.Binding{
			Adapter:  entry.Provider,
			Platform: runtime.GOOS + "/" + runtime.GOARCH,
		}, true
	}
	return discovery.Binding{}, false
}

// ReadSource implements release.Directory.
func (d releaseDirectory) ReadSource(ctx context.Context, serverID string) (release.SourceDirectory, bool) {
	view, ok, err := d.catalog.Get(ctx, catalog.ServerID(serverID))
	if err != nil || !ok {
		return release.SourceDirectory{}, false
	}
	directory, err := d.catalog.Directory(ctx, catalog.ServerID(serverID), "", catalog.Grant{})
	if err != nil {
		return release.SourceDirectory{}, false
	}
	adapters := map[string]string{}
	for _, runtime := range directory.Runtimes {
		adapters[runtime.WorkspaceID+"/"+runtime.RuntimeID] = runtime.Provider
	}
	source := release.SourceDirectory{
		ServerID: serverID,
		Synced:   view.Sync == string(catalog.SyncOK) && view.Generation > 0,
		Stale:    view.Stale,
		Detail:   view.LastError,
	}
	for _, workspace := range directory.Workspaces {
		source.Workspaces = append(source.Workspaces, release.DirectoryWorkspace{
			ID: workspace.WorkspaceID, Name: workspace.Name,
		})
	}
	for _, agent := range directory.Agents {
		source.Agents = append(source.Agents, release.DirectoryAgent{
			WorkspaceID: agent.WorkspaceID, AgentID: agent.AgentID, AgentName: agent.Name,
			RuntimeID: agent.RuntimeID, Adapter: adapters[agent.WorkspaceID+"/"+agent.RuntimeID],
			Managed: agent.Management == string(catalog.ManagedForeman),
		})
	}
	return source, true
}

// releaseRunIndex projects the coordinator's live attempts onto the impact
// preview. The adapter is the provider half of the claim's frozen runtime
// identity; an attempt without one stays unknown rather than guessing.
type releaseRunIndex struct {
	registry *registry.Registry
	sourceID func(context.Context, string) (catalog.ServerID, bool)
}

// Running implements release.RunIndex.
func (i releaseRunIndex) Running(context.Context) []release.RunFact {
	if i.registry == nil {
		return nil
	}
	entries := i.registry.List()
	out := make([]release.RunFact, 0, len(entries))
	for _, entry := range entries {
		// A claimed attempt is in flight before it starts: queued and
		// initializing rows still keep the release facts frozen at claim.
		if entry.State == registry.StateTerminal {
			continue
		}
		serverID := ""
		if i.sourceID != nil {
			if id, ok := i.sourceID(context.Background(), entry.WorkspaceID); ok {
				serverID = string(id)
			}
		}
		adapter := ""
		if provider, _, ok := registry.SplitRuntimeIdentity(entry.RuntimeIdentity); ok {
			adapter = provider
		}
		out = append(out, release.RunFact{
			TaskID: entry.TaskID, ServerID: serverID, WorkspaceID: entry.WorkspaceID,
			AgentID: entry.AgentID, AgentName: entry.AgentName, Adapter: adapter,
			MulticaVersion: entry.CLIVersion, MulticaDigests: entry.CLIDigests,
			AgentVersion: entry.AgentCLIVersion, AgentDigests: entry.AgentCLIDigests,
			CLIKnown: entry.CLIFactsKnown, AgentKnown: entry.AgentFactsKnown,
			ConfigRevision: entry.ConfigRevision, ConfigKnown: entry.ConfigFactsKnown,
			StartedAt: entry.StartedAt,
		})
	}
	return out
}
