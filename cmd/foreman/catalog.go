package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/console"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// EnvCatalogCredentialsDir names the directory whose files are the registered
// sources' credentials: one file per credential_ref, mounted as a Secret. The
// catalog stores the reference only; the value is read per request and never
// enters a response or an audit row (04-architecture.md §8).
const EnvCatalogCredentialsDir = "FOREMAN_SOURCE_CREDENTIALS_DIR"

// defaultCatalogCredentialsDir is where the manifest mounts the Secret. A
// deployment that never registers a source may leave it absent; registration
// then answers with the unresolvable reference instead of a fake success.
const defaultCatalogCredentialsDir = "/etc/foreman/catalog-credentials"

// EnvCatalogStateFile names the document that keeps the source registrations
// across a restart. Snapshots are not kept: they are a read model one sync
// rebuilds.
const EnvCatalogStateFile = "FOREMAN_SOURCE_STATE_FILE"

// defaultCatalogStateFile sits on the same writable emptyDir as the durable
// terminal-report queue.
const defaultCatalogStateFile = "/var/lib/foreman/catalog-sources.json"

// newCatalogService builds the multi-source directory, the binding write-back
// and the overview read model, plus the management surface over them.
func newCatalogService(
	getenv func(string) string,
	clientSet *proxy.ClientSet,
	reg *registry.Registry,
	log *slog.Logger,
	environmentCount func(context.Context, catalog.Grant) int,
) (*catalog.Service, *catalog.AdminHandler, error) {
	dir := strings.TrimSpace(getenv(EnvCatalogCredentialsDir))
	if dir == "" {
		dir = defaultCatalogCredentialsDir
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		log.Warn("catalog.credentials_dir_absent",
			"dir", dir,
			"detail", "no source credential directory is mounted; registration will answer with an unresolvable credential reference")
	}
	credentials := catalog.FileCredentials{Dir: dir}
	stateFile := strings.TrimSpace(getenv(EnvCatalogStateFile))
	if stateFile == "" {
		stateFile = defaultCatalogStateFile
	}
	store, err := catalog.NewFileStore(stateFile)
	if err != nil {
		return nil, nil, err
	}
	runIndex := &catalogRunIndex{registry: reg}
	service, err := catalog.New(catalog.Config{
		Store:             store,
		Upstream:          catalog.NewHTTPUpstream(credentials),
		Credentials:       credentials,
		ManagedRuntimeIDs: ownRuntimeIDs(clientSet),
		RunIndex:          runIndex,
		EnvironmentCount:  environmentCount,
		Logger:            log,
	})
	if err != nil {
		return nil, nil, err
	}
	runIndex.sourceID = runSourceResolver(service, clientSet)
	admin, err := catalog.NewAdminHandler(catalog.AdminConfig{
		Catalog: service,
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
			return service.GrantForScopes(r.Context(), converted), true
		},
		Actor: func(*http.Request) string { return "ops-credential" },
	})
	if err != nil {
		return nil, nil, err
	}
	return service, admin, nil
}

// runSourceResolver freezes a run's source to the claiming client's fixed
// workspace→URL identity, then to the source registry's URL. The synced
// directory never takes part: adding a source with the same workspace id, or
// deleting the workspace upstream, cannot move an existing attempt to another
// server.
func runSourceResolver(service *catalog.Service, clientSet *proxy.ClientSet) func(context.Context, string) (catalog.ServerID, bool) {
	return func(ctx context.Context, workspaceID string) (catalog.ServerID, bool) {
		client, ok := clientSet.For(workspaceID)
		if !ok {
			return "", false
		}
		return service.SourceIDForURL(ctx, client.ServerURL())
	}
}

// ownRuntimeIDs answers the real runtimes this deployment registered in one
// source and workspace. Runtime ids are opaque and unique per upstream server,
// so a source is matched by its URL: a different server's identically named
// runtime is not Foreman's (04-architecture.md §3.2 禁止按显示名推断 owner).
func ownRuntimeIDs(clientSet *proxy.ClientSet) func(catalog.Source, string) []string {
	return func(src catalog.Source, workspaceID string) []string {
		if clientSet == nil {
			return nil
		}
		for _, client := range clientSet.All() {
			if client.WorkspaceID() != workspaceID {
				continue
			}
			if strings.TrimRight(client.ServerURL(), "/") != strings.TrimRight(src.URL, "/") {
				continue
			}
			return client.RuntimeIDs()
		}
		return nil
	}
}

// catalogRunIndex projects the coordinator's live entries onto the overview's
// attempt rows. Only an attempt with a start fact counts as executing: queued,
// finished and awaiting-report entries are not "running" (04-architecture.md
// §2 以实际执行阶段为准).
type catalogRunIndex struct {
	registry *registry.Registry
	// sourceID attributes one workspace to its registered source; nil leaves
	// the row's server unknown rather than guessing.
	sourceID func(context.Context, string) (catalog.ServerID, bool)
}

// RunningAttempts answers the attempts that are actually executing.
func (i *catalogRunIndex) RunningAttempts() []catalog.RunAttempt {
	if i == nil || i.registry == nil {
		return nil
	}
	entries := i.registry.List()
	out := make([]catalog.RunAttempt, 0, len(entries))
	for _, entry := range entries {
		if entry.State == registry.StateTerminal || entry.StartedAt.IsZero() {
			continue
		}
		serverID := ""
		if entry.SourceID != "" {
			// The entry's frozen source is the authoritative attribution;
			// resolving the workspace again could pick another server that
			// exposes the same id.
			serverID = entry.SourceID
		} else if i.sourceID != nil {
			if id, ok := i.sourceID(context.Background(), entry.WorkspaceID); ok {
				serverID = string(id)
			}
		}
		out = append(out, catalog.RunAttempt{
			ServerID:            serverID,
			WorkspaceID:         entry.WorkspaceID,
			AgentID:             entry.AgentID,
			AgentName:           entry.AgentName,
			IssueID:             entry.IssueID,
			IssueIdentifier:     entry.IssueIdentifier,
			Status:              string(entry.State),
			StartedAt:           entry.StartedAt,
			PostureKnown:        entry.SandboxPosture.Known,
			PosturePrivileged:   entry.SandboxPosture.Privileged,
			PostureRunAsUser:    entry.SandboxPosture.RunAsUser,
			PostureRunAsUserSet: entry.SandboxPosture.RunAsUserSet,
			PostureRunAsNonRoot: entry.SandboxPosture.RunAsNonRoot,
		})
	}
	return out
}

// newOpsRoot composes the management table and the console beside the ops
// table on the one ops listener. Each table keeps its own routes and its own
// credential check; the management surface reuses the deployment's ops
// credential face instead of loading a second one.
//
// The console shell is the one /admin/ path answered without a credential: it
// is a browser navigation, which cannot carry an Authorization header, and it
// carries no data — every value it displays is fetched from the gated API
// beside it with the credential the operator types into the page. Serving the
// shell through the gate would leave the surface unopenable in a browser;
// baking a credential into the shell would publish the deployment's
// administrator token to anyone who can reach the port.
func newOpsRoot(ops http.Handler, admin http.Handler, credentials []observability.OpsCredential) http.Handler {
	gate := observability.NewCredentialGate(credentials)
	mux := http.NewServeMux()
	shell := console.Handler()
	mux.Handle("GET /admin", shell)
	mux.Handle("GET /admin/{$}", shell)
	mux.Handle("GET /admin/assets/", shell)
	if admin == nil {
		mux.Handle("/", ops)
		return mux
	}
	mux.Handle("/admin/", gate.Wrap(admin))
	mux.Handle("/", ops)
	return mux
}
