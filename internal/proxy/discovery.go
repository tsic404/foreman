package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/tsic404/foreman/internal/workspace"
)

// §9.1 发现来源 vocabulary: the three shapes a startup discovery can take
// (ADR-049 decision 1). The values are fixed — the startup event and the
// deployment note use the same words.
const (
	// DiscoverySourceUserVisible: user-scoped credential, no manifest — the
	// visible set is the discovery set.
	DiscoverySourceUserVisible = "user_scope_visible"
	// DiscoverySourceUserManifest: user-scoped credential + manifest — the
	// manifest ∩ visible set is the discovery set.
	DiscoverySourceUserManifest = "user_scope_manifest"
	// DiscoverySourceWorkspaceScope: per-workspace credentials — the
	// configured pairs are the discovery set.
	DiscoverySourceWorkspaceScope = "workspace_scope_credentials"
)

// Discovery is the startup discovery result (ADR-049 decision 1): the source,
// the discovery set and — in the manifest form — the visible workspaces the
// manifest excluded.
type Discovery struct {
	Source   string
	Scope    string
	Set      []workspace.Ref
	Excluded []workspace.Ref
}

// ListWorkspaces performs C-17 and returns the credential's visible workspace
// set verbatim (§1.2 C-17). The client carries no workspace identity for this
// call: the answer is what discovery turns into one.
func (c *Client) ListWorkspaces(ctx context.Context) ([]workspace.Ref, error) {
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodGet, "/api/daemon/workspaces", nil)
	if err != nil {
		return nil, fmt.Errorf("list workspaces: %w", err)
	}
	if code < 200 || code >= 300 {
		return nil, fmt.Errorf("list workspaces: %w", &StatusError{Code: code, Body: raw})
	}
	var list []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("list workspaces: decode: %w", err)
	}
	out := make([]workspace.Ref, 0, len(list))
	for _, w := range list {
		id := strings.TrimSpace(w.ID)
		if id == "" {
			continue
		}
		out = append(out, workspace.Ref{ID: id, Name: w.Name})
	}
	return out, nil
}

// ValidateWorkspaceScope is the workspace-scoped credential form's startup
// self-check (§1.1, §1.2 C-17): the answer must be exactly the configured
// workspace. Anything else — a different workspace, a superset, an empty
// answer — is a configuration error, not a transient failure. The answer also
// seeds the display name S-1 serves.
func (c *Client) ValidateWorkspaceScope(ctx context.Context) error {
	list, err := c.ListWorkspaces(ctx)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(list))
	for _, w := range list {
		ids = append(ids, w.ID)
	}
	if len(list) != 1 || list[0].ID != c.cfg.WorkspaceID {
		return &WorkspaceMismatchError{Configured: c.cfg.WorkspaceID, Visible: ids}
	}
	c.mu.Lock()
	c.workspace = list[0].Name
	c.mu.Unlock()
	return nil
}

// DiscoverWorkspaces runs the user-scoped credential form's discovery: C-17
// once, the manifest rule applied to its answer (ADR-049 decision 1), and the
// §9.1 source resolved. An empty visible set and a manifest entry outside it
// are startup configuration errors; every failure refuses startup (§1.1,
// AC-96).
func (c *Client) DiscoverWorkspaces(ctx context.Context, manifest []string) (Discovery, error) {
	visible, err := c.ListWorkspaces(ctx)
	if err != nil {
		return Discovery{}, err
	}
	if len(visible) == 0 {
		return Discovery{}, errors.New("workspace discovery returned an empty set")
	}
	set, excluded, err := workspace.ResolveDiscoverySet(visible, manifest)
	if err != nil {
		return Discovery{}, err
	}
	source := DiscoverySourceUserVisible
	if len(manifest) > 0 {
		source = DiscoverySourceUserManifest
	}
	return Discovery{Source: source, Scope: workspace.ScopeUser.String(), Set: set, Excluded: excluded}, nil
}

// LogDiscoveryEvent emits §9.1's startup discovery event: one info line with
// the source, the discovery set and the manifest-excluded workspaces (each
// with id and display name). It runs once per process — the set is fixed for
// the process lifetime.
func LogDiscoveryEvent(log *slog.Logger, e Discovery) {
	log.Info("workspace.discovery",
		"source", e.Source,
		"credential_scope", e.Scope,
		"workspaces", refsValue(e.Set),
		"excluded", refsValue(e.Excluded),
	)
}

// LogDiscoveryFailure emits §9.1's discovery-failure event: the reason and
// the credential scope, so a refused startup names which side failed.
func LogDiscoveryFailure(log *slog.Logger, scope string, err error) {
	log.Error("workspace.discovery_failed",
		"credential_scope", scope,
		"err", err.Error(),
	)
}

// LogServingEvent emits §9.1's post-probe service event: the effective
// service scope and the discovered workspaces the probe never registered.
// Unregistered members are never silent — the caller also logs one warn per
// member (§10 多 workspace 前置).
func LogServingEvent(log *slog.Logger, served, unregistered []workspace.Ref) {
	log.Info("workspace.serving",
		"workspaces", refsValue(served),
		"unregistered", refsValue(unregistered),
	)
}

// refsValue renders a ref list as [{id,name}] for the structured log.
func refsValue(refs []workspace.Ref) []map[string]string {
	out := make([]map[string]string, 0, len(refs))
	for _, ref := range refs {
		out = append(out, map[string]string{"id": ref.ID, "name": ref.Name})
	}
	return out
}
