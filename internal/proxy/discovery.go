package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/tsic404/foreman/internal/workspace"
)

// Discovery is the C17 answer: the workspace set a credential can act for,
// after the manifest rule.
type Discovery struct {
	Set      []workspace.Ref
	Excluded []workspace.Ref
}

// IDs returns the discovered workspace ids in answer order.
func (d Discovery) IDs() []string {
	ids := make([]string, 0, len(d.Set))
	for _, ref := range d.Set {
		ids = append(ids, ref.ID)
	}
	return ids
}

// ListWorkspaces performs C17 and returns the credential's visible workspace
// set verbatim.
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
	for _, ws := range list {
		if ws.ID == "" || ws.Name == "" {
			return nil, fmt.Errorf("C17 answer has no display name for workspace %q", ws.ID)
		}
		out = append(out, workspace.Ref{ID: ws.ID, Name: ws.Name})
	}
	return out, nil
}

// DiscoverWorkspaces runs C17 and applies the manifest rule, refusing an
// empty or unservable answer.
func (c *Client) DiscoverWorkspaces(ctx context.Context, manifest []string) (Discovery, error) {
	visible, err := c.ListWorkspaces(ctx)
	if err != nil {
		return Discovery{}, err
	}
	if len(visible) == 0 {
		return Discovery{}, fmt.Errorf("C17 answered no workspace for this credential")
	}
	set, excluded, err := workspace.ResolveDiscoverySet(visible, manifest)
	if err != nil {
		return Discovery{}, err
	}
	if len(set) == 0 {
		return Discovery{}, fmt.Errorf("discovery set is empty after the manifest rule")
	}
	return Discovery{Set: set, Excluded: excluded}, nil
}