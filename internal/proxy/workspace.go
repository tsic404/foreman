package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// WorkspaceState is the workspace state carrier (§1.7): the settings, repos
// and repos_version values are kept verbatim. Nil raw fields mean "the upstream
// did not give this key" and must stay absent from the daemon-facing answer;
// an explicit `null`, `{}` or any other shape is a value and must be replayed
// as such.
type WorkspaceState struct {
	Repos json.RawMessage
	// ReposVersion keeps the key's presence faithful: an upstream answer that
	// does not carry repos_version leaves the key absent, an explicit `null`
	// replays as `null` and a value replays byte for byte (the server may send
	// a shape this build does not interpret).
	ReposVersion json.RawMessage
	Settings     json.RawMessage
	// Seeded marks a successful C-1. Before it the S-2 window answers an empty
	// repo set and no settings, and never calls C-22.
	Seeded bool
}

func (s WorkspaceState) clone() WorkspaceState {
	out := s
	out.Repos = append(json.RawMessage(nil), s.Repos...)
	out.ReposVersion = append(json.RawMessage(nil), s.ReposVersion...)
	out.Settings = append(json.RawMessage(nil), s.Settings...)
	return out
}

// reposRefresh is the C-22 single-flight slot: concurrent S-2 requests wait on
// one upstream call.
type reposRefresh struct {
	done chan struct{}
	err  error
}

// RefreshWorkspaceState runs C-22 once for one S-2 request. Concurrent callers
// share the in-flight call; the leader's request is detached from the caller's
// context and bounded by WorkspaceStateTimeout, so a disconnecting daemon
// cannot cancel the refresh for everyone. A failure is non-fatal: the caller
// answers from the last successful snapshot (§1.7).
func (c *Client) RefreshWorkspaceState(ctx context.Context) error {
	if !c.WorkspaceState().Seeded {
		// The pre-C1 window never calls C-22 (§1.7 注册前窗口), so the refresh
		// has no snapshot to answer from: §9.2's `failed` outcome. The S-2
		// answer itself stays the designed empty pre-C1 body.
		c.metrics.WorkspaceStateRefresh(WorkspaceStateFailure)
		return nil
	}
	c.reposMu.Lock()
	if call := c.reposCall; call != nil {
		c.reposMu.Unlock()
		select {
		case <-call.done:
			return call.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	call := &reposRefresh{done: make(chan struct{})}
	c.reposCall = call
	c.reposMu.Unlock()

	timeout := c.cfg.WorkspaceStateTimeout
	if timeout <= 0 {
		timeout = DefaultWorkspaceStateTimeout
	}
	refreshCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	call.err = c.fetchWorkspaceState(refreshCtx)

	c.reposMu.Lock()
	c.reposCall = nil
	c.reposMu.Unlock()
	close(call.done)

	if call.err != nil {
		// The refresh failed with a seeded snapshot in hand, so the answer
		// replays it: stale_replay (§1.7). The register-before window has no
		// snapshot and records `failed` instead.
		c.metrics.WorkspaceStateRefresh(WorkspaceStateStaleReplay)
		c.log.Warn("workspace.state_refresh_failed",
			"err", call.err, "replay", "last_snapshot")
		return call.err
	}
	c.metrics.WorkspaceStateRefresh(WorkspaceStateSuccess)
	return nil
}

// fetchWorkspaceState is C-22: one upstream call whose successful answer
// replaces the whole carrier (a missing settings key clears the old value,
// matching the daemon's override semantics, §1.7).
func (c *Client) fetchWorkspaceState(ctx context.Context) error {
	path := fmt.Sprintf("/api/daemon/workspaces/%s/repos", url.PathEscape(c.cfg.WorkspaceID))
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("workspace repos: %w", err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("workspace repos: %w", &StatusError{Code: code, Body: raw})
	}
	var resp map[string]json.RawMessage
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("workspace repos: decode: %w", err)
	}
	// Key presence is the carrier's contract, so the answer is read as a raw
	// map: an absent key stays nil (the daemon-facing answer omits it), while
	// `null` and every other shape replay verbatim.
	c.mu.Lock()
	c.state = WorkspaceState{
		Repos:        resp["repos"],
		ReposVersion: resp["repos_version"],
		Settings:     resp["settings"],
		Seeded:       true,
	}
	c.mu.Unlock()
	return nil
}

// workspaceStateResponse builds the S-2 / S-4 workspace fields with presence
// fidelity: repos and repos_version are always part of the answers (the pre-C1
// window answers an empty repo set), while settings appears only when the
// upstream gave the key (§1.7).
func workspaceStateResponse(state WorkspaceState) map[string]any {
	out := map[string]any{}
	if state.Seeded {
		if state.Repos != nil {
			out["repos"] = state.Repos
		}
		if state.ReposVersion != nil {
			out["repos_version"] = state.ReposVersion
		}
		if state.Settings != nil {
			out["settings"] = state.Settings
		}
		return out
	}
	// Register-before window: empty repo set, no settings, no C-22.
	out["repos"] = noRepos
	out["repos_version"] = ""
	return out
}
