// Package startup holds the startup gate cmd/foreman runs before it serves:
// the per-workspace credential self-check of 02-contracts §1.1. It lives
// outside package main so the assembly-level acceptance tests drive the same
// gate production does.
package startup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/workspace"
)

// RunSelfCheck runs the startup credential checks: a C-18 snapshot per
// workspace (never fatal, §1.6), then the workspace-scoped C-17 self-check.
// Three findings refuse startup: the C-17 set is not exactly the configured
// workspace, the credential is rejected outright (401/403), or an answer
// entry carries no display name. Network and 5xx failures only warn — the
// loops converge with the §1.5 backoff (AC-86). The returned error is what
// main exits non-zero on; it names the workspace in the event.
func RunSelfCheck(ctx context.Context, scope workspace.CredentialScope, clients []*proxy.Client, log *slog.Logger) error {
	for _, c := range clients {
		if err := c.RuntimeProfiles(ctx); err != nil {
			log.Warn("runtime.profiles_snapshot_failed", "workspace_id", c.WorkspaceID(), "err", err.Error())
		}
		if scope != workspace.ScopeWorkspace {
			continue
		}
		if err := c.ValidateWorkspaceScope(ctx); err != nil {
			if !proxy.IsStartupFatal(err) {
				// Transient: warn and let the workspace's loops retry (§1.5).
				log.Warn("workspace.credential_check_failed", "workspace_id", c.WorkspaceID(), "err", err.Error())
				continue
			}
			stage := "workspace_credential"
			var mismatch *proxy.WorkspaceMismatchError
			var answer *proxy.DiscoveryAnswerError
			if errors.As(err, &answer) {
				stage = "workspace_discovery"
			}
			fields := []any{"stage", stage, "workspace_id", c.WorkspaceID(), "err", err.Error()}
			if errors.As(err, &mismatch) {
				fields = append(fields, "visible_workspaces", mismatch.Visible)
			}
			log.Error("foreman.startup_aborted", fields...)
			return fmt.Errorf("workspace %s startup self-check failed: %w", c.WorkspaceID(), err)
		}
	}
	if scope == workspace.ScopeWorkspace {
		set := make([]workspace.Ref, 0, len(clients))
		for _, c := range clients {
			id, name := c.Workspace()
			set = append(set, workspace.Ref{ID: id, Name: name})
		}
		proxy.LogDiscoveryEvent(log, proxy.Discovery{
			Source: proxy.DiscoverySourceWorkspaceScope,
			Scope:  workspace.ScopeWorkspace.String(),
			Set:    set,
		})
	}
	return nil
}
