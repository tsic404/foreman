// Package bootstrap resolves one replica's startup declaration: the
// control-plane holder runs the startup probe unless a published declaration
// already exists (restart / takeover reuse it with no re-registration), and a
// follower adopts what the holder published (02-contracts.md §0 术语
// 「leader」, §1.1 注册体的取值来源, AC-77). cmd/foreman and the
// assembly-level acceptance tests share this code so the startup path is
// exercised by the same functions in production and under test.
package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/tsic404/foreman/internal/coordination"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
	"github.com/tsic404/foreman/internal/state"
)

// StartupDiscoveries resolves this replica's declaration set.
//
// The startup probe is a deployment-level discovery, not a per-workspace one:
// RunStartupProbe has no workspace filter and the probe daemon registers
// according to the template's own scope (02-contracts §1.1). What prevents a
// re-registration is the merge below: MergeDiscoveries adopts the published
// declaration of every workspace that has one and takes the probe body only
// for the workspaces that have none, so an identity that was already
// registered is never declared again (AC-77).
//
// A follower only reads what the holder published and never probes or
// registers. A declaration read failure is not an empty declaration set: the
// follower keeps waiting (and warns) while the holder fails fast.
func StartupDiscoveries(
	ctx context.Context,
	leader *coordination.Leader,
	coord state.CoordStore,
	sched *scheduler.Scheduler,
	clients []*proxy.Client,
	probeWindow time.Duration,
	log *slog.Logger,
) (map[string]registry.Discovery, error) {
	wait := probeWindow + scheduler.ProbeEndpointWait(probeWindow) + 30*time.Second
	if wait < time.Minute {
		wait = time.Minute
	}
	deadline := time.Now().Add(wait)
	for {
		records, missing, err := LoadDeclarations(ctx, coord, clients)
		switch {
		case err != nil && leader.Held():
			// The holder cannot reconcile or probe against a store it cannot
			// read: fail fast instead of reporting a false "nothing declared".
			return nil, err
		case err == nil && len(missing) == 0:
			log.Info("runtime.declaration_adopted", "reason", "published_by_control_plane_holder")
			return AdoptDeclarations(clients, records, log), nil
		case leader.Held():
			// The probe's scope is the template's, not this list; the merge
			// keeps the published identities and uses the probe only for the
			// workspaces that have none.
			probed, perr := sched.RunStartupProbe(ctx)
			if perr != nil {
				return nil, perr
			}
			log.Info("runtime.declaration_merged",
				"adopted", len(records), "probed", len(probed), "missing", len(missing))
			return MergeDiscoveries(clients, records, probed, log), nil
		case err != nil:
			// A follower must not turn a store failure into an empty
			// declaration set: it keeps waiting for the holder's publication.
			log.Warn("runtime.declaration_read_failed", "err", err.Error())
		}
		if time.Now().After(deadline) {
			return nil, errors.New("startup declaration was not published by the control-plane holder")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// LoadDeclarations reads the published declaration of every served workspace
// and lists the workspaces that have none yet.
func LoadDeclarations(ctx context.Context, coord state.CoordStore, clients []*proxy.Client) (map[string]proxy.DeclarationRecord, []string, error) {
	out := make(map[string]proxy.DeclarationRecord, len(clients))
	var missing []string
	for _, c := range clients {
		ws := c.WorkspaceID()
		raw, ok, err := coord.GetDeclaration(ctx, ws)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			missing = append(missing, ws)
			continue
		}
		var rec proxy.DeclarationRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return nil, nil, err
		}
		if len(rec.Runtimes) == 0 || len(rec.RuntimeIDs) == 0 {
			missing = append(missing, ws)
			continue
		}
		out[ws] = rec
	}
	return out, missing, nil
}

// AdoptDeclarations installs the stored declarations without C1 and renders
// the discovery map the serving event needs.
func AdoptDeclarations(clients []*proxy.Client, records map[string]proxy.DeclarationRecord, log *slog.Logger) map[string]registry.Discovery {
	return MergeDiscoveries(clients, records, nil, log)
}

// MergeDiscoveries adopts every published declaration and uses the probe's
// discovery only for the workspaces that had none. A workspace with a
// published identity is never re-declared from the probe body.
func MergeDiscoveries(clients []*proxy.Client, records map[string]proxy.DeclarationRecord, probed map[string]registry.Discovery, log *slog.Logger) map[string]registry.Discovery {
	out := make(map[string]registry.Discovery, len(clients))
	for _, c := range clients {
		ws := c.WorkspaceID()
		if rec, ok := records[ws]; ok {
			if err := c.Adopt(rec); err != nil {
				log.Warn("runtime.declaration_adopt_failed", "workspace_id", ws, "err", err.Error())
				continue
			}
			out[ws] = c.Declaration()
			continue
		}
		if d, ok := probed[ws]; ok {
			out[ws] = d
		}
	}
	return out
}

// PublishDeclaration stores one workspace's registration identity after its
// C1: the body the startup probe produced plus the server-assigned runtime
// ids, so a follower or a future takeover adopts the same rows (AC-77).
func PublishDeclaration(ctx context.Context, coord state.CoordStore, workspaceID string, cl *proxy.Client) error {
	raw, err := json.Marshal(cl.DeclarationRecord())
	if err != nil {
		return err
	}
	return coord.PutDeclaration(ctx, workspaceID, raw)
}
