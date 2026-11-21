// Package bootstrap resolves one replica's startup declaration: the
// control-plane holder registers the published CLI/config declarations unless
// a published declaration already exists (restart / takeover reuse it with no
// re-registration), and a follower adopts what the holder published
// (02-contracts.md §0 术语「leader」, §1.1 注册体的取值来源, AC-77).
// cmd/foreman and the assembly-level acceptance tests share this code so the
// startup path is exercised by the same functions in production and under
// test.
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
	"github.com/tsic404/foreman/internal/scope"
	"github.com/tsic404/foreman/internal/state"
)

// StartupProber runs the startup discovery of one registered source; satisfied
// by *scheduler.Scheduler and by the control-plane discovery seam. A source's
// failure is that source's failure: the holder keeps discovering the others
// and reports the partial result, so one unreachable server cannot keep a
// healthy source's control loop from starting.
type StartupProber interface {
	RunStartupProbeInSource(ctx context.Context, sourceID string) (map[string]registry.Discovery, error)
}

// StartupDiscoveries resolves this replica's declaration set through one
// whole-set discover function.
//
// The control-plane discoverer is the only cold-start source: it reads the
// verified CLI/config declarations and registers them in-process. No probe Job
// is created, and a workspace with no verified declaration stays unavailable.
// What prevents a re-registration is the merge below: MergeDiscoveries adopts
// the published declaration of every workspace that has one and takes the
// control-plane body only for the workspaces that have none (AC-77).
//
// A follower only reads what the holder published and never probes or
// registers. A declaration read failure is not an empty declaration set: the
// follower keeps waiting (and warns) while the holder fails fast.
func StartupDiscoveries(
	ctx context.Context,
	leader *coordination.Leader,
	coord state.CoordStore,
	discover func(context.Context) (map[string]registry.Discovery, error),
	clients []*proxy.Client,
	declarationWindow time.Duration,
	log *slog.Logger,
) (map[string]registry.Discovery, error) {
	// The holder no longer runs a probe Job, so the wait only bounds how long
	// a follower waits for the holder's published declaration.
	wait := declarationWindow + 30*time.Second
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
			if discover == nil {
				return nil, errors.New("no control-plane discovery is wired")
			}
			discovered, derr := discover(ctx)
			if derr != nil {
				return nil, derr
			}
			log.Info("runtime.declaration_merged",
				"adopted", len(records), "discovered", len(discovered), "missing", len(missing))
			return MergeDiscoveries(clients, records, discovered, log), nil
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

// StartupDiscoveriesDetailed is StartupDiscoveries with the explicit set of
// sources whose startup discovery failed. A source that answered but had no
// registered workspace is not in the set: that is a declaration fact, not an
// unreachable server, and re-discovering it would never change the answer.
func StartupDiscoveriesDetailed(
	ctx context.Context,
	leader *coordination.Leader,
	coord state.CoordStore,
	prober StartupProber,
	clients []*proxy.Client,
	probeWindow time.Duration,
	log *slog.Logger,
) (map[string]registry.Discovery, []string, error) {
	wait := probeWindow + 30*time.Second
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
			return nil, nil, err
		case err == nil && len(missing) == 0:
			log.Info("runtime.declaration_adopted", "reason", "published_by_control_plane_holder")
			return AdoptDeclarations(clients, records, log), nil, nil
		case leader.Held():
			// One discovery per source: a source-scoped client is registered
			// by a discovery carrying that source's identity, and a source
			// whose identities are already published is not discovered again.
			if prober == nil {
				return nil, nil, errors.New("startup discovery is not wired")
			}
			probed := make(map[string]registry.Discovery, len(missing))
			var failures []error
			var failedSources []string
			for _, sourceID := range missingSources(clients, missing) {
				found, perr := prober.RunStartupProbeInSource(ctx, sourceID)
				if perr != nil {
					// The failure domain is the source (§1.4): log it with the
					// source id, keep discovering the others, and let the merge
					// declare only what was observed.
					log.Error("runtime.startup_discovery_failed",
						"source_id", sourceID, "err", perr.Error())
					failures = append(failures, perr)
					failedSources = append(failedSources, sourceID)
					continue
				}
				for workspaceID, discovery := range found {
					probed[scope.Key(sourceID, workspaceID)] = discovery
				}
			}
			if len(records) == 0 && len(probed) == 0 && len(failures) > 0 {
				// Nothing was observed anywhere: the template (or every
				// probe) is not runnable, and Foreman must not declare an
				// empty runtime set (AC-36).
				return nil, nil, errors.Join(failures...)
			}
			log.Info("runtime.declaration_merged",
				"adopted", len(records), "discovered", len(probed), "missing", len(missing))
			return MergeDiscoveries(clients, records, probed, log), failedSources, nil
		case err != nil:
			// A follower must not turn a store failure into an empty
			// declaration set: it keeps waiting for the holder's publication.
			log.Warn("runtime.declaration_read_failed", "err", err.Error())
		}
		if time.Now().After(deadline) {
			return nil, nil, errors.New("startup declaration was not published by the control-plane holder")
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
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
		key := c.ScopeKey()
		raw, ok, err := coord.GetDeclaration(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			missing = append(missing, key)
			continue
		}
		var rec proxy.DeclarationRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return nil, nil, err
		}
		if len(rec.Runtimes) == 0 || len(rec.RuntimeIDs) == 0 {
			missing = append(missing, key)
			continue
		}
		out[key] = rec
	}
	return out, missing, nil
}

// missingSources lists the sources with at least one unpublished workspace, in
// client order so the probe sequence is deterministic. A legacy source-less
// client yields the empty source id, which keeps the single-source form.
func missingSources(clients []*proxy.Client, missing []string) []string {
	unpublished := make(map[string]bool, len(missing))
	for _, key := range missing {
		unpublished[key] = true
	}
	var out []string
	seen := make(map[string]bool)
	for _, c := range clients {
		if !unpublished[c.ScopeKey()] || seen[c.SourceID()] {
			continue
		}
		seen[c.SourceID()] = true
		out = append(out, c.SourceID())
	}
	return out
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
		key := c.ScopeKey()
		if rec, ok := records[key]; ok {
			if err := c.Adopt(rec); err != nil {
				log.Warn("runtime.declaration_adopt_failed", "workspace_id", c.WorkspaceID(), "source_id", c.SourceID(), "err", err.Error())
				continue
			}
			out[key] = c.Declaration()
			continue
		}
		if d, ok := probed[key]; ok {
			out[key] = d
		}
	}
	return out
}

// PublishDeclaration stores one workspace's registration identity after its
// C1: the body the control-plane discovery produced plus the server-assigned
// runtime ids, so a follower or a future takeover adopts the same rows
// (AC-77). The key is the client's source+workspace scope: the same workspace
// id on two servers has two identities.
func PublishDeclaration(ctx context.Context, coord state.CoordStore, scopeKey string, cl *proxy.Client) error {
	raw, err := json.Marshal(cl.DeclarationRecord())
	if err != nil {
		return err
	}
	return coord.PutDeclaration(ctx, scopeKey, raw)
}
