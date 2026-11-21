package catalog

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Sync reads one source's complete directory and publishes a new generation.
// The previous generation stays current until the whole read validated: a
// failure, a truncated page or a revoked credential leaves it untouched
// (04-architecture.md §3.1, 02-contracts.md V2-C02).
func (s *Service) Sync(ctx context.Context, id ServerID) (SyncReport, error) {
	s.mu.Lock()
	src, ok, err := s.store.GetSource(ctx, id)
	if err != nil {
		s.mu.Unlock()
		return SyncReport{}, err
	}
	if !ok {
		s.mu.Unlock()
		return SyncReport{}, ErrSourceNotFound
	}
	if s.syncing[id] {
		s.mu.Unlock()
		return SyncReport{}, ErrSyncInFlight
	}
	s.syncing[id] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.syncing, id)
		s.mu.Unlock()
	}()
	return s.syncSource(ctx, src)
}

// syncSource performs the read outside the service lock, then publishes under
// it. Two sources never block each other's network reads.
func (s *Service) syncSource(ctx context.Context, src Source) (SyncReport, error) {
	started := s.now()
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SyncTimeout)
	defer cancel()

	// A binding confirmed by read-back after this point is newer truth than
	// anything this sync reads. The epoch lets the publish step keep that
	// value instead of reverting the directory to the read's older answer.
	s.mu.Lock()
	startEpoch := s.bindingEpoch[src.ID]
	s.mu.Unlock()

	workspaces, err := s.upstream.ListWorkspaces(callCtx, src)
	if err != nil {
		return s.failSync(ctx, src, started, err)
	}
	workspaces, err = scopeWorkspaces(src, workspaces)
	if err != nil {
		return s.failSync(ctx, src, started, err)
	}

	next := Snapshot{ServerID: src.ID, SyncedAt: started, Workspaces: workspaces}
	for _, workspace := range workspaces {
		catalog, err := s.upstream.WorkspaceCatalog(callCtx, src, workspace.ID)
		if err != nil {
			return s.failSync(ctx, src, started, err)
		}
		managed := map[string]bool{}
		if s.cfg.ManagedRuntimeIDs != nil {
			for _, runtimeID := range s.cfg.ManagedRuntimeIDs(src, workspace.ID) {
				if runtimeID != "" {
					managed[runtimeID] = true
				}
			}
		}
		next.Projects = append(next.Projects, catalog.Projects...)
		next.Runtimes = append(next.Runtimes, catalog.Runtimes...)
		for _, agent := range catalog.Agents {
			agent.Management = managementFor(agent, managed)
			next.Agents = append(next.Agents, agent)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok, err := s.store.GetSource(ctx, src.ID)
	if err != nil {
		return SyncReport{}, err
	}
	if !ok {
		return SyncReport{}, ErrSourceNotFound
	}
	previous, hasPrevious, err := s.store.GetSnapshot(ctx, src.ID)
	if err != nil {
		return SyncReport{}, err
	}
	generation := uint64(1)
	if hasPrevious {
		generation = previous.Generation + 1
		next.Tombstones = diffTombstones(previous, next, generation, started)
	}
	s.applyConfirmedBindingsLocked(src.ID, startEpoch, &next)
	next.Generation = generation
	if err := s.store.PutSnapshot(ctx, next); err != nil {
		return SyncReport{}, err
	}
	finished := s.now()
	current.Connection = ConnectionOK
	current.Sync = SyncOK
	current.Generation = generation
	current.LastAttemptAt = finished
	current.LastSuccessAt = finished
	current.LastError = ""
	current.FailureCount = 0
	current.NextSyncAt = nextSyncAt(current, finished)
	if err := s.store.PutSource(ctx, current); err != nil {
		return SyncReport{}, err
	}
	s.appendAuditLocked(ctx, AuditEntry{
		At: finished, Actor: "system", Action: AuditSyncSource,
		ServerID: string(src.ID), Result: AuditOK,
		Detail: fmt.Sprintf("generation %d", generation),
	})
	return SyncReport{
		ServerID:   string(src.ID),
		Result:     "ok",
		Generation: generation,
		SyncedAt:   started,
		Workspaces: len(next.Workspaces),
		Projects:   len(next.Projects),
		Agents:     len(next.Agents),
		Runtimes:   len(next.Runtimes),
		Tombstones: len(next.Tombstones),
	}, nil
}

// applyConfirmedBindingsLocked overlays read-back-confirmed binding values
// recorded after a sync started onto the sync's older answer. The directory
// rows keep their fresh names and memberships; only the three binding fields
// follow the confirmation. A confirmed agent the sync no longer saw is not
// resurrected — but its deletion tombstone is dropped, because the read-back
// that confirmed it is newer evidence than the sync's absence.
func (s *Service) applyConfirmedBindingsLocked(id ServerID, startedAtEpoch uint64, next *Snapshot) {
	confirmed := s.confirmed[id]
	if len(confirmed) == 0 {
		return
	}
	for _, binding := range confirmed {
		if binding.epoch <= startedAtEpoch {
			continue
		}
		for i := range next.Agents {
			if next.Agents[i].WorkspaceID != binding.workspaceID || next.Agents[i].ID != binding.agentID {
				continue
			}
			next.Agents[i].RuntimeID = binding.runtimeID
			next.Agents[i].RuntimeBound = binding.runtimeBound
			next.Agents[i].Management = binding.management
		}
		next.Tombstones = slices.DeleteFunc(next.Tombstones, func(tombstone Tombstone) bool {
			return tombstone.Kind == TombstoneAgent &&
				tombstone.WorkspaceID == binding.workspaceID && tombstone.ID == binding.agentID
		})
	}
}

// failSync records a failed attempt and keeps the current generation. It is the
// observable half of the isolation contract: the report names the generation
// that stayed current, and no other source's record is touched.
func (s *Service) failSync(ctx context.Context, src Source, started time.Time, cause error) (SyncReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok, err := s.store.GetSource(ctx, src.ID)
	if err != nil {
		return SyncReport{}, err
	}
	if !ok {
		return SyncReport{}, ErrSourceNotFound
	}
	now := s.now()
	current.LastAttemptAt = now
	current.Sync = SyncFailed
	current.FailureCount++
	current.LastError = boundedError(cause)
	switch {
	case IsUnauthorized(cause):
		// 平台撤权立即生效: a revoked credential is not a stale-cache state.
		current.Connection = ConnectionUnauthorized
	case current.Connection != ConnectionOK:
		current.Connection = ConnectionFailed
	}
	current.NextSyncAt = nextSyncAt(current, now)
	if err := s.store.PutSource(ctx, current); err != nil {
		return SyncReport{}, err
	}
	kept := current.Generation
	s.appendAuditLocked(ctx, AuditEntry{
		At: now, Actor: "system", Action: AuditSyncSource,
		ServerID: string(src.ID), Result: AuditFailed, Detail: current.LastError,
	})
	s.log.Warn("catalog.sync_failed",
		"server_id", string(src.ID), "kept_generation", kept, "err", current.LastError)
	return SyncReport{
		ServerID:       string(src.ID),
		Result:         "failed",
		Generation:     kept,
		KeptGeneration: kept,
		SyncedAt:       started,
		Detail:         current.LastError,
	}, cause
}

// scopeWorkspaces applies the source's allowed set. A configured workspace the
// credential no longer answers for is a scope failure, not a silent narrowing:
// permission revocation has to be visible (04-architecture.md §3.1).
func scopeWorkspaces(src Source, workspaces []Workspace) ([]Workspace, error) {
	if len(src.AllowedWorkspaces) == 0 {
		return workspaces, nil
	}
	present := make(map[string]bool, len(workspaces))
	for _, workspace := range workspaces {
		present[workspace.ID] = true
	}
	var missing []string
	for _, allowed := range src.AllowedWorkspaces {
		if !present[allowed] {
			missing = append(missing, allowed)
		}
	}
	if len(missing) > 0 {
		return nil, &UpstreamError{
			Op: "scope workspaces", Kind: UpstreamUnauthorized,
			Message: fmt.Sprintf("the credential no longer answers for %d configured workspace(s)", len(missing)),
		}
	}
	out := make([]Workspace, 0, len(src.AllowedWorkspaces))
	for _, workspace := range workspaces {
		for _, allowed := range src.AllowedWorkspaces {
			if workspace.ID == allowed {
				out = append(out, workspace)
				break
			}
		}
	}
	return out, nil
}

// diffTombstones records objects a complete sync confirmed deleted. A tombstone
// survives while the object stays absent and is dropped when it reappears.
func diffTombstones(previous, next Snapshot, generation uint64, removedAt time.Time) []Tombstone {
	present := map[string]bool{}
	for _, workspace := range next.Workspaces {
		present["workspace/"+workspace.ID+"/"+workspace.ID] = true
	}
	for _, project := range next.Projects {
		present["project/"+project.WorkspaceID+"/"+project.ID] = true
	}
	for _, agent := range next.Agents {
		present["agent/"+agent.WorkspaceID+"/"+agent.ID] = true
	}
	for _, runtime := range next.Runtimes {
		present["runtime/"+runtime.WorkspaceID+"/"+runtime.ID] = true
	}

	byKey := map[string]Tombstone{}
	var order []string
	add := func(kind TombstoneKind, workspaceID, id string, at time.Time) {
		key := string(kind) + "/" + workspaceID + "/" + id
		if present[key] {
			return
		}
		if _, exists := byKey[key]; exists {
			return
		}
		byKey[key] = Tombstone{Kind: kind, WorkspaceID: workspaceID, ID: id, RemovedAt: at, Generation: generation}
		order = append(order, key)
	}
	for _, tombstone := range previous.Tombstones {
		add(tombstone.Kind, tombstone.WorkspaceID, tombstone.ID, tombstone.RemovedAt)
	}
	for _, workspace := range previous.Workspaces {
		add(TombstoneWorkspace, workspace.ID, workspace.ID, removedAt)
	}
	for _, project := range previous.Projects {
		add(TombstoneProject, project.WorkspaceID, project.ID, removedAt)
	}
	for _, agent := range previous.Agents {
		add(TombstoneAgent, agent.WorkspaceID, agent.ID, removedAt)
	}
	for _, runtime := range previous.Runtimes {
		add(TombstoneRuntime, runtime.WorkspaceID, runtime.ID, removedAt)
	}
	out := make([]Tombstone, 0, len(order))
	for _, key := range order {
		out = append(out, byKey[key])
	}
	return out
}

// boundedError clips a failure for the source record and the audit row.
func boundedError(err error) string {
	const max = 300
	if err == nil {
		return ""
	}
	text := strings.TrimSpace(err.Error())
	if len(text) > max {
		return text[:max] + "…"
	}
	return text
}
