package registry

import (
	"context"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/scope"
)

// sharedOpTimeout bounds one synchronous stage-two store operation on a hot
// path. A store that cannot answer inside the bound fails the operation
// closed (the caller answers 503 / drops the claim) instead of stalling the
// daemon face.
const sharedOpTimeout = 2 * time.Second

// daemonIDPrefix is the foreman daemon-identity prefix (ADR-003): the daemon
// id is "fm-<task_id>", which is what lets a partitioned lookup derive the
// task id from the token's job identity without a second index.
const daemonIDPrefix = "fm-"

// legacyDaemonTaskID recovers the task id from the pre-index daemon shape
// fm-<task id>. A source-scoped daemon identity (fm-<task id>-<discriminator>)
// yields a non-matching id, so it is answered only by the daemon index.
func legacyDaemonTaskID(daemonID string) string {
	taskID, found := strings.CutPrefix(daemonID, daemonIDPrefix)
	if !found {
		return ""
	}
	return taskID
}

func (r *Registry) sharedCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), sharedOpTimeout)
}

// releaseSlotsShared returns the task's slot on both levels (§10 两级在途上限).
// The counter is floor-guarded, so a row restored without a matching
// reservation cannot drive it negative.
func (r *Registry) releaseSlotsShared(ws string) {
	if r.shared == nil {
		return
	}
	ctx, cancel := r.sharedCtx()
	defer cancel()
	if ws != "" {
		_ = r.shared.ReleaseEntry(ctx, ws)
	}
	_ = r.shared.ReleaseEntry(ctx, "")
}

// writeShared persists a full row through the stage-two face. It is the claim
// time path (the caller holds the task write lock), so it is synchronous: the
// row must be visible to the instance that will serve the Job's daemon. A
// claim's reservations retire in the same store step as the row, so a
// reconciliation never reads a row and its reservation marker together.
func (r *Registry) writeShared(e TaskEntry, retire []SlotRetirement) error {
	ctx, cancel := r.sharedCtx()
	defer cancel()
	var err error
	if len(retire) == 0 {
		err = r.shared.SetEntry(ctx, e.ScopeKey(), e.TaskID, e)
	} else {
		err = r.shared.SetEntryRetiringSlots(ctx, e.ScopeKey(), e.TaskID, e, retire)
	}
	if err != nil {
		r.stateFailure("put_entry", e.TaskID, err)
		return err
	}
	return nil
}

// mergeShared hands an in-place field update to the ordered writer: the store
// round trip never runs under r.mu, and the merge excludes every gate field so
// it can never revert a decision another instance made.
func (r *Registry) mergeShared(taskID string, e TaskEntry, p EntryPatch) {
	r.enqueueState(stateOp{kind: stateOpMerge, ws: e.ScopeKey(), taskID: taskID, patch: p})
}

// mergeNow is the synchronous merge used by the adoption paths that must be
// visible before the response leaves (the token digest). The write's failure
// is returned as well as reported: a fact the shared row does not carry is
// not adjudicable on another instance, so the caller must not hand it out.
func (r *Registry) mergeNow(taskID string, e TaskEntry, p EntryPatch) error {
	if r.shared == nil {
		return nil
	}
	ctx, cancel := r.sharedCtx()
	defer cancel()
	if err := r.shared.MergeEntry(ctx, e.ScopeKey(), taskID, p); err != nil {
		r.stateFailure("merge_entry", taskID, err)
		return err
	}
	return nil
}

// Cache inserts one row read from the shared state into the local index
// without writing it back: the shared row stays the authority, the local copy
// is the bounded positive cache the daemon face reads (ADR-046 决策 2).
func (r *Registry) Cache(e TaskEntry) {
	if e.TaskID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cacheEntryLocked(e)
}

// cacheEntryLocked inserts e into the in-memory indexes. A terminal cached row
// is never regressed by a stale non-terminal read; the runtime row is restored
// so the job runtime's heartbeat and prepare-lease paths work on this
// instance.
func (r *Registry) cacheEntryLocked(e TaskEntry) {
	key := e.Key()
	if older, ok := r.live[key]; ok {
		if older.State == StateTerminal && e.State != StateTerminal {
			return
		}
		r.dropIndexesLocked(older)
	}
	r.live[key] = e
	r.indexLocked(e)
	if e.JobRuntimeID != "" {
		if _, ok := r.runtimes[e.JobRuntimeID]; !ok {
			r.runtimes[e.JobRuntimeID] = JobRuntime{
				JobRuntimeID:    e.JobRuntimeID,
				DaemonID:        e.DaemonID,
				TaskID:          e.TaskID,
				Provider:        "omp",
				Name:            "foreman-job",
				Online:          true,
				LastHeartbeatAt: r.now(),
			}
			if e.DaemonID != "" {
				r.runtimeByDaemon[e.DaemonID] = e.JobRuntimeID
			}
		}
	}
}

// GetInWorkspace returns the entry of taskID in workspace ws. With the
// shared face configured the row is the adjudication source on every read
// (F11: 裁决只以共享状态为准); the local copy only answers while the store is
// unreachable (the outage grace window). A store that authoritatively holds no
// row drops the local copy — a row lost with the store must not keep
// authorizing its credential (AC-78).
func (r *Registry) GetInWorkspace(ws, taskID string) (TaskEntry, bool) {
	if r.shared != nil && ws != "" && taskID != "" {
		if e, ok, err := r.readShared(ws, taskID); err == nil {
			if !ok {
				r.dropLocalInWorkspace(ws, taskID)
			}
			return e, ok
		}
	}
	r.mu.RLock()
	if ws == "" {
		key, ok := r.lookupKeyLocked(taskID, nil)
		if !ok {
			r.mu.RUnlock()
			return TaskEntry{}, false
		}
		e, ok := r.live[key]
		r.mu.RUnlock()
		return e, ok
	}
	e, ok := r.live[keyInScope(ws, taskID)]
	r.mu.RUnlock()
	return e, ok
}

// GetInScope returns one live row by its source/workspace partition without
// treating a miss as evidence that the local cache is wrong. The ops read face
// uses it: a read-only projection must never evict a row another path still
// owns. The shared row stays authoritative when the store answers.
func (r *Registry) GetInScope(scopeKey, taskID string) (TaskEntry, bool) {
	if r.shared != nil && scopeKey != "" && taskID != "" {
		if e, ok, err := r.readShared(scopeKey, taskID); err == nil {
			return e, ok
		}
	}
	r.mu.RLock()
	if scopeKey == "" {
		key, ok := r.lookupKeyLocked(taskID, nil)
		if !ok {
			r.mu.RUnlock()
			return TaskEntry{}, false
		}
		e, ok := r.live[key]
		r.mu.RUnlock()
		return e, ok
	}
	e, ok := r.live[keyInScope(scopeKey, taskID)]
	r.mu.RUnlock()
	return e, ok
}

// dropLocal removes one row from the local index without writing anything
// back (the shared row is the authority and says it is gone).
func (r *Registry) dropLocal(taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, e := range r.live {
		if e.TaskID != taskID {
			continue
		}
		r.dropIndexesLocked(e)
		delete(r.live, key)
		r.dropRuntimeLocked(e.JobRuntimeID, e.DaemonID)
	}
}

// dropLocalInWorkspace removes the workspace-only copy after the shared store
// authoritatively lost it. A source-scoped row of the same task id is another
// attempt and is left alone.
func (r *Registry) dropLocalInWorkspace(ws, taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := keyInScope(ws, taskID)
	if e, ok := r.live[key]; ok && e.SourceID == "" {
		r.dropIndexesLocked(e)
		delete(r.live, key)
		r.dropRuntimeLocked(e.JobRuntimeID, e.DaemonID)
	}
}

// dropLocalByDaemon removes the cached row a Job identity no longer resolves
// to in the shared store. The daemon identity is unique per attempt.
func (r *Registry) dropLocalByDaemon(daemonID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := r.byDaemon[daemonID]
	r.dropLocalKeyLocked(key)
}

// dropLocalByKey removes one local row by its index key.
func (r *Registry) dropLocalByKey(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dropLocalKeyLocked(key)
}

func (r *Registry) dropLocalKeyLocked(key string) {
	if key == "" {
		return
	}
	if e, ok := r.live[key]; ok {
		r.dropIndexesLocked(e)
		delete(r.live, key)
		r.dropRuntimeLocked(e.JobRuntimeID, e.DaemonID)
	}
}

// ByDaemonInWorkspace returns the entry served by daemonID in ws. The daemon
// identity carries the task id (ADR-003), so the partitioned lookup needs no
// second index.
func (r *Registry) ByDaemonInWorkspace(ws, daemonID string) (TaskEntry, bool) {
	if r.shared != nil && daemonID != "" {
		// The daemon index is the authoritative token face: the daemon id is
		// the Job Token's j claim, not the template's display name.
		ctx, cancel := r.sharedCtx()
		e, ok, err := r.shared.GetEntryByDaemon(ctx, daemonID)
		cancel()
		if err == nil {
			if ok && e.DaemonID == daemonID && (ws == "" || e.WorkspaceID == ws) {
				r.Cache(e)
				return e, true
			}
			if !ok && ws != "" {
				// Rows written before the daemon index existed still answer
				// through the historical fm-<task id> shape. The row's own
				// daemon identity is re-checked, so the compatibility path
				// cannot authorize a different attempt.
				if taskID := legacyDaemonTaskID(daemonID); taskID != "" {
					if e, found := r.GetInWorkspace(ws, taskID); found && e.DaemonID == daemonID {
						return e, true
					}
				}
			}
			r.dropLocalByDaemon(daemonID)
			return TaskEntry{}, false
		}
	}
	taskID, found := strings.CutPrefix(daemonID, daemonIDPrefix)
	if found && taskID != "" && ws != "" {
		return r.GetInWorkspace(ws, taskID)
	}
	r.mu.RLock()
	id, ok := r.byDaemon[daemonID]
	if ok {
		e, ok := r.live[id]
		r.mu.RUnlock()
		return e, ok
	}
	r.mu.RUnlock()
	if !found || taskID == "" {
		return TaskEntry{}, false
	}
	return r.fetchShared(ws, taskID)
}

// ByDaemonInSourceWorkspace is the source-aware token lookup. The daemon
// identity is still the Job name, but the same workspace id can exist on two
// servers, so both the local row and the shared fallback must match the
// token's source as well as its workspace (02-contracts V2-C03).
func (r *Registry) ByDaemonInSourceWorkspace(sourceID, ws, daemonID string) (TaskEntry, bool) {
	if sourceID == "" {
		return r.ByDaemonInWorkspace(ws, daemonID)
	}
	if r.shared != nil && daemonID != "" {
		// The shared daemon index is the adjudication source, not the local
		// cache. A local hit is only a copy: another instance may have
		// deleted the row, and a token must stop authorizing the moment the
		// authoritative mapping is gone (V2-C03 共享恢复与凭据). A store
		// error is the only case that may fall back to the cached row. The
		// index is keyed by the daemon identity, which is the token's claim
		// and not the template's display name (a custom metadata.name renders
		// a different Job name).
		ctx, cancel := r.sharedCtx()
		e, ok, err := r.shared.GetEntryByDaemon(ctx, daemonID)
		cancel()
		if err == nil {
			if ok && e.SourceID == sourceID && e.WorkspaceID == ws && e.DaemonID == daemonID {
				r.Cache(e)
				return e, true
			}
			if !ok {
				// The pre-index daemon shape, verified against the shared
				// row's own identity; see ByDaemonInWorkspace.
				if taskID := legacyDaemonTaskID(daemonID); taskID != "" {
					if e, found := r.GetInScope(scope.Key(sourceID, ws), taskID); found &&
						e.SourceID == sourceID && e.WorkspaceID == ws && e.DaemonID == daemonID {
						return e, true
					}
				}
			}
			r.dropLocalByDaemon(daemonID)
			return TaskEntry{}, false
		}
	}
	r.mu.RLock()
	id, ok := r.byDaemon[daemonID]
	if ok {
		e, ok := r.live[id]
		r.mu.RUnlock()
		if ok && e.SourceID == sourceID && e.WorkspaceID == ws {
			return e, true
		}
		return TaskEntry{}, false
	}
	r.mu.RUnlock()
	return TaskEntry{}, false
}

// IsDoneInSource answers the late-replay index under the source and workspace
// partition.
func (r *Registry) IsDoneInSource(sourceID, ws, taskID string) bool {
	_, _, ok := r.done.get(keyInScope(scope.Key(sourceID, ws), taskID))
	return ok
}

// TaskInScope resolves one task's attempt by its exact source/workspace
// partition. The shared row is the authority when the store is attached; a
// store error falls back to the local row only. ok=false means no attempt of
// this scope holds the task id — the caller must not treat that as "the task
// exists somewhere else".
func (r *Registry) TaskInScope(sourceID, workspaceID, taskID string) (TaskEntry, bool) {
	e, ok := r.GetInScope(scope.Key(sourceID, workspaceID), taskID)
	if !ok || e.SourceID != sourceID || e.WorkspaceID != workspaceID || e.TaskID != taskID {
		return TaskEntry{}, false
	}
	return e, true
}

// ByJobInWorkspace returns the entry whose Job is jobName, reading the shared
// Job index when the local index misses.
func (r *Registry) ByJobInWorkspace(ws, jobName string) (TaskEntry, bool) {
	if r.shared != nil && jobName != "" {
		ctx, cancel := r.sharedCtx()
		e, ok, err := r.shared.GetEntryByJob(ctx, jobName)
		cancel()
		if err == nil {
			if !ok {
				r.mu.RLock()
				id := r.byJob[jobName]
				r.mu.RUnlock()
				if id != "" {
					r.dropLocalByKey(id)
				}
				return TaskEntry{}, false
			}
			if ws != "" && e.WorkspaceID != ws {
				return TaskEntry{}, false
			}
			r.Cache(e)
			return e, true
		}
	}
	r.mu.RLock()
	if id, ok := r.byJob[jobName]; ok {
		e, ok := r.live[id]
		r.mu.RUnlock()
		return e, ok
	}
	r.mu.RUnlock()
	return TaskEntry{}, false
}

// readShared reads one row from the shared state; ok=false with a nil error is
// the authoritative "no such row".
func (r *Registry) readShared(ws, taskID string) (TaskEntry, bool, error) {
	if r.shared == nil || ws == "" || taskID == "" {
		return TaskEntry{}, false, nil
	}
	ctx, cancel := r.sharedCtx()
	defer cancel()
	e, ok, err := r.shared.GetEntry(ctx, ws, taskID)
	if err != nil || !ok {
		return TaskEntry{}, false, err
	}
	r.Cache(e)
	return e, true, nil
}

// fetchShared reads one row from the shared state and caches it, folding a
// store error into a miss (the caller's cache/grace path owns the outage).
func (r *Registry) fetchShared(ws, taskID string) (TaskEntry, bool) {
	e, ok, err := r.readShared(ws, taskID)
	if err != nil {
		return TaskEntry{}, false
	}
	return e, ok
}

// SettledInWorkspace answers the retained terminal record of a task the local
// index does not know: the settled row is the shared post-reclaim answer face
// (§4.3 终态记录保留窗).
func (r *Registry) SettledInWorkspace(ws, taskID string) (TaskEntry, bool) {
	if e, ok := r.Settled(taskID, ws); ok {
		return e, true
	}
	if r.state == nil || ws == "" || taskID == "" {
		return TaskEntry{}, false
	}
	ctx, cancel := r.sharedCtx()
	defer cancel()
	e, ok, err := r.state.GetSettled(ctx, ws, taskID)
	if err != nil || !ok {
		return TaskEntry{}, false
	}
	r.mu.Lock()
	at := e.CompletedAt
	if at.IsZero() {
		at = e.DrainDeadline
	}
	r.done.put(e.Key(), at, settledRecord(e))
	r.mu.Unlock()
	return e, true
}

// SettledInScope answers the retained terminal record under one
// source/workspace partition.
func (r *Registry) SettledInScope(scopeKey, taskID string) (TaskEntry, bool) {
	if e, ok := r.Settled(taskID, scopeKey); ok {
		return e, true
	}
	if r.state == nil || scopeKey == "" || taskID == "" {
		return TaskEntry{}, false
	}
	ctx, cancel := r.sharedCtx()
	defer cancel()
	e, ok, err := r.state.GetSettled(ctx, scopeKey, taskID)
	if err != nil || !ok {
		return TaskEntry{}, false
	}
	r.mu.Lock()
	at := e.CompletedAt
	if at.IsZero() {
		at = e.DrainDeadline
	}
	r.done.put(e.Key(), at, settledRecord(e))
	r.mu.Unlock()
	return e, true
}

// ListInWorkspace returns the rows of one workspace for the ops projection.
// With the shared face configured the list comes from the store (any instance
// answers the same, §9.3 多实例口径); a store failure falls back to the local
// index. An empty workspace returns every row (the legacy single-workspace
// shape).
func (r *Registry) ListInWorkspace(workspaceID string) []TaskEntry {
	if r.shared != nil {
		ctx, cancel := r.sharedCtx()
		defer cancel()
		if entries, err := r.shared.ListEntries(ctx); err == nil {
			out := make([]TaskEntry, 0, len(entries))
			for _, e := range entries {
				if workspaceID == "" || e.WorkspaceID == workspaceID {
					out = append(out, e)
				}
			}
			return out
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TaskEntry, 0, len(r.live))
	for _, e := range r.live {
		if workspaceID == "" || e.WorkspaceID == workspaceID {
			out = append(out, e)
		}
	}
	return out
}

// ListInScope returns the rows of one source/workspace partition. An empty
// partition key keeps the legacy every-row shape.
func (r *Registry) ListInScope(scopeKey string) []TaskEntry {
	if r.shared != nil {
		ctx, cancel := r.sharedCtx()
		defer cancel()
		if entries, err := r.shared.ListEntries(ctx); err == nil {
			out := make([]TaskEntry, 0, len(entries))
			for _, e := range entries {
				if scopeKey == "" || e.ScopeKey() == scopeKey {
					out = append(out, e)
				}
			}
			return out
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TaskEntry, 0, len(r.live))
	for _, e := range r.live {
		if scopeKey == "" || e.ScopeKey() == scopeKey {
			out = append(out, e)
		}
	}
	return out
}

// PersistShared writes one row through the shared face. The K8s rebuild uses
// it so a rebuilt mapping row is the shared adjudication truth, not only a
// local cache entry.
func (r *Registry) PersistShared(ctx context.Context, e TaskEntry) error {
	if r.shared == nil {
		return nil
	}
	return r.shared.SetEntry(ctx, e.ScopeKey(), e.TaskID, e)
}

// Resync replaces the local index with the shared state's rows. The takeover
// path calls it before the new control plane serves (AC-77 接管先对账后清扫):
// rows the store no longer holds are dropped (the shared state is the
// adjudication source, F11), and the settled records are re-seeded.
func (r *Registry) Resync(ctx context.Context) error {
	if r.shared == nil {
		return nil
	}
	entries, err := r.shared.ListEntries(ctx)
	if err != nil {
		return err
	}
	settled, err := r.shared.ListSettled(ctx)
	if err != nil {
		return err
	}
	revoked, err := r.shared.ListRevoked(ctx)
	_ = revoked // the issuer owns the revocation cache; read here only to
	// prove the set is reachable before the new leader serves.
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	live := make(map[string]TaskEntry, len(entries))
	byJob := make(map[string]string, len(entries))
	byDaemon := make(map[string]string, len(entries))
	runtimes := make(map[string]JobRuntime, len(entries))
	runtimeByDaemon := make(map[string]string, len(entries))
	for _, e := range entries {
		if e.TaskID == "" {
			continue
		}
		key := e.Key()
		live[key] = e
		if e.JobName != "" {
			byJob[e.JobName] = key
		}
		if e.DaemonID != "" {
			byDaemon[e.DaemonID] = key
		}
		if e.JobRuntimeID != "" {
			runtimes[e.JobRuntimeID] = JobRuntime{
				JobRuntimeID:    e.JobRuntimeID,
				DaemonID:        e.DaemonID,
				TaskID:          e.TaskID,
				Provider:        "omp",
				Name:            "foreman-job",
				Online:          true,
				LastHeartbeatAt: r.now(),
			}
			if e.DaemonID != "" {
				runtimeByDaemon[e.DaemonID] = e.JobRuntimeID
			}
		}
	}
	r.live = live
	r.byJob = byJob
	r.byDaemon = byDaemon
	r.runtimes = runtimes
	r.runtimeByDaemon = runtimeByDaemon
	done := newTTLMap[TaskEntry](DoneIndexTTL, r.now)
	for _, rec := range settled {
		if rec.TaskID == "" {
			continue
		}
		at := rec.CompletedAt
		if at.IsZero() {
			at = rec.DrainDeadline
		}
		done.put(rec.Key(), at, settledRecord(rec))
	}
	r.done = done
	return nil
}
