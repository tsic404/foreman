package registry

import (
	"encoding/json"
	"path"
	"strings"
	"time"
)

// sessionNodeRow is the shared soft-affinity row (§4.5: the index is visible
// across instances and partitioned by workspace). Only node drives a decision;
// the other fields are carried so a reader from an older build still sees the
// row it expects during a rolling upgrade — learned_at is never an expiry
// input, the row's lifetime is the store-side TTL.
type sessionNodeRow struct {
	WorkspaceID string    `json:"workspace_id"`
	Key         string    `json:"key"`
	Node        string    `json:"node"`
	LearnedAt   time.Time `json:"learned_at"`
}

// SessionKey normalizes a session identifier into the soft-affinity index key
// (§4.5 归一化): an empty value stays empty (first run),
// a value without "/" is a bare id and stays as is, and a path keeps only its
// base name without extension. The learning side (the terminal report body's
// session_id) and the payload side (prior_session_id) MUST call this one
// function — it removes path spelling differences, not session ambiguity, so
// two callers normalizing differently would simply stop matching.
func SessionKey(v string) string {
	if v == "" {
		return ""
	}
	if !strings.Contains(v, "/") {
		return v
	}
	base := path.Base(v)
	if ext := path.Ext(base); ext != "" {
		base = strings.TrimSuffix(base, ext)
	}
	return base
}

// sessionNodeEntry is one learned row in this process's index: the node, the
// local-clock instant this process learned or read it, and the local-clock
// instant the entry expires (zero = never). Both timestamps are this process's
// clock — a row learned elsewhere is cached from its remaining store lifetime,
// never from the writer's stamp.
type sessionNodeEntry struct {
	node      string
	learnedAt time.Time
	expiresAt time.Time
}

// localSessionExpiry is the local-clock deadline of an entry learned here: the
// configured retention, or zero (never) when expiry is switched off.
func (r *Registry) localSessionExpiry(now time.Time) time.Time {
	if r.sessionTTL <= 0 {
		return time.Time{}
	}
	return now.Add(r.sessionTTL)
}

// LearnSession records the node a session's file lives on, keyed by
// (workspace, normalized session key) — the session index is partitioned by
// workspace (§4.5). The only learning face is the terminal report body
// (S13/S14), so an empty key or an unknown node leaves the index untouched
// rather than storing a key that can never hit. Learning the same key again
// overwrites node and timestamp: the later report describes where the session
// is now.
func (r *Registry) LearnSession(workspaceID, key, nodeName string) {
	if key == "" || nodeName == "" {
		return
	}
	now := r.now()
	r.mu.Lock()
	r.sessionNode[sessionIndexKey(workspaceID, key)] = sessionNodeEntry{node: nodeName, learnedAt: now, expiresAt: r.localSessionExpiry(now)}
	r.mu.Unlock()
	if r.shared == nil {
		return
	}
	raw, err := json.Marshal(sessionNodeRow{WorkspaceID: workspaceID, Key: key, Node: nodeName, LearnedAt: now})
	if err != nil {
		return
	}
	ctx, cancel := r.sharedCtx()
	defer cancel()
	if err := r.shared.PutSessionNode(ctx, workspaceID, key, raw, r.sessionTTL); err != nil {
		r.stateFailure("session_node", key, err)
	}
}

// LastNodeForSession implements jobbuilder.NodeIndex: the node a session's
// file was last seen on, "" when the (workspace, key) pair is unknown — which,
// like an expired or saturated entry, means "no soft affinity for this task".
func (r *Registry) LastNodeForSession(workspaceID, key string) string {
	if key == "" {
		return ""
	}
	r.mu.RLock()
	node := r.sessionNode[sessionIndexKey(workspaceID, key)].node
	r.mu.RUnlock()
	if node != "" || r.shared == nil {
		return node
	}
	// The index is shared: a session learned on another instance is a hit
	// here (§4.5 跨实例可见). A missed or failed read degrades to "no soft
	// affinity", never an error.
	ctx, cancel := r.sharedCtx()
	defer cancel()
	shared, ok, err := r.shared.GetSessionNode(ctx, workspaceID, key)
	if err != nil || !ok {
		return ""
	}
	var row sessionNodeRow
	if json.Unmarshal(shared.Raw, &row) != nil || row.Node == "" {
		return ""
	}
	now := r.now()
	entry := sessionNodeEntry{node: row.Node, learnedAt: now}
	if shared.Remaining > 0 {
		entry.expiresAt = now.Add(shared.Remaining)
	}
	r.mu.Lock()
	r.sessionNode[sessionIndexKey(workspaceID, key)] = entry
	r.mu.Unlock()
	return row.Node
}

// sessionIndexKey is the workspace-partitioned session index key. The
// separator is not valid in either component, so no pair collides with
// another.
func sessionIndexKey(workspaceID, key string) string {
	return workspaceID + "\x00" + key
}

// PruneExpiredSessions evicts this process's index entries whose retention has
// passed and returns how many were dropped (FOREMAN_AGENT_STATE_TTL, run once
// per Reconcile round). A non-positive ttl disables expiry entirely: the index
// is a hint, so keeping a stale entry only costs a preference the scheduler may
// ignore anyway. Entries carry a local-clock deadline; a row learned on another
// instance gets its deadline from the store's remaining lifetime (PTTL), so no
// two hosts' timestamps are ever compared. The shared rows themselves expire by
// their store-side TTL — the leader does not delete other instances' rows by
// its own clock.
func (r *Registry) PruneExpiredSessions(ttl time.Duration) int {
	if ttl <= 0 {
		return 0
	}
	now := r.now()
	deadline := now.Add(-ttl)
	r.mu.Lock()
	dropped := 0
	for key, entry := range r.sessionNode {
		if entry.learnedAt.Before(deadline) || (!entry.expiresAt.IsZero() && !entry.expiresAt.After(now)) {
			delete(r.sessionNode, key)
			dropped++
		}
	}
	r.mu.Unlock()
	return dropped
}
