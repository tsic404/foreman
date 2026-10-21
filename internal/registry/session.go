package registry

import (
	"encoding/json"
	"path"
	"strings"
	"time"
)

// sessionNodeRow is the shared soft-affinity row (§4.5: the index is visible
// across instances and partitioned by workspace).
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

// sessionNodeEntry is one learned row: the node that held a session's file
// when the session's task reported its verdict, and when Foreman learned it.
type sessionNodeEntry struct {
	node      string
	learnedAt time.Time
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
	r.sessionNode[sessionIndexKey(workspaceID, key)] = sessionNodeEntry{node: nodeName, learnedAt: now}
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
	raw, ok, err := r.shared.GetSessionNode(ctx, workspaceID, key)
	if err != nil || !ok {
		return ""
	}
	var row sessionNodeRow
	if json.Unmarshal(raw, &row) != nil || row.Node == "" {
		return ""
	}
	r.mu.Lock()
	r.sessionNode[sessionIndexKey(workspaceID, key)] = sessionNodeEntry{node: row.Node, learnedAt: row.LearnedAt}
	r.mu.Unlock()
	return row.Node
}

// sessionIndexKey is the workspace-partitioned session index key. The
// separator is not valid in either component, so no pair collides with
// another.
func sessionIndexKey(workspaceID, key string) string {
	return workspaceID + "\x00" + key
}

// PruneExpiredSessions evicts the entries whose learned_at is older than ttl
// and returns how many were dropped (FOREMAN_AGENT_STATE_TTL, run once per
// Reconcile round). A non-positive ttl disables expiry entirely: the index is
// a memory-only hint, so keeping a stale entry only costs a preference the
// scheduler may ignore anyway. Expiry is the reconcile round's job, never the
// lookup's, so the injected item and the retention window share one clock.
func (r *Registry) PruneExpiredSessions(ttl time.Duration) int {
	if ttl <= 0 {
		return 0
	}
	deadline := r.now().Add(-ttl)
	r.mu.Lock()
	dropped := 0
	var expired []string
	for key, entry := range r.sessionNode {
		if entry.learnedAt.Before(deadline) {
			delete(r.sessionNode, key)
			expired = append(expired, key)
			dropped++
		}
	}
	r.mu.Unlock()
	if r.shared == nil {
		return dropped
	}
	// The shared rows are pruned on the same window. The round runs on the
	// control-plane holder (the only instance reconciling), so the sweep is
	// single-writer. The row body carries its own (workspace, key), so the
	// deletion does not depend on the store's key layout.
	ctx, cancel := r.sharedCtx()
	defer cancel()
	for _, key := range expired {
		ws, sessionKey, ok := strings.Cut(key, "\x00")
		if !ok {
			continue
		}
		if err := r.shared.DeleteSessionNode(ctx, ws, sessionKey); err != nil {
			r.stateFailure("session_node_prune", sessionKey, err)
		}
	}
	rows, err := r.shared.ListSessionNodes(ctx)
	if err != nil {
		return dropped
	}
	for _, raw := range rows {
		var row sessionNodeRow
		if json.Unmarshal(raw, &row) != nil {
			continue
		}
		if !row.LearnedAt.Before(deadline) {
			continue
		}
		if row.WorkspaceID == "" || row.Key == "" {
			continue
		}
		_ = r.shared.DeleteSessionNode(ctx, row.WorkspaceID, row.Key)
	}
	return dropped
}
