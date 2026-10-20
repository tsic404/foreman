package registry

import (
	"path"
	"strings"
	"time"
)

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
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessionNode[sessionIndexKey(workspaceID, key)] = sessionNodeEntry{node: nodeName, learnedAt: r.now()}
}

// LastNodeForSession implements jobbuilder.NodeIndex: the node a session's
// file was last seen on, "" when the (workspace, key) pair is unknown — which,
// like an expired or saturated entry, means "no soft affinity for this task".
func (r *Registry) LastNodeForSession(workspaceID, key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sessionNode[sessionIndexKey(workspaceID, key)].node
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
	r.mu.Lock()
	defer r.mu.Unlock()
	deadline := r.now().Add(-ttl)
	dropped := 0
	for key, entry := range r.sessionNode {
		if entry.learnedAt.Before(deadline) {
			delete(r.sessionNode, key)
			dropped++
		}
	}
	return dropped
}
