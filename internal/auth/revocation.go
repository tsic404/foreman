package auth

import (
	"sync"
	"time"
)

// RevocationSet tracks revoked Job Tokens by SHA-256 digest in process memory,
// partitioned by workspace (§6.1): a digest revoked in one workspace must never
// adjudicate another workspace's token. Entries live only for the token's
// remaining validity window, so the set stays small and needs no persistence.
//
// Its one irreplaceable job (ADR-017) is the same-name reuse face: after a
// task is re-dispatched or its Job is rebuilt under the same name, the entry
// reappears and would otherwise authorize the predecessor's token. Once the
// entry is gone for good, Verify's entry adjudication rejects the token first.
type RevocationSet struct {
	mu      sync.Mutex
	now     func() time.Time
	entries map[string]time.Time // workspace + NUL + hex sha256 → valid until
}

// NewRevocationSet builds an empty set; nil now uses the wall clock.
func NewRevocationSet(now func() time.Time) *RevocationSet {
	if now == nil {
		now = time.Now
	}
	return &RevocationSet{now: now, entries: make(map[string]time.Time)}
}

// revocationKey carries the set's two dimensions in one map key. A workspace
// id never contains NUL, so the join cannot be ambiguous.
func revocationKey(workspaceID, hash string) string {
	return workspaceID + "\x00" + hash
}

// Revoke records the credential digest of one workspace until validUntil. hash
// is the hex SHA-256 of the token (Issuer.Hash). A token whose validity window
// already ended needs no entry: Verify's expiry step rejects it. Insertion
// amortizes a sweep of lapsed entries, so the set cannot grow without bound.
func (r *RevocationSet) Revoke(workspaceID, hash string, validUntil time.Time) {
	if hash == "" || !r.now().Before(validUntil) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for key, until := range r.entries {
		if !now.Before(until) {
			delete(r.entries, key)
		}
	}
	r.entries[revocationKey(workspaceID, hash)] = validUntil
}

// IsRevoked reports whether the credential digest is revoked for that
// workspace; lapsed entries are evicted on lookup as a fast path (insert-time
// sweep is the bound). Both this method and Revoke take the digest: the set is
// keyed by it, and a raw-token lookup could only ever answer false and leave
// the caller believing a revocation had failed.
func (r *RevocationSet) IsRevoked(workspaceID, hash string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := revocationKey(workspaceID, hash)
	until, ok := r.entries[key]
	if !ok {
		return false
	}
	if !r.now().Before(until) {
		delete(r.entries, key)
		return false
	}
	return true
}
