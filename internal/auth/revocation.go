package auth

import (
	"crypto/sha256"
	"sync"
	"time"
)

// RevocationSet tracks revoked Job Tokens by SHA-256 digest in process
// memory. Entries live only for the token's remaining validity window, so
// the set stays small and needs no persistence: after terminal the registry
// lookup in Verify rejects the token anyway.
type RevocationSet struct {
	mu      sync.Mutex
	now     func() time.Time
	entries map[[sha256.Size]byte]time.Time
}

// NewRevocationSet builds an empty set; nil now uses the wall clock.
func NewRevocationSet(now func() time.Time) *RevocationSet {
	if now == nil {
		now = time.Now
	}
	return &RevocationSet{now: now, entries: make(map[[sha256.Size]byte]time.Time)}
}

// Revoke records the token digest until validUntil. A token whose validity
// window already ended needs no entry: Verify's expiry step rejects it.
// Insertion amortizes a sweep of lapsed entries: after terminal a token is
// never presented again (Verify's registry step rejects it first), so
// lookup-time eviction alone would let the set grow without bound.
func (r *RevocationSet) Revoke(token string, validUntil time.Time) {
	if !r.now().Before(validUntil) {
		return
	}
	digest := sha256.Sum256([]byte(token))
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for d, until := range r.entries {
		if !now.Before(until) {
			delete(r.entries, d)
		}
	}
	r.entries[digest] = validUntil
}

// IsRevoked reports whether the token digest is in the set; lapsed entries
// are evicted on lookup as a fast path (insert-time sweep is the bound).
func (r *RevocationSet) IsRevoked(token string) bool {
	digest := sha256.Sum256([]byte(token))
	r.mu.Lock()
	defer r.mu.Unlock()
	until, ok := r.entries[digest]
	if !ok {
		return false
	}
	if !r.now().Before(until) {
		delete(r.entries, digest)
		return false
	}
	return true
}
