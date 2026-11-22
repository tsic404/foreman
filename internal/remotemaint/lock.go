package remotemaint

import (
	"context"
	"sync"
)

// KeyedLocker is the in-process domain lock. The writer path and the
// maintenance path share one instance, so a bare-cache write and its
// reclamation of the same environment/source/workspace never overlap.
type KeyedLocker struct {
	mu    sync.Mutex
	locks map[string]chan struct{}
}

// NewKeyedLocker returns an empty locker.
func NewKeyedLocker() *KeyedLocker {
	return &KeyedLocker{locks: map[string]chan struct{}{}}
}

// Lock blocks until the domain is free or ctx ends.
func (l *KeyedLocker) Lock(ctx context.Context, key string) (func(), error) {
	l.mu.Lock()
	if l.locks == nil {
		l.locks = map[string]chan struct{}{}
	}
	gate, ok := l.locks[key]
	if !ok {
		gate = make(chan struct{}, 1)
		l.locks[key] = gate
	}
	l.mu.Unlock()
	select {
	case gate <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-gate }) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// MemoryAuditor keeps the round's decisions for an acceptance read.
type MemoryAuditor struct {
	mu      sync.Mutex
	entries []AuditEntry
}

// NewMemoryAuditor returns an empty auditor.
func NewMemoryAuditor() *MemoryAuditor { return &MemoryAuditor{} }

// Record appends one decision.
func (a *MemoryAuditor) Record(_ context.Context, entry AuditEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = append(a.entries, entry)
}

// Entries returns a copy of every recorded decision.
func (a *MemoryAuditor) Entries() []AuditEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]AuditEntry, len(a.entries))
	copy(out, a.entries)
	return out
}
