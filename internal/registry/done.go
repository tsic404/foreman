package registry

import (
	"sync"
	"time"
)

// ttlMap is the done index: terminal task_id → completion time. Entries
// expire after ttl (24h per contract §3.1); expiry is lazy on read with an
// amortized sweep on write, which is enough at the 10² in-flight scale.
type ttlMap struct {
	mu  sync.Mutex
	ttl time.Duration
	now func() time.Time
	m   map[string]time.Time
}

func newTTLMap(ttl time.Duration, now func() time.Time) *ttlMap {
	return &ttlMap{ttl: ttl, now: now, m: make(map[string]time.Time)}
}

func (t *ttlMap) put(key string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.m[key] = at
	// Amortized sweep: keeps the map bounded without a background goroutine.
	if len(t.m) > 1024 {
		t.sweepLocked()
	}
}

// has reports whether key was put within the ttl window.
func (t *ttlMap) has(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	at, ok := t.m[key]
	if !ok {
		return false
	}
	if t.now().Sub(at) > t.ttl {
		delete(t.m, key)
		return false
	}
	return true
}

func (t *ttlMap) sweepLocked() {
	cutoff := t.now().Add(-t.ttl)
	for k, at := range t.m {
		if at.Before(cutoff) {
			delete(t.m, k)
		}
	}
}
