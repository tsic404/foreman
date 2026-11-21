package registry

import (
	"strings"
	"sync"
	"time"
)

// ttlMap is a lazily-expiring keyed store: entries older than ttl are dropped
// on read, with an amortized sweep on write, which is enough at the 10²
// in-flight scale.
type ttlMap[T any] struct {
	mu  sync.Mutex
	ttl time.Duration
	now func() time.Time
	m   map[string]ttlValue[T]
}

type ttlValue[T any] struct {
	at time.Time
	v  T
}

func newTTLMap[T any](ttl time.Duration, now func() time.Time) *ttlMap[T] {
	return &ttlMap[T]{ttl: ttl, now: now, m: make(map[string]ttlValue[T])}
}

func (t *ttlMap[T]) put(key string, at time.Time, v T) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.m[key] = ttlValue[T]{at: at, v: v}
	// Amortized sweep: keeps the map bounded without a background goroutine.
	if len(t.m) > 1024 {
		t.sweepLocked()
	}
}

// has reports whether key was put within the ttl window.
func (t *ttlMap[T]) has(key string) bool {
	_, _, ok := t.get(key)
	return ok
}

// hasTask reports whether any live key names taskID, under any partition.
func (t *ttlMap[T]) hasTask(taskID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	cutoff := t.now().Add(-t.ttl)
	for key, entry := range t.m {
		if entry.at.Before(cutoff) {
			delete(t.m, key)
			continue
		}
		if key == taskID || strings.HasSuffix(key, "\x00"+taskID) {
			return true
		}
	}
	return false
}

// findTask resolves a bare task id to its key when exactly one live entry
// names it. A partitioned store can carry the same id twice; that ambiguity
// is reported as not found rather than resolved to one partition.
func (t *ttlMap[T]) findTask(taskID string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	cutoff := t.now().Add(-t.ttl)
	found := ""
	for key, entry := range t.m {
		if entry.at.Before(cutoff) {
			delete(t.m, key)
			continue
		}
		if key == taskID || strings.HasSuffix(key, "\x00"+taskID) {
			if found != "" {
				return "", false
			}
			found = key
		}
	}
	return found, found != ""
}

// findScopedTask resolves a bare task id to the one partitioned key that
// names it, ignoring the historical bare key. The live index prefers the
// partitioned row when both shapes exist, so the settled index does too.
func (t *ttlMap[T]) findScopedTask(taskID string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	cutoff := t.now().Add(-t.ttl)
	found := ""
	for key, entry := range t.m {
		if entry.at.Before(cutoff) {
			delete(t.m, key)
			continue
		}
		if key == taskID || !strings.HasSuffix(key, "\x00"+taskID) {
			continue
		}
		if found != "" {
			return "", false
		}
		found = key
	}
	return found, found != ""
}

// get returns the value and time recorded for key when it is still within the
// ttl window. The time backs the drain duration (verdict → reclaim), which
// must not be restarted by a repeated terminal report.
func (t *ttlMap[T]) get(key string) (T, time.Time, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.m[key]
	if !ok {
		var zero T
		return zero, time.Time{}, false
	}
	if t.now().Sub(entry.at) > t.ttl {
		delete(t.m, key)
		var zero T
		return zero, time.Time{}, false
	}
	return entry.v, entry.at, true
}

func (t *ttlMap[T]) sweepLocked() {
	cutoff := t.now().Add(-t.ttl)
	for k, entry := range t.m {
		if entry.at.Before(cutoff) {
			delete(t.m, k)
		}
	}
}
