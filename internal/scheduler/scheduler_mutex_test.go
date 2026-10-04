package scheduler

import (
	"context"
	"testing"
	"time"
)

// lenEntry reads the live key count under the mutex (same-package test seam;
// no exported getter just for tests).
func (k *keyedMutex) lenEntries() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.m)
}

// refsOf reads the refcount of one key under the mutex.
func (k *keyedMutex) refsOf(key string) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	if rm, ok := k.m[key]; ok {
		return rm.refs
	}
	return -1
}

func TestKeyedMutexReclaimsEntry(t *testing.T) {
	k := newKeyedMutex()

	unlock := k.Lock("t1")
	if got := k.lenEntries(); got != 1 {
		t.Fatalf("entries = %d, want 1", got)
	}

	// A blocked waiter is counted in refs before it holds the lock.
	waiterHeld := make(chan struct{})
	releaseWaiter := make(chan struct{})
	waiterDone := make(chan struct{})
	go func() {
		u := k.Lock("t1")
		close(waiterHeld)
		<-releaseWaiter
		u()
		close(waiterDone)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for k.refsOf("t1") != 2 {
		if time.Now().After(deadline) {
			t.Fatal("waiter never entered refs")
		}
		time.Sleep(time.Millisecond)
	}
	unlock()

	select {
	case <-waiterHeld:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter never acquired the lock")
	}
	// While the waiter holds the lock the entry must still exist.
	if got := k.lenEntries(); got != 1 {
		t.Fatalf("entries = %d while waiter holds lock, want 1", got)
	}

	close(releaseWaiter)
	<-waiterDone
	if got := k.lenEntries(); got != 0 {
		t.Fatalf("entries = %d after all unlocks, want 0 (leak)", got)
	}
}

func TestKeyedMutexSeparateKeysAreIndependent(t *testing.T) {
	k := newKeyedMutex()
	u1 := k.Lock("t1")
	// A different key must not block behind t1's holder.
	done := make(chan struct{})
	go func() { u2 := k.Lock("t2"); u2(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("independent key blocked")
	}
	u1()
	if got := k.lenEntries(); got != 0 {
		t.Fatalf("entries = %d, want 0", got)
	}
}

func TestConcurrentOnClaimLeavesNoMutexEntry(t *testing.T) {
	f := newFixture(t)
	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if got := f.sched.claimMu.lenEntries(); got != 0 {
		t.Fatalf("claimMu entries = %d after OnClaim, want 0", got)
	}
}
