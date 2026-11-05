package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/state"
)

// renewalCountingStore observes the holder's renewals while delegating them to
// the real store, so a case can wait for the window it needs instead of
// sleeping past it.
type renewalCountingStore struct {
	state.CoordStore
	renewer  state.TaskLockRenewer
	renewals chan struct{}
}

func (s *renewalCountingStore) RenewTaskLock(ctx context.Context, ws, taskID, owner string, ttl time.Duration) (bool, error) {
	ok, err := s.renewer.RenewTaskLock(ctx, ws, taskID, owner, ttl)
	if ok && err == nil {
		select {
		case s.renewals <- struct{}{}:
		default:
		}
	}
	return ok, err
}

// TestTaskLockStaysHeldPastItsTTL pins the lease's renewal face: while the
// holder is working, the lock must outlive the TTL it was acquired with,
// otherwise a second instance takes the same task mid-decision and the
// cross-instance single write lapses (§4.2 跨实例单写).
func TestTaskLockStaysHeldPastItsTTL(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	// TTLs follow the wall clock, or the held-past-TTL check cannot fail.
	mr.ClockTTL(true)
	st, err := state.New(state.Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    48 * time.Hour,
	})
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	coord := st.(state.CoordStore)
	counting := &renewalCountingStore{
		CoordStore: coord,
		renewer:    st.(state.TaskLockRenewer),
		renewals:   make(chan struct{}, 4),
	}
	f := newFixture(t)
	f.sched.shared = counting
	f.sched.instanceID = "inst-1"
	ttl := 400 * time.Millisecond
	f.sched.taskLockTTL = ttl

	acquired := time.Now()
	unlock, err := f.sched.lockTaskIn(ctx, "ws-1", "task-1")
	if err != nil {
		t.Fatalf("lockTaskIn: %v", err)
	}
	// Two renewals arm the lease at least one TTL past the original deadline;
	// waiting beyond that deadline is what makes the assertion discriminating.
	for range 2 {
		select {
		case <-counting.renewals:
		case <-time.After(5 * time.Second):
			t.Fatal("the holder stopped renewing its lease")
		}
	}
	if wait := acquired.Add(5 * ttl / 4).Sub(time.Now()); wait > 0 {
		time.Sleep(wait)
	}
	if ok, err := coord.LockTask(ctx, "ws-1", "task-1", "inst-2/1", time.Second); err != nil || ok {
		t.Fatalf("the lease lapsed under the holder: ok=%v err=%v", ok, err)
	}
	unlock()
	if ok, err := coord.LockTask(ctx, "ws-1", "task-1", "inst-2/1", time.Second); err != nil || !ok {
		t.Fatalf("the released lease is not free: ok=%v err=%v", ok, err)
	}
}

// flakyRenewalStore fails exactly one renewal before delegating, modelling a
// store that hiccupped while the holder was still working.
type flakyRenewalStore struct {
	state.CoordStore
	renewer  state.TaskLockRenewer
	failed   chan struct{}
	renewals chan struct{}
	injected bool
}

func (s *flakyRenewalStore) RenewTaskLock(ctx context.Context, ws, taskID, owner string, ttl time.Duration) (bool, error) {
	if !s.injected {
		s.injected = true
		close(s.failed)
		return false, errors.New("shared state unavailable")
	}
	ok, err := s.renewer.RenewTaskLock(ctx, ws, taskID, owner, ttl)
	if ok && err == nil {
		select {
		case s.renewals <- struct{}{}:
		default:
		}
	}
	return ok, err
}

// TestTaskLockSurvivesATransientRenewalError pins the renewer's other exit
// path: a store error says nothing about whether the hold is still live, so
// the renewer must keep going and the lock must still guard the task past its
// original TTL. Quitting on that error drops the lease mid-decision and lets
// another instance take the same task (§4.2 跨实例单写).
func TestTaskLockSurvivesATransientRenewalError(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	// TTLs follow the wall clock, or the held-past-TTL check cannot fail.
	mr.ClockTTL(true)
	st, err := state.New(state.Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    48 * time.Hour,
	})
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	coord := st.(state.CoordStore)
	flaky := &flakyRenewalStore{
		CoordStore: coord,
		renewer:    st.(state.TaskLockRenewer),
		failed:     make(chan struct{}),
		renewals:   make(chan struct{}, 4),
	}
	f := newFixture(t)
	f.sched.shared = flaky
	f.sched.instanceID = "inst-1"
	ttl := 900 * time.Millisecond
	f.sched.taskLockTTL = ttl

	acquired := time.Now()
	unlock, err := f.sched.lockTaskIn(ctx, "ws-1", "task-1")
	if err != nil {
		t.Fatalf("lockTaskIn: %v", err)
	}
	select {
	case <-flaky.failed:
	case <-time.After(5 * time.Second):
		t.Fatal("the holder never renewed its lease")
	}
	// One successful renewal after the error proves the renewer is still
	// running; a renewer that quit at the failed tick never sends it. This
	// records the stall and falls through, so the lease check below reports
	// the consequence on its own instead of being masked.
	select {
	case <-flaky.renewals:
	case <-time.After(5 * time.Second):
		t.Error("the renewer stopped after one transient store error")
	}
	if wait := acquired.Add(5 * ttl / 4).Sub(time.Now()); wait > 0 {
		time.Sleep(wait)
	}
	if ok, err := coord.LockTask(ctx, "ws-1", "task-1", "inst-2/1", time.Second); err != nil || ok {
		t.Fatalf("a transient renewal error cost the holder its lease: ok=%v err=%v", ok, err)
	}
	unlock()
	if ok, err := coord.LockTask(ctx, "ws-1", "task-1", "inst-2/1", time.Second); err != nil || !ok {
		t.Fatalf("the released lease is not free: ok=%v err=%v", ok, err)
	}
}

// TestTaskLockOwnerNamesEachAcquisition pins the value that makes a late
// unlock harmless: two acquisitions in one process must name different holds,
// or the first one's unlock deletes the second one's lock.
func TestTaskLockOwnerNamesEachAcquisition(t *testing.T) {
	f := newFixture(t)
	store := newLockStore()
	f.sched.shared = store
	f.sched.instanceID = "inst-1"
	f.sched.taskLockTTL = time.Minute

	for range 2 {
		unlock, err := f.sched.lockTaskIn(context.Background(), "ws-1", "task-1")
		if err != nil {
			t.Fatalf("lockTaskIn: %v", err)
		}
		unlock()
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.calls) != 2 {
		t.Fatalf("LockTask calls = %d, want one per acquisition", len(store.calls))
	}
	first, second := store.calls[0].owner, store.calls[1].owner
	if first == second {
		t.Fatalf("both acquisitions name the same hold %q", first)
	}
	for _, owner := range []string{first, second} {
		if !strings.HasPrefix(owner, "inst-1/") {
			t.Errorf("owner = %q, want the instance id and an acquisition suffix", owner)
		}
	}
}

// failingResetStore answers the takeover's exact counter write with an error;
// the reads it needs answer empty.
type failingResetStore struct {
	state.CoordStore
}

func (s *failingResetStore) ListEntries(context.Context) ([]registry.TaskEntry, error) {
	return nil, nil
}

func (s *failingResetStore) ResetSlots(context.Context, string, int64) (int64, int64, error) {
	return 0, 0, errors.New("shared state unavailable")
}

// TestOnTakeoverRecordsAFailedSlotReset pins the reconciliation metric's
// truthfulness: a takeover whose exact counter write failed must report
// failed, not ok — otherwise the operator reads a repair that never happened
// while the counters stay unrepaired (§9.1, AC-76/AC-78).
func TestOnTakeoverRecordsAFailedSlotReset(t *testing.T) {
	f := newFixture(t)
	f.sched.shared = &failingResetStore{}
	if err := f.sched.OnTakeover(context.Background()); err == nil {
		t.Fatal("OnTakeover: want the reset failure surfaced")
	}
	got := f.metrics.reconcile
	if len(got) != 1 || got[0] != "failed" {
		t.Fatalf("reconcile results = %v, want [failed]", got)
	}
}
