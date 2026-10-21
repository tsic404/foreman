package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/state"
)

// fakeCoordStore is the shared-store seam of these unit tests: only the slot
// primitives are real, and their failures are scriptable.
type fakeCoordStore struct {
	state.CoordStore

	mu           sync.Mutex
	counts       map[string]int64
	reservations map[string]int64
	snapshotErr  error
	consumeFails int
	// consumeFailsAfterApply models a pop that landed while its response was
	// lost: the member is gone and the caller saw an error.
	consumeFailsAfterApply int
	consumeCalls           int
	setCalls               []slotWrite
	casCalls               []slotWrite
}

type slotWrite struct {
	ws       string
	expected int64
	next     int64
}

func (f *fakeCoordStore) ConsumeSlotReservation(_ context.Context, ws string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumeCalls++
	if f.consumeFailsAfterApply > 0 {
		f.consumeFailsAfterApply--
		if f.reservations[ws] > 0 {
			f.reservations[ws]--
		}
		return errors.New("consume response lost")
	}
	if f.consumeFails > 0 {
		f.consumeFails--
		return errors.New("consume down")
	}
	if f.reservations[ws] > 0 {
		f.reservations[ws]--
	}
	return nil
}

func (f *fakeCoordStore) SlotSnapshot(_ context.Context, ws string) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snapshotErr != nil {
		return 0, 0, f.snapshotErr
	}
	return f.counts[ws], f.reservations[ws], nil
}

func (f *fakeCoordStore) SetSlots(_ context.Context, ws string, n int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setCalls = append(f.setCalls, slotWrite{ws: ws, next: n})
	f.counts[ws] = n
	return nil
}

func (f *fakeCoordStore) CompareAndSetSlots(_ context.Context, ws string, expected, next int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.counts[ws] != expected {
		return false, nil
	}
	f.casCalls = append(f.casCalls, slotWrite{ws: ws, expected: expected, next: next})
	f.counts[ws] = next
	return true, nil
}

func (f *fakeCoordStore) ClearSlotReservations(_ context.Context, ws string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reservations[ws] = 0
	return nil
}

type recMetrics struct {
	Metrics
	mu      sync.Mutex
	errors  []string
	slotsAt map[string]int64
}

func (m *recMetrics) StateError(op string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors = append(m.errors, op)
}

func (m *recMetrics) SharedSlots(ws string, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.slotsAt == nil {
		m.slotsAt = map[string]int64{}
	}
	m.slotsAt[ws] = int64(n)
}

func newSlotTestScheduler(t *testing.T, store *fakeCoordStore, ws ...string) (*Scheduler, *recMetrics) {
	t.Helper()
	reg := registry.New(time.Now)
	m := &recMetrics{}
	return &Scheduler{
		cfg:     Config{Workspaces: ws, MaxInflightJobsPerWorkspace: 2},
		reg:     reg,
		shared:  store,
		metrics: m,
		log:     slog.New(slog.NewJSONHandler(io.Discard, nil)),
		now:     time.Now,
	}, m
}

// TestConsumeSlotsReportsFailureWithoutRetry pins that a failed consume is
// visible but never retried: a retry of a non-idempotent pop could remove a
// different pending claim's reservation. The failed level keeps its pending
// member (the expiry purge repairs it) and the other level still consumes.
func TestConsumeSlotsReportsFailureWithoutRetry(t *testing.T) {
	store := &fakeCoordStore{consumeFails: 1, reservations: map[string]int64{"ws-a": 2}}
	s, m := newSlotTestScheduler(t, store, "ws-a")
	s.consumeSlots(context.Background(), "ws-a")

	if store.consumeCalls != 2 {
		t.Fatalf("consume calls = %d, want 2 (one attempt per level, no retry)", store.consumeCalls)
	}
	if got := store.reservations["ws-a"]; got != 2 {
		t.Fatalf("ws-a reservations = %d, want 2 (a failed attempt pops nothing, a retry would pop another claim's slot)", got)
	}
	if len(m.errors) != 1 || m.errors[0] != "consume_slot" {
		t.Fatalf("state errors = %v, want one consume_slot", m.errors)
	}
}

// TestConsumeSlotsLostResponsePopsOnlyItsOwnReservation pins the hazard the
// retry used to open: when the pop landed but its response was lost, the
// single attempt removes exactly one member. A retry would pop the second —
// another pending claim's reservation — and leave the window counter below the
// true in-flight count.
func TestConsumeSlotsLostResponsePopsOnlyItsOwnReservation(t *testing.T) {
	store := &fakeCoordStore{
		consumeFailsAfterApply: 1,
		reservations:           map[string]int64{"ws-a": 2},
	}
	s, m := newSlotTestScheduler(t, store, "ws-a")
	s.consumeSlots(context.Background(), "ws-a")

	if store.consumeCalls != 2 {
		t.Fatalf("consume calls = %d, want 2 (a lost response is not retried)", store.consumeCalls)
	}
	if got := store.reservations["ws-a"]; got != 1 {
		t.Fatalf("ws-a reservations = %d, want 1 (only the applied pop; a retry would have popped the other claim's reservation)", got)
	}
	if len(m.errors) != 1 || m.errors[0] != "consume_slot" {
		t.Fatalf("state errors = %v, want one consume_slot", m.errors)
	}
}

// TestConsumeSlotsFailureOnBothLevelsNeverRetries pins the single-attempt
// bound per level even when both levels fail.
func TestConsumeSlotsFailureOnBothLevelsNeverRetries(t *testing.T) {
	store := &fakeCoordStore{consumeFails: 2}
	s, m := newSlotTestScheduler(t, store, "ws-a")
	s.consumeSlots(context.Background(), "ws-a")

	if store.consumeCalls != 2 {
		t.Fatalf("consume calls = %d, want 2 (one attempt per level)", store.consumeCalls)
	}
	if len(m.errors) != 2 {
		t.Fatalf("state errors = %v, want two consume_slot", m.errors)
	}
}

// TestReconcileSlotsRaisesToTheRowFloor pins the periodic repair of a lost
// decrement: a live row below the counter is a slot the row owns, so the
// periodic round raises the counter to the row count instead of only lowering.
func TestReconcileSlotsRaisesToTheRowFloor(t *testing.T) {
	store := &fakeCoordStore{counts: map[string]int64{"": 0, "ws-a": 0}}
	s, m := newSlotTestScheduler(t, store, "ws-a")
	if err := s.reg.Put(registry.TaskEntry{TaskID: "t-1", WorkspaceID: "ws-a", DaemonID: "d-1", State: registry.StateRunning}); err != nil {
		t.Fatalf("put: %v", err)
	}

	if err := s.ReconcileSlots(context.Background()); err != nil {
		t.Fatalf("ReconcileSlots: %v", err)
	}
	if len(store.casCalls) != 2 {
		t.Fatalf("cas calls = %+v, want the workspace and the global level", store.casCalls)
	}
	for _, c := range store.casCalls {
		if c.expected != 0 || c.next != 1 {
			t.Fatalf("cas %s: %d -> %d, want 0 -> 1 (the live row is the floor)", c.ws, c.expected, c.next)
		}
	}
	if m.slotsAt[""] != 1 || m.slotsAt["ws-a"] != 1 {
		t.Fatalf("shared slots gauge = %v, want both levels at 1", m.slotsAt)
	}

	// Idempotent: the next round finds count == rows + reservations.
	store.mu.Lock()
	store.casCalls = nil
	store.mu.Unlock()
	if err := s.ReconcileSlots(context.Background()); err != nil {
		t.Fatalf("second ReconcileSlots: %v", err)
	}
	if len(store.casCalls) != 0 {
		t.Fatalf("a converged round wrote again: %+v", store.casCalls)
	}
}

// TestReconcileSlotsStillLowersALeak pins the other periodic direction: a
// counter above the rows plus live reservations is lowered.
func TestReconcileSlotsStillLowersALeak(t *testing.T) {
	store := &fakeCoordStore{counts: map[string]int64{"": 5, "ws-a": 5}}
	s, _ := newSlotTestScheduler(t, store, "ws-a")
	if err := s.reg.Put(registry.TaskEntry{TaskID: "t-1", WorkspaceID: "ws-a", DaemonID: "d-1", State: registry.StateRunning}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.ReconcileSlots(context.Background()); err != nil {
		t.Fatalf("ReconcileSlots: %v", err)
	}
	for _, c := range store.casCalls {
		if c.next != 1 {
			t.Fatalf("cas %s -> %d, want 1", c.ws, c.next)
		}
	}
}

// TestReconcileSlotsEmptyWorkspaceCountsGloballyOnly pins the level split: a
// row with no workspace id raises the global counter but never becomes a
// workspace key that could overwrite it.
func TestReconcileSlotsEmptyWorkspaceCountsGloballyOnly(t *testing.T) {
	store := &fakeCoordStore{counts: map[string]int64{"": 0, "ws-a": 0}}
	s, _ := newSlotTestScheduler(t, store, "ws-a")
	if err := s.reg.Put(registry.TaskEntry{TaskID: "t-1", WorkspaceID: "", DaemonID: "d-1", State: registry.StateRunning}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.ReconcileSlots(context.Background()); err != nil {
		t.Fatalf("ReconcileSlots: %v", err)
	}
	if len(store.casCalls) != 1 || store.casCalls[0].ws != "" || store.casCalls[0].next != 1 {
		t.Fatalf("cas calls = %+v, want only the global level raised to 1", store.casCalls)
	}
}

// TestReconcileSlotsReportsSnapshotFailure pins fail-closed: a store read
// failure is a state error and no write.
func TestReconcileSlotsReportsSnapshotFailure(t *testing.T) {
	store := &fakeCoordStore{snapshotErr: errors.New("store down")}
	s, m := newSlotTestScheduler(t, store, "ws-a")
	err := s.ReconcileSlots(context.Background())
	if !errors.Is(err, ErrStoreUnavailable) {
		t.Fatalf("err = %v, want ErrStoreUnavailable", err)
	}
	if len(m.errors) != 1 || m.errors[0] != "read_slots" {
		t.Fatalf("state errors = %v, want read_slots", m.errors)
	}
	if len(store.casCalls) != 0 || len(store.setCalls) != 0 {
		t.Fatalf("wrote after a failed read: cas=%v set=%v", store.casCalls, store.setCalls)
	}
}
