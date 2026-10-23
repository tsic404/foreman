package scheduler

import (
	"context"
	"errors"
	"fmt"
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

	mu     sync.Mutex
	counts map[string]int64
	// pending lists each level's reservation ids. A consume removes the id it
	// names, like the store's targeted delete, so a test can tell whose marker
	// went away.
	pending      map[string][]string
	nextID       int
	snapshotErr  error
	consumeFails int
	// consumeFailsAfterApply models a delete that landed while its response
	// was lost: the named member is gone and the caller saw an error.
	consumeFailsAfterApply int
	consumeCalls           int
	setCalls               []slotWrite
	casCalls               []slotWrite
	// workspaceReserveHook models the call that spends the last of a call
	// budget: it runs inside the workspace reservation and leaves the caller's
	// context dead, the way an exhausted store call does.
	workspaceReserveHook func()
	// releaseRejected records releases the store refused because the context
	// they were issued under was already dead (a real client reports the
	// context error instead of writing).
	releaseRejected []string
}

type slotWrite struct {
	ws       string
	expected int64
	next     int64
}

func (f *fakeCoordStore) ConsumeSlotReservation(_ context.Context, ws, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumeCalls++
	if f.consumeFailsAfterApply > 0 {
		f.consumeFailsAfterApply--
		f.removeReservation(ws, id)
		return errors.New("consume response lost")
	}
	if f.consumeFails > 0 {
		f.consumeFails--
		return errors.New("consume down")
	}
	f.removeReservation(ws, id)
	return nil
}

// ReserveSlot mints the reservation id the store keeps as the zset member and
// hands it back as the caller's retire handle. The workspace hook models the
// call that spends the last of a call budget: it runs inside the workspace
// reservation and leaves the caller's context dead.
func (f *fakeCoordStore) ReserveSlot(ctx context.Context, ws string, limit int) (string, bool, error) {
	if ws != "" && f.workspaceReserveHook != nil {
		hook := f.workspaceReserveHook
		f.workspaceReserveHook = nil
		hook()
		return "", false, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.counts[ws] >= int64(limit) {
		return "", false, nil
	}
	if f.pending == nil {
		f.pending = map[string][]string{}
	}
	f.counts[ws]++
	f.nextID++
	id := fmt.Sprintf("%s#%d", ws, f.nextID)
	f.pending[ws] = append(f.pending[ws], id)
	return id, true, nil
}

// ReleaseSlot returns the slot only when the named member is still pending,
// like the store's ZREM-guarded decrement: an id the lease expiry already
// purged must not lower the counter a second time. A dead context is recorded
// so a test can tell a refused compensation from an applied one.
func (f *fakeCoordStore) ReleaseSlot(ctx context.Context, ws, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		f.releaseRejected = append(f.releaseRejected, ws)
		return err
	}
	if !f.removeReservation(ws, id) {
		return nil
	}
	if f.counts[ws] > 0 {
		f.counts[ws]--
	}
	return nil
}

// removeReservation deletes one pending id and reports whether it was there;
// an id that is not pending is a no-op, like the store's ZREM of an already
// purged member.
func (f *fakeCoordStore) removeReservation(ws, id string) bool {
	ids := f.pending[ws]
	for i, member := range ids {
		if member == id {
			f.pending[ws] = append(ids[:i:i], ids[i+1:]...)
			return true
		}
	}
	return false
}

func (f *fakeCoordStore) hasPending(ws, id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, member := range f.pending[ws] {
		if member == id {
			return true
		}
	}
	return false
}

func (f *fakeCoordStore) pendingLen(ws string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pending[ws])
}

func (f *fakeCoordStore) SlotSnapshot(_ context.Context, ws string) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snapshotErr != nil {
		return 0, 0, f.snapshotErr
	}
	return f.counts[ws], int64(len(f.pending[ws])), nil
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
	delete(f.pending, ws)
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
		cfg:     Config{Workspaces: ws, MaxInflightJobs: 4, MaxInflightJobsPerWorkspace: 2},
		reg:     reg,
		shared:  store,
		metrics: m,
		log:     slog.New(slog.NewJSONHandler(io.Discard, nil)),
		now:     time.Now,
	}, m
}

// slotPair is the reservation handle one claim call takes: the workspace
// level and the global level.
func slotPair(ws, globalID, levelID string) SlotReservation {
	return SlotReservation{workspaceID: ws, globalID: globalID, levelID: levelID}
}

// TestConsumeSlotsRetiresOnlyTheHandlesOwnMarkers pins that the pair handed in
// is the pair that goes away: another claim's pending marker at the same levels
// stays untouched, and the failed response is not retried.
func TestConsumeSlotsRetiresOnlyTheHandlesOwnMarkers(t *testing.T) {
	store := &fakeCoordStore{
		consumeFailsAfterApply: 1,
		pending: map[string][]string{
			"ws-a": {"w-mine", "w-other"},
			"":     {"g-mine", "g-other"},
		},
	}
	s, m := newSlotTestScheduler(t, store, "ws-a")
	s.consumeSlots(context.Background(), slotPair("ws-a", "g-mine", "w-mine"))

	if store.consumeCalls != 2 {
		t.Fatalf("consume calls = %d, want 2 (one attempt per level, no retry)", store.consumeCalls)
	}
	for _, tc := range []struct{ ws, gone, kept string }{
		{"ws-a", "w-mine", "w-other"},
		{"", "g-mine", "g-other"},
	} {
		if store.hasPending(tc.ws, tc.gone) {
			t.Fatalf("level %q still holds the retired marker %q", tc.ws, tc.gone)
		}
		if !store.hasPending(tc.ws, tc.kept) {
			t.Fatalf("level %q lost the other claim's marker %q", tc.ws, tc.kept)
		}
	}
	if len(m.errors) != 1 || m.errors[0] != "consume_slot" {
		t.Fatalf("state errors = %v, want one consume_slot", m.errors)
	}
}

// TestConsumeSlotsReportsFailureWithoutRetry pins that a failed consume is
// visible but attempted once: the marker keeps its lease, and the expiry purge
// plus the periodic reconciliation repair the counter.
func TestConsumeSlotsReportsFailureWithoutRetry(t *testing.T) {
	store := &fakeCoordStore{
		consumeFails: 1,
		pending:      map[string][]string{"ws-a": {"w-mine"}},
	}
	s, m := newSlotTestScheduler(t, store, "ws-a")
	s.consumeSlots(context.Background(), slotPair("ws-a", "g-mine", "w-mine"))

	if store.consumeCalls != 2 {
		t.Fatalf("consume calls = %d, want 2 (one attempt per level, no retry)", store.consumeCalls)
	}
	if !store.hasPending("ws-a", "w-mine") {
		t.Fatal("the failed level dropped its marker")
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
	s.consumeSlots(context.Background(), slotPair("ws-a", "g-mine", "w-mine"))

	if store.consumeCalls != 2 {
		t.Fatalf("consume calls = %d, want 2 (one attempt per level)", store.consumeCalls)
	}
	if len(m.errors) != 2 {
		t.Fatalf("state errors = %v, want two consume_slot", m.errors)
	}
}

// TestReserveSlotsCompensationSurvivesAnExhaustedCall pins that the global slot
// a refused workspace reservation already took is written back even when the
// caller's budget is spent: a compensation issued under the dying context is a
// guaranteed failure and leaks the reservation until the TTL/reconcile edge.
func TestReserveSlotsCompensationSurvivesAnExhaustedCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &fakeCoordStore{
		counts:               map[string]int64{"": 0},
		workspaceReserveHook: cancel,
	}
	s, _ := newSlotTestScheduler(t, store, "ws-a")

	if _, err := s.reserveSlots(ctx, "ws-a"); !errors.Is(err, ErrStoreUnavailable) {
		t.Fatalf("reserveSlots = %v, want ErrStoreUnavailable (the workspace half failed)", err)
	}
	if len(store.releaseRejected) != 0 {
		t.Fatalf("compensation refused for a dead context: %v", store.releaseRejected)
	}
	if got := store.counts[""]; got != 0 {
		t.Fatalf("global counter = %d, want 0 (the taken slot was returned)", got)
	}
	if got := store.pendingLen(""); got != 0 {
		t.Fatalf("global reservations = %d, want 0", got)
	}
}

// TestReserveSlotsRefusedWorkspaceReturnsTheGlobalSlot pins the other refusal:
// a workspace level at its cap returns the global slot it already took.
func TestReserveSlotsRefusedWorkspaceReturnsTheGlobalSlot(t *testing.T) {
	store := &fakeCoordStore{
		counts: map[string]int64{"": 0, "ws-a": 2},
	}
	s, _ := newSlotTestScheduler(t, store, "ws-a")

	if _, err := s.reserveSlots(context.Background(), "ws-a"); !errors.Is(err, ErrNoSlot) {
		t.Fatalf("reserveSlots = %v, want ErrNoSlot", err)
	}
	if got := store.counts[""]; got != 0 {
		t.Fatalf("global counter = %d, want 0 (the taken slot was returned)", got)
	}
	if got := store.pendingLen(""); got != 0 {
		t.Fatalf("global reservations = %d, want 0 (the compensation retired its own id)", got)
	}
	if got := store.counts["ws-a"]; got != 2 {
		t.Fatalf("workspace counter = %d, want 2 (no workspace reservation was taken)", got)
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
