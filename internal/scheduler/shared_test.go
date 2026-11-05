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

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/state"
)

// fakeCoordStore is the shared-store seam of these unit tests: only the slot
// primitives are real, and their failures are scriptable.
type fakeCoordStore struct {
	state.CoordStore

	mu     sync.Mutex
	counts map[string]int64
	// pending lists each level's reservation ids. A release removes the id it
	// names, like the store's targeted delete, so a test can tell whose marker
	// went away.
	pending map[string][]string
	// rows is the shared state's live-row set: the reconciliation's row face.
	rows        []registry.TaskEntry
	nextID      int
	snapshotErr error
	// snapshotHook runs once, after a snapshot read and before its caller sees
	// the value, so a test can land a claim in that window.
	snapshotHook func(ws string)
	resetCalls   []slotWrite
	casCalls     []slotWrite
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
	before   int64
	cleared  int64
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
	count, reservations := f.counts[ws], int64(len(f.pending[ws]))
	err := f.snapshotErr
	hook := f.snapshotHook
	f.snapshotHook = nil
	f.mu.Unlock()
	if err != nil {
		return 0, 0, err
	}
	// The hook runs after the read and before the caller sees it, the way a
	// claim reservation landing between the read and the write does.
	if hook != nil {
		hook(ws)
	}
	return count, reservations, nil
}

func (f *fakeCoordStore) ResetSlots(_ context.Context, ws string, n int64) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	before := f.counts[ws]
	cleared := int64(len(f.pending[ws]))
	f.resetCalls = append(f.resetCalls, slotWrite{ws: ws, before: before, next: n, cleared: cleared})
	delete(f.pending, ws)
	f.counts[ws] = n
	return before, cleared, nil
}

func (f *fakeCoordStore) CompareAndSetSlots(_ context.Context, ws string, expected, expectedReservations, next int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.counts[ws] != expected || int64(len(f.pending[ws])) != expectedReservations {
		return false, nil
	}
	f.casCalls = append(f.casCalls, slotWrite{ws: ws, expected: expected, next: next})
	f.counts[ws] = next
	return true, nil
}

// ListEntries is the reconciliation's row face: the store's rows, not the
// process-local index.
func (f *fakeCoordStore) ListEntries(context.Context) ([]registry.TaskEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]registry.TaskEntry(nil), f.rows...), nil
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
	store := &fakeCoordStore{
		counts: map[string]int64{"": 0, "ws-a": 0},
		rows:   []registry.TaskEntry{{TaskID: "t-1", WorkspaceID: "ws-a", DaemonID: "d-1", State: registry.StateRunning}},
	}
	s, m := newSlotTestScheduler(t, store, "ws-a")

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

// TestReconcileSlotsExactResetsEachLevelInOneStep pins the takeover write: the
// counter becomes the live-row count and that level's pending markers go away
// in the same store call, so a reservation can never land between a clear and
// a separate counter write. A reservation taken after the reset counts
// normally.
func TestReconcileSlotsExactResetsEachLevelInOneStep(t *testing.T) {
	store := &fakeCoordStore{
		counts:  map[string]int64{"": 2, "ws-a": 2},
		pending: map[string][]string{"": {"g-dead"}, "ws-a": {"w-dead"}},
		rows:    []registry.TaskEntry{{TaskID: "t-1", WorkspaceID: "ws-a", DaemonID: "d-1", State: registry.StateRunning}},
	}
	s, _ := newSlotTestScheduler(t, store, "ws-a")
	if err := s.reconcileSlots(context.Background(), true); err != nil {
		t.Fatalf("reconcileSlots(exact): %v", err)
	}
	if len(store.resetCalls) != 2 {
		t.Fatalf("reset calls = %+v, want the global and the workspace level", store.resetCalls)
	}
	for _, c := range store.resetCalls {
		if c.next != 1 {
			t.Fatalf("reset %q -> %d, want 1 (the live row)", c.ws, c.next)
		}
		if c.before != 2 || c.cleared != 1 {
			t.Fatalf("reset %q reported before=%d cleared=%d, want 2/1", c.ws, c.before, c.cleared)
		}
	}
	if len(store.casCalls) != 0 {
		t.Fatalf("the exact path used the periodic CAS: %+v", store.casCalls)
	}
	for ws, want := range map[string]int64{"": 1, "ws-a": 1} {
		if got := store.counts[ws]; got != want {
			t.Fatalf("counter %q = %d, want %d", ws, got, want)
		}
		if got := store.pendingLen(ws); got != 0 {
			t.Fatalf("level %q kept %d reservation markers, want 0", ws, got)
		}
	}

	// A reservation taken after the reset is counted, so the exact write never
	// leaves a level unable to accept claims.
	id, ok, err := store.ReserveSlot(context.Background(), "", 4)
	if err != nil || !ok {
		t.Fatalf("reserve after reset: ok=%v err=%v", ok, err)
	}
	if got := store.counts[""]; got != 2 {
		t.Fatalf("counter after a post-reset reservation = %d, want 2", got)
	}
	if !store.hasPending("", id) {
		t.Fatal("the post-reset reservation lost its marker")
	}
}

// TestReconcileSlotsStillLowersALeak pins the other periodic direction: a
// counter above the rows plus live reservations is lowered.
func TestReconcileSlotsStillLowersALeak(t *testing.T) {
	store := &fakeCoordStore{
		counts: map[string]int64{"": 5, "ws-a": 5},
		rows:   []registry.TaskEntry{{TaskID: "t-1", WorkspaceID: "ws-a", DaemonID: "d-1", State: registry.StateRunning}},
	}
	s, _ := newSlotTestScheduler(t, store, "ws-a")
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
	store := &fakeCoordStore{
		counts: map[string]int64{"": 0, "ws-a": 0},
		rows:   []registry.TaskEntry{{TaskID: "t-1", WorkspaceID: "", DaemonID: "d-1", State: registry.StateRunning}},
	}
	s, _ := newSlotTestScheduler(t, store, "ws-a")
	if err := s.ReconcileSlots(context.Background()); err != nil {
		t.Fatalf("ReconcileSlots: %v", err)
	}
	if len(store.casCalls) != 1 || store.casCalls[0].ws != "" || store.casCalls[0].next != 1 {
		t.Fatalf("cas calls = %+v, want only the global level raised to 1", store.casCalls)
	}
}

// TestReconcileSlotsCountsALiveReservationWithTheRows pins the counter's own
// invariant: a level is its live rows plus its live reservations. A
// reservation holds its slot before its row exists, so a level whose counter
// merely equals its rows is one slot short, and the reservation's release is
// what exposes that as a counter below the live rows.
func TestReconcileSlotsCountsALiveReservationWithTheRows(t *testing.T) {
	store := &fakeCoordStore{
		counts:  map[string]int64{"": 3, "ws-a": 2},
		pending: map[string][]string{"": {"g-live"}},
		rows: []registry.TaskEntry{
			{TaskID: "t-1", WorkspaceID: "ws-a", DaemonID: "d-1", State: registry.StateRunning},
			{TaskID: "t-2", WorkspaceID: "ws-a", DaemonID: "d-2", State: registry.StateRunning},
			{TaskID: "t-nos", DaemonID: "d-3", State: registry.StateDaemonRegistered},
		},
	}
	s, _ := newSlotTestScheduler(t, store, "ws-a")
	if err := s.ReconcileSlots(context.Background()); err != nil {
		t.Fatalf("ReconcileSlots: %v", err)
	}
	if got := store.counts[""]; got != 4 {
		t.Fatalf("global counter = %d, want 4 (three live rows plus the live reservation)", got)
	}
	if got := store.counts["ws-a"]; got != 2 {
		t.Fatalf("workspace counter = %d, want 2 (its rows already own its slots)", got)
	}
	// The reservation returns and the counter lands on the live rows exactly,
	// instead of one below them.
	if err := store.ReleaseSlot(context.Background(), "", "g-live"); err != nil {
		t.Fatalf("release the live reservation: %v", err)
	}
	if got := store.counts[""]; got != 3 {
		t.Fatalf("global counter after the reservation returned = %d, want 3 (the live rows)", got)
	}
	// Converged: the next round has nothing to write.
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

// TestReconcileSlotsDoesNotDoubleCountAConvertedReservation pins the claim's
// slot conversion: the row retires its reservation marker in the same store
// step, so a round whose snapshot predates that step must recompute instead of
// writing the rows plus a marker the row already paid for. Without the guard
// the level would settle one above its live rows and keep that extra slot out
// of the budget for good — the shape AC-100's shared form hit.
func TestReconcileSlotsDoesNotDoubleCountAConvertedReservation(t *testing.T) {
	store := &fakeCoordStore{
		counts:  map[string]int64{"ws-a": 1},
		pending: map[string][]string{"ws-a": {"w-claim"}},
	}
	s, _ := newSlotTestScheduler(t, store, "ws-a")
	// The conversion lands between the snapshot and the rows read: the row
	// becomes visible and the marker goes away, the counter untouched (the
	// reservation's +1 is the row's slot now).
	store.snapshotHook = func(ws string) {
		store.mu.Lock()
		defer store.mu.Unlock()
		store.rows = append(store.rows, registry.TaskEntry{
			TaskID: "t-1", WorkspaceID: "ws-a", DaemonID: "d-1", State: registry.StateRunning,
		})
		store.removeReservation(ws, "w-claim")
	}
	if err := s.repairLevel(context.Background(), "ws-a"); err != nil {
		t.Fatalf("repairLevel: %v", err)
	}
	if got := store.counts["ws-a"]; got != 1 {
		t.Fatalf("workspace counter = %d, want 1 (the row owns the slot the marker held)", got)
	}
	if len(store.casCalls) != 0 {
		t.Fatalf("the round wrote again after the conversion: %+v", store.casCalls)
	}
}

// TestReconcileSlotsRetriesWhenAClaimLandsInTheWindow pins the round's other
// half: a claim that lands between the read and the write declines the
// compare-and-set, and the round recomputes rather than leaving the level
// short until the next interval.
func TestReconcileSlotsRetriesWhenAClaimLandsInTheWindow(t *testing.T) {
	store := &fakeCoordStore{
		counts:  map[string]int64{"": 2, "ws-a": 2},
		pending: map[string][]string{},
		rows: []registry.TaskEntry{
			{TaskID: "t-1", WorkspaceID: "ws-a", DaemonID: "d-1", State: registry.StateRunning},
			{TaskID: "t-2", WorkspaceID: "ws-a", DaemonID: "d-2", State: registry.StateRunning},
			{TaskID: "t-nos", DaemonID: "d-3", State: registry.StateDaemonRegistered},
		},
	}
	s, _ := newSlotTestScheduler(t, store, "ws-a")
	// A claim reserves right after the first global read: the counter becomes
	// 3 with a live marker, so the write the round had computed (2 -> 3) must
	// not land.
	store.snapshotHook = func(ws string) {
		if ws != "" {
			return
		}
		store.mu.Lock()
		store.counts[""]++
		store.pending[""] = append(store.pending[""], "g-claim")
		store.mu.Unlock()
	}
	if err := s.ReconcileSlots(context.Background()); err != nil {
		t.Fatalf("ReconcileSlots: %v", err)
	}
	if got := store.counts[""]; got != 4 {
		t.Fatalf("global counter = %d, want 4 (three live rows plus the claim's reservation)", got)
	}
	if len(store.casCalls) != 1 || store.casCalls[0].ws != "" || store.casCalls[0].expected != 3 || store.casCalls[0].next != 4 {
		t.Fatalf("cas calls = %+v, want one global write 3 -> 4 after the declined attempt", store.casCalls)
	}
	if err := store.ReleaseSlot(context.Background(), "", "g-claim"); err != nil {
		t.Fatalf("release the claim's reservation: %v", err)
	}
	if got := store.counts[""]; got != 3 {
		t.Fatalf("global counter after the claim's reservation returned = %d, want 3 (the live rows)", got)
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
	if len(store.casCalls) != 0 || len(store.resetCalls) != 0 {
		t.Fatalf("wrote after a failed read: cas=%v reset=%v", store.casCalls, store.resetCalls)
	}
}

// TestContinueClaimRefusesANameThatDisagreesWithTheRow pins the continuation's
// name identity: the entry's job_name is the key every later round uses
// (missing-Job compensation, deletion, delivery), so a render that composes a
// different one — a row restored without the claim identity, the pre-upgrade
// shape — is refused before any object is placed and converges on the existing
// create-failure attribution (§4.3 未完成创建的接续, AC-75 至多一个活跃 Job).
// Placing it would leave a Job no row manages and fail the running task as
// job_missing once the recorded name's window passed.
func TestContinueClaimRefusesANameThatDisagreesWithTheRow(t *testing.T) {
	f := newFixture(t)
	f.builder.job = &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "fm-agent-task-1"}}
	if err := f.reg.Put(registry.TaskEntry{
		TaskID:      "task-1",
		WorkspaceID: "ws-1",
		JobName:     "fm-task-1",
		DaemonID:    "fm-task-1",
		State:       registry.StatePending,
		ClaimedAt:   testNow,
		Attempt:     1,
		Payload:     claimPayload("task-1"),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	err := f.sched.ContinueClaim(context.Background(), "task-1")
	if err == nil {
		t.Fatal("ContinueClaim placed a Job under a name the row does not hold")
	}
	if len(f.jobs.createdJobs) != 0 || len(f.jobs.createdSecrets) != 0 {
		t.Fatalf("objects created for the refused name: jobs=%v secrets=%v",
			f.jobs.createdJobs, f.jobs.createdSecrets)
	}
	e, ok := f.reg.Get("task-1")
	if !ok || !e.IsTerminal() || e.Result != registry.ResultFailed {
		t.Fatalf("entry = %+v (ok=%v), want the create-failure verdict", e, ok)
	}
	if e.FailureReason != FailureReasonJobCreateFailed {
		t.Fatalf("failure reason = %q, want %q", e.FailureReason, FailureReasonJobCreateFailed)
	}
	if e.JobName != "fm-task-1" {
		t.Fatalf("entry job name = %q, want the recorded name kept", e.JobName)
	}
}
