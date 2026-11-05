package state

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/tsic404/foreman/internal/registry"
)

func coordTestStore(t *testing.T) (Store, CoordStore, *time.Time, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	now := time.Now()
	clock := func() time.Time { return now }
	s, err := New(Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    48 * time.Hour,
	}, WithClock(clock))
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	return s, s.(CoordStore), &now, mr
}

func coordTestEntry(taskID, payload string) registry.TaskEntry {
	return registry.TaskEntry{
		TaskID:       taskID,
		AgentID:      "agent-1",
		WorkspaceID:  "ws-1",
		JobName:      "fm-" + taskID,
		DaemonID:     "fm-" + taskID,
		JobRuntimeID: "rt-" + taskID,
		State:        registry.StateDaemonRegistered,
		ClaimedAt:    time.Unix(1_700_000_000, 0),
		Payload:      []byte(payload),
	}
}

func TestCoordEnqueueIsSingleAndKeepsThePayload(t *testing.T) {
	ctx := context.Background()
	_, store, _, _ := coordTestStore(t)
	first := coordTestEntry("task-1", `{"body":"first"}`)
	created, err := store.EnqueueEntry(ctx, "ws-1", "task-1", first)
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}
	// A repeated enqueue is the idempotent hit and never overwrites the body.
	created, err = store.EnqueueEntry(ctx, "ws-1", "task-1", coordTestEntry("task-1", `{"body":"second"}`))
	if err != nil || created {
		t.Fatalf("second enqueue: created=%v err=%v", created, err)
	}
	got, ok, err := store.GetEntry(ctx, "ws-1", "task-1")
	if err != nil || !ok {
		t.Fatalf("GetEntry: ok=%v err=%v", ok, err)
	}
	if string(got.Payload) != `{"body":"first"}` {
		t.Fatalf("payload = %s, want the first body", got.Payload)
	}
	if depth, err := store.QueueDepth(ctx); err != nil || depth != 1 {
		t.Fatalf("queue depth = %d (err %v), want 1", depth, err)
	}
}

func TestCoordPickupIsSingleFlightAndRearms(t *testing.T) {
	ctx := context.Background()
	_, store, now, mr := coordTestStore(t)
	if _, err := store.EnqueueEntry(ctx, "ws-1", "task-1", coordTestEntry("task-1", `{"body":"x"}`)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	window := 100 * time.Millisecond
	var wg sync.WaitGroup
	results := make([]Pickup, 2)
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = store.PickupDelivery(ctx, "ws-1", "task-1", window)
		}(i)
	}
	wg.Wait()
	delivered := 0
	for i, res := range results {
		if errs[i] != nil {
			t.Fatalf("pickup %d: %v", i, errs[i])
		}
		if res.Delivered {
			delivered++
		}
	}
	if delivered != 1 {
		t.Fatalf("pickups delivered %d times, want exactly one", delivered)
	}
	entry, ok, err := store.GetEntry(ctx, "ws-1", "task-1")
	if err != nil || !ok || entry.Deliveries != 1 {
		t.Fatalf("row after pickup = %+v (ok=%v err=%v), want one commit", entry, ok, err)
	}
	// Inside the window: empty, counted as the lost-response claim.
	held, err := store.PickupDelivery(ctx, "ws-1", "task-1", window)
	if err != nil {
		t.Fatalf("in-window pickup: %v", err)
	}
	if held.Delivered || !held.Held {
		t.Fatalf("in-window pickup = %+v, want held and not delivered", held)
	}
	// Past the window: the row returns to the takeable state. The window is a
	// lease on the store's clock, so the probe advances that clock.
	mr.SetTime(time.Now().Add(window + time.Second))
	again, err := store.PickupDelivery(ctx, "ws-1", "task-1", window)
	if err != nil {
		t.Fatalf("post-window pickup: %v", err)
	}
	if !again.Delivered || again.Entry.Deliveries != 2 {
		t.Fatalf("post-window pickup = %+v, want the second delivery", again)
	}
	// The start fact settles the row and moves it out of the queue.
	entry, first, err := store.ConfirmStart(ctx, "ws-1", "task-1", now.Add(time.Second), true)
	if err != nil || !first || entry.State != registry.StateRunning {
		t.Fatalf("ConfirmStart: first=%v state=%s err=%v", first, entry.State, err)
	}
	if depth, err := store.QueueDepth(ctx); err != nil || depth != 0 {
		t.Fatalf("queue depth after the confirmation = %d (err %v), want 0", depth, err)
	}
	if _, first, err := store.ConfirmStart(ctx, "ws-1", "task-1", now.Add(2*time.Second), true); err != nil || first {
		t.Fatalf("second start fact: first=%v err=%v, want false", first, err)
	}
	late, err := store.PickupDelivery(ctx, "ws-1", "task-1", window)
	if err != nil || late.Delivered {
		t.Fatalf("post-start pickup delivered=%v err=%v, want none", late.Delivered, err)
	}
}

func TestCoordTerminalIsWrittenOnce(t *testing.T) {
	ctx := context.Background()
	full, store, now, _ := coordTestStore(t)
	if _, err := store.EnqueueEntry(ctx, "ws-1", "task-1", coordTestEntry("task-1", `{"body":"x"}`)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	entry, first, err := store.CommitTerminal(ctx, "ws-1", "task-1", registry.ResultFailed, "job_failed", *now, now.Add(time.Minute))
	if err != nil || !first || entry.State != registry.StateTerminal {
		t.Fatalf("terminal: first=%v state=%s err=%v", first, entry.State, err)
	}
	if _, first, err := store.CommitTerminal(ctx, "ws-1", "task-1", registry.ResultCompleted, "", *now, *now); err != nil || first {
		t.Fatalf("second terminal: first=%v err=%v, want false", first, err)
	}
	if depth, err := store.QueueDepth(ctx); err != nil || depth != 0 {
		t.Fatalf("queue depth after terminal = %d (err %v), want 0", depth, err)
	}
	settled, ok, err := full.GetSettled(ctx, "ws-1", "task-1")
	if err != nil || !ok || settled.Result != registry.ResultFailed {
		t.Fatalf("settled record = %+v (ok=%v err=%v), want the first verdict", settled, ok, err)
	}
}

func TestCoordMergeCannotRevertAGateField(t *testing.T) {
	ctx := context.Background()
	_, store, now, _ := coordTestStore(t)
	if _, err := store.EnqueueEntry(ctx, "ws-1", "task-1", coordTestEntry("task-1", `{"body":"x"}`)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, first, err := store.CommitTerminal(ctx, "ws-1", "task-1", registry.ResultCompleted, "", *now, *now); err != nil || !first {
		t.Fatalf("terminal: first=%v err=%v", first, err)
	}
	status := "running"
	if err := store.MergeEntry(ctx, "ws-1", "task-1", registry.EntryPatch{LastStatusSeen: &status}); err != nil {
		t.Fatalf("merge: %v", err)
	}
	entry, ok, err := store.GetEntry(ctx, "ws-1", "task-1")
	if err != nil || !ok {
		t.Fatalf("GetEntry: ok=%v err=%v", ok, err)
	}
	if entry.State != registry.StateTerminal || entry.Result != registry.ResultCompleted {
		t.Fatalf("row = %+v, want the terminal verdict intact", entry)
	}
	if entry.LastStatusSeen != "running" {
		t.Fatalf("last_status_seen = %q, want the merged value", entry.LastStatusSeen)
	}
}

// TestCoordSlotRetireOnlyTouchesItsOwnReservation pins the reservation id as an
// identity: the claim's row write and a release delete the marker they name,
// never the oldest pending one that another claim still owns.
func TestCoordSlotRetireOnlyTouchesItsOwnReservation(t *testing.T) {
	ctx := context.Background()
	_, store, _, mr := coordTestStore(t)
	other, ok, err := store.ReserveSlot(ctx, "", 10)
	if err != nil || !ok {
		t.Fatalf("reserve other: ok=%v err=%v", ok, err)
	}
	mine, ok, err := store.ReserveSlot(ctx, "", 10)
	if err != nil || !ok {
		t.Fatalf("reserve mine: ok=%v err=%v", ok, err)
	}
	if mine == other {
		t.Fatalf("two reservations share the id %q", mine)
	}

	// The claim's row retires only the caller's marker; the counter keeps
	// counting the row that now owns the slot.
	row := coordTestEntry("t-1", `{"id":"t-1"}`)
	row.JobName = "fm-t-1"
	if err := store.SetEntryRetiringSlots(ctx, "ws-1", "t-1", row, []registry.SlotRetirement{{ID: mine}}); err != nil {
		t.Fatalf("retire mine: %v", err)
	}
	assertReservations(t, mr, keySlotResAll, other)
	if n, live, err := store.SlotSnapshot(ctx, ""); err != nil || n != 2 || live != 1 {
		t.Fatalf("after the conversion: counter=%d live=%d err=%v, want (2,1)", n, live, err)
	}

	// Retiring a marker that is already gone leaves the counter alone: a
	// repeated release must not decrement a live row's slot.
	if err := store.ReleaseSlot(ctx, "", mine); err != nil {
		t.Fatalf("release retired marker: %v", err)
	}
	if n, live, err := store.SlotSnapshot(ctx, ""); err != nil || n != 2 || live != 1 {
		t.Fatalf("after repeated release: counter=%d live=%d err=%v, want (2,1)", n, live, err)
	}

	if err := store.ReleaseSlot(ctx, "", other); err != nil {
		t.Fatalf("release other: %v", err)
	}
	assertReservations(t, mr, keySlotResAll)
	if n, live, err := store.SlotSnapshot(ctx, ""); err != nil || n != 1 || live != 0 {
		t.Fatalf("after release: counter=%d live=%d err=%v, want (1,0)", n, live, err)
	}

	// The lease purge already returned a stale reservation's slot, so retiring
	// its id afterwards must not decrement the counter a second time.
	stale, ok, err := store.ReserveSlot(ctx, "", 10)
	if err != nil || !ok {
		t.Fatalf("reserve stale: ok=%v err=%v", ok, err)
	}
	mr.SetTime(time.Now().Add(reservationTTL + time.Minute))
	if _, live, err := store.SlotSnapshot(ctx, ""); err != nil || live != 0 {
		t.Fatalf("live reservations after expiry = %d (err %v), want 0", live, err)
	}
	staleRow := coordTestEntry("t-2", `{"id":"t-2"}`)
	staleRow.JobName = "fm-t-2"
	if err := store.SetEntryRetiringSlots(ctx, "ws-1", "t-2", staleRow, []registry.SlotRetirement{{ID: stale}}); err != nil {
		t.Fatalf("retire stale: %v", err)
	}
	if err := store.ReleaseSlot(ctx, "", stale); err != nil {
		t.Fatalf("release stale: %v", err)
	}
	if n, err := store.Slots(ctx, ""); err != nil || n != 1 {
		t.Fatalf("counter after retiring the purged marker = %d (err %v), want 1", n, err)
	}
}

func TestCoordSlotReserveNonPositiveLimitIsUncapped(t *testing.T) {
	ctx := context.Background()
	_, store, _, _ := coordTestStore(t)
	// A non-positive limit is the "no cap" sentinel the interface documents,
	// not a zero budget: every reservation at such a level must be admitted.
	for i := range 3 {
		if _, ok, err := store.ReserveSlot(ctx, "ws-1", 0); err != nil || !ok {
			t.Fatalf("workspace reserve %d at limit 0: ok=%v err=%v, want true", i+1, ok, err)
		}
	}
	if n, err := store.Slots(ctx, "ws-1"); err != nil || n != 3 {
		t.Fatalf("workspace slots = %d (err %v), want 3", n, err)
	}
	if _, ok, err := store.ReserveSlot(ctx, "", -1); err != nil || !ok {
		t.Fatalf("global reserve at limit -1: ok=%v err=%v, want true", ok, err)
	}
	if n, err := store.Slots(ctx, ""); err != nil || n != 1 {
		t.Fatalf("global slots = %d (err %v), want 1", n, err)
	}
}

// assertReservations checks one level's pending reservation ids as a set.
func assertReservations(t *testing.T, mr *miniredis.Miniredis, key string, want ...string) {
	t.Helper()
	if !mr.Exists(key) {
		// An empty ZSET loses its key, which is exactly "no pending marker".
		if len(want) > 0 {
			t.Fatalf("reservations at %s are gone, want %v", key, want)
		}
		return
	}
	got, err := mr.ZMembers(key)
	if err != nil {
		t.Fatalf("zmembers %s: %v", key, err)
	}
	if len(got) != len(want) {
		t.Fatalf("reservations = %v, want %v", got, want)
	}
	for _, id := range want {
		found := false
		for _, member := range got {
			if member == id {
				found = true
			}
		}
		if !found {
			t.Fatalf("reservations = %v, want %v", got, want)
		}
	}
}

func TestCoordSlotsAndLocks(t *testing.T) {
	ctx := context.Background()
	_, store, _, mr := coordTestStore(t)
	first, ok, err := store.ReserveSlot(ctx, "", 2)
	if err != nil || !ok {
		t.Fatalf("reserve 1: ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.ReserveSlot(ctx, "", 2); err != nil || !ok {
		t.Fatalf("reserve 2: ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.ReserveSlot(ctx, "", 2); err != nil || ok {
		t.Fatalf("reserve past the cap: ok=%v err=%v, want false", ok, err)
	}
	if err := store.ReleaseSlot(ctx, "", first); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, ok, err := store.ReserveSlot(ctx, "", 2); err != nil || !ok {
		t.Fatalf("reserve after release: ok=%v err=%v", ok, err)
	}
	if n, err := store.Slots(ctx, ""); err != nil || n != 2 {
		t.Fatalf("slots = %d (err %v), want 2", n, err)
	}
	if _, _, err := store.ResetSlots(ctx, "", 0); err != nil {
		t.Fatalf("reset slots: %v", err)
	}

	// The task lock is owner-checked and lease-bounded.
	if ok, err := store.LockTask(ctx, "ws-1", "task-1", "owner-a", time.Second); err != nil || !ok {
		t.Fatalf("lock: ok=%v err=%v", ok, err)
	}
	if ok, err := store.LockTask(ctx, "ws-1", "task-1", "owner-b", time.Second); err != nil || ok {
		t.Fatalf("contended lock: ok=%v err=%v, want false", ok, err)
	}
	if err := store.UnlockTask(ctx, "ws-1", "task-1", "owner-b"); err != nil {
		t.Fatalf("foreign unlock: %v", err)
	}
	if ok, err := store.LockTask(ctx, "ws-1", "task-1", "owner-b", time.Second); err != nil || ok {
		t.Fatalf("lock after a foreign unlock attempt: ok=%v err=%v, want false", ok, err)
	}
	// The holder's TTL lapsed on the store's clock: the lock is takeable
	// without a release.
	mr.FastForward(2 * time.Second)
	if ok, err := store.LockTask(ctx, "ws-1", "task-1", "owner-b", time.Second); err != nil || !ok {
		t.Fatalf("expired lock: ok=%v err=%v, want true", ok, err)
	}
	if err := store.UnlockTask(ctx, "ws-1", "task-1", "owner-b"); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestCoordLeaderLeaseExpires(t *testing.T) {
	ctx := context.Background()
	_, store, _, mr := coordTestStore(t)
	if ok, err := store.AcquireLeadership(ctx, "a", time.Second); err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	if ok, err := store.AcquireLeadership(ctx, "b", time.Second); err != nil || ok {
		t.Fatalf("contended acquire: ok=%v err=%v, want false", ok, err)
	}
	if holder, err := store.LeadershipHolder(ctx); err != nil || holder != "a" {
		t.Fatalf("holder = %q (err %v), want a", holder, err)
	}
	if ok, err := store.RenewLeadership(ctx, "b", time.Second); err != nil || ok {
		t.Fatalf("foreign renew: ok=%v err=%v, want false", ok, err)
	}
	mr.FastForward(2 * time.Second)
	if ok, err := store.AcquireLeadership(ctx, "b", time.Second); err != nil || !ok {
		t.Fatalf("acquire after expiry: ok=%v err=%v, want true", ok, err)
	}
	if ok, err := store.RenewLeadership(ctx, "b", time.Second); err != nil || !ok {
		t.Fatalf("renew: ok=%v err=%v, want true", ok, err)
	}
}

func TestCoordSessionIndex(t *testing.T) {
	ctx := context.Background()
	_, store, _, _ := coordTestStore(t)
	raw := []byte(`{"node":"node-a","learned_at":"2026-10-21T00:00:00Z"}`)
	if err := store.PutSessionNode(ctx, "ws-1", "sess-1", raw, time.Hour); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, ok, err := store.GetSessionNode(ctx, "ws-1", "sess-1")
	if err != nil || !ok || string(got.Raw) != string(raw) {
		t.Fatalf("get = %s (ok=%v err=%v)", got.Raw, ok, err)
	}
	if got.Remaining <= 0 || got.Remaining > time.Hour {
		t.Fatalf("remaining = %v, want (0, 1h]", got.Remaining)
	}
	if _, ok, err := store.GetSessionNode(ctx, "ws-2", "sess-1"); err != nil || ok {
		t.Fatalf("cross-workspace get: ok=%v err=%v, want a miss", ok, err)
	}
}

// TestCoordSlotSnapshotCAS pins the reconciliation's atomic read + conditional
// write: a reservation taken between the snapshot and the write, or a
// conversion that retired a marker after it, must make the CAS decline, and
// an unchanged pair must be writable.
func TestCoordSlotSnapshotCAS(t *testing.T) {
	ctx := context.Background()
	_, store, _, mr := coordTestStore(t)
	if _, ok, err := store.ReserveSlot(ctx, "", 10); err != nil || !ok {
		t.Fatalf("reserve: ok=%v err=%v", ok, err)
	}
	count, reservations, err := store.SlotSnapshot(ctx, "")
	if err != nil || count != 1 || reservations != 1 {
		t.Fatalf("snapshot = (%d,%d) err=%v, want (1,1)", count, reservations, err)
	}

	// A claim reserves after the snapshot: the CAS must decline.
	if _, ok, err := store.ReserveSlot(ctx, "", 10); err != nil || !ok {
		t.Fatalf("second reserve: ok=%v err=%v", ok, err)
	}
	swapped, err := store.CompareAndSetSlots(ctx, "", count, reservations, 0)
	if err != nil {
		t.Fatalf("cas: %v", err)
	}
	if swapped {
		t.Fatal("CAS succeeded after a concurrent reservation, losing its +1")
	}
	if got, _ := store.Slots(ctx, ""); got != 2 {
		t.Fatalf("counter after the declined CAS = %d, want 2", got)
	}

	// No concurrent change: the CAS lands.
	count, reservations, err = store.SlotSnapshot(ctx, "")
	if err != nil || count != 2 || reservations != 2 {
		t.Fatalf("fresh snapshot = (%d,%d) err=%v, want (2,2)", count, reservations, err)
	}
	swapped, err = store.CompareAndSetSlots(ctx, "", count, reservations, count)
	if err != nil || !swapped {
		t.Fatalf("cas unchanged: swapped=%v err=%v", swapped, err)
	}
	if got, _ := store.Slots(ctx, ""); got != 2 {
		t.Fatalf("counter after the CAS = %d, want 2", got)
	}

	// Expiry is part of the snapshot: past the reservation TTL (store clock)
	// the slot is returned and the reservation count drops to zero. The score
	// is written with the server's TIME, so the server clock is what moves.
	mr.SetTime(time.Now().Add(reservationTTL + time.Minute))
	count, reservations, err = store.SlotSnapshot(ctx, "")
	if err != nil || count != 0 || reservations != 0 {
		t.Fatalf("snapshot after expiry = (%d,%d) err=%v, want (0,0)", count, reservations, err)
	}
}

// TestCoordSlotCASGuardsTheMarkerCount pins the conversion guard: a claim's row
// retires its reservation marker without moving the counter, so a round whose
// snapshot predates that step still sees the marker. The guard must decline the
// write the stale snapshot produced — the row now owns the slot the marker
// counted, and writing the doubled value would keep one slot out of the budget
// for good.
func TestCoordSlotCASGuardsTheMarkerCount(t *testing.T) {
	ctx := context.Background()
	_, store, _, _ := coordTestStore(t)
	id, ok, err := store.ReserveSlot(ctx, "", 10)
	if err != nil || !ok {
		t.Fatalf("reserve: ok=%v err=%v", ok, err)
	}
	count, reservations, err := store.SlotSnapshot(ctx, "")
	if err != nil || count != 1 || reservations != 1 {
		t.Fatalf("snapshot = (%d,%d) err=%v, want (1,1)", count, reservations, err)
	}
	// The claim's row lands with the marker retired; the counter is untouched
	// because the reservation's +1 is the row's slot now.
	row := coordTestEntry("t-1", `{"id":"t-1"}`)
	row.JobName = "fm-t-1"
	if err := store.SetEntryRetiringSlots(ctx, "ws-1", "t-1", row, []registry.SlotRetirement{{ID: id}}); err != nil {
		t.Fatalf("retire: %v", err)
	}
	swapped, err := store.CompareAndSetSlots(ctx, "", count, reservations, 0)
	if err != nil {
		t.Fatalf("cas: %v", err)
	}
	if swapped {
		t.Fatal("CAS succeeded after the marker retired, writing a counter below the live row")
	}
	if got, _ := store.Slots(ctx, ""); got != 1 {
		t.Fatalf("counter after the declined CAS = %d, want 1", got)
	}
	// The recomputed round sees the conversion and finds the level exact.
	count, reservations, err = store.SlotSnapshot(ctx, "")
	if err != nil || count != 1 || reservations != 0 {
		t.Fatalf("fresh snapshot = (%d,%d) err=%v, want (1,0)", count, reservations, err)
	}
	if swapped, err := store.CompareAndSetSlots(ctx, "", count, reservations, count); err != nil || !swapped {
		t.Fatalf("cas on the fresh pair: swapped=%v err=%v", swapped, err)
	}
}

// TestCoordSetEntryRetiringSlotsConvertsInOneStep pins the claim's conversion:
// the row (with its Job index) and the retirement of its reservation markers
// are one store step, so a reader of the counter, the markers and the rows can
// never find the same in-flight slot on both sides of the handoff. Retiring a
// marker also survives the reservation TTL: the row owns the slot, so the
// expiry purge has nothing left to return.
func TestCoordSetEntryRetiringSlotsConvertsInOneStep(t *testing.T) {
	ctx := context.Background()
	_, store, _, mr := coordTestStore(t)
	globalID, ok, err := store.ReserveSlot(ctx, "", 4)
	if err != nil || !ok {
		t.Fatalf("global reserve: ok=%v err=%v", ok, err)
	}
	levelID, ok, err := store.ReserveSlot(ctx, "ws-1", 4)
	if err != nil || !ok {
		t.Fatalf("workspace reserve: ok=%v err=%v", ok, err)
	}
	e := coordTestEntry("t-1", `{"id":"t-1"}`)
	e.JobName = "fm-t-1"
	if err := store.SetEntryRetiringSlots(ctx, "ws-1", "t-1", e, []registry.SlotRetirement{
		{WorkspaceID: "ws-1", ID: levelID},
		{ID: globalID},
	}); err != nil {
		t.Fatalf("SetEntryRetiringSlots: %v", err)
	}
	for _, ws := range []string{"", "ws-1"} {
		count, reservations, err := store.SlotSnapshot(ctx, ws)
		if err != nil || count != 1 || reservations != 0 {
			t.Fatalf("level %q snapshot = (%d,%d) err=%v, want (1,0)", ws, count, reservations, err)
		}
	}
	if got, ok, err := store.GetEntry(ctx, "ws-1", "t-1"); err != nil || !ok || got.TaskID != "t-1" {
		t.Fatalf("row = %+v (ok=%v err=%v), want t-1", got, ok, err)
	}
	if byJob, ok, err := store.GetEntryByJob(ctx, "fm-t-1"); err != nil || !ok || byJob.TaskID != "t-1" {
		t.Fatalf("row by Job = %+v (ok=%v err=%v), want t-1", byJob, ok, err)
	}
	// The retired markers cannot come back through the lease purge.
	mr.SetTime(time.Now().Add(reservationTTL + time.Minute))
	if count, reservations, err := store.SlotSnapshot(ctx, ""); err != nil || count != 1 || reservations != 0 {
		t.Fatalf("snapshot after the TTL = (%d,%d) err=%v, want (1,0)", count, reservations, err)
	}
}

// TestCoordResetSlotsDropsMarkersAndWritesInOneStep pins the takeover write:
// the reset drops every pending marker of the level while writing the counter,
// returns what it found, and leaves the level ready to take new reservations.
// A marker that survived the reset with its +1 overwritten would later be
// purged as a second decrement of a slot it never owned.
func TestCoordResetSlotsDropsMarkersAndWritesInOneStep(t *testing.T) {
	ctx := context.Background()
	_, store, _, mr := coordTestStore(t)
	if _, ok, err := store.ReserveSlot(ctx, "", 10); err != nil || !ok {
		t.Fatalf("global reserve: ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.ReserveSlot(ctx, "ws-1", 10); err != nil || !ok {
		t.Fatalf("workspace reserve: ok=%v err=%v", ok, err)
	}

	before, cleared, err := store.ResetSlots(ctx, "", 3)
	if err != nil || before != 1 || cleared != 1 {
		t.Fatalf("global reset = (%d,%d) err=%v, want (1,1)", before, cleared, err)
	}
	if got, err := store.Slots(ctx, ""); err != nil || got != 3 {
		t.Fatalf("global counter = %d (err %v), want 3", got, err)
	}
	assertReservations(t, mr, keySlotResAll)
	if _, live, err := store.SlotSnapshot(ctx, ""); err != nil || live != 0 {
		t.Fatalf("global live reservations after reset = %d (err %v), want 0", live, err)
	}

	// A reset of one level never touches another level's markers or counter.
	before, cleared, err = store.ResetSlots(ctx, "ws-1", 0)
	if err != nil || before != 1 || cleared != 1 {
		t.Fatalf("workspace reset = (%d,%d) err=%v, want (1,1)", before, cleared, err)
	}
	if got, err := store.Slots(ctx, "ws-1"); err != nil || got != 0 {
		t.Fatalf("workspace counter = %d (err %v), want 0", got, err)
	}
	assertReservations(t, mr, keySlotResWS+"ws-1")

	// The level accepts new reservations after the reset and counts each one.
	if _, ok, err := store.ReserveSlot(ctx, "", 10); err != nil || !ok {
		t.Fatalf("reserve after reset: ok=%v err=%v", ok, err)
	}
	if got, err := store.Slots(ctx, ""); err != nil || got != 4 {
		t.Fatalf("global counter after a post-reset reserve = %d (err %v), want 4", got, err)
	}
}

// TestCoordResetSlotsNeverLosesAConcurrentReservation pins the atomicity the
// takeover relies on: a reservation taken while the level is being reset
// either lands after the reset (counter and marker both present) or is dropped
// by it (both gone). A marker can never survive with its +1 overwritten,
// because the next purge would then decrement that slot a second time.
func TestCoordResetSlotsNeverLosesAConcurrentReservation(t *testing.T) {
	ctx := context.Background()
	_, store, _, _ := coordTestStore(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 500 {
			if _, _, err := store.ReserveSlot(ctx, "", 1<<30); err != nil {
				return
			}
		}
	}()
	for range 50 {
		if _, _, err := store.ResetSlots(ctx, "", 0); err != nil {
			t.Fatalf("reset: %v", err)
		}
	}
	<-done
	count, live, err := store.SlotSnapshot(ctx, "")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if count != live {
		t.Fatalf("counter = %d with %d live markers, want the two equal", count, live)
	}
}

// coordStoreAt builds one store over an existing miniredis with the given
// clock, so one test can stand two instances with different clocks on the same
// shared store.
func coordStoreAt(t *testing.T, mr *miniredis.Miniredis, now func() time.Time) CoordStore {
	t.Helper()
	s, err := New(Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    48 * time.Hour,
	}, WithClock(now))
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	return s.(CoordStore)
}

// TestCoordLeaseIgnoresClientClockSkew pins the expiry authority: a live lock
// and a live lease are held by the store-side TTL, so an instance whose clock
// runs an hour ahead must not preempt them. Only the server clock ending the
// TTL releases them.
func TestCoordLeaseIgnoresClientClockSkew(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	onTime := coordStoreAt(t, mr, time.Now)
	ahead := coordStoreAt(t, mr, func() time.Time { return time.Now().Add(time.Hour) })

	if ok, err := onTime.LockTask(ctx, "ws-1", "task-1", "owner-a", time.Second); err != nil || !ok {
		t.Fatalf("lock: ok=%v err=%v", ok, err)
	}
	if ok, err := ahead.LockTask(ctx, "ws-1", "task-1", "owner-b", time.Second); err != nil || ok {
		t.Fatalf("the fast clock preempted a live task lock: ok=%v err=%v", ok, err)
	}
	if ok, err := onTime.AcquireLeadership(ctx, "a", time.Second); err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	if ok, err := ahead.AcquireLeadership(ctx, "b", time.Second); err != nil || ok {
		t.Fatalf("the fast clock preempted a live lease: ok=%v err=%v", ok, err)
	}

	// The server clock ends the TTL: now either instance may take both.
	mr.FastForward(2 * time.Second)
	if ok, err := ahead.LockTask(ctx, "ws-1", "task-1", "owner-b", time.Second); err != nil || !ok {
		t.Fatalf("expired lock not takeable: ok=%v err=%v", ok, err)
	}
	if ok, err := ahead.AcquireLeadership(ctx, "b", time.Second); err != nil || !ok {
		t.Fatalf("expired lease not takeable: ok=%v err=%v", ok, err)
	}
}

// TestCoordSlotsIgnoreClientClockSkew pins the reservation clock: scores and
// the purge cutoff come from the store's TIME, so a fast instance neither
// returns another instance's live reservation nor slips past the cap.
func TestCoordSlotsIgnoreClientClockSkew(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	onTime := coordStoreAt(t, mr, time.Now)
	ahead := coordStoreAt(t, mr, func() time.Time { return time.Now().Add(time.Hour) })

	if _, ok, err := onTime.ReserveSlot(ctx, "", 1); err != nil || !ok {
		t.Fatalf("reserve: ok=%v err=%v", ok, err)
	}
	if _, ok, err := ahead.ReserveSlot(ctx, "", 1); err != nil || ok {
		t.Fatalf("the fast clock purged a live reservation: ok=%v err=%v", ok, err)
	}
	if n, live, err := ahead.SlotSnapshot(ctx, ""); err != nil || n != 1 || live != 1 {
		t.Fatalf("snapshot = (%d,%d) err=%v, want (1,1)", n, live, err)
	}

	// The reservation TTL lapses on the store's clock: the slot comes back.
	mr.SetTime(time.Now().Add(reservationTTL + time.Minute))
	if _, ok, err := ahead.ReserveSlot(ctx, "", 1); err != nil || !ok {
		t.Fatalf("expired reservation was not returned: ok=%v err=%v", ok, err)
	}
}

// TestCoordLockRenewExtendsTheLease pins the holder's side of the lease: a
// hold that keeps renewing must not lapse at its original TTL, while a
// renewal from any other owner is refused and leaves the holder's lease
// untouched. The clock is the store's (the PX expiry), so the case runs on
// real time.
func TestCoordLockRenewExtendsTheLease(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	// TTLs follow the wall clock, or the original lease never lapses.
	mr.ClockTTL(true)
	store := coordStoreAt(t, mr, time.Now)
	renewer := store.(TaskLockRenewer)

	ttl := 200 * time.Millisecond
	if ok, err := store.LockTask(ctx, "ws-1", "task-1", "owner-a", ttl); err != nil || !ok {
		t.Fatalf("lock: ok=%v err=%v", ok, err)
	}
	// A different owner cannot extend a lease it does not hold.
	if ok, err := renewer.RenewTaskLock(ctx, "ws-1", "task-1", "owner-b", ttl); err != nil || ok {
		t.Fatalf("foreign renewal: ok=%v err=%v, want refused", ok, err)
	}
	// Renew past the original deadline: each renewal re-arms the full TTL.
	deadline := time.Now().Add(ttl)
	for time.Now().Before(deadline.Add(50 * time.Millisecond)) {
		if ok, err := renewer.RenewTaskLock(ctx, "ws-1", "task-1", "owner-a", ttl); err != nil || !ok {
			t.Fatalf("renewal: ok=%v err=%v", ok, err)
		}
		time.Sleep(ttl / 4)
	}
	// The original lease would have lapsed by now; the renewed one is live.
	if ok, err := store.LockTask(ctx, "ws-1", "task-1", "owner-b", ttl); err != nil || ok {
		t.Fatalf("a renewed lease was preempted: ok=%v err=%v", ok, err)
	}
	if err := store.UnlockTask(ctx, "ws-1", "task-1", "owner-a"); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if ok, err := store.LockTask(ctx, "ws-1", "task-1", "owner-b", ttl); err != nil || !ok {
		t.Fatalf("the released lock is not free: ok=%v err=%v", ok, err)
	}
}

// TestCoordStaleUnlockCannotReleaseANewerHold pins the owner check against the
// same-process race: an unlock that arrives after the lease was re-acquired
// names the previous acquisition and must leave the current hold alone. A
// constant per-process owner would delete the newer holder's lock here.
func TestCoordStaleUnlockCannotReleaseANewerHold(t *testing.T) {
	ctx := context.Background()
	_, store, _, _ := coordTestStore(t)
	ttl := time.Minute
	if ok, err := store.LockTask(ctx, "ws-1", "task-1", "inst-1/1", ttl); err != nil || !ok {
		t.Fatalf("first lock: ok=%v err=%v", ok, err)
	}
	if err := store.UnlockTask(ctx, "ws-1", "task-1", "inst-1/1"); err != nil {
		t.Fatalf("first unlock: %v", err)
	}
	if ok, err := store.LockTask(ctx, "ws-1", "task-1", "inst-1/2", ttl); err != nil || !ok {
		t.Fatalf("second lock: ok=%v err=%v", ok, err)
	}
	// The first acquisition's unlock arrives late.
	if err := store.UnlockTask(ctx, "ws-1", "task-1", "inst-1/1"); err != nil {
		t.Fatalf("stale unlock: %v", err)
	}
	if ok, err := store.LockTask(ctx, "ws-1", "task-1", "inst-1/3", ttl); err != nil || ok {
		t.Fatalf("a stale unlock released the current hold: ok=%v err=%v", ok, err)
	}
	// The current owner still releases its own hold.
	if err := store.UnlockTask(ctx, "ws-1", "task-1", "inst-1/2"); err != nil {
		t.Fatalf("second unlock: %v", err)
	}
	if ok, err := store.LockTask(ctx, "ws-1", "task-1", "inst-1/3", ttl); err != nil || !ok {
		t.Fatalf("the current owner could not release: ok=%v err=%v", ok, err)
	}
}

// TestCoordDeliveryWindowIgnoresClientClockSkew pins the confirmation window's
// clock: the window is written and judged on the store's clock, so a writer
// whose clock runs an hour slow does not hand the payload out a second time
// immediately, and a writer an hour fast does not hold it past the window.
func TestCoordDeliveryWindowIgnoresClientClockSkew(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	onTime := coordStoreAt(t, mr, time.Now)
	slow := coordStoreAt(t, mr, func() time.Time { return time.Now().Add(-time.Hour) })
	fast := coordStoreAt(t, mr, func() time.Time { return time.Now().Add(time.Hour) })

	window := time.Minute
	for _, taskID := range []string{"task-slow", "task-fast"} {
		if _, err := onTime.EnqueueEntry(ctx, "ws-1", taskID, coordTestEntry(taskID, `{"body":"x"}`)); err != nil {
			t.Fatalf("enqueue %s: %v", taskID, err)
		}
	}

	// The slow writer commits the delivery; the window is still open for a
	// reader on the right clock.
	pickup, err := slow.PickupDelivery(ctx, "ws-1", "task-slow", window)
	if err != nil || !pickup.Delivered {
		t.Fatalf("slow-clock delivery: delivered=%v err=%v", pickup.Delivered, err)
	}
	held, err := onTime.PickupDelivery(ctx, "ws-1", "task-slow", window)
	if err != nil {
		t.Fatalf("in-window pickup: %v", err)
	}
	if held.Delivered || !held.Held {
		t.Fatalf("a slow writer's clock shortened the window: %+v", held)
	}

	// The fast writer commits; the window ends on the store's clock, not an
	// hour from the writer's now.
	if fastPickup, err := fast.PickupDelivery(ctx, "ws-1", "task-fast", window); err != nil || !fastPickup.Delivered {
		t.Fatalf("fast-clock delivery: delivered=%v err=%v", fastPickup.Delivered, err)
	}
	mr.SetTime(time.Now().Add(window + time.Second))
	again, err := onTime.PickupDelivery(ctx, "ws-1", "task-fast", window)
	if err != nil {
		t.Fatalf("post-window pickup: %v", err)
	}
	if !again.Delivered {
		t.Fatalf("a fast writer's clock extended the window past its end: %+v", again)
	}
}

// A row without a Job name owns no name index: writing it anyway would put
// every such row on one shared, meaningless key.
func TestCoordEntryWithoutJobNameWritesNoIndex(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	s, err := New(Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    48 * time.Hour,
	}, WithClock(time.Now))
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	store := s.(CoordStore)
	entry := coordTestEntry("task-1", `{"body":"x"}`)
	entry.JobName = ""
	if err := store.SetEntry(ctx, "ws-1", "task-1", entry); err != nil {
		t.Fatalf("SetEntry: %v", err)
	}
	for _, key := range mr.Keys() {
		if strings.HasPrefix(key, keyJobIndex) {
			t.Fatalf("wrote job index %q for a row without a Job name", key)
		}
	}
	if got, ok, err := s.GetEntry(ctx, "ws-1", "task-1"); err != nil || !ok || got.TaskID != "task-1" {
		t.Fatalf("GetEntry: ok=%v err=%v", ok, err)
	}
}

// outageStore builds a coord store over miniredis and records the availability
// events the outage face emits (§9.1).
func outageStore(t *testing.T) (CoordStore, *miniredis.Miniredis, *[]string) {
	t.Helper()
	mr := miniredis.RunT(t)
	var events []string
	s, err := New(Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    time.Hour,
	}, WithEventHook(func(event string, _ time.Duration, _ error) {
		events = append(events, event)
	}))
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	return s.(CoordStore), mr, &events
}

// awaitDetached waits for the store operations that outlived their caller, so
// the assertions below read a settled availability face: the caller stopped
// waiting, the operation still concluded (§10 判定).
func awaitDetached(t *testing.T, store CoordStore) {
	t.Helper()
	store.(*redisStore).detached.wait()
}

// TestCoordCancelledOperationIsNotAnOutage pins the availability
// classification: a caller that cancels (the shutdown path, an abandoned
// request) says nothing about the store, so it neither starts nor clears an
// outage.
func TestCoordCancelledOperationIsNotAnOutage(t *testing.T) {
	store, mr, events := outageStore(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	// A caller that is already done never starts the operation, so the queue
	// marker survives the call: no write happens behind a wait that ended
	// before it began.
	queueKey := store.(*redisStore).queueKey("ws-1", "task-1")
	if err := mr.Set(queueKey, "1"); err != nil {
		t.Fatalf("seed queue marker: %v", err)
	}
	if err := store.DropQueue(cancelled, "ws-1", "task-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled op = %v, want context.Canceled", err)
	}
	awaitDetached(t, store)
	if !mr.Exists(queueKey) {
		t.Fatal("a pre-cancelled caller still executed the store write")
	}
	if !store.Available() || store.DownFor() != 0 {
		t.Fatalf("cancelled op moved the outage face: available=%v downFor=%s", store.Available(), store.DownFor())
	}
	if len(*events) != 0 {
		t.Fatalf("cancelled op emitted events %v", *events)
	}

	// A real failure opens the outage; a later cancellation must not close it.
	mr.SetError("outage")
	if err := store.DropQueue(context.Background(), "ws-1", "task-1"); err == nil {
		t.Fatal("store error was not reported")
	}
	if store.Available() {
		t.Fatal("a real store failure did not mark the store down")
	}
	if err := store.DropQueue(cancelled, "ws-1", "task-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled op during an outage = %v, want context.Canceled", err)
	}
	awaitDetached(t, store)
	if store.Available() {
		t.Fatal("a cancelled op cleared the outage")
	}
	if got := *events; len(got) != 1 || got[0] != "unavailable" {
		t.Fatalf("events = %v, want only the unavailable transition", got)
	}
}

// TestCoordStoreFailureIsAnOutage pins the other side: a genuine store failure
// still opens the outage and its recovery closes it.
func TestCoordStoreFailureIsAnOutage(t *testing.T) {
	store, mr, events := outageStore(t)
	mr.SetError("outage")
	if err := store.DropQueue(context.Background(), "ws-1", "task-1"); err == nil {
		t.Fatal("store failure was not reported")
	}
	if store.Available() || store.DownFor() == 0 {
		t.Fatalf("real failure did not open the outage: available=%v downFor=%s", store.Available(), store.DownFor())
	}
	mr.SetError("")
	if err := store.DropQueue(context.Background(), "ws-1", "task-1"); err != nil {
		t.Fatalf("recovered store: %v", err)
	}
	if !store.Available() || store.DownFor() != 0 {
		t.Fatalf("recovery did not clear the outage: available=%v downFor=%s", store.Available(), store.DownFor())
	}
	if got := *events; len(got) != 2 || got[0] != "unavailable" || got[1] != "recovered" {
		t.Fatalf("events = %v, want unavailable then recovered", got)
	}
}
