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
	_, store, now, _ := coordTestStore(t)
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
	// Past the window: the row returns to the takeable state.
	*now = now.Add(window + time.Millisecond)
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
// identity: consume and release delete the marker they name, never the oldest
// pending one that another claim still owns.
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

	// The consume retires only the caller's marker; the counter keeps counting
	// the row that now owns the slot.
	if err := store.ConsumeSlotReservation(ctx, "", mine); err != nil {
		t.Fatalf("consume mine: %v", err)
	}
	assertReservations(t, mr, keySlotResAll, other)
	if n, live, err := store.SlotSnapshot(ctx, ""); err != nil || n != 2 || live != 1 {
		t.Fatalf("after consume: counter=%d live=%d err=%v, want (2,1)", n, live, err)
	}

	// Retiring a marker that is already gone leaves the counter alone: a
	// repeated release must not decrement a live row's slot.
	if err := store.ReleaseSlot(ctx, "", mine); err != nil {
		t.Fatalf("release consumed marker: %v", err)
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
	if err := store.ConsumeSlotReservation(ctx, "", stale); err != nil {
		t.Fatalf("consume stale: %v", err)
	}
	if err := store.ReleaseSlot(ctx, "", stale); err != nil {
		t.Fatalf("release stale: %v", err)
	}
	if n, err := store.Slots(ctx, ""); err != nil || n != 1 {
		t.Fatalf("counter after retiring the purged marker = %d (err %v), want 1", n, err)
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
// write: a reservation taken between the snapshot and the write must make the
// CAS decline (never lose the +1), and an unchanged counter must be writable.
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
	swapped, err := store.CompareAndSetSlots(ctx, "", count, 0)
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
	count, _, _ = store.SlotSnapshot(ctx, "")
	swapped, err = store.CompareAndSetSlots(ctx, "", count, 1)
	if err != nil || !swapped {
		t.Fatalf("cas unchanged: swapped=%v err=%v", swapped, err)
	}
	if got, _ := store.Slots(ctx, ""); got != 1 {
		t.Fatalf("counter after the CAS = %d, want 1", got)
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

// TestCoordCancelledOperationIsNotAnOutage pins the availability
// classification: a caller that cancels (the shutdown path, an abandoned
// request) says nothing about the store, so it neither starts nor clears an
// outage.
func TestCoordCancelledOperationIsNotAnOutage(t *testing.T) {
	store, mr, events := outageStore(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.DropQueue(cancelled, "ws-1", "task-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled op = %v, want context.Canceled", err)
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
