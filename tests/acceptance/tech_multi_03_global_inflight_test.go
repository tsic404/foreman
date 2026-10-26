// TC-tech-multi-03 implements AC-76: the in-flight ceiling is cluster-level —
// the control-plane holder reserves and claims, a follower never claims, the
// shared counter never exceeds the cap, and a dead holder's unused
// reservations are reclaimed in one reconciliation round (§10 两级在途上限).
package acceptance

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/state"
)

func TestTechMulti03GlobalInflightLimitAndSlotReconciliation(t *testing.T) {
	const limit = 4
	tw := newTwoInstances(t, twoInstanceOptions{
		maxInflight: limit,
		leaderCfg:   smallLeaderCfg(2 * time.Second),
	})
	tw.declareAll(t)
	tw.a.startLeader(t)
	for i := range 6 {
		tw.a.upstream.enqueueTask(fmt.Sprintf("task-%d", i))
	}

	// Phase 1: the holder claims up to the cluster cap, and the counter never
	// exceeds it while the loop runs.
	stopA := startConcurrency01ClaimLoop(t, tw.a)
	// The counter is reserved before C3, so reaching the cap says the batch
	// holds its slots — not that its claims have landed. Stopping the loop on
	// the counter alone cancels a batch still in flight: the server has
	// already dispatched those tasks, yet their rows never appear, and the
	// later phases would address entries that do not exist.
	for i := range limit {
		tw.awaitClaimedJob(t, fmt.Sprintf("task-%d", i))
	}
	tw.waitSlots(t, limit)
	maxSeen := tw.sampleSlots(t, 600*time.Millisecond)
	stopA()
	if maxSeen > limit {
		t.Fatalf("shared counter reached %d, want at most %d", maxSeen, limit)
	}
	if live := tw.liveJobs(t); live > limit {
		t.Fatalf("live Jobs = %d, want at most %d", live, limit)
	}

	// Phase 2: a follower produces no claim. With the holder parked, the
	// server-side claim count must not move while the follower's loop runs.
	c3Before := len(tw.a.upstream.calls("POST", "/api/daemon/tasks/claim"))
	stopB := startConcurrency01ClaimLoop(t, tw.b)
	time.Sleep(400 * time.Millisecond)
	if c3After := len(tw.a.upstream.calls("POST", "/api/daemon/tasks/claim")); c3After != c3Before {
		t.Fatalf("follower issued claims: %d -> %d", c3Before, c3After)
	}
	stopB()

	// Phase 3: the holder dies and the survivor takes over. Settling one task
	// frees its slot and the new holder claims the next queued one, back up to
	// the cap.
	tw.a.crashLeader(t)
	tw.b.startLeader(t)
	stopClaimB := startConcurrency01ClaimLoop(t, tw.b)
	defer stopClaimB()

	tw.settleTask(t, tw.b, "task-0")
	tw.waitForTaskJob(t, "task-4", 10*time.Second)
	tw.waitSlots(t, limit)

	// Phase 4: the dead holder's unused reservation is a leak; the takeover
	// reconciliation lowers the counter to the row truth and the freed budget
	// is usable within one round.
	tw.settleTask(t, tw.b, "task-1")
	tw.waitForTaskJob(t, "task-5", 10*time.Second)
	tw.waitSlots(t, limit)
	tw.settleTask(t, tw.b, "task-2")
	tw.waitSlots(t, limit-1)
	ctx := context.Background()
	_, reserved, err := tw.coord.ReserveSlot(ctx, "", limit+1) // limit > current forces one stray slot
	if err != nil || !reserved {
		t.Fatalf("stray reservation: ok=%v err=%v", reserved, err)
	}
	tw.waitSlots(t, limit)
	// The counter says the cap is reached, so the queued task is not claimed.
	tw.a.upstream.enqueueTask("task-6")
	time.Sleep(300 * time.Millisecond)
	if tw.a.jobs.jobCreated("fm-task-6") {
		t.Fatal("a claim happened while the leaked counter read as full")
	}
	// A new takeover round reconciles the leak; the holder claims again.
	tw.b.stopLeader(t)
	tw.b.startLeader(t)
	tw.waitForTaskJob(t, "task-6", 10*time.Second)
	if slots, err := tw.coord.Slots(ctx, ""); err != nil || slots > limit {
		t.Fatalf("shared counter = %d (err %v) after reconciliation, want at most %d", slots, err, limit)
	}
}

// TestTechMulti03TakeoverResetDropsAReservationInFlight is AC-76's concurrent
// face: a zombie previous holder takes one more reservation inside the new
// holder's reset window. The serving path never puts one there — the claim loop
// is gated on l.leader() and OnAcquire completes before leader.held is set — so
// the window is built at the reset's own store calls (resetWindowZombie).
func TestTechMulti03TakeoverResetDropsAReservationInFlight(t *testing.T) {
	const limit = 2
	var zombie *resetWindowZombie
	tw := newTwoInstances(t, twoInstanceOptions{
		maxInflight: limit,
		leaderCfg:   smallLeaderCfg(1500 * time.Millisecond),
		wrap: func(store state.Store, coord state.CoordStore) (state.Store, state.CoordStore) {
			zombie = &resetWindowZombie{CoordStore: coord, limit: limit + 1}
			return store, zombie
		},
	})
	tw.declareAll(t)
	tw.a.startLeader(t)
	for i := range limit {
		tw.a.upstream.enqueueTask(fmt.Sprintf("task-%d", i))
	}
	stopA := startConcurrency01ClaimLoop(t, tw.a)
	// The Job object is not the batch's last fact: the row's job_created
	// commit follows it, and a stop in between leaves a pending row that a
	// later S-4 registration only answers without advancing.
	for i := range limit {
		tw.awaitClaimedJob(t, fmt.Sprintf("task-%d", i))
	}
	tw.waitSlots(t, limit)
	stopA()
	ctx := context.Background()
	if _, reservations, err := tw.coord.SlotSnapshot(ctx, ""); err != nil || reservations != 0 {
		t.Fatalf("reservations before the takeover = %d (err %v), want 0", reservations, err)
	}

	// The zombie holder wakes up inside the survivor's reset and reserves once.
	zombie.arm()
	tw.a.crashLeader(t)
	tw.b.startLeader(t)
	zombie.disarm()
	id, took, err := zombie.took()
	if err != nil || !took {
		t.Fatalf("the reset window carried no reservation: id=%q ok=%v err=%v", id, took, err)
	}

	// One round after the takeover: the counter is the live-row truth and no
	// marker of the dead holder survives (§10 两级在途上限，AC-76 末句).
	liveRows := liveRowCount(t, tw.b)
	if int(liveRows) != limit {
		t.Fatalf("live rows = %d after the takeover, want %d", liveRows, limit)
	}
	count, reservations, err := tw.coord.SlotSnapshot(ctx, "")
	if err != nil {
		t.Fatalf("SlotSnapshot: %v", err)
	}
	if reservations != 0 {
		t.Fatalf("takeover left %d pending reservation(s), want 0", reservations)
	}
	if count != liveRows {
		t.Fatalf("global counter = %d after the takeover, want the %d live rows", count, liveRows)
	}

	// Retiring the zombie's reservation must not return a slot the reset
	// already reclaimed: no permanent under-count below the live rows.
	if err := tw.coord.ReleaseSlot(ctx, "", id); err != nil {
		t.Fatalf("ReleaseSlot: %v", err)
	}
	if count, reservations, err = tw.coord.SlotSnapshot(ctx, ""); err != nil {
		t.Fatalf("SlotSnapshot after the release: %v", err)
	}
	if count != liveRows || reservations != 0 {
		t.Fatalf("counter = %d (reservations %d) after the zombie's reservation was retired, want %d (reservations 0)",
			count, reservations, liveRows)
	}

	// The reclaimed budget is exact: with the cap filled by live rows the
	// queued task is not claimed, and one settlement frees exactly one slot.
	stopB := startConcurrency01ClaimLoop(t, tw.b)
	defer stopB()
	tw.a.upstream.enqueueTask("task-next")
	time.Sleep(300 * time.Millisecond)
	if tw.a.jobs.jobCreated("fm-task-next") {
		t.Fatal("a claim was issued while the counter already matched the cap")
	}
	tw.settleTask(t, tw.b, "task-0")
	tw.waitForTaskJob(t, "task-next", 10*time.Second)
	tw.waitSlots(t, limit)
	if got := liveRowCount(t, tw.b); got != liveRows {
		t.Fatalf("live rows = %d after one settle and one claim, want %d", got, liveRows)
	}
}

// resetWindowZombie decorates the shared coordination face for the reset
// window: a resurrected holder takes a reservation, from another goroutine,
// while a reset-shaped store call of the global level is in flight. ResetSlots
// is the atomic reset's single call; SlotSnapshot is the split reset's read, so
// the last reservation lands after it and before the counter write — the shape
// that drops the +1 and keeps the marker.
type resetWindowZombie struct {
	state.CoordStore

	limit int

	mu   sync.Mutex
	armd bool
	ids  []string
	err  error
}

func (z *resetWindowZombie) arm() {
	z.mu.Lock()
	defer z.mu.Unlock()
	z.armd, z.ids, z.err = true, nil, nil
}

func (z *resetWindowZombie) disarm() {
	z.mu.Lock()
	defer z.mu.Unlock()
	z.armd = false
}

// took names the reservation the window carried last ("" when none).
func (z *resetWindowZombie) took() (string, bool, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if len(z.ids) == 0 {
		return "", false, z.err
	}
	return z.ids[len(z.ids)-1], true, z.err
}

func (z *resetWindowZombie) reserve() {
	z.mu.Lock()
	armed, limit := z.armd, z.limit
	z.mu.Unlock()
	if !armed {
		return
	}

	done := make(chan struct{})
	var id string
	var ok bool
	var err error
	go func() {
		defer close(done)
		id, ok, err = z.CoordStore.ReserveSlot(context.Background(), "", limit)
	}()
	<-done

	z.mu.Lock()
	defer z.mu.Unlock()
	if !z.armd {
		return
	}
	if err != nil {
		z.err = err
	} else if ok {
		z.ids = append(z.ids, id)
	}
}

func (z *resetWindowZombie) ResetSlots(ctx context.Context, ws string, n int64) (int64, int64, error) {
	if ws == "" {
		z.reserve()
	}
	return z.CoordStore.ResetSlots(ctx, ws, n)
}

func (z *resetWindowZombie) SlotSnapshot(ctx context.Context, ws string) (int64, int64, error) {
	count, reservations, err := z.CoordStore.SlotSnapshot(ctx, ws)
	if ws == "" {
		z.reserve()
	}
	return count, reservations, err
}

// liveRowCount counts h's non-terminal rows, the reset's own row truth.
func liveRowCount(t *testing.T, h *harness) int64 {
	t.Helper()
	var n int64
	for _, e := range h.reg.List() {
		if e.State != registry.StateTerminal {
			n++
		}
	}
	return n
}

// waitSlots blocks until the shared global counter reaches n.
func (tw *twoInstances) waitSlots(t *testing.T, n int64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		got, err := tw.coord.Slots(context.Background(), "")
		if err == nil && got == n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, err := tw.coord.Slots(context.Background(), "")
	_, gRes, _ := tw.coord.SlotSnapshot(context.Background(), "")
	_, wRes, _ := tw.coord.SlotSnapshot(context.Background(), "ws-1")
	wSlots, _ := tw.coord.Slots(context.Background(), "ws-1")
	_, eRes, _ := tw.coord.SlotSnapshot(context.Background(), "ws-empty")
	eSlots, _ := tw.coord.Slots(context.Background(), "ws-empty")
	t.Fatalf("shared counter = %d (err %v), want %d; ws-1 slots=%d res=%d; ws-empty slots=%d res=%d; global res=%d; live_jobs=%d; workspaces=%v\nreconcile logs: %s",
		got, err, n, wSlots, wRes, eSlots, eRes, gRes, tw.liveJobs(t), tw.a.sched.WorkspaceIDs(), reconcileLogLines(tw.a.logs.String()))
}

// sampleSlots samples the shared counter for d and returns the maximum seen.
func (tw *twoInstances) sampleSlots(t *testing.T, d time.Duration) int64 {
	t.Helper()
	var maxSeen int64
	var mu sync.Mutex
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if got, err := tw.coord.Slots(context.Background(), ""); err == nil {
				mu.Lock()
				if got > maxSeen {
					maxSeen = got
				}
				mu.Unlock()
			}
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(d)
	close(stop)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	return maxSeen
}

// liveJobs counts the Job objects currently held by the shared fake cluster.
func (tw *twoInstances) liveJobs(t *testing.T) int {
	t.Helper()
	return tw.a.jobs.liveJobs()
}

// waitLiveJob blocks until the task's Job is present on the live cluster face.
func (tw *twoInstances) waitLiveJob(t *testing.T, taskID string, timeout time.Duration) {
	t.Helper()
	name := "fm-" + taskID
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tw.a.jobs.liveJobCount(name) > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no live Job %s", name)
}

// waitForTaskJob blocks until the task's Job exists.
func (tw *twoInstances) waitForTaskJob(t *testing.T, taskID string, timeout time.Duration) {
	t.Helper()
	name := "fm-" + taskID
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tw.a.jobs.jobCreated(name) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	slots, _ := tw.coord.Slots(context.Background(), "")
	wsSlots, _ := tw.coord.Slots(context.Background(), "ws-1")
	t.Fatalf("Job %s was never created; slots=%d ws_slots=%d server_queue=%d live_jobs=%d; A logs: %s",
		name, slots, wsSlots, tw.a.upstream.queuedCount(), tw.a.jobs.liveJobs(), tw.a.logs.String())
}

// settleTask walks one task to a terminal verdict through h's daemon face, so
// its in-flight slot is released on the shared counter.
func (tw *twoInstances) settleTask(t *testing.T, h *harness, taskID string) {
	t.Helper()
	token := tw.token(t, h, taskID)
	tw.registerVia(t, h, taskID, token)
	if got := tw.claimVia(t, h, taskID, token); len(got) != 1 {
		t.Fatalf("%s delivery = %d tasks, want 1", taskID, len(got))
	}
	tw.startVia(t, h, taskID, token)
	tw.completeVia(t, h, taskID, token)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if e, ok := tw.entry(t, taskID); ok && e.State == registry.StateTerminal {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s never reached terminal state", taskID)
}

// reconcileLogLines extracts the two-level reconciliation event lines.
func reconcileLogLines(raw string) string {
	var out []string
	for _, line := range strings.Split(raw, "\n") {
		if strings.Contains(line, "coordination.slots_reconciled") {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}
