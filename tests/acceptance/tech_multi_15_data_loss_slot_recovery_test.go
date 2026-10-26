// TC-tech-multi-15 / AC-76, AC-78: the shared state loses its data with no
// process restart. The takeover rebuilds the mapping from the surviving K8s
// objects and writes the two-level counters from those rows; the dead holder's
// unused reservations — a row-less workspace's level and the global level —
// are reclaimed in the same round. The recovered counter stays usable and
// never exceeds the cap.
package acceptance

import (
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

func TestTechMulti15DataLossSlotRecovery(t *testing.T) {
	const limit = 3
	tw := newTwoInstances(t, twoInstanceOptions{
		maxInflight: limit,
		workspaces:  []string{"ws-1", "ws-empty"},
		leaderCfg:   smallLeaderCfg(600 * time.Millisecond),
	})
	ctx := t.Context()
	tw.declareAll(t)
	tw.a.startLeader(t)
	stopClaim := startConcurrency01ClaimLoop(t, tw.a)
	defer stopClaim()
	for _, taskID := range []string{"task-0", "task-1", "task-2"} {
		tw.a.upstream.enqueueTask(taskID)
	}
	for _, taskID := range []string{"task-0", "task-1", "task-2"} {
		tw.waitLiveJob(t, taskID, 10*time.Second)
	}
	tw.waitSlots(t, limit)
	if live := tw.liveJobs(t); live != limit {
		t.Fatalf("live Jobs = %d, want %d", live, limit)
	}

	// Data loss without a process restart: the store is flushed while the Jobs
	// keep existing. The rebuilt rows land in the store, then the takeover
	// must write the counters from those rows (they were zeroed by the loss).
	tw.a.crashLeader(t)
	stopClaim()
	tw.mr.FlushAll()
	if err := tw.a.sched.Rebuild(ctx); err != nil {
		t.Fatalf("Rebuild after data loss: %v", err)
	}
	// A dead holder's unused reservations: one on the row-less workspace, one
	// global. Both must be reclaimed by the same takeover round (AC-76).
	if _, ok, err := tw.coord.ReserveSlot(ctx, "ws-empty", 10); err != nil || !ok {
		t.Fatalf("stray workspace reservation: ok=%v err=%v", ok, err)
	}
	if _, ok, err := tw.coord.ReserveSlot(ctx, "", 10); err != nil || !ok {
		t.Fatalf("stray global reservation: ok=%v err=%v", ok, err)
	}

	tw.a.startLeader(t)
	tw.waitSlots(t, limit)
	if n, err := tw.coord.Slots(ctx, "ws-empty"); err != nil || n != 0 {
		t.Fatalf("row-less workspace slots = %d (err %v), want 0 (stale reservation reclaimed)", n, err)
	}
	if _, n, err := tw.coord.SlotSnapshot(ctx, ""); err != nil || n != 0 {
		t.Fatalf("global reservations = %d (err %v), want 0 after the takeover", n, err)
	}
	if _, n, err := tw.coord.SlotSnapshot(ctx, "ws-empty"); err != nil || n != 0 {
		t.Fatalf("row-less workspace reservations = %d (err %v), want 0", n, err)
	}

	// The recovered counter is usable and never exceeds the cap: settling one
	// task frees exactly one slot, and the next claim fills it.
	tw.settleRebuilt(t, "task-0")
	tw.waitSlots(t, limit-1)
	startClaimAgain := startConcurrency01ClaimLoop(t, tw.a)
	defer startClaimAgain()
	tw.a.upstream.enqueueTask("task-3")
	tw.waitLiveJob(t, "task-3", 10*time.Second)
	tw.waitSlots(t, limit)
	if n, err := tw.coord.Slots(ctx, ""); err != nil || n > limit {
		t.Fatalf("slots = %d (err %v), want at most %d", n, err, limit)
	}
}

// TestTechMulti15EmptyWorkspaceCountsGloballyOnly pins the level split the
// design lists as this case's evidence: a live row with an empty workspace
// counts in the global total only, never as a workspace level.
func TestTechMulti15EmptyWorkspaceCountsGloballyOnly(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{
		maxInflight: 3,
		leaderCfg:   smallLeaderCfg(time.Second),
	})
	ctx := t.Context()
	tw.declareAll(t)
	tw.a.startLeader(t)
	stopClaim := startConcurrency01ClaimLoop(t, tw.a)
	defer stopClaim()
	for _, taskID := range []string{"task-a", "task-b"} {
		tw.a.upstream.enqueueTask(taskID)
		tw.waitLiveJob(t, taskID, 10*time.Second)
	}
	tw.waitSlots(t, 2)
	// The setup claims are done. An idle loop with a free slot still cycles a
	// reservation on both levels every interval, so the counters below are only
	// the settled state after the loop has exited and released what it took.
	stopClaim()

	if err := tw.a.reg.Put(registry.TaskEntry{
		TaskID:       "task-nos",
		JobName:      "fm-task-nos",
		DaemonID:     "fm-task-nos",
		JobRuntimeID: "rt-nos",
		State:        registry.StateDaemonRegistered,
		ClaimedAt:    time.Now(),
	}); err != nil {
		t.Fatalf("seed empty-workspace row: %v", err)
	}
	if err := tw.a.sched.ReconcileSlots(ctx); err != nil {
		t.Fatalf("ReconcileSlots: %v", err)
	}
	if got, err := tw.coord.Slots(ctx, ""); err != nil || got != 3 {
		t.Fatalf("global counter after reconcile = %d (err %v), want 3 (the malformed row counts once, never as the whole level)", got, err)
	}
	if got, err := tw.coord.Slots(ctx, "ws-1"); err != nil || got != 2 {
		t.Fatalf("workspace counter after reconcile = %d (err %v), want 2 (the empty-workspace row is not a workspace level)", got, err)
	}
}

// settleRebuilt settles a rebuilt (payload-less) entry through the daemon
// face: registration is not needed for S-8/S-13, and the terminal transition
// releases the entry's in-flight slot.
func (tw *twoInstances) settleRebuilt(t *testing.T, taskID string) {
	t.Helper()
	token := tw.token(t, tw.a, taskID)
	tw.startVia(t, tw.a, taskID, token)
	tw.completeVia(t, tw.a, taskID, token)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if e, ok := tw.entry(t, taskID); ok && e.State == registry.StateTerminal {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s never reached terminal state", taskID)
}
