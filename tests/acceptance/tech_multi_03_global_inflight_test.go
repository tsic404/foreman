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
