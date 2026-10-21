// TC-tech-multi-06 implements AC-78's data-loss face: after the shared rows
// are gone the mapping is rebuilt from the K8s objects (idempotently), a task
// whose token entry cannot be rebuilt is 401 and converges by the redispatched
// path, a lost queue row converges by "discard and let the server re-dispatch",
// and no task runs twice (§10 共享状态数据丢失).
package acceptance

import (
	"context"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

func TestTechMulti06SharedStateDataLoss(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{
		leaderCfg: smallLeaderCfg(2 * time.Second),
	})
	tw.declareAll(t)
	tw.a.startLeader(t)
	stopClaim := startConcurrency01ClaimLoop(t, tw.a)
	defer stopClaim()

	// task-live: fully running; its Job is the K8s truth the rebuild reads.
	tw.a.upstream.enqueueTask("task-live")
	tw.waitForTaskJob(t, "task-live", 10*time.Second)
	liveToken := tw.token(t, tw.a, "task-live")
	tw.registerVia(t, tw.b, "task-live", liveToken)
	if got := tw.claimVia(t, tw.b, "task-live", liveToken); len(got) != 1 {
		t.Fatalf("live delivery = %d, want 1", len(got))
	}
	tw.startVia(t, tw.b, "task-live", liveToken)
	tw.a.upstream.setTaskStatus("task-live", "running")

	// task-ghost: a persisted claim without any K8s object (the 401 face).
	ghost := registry.TaskEntry{
		TaskID:       "task-ghost",
		AgentID:      "agent-1",
		WorkspaceID:  "ws-1",
		JobNamespace: "multica-agents",
		JobName:      "fm-task-ghost",
		DaemonID:     "fm-task-ghost",
		JobRuntimeID: "rt-ghost",
		State:        registry.StatePending,
		Attempt:      1,
		ClaimedAt:    time.Now(),
		Payload:      deliveryPayload("task-ghost"),
	}
	if _, err := tw.coord.EnqueueEntry(context.Background(), "ws-1", "task-ghost", ghost); err != nil {
		t.Fatalf("persist ghost claim: %v", err)
	}
	ghostToken := tw.token(t, tw.a, "task-ghost")

	// task-queue: claimed and enqueued but never delivered; its queue row is
	// what the loss drops.
	tw.a.upstream.enqueueTask("task-queue")
	tw.waitForTaskJob(t, "task-queue", 10*time.Second)
	queueToken := tw.token(t, tw.a, "task-queue")

	// Data loss: everything in the shared store is gone.
	tw.a.crashLeader(t)
	stopClaim()
	tw.mr.FlushAll()

	// The holder restarts, resyncs the (empty) store and rebuilds the index
	// from the K8s objects — repeatedly, without duplicating anything.
	tw.a.startLeader(t)
	for range 2 {
		if err := tw.a.sched.Rebuild(context.Background()); err != nil {
			t.Fatalf("Rebuild after data loss: %v", err)
		}
	}
	if n := tw.a.jobs.jobCreatedCount("fm-task-live"); n != 1 {
		t.Fatalf("Job create calls for the live task = %d, want exactly one", n)
	}
	// The live task's row is back in the shared store (the rebuild writes it
	// through), so its credential is valid again.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := tw.entry(t, "task-live"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the K8s rebuild did not restore the live row")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if code := tw.statusVia(t, tw.b, "task-live", liveToken); code != 200 {
		t.Fatalf("live task after rebuild = %d, want 200", code)
	}

	// The ghost has no K8s object and no row: its token is rejected and the
	// task converges by the failure path.
	if code := tw.statusVia(t, tw.b, "task-ghost", ghostToken); code != 401 {
		t.Fatalf("ghost token after data loss = %d, want 401", code)
	}

	// The lost queue row converges through "discard, server re-dispatches":
	// the payload-less rebuilt row is discarded (its Job deleted, the entry
	// dropped), and a fresh claim re-creates it exactly once.
	if err := tw.a.sched.Reconcile(context.Background()); err != nil {
		t.Logf("reconcile after data loss: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		if _, ok := tw.entry(t, "task-queue"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the queue-lost row was never discarded")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !tw.a.jobs.jobDeleted("fm-task-queue") {
		t.Fatal("the queue-lost Job was not torn down")
	}
	tw.a.upstream.enqueueTask("task-queue")
	startClaimAgain := startConcurrency01ClaimLoop(t, tw.a)
	defer startClaimAgain()
	tw.waitLiveJob(t, "task-queue", 10*time.Second)
	if n := tw.a.jobs.liveJobCount("fm-task-queue"); n != 1 {
		t.Fatalf("live Jobs for the redispatch = %d, want exactly one", n)
	}
	// The re-dispatched task runs end to end once.
	tw.registerVia(t, tw.b, "task-queue", queueToken)
	if got := tw.claimVia(t, tw.b, "task-queue", queueToken); len(got) != 1 {
		t.Fatalf("redispatch delivery = %d, want 1", len(got))
	}
	tw.startVia(t, tw.b, "task-queue", queueToken)
	tw.completeVia(t, tw.b, "task-queue", queueToken)
	deadline = time.Now().Add(10 * time.Second)
	for {
		if e, ok := tw.entry(t, "task-queue"); ok && e.IsTerminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the re-dispatched task never reached terminal state")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := tw.a.jobs.deletedCount("fm-task-queue"); n != 2 {
		// One delete for the discarded stale Job, one for the settled one.
		t.Fatalf("Job deletions = %d, want 2 (discard + terminal)", n)
	}
}
