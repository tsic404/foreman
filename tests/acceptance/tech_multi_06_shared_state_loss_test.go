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
	"github.com/tsic404/foreman/internal/scheduler"
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
	tw.awaitClaimedJob(t, "task-live")
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

	// The scheduler's C13 seam is a separate fixture from the daemon face; it
	// must agree with the world this case built, or its default "dispatched"
	// would read the running task as a pre-start claim and discard its Job.
	tw.a.server.setStatus("task-live", "running")
	tw.a.server.setStatus("task-queue", taskStatusDispatched)

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
	tw.awaitClaimedJob(t, "task-queue")
	if n := tw.a.jobs.liveJobCount("fm-task-queue"); n != 1 {
		t.Fatalf("live Jobs for the redispatch = %d, want exactly one", n)
	}
	// The Job object is not the registration gate; the row's job_created commit
	// is (a registration read while the row is still pending advances nothing).
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
	// Data loss is not a task failure (§10): the 401'd ghost and the discarded
	// queue task converge on the server's own lease handling. The scheduler's
	// ServerClient is the in-process seam in this fixture, so its forwards are
	// where a synthesized fail would land.
	for _, taskID := range []string{"task-ghost", "task-queue"} {
		reports := 0
		for _, req := range tw.a.server.forwarded(scheduler.EPFail) {
			if req.Path == taskID {
				reports++
			}
		}
		if reports != 0 {
			t.Fatalf("failure reports for %s = %d, want 0: data loss is not a task failure", taskID, reports)
		}
	}
	// The only Jobs this case deletes are the queue task's: one for the
	// discarded stale Job, one for the settled Job. A rebuild that deletes
	// anything else (the live Job, or an object that never existed) shows up
	// here as a third name.
	tw.a.jobs.mu.Lock()
	deleted := append([]string(nil), tw.a.jobs.deletedJobs...)
	tw.a.jobs.mu.Unlock()
	if len(deleted) != 2 {
		t.Fatalf("Job deletions = %d (%v), want exactly 2 (discard + terminal)", len(deleted), deleted)
	}
	for _, name := range deleted {
		if name != "fm-task-queue" {
			t.Fatalf("Job deletion of %q, want only fm-task-queue: the rebuild must not delete K8s truth", name)
		}
	}
}
