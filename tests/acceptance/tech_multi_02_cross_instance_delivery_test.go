// TC-tech-multi-02 implements AC-75's delivery face: a daemon alternating
// between two instances receives the task exactly once per confirmation
// window, the first hand-over may land on the instance that did not create the
// Job, the payload is verbatim (only runtime_id rewritten), the terminal
// verdict and the Job deletion happen once, and an instance killed before the
// Job exists does not lose the task (§5 单次派发, §4.3 未完成创建的接续).
package acceptance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

func TestTechMulti02CrossInstanceDeliveryAndTerminalSingleWrite(t *testing.T) {
	window := 300 * time.Millisecond
	tw := newTwoInstances(t, twoInstanceOptions{deliveryWindow: window, leaderCfg: smallLeaderCfg(2 * time.Second)})
	tw.declareAll(t)
	tw.a.startLeader(t)

	tw.a.upstream.enqueueTask("task-2")
	tw.startClaims(t)
	tw.awaitClaimedJob(t, "task-2")

	token := tw.token(t, tw.a, "task-2")
	// The daemon registers and claims through B — the instance that did NOT
	// create the Job (§2.1 任实例服务).
	tw.registerVia(t, tw.b, "task-2", token)

	claimed := tw.claimVia(t, tw.b, "task-2", token)
	if len(claimed) != 1 {
		t.Fatalf("first delivery on B = %d tasks, want 1", len(claimed))
	}
	wantBody := deliveryBodyComparable(t, deliveryPayload("task-2"))
	gotBody := deliveryBodyComparable(t, claimed[0])
	if fmt.Sprint(gotBody) != fmt.Sprint(wantBody) {
		t.Fatalf("delivered body = %v, want the claim payload %v (only runtime_id may change)", gotBody, wantBody)
	}
	// Inside the confirmation window the other instance answers empty (the
	// response may still be in flight).
	if again := tw.claimVia(t, tw.a, "task-2", token); len(again) != 0 {
		t.Fatalf("in-window claim on A delivered %d tasks, want none", len(again))
	}
	// Past the window the take lease releases the payload once.
	time.Sleep(window + multiSharedWindowPad)
	redelivered := tw.claimVia(t, tw.a, "task-2", token)
	if len(redelivered) != 1 {
		t.Fatalf("re-delivery on A = %d tasks, want 1", len(redelivered))
	}
	if fmt.Sprint(deliveryBodyComparable(t, redelivered[0])) != fmt.Sprint(wantBody) {
		t.Fatal("re-delivered body is not the claim payload")
	}

	// Terminal through the other instance: written once, Job deleted once.
	tw.startVia(t, tw.b, "task-2", token)
	tw.completeVia(t, tw.a, "task-2", token)
	deadline := time.Now().Add(10 * time.Second)
	for {
		e, ok := tw.entry(t, "task-2")
		if ok && e.IsTerminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task-2 never reached terminal state")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The replay through B is the same decided terminal: the server's applied
	// write set holds exactly one verdict for the task.
	if writes := tw.a.server.appliedTerminalWrites("task-2"); len(writes) != 1 {
		t.Fatalf("applied terminal writes on A = %d, want one", len(writes))
	}
	if n := tw.a.jobs.deletedCount("fm-task-2"); n != 1 {
		t.Fatalf("Job deletions = %d, want exactly one", n)
	}
	// Both instances project the same row (§4.1, AC-75).
	tw.assertProjectionsEqual(t, "task-2")

	// The shared row carries the two commits and the start fact; the start
	// confirmation moved the row out of the queue.
	entry, _ := tw.entry(t, "task-2")
	if entry.Deliveries != 2 {
		t.Fatalf("shared deliveries = %d, want 2 commits (first + window release)", entry.Deliveries)
	}
	if entry.StartedAt.IsZero() {
		t.Fatal("shared row lost the start fact")
	}
	depth, err := tw.coord.QueueDepth(context.Background())
	if err != nil {
		t.Fatalf("QueueDepth: %v", err)
	}
	if depth != 0 {
		t.Fatalf("queue depth = %d after the confirmation, want 0", depth)
	}
}

// TestTechMulti02ClaimContinuesAfterInstanceDeath covers the injected
// "instance killed before creating the Job": the claim row and its queue entry
// survive, and the takeover's convergence round continues the creation instead
// of failing a task that never ran (§4.3 未完成创建的接续).
func TestTechMulti02ClaimContinuesAfterInstanceDeath(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{
		deliveryWindow: 300 * time.Millisecond,
		leaderCfg:      smallLeaderCfg(time.Second),
	})
	tw.declareAll(t)

	// The dead instance had persisted the claim (row + queue entry) and
	// enqueued it, then died before the Job object existed. The shared state
	// is exactly what the claim path writes up to that point.
	dead := registry.TaskEntry{
		TaskID:         "task-dead",
		AgentID:        "agent-1",
		WorkspaceID:    "ws-1",
		JobNamespace:   "multica-agents",
		JobName:        "fm-task-dead",
		DaemonID:       "fm-task-dead",
		JobRuntimeID:   "rt-dead",
		State:          registry.StatePending,
		Attempt:        1,
		ClaimedAt:      time.Now(),
		BootDeadline:   time.Now().Add(time.Minute),
		LeaseKeepUntil: time.Now().Add(45 * time.Second),
		Payload:        deliveryPayload("task-dead"),
	}
	if _, err := tw.coord.EnqueueEntry(context.Background(), "ws-1", "task-dead", dead); err != nil {
		t.Fatalf("persist the pre-crash claim: %v", err)
	}
	tw.a.upstream.setTaskStatus("task-dead", "dispatched")

	// The survivor takes over (the dead instance never held a lease in this
	// scenario): its takeover resync sees the row, and the convergence round
	// continues the creation exactly once.
	tw.b.startLeader(t)
	if err := tw.b.sched.Reconcile(context.Background()); err != nil {
		t.Fatalf("takeover reconcile: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !tw.b.jobs.jobCreated("fm-task-dead") {
		if time.Now().After(deadline) {
			t.Fatal("the continued claim never created the Job")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := tw.b.jobs.liveJobCount("fm-task-dead"); n != 1 {
		t.Fatalf("live Jobs for fm-task-dead = %d, want exactly one", n)
	}
	if e, ok := tw.entry(t, "task-dead"); !ok || e.State != registry.StateJobCreated {
		t.Fatalf("shared row = %+v, want job_created after the continuation", e)
	}
	// A second convergence round is idempotent: no second Job, no duplicate
	// write.
	if err := tw.b.sched.Reconcile(context.Background()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if n := tw.b.jobs.jobCreatedCount("fm-task-dead"); n != 1 {
		t.Fatalf("Job create calls = %d, want exactly one", n)
	}
}
