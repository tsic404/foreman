// TC-tech-multi-02 implements AC-75's delivery face: a daemon alternating
// between two instances receives the task exactly once per confirmation
// window, the first hand-over may land on the instance that did not create the
// Job, the payload is verbatim (only runtime_id rewritten), the terminal
// verdict and the Job deletion happen once, and an instance killed before the
// Job exists does not lose the task (§5 单次派发, §4.3 未完成创建的接续).
package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
	"github.com/tsic404/foreman/internal/state"
)

func TestTechMulti02CrossInstanceDeliveryAndTerminalSingleWrite(t *testing.T) {
	// The window only has to outlast the probe that reads it: the in-window
	// claim runs a few milliseconds after the delivery commit, and a loaded
	// machine must not be able to schedule it past the lease.
	window := time.Second
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

	// The delivery is what the case judges, not the request that carried it:
	// the daemon keeps claiming, so the batch with the payload is awaited.
	claimed := tw.awaitDelivery(t, tw.b, "task-2", token, 10*time.Second)
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
	// Past the window the take lease releases the payload once. The window's
	// end is observed through the claim it releases — an in-window claim is
	// answered empty — instead of a fixed sleep a loaded machine can
	// overshoot; the wait itself stays bounded by the window plus the pad.
	redelivered := tw.awaitDelivery(t, tw.a, "task-2", token, window+multiSharedWindowPad)
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

// multi02NameTemplate names the Job after the claim's agent segment and task
// key, so the continuation can only reproduce the row's job_name when the row
// carried the claim's identity (the fallback segment is "agent").
const multi02NameTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<agent-segment>-<task-key>
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`

// multi02ClaimPayload is the claim whose agent name and session path have to
// survive the row: the agent segment of the Job's name, the agent-id label and
// the soft-affinity key all read them.
func multi02ClaimPayload(taskID string) json.RawMessage {
	return json.RawMessage(`{
		"id": "` + taskID + `", "agent_id": "agent-1", "workspace_id": "ws-1",
		"issue_id": "issue-1", "issue_identifier": "TSI 4592",
		"runtime_id": "rid-omp", "prior_session_id": "/state/home/.multica/pi-sessions/sess-1.jsonl",
		"agent": {"name": "OMP Bot", "model": "k3"}
	}`)
}

// multi02CrashBuilder stops one instance's claim inside its create window: the
// shared row and its queue entry are written, the render never returns, and no
// object exists. The goroutine is never released — that instance is dead for
// the rest of the scenario, and the survivor is what continues the claim
// (§4.3 未完成创建的接续).
type multi02CrashBuilder struct {
	inner   probeBuilder
	taskID  string
	crashed chan struct{}
}

func (b *multi02CrashBuilder) Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, string, error) {
	if e.TaskID == b.taskID {
		close(b.crashed)
		select {}
	}
	return b.inner.Build(e)
}

func (b *multi02CrashBuilder) ResolveName(e jobbuilder.TaskEntry) (string, error) {
	return b.inner.ResolveName(e)
}

func (b *multi02CrashBuilder) RequiresProjectIdentity(e jobbuilder.TaskEntry) bool {
	return b.inner.RequiresProjectIdentity(e)
}

func (b *multi02CrashBuilder) BuildCatalogProbe(requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, error) {
	return b.inner.BuildCatalogProbe(requestID, jobRuntimeID, workspaceID, budget)
}

func (b *multi02CrashBuilder) probeToken(jobName string) string { return b.inner.probeToken(jobName) }

// deadProcessStore stops the dead instance's renewals once the crash is
// signalled: a process that is gone never reaches the store again, so the
// write lock it held lapses on its TTL as a real crash leaves it (§5 写锁自带
// 租约上界). Every other command still goes to the shared store.
type deadProcessStore struct {
	state.CoordStore
	renewer state.TaskLockRenewer
	crashed <-chan struct{}
}

func (s *deadProcessStore) RenewTaskLock(ctx context.Context, ws, taskID, owner string, ttl time.Duration) (bool, error) {
	select {
	case <-s.crashed:
		return false, nil
	default:
	}
	return s.renewer.RenewTaskLock(ctx, ws, taskID, owner, ttl)
}

// TestTechMulti02ClaimContinuesAfterInstanceDeath covers the injected
// "instance killed before creating the Job" through the production claim path:
// the claim persists the row and its queue entry, the process dies inside the
// render, and the takeover's convergence round continues the creation. The
// continued Job must be the one the row names — the same name the dead claim
// composed — with no second object behind it and no failure for the task
// (§4.3 未完成创建的接续, AC-75 至多一个活跃 Job).
func TestTechMulti02ClaimContinuesAfterInstanceDeath(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{
		deliveryWindow: 300 * time.Millisecond,
		leaderCfg:      smallLeaderCfg(time.Second),
	})
	tw.declareAll(t)
	template := writeTemplateFile(t, multi02NameTemplate)
	tw.a.useRealBuilderWithTemplate(t, template)
	tw.b.useRealBuilderWithTemplate(t, template)

	const taskID = "task-crash"
	payload := multi02ClaimPayload(taskID)
	// The crash window: the claim has written its row and queue entry, and the
	// render it dies in is the next thing that would touch anything.
	crashed := make(chan struct{})
	tw.a.builder = &multi02CrashBuilder{inner: tw.a.builder, taskID: taskID, crashed: crashed}
	// The dead instance never renews again: its lease lapses on its TTL and
	// the survivor can take the task over.
	tw.a.coord = &deadProcessStore{
		CoordStore: tw.a.coord,
		renewer:    tw.a.coord.(state.TaskLockRenewer),
		crashed:    crashed,
	}
	tw.a.rebuildScheduler(t, func(*scheduler.Config) {})
	go func() { _ = tw.a.sched.OnClaimFor(context.Background(), "ws-1", payload) }()
	select {
	case <-crashed:
	case <-time.After(10 * time.Second):
		t.Fatal("the claim never reached the crash window")
	}

	// The row is what the continuation renders from: it must carry the claim's
	// identity, or the render falls back to the "agent" segment.
	row, ok := tw.entry(t, taskID)
	if !ok {
		t.Fatal("the dead claim left no shared row")
	}
	const wantName = "fm-omp-bot-taskcrash"
	if row.JobName != wantName {
		t.Fatalf("row job_name = %q, want the claim's composed name %q", row.JobName, wantName)
	}
	if row.AgentName != "OMP Bot" || row.PriorSessionID != "/state/home/.multica/pi-sessions/sess-1.jsonl" {
		t.Fatalf("row identity = agent %q / session %q, want the claim's values",
			row.AgentName, row.PriorSessionID)
	}
	// The crash landed before any object: nothing was placed under the
	// recorded name or under the name a row without the identity would render.
	if tw.a.jobs.jobCreated(wantName) || tw.a.jobs.jobCreated("fm-agent-taskcrash") {
		t.Fatal("the dead claim placed an object before it died")
	}

	// The survivor takes over (the dead instance never held a lease in this
	// scenario): its takeover resync sees the row, and the convergence round
	// continues the creation exactly once — the dead instance stopped renewing,
	// so the write lock it held lapses on its TTL.
	tw.b.startLeader(t)
	deadline := time.Now().Add(20 * time.Second)
	for !tw.b.jobs.jobCreated(wantName) {
		if time.Now().After(deadline) {
			e, _ := tw.entry(t, taskID)
			t.Fatalf("the continued claim never created %s; row=%s/%s", wantName, e.State, e.FailureReason)
		}
		_ = tw.b.sched.Reconcile(context.Background())
		time.Sleep(20 * time.Millisecond)
	}
	if n := tw.b.jobs.liveJobCount(wantName); n != 1 {
		t.Fatalf("live Jobs named %s = %d, want exactly one", wantName, n)
	}
	if n := tw.b.jobs.liveJobCount("fm-agent-taskcrash"); n != 0 {
		t.Fatalf("orphan Jobs under the fallback name = %d, want none", n)
	}
	continued, ok := tw.entry(t, taskID)
	if !ok || continued.State != registry.StateJobCreated {
		t.Fatalf("shared row = %+v (ok=%v), want job_created after the continuation", continued, ok)
	}

	// A second convergence round is idempotent: no second Job, and no verdict
	// for a task that never ran (the recorded name is what the missing-Job
	// compensation looks up, and that object exists).
	if err := tw.b.sched.Reconcile(context.Background()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if n := tw.b.jobs.jobCreatedCount(wantName); n != 1 {
		t.Fatalf("Job create calls = %d, want exactly one", n)
	}
	if fails := tw.b.server.forwarded(scheduler.EPFail); len(fails) != 0 {
		t.Fatalf("failure reports through B = %+v, want none for a continued claim", fails)
	}
	if e, _ := tw.entry(t, taskID); e.IsTerminal() {
		t.Fatalf("row settled %s/%s, want the continued claim left running", e.State, e.FailureReason)
	}
}
