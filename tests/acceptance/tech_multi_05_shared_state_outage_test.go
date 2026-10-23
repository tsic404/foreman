// TC-tech-multi-05 implements AC-78's outage face: with the shared state
// unavailable new claims, enqueues and token issuance stop; in-flight tasks
// keep being adjudicated from the local positive cache inside the grace
// window; past it every request fails closed (401/503); after recovery the
// reconciling holder resumes claims (§10 共享状态不可用).
package acceptance

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/state"
)

func TestTechMulti05SharedStateOutage(t *testing.T) {
	grace := 700 * time.Millisecond
	tw := newTwoInstances(t, twoInstanceOptions{
		grace:     grace,
		leaderCfg: smallLeaderCfg(2 * time.Second),
	})
	tw.declareAll(t)
	tw.a.startLeader(t)
	stopClaim := startConcurrency01ClaimLoop(t, tw.a)
	defer stopClaim()

	// One running task (the in-flight face) and one registered-but-undelivered
	// task (the token-issuance face).
	tw.a.upstream.enqueueTask("task-live")
	tw.waitForTaskJob(t, "task-live", 10*time.Second)
	liveToken := tw.token(t, tw.a, "task-live")
	tw.registerVia(t, tw.b, "task-live", liveToken)
	if got := tw.claimVia(t, tw.b, "task-live", liveToken); len(got) != 1 {
		t.Fatalf("live delivery = %d tasks, want 1", len(got))
	}
	tw.startVia(t, tw.b, "task-live", liveToken)
	tw.a.upstream.setTaskStatus("task-live", "running")

	tw.a.upstream.enqueueTask("task-waiting")
	tw.waitForTaskJob(t, "task-waiting", 10*time.Second)
	waitToken := tw.token(t, tw.a, "task-waiting")
	tw.registerVia(t, tw.b, "task-waiting", waitToken)

	// Outage. The store answers no command from now on.
	tw.mr.SetError("shared state outage")
	outageAt := time.Now()

	// Inside the grace window the cached row still adjudicates the in-flight
	// task's requests.
	if code := tw.statusVia(t, tw.b, "task-live", liveToken); code != http.StatusOK {
		t.Fatalf("in-grace daemon request = %d, want 200", code)
	}
	// A delivery cannot be adjudicated without the shared row: fail closed.
	code, _ := tw.claimCode(t, tw.b, "task-waiting", waitToken)
	if code != http.StatusServiceUnavailable && code != http.StatusUnauthorized {
		t.Fatalf("delivery during outage = %d, want 503/401", code)
	}
	// New claims stop: once the instance has observed the outage (its own
	// store face reports unavailable), the claim loop's budget reads fail and
	// no C3 goes out. The enqueue happens after that point, so an in-flight
	// claim cannot have carried the task.
	deadline := time.Now().Add(5 * time.Second)
	for tw.a.coord.Available() {
		if time.Now().After(deadline) {
			t.Fatal("the instance never observed the shared-state outage")
		}
		time.Sleep(5 * time.Millisecond)
	}
	claimsBefore := len(tw.a.upstream.calls("POST", "/api/daemon/tasks/claim"))
	tw.a.upstream.enqueueTask("task-new")
	time.Sleep(300 * time.Millisecond)
	if claimsAfter := len(tw.a.upstream.calls("POST", "/api/daemon/tasks/claim")); claimsAfter != claimsBefore {
		t.Fatalf("claims continued during the outage: %d -> %d", claimsBefore, claimsAfter)
	}
	if tw.a.jobs.jobCreated("fm-task-new") {
		t.Fatal("a new claim happened while the shared state was down")
	}

	// Past the grace window the cached facts no longer adjudicate: fail
	// closed (the issuer's health gate answers 503).
	for time.Since(outageAt) <= grace+150*time.Millisecond {
		time.Sleep(20 * time.Millisecond)
	}
	if code := tw.statusVia(t, tw.b, "task-live", liveToken); code != http.StatusServiceUnavailable && code != http.StatusUnauthorized {
		t.Fatalf("post-grace daemon request = %d, want 503/401", code)
	}

	// Recovery: the holder re-acquires the lease (the outage lost it), the
	// takeover reconciles, and claims resume.
	tw.mr.SetError("")
	deadline = time.Now().Add(15 * time.Second)
	for !tw.a.leaderHeld() {
		if time.Now().After(deadline) {
			t.Fatal("the holder never re-acquired the lease after recovery")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopClaim()
	startClaim := startConcurrency01ClaimLoop(t, tw.a)
	defer startClaim()
	// A task enqueued after recovery is claimed again: the control plane is
	// back. (A claim that was in flight when the outage began is the server's
	// re-dispatch case and is not what this assertion measures.)
	tw.a.upstream.enqueueTask("task-after-recovery")
	tw.waitForTaskJob(t, "task-after-recovery", 15*time.Second)

	// §9.1: the availability transitions are events, not silence.
	lines := tw.stateEventLines()
	var sawUnavailable, sawRecovered bool
	for _, line := range lines {
		if strings.HasPrefix(line, "unavailable") {
			sawUnavailable = true
		}
		if strings.HasPrefix(line, "recovered") {
			sawRecovered = true
		}
	}
	if !sawUnavailable || !sawRecovered {
		t.Fatalf("state events = %v, want an unavailable and a recovered transition", lines)
	}

	// The in-flight task's terminal report lands after recovery and writes the
	// shared verdict once.
	tw.completeVia(t, tw.b, "task-live", liveToken)
	deadline = time.Now().Add(10 * time.Second)
	for {
		e, ok := tw.entry(t, "task-live")
		if ok && e.State == registry.StateTerminal {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("in-flight task did not converge after recovery: %+v", e)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestTechMulti05SharedStateOutageStopsIssuance covers the issuance half in
// isolation: with the store down a fresh delivery has no shared row to gate it
// and is refused, and the refusal is visible in the event log.
func TestTechMulti05SharedStateOutageStopsIssuance(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{
		grace:     50 * time.Millisecond,
		leaderCfg: smallLeaderCfg(time.Second),
	})
	tw.declareAll(t)
	tw.a.startLeader(t)
	stopClaim := startConcurrency01ClaimLoop(t, tw.a)
	defer stopClaim()
	tw.a.upstream.enqueueTask("task-issue")
	tw.waitForTaskJob(t, "task-issue", 10*time.Second)
	token := tw.token(t, tw.a, "task-issue")
	tw.registerVia(t, tw.b, "task-issue", token)

	tw.mr.SetError("outage")
	defer tw.mr.SetError("")

	// The token's entry adjudication itself is cache-served; the issuance
	// (pickup) needs the shared row and fails closed.
	code, _ := tw.claimCode(t, tw.b, "task-issue", token)
	if code != http.StatusServiceUnavailable && code != http.StatusUnauthorized {
		t.Fatalf("issuance during outage = %d, want 503/401", code)
	}
}

// mergeGateStore fails the token-digest merge of one armed task and passes
// every other command through: the store stays reachable, so a scenario
// isolates the digest write from a full outage.
type mergeGateStore struct {
	state.CoordStore
	phaseOne state.Store

	mu      sync.Mutex
	failFor string
	failed  int
}

// arm fails the next digest merge of taskID (once: the recovery half must
// land on the shared row).
func (s *mergeGateStore) arm(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failFor = taskID
}

// mergeFailures reports how many digest merges the gate refused.
func (s *mergeGateStore) mergeFailures() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed
}

func (s *mergeGateStore) MergeEntry(ctx context.Context, ws, taskID string, p registry.EntryPatch) error {
	s.mu.Lock()
	fail := p.TokenHash != nil && s.failFor != "" && s.failFor == taskID
	if fail {
		s.failFor = ""
		s.failed++
	}
	s.mu.Unlock()
	if fail {
		return errors.New("shared state: merge unavailable")
	}
	return s.CoordStore.MergeEntry(ctx, ws, taskID, p)
}

// The stage-one face of the same store.

func (s *mergeGateStore) PutEntry(ctx context.Context, ws, taskID string, e registry.TaskEntry) error {
	return s.phaseOne.PutEntry(ctx, ws, taskID, e)
}

func (s *mergeGateStore) PutSettled(ctx context.Context, ws, taskID string, e registry.TaskEntry) error {
	return s.phaseOne.PutSettled(ctx, ws, taskID, e)
}

func (s *mergeGateStore) GetSettled(ctx context.Context, ws, taskID string) (registry.TaskEntry, bool, error) {
	return s.phaseOne.GetSettled(ctx, ws, taskID)
}

func (s *mergeGateStore) DeleteSettled(ctx context.Context, ws, taskID string) error {
	return s.phaseOne.DeleteSettled(ctx, ws, taskID)
}

func (s *mergeGateStore) Revoke(ctx context.Context, ws, hash string, until time.Time) error {
	return s.phaseOne.Revoke(ctx, ws, hash, until)
}

func (s *mergeGateStore) IsRevoked(ctx context.Context, ws, hash string) (bool, error) {
	return s.phaseOne.IsRevoked(ctx, ws, hash)
}

func (s *mergeGateStore) Ping(ctx context.Context) error { return s.phaseOne.Ping(ctx) }

// TestTechMulti05DigestWriteFailureStopsIssuance is AC-78's token-issuance
// face at its narrowest point: every store command answers except the
// token-digest merge. The claim issues no credential and no Job, and its slot
// returns with the dropped entry; once the merge lands the claim succeeds and
// the other instance reads the digest off the shared row.
func TestTechMulti05DigestWriteFailureStopsIssuance(t *testing.T) {
	var gate *mergeGateStore
	tw := newTwoInstances(t, twoInstanceOptions{
		leaderCfg: smallLeaderCfg(2 * time.Second),
		wrap: func(store state.Store, coord state.CoordStore) (state.Store, state.CoordStore) {
			gate = &mergeGateStore{CoordStore: coord, phaseOne: store}
			return gate, gate
		},
	})
	tw.declareAll(t)
	tw.a.startLeader(t)
	stopClaim := startConcurrency01ClaimLoop(t, tw.a)
	defer stopClaim()

	gate.arm("task-digest")
	tw.a.upstream.enqueueTask("task-digest")
	waitFor(t, "the claim whose digest merge failed", 10*time.Second, func() bool {
		return gate.mergeFailures() > 0
	})
	waitFor(t, "the failed claim's entry to be dropped", 10*time.Second, func() bool {
		_, ok := tw.entry(t, "task-digest")
		return !ok
	})

	if tw.a.jobs.jobCreated("fm-task-digest") {
		t.Fatal("a Job was created although the token digest never reached the shared row")
	}
	tw.a.jobs.mu.Lock()
	secrets := len(tw.a.jobs.createdSecrets)
	tw.a.jobs.mu.Unlock()
	if secrets != 0 {
		t.Fatalf("credential Secrets created = %d, want 0: no token may be issued", secrets)
	}
	if calls := tw.a.upstream.calls("POST", "/api/daemon/tasks/task-digest/fail"); len(calls) != 0 {
		t.Fatalf("failure reports to the server = %d, want 0: an outage is not a task failure", len(calls))
	}
	if got := tw.a.upstream.taskStatus("task-digest"); got != taskStatusDispatched {
		t.Fatalf("server task status = %q, want dispatched (the task stays with the server)", got)
	}
	// The dropped entry releases the slot the claim took: the outage leaks no
	// in-flight capacity (AC-78's 计数均不增).
	waitFor(t, "the in-flight slot to return", 5*time.Second, func() bool {
		n, err := tw.coord.Slots(context.Background(), "")
		return err == nil && n == 0
	})

	// Recovery: the server's lease lapses and re-dispatches; the digest lands
	// on the shared row and the other instance adjudicates with it.
	if !tw.a.upstream.requeueTask("task-digest") {
		t.Fatal("task-digest was not dispatched when the requeue was requested")
	}
	tw.waitForTaskJob(t, "task-digest", 10*time.Second)
	e, ok := tw.entry(t, "task-digest")
	if !ok || e.TokenHash == "" {
		t.Fatalf("shared row = %+v (ok=%v), want the token digest", e, ok)
	}
	token := tw.a.builder.probeToken("fm-task-digest")
	if e.TokenHash != tw.a.issuer.Hash(token) {
		t.Fatalf("token_hash = %q, want the digest of the issued credential", e.TokenHash)
	}
	other, ok := tw.b.reg.GetInWorkspace("ws-1", "task-digest")
	if !ok || other.TokenHash != e.TokenHash {
		t.Fatalf("instance B row = %+v (ok=%v), want the same digest", other, ok)
	}
}
