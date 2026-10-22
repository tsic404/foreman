// TC-tech-multi-05 implements AC-78's outage face: with the shared state
// unavailable new claims, enqueues and token issuance stop; in-flight tasks
// keep being adjudicated from the local positive cache inside the grace
// window; past it every request fails closed (401/503); after recovery the
// reconciling holder resumes claims (§10 共享状态不可用).
package acceptance

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
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
	for !tw.a.leader.Held() {
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
