// TC-tech-multi-04 implements AC-77: the control plane (registration,
// heartbeat, subscription, orphan reclaim, claim, convergence) is held by one
// instance; killing it recovers the same actions on the survivor within the
// lease plus takeover window, with the same registration identity and no new
// C1, while the Job daemon face keeps answering and no task is lost or run
// twice (§10 控制面接管有界).
package acceptance

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
)

func TestTechMulti04ControlPlaneTakeover(t *testing.T) {
	leaseTTL := time.Second
	tw := newTwoInstances(t, twoInstanceOptions{leaderCfg: smallLeaderCfg(leaseTTL)})
	tw.declareAll(t)
	tw.a.startLeader(t)
	tw.b.runLeader(t)

	// Both instances run the production loops; only the holder's produce
	// traffic. The heartbeat loop is the observable control-plane stream.
	stopHB := startHeartbeatLoop(t, tw.a)
	defer stopHB()
	stopHBB := startHeartbeatLoop(t, tw.b)
	defer stopHBB()
	stopClaimA := startConcurrency01ClaimLoop(t, tw.a)
	defer stopClaimA()
	stopClaimB := startConcurrency01ClaimLoop(t, tw.b)
	defer stopClaimB()

	// The holder's heartbeats reach the server under the declared identities.
	// Wait for every registered runtime, not the first stream to land: a stream
	// whose first send comes later would read as a new identity after takeover.
	registered := tw.a.client.RuntimeIDs()
	deadline := time.Now().Add(10 * time.Second)
	idsBefore := tw.a.upstream.heartbeatRuntimeIDs()
	for !heartbeatsCover(idsBefore, registered) {
		if time.Now().After(deadline) {
			t.Fatalf("the holder heartbeated %v, want every registered runtime %v", idsBefore, registered)
		}
		time.Sleep(10 * time.Millisecond)
		idsBefore = tw.a.upstream.heartbeatRuntimeIDs()
	}
	c1Before := len(tw.a.upstream.calls("POST", "/api/daemon/register"))

	tw.a.upstream.enqueueTask("task-takeover")
	tw.waitForTaskJob(t, "task-takeover", 10*time.Second)

	// The Job daemon face answers through the survivor while the holder is
	// alive.
	token := tw.token(t, tw.a, "task-takeover")
	tw.registerVia(t, tw.b, "task-takeover", token)
	tw.a.upstream.setTaskStatus("task-takeover", "running")
	if code := tw.statusVia(t, tw.b, "task-takeover", token); code != http.StatusOK {
		t.Fatalf("daemon face through B = %d, want 200", code)
	}

	// Kill the holder: its lease loop and its control loops stop. The takeover
	// lower bound is the lease the store actually held, not the kill instant:
	// the holder's last renewal lands up to one renewal tick before the kill,
	// so its lease expires up to a tick before killedAt + leaseTTL.
	killedAt := time.Now()
	tw.a.crashLeader(t)
	leaseExpiresAt := tw.a.leaseExpiryLower()
	if leaseExpiresAt.IsZero() {
		t.Fatal("the killed holder recorded no lease grant")
	}
	stopClaimA()
	stopHB()
	tw.a.upstream.enqueueTask("task-after-takeover")
	time.Sleep(leaseTTL / 3)
	if tw.a.jobs.jobCreated("fm-task-after-takeover") {
		t.Fatal("a claim happened inside the takeover window")
	}
	// The daemon face of the survivor stays available during the window.
	if code := tw.statusVia(t, tw.b, "task-takeover", token); code != http.StatusOK {
		t.Fatalf("daemon face during takeover = %d, want 200", code)
	}

	// The survivor takes over: its lease grant is the takeover instant, and it
	// must not precede the dead holder's expiry. The seam records the instant
	// after that grant's store call, so the value never understates the real
	// grant and the claim loop + Job creation stay outside the measurement.
	deadline = time.Now().Add(10 * time.Second)
	for !tw.b.leaderHeld() {
		if time.Now().After(deadline) {
			holder, _ := tw.coord.LeadershipHolder(context.Background())
			t.Fatalf("the survivor never acquired the lease; holder=%q\nB logs: %s", holder, tw.b.logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	takeoverAt := tw.b.leaseAcquiredAt()
	if takeoverAt.IsZero() {
		t.Fatal("the survivor recorded no lease acquisition")
	}
	if takeoverAt.Before(leaseExpiresAt) {
		t.Fatalf("survivor acquired the lease at %s, earlier than the dead holder's expiry %s (killed at %s)",
			takeoverAt.Format(time.RFC3339Nano), leaseExpiresAt.Format(time.RFC3339Nano), killedAt.Format(time.RFC3339Nano))
	}
	tw.waitForTaskJob(t, "task-after-takeover", 10*time.Second)

	// Identity continuity: no re-registration, and the heartbeats resume with
	// the same real runtime ids.
	time.Sleep(300 * time.Millisecond)
	if c1After := len(tw.a.upstream.calls("POST", "/api/daemon/register")); c1After != c1Before {
		t.Fatalf("C1 calls after takeover = %d, want the pre-kill %d (no re-registration)", c1After, c1Before)
	}
	idsAfter := tw.a.upstream.heartbeatRuntimeIDs()
	for id := range idsAfter {
		if _, known := idsBefore[id]; !known {
			t.Fatalf("heartbeat resumed under a new runtime id %s; registered=%v before=%v after=%v", id, registered, idsBefore, idsAfter)
		}
	}
	before := idsBefore["rid-omp"]
	deadline = time.Now().Add(5 * time.Second)
	for tw.a.upstream.heartbeatRuntimeIDs()["rid-omp"] <= before {
		if time.Now().After(deadline) {
			t.Fatal("the survivor's heartbeats never resumed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// No duplicate execution or lost terminal: the pre-kill task settles
	// once through the survivor.
	tw.startVia(t, tw.b, "task-takeover", token)
	tw.completeVia(t, tw.b, "task-takeover", token)
	if e, ok := tw.entry(t, "task-takeover"); !ok || !e.IsTerminal() {
		t.Fatalf("pre-kill task row = %+v, want terminal", e)
	}
	if n := tw.a.jobs.deletedCount("fm-task-takeover"); n != 1 {
		t.Fatalf("Job deletions = %d, want exactly one", n)
	}
}

// heartbeatsCover reports whether every registered runtime id heartbeated; an
// empty registration never covers.
func heartbeatsCover(counts map[string]int, ids []string) bool {
	if len(ids) == 0 {
		return false
	}
	for _, id := range ids {
		if counts[id] == 0 {
			return false
		}
	}
	return true
}

// startHeartbeatLoop runs the production heartbeat loop gated on the harness's
// leader, so a follower opens no C-2 stream.
func startHeartbeatLoop(t *testing.T, h *harness) func() {
	cancel, wait := startHeartbeatLoopDetached(t, h)
	var once sync.Once
	return func() { once.Do(func() { cancel(); wait() }) }
}

// startHeartbeatLoopDetached is the loop above with its stop split into
// cancel and wait, for scenarios that pin the exit ordering.
func startHeartbeatLoopDetached(t *testing.T, h *harness) (cancel func(), wait func()) {
	t.Helper()
	reporter := proxy.NewModelCatalogReporter(h.cfg, h.client, h.sched, h.client.CatalogSlots(), h.metrics)
	loop := proxy.NewHeartbeatLoop(h.client, reporter)
	loop.SetLeaderGate(h.leaderHeld)
	ctx, cancelCtx := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = loop.Run(ctx)
	}()
	var once sync.Once
	cancel = func() { once.Do(cancelCtx) }
	wait = func() { <-done }
	t.Cleanup(func() { cancel(); wait() })
	return cancel, wait
}
