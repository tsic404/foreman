// TC-tech-multi-13 / AC-107: shutdown narrows deregistration. C14 is sent only
// when this process is the last live instance and the shared store answered,
// once per workspace identity and carrying that identity's full real runtime
// set. The case covers the three forms: a single instance with several
// workspace identities, two live instances with one shutting down (leader and
// follower), and a last instance whose shared store is unreachable. Every exit
// drives lifecycle.Shutdown, the production sequence: the shutdown marker
// precedes the cancellation and C14 precedes the loop drain, so the window
// between shutdown and the drained loops is observable (§4.6 优雅关闭).
package acceptance

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/coordination"
	"github.com/tsic404/foreman/internal/lifecycle"
)

// TestTechMulti13SingleInstanceDeregistersEveryIdentity covers the
// single-instance form on a two-workspace deployment: both identities have a
// server-side row and an in-flight Job, the claim loops stop as shutdown
// starts, and the exit sends one C14 per identity carrying its whole runtime
// set — so both rows are offline when the process is gone.
func TestTechMulti13SingleInstanceDeregistersEveryIdentity(t *testing.T) {
	m := newMultiWSShared(t, "ws-a", "ws-b")
	logs := &syncBuffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))

	// The expected sets are bound here, not read back from the client the exit
	// uses: a C14 that drops one runtime must fail both the set check and the
	// row check for the id it dropped.
	runtimes := map[string][]string{
		"ws-a": {"rid-omp-ws-a", "rid-zz-ws-a"},
		"ws-b": {"rid-omp-ws-b", "rid-zz-ws-b"},
	}
	for _, ws := range []string{"ws-a", "ws-b"} {
		m.upstream[ws].setRuntimeID("omp", "", runtimes[ws][0])
		m.upstream[ws].setRuntimeID("zz", "", runtimes[ws][1])
		if err := m.clients[ws].RegisterWithDiscovery(t.Context(), declaredRuntimes()); err != nil {
			t.Fatalf("C-1 for %s: %v", ws, err)
		}
	}

	// Each identity has one in-flight task with its Job, driven by the
	// production claim loop rather than a direct scheduler call.
	m.upstream["ws-a"].enqueueTaskFor("task-a1", "ws-a")
	m.upstream["ws-b"].enqueueTaskFor("task-b1", "ws-b")
	rounds := map[string]*claimRoundClock{"ws-a": {}, "ws-b": {}}
	cancelA, waitA := m.startClaimLoopDetachedFor("ws-a", rounds["ws-a"])
	cancelB, waitB := m.startClaimLoopDetachedFor("ws-b", rounds["ws-b"])
	awaitCondition(t, "both workspaces dispatched their in-flight task", func() bool {
		return m.jobs.jobCreated("fm-task-a1") && m.jobs.jobCreated("fm-task-b1")
	})

	leader := coordination.New(m.coord, m.instanceID, coordination.Config{
		LeaseTTL:       time.Second,
		TakeoverWindow: 50 * time.Millisecond,
		InstanceTTL:    time.Minute,
		TaskLockTTL:    time.Second,
	})
	if err := m.coord.HeartbeatInstance(t.Context(), m.instanceID, time.Minute); err != nil {
		t.Fatalf("HeartbeatInstance: %v", err)
	}

	// The exit goes through the production sequence: the marker is taken
	// before the loops are cancelled, and C14 is sent before the drain. The
	// cancel hook stamps its own instant so the ordering is judged, not assumed.
	var loopsDrainedAt time.Time
	var cancelAt time.Time
	started := lifecycle.Shutdown(t.Context(), lifecycle.ShutdownOptions{
		Cancel: func() { cancelAt = time.Now(); cancelA(); cancelB() },
		Gate:   leader,
		Served: []lifecycle.Deregisterer{m.clients["ws-a"], m.clients["ws-b"]},
		Wait: func(context.Context) {
			waitA()
			waitB()
			loopsDrainedAt = time.Now()
		},
		Log: log,
	})
	// Same-tick equality is legal; only a cancel before the marker is inversion.
	if cancelAt.Before(started) {
		t.Fatalf("loops were cancelled %v before the shutdown marker %v: exit order inverted", cancelAt, started)
	}

	// One C14 per identity, carrying that identity's full real runtime set.
	for _, ws := range []string{"ws-a", "ws-b"} {
		stub := m.upstream[ws]
		deregisters := stub.calls("POST", "/api/daemon/deregister")
		if len(deregisters) != 1 {
			t.Fatalf("%s deregistration requests = %d, want exactly one", ws, len(deregisters))
		}
		c14 := deregisters[0]
		if c14.At.After(loopsDrainedAt) {
			t.Fatalf("%s C14 arrived after the loops drained: the exit waited for the drain", ws)
		}
		var body struct {
			RuntimeIDs []string `json:"runtime_ids"`
		}
		if err := json.Unmarshal(c14.Body, &body); err != nil {
			t.Fatalf("decode %s deregistration body: %v", ws, err)
		}
		if !sameStringSet(body.RuntimeIDs, runtimes[ws]) {
			t.Fatalf("%s deregistration carried %v, want the full identity set %v",
				ws, body.RuntimeIDs, runtimes[ws])
		}
		// Process exit complete: every row of the identity is offline.
		for _, id := range runtimes[ws] {
			if got := stub.rowStatus(id); got != "offline" {
				t.Fatalf("%s row %s after exit = %q, want offline", ws, id, got)
			}
		}
		// No claim round started after the marker, judged where the round
		// starts rather than where a request lands: a claim already in flight
		// at the cancellation is recorded by the server after the marker
		// without being a shutdown leak, so its arrival is context only.
		last, count := rounds[ws].window()
		if count == 0 || last.After(started) {
			t.Fatalf("%s claim rounds = %d, last started %v vs shutdown marker %v (last claim arrived %v)",
				ws, count, last, started, lastClaimArrival(stub))
		}
	}

	// The in-flight Jobs were left to run: no delete reached the cluster.
	for _, name := range []string{"fm-task-a1", "fm-task-b1"} {
		if n := m.jobs.deletedCount(name); n != 0 {
			t.Fatalf("%s deleted by shutdown %d time(s), want left in flight", name, n)
		}
		if !m.jobs.jobCreated(name) {
			t.Fatalf("%s disappeared during shutdown", name)
		}
	}
	if strings.Contains(logs.String(), "daemon.deregister_skipped") {
		t.Fatalf("the last instance skipped deregistration: %s", logs.String())
	}
}

// TestTechMulti13SurvivorKeepsTheIdentity covers the two-instance form twice:
// the follower shuts down while the leader lives, and the leader shuts down
// while the follower lives. Both exits send zero C14; the survivor keeps
// heartbeating and claiming under the same identity, and the rows stay online.
func TestTechMulti13SurvivorKeepsTheIdentity(t *testing.T) {
	t.Run("follower exits", func(t *testing.T) {
		tw := newTwoInstances(t, twoInstanceOptions{leaderCfg: smallLeaderCfg(time.Second)})
		tw.declareAll(t)
		tw.a.startLeader(t)
		tw.b.runLeader(t)
		assertTwoInstanceRows(t, tw)
		stopA := startHeartbeatLoop(t, tw.a)
		cancelHeartbeatB, waitHeartbeatB := startHeartbeatLoopDetached(t, tw.b)
		stopClaimA := startConcurrency01ClaimLoop(t, tw.a)
		cancelClaimB, waitClaimB := startConcurrency01ClaimLoopDetached(t, tw.b)
		defer stopA()
		defer stopClaimA()

		tw.a.upstream.enqueueTask("task-before")
		tw.waitForTaskJob(t, "task-before", 10*time.Second)

		exiting := tw.b.currentLeader()
		logs := &syncBuffer{}
		var waitLeader func()
		lifecycle.Shutdown(t.Context(), lifecycle.ShutdownOptions{
			Cancel: func() {
				cancelClaimB()
				cancelHeartbeatB()
				waitLeader = tw.b.cancelLeader()
			},
			Gate:   exiting,
			Served: []lifecycle.Deregisterer{tw.b.client},
			Wait: func(context.Context) {
				waitClaimB()
				waitHeartbeatB()
				waitLeader()
			},
			Log: slog.New(slog.NewJSONHandler(logs, nil)),
		})

		assertZeroC14(t, tw)
		assertRowsOnline(t, tw)
		// The survivor is still the holder: its C2 stream was never stopped,
		// so the next beat is one full cadence (15s) away.
		assertSurvivorContinues(t, tw, 16*time.Second)
		if !strings.Contains(logs.String(), "another instance is alive") {
			t.Fatalf("the follower's exit did not name the surviving instance: %s", logs.String())
		}
		if n := tw.a.jobs.deletedCount("fm-task-before"); n != 0 {
			t.Fatalf("in-flight Job deleted by the follower's exit %d time(s)", n)
		}
	})

	t.Run("leader exits", func(t *testing.T) {
		leaseTTL := time.Second
		tw := newTwoInstances(t, twoInstanceOptions{leaderCfg: smallLeaderCfg(leaseTTL)})
		tw.declareAll(t)
		tw.a.startLeader(t)
		tw.b.runLeader(t)
		assertTwoInstanceRows(t, tw)
		cancelHeartbeatA, waitHeartbeatA := startHeartbeatLoopDetached(t, tw.a)
		stopB := startHeartbeatLoop(t, tw.b)
		cancelClaimA, waitClaimA := startConcurrency01ClaimLoopDetached(t, tw.a)
		stopClaimB := startConcurrency01ClaimLoop(t, tw.b)
		defer stopB()
		defer stopClaimB()

		tw.a.upstream.enqueueTask("task-before")
		tw.waitForTaskJob(t, "task-before", 10*time.Second)
		registersBefore := len(tw.a.upstream.calls("POST", "/api/daemon/register"))

		exiting := tw.a.currentLeader()
		logs := &syncBuffer{}
		var waitLeader func()
		lifecycle.Shutdown(t.Context(), lifecycle.ShutdownOptions{
			Cancel: func() {
				cancelClaimA()
				cancelHeartbeatA()
				waitLeader = tw.a.cancelLeader()
			},
			Gate:   exiting,
			Served: []lifecycle.Deregisterer{tw.a.client},
			Wait: func(context.Context) {
				waitClaimA()
				waitHeartbeatA()
				waitLeader()
			},
			Log: slog.New(slog.NewJSONHandler(logs, nil)),
		})

		assertZeroC14(t, tw)
		assertRowsOnline(t, tw)
		if !strings.Contains(logs.String(), "another instance is alive") {
			t.Fatalf("the leader's exit did not name the surviving instance: %s", logs.String())
		}

		// The survivor takes the lease over and resumes both streams under the
		// same registration identity: no new C-1, same runtime ids.
		awaitCondition(t, "the survivor acquired the lease", tw.b.leaderHeld)
		assertSurvivorContinues(t, tw, 5*time.Second)
		if got := len(tw.a.upstream.calls("POST", "/api/daemon/register")); got != registersBefore {
			t.Fatalf("the survivor re-registered after the leader's exit: C-1 %d -> %d", registersBefore, got)
		}
		if n := tw.a.jobs.deletedCount("fm-task-before"); n != 0 {
			t.Fatalf("in-flight Job deleted by the leader's exit %d time(s)", n)
		}
	})
}

// TestTechMulti13UnreachableSharedStateSendsNothing covers the third form: the
// last instance's shared store fails, so the exit cannot prove the instance
// count. Zero C14 is sent and the rows stay online; convergence is left to the
// server's own no-heartbeat expiry (not modelled by the stub, which is exactly
// the point — Foreman added no offline transition of its own).
func TestTechMulti13UnreachableSharedStateSendsNothing(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{leaderCfg: smallLeaderCfg(time.Second)})
	tw.declareAll(t)
	tw.a.startLeader(t)
	// Only this instance is alive; another row would make the verdict false
	// for a reason other than the store.
	if err := tw.coord.HeartbeatInstance(t.Context(), "instance-a", time.Minute); err != nil {
		t.Fatalf("HeartbeatInstance: %v", err)
	}
	if ids, err := tw.coord.ListInstances(t.Context()); err != nil || len(ids) != 1 {
		t.Fatalf("instance rows before the outage = %v (err %v), want only instance-a", ids, err)
	}

	tw.mr.SetError("shared state outage")
	logs := &syncBuffer{}
	lifecycle.Shutdown(t.Context(), lifecycle.ShutdownOptions{
		Gate:   tw.a.currentLeader(),
		Served: []lifecycle.Deregisterer{tw.a.client},
		Log:    slog.New(slog.NewJSONHandler(logs, nil)),
	})

	if tw.coord.Available() {
		t.Fatal("the store failure was not observed by the shutdown verdict")
	}
	if !strings.Contains(logs.String(), "shared state unavailable") {
		t.Fatalf("the exit did not name the unreachable store: %s", logs.String())
	}
	assertZeroC14(t, tw)
	assertRowsOnline(t, tw)
}

// assertTwoInstanceRows pins the fixture precondition: both liveness rows
// exist, so a last-instance verdict is genuinely false.
func assertTwoInstanceRows(t *testing.T, tw *twoInstances) {
	t.Helper()
	for _, id := range []string{"instance-a", "instance-b"} {
		if err := tw.coord.HeartbeatInstance(t.Context(), id, time.Minute); err != nil {
			t.Fatalf("HeartbeatInstance(%s): %v", id, err)
		}
	}
	ids, err := tw.coord.ListInstances(t.Context())
	if err != nil || len(ids) != 2 {
		t.Fatalf("instance rows = %v (err %v), want both instances", ids, err)
	}
}

// assertZeroC14 pins the narrowing verdict on the wire.
func assertZeroC14(t *testing.T, tw *twoInstances) {
	t.Helper()
	if got := len(tw.a.upstream.calls("POST", "/api/daemon/deregister")); got != 0 {
		t.Fatalf("deregistration requests = %d, want zero", got)
	}
}

// assertRowsOnline pins that the exit moved no server-side row. On this stub
// it is a corollary of assertZeroC14, not an independent judgment: a row only
// changes on C-1 (online) and C-14 (offline). It stays as the wire-visible
// consequence a reader checks directly.
func assertRowsOnline(t *testing.T, tw *twoInstances) {
	t.Helper()
	for _, id := range []string{"rid-omp", "rid-zz"} {
		if got := tw.a.upstream.rowStatus(id); got != "online" {
			t.Fatalf("row %s after the exit = %q, want online", id, got)
		}
	}
}

// assertSurvivorContinues waits for the surviving instance's heartbeat stream
// to advance and for a fresh claim to complete, under the unchanged identity.
// The heartbeat baseline is sampled at call time; a stream that was never
// stopped beats again only after the contract's 15s cadence, so the caller
// chooses a window that covers it.
func assertSurvivorContinues(t *testing.T, tw *twoInstances, heartbeatWindow time.Duration) {
	t.Helper()
	heartbeatsBefore := tw.a.upstream.heartbeatRuntimeIDs()["rid-omp"]
	awaitConditionWithin(t, heartbeatWindow, "the survivor's heartbeats to advance", func() bool {
		return tw.a.upstream.heartbeatRuntimeIDs()["rid-omp"] > heartbeatsBefore
	})
	ids := tw.a.upstream.heartbeatRuntimeIDs()
	for id := range ids {
		if id != "rid-omp" && id != "rid-zz" {
			t.Fatalf("the survivor heartbeats under a new runtime id %s", id)
		}
	}
	tw.a.upstream.enqueueTask("task-after")
	tw.waitForTaskJob(t, "task-after", 10*time.Second)
}

// awaitConditionWithin is awaitCondition with the scenario's own window: the
// C2 cadence (15s) is the one wait in this case that outlives the shared 5s.
func awaitConditionWithin(t *testing.T, window time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// lastClaimArrival reports when the newest claim request reached the stub; it
// only annotates a window failure, since an in-flight claim may land late.
func lastClaimArrival(stub *upstreamStub) time.Time {
	claims := stub.calls("POST", "/api/daemon/tasks/claim")
	if len(claims) == 0 {
		return time.Time{}
	}
	return claims[len(claims)-1].At
}

// sameStringSet compares two id lists as sets (the runtime set is a set; C14
// order is not part of the contract).
func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, s := range a {
		seen[s]++
	}
	for _, s := range b {
		seen[s]--
		if seen[s] < 0 {
			return false
		}
	}
	return true
}
