// Assembly-level evidence for the stage-two acceptance criteria (AC-74…AC-78).
//
// Unlike the TC-tech-multi-0x cases, these drive the production composition:
// the startup path is bootstrap.StartupDiscoveries, the control loops are
// built by proxy.NewControlPlane with the same leader gate and declaration
// publisher cmd/foreman wires. They cover the three assembly faces the review
// asked for:
//   - two replicas start (leader probe → declaration publish → follower
//     adopt, no second probe and no re-registration);
//   - a non-holder makes zero upstream control-plane calls (C1/C2/C3/C20);
//   - after a shared-state data loss (no process restart) the two-level
//     counters recover from the rebuilt rows, including a row-less
//     workspace's stale reservation.
package acceptance

import (
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/bootstrap"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/state"
)

// assemblyLogger renders one harness's logs to its own buffer.
func assemblyLogger(h *harness) *slog.Logger {
	return slog.New(slog.NewJSONHandler(h.logs, nil))
}

// waitDeclaration blocks until every workspace's registration identity is
// published through the production loader.
func waitDeclaration(t *testing.T, coord state.CoordStore, clients []*proxy.Client) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, missing, err := bootstrap.LoadDeclarations(context.Background(), coord, clients); err == nil && len(missing) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the control-plane holder never published the registration declaration")
}

// upstreamControlCounts snapshots the shared stub's control-plane counters.
type upstreamControlCounts struct {
	c1        int
	heartbeat map[string]int
	c3        int
	ws        int
}

func controlCounts(stub *upstreamStub) upstreamControlCounts {
	return upstreamControlCounts{
		c1:        len(stub.calls("POST", "/api/daemon/register")),
		heartbeat: stub.heartbeatRuntimeIDs(),
		c3:        len(stub.calls("POST", "/api/daemon/tasks/claim")),
		ws:        stub.wsDialCount(),
	}
}

func TestMultiAssemblyTwoReplicaStartup(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{
		wsEnabled: true,
		leaderCfg: smallLeaderCfg(800 * time.Millisecond),
	})
	ctx := t.Context()

	// Replica A is the holder; replica B runs its lease loop as a follower.
	tw.a.autoRegisterStartupProbe(t, "ws-1")
	tw.a.startLeader(t)
	tw.b.runLeader(t)

	// A's startup: the startup probe runs (holder), the discovery is declared
	// and the claim loop's C1 publishes the registration identity.
	discA, err := bootstrap.StartupDiscoveries(ctx, tw.a.currentLeader(), tw.a.coord, tw.a.sched, tw.a.clientList(), 3*time.Second, assemblyLogger(tw.a))
	if err != nil {
		t.Fatalf("A StartupDiscoveries: %v", err)
	}
	if len(discA) != 1 {
		t.Fatalf("A discoveries = %v, want one workspace", discA)
	}
	if len(tw.a.client.RuntimeIDs()) == 0 {
		tw.a.client.Declare(discA["ws-1"])
	}
	stopA := tw.a.startAssemblyControlPlane(t, tw.a.leaderHeld, func(ctx context.Context, cl *proxy.Client) error {
		return bootstrap.PublishDeclaration(ctx, tw.a.coord, "ws-1", cl)
	})
	waitDeclaration(t, tw.a.coord, tw.a.clientList())
	if n := len(tw.a.jobs.probeJobsCreated()); n != 1 {
		t.Fatalf("probe Jobs = %d, want exactly one", n)
	}
	// Park A's loops so the follower's own upstream face can be measured.
	stopA()

	// B's startup: a follower adopts the published declaration — no probe, no
	// C1 — and carries the same real runtime ids.
	discB, err := bootstrap.StartupDiscoveries(ctx, tw.b.currentLeader(), tw.b.coord, tw.b.sched, tw.b.clientList(), 3*time.Second, assemblyLogger(tw.b))
	if err != nil {
		t.Fatalf("B StartupDiscoveries: %v", err)
	}
	if len(discB) != 1 {
		t.Fatalf("B discoveries = %v, want one workspace", discB)
	}
	if n := len(tw.a.jobs.probeJobsCreated()); n != 1 {
		t.Fatalf("probe Jobs after B's startup = %d, want A's single probe", n)
	}
	idsA := append([]string(nil), tw.a.client.RuntimeIDs()...)
	idsB := append([]string(nil), tw.b.client.RuntimeIDs()...)
	sort.Strings(idsA)
	sort.Strings(idsB)
	if !reflect.DeepEqual(idsA, idsB) {
		t.Fatalf("runtime ids A=%v B=%v, want the adopted identity set", idsA, idsB)
	}

	// Non-holder zero: B's full loop set runs gated, so no upstream control
	// call moves (C1, C2 heartbeat, C3 claim, C20 subscription).
	before := controlCounts(tw.a.upstream)
	stopB := tw.b.startAssemblyControlPlane(t, tw.b.leaderHeld, nil)
	time.Sleep(1200 * time.Millisecond)
	after := controlCounts(tw.a.upstream)
	if after.c1 != before.c1 {
		t.Fatalf("follower C1 calls: %d -> %d, want 0", before.c1, after.c1)
	}
	if !reflect.DeepEqual(after.heartbeat, before.heartbeat) {
		t.Fatalf("follower heartbeats: %v -> %v, want none", before.heartbeat, after.heartbeat)
	}
	if after.c3 != before.c3 {
		t.Fatalf("follower claims: %d -> %d, want 0", before.c3, after.c3)
	}
	if after.ws != before.ws {
		t.Fatalf("follower WS dials: %d -> %d, want 0", before.ws, after.ws)
	}

	// §1.1: prepare-lease renewal is NOT a holder-only action — the follower
	// still forwards it. A leaseable row on B must produce a C-4 call while B
	// is a follower.
	c4Before := tw.a.upstream.prepareLeaseCalls()
	if err := tw.b.reg.Put(registry.TaskEntry{
		TaskID:        "task-c4",
		WorkspaceID:   "ws-1",
		JobName:       "fm-task-c4",
		DaemonID:      "fm-task-c4",
		JobRuntimeID:  "rt-c4",
		RealRuntimeID: "rid-zz",
		State:         registry.StateDaemonRegistered,
		ClaimedAt:     time.Now(),
		Payload:       json.RawMessage(`{"id":"task-c4"}`),
	}); err != nil {
		t.Fatalf("seed leaseable row: %v", err)
	}
	c4Deadline := time.Now().Add(5 * time.Second)
	for tw.a.upstream.prepareLeaseCalls() == c4Before {
		if time.Now().After(c4Deadline) {
			t.Fatal("the follower never renewed a prepare lease (C-4 must not be leader-gated)")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Takeover: A dies; B acquires and resumes the same control plane with the
	// same registration identity (no C1) and claims a queued task.
	tw.a.crashLeader(t)
	tw.a.upstream.enqueueTask("task-takeover")
	deadline := time.Now().Add(10 * time.Second)
	for !tw.b.leaderHeld() {
		if time.Now().After(deadline) {
			t.Fatal("the follower never acquired the lease")
		}
		time.Sleep(10 * time.Millisecond)
	}
	tw.waitForTaskJob(t, "task-takeover", 10*time.Second)
	if got := len(tw.a.upstream.calls("POST", "/api/daemon/register")); got != before.c1 {
		t.Fatalf("C1 calls after takeover = %d, want the pre-kill %d (no re-registration)", got, before.c1)
	}
	deadline = time.Now().Add(5 * time.Second)
	for !heartbeatAdvanced(tw.a.upstream, before.heartbeat) {
		if time.Now().After(deadline) {
			t.Fatal("the survivor's heartbeat streams never resumed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ids := tw.a.upstream.heartbeatRuntimeIDs(); !sameKeys(ids, before.heartbeat) {
		t.Fatalf("heartbeat identities changed: %v -> %v", before.heartbeat, ids)
	}
	stopB()
}

// heartbeatAdvanced reports whether any heartbeat count grew.
func heartbeatAdvanced(stub *upstreamStub, before map[string]int) bool {
	for id, n := range stub.heartbeatRuntimeIDs() {
		if n > before[id] {
			return true
		}
	}
	return false
}

// sameKeys reports whether both maps share the same key set.
func sameKeys(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func TestMultiAssemblyDataLossSlotRecovery(t *testing.T) {
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
	if ok, err := tw.coord.ReserveSlot(ctx, "ws-empty", 10); err != nil || !ok {
		t.Fatalf("stray workspace reservation: ok=%v err=%v", ok, err)
	}
	if ok, err := tw.coord.ReserveSlot(ctx, "", 10); err != nil || !ok {
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

// TestMultiAssemblySessionLearningAcrossInstances covers the shared
// soft-affinity face (§4.1/§4.5): the node observed on the claiming instance
// is in the shared row, so a terminal report that lands on the other instance
// still learns the session → node pair, and both instances read it back.
func TestMultiAssemblySessionLearningAcrossInstances(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{leaderCfg: smallLeaderCfg(time.Second)})
	tw.declareAll(t)
	tw.a.startLeader(t)
	tw.a.upstream.enqueueTask("task-sess")
	tw.startClaims(t)
	tw.waitLiveJob(t, "task-sess", 10*time.Second)

	// The pod watcher records the node on the claiming instance; the shared
	// row must carry it before the report reaches the other instance.
	tw.a.reg.SetNode("task-sess", "node-a")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if e, ok := tw.entry(t, "task-sess"); ok && e.NodeName == "node-a" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node_name never reached the shared row")
		}
		time.Sleep(5 * time.Millisecond)
	}

	token := tw.token(t, tw.a, "task-sess")
	tw.registerVia(t, tw.b, "task-sess", token)
	if got := tw.claimVia(t, tw.b, "task-sess", token); len(got) != 1 {
		t.Fatalf("delivery = %d tasks, want 1", len(got))
	}
	tw.startVia(t, tw.b, "task-sess", token)

	// The terminal report — carrying the session id — lands on B.
	if code, body := tw.b.call("POST", "/api/daemon/tasks/task-sess/complete", token,
		[]byte(`{"session_id":"/tmp/sessions/sess-abc.jsonl"}`)); code != 200 {
		t.Fatalf("S13 through B = %d (%s)", code, body)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		if tw.a.reg.LastNodeForSession("ws-1", "sess-abc") == "node-a" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the session index was not learned from the cross-instance report")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := tw.b.reg.LastNodeForSession("ws-1", "sess-abc"); got != "node-a" {
		t.Fatalf("B's session lookup = %q, want node-a", got)
	}
}

// TestMultiAssemblySessionIndexSharedAndExpired covers the shared index's
// lifetime face: a row written by one instance is readable on the other, and
// its retention is enforced by the store's own TTL — the server clock — so no
// instance deletes a row by comparing its clock with the writer's stamp.
func TestMultiAssemblySessionIndexSharedAndExpired(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{})
	ctx := t.Context()
	raw, err := json.Marshal(map[string]any{
		"workspace_id": "ws-1",
		"key":          "sess-old",
		"node":         "node-a",
		"learned_at":   time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal session row: %v", err)
	}
	if err := tw.a.coord.PutSessionNode(ctx, "ws-1", "sess-old", raw, time.Hour); err != nil {
		t.Fatalf("PutSessionNode: %v", err)
	}
	if got := tw.b.reg.LastNodeForSession("ws-1", "sess-old"); got != "node-a" {
		t.Fatalf("cross-instance session lookup = %q, want node-a", got)
	}
	tw.mr.FastForward(time.Hour + time.Second)
	if _, ok, err := tw.a.coord.GetSessionNode(ctx, "ws-1", "sess-old"); err != nil || ok {
		t.Fatalf("shared session row survived its TTL (ok=%v err=%v)", ok, err)
	}
	if got := tw.a.reg.LastNodeForSession("ws-1", "sess-old"); got != "" {
		t.Fatalf("lookup after the TTL = %q, want a miss", got)
	}
}

// TestMultiAssemblySessionIndexSurvivesWriterClockSkew pins the other half of
// the lifetime face: a row whose writer's learned_at is 5h old but whose store
// TTL is 3h is not expired by the reconciling instance's prune. The writer's
// stamp never decides a row's lifetime; reading the shared side (not B's
// cache, whose hit would hide a deleted row) proves the row survived.
func TestMultiAssemblySessionIndexSurvivesWriterClockSkew(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{})
	ctx := t.Context()
	raw, err := json.Marshal(map[string]any{
		"workspace_id": "ws-1",
		"key":          "sess-skew",
		"node":         "node-a",
		"learned_at":   time.Now().Add(-5 * time.Hour),
	})
	if err != nil {
		t.Fatalf("marshal session row: %v", err)
	}
	if err := tw.a.coord.PutSessionNode(ctx, "ws-1", "sess-skew", raw, 3*time.Hour); err != nil {
		t.Fatalf("PutSessionNode: %v", err)
	}
	if got := tw.b.reg.LastNodeForSession("ws-1", "sess-skew"); got != "node-a" {
		t.Fatalf("cross-instance session lookup = %q, want node-a", got)
	}
	if dropped := tw.b.reg.PruneExpiredSessions(3 * time.Hour); dropped != 0 {
		t.Fatalf("prune dropped %d entries: a slow writer's stamp expired a live row", dropped)
	}
	if _, ok, err := tw.a.coord.GetSessionNode(ctx, "ws-1", "sess-skew"); err != nil || !ok {
		t.Fatalf("shared row gone after prune (ok=%v err=%v)", ok, err)
	}
}

// TestMultiAssemblyProbeSessionUpdateFailsClosed covers the shared probe
// session's write face: a store failure must report the update as unapplied
// (not silently land it in the local cache), so a probe never appears
// registered while its session is not shared (§6.3, §10 共享状态不可用).
func TestMultiAssemblyProbeSessionUpdateFailsClosed(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{})
	if err := tw.a.probes.PutProbe(registry.ProbeSession{
		JobName:    "probe-1",
		JobRuntime: "rt-probe-1",
	}); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}
	tw.mr.SetError("outage")
	defer tw.mr.SetError("")

	updated, ok := tw.a.probes.UpdateProbe("probe-1", func(s *registry.ProbeSession) {
		s.NodeName = "node-a"
	})
	if ok {
		t.Fatal("an unpersisted probe session update was reported as applied")
	}
	if updated.JobName != "" {
		t.Fatalf("failed update returned a session: %+v", updated)
	}
}

// TestMultiAssemblyEmptyWorkspaceDoesNotClobberGlobalCounter pins the level
// separation (§10 两级在途上限): a live row whose workspace is empty (the
// malformed-claim shape) counts in the global total only. It must never be
// treated as a workspace level — doing so would overwrite the global counter
// with that single row and disable the cluster cap. The periodic repair raises
// the global counter to the live-row floor, so the row is counted once, as the
// third live row, and the workspace level stays at its own two.
func TestMultiAssemblyEmptyWorkspaceDoesNotClobberGlobalCounter(t *testing.T) {
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
