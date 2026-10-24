// TC-tech-multi-14 / AC-77, AC-87: two replicas start through the
// production composition. The startup probe runs once, on the holder; the
// follower adopts the published declaration unchanged and emits no
// registration, heartbeat, subscription or claim, while prepare-lease renewal
// keeps flowing. After the holder dies the survivor continues under the same
// declared identity — no re-registration, unchanged heartbeat identities, and
// the queued task is claimed once the takeover window passes.
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

func TestTechMulti14TwoReplicaStartup(t *testing.T) {
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
