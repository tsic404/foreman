// TC-tech-forbidden-10 (AC-14, F11): the authoritative state is not the
// process. Two instances read the same row and the same token verdict from the
// shared state — an instance that never saw the claim reaches the identical
// conclusion after restoring the shared rows, and a revocation written by one
// instance rejects the token on the other. The control plane is single-holder:
// a follower issues no control-plane request, claim included. The adjudication
// source is partitioned by workspace: one workspace's entries and counters are
// not read through another's key (§0, §4.1, §10, ADR-046 决策 2).
package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// forbidden10Payload is a claim payload naming the claiming identity's
// workspace; the scheduler refuses a payload that names another one.
func forbidden10Payload(taskID, workspaceID string) json.RawMessage {
	return json.RawMessage(`{
		"id": "` + taskID + `", "agent_id": "agent-1", "workspace_id": "` + workspaceID + `",
		"runtime_id": "rid-zz", "agent": {"model": "k3"}
	}`)
}

func TestForbidden10AuthorityStateIsSharedNotInProcess(t *testing.T) {
	ctx := context.Background()
	tw := newTwoInstances(t, twoInstanceOptions{
		workspaces:       []string{"ws-1", "ws-2"},
		maxInflightPerWS: 10,
	})
	tw.declareAll(t)
	tw.a.startLeader(t)

	// The holder claims one task per workspace. The rows land in the shared
	// state; B has processed none of them.
	for _, tc := range []struct{ workspaceID, taskID string }{
		{"ws-1", "task-a"},
		{"ws-2", "task-b"},
	} {
		if err := tw.a.sched.OnClaimFor(ctx, tc.workspaceID, forbidden10Payload(tc.taskID, tc.workspaceID)); err != nil {
			t.Fatalf("OnClaimFor(%s, %s): %v", tc.workspaceID, tc.taskID, err)
		}
		if _, ok := tw.b.reg.Get(tc.taskID); ok {
			t.Fatalf("B already held %s before restoring the shared rows", tc.taskID)
		}
	}

	// B restores from the shared state and reaches the same conclusion: the
	// row is the authority, not the process that wrote it.
	if _, _, err := tw.b.reg.Restore(ctx); err != nil {
		t.Fatalf("B Restore: %v", err)
	}
	for _, taskID := range []string{"task-a", "task-b"} {
		a, okA := tw.a.reg.Get(taskID)
		b, okB := tw.b.reg.Get(taskID)
		if !okA || !okB {
			t.Fatalf("row %s missing (a=%v b=%v)", taskID, okA, okB)
		}
		pa, pb := multiSharedProjection(t, a), multiSharedProjection(t, b)
		if len(pa) != len(pb) {
			t.Fatalf("row %s projection widths differ: %d vs %d", taskID, len(pa), len(pb))
		}
		for key, want := range pa {
			if got := fmt.Sprint(pb[key]); got != fmt.Sprint(want) {
				t.Errorf("row %s field %s = %s on B, want %s on A", taskID, key, got, want)
			}
		}
	}

	// The same token verdict on both instances, then a revocation written by A
	// rejects it on B: the shared revocation set is the adjudication source.
	token := tw.token(t, tw.a, "task-a")
	tw.registerVia(t, tw.b, "task-a", token)
	tw.a.upstream.setTaskStatus("task-a", "running")
	if code := tw.statusVia(t, tw.b, "task-a", token); code != http.StatusOK {
		t.Fatalf("B status with A's token = %d, want 200", code)
	}
	if code := tw.statusVia(t, tw.a, "task-a", token); code != http.StatusOK {
		t.Fatalf("A status with its own token = %d, want 200", code)
	}
	tw.a.issuer.Revoke(tw.a.issuer.Hash(token), "ws-1")
	if code := tw.statusVia(t, tw.b, "task-a", token); code != http.StatusUnauthorized {
		t.Fatalf("B status after A's revocation = %d, want 401", code)
	}

	// Workspace partition: the shared rows and the two-level counters are read
	// through the workspace key, never through another workspace's.
	if _, ok := tw.b.reg.GetInWorkspace("ws-1", "task-b"); ok {
		t.Error("ws-2's row is readable through the ws-1 key")
	}
	if _, ok := tw.b.reg.GetInWorkspace("ws-2", "task-b"); !ok {
		t.Error("ws-2's row is not readable through its own key")
	}
	if _, ok := tw.b.reg.GetInWorkspace("ws-2", "task-a"); ok {
		t.Error("ws-1's row is readable through the ws-2 key")
	}
	if got, err := tw.coord.Slots(ctx, "ws-1"); err != nil || got != 1 {
		t.Errorf("ws-1 slots = %d (err %v), want 1", got, err)
	}
	if got, err := tw.coord.Slots(ctx, "ws-2"); err != nil || got != 1 {
		t.Errorf("ws-2 slots = %d (err %v), want 1", got, err)
	}

	// The follower issues no control-plane request, claim included: with B's
	// control plane running behind a closed lease gate the C-1/C-2/C-3 counts
	// must not move, while the holder's claim still does.
	c1Before := len(tw.a.upstream.calls("POST", "/api/daemon/register"))
	c2Before := len(tw.a.upstream.calls("POST", "/api/daemon/heartbeat"))
	c3Before := len(tw.a.upstream.calls("POST", "/api/daemon/tasks/claim"))

	stopFollower := tw.b.startAssemblyControlPlane(t, func() bool { return false }, nil)
	time.Sleep(400 * time.Millisecond)
	stopFollower()
	if got := len(tw.a.upstream.calls("POST", "/api/daemon/register")); got != c1Before {
		t.Errorf("follower sent C-1: %d -> %d", c1Before, got)
	}
	if got := len(tw.a.upstream.calls("POST", "/api/daemon/heartbeat")); got != c2Before {
		t.Errorf("follower sent C-2: %d -> %d", c2Before, got)
	}
	if got := len(tw.a.upstream.calls("POST", "/api/daemon/tasks/claim")); got != c3Before {
		t.Errorf("follower sent C-3: %d -> %d", c3Before, got)
	}

	tw.a.upstream.enqueueTask("task-c")
	stopHolder := tw.a.startAssemblyControlPlane(t, tw.a.leaderHeld, nil)
	defer stopHolder()
	tw.awaitClaimedJob(t, "task-c")
	if got := len(tw.a.upstream.calls("POST", "/api/daemon/tasks/claim")); got <= c3Before {
		t.Errorf("holder claims = %d, want more than the %d before it started", got, c3Before)
	}
}
