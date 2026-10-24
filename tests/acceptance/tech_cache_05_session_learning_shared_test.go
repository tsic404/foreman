// TC-tech-cache-05 / AC-25 (multi-instance variant): the session → node pair
// learned from a terminal report lands in the shared index, so a report that
// arrives at the other instance still learns it and both instances read the
// same pair back (§4.5 索引跨实例可见).
package acceptance

import (
	"testing"
	"time"
)

func TestTechCache05SessionLearningAcrossInstances(t *testing.T) {
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
