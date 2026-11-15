package registry

import (
	"encoding/json"
	"testing"
	"time"
)

func testEntry(taskID, ws string) TaskEntry {
	return TaskEntry{
		TaskID: taskID, WorkspaceID: ws, DaemonID: "worker-" + taskID,
		RuntimeID: "runtime-" + taskID, State: StatePending,
	}
}

func TestLifecycleAndReclaim(t *testing.T) {
	r := New(WithClock(func() time.Time { return time.Unix(100, 0) }))
	if err := r.Put(testEntry("t1", "ws-1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Duplicate put refused.
	if err := r.Put(testEntry("t1", "ws-1")); err == nil {
		t.Fatal("duplicate put should fail")
	}
	// Deliver moves pending → delivering and sets the payload.
	payload, _ := json.Marshal(map[string]string{"id": "t1"})
	e, ok := r.MarkDelivering("worker-t1", r.Now(), payload)
	if !ok || e.State != StateDelivering {
		t.Fatalf("mark delivering: %+v ok=%v", e, ok)
	}
	// Start → running.
	e, ok = r.MarkStarted("t1", r.Now())
	if !ok || e.State != StateRunning {
		t.Fatalf("mark started: %+v ok=%v", e, ok)
	}
	// Terminal is immutable: a second terminal write reports first=false.
	e, first, err := r.MarkTerminal("t1", ResultCompleted, "", r.Now())
	if err != nil || !first || e.State != StateTerminal {
		t.Fatalf("mark terminal: %+v first=%v err=%v", e, first, err)
	}
	_, first, err = r.MarkTerminal("t1", ResultFailed, "", r.Now())
	if err != nil || first {
		t.Fatalf("second terminal write should report first=false, got first=%v err=%v", first, err)
	}
	if e.Result != ResultCompleted {
		t.Fatalf("terminal result should stay completed, got %s", e.Result)
	}
	// The daemon index dropped, so a cross-check finds no live entry.
	if _, ok := r.ByDaemonInWorkspace("ws-1", "worker-t1"); ok {
		t.Fatal("terminal entry should drop the daemon index")
	}
	// Reclaim keeps the done marker.
	if err := r.Delete("t1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !r.IsDone("ws-1", "t1") {
		t.Fatal("done marker should persist after reclaim")
	}
}

func TestDaemonAffinityAndBusy(t *testing.T) {
	r := New()
	r.PutRuntime(Runtime{
		RuntimeID: "d1", DaemonID: "d1", WorkspaceID: "ws-1",
		Online: true, RegisteredAt: r.Now(),
	})
	r.LearnSession("ws-1", "sess-x", "d1")
	rt, _ := r.RuntimeByID("d1")
	if !rt.HoldsSession("sess-x") {
		t.Fatal("daemon should hold learned session")
	}
	if r.BusyFor("d1") != 0 {
		t.Fatal("new daemon starts idle")
	}
}

func TestTerminalImmutableOnPut(t *testing.T) {
	r := New()
	if err := r.Put(testEntry("t1", "ws-1")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.MarkTerminal("t1", ResultFailed, "", r.Now()); err != nil {
		t.Fatal(err)
	}
	live := testEntry("t1", "ws-1")
	live.State = StatePending
	if err := r.Put(live); err == nil {
		t.Fatal("re-inserting a terminal task as live must be refused")
	}
}

func TestExpireStaleMarksSilentDaemonsOffline(t *testing.T) {
	r := New()
	now := time.Now()
	r.PutRuntime(Runtime{RuntimeID: "live", DaemonID: "d1", WorkspaceID: "ws", Online: true})
	r.PutRuntime(Runtime{RuntimeID: "dead", DaemonID: "d2", WorkspaceID: "ws", Online: true})
	r.TouchHeartbeat("live", now)
	r.TouchHeartbeat("dead", now.Add(-2*time.Minute))

	expired := r.ExpireStale(now, time.Minute)
	if len(expired) != 1 || expired[0] != "dead" {
		t.Fatalf("expired = %v, want [dead]", expired)
	}
	if rt, _ := r.RuntimeByID("live"); !rt.Online {
		t.Fatal("a daemon that just heartbeat must stay online")
	}
	if rt, _ := r.RuntimeByID("dead"); rt.Online {
		t.Fatal("a silent daemon must be marked offline")
	}
	// Second sweep is a no-op: the flag was already flipped.
	if again := r.ExpireStale(now, time.Minute); len(again) != 0 {
		t.Fatalf("second sweep expired %v, want none", again)
	}
}

func TestDaemonHoldsSeveralTasks(t *testing.T) {
	r := New()
	now := time.Now()
	r.PutRuntime(Runtime{RuntimeID: "d1", DaemonID: "d1", WorkspaceID: "ws", Online: true, RegisteredAt: now})
	for _, id := range []string{"t1", "t2", "t3"} {
		if err := r.Put(TaskEntry{
			TaskID: id, WorkspaceID: "ws", DaemonID: "d1", DaemonRuntimeID: "d1",
			State: StateDaemonRegistered, ClaimedAt: now, Payload: []byte(`{"id":"` + id + `"}`),
		}); err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
	}
	// Binding a second task must not hide the first: the old single-slot index
	// overwrote it, so a daemon with two tasks could only ever be handed one
	// and the other stranded until its lease lapsed.
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		e, ok := r.ByDaemonInWorkspace("ws", "d1")
		if !ok {
			t.Fatalf("iteration %d: daemon has no task", i)
		}
		if seen[e.TaskID] {
			t.Fatalf("iteration %d: %s handed twice", i, e.TaskID)
		}
		seen[e.TaskID] = true
		if _, ok := r.MarkDelivering("d1", now, e.Payload); !ok {
			t.Fatalf("iteration %d: mark delivering %s failed", i, e.TaskID)
		}
		if i == 0 && r.BusyFor("d1") != 3 {
			t.Fatalf("busy = %d, want 3", r.BusyFor("d1"))
		}
	}
	if len(seen) != 3 {
		t.Fatalf("saw %d tasks, want 3", len(seen))
	}
}
