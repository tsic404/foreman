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
	if rt.Busy != 0 {
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
