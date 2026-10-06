package registry

import (
	"errors"
	"testing"
	"time"
)

var testNow = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

func fixedClock() func() time.Time { return func() time.Time { return testNow } }

func entryFor(taskID string) TaskEntry {
	return TaskEntry{
		TaskID:       taskID,
		IssueID:      "issue-" + taskID,
		JobName:      "fm-" + taskID,
		DaemonID:     "fm-" + taskID,
		JobRuntimeID: "rt-" + taskID,
		State:        StatePending,
	}
}

func TestPutGetAndSecondaryIndexes(t *testing.T) {
	r := New(fixedClock())
	e := entryFor("t1")
	if err := r.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got, ok := r.Get("t1"); !ok || got.JobName != "fm-t1" {
		t.Fatalf("Get = %v, %v", got, ok)
	}
	if got, ok := r.ByJob("fm-t1"); !ok || got.TaskID != "t1" {
		t.Fatalf("ByJob = %v, %v", got, ok)
	}
	if got, ok := r.ByDaemon("fm-t1"); !ok || got.TaskID != "t1" {
		t.Fatalf("ByDaemon = %v, %v", got, ok)
	}
	if _, ok := r.ByJob("nope"); ok {
		t.Fatal("ByJob hit for unknown job")
	}
	if err := r.Put(TaskEntry{State: StatePending}); err == nil {
		t.Fatal("Put without task_id accepted")
	}
}

func TestPutUpdateRefreshesIndexes(t *testing.T) {
	r := New(fixedClock())
	e := entryFor("t1")
	_ = r.Put(e)
	e.State = StateRunning
	if err := r.Put(e); err != nil {
		t.Fatalf("Put update: %v", err)
	}
	got, _ := r.ByJob("fm-t1")
	if got.State != StateRunning {
		t.Fatalf("stale entry after update: %v", got.State)
	}
	if n := len(r.List()); n != 1 {
		t.Fatalf("List len = %d, want 1", n)
	}
}

func TestInflightCountsOnlyNonTerminal(t *testing.T) {
	r := New(fixedClock())
	_ = r.Put(entryFor("t1"))
	_ = r.Put(entryFor("t2"))
	if n := r.Inflight(); n != 2 {
		t.Fatalf("Inflight = %d, want 2", n)
	}
	if _, err := r.MarkTerminal("t1", ResultCompleted, testNow); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if n := r.Inflight(); n != 1 {
		t.Fatalf("Inflight after terminal = %d, want 1", n)
	}
	// Terminal entry stays listed until Delete (cleanup retry path).
	if n := len(r.List()); n != 2 {
		t.Fatalf("List len = %d, want 2", n)
	}
}

func TestMarkTerminalWritesDoneIndexAndIsImmutable(t *testing.T) {
	r := New(fixedClock())
	_ = r.Put(entryFor("t1"))
	e, err := r.MarkTerminal("t1", ResultFailed, testNow)
	if err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if e.State != StateTerminal || e.Result != ResultFailed {
		t.Fatalf("entry = %v/%v", e.State, e.Result)
	}
	if !r.IsDone("t1") {
		t.Fatal("IsDone false after MarkTerminal")
	}
	e.State = StateRunning
	if err := r.Put(e); !errors.Is(err, ErrTerminalImmutable) {
		t.Fatalf("terminal regression accepted: %v", err)
	}
	if _, err := r.MarkTerminal("unknown", ResultFailed, testNow); err == nil {
		t.Fatal("MarkTerminal on unknown task accepted")
	}
}

func TestDeleteKeepsDoneRecord(t *testing.T) {
	r := New(fixedClock())
	_ = r.Put(entryFor("t1"))
	_, _ = r.MarkTerminal("t1", ResultCompleted, testNow)
	if err := r.Delete("t1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := r.Get("t1"); ok {
		t.Fatal("entry survived Delete")
	}
	if _, ok := r.ByJob("fm-t1"); ok {
		t.Fatal("job index survived Delete")
	}
	if !r.IsDone("t1") {
		t.Fatal("done record lost with Delete")
	}
	// A non-terminal Put for the same ID is possible again after Delete;
	// claim-side dedup is IsDone's job, not Put's.
	if err := r.Put(entryFor("t1")); err != nil {
		t.Fatalf("re-Put after Delete: %v", err)
	}
}

func TestDoneIndexExpires(t *testing.T) {
	now := testNow
	r := New(func() time.Time { return now })
	_ = r.Put(entryFor("t1"))
	_, _ = r.MarkTerminal("t1", ResultCompleted, now)
	now = now.Add(DoneIndexTTL + time.Minute)
	if r.IsDone("t1") {
		t.Fatal("done record past TTL still reported")
	}
}

func TestMarkDaemonRegistered(t *testing.T) {
	r := New(fixedClock())
	e := entryFor("t1")
	e.State = StateJobCreated
	_ = r.Put(e)
	e, ok := r.MarkDaemonRegistered("fm-t1", testNow)
	if !ok || e.State != StateDaemonRegistered || e.DaemonRegisteredAt != testNow {
		t.Fatalf("MarkDaemonRegistered = %v, %v", e, ok)
	}

	// A pending entry (Job not yet created) cannot have a daemon.
	_ = r.Put(entryFor("t2"))
	if _, ok := r.MarkDaemonRegistered("fm-t2", testNow); ok {
		t.Fatal("pending entry accepted a daemon registration")
	}

	// A running entry is not regressed by a duplicate S4.
	e.State = StateRunning
	_ = r.Put(e)
	e, ok = r.MarkDaemonRegistered("fm-t1", testNow.Add(time.Minute))
	if !ok || e.State != StateRunning {
		t.Fatalf("running entry regressed: %v", e.State)
	}

	if _, ok := r.MarkDaemonRegistered("ghost", testNow); ok {
		t.Fatal("unknown daemon registered")
	}
}

func TestNodeIndex(t *testing.T) {
	r := New(fixedClock())
	if got := r.LastNodeForIssue("issue-t1"); got != "" {
		t.Fatalf("LastNodeForIssue = %q, want empty", got)
	}
	_ = r.Put(entryFor("t1"))
	r.SetNode("t1", "node-a")
	if got := r.LastNodeForIssue("issue-t1"); got != "node-a" {
		t.Fatalf("LastNodeForIssue = %q", got)
	}
	if e, _ := r.Get("t1"); e.NodeName != "node-a" {
		t.Fatalf("entry node = %q", e.NodeName)
	}
	r.SetNode("ghost", "node-b") // no panic on unknown task
}

func TestActiveJobsByNode(t *testing.T) {
	r := New(fixedClock())
	for _, id := range []string{"t1", "t2", "t3"} {
		if err := r.Put(entryFor(id)); err != nil {
			t.Fatalf("Put(%s): %v", id, err)
		}
	}
	r.SetNode("t1", "node-a")
	r.SetNode("t2", "node-a")
	r.SetNode("t3", "node-b")

	load := r.ActiveJobsByNode()
	if load["node-a"] != 2 || load["node-b"] != 1 || len(load) != 2 {
		t.Fatalf("ActiveJobsByNode = %v, want node-a=2 node-b=1", load)
	}
	// Terminal Jobs do not hold node capacity: their pod is on its way out.
	if _, err := r.MarkTerminal("t2", ResultCompleted, testNow); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if load = r.ActiveJobsByNode(); load["node-a"] != 1 {
		t.Fatalf("after terminal: node-a = %d, want 1", load["node-a"])
	}
	// t3's node_name is recorded but its pod never landed on it — the entry
	// still counts: the Job holds the node until it settles.
	if load["node-b"] != 1 {
		t.Fatalf("node-b = %d, want 1", load["node-b"])
	}
}

func TestNodeSaturationMarkers(t *testing.T) {
	r := New(fixedClock())
	if r.NodeSaturated("node-a") {
		t.Fatal("fresh registry reports a saturated node")
	}
	r.SetSaturatedNodes([]string{"node-b", "node-a", ""})
	if nodes := r.SaturatedNodes(); len(nodes) != 2 || nodes[0] != "node-a" || nodes[1] != "node-b" {
		t.Fatalf("SaturatedNodes = %v, want [node-a node-b]", nodes)
	}
	if !r.NodeSaturated("node-a") || r.NodeSaturated("node-c") {
		t.Fatalf("marker lookup wrong: %v", r.SaturatedNodes())
	}
	// The set is replaced wholesale: a node that fell below the cap must
	// lose its marker, and stale nodes must not survive.
	r.SetSaturatedNodes(nil)
	if r.NodeSaturated("node-a") || len(r.SaturatedNodes()) != 0 {
		t.Fatalf("markers survived an empty replacement: %v", r.SaturatedNodes())
	}
}

func TestMarkStarted(t *testing.T) {
	r := New(fixedClock())
	e := entryFor("t1")
	e.State = StateDelivering
	_ = r.Put(e)

	e, ok := r.MarkStarted("t1", testNow)
	if !ok || e.State != StateRunning || e.StartedAt != testNow {
		t.Fatalf("MarkStarted = %v, %v", e, ok)
	}
	// started_at is written once.
	e, ok = r.MarkStarted("t1", testNow.Add(time.Minute))
	if !ok || e.StartedAt != testNow {
		t.Fatalf("started_at overwritten: %v", e.StartedAt)
	}
	// Terminal wins over a late start.
	_, _ = r.MarkTerminal("t1", ResultCompleted, testNow)
	if _, ok := r.MarkStarted("t1", testNow); ok {
		t.Fatal("late start regressed a terminal entry")
	}
	if _, ok := r.MarkStarted("ghost", testNow); ok {
		t.Fatal("unknown task marked started")
	}
}

func TestMarkDelivering(t *testing.T) {
	r := New(fixedClock())
	e := entryFor("t1")
	e.State = StateDaemonRegistered
	_ = r.Put(e)

	e, ok := r.MarkDelivering("fm-t1", testNow)
	if !ok || e.State != StateDelivering || e.DeliveredAt != testNow {
		t.Fatalf("MarkDelivering = %v, %v", e, ok)
	}
	// At most once.
	if _, ok := r.MarkDelivering("fm-t1", testNow); ok {
		t.Fatal("second delivery accepted")
	}
	// A job_created entry is not deliverable (daemon must register first).
	_ = r.Put(entryFor("t2"))
	e2, _ := r.Get("t2")
	e2.State = StateJobCreated
	_ = r.Put(e2)
	if _, ok := r.MarkDelivering("fm-t2", testNow); ok {
		t.Fatal("job_created entry delivered")
	}
	if _, ok := r.MarkDelivering("ghost", testNow); ok {
		t.Fatal("unknown daemon delivered")
	}
}

func TestDeleteCascadesRuntimeIndex(t *testing.T) {
	r := New(fixedClock())
	e := entryFor("t1")
	_ = r.Put(e)
	r.PutRuntime(JobRuntime{JobRuntimeID: "rt-t1", DaemonID: "fm-t1", TaskID: "t1"})

	if err := r.Delete("t1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := r.RuntimeByID("rt-t1"); ok {
		t.Fatal("runtime row survived entry deletion")
	}
	if _, ok := r.RuntimeByDaemon("fm-t1"); ok {
		t.Fatal("daemon reverse index survived entry deletion")
	}

	// DeleteRuntime standalone also clears the reverse index.
	r.PutRuntime(JobRuntime{JobRuntimeID: "rt-2", DaemonID: "fm-t2", TaskID: "t2"})
	r.DeleteRuntime("rt-2")
	if _, ok := r.RuntimeByDaemon("fm-t2"); ok {
		t.Fatal("reverse index survived DeleteRuntime")
	}
}

func TestSetLastStatusSeenPreservesOtherFields(t *testing.T) {
	r := New(fixedClock())
	e := entryFor("t1")
	e.State = StateDaemonRegistered
	_ = r.Put(e)
	if _, ok := r.MarkStarted("t1", testNow); !ok {
		t.Fatal("MarkStarted failed")
	}
	r.SetLastStatusSeen("t1", "running")
	got, _ := r.Get("t1")
	if got.LastStatusSeen != "running" {
		t.Fatalf("last_status_seen = %q", got.LastStatusSeen)
	}
	if got.State != StateRunning || got.StartedAt != testNow {
		t.Fatalf("SetLastStatusSeen clobbered fields: %+v", got)
	}
	r.SetLastStatusSeen("ghost", "running") // no panic on unknown task
}

func TestRuntimeIndex(t *testing.T) {
	r := New(fixedClock())
	rt := JobRuntime{JobRuntimeID: "rt-1", DaemonID: "fm-t1", TaskID: "t1", Provider: "omp", Name: "foreman-job", Online: true}
	r.PutRuntime(rt)
	if got, ok := r.RuntimeByID("rt-1"); !ok || got.TaskID != "t1" {
		t.Fatalf("RuntimeByID = %v, %v", got, ok)
	}
	if got, ok := r.RuntimeByDaemon("fm-t1"); !ok || got.JobRuntimeID != "rt-1" {
		t.Fatalf("RuntimeByDaemon = %v, %v", got, ok)
	}
	r.TouchHeartbeat("rt-1", testNow)
	got, _ := r.RuntimeByID("rt-1")
	if got.LastHeartbeatAt != testNow {
		t.Fatalf("heartbeat = %v", got.LastHeartbeatAt)
	}
	r.TouchHeartbeat("ghost", testNow) // no panic
}
