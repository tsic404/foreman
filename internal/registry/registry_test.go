package registry

import (
	"encoding/json"
	"errors"
	"sync"
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

func TestPutIfAbsentKeepsTheLiveEntry(t *testing.T) {
	r := New(fixedClock())
	live := entryFor("t1")
	live.State = StateRunning
	if err := r.Put(live); err != nil {
		t.Fatalf("Put: %v", err)
	}

	projection := entryFor("t1")
	projection.State = StateJobCreated
	projection.JobName = "fm-other"
	inserted, err := r.PutIfAbsent(projection)
	if err != nil || inserted {
		t.Fatalf("PutIfAbsent over a live entry = %v, %v; want false, nil", inserted, err)
	}
	got, _ := r.Get("t1")
	if got.State != StateRunning || got.JobName != "fm-t1" {
		t.Fatalf("entry = %+v, want the live entry untouched", got)
	}
	if _, ok := r.ByJob("fm-other"); ok {
		t.Fatal("index written for a rejected entry")
	}

	inserted, err = r.PutIfAbsent(entryFor("t2"))
	if err != nil || !inserted {
		t.Fatalf("PutIfAbsent on an absent task = %v, %v; want true, nil", inserted, err)
	}
	if got, ok := r.ByJob("fm-t2"); !ok || got.TaskID != "t2" {
		t.Fatalf("ByJob after insert = %v, %v", got, ok)
	}
	if _, err := r.PutIfAbsent(TaskEntry{}); err == nil {
		t.Fatal("PutIfAbsent without task_id accepted")
	}
}

func TestInflightCountsOnlyNonTerminal(t *testing.T) {
	r := New(fixedClock())
	_ = r.Put(entryFor("t1"))
	_ = r.Put(entryFor("t2"))
	if n := r.Inflight(); n != 2 {
		t.Fatalf("Inflight = %d, want 2", n)
	}
	if _, _, err := r.MarkTerminal("t1", ResultCompleted, "", testNow); err != nil {
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
	e, first, err := r.MarkTerminal("t1", ResultFailed, "", testNow)
	if err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if !first {
		t.Fatal("the first terminal write did not report the transition")
	}
	if e.State != StateTerminal || e.Result != ResultFailed {
		t.Fatalf("entry = %v/%v", e.State, e.Result)
	}
	if !r.IsDone("", "t1") {
		t.Fatal("IsDone false after MarkTerminal")
	}
	// A repeated terminal write is an idempotent cleanup: the first verdict
	// stands and the caller is told it did not transition the entry.
	if replay, first, err := r.MarkTerminal("t1", ResultCompleted, "job_failed", testNow); err != nil {
		t.Fatalf("repeated MarkTerminal: %v", err)
	} else if first || replay.Result != ResultFailed || replay.FailureReason != "" {
		t.Fatalf("replay = %+v, first=%v; want the original verdict", replay, first)
	}
	e.State = StateRunning
	if err := r.Put(e); !errors.Is(err, ErrTerminalImmutable) {
		t.Fatalf("terminal regression accepted: %v", err)
	}
	if _, _, err := r.MarkTerminal("unknown", ResultFailed, "", testNow); err == nil {
		t.Fatal("MarkTerminal on unknown task accepted")
	}
}

func TestSettleNoResultOnlyReplacesTheReportsOwnVerdict(t *testing.T) {
	r := New(fixedClock())
	_ = r.Put(entryFor("t1"))
	_, first, err := r.MarkTerminal("t1", ResultCompleted, "", testNow)
	if err != nil || !first {
		t.Fatalf("MarkTerminal = first %v, %v", first, err)
	}

	// Another path's verdict is not the report's to replace.
	if _, ok := r.SettleNoResult("t1", ResultFailed); ok {
		t.Fatal("the in-place decision replaced another verdict")
	}
	if got, _ := r.Get("t1"); got.Result != ResultCompleted {
		t.Fatalf("result = %q, want the standing verdict", got.Result)
	}

	// The report's own verdict becomes the empty result, live and settled.
	got, ok := r.SettleNoResult("t1", ResultCompleted)
	if !ok || got.Result != "" || got.FailureReason != "" {
		t.Fatalf("in-place decision = %+v, %v; want no result", got, ok)
	}
	record, ok := r.Settled("t1")
	if !ok || record.Result != "" {
		t.Fatalf("settled record = %+v, %v; want the empty result", record, ok)
	}

	// A replay carrying the old expected value matches nothing: the empty
	// result is a decided verdict too.
	if _, ok := r.SettleNoResult("t1", ResultCompleted); ok {
		t.Fatal("a replay re-applied the in-place decision")
	}
}

func TestDeleteKeepsDoneRecord(t *testing.T) {
	r := New(fixedClock())
	_ = r.Put(entryFor("t1"))
	_, _, _ = r.MarkTerminal("t1", ResultCompleted, "", testNow)
	if err := r.Delete("t1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := r.Get("t1"); ok {
		t.Fatal("entry survived Delete")
	}
	if _, ok := r.ByJob("fm-t1"); ok {
		t.Fatal("job index survived Delete")
	}
	if !r.IsDone("", "t1") {
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
	_, _, _ = r.MarkTerminal("t1", ResultCompleted, "", now)
	now = now.Add(DoneIndexTTL + time.Minute)
	if r.IsDone("", "t1") {
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

func TestNodePlacement(t *testing.T) {
	r := New(fixedClock())
	_ = r.Put(entryFor("t1"))
	r.SetNode("t1", "node-a")
	if e, _ := r.Get("t1"); e.NodeName != "node-a" {
		t.Fatalf("entry node = %q", e.NodeName)
	}
	// Placement is not a learning point: the soft-affinity index only ever
	// learns from a terminal report (ADR-015), and it has no issue key.
	if got := r.LastNodeForSession("", "issue-t1"); got != "" {
		t.Fatalf("LastNodeForSession(issue key) = %q, want empty", got)
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
	if _, _, err := r.MarkTerminal("t2", ResultCompleted, "", testNow); err != nil {
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
	if r.Saturated("node-a") {
		t.Fatal("fresh registry reports a saturated node")
	}
	r.SetSaturatedNodes([]string{"node-b", "node-a", ""})
	if nodes := r.SaturatedNodes(); len(nodes) != 2 || nodes[0] != "node-a" || nodes[1] != "node-b" {
		t.Fatalf("SaturatedNodes = %v, want [node-a node-b]", nodes)
	}
	if !r.Saturated("node-a") || r.Saturated("node-c") {
		t.Fatalf("marker lookup wrong: %v", r.SaturatedNodes())
	}
	// The set is replaced wholesale: a node that fell below the cap must
	// lose its marker, and stale nodes must not survive.
	r.SetSaturatedNodes(nil)
	if r.Saturated("node-a") || len(r.SaturatedNodes()) != 0 {
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
	_, _, _ = r.MarkTerminal("t1", ResultCompleted, "", testNow)
	if _, ok := r.MarkStarted("t1", testNow); ok {
		t.Fatal("late start regressed a terminal entry")
	}
	if _, ok := r.MarkStarted("ghost", testNow); ok {
		t.Fatal("unknown task marked started")
	}
}

func TestMarkStartFact(t *testing.T) {
	r := New(fixedClock())
	e := entryFor("t1")
	e.State = StateJobCreated
	_ = r.Put(e)

	// The adoption write records the fact and leaves the lifecycle state at
	// the rebuild's initial phase (ADR-026 后果).
	got, ok := r.MarkStartFact("t1", testNow)
	if !ok || got.State != StateJobCreated || got.StartedAt != testNow {
		t.Fatalf("MarkStartFact = %v, %v", got, ok)
	}
	// started_at is written once; a later observation does not overwrite it.
	got, ok = r.MarkStartFact("t1", testNow.Add(time.Minute))
	if !ok || got.StartedAt != testNow {
		t.Fatalf("started_at overwritten: %v", got.StartedAt)
	}
	// Terminal wins over a late fact.
	_, _, _ = r.MarkTerminal("t1", ResultCompleted, "", testNow)
	if _, ok := r.MarkStartFact("t1", testNow); ok {
		t.Fatal("late start fact regressed a terminal entry")
	}
	if _, ok := r.MarkStartFact("ghost", testNow); ok {
		t.Fatal("unknown task marked started")
	}
}

func TestMarkDelivering(t *testing.T) {
	r := New(fixedClock())
	e := entryFor("t1")
	e.State = StateDaemonRegistered
	_ = r.Put(e)

	// The delivery records the body the daemon received (S6 rewrote
	// runtime_id), replacing the claim-time payload the ops preview must
	// never serve.
	delivered := json.RawMessage(`{"id":"t1","runtime_id":"rt-t1"}`)
	e, ok := r.MarkDelivering("fm-t1", testNow, delivered)
	if !ok || e.State != StateDelivering || e.DeliveredAt != testNow {
		t.Fatalf("MarkDelivering = %v, %v", e, ok)
	}
	if string(e.Payload) != string(delivered) {
		t.Fatalf("payload = %s, want the delivered body", e.Payload)
	}
	// At most once.
	if _, ok := r.MarkDelivering("fm-t1", testNow, json.RawMessage(`{"id":"over"}`)); ok {
		t.Fatal("second delivery accepted")
	}
	if stored, _ := r.Get("t1"); string(stored.Payload) != string(delivered) {
		t.Fatalf("a rejected delivery overwrote the payload: %s", stored.Payload)
	}
	// A job_created entry is not deliverable (daemon must register first).
	_ = r.Put(entryFor("t2"))
	e2, _ := r.Get("t2")
	e2.State = StateJobCreated
	_ = r.Put(e2)
	if _, ok := r.MarkDelivering("fm-t2", testNow, delivered); ok {
		t.Fatal("job_created entry delivered")
	}
	if _, ok := r.MarkDelivering("ghost", testNow, delivered); ok {
		t.Fatal("unknown daemon delivered")
	}
	if stored, _ := r.Get("t2"); stored.Payload != nil {
		t.Fatalf("a rejected delivery stored a payload: %s", stored.Payload)
	}
}

// TestMarkDeliveringRearmsAfterTheWindow guards ADR-032's re-delivery gate:
// an expired confirmation window releases the payload for one more commit
// only while the entry still has no start fact, and a registry without a
// window commits the delivery exactly once.
func TestMarkDeliveringRearmsAfterTheWindow(t *testing.T) {
	window := 30 * time.Second
	r := New(fixedClock(), WithDeliveryConfirmTimeout(window))
	e := entryFor("t1")
	e.State = StateDaemonRegistered
	if err := r.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}

	first := json.RawMessage(`{"id":"t1","runtime_id":"rt-t1"}`)
	got, ok := r.MarkDelivering("fm-t1", testNow, first)
	if !ok || got.Deliveries != 1 || got.DeliveryDeadline != testNow.Add(window) {
		t.Fatalf("first delivery = %v (%v), want one commit with the window armed", got, ok)
	}
	if !got.DeliveryUnconfirmed(testNow.Add(window)) || got.DeliveryRearmable(testNow.Add(window)) {
		t.Errorf("inside the window the commit must stand, not be released: %v", got)
	}
	// The boundary is exclusive: the commit holds until the deadline passes.
	if _, ok := r.MarkDelivering("fm-t1", testNow.Add(window), first); ok {
		t.Fatal("a claim at the window's deadline re-delivered")
	}

	// Past the window the payload is released, with a fresh window and the
	// preview serving the latest body.
	released := testNow.Add(window + time.Second)
	second := json.RawMessage(`{"id":"t1","runtime_id":"rt-t1","attempt":2}`)
	got, ok = r.MarkDelivering("fm-t1", released, second)
	if !ok {
		t.Fatal("the expired window did not release the delivery")
	}
	if got.Deliveries != 2 || got.DeliveredAt != released || got.DeliveryDeadline != released.Add(window) {
		t.Fatalf("re-delivery = %+v, want a second commit with its own window", got)
	}
	if string(got.Payload) != string(second) {
		t.Errorf("payload = %s, want the re-delivered body", got.Payload)
	}
	if !got.DeliveryUnconfirmed(released.Add(window)) || got.DeliveryRearmable(released.Add(window)) {
		t.Errorf("the re-delivery must re-open the window: %v", got)
	}

	// A start fact settles the delivery for good: no window releases it.
	if _, ok := r.MarkStarted("t1", released.Add(time.Millisecond)); !ok {
		t.Fatal("MarkStarted")
	}
	if _, ok := r.MarkDelivering("fm-t1", released.Add(window+time.Second), second); ok {
		t.Fatal("a started entry was re-delivered")
	}

	// No window configured: the exit is off and the delivery is committed once.
	r2 := New(fixedClock())
	e2 := entryFor("t2")
	e2.State = StateDaemonRegistered
	if err := r2.Put(e2); err != nil {
		t.Fatalf("Put t2: %v", err)
	}
	if _, ok := r2.MarkDelivering("fm-t2", testNow, first); !ok {
		t.Fatal("first delivery refused without a window")
	}
	if _, ok := r2.MarkDelivering("fm-t2", testNow.Add(time.Hour), first); ok {
		t.Fatal("a registry without a confirmation window re-delivered")
	}
	// Without a window the commit can never be released — it stays the
	// unconfirmed delivery the boot bound owns.
	stored, _ := r2.Get("t2")
	if !stored.DeliveryUnconfirmed(testNow.Add(time.Hour)) || stored.DeliveryRearmable(testNow.Add(time.Hour)) {
		t.Errorf("an unwindowed delivery must stay unconfirmed, not re-deliverable: %v", stored)
	}
}

// TestClaimsAfterDeliveryResetsOnNewCommit guards the counter's per-commit
// reading (§4.1): it answers how many empty S6 claims followed the current
// delivery commit, i.e. before the commit was confirmed or replaced by the
// next one. A new commit starts from zero; the rearm event carries the
// replaced commit's count separately.
func TestClaimsAfterDeliveryResetsOnNewCommit(t *testing.T) {
	window := 30 * time.Second
	r := New(fixedClock(), WithDeliveryConfirmTimeout(window))
	e := entryFor("t1")
	e.State = StateDaemonRegistered
	if err := r.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	body := json.RawMessage(`{"id":"t1","runtime_id":"rt-t1"}`)

	// First window: the commit, then two empty claims answered inside it.
	if _, ok := r.MarkDelivering("fm-t1", testNow, body); !ok {
		t.Fatal("first delivery refused")
	}
	r.NoteClaimAfterDelivery("t1")
	r.NoteClaimAfterDelivery("t1")

	// The expired window releases the payload again: the new commit starts
	// the count from zero (the replaced commit's count travels in the rearm
	// event, §4.1).
	second := testNow.Add(window + time.Second)
	if _, ok := r.MarkDelivering("fm-t1", second, body); !ok {
		t.Fatal("the expired window did not release the delivery")
	}
	if got, _ := r.Get("t1"); got.ClaimsAfterDelivery != 0 {
		t.Fatalf("claims_after_delivery after the second commit = %d, want 0 (a new commit starts the count)",
			got.ClaimsAfterDelivery)
	}
	r.NoteClaimAfterDelivery("t1")

	// The third commit replaces the second: its count is the second window's
	// own, then it resets again.
	third := second.Add(window + time.Second)
	got, ok := r.MarkDelivering("fm-t1", third, body)
	if !ok {
		t.Fatal("the second expired window did not release the delivery")
	}
	if got.Deliveries != 3 {
		t.Fatalf("deliveries = %d, want 3 (three commits, none confirmed)", got.Deliveries)
	}
	if got.ClaimsAfterDelivery != 0 {
		t.Fatalf("claims_after_delivery at the third commit = %d, want 0", got.ClaimsAfterDelivery)
	}
	r.NoteClaimAfterDelivery("t1")
	if got, _ := r.Get("t1"); got.ClaimsAfterDelivery != 1 {
		t.Fatalf("claims_after_delivery after another empty claim = %d, want 1", got.ClaimsAfterDelivery)
	}

	// The start fact settles the delivery face; the diagnostic the settled
	// record answers with keeps the current commit's count.
	if _, ok := r.MarkStarted("t1", third.Add(time.Second)); !ok {
		t.Fatal("MarkStarted")
	}
	if settled, _ := r.Get("t1"); settled.ClaimsAfterDelivery != 1 {
		t.Errorf("claims_after_delivery after the start fact = %d, want 1", settled.ClaimsAfterDelivery)
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

// TestDeleteKeepsAReissuedOwnersIndexes reproduces a job/daemon key reissued
// to another entry: deleting the previous owner must not unmap the new one.
func TestDeleteKeepsAReissuedOwnersIndexes(t *testing.T) {
	r := New(fixedClock())
	old := entryFor("t1")
	if err := r.Put(old); err != nil {
		t.Fatalf("Put old: %v", err)
	}
	newer := entryFor("t2")
	newer.JobName = old.JobName
	newer.DaemonID = old.DaemonID
	if err := r.Put(newer); err != nil {
		t.Fatalf("Put newer: %v", err)
	}

	if err := r.Delete("t1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got, ok := r.ByJob(old.JobName); !ok || got.TaskID != "t2" {
		t.Fatalf("ByJob(%q) = %v, %v; want t2", old.JobName, got, ok)
	}
	if got, ok := r.ByDaemon(old.DaemonID); !ok || got.TaskID != "t2" {
		t.Fatalf("ByDaemon(%q) = %v, %v; want t2", old.DaemonID, got, ok)
	}
}

// TestPutKeepsAReissuedOwnersIndexes covers the replace path: re-Putting the
// old entry after another one took its keys must leave that mapping intact.
func TestPutKeepsAReissuedOwnersIndexes(t *testing.T) {
	r := New(fixedClock())
	old := entryFor("t1")
	if err := r.Put(old); err != nil {
		t.Fatalf("Put old: %v", err)
	}
	newer := entryFor("t2")
	newer.JobName = old.JobName
	newer.DaemonID = old.DaemonID
	if err := r.Put(newer); err != nil {
		t.Fatalf("Put newer: %v", err)
	}

	replaced := old
	replaced.JobName = "fm-t1-next"
	replaced.DaemonID = "fm-t1-next"
	if err := r.Put(replaced); err != nil {
		t.Fatalf("Put replaced: %v", err)
	}
	if got, ok := r.ByJob(old.JobName); !ok || got.TaskID != "t2" {
		t.Fatalf("ByJob(%q) = %v, %v; want t2", old.JobName, got, ok)
	}
	if got, ok := r.ByDaemon(old.DaemonID); !ok || got.TaskID != "t2" {
		t.Fatalf("ByDaemon(%q) = %v, %v; want t2", old.DaemonID, got, ok)
	}
	if got, ok := r.ByJob(replaced.JobName); !ok || got.TaskID != "t1" {
		t.Fatalf("ByJob(%q) = %v, %v; want t1", replaced.JobName, got, ok)
	}
}

// TestRuntimeCleanupKeepsAReRegisteredRowsIndex: re-registration re-keys the
// daemon to a newer runtime row; dropping the superseded row keeps it.
func TestRuntimeCleanupKeepsAReRegisteredRowsIndex(t *testing.T) {
	r := New(fixedClock())
	e := entryFor("t1")
	if err := r.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	r.PutRuntime(JobRuntime{JobRuntimeID: e.JobRuntimeID, DaemonID: e.DaemonID, TaskID: e.TaskID})
	renewed := JobRuntime{JobRuntimeID: "rt-t1-next", DaemonID: e.DaemonID, TaskID: e.TaskID}
	r.PutRuntime(renewed)

	if err := r.Delete("t1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got, ok := r.RuntimeByDaemon(e.DaemonID); !ok || got.JobRuntimeID != renewed.JobRuntimeID {
		t.Fatalf("RuntimeByDaemon(%q) = %v, %v; want %q", e.DaemonID, got, ok, renewed.JobRuntimeID)
	}

	r.PutRuntime(JobRuntime{JobRuntimeID: "rt-stale", DaemonID: e.DaemonID, TaskID: e.TaskID})
	r.PutRuntime(renewed)
	r.DeleteRuntime("rt-stale")
	if got, ok := r.RuntimeByDaemon(e.DaemonID); !ok || got.JobRuntimeID != renewed.JobRuntimeID {
		t.Fatalf("RuntimeByDaemon after DeleteRuntime = %v, %v; want %q", got, ok, renewed.JobRuntimeID)
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

// TestSettledRecordRetainsVerdictAndDropsSecrets guards ADR-032's retention
// face: the settled index keeps the terminal projection the ops endpoint
// answers with after the credential reclaim deleted the live row, without the
// payload body or the token digest, and only for the retention window.
func TestSettledRecordRetainsVerdictAndDropsSecrets(t *testing.T) {
	now := testNow
	r := New(func() time.Time { return now }, WithDrainTimeout(time.Minute), WithDeliveryConfirmTimeout(30*time.Second))

	secret := json.RawMessage(`{"id":"t1","auth_token":"mat_secret"}`)
	e := entryFor("t1")
	e.State = StateDaemonRegistered
	e.Payload = secret
	e.TokenHash = "sha256:deadbeef"
	if err := r.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	delivered, ok := r.MarkDelivering("fm-t1", now, secret)
	if !ok || delivered.DeliveryDeadline != now.Add(30*time.Second) {
		t.Fatalf("delivery window = %v (ok %v), want the commit plus 30s", delivered.DeliveryDeadline, ok)
	}
	if _, first, err := r.MarkTerminal("t1", ResultFailed, "delivery_unconfirmed", time.Time{}); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	} else if !first {
		t.Fatal("the first terminal write did not report the transition")
	}
	record, ok := r.Settled("t1")
	if !ok {
		t.Fatal("no settled record after the verdict")
	}
	if record.FailureReason != "delivery_unconfirmed" {
		t.Errorf("retained reason = %q, want the verdict's attribution", record.FailureReason)
	}
	if record.Payload != nil || record.TokenHash != "" {
		t.Errorf("the retained record kept the payload or the token digest: %+v", record)
	}

	// A repeated terminal report is an idempotent cleanup: it must neither
	// reclassify the verdict nor erase the attribution that started the drain.
	again, first, err := r.MarkTerminal("t1", ResultCompleted, "job_failed", time.Time{})
	if err != nil {
		t.Fatalf("repeated MarkTerminal: %v", err)
	}
	if first {
		t.Error("a repeated terminal report reported a first transition")
	}
	if again.Result != ResultFailed || again.FailureReason != "delivery_unconfirmed" {
		t.Errorf("replay rewrote the verdict: result=%q reason=%q", again.Result, again.FailureReason)
	}
	if !again.CompletedAt.Equal(record.CompletedAt) || !again.DrainDeadline.Equal(record.DrainDeadline) {
		t.Errorf("replay moved the verdict instant or the drain bound: %+v", again)
	}
	if retained, _ := r.Settled("t1"); retained.Result != ResultFailed || retained.FailureReason != "delivery_unconfirmed" {
		t.Errorf("replay rewrote the settled record: result=%q reason=%q", retained.Result, retained.FailureReason)
	}

	// Past the retention window the record is gone, and a reclaimed task is
	// once more indistinguishable from one that never existed.
	now = now.Add(DoneIndexTTL + time.Second)
	if _, ok := r.Settled("t1"); ok {
		t.Error("the record outlived its retention window")
	}
}

// TestDeliveryWindowUnarmedWithoutTheOption guards the "0 = off" form: without
// the option no delivery deadline is written, so the lost-response form falls
// back to the boot upper bound alone.
func TestDeliveryWindowUnarmedWithoutTheOption(t *testing.T) {
	r := New(fixedClock())
	e := entryFor("t1")
	e.State = StateDaemonRegistered
	if err := r.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	delivered, ok := r.MarkDelivering("fm-t1", testNow, json.RawMessage(`{"id":"t1"}`))
	if !ok {
		t.Fatal("delivery rejected")
	}
	if !delivered.DeliveryDeadline.IsZero() {
		t.Fatalf("delivery deadline = %v, want none without the option", delivered.DeliveryDeadline)
	}
}

// TestRuntimeFieldSettersPreserveEachOther pins the lock-scoped single-field
// writes: a concurrent heartbeat and a delivery record must both survive. The
// old read-modify-write from a snapshot would write the stale heartbeat fields
// back and lose one of the two.
func TestRuntimeFieldSettersPreserveEachOther(t *testing.T) {
	r := New(time.Now)
	r.PutRuntime(JobRuntime{JobRuntimeID: "rid-1", DaemonID: "fm-1", Online: true})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			r.TouchHeartbeat("rid-1", time.Unix(int64(i+1), 0))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			r.SetRuntimeDeliveredTask("rid-1", "task-1")
		}
	}()
	wg.Wait()

	rt, ok := r.RuntimeByID("rid-1")
	if !ok {
		t.Fatal("runtime missing")
	}
	if rt.DeliveredTaskID != "task-1" {
		t.Errorf("DeliveredTaskID = %q, want the delivered task", rt.DeliveredTaskID)
	}
	if rt.LastHeartbeatAt.IsZero() || !rt.Online {
		t.Errorf("heartbeat fields lost: %+v", rt)
	}
}
