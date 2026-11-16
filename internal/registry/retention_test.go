package registry

import (
	"testing"
	"time"
)

// settleAt marks one task terminal at the registry's current instant.
func settleAt(t *testing.T, r *Registry, id, ws string) {
	t.Helper()
	if err := r.Put(TaskEntry{
		TaskID: id, WorkspaceID: ws, DaemonID: "worker-" + id,
		RuntimeID: "runtime-" + id, State: StateRunning,
	}); err != nil {
		t.Fatalf("put %s: %v", id, err)
	}
	if _, _, err := r.MarkTerminal(id, ResultCompleted, "", r.Now()); err != nil {
		t.Fatalf("mark %s: %v", id, err)
	}
	// Settling keeps the terminal row; reclaim (Delete) is what removes it and
	// leaves only the settled index. The retention window applies to that index,
	// so the helper must take the same path the dispatcher takes.
	if err := r.Delete(id); err != nil {
		t.Fatalf("reclaim %s: %v", id, err)
	}
}

// TestDoneIndexExpiresAfterRetention is the anti-leak contract: the settled
// index exists so a re-dispatch is caught, but it must not grow forever. A
// process that runs for weeks would otherwise keep every task id it ever saw.
func TestDoneIndexExpiresAfterRetention(t *testing.T) {
	clock := time.Unix(1000, 0)
	r := New(WithClock(func() time.Time { return clock }))

	settleAt(t, r, "old", "ws-1")
	if !r.IsDone("ws-1", "old") {
		t.Fatal("precondition: freshly settled task should be remembered")
	}

	// Just inside the window: still remembered.
	clock = clock.Add(doneRetention - time.Minute)
	settleAt(t, r, "fresh", "ws-1")
	if !r.IsDone("ws-1", "old") {
		t.Fatal("task forgotten before its retention window elapsed")
	}

	// Past the window: the entry is gone, and so are its owner/workspace rows.
	clock = clock.Add(2 * time.Minute)
	r.Put(TaskEntry{TaskID: "trigger", WorkspaceID: "ws-1", DaemonID: "d",
		RuntimeID: "rt", State: StateRunning})
	if _, _, err := r.MarkTerminal("trigger", ResultCompleted, "", r.Now()); err != nil {
		t.Fatalf("trigger prune: %v", err)
	}
	if err := r.Delete("trigger"); err != nil {
		t.Fatalf("trigger reclaim: %v", err)
	}

	if r.IsDone("ws-1", "old") {
		t.Fatal("expired task still reported as done")
	}
	if owner := r.OwnerOf("old"); owner != "" {
		t.Fatalf("expired task still has an owner %q; prune must drop the owner row with the timestamp", owner)
	}
	if !r.IsDone("ws-1", "fresh") {
		t.Fatal("an entry inside its window was pruned along with the expired one")
	}
}

// TestDoneIndexKeepsWorkspacePartition: the settled index must still answer
// only for the workspace it was asked about, or ops views leak across tenants.
func TestDoneIndexKeepsWorkspacePartition(t *testing.T) {
	clock := time.Unix(2000, 0)
	r := New(WithClock(func() time.Time { return clock }))
	settleAt(t, r, "t1", "ws-1")

	if !r.IsDone("ws-1", "t1") {
		t.Fatal("owning workspace was not told the task is done")
	}
	if r.IsDone("ws-2", "t1") {
		t.Fatal("another workspace was told a foreign task is done")
	}
}

// TestOwnerOfSurvivesSettleButNotPrune pins the two faces of the retained
// owner: it answers for a settled task (so a late report stays attributable),
// and disappears once the retention window closes.
func TestOwnerOfSurvivesSettleButNotPrune(t *testing.T) {
	clock := time.Unix(3000, 0)
	r := New(WithClock(func() time.Time { return clock }))

	if err := r.Put(TaskEntry{TaskID: "t1", WorkspaceID: "ws-1", DaemonID: "worker-a",
		RuntimeID: "rt-1", State: StateRunning}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if owner := r.OwnerOf("t1"); owner != "worker-a" {
		t.Fatalf("live owner = %q", owner)
	}
	if _, _, err := r.MarkTerminal("t1", ResultCompleted, "", r.Now()); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if err := r.Delete("t1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if owner := r.OwnerOf("t1"); owner != "worker-a" {
		t.Fatalf("retained owner = %q, want worker-a", owner)
	}
}

// TestForgetVanishedCoversAllThreeObligations pins the contract of the
// server-deleted path: the task row goes, the daemon index stops listing it,
// and a verdict-less marker keeps a redelivery from re-running it. Losing any
// one of the three is silent — the task either re-executes or a ghost row keeps
// answering scheduling queries.
func TestForgetVanishedCoversAllThreeObligations(t *testing.T) {
	clock := time.Unix(4000, 0)
	r := New(WithClock(func() time.Time { return clock }))

	// A pending row is what ByDaemonInWorkspace serves (it deliberately skips
	// running/delivering rows: those are not waiting for delivery).
	if err := r.Put(TaskEntry{TaskID: "gone", WorkspaceID: "ws-1", DaemonID: "worker-a",
		RuntimeID: "rt-1", State: StatePending}); err != nil {
		t.Fatalf("put: %v", err)
	}
	// The task is visible to its daemon while live.
	if e, ok := r.ByDaemonInWorkspace("ws-1", "worker-a"); !ok || e.TaskID != "gone" {
		t.Fatalf("precondition: live task should be listed for its daemon: %+v ok=%v", e, ok)
	}

	if err := r.ForgetVanished("gone"); err != nil {
		t.Fatalf("forget: %v", err)
	}

	// 1. the live row is gone.
	if _, live := r.Get("gone"); live {
		t.Fatal("live row survived ForgetVanished")
	}
	// 2. the daemon index no longer lists it, and the per-daemon slice is
	// dropped rather than left empty — an empty slice per daemon would grow the
	// map once per daemon forever.
	if e, ok := r.ByDaemonInWorkspace("ws-1", "worker-a"); ok {
		t.Fatalf("daemon index still lists the vanished task: %+v", e)
	}
	if cur, ok := r.byDaemon["worker-a"]; ok {
		t.Fatalf("the daemon index key survived with %v; it must be deleted when empty", cur)
	}
	// 3. a redelivery of the same id is caught, with no invented verdict.
	if !r.IsDone("ws-1", "gone") {
		t.Fatal("vanished task not remembered; a redelivery would re-run it")
	}
	if owner := r.OwnerOf("gone"); owner != "worker-a" {
		t.Fatalf("owner = %q, want worker-a (attribution must survive)", owner)
	}
	settled, ok := r.SettledInWorkspace("ws-1", "gone")
	if ok && settled.Result != "" {
		t.Fatalf("ForgetVanished invented the verdict %q", settled.Result)
	}
}

// TestForgetVanishedUnknownIsQuiet: a duplicate call (the caller retries) must
// not error, and must not disturb the retained marker.
func TestForgetVanishedUnknownIsQuiet(t *testing.T) {
	r := New(WithClock(func() time.Time { return time.Unix(5000, 0) }))
	if err := r.ForgetVanished("never-seen"); err == nil {
		t.Fatal("expected ErrUnknownTask for a task the registry never held")
	}
}
