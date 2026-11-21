package registry_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scope"
	"github.com/tsic404/foreman/internal/state"
)

func sharedStore(t *testing.T) (state.Store, state.CoordStore) {
	t.Helper()
	st, coord, _ := sharedStoreWithRedis(t)
	return st, coord
}

func sharedStoreWithRedis(t *testing.T) (state.Store, state.CoordStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	st, err := state.New(state.Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    48 * time.Hour,
	})
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	return st, st.(state.CoordStore), mr
}

func scopedEntry(taskID, sourceID string) registry.TaskEntry {
	daemon := "fm-" + taskID + "-" + sourceID
	return registry.TaskEntry{
		TaskID:       taskID,
		SourceID:     sourceID,
		WorkspaceID:  "ws-shared",
		JobName:      daemon,
		DaemonID:     daemon,
		JobRuntimeID: "jr-" + daemon,
		State:        registry.StateRunning,
		ClaimedAt:    time.Now(),
	}
}

// TestDaemonIdentityIndexResolvesACustomDisplayName pins the token face to the
// daemon id. A template may render a custom metadata.name, so the Job-name
// index cannot answer a Job Token lookup: the two names are different faces of
// the same attempt (02-contracts V2-C03 共享恢复与凭据, ADR-034).
func TestDaemonIdentityIndexResolvesACustomDisplayName(t *testing.T) {
	ctx := context.Background()
	st, _ := sharedStore(t)
	a := registry.New(time.Now, registry.WithStateStore(st))
	t.Cleanup(func() { a.Close(ctx) })
	b := registry.New(time.Now, registry.WithStateStore(st))
	t.Cleanup(func() { b.Close(ctx) })

	e := scopedEntry("task-custom", "src-a")
	e.JobName = "team-review-display"
	e.JobRuntimeID = "jr-custom"
	if e.JobName == e.DaemonID {
		t.Fatal("fixture must keep the display name distinct from the daemon identity")
	}
	if err := a.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The second replica answers the Job Token by the daemon identity, in the
	// source scope and in the bare-workspace form.
	got, ok := b.ByDaemonInSourceWorkspace("src-a", "ws-shared", e.DaemonID)
	if !ok {
		t.Fatalf("daemon %q did not resolve on the second replica (custom display name %q)", e.DaemonID, e.JobName)
	}
	if got.TaskID != e.TaskID || got.DaemonID != e.DaemonID || got.JobName != e.JobName {
		t.Fatalf("resolved %+v, want the attempt %+v", got, e)
	}
	if got, ok := b.ByDaemonInWorkspace("ws-shared", e.DaemonID); !ok || got.TaskID != e.TaskID {
		t.Fatalf("bare-workspace daemon lookup = %+v, %v; want task %q", got, ok, e.TaskID)
	}
	// The display-name face still resolves through the Job index.
	if got, ok := b.ByJobInWorkspace("ws-shared", e.JobName); !ok || got.DaemonID != e.DaemonID {
		t.Fatalf("Job-name lookup = %+v, %v; want daemon %q", got, ok, e.DaemonID)
	}

	// Removing the authoritative row revokes the token even though the second
	// replica cached the attempt.
	if err := st.DeleteEntry(ctx, scope.Key("src-a", "ws-shared"), e.TaskID); err != nil {
		t.Fatalf("DeleteEntry: %v", err)
	}
	if _, ok := b.ByDaemonInSourceWorkspace("src-a", "ws-shared", e.DaemonID); ok {
		t.Fatal("the second replica authorized the token after the shared row was deleted")
	}
}

// TestPreIndexRowStillResolvesByLegacyDaemonShape pins the upgrade path: a row
// written before the daemon index existed (only the historical fm-<task id>
// shape) still resolves, but only through the shared row whose own daemon
// identity matches the token.
func TestPreIndexRowStillResolvesByLegacyDaemonShape(t *testing.T) {
	ctx := context.Background()
	st, _, mr := sharedStoreWithRedis(t)
	a := registry.New(time.Now, registry.WithStateStore(st))
	t.Cleanup(func() { a.Close(ctx) })
	b := registry.New(time.Now, registry.WithStateStore(st))
	t.Cleanup(func() { b.Close(ctx) })

	e := scopedEntry("task-legacy", "src-a")
	// The pre-index identity: fm-<task id>, equal to the default Job name.
	e.DaemonID = "fm-" + e.TaskID
	e.JobName = e.DaemonID
	if err := a.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// A row written by the older binary carries no daemon index entry.
	mr.Del("foreman:daemonidx:" + e.DaemonID)

	got, ok := b.ByDaemonInSourceWorkspace("src-a", "ws-shared", e.DaemonID)
	if !ok {
		t.Fatalf("pre-index row for daemon %q did not resolve", e.DaemonID)
	}
	if got.TaskID != e.TaskID || got.DaemonID != e.DaemonID || got.SourceID != e.SourceID {
		t.Fatalf("resolved %+v, want the pre-index attempt %+v", got, e)
	}

	// The pre-ADR-047 single-source row has no source partition at all and
	// answers the bare-workspace form.
	legacy := scopedEntry("task-solo", "")
	legacy.WorkspaceID = "ws-solo"
	legacy.DaemonID = "fm-" + legacy.TaskID
	legacy.JobName = legacy.DaemonID
	if err := a.Put(legacy); err != nil {
		t.Fatalf("Put legacy: %v", err)
	}
	mr.Del("foreman:daemonidx:" + legacy.DaemonID)
	if got, ok := b.ByDaemonInWorkspace("ws-solo", legacy.DaemonID); !ok || got.TaskID != legacy.TaskID {
		t.Fatalf("bare-workspace pre-index lookup = %+v, %v", got, ok)
	}
}

// TestSharedAdjudicationBeatsTheLocalCache pins the shared face as the
// authority: a replica that cached a row must stop answering for it the
// moment another instance deletes the shared row. A local hit that short-
// circuits the shared read keeps a token valid after its mapping is gone.
func TestSharedAdjudicationBeatsTheLocalCache(t *testing.T) {
	ctx := context.Background()
	st, _ := sharedStore(t)
	a := registry.New(time.Now, registry.WithStateStore(st))
	t.Cleanup(func() { a.Close(ctx) })
	b := registry.New(time.Now, registry.WithStateStore(st))
	t.Cleanup(func() { b.Close(ctx) })

	e := scopedEntry("task-1", "src-a")
	if err := a.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, ok := b.ByDaemonInSourceWorkspace("src-a", "ws-shared", e.DaemonID); !ok {
		t.Fatal("the shared row did not resolve the daemon on the second replica")
	}
	// Another instance reclaimed the credential: the shared row is gone.
	if err := st.DeleteEntry(ctx, scope.Key("src-a", "ws-shared"), "task-1"); err != nil {
		t.Fatalf("DeleteEntry: %v", err)
	}
	if _, ok := b.ByDaemonInSourceWorkspace("src-a", "ws-shared", e.DaemonID); ok {
		t.Fatal("the second replica still authorized the token from its local cache")
	}
	if _, ok := b.Get("task-1", e.ScopeKey()); ok {
		t.Fatal("the stale local row survived the authoritative miss")
	}
}

// TestSlotsAreReleasedInTheReservationDomain pins the two-level counter's
// release domain: reservations are taken under the bare workspace, so a
// terminal or discarded attempt must return its slots there. Releasing the
// composite source/workspace key leaves the workspace counter full and blocks
// every later claim.
func TestSlotsAreReleasedInTheReservationDomain(t *testing.T) {
	ctx := context.Background()
	st, coord := sharedStore(t)
	reg := registry.New(time.Now, registry.WithStateStore(st))
	t.Cleanup(func() { reg.Close(ctx) })

	take := func(t *testing.T) []registry.SlotRetirement {
		t.Helper()
		globalID, ok, err := coord.ReserveSlot(ctx, "", 4)
		if err != nil || !ok {
			t.Fatalf("reserve global slot = %q, %v, %v", globalID, ok, err)
		}
		workspaceID, ok, err := coord.ReserveSlot(ctx, "ws-shared", 4)
		if err != nil || !ok {
			t.Fatalf("reserve workspace slot = %q, %v, %v", workspaceID, ok, err)
		}
		return []registry.SlotRetirement{
			{WorkspaceID: "ws-shared", ID: workspaceID},
			{ID: globalID},
		}
	}
	assertSlots := func(t *testing.T) {
		t.Helper()
		for _, ws := range []string{"", "ws-shared"} {
			if n, err := coord.Slots(ctx, ws); err != nil || n != 0 {
				t.Fatalf("shared slots[%q] = %d, %v; want the attempt's slot returned", ws, n, err)
			}
		}
	}

	// Terminal: MarkTerminal returns the row's slot.
	terminal := scopedEntry("task-terminal", "src-a")
	if err := reg.PutTakingSlots(terminal, take(t)); err != nil {
		t.Fatalf("PutTakingSlots: %v", err)
	}
	if n, _ := coord.Slots(ctx, "ws-shared"); n != 1 {
		t.Fatalf("workspace slots before the verdict = %d, want the row holding one", n)
	}
	if _, _, err := reg.MarkTerminal("task-terminal", registry.ResultCompleted, "", time.Now(), terminal.ScopeKey()); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	assertSlots(t)

	// Discard: Delete releases a row that never reached a verdict.
	discarded := scopedEntry("task-discarded", "src-a")
	if err := reg.PutTakingSlots(discarded, take(t)); err != nil {
		t.Fatalf("PutTakingSlots: %v", err)
	}
	if err := reg.Delete("task-discarded", discarded.ScopeKey()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	assertSlots(t)
}

// TestSameTaskIDOnTwoSourcesIsTwoAttempts pins the attempt identity: one
// upstream task id on two registered servers is two rows, not one duplicate.
// A bare-id read is ambiguous, the scoped read resolves each, and a verdict
// on one leaves the other running.
func TestSameTaskIDOnTwoSourcesIsTwoAttempts(t *testing.T) {
	reg := registry.New(time.Now)
	a := scopedEntry("task-1", "src-a")
	b := scopedEntry("task-1", "src-b")
	if err := reg.Put(a); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := reg.Put(b); err != nil {
		t.Fatalf("Put b: %v", err)
	}

	if _, ok := reg.Get("task-1"); ok {
		t.Fatal("a bare task id resolved two attempts to one of them")
	}
	if got, ok := reg.Get("task-1", a.ScopeKey()); !ok || got.SourceID != "src-a" {
		t.Fatalf("scoped read a = %+v, %v", got, ok)
	}
	if got, ok := reg.Get("task-1", b.ScopeKey()); !ok || got.SourceID != "src-b" {
		t.Fatalf("scoped read b = %+v, %v", got, ok)
	}

	if _, _, err := reg.MarkTerminal("task-1", registry.ResultCompleted, "", time.Now(), a.ScopeKey()); err != nil {
		t.Fatalf("MarkTerminal a: %v", err)
	}
	if !reg.IsDoneInSource("src-a", "ws-shared", "task-1") {
		t.Fatal("the verdict on src-a is not in its own done partition")
	}
	if reg.IsDoneInSource("src-b", "ws-shared", "task-1") {
		t.Fatal("the verdict on src-a settled the same task id on src-b")
	}
	if got, ok := reg.Get("task-1", b.ScopeKey()); !ok || got.IsTerminal() {
		t.Fatalf("src-b entry = %+v, %v; want it still running", got, ok)
	}
}

// TestOpsReadDoesNotEvictTheLocalRow pins the read-only face: a scoped read
// that misses in the shared store must not delete the local copy another
// path still serves. The detail endpoint is a projection, not a reclaim.
func TestOpsReadDoesNotEvictTheLocalRow(t *testing.T) {
	ctx := context.Background()
	st, _ := sharedStore(t)
	reg := registry.New(time.Now, registry.WithStateStore(st))
	t.Cleanup(func() { reg.Close(ctx) })

	e := scopedEntry("task-1", "src-a")
	reg.Cache(e)
	if err := st.DeleteEntry(ctx, scope.Key("src-a", "ws-shared"), "task-1"); err != nil {
		t.Fatalf("DeleteEntry: %v", err)
	}
	if _, ok := reg.GetInScope(e.ScopeKey(), "task-1"); ok {
		t.Fatal("the shared miss answered from an evicted/ignored local row")
	}
	if _, ok := reg.Get("task-1", e.ScopeKey()); !ok {
		t.Fatal("the read-only projection evicted the correct local row")
	}
}
