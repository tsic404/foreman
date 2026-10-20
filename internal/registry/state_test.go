package registry

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeStateStore records the registry's shared-state traffic and answers the
// restore scan from programmed rows.
type fakeStateStore struct {
	puts    map[string]TaskEntry
	deletes []string
	settled map[string]TaskEntry
	revoked map[string]time.Time

	pingErr    error
	listErr    error
	listAnswer []TaskEntry
}

func newFakeStateStore() *fakeStateStore {
	return &fakeStateStore{
		puts:    make(map[string]TaskEntry),
		settled: make(map[string]TaskEntry),
		revoked: make(map[string]time.Time),
	}
}

func (f *fakeStateStore) PutEntry(_ context.Context, _, taskID string, e TaskEntry) error {
	f.puts[taskID] = e
	return nil
}
func (f *fakeStateStore) GetEntry(context.Context, string, string) (TaskEntry, bool, error) {
	return TaskEntry{}, false, nil
}
func (f *fakeStateStore) ListEntries(context.Context) ([]TaskEntry, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.listAnswer, nil
}
func (f *fakeStateStore) DeleteEntry(_ context.Context, _, taskID string) error {
	f.deletes = append(f.deletes, taskID)
	delete(f.puts, taskID)
	return nil
}
func (f *fakeStateStore) PutSettled(_ context.Context, _, taskID string, e TaskEntry) error {
	f.settled[taskID] = e
	return nil
}
func (f *fakeStateStore) GetSettled(context.Context, string, string) (TaskEntry, bool, error) {
	return TaskEntry{}, false, nil
}
func (f *fakeStateStore) ListSettled(context.Context) ([]TaskEntry, error) {
	var out []TaskEntry
	for _, e := range f.settled {
		out = append(out, e)
	}
	return out, nil
}
func (f *fakeStateStore) Revoke(_ context.Context, _, hash string, until time.Time) error {
	f.revoked[hash] = until
	return nil
}
func (f *fakeStateStore) IsRevoked(_ context.Context, _, hash string) (bool, error) {
	_, ok := f.revoked[hash]
	return ok, nil
}
func (f *fakeStateStore) ListRevoked(context.Context) (map[string]time.Time, error) {
	return f.revoked, nil
}
func (f *fakeStateStore) Ping(context.Context) error { return f.pingErr }

// flushState drains the ordered writer so the assertions read the applied
// store state (the writer is asynchronous by design: no store I/O runs under
// the registry lock).
func flushState(t *testing.T, r *Registry) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.FlushState(ctx); err != nil {
		t.Fatalf("FlushState: %v", err)
	}
}

func TestRegistryPersistsTransitions(t *testing.T) {
	store := newFakeStateStore()
	r := New(fixedClock(), WithStateStore(store))
	e := entryFor("t1")
	e.WorkspaceID = "ws-1"

	if err := r.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	flushState(t, r)
	if got, ok := store.puts["t1"]; !ok || got.State != StatePending {
		t.Fatalf("Put did not persist the row: %+v", got)
	}

	if _, ok := r.MarkJobCreated("t1", testNow); !ok {
		t.Fatal("MarkJobCreated failed")
	}
	flushState(t, r)
	if got := store.puts["t1"]; got.State != StateJobCreated {
		t.Fatalf("MarkJobCreated persisted state %q, want job_created", got.State)
	}

	if _, ok := r.MarkDaemonRegistered("fm-t1", testNow); !ok {
		t.Fatal("MarkDaemonRegistered failed")
	}
	flushState(t, r)
	if got := store.puts["t1"]; got.State != StateDaemonRegistered {
		t.Fatalf("MarkDaemonRegistered persisted state %q", got.State)
	}

	if _, ok := r.MarkDelivering("fm-t1", testNow, []byte(`{"id":"t1"}`)); !ok {
		t.Fatal("MarkDelivering failed")
	}
	flushState(t, r)
	got := store.puts["t1"]
	if got.State != StateDelivering || got.Deliveries != 1 || !got.DeliveredAt.Equal(testNow) {
		t.Fatalf("MarkDelivering persisted %+v", got)
	}
	if string(got.Payload) != `{"id":"t1"}` {
		t.Fatal("the delivery body must persist in the mapping record (ADR-046 决策 0)")
	}

	if _, ok := r.MarkStarted("t1", testNow); !ok {
		t.Fatal("MarkStarted failed")
	}
	flushState(t, r)
	if got := store.puts["t1"]; got.State != StateRunning || got.StartedAt.IsZero() {
		t.Fatalf("MarkStarted persisted %+v", got)
	}

	if _, _, err := r.MarkTerminal("t1", ResultCompleted, "", testNow); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	flushState(t, r)
	if got := store.puts["t1"]; !got.IsTerminal() || !got.IsDraining() {
		t.Fatalf("MarkTerminal persisted %+v", got)
	}
	settled, ok := store.settled["t1"]
	if !ok || !settled.IsTerminal() {
		t.Fatalf("MarkTerminal did not persist the settled record: %+v", settled)
	}
	if settled.TokenHash != "" || settled.Payload != nil {
		t.Fatal("the settled record carries memory-only secrets")
	}

	if err := r.Delete("t1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	flushState(t, r)
	if len(store.deletes) != 1 || store.deletes[0] != "t1" {
		t.Fatalf("Delete persisted %v, want [t1]", store.deletes)
	}
}

func TestRegistryRestoreRebuildsIndexesAndRuntime(t *testing.T) {
	store := newFakeStateStore()
	restoredAt := time.Date(2026, 10, 4, 11, 0, 0, 0, time.UTC)
	store.listAnswer = []TaskEntry{{
		TaskID:       "t1",
		WorkspaceID:  "ws-1",
		JobName:      "fm-t1",
		DaemonID:     "fm-t1",
		JobRuntimeID: "rt-t1",
		State:        StateRunning,
		StartedAt:    restoredAt,
		Payload:      []byte(`{"id":"t1"}`),
		TokenHash:    "digest",
	}}
	store.settled["t9"] = TaskEntry{
		TaskID:      "t9",
		WorkspaceID: "ws-1",
		State:       StateTerminal,
		Result:      ResultCompleted,
		CompletedAt: restoredAt,
	}

	r := New(fixedClock(), WithStateStore(store))
	entries, answered, err := r.Restore(context.Background())
	if err != nil || !answered || len(entries) != 1 {
		t.Fatalf("Restore = %d entries, answered %v, err %v", len(entries), answered, err)
	}
	got, ok := r.ByJob("fm-t1")
	if !ok {
		t.Fatal("restored entry not indexed by Job name")
	}
	if got.State != StateRunning || !got.StartedAt.Equal(restoredAt) {
		t.Fatalf("restored entry = %+v", got)
	}
	if string(got.Payload) != `{"id":"t1"}` || got.TokenHash != "digest" {
		t.Fatal("the mapping record's non-projected faces were not restored")
	}
	if _, ok := r.ByDaemon("fm-t1"); !ok {
		t.Fatal("restored entry not indexed by daemon id")
	}
	if _, ok := r.RuntimeByID("rt-t1"); !ok {
		t.Fatal("restored entry's job runtime row missing")
	}
	if settled, ok := r.Settled("t9"); !ok || settled.Result != ResultCompleted {
		t.Fatalf("settled record not restored: %+v, %v", settled, ok)
	}
	if n := r.Inflight(); n != 1 {
		t.Fatalf("Inflight = %d, want 1", n)
	}
}

func TestRegistryRestoreReportsStoreOutage(t *testing.T) {
	store := newFakeStateStore()
	store.pingErr = errors.New("connection refused")
	r := New(fixedClock(), WithStateStore(store))
	entries, answered, err := r.Restore(context.Background())
	// A store outage is an error (the caller logs registry.restore_failed)
	// and a miss: the K8s rebuild owns recovery.
	if err == nil {
		t.Fatal("Restore swallowed the store outage")
	}
	if answered || len(entries) != 0 {
		t.Fatalf("Restore answered %v with %d entries, want a miss", answered, len(entries))
	}
}

func TestRegistryWithoutStoreStaysInMemory(t *testing.T) {
	r := New(fixedClock())
	if err := r.Put(entryFor("t1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, answered, err := r.Restore(context.Background()); err != nil || answered {
		t.Fatalf("Restore without a store answered %v, %v", answered, err)
	}
}

// slowStateStore blocks every write until released: it proves the registry
// lock is never held across store I/O (C13 every 5s per entry and C4 per
// renewal would otherwise serialize the daemon face behind a slow store).
type slowStateStore struct {
	*fakeStateStore
	gate chan struct{}
}

func (s *slowStateStore) PutEntry(ctx context.Context, ws, taskID string, e TaskEntry) error {
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.fakeStateStore.PutEntry(ctx, ws, taskID, e)
}

func TestRegistryReadsDoNotBlockOnAStalledStore(t *testing.T) {
	inner := newFakeStateStore()
	store := &slowStateStore{fakeStateStore: inner, gate: make(chan struct{})}
	r := New(fixedClock(), WithStateStore(store))
	if err := r.Put(entryFor("t1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The writer is parked on the first write; a registry read must still
	// answer immediately (the put returned without waiting for the store).
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			if _, ok := r.Get("t1"); !ok {
				t.Error("entry vanished while the writer was stalled")
				return
			}
			r.ByDaemon("fm-t1")
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("registry reads blocked on the stalled store writer")
	}
	close(store.gate)
	flushState(t, r)
	if _, ok := inner.puts["t1"]; !ok {
		t.Fatal("the queued write never landed after the writer was released")
	}
}

func TestRegistryFlushReportsAStuckQueue(t *testing.T) {
	inner := newFakeStateStore()
	store := &slowStateStore{fakeStateStore: inner, gate: make(chan struct{})}
	r := New(fixedClock(), WithStateStore(store))
	if err := r.Put(entryFor("t1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := r.FlushState(ctx); err == nil {
		t.Fatal("FlushState reported success with a write still in flight")
	}
	close(store.gate)
	flushState(t, r)
}
