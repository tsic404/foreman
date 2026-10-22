package recovery

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
	"github.com/tsic404/foreman/internal/state"
)

// digestFailingStore is an otherwise healthy shared store that refuses the
// token-digest merge: the row cannot take a digest while every other command
// answers, so a scenario isolates the digest write from a full outage.
type digestFailingStore struct {
	state.CoordStore
	phaseOne state.Store

	mu      sync.Mutex
	failing bool
	merges  int
}

// arm refuses every following digest merge.
func (s *digestFailingStore) arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failing = true
}

// heal lets the digest merges through again.
func (s *digestFailingStore) heal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failing = false
}

// mergeFailures counts the refused digest merges.
func (s *digestFailingStore) mergeFailures() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.merges
}

func (s *digestFailingStore) MergeEntry(ctx context.Context, ws, taskID string, p registry.EntryPatch) error {
	s.mu.Lock()
	fail := p.TokenHash != nil && s.failing
	if fail {
		s.merges++
	}
	s.mu.Unlock()
	if fail {
		return errors.New("shared state: merge unavailable")
	}
	return s.CoordStore.MergeEntry(ctx, ws, taskID, p)
}

// The stage-one face of the same store.

func (s *digestFailingStore) PutEntry(ctx context.Context, ws, taskID string, e registry.TaskEntry) error {
	return s.phaseOne.PutEntry(ctx, ws, taskID, e)
}

func (s *digestFailingStore) PutSettled(ctx context.Context, ws, taskID string, e registry.TaskEntry) error {
	return s.phaseOne.PutSettled(ctx, ws, taskID, e)
}

func (s *digestFailingStore) GetSettled(ctx context.Context, ws, taskID string) (registry.TaskEntry, bool, error) {
	return s.phaseOne.GetSettled(ctx, ws, taskID)
}

func (s *digestFailingStore) DeleteSettled(ctx context.Context, ws, taskID string) error {
	return s.phaseOne.DeleteSettled(ctx, ws, taskID)
}

func (s *digestFailingStore) Revoke(ctx context.Context, ws, hash string, until time.Time) error {
	return s.phaseOne.Revoke(ctx, ws, hash, until)
}

func (s *digestFailingStore) IsRevoked(ctx context.Context, ws, hash string) (bool, error) {
	return s.phaseOne.IsRevoked(ctx, ws, hash)
}

func (s *digestFailingStore) Ping(ctx context.Context) error { return s.phaseOne.Ping(ctx) }

// TestContinuationKeepsTheEntryThroughASustainedDigestOutage is §4.3's
// continuation face under ADR-046 决策 7: a pre-delivery row whose Job was
// never created is continued, never read as a missing Job. While the digest
// cannot reach the shared row every round retries the continuation and creates
// nothing — no Job, no Secret, no failure report — and the entry stays; once
// the store takes the digest, the same round completes the claim.
func TestContinuationKeepsTheEntryThroughASustainedDigestOutage(t *testing.T) {
	mr := miniredis.RunT(t)
	st, err := state.New(state.Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    time.Hour,
		Grace:       time.Minute,
	})
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	coord := st.(state.CoordStore)
	store := &digestFailingStore{CoordStore: coord, phaseOne: st}
	store.arm()

	reg := registry.New(time.Now,
		registry.WithDrainTimeout(schedConfig().JobDrainTimeout),
		registry.WithStateStore(store))
	jobs := newFakeJobClient()
	// The server still holds the task: the missing-Job compensation would
	// report job_missing for it rather than settle a "task gone".
	const taskID = "task-1"
	server := newFakeServerClient(map[string]string{taskID: "dispatched"})
	pending, err := scheduler.NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	sched, err := scheduler.New(schedConfig(), reg, jobs, stubBuilder{}, server, nil,
		scheduler.WithPendingReports(pending),
		scheduler.WithRevoker(&fakeRevoker{}),
		scheduler.WithProbeRegistry(registry.NewProbeRegistry()),
		scheduler.WithSharedStore(coord, "instance-1", time.Second))
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}

	// The row a claim persisted before its process died: pending, payload in
	// the shared row, no Job ever created, and the claim old enough that a
	// missing Job would be compensated without further grace.
	if err := reg.Put(registry.TaskEntry{
		TaskID:      taskID,
		WorkspaceID: "w1",
		JobName:     fakeJobName(taskID),
		DaemonID:    fakeJobName(taskID),
		State:       registry.StatePending,
		Payload:     []byte(`{"id":"task-1","workspace_id":"w1"}`),
		ClaimedAt:   time.Now().Add(-time.Hour),
		Attempt:     1,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rec, err := New(Config{ReconcileInterval: time.Second}, reg, &fakeObjects{}, server, sched)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	// Shrunk to zero: every round after the first missing observation is past
	// the window, so an entry that entered it would be settled as job_missing.
	rec.jobGoneWindow = 0

	ctx := context.Background()
	roundErrs := make([]error, 0, 3)
	for round := 1; round <= 3; round++ {
		roundErrs = append(roundErrs, rec.Reconcile(ctx))
	}
	if got := server.forwarded(scheduler.EPFail); len(got) != 0 {
		t.Fatalf("failure reports = %+v, want none: a shared-state outage is not a task failure", got)
	}
	if n := store.mergeFailures(); n < 3 {
		t.Fatalf("digest merges = %d, want the continuation retried every round", n)
	}
	if e, ok := reg.Get(taskID); !ok || e.State != registry.StatePending {
		t.Fatalf("local entry = %+v (ok=%v), want the pending entry kept", e, ok)
	}
	if reg.IsDone("w1", taskID) {
		t.Fatal("the task was settled although it never ran")
	}
	row, ok, err := coord.GetEntry(ctx, "w1", taskID)
	if err != nil || !ok || row.State != registry.StatePending || row.TokenHash != "" {
		t.Fatalf("shared row = %+v (ok=%v, err=%v), want the pending row without a digest", row, ok, err)
	}
	if jobs.has(fakeJobName(taskID)) {
		t.Fatal("a Job was created although the digest never reached the shared row")
	}
	if jobs.hasSecret(fakeJobName(taskID) + "-cred") {
		t.Fatal("a credential Secret was created although the digest never reached the shared row")
	}
	// The outage is surfaced, never swallowed: the loop's round reports it.
	for round, err := range roundErrs {
		if !errors.Is(err, scheduler.ErrStoreUnavailable) {
			t.Fatalf("round %d: error = %v, want ErrStoreUnavailable", round+1, err)
		}
	}

	// The store recovers: the next round continues the claim and the digest
	// lands on the row.
	store.heal()
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile after recovery: %v", err)
	}
	if !jobs.has(fakeJobName(taskID)) || !jobs.hasSecret(fakeJobName(taskID)+"-cred") {
		t.Fatal("the recovered round did not continue the claim")
	}
	row, ok, err = coord.GetEntry(ctx, "w1", taskID)
	if err != nil || !ok || row.TokenHash != "hash-of-"+taskID {
		t.Fatalf("shared row = %+v (ok=%v, err=%v), want the token digest", row, ok, err)
	}
}
