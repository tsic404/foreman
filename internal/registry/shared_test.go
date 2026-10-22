package registry

import (
	"context"
	"errors"
	"testing"
	"time"
)

// mergeFailingStore is the shared face of one token-digest write: every merge
// answers the programmed error. The embedded interface keeps the fixture free
// of the unrelated shared methods; the explicit StateStore methods are what
// New needs to promote it to the shared face.
type mergeFailingStore struct {
	SharedStore

	mergeErr error
	merges   []EntryPatch
}

func (f *mergeFailingStore) Shared() bool { return true }

func (f *mergeFailingStore) MergeEntry(_ context.Context, _, _ string, p EntryPatch) error {
	f.merges = append(f.merges, p)
	return f.mergeErr
}

func (f *mergeFailingStore) PutEntry(context.Context, string, string, TaskEntry) error { return nil }
func (f *mergeFailingStore) PutSettled(context.Context, string, string, TaskEntry) error {
	return nil
}
func (f *mergeFailingStore) GetSettled(context.Context, string, string) (TaskEntry, bool, error) {
	return TaskEntry{}, false, nil
}
func (f *mergeFailingStore) Revoke(context.Context, string, string, time.Time) error { return nil }
func (f *mergeFailingStore) IsRevoked(context.Context, string, string) (bool, error) {
	return false, nil
}
func (f *mergeFailingStore) Ping(context.Context) error { return nil }

// TestSetTokenHashReportsTheSharedWriteFailure pins the digest write's failure
// face: the caller must see the store's error (the credential cannot be
// adjudicated on another instance) while the local digest survives for this
// instance's reclaim.
func TestSetTokenHashReportsTheSharedWriteFailure(t *testing.T) {
	storeErr := errors.New("shared state unavailable")
	store := &mergeFailingStore{mergeErr: storeErr}
	var failures []string
	r := New(time.Now,
		WithStateStore(store),
		WithStateErrorHook(func(op, taskID string, err error) { failures = append(failures, op) }),
	)
	r.Cache(TaskEntry{TaskID: "task-1", WorkspaceID: "ws-1", State: StatePending})

	if err := r.SetTokenHash("task-1", "digest-1"); !errors.Is(err, storeErr) {
		t.Fatalf("SetTokenHash error = %v, want the store's failure", err)
	}
	if len(store.merges) != 1 || store.merges[0].TokenHash == nil || *store.merges[0].TokenHash != "digest-1" {
		t.Fatalf("merges = %+v, want one digest patch", store.merges)
	}
	if len(failures) != 1 || failures[0] != "merge_entry" {
		t.Fatalf("state failures = %v, want one merge_entry", failures)
	}
	if e, ok := r.Get("task-1"); !ok || e.TokenHash != "digest-1" {
		t.Fatalf("local entry = %+v (ok=%v), want the digest kept for the in-process reclaim", e, ok)
	}
}

// TestSetTokenHashRejectsAMissingEntry pins the fail-closed face of the write's
// precondition: with no live entry nothing records the digest, so the caller
// must not be told the write landed and issue the credential anyway.
func TestSetTokenHashRejectsAMissingEntry(t *testing.T) {
	store := &mergeFailingStore{mergeErr: errors.New("merge must not be reached")}
	r := New(time.Now, WithStateStore(store))

	if err := r.SetTokenHash("absent", "digest-1"); !errors.Is(err, ErrNoLiveEntry) {
		t.Fatalf("SetTokenHash error = %v, want ErrNoLiveEntry", err)
	}
	if len(store.merges) != 0 {
		t.Fatalf("merges = %+v, want none without a live entry", store.merges)
	}
}
