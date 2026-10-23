package auth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeRevokeStore keeps the workspace dimension the shared face carries: the
// partition is what the restart seed must preserve (§6.1).
type fakeRevokeStore struct {
	revoked map[string]time.Time // workspace + NUL + digest
	// readErr is the per-request read outage (§10): ListRevoked still answers
	// (the seed needs it), every IsRevoked fails. It is how a test pins the
	// in-process set as the only adjudication source after a restart.
	readErr error
}

func newFakeRevokeStore() *fakeRevokeStore {
	return &fakeRevokeStore{revoked: map[string]time.Time{}}
}

func (f *fakeRevokeStore) Revoke(_ context.Context, ws, hash string, until time.Time) error {
	f.revoked[ws+"\x00"+hash] = until
	return nil
}
func (f *fakeRevokeStore) IsRevoked(_ context.Context, ws, hash string) (bool, error) {
	if f.readErr != nil {
		return false, f.readErr
	}
	_, ok := f.revoked[ws+"\x00"+hash]
	return ok, nil
}
func (f *fakeRevokeStore) ListRevoked(context.Context) (map[string]map[string]time.Time, error) {
	out := map[string]map[string]time.Time{}
	for key, until := range f.revoked {
		ws, hash, found := strings.Cut(key, "\x00")
		if !found {
			continue
		}
		if out[ws] == nil {
			out[ws] = map[string]time.Time{}
		}
		out[ws][hash] = until
	}
	return out, nil
}

func TestRevokeWritesSharedStoreAndSeedsCache(t *testing.T) {
	store := newFakeRevokeStore()
	reg := fakeRegistry{"fm-task-1": {JobRuntimeID: "rt-1", WorkspaceID: "ws-1"}}
	iss, err := NewIssuer(testKey, reg, fakeDone{}, time.Hour, WithRevocationStore(store))
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	token, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	hash := iss.Hash(token)
	iss.Revoke(hash, "ws-1")
	if _, ok := store.revoked["ws-1\x00"+hash]; !ok {
		t.Fatal("Revoke did not write the shared store")
	}
	// The in-process set (written through by Revoke) rejects.
	if _, err := iss.Verify(token, RequestScope{}); err == nil {
		t.Fatal("revoked token still verifies")
	}

	// A fresh issuer (restart) seeds its cache from the store and rejects too.
	iss2, err := NewIssuer(testKey, reg, fakeDone{}, time.Hour, WithRevocationStore(store))
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	if err := iss2.RestoreRevocations(context.Background()); err != nil {
		t.Fatalf("RestoreRevocations: %v", err)
	}
	if _, err := iss2.Verify(token, RequestScope{}); err == nil {
		t.Fatal("restarted issuer accepted a revoked token")
	}
}

// TestRestoreRevocationsKeepsWorkspacePartition pins §6.1's partition across a
// restart seed: a digest the shared set holds under another workspace must not
// adjudicate this workspace's token. The digest under test is B's own token
// digest recorded under A, which is the shape a cross-workspace digest
// collision would produce.
func TestRestoreRevocationsKeepsWorkspacePartition(t *testing.T) {
	store := newFakeRevokeStore()
	reg := fakeRegistry{
		"fm-task-a": {JobRuntimeID: "rt-a", WorkspaceID: "ws-a"},
		"fm-task-b": {JobRuntimeID: "rt-b", WorkspaceID: "ws-b"},
	}
	iss, err := NewIssuer(testKey, reg, fakeDone{}, time.Hour, WithRevocationStore(store))
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	tokenB, err := iss.Issue("fm-task-b", "task-b", "ws-b", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	ctx := context.Background()
	digestB := iss.Hash(tokenB)
	if err := store.Revoke(ctx, "ws-a", digestB, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	restarted, err := NewIssuer(testKey, reg, fakeDone{}, time.Hour, WithRevocationStore(store))
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	if err := restarted.RestoreRevocations(ctx); err != nil {
		t.Fatalf("RestoreRevocations: %v", err)
	}
	if _, err := restarted.Verify(tokenB, RequestScope{}); err != nil {
		t.Fatalf("ws-b token rejected by a ws-a revocation: %v", err)
	}

	// Control: the same digest recorded under ws-b is this token's own
	// revocation, so the restored set must reject it.
	if err := store.Revoke(ctx, "ws-b", digestB, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	controlled, err := NewIssuer(testKey, reg, fakeDone{}, time.Hour, WithRevocationStore(store))
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	if err := controlled.RestoreRevocations(ctx); err != nil {
		t.Fatalf("RestoreRevocations: %v", err)
	}
	if _, err := controlled.Verify(tokenB, RequestScope{}); !errors.Is(err, ErrRevoked) {
		t.Fatalf("restored ws-b revocation: Verify err = %v, want ErrRevoked", err)
	}
}

// errRevokeRead is the shared read failing after the restart seed.
var errRevokeRead = errors.New("shared revocation read failed")

// seedThenLoseSharedRead restarts the issuer, seeds it from the store, then
// makes every shared read fail: from there the in-process set alone
// adjudicates, which is the only shape in which the seed's workspace grouping
// decides a verdict.
func seedThenLoseSharedRead(t *testing.T, jobs JobLookup, store *fakeRevokeStore) *Issuer {
	t.Helper()
	restarted, err := NewIssuer(testKey, jobs, fakeDone{}, time.Hour, WithRevocationStore(store))
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	if err := restarted.RestoreRevocations(context.Background()); err != nil {
		t.Fatalf("RestoreRevocations: %v", err)
	}
	store.readErr = errRevokeRead
	// Self-proof: the injection is armed, so every verdict that follows can
	// only come from the seed.
	if _, err := store.IsRevoked(context.Background(), "ws-a", "seed-probe"); !errors.Is(err, errRevokeRead) {
		t.Fatalf("read-outage injection did not arm: err = %v, want %v", err, errRevokeRead)
	}
	return restarted
}

// TestRestoreRevocationsSeedsWithoutSharedRead pins the seed as a verdict
// source of its own. The store read is down, so nothing but the restoration's
// workspace grouping reaches adjudication: a seed that flattens the grouping
// (every digest under one empty workspace key) lets the ws-a token verify
// here, while with the store read up the same mutation hides behind the
// per-request lookup and the suite stays green.
func TestRestoreRevocationsSeedsWithoutSharedRead(t *testing.T) {
	store := newFakeRevokeStore()
	reg := fakeRegistry{
		"fm-task-a": {JobRuntimeID: "rt-a", WorkspaceID: "ws-a"},
		"fm-task-b": {JobRuntimeID: "rt-b", WorkspaceID: "ws-b"},
	}
	iss, err := NewIssuer(testKey, reg, fakeDone{}, time.Hour, WithRevocationStore(store))
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	tokenA, err := iss.Issue("fm-task-a", "task-a", "ws-a", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	tokenB, err := iss.Issue("fm-task-b", "task-b", "ws-b", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	ctx := context.Background()
	until := time.Now().Add(time.Hour)
	// A's own revocation, plus B's digest recorded under A: the partition
	// keeps the second out of B's adjudication.
	if err := store.Revoke(ctx, "ws-a", iss.Hash(tokenA), until); err != nil {
		t.Fatalf("Revoke(ws-a, A): %v", err)
	}
	if err := store.Revoke(ctx, "ws-a", iss.Hash(tokenB), until); err != nil {
		t.Fatalf("Revoke(ws-a, B): %v", err)
	}

	restarted := seedThenLoseSharedRead(t, reg, store)
	if _, err := restarted.Verify(tokenA, RequestScope{}); !errors.Is(err, ErrRevoked) {
		t.Fatalf("seeded ws-a revocation: Verify err = %v, want ErrRevoked", err)
	}
	if _, err := restarted.Verify(tokenB, RequestScope{}); err != nil {
		t.Fatalf("ws-a's revocation adjudicated ws-b's token: %v", err)
	}

	// Control: B's own revocation, seeded the same way, must reject — without
	// it the two assertions above would also hold for an empty seed.
	if err := store.Revoke(ctx, "ws-b", iss.Hash(tokenB), until); err != nil {
		t.Fatalf("Revoke(ws-b, B): %v", err)
	}
	controlled := seedThenLoseSharedRead(t, reg, store)
	if _, err := controlled.Verify(tokenB, RequestScope{}); !errors.Is(err, ErrRevoked) {
		t.Fatalf("seeded ws-b revocation: Verify err = %v, want ErrRevoked", err)
	}
}
