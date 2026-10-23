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
}

func newFakeRevokeStore() *fakeRevokeStore {
	return &fakeRevokeStore{revoked: map[string]time.Time{}}
}

func (f *fakeRevokeStore) Revoke(_ context.Context, ws, hash string, until time.Time) error {
	f.revoked[ws+"\x00"+hash] = until
	return nil
}
func (f *fakeRevokeStore) IsRevoked(_ context.Context, ws, hash string) (bool, error) {
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
