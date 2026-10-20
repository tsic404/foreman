package auth

import (
	"context"
	"strings"
	"testing"
	"time"
)

type fakeRevokeStore struct {
	revoked map[string]time.Time
}

func newFakeRevokeStore() *fakeRevokeStore { return &fakeRevokeStore{revoked: map[string]time.Time{}} }

func (f *fakeRevokeStore) Revoke(_ context.Context, ws, hash string, until time.Time) error {
	f.revoked[ws+"\x00"+hash] = until
	return nil
}
func (f *fakeRevokeStore) IsRevoked(_ context.Context, ws, hash string) (bool, error) {
	_, ok := f.revoked[ws+"\x00"+hash]
	return ok, nil
}
func (f *fakeRevokeStore) ListRevoked(context.Context) (map[string]time.Time, error) {
	out := map[string]time.Time{}
	for key, until := range f.revoked {
		// key = ws + NUL + digest
		if i := strings.LastIndexByte(key, 0); i >= 0 {
			out[key[i+1:]] = until
		}
	}
	return out, nil
}

func TestRevokeWritesSharedStoreAndSeedsCache(t *testing.T) {
	store := newFakeRevokeStore()
	reg := fakeRegistry{"fm-task-1": {JobRuntimeID: "rt-1"}}
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
