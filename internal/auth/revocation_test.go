package auth

import (
	"testing"
	"time"
)

func TestRevocationSetRevokeAndCheck(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	set := NewRevocationSet(fixedClock(now))

	set.Revoke("token-a", now.Add(time.Hour))
	if !set.IsRevoked("token-a") {
		t.Fatal("token-a should be revoked")
	}
	if set.IsRevoked("token-b") {
		t.Fatal("token-b was never revoked")
	}
}

func TestRevocationSetSkipsExpiredWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	set := NewRevocationSet(fixedClock(now))

	set.Revoke("token-a", now.Add(-time.Minute))
	if set.IsRevoked("token-a") {
		t.Fatal("a token past its validity window needs no revocation entry")
	}
	if len(set.entries) != 0 {
		t.Fatalf("entries = %d, want 0", len(set.entries))
	}
}

func TestRevocationSetEvictsExpiredEntries(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	now := start
	set := NewRevocationSet(func() time.Time { return now })

	set.Revoke("token-a", start.Add(time.Hour))
	now = start.Add(2 * time.Hour)
	if set.IsRevoked("token-a") {
		t.Fatal("entry should lapse with the token validity window")
	}
	if len(set.entries) != 0 {
		t.Fatalf("expired entry not evicted: %d entries", len(set.entries))
	}
}

func TestRevocationSetSweepOnInsert(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	now := start
	set := NewRevocationSet(func() time.Time { return now })

	set.Revoke("token-a", start.Add(time.Hour))
	set.Revoke("token-b", start.Add(time.Hour))

	// Time advances past both windows with no IsRevoked query; the next
	// insert must sweep the lapsed entries.
	now = start.Add(2 * time.Hour)
	set.Revoke("token-c", now.Add(time.Hour))
	if len(set.entries) != 1 {
		t.Fatalf("entries = %d, want 1 (lapsed entries swept on insert)", len(set.entries))
	}
	if !set.IsRevoked("token-c") {
		t.Fatal("token-c should be revoked")
	}
}

func TestIssuerRevokeUsesTokenValidityWindow(t *testing.T) {
	issued := time.Unix(1_700_000_000, 0)
	now := issued
	jobs := fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1"}}
	iss, err := NewIssuer(testKey, jobs, fakeDone{}, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	token, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	iss.Revoke(token)
	if !iss.revocations.IsRevoked(token) {
		t.Fatal("token should be revoked")
	}

	// The revocation entry outlives the skew window; afterwards Verify
	// rejects on expiry, not revocation.
	now = issued.Add(time.Hour).Add(ClockSkew).Add(time.Second)
	if iss.revocations.IsRevoked(token) {
		t.Fatal("revocation entry should lapse with the validity window")
	}
}
