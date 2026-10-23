package auth

import (
	"testing"
	"time"
)

// digest is the key the revocation set stores: the hex SHA-256 of the token
// (Issuer.Hash). Both Revoke and IsRevoked take that digest.
func digest(token string) string { return new(Issuer).Hash(token) }

func TestRevocationSetRevokeAndCheck(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	set := NewRevocationSet(fixedClock(now))

	set.Revoke("ws-1", digest("token-a"), now.Add(time.Hour))
	if !set.IsRevoked("ws-1", digest("token-a")) {
		t.Fatal("token-a should be revoked")
	}
	if set.IsRevoked("ws-1", digest("token-b")) {
		t.Fatal("token-b was never revoked")
	}
	// The workspace partitions the set: the same digest revoked in ws-1 must
	// leave ws-2's adjudication untouched (§6.1).
	if set.IsRevoked("ws-2", digest("token-a")) {
		t.Fatal("a ws-1 revocation must not reach ws-2")
	}

	// The set is keyed by digest: revoking the raw token string records a key
	// no digest lookup can match, which is why both sides take the digest.
	set.Revoke("ws-1", "token-c", now.Add(time.Hour))
	if set.IsRevoked("ws-1", digest("token-c")) {
		t.Fatal("revocation must be keyed by digest, not the raw token")
	}
}

func TestRevocationSetSkipsExpiredWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	set := NewRevocationSet(fixedClock(now))

	set.Revoke("ws-1", digest("token-a"), now.Add(-time.Minute))
	if set.IsRevoked("ws-1", digest("token-a")) {
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

	set.Revoke("ws-1", digest("token-a"), start.Add(time.Hour))
	now = start.Add(2 * time.Hour)
	if set.IsRevoked("ws-1", digest("token-a")) {
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

	set.Revoke("ws-1", digest("token-a"), start.Add(time.Hour))
	set.Revoke("ws-1", digest("token-b"), start.Add(time.Hour))

	// Time advances past both windows with no IsRevoked query; the next
	// insert must sweep the lapsed entries.
	now = start.Add(2 * time.Hour)
	set.Revoke("ws-1", digest("token-c"), now.Add(time.Hour))
	if len(set.entries) != 1 {
		t.Fatalf("entries = %d, want 1 (lapsed entries swept on insert)", len(set.entries))
	}
	if !set.IsRevoked("ws-1", digest("token-c")) {
		t.Fatal("token-c should be revoked")
	}
}

func TestIssuerRevokeUsesTokenValidityWindow(t *testing.T) {
	issued := time.Unix(1_700_000_000, 0)
	now := issued
	jobs := fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1", WorkspaceID: "ws-1"}}
	iss, err := NewIssuer(testKey, jobs, fakeDone{}, testTokenTTL, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	token, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	iss.Revoke(iss.Hash(token), "ws-1")
	if !iss.revocations.IsRevoked("ws-1", iss.Hash(token)) {
		t.Fatal("token should be revoked")
	}

	// The revocation entry outlives the skew window; afterwards Verify
	// rejects on expiry, not revocation.
	now = issued.Add(time.Hour).Add(ClockSkew).Add(time.Second)
	if iss.revocations.IsRevoked("ws-1", iss.Hash(token)) {
		t.Fatal("revocation entry should lapse with the validity window")
	}
}
