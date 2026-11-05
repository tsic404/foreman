package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

// b64URLAlphabet is the raw-base64url alphabet in decode order.
const b64URLAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

// rewriteTrailingPadding re-spells a raw-base64url segment with a different
// final character while keeping every decoded byte: the last character of a
// 43-character (32-byte) segment carries four data bits plus two padding bits,
// so flipping its lowest bit is invisible to a non-strict decoder.
func rewriteTrailingPadding(t *testing.T, segment string) string {
	t.Helper()
	if len(segment)%4 == 0 || len(segment)%4 == 1 {
		t.Fatalf("segment %q has no trailing padding bits to rewrite", segment)
	}
	i := strings.IndexByte(b64URLAlphabet, segment[len(segment)-1])
	if i < 0 {
		t.Fatalf("segment %q does not end in a base64url character", segment)
	}
	return segment[:len(segment)-1] + string(b64URLAlphabet[i^1])
}

// splitToken returns the payload and signature segments of a wire token.
func splitToken(t *testing.T, token string) (string, string) {
	t.Helper()
	body, found := strings.CutPrefix(token, TokenPrefix)
	if !found {
		t.Fatalf("%q carries no Job Token prefix", token)
	}
	payloadSegment, sigSegment, ok := strings.Cut(body, ".")
	if !ok {
		t.Fatalf("%q has no signature segment", token)
	}
	return payloadSegment, sigSegment
}

// TestVerifyRejectsRewrittenSignaturePadding pins the single-wire-form rule
// the revocation key rests on (§6.1: the set is keyed by the token's digest,
// ADR-017: the digest written by the reclaim must be the digest the check
// computes). base64url admits several spellings of the same signature bytes;
// accepting one of them makes the token string non-unique, so a same-name
// reuse (the one revocation hit face) would look up a digest nobody wrote.
func TestVerifyRejectsRewrittenSignaturePadding(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	jobs := fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1", WorkspaceID: "ws-1"}}
	iss := newTestIssuer(now, jobs, fakeDone{})
	token, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := iss.Verify(token, RequestScope{}); err != nil {
		t.Fatalf("issued token: %v", err)
	}

	payloadSegment, sigSegment := splitToken(t, token)
	rewritten := TokenPrefix + payloadSegment + "." + rewriteTrailingPadding(t, sigSegment)

	// Premise of the negative case: the rewrite keeps the signature bytes, so
	// the HMAC compare alone cannot tell the two strings apart.
	want, err := base64.RawURLEncoding.DecodeString(sigSegment)
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	_, rewrittenSegment := splitToken(t, rewritten)
	got, err := base64.RawURLEncoding.DecodeString(rewrittenSegment)
	if err != nil {
		t.Fatalf("decode rewritten signature: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("fixture: rewrite changed the signature bytes: %x vs %x", got, want)
	}

	_, err = iss.Verify(rewritten, RequestScope{})
	if !errors.Is(err, ErrMalformedToken) {
		t.Fatalf("rewritten padding: err = %v, want ErrMalformedToken", err)
	}
	if code := HTTPStatus(err); code != http.StatusUnauthorized {
		t.Errorf("rewritten padding: status = %d, want 401", code)
	}
	if reason := Reason(err); reason != "malformed" {
		t.Errorf("rewritten padding: reason = %q, want the closed-enum value malformed", reason)
	}
}

// TestVerifyRejectsRewrittenPayloadPadding covers the other segment: the
// signature is recomputed over the re-spelled payload, so only the canonical
// form check can reject the token. The payload is one byte past a three-byte
// boundary, which is what leaves padding bits in its last character.
func TestVerifyRejectsRewrittenPayloadPadding(t *testing.T) {
	payload := `{"j":"fm-100","t":"task-1","w":"ws-1","e":9999999999}`
	segment := base64.RawURLEncoding.EncodeToString([]byte(payload))
	rewrittenSegment := rewriteTrailingPadding(t, segment)
	if decoded, err := base64.RawURLEncoding.DecodeString(rewrittenSegment); err != nil ||
		string(decoded) != payload {
		t.Fatalf("fixture: rewrite changed the payload (%q, %v)", decoded, err)
	}

	mac := hmac.New(sha256.New, testKey)
	mac.Write([]byte(TokenPrefix + rewrittenSegment))
	token := TokenPrefix + rewrittenSegment + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	iss := newTestIssuer(time.Unix(1_700_000_000, 0), fakeRegistry{}, fakeDone{})
	if _, err := iss.Verify(token, RequestScope{}); !errors.Is(err, ErrMalformedToken) {
		t.Fatalf("rewritten payload: err = %v, want ErrMalformedToken", err)
	}
}

// TestHashKeysTheCanonicalToken pins the revocation key's form (§6.1): the
// digest is taken over the canonical wire form, so a re-spelled segment that
// carries the same bytes maps to the digest the reclaim wrote. Keying the set
// by the received spelling would make the revocation miss for every rewrite.
func TestHashKeysTheCanonicalToken(t *testing.T) {
	iss := newTestIssuer(time.Unix(1_700_000_000, 0), fakeRegistry{}, fakeDone{})
	token, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	payloadSegment, sigSegment := splitToken(t, token)
	rewritten := TokenPrefix + payloadSegment + "." + rewriteTrailingPadding(t, sigSegment)

	if got, want := iss.Hash(rewritten), iss.Hash(token); got != want {
		t.Fatalf("Hash(rewritten) = %s, want the canonical digest %s", got, want)
	}
}

// TestVerifyRevokedRejectsRewrittenSignatureOnAReusedEntry is ADR-017's only
// revocation hit face: the entry exists again (a re-dispatched task or a
// rebuilt same-name Job) while the predecessor's digest is revoked. The
// rewritten token must not slip past the revocation lookup.
func TestVerifyRevokedRejectsRewrittenSignatureOnAReusedEntry(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	jobs := fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1", WorkspaceID: "ws-1"}}
	iss := newTestIssuer(now, jobs, fakeDone{})
	token, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	iss.Revoke(iss.Hash(token), "ws-1")

	payloadSegment, sigSegment := splitToken(t, token)
	rewritten := TokenPrefix + payloadSegment + "." + rewriteTrailingPadding(t, sigSegment)
	if _, err := iss.Verify(rewritten, RequestScope{}); err == nil {
		t.Fatal("rewritten token of a revoked digest passed verification on a reused entry")
	}
}
