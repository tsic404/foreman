package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

var testKey = bytes.Repeat([]byte{0x42}, MinKeyBytes)

type fakeRegistry map[string]JobEntry

func (f fakeRegistry) ByDaemon(daemonID string) (JobEntry, bool) {
	e, ok := f[daemonID]
	return e, ok
}

type fakeDone map[string]bool

func (f fakeDone) IsDone(taskID string) bool { return f[taskID] }

func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

func newTestIssuer(now time.Time, jobs JobLookup, done DoneIndex) *Issuer {
	i, err := NewIssuer(testKey, jobs, done, WithClock(fixedClock(now)))
	if err != nil {
		panic(err)
	}
	return i
}

func TestIssueTokenFormat(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	iss := newTestIssuer(now, fakeRegistry{}, fakeDone{})

	token, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// Independently reconstruct the contract wire format:
	// "fmj_" + b64url(payload) + "." + b64url(hmac(key, "fmj_"+b64url(payload))).
	wantPayload := `{"j":"fm-task-1","t":"task-1","w":"ws-1","e":1700003600}`
	segment := base64.RawURLEncoding.EncodeToString([]byte(wantPayload))
	mac := hmac.New(sha256.New, testKey)
	mac.Write([]byte(TokenPrefix + segment))
	want := TokenPrefix + segment + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if token != want {
		t.Fatalf("token = %q, want %q", token, want)
	}
	if strings.Contains(token, "=") {
		t.Fatalf("token uses padded encoding: %q", token)
	}
}

func TestIssueRejectsBadArgs(t *testing.T) {
	iss := newTestIssuer(time.Now(), fakeRegistry{}, fakeDone{})
	for _, tc := range []struct {
		name                         string
		jobName, taskID, workspaceID string
		ttl                          time.Duration
	}{
		{"empty job", "", "t", "w", time.Hour},
		{"empty task", "j", "", "w", time.Hour},
		{"empty workspace", "j", "t", "", time.Hour},
		{"zero ttl", "j", "t", "w", 0},
		{"negative ttl", "j", "t", "w", -time.Hour},
	} {
		if _, err := iss.Issue(tc.jobName, tc.taskID, tc.workspaceID, tc.ttl); err == nil {
			t.Errorf("%s: expected error", tc.name)
		}
	}
}

func TestNewIssuerRejectsShortKey(t *testing.T) {
	_, err := NewIssuer([]byte("short"), fakeRegistry{}, fakeDone{})
	if !errors.Is(err, ErrKeyTooShort) {
		t.Fatalf("err = %v, want ErrKeyTooShort", err)
	}
}

func TestVerifyOK(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	jobs := fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1"}}
	iss := newTestIssuer(now, jobs, fakeDone{})
	token, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	claims, err := iss.Verify(token, RequestScope{TaskID: "task-1", RuntimeID: "rid-1"})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.JobName != "fm-task-1" || claims.TaskID != "task-1" || claims.WorkspaceID != "ws-1" {
		t.Fatalf("claims = %+v", claims)
	}
	if want := now.Add(time.Hour); !claims.ExpiresAt.Equal(want) {
		t.Fatalf("ExpiresAt = %s, want %s", claims.ExpiresAt, want)
	}
	if claims.IsLateReplay {
		t.Fatal("IsLateReplay set on the happy path")
	}
}

func TestVerifyMalformed(t *testing.T) {
	validSegment := base64.RawURLEncoding.EncodeToString([]byte(`{"j":"fm-1","t":"task-1","w":"ws-1","e":9999999999}`))
	cases := map[string]string{
		"empty":              "",
		"wrong prefix":       "mat_abc.def",
		"prefix only":        "fmj_",
		"missing segment":    "fmj_abc",
		"empty payload":      "fmj_.c2ln",
		"empty signature":    "fmj_" + validSegment + ".",
		"bad payload b64":    "fmj_!!!.c2ln",
		"payload not json":   "fmj_" + base64.RawURLEncoding.EncodeToString([]byte("not json")) + ".c2ln",
		"payload incomplete": "fmj_" + base64.RawURLEncoding.EncodeToString([]byte(`{"j":"fm-1"}`)) + ".c2ln",
		"payload bad types":  "fmj_" + base64.RawURLEncoding.EncodeToString([]byte(`{"j":1,"t":"task-1","w":"ws-1","e":2}`)) + ".c2ln",
		"bad signature b64":  "fmj_" + validSegment + ".!!!",
	}
	iss := newTestIssuer(time.Now(), fakeRegistry{}, fakeDone{})
	for name, token := range cases {
		_, err := iss.Verify(token, RequestScope{})
		if !errors.Is(err, ErrMalformedToken) {
			t.Errorf("%s: err = %v, want ErrMalformedToken", name, err)
		}
		if got := HTTPStatus(err); got != http.StatusUnauthorized {
			t.Errorf("%s: status = %d, want 401", name, got)
		}
	}
}

func TestVerifyBadSignature(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	jobs := fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1"}}
	iss := newTestIssuer(now, jobs, fakeDone{})
	token, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// Tamper with the last signature character.
	tampered := token[:len(token)-1] + "A"
	if tampered == token {
		tampered = token[:len(token)-1] + "B"
	}
	if _, err := iss.Verify(tampered, RequestScope{}); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("tampered: err = %v, want ErrBadSignature", err)
	}

	// A token signed by another key.
	other := newTestIssuer(now, jobs, fakeDone{})
	other.key = bytes.Repeat([]byte{0x24}, MinKeyBytes)
	foreign, err := other.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := iss.Verify(foreign, RequestScope{}); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("foreign key: err = %v, want ErrBadSignature", err)
	}
}

func TestVerifyExpired(t *testing.T) {
	issued := time.Unix(1_700_000_000, 0)
	jobs := fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1"}}
	token, err := newTestIssuer(issued, jobs, fakeDone{}).Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// Within the clock-skew grace: still valid.
	withinSkew := newTestIssuer(issued.Add(time.Hour).Add(ClockSkew).Add(-time.Second), jobs, fakeDone{})
	if _, err := withinSkew.Verify(token, RequestScope{}); err != nil {
		t.Fatalf("within skew: %v", err)
	}

	// Past expiry + skew: rejected before registry/revocation lookups.
	expired := newTestIssuer(issued.Add(time.Hour).Add(ClockSkew).Add(time.Second), jobs, fakeDone{})
	if _, err := expired.Verify(token, RequestScope{}); !errors.Is(err, ErrExpired) {
		t.Fatalf("err = %v, want ErrExpired", err)
	}
}

func TestVerifyRevoked(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	jobs := fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1"}}
	iss := newTestIssuer(now, jobs, fakeDone{})
	token, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	iss.Revoke(token)
	if _, err := iss.Verify(token, RequestScope{}); !errors.Is(err, ErrRevoked) {
		t.Fatalf("err = %v, want ErrRevoked", err)
	}

	// Revocation precedes the registry lookup: an unknown job still reports revoked.
	other, err := iss.Issue("fm-elsewhere", "task-2", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	iss.Revoke(other)
	if _, err := iss.Verify(other, RequestScope{}); !errors.Is(err, ErrRevoked) {
		t.Fatalf("err = %v, want ErrRevoked", err)
	}
}

func TestVerifyExpiredBeatsRevoked(t *testing.T) {
	issued := time.Unix(1_700_000_000, 0)
	jobs := fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1"}}
	token, err := newTestIssuer(issued, jobs, fakeDone{}).Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	later := newTestIssuer(issued.Add(2*time.Hour), jobs, fakeDone{})
	later.Revoke(token)
	if _, err := later.Verify(token, RequestScope{}); !errors.Is(err, ErrExpired) {
		t.Fatalf("err = %v, want ErrExpired (checked before revocation)", err)
	}
}

func TestVerifyUnknownJob(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	token, err := newTestIssuer(now, fakeRegistry{}, fakeDone{}).Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	cases := map[string]struct {
		jobs JobLookup
		done DoneIndex
		req  RequestScope
	}{
		"registry miss":                  {fakeRegistry{}, fakeDone{}, RequestScope{TaskID: "task-1"}},
		"terminal entry":                 {fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1", IsTerminal: true}}, fakeDone{}, RequestScope{TaskID: "task-1"}},
		"done but non-terminal endpoint": {fakeRegistry{}, fakeDone{"task-1": true}, RequestScope{TaskID: "task-1"}},
	}
	for name, tc := range cases {
		iss := newTestIssuer(now, tc.jobs, tc.done)
		if _, err := iss.Verify(token, tc.req); !errors.Is(err, ErrUnknownJob) {
			t.Errorf("%s: err = %v, want ErrUnknownJob", name, err)
		}
	}
}

func TestVerifyLateReplay(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	token, err := newTestIssuer(now, fakeRegistry{}, fakeDone{}).Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	req := RequestScope{TaskID: "task-1", IsTerminalEndpoint: true}

	// Entry already removed (mapping deleted after terminal).
	iss := newTestIssuer(now, fakeRegistry{}, fakeDone{"task-1": true})
	claims, err := iss.Verify(token, req)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !claims.IsLateReplay || claims.TaskID != "task-1" {
		t.Fatalf("claims = %+v, want IsLateReplay for task-1", claims)
	}

	// Entry still present but terminal.
	iss = newTestIssuer(now, fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1", IsTerminal: true}}, fakeDone{"task-1": true})
	claims, err = iss.Verify(token, req)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !claims.IsLateReplay {
		t.Fatalf("claims = %+v, want IsLateReplay", claims)
	}
}

func TestVerifyScopeMismatch(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	jobs := fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1"}}
	iss := newTestIssuer(now, jobs, fakeDone{})
	token, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	for name, req := range map[string]RequestScope{
		"other task":    {TaskID: "task-2"},
		"other runtime": {TaskID: "task-1", RuntimeID: "rid-2"},
	} {
		_, err := iss.Verify(token, req)
		if !errors.Is(err, ErrScopeMismatch) {
			t.Errorf("%s: err = %v, want ErrScopeMismatch", name, err)
		}
		if got := HTTPStatus(err); got != http.StatusForbidden {
			t.Errorf("%s: status = %d, want 403", name, got)
		}
	}

	// Endpoints without resource identifiers skip the scope check.
	if _, err := iss.Verify(token, RequestScope{}); err != nil {
		t.Fatalf("scope-less request: %v", err)
	}
}

func TestHTTPStatus(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{ErrMalformedToken, http.StatusUnauthorized},
		{ErrBadSignature, http.StatusUnauthorized},
		{ErrExpired, http.StatusUnauthorized},
		{ErrRevoked, http.StatusUnauthorized},
		{ErrUnknownJob, http.StatusUnauthorized},
		{ErrScopeMismatch, http.StatusForbidden},
		{fmt.Errorf("wrap: %w", ErrExpired), http.StatusUnauthorized},
		{errors.New("boom"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		if got := HTTPStatus(tc.err); got != tc.want {
			t.Errorf("HTTPStatus(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

func TestReason(t *testing.T) {
	cases := map[error]string{
		ErrMalformedToken: "malformed_token",
		ErrBadSignature:   "bad_signature",
		ErrExpired:        "expired",
		ErrRevoked:        "revoked",
		ErrUnknownJob:     "unknown_job",
		ErrScopeMismatch:  "scope_mismatch",
		errors.New("x"):   "unknown",
	}
	for err, want := range cases {
		if got := Reason(err); got != want {
			t.Errorf("Reason(%v) = %q, want %q", err, got, want)
		}
	}
}

func TestRevokeIgnoresMalformedToken(t *testing.T) {
	iss := newTestIssuer(time.Now(), fakeRegistry{}, fakeDone{})
	iss.Revoke("not a token")
	if _, err := iss.Verify("not a token", RequestScope{}); !errors.Is(err, ErrMalformedToken) {
		t.Fatalf("err = %v, want ErrMalformedToken", err)
	}
}
