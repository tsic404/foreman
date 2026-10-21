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

const (
	probeJobName = "fm-catalog-req-1"
	probeRuntime = "rid-probe"
	probeRequest = "req-1"
	probeSelfID  = "self-1"
)

type fakeProbes map[string]ProbeEntry

func (f fakeProbes) ProbeByJob(jobName string) (ProbeEntry, bool) {
	e, ok := f[jobName]
	return e, ok
}

// newProbeIssuer builds a test Issuer with the probe lookup injected; a nil
// lookup models the option not being set.
func newProbeIssuer(now time.Time, jobs JobLookup, done DoneIndex, probes ProbeLookup) *Issuer {
	i, err := NewIssuer(testKey, jobs, done, testTokenTTL, WithClock(fixedClock(now)), WithProbeLookup(probes))
	if err != nil {
		panic(err)
	}
	return i
}

// signedToken assembles a token around an arbitrary payload, so tests can
// exercise payload shapes the Issuer never produces.
func signedToken(payload string) string {
	segment := base64.RawURLEncoding.EncodeToString([]byte(payload))
	input := TokenPrefix + segment
	mac := hmac.New(sha256.New, testKey)
	mac.Write([]byte(input))
	return input + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// probeFixture issues a probe token and returns it with the session that
// binds it, sharing one clock and key.
func probeFixture(t *testing.T, now time.Time) (string, ProbeEntry) {
	t.Helper()
	iss := newTestIssuer(now, fakeRegistry{}, fakeDone{})
	token, err := iss.IssueProbe(probeJobName, probeRequest, "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("IssueProbe: %v", err)
	}
	session := ProbeEntry{
		JobRuntime:   probeRuntime,
		RequestID:    probeRequest,
		SelfMintedID: probeSelfID,
		TokenHash:    iss.Hash(token),
	}
	return token, session
}

func TestIssueProbeTokenFormat(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	iss := newTestIssuer(now, fakeRegistry{}, fakeDone{})

	token, err := iss.IssueProbe(probeJobName, probeRequest, "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("IssueProbe: %v", err)
	}

	// The probe payload carries the role and no task id; field order is the
	// declared one (r after t, omitted t leaves the object without a "t" key).
	wantPayload := `{"j":"fm-catalog-req-1","r":"catalog-probe","w":"ws-1","e":1700003600}`
	segment := base64.RawURLEncoding.EncodeToString([]byte(wantPayload))
	if got := signedToken(wantPayload); token != got {
		t.Fatalf("token = %q, want %q", token, got)
	}
	if !strings.HasPrefix(token, TokenPrefix+segment+".") {
		t.Fatalf("token = %q, want payload segment %q", token, segment)
	}
	if strings.Contains(wantPayload, `"t"`) {
		t.Fatalf("probe payload carries a task id: %s", wantPayload)
	}
}

func TestIssueProbeRejectsBadArgs(t *testing.T) {
	iss := newTestIssuer(time.Now(), fakeRegistry{}, fakeDone{})
	for _, tc := range []struct {
		name                     string
		jobName, requestID, wsID string
		ttl                      time.Duration
	}{
		{"empty job", "", "req-1", "ws-1", time.Hour},
		{"empty request", "j", "", "ws-1", time.Hour},
		{"empty workspace", "j", "req-1", "", time.Hour},
		{"zero ttl", "j", "req-1", "ws-1", 0},
		{"negative ttl", "j", "req-1", "ws-1", -time.Hour},
	} {
		if _, err := iss.IssueProbe(tc.jobName, tc.requestID, tc.wsID, tc.ttl); err == nil {
			t.Errorf("%s: expected error", tc.name)
		}
	}
}

// TestVerifyProbeOK covers the probe happy path: the session bounds the scope
// by runtime id and by either of the two request ids (command-driven
// RequestID, startup-discovery SelfMintedID), and the claims carry the role.
func TestVerifyProbeOK(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	token, session := probeFixture(t, now)
	iss := newProbeIssuer(now, fakeRegistry{}, fakeDone{}, fakeProbes{probeJobName: session})

	for name, req := range map[string]RequestScope{
		"command-driven request id": {RuntimeID: probeRuntime, RequestID: probeRequest},
		"startup-discovery self id": {RuntimeID: probeRuntime, RequestID: probeSelfID},
		"runtime id only":           {RuntimeID: probeRuntime},
		"no scope at all":           {},
	} {
		claims, err := iss.Verify(token, req)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if claims.Role != RoleCatalogProbe {
			t.Errorf("%s: Role = %q, want %q", name, claims.Role, RoleCatalogProbe)
		}
		if claims.JobName != probeJobName || claims.TaskID != "" || claims.WorkspaceID != "ws-1" {
			t.Errorf("%s: claims = %+v", name, claims)
		}
		if want := now.Add(time.Hour); !claims.ExpiresAt.Equal(want) {
			t.Errorf("%s: ExpiresAt = %s, want %s", name, claims.ExpiresAt, want)
		}
		if claims.IsLateReplay {
			t.Errorf("%s: IsLateReplay set on the probe path", name)
		}
	}

	// A session without a recorded hash skips the hash check.
	nohash := session
	nohash.TokenHash = ""
	iss = newProbeIssuer(now, fakeRegistry{}, fakeDone{}, fakeProbes{probeJobName: nohash})
	if _, err := iss.Verify(token, RequestScope{RuntimeID: probeRuntime, RequestID: probeRequest}); err != nil {
		t.Fatalf("empty session hash: %v", err)
	}
}

func TestVerifyProbeScopeMismatch(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	token, session := probeFixture(t, now)
	iss := newProbeIssuer(now, fakeRegistry{}, fakeDone{}, fakeProbes{probeJobName: session})

	for name, req := range map[string]RequestScope{
		"other runtime":   {RuntimeID: "rid-other", RequestID: probeRequest},
		"unknown request": {RuntimeID: probeRuntime, RequestID: "req-other"},
		// The probe has no task face at all: a {tid} from the path is out of
		// scope even when the runtime and request ids are its own. This is the
		// guard that keeps a probe token away from S9/S20/S26, which forward
		// with Foreman's own server credential and can settle an in-flight
		// task through S9's 404 (§6.3 作用域行).
		"task id on the path": {TaskID: "task-elsewhere", RuntimeID: probeRuntime, RequestID: probeRequest},
		"task id only":        {TaskID: "task-elsewhere"},
	} {
		_, err := iss.Verify(token, req)
		if !errors.Is(err, ErrScopeMismatch) {
			t.Errorf("%s: err = %v, want ErrScopeMismatch", name, err)
		}
		if got := HTTPStatus(err); got != http.StatusForbidden {
			t.Errorf("%s: status = %d, want 403", name, got)
		}
		if got := Reason(err); got != "scope" {
			t.Errorf("%s: Reason = %q, want scope", name, got)
		}
	}
}

// TestVerifyProbeUnknownSession pins the probe branch's sole adjudication
// face: with no session the token is an unknown Job, and neither a task-face
// registry entry, a done-index hit nor a terminal endpoint rescues it.
func TestVerifyProbeUnknownSession(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	token, session := probeFixture(t, now)

	cases := map[string]struct {
		jobs   JobLookup
		probes ProbeLookup
		req    RequestScope
	}{
		"lookup not injected":         {fakeRegistry{}, nil, RequestScope{RuntimeID: probeRuntime, RequestID: probeRequest}},
		"lookup miss":                 {fakeRegistry{}, fakeProbes{"fm-catalog-other": session}, RequestScope{RuntimeID: probeRuntime}},
		"task entry cannot rescue it": {fakeRegistry{probeJobName: {JobRuntimeID: probeRuntime, WorkspaceID: "ws-1"}}, fakeProbes{}, RequestScope{RuntimeID: probeRuntime}},
		"terminal endpoint":           {fakeRegistry{}, nil, RequestScope{RuntimeID: probeRuntime, IsTerminalEndpoint: true}},
	}
	for name, tc := range cases {
		iss := newProbeIssuer(now, tc.jobs, fakeDone{probeRequest: true}, tc.probes)
		_, err := iss.Verify(token, tc.req)
		if !errors.Is(err, ErrUnknownJob) {
			t.Errorf("%s: err = %v, want ErrUnknownJob", name, err)
		}
		if got := HTTPStatus(err); got != http.StatusUnauthorized {
			t.Errorf("%s: status = %d, want 401", name, got)
		}
		if got := Reason(err); got != "unknown_job" {
			t.Errorf("%s: Reason = %q, want unknown_job", name, got)
		}
	}
}

func TestVerifyProbeTokenHashMismatch(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	token, session := probeFixture(t, now)

	// Same job name re-issued (same-name request_id reuse): the live session
	// holds a different digest, so the predecessor's token is revoked.
	session.TokenHash = newTestIssuer(now, fakeRegistry{}, fakeDone{}).Hash("fmj_other")
	iss := newProbeIssuer(now, fakeRegistry{}, fakeDone{}, fakeProbes{probeJobName: session})
	_, err := iss.Verify(token, RequestScope{RuntimeID: probeRuntime, RequestID: probeRequest})
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("err = %v, want ErrRevoked", err)
	}
	if got := HTTPStatus(err); got != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", got)
	}
	if got := Reason(err); got != "revoked" {
		t.Errorf("Reason = %q, want revoked", got)
	}
}

func TestVerifyProbeExpired(t *testing.T) {
	issued := time.Unix(1_700_000_000, 0)
	token, session := probeFixture(t, issued)
	probes := fakeProbes{probeJobName: session}

	withinSkew := newProbeIssuer(issued.Add(time.Hour).Add(ClockSkew).Add(-time.Second), fakeRegistry{}, fakeDone{}, probes)
	if _, err := withinSkew.Verify(token, RequestScope{RuntimeID: probeRuntime, RequestID: probeRequest}); err != nil {
		t.Fatalf("within skew: %v", err)
	}

	expired := newProbeIssuer(issued.Add(time.Hour).Add(ClockSkew).Add(time.Second), fakeRegistry{}, fakeDone{}, probes)
	if _, err := expired.Verify(token, RequestScope{RuntimeID: probeRuntime, RequestID: probeRequest}); !errors.Is(err, ErrExpired) {
		t.Fatalf("err = %v, want ErrExpired", err)
	}
}

// TestProbeAndTaskFacesAreDisjoint pins that the two identity tables never
// cross: a probe token ignores the task-face revocation set, and a task token
// ignores the probe session table.
func TestProbeAndTaskFacesAreDisjoint(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	probeToken, session := probeFixture(t, now)
	jobs := fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1", WorkspaceID: "ws-1"}}
	iss := newProbeIssuer(now, jobs, fakeDone{}, fakeProbes{probeJobName: session})

	// A revoked digest on the task face does not touch the probe branch.
	iss.Revoke(iss.Hash(probeToken), "ws-1")
	if _, err := iss.Verify(probeToken, RequestScope{RuntimeID: probeRuntime, RequestID: probeRequest}); err != nil {
		t.Fatalf("probe token consulted the revocation set: %v", err)
	}

	// A task token naming the probe's job name is still adjudicated by the
	// task registry: the probe session does not stand in for a task entry.
	orphan, err := iss.Issue(probeJobName, "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := iss.Verify(orphan, RequestScope{RuntimeID: probeRuntime}); !errors.Is(err, ErrUnknownJob) {
		t.Fatalf("err = %v, want ErrUnknownJob (probe session is not a task entry)", err)
	}

	// A task token whose task-facing entry exists verifies normally with the
	// probe lookup injected, and never reports a role.
	taskToken, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	claims, err := iss.Verify(taskToken, RequestScope{TaskID: "task-1", RuntimeID: "rid-1"})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.Role != "" {
		t.Fatalf("Role = %q, want empty for a task token", claims.Role)
	}
	// The probe session table is not consulted for a task token: a probe token
	// with the same payload job name and a matching session still fails the
	// task lookup above, while this task token succeeds on the registry alone.
	foreign, err := iss.IssueProbe("fm-task-1", "req-9", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("IssueProbe: %v", err)
	}
	if _, err := iss.Verify(foreign, RequestScope{RuntimeID: "rid-1"}); !errors.Is(err, ErrUnknownJob) {
		t.Fatalf("probe token under a task job name: err = %v, want ErrUnknownJob", err)
	}
}

// TestProbePayloadShapes covers payload validity: a probe payload has no "t"
// and parses, while a payload with neither "t" nor "r" is malformed.
func TestProbePayloadShapes(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	// Hand-built probe token: no "t" key at all.
	token := signedToken(`{"j":"fm-catalog-req-1","r":"catalog-probe","w":"ws-1","e":9999999999}`)
	session := ProbeEntry{TokenHash: newTestIssuer(now, fakeRegistry{}, fakeDone{}).Hash(token)}
	iss := newProbeIssuer(now, fakeRegistry{}, fakeDone{}, fakeProbes{probeJobName: session})

	claims, err := iss.Verify(token, RequestScope{})
	if err != nil {
		t.Fatalf("hand-built probe token: %v", err)
	}
	if claims.Role != RoleCatalogProbe || claims.TaskID != "" {
		t.Fatalf("claims = %+v, want a role-carrying probe claim", claims)
	}

	// Neither face set: rejected as malformed before any adjudication.
	malformed := signedToken(`{"j":"fm-1","w":"ws-1","e":9999999999}`)
	for name, tok := range map[string]string{
		"no t and no r":       malformed,
		"empty t and empty r": signedToken(`{"j":"fm-1","t":"","r":"","w":"ws-1","e":9999999999}`),
		"no job name":         signedToken(`{"r":"catalog-probe","w":"ws-1","e":9999999999}`),
	} {
		if _, err := iss.Verify(tok, RequestScope{}); !errors.Is(err, ErrMalformedToken) {
			t.Errorf("%s: err = %v, want ErrMalformedToken", name, err)
		}
	}
}

// TestIssueProbeIgnoresNonProbePayload guards the wire contract: the task
// issuer still emits no role key, so the two payload shapes stay distinct.
func TestIssueProbeIgnoresNonProbePayload(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	iss := newTestIssuer(now, fakeRegistry{"fm-task-1": {JobRuntimeID: "rid-1"}}, fakeDone{})
	token, err := iss.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(strings.SplitN(token, ".", 2)[0], TokenPrefix))
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if bytes.Contains(raw, []byte(`"r"`)) {
		t.Fatalf("task payload carries a role: %s", raw)
	}
	if !bytes.Contains(raw, []byte(`"t":"task-1"`)) {
		t.Fatalf("task payload lost its task id: %s", raw)
	}
}
