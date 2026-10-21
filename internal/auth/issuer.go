// Package auth implements Foreman's two-way authentication: it signs and
// statelessly verifies per-Job Tokens (fmj_), and loads Foreman's own server
// credential. Design: 02-contracts.md §2.1/§6, ADR-007.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// ClockSkew is the tolerated clock drift between a Job pod and Foreman.
const ClockSkew = 5 * time.Minute

// RoleCatalogProbe is the payload role of a catalog-probe Job Token
// (ADR-019). It is the discriminator of the probe branch in Verify: a probe
// carries no task ID and is adjudicated by the probe session table alone.
const RoleCatalogProbe = "catalog-probe"

// Claims is the verified content of a Job Token.
type Claims struct {
	JobName     string
	TaskID      string
	WorkspaceID string
	// Role is "" for a task Job and RoleCatalogProbe for a catalog probe; a
	// probe token carries no TaskID.
	Role      string
	ExpiresAt time.Time
	// IsLateReplay marks a terminal-endpoint retry that arrived after the
	// task reached terminal state; the routing layer answers 200
	// idempotently and must not run any side effects.
	IsLateReplay bool
}

// RequestScope carries the resource identifiers extracted from the request
// (path params, body) that a Job Token is checked against.
type RequestScope struct {
	TaskID    string // path {tid}; empty when the endpoint carries none
	RuntimeID string // path {rid}; empty when the endpoint carries none
	// RequestID is the S27 path {req}; empty elsewhere. For a probe it must
	// match the session's RequestID (command-driven) or SelfMintedID
	// (startup discovery).
	RequestID string
	// IsTerminalEndpoint is true for the terminal report endpoints
	// (complete / fail / cancel-ack); it gates the late-replay path.
	IsTerminalEndpoint bool
}

// JobLookup is the auth module's read view of the task registry.
type JobLookup interface {
	// ByDaemon returns the entry bound to daemonID (= job name); ok is false
	// when no entry exists. Entry existence IS credential validity: an entry
	// survives its task's verdict for as long as its Job does (draining),
	// and disappears at the credential reclaim (ADR-017).
	ByDaemon(daemonID string) (JobEntry, bool)
}

// JobEntry is the subset of a registry entry that Verify needs. WorkspaceID
// is part of the resource adjudication: a token only ever addresses entries of
// its own workspace (§2.1).
type JobEntry struct {
	JobRuntimeID string
	WorkspaceID  string
}

// DoneIndex reports tasks that reached terminal state within the retention
// window (24h); it backs the late-replay idempotent path. The workspace is
// part of the key: the same task id in another workspace is a different task
// (§4.1).
type DoneIndex interface {
	IsDone(workspaceID, taskID string) bool
}

// ProbeEntry is the subset of a probe session that Verify needs. WorkspaceID
// is empty for startup / registration-discovery probes, whose registration
// scope spans every discovery-set workspace (§1.2).
type ProbeEntry struct {
	JobRuntime   string
	RequestID    string
	SelfMintedID string
	TokenHash    string
	WorkspaceID  string
}

// ProbeLookup is the auth module's read view of the probe session table, the
// authoritative state of catalog-probe Jobs (ADR-019).
type ProbeLookup interface {
	// ProbeByJob returns the session bound to jobName (fm-catalog-<req>); ok
	// is false when no session exists.
	ProbeByJob(jobName string) (ProbeEntry, bool)
}

// Issuer signs and verifies Job Tokens. Verification is stateless apart from
// the registry/done-index seams and the in-process revocation set, so a
// Foreman restart keeps validating in-flight Jobs.
type Issuer struct {
	key         []byte
	tokenTTL    time.Duration
	now         func() time.Time
	jobs        JobLookup
	done        DoneIndex
	probes      ProbeLookup
	revocations *RevocationSet
}

// Option customizes an Issuer.
type Option func(*Issuer)

// WithClock overrides the clock (tests).
func WithClock(now func() time.Time) Option {
	return func(i *Issuer) { i.now = now }
}

// WithProbeLookup injects the probe session table; it is required for probe
// tokens (without it every probe token is an unknown Job) and is never
// consulted for task tokens.
func WithProbeLookup(p ProbeLookup) Option {
	return func(i *Issuer) { i.probes = p }
}

// NewIssuer builds an Issuer from the decoded HMAC key (>= MinKeyBytes) and
// the issued token TTL, which bounds the revocation window.
func NewIssuer(key []byte, jobs JobLookup, done DoneIndex, tokenTTL time.Duration, opts ...Option) (*Issuer, error) {
	if len(key) < MinKeyBytes {
		return nil, fmt.Errorf("%w: got %d bytes, need >= %d", ErrKeyTooShort, len(key), MinKeyBytes)
	}
	if tokenTTL <= 0 {
		return nil, errors.New("auth: token TTL must be positive")
	}
	i := &Issuer{key: key, tokenTTL: tokenTTL, now: time.Now, jobs: jobs, done: done}
	for _, opt := range opts {
		opt(i)
	}
	i.revocations = NewRevocationSet(i.now)
	return i, nil
}

// Issue creates a Job Token bound to jobName/taskID/workspaceID, valid for
// ttl from now.
func (i *Issuer) Issue(jobName, taskID, workspaceID string, ttl time.Duration) (string, error) {
	if jobName == "" || taskID == "" || workspaceID == "" {
		return "", errors.New("auth: job name, task ID and workspace ID are required")
	}
	if ttl <= 0 {
		return "", errors.New("auth: ttl must be positive")
	}
	segment, err := encodePayload(tokenPayload{
		JobName:     jobName,
		TaskID:      taskID,
		WorkspaceID: workspaceID,
		Expiry:      i.now().Add(ttl).Unix(),
	})
	if err != nil {
		return "", err
	}
	return i.assemble(segment), nil
}

// IssueProbe signs a catalog-probe Job Token: payload
// {"j":jobName,"r":"catalog-probe","w":workspaceID,"e":exp} with no "t",
// because a probe never maps to a task (ADR-019). requestID is not part of the
// payload; it is only validated here to keep the signature at parity with the
// scheduler's Start.
func (i *Issuer) IssueProbe(jobName, requestID, workspaceID string, ttl time.Duration) (string, error) {
	if jobName == "" || requestID == "" || workspaceID == "" {
		return "", errors.New("auth: job name, request ID and workspace ID are required")
	}
	if ttl <= 0 {
		return "", errors.New("auth: ttl must be positive")
	}
	segment, err := encodePayload(tokenPayload{
		JobName:     jobName,
		Role:        RoleCatalogProbe,
		WorkspaceID: workspaceID,
		Expiry:      i.now().Add(ttl).Unix(),
	})
	if err != nil {
		return "", err
	}
	return i.assemble(segment), nil
}

// assemble builds the wire token "fmj_"+segment+"."+b64url(hmac over the
// prefix and segment).
func (i *Issuer) assemble(segment string) string {
	signingInput := TokenPrefix + segment
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(i.sign(signingInput))
}

// sign returns HMAC-SHA256(key, input).
func (i *Issuer) sign(input string) []byte {
	mac := hmac.New(sha256.New, i.key)
	mac.Write([]byte(input))
	return mac.Sum(nil)
}

// Verify runs the fixed check of the auth module design and returns the
// token's claims or one of the structured errors. A probe token (payload role
// catalog-probe) is adjudicated by the probe session table alone (ADR-019); a
// terminal task in the done index answering a terminal endpoint yields claims
// with IsLateReplay set and no error.
func (i *Issuer) Verify(token string, req RequestScope) (Claims, error) {
	// 1. Prefix / segment / format check.
	payload, segment, signature, err := parseToken(token)
	if err != nil {
		return Claims{}, err
	}
	claims := Claims{
		JobName:     payload.JobName,
		TaskID:      payload.TaskID,
		WorkspaceID: payload.WorkspaceID,
		ExpiresAt:   time.Unix(payload.Expiry, 0),
	}
	// 2. Recompute HMAC over "fmj_"+payload, constant-time compare.
	if !hmac.Equal(i.sign(TokenPrefix+segment), signature) {
		return Claims{}, ErrBadSignature
	}
	// 3. Expiry, tolerating ClockSkew between pod and Foreman.
	if i.now().After(claims.ExpiresAt.Add(ClockSkew)) {
		return Claims{}, fmt.Errorf("%w: expired at %s", ErrExpired, claims.ExpiresAt.Format(time.RFC3339))
	}
	// 4. Entry adjudication — ahead of the revocation check (ADR-017). The
	// entry's existence is the credential's validity (alive or draining), so
	// after the reclaim deleted it, "no entry" is the authoritative verdict:
	// a terminal endpoint retried by a daemon that already reported is a
	// late replay (200 idempotent), everything else is an unknown Job.
	//
	// A probe token takes the probe branch first: the probe session table is
	// its sole adjudication face, and neither the task registry nor the done
	// index nor the task-face revocation set is consulted (ADR-019).
	if payload.Role == RoleCatalogProbe {
		var session ProbeEntry
		var found bool
		if i.probes != nil {
			session, found = i.probes.ProbeByJob(payload.JobName)
		}
		if !found {
			return Claims{}, fmt.Errorf("%w: %s", ErrUnknownJob, payload.JobName)
		}
		if session.TokenHash != "" && i.Hash(token) != session.TokenHash {
			return Claims{}, ErrRevoked
		}
		// A workspace-pinned probe (a catalog probe) only answers for its own
		// workspace; startup / discovery probes carry no pin and span the
		// discovery set (§6.3).
		if session.WorkspaceID != "" && session.WorkspaceID != payload.WorkspaceID {
			return Claims{}, fmt.Errorf("%w: workspace %s", ErrScopeMismatch, payload.WorkspaceID)
		}
		claims.Role = RoleCatalogProbe
		// The probe has no task face at all: any {tid} on the path is out of
		// scope, never a resource to adjudicate (§6.3 作用域行). Without
		// this, a probe token could drive the task-addressed endpoints
		// (S9/S20/S26 forward with Foreman's own server credential) and
		// even settle an in-flight task via S9's 404.
		if req.TaskID != "" {
			return Claims{}, fmt.Errorf("%w: probe carries no task scope, got %s", ErrScopeMismatch, req.TaskID)
		}
		if req.RuntimeID != "" && req.RuntimeID != session.JobRuntime {
			return Claims{}, fmt.Errorf("%w: runtime %s", ErrScopeMismatch, req.RuntimeID)
		}
		if req.RequestID != "" && req.RequestID != session.RequestID && req.RequestID != session.SelfMintedID {
			return Claims{}, fmt.Errorf("%w: request %s", ErrScopeMismatch, req.RequestID)
		}
		return claims, nil
	}
	entry, ok := i.jobs.ByDaemon(payload.JobName)
	if !ok {
		if i.done.IsDone(payload.WorkspaceID, payload.TaskID) && req.IsTerminalEndpoint {
			// The replay is idempotent only for the token's own task: the
			// done index proves that task settled, never that the request
			// addresses it (§2.1 rule 1). Without this check any settled
			// task's token would be answered 200 on every task's terminal
			// endpoint.
			if req.TaskID != "" && req.TaskID != payload.TaskID {
				return Claims{}, fmt.Errorf("%w: task %s", ErrScopeMismatch, req.TaskID)
			}
			claims.IsLateReplay = true
			return claims, nil
		}
		return Claims{}, fmt.Errorf("%w: %s", ErrUnknownJob, payload.JobName)
	}
	// 4b. The token's workspace must equal the entry's: a Job Token of one
	// workspace addressing another workspace's task is the cross-workspace
	// impersonation case (§2.1, AC-82).
	if entry.WorkspaceID != payload.WorkspaceID {
		return Claims{}, fmt.Errorf("%w: workspace %s", ErrScopeMismatch, payload.WorkspaceID)
	}
	// 5. Revocation set — reachable only while the entry exists, which is
	// exactly the same-name reuse face (a re-dispatched task or a rebuilt Job
	// brought the entry back): the predecessor's token must never be
	// accepted again (ADR-017).
	if i.revocations.IsRevoked(i.Hash(token)) {
		return Claims{}, ErrRevoked
	}
	// 6. The request resource must match the token scope.
	if req.TaskID != "" && req.TaskID != payload.TaskID {
		return Claims{}, fmt.Errorf("%w: task %s", ErrScopeMismatch, req.TaskID)
	}
	if req.RuntimeID != "" && req.RuntimeID != entry.JobRuntimeID {
		return Claims{}, fmt.Errorf("%w: runtime %s", ErrScopeMismatch, req.RuntimeID)
	}
	// 7. ok.
	return claims, nil
}

// Hash returns the hex SHA-256 digest of a Job Token. It is the value the
// entry carries in token_hash and the revocation set is keyed by (ADR-017):
// the digest travels from the builder to the reclaim path, never the token.
func (i *Issuer) Hash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// Revoke adds a token digest to the in-process revocation set for the rest of
// the token's validity window. The credential reclaim calls it before deleting
// the Secret and the entry (ADR-017); see RevocationSet for the face it
// protects.
//
// The window is the *upper bound approximation* of §6.1's "TTL = the
// token's remaining validity": the reclaim runs after the issue, so the
// remaining time is ≤ the issued TTL, and `now + TTL + ClockSkew` therefore
// never expires an entry early (conservative direction — the digest may
// outlive the token, never the reverse).
func (i *Issuer) Revoke(tokenHash string) {
	i.revocations.Revoke(tokenHash, i.now().Add(i.tokenTTL+ClockSkew))
}
