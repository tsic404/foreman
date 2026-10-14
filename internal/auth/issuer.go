// Package auth implements Foreman's two-way authentication: it signs and
// statelessly verifies per-Job Tokens (fmj_), and loads Foreman's own server
// credential. Design: docs/05-modules/auth.md, ADR-007.
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

// Claims is the verified content of a Job Token.
type Claims struct {
	JobName     string
	TaskID      string
	WorkspaceID string
	ExpiresAt   time.Time
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

// JobEntry is the subset of a registry entry that Verify needs.
type JobEntry struct {
	JobRuntimeID string
}

// DoneIndex reports tasks that reached terminal state within the retention
// window (24h); it backs the late-replay idempotent path.
type DoneIndex interface {
	IsDone(taskID string) bool
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
	revocations *RevocationSet
}

// Option customizes an Issuer.
type Option func(*Issuer)

// WithClock overrides the clock (tests).
func WithClock(now func() time.Time) Option {
	return func(i *Issuer) { i.now = now }
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
	signingInput := TokenPrefix + segment
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(i.sign(signingInput)), nil
}

// sign returns HMAC-SHA256(key, input).
func (i *Issuer) sign(input string) []byte {
	mac := hmac.New(sha256.New, i.key)
	mac.Write([]byte(input))
	return mac.Sum(nil)
}

// Verify runs the fixed seven-step check of the auth module design and
// returns the token's claims or one of the structured errors. A terminal
// task in the done index answering a terminal endpoint yields claims with
// IsLateReplay set and no error.
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
	entry, ok := i.jobs.ByDaemon(payload.JobName)
	if !ok {
		if i.done.IsDone(payload.TaskID) && req.IsTerminalEndpoint {
			claims.IsLateReplay = true
			return claims, nil
		}
		return Claims{}, fmt.Errorf("%w: %s", ErrUnknownJob, payload.JobName)
	}
	// 5. Revocation set — reachable only while the entry exists, which is
	// exactly the same-name reuse face (a re-dispatched task or a rebuilt Job
	// brought the entry back): the predecessor's token must never be
	// accepted again (ADR-017).
	if i.revocations.IsRevoked(token) {
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
// The window is the *upper bound approximation* of auth.md's "TTL = the
// token's remaining validity": the reclaim runs after the issue, so the
// remaining time is ≤ the issued TTL, and `now + TTL + ClockSkew` therefore
// never expires an entry early (conservative direction — the digest may
// outlive the token, never the reverse).
func (i *Issuer) Revoke(tokenHash string) {
	i.revocations.Revoke(tokenHash, i.now().Add(i.tokenTTL+ClockSkew))
}
