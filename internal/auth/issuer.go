// Package auth implements Foreman's two-way authentication: it signs and
// statelessly verifies per-Job Tokens (fmj_), and loads Foreman's own server
// credential. Design: 02-contracts.md §2.1/§6, ADR-007.
package auth

import (
	"context"
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
	// ByDaemonInWorkspace returns the entry bound to daemonID (= job name) in
	// workspaceID; ok is false when no entry exists. Entry existence IS
	// credential validity: an entry survives its task's verdict for as long
	// as its Job does (draining), and disappears at the credential reclaim
	// (ADR-017). Stage two's lookup is partitioned: a local miss reads the
	// shared row, so any instance authenticates any Job (§2.1 任实例服务).
	ByDaemonInWorkspace(workspaceID, daemonID string) (JobEntry, bool)
}

// JobEntry is the subset of a registry entry that Verify needs. WorkspaceID
// is part of the resource adjudication: a token only ever addresses entries of
// its own workspace (§2.1).
type JobEntry struct {
	JobRuntimeID string
	WorkspaceID  string
	// Terminal reports the entry's terminal state: a revoked token may still
	// replay a terminal endpoint of its own already-settled task (§6.4, AC-74).
	Terminal bool
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
	// revokeStore is the shared revocation face (ADR-046 阶段一): when
	// wired, Revoke writes the digest there and IsRevoked consults it,
	// making the revocation set cross-restart persistent and (stage two)
	// cross-instance consistent. Nil keeps the in-process set.
	revokeStore RevocationStore
	// revokeErr observes shared-state failures (§9.1: the shared face's
	// unavailability is an event, never silent).
	revokeErr func(op string, err error)
	// health is the shared state's availability face plus the cache grace
	// window (§10 共享状态不可用). Nil keeps the stage-one behaviour.
	health StateHealth
	grace  time.Duration
}

// StateHealth is the shared state's outage face the issuer's fail-closed path
// reads (satisfied by *state.Health).
type StateHealth interface {
	Available() bool
	DownFor() time.Duration
}

// WithSharedHealth wires the outage face and the cache grace window.
func WithSharedHealth(health StateHealth, grace time.Duration) Option {
	return func(i *Issuer) { i.health = health; i.grace = grace }
}

// RevocationStore is the shared revocation set face; satisfied by
// internal/state.Store.
type RevocationStore interface {
	Revoke(ctx context.Context, ws, hash string, validUntil time.Time) error
	// IsRevoked is the cross-instance read. Stage one does not call it: the
	// in-process set is the complete view (seeded at startup, written
	// through on every revoke), so a per-request store read would only add
	// latency. It is the seam stage two's bounded positive cache runs
	// against (§6.1 跨实例一致).
	IsRevoked(ctx context.Context, ws, hash string) (bool, error)
	// ListRevoked lists the live revocations so a restart can seed the
	// in-process positive cache (the safe direction: a cached digest can
	// only reject a token early, never accept one).
	ListRevoked(ctx context.Context) (map[string]time.Time, error)
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

// WithRevocationStore wires the shared revocation face (ADR-046 阶段一). The
// in-process RevocationSet stays as the local positive cache: it is seeded
// from the store at startup (RestoreRevocations), every revoke writes through,
// and it is what the same-instance fast path reads.
func WithRevocationStore(s RevocationStore) Option {
	return func(i *Issuer) { i.revokeStore = s }
}

// WithRevocationErrorHook observes shared-state failures of the revocation
// face (§9.1). Optional: tests and deployments without the face run without
// one.
func WithRevocationErrorHook(h func(op string, err error)) Option {
	return func(i *Issuer) { i.revokeErr = h }
}

// revocationCheckTimeout bounds one shared-store revocation lookup, so a slow
// store never stalls the daemon face.
const revocationCheckTimeout = 2 * time.Second

// RestoreRevocations seeds the in-process revocation cache from the shared
// store. It is the startup face of ADR-046 阶段一's revocation persistence: a
// digest revoked before the restart must still be rejected after it. A store
// failure leaves the cache as it was (empty) — the per-request store lookup
// remains the fallback.
func (i *Issuer) RestoreRevocations(ctx context.Context) error {
	if i.revokeStore == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, revocationCheckTimeout)
	defer cancel()
	revoked, err := i.revokeStore.ListRevoked(ctx)
	if err != nil {
		if i.revokeErr != nil {
			i.revokeErr("list_revoked", err)
		}
		return err
	}
	now := i.now()
	for hash, until := range revoked {
		if until.After(now) {
			i.revocations.Revoke(hash, until)
		}
	}
	return nil
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
	if i.health != nil && !i.health.Available() && i.health.DownFor() > i.grace {
		// Beyond the grace window the cached facts no longer adjudicate.
		return Claims{}, fmt.Errorf("%w: outage %s", ErrStateUnavailable, i.health.DownFor())
	}
	entry, ok := i.jobs.ByDaemonInWorkspace(payload.WorkspaceID, payload.JobName)
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
	// accepted again (ADR-017). The in-process set is the complete stage-one
	// view: it is seeded from the shared store at startup
	// (RestoreRevocations) and every Revoke writes through, so the hot path
	// needs no store round trip. The cross-instance read (bounded positive
	// cache with the revocation window) is stage two's contract (§6.1
	// 跨实例一致); doing it here would put a store call on every daemon
	// request for no stage-one gain.
	digest := i.Hash(token)
	if i.revocations.IsRevoked(digest) {
		return revocationVerdict(entry, payload, req, claims)
	}
	if i.revokeStore != nil {
		// Stage two's cross-instance read (§6.1 跨实例一致): a digest revoked
		// on another instance must be seen here inside the bounded window.
		// The read is bounded; a failed read is fail-closed on the token face
		// (the entry existence above already passed).
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		revoked, err := i.revokeStore.IsRevoked(ctx, payload.WorkspaceID, digest)
		cancel()
		if err != nil {
			// The outage face: within the grace window the in-process set is
			// the adjudication source (a revocation written elsewhere during
			// the outage is the bounded-window tolerance, §6.1); beyond it the
			// health gate at the top of Verify already failed closed.
			if i.revokeErr != nil {
				i.revokeErr("read", err)
			}
		}
		if revoked {
			i.revocations.Revoke(digest, claims.ExpiresAt.Add(ClockSkew))
			return revocationVerdict(entry, payload, req, claims)
		}
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

// Revoke adds a token digest to the revocation set for the rest of
// the token's validity window. The credential reclaim calls it before deleting
// the Secret and the entry (ADR-017); see RevocationSet for the face it
// protects. workspace partitions the revocation set (§6.1): the shared face
// records the digest under that workspace, so a revocation never leaks into
// another workspace's adjudication.
//
// The window is the *upper bound approximation* of §6.1's "TTL = the
// token's remaining validity": the reclaim runs after the issue, so the
// remaining time is ≤ the issued TTL, and `now + TTL + ClockSkew` therefore
// never expires an entry early (conservative direction — the digest may
// outlive the token, never the reverse).
func (i *Issuer) Revoke(tokenHash, workspaceID string) {
	i.revocations.Revoke(tokenHash, i.now().Add(i.tokenTTL+ClockSkew))
	if i.revokeStore != nil {
		// The shared face is the cross-restart authority. A failure leaves
		// the in-process set holding the digest, so the same-instance face
		// is safe; the shared miss is the outage the bounded-window
		// consistency tolerates, and the reclaim must not fail on it.
		ctx, cancel := context.WithTimeout(context.Background(), revocationCheckTimeout)
		defer cancel()
		if err := i.revokeStore.Revoke(ctx, workspaceID, tokenHash,
			i.now().Add(i.tokenTTL+ClockSkew)); err != nil && i.revokeErr != nil {
			i.revokeErr("revoke", err)
		}
	}
}

// revocationVerdict is the revoked-token face (§6.4, AC-74). A terminal
// endpoint of the token's own settled task is an idempotent replay; the same
// endpoint with a foreign task or runtime id is the §2.1 rule-1 scope
// mismatch (403), judged before the revocation verdict; every other endpoint
// stays 401.
func revocationVerdict(entry JobEntry, payload tokenPayload, req RequestScope, claims Claims) (Claims, error) {
	if req.IsTerminalEndpoint {
		if req.TaskID != "" && req.TaskID != payload.TaskID {
			return Claims{}, fmt.Errorf("%w: task %s", ErrScopeMismatch, req.TaskID)
		}
		if req.RuntimeID != "" && req.RuntimeID != entry.JobRuntimeID {
			return Claims{}, fmt.Errorf("%w: runtime %s", ErrScopeMismatch, req.RuntimeID)
		}
		if entry.Terminal {
			claims.IsLateReplay = true
			return claims, nil
		}
	}
	return Claims{}, ErrRevoked
}
