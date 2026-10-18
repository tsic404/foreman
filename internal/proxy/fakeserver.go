package proxy

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// maxRequestBody bounds daemon/agent request bodies (cluster-internal
// surface; the contract pins no limit).
const maxRequestBody = 32 << 20

// TaskScheduler is the fake server's scheduling surface (§2.1):
// S6 delivery, S8–S17 report handling, task-vanish settlement.
type TaskScheduler interface {
	Deliver(daemonID, runtimeID string, maxTasks int) ([]json.RawMessage, error)
	OnReport(ctx context.Context, ep scheduler.Endpoint, e registry.TaskEntry, body []byte) (int, []byte, error)
	OnTaskVanished(ctx context.Context, taskID string) error
	// FailJob settles a Job whose daemon was refused at S4: the entry reaches
	// terminal/failed without waiting out the boot deadline (S-4).
	FailJob(ctx context.Context, jobName, reason string) error
}

// FakeServer is the J→F face: it speaks the official server protocol to the
// Job daemons (§2.2). ServeHTTP applies the three routing rules — fmj_
// Job Tokens to the local daemon face, any other credential verbatim to
// the real server (S29), none to 401.
type FakeServer struct {
	issuer  *auth.Issuer
	reg     *registry.Registry
	probes  *registry.ProbeRegistry // catalog-probe sessions; nil disables the probe face
	sched   TaskScheduler
	client  *Client
	ws      *WSHandler // nil when FOREMAN_WS_ENABLED=false
	metrics Metrics
	log     *slog.Logger
	mux     *http.ServeMux

	// registered lists every pattern routes() registered, in registration
	// order. The daemon-face census (protocol_test.go) compares this set
	// against the pinned upstream daemon API both ways: a route cannot be
	// added to or dropped from the local face without the census moving too.
	registered []string

	// legacyClaimMu guards legacyClaimed: the per-entry dedup of the legacy
	// claim fallback event. The set lives for the process lifetime — the
	// contract scopes the dedup there (ADR-042), so it is never evicted.
	legacyClaimMu sync.Mutex
	legacyClaimed map[string]struct{}
}

// FakeServerOption customizes a FakeServer.
type FakeServerOption func(*FakeServer)

// WithProbeRegistry wires the catalog-probe session table: it is what makes
// S4/S5/S19/S27 answer a probe token (§2.1 规则 1 的探针分支).
func WithProbeRegistry(probes *registry.ProbeRegistry) FakeServerOption {
	return func(s *FakeServer) { s.probes = probes }
}

// WithServerLogger overrides the fake server's logger (tests).
func WithServerLogger(l *slog.Logger) FakeServerOption {
	return func(s *FakeServer) { s.log = l }
}

// NewFakeServer builds the handler with the full §2.2 endpoint table.
// ws may be nil; the S30 route is then not registered and WS dials get a
// 404 (the FOREMAN_WS_ENABLED=false degradation).
func NewFakeServer(issuer *auth.Issuer, reg *registry.Registry, sched TaskScheduler, client *Client, ws *WSHandler, metrics Metrics, opts ...FakeServerOption) *FakeServer {
	s := &FakeServer{
		issuer:  issuer,
		reg:     reg,
		sched:   sched,
		client:  client,
		ws:      ws,
		metrics: orNoopMetrics(metrics),
		log:     slog.Default().With("component", "fake-server"),
		mux:     http.NewServeMux(),

		legacyClaimed: make(map[string]struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.routes()
	return s
}

// routes registers the local daemon face (§2.2 table; §2.1).
// Every registration goes through register/handleFn so the pattern set stays
// readable (registered) — the daemon-face census in protocol_test.go is what
// requires it.
func (s *FakeServer) routes() {
	m := s.mux
	register := func(pattern string, h http.Handler) {
		s.registered = append(s.registered, pattern)
		m.Handle(pattern, h)
	}
	handleFn := func(pattern string, h http.HandlerFunc) { register(pattern, h) }
	handleFn("GET /api/daemon/workspaces", s.handleWorkspaces)                                               // S1
	handleFn("GET /api/workspaces", s.handleWorkspaces)                                                      // S28
	handleFn("GET /api/daemon/workspaces/{ws}/repos", s.handleRepos)                                         // S2
	handleFn("GET /api/daemon/workspaces/{ws}/runtime-profiles", s.handle404)                                // S3
	handleFn("POST /api/daemon/workspaces/{ws}/issues/gc-check", s.handleIssueGCCheck)                       // S21
	handleFn("POST /api/daemon/register", s.handleRegister)                                                  // S4
	handleFn("POST /api/daemon/heartbeat", s.handleHeartbeat)                                                // S5
	handleFn("POST /api/daemon/deregister", s.handleDeregister)                                              // S19
	handleFn("POST /api/daemon/tasks/claim", s.handleClaim)                                                  // S6
	handleFn("POST /api/daemon/runtimes/{rid}/tasks/claim", s.handleLegacyClaim)                             // S32 (no delivery face, ADR-042)
	handleFn("GET /api/daemon/tasks/{tid}/status", s.handleTaskStatus)                                       // S9
	handleFn("GET /api/daemon/tasks/{tid}/gc-check", s.handleTaskGCCheck)                                    // S20
	handleFn("POST /api/daemon/tasks/{tid}/start", s.reportHandler(scheduler.EPStart))                       // S8
	handleFn("POST /api/daemon/tasks/{tid}/progress", s.reportHandler(scheduler.EPProgress))                 // S10
	handleFn("POST /api/daemon/tasks/{tid}/messages", s.reportHandler(scheduler.EPMessages))                 // S11
	handleFn("POST /api/daemon/tasks/{tid}/usage", s.reportHandler(scheduler.EPUsage))                       // S12
	handleFn("POST /api/daemon/tasks/{tid}/complete", s.reportHandler(scheduler.EPComplete))                 // S13
	handleFn("POST /api/daemon/tasks/{tid}/fail", s.reportHandler(scheduler.EPFail))                         // S14
	handleFn("POST /api/daemon/tasks/{tid}/cancel-ack", s.reportHandler(scheduler.EPCancelAck))              // S15
	handleFn("POST /api/daemon/tasks/{tid}/session", s.reportHandler(scheduler.EPSession))                   // S16
	handleFn("POST /api/daemon/tasks/{tid}/wait-local-directory", s.reportHandler(scheduler.EPWaitLocalDir)) // S17
	// S23 is defensive-only this version: these calls carry the caller's
	// own credential and are forwarded verbatim (§2.1 rule 2 semantics).
	// The Job Token is still verified first — an invalid fmj_ must 401
	// locally, never reach the upstream.
	handleFn("GET /api/daemon/tasks/{tid}/plugin-hooks", s.handleS23)
	handleFn("POST /api/daemon/tasks/{tid}/plugin-hooks", s.handleS23)
	handleFn("GET /api/daemon/tasks/{tid}/remote-mcp/{cid}/credential", s.handleS23)
	handleFn("GET /api/daemon/tasks/{tid}/plugin-mcp/{cid}/credential", s.handleS23)
	handleFn("POST /api/daemon/runtimes/{rid}/tasks/{tid}/prepare-lease", s.handlePrepareLease)         // S7
	handleFn("POST /api/daemon/runtimes/{rid}/tasks/{tid}/skill-bundles/resolve", s.handleSkillBundles) // S26
	handleFn("POST /api/daemon/runtimes/{rid}/recover-orphans", s.handleRecoverOrphans)                 // S18
	handleFn("POST /api/daemon/runtimes/{rid}/models/{req}/result", s.handleModelResult)                // S27
	handleFn("POST /api/daemon/runtimes/{rid}/local-skills/{req}/result", s.handle404)                  // S27
	handleFn("POST /api/daemon/runtimes/{rid}/local-skills/import/{req}/result", s.handle404)           // S27
	handleFn("POST /api/daemon/runtimes/{rid}/update/{req}/result", s.handle404)                        // S27
	handleFn("GET /api/daemon/issues/{id}/gc-check", s.handle404)                                       // S22
	handleFn("GET /api/daemon/chat-sessions/{id}/gc-check", s.handle404)                                // S22
	handleFn("GET /api/daemon/autopilot-runs/{id}/gc-check", s.handle404)                               // S22
	handleFn("POST /api/tokens/current/renew", s.handleTokenRenew)                                      // S24
	if s.ws != nil {
		register("GET /api/daemon/ws", s.ws) // S30
	}
	// Everything else under /api/ with a Job Token is not a local endpoint.
	handleFn("/api/", s.handleNotFound)
}

// registeredPatterns returns the patterns routes() registered, in order. It
// is the read side of the daemon-face census (protocol_test.go): the census
// asserts this set against the pinned upstream daemon API, so an undeclared
// local route is a test failure rather than a silent addition to the face.
func (s *FakeServer) registeredPatterns() []string {
	return append([]string(nil), s.registered...)
}

// ServeHTTP applies the §2.1 routing rules before the endpoint table.
func (s *FakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/api/") {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	token, ok := bearerToken(r)
	if !ok {
		// Rule 3: no credential → 401, never forwarded.
		writeError(w, http.StatusUnauthorized, "authorization required")
		return
	}
	if auth.HasJobTokenPrefix(token) {
		// Rule 1: the local daemon face.
		s.mux.ServeHTTP(w, r)
		return
	}
	// Rule 2: any other credential (agent mat_, remote-mcp) → S29.
	s.handlePassthrough(w, r)
}

// authenticate verifies the Job Token against the request scope (§2.1 rule
// 1 + resource check). On failure it writes the mapped status (401/403).
func (s *FakeServer) authenticate(w http.ResponseWriter, r *http.Request, scope auth.RequestScope) (auth.Claims, bool) {
	token, _ := bearerToken(r)
	claims, err := s.issuer.Verify(token, scope)
	if err != nil {
		s.metrics.AuthFailure(auth.Reason(err))
		s.log.Warn("job token rejected",
			"remote_addr", r.RemoteAddr, "path", r.URL.Path, "reason", auth.Reason(err))
		writeError(w, auth.HTTPStatus(err), http.StatusText(auth.HTTPStatus(err)))
		return auth.Claims{}, false
	}
	return claims, true
}

// ---- shared helpers ----

// bearerToken extracts the Bearer credential.
func bearerToken(r *http.Request) (string, bool) {
	h := strings.TrimSpace(r.Header.Get("Authorization"))
	if h == "" {
		return "", false
	}
	token, found := strings.CutPrefix(h, "Bearer ")
	if !found || strings.TrimSpace(token) == "" {
		return "", false
	}
	return strings.TrimSpace(token), true
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, "unreadable request body")
		return nil, false
	}
	return body, true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "encode response")
		return
	}
	writeRaw(w, code, raw)
}

// writeRaw replies with an upstream response verbatim (§2.4 forwarding).
func writeRaw(w http.ResponseWriter, code int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

// writeError is the unified error shape of the daemon face.
func writeError(w http.ResponseWriter, code int, msg string) {
	raw, _ := json.Marshal(map[string]string{"error": msg})
	writeRaw(w, code, raw)
}

// entryFor re-reads the token's mapping entry; it may have been deleted
// between Verify and here (terminal cleanup).
func (s *FakeServer) entryFor(w http.ResponseWriter, claims auth.Claims) (registry.TaskEntry, bool) {
	e, ok := s.reg.ByDaemon(claims.JobName)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown task")
		return registry.TaskEntry{}, false
	}
	return e, true
}

// handle404 covers S3/S22/S27: fixed 404s that switch the daemon off the
// path (runtime profiles, legacy gc-checks, pending_* results).
func (s *FakeServer) handle404(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r, auth.RequestScope{}); !ok {
		return
	}
	writeError(w, http.StatusNotFound, "not found")
}

// handleNotFound is the fmj_-authenticated catch-all for unknown /api/
// paths (a real server 404 shape, not a passthrough).
func (s *FakeServer) handleNotFound(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r, auth.RequestScope{}); !ok {
		return
	}
	writeError(w, http.StatusNotFound, "not found")
}

// handleLegacyClaim is the per-runtime claim fallback (S32). The path stays a
// 404 — it carries no delivery face — but a task daemon asking here is the only
// observable signal that it switched off the batch path, so its first request
// per entry is recorded (ADR-042).
func (s *FakeServer) handleLegacyClaim(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(w, r, auth.RequestScope{RuntimeID: r.PathValue("rid")})
	if !ok {
		return
	}
	if claims.Role != catalogProbeRole {
		if e, found := s.reg.ByDaemon(claims.JobName); found && s.noteLegacyClaim(e) {
			s.log.Warn("task.legacy_claim_request",
				"task_id", e.TaskID, "job_name", e.JobName,
				"daemon_id", e.DaemonID, "job_runtime_id", e.JobRuntimeID)
		}
	}
	writeError(w, http.StatusNotFound, "not found")
}

// noteLegacyClaim records the first legacy-claim request for an entry instance.
// The key pairs the task id with the claim stamp: a re-dispatched task reuses
// the daemon identity by design, so the entry's start is what marks a new
// instance. The set lives for the process lifetime (ADR-042).
func (s *FakeServer) noteLegacyClaim(e registry.TaskEntry) bool {
	key := e.TaskID + "\x00" + e.ClaimedAt.UTC().Format(time.RFC3339Nano)
	s.legacyClaimMu.Lock()
	defer s.legacyClaimMu.Unlock()
	if _, seen := s.legacyClaimed[key]; seen {
		return false
	}
	s.legacyClaimed[key] = struct{}{}
	return true
}

// jobLookup adapts the registry to auth.JobLookup.
type jobLookup struct{ reg *registry.Registry }

func (a jobLookup) ByDaemon(daemonID string) (auth.JobEntry, bool) {
	e, ok := a.reg.ByDaemon(daemonID)
	if !ok {
		return auth.JobEntry{}, false
	}
	return auth.JobEntry{JobRuntimeID: e.JobRuntimeID}, true
}

// JobTokenIssuer builds the Job Token issuer over the registry's read view
// (the auth ↔ registry wiring the Job-facing surfaces share). tokenTTL is the
// issued lifetime; it also bounds the revocation window (ADR-017). probes is
// the catalog-probe session table a probe token is adjudicated against; a nil
// one leaves probe tokens unverifiable.
func JobTokenIssuer(key []byte, reg *registry.Registry, probes *registry.ProbeRegistry, tokenTTL time.Duration) (*auth.Issuer, error) {
	opts := []auth.Option{}
	if probes != nil {
		opts = append(opts, auth.WithProbeLookup(probeLookup{probes}))
	}
	return auth.NewIssuer(key, jobLookup{reg}, reg, tokenTTL, opts...)
}

// probeLookup adapts the probe session table to auth's read view.
type probeLookup struct{ probes *registry.ProbeRegistry }

func (p probeLookup) ProbeByJob(jobName string) (auth.ProbeEntry, bool) {
	session, ok := p.probes.ProbeByJob(jobName)
	if !ok {
		return auth.ProbeEntry{}, false
	}
	return auth.ProbeEntry{
		JobRuntime:   session.JobRuntime,
		RequestID:    session.RequestID,
		SelfMintedID: session.SelfMintedID,
		TokenHash:    session.TokenHash,
	}, true
}
