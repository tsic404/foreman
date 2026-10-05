package proxy

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// maxRequestBody bounds daemon/agent request bodies (cluster-internal
// surface; the contract pins no limit).
const maxRequestBody = 32 << 20

// TaskScheduler is the fake server's scheduling surface (proxy.md
// §请求路由): S6 delivery, S8–S17 report handling, task-vanish settlement.
type TaskScheduler interface {
	Deliver(daemonID, runtimeID string, maxTasks int) ([]json.RawMessage, error)
	OnReport(ctx context.Context, ep scheduler.Endpoint, e registry.TaskEntry, body []byte) (int, []byte, error)
	OnTaskVanished(ctx context.Context, taskID string) error
}

// FakeServer is the J→F face: it speaks the official server protocol to the
// Job daemons (§1.2). ServeHTTP applies the three routing rules — fmj_
// Job Tokens to the local daemon face, any other credential verbatim to
// the real server (S29), none to 401.
type FakeServer struct {
	issuer  *auth.Issuer
	reg     *registry.Registry
	sched   TaskScheduler
	client  *Client
	ws      *WSHandler // nil when FOREMAN_WS_ENABLED=false
	metrics Metrics
	log     *slog.Logger
	mux     *http.ServeMux
}

// NewFakeServer builds the handler with the full §1.2 routing table.
// ws may be nil; the S30 route is then not registered and WS dials get a
// 404 (the FOREMAN_WS_ENABLED=false degradation, §5.1).
func NewFakeServer(issuer *auth.Issuer, reg *registry.Registry, sched TaskScheduler, client *Client, ws *WSHandler, metrics Metrics) *FakeServer {
	s := &FakeServer{
		issuer:  issuer,
		reg:     reg,
		sched:   sched,
		client:  client,
		ws:      ws,
		metrics: orNoopMetrics(metrics),
		log:     slog.Default().With("component", "fake-server"),
		mux:     http.NewServeMux(),
	}
	s.routes()
	return s
}

// routes registers the local daemon face (§1.2 table; proxy.md §请求路由).
func (s *FakeServer) routes() {
	m := s.mux
	m.HandleFunc("GET /api/daemon/workspaces", s.handleWorkspaces)                                               // S1
	m.HandleFunc("GET /api/workspaces", s.handleWorkspaces)                                                      // S28
	m.HandleFunc("GET /api/daemon/workspaces/{ws}/repos", s.handleRepos)                                         // S2
	m.HandleFunc("GET /api/daemon/workspaces/{ws}/runtime-profiles", s.handle404)                                // S3
	m.HandleFunc("POST /api/daemon/workspaces/{ws}/issues/gc-check", s.handleIssueGCCheck)                       // S21
	m.HandleFunc("POST /api/daemon/register", s.handleRegister)                                                  // S4
	m.HandleFunc("POST /api/daemon/heartbeat", s.handleHeartbeat)                                                // S5
	m.HandleFunc("POST /api/daemon/deregister", s.handleDeregister)                                              // S19
	m.HandleFunc("POST /api/daemon/tasks/claim", s.handleClaim)                                                  // S6
	m.HandleFunc("GET /api/daemon/tasks/{tid}/status", s.handleTaskStatus)                                       // S9
	m.HandleFunc("GET /api/daemon/tasks/{tid}/gc-check", s.handleTaskGCCheck)                                    // S20
	m.HandleFunc("POST /api/daemon/tasks/{tid}/start", s.reportHandler(scheduler.EPStart))                       // S8
	m.HandleFunc("POST /api/daemon/tasks/{tid}/progress", s.reportHandler(scheduler.EPProgress))                 // S10
	m.HandleFunc("POST /api/daemon/tasks/{tid}/messages", s.reportHandler(scheduler.EPMessages))                 // S11
	m.HandleFunc("POST /api/daemon/tasks/{tid}/usage", s.reportHandler(scheduler.EPUsage))                       // S12
	m.HandleFunc("POST /api/daemon/tasks/{tid}/complete", s.reportHandler(scheduler.EPComplete))                 // S13
	m.HandleFunc("POST /api/daemon/tasks/{tid}/fail", s.reportHandler(scheduler.EPFail))                         // S14
	m.HandleFunc("POST /api/daemon/tasks/{tid}/cancel-ack", s.reportHandler(scheduler.EPCancelAck))              // S15
	m.HandleFunc("POST /api/daemon/tasks/{tid}/session", s.reportHandler(scheduler.EPSession))                   // S16
	m.HandleFunc("POST /api/daemon/tasks/{tid}/wait-local-directory", s.reportHandler(scheduler.EPWaitLocalDir)) // S17
	// S23 is defensive-only this version: these calls carry the caller's
	// own credential and are forwarded verbatim (§1.2 rule 2 semantics).
	// The Job Token is still verified first — an invalid fmj_ must 401
	// locally, never reach the upstream.
	m.HandleFunc("GET /api/daemon/tasks/{tid}/plugin-hooks", s.handleS23)
	m.HandleFunc("POST /api/daemon/tasks/{tid}/plugin-hooks", s.handleS23)
	m.HandleFunc("GET /api/daemon/tasks/{tid}/remote-mcp/{cid}/credential", s.handleS23)
	m.HandleFunc("GET /api/daemon/tasks/{tid}/plugin-mcp/{cid}/credential", s.handleS23)
	m.HandleFunc("POST /api/daemon/runtimes/{rid}/tasks/{tid}/prepare-lease", s.handlePrepareLease)         // S7
	m.HandleFunc("POST /api/daemon/runtimes/{rid}/tasks/{tid}/skill-bundles/resolve", s.handleSkillBundles) // S26
	m.HandleFunc("POST /api/daemon/runtimes/{rid}/recover-orphans", s.handleRecoverOrphans)                 // S18
	m.HandleFunc("POST /api/daemon/runtimes/{rid}/models/{req}/result", s.handle404)                        // S27
	m.HandleFunc("POST /api/daemon/runtimes/{rid}/local-skills/{req}/result", s.handle404)                  // S27
	m.HandleFunc("POST /api/daemon/runtimes/{rid}/local-skills/import/{req}/result", s.handle404)           // S27
	m.HandleFunc("POST /api/daemon/runtimes/{rid}/update/{req}/result", s.handle404)                        // S27
	m.HandleFunc("GET /api/daemon/issues/{id}/gc-check", s.handle404)                                       // S22
	m.HandleFunc("GET /api/daemon/chat-sessions/{id}/gc-check", s.handle404)                                // S22
	m.HandleFunc("GET /api/daemon/autopilot-runs/{id}/gc-check", s.handle404)                               // S22
	m.HandleFunc("POST /api/tokens/current/renew", s.handleTokenRenew)                                      // S24
	if s.ws != nil {
		m.Handle("GET /api/daemon/ws", s.ws) // S30
	}
	// Everything else under /api/ with a Job Token is not a local endpoint.
	m.HandleFunc("/api/", s.handleNotFound)
}

// ServeHTTP applies the §1.2 routing rules before the endpoint table.
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

// authenticate verifies the Job Token against the request scope (§1.2 rule
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

// writeRaw replies with an upstream response verbatim (§1.2 forwarding).
func writeRaw(w http.ResponseWriter, code int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

// writeError is the §1.2 unified error shape.
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

// jobLookup adapts the registry to auth.JobLookup.
type jobLookup struct{ reg *registry.Registry }

func (a jobLookup) ByDaemon(daemonID string) (auth.JobEntry, bool) {
	e, ok := a.reg.ByDaemon(daemonID)
	if !ok {
		return auth.JobEntry{}, false
	}
	return auth.JobEntry{JobRuntimeID: e.JobRuntimeID, IsTerminal: e.IsTerminal()}, true
}

// JobTokenIssuer builds the Job Token issuer over the registry's read view
// (the auth ↔ registry wiring the Job-facing surfaces share).
func JobTokenIssuer(key []byte, reg *registry.Registry) (*auth.Issuer, error) {
	return auth.NewIssuer(key, jobLookup{reg}, reg)
}
