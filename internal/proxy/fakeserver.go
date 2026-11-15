// Package proxy's fake server: the face the long-lived daemons dial. Every
// request needs a daemon credential; a non-fmj_ bearer token was always
// forwarded verbatim to the real server in v1 (§2.1 rule 2), and that passthrough
// is gone in v2 — a credential that is not one of our daemons' is 401.
package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/daemonauth"
	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/registry"
)

// Dispatcher is the fake server's scheduling surface: delivery, report
// handling, task-vanish settlement.
type Dispatcher interface {
	Deliver(ctx context.Context, workspaceID, daemonID, runtimeID string, maxTasks int) (dispatch.Delivery, error)
	OnReport(ctx context.Context, ep dispatch.Endpoint, e registry.TaskEntry, body []byte, sessionID string) (int, []byte, error)
	OnTaskVanished(ctx context.Context, taskID string) error
	OnBootTimeout(ctx context.Context, taskID string) error
}

// SkillResolver forwards the daemon's S-26 skill-bundle resolve to the real
// multica server with Foreman's credential (C19). Nil answers an empty set.
type SkillResolver func(ctx context.Context, taskID string, body []byte) (int, []byte, error)

// LeaseForwarder forwards the daemon's S-7 prepare-lease to multica as C4.
type LeaseForwarder func(ctx context.Context, workspaceID, taskID string) (bool, error)

// WithLeaseForwarder wires the S-7 → C4 seam.
func WithLeaseForwarder(fn LeaseForwarder) ServerOption {
	return func(s *FakeServer) { s.lease = fn }
}

// FakeServer is the D→F face: it speaks the official server protocol to the
// registered daemons. ServeHTTP applies two rules — a configured daemon
// credential to the local daemon face, anything else 401.
type FakeServer struct {
	creds   *daemonauth.Store
	reg     *registry.Registry
	disp    Dispatcher
	metrics Metrics
	log     *slog.Logger
	mux     *http.ServeMux
	skills  SkillResolver
	lease   LeaseForwarder
	// onDaemonChanged fires after a daemon registers or deregisters, so the
	// assembly can re-derive its C1 declaration from the registry and have the
	// claim loop tell the server about the new runtime set.
	onDaemonChanged func()
}

// ServerOption customizes a FakeServer.
type ServerOption func(*FakeServer)

// WithServerMetrics wires the metrics seam.
func WithServerMetrics(m Metrics) ServerOption {
	return func(s *FakeServer) { s.metrics = orNoopMetrics(m) }
}

// WithServerLogger overrides the logger.
func WithServerLogger(l *slog.Logger) ServerOption {
	return func(s *FakeServer) { s.log = l.With("component", "fake-server") }
}

// WithDaemonChangeHook wires the post-registration callback.
func WithDaemonChangeHook(fn func()) ServerOption {
	return func(s *FakeServer) { s.onDaemonChanged = fn }
}

// WithSkillResolver wires the S-26 forward seam.
func WithSkillResolver(fn SkillResolver) ServerOption {
	return func(s *FakeServer) { s.skills = fn }
}

// notifyChanged runs the daemon-change hook, if wired.
func (s *FakeServer) notifyChanged() {
	if s.onDaemonChanged != nil {
		s.onDaemonChanged()
	}
}

// NewFakeServer builds the daemon face.
func NewFakeServer(creds *daemonauth.Store, reg *registry.Registry, disp Dispatcher, opts ...ServerOption) *FakeServer {
	s := &FakeServer{
		creds:   creds,
		reg:     reg,
		disp:    disp,
		metrics: noopMetrics{},
		log:     slog.Default().With("component", "fake-server"),
		mux:     http.NewServeMux(),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.routes()
	return s
}

// ServeHTTP applies the routing rules before the endpoint table.
func (s *FakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/api/") {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	s.mux.ServeHTTP(w, r)
}

// ---- routing ----

func (s *FakeServer) routes() {
	m := s.mux
	handle := func(pattern string, h http.HandlerFunc) { m.Handle(pattern, h) }
	handle("GET /api/daemon/workspaces", s.handleWorkspaces)                                              // S1
	handle("GET /api/workspaces", s.handleWorkspaces)                                                     // S28
	handle("POST /api/daemon/register", s.handleRegister)                                                 // S4
	handle("POST /api/daemon/heartbeat", s.handleHeartbeat)                                               // S5
	handle("POST /api/daemon/deregister", s.handleDeregister)                                             // S19
	handle("POST /api/daemon/tasks/claim", s.handleClaim)                                                 // S6
	handle("GET /api/daemon/tasks/{tid}/status", s.handleTaskStatus)                                      // S9
	handle("POST /api/daemon/tasks/{tid}/start", s.reportHandler(dispatch.EPStart))                       // S8
	handle("POST /api/daemon/tasks/{tid}/progress", s.reportHandler(dispatch.EPProgress))                 // S10
	handle("POST /api/daemon/tasks/{tid}/messages", s.reportHandler(dispatch.EPMessages))                 // S11
	handle("POST /api/daemon/tasks/{tid}/usage", s.reportHandler(dispatch.EPUsage))                       // S12
	handle("POST /api/daemon/tasks/{tid}/complete", s.reportHandler(dispatch.EPComplete))                 // S13
	handle("POST /api/daemon/tasks/{tid}/fail", s.reportHandler(dispatch.EPFail))                         // S14
	handle("POST /api/daemon/tasks/{tid}/cancel-ack", s.reportHandler(dispatch.EPCancelAck))              // S15
	handle("POST /api/daemon/tasks/{tid}/session", s.reportHandler(dispatch.EPSession))                   // S16
	handle("POST /api/daemon/tasks/{tid}/wait-local-directory", s.reportHandler(dispatch.EPWaitLocalDir)) // S17
	handle("POST /api/daemon/runtimes/{rid}/tasks/{tid}/prepare-lease", s.handlePrepareLease)             // S7
	handle("GET /api/daemon/workspaces/{ws}/repos", s.handleRepos)                                        // S2
	handle("GET /api/daemon/workspaces/{ws}/runtime-profiles", s.handleRuntimeProfiles)                   // S3
	handle("POST /api/daemon/runtimes/{rid}/tasks/{tid}/skill-bundles/resolve", s.handleSkillBundles)     // S26
	handle("POST /api/tokens/current/renew", s.handleTokenRenew)                                          // S24
	// Everything else under /api/ with a daemon credential is not a local
	// endpoint.
	handle("/api/", s.handleNotFound)
}

// ---- auth ----

// credential resolves and validates a request's bearer token against the
// configured daemon set, failing closed on every defect.
func (s *FakeServer) credential(w http.ResponseWriter, r *http.Request) (daemonauth.Credential, bool) {
	token, ok := bearerToken(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "daemon credential required")
		return daemonauth.Credential{}, false
	}
	cred, err := s.creds.Verify(token)
	if err != nil {
		s.metrics.AuthFailure(reason(err))
		s.log.Warn("daemon credential rejected", "remote_addr", r.RemoteAddr, "path", r.URL.Path)
		writeError(w, http.StatusUnauthorized, "unknown daemon credential")
		return daemonauth.Credential{}, false
	}
	return cred, true
}

// ---- S1/S28 workspaces ----

func (s *FakeServer) handleWorkspaces(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.credential(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, []map[string]string{{"id": cred.WorkspaceID, "name": ""}})
}

// ---- S4 register ----

type daemonRegisterRequest struct {
	DaemonID    string            `json:"daemon_id"`
	WorkspaceID string            `json:"workspace_id"`
	Runtimes    []registerRuntime `json:"runtimes"`
}

type registerRuntime struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Version   string `json:"version"`
	ProfileID string `json:"profile_id"`
	Status    string `json:"status"`
}

func (s *FakeServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.credential(w, r)
	if !ok {
		return
	}
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	var req daemonRegisterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid register body")
		return
	}
	if req.DaemonID != "" && req.DaemonID != cred.DaemonID {
		writeError(w, http.StatusForbidden, "daemon_id does not belong to this credential")
		return
	}
	if req.WorkspaceID != "" && req.WorkspaceID != cred.WorkspaceID {
		writeError(w, http.StatusForbidden, "workspace does not belong to this credential")
		return
	}
	if len(req.Runtimes) == 0 {
		writeError(w, http.StatusBadRequest, "register requires at least one runtime")
		return
	}
	// A long-lived daemon registers its runtime set once and keeps it across
	// tasks. Its runtime id is the self-chosen one: stable, so Foreman can
	// target it and the server's real-runtime id stays alive.
	runtimeID := req.DaemonID // the daemon's identity is its runtime's identity
	rt := registry.Runtime{
		RuntimeID:    runtimeID,
		DaemonID:     cred.DaemonID,
		Provider:     req.Runtimes[0].Type,
		ProfileID:    req.Runtimes[0].ProfileID,
		Name:         req.Runtimes[0].Name,
		WorkspaceID:  cred.WorkspaceID,
		Online:       true,
		RegisteredAt: s.reg.Now(),
	}
	s.reg.PutRuntime(rt)
	s.notifyChanged()
	answer := map[string]any{
		"id":         runtimeID,
		"name":       rt.Name,
		"provider":   rt.Provider,
		"status":     "online",
		"profile_id": nil,
	}
	if rt.ProfileID != "" {
		answer["profile_id"] = rt.ProfileID
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"runtimes":      []any{answer},
		"repos":         []any{},
		"repos_version": "",
	})
	s.log.Info("daemon.registered",
		"daemon_id", cred.DaemonID, "runtime_id", runtimeID, "workspace_id", cred.WorkspaceID,
		"remote_addr", r.RemoteAddr)
}

// ---- S5 heartbeat ----

func (s *FakeServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.credential(w, r)
	if !ok {
		return
	}
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	var req struct {
		RuntimeID string `json:"runtime_id"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.RuntimeID == "" {
		writeError(w, http.StatusBadRequest, "invalid heartbeat body")
		return
	}
	rt, known := s.reg.RuntimeByID(req.RuntimeID)
	if !known || rt.DaemonID != cred.DaemonID {
		writeError(w, http.StatusNotFound, "runtime not found")
		return
	}
	s.reg.TouchHeartbeat(req.RuntimeID, s.reg.Now())
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "runtime_id": req.RuntimeID})
}

// ---- S19 deregister ----

func (s *FakeServer) handleDeregister(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.credential(w, r)
	if !ok {
		return
	}
	if rt, known := s.reg.RuntimeByDaemon(cred.DaemonID); known {
		s.reg.SetRuntimeOffline(rt.RuntimeID)
		s.notifyChanged()
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- S6 claim ----

type daemonClaimRequest struct {
	DaemonID   string   `json:"daemon_id"`
	RuntimeIDs []string `json:"runtime_ids"`
	MaxTasks   int      `json:"max_tasks"`
}

func (s *FakeServer) handleClaim(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.credential(w, r)
	if !ok {
		return
	}
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	var req daemonClaimRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid claim body")
		return
	}
	if req.DaemonID != "" && req.DaemonID != cred.DaemonID {
		writeError(w, http.StatusForbidden, "daemon_id does not belong to this credential")
		return
	}
	runtimeID := ""
	if len(req.RuntimeIDs) > 0 {
		runtimeID = req.RuntimeIDs[0]
	}
	delivery, err := s.disp.Deliver(r.Context(), cred.WorkspaceID, cred.DaemonID, runtimeID, req.MaxTasks)
	if err != nil {
		if errors.Is(err, dispatch.ErrUnknownDaemon) || errors.Is(err, dispatch.ErrRuntimeMismatch) {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		s.log.Error("deliver failed", "daemon_id", cred.DaemonID, "err", err)
		writeError(w, http.StatusInternalServerError, "deliver failed")
		return
	}
	if delivery.Delivered {
		s.metrics.DeliveryClaim("delivered")
		s.metrics.DeliveryLatency(delivery.Latency.Seconds())
	} else {
		s.metrics.DeliveryClaim("empty")
	}
	tasks := delivery.Tasks
	if tasks == nil {
		tasks = []json.RawMessage{}
	}
	writeRaw(w, http.StatusOK, mustMarshal(map[string]any{"tasks": tasks}))
}

// ---- S7 prepare-lease ----

func (s *FakeServer) handlePrepareLease(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.credential(w, r)
	if !ok {
		return
	}
	rid := r.PathValue("rid")
	rt, known := s.reg.RuntimeByID(rid)
	if !known || rt.DaemonID != cred.DaemonID {
		writeError(w, http.StatusNotFound, "runtime not found")
		return
	}
	// The daemon renews the server-side lease of the task it was handed: this
	// must reach multica as C4, or the prepare lease lapses and the server
	// rejects the later start with runtime_offline. Forward to multica when a
	// seam is wired; otherwise answer ok locally (test fallback).
	tid := r.PathValue("tid")
	if s.lease != nil {
		ready, err := s.lease(ctxFor(r), cred.WorkspaceID, tid)
		if err != nil {
			writeError(w, http.StatusBadGateway, "prepare-lease forward failed")
			return
		}
		if !ready {
			writeError(w, http.StatusBadRequest, "task not renewable")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ctxFor returns the request context.
func ctxFor(r *http.Request) context.Context { return r.Context() }

// ---- S2 repos ----

// handleRepos answers S-2 with an empty repo set: v2 carries no node-local
// repository cache (the resident daemon owns its workspace state), so there
// is nothing to report.
func (s *FakeServer) handleRepos(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.credential(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"repos": []any{}, "repos_version": "", "workspace_id": cred.WorkspaceID})
}

// ---- S3 runtime profiles ----

// handleRuntimeProfiles answers S-3 with an empty profile list: the daemon
// runs the runtime it registered, and v2 has no server-side profile set to
// serve.
func (s *FakeServer) handleRuntimeProfiles(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.credential(w, r); !ok {
		return
	}
	writeJSON(w, http.StatusOK, []any{})
}

// ---- S24 token renew ----

// handleTokenRenew answers S-24 locally: the daemon credential is static, so
// there is nothing to renew against.
func (s *FakeServer) handleTokenRenew(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.credential(w, r); !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"expires_at": time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
		"renewed":    true,
	})
}

// ---- S26 skill bundles ----

// handleSkillBundles answers S-26 with an empty bundle set. The resident
// daemon resolves skills itself from its own cache; v2 has no skill-bundle
// proxy, so an empty set is the honest answer (a daemon that needs a bundle
// Foreman does not carry falls back to its own resolution).
func (s *FakeServer) handleSkillBundles(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.credential(w, r); !ok {
		return
	}
	if s.skills == nil {
		writeJSON(w, http.StatusOK, map[string]any{"skills": []any{}})
		return
	}
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	code, resp, err := s.skills(r.Context(), r.PathValue("tid"), body)
	if err != nil {
		writeError(w, http.StatusBadGateway, "skill bundle resolve failed")
		return
	}
	writeRaw(w, code, resp)
}

// ---- S9 status ----

func (s *FakeServer) handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.credential(w, r)
	if !ok {
		return
	}
	tid := r.PathValue("tid")
	e, known := s.reg.Get(tid)
	if !known || e.WorkspaceID != cred.WorkspaceID {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	status := string(e.State)
	if e.IsTerminal() {
		status = string(e.Result)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": status})
}

// ---- S8/S10-S17 forwarded reports ----

func (s *FakeServer) reportHandler(ep dispatch.Endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tid := r.PathValue("tid")
		cred, ok := s.credential(w, r)
		if !ok {
			return
		}
		e, known := s.reg.ByDaemonInWorkspace(cred.WorkspaceID, cred.DaemonID)
		if !known || e.TaskID != tid {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		body, ok := s.readBody(w, r)
		if !ok {
			return
		}
		sessionID := ""
		if learnsSession(ep) {
			sessionID = s.learnSession(e, body)
		}
		code, resp, err := s.disp.OnReport(r.Context(), ep, e, body, sessionID)
		if err != nil {
			s.log.Error("report forward failed", "task_id", tid, "endpoint", string(ep), "err", err)
			writeError(w, http.StatusBadGateway, "upstream unavailable")
			return
		}
		writeRaw(w, code, resp)
	}
}

// learnsSession reports whether ep's body carries the session id that teaches
// the session index (ADR-015): the terminal reports S13/S14.
func learnsSession(ep dispatch.Endpoint) bool {
	return ep == dispatch.EPComplete || ep == dispatch.EPFail
}

func (s *FakeServer) learnSession(e registry.TaskEntry, body []byte) string {
	var report struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(body, &report); err != nil {
		return ""
	}
	if report.SessionID != "" && e.RuntimeID != "" {
		s.reg.LearnSession(e.WorkspaceID, report.SessionID, e.RuntimeID)
	}
	return report.SessionID
}

// ---- S404 ----

func (s *FakeServer) handleNotFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotFound, "not found")
}

// ---- helpers ----

func (s *FakeServer) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := readLimited(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "unreadable body")
		return nil, false
	}
	if len(body) > maxBodyBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "body too large")
		return nil, false
	}
	return body, true
}

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

func writeJSON(w http.ResponseWriter, status int, v any) {
	raw := mustMarshal(v)
	writeRaw(w, status, raw)
}

func writeRaw(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func reason(err error) string {
	switch {
	case errors.Is(err, daemonauth.ErrMissingCredential):
		return "missing"
	case errors.Is(err, daemonauth.ErrUnknownCredential):
		return "unknown"
	default:
		return "invalid"
	}
}

var _ = time.Now
