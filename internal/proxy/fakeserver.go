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
	"strconv"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/daemonauth"
	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/registry"
)

// Dispatcher is the fake server's scheduling surface: delivery, report
// handling, task-vanish settlement.
type Dispatcher interface {
	Deliver(ctx context.Context, workspaceID, daemonID string, runtimeIDs []string, maxTasks int) (dispatch.Delivery, error)
	OnReport(ctx context.Context, ep dispatch.Endpoint, e registry.TaskEntry, body []byte, sessionID string) (int, []byte, error)
	OnTaskVanished(ctx context.Context, taskID string) error
	OnBootTimeout(ctx context.Context, taskID string) error
}

// SkillResolver forwards the daemon's S-26 skill-bundle resolve to the real
// multica server with Foreman's credential (C19). Nil answers an empty set.
type SkillResolver func(ctx context.Context, taskID string, body []byte) (int, []byte, error)

// LeaseForwarder forwards the daemon's S-7 prepare-lease to multica as C4.
type LeaseForwarder func(ctx context.Context, workspaceID, taskID string) (bool, error)

// TaskForwarder forwards one task-addressed daemon call (S-31 context
// receipt) to the multica server verbatim.
type TaskForwarder func(ctx context.Context, taskID string, body []byte) (int, []byte, error)

// WithLeaseForwarder wires the S-7 → C4 seam.
func WithLeaseForwarder(fn LeaseForwarder) ServerOption {
	return func(s *FakeServer) { s.lease = fn }
}

// WithTaskForwarder wires the S-31 context-receipt seam.
func WithTaskForwarder(fn TaskForwarder) ServerOption {
	return func(s *FakeServer) { s.receipt = fn }
}

// FakeServer is the D→F face: it speaks the official server protocol to the
// registered daemons. ServeHTTP applies two rules — a configured daemon
// credential to the local daemon face, anything else 401.
type FakeServer struct {
	creds       *daemonauth.Store
	reg         *registry.Registry
	disp        Dispatcher
	metrics     Metrics
	log         *slog.Logger
	mux         *http.ServeMux
	skills      SkillResolver
	lease       LeaseForwarder
	reposFwd    WorkspaceForwarder
	profilesFwd WorkspaceForwarder
	receipt     TaskForwarder
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
	handle("POST /api/daemon/runtimes/{rid}/recover-orphans", s.handleRecoverOrphans)                     // S18
	handle("POST /api/daemon/tasks/{tid}/context-receipt", s.handleContextReceipt)                        // S31 (v0.9.0)
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
		// S-28 is a user-API path that happens to be mounted here for protocol
		// completeness, so a CLI (or an agent shelling out to one) reaching it
		// with a user PAT is expected, not a fault. Log that case at DEBUG so it
		// cannot bury genuine credential problems.
		level := s.log.Warn
		if r.URL.Path == "/api/workspaces" {
			level = s.log.Debug
		}
		level("daemon credential rejected", "remote_addr", r.RemoteAddr,
			"path", r.URL.Path, "reason", reason(err))
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
	DeviceName  string            `json:"device_name"`
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
	// A long-lived daemon registers its whole runtime set once and keeps it
	// across tasks. Each declared runtime gets its own registry row keyed by a
	// stable per-runtime identity: the daemon's own id for the builtin runtime,
	// and daemon-id/profile-id for a custom runtime profile. The server's real
	// id is assigned later (C1 response), so the daemon's self-chosen id stays
	// the targeting key Foreman uses on delivery (§runtime-id 双轨).
	answer := make([]map[string]any, 0, len(req.Runtimes))
	for i, decl := range req.Runtimes {
		// The key must be URL-safe: the daemon embeds it in runtime-scoped
		// paths (prepare-lease, skill bundles), and a slash would split the
		// path into extra segments and miss every route. A dot is the separator.
		key := req.DaemonID
		if i > 0 || decl.ProfileID != "" {
			profile := decl.ProfileID
			if profile == "" {
				profile = "builtin-" + strconv.Itoa(i)
			}
			key = req.DaemonID + "." + profile
		}
		rt := registry.Runtime{
			RuntimeID:    key,
			DaemonID:     cred.DaemonID,
			Provider:     decl.Type,
			ProfileID:    decl.ProfileID,
			Name:         decl.Name,
			WorkspaceID:  cred.WorkspaceID,
			DeviceName:   req.DeviceName,
			Online:       true,
			RegisteredAt: s.reg.Now(),
		}
		s.reg.PutRuntime(rt)
		entry := map[string]any{
			"id":       key,
			"name":     rt.Name,
			"provider": rt.Provider,
			"status":   "online",
		}
		if rt.ProfileID != "" {
			entry["profile_id"] = rt.ProfileID
		} else {
			entry["profile_id"] = nil
		}
		answer = append(answer, entry)
	}
	s.notifyChanged()
	writeJSON(w, http.StatusOK, map[string]any{
		"runtimes":      answer,
		"repos":         []any{},
		"repos_version": "",
	})
	s.log.Info("daemon.registered",
		"daemon_id", cred.DaemonID, "runtimes", len(answer), "workspace_id", cred.WorkspaceID,
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
	// C-14 names a daemon identity, and a daemon declares its whole runtime
	// set in one C-1 — so the whole set goes offline together. Flipping one
	// arbitrary row would leave the daemon's other runtimes dispatchable until
	// the heartbeat sweep noticed.
	if flipped := s.reg.SetDaemonOffline(cred.DaemonID); len(flipped) > 0 {
		s.log.Info("daemon.deregistered", "daemon_id", cred.DaemonID, "runtimes", flipped)
		s.notifyChanged()
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- S6 claim ----

// daemonClaimRequest is the S-6 body. A daemon claims for its whole runtime
// set in one call: runtime_ids lists every runtime it registered, so a
// multi-runtime daemon is answered through whichever of them Foreman bound
// the task to.
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
	delivery, err := s.disp.Deliver(r.Context(), cred.WorkspaceID, cred.DaemonID, req.RuntimeIDs, req.MaxTasks)
	if err != nil {
		// "No task bound to this daemon right now" is the ordinary case, not a
		// fault: a daemon polls on its own cadence and regularly arrives before
		// Foreman has claimed anything for it, and a multi-runtime daemon sends
		// one claim per runtime. Answer it as an empty claim and keep the log
		// quiet — this fires several times a minute per daemon. Only a genuine
		// dispatch failure is worth an error line.
		if errors.Is(err, dispatch.ErrRuntimeMismatch) || errors.Is(err, dispatch.ErrUnknownDaemon) {
			s.metrics.DeliveryClaim("empty")
			s.log.Debug("claim_deferred", "reason", "no_bound_task",
				"daemon_id", cred.DaemonID, "runtime_ids", req.RuntimeIDs)
			writeRaw(w, http.StatusOK, mustMarshal(map[string]any{"tasks": []any{}}))
			return
		}
		s.log.Error("deliver failed", "daemon_id", cred.DaemonID, "workspace_id", cred.WorkspaceID,
			"runtime_ids", req.RuntimeIDs, "err", err)
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
	//
	// The runtime check above proves the caller owns {rid}; it does not prove
	// {tid} is that runtime's task. Checking both keeps one daemon from
	// renewing (and thereby probing) another's lease through its own runtime id.
	tid := r.PathValue("tid")
	if owner := s.reg.OwnerOf(tid); owner != "" && owner != cred.DaemonID {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	if s.lease != nil {
		ready, err := s.lease(ctxFor(r), cred.WorkspaceID, tid)
		if err != nil {
			// The daemon turns this into a failed start with runtime_offline.
			// Without a line here that symptom has no matching cause anywhere.
			s.log.Error("prepare-lease forward failed",
				"task_id", tid, "runtime_id", rid, "daemon_id", cred.DaemonID, "err", err)
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

// handleRecoverOrphans answers S-18 with zeros: orphans inside a long-lived
// daemon's run set are the daemon's own business — it reaps its run
// directories itself — so Foreman always reports nothing to recover. The
// daemon calls this once after a re-registration.
func (s *FakeServer) handleRecoverOrphans(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.credential(w, r); !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"orphaned": 0, "retried": 0})
}

// handleContextReceipt forwards S-31 (v0.9.0) to the multica server: the
// daemon acknowledges the task context it received, and the server records
// the receipt against the task. The forward rewrites nothing — path and body
// travel as the daemon sent them.
func (s *FakeServer) handleContextReceipt(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.credential(w, r)
	if !ok {
		return
	}
	// The receipt acknowledges the context of the task this daemon was handed,
	// so a foreign daemon must not be able to post one for another's task. The
	// retained owner is consulted as well as the live row, the same rule the
	// report routes use — a settled task keeps its owner. A task with no owner
	// at all (after a Foreman restart) passes: the receipt is self-describing.
	if owner := s.reg.OwnerOf(r.PathValue("tid")); owner != "" && owner != cred.DaemonID {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	if s.receipt == nil {
		// Without the forward seam the honest answer is the upstream's own
		// "does not accept receipts" shape, which the daemon treats as a
		// one-time capability probe and never retries.
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	code, resp, err := s.receipt(r.Context(), r.PathValue("tid"), body)
	if err != nil {
		s.log.Error("context receipt forward failed",
			"task_id", r.PathValue("tid"), "err", err)
		writeError(w, http.StatusBadGateway, "context receipt forward failed")
		return
	}
	writeRaw(w, code, resp)
}

// ctxFor returns the request context.
func ctxFor(r *http.Request) context.Context { return r.Context() }

// ---- S2 repos / S3 runtime profiles ----

// ErrUnboundTask reports a task-addressed forward with no local binding: the
// request is well-formed but there is nothing to resolve it against.
var ErrUnboundTask = errors.New("task is not bound")

// WorkspaceForwarder forwards one workspace-scoped read (C22 repos, C18
// runtime profiles) to the multica server with that workspace's credential.
type WorkspaceForwarder func(ctx context.Context, workspaceID string) (int, []byte, error)

// WithWorkspaceForwarder wires the S-2/S-3 forward seam. A nil repos or
// profiles forwarder answers the v2 static shape (empty set), which the
// daemon treats as "no workspace state, built-in runtimes".
func WithWorkspaceForwarders(repos, profiles WorkspaceForwarder) ServerOption {
	return func(s *FakeServer) { s.reposFwd = repos; s.profilesFwd = profiles }
}

// handleRepos forwards S-2 to C-22 verbatim: the daemon reads the repo set,
// the repos_version and the settings map, and Foreman never interprets any of
// them (§1.7). An upstream failure answers 502 — the daemon keeps its
// previous state and continues.
func (s *FakeServer) handleRepos(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.credential(w, r)
	if !ok {
		return
	}
	if s.reposFwd == nil {
		writeJSON(w, http.StatusOK, map[string]any{"repos": []any{}, "repos_version": "", "workspace_id": cred.WorkspaceID})
		return
	}
	code, body, err := s.reposFwd(r.Context(), cred.WorkspaceID)
	if err != nil {
		// The daemon silently keeps its previous repo state on a 502; this is
		// the only place the cause is visible.
		s.log.Error("workspace repos forward failed",
			"workspace_id", cred.WorkspaceID, "err", err)
		writeError(w, http.StatusBadGateway, "workspace repos forward failed")
		return
	}
	writeRaw(w, code, body)
}

// handleRuntimeProfiles forwards S-3 to C-18 verbatim: a 404 is the
// workspace's own "no custom profiles" answer and reaches the daemon as 404
// (it continues with its built-in runtimes), a 200 replays the profile list
// byte for byte.
func (s *FakeServer) handleRuntimeProfiles(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.credential(w, r)
	if !ok {
		return
	}
	// The workspace is the credential's, never the path's: `{ws}` is
	// caller-controlled, so forwarding it would let one workspace's daemon
	// read another's profile list. handleRepos (S-2) already takes this
	// shape; this route is its sibling and must match.
	if ws := r.PathValue("ws"); ws != "" && ws != cred.WorkspaceID {
		writeError(w, http.StatusNotFound, "workspace not found")
		return
	}
	if s.profilesFwd == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	code, body, err := s.profilesFwd(r.Context(), cred.WorkspaceID)
	if err != nil {
		// The daemon falls back to its built-in runtimes on a 502; the cause
		// otherwise leaves no trace on the Foreman side.
		s.log.Error("runtime profiles forward failed",
			"workspace_id", cred.WorkspaceID, "err", err)
		writeError(w, http.StatusBadGateway, "runtime profiles forward failed")
		return
	}
	writeRaw(w, code, body)
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
	cred, ok := s.credential(w, r)
	if !ok {
		return
	}
	// S-26 resolves under the task's own runtime, so only the daemon holding
	// that task may ask. Without this check a daemon could point the resolve at
	// another daemon's task id (and an arbitrary runtime id in the path) and
	// read its bundle set back.
	//
	// The retained owner is consulted too: once a task settles its live row is
	// reclaimed, and testing only the live row would let any daemon resolve
	// against a settled task another daemon owned. A task with no owner at all
	// (after a Foreman restart) is still allowed through — the resolver answers
	// ErrUnboundTask → 404 for exactly that case.
	if owner := s.reg.OwnerOf(r.PathValue("tid")); owner != "" && owner != cred.DaemonID {
		writeError(w, http.StatusNotFound, "task not found")
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
		// "This task is not mine to resolve" is a 404 — the daemon's own answer
		// for a task it holds nothing for. Only an upstream/transport failure is
		// a 502; conflating the two makes the daemon retry a task that no longer
		// exists.
		if errors.Is(err, ErrUnboundTask) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		// A 502 here makes the daemon fall back to its own cache; the upstream
		// cause must still be attributable somewhere.
		s.log.Error("skill bundle resolve failed",
			"task_id", r.PathValue("tid"), "err", err)
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
	// Every task-scoped route checks the daemon binding, not just the
	// workspace: a shared workspace can host several daemons, and one task's
	// state is its holder's business. Without this a daemon could read the
	// progress and result of another daemon's task.
	if !known || e.WorkspaceID != cred.WorkspaceID || e.DaemonID != cred.DaemonID {
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
		// Reports address one task by id. Looking up "the daemon's current
		// task" instead would break as soon as a daemon holds more than one:
		// the id under report need not be the one the index yields next.
		//
		// A missing entry is not a reason to drop the report. Foreman's
		// registry is in-memory, so a Foreman restart loses every binding while
		// the daemons keep running their tasks; refusing their reports would
		// strand those tasks on the server forever (the daemon retries a
		// rejected terminal report indefinitely). The report is self-describing
		// — task id plus the daemon's own credential — which is all the forward
		// needs, so an unbound report is forwarded rather than rejected.
		e, known := s.reg.GetInWorkspace(cred.WorkspaceID, tid)
		if known && e.DaemonID != cred.DaemonID {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		if !known {
			// The live row is gone (a restart, or the task already settled).
			// The retained owner still tells us whose task this was, so a
			// report from any other daemon is refused instead of forwarded.
			if owner := s.reg.OwnerOf(tid); owner != "" && owner != cred.DaemonID {
				writeError(w, http.StatusNotFound, "task not found")
				return
			}
			e = registry.TaskEntry{
				TaskID:      tid,
				WorkspaceID: cred.WorkspaceID,
				DaemonID:    cred.DaemonID,
			}
			s.log.Debug("report for unbound task", "task_id", tid, "daemon_id", cred.DaemonID,
				"endpoint", string(ep))
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
	// The catch-all answers only an authenticated caller. Every real route
	// guards inside its handler, which leaves this one reachable without a
	// credential — and its 404 would tell an anonymous caller which paths exist
	// (a mounted route 401s, an unmounted one 404s). Applying the same guard
	// here makes the two indistinguishable.
	if _, ok := s.credential(w, r); !ok {
		return
	}
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
