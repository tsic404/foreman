package proxy

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// tokenRenewResponseTTL is the S24 synthetic expiry (§2.2: now+24h; the Job
// Token is not actually renewed through this endpoint).
const tokenRenewResponseTTL = 24 * time.Hour

// ---- S1/S28 workspaces ----

// handleWorkspaces answers with the Job's single workspace (S1; also the
// legacy /api/workspaces fallback S28). It must succeed, else the daemon
// finds no runtime. A task Job and a catalog probe see their own workspace;
// a startup / registration-discovery probe sees the discovery set (§2.2
// S-1) — the set the deployment's credentials scope to, from which the
// template's daemon registers without narrowing (ADR-049).
func (s *FakeServer) handleWorkspaces(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(w, r, auth.RequestScope{})
	if !ok {
		return
	}
	ids := []string{claims.WorkspaceID}
	if claims.Role == catalogProbeRole {
		if session, found := s.probeSession(claims); found && session.WorkspaceID == "" {
			ids = s.discoverySet()
		}
	}
	answer := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		client := s.clientFor(id)
		if client == nil {
			// S-1 promises the Job's own workspace (single element). A
			// workspace this deployment does not serve is a routing mismatch,
			// answered like the res/{ws} routes — a short list would read as
			// "this workspace exists and has no name".
			writeError(w, http.StatusForbidden, "workspace is not configured")
			return
		}
		_, name := client.Workspace()
		answer = append(answer, map[string]string{"id": id, "name": name})
	}
	writeJSON(w, http.StatusOK, answer)
}

// ---- S2 repos ----

// handleRepos answers S-2. Every request triggers C-22 (§1.7): the refreshed
// workspace state replaces the cache on success and the last snapshot answers
// on failure; the register-before window answers an empty repo set with no
// settings and never calls C-22.
func (s *FakeServer) handleRepos(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(w, r, auth.RequestScope{})
	if !ok {
		return
	}
	ws := r.PathValue("ws")
	if !s.allowsWorkspace(claims, ws) {
		writeError(w, http.StatusForbidden, "workspace does not belong to this job")
		return
	}
	client := s.clientFor(ws)
	if client == nil {
		writeError(w, http.StatusForbidden, "workspace is not configured")
		return
	}
	// A failed refresh is non-fatal: the answer replays the last successful
	// snapshot and never blocks the checkout (§1.7). The failure event and
	// metric are recorded by the client seam itself.
	_ = client.RefreshWorkspaceState(r.Context())
	body := workspaceStateResponse(client.WorkspaceState())
	body["workspace_id"] = ws
	writeRaw(w, http.StatusOK, mustMarshal(body))
}

// ---- S3 runtime profiles ----

// handleRuntimeProfiles answers S-3 from the C-18 snapshot: 200 replays the
// current replay source byte for byte, 404 stands for a C-18 404, and 503 for
// a snapshot that never succeeded (§1.6). The daemon treats any failure as
// best-effort and continues with its builtin runtimes.
func (s *FakeServer) handleRuntimeProfiles(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(w, r, auth.RequestScope{})
	if !ok {
		return
	}
	ws := r.PathValue("ws")
	if !s.allowsWorkspace(claims, ws) {
		writeError(w, http.StatusForbidden, "workspace does not belong to this job")
		return
	}
	client := s.clientFor(ws)
	if client == nil {
		writeError(w, http.StatusForbidden, "workspace is not configured")
		return
	}
	code, body := client.S3Response()
	if code == http.StatusOK {
		writeRaw(w, code, body)
		return
	}
	writeError(w, code, "runtime profiles unavailable")
}

// ---- S4 register ----

type daemonRegisterRequest struct {
	DaemonID string `json:"daemon_id"`
	// WorkspaceID is the workspace this registration belongs to. The official
	// daemon sends it per registration; an older body omits it and the Job
	// Token's workspace stands in (§1.2 控制面逐套).
	WorkspaceID string `json:"workspace_id"`
	// CLIVersion is the template CLI's version: the startup probe's value
	// becomes C1's cli_version (C-1 的取值来源与声明域).
	CLIVersion string            `json:"cli_version"`
	Runtimes   []registerRuntime `json:"runtimes"`
	// FailedProfiles travels verbatim into C1; Foreman never interprets it.
	FailedProfiles []json.RawMessage `json:"failed_profiles"`
}

// registerRuntime is one entry of a daemon's registration body.
type registerRuntime struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Version   string `json:"version"`
	ProfileID string `json:"profile_id"`
}

// handleRegister answers S4 with the single job runtime (the daemon's other
// runtimes are dropped), records the registration and notifies a connected
// WS that the task is deliverable.
func (s *FakeServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(w, r, auth.RequestScope{})
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
	if req.DaemonID != "" && req.DaemonID != claims.JobName {
		writeError(w, http.StatusForbidden, "daemon_id does not belong to this job")
		return
	}
	if claims.Role != catalogProbeRole && req.WorkspaceID != "" && req.WorkspaceID != claims.WorkspaceID {
		// A task Job registers only under its own workspace; a workspace
		// mismatch is the cross-workspace impersonation case (§2.1).
		writeError(w, http.StatusForbidden, "workspace does not belong to this job")
		return
	}
	if claims.Role == catalogProbeRole {
		s.registerProbe(w, r, claims, req)
		return
	}
	e, ok := s.entryFor(w, claims)
	if !ok {
		return
	}
	// The Job's runtime identity is the one its claim was dispatched under,
	// recorded on the entry at claim time (§2.3): the server-side row id may
	// have moved since — a rebuild rewrites it — but the identity cannot. An
	// entry that carries no record (rebuilt after a restart, or a claim that
	// raced a declaration move) falls back to the live declaration, which can
	// only ever confirm an id it currently holds; when neither resolves, the
	// registration is refused instead of answered for another profile or for
	// the same type's builtin.
	client := s.clientFor(e.WorkspaceID)
	if client == nil {
		writeError(w, http.StatusForbidden, "workspace is not configured")
		return
	}
	identity := e.RuntimeIdentity
	if identity == "" {
		identity, _ = client.RuntimeIdentity(e.RealRuntimeID)
	}
	declared, matched := selectDeclaredRuntime(req.Runtimes, identity)
	if !matched {
		s.log.Error("job declared no runtime for its identity; refusing registration",
			"task_id", e.TaskID, "job_name", e.JobName, "identity", identity)
		writeError(w, http.StatusConflict, "runtime identity not declared by this job")
		if err := s.sched.FailJob(r.Context(), e.JobName, scheduler.FailureReasonDaemonNotRegistered); err != nil {
			s.log.Error("settle refused registration failed", "task_id", e.TaskID, "err", err)
		}
		return
	}
	name, provider := declared.Name, declared.Type
	if name == "" {
		name = "foreman-job"
	}
	if provider == "" {
		provider = "omp"
	}
	s.reg.PutRuntime(registry.JobRuntime{
		JobRuntimeID:    e.JobRuntimeID,
		DaemonID:        claims.JobName,
		TaskID:          e.TaskID,
		Provider:        provider,
		Name:            name,
		Online:          true,
		LastHeartbeatAt: time.Now(),
	})
	registered, stillLive := s.reg.MarkDaemonRegistered(claims.JobName, time.Now())
	s.log.Info("daemon.registered",
		"task_id", e.TaskID, "job_name", e.JobName, "workspace_id", e.WorkspaceID,
		"daemon_id", claims.JobName, "job_runtime_id", e.JobRuntimeID,
		"profile_id", declared.ProfileID,
		"remote_addr", r.RemoteAddr)

	answer := map[string]any{
		"id":         e.JobRuntimeID,
		"name":       name,
		"provider":   provider,
		"status":     "online",
		"profile_id": nil, // builtin: the wire shape the daemon expects (§2.3)
	}
	if declared.ProfileID != "" {
		answer["profile_id"] = declared.ProfileID
	}
	respBody := workspaceStateResponse(client.WorkspaceState())
	respBody["runtimes"] = []map[string]any{answer}
	writeRaw(w, http.StatusOK, mustMarshal(respBody))
	// The task is deliverable from this moment (S30 push; idempotent —
	// delivery itself is at-most-once via the registry). An entry that settled
	// or entered drain between the lookup and the state write wakes nobody:
	// the daemon's claim would only receive an empty batch.
	if s.ws != nil && stillLive && !registered.IsTerminal() && !registered.IsDraining() {
		s.ws.NotifyTaskAvailable(claims.JobName, registered.TaskID)
	}
}

// ---- S5 heartbeat ----

// handleHeartbeat answers S5 with the fixed ok body — never any pending_*
// field (S-5) — and refreshes the shared heartbeat clock. An unknown
// runtime gets a 404 so the daemon re-registers (§1.4).
func (s *FakeServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(w, r, auth.RequestScope{})
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
	if claims.Role == catalogProbeRole {
		s.heartbeatProbe(w, claims, req.RuntimeID)
		return
	}
	// The runtime row is instance-local; the entry is the shared truth. A
	// runtime id that is not this Job's is answered 404 so the daemon
	// re-registers (§1.4); the job's own runtime is (re)seeded locally when
	// this instance serves the Job first (§2.1 任实例服务).
	e, ok := s.reg.ByDaemonInWorkspace(claims.WorkspaceID, claims.JobName)
	if !ok {
		writeError(w, http.StatusNotFound, "runtime not found")
		return
	}
	if req.RuntimeID != e.JobRuntimeID {
		if _, known := s.reg.RuntimeByID(req.RuntimeID); known {
			// Another Job's runtime: the cross-Job impersonation case
			// (§2.1 rule 1).
			writeError(w, http.StatusForbidden, "runtime does not belong to this job")
			return
		}
		// Unknown runtime → 404 so the daemon re-registers (§1.4).
		writeError(w, http.StatusNotFound, "runtime not found")
		return
	}
	s.reg.PutRuntime(registry.JobRuntime{
		JobRuntimeID:    e.JobRuntimeID,
		DaemonID:        e.DaemonID,
		TaskID:          e.TaskID,
		Provider:        "omp",
		Name:            "foreman-job",
		Online:          true,
		LastHeartbeatAt: time.Now(),
	})
	s.reg.TouchHeartbeat(req.RuntimeID, time.Now())
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "runtime_id": req.RuntimeID})
}

// ---- S19 deregister ----

// handleDeregister marks the Job's runtime offline (S19); the Job keeps
// running until its terminal report.
func (s *FakeServer) handleDeregister(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(w, r, auth.RequestScope{})
	if !ok {
		return
	}
	if claims.Role == catalogProbeRole {
		// Only a live owner has a handshake to acknowledge: a reaped session
		// leaves the token with nothing to deregister (§6.3 会话不存在 ⇒ 恒 401).
		if _, found := s.probeSession(claims); !found {
			writeError(w, http.StatusUnauthorized, "unknown probe session")
			return
		}
		s.cancelProbe(w)
		return
	}
	if rt, ok := s.reg.RuntimeByDaemon(claims.JobName); ok {
		s.reg.SetRuntimeOffline(rt.JobRuntimeID)
	} else if e, ok := s.reg.ByDaemonInWorkspace(claims.WorkspaceID, claims.JobName); ok {
		s.reg.SetRuntimeOffline(e.JobRuntimeID)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- S6 claim / delivery ----

type daemonClaimRequest struct {
	DaemonID   string   `json:"daemon_id"`
	RuntimeIDs []string `json:"runtime_ids"`
	MaxTasks   int      `json:"max_tasks"`
}

// handleClaim delivers the Job's task at most once (S6; §2.5).
func (s *FakeServer) handleClaim(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(w, r, auth.RequestScope{})
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
	if req.DaemonID != claims.JobName {
		writeError(w, http.StatusForbidden, "daemon_id does not belong to this job")
		return
	}
	if claims.Role == catalogProbeRole {
		// A probe carries no task face: always an empty batch, never a 403
		// and never a registry lookup (S-6).
		writeRaw(w, http.StatusOK, mustMarshal(map[string]any{"tasks": []json.RawMessage{}}))
		return
	}
	runtimeID := ""
	if len(req.RuntimeIDs) > 0 {
		runtimeID = req.RuntimeIDs[0]
	}
	// This read answers the ownership checks and the latency measure. It is not
	// the empty batch's classification face: a concurrent claim can commit a
	// delivery before the registry's gate, so the default branch re-reads the
	// entry after the attempt and classifies it there (ADR-032, §4.1).
	pending, pendingOK := s.reg.ByDaemonInWorkspace(claims.WorkspaceID, req.DaemonID)
	if pendingOK && pending.WorkspaceID != claims.WorkspaceID {
		// The token addresses another workspace's task: the cross-workspace
		// impersonation case, judged like a cross-Job one (§2.1).
		writeError(w, http.StatusForbidden, "task does not belong to this workspace")
		return
	}
	if pendingOK {
		// Every listed runtime must be this Job's own (S6 归属判据): one
		// foreign value refuses the batch even beside the Job's own id.
		for _, rid := range req.RuntimeIDs {
			if rid != pending.JobRuntimeID {
				writeError(w, http.StatusForbidden, "runtime does not belong to this job")
				return
			}
		}
	}
	delivery, err := s.sched.Deliver(r.Context(), claims.WorkspaceID, req.DaemonID, runtimeID, req.MaxTasks)
	if err != nil {
		if errors.Is(err, scheduler.ErrUnknownDaemon) || errors.Is(err, scheduler.ErrRuntimeMismatch) {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		if errors.Is(err, scheduler.ErrStoreUnavailable) {
			// Fail closed: a delivery that cannot be adjudicated against the
			// shared row is never handed over (§10 共享状态不可用).
			writeError(w, http.StatusServiceUnavailable, "shared state unavailable")
			return
		}
		s.log.Error("deliver failed", "daemon_id", req.DaemonID, "err", err)
		writeError(w, http.StatusInternalServerError, "deliver failed")
		return
	}
	switch {
	case delivery.Delivered && delivery.Deliveries > 1:
		// The window's exit: the delivery committed at delivered_at was never
		// confirmed, and this claim exercised the re-delivery the shared
		// lease released (the scheduler records the event).
		s.metrics.DeliveryClaim(DeliveryClaimRedelivered)
	case delivery.Delivered:
		s.metrics.DeliveryClaim(DeliveryClaimDelivered)
		// The span is the scheduler's: it measured claim -> commit on the
		// clock that stamped both ends (§9.2 交付延迟).
		s.metrics.DeliveryLatency(delivery.Latency.Seconds())
	case delivery.Held:
		// Inside the confirmation window: the request arrived and the
		// response was lost. The shared pickup already bumped the per-entry
		// counter; the stage-one path counts it in the default branch.
		s.metrics.DeliveryClaim(DeliveryClaimEmptyUnconfirmed)
	default:
		// The kind follows the gate's instant, not this request's snapshot: a
		// delivery committed by a concurrent claim in between is still the
		// lost-response signature and counts on the entry, while a decided
		// entry's empties (drain never delivers) stay plain (§4.1 交付后的
		// claim 计数). Re-read once, after the attempt.
		if entry, ok := s.reg.ByDaemonInWorkspace(claims.WorkspaceID, req.DaemonID); ok &&
			entry.DeliveryUnconfirmed(s.reg.Now()) {
			s.metrics.DeliveryClaim(DeliveryClaimEmptyUnconfirmed)
			s.reg.NoteClaimAfterDelivery(entry.TaskID)
		} else {
			s.metrics.DeliveryClaim(DeliveryClaimEmpty)
		}
	}
	tasks := delivery.Tasks
	if tasks == nil {
		tasks = []json.RawMessage{}
	}
	writeRaw(w, http.StatusOK, mustMarshal(map[string]any{"tasks": tasks}))
}

// ---- S7 prepare-lease ----

// handlePrepareLease renews the daemon-visible lease and forwards C4 in the
// foreground with the real runtime id (S7). The daemon's answer is fixed;
// the C4 outcome drives local settlement.
func (s *FakeServer) handlePrepareLease(w http.ResponseWriter, r *http.Request) {
	tid, rid := r.PathValue("tid"), r.PathValue("rid")
	claims, ok := s.authenticate(w, r, auth.RequestScope{TaskID: tid, RuntimeID: rid})
	if !ok {
		return
	}
	entry, ok := s.reg.ByDaemonInWorkspace(claims.WorkspaceID, claims.JobName)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if entry.RealRuntimeID == "" {
		// A rebuilt entry has no dispatch record, so C4 cannot be addressed:
		// the malformed path's 404 would read as a vanished task. Startup
		// convergence owns the entry (ADR-026), and no server lease exists to
		// restamp locally.
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	client := s.clientFor(entry.WorkspaceID)
	if client == nil {
		writeError(w, http.StatusForbidden, "workspace is not configured")
		return
	}
	ready, err := client.PrepareLease(r.Context(), entry.RealRuntimeID, tid)
	switch {
	case errors.Is(err, scheduler.ErrTaskNotFound):
		if verr := s.sched.OnTaskVanished(r.Context(), tid); verr != nil {
			s.log.Error("task vanish settlement failed", "task_id", tid, "err", verr)
		}
	case err != nil:
		s.log.Debug("prepare-lease forward failed", "task_id", tid, "job_name", claims.JobName, "err", err)
	case ready:
		// Local renewal bookkeeping (§2.2 S-7): the daemon's
		// renewal restamped the same server lease the keeper renews, so the
		// entry's lease_keep_until advances here too (§4.1) and the
		// same Info observation line is emitted. The keeper stamps the same
		// projection field from this registry clock by construction
		// (NewLeaseKeeper defaults to reg.Now), so both writers share one
		// observation instant.
		until := s.reg.Now().Add(scheduler.PrepareLeaseTTL)
		s.reg.SetLeaseKeepUntil(tid, until)
		s.log.Info("lease.renewed", "task_id", tid, "job_name", claims.JobName,
			"lease_keep_until", registry.ProjectedTime(until))
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- S9 status ----

// handleTaskStatus forwards C13 so the daemon can discover cancellation. The
// upstream status code and body bytes go back verbatim — it is the daemon's
// own status face, and only the task-vanished 404 is settled locally (the
// forward switch: non-terminal 404 → OnTaskVanished; §4.6).
func (s *FakeServer) handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	tid := r.PathValue("tid")
	claims, ok := s.authenticate(w, r, auth.RequestScope{TaskID: tid})
	if !ok {
		return
	}
	client := s.clientFor(claims.WorkspaceID)
	if client == nil {
		writeError(w, http.StatusForbidden, "workspace is not configured")
		return
	}
	code, resp, err := client.TaskStatusRaw(r.Context(), tid)
	if errors.Is(err, scheduler.ErrTaskNotFound) {
		if verr := s.sched.OnTaskVanished(r.Context(), tid); verr != nil {
			s.log.Error("task vanish settlement failed", "task_id", tid, "err", verr)
		}
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusBadGateway, "upstream unavailable")
		return
	}
	writeRaw(w, code, resp)
}

// ---- S20 task gc-check ----

// handleTaskGCCheck forwards the gc-check query to the server verbatim
// (GC is disabled Job-side; the endpoint stays truthful if enabled, S20).
func (s *FakeServer) handleTaskGCCheck(w http.ResponseWriter, r *http.Request) {
	tid := r.PathValue("tid")
	claims, ok := s.authenticate(w, r, auth.RequestScope{TaskID: tid})
	if !ok {
		return
	}
	client := s.clientFor(claims.WorkspaceID)
	if client == nil {
		writeError(w, http.StatusForbidden, "workspace is not configured")
		return
	}
	code, resp, err := client.GCCheck(r.Context(), tid)
	if err != nil {
		writeError(w, http.StatusBadGateway, "upstream unavailable")
		return
	}
	writeRaw(w, code, resp)
}

// ---- S8/S10–S17 forwarded reports ----

// reportHandler forwards one lifecycle callback (S8, S10–S17) through the
// scheduler; terminal endpoints carry the late-replay idempotent path.
func (s *FakeServer) reportHandler(ep scheduler.Endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tid := r.PathValue("tid")
		claims, ok := s.authenticate(w, r, auth.RequestScope{TaskID: tid, IsTerminalEndpoint: ep.IsTerminal()})
		if !ok {
			return
		}
		if claims.IsLateReplay {
			// The task settled and the report already landed: idempotent
			// 200, no side effects (§1.3 迟到重放).
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}
		e, ok := s.entryFor(w, claims)
		if !ok {
			return
		}
		body, ok := s.readBody(w, r)
		if !ok {
			return
		}
		// The learning slot is the only place a report body's field is read:
		// it hands the S13/S14 session value to the report face, which the
		// attribution gate's compensation body carries back (ADR-031).
		sessionID := ""
		if learnsSession(ep) {
			sessionID = s.learnSession(e, body)
		}
		code, resp, err := s.sched.OnReport(r.Context(), ep, e, body, sessionID)
		if err != nil {
			s.log.Error("report forward failed",
				"task_id", e.TaskID, "job_name", e.JobName, "endpoint", string(ep), "err", err)
			writeError(w, http.StatusBadGateway, "upstream unavailable")
			return
		}
		if ep.IsTerminal() && code >= 200 && code < 300 && s.ws != nil {
			// S30: the connection closes when the task is terminal.
			s.ws.CloseJob(claims.JobName)
		}
		writeRaw(w, code, resp)
	}
}

// learnsSession reports whether ep's body is a learning face of the session
// index (ADR-015): the terminal reports S13/S14, whose session_id is the id
// the server later hands back as prior_session_id. S15 carries no session id,
// and S16 is provider-dependent (a pi-family daemon never pins a session), so
// neither may teach the index.
func learnsSession(ep scheduler.Endpoint) bool {
	return ep == scheduler.EPComplete || ep == scheduler.EPFail
}

// learnSession records sessionKey(session_id) → the reporting task's node,
// before the report is forwarded: the learning decision does not depend on
// the forward result, because a failed forward is retried from the pending
// queue rather than dropped. A body without a session id, or a task whose pod
// placement was never observed, teaches nothing — the index holds no key that
// could not hit.
//
// It returns the raw session value the same read produced, even when nothing
// was learned: the attribution gate's compensation body carries it back to
// the server (ADR-031), so the single-field read stays in this one place.
func (s *FakeServer) learnSession(e registry.TaskEntry, body []byte) string {
	var report struct {
		SessionID string `json:"session_id"`
	}
	// Only session_id is read; a body that is not an object simply has none.
	if err := json.Unmarshal(body, &report); err != nil {
		return ""
	}
	if e.NodeName != "" {
		s.reg.LearnSession(e.WorkspaceID, registry.SessionKey(report.SessionID), e.NodeName)
	}
	return report.SessionID
}

// ---- S18 recover-orphans ----

// handleRecoverOrphans answers with zeros: orphans inside Jobs are the
// recovery module's business, never the Job daemon's (S18).
func (s *FakeServer) handleRecoverOrphans(w http.ResponseWriter, r *http.Request) {
	rid := r.PathValue("rid")
	if _, ok := s.authenticate(w, r, auth.RequestScope{RuntimeID: rid}); !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"orphaned": 0, "retried": 0})
}

// ---- S21 issues gc-check ----

// handleIssueGCCheck answers empty: nothing is collectable (S21).
func (s *FakeServer) handleIssueGCCheck(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r, auth.RequestScope{}); !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"issues": []any{}})
}

// ---- S24 token renew ----

// handleTokenRenew synthesizes the renew response locally: the Job Token is
// not renewed through this endpoint (S24).
func (s *FakeServer) handleTokenRenew(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r, auth.RequestScope{}); !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"expires_at": time.Now().Add(tokenRenewResponseTTL).UTC().Format(time.RFC3339),
		"renewed":    true,
	})
}

// ---- S26 skill bundles ----

// handleSkillBundles answers the resolve call from the §1.8 cache (S26): a
// fresh entry replays locally, a miss forwards as C19 with the {rid} rewritten
// to the real runtime id, and a failed upstream may still be answered from an
// entry inside the downgrade window. The payload bytes are the only cache key
// input; the task and runtime ids never reach it.
func (s *FakeServer) handleSkillBundles(w http.ResponseWriter, r *http.Request) {
	tid, rid := r.PathValue("tid"), r.PathValue("rid")
	claims, ok := s.authenticate(w, r, auth.RequestScope{TaskID: tid, RuntimeID: rid})
	if !ok {
		return
	}
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	code, resp, err := s.bundlesFor(claims.WorkspaceID).Resolve(r.Context(), tid, rid, body)
	if err != nil {
		writeError(w, http.StatusBadGateway, "upstream unavailable")
		return
	}
	writeRaw(w, code, resp)
}

// handleS23 guards the defensive S23 paths with Job Token verification
// before the verbatim forward (S23 rides the rule-2 passthrough with the
// caller's credential, but an invalid fmj_ must 401 locally).
func (s *FakeServer) handleS23(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r, auth.RequestScope{TaskID: r.PathValue("tid")}); !ok {
		return
	}
	s.handlePassthrough(w, r)
}

// ---- S29 passthrough ----

// handlePassthrough forwards any non-fmj_ /api/* call to the real server
// verbatim (§2.1 rule 2 + ADR-020 + ADR-046): the caller's whole header set and
// body go out as given, the upstream status, header set and body come back as
// given. This call site neither picks nor adds fields — the exclusion set lives
// in Passthrough alone. Upstream 5xx and failures before a response head map to
// 502; a body that breaks once the head is out reaches the caller as a
// truncated response, which is what a direct connection produces too.
func (s *FakeServer) handlePassthrough(w http.ResponseWriter, r *http.Request) {
	resp, err := s.client.Passthrough(r.Context(), r.Method, r.URL.EscapedPath(), r.URL.RawQuery, r.Header, r.Body, r.ContentLength)
	if err != nil || resp.StatusCode >= http.StatusInternalServerError {
		if resp != nil {
			_ = resp.Body.Close()
		}
		writeError(w, http.StatusBadGateway, "upstream unavailable")
		return
	}
	defer resp.Body.Close()
	writeUpstream(w, resp)
}

// writeUpstream streams a passthrough response verbatim (S-29): the upstream's
// fields, then its status, then its body as it arrives. It is not the
// local-response outlet — writeRaw stamps Content-Type: application/json on
// the responses Foreman synthesizes, which would rewrite this one. Nil values
// suppress the headers net/http would otherwise add by itself (Content-Type
// sniffing, Date), so the caller sees exactly what came back. A body length the
// upstream declared is restated as the caller-side framing; an undeclared one
// becomes chunked framing.
func writeUpstream(w http.ResponseWriter, resp *PassthroughResponse) {
	dst := w.Header()
	for k, vv := range resp.Header {
		dst[k] = append([]string(nil), vv...)
	}
	if _, ok := dst["Content-Type"]; !ok {
		dst["Content-Type"] = nil
	}
	if _, ok := dst["Date"]; !ok {
		dst["Date"] = nil
	}
	if resp.ContentLength >= 0 && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotModified {
		dst["Content-Length"] = []string{strconv.FormatInt(resp.ContentLength, 10)}
	}
	w.WriteHeader(resp.StatusCode)
	// The head goes out before the body is copied: the caller must see the
	// upstream's status even if the body breaks below, and a large answer must
	// start arriving while the upstream is still writing it.
	rc := http.NewResponseController(w)
	if err := rc.Flush(); err != nil {
		return
	}
	if _, err := io.Copy(flushWriter{w: w, rc: rc}, resp.Body); err != nil {
		// A body that breaks mid-stream must not look complete: returning
		// normally would end chunked framing at the last byte received, which
		// the caller cannot tell from a whole answer. Dropping the connection
		// is the shape a direct connection produces (§2.4).
		panic(http.ErrAbortHandler)
	}
}

// flushWriter flushes every written chunk before the copy reads the next one.
// Without the per-chunk flush the chunked framing buffer keeps the tail, and
// the abort above drops bytes the upstream had already produced: the delivered
// prefix would then depend on the upstream's chunk size rather than match a
// direct connection (§2.4).
type flushWriter struct {
	w  io.Writer
	rc *http.ResponseController
}

func (f flushWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	if err != nil {
		return n, err
	}
	if err := f.rc.Flush(); err != nil {
		return n, err
	}
	return n, nil
}
