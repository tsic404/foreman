package proxy

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// tokenRenewResponseTTL is the S24 synthetic expiry (§1.2: now+24h; the Job
// Token is not actually renewed through this endpoint).
const tokenRenewResponseTTL = 24 * time.Hour

// ---- S1/S28 workspaces ----

// handleWorkspaces answers with the Job's single workspace (S1; also the
// legacy /api/workspaces fallback S28). It must succeed, else the daemon
// finds no runtime.
func (s *FakeServer) handleWorkspaces(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r, auth.RequestScope{}); !ok {
		return
	}
	id, name := s.client.Workspace()
	writeJSON(w, http.StatusOK, []map[string]string{{"id": id, "name": name}})
}

// ---- S2 repos ----

// handleRepos returns the repos block cached from Foreman's own C1
// registration (S2).
func (s *FakeServer) handleRepos(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(w, r, auth.RequestScope{})
	if !ok {
		return
	}
	if ws := r.PathValue("ws"); ws != claims.WorkspaceID {
		writeError(w, http.StatusForbidden, "workspace does not belong to this job")
		return
	}
	repos, version := s.client.Registration()
	if repos == nil {
		// The register-before window (the startup probe's S1/S2 calls): C1 has
		// not run yet, so there is nothing cached to hand back — an empty repos
		// list, which never blocks the registration (§1.2 S2).
		repos = noRepos
	}
	writeRaw(w, http.StatusOK, mustMarshal(map[string]any{
		"workspace_id":  claims.WorkspaceID,
		"repos":         repos,
		"repos_version": version,
		"settings":      nil,
	}))
}

// ---- S4 register ----

type daemonRegisterRequest struct {
	DaemonID string `json:"daemon_id"`
	// CLIVersion is the template CLI's version: the startup probe's value
	// becomes C1's cli_version (§1.1 C1 的取值来源与声明域).
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
	body, ok := readBody(w, r)
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
	if claims.Role == catalogProbeRole {
		s.registerProbe(w, r, claims, req)
		return
	}
	e, ok := s.entryFor(w, claims)
	if !ok {
		return
	}
	// The Job's provider is the provider of the real runtime its claim was
	// dispatched under; the declared entry of that type is the one echoed
	// back (§1.2 S4 的选条与拒绝注册面).
	jobProvider, known := s.client.ProviderForRuntime(e.RealRuntimeID)
	if !known {
		// A rebuilt entry has no dispatch record: fall back to the oldest
		// behaviour (one Job, one provider) instead of refusing a running
		// Job's daemon.
		s.log.Warn("job provider unknown at register; using the declared entry as reported",
			"task_id", e.TaskID, "job_name", e.JobName)
	}
	declared, matched := selectDeclaredRuntime(req.Runtimes, jobProvider)
	if known && !matched {
		s.log.Error("job declared no runtime for its provider; refusing registration",
			"task_id", e.TaskID, "job_name", e.JobName, "provider", jobProvider)
		writeError(w, http.StatusConflict, "runtime provider not declared by this job")
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
	s.reg.MarkDaemonRegistered(claims.JobName, time.Now())
	s.log.Info("daemon.registered",
		"task_id", e.TaskID, "job_name", e.JobName,
		"daemon_id", claims.JobName, "job_runtime_id", e.JobRuntimeID,
		"remote_addr", r.RemoteAddr)

	repos, version := s.client.Registration()
	if repos == nil {
		repos = json.RawMessage("[]")
	}
	writeRaw(w, http.StatusOK, mustMarshal(map[string]any{
		"runtimes": []map[string]string{{
			"id":       e.JobRuntimeID,
			"name":     name,
			"provider": provider,
			"status":   "online",
		}},
		"repos":         repos,
		"repos_version": version,
		"settings":      nil,
	}))
	// The task is deliverable from this moment (S30 push; idempotent —
	// delivery itself is at-most-once via the registry).
	if s.ws != nil {
		s.ws.NotifyTaskAvailable(claims.JobName, e.TaskID)
	}
}

// ---- S5 heartbeat ----

// handleHeartbeat answers S5 with the fixed ok body — never any pending_*
// field (§1.2 S5) — and refreshes the shared heartbeat clock. An unknown
// runtime gets a 404 so the daemon re-registers (scenario #12).
func (s *FakeServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(w, r, auth.RequestScope{})
	if !ok {
		return
	}
	body, ok := readBody(w, r)
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
	rt, ok := s.reg.RuntimeByID(req.RuntimeID)
	if !ok {
		writeError(w, http.StatusNotFound, "runtime not found")
		return
	}
	if rt.DaemonID != claims.JobName {
		writeError(w, http.StatusForbidden, "runtime does not belong to this job")
		return
	}
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
		s.cancelProbe(w)
		return
	}
	if rt, ok := s.reg.RuntimeByDaemon(claims.JobName); ok {
		rt.Online = false
		s.reg.PutRuntime(rt)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- S6 claim / delivery ----

type daemonClaimRequest struct {
	DaemonID   string   `json:"daemon_id"`
	RuntimeIDs []string `json:"runtime_ids"`
	MaxTasks   int      `json:"max_tasks"`
}

// handleClaim delivers the Job's task at most once (S6; proxy.md §任务交付).
func (s *FakeServer) handleClaim(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(w, r, auth.RequestScope{})
	if !ok {
		return
	}
	body, ok := readBody(w, r)
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
		// and never a registry lookup (§1.2 S6).
		writeRaw(w, http.StatusOK, mustMarshal(map[string]any{"tasks": []json.RawMessage{}}))
		return
	}
	runtimeID := ""
	if len(req.RuntimeIDs) > 0 {
		runtimeID = req.RuntimeIDs[0]
	}
	// Read the delivery face before the delivery attempt: an empty batch that
	// answers an already-committed delivery is the lost-response signature
	// (ADR-030), and the face that tells the kinds of empty batch apart is gone
	// once the entry settles. A decided entry's empty batches are the designed
	// ones (drain never delivers), so they stay plain empties. The commit count
	// is the classification's monotone fact — a commit that is not the first one
	// is the re-delivery the expired window released — while the window test
	// itself stays in the registry's gate (one clock, one decision).
	pending, pendingOK := s.reg.ByDaemon(req.DaemonID)
	unconfirmed := pendingOK && pending.DeliveryUnconfirmed(s.reg.Now())
	tasks, err := s.sched.Deliver(req.DaemonID, runtimeID, req.MaxTasks)
	if err != nil {
		if errors.Is(err, scheduler.ErrUnknownDaemon) || errors.Is(err, scheduler.ErrRuntimeMismatch) {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		s.log.Error("deliver failed", "daemon_id", req.DaemonID, "err", err)
		writeError(w, http.StatusInternalServerError, "deliver failed")
		return
	}
	switch {
	case len(tasks) > 0 && pendingOK && pending.Deliveries > 0:
		// The window's exit: the delivery committed at delivered_at was never
		// confirmed, and this claim exercised the re-delivery the registry's
		// gate released (the scheduler records the event).
		s.metrics.DeliveryClaim(DeliveryClaimRedelivered)
	case len(tasks) > 0:
		s.metrics.DeliveryClaim(DeliveryClaimDelivered)
		if pendingOK {
			s.metrics.DeliveryLatency(time.Since(pending.ClaimedAt).Seconds())
		}
	case unconfirmed:
		// The daemon keeps asking inside the window (or with the exit off):
		// the request arrived and the response was lost, which is what the
		// per-entry counter records for the post-hoc query face.
		s.metrics.DeliveryClaim(DeliveryClaimEmptyUnconfirmed)
		s.reg.NoteClaimAfterDelivery(pending.TaskID)
	default:
		s.metrics.DeliveryClaim(DeliveryClaimEmpty)
	}
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
	entry, ok := s.reg.ByDaemon(claims.JobName)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	ready, err := s.client.PrepareLease(r.Context(), entry.RealRuntimeID, tid)
	switch {
	case errors.Is(err, scheduler.ErrTaskNotFound):
		if verr := s.sched.OnTaskVanished(r.Context(), tid); verr != nil {
			s.log.Error("task vanish settlement failed", "task_id", tid, "err", verr)
		}
	case err != nil:
		s.log.Debug("prepare-lease forward failed", "task_id", tid, "job_name", claims.JobName, "err", err)
	case ready:
		// Local renewal bookkeeping (proxy.md §请求路由 S7): the daemon's
		// renewal restamped the same server lease the keeper renews, so the
		// entry's lease_keep_until advances here too (contract §3.1) and the
		// same Info observation line is emitted (protocol-lease.md 场景 A).
		until := time.Now().Add(scheduler.PrepareLeaseTTL)
		s.reg.SetLeaseKeepUntil(tid, until)
		s.log.Info("lease.renewed", "task_id", tid, "job_name", claims.JobName,
			"lease_keep_until", registry.ProjectedTime(until))
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- S9 status ----

// handleTaskStatus forwards C13 so the daemon can discover cancellation. A
// server-side 404 settles the mapping locally (proxy.md 转发 switch:
// non-terminal 404 → OnTaskVanished; failure-handling scenario #9).
func (s *FakeServer) handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	tid := r.PathValue("tid")
	if _, ok := s.authenticate(w, r, auth.RequestScope{TaskID: tid}); !ok {
		return
	}
	status, err := s.client.TaskStatus(r.Context(), tid)
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
	writeJSON(w, http.StatusOK, map[string]string{"status": status})
}

// ---- S20 task gc-check ----

// handleTaskGCCheck forwards the gc-check query to the server verbatim
// (GC is disabled Job-side; the endpoint stays truthful if enabled, S20).
func (s *FakeServer) handleTaskGCCheck(w http.ResponseWriter, r *http.Request) {
	tid := r.PathValue("tid")
	if _, ok := s.authenticate(w, r, auth.RequestScope{TaskID: tid}); !ok {
		return
	}
	code, resp, err := s.client.GCCheck(r.Context(), tid)
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
			// 200, no side effects (proxy.md 迟到重放).
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}
		e, ok := s.entryFor(w, claims)
		if !ok {
			return
		}
		body, ok := readBody(w, r)
		if !ok {
			return
		}
		if learnsSession(ep) {
			s.learnSession(e, body)
		}
		code, resp, err := s.sched.OnReport(r.Context(), ep, e, body)
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
func (s *FakeServer) learnSession(e registry.TaskEntry, body []byte) {
	if e.NodeName == "" {
		return
	}
	var report struct {
		SessionID string `json:"session_id"`
	}
	// Only session_id is read; a body that is not an object simply has none.
	if err := json.Unmarshal(body, &report); err != nil {
		return
	}
	s.reg.LearnSession(registry.SessionKey(report.SessionID), e.NodeName)
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

// handleSkillBundles forwards the resolve call as C19: the {rid} is
// rewritten to the real runtime id and Foreman's credential is used (S26).
func (s *FakeServer) handleSkillBundles(w http.ResponseWriter, r *http.Request) {
	tid, rid := r.PathValue("tid"), r.PathValue("rid")
	if _, ok := s.authenticate(w, r, auth.RequestScope{TaskID: tid, RuntimeID: rid}); !ok {
		return
	}
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	code, resp, err := s.client.ResolveSkillBundles(r.Context(), tid, body)
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
// verbatim (§1.2 rule 2 + ADR-020): the caller's whole header set and body
// go out as given, the upstream status, header set and body come back as
// given. This call site neither picks nor adds fields — the exclusion set
// lives in Passthrough alone. Upstream 5xx and transport failures map to 502.
func (s *FakeServer) handlePassthrough(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	code, header, resp, err := s.client.Passthrough(r.Context(), r.Method, r.URL.EscapedPath(), r.URL.RawQuery, r.Header, body)
	if err != nil || code >= http.StatusInternalServerError {
		writeError(w, http.StatusBadGateway, "upstream unavailable")
		return
	}
	writeUpstream(w, code, header, resp)
}

// writeUpstream replays a passthrough response verbatim (§1.2 S29): the
// upstream's fields, then its status, then its body bytes. It is not the
// local-response outlet — writeRaw stamps Content-Type: application/json on
// the responses Foreman synthesizes, which would rewrite this one. Nil
// values suppress the headers net/http would otherwise add by itself
// (Content-Type sniffing, Date), so the caller sees exactly what came back.
func writeUpstream(w http.ResponseWriter, code int, header http.Header, body []byte) {
	dst := w.Header()
	for k, vv := range header {
		dst[k] = append([]string(nil), vv...)
	}
	if _, ok := dst["Content-Type"]; !ok {
		dst["Content-Type"] = nil
	}
	if _, ok := dst["Date"]; !ok {
		dst["Date"] = nil
	}
	w.WriteHeader(code)
	_, _ = w.Write(body)
}
