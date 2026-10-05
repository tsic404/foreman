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
		repos = json.RawMessage("[]")
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
	Runtimes []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"runtimes"`
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
	e, ok := s.entryFor(w, claims)
	if !ok {
		return
	}
	name, provider := "foreman-job", "omp"
	if len(req.Runtimes) > 0 {
		if req.Runtimes[0].Name != "" {
			name = req.Runtimes[0].Name
		}
		if req.Runtimes[0].Type != "" {
			provider = req.Runtimes[0].Type
		}
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
	runtimeID := ""
	if len(req.RuntimeIDs) > 0 {
		runtimeID = req.RuntimeIDs[0]
	}
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
	_, err := s.client.PrepareLease(r.Context(), tid)
	if errors.Is(err, scheduler.ErrTaskNotFound) {
		if verr := s.sched.OnTaskVanished(r.Context(), tid); verr != nil {
			s.log.Error("task vanish settlement failed", "task_id", tid, "err", verr)
		}
	} else if err != nil {
		s.log.Debug("prepare-lease forward failed", "task_id", tid, "job_name", claims.JobName, "err", err)
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
// verbatim (§1.2 rule 2): method, raw path, raw query, body and
// Authorization unchanged. Upstream 5xx and transport failures map to 502;
// everything else returns verbatim.
func (s *FakeServer) handlePassthrough(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	code, resp, err := s.client.Passthrough(r.Context(), r.Method, r.URL.EscapedPath(), r.URL.RawQuery, body, r.Header.Get("Authorization"))
	if err != nil {
		writeError(w, http.StatusBadGateway, "upstream unavailable")
		return
	}
	if code >= 500 {
		writeError(w, http.StatusBadGateway, "upstream unavailable")
		return
	}
	writeRaw(w, code, resp)
}
