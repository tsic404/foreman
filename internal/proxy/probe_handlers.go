package proxy

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/registry"
)

// catalogProbeRole mirrors the token role for handlers that must branch before
// consulting the task registry.
const catalogProbeRole = auth.RoleCatalogProbe

// probeSession resolves the caller's probe session from its Job Token claims.
func (s *FakeServer) probeSession(claims auth.Claims) (registry.ProbeSession, bool) {
	if s.probes == nil {
		return registry.ProbeSession{}, false
	}
	return s.probes.ProbeByJob(claims.JobName)
}

// probeCommand returns the command id this probe must report on, and whether
// one is still outstanding. The session's mode is the only authority (§1.3):
// a startup-discovery probe carries Foreman's own minted id, a command-driven
// probe the C2 ack's id verbatim, and a registration-discovery probe owns no
// command at all — its ack must never ask for a catalog run.
func probeCommand(session registry.ProbeSession) (string, bool) {
	if !session.RelayedAt.IsZero() {
		return "", false
	}
	switch session.Mode {
	case registry.ProbeStartupDiscovery:
		return session.SelfMintedID, session.SelfMintedID != ""
	case registry.ProbeCommandDriven:
		return session.RequestID, session.RequestID != ""
	default:
		return "", false
	}
}

// registerProbe answers S4 for a catalog-probe Job: the probe's own job runtime
// is echoed back, and — for the modes whose registration is a declaration
// (startup discovery, registration discovery) — its body becomes the truth
// source C1 declares.
func (s *FakeServer) registerProbe(w http.ResponseWriter, r *http.Request, claims auth.Claims, req daemonRegisterRequest) {
	session, ok := s.probeSession(claims)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unknown probe session")
		return
	}
	if len(req.Runtimes) == 0 {
		// Zero declared runtimes: refusing the registration is the probe's
		// third failure class (pod up, daemon not registered), which the
		// startup window turns into the exit decision (S-4).
		s.log.Error("probe declared no runtime; refusing registration", "job_name", claims.JobName)
		writeError(w, http.StatusConflict, "no runtime declared")
		return
	}
	first := probeServedRuntime(req.Runtimes)
	workspaceID := req.WorkspaceID
	if workspaceID == "" {
		workspaceID = claims.WorkspaceID
	}
	client := s.clientFor(workspaceID)
	if client == nil {
		writeError(w, http.StatusForbidden, "workspace is not configured")
		return
	}
	if session.WorkspaceID != "" && session.WorkspaceID != workspaceID {
		// A workspace-pinned probe (catalog) registers only for its own
		// workspace; a startup / discovery probe may register for any
		// configured one (§1.2).
		writeError(w, http.StatusForbidden, "probe does not serve this workspace")
		return
	}
	now := time.Now()
	_, applied := s.probes.UpdateProbe(claims.JobName, func(session *registry.ProbeSession) {
		if !session.CapturesRegistration() {
			return
		}
		// Startup and registration-discovery probes only: this registration
		// body is C1's truth, captured per workspace because one probe may
		// register to several (§1.2).
		registration := registry.ProbeRegistration{
			RegisteredAt:   now,
			CLIVersion:     req.CLIVersion,
			Runtimes:       discoveredRuntimes(req.Runtimes),
			FailedProfiles: req.FailedProfiles,
		}
		if session.Registrations == nil {
			session.Registrations = map[string]registry.ProbeRegistration{}
		}
		session.Registrations[workspaceID] = registration
		if session.RegisteredAt.IsZero() {
			// The first registration is the completion signal; the mirrored
			// fields keep the single-workspace readers working.
			session.RegisteredAt = now
			session.CLIVersion = registration.CLIVersion
			session.Runtimes = registration.Runtimes
			session.FailedProfiles = registration.FailedProfiles
		}
	})
	if !applied {
		// The session was reaped between the lookup above and this update, so
		// there is no authority left to answer (§6.3 会话不存在 ⇒ 恒 401); a
		// command-driven probe may not slip through either.
		writeError(w, http.StatusUnauthorized, "unknown probe session")
		return
	}
	s.log.Info("daemon.registered",
		"job_name", claims.JobName, "daemon_id", claims.JobName,
		"workspace_id", workspaceID, "job_runtime_id", session.JobRuntime, "probe", true,
		"remote_addr", r.RemoteAddr)

	repos := client.WorkspaceState()
	respBody := workspaceStateResponse(repos)
	answer := map[string]any{
		"id":         session.JobRuntime,
		"name":       first.Name,
		"provider":   first.Type,
		"status":     "online",
		"profile_id": nil,
	}
	if first.ProfileID != "" {
		answer["profile_id"] = first.ProfileID
	}
	respBody["runtimes"] = []map[string]any{answer}
	writeRaw(w, http.StatusOK, mustMarshal(respBody))
}

// selectDeclaredRuntime picks the declared entry answering one Job: the
// request-side identity (registry.RuntimeIdentityKey) must match by value, and
// a repeated identity resolves by name order like the probe path (§2.3). An
// unknown identity matches nothing — the caller refuses the registration.
func selectDeclaredRuntime(runtimes []registerRuntime, identity string) (registerRuntime, bool) {
	if identity == "" {
		return registerRuntime{}, false
	}
	candidates := make([]registerRuntime, 0, len(runtimes))
	for _, rt := range runtimes {
		if registry.RuntimeIdentityKey(rt.Type, rt.ProfileID) == identity {
			candidates = append(candidates, rt)
		}
	}
	if len(candidates) == 0 {
		return registerRuntime{}, false
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Name < candidates[j].Name })
	return candidates[0], true
}

// probeServedRuntime picks the runtime a probe serves: the first entry by name,
// the same stable ordering C1 declares (C-1 的取值来源与声明域).
func probeServedRuntime(runtimes []registerRuntime) registerRuntime {
	ordered := append([]registerRuntime(nil), runtimes...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	return ordered[0]
}

func discoveredRuntimes(runtimes []registerRuntime) []registry.DiscoveredRuntime {
	discovered := make([]registry.DiscoveredRuntime, 0, len(runtimes))
	for _, rt := range runtimes {
		discovered = append(discovered, registry.DiscoveredRuntime{
			Name: rt.Name, Type: rt.Type, Version: rt.Version, ProfileID: rt.ProfileID,
		})
	}
	return discovered
}

// heartbeatProbe answers S5 for a probe: the ack carries pending_model_list
// while a command is outstanding, and nothing otherwise (S-5 按角色分叉).
func (s *FakeServer) heartbeatProbe(w http.ResponseWriter, claims auth.Claims, runtimeID string) {
	session, ok := s.probeSession(claims)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unknown probe session")
		return
	}
	if runtimeID != session.JobRuntime {
		writeError(w, http.StatusForbidden, "runtime does not belong to this probe")
		return
	}
	ack := map[string]any{"status": "ok", "runtime_id": runtimeID}
	if command, pending := probeCommand(session); pending {
		ack["pending_model_list"] = map[string]string{"id": command}
	}
	writeRaw(w, http.StatusOK, mustMarshal(ack))
}

// noRepos is the empty repos block the pre-C1 window answers (S-2).
var noRepos = json.RawMessage("[]")

// handleModelResult is S27's models path. A task Job always gets 404 (F9); a
// probe Job's result is routed by its source: the startup-discovery id is
// absorbed into the slot store without any C21, the command id is relayed
// byte for byte (S-27).
func (s *FakeServer) handleModelResult(w http.ResponseWriter, r *http.Request) {
	tid, rid, req := r.PathValue("tid"), r.PathValue("rid"), r.PathValue("req")
	claims, ok := s.authenticate(w, r, auth.RequestScope{
		TaskID: tid, RuntimeID: rid, RequestID: req,
	})
	if !ok {
		return
	}
	if claims.Role != catalogProbeRole {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	session, ok := s.probeSession(claims)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unknown probe session")
		return
	}
	now := time.Now()
	switch {
	case session.SelfMintedID != "" && req == session.SelfMintedID:
		// Startup discovery: no server request exists, so the result is
		// absorbed locally and the daemon gets the C21 success shape back.
		// The slot is filled for the declaration's first entry (§1.3: 按声明
		// 顺序), never a name-sorted pick — sorting would also mutate the
		// slice the probe session shares.
		provider, profileID := "", ""
		if len(session.Runtimes) > 0 {
			provider, profileID = session.Runtimes[0].Type, session.Runtimes[0].ProfileID
		}
		if provider == "" {
			s.log.Error("startup discovery landed without a declared runtime", "job_name", claims.JobName)
			writeError(w, http.StatusConflict, "no runtime declared")
			return
		}
		workspaceID := session.WorkspaceID
		if workspaceID == "" {
			workspaceID = claims.WorkspaceID
		}
		if client := s.clientFor(workspaceID); client != nil {
			client.stageStartupCatalog(provider, profileID, body)
		}
		s.probes.UpdateProbe(claims.JobName, func(probe *registry.ProbeSession) {
			probe.RelayedAt = now
			probe.RelayedCode = http.StatusOK
		})
		s.log.Info("model.catalog_absorbed", "job_name", claims.JobName, "provider", provider)
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case session.RequestID != "" && req == session.RequestID:
		// The relay retries transport errors and 5xx with C-21's backoff,
		// bounded by the probe's own budget (the consumption-to-answer window);
		// a 4xx is the upstream's verdict and goes back as it is.
		client := s.clientFor(session.WorkspaceID)
		if client == nil {
			writeError(w, http.StatusForbidden, "workspace is not configured")
			return
		}
		status, resp, err := relayCatalog(r.Context(), client, session.RealRuntimeID, req, body, session.Deadline, s.log)
		code := status
		if err != nil {
			s.log.Error("model catalog relay exhausted", "job_name", claims.JobName, "err", err)
			// Code 0 marks "nothing was relayed" for the waiter: it is not a
			// status any C21 answer can carry.
			code = 0
			writeError(w, http.StatusBadGateway, "upstream unavailable")
		} else {
			writeRaw(w, status, resp)
		}
		s.probes.UpdateProbe(claims.JobName, func(probe *registry.ProbeSession) {
			probe.RelayedAt = now
			probe.RelayedCode = code
		})
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

// cancelProbe is S19 for a probe: the probe is torn down by its lifecycle, so
// the握手 is acknowledged without touching the task registry.
func (s *FakeServer) cancelProbe(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
