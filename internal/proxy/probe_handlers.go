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
// one is still outstanding: command-driven probes carry the C2 ack's id
// verbatim, a startup-discovery probe carries Foreman's own minted id
// (§1.2 S5/S27).
func probeCommand(session registry.ProbeSession) (string, bool) {
	if !session.RelayedAt.IsZero() {
		return "", false
	}
	if session.SelfMintedID != "" {
		return session.SelfMintedID, true
	}
	if session.RequestID != "" {
		return session.RequestID, true
	}
	return "", false
}

// registerProbe answers S4 for a catalog-probe Job: the probe's own job runtime
// is echoed back, and (in startup-discovery mode) its registration body becomes
// the truth source C1 declares.
func (s *FakeServer) registerProbe(w http.ResponseWriter, r *http.Request, claims auth.Claims, req daemonRegisterRequest) {
	session, ok := s.probeSession(claims)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unknown probe session")
		return
	}
	if len(req.Runtimes) == 0 {
		// Zero declared runtimes: refusing the registration is the probe's
		// third failure class (pod up, daemon not registered), which the
		// startup window turns into the exit decision (§1.2 S4).
		s.log.Error("probe declared no runtime; refusing registration", "job_name", claims.JobName)
		writeError(w, http.StatusConflict, "no runtime declared")
		return
	}
	first := probeServedRuntime(req.Runtimes)
	now := time.Now()
	updated, _ := s.probes.UpdateProbe(claims.JobName, func(session *registry.ProbeSession) {
		if session.RealRuntimeID != "" {
			return
		}
		// Startup-discovery mode only: this registration body is C1's truth.
		session.RegisteredAt = now
		session.CLIVersion = req.CLIVersion
		session.Runtimes = discoveredRuntimes(req.Runtimes)
		session.FailedProfiles = req.FailedProfiles
	})
	if updated.RegisteredAt.IsZero() && session.RealRuntimeID == "" {
		writeError(w, http.StatusUnauthorized, "unknown probe session")
		return
	}
	s.log.Info("daemon.registered",
		"job_name", claims.JobName, "daemon_id", claims.JobName,
		"job_runtime_id", session.JobRuntime, "probe", true,
		"remote_addr", r.RemoteAddr)

	repos, version := s.client.Registration()
	if repos == nil {
		repos = noRepos
	}
	writeRaw(w, http.StatusOK, mustMarshal(map[string]any{
		"runtimes": []map[string]string{{
			"id":       session.JobRuntime,
			"name":     first.Name,
			"provider": first.Type,
			"status":   "online",
		}},
		"repos":         repos,
		"repos_version": version,
		"settings":      nil,
	}))
}

// selectDeclaredRuntime picks the declared entry answering one Job: the type
// must equal the Job's provider, an empty profile_id wins, and ties resolve by
// name (§1.2 S4 的选条与拒绝注册面).
func selectDeclaredRuntime(runtimes []registerRuntime, provider string) (registerRuntime, bool) {
	if len(runtimes) == 0 {
		return registerRuntime{}, false
	}
	if provider == "" {
		return runtimes[0], true
	}
	candidates := make([]registerRuntime, 0, len(runtimes))
	for _, rt := range runtimes {
		if rt.Type == provider {
			candidates = append(candidates, rt)
		}
	}
	if len(candidates) == 0 {
		return registerRuntime{}, false
	}
	plain := make([]registerRuntime, 0, len(candidates))
	for _, rt := range candidates {
		if rt.ProfileID == "" {
			plain = append(plain, rt)
		}
	}
	if len(plain) == 0 {
		plain = candidates
	}
	sort.SliceStable(plain, func(i, j int) bool { return plain[i].Name < plain[j].Name })
	return plain[0], true
}

// probeServedRuntime picks the runtime a probe serves: the first entry by name,
// the same stable ordering C1 declares (§1.1 C1 的取值来源与声明域).
func probeServedRuntime(runtimes []registerRuntime) registerRuntime {
	ordered := append([]registerRuntime(nil), runtimes...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	return ordered[0]
}

func discoveredRuntimes(runtimes []registerRuntime) []registry.DiscoveredRuntime {
	discovered := make([]registry.DiscoveredRuntime, 0, len(runtimes))
	for _, rt := range runtimes {
		discovered = append(discovered, registry.DiscoveredRuntime{Name: rt.Name, Type: rt.Type, Version: rt.Version})
	}
	return discovered
}

// heartbeatProbe answers S5 for a probe: the ack carries pending_model_list
// while a command is outstanding, and nothing otherwise (§1.2 S5 按角色分叉).
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

// noRepos is the empty repos block the pre-C1 window answers (§1.2 S2).
var noRepos = json.RawMessage("[]")

// handleModelResult is S27's models path. A task Job always gets 404 (F9); a
// probe Job's result is routed by its source: the startup-discovery id is
// absorbed into the slot store without any C21, the command id is relayed
// byte for byte (§1.2 S27).
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
	body, ok := readBody(w, r)
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
		provider := ""
		if len(session.Runtimes) > 0 {
			discovered := session.Runtimes
			sort.SliceStable(discovered, func(i, j int) bool { return discovered[i].Name < discovered[j].Name })
			provider = discovered[0].Type
		}
		if provider == "" {
			s.log.Error("startup discovery landed without a declared runtime", "job_name", claims.JobName)
			writeError(w, http.StatusConflict, "no runtime declared")
			return
		}
		s.client.stageStartupCatalog(provider, body)
		s.probes.UpdateProbe(claims.JobName, func(probe *registry.ProbeSession) {
			probe.RelayedAt = now
			probe.RelayedCode = http.StatusOK
		})
		s.log.Info("model.catalog_absorbed", "job_name", claims.JobName, "provider", provider)
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case session.RequestID != "" && req == session.RequestID:
		// The relay retries transport errors and 5xx with §1.1 C21's backoff,
		// bounded by the probe's own budget (the consumption-to-answer window);
		// a 4xx is the upstream's verdict and goes back as it is.
		status, resp, err := relayCatalog(r.Context(), s.client, session.RealRuntimeID, req, body, session.Deadline, s.log)
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
