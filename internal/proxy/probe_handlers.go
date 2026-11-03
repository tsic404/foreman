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
	var served registerRuntime
	var selected bool
	_, applied := s.probes.UpdateProbe(claims.JobName, func(session *registry.ProbeSession) {
		// The answer is picked off the session's own mode: a command-driven
		// probe serves one real runtime, a declaration probe the name-first
		// entry (§2.3). A reaped session decides nothing — its update below is
		// what fails, so the 401 keeps precedence over the identity verdict.
		served, selected = probeAnswerRuntime(client, *session, req.Runtimes)
		if !selected {
			return
		}
		// The S4 answer is the entry the probe's own discovery runs against, so
		// the session keeps it: a startup result lands on that entry's slot
		// (§1.3), never on another ordering of the same declaration.
		//
		// The key is the workspace the result arrives on — the Job Token's own —
		// because S-27 carries no workspace and is authenticated by that same
		// token. Keying by the registration's declared workspace instead would
		// store the answer where the absorb cannot read it (§1.2: every
		// workspace declares the same runtime list, so the answer is the same).
		if session.Served == nil {
			session.Served = map[string]registry.DiscoveredRuntime{}
		}
		session.Served[claims.WorkspaceID] = registry.DiscoveredRuntime{
			Name: served.Name, Type: served.Type, Version: served.Version, ProfileID: served.ProfileID,
		}
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
	if !selected {
		// The command-driven probe's own runtime is not in its declaration, so
		// no entry would run the startup command of the runtime the result is
		// relayed to (§2.3 零命中即拒绝). The daemon only sees the body, and
		// this is a different condition from an empty declaration: it declared
		// runtimes, just not the one this probe serves.
		s.log.Error("probe registration names no runtime for the requested runtime",
			"job_name", claims.JobName, "runtime_id", session.RealRuntimeID)
		writeError(w, http.StatusConflict, "registration does not declare this probe's runtime")
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
		"name":       served.Name,
		"provider":   served.Type,
		"status":     "online",
		"profile_id": nil,
	}
	if served.ProfileID != "" {
		answer["profile_id"] = served.ProfileID
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

// probeAnswerRuntime picks the entry S4 answers with (§2.3 探针 Job 的选条). A
// command-driven probe serves the one real runtime it was started for: its
// answer must name that runtime's identity, because the result is relayed to it
// and any other entry's startup command would discover a different catalog. The
// declaration probes carry no runtime and answer the name-first entry, which is
// therefore also the entry their discovery runs against.
func probeAnswerRuntime(client *Client, session registry.ProbeSession, runtimes []registerRuntime) (registerRuntime, bool) {
	if session.Mode != registry.ProbeCommandDriven {
		return probeServedRuntime(runtimes), true
	}
	ref, ok := client.RefForRuntime(session.RealRuntimeID)
	if !ok {
		return registerRuntime{}, false
	}
	return selectDeclaredRuntime(runtimes, ref.Key())
}

// probeServedRuntime picks a declaration probe's answer: the first entry by name
// (§2.3 探针 Job 的选条, the stable payload ordering).
func probeServedRuntime(runtimes []registerRuntime) registerRuntime {
	ordered := append([]registerRuntime(nil), runtimes...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	return ordered[0]
}

// declaredServedRuntime recovers the entry a session's S4 answer pointed at when
// the session carries no recorded answer: an instance that predates the record
// answered with the declaration's name-first entry (§2.3), and that declaration
// is still in the session. A session that captured no declaration for this
// workspace has nothing to recover from.
func declaredServedRuntime(session registry.ProbeSession, workspaceID string) (registry.DiscoveredRuntime, bool) {
	declared, ok := session.Registrations[workspaceID]
	if !ok {
		if len(session.Registrations) > 0 {
			return registry.DiscoveredRuntime{}, false
		}
		// A session written before per-workspace registrations keeps only the
		// first declaration's mirror (§1.2).
		declared = registry.ProbeRegistration{Runtimes: session.Runtimes}
	}
	if len(declared.Runtimes) == 0 {
		return registry.DiscoveredRuntime{}, false
	}
	first := declared.Runtimes[0]
	for _, runtime := range declared.Runtimes[1:] {
		if runtime.Name < first.Name {
			first = runtime
		}
	}
	return first, true
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
		// The slot is the one the probe's own discovery ran for — the entry S4
		// answered with (§1.3), never another order of the same declaration.
		// The absorb belongs to the workspace the Job Token names: S-27 carries
		// no workspace of its own, and Verify has already refused a token whose
		// workspace contradicts a pinned session, so this is the one source.
		workspaceID := claims.WorkspaceID
		client := s.clientFor(workspaceID)
		if client == nil {
			// Without a configured workspace there is no slot to fill; the
			// success shape would report a landing that never happened and
			// hide the misconfiguration (§1.3 失败面).
			s.log.Error("startup discovery absorb refused: workspace is not configured",
				"job_name", claims.JobName, "workspace_id", workspaceID)
			writeError(w, http.StatusForbidden, "workspace is not configured")
			return
		}
		served, servedOK := session.Served[workspaceID]
		if !servedOK || served.Type == "" {
			// A session written by an instance that predates the recorded answer
			// (rolling upgrade, shared session store) carries the declaration but
			// no served entry. That instance answered S4 with the declaration's
			// name-first entry (§2.3), so the entry its discovery ran for is
			// recoverable — dropping the result would cost the runtime its
			// startup slot, not protect it (§1.3 无槽回落).
			served, servedOK = declaredServedRuntime(session, workspaceID)
		}
		if !servedOK || served.Type == "" {
			s.log.Error("startup discovery landed without a served runtime",
				"job_name", claims.JobName, "workspace_id", workspaceID)
			writeError(w, http.StatusConflict, "probe session carries no served runtime")
			return
		}
		provider, profileID := served.Type, served.ProfileID
		// The write-back is the only authority, and it lands before the slot:
		// a reap between the lookup above and here must leave neither a staged
		// body nor the success shape behind (§6.3 会话不存在 ⇒ 恒 401).
		if _, applied := s.probes.UpdateProbe(claims.JobName, func(probe *registry.ProbeSession) {
			probe.RelayedAt = now
			probe.RelayedCode = http.StatusOK
		}); !applied {
			writeError(w, http.StatusUnauthorized, "unknown probe session")
			return
		}
		client.stageStartupCatalog(provider, profileID, body)
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
		// The relay must not outrun the verdict: an empty write-back proves the
		// session still takes one, and a reap between the lookup above and this
		// update keeps the C21 from being sent at all (§6.3 会话不存在 ⇒ 恒 401).
		if _, applied := s.probes.UpdateProbe(claims.JobName, func(*registry.ProbeSession) {}); !applied {
			writeError(w, http.StatusUnauthorized, "unknown probe session")
			return
		}
		status, resp, err := relayCatalog(r.Context(), client, session.RealRuntimeID, req, body, session.Deadline, s.log)
		code := status
		if err != nil {
			s.log.Error("model catalog relay exhausted", "job_name", claims.JobName, "err", err)
			// Code 0 marks "nothing was relayed" for the waiter: it is not a
			// status any C21 answer can carry.
			code = 0
		}
		// The answer is gated on the same verdict: an upstream reply the
		// session can no longer record has no owner waiting for it.
		if _, applied := s.probes.UpdateProbe(claims.JobName, func(probe *registry.ProbeSession) {
			probe.RelayedAt = now
			probe.RelayedCode = code
		}); !applied {
			writeError(w, http.StatusUnauthorized, "unknown probe session")
			return
		}
		if err != nil {
			writeError(w, http.StatusBadGateway, "upstream unavailable")
		} else {
			writeRaw(w, status, resp)
		}
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

// cancelProbe is S19 for a probe: the probe is torn down by its lifecycle, so
// the握手 is acknowledged without touching the task registry.
func (s *FakeServer) cancelProbe(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
