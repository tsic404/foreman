package proxy

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/registry"
)

// startProbeToken starts a probe through the scheduler and returns the handle
// plus a Job Token whose digest matches the session (the real builder asks the
// issuer for the token; the test fake cannot). A real runtime id selects the
// command-driven mode, an empty one the startup-discovery mode.
func startProbeToken(t *testing.T, r *testRig, requestID, realRuntimeID string) (string, registry.ProbeSession) {
	t.Helper()
	return startProbeTokenBudget(t, r, requestID, realRuntimeID, 30*time.Second)
}

// startProbeTokenBudget is startProbeToken with an explicit probe budget (the
// C21 retry budget of the command-driven relay).
func startProbeTokenBudget(t *testing.T, r *testRig, requestID, realRuntimeID string, budget time.Duration) (string, registry.ProbeSession) {
	t.Helper()
	mode := registry.ProbeStartupDiscovery
	if realRuntimeID != "" {
		mode = registry.ProbeCommandDriven
	}
	return startProbeTokenMode(t, r, requestID, realRuntimeID, mode, budget)
}

// startProbeTokenMode is startProbeTokenBudget with the probe mode under the
// caller's control: the registration-discovery probe owns no command.
func startProbeTokenMode(t *testing.T, r *testRig, requestID, realRuntimeID string, mode registry.ProbeMode, budget time.Duration) (string, registry.ProbeSession) {
	t.Helper()
	workspaceID := ""
	if mode == registry.ProbeCommandDriven {
		workspaceID = "ws-1"
	}
	handle, err := r.sched.StartProbe(t.Context(), requestID, realRuntimeID, workspaceID, mode, budget)
	if err != nil {
		t.Fatalf("StartProbe: %v", err)
	}
	token, err := r.issuer.IssueProbe(handle.JobName, requestID, "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("IssueProbe: %v", err)
	}
	session, ok := r.probes.UpdateProbe(handle.JobName, func(s *registry.ProbeSession) {
		s.TokenHash = r.issuer.Hash(token)
	})
	if !ok {
		t.Fatalf("probe session %s missing", handle.JobName)
	}
	return token, session
}

func TestProbeRegisterCapturesStartupTruth(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeToken(t, r, "startup-1", "")

	code, body := r.call(t, "POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-catalog-startup-1",
		"cli_version": "0.9.9",
		"runtimes": [
			{"name": "zz-omp", "type": "zz", "version": "2.0.0"},
			{"name": "aa-omp", "type": "omp", "version": "1.0.0"}
		],
		"failed_profiles": [{"profile_id": "pf-1", "command_name": "x", "reason": "y"}]
	}`))
	if code != 200 {
		t.Fatalf("probe register = %d: %s", code, body)
	}
	var resp struct {
		Runtimes []struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			Provider string `json:"provider"`
			Status   string `json:"status"`
		} `json:"runtimes"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	if len(resp.Runtimes) != 1 {
		t.Fatalf("probe register must answer exactly one job runtime: %s", body)
	}
	if resp.Runtimes[0].ID != session.JobRuntime {
		t.Errorf("job runtime id = %q, want the minted %q", resp.Runtimes[0].ID, session.JobRuntime)
	}
	// The probe serves the first runtime by name; C1 declares that ordering.
	if resp.Runtimes[0].Name != "aa-omp" || resp.Runtimes[0].Provider != "omp" {
		t.Errorf("served runtime = %+v, want the first entry by name", resp.Runtimes[0])
	}

	updated, _ := r.probes.ProbeByJob(session.JobName)
	if updated.RegisteredAt.IsZero() {
		t.Fatal("the registration must be recorded as the startup truth source")
	}
	if updated.CLIVersion != "0.9.9" || len(updated.Runtimes) != 2 || updated.Runtimes[1].Type != "omp" {
		t.Errorf("captured discovery = %+v", updated)
	}
	if len(updated.FailedProfiles) != 1 {
		t.Errorf("failed_profiles = %v, want it carried verbatim", updated.FailedProfiles)
	}
}

func TestProbeRegisterRefusesEmptyDeclaration(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeToken(t, r, "cmd-empty", "rid-real")

	code, _ := r.call(t, "POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-catalog-cmd-empty", "runtimes": []
	}`))
	if code != 409 {
		t.Fatalf("empty probe declaration = %d, want 409 (refused registration)", code)
	}
	updated, _ := r.probes.ProbeByJob(session.JobName)
	if !updated.RegisteredAt.IsZero() {
		t.Error("a refused registration must not be recorded as the startup truth")
	}
}

func TestProbeHeartbeatDeliversCommandUntilRelayed(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeToken(t, r, "startup-2", "")

	code, body := r.call(t, "POST", "/api/daemon/heartbeat", token,
		[]byte(`{"runtime_id": "`+session.JobRuntime+`"}`))
	if code != 200 {
		t.Fatalf("probe heartbeat = %d: %s", code, body)
	}
	var ack struct {
		Status           string `json:"status"`
		PendingModelList *struct {
			ID string `json:"id"`
		} `json:"pending_model_list"`
	}
	if err := json.Unmarshal(body, &ack); err != nil {
		t.Fatalf("decode ack: %v (%s)", err, body)
	}
	if ack.PendingModelList == nil || ack.PendingModelList.ID != session.SelfMintedID {
		t.Fatalf("ack = %s, want the minted command %q", body, session.SelfMintedID)
	}

	r.probes.UpdateProbe(session.JobName, func(s *registry.ProbeSession) {
		s.RelayedAt = time.Now()
	})
	_, body = r.call(t, "POST", "/api/daemon/heartbeat", token,
		[]byte(`{"runtime_id": "`+session.JobRuntime+`"}`))
	if strings.Contains(string(body), "pending_model_list") {
		t.Errorf("a relayed probe must not be handed another command: %s", body)
	}
}

func TestProbeHeartbeatRejectsForeignRuntime(t *testing.T) {
	r := newTestRig(t)
	token, _ := startProbeToken(t, r, "startup-3", "")
	code, _ := r.call(t, "POST", "/api/daemon/heartbeat", token, []byte(`{"runtime_id": "someone-else"}`))
	if code != 403 {
		t.Fatalf("foreign runtime id = %d, want 403", code)
	}
}

func TestProbeStartupSlotUsesDeclarationOrder(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeToken(t, r, "startup-order", "")
	code, body := r.call(t, "POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-catalog-startup-order",
		"runtimes": [
			{"name": "zz-omp", "type": "omp", "version": "1.0.0"},
			{"name": "aa-claude", "type": "claude", "version": "1.0.0"}
		]
	}`))
	if code != 200 {
		t.Fatalf("probe register = %d: %s", code, body)
	}
	discovery := []byte(`{"status":"completed","models":[{"id":"m1"}]}`)
	code, body = r.call(t, "POST",
		"/api/daemon/runtimes/"+session.JobRuntime+"/models/"+session.SelfMintedID+"/result", token, discovery)
	if code != 200 {
		t.Fatalf("startup absorb = %d: %s", code, body)
	}
	// §1.3: the startup slot is the declaration's first entry, not the
	// name-first one (aa-claude would win a name sort).
	if staged, ok := r.client.startup.Take("omp", ""); !ok || string(staged) != string(discovery) {
		t.Fatalf("omp slot = %q (ok=%v), want the discovery body", staged, ok)
	}
	if _, ok := r.client.startup.Take("claude", ""); ok {
		t.Error("the name-first entry got the startup slot; the declaration order is the rule")
	}
}

func TestProbeModelResultAbsorbsStartupDiscovery(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeToken(t, r, "startup-4", "")
	r.call(t, "POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-catalog-startup-4",
		"runtimes": [{"name": "aa-omp", "type": "omp", "version": "1.0.0"}]
	}`))

	discoveryBody := []byte(`{"status":"completed","models":[{"id":"m1"}]}`)
	code, body := r.call(t, "POST",
		"/api/daemon/runtimes/"+session.JobRuntime+"/models/"+session.SelfMintedID+"/result", token, discoveryBody)
	if code != 200 {
		t.Fatalf("startup absorb = %d: %s", code, body)
	}
	if got := len(r.stub.calls("POST", "/api/daemon/runtimes/rid-real/models/"+session.SelfMintedID+"/result")); got != 0 {
		t.Error("a startup discovery must not produce any C21")
	}
	staged, ok := r.client.startup.Take("omp", "")
	if !ok || string(staged) != string(discoveryBody) {
		t.Fatalf("staged discovery = %q (ok=%v), want the reported bytes verbatim", staged, ok)
	}
	updated, _ := r.probes.ProbeByJob(session.JobName)
	if updated.RelayedAt.IsZero() || updated.RelayedCode != 200 {
		t.Errorf("the absorb must set the completion signal: %+v", updated)
	}
}

// A startup discovery whose workspace this deployment does not serve has no
// slot to fill: the absorb is refused (403, like the relay branch) instead of
// answering the success shape and claiming a landing that never happened.
func TestProbeModelResultRefusesStartupAbsorbForUnservedWorkspace(t *testing.T) {
	r := newTestRig(t)
	token, err := r.issuer.IssueProbe("fm-catalog-startup-other", "startup-other", "ws-other", time.Hour)
	if err != nil {
		t.Fatalf("IssueProbe: %v", err)
	}
	session := registry.ProbeSession{
		JobName:      "fm-catalog-startup-other",
		Mode:         registry.ProbeStartupDiscovery,
		JobRuntime:   "rid-startup-other",
		SelfMintedID: "startup-other",
		TokenHash:    r.issuer.Hash(token),
		Runtimes:     []registry.DiscoveredRuntime{{Name: "aa-omp", Type: "omp"}},
	}
	if err := r.probes.PutProbe(session); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}

	code, body := r.call(t, "POST",
		"/api/daemon/runtimes/"+session.JobRuntime+"/models/"+session.SelfMintedID+"/result",
		token, []byte(`{"status":"completed","models":[]}`))
	if code != http.StatusForbidden {
		t.Fatalf("absorb for an unserved workspace = %d: %s, want 403", code, body)
	}
	updated, _ := r.probes.ProbeByJob(session.JobName)
	if !updated.RelayedAt.IsZero() || updated.RelayedCode != 0 {
		t.Errorf("a refused absorb must not claim completion: %+v", updated)
	}
	if staged, ok := r.client.startup.Take("omp", ""); ok {
		t.Errorf("a refused absorb must not stage the body: %q", staged)
	}
}

func TestProbeModelResultRelaysCommandDriven(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeToken(t, r, "cmd-1", "rid-real")
	upstreamBody := []byte(`{"status":"completed","models":[]}`)
	// §1.3: the upstream success response is replayed to the probe daemon
	// verbatim ("原样回给探针 daemon") — a fixed local confirmation would
	// drop fields the daemon's own report face carries.
	upstreamAck := []byte(`{"status":"ok","request_id":"cmd-1"}`)
	r.stub.on("POST", "/api/daemon/runtimes/rid-real/models/cmd-1/result", func(w http.ResponseWriter, _ *http.Request, body []byte) {
		if string(body) != string(upstreamBody) {
			t.Errorf("C21 body = %s, want it verbatim", body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(upstreamAck)
	})

	code, body := r.call(t, "POST",
		"/api/daemon/runtimes/"+session.JobRuntime+"/models/cmd-1/result", token, upstreamBody)
	if code != 200 {
		t.Fatalf("command relay = %d: %s", code, body)
	}
	if string(body) != string(upstreamAck) {
		t.Errorf("S27 response = %s, want the upstream success body verbatim", body)
	}
	if got := len(r.stub.calls("POST", "/api/daemon/runtimes/rid-real/models/cmd-1/result")); got != 1 {
		t.Fatalf("C21 calls = %d, want 1", got)
	}
	updated, _ := r.probes.ProbeByJob(session.JobName)
	if updated.RelayedAt.IsZero() || updated.RelayedCode != 200 {
		t.Errorf("relay signal = %+v", updated)
	}
}

func TestProbeModelResultRelayRetriesTransient5xx(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeToken(t, r, "cmd-retry", "rid-real")
	attempts := 0
	r.stub.on("POST", "/api/daemon/runtimes/rid-real/models/cmd-retry/result", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeJSONStub(w, 200, map[string]string{"status": "ok"})
	})

	code, body := r.call(t, "POST",
		"/api/daemon/runtimes/"+session.JobRuntime+"/models/cmd-retry/result", token, []byte(`{"status":"completed"}`))
	if code != 200 {
		t.Fatalf("relay = %d: %s, want the retried 200", code, body)
	}
	if attempts != 3 {
		t.Errorf("upstream attempts = %d, want the §1.2 C-21 backoff to retry 5xx", attempts)
	}
	updated, _ := r.probes.ProbeByJob(session.JobName)
	if updated.RelayedCode != 200 {
		t.Errorf("relayed code = %d, want the final upstream status", updated.RelayedCode)
	}
}

func TestProbeModelResultRelayExhaustionRecordsZero(t *testing.T) {
	r := newTestRig(t)
	// A short probe budget makes the retry budget expire quickly.
	token, session := startProbeTokenBudget(t, r, "cmd-doomed", "rid-real", 150*time.Millisecond)
	attempts := 0
	r.stub.on("POST", "/api/daemon/runtimes/rid-real/models/cmd-doomed/result", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	code, _ := r.call(t, "POST",
		"/api/daemon/runtimes/"+session.JobRuntime+"/models/cmd-doomed/result", token, []byte(`{"status":"completed"}`))
	if code != http.StatusBadGateway {
		t.Fatalf("exhausted relay = %d, want 502 to the probe daemon", code)
	}
	if attempts < 2 {
		t.Errorf("upstream attempts = %d, want the transport errors retried", attempts)
	}
	updated, _ := r.probes.ProbeByJob(session.JobName)
	if updated.RelayedAt.IsZero() {
		t.Fatal("an exhausted relay still completes the wait")
	}
	if updated.RelayedCode != 0 {
		t.Errorf("relayed code = %d, want 0 (nothing was relayed)", updated.RelayedCode)
	}
}

func TestProbeModelResult4xxIsNotRetried(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeToken(t, r, "cmd-4xx", "rid-real")
	attempts := 0
	r.stub.on("POST", "/api/daemon/runtimes/rid-real/models/cmd-4xx/result", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		attempts++
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad request"}`))
	})

	code, body := r.call(t, "POST",
		"/api/daemon/runtimes/"+session.JobRuntime+"/models/cmd-4xx/result", token, []byte(`{}`))
	if code != http.StatusBadRequest {
		t.Fatalf("4xx relay = %d (%s), want the upstream verdict verbatim", code, body)
	}
	if attempts != 1 {
		t.Errorf("upstream attempts = %d, want 4xx treated as terminal", attempts)
	}
}

func TestProbeModelResultUnknownRequestIs403(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeToken(t, r, "cmd-2", "rid-real")
	code, _ := r.call(t, "POST",
		"/api/daemon/runtimes/"+session.JobRuntime+"/models/not-mine/result", token, []byte(`{}`))
	// §10「探针凭据不越界」: a {req} outside the session's two ids is a scope
	// violation (403), never a routing-level 404.
	if code != 403 {
		t.Fatalf("unknown {req} = %d, want 403", code)
	}
}

// TestProbeTokenOnTaskPathsIs403 pins blocking finding #1: a probe Job Token
// has no task face, so the task-addressed endpoints reject it before any
// upstream call or task settlement can happen.
func TestProbeTokenOnTaskPathsIs403(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeToken(t, r, "cmd-tid", "rid-real")
	r.stub.on("GET", "/api/daemon/tasks/t-inflight/status", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		t.Error("a probe token must never reach C13")
		writeJSONStub(w, 200, map[string]string{"status": "running"})
	})
	r.stub.on("POST", "/api/daemon/runtimes/rid-real/tasks/t-inflight/skill-bundles/resolve", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		t.Error("a probe token must never reach C19")
		writeJSONStub(w, 200, map[string]string{"bundle": "x"})
	})
	r.stub.on("POST", "/api/daemon/runtimes/"+session.JobRuntime+"/recover-orphans", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		t.Error("a probe token must never reach C15")
		writeJSONStub(w, 200, map[string]int{"orphaned": 0})
	})

	paths := []struct{ method, path string }{
		{"GET", "/api/daemon/tasks/t-inflight/status"},                                                     // S9
		{"GET", "/api/daemon/tasks/t-inflight/gc-check"},                                                   // S20
		{"POST", "/api/daemon/runtimes/" + session.JobRuntime + "/tasks/t-inflight/skill-bundles/resolve"}, // S26
	}
	for _, p := range paths {
		code, body := r.call(t, p.method, p.path, token, []byte(`{}`))
		if code != 403 {
			t.Errorf("%s %s = %d (%s), want 403 (probe carries no task scope)", p.method, p.path, code, body)
		}
	}
}

func TestTaskJobModelResultIs404(t *testing.T) {
	r := newTestRig(t)
	r.claimTask(t, "t-s27")
	token := r.token(t, "t-s27")
	jobRuntime := r.registerDaemon(t, "t-s27", token)

	code, _ := r.call(t, "POST", "/api/daemon/runtimes/"+jobRuntime+"/models/req-1/result", token, []byte(`{}`))
	if code != 404 {
		t.Fatalf("task Job S27 = %d, want 404 (F9: task Jobs are never a catalog source)", code)
	}
	if got := len(r.stub.calls("POST", "/api/daemon/runtimes/rid-real/models/req-1/result")); got != 0 {
		t.Error("a task Job's result must never be forwarded")
	}
}

func TestProbeClaimDeregisterAndWS(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeToken(t, r, "startup-5", "")

	code, body := r.call(t, "POST", "/api/daemon/tasks/claim", token,
		[]byte(`{"daemon_id": "fm-catalog-startup-5", "runtime_ids": ["`+session.JobRuntime+`"], "max_tasks": 1}`))
	if code != 200 || !strings.Contains(string(body), `"tasks":[]`) {
		t.Fatalf("probe claim = %d: %s, want an empty batch", code, body)
	}
	code, _ = r.call(t, "POST", "/api/daemon/deregister", token, []byte(`{"runtime_ids": ["`+session.JobRuntime+`"]}`))
	if code != 200 {
		t.Fatalf("probe deregister = %d, want 200", code)
	}
	// S30 is not open to probes: the daemon falls back to HTTP heartbeats.
	code, _ = r.call(t, "GET", "/api/daemon/ws?runtime_ids="+session.JobRuntime, token, nil)
	if code != 404 {
		t.Fatalf("probe WS = %d, want 404", code)
	}
}

func TestProbeTokenScopeMismatchIsForbidden(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeToken(t, r, "cmd-3", "rid-real")

	// {rid} must be the probe's own job runtime, even if it is otherwise ours.
	code, _ := r.call(t, "POST", "/api/daemon/runtimes/other-runtime/models/cmd-3/result", token, []byte(`{}`))
	if code != 403 {
		t.Fatalf("foreign {rid} = %d, want 403", code)
	}
	if _, err := r.issuer.Verify(token, auth.RequestScope{RuntimeID: session.JobRuntime}); err != nil {
		t.Fatalf("the session's own rid must verify: %v", err)
	}
	// A probe token never reaches the task surface.
	code, _ = r.call(t, "POST", "/api/daemon/tasks/claim", token,
		[]byte(`{"daemon_id": "fm-catalog-cmd-3", "runtime_ids": ["`+session.JobRuntime+`"], "max_tasks": 1}`))
	if code != 200 {
		t.Fatalf("probe claim = %d", code)
	}
}

// TestProbeRegisterOnlyHeartbeatOwnsNoCommand pins the §1.6 discovery probe's
// command face: it registers and is cleaned up, so its heartbeat ack must never
// carry pending_model_list — and with no command there is nothing a result could
// be absorbed for either.
func TestProbeRegisterOnlyHeartbeatOwnsNoCommand(t *testing.T) {
	r := newTestRig(t)
	token, session := startProbeTokenMode(t, r, "discovery-1", "", registry.ProbeRegisterOnly, 30*time.Second)

	code, body := r.call(t, "POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-catalog-discovery-1",
		"cli_version": "0.9.9",
		"runtimes": [{"name": "aa-omp", "type": "omp", "version": "1.0.0"}]
	}`))
	if code != 200 {
		t.Fatalf("discovery probe register = %d: %s", code, body)
	}
	updated, _ := r.probes.ProbeByJob(session.JobName)
	if updated.RegisteredAt.IsZero() {
		t.Fatal("the discovery probe's registration body is the C1 truth source and must be recorded")
	}
	if updated.SelfMintedID != "" || updated.RequestID != "" {
		t.Fatalf("a registration-only session owns no command: %+v", updated)
	}

	code, body = r.call(t, "POST", "/api/daemon/heartbeat", token,
		[]byte(`{"runtime_id": "`+session.JobRuntime+`"}`))
	if code != 200 {
		t.Fatalf("probe heartbeat = %d: %s", code, body)
	}
	if strings.Contains(string(body), "pending_model_list") {
		t.Errorf("a registration-only probe must never be handed a model catalog command: %s", body)
	}
	// A result for the id the startup path would have minted has no owner here:
	// the session carries no command, so the probe token's scope rejects it
	// before any handler runs.
	code, body = r.call(t, "POST",
		"/api/daemon/runtimes/"+session.JobRuntime+"/models/discovery-1/result", token, []byte(`{"models":[]}`))
	if code == 200 {
		t.Errorf("register-only result = %d: %s, want a refusal (no command, no slot)", code, body)
	}
	if staged, ok := r.client.startup.Take("omp", ""); ok {
		t.Errorf("a registration-only probe must not stage a catalog body: %q", staged)
	}
}
