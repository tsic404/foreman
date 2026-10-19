// TC-tech-protocol-02 (AC-59): the daemon-face protocol census and carrier
// fidelity. Every upstream daemon-facing route is declared as one of four
// treatments; the registered pattern set equals the declared set both ways
// (the conditional WS route included); refused and unmodelled routes answer
// the declared 404 with zero upstream calls; forwarded routes reach the
// upstream with the caller's credential and replay its status, header and
// body; and the carriers that reproduce upstream fields — the claim payload,
// the S-20 answer and the workspace-state block on S-2/S-4 — travel verbatim.
package acceptance

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/proxy"
)

// tcProtocol02Handling is one route's declared treatment (§2.2).
type tcProtocol02Handling string

const (
	tcFaceServed     tcProtocol02Handling = "served"
	tcFaceForwarded  tcProtocol02Handling = "forwarded"
	tcFaceRefused    tcProtocol02Handling = "refused"
	tcFaceUnmodelled tcProtocol02Handling = "unmodelled"
)

// tcProtocol02Entry is one upstream route plus Foreman's declared handling.
type tcProtocol02Entry struct {
	name     string
	upstream string
	local    string
	design   string
	handling tcProtocol02Handling
	probe    *tcProtocol02Probe
}

// tcProtocol02Probe drives one entry through the real fake server. {tid},
// {rid}, {ws}, {req}, {cid} and {id} are substituted with the fixture's
// values.
type tcProtocol02Probe struct {
	method   string
	path     string
	body     string
	want     int
	upstream string
}

// tcProtocol02Census is the daemon-face snapshot: multica server 18fa9f7,
// cmd/server/router.go's /api/daemon group plus /api/tokens/current/renew and
// the legacy /api/workspaces. The acceptance form duplicates the in-package
// census on purpose — this file is the AC-59 executable.
var tcProtocol02Census = []tcProtocol02Entry{
	// ---- served ----
	{name: "S-4 register", upstream: "POST /api/daemon/register", local: "POST /api/daemon/register", design: "S-4", handling: tcFaceServed},
	{name: "S-19 deregister", upstream: "POST /api/daemon/deregister", local: "POST /api/daemon/deregister", design: "S-19", handling: tcFaceServed},
	{name: "S-5 heartbeat", upstream: "POST /api/daemon/heartbeat", local: "POST /api/daemon/heartbeat", design: "S-5", handling: tcFaceServed},
	{name: "S-30 ws", upstream: "GET /api/daemon/ws", local: "GET /api/daemon/ws", design: "S-30", handling: tcFaceServed},
	{name: "S-1 workspaces", upstream: "GET /api/daemon/workspaces", local: "GET /api/daemon/workspaces", design: "S-1", handling: tcFaceServed},
	{name: "S-2 repos", upstream: "GET /api/daemon/workspaces/{workspaceId}/repos", local: "GET /api/daemon/workspaces/{ws}/repos", design: "S-2", handling: tcFaceServed},
	{name: "S-3 runtime profiles", upstream: "GET /api/daemon/workspaces/{workspaceId}/runtime-profiles", local: "GET /api/daemon/workspaces/{ws}/runtime-profiles", design: "S-3", handling: tcFaceServed},
	{name: "S-6 claim", upstream: "POST /api/daemon/tasks/claim", local: "POST /api/daemon/tasks/claim", design: "S-6", handling: tcFaceServed},
	{name: "S-7 prepare-lease", upstream: "POST /api/daemon/runtimes/{runtimeId}/tasks/{taskId}/prepare-lease", local: "POST /api/daemon/runtimes/{rid}/tasks/{tid}/prepare-lease", design: "S-7", handling: tcFaceServed},
	{name: "S-26 skill-bundles resolve", upstream: "POST /api/daemon/runtimes/{runtimeId}/tasks/{taskId}/skill-bundles/resolve", local: "POST /api/daemon/runtimes/{rid}/tasks/{tid}/skill-bundles/resolve", design: "S-26", handling: tcFaceServed},
	{name: "S-9 task status", upstream: "GET /api/daemon/tasks/{taskId}/status", local: "GET /api/daemon/tasks/{tid}/status", design: "S-9", handling: tcFaceServed},
	{name: "S-8 start", upstream: "POST /api/daemon/tasks/{taskId}/start", local: "POST /api/daemon/tasks/{tid}/start", design: "S-8", handling: tcFaceServed},
	{name: "S-10 progress", upstream: "POST /api/daemon/tasks/{taskId}/progress", local: "POST /api/daemon/tasks/{tid}/progress", design: "S-10", handling: tcFaceServed},
	{name: "S-11 messages", upstream: "POST /api/daemon/tasks/{taskId}/messages", local: "POST /api/daemon/tasks/{tid}/messages", design: "S-11", handling: tcFaceServed},
	{name: "S-12 usage", upstream: "POST /api/daemon/tasks/{taskId}/usage", local: "POST /api/daemon/tasks/{tid}/usage", design: "S-12", handling: tcFaceServed},
	{name: "S-13 complete", upstream: "POST /api/daemon/tasks/{taskId}/complete", local: "POST /api/daemon/tasks/{tid}/complete", design: "S-13", handling: tcFaceServed},
	{name: "S-14 fail", upstream: "POST /api/daemon/tasks/{taskId}/fail", local: "POST /api/daemon/tasks/{tid}/fail", design: "S-14", handling: tcFaceServed},
	{name: "S-15 cancel-ack", upstream: "POST /api/daemon/tasks/{taskId}/cancel-ack", local: "POST /api/daemon/tasks/{tid}/cancel-ack", design: "S-15", handling: tcFaceServed},
	{name: "S-16 session", upstream: "POST /api/daemon/tasks/{taskId}/session", local: "POST /api/daemon/tasks/{tid}/session", design: "S-16", handling: tcFaceServed},
	{name: "S-17 wait-local-directory", upstream: "POST /api/daemon/tasks/{taskId}/wait-local-directory", local: "POST /api/daemon/tasks/{tid}/wait-local-directory", design: "S-17", handling: tcFaceServed},
	{name: "S-18 recover-orphans", upstream: "POST /api/daemon/runtimes/{runtimeId}/recover-orphans", local: "POST /api/daemon/runtimes/{rid}/recover-orphans", design: "S-18", handling: tcFaceServed},
	{name: "S-21 issue gc-check (batch)", upstream: "POST /api/daemon/workspaces/{workspaceId}/issues/gc-check", local: "POST /api/daemon/workspaces/{ws}/issues/gc-check", design: "S-21", handling: tcFaceServed},
	{name: "S-20 task gc-check", upstream: "GET /api/daemon/tasks/{taskId}/gc-check", local: "GET /api/daemon/tasks/{tid}/gc-check", design: "S-20", handling: tcFaceServed},
	{name: "S-24 token renew", upstream: "POST /api/tokens/current/renew", local: "POST /api/tokens/current/renew", design: "S-24", handling: tcFaceServed},
	{name: "S-28 workspaces (legacy)", upstream: "GET /api/workspaces", local: "GET /api/workspaces", design: "S-28", handling: tcFaceServed},

	// ---- refused ----
	{name: "S-32 legacy per-runtime claim", upstream: "POST /api/daemon/runtimes/{runtimeId}/tasks/claim", local: "POST /api/daemon/runtimes/{rid}/tasks/claim", design: "S-32", handling: tcFaceRefused,
		probe: &tcProtocol02Probe{method: "POST", path: "/api/daemon/runtimes/{rid}/tasks/claim", body: "{}", want: http.StatusNotFound}},
	{name: "S-27 update result", upstream: "POST /api/daemon/runtimes/{runtimeId}/update/{updateId}/result", local: "POST /api/daemon/runtimes/{rid}/update/{req}/result", design: "S-27", handling: tcFaceRefused,
		probe: &tcProtocol02Probe{method: "POST", path: "/api/daemon/runtimes/{rid}/update/req-1/result", body: "{}", want: http.StatusNotFound}},
	{name: "S-27 local skills result", upstream: "POST /api/daemon/runtimes/{runtimeId}/local-skills/{requestId}/result", local: "POST /api/daemon/runtimes/{rid}/local-skills/{req}/result", design: "S-27", handling: tcFaceRefused,
		probe: &tcProtocol02Probe{method: "POST", path: "/api/daemon/runtimes/{rid}/local-skills/req-1/result", body: "{}", want: http.StatusNotFound}},
	{name: "S-27 local skills import result", upstream: "POST /api/daemon/runtimes/{runtimeId}/local-skills/import/{requestId}/result", local: "POST /api/daemon/runtimes/{rid}/local-skills/import/{req}/result", design: "S-27", handling: tcFaceRefused,
		probe: &tcProtocol02Probe{method: "POST", path: "/api/daemon/runtimes/{rid}/local-skills/import/req-1/result", body: "{}", want: http.StatusNotFound}},
	{name: "S-27 model result", upstream: "POST /api/daemon/runtimes/{runtimeId}/models/{requestId}/result", local: "POST /api/daemon/runtimes/{rid}/models/{req}/result", design: "S-27", handling: tcFaceRefused,
		probe: &tcProtocol02Probe{method: "POST", path: "/api/daemon/runtimes/{rid}/models/req-1/result", body: "{}", want: http.StatusNotFound}},
	{name: "S-22 issue gc-check", upstream: "GET /api/daemon/issues/{issueId}/gc-check", local: "GET /api/daemon/issues/{id}/gc-check", design: "S-22", handling: tcFaceRefused,
		probe: &tcProtocol02Probe{method: "GET", path: "/api/daemon/issues/issue-1/gc-check", want: http.StatusNotFound}},
	{name: "S-22 chat-session gc-check", upstream: "GET /api/daemon/chat-sessions/{sessionId}/gc-check", local: "GET /api/daemon/chat-sessions/{id}/gc-check", design: "S-22", handling: tcFaceRefused,
		probe: &tcProtocol02Probe{method: "GET", path: "/api/daemon/chat-sessions/session-1/gc-check", want: http.StatusNotFound}},
	{name: "S-22 autopilot-run gc-check", upstream: "GET /api/daemon/autopilot-runs/{runId}/gc-check", local: "GET /api/daemon/autopilot-runs/{id}/gc-check", design: "S-22", handling: tcFaceRefused,
		probe: &tcProtocol02Probe{method: "GET", path: "/api/daemon/autopilot-runs/run-1/gc-check", want: http.StatusNotFound}},

	// ---- forwarded ----
	{name: "S-23 plugin-hooks (POST)", upstream: "POST /api/daemon/tasks/{id}/plugin-hooks", local: "POST /api/daemon/tasks/{tid}/plugin-hooks", design: "S-23", handling: tcFaceForwarded,
		probe: &tcProtocol02Probe{method: "POST", path: "/api/daemon/tasks/{tid}/plugin-hooks", body: `{"hook":"a"}`, want: http.StatusTeapot,
			upstream: "/api/daemon/tasks/{tid}/plugin-hooks"}},
	{name: "S-23 plugin-hooks (GET)", local: "GET /api/daemon/tasks/{tid}/plugin-hooks", design: "S-23", handling: tcFaceForwarded,
		probe: &tcProtocol02Probe{method: "GET", path: "/api/daemon/tasks/{tid}/plugin-hooks", want: http.StatusTeapot,
			upstream: "/api/daemon/tasks/{tid}/plugin-hooks"}},
	{name: "S-23 plugin-mcp credential", upstream: "GET /api/daemon/tasks/{id}/plugin-mcp/{contributionId}/credential", local: "GET /api/daemon/tasks/{tid}/plugin-mcp/{cid}/credential", design: "S-23", handling: tcFaceForwarded,
		probe: &tcProtocol02Probe{method: "GET", path: "/api/daemon/tasks/{tid}/plugin-mcp/cid-1/credential", want: http.StatusTeapot,
			upstream: "/api/daemon/tasks/{tid}/plugin-mcp/cid-1/credential"}},
	{name: "S-23 remote-mcp credential", upstream: "GET /api/daemon/tasks/{id}/remote-mcp/{contributionId}/credential", local: "GET /api/daemon/tasks/{tid}/remote-mcp/{cid}/credential", design: "S-23", handling: tcFaceForwarded,
		probe: &tcProtocol02Probe{method: "GET", path: "/api/daemon/tasks/{tid}/remote-mcp/cid-1/credential", want: http.StatusTeapot,
			upstream: "/api/daemon/tasks/{tid}/remote-mcp/cid-1/credential"}},

	// ---- unmodelled ----
	{name: "unmodelled: batch claim alias", upstream: "POST /api/daemon/claim", handling: tcFaceUnmodelled,
		probe: &tcProtocol02Probe{method: "POST", path: "/api/daemon/claim", body: "{}", want: http.StatusNotFound}},
	{name: "unmodelled: pending tasks", upstream: "GET /api/daemon/runtimes/{runtimeId}/tasks/pending", handling: tcFaceUnmodelled,
		probe: &tcProtocol02Probe{method: "GET", path: "/api/daemon/runtimes/{rid}/tasks/pending", want: http.StatusNotFound}},
	{name: "unmodelled: supplement claim", upstream: "POST /api/daemon/tasks/{taskId}/supplements/claim", handling: tcFaceUnmodelled,
		probe: &tcProtocol02Probe{method: "POST", path: "/api/daemon/tasks/{tid}/supplements/claim", body: "{}", want: http.StatusNotFound}},
	{name: "unmodelled: supplement ack", upstream: "POST /api/daemon/tasks/{taskId}/supplements/{commentId}/ack", handling: tcFaceUnmodelled,
		probe: &tcProtocol02Probe{method: "POST", path: "/api/daemon/tasks/{tid}/supplements/comment-1/ack", body: "{}", want: http.StatusNotFound}},
	{name: "unmodelled: task messages (GET)", upstream: "GET /api/daemon/tasks/{taskId}/messages", handling: tcFaceUnmodelled,
		probe: &tcProtocol02Probe{method: "GET", path: "/api/daemon/tasks/{tid}/messages", want: http.StatusNotFound}},
	{name: "unmodelled: user runtime list", upstream: "GET /api/runtimes", handling: tcFaceUnmodelled,
		probe: &tcProtocol02Probe{method: "GET", path: "/api/runtimes", want: http.StatusNotFound}},
}

// tcProtocol02Path substitutes the probe placeholders.
func tcProtocol02Path(path, taskID, jobRuntimeID string) string {
	return strings.NewReplacer(
		"{tid}", taskID, "{rid}", jobRuntimeID, "{ws}", "ws-1",
		"{req}", "req-1", "{cid}", "cid-1", "{id}", "issue-1",
	).Replace(path)
}

// tcProtocol02Call performs one request and returns the answer head too.
func tcProtocol02Call(t *testing.T, h *harness, method, path, token string, body []byte) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, h.jobsHTTP.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read answer: %v", err)
	}
	return resp.StatusCode, resp.Header, raw
}

// tcProtocol02Fixture claims one task and registers its daemon, returning the
// token and the Job runtime id the path placeholders need.
func tcProtocol02Fixture(t *testing.T) (*harness, string, string) {
	t.Helper()
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("t-face")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "t-face")
	registerTaskDaemon(t, h, "t-face", token)
	entry, ok := h.reg.Get("t-face")
	if !ok {
		t.Fatal("entry t-face missing")
	}
	return h, token, entry.JobRuntimeID
}

func TestProtocol02RegisteredSetEqualsDeclaredSet(t *testing.T) {
	h := newDeliveryHarness(t)

	// The full table is registered only when the WS handler is wired
	// (conditional registration, S-30).
	ws := proxy.NewWSHandler(h.issuer, h.reg, h.metrics)
	withWS := proxy.NewFakeServer(h.issuer, h.reg, h.sched, h.client, ws, h.metrics,
		proxy.WithProbeRegistry(h.probes), proxy.WithSkillBundles(h.bundles))
	registered := map[string]bool{}
	for _, pattern := range withWS.RegisteredPatterns() {
		if pattern == "/api/" {
			continue // the catch-all is the face's last resort, not a route
		}
		if registered[pattern] {
			t.Errorf("pattern %q is registered twice", pattern)
		}
		registered[pattern] = true
	}
	declared := map[string]string{}
	for _, entry := range tcProtocol02Census {
		if entry.handling == tcFaceUnmodelled {
			if entry.local != "" {
				t.Errorf("%s: an unmodelled entry must not declare a local pattern", entry.name)
			}
			continue
		}
		if entry.local == "" {
			t.Errorf("%s: a %s entry must declare its local pattern", entry.name, entry.handling)
			continue
		}
		if prev, duplicate := declared[entry.local]; duplicate {
			t.Errorf("pattern %s declared twice (%s, %s)", entry.local, prev, entry.name)
		}
		declared[entry.local] = entry.name
	}
	for pattern, name := range declared {
		if !registered[pattern] {
			t.Errorf("%s: declared pattern %q is not registered", name, pattern)
		}
	}
	for pattern := range registered {
		if _, ok := declared[pattern]; !ok {
			t.Errorf("pattern %q is registered but not declared in the census", pattern)
		}
	}

	// The harness's own server has no WS handler: S-30 is absent and every
	// other declared pattern is present.
	plain, ok := h.handler.(*proxy.FakeServer)
	if !ok {
		t.Fatalf("harness handler is %T, want *proxy.FakeServer", h.handler)
	}
	plainPatterns := map[string]bool{}
	for _, pattern := range plain.RegisteredPatterns() {
		plainPatterns[pattern] = true
	}
	if plainPatterns["GET /api/daemon/ws"] {
		t.Error("the WS route is registered without a WS handler")
	}
	for pattern := range declared {
		if pattern == "GET /api/daemon/ws" {
			continue
		}
		if !plainPatterns[pattern] {
			t.Errorf("declared pattern %q is not registered on the plain server", pattern)
		}
	}
}

func TestProtocol02UnservedRoutesRefuseLocally(t *testing.T) {
	for _, entry := range tcProtocol02Census {
		probe := entry.probe
		if probe == nil || probe.upstream != "" {
			continue
		}
		t.Run(entry.name, func(t *testing.T) {
			h, token, jobRuntimeID := tcProtocol02Fixture(t)
			path := tcProtocol02Path(probe.path, "t-face", jobRuntimeID)
			before := len(h.upstream.all())
			code, _, body := tcProtocol02Call(t, h, probe.method, path, token, []byte(probe.body))
			if code != probe.want {
				t.Fatalf("%s %s = %d (%s), want %d", probe.method, path, code, body, probe.want)
			}
			if after := len(h.upstream.all()); after != before {
				t.Errorf("%s %s reached the upstream (%d new calls); a refused route is answered locally",
					probe.method, path, after-before)
			}
			var errBody struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(body, &errBody); err != nil || errBody.Error == "" {
				t.Errorf("%s %s body = %s, want the {\"error\":…} shape", probe.method, path, body)
			}
		})
	}
}

func TestProtocol02ForwardedRoutesReachUpstream(t *testing.T) {
	for _, entry := range tcProtocol02Census {
		probe := entry.probe
		if probe == nil || probe.upstream == "" {
			continue
		}
		t.Run(entry.name, func(t *testing.T) {
			h, token, jobRuntimeID := tcProtocol02Fixture(t)
			path := tcProtocol02Path(probe.path, "t-face", jobRuntimeID)
			upstreamPath := tcProtocol02Path(probe.upstream, "t-face", jobRuntimeID)

			code, header, body := tcProtocol02Call(t, h, probe.method, path, token, []byte(probe.body))
			calls := h.upstream.calls(probe.method, upstreamPath)
			if len(calls) != 1 {
				t.Fatalf("upstream calls at %s = %d, want 1", upstreamPath, len(calls))
			}
			if got := calls[0].Header.Get("Authorization"); got != "Bearer "+token {
				t.Errorf("upstream Authorization = %q, want the caller's own token verbatim", got)
			}
			if code != probe.want {
				t.Errorf("%s %s = %d, want the upstream status %d replayed", probe.method, path, code, probe.want)
			}
			if string(body) != `{"credential_header":"x","credential":"v","x_future_field":true}` {
				t.Errorf("answer body = %s, want the upstream bytes verbatim", body)
			}
			if got := header.Get("X-Face-Upstream"); got != "kept" {
				t.Errorf("answer X-Face-Upstream = %q, want the upstream's header replayed", got)
			}
			if got := header.Get("Content-Type"); got != "application/json" {
				t.Errorf("answer Content-Type = %q, want the upstream's value", got)
			}
		})
	}
}

// tcProtocol02SameJSON compares two documents semantically (key order and
// whitespace are not part of the comparison; numbers and unknown keys are).
func tcProtocol02SameJSON(t *testing.T, got, want []byte) {
	t.Helper()
	decode := func(raw []byte) any {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var value any
		if err := dec.Decode(&value); err != nil {
			t.Fatalf("decode JSON: %v (%s)", err, raw)
		}
		return value
	}
	if g, w := decode(got), decode(want); !reflect.DeepEqual(g, w) {
		t.Errorf("JSON differs\n got: %s\nwant: %s", got, want)
	}
}

func TestProtocol02CarrierFidelity(t *testing.T) {
	t.Run("claim payload keeps every field but runtime_id", func(t *testing.T) {
		h := newDeliveryHarness(t)
		payload := json.RawMessage(`{
			"id": "t-carry", "agent_id": "agent-1", "issue_id": "issue-1",
			"issue_identifier": "TSI-1", "workspace_id": "ws-1",
			"runtime_id": "rid-zz", "dispatched_at": "2026-10-18T00:00:00Z",
			"auth_token": "mat_tasktoken", "start_claim_supported": true,
			"agent": {"model": "k3", "thinking": {"level": 2}},
			"repos": [{"url": "https://example.invalid/r", "ref": "main"}],
			"remote_mcp_connections": [{"id": "mcp-1", "daemon_token": "tok"}],
			"plugin_hook_tools": ["a", "b"],
			"coalesced_comments": [{"id": "c-1", "body": "hi"}],
			"prior_session_id": "sess-1", "trigger_kind": "assign",
			"x_future_field": {"nested": [1, 2, {"deep": true}], "null": null}
		}`)
		if err := h.sched.OnClaim(t.Context(), payload); err != nil {
			t.Fatalf("OnClaim: %v", err)
		}
		entry, _ := h.reg.Get("t-carry")
		token := issueJobToken(t, h, "t-carry")
		registerTaskDaemon(t, h, "t-carry", token)
		raw := claimTask(t, h, "t-carry", token)
		var answer struct {
			Tasks []json.RawMessage `json:"tasks"`
		}
		if err := json.Unmarshal(raw, &answer); err != nil || len(answer.Tasks) != 1 {
			t.Fatalf("S6 = %s (%v)", raw, err)
		}
		var want map[string]json.RawMessage
		if err := json.Unmarshal(payload, &want); err != nil {
			t.Fatalf("payload: %v", err)
		}
		want["runtime_id"], _ = json.Marshal(entry.JobRuntimeID)
		wantRaw, _ := json.Marshal(want)
		tcProtocol02SameJSON(t, answer.Tasks[0], wantRaw)
	})

	t.Run("task gc-check replays upstream status and bytes", func(t *testing.T) {
		h, token, _ := tcProtocol02Fixture(t)
		upstreamBody := `{"status":"completed","completed_at":"2026-10-05T00:00:00Z","x_future_field":[1,2]}`
		h.upstream.setGCCheck(http.StatusOK, upstreamBody)
		code, _, body := tcProtocol02Call(t, h, "GET", "/api/daemon/tasks/t-face/gc-check", token, nil)
		if code != http.StatusOK || string(body) != upstreamBody {
			t.Errorf("S20 = %d %s, want 200 %s verbatim", code, body, upstreamBody)
		}
	})

	t.Run("S-2 and S-4 carry the workspace state verbatim", func(t *testing.T) {
		h := newHarness(t, false)
		repos := `[{"url":"https://example.invalid/r","description":"d","ref":"main","x_repo_key":{"nested":1}}]`
		settings := `{"github_enabled":true,"co_authored_by_enabled":false}`
		h.upstream.setRegisterWorkspace(repos, "abc123", settings)
		h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
		h.declare(declaredRuntimes())
		if err := h.sched.OnClaim(t.Context(), deliveryPayload("t-state")); err != nil {
			t.Fatalf("OnClaim: %v", err)
		}
		token := issueJobToken(t, h, "t-state")
		registerTaskDaemon(t, h, "t-state", token)

		for _, call := range []struct {
			label, method, path string
		}{
			{"S-2", "GET", "/api/daemon/workspaces/ws-1/repos"},
			{"S-4", "POST", "/api/daemon/register"},
		} {
			body := []byte(`{"daemon_id":"fm-t-state","runtimes":[{"name":"foreman-zz","type":"zz","version":"3.1.4"}]}`)
			if call.method == "GET" {
				body = nil
			}
			code, _, raw := tcProtocol02Call(t, h, call.method, call.path, token, body)
			if code != http.StatusOK {
				t.Fatalf("%s = %d (%s), want 200", call.label, code, raw)
			}
			var resp map[string]json.RawMessage
			if err := json.Unmarshal(raw, &resp); err != nil {
				t.Fatalf("%s: %v", call.label, err)
			}
			for key, want := range map[string]string{
				"repos": repos, "repos_version": `"abc123"`, "settings": settings,
			} {
				got, present := resp[key]
				if !present {
					t.Errorf("%s: %s is absent; the upstream gave it", call.label, key)
					continue
				}
				tcProtocol02SameJSON(t, got, []byte(want))
			}
		}
	})
}
