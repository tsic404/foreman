// TC-tech-workspace-state-01 implements AC-58 in the form this repo can
// execute: the C-1 seed, the S-2-triggered C-22 refresh (single-flight, bounded
// timeout), verbatim field replay with upstream presence semantics, the
// failure replay path with its event/metric, and the register-before window.
// The daemon-side git behaviour (hook installation / trailer) needs a Job image
// and stays QA-side; the wire facts it consumes are asserted here.
package acceptance

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// workspaceBody builds one C-22 answer. settings == nil omits the key; a
// non-nil raw value is emitted verbatim (including `null` / `{}`).
func workspaceBody(repos, version string, settings json.RawMessage) string {
	body := map[string]any{"workspace_id": "ws-1", "repos": json.RawMessage(repos), "repos_version": version}
	if settings != nil {
		body["settings"] = settings
	}
	raw, _ := json.Marshal(body)
	return string(raw)
}

func decodeWorkspace(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var out map[string]json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode workspace answer: %v (%s)", err, raw)
	}
	return out
}

// callAsync drives one Job-side request with its own bounded client, so a
// concurrency scenario can hold many requests in flight.
func callAsync(h *harness, method, path, token string, body []byte) (int, []byte, error) {
	req, err := http.NewRequest(method, h.jobsHTTP.URL+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	return resp.StatusCode, raw, err
}

func workspaceRefreshCount(h *harness, result string) int {
	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()
	n := 0
	for _, got := range h.metrics.workspaceResults {
		if got == result {
			n++
		}
	}
	return n
}

func TestTechWorkspaceState01SeedRefreshAndPresence(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.setRuntimeID("omp", "", "rid-omp")

	// Pre-registration window: the task exists (its token authenticates S-2),
	// but C-1 has not run.
	payload := json.RawMessage(`{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1"
	}`)
	if err := h.sched.OnClaim(t.Context(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	tok := taskToken(t, h, "task-1")
	code, body := h.call("GET", "/api/daemon/workspaces/ws-1/repos", tok, nil)
	if code != 200 {
		t.Fatalf("pre-C1 S-2 = %d: %s", code, body)
	}
	pre := decodeWorkspace(t, body)
	if _, has := pre["settings"]; has {
		t.Errorf("pre-C1 S-2 carries settings: %s", body)
	}
	if len(pre["repos"]) != 2 || string(pre["repos"]) != "[]" {
		t.Errorf("pre-C1 repos = %s, want an empty list", pre["repos"])
	}
	if n := h.upstream.reposCallCount(); n != 0 {
		t.Fatalf("pre-C1 C-22 calls = %d, want 0", n)
	}

	// C-1 seeds the carrier verbatim, unknown keys included.
	seedRepos := `[{"url":"https://example.invalid/a","ref":"main","x_repo_key":{"n":1}}]`
	seedSettings := `{"co_authored_by_enabled":false,"github_enabled":false,"x_unknown":{"keep":true}}`
	h.upstream.setRegisterWorkspace(seedRepos, "seed-v1", seedSettings)
	h.declare(registryDiscovery("omp"))

	// S-2 triggers C-22 (default 404 in this harness) and replays the seed.
	code, body = h.call("GET", "/api/daemon/workspaces/ws-1/repos", tok, nil)
	if code != 200 {
		t.Fatalf("S-2 = %d: %s", code, body)
	}
	if n := h.upstream.reposCallCount(); n != 1 {
		t.Fatalf("C-22 calls after one S-2 = %d, want 1", n)
	}
	seeded := decodeWorkspace(t, body)
	assertRawEqual(t, "S-2 repos", seeded["repos"], seedRepos)
	assertRawEqual(t, "S-2 settings", seeded["settings"], seedSettings)
	if string(seeded["repos_version"]) != `"seed-v1"` {
		t.Errorf("S-2 repos_version = %s", seeded["repos_version"])
	}

	// S-4 uses the same carrier (and does not refresh it).
	code, s4 := registerJobDaemon(t, h, "task-1", tok, `[{"name":"foreman-job","type":"omp"}]`)
	if code != 200 {
		t.Fatalf("S-4 = %d: %s", code, s4)
	}
	registered := decodeWorkspace(t, s4)
	assertRawEqual(t, "S-4 repos", registered["repos"], seedRepos)
	assertRawEqual(t, "S-4 settings", registered["settings"], seedSettings)
	if n := h.upstream.reposCallCount(); n != 1 {
		t.Errorf("S-4 triggered C-22: calls = %d, want 1", n)
	}

	// A successful C-22 replaces the whole carrier: new repos, new version,
	// and `{}` is a value that must appear as `{}`.
	nextRepos := `[{"url":"https://example.invalid/b"}]`
	h.upstream.setRepos(200, workspaceBody(nextRepos, "v2", json.RawMessage(`{}`)), nil)
	code, body = h.call("GET", "/api/daemon/workspaces/ws-1/repos", tok, nil)
	if code != 200 {
		t.Fatalf("S-2 after refresh = %d: %s", code, body)
	}
	refreshed := decodeWorkspace(t, body)
	assertRawEqual(t, "refreshed repos", refreshed["repos"], nextRepos)
	if string(refreshed["settings"]) != `{}` {
		t.Errorf("settings {} = %s, want the empty object verbatim", refreshed["settings"])
	}
	if string(refreshed["repos_version"]) != `"v2"` {
		t.Errorf("repos_version = %s", refreshed["repos_version"])
	}

	// An upstream answer without settings clears the old value (the key is
	// absent, not null and not the previous object).
	h.upstream.setRepos(200, workspaceBody(nextRepos, "v3", nil), nil)
	code, body = h.call("GET", "/api/daemon/workspaces/ws-1/repos", tok, nil)
	if code != 200 {
		t.Fatalf("S-2 without settings = %d: %s", code, body)
	}
	cleared := decodeWorkspace(t, body)
	if _, has := cleared["settings"]; has {
		t.Errorf("settings key survived an upstream omission: %s", body)
	}

	// An explicit null is a value: it appears as null.
	h.upstream.setRepos(200, workspaceBody(nextRepos, "v4", json.RawMessage(`null`)), nil)
	code, body = h.call("GET", "/api/daemon/workspaces/ws-1/repos", tok, nil)
	if code != 200 {
		t.Fatalf("S-2 with null settings = %d: %s", code, body)
	}
	nulled := decodeWorkspace(t, body)
	if string(nulled["settings"]) != "null" {
		t.Errorf("settings null = %s, want an explicit null", nulled["settings"])
	}

	// A failed C-22 replays the last successful snapshot and records the
	// stale-replay metric; the checkout answer is still a 200.
	before := workspaceRefreshCount(h, "stale_replay")
	h.upstream.setRepos(500, `{"error":"boom"}`, nil)
	code, body = h.call("GET", "/api/daemon/workspaces/ws-1/repos", tok, nil)
	if code != 200 {
		t.Fatalf("S-2 after a failed refresh = %d: %s", code, body)
	}
	replayed := decodeWorkspace(t, body)
	assertRawEqual(t, "replayed repos", replayed["repos"], nextRepos)
	if string(replayed["settings"]) != "null" {
		t.Errorf("replayed settings = %s, want the last successful null", replayed["settings"])
	}
	if got := workspaceRefreshCount(h, "stale_replay"); got != before+1 {
		t.Errorf("stale_replay metric = %d, want %d", got, before+1)
	}
}

func TestTechWorkspaceState01RefreshIsSingleFlight(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.setRuntimeID("omp", "", "rid-omp")
	h.upstream.setRegisterWorkspace(`[]`, "v1", `{"github_enabled":false}`)
	h.declare(registryDiscovery("omp"))

	payload := json.RawMessage(`{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1"
	}`)
	if err := h.sched.OnClaim(t.Context(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	tok := taskToken(t, h, "task-1")

	// One S-2 primes the carrier; the single-flight measurement starts after.
	h.call("GET", "/api/daemon/workspaces/ws-1/repos", tok, nil)
	baseline := h.upstream.reposCallCount()

	gate := make(chan struct{})
	repos := `[{"url":"https://example.invalid/c"}]`
	h.upstream.setRepos(200, workspaceBody(repos, "v9", json.RawMessage(`{"github_enabled":true}`)), gate)

	const callers = 8
	type answer struct {
		code int
		body []byte
		err  error
	}
	results := make(chan answer, callers)
	for range callers {
		go func() {
			code, body, err := callAsync(h, "GET", "/api/daemon/workspaces/ws-1/repos", tok, nil)
			results <- answer{code: code, body: body, err: err}
		}()
	}
	// Wait until the single upstream call is in flight, then let it answer.
	waitFor(t, "the C-22 refresh to be in flight", 5*time.Second, func() bool {
		return h.upstream.reposCallCount() == baseline+1
	})
	time.Sleep(50 * time.Millisecond) // let the followers join the in-flight call
	close(gate)

	for range callers {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatalf("concurrent S-2: %v", got.err)
			}
			if got.code != 200 {
				t.Fatalf("concurrent S-2 = %d: %s", got.code, got.body)
			}
			resp := decodeWorkspace(t, got.body)
			assertRawEqual(t, "single-flight repos", resp["repos"], repos)
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent S-2 did not answer")
		}
	}
	if n := h.upstream.reposCallCount(); n != baseline+1 {
		t.Errorf("C-22 calls = %d, want exactly one for %d concurrent S-2 requests", n-baseline, callers)
	}
	if n := workspaceRefreshCount(h, "success"); n == 0 {
		t.Error("a successful refresh must record its metric")
	}
}

// registryDiscovery is the minimal startup discovery of one builtin runtime.
func registryDiscovery(provider string) registry.Discovery {
	return registry.Discovery{
		CLIVersion: "0.6.1",
		Runtimes:   []registry.DiscoveredRuntime{{Name: "foreman-" + provider, Type: provider, Version: "0.6.1"}},
	}
}

// assertRawEqual compares two JSON documents semantically.
func assertRawEqual(t *testing.T, what string, got json.RawMessage, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("%s: decode got: %v (%s)", what, err, got)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("%s: decode want: %v (%s)", what, err, want)
	}
	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)
	if !bytes.Equal(gb, wb) {
		t.Errorf("%s = %s, want %s (semantic)", what, got, want)
	}
}
