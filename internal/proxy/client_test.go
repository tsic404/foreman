package proxy

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/scheduler"
)

// recordedRequest captures one inbound call at the stub upstream.
type recordedRequest struct {
	Method      string
	Path        string
	EscapedPath string
	RawQuery    string
	Body        []byte
	Header      http.Header
}

// stubUpstream is the controllable fake server (protocol-stub spec).
type stubUpstream struct {
	mu     sync.Mutex
	reqs   []recordedRequest
	routes map[string]func(w http.ResponseWriter, r *http.Request, body []byte)
}

func newStubUpstream(t *testing.T) (*httptest.Server, *stubUpstream) {
	t.Helper()
	stub := &stubUpstream{routes: map[string]func(w http.ResponseWriter, r *http.Request, body []byte){}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 0)
		if r.Body != nil {
			raw, err := io.ReadAll(r.Body)
			if err == nil {
				body = raw
			}
		}
		stub.mu.Lock()
		stub.reqs = append(stub.reqs, recordedRequest{r.Method, r.URL.Path, r.URL.EscapedPath(), r.URL.RawQuery, body, r.Header.Clone()})
		stub.mu.Unlock()
		if h, ok := stub.routes[r.Method+" "+r.URL.Path]; ok {
			h(w, r, body)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not found"}`))
	}))
	t.Cleanup(srv.Close)
	return srv, stub
}

func (s *stubUpstream) on(method, path string, h func(w http.ResponseWriter, r *http.Request, body []byte)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.routes[method+" "+path] = h
}

func (s *stubUpstream) calls(method, path string) []recordedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []recordedRequest
	for _, r := range s.reqs {
		if r.Method == method && r.Path == path {
			out = append(out, r)
		}
	}
	return out
}

func (s *stubUpstream) all() []recordedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedRequest(nil), s.reqs...)
}

func writeJSONStub(w http.ResponseWriter, code int, v any) {
	raw, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}

func testConfig(serverURL string) Config {
	return Config{
		ServerURL:            serverURL,
		WorkspaceID:          "ws-1",
		DaemonID:             "foreman",
		Version:              "0.1.0",
		RuntimeName:          "foreman-omp",
		RuntimeType:          "omp",
		RuntimeVersion:       "18.0.0",
		ClaimInterval:        time.Second,
		ClaimIntervalIdle:    time.Second,
		LeaseRefreshInterval: time.Second,
		WSEnabled:            true,
	}
}

// stubRegister answers C1 with the given runtime id.
func stubRegister(stub *stubUpstream, runtimeID string) {
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, r *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{
			"runtimes": []map[string]string{{
				"id": runtimeID, "name": "foreman-omp", "provider": "omp", "status": "online",
			}},
			"repos":         []map[string]string{{"url": "https://github.com/tsic404/foreman"}},
			"repos_version": "abc123",
			"settings":      nil,
		})
	})
}

func TestRegisterSendsContractBody(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	c := NewClient(testConfig(srv.URL), "mdt_test")

	if err := c.Register(t.Context()); err != nil {
		t.Fatalf("Register: %v", err)
	}
	calls := stub.calls("POST", "/api/daemon/register")
	if len(calls) != 1 {
		t.Fatalf("register calls = %d, want 1", len(calls))
	}
	req := calls[0]
	if got := req.Header.Get("X-Client-Capabilities"); got != "skill-bundles-v1,coalesced-comments-v1,rpc-v1" {
		t.Errorf("capabilities = %q", got)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer mdt_test" {
		t.Errorf("authorization = %q", got)
	}
	if got := req.Header.Get("X-Client-Platform"); got != "daemon" {
		t.Errorf("platform = %q", got)
	}
	if got := req.Header.Get("X-Client-OS"); got != "linux" {
		t.Errorf("os = %q", got)
	}
	if got := req.Header.Get("X-Client-Version"); got != "0.1.0" {
		t.Errorf("version = %q", got)
	}
	var body map[string]any
	if err := json.Unmarshal(req.Body, &body); err != nil {
		t.Fatalf("body: %v", err)
	}
	if body["workspace_id"] != "ws-1" || body["daemon_id"] != "foreman" {
		t.Errorf("identity fields wrong: %v", body)
	}
	if _, hasProvider := body["provider"]; hasProvider {
		t.Errorf("request must use type, not provider: %v", body)
	}
	legacy, _ := body["legacy_daemon_ids"].([]any)
	if legacy == nil || len(legacy) != 0 {
		t.Errorf("legacy_daemon_ids must be an empty array: %v", body["legacy_daemon_ids"])
	}
	runtimes, _ := body["runtimes"].([]any)
	if len(runtimes) != 1 {
		t.Fatalf("runtimes = %v", body["runtimes"])
	}
	rt := runtimes[0].(map[string]any)
	if rt["type"] != "omp" || rt["name"] != "foreman-omp" || rt["status"] != "online" {
		t.Errorf("runtime payload wrong: %v", rt)
	}
	if c.RuntimeID() != "rid-1" {
		t.Errorf("cached runtime id = %q", c.RuntimeID())
	}
	repos, version := c.Registration()
	if version != "abc123" || len(repos) == 0 {
		t.Errorf("registration cache = %s %q", repos, version)
	}
}

func TestReRegisterNotifiesRuntimeSetAndRecoversOrphans(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	c := NewClient(testConfig(srv.URL), "mdt_test")
	if err := c.Register(t.Context()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// The runtime row gets deleted server-side; the next rid-dependent call
	// re-registers and retries with the fresh id.
	stub.on("POST", "/api/daemon/runtimes/rid-1/recover-orphans", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]int{"orphaned": 0, "retried": 0})
	})
	stub.on("POST", "/api/daemon/runtimes/rid-2/recover-orphans", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]int{"orphaned": 1, "retried": 1})
	})
	stub.on("POST", "/api/daemon/heartbeat", func(w http.ResponseWriter, _ *http.Request, body []byte) {
		var b map[string]any
		_ = json.Unmarshal(body, &b)
		if b["runtime_id"] == "rid-1" {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":"runtime not found"}`))
			return
		}
		writeJSONStub(w, 200, map[string]string{"status": "ok"})
	})
	stubRegister(stub, "rid-2")

	if err := c.Heartbeat(t.Context()); err != nil {
		t.Fatalf("Heartbeat after runtime loss: %v", err)
	}
	if c.RuntimeID() != "rid-2" {
		t.Fatalf("runtime id = %q, want rid-2", c.RuntimeID())
	}
	select {
	case <-c.RuntimeSet():
	default:
		t.Fatal("runtime-set change not signaled")
	}
	if got := len(stub.calls("POST", "/api/daemon/runtimes/rid-2/recover-orphans")); got != 1 {
		t.Errorf("C15 recover-orphans calls = %d, want 1", got)
	}
	// The retry used the fresh id.
	heartbeats := stub.calls("POST", "/api/daemon/heartbeat")
	if len(heartbeats) != 2 {
		t.Fatalf("heartbeat calls = %d, want 2", len(heartbeats))
	}
	var last map[string]any
	_ = json.Unmarshal(heartbeats[1].Body, &last)
	if last["runtime_id"] != "rid-2" {
		t.Errorf("retry used runtime_id %v", last["runtime_id"])
	}
}

func TestClaimTasksRequestShape(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/daemon/tasks/claim", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{"tasks": []json.RawMessage{json.RawMessage(`{"id":"t1"}`)}})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	if _, err := c.ClaimTasks(t.Context(), 0); err != nil {
		t.Fatalf("ClaimTasks(0): %v", err)
	}
	if got := len(stub.calls("POST", "/api/daemon/tasks/claim")); got != 0 {
		t.Fatalf("max_tasks=0 must not call the server, got %d calls", got)
	}

	tasks, err := c.ClaimTasks(t.Context(), 5)
	if err != nil {
		t.Fatalf("ClaimTasks: %v", err)
	}
	if len(tasks) != 1 || string(tasks[0]) != `{"id":"t1"}` {
		t.Fatalf("tasks = %s", tasks)
	}
	calls := stub.calls("POST", "/api/daemon/tasks/claim")
	var body map[string]any
	_ = json.Unmarshal(calls[0].Body, &body)
	if body["daemon_id"] != "foreman" || body["max_tasks"] != float64(5) {
		t.Errorf("claim body = %v", body)
	}
	ids, _ := body["runtime_ids"].([]any)
	if len(ids) != 1 || ids[0] != "rid-1" {
		t.Errorf("runtime_ids = %v", body["runtime_ids"])
	}
}

func TestClaimTasks401Recorded(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/daemon/tasks/claim", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"error":"token invalid"}`))
	})
	var metrics fakeMetrics
	c := NewClient(testConfig(srv.URL), "mdt_test", WithMetrics(&metrics))

	_, err := c.ClaimTasks(t.Context(), 1)
	var se *StatusError
	if !errors.As(err, &se) || se.Code != 401 {
		t.Fatalf("err = %v, want 401 StatusError", err)
	}
	if len(metrics.claimErrors) != 1 || metrics.claimErrors[0] != "401" {
		t.Errorf("claim error metric = %v", metrics.claimErrors)
	}
}

func TestPrepareLeaseSemantics(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	codes := map[string]int{"task-ok": 200, "task-not-ready": 400, "task-gone": 404}
	stub.on("POST", "/api/daemon/runtimes/rid-1/tasks/task-ok/prepare-lease", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, codes["task-ok"], map[string]string{"status": "ok"})
	})
	stub.on("POST", "/api/daemon/runtimes/rid-1/tasks/task-not-ready/prepare-lease", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(codes["task-not-ready"])
		_, _ = w.Write([]byte(`{"error":"not renewable"}`))
	})
	stub.on("POST", "/api/daemon/runtimes/rid-1/tasks/task-gone/prepare-lease", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(codes["task-gone"])
		_, _ = w.Write([]byte(`{"error":"task not found"}`))
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	ready, err := c.PrepareLease(t.Context(), "task-ok")
	if err != nil || !ready {
		t.Errorf("200: ready=%v err=%v, want ready=true", ready, err)
	}
	ready, err = c.PrepareLease(t.Context(), "task-not-ready")
	if err != nil || ready {
		t.Errorf("400: ready=%v err=%v, want ready=false without error (semantic)", ready, err)
	}
	_, err = c.PrepareLease(t.Context(), "task-gone")
	if !errors.Is(err, scheduler.ErrTaskNotFound) {
		t.Errorf("404: err=%v, want ErrTaskNotFound", err)
	}
}

func TestForwardTerminalRetryBudget(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	attempts := 0
	stub.on("POST", "/api/daemon/tasks/t1/complete", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(503)
			return
		}
		writeJSONStub(w, 200, map[string]string{"id": "t1"})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test", WithTerminalRetryWaits([]time.Duration{0, 0, 0, 0, 0, 0}))

	code, _, err := c.Forward(t.Context(), scheduler.EPComplete, "t1", []byte(`{"result":"ok"}`))
	if err != nil || code != 200 {
		t.Fatalf("Forward = %d, %v", code, err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestForwardTerminalExhaustion(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/daemon/tasks/t1/fail", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(503)
	})
	c := NewClient(testConfig(srv.URL), "mdt_test", WithTerminalRetryWaits([]time.Duration{0, 0, 0, 0, 0, 0}))

	_, _, err := c.Forward(t.Context(), scheduler.EPFail, "t1", []byte(`{"error":"x"}`))
	if !errors.Is(err, ErrTerminalUndelivered) {
		t.Fatalf("err = %v, want ErrTerminalUndelivered", err)
	}
	if got := len(stub.calls("POST", "/api/daemon/tasks/t1/fail")); got != 6 {
		t.Fatalf("attempts = %d, want 6 (full §4 budget)", got)
	}
}

func TestForwardTerminal4xxIsPermanent(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/daemon/tasks/t1/complete", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":"bad request"}`))
	})
	c := NewClient(testConfig(srv.URL), "mdt_test", WithTerminalRetryWaits([]time.Duration{0, 0, 0, 0, 0, 0}))

	code, _, err := c.Forward(t.Context(), scheduler.EPComplete, "t1", []byte(`{}`))
	if err != nil || code != 400 {
		t.Fatalf("Forward = %d, %v", code, err)
	}
	if got := len(stub.calls("POST", "/api/daemon/tasks/t1/complete")); got != 1 {
		t.Fatalf("attempts = %d, want 1 (4xx is permanent)", got)
	}
}

func TestForwardNonTerminalNotRetried(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/daemon/tasks/t1/progress", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(503)
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	code, _, err := c.Forward(t.Context(), scheduler.EPProgress, "t1", []byte(`{"msg":"x"}`))
	if err != nil || code != 503 {
		t.Fatalf("Forward = %d, %v", code, err)
	}
	if got := len(stub.calls("POST", "/api/daemon/tasks/t1/progress")); got != 1 {
		t.Fatalf("attempts = %d, want 1 (the daemon owns progress retries)", got)
	}
}

func TestPassthroughIsVerbatim(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("POST", "/api/issues/42/comments", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"id":7}`))
	})
	var metrics fakeMetrics
	c := NewClient(testConfig(srv.URL), "mdt_test", WithMetrics(&metrics))

	code, resp, err := c.Passthrough(t.Context(), "POST", "/api/issues/42/comments", "b=2&a=1", []byte(`{"body":"hi"}`), "Bearer mat_tasktoken")
	if err != nil || code != 201 {
		t.Fatalf("Passthrough = %d, %v", code, err)
	}
	if string(resp) != `{"id":7}` {
		t.Errorf("response = %s", resp)
	}
	calls := stub.calls("POST", "/api/issues/42/comments")
	if len(calls) != 1 {
		t.Fatalf("calls = %d", len(calls))
	}
	got := calls[0]
	if got.Header.Get("Authorization") != "Bearer mat_tasktoken" {
		t.Errorf("authorization rewritten: %q", got.Header.Get("Authorization"))
	}
	if got.Header.Get("X-Client-Capabilities") != "" {
		t.Errorf("daemon identity headers must not be injected: %q", got.Header.Get("X-Client-Capabilities"))
	}
	// The query is forwarded byte-verbatim (no decode/re-encode reorder).
	if got.RawQuery != "b=2&a=1" || string(got.Body) != `{"body":"hi"}` {
		t.Errorf("query/body not verbatim: %q %s", got.RawQuery, got.Body)
	}
	// The S29 forward is metered under the agent-api endpoint label.
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if len(metrics.forwards) != 1 || metrics.forwards[0] != EndpointAgentAPI {
		t.Errorf("forward metric = %v, want [%q]", metrics.forwards, EndpointAgentAPI)
	}
}

func TestPassthroughKeepsEscapedPathSegments(t *testing.T) {
	srv, stub := newStubUpstream(t)
	// The stub router keys on the decoded path; the assertion is on the
	// escaped form.
	stub.on("GET", "/api/issues/a/b", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"ok": "1"})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	code, _, err := c.Passthrough(t.Context(), "GET", "/api/issues/a%2Fb", "", nil, "Bearer mat_tasktoken")
	if err != nil || code != 200 {
		t.Fatalf("Passthrough = %d, %v", code, err)
	}
	calls := stub.calls("GET", "/api/issues/a/b")
	if len(calls) != 1 || calls[0].EscapedPath != "/api/issues/a%2Fb" {
		t.Fatalf("escaped path not preserved: %+v", calls)
	}
}

func TestHeartbeatSurfacesNon2xx(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/daemon/heartbeat", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"error":"token invalid"}`))
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	err := c.Heartbeat(t.Context())
	var se *StatusError
	if !errors.As(err, &se) || se.Code != 401 {
		t.Fatalf("Heartbeat = %v, want a 401 StatusError (the loop's backoff depends on it)", err)
	}
}

func TestRebuildSignalSurvivesFailedReregister(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	c := NewClient(testConfig(srv.URL), "mdt_test")
	if err := c.Register(t.Context()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// The runtime row dies; the first re-register fails (server trouble).
	stub.on("POST", "/api/daemon/heartbeat", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"error":"runtime not found"}`))
	})
	var failRegister atomic.Bool
	failRegister.Store(true)
	var orphanCalls atomic.Int32
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, r *http.Request, body []byte) {
		if failRegister.Load() {
			w.WriteHeader(503)
			return
		}
		writeJSONStub(w, 200, map[string]any{
			"runtimes": []map[string]string{{"id": "rid-2", "name": "foreman-omp", "provider": "omp", "status": "online"}},
			"repos":    []map[string]string{}, "repos_version": "v2", "settings": nil,
		})
	})
	stub.on("POST", "/api/daemon/runtimes/rid-2/recover-orphans", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		orphanCalls.Add(1)
		writeJSONStub(w, 200, map[string]int{"orphaned": 0, "retried": 0})
	})

	if err := c.Heartbeat(t.Context()); err == nil {
		t.Fatal("heartbeat must fail while re-registration fails")
	}
	// The register failure cleared the cached id but kept the rebuild fact:
	// the next successful register must still fire C15 + the runtime-set
	// signal.
	failRegister.Store(false)
	if err := c.Register(t.Context()); err != nil {
		t.Fatalf("second Register: %v", err)
	}
	if c.RuntimeID() != "rid-2" {
		t.Fatalf("runtime id = %q", c.RuntimeID())
	}
	if got := orphanCalls.Load(); got != 1 {
		t.Errorf("C15 calls = %d, want 1 (runtime was rebuilt)", got)
	}
	select {
	case <-c.RuntimeSet():
	default:
		t.Error("runtime-set signal lost across a failed re-register")
	}
}

func TestTerminalRetryBudgetMatchesContract(t *testing.T) {
	want := []time.Duration{0, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, 64 * time.Second}
	if !slices.Equal(terminalRetryWaits, want) {
		t.Fatalf("terminalRetryWaits = %v, want the §4 budget %v", terminalRetryWaits, want)
	}
}

func TestTaskStatus(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("GET", "/api/daemon/tasks/t1/status", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "running"})
	})
	stub.on("GET", "/api/daemon/tasks/gone/status", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"error":"task not found"}`))
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	st, err := c.TaskStatus(t.Context(), "t1")
	if err != nil || st != "running" {
		t.Fatalf("TaskStatus = %q, %v", st, err)
	}
	if _, err := c.TaskStatus(t.Context(), "gone"); !errors.Is(err, scheduler.ErrTaskNotFound) {
		t.Fatalf("gone: err = %v, want ErrTaskNotFound", err)
	}
}

func TestSkillBundlesUsesRealRuntimeAndForemanCredential(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-real")
	stub.on("POST", "/api/daemon/runtimes/rid-real/tasks/t1/skill-bundles/resolve", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"bundle": "data"})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	code, resp, err := c.ResolveSkillBundles(t.Context(), "t1", []byte(`{"skills":["a"]}`))
	if err != nil || code != 200 {
		t.Fatalf("ResolveSkillBundles = %d, %v", code, err)
	}
	if string(resp) != `{"bundle":"data"}` {
		t.Errorf("resp = %s", resp)
	}
	calls := stub.calls("POST", "/api/daemon/runtimes/rid-real/tasks/t1/skill-bundles/resolve")
	if len(calls) != 1 {
		t.Fatalf("calls = %d (path must carry the real runtime id)", len(calls))
	}
	if got := calls[0].Header.Get("Authorization"); got != "Bearer mdt_test" {
		t.Errorf("C19 must use Foreman's credential, got %q", got)
	}
	if string(calls[0].Body) != `{"skills":["a"]}` {
		t.Errorf("body not verbatim: %s", calls[0].Body)
	}
}

func TestPreflightCoversC1C16C17C18(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("GET", "/api/daemon/workspaces", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, []map[string]string{{"id": "ws-1", "name": "foreman-ws"}})
	})
	// C18 404 is tolerated.
	stub.on("POST", "/api/tokens/current/renew", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{"expires_at": time.Now().Add(72 * time.Hour), "renewed": true})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	if err := c.Preflight(t.Context()); err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	_, name := c.Workspace()
	if name != "foreman-ws" {
		t.Errorf("workspace name = %q", name)
	}
	if got := len(stub.calls("POST", "/api/tokens/current/renew")); got != 1 {
		t.Errorf("C16 renew calls = %d", got)
	}
	if got := len(stub.calls("GET", "/api/daemon/workspaces/ws-1/runtime-profiles")); got != 1 {
		t.Errorf("C18 calls = %d", got)
	}
}

func TestPreflightFailsWhenWorkspaceInvisible(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("GET", "/api/daemon/workspaces", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, []map[string]string{{"id": "other-ws", "name": "x"}})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")
	if err := c.Preflight(t.Context()); err == nil {
		t.Fatal("Preflight must fail when the configured workspace is not visible")
	}
}

func TestDeregister(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/daemon/deregister", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "ok"})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")
	if err := c.Register(t.Context()); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := c.Deregister(t.Context()); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	calls := stub.calls("POST", "/api/daemon/deregister")
	var body map[string]any
	_ = json.Unmarshal(calls[0].Body, &body)
	ids, _ := body["runtime_ids"].([]any)
	if len(ids) != 1 || ids[0] != "rid-1" {
		t.Errorf("deregister body = %v", body)
	}
}

func TestGCCheckForwardedVerbatim(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("GET", "/api/daemon/tasks/t1/gc-check", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "completed", "completed_at": "2026-10-05T00:00:00Z"})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")
	code, resp, err := c.GCCheck(t.Context(), "t1")
	if err != nil || code != 200 {
		t.Fatalf("GCCheck = %d, %v", code, err)
	}
	var body map[string]string
	_ = json.Unmarshal(resp, &body)
	if body["status"] != "completed" || body["completed_at"] == "" {
		t.Errorf("gc-check = %v", body)
	}
}

// fakeMetrics records the proxy metrics seam.
type fakeMetrics struct {
	mu          sync.Mutex
	forwards    []string
	claimErrors []string
	authFails   []string
	wsConnected map[string]int
	wsReconnect []string
}

func (m *fakeMetrics) Forward(endpoint string, _ int, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.forwards = append(m.forwards, endpoint)
}

func (m *fakeMetrics) ClaimError(code string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.claimErrors = append(m.claimErrors, code)
}

func (m *fakeMetrics) AuthFailure(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.authFails = append(m.authFails, reason)
}

func (m *fakeMetrics) WSConnect(side string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wsConnected == nil {
		m.wsConnected = map[string]int{}
	}
	m.wsConnected[side]++
}

func (m *fakeMetrics) WSDisconnect(side string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wsConnected == nil {
		m.wsConnected = map[string]int{}
	}
	m.wsConnected[side]--
}

func (m *fakeMetrics) WSReconnect(side string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.wsReconnect = append(m.wsReconnect, side)
}

// wsCount reads the current connection gauge for a side (locked).
func (m *fakeMetrics) wsCount(side string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.wsConnected[side]
}

// reconnectCount reads the reconnect counter for a side (locked).
func (m *fakeMetrics) reconnectCount(side string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, s := range m.wsReconnect {
		if s == side {
			n++
		}
	}
	return n
}
