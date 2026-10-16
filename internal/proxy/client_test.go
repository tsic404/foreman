package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// recordedRequest captures one inbound call at the stub upstream.
type recordedRequest struct {
	Method        string
	Path          string
	EscapedPath   string
	RawQuery      string
	Body          []byte
	Header        http.Header
	Host          string
	ContentLength int64
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
		stub.reqs = append(stub.reqs, recordedRequest{r.Method, r.URL.Path, r.URL.EscapedPath(), r.URL.RawQuery, body, r.Header.Clone(), r.Host, r.ContentLength})
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
		ClaimInterval:        time.Second,
		ClaimIntervalIdle:    time.Second,
		LeaseRefreshInterval: time.Second,
		WSEnabled:            true,
		ModelCatalogEnabled:  true,
		ModelCatalogTimeout:  45 * time.Second,
		ModelCatalogCacheTTL: 6 * time.Hour,
	}
}

// testDiscovery is the startup discovery the client tests declare: one omp
// runtime, exactly what the startup probe would report (ADR-025).
func testDiscovery() registry.Discovery {
	return registry.Discovery{
		CLIVersion: "18.0.0",
		Runtimes: []registry.DiscoveredRuntime{
			{Name: "foreman-omp", Type: "omp", Version: "18.0.0"},
		},
	}
}

// registerDeclared performs the C1 declaration the startup orchestration hands
// over (the client never registers without one).
func registerDeclared(t *testing.T, c *Client) {
	t.Helper()
	if err := c.RegisterWithDiscovery(t.Context(), testDiscovery()); err != nil {
		t.Fatalf("RegisterWithDiscovery: %v", err)
	}
	drainChanges(c)
}

// drainChanges consumes the runtime-set notifications already queued (the
// appearance of the declared set), so a test observes only later moves.
func drainChanges(c *Client) {
	for {
		select {
		case <-c.Changes():
		default:
			return
		}
	}
}

// firstRuntimeID is the single declared runtime id ("" when unregistered).
func firstRuntimeID(c *Client) string {
	ids := c.RuntimeIDs()
	if len(ids) == 0 {
		return ""
	}
	return ids[0]
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

	registerDeclared(t, c)
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
	// The request is already past the probe's registration, so the header
	// carries the template CLI's version — the same value as cli_version
	// (§1.1 C1 的取值来源与声明域, AC-37).
	if got := req.Header.Get("X-Client-Version"); got != "18.0.0" {
		t.Errorf("version = %q, want the discovery's cli_version", got)
	}
	if body := string(req.Body); !strings.Contains(body, `"cli_version":"18.0.0"`) {
		t.Errorf("cli_version must come from the startup discovery: %s", body)
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
	if got := firstRuntimeID(c); got != "rid-1" {
		t.Errorf("cached runtime id = %q", got)
	}
	if got := c.CLIVersion(); got != "18.0.0" {
		t.Errorf("X-Client-Version after C1 = %q, want the discovery's cli_version", got)
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
	registerDeclared(t, c)

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

	if _, err := c.Heartbeat(t.Context(), "rid-1"); err != nil {
		t.Fatalf("Heartbeat after runtime loss: %v", err)
	}
	if got := firstRuntimeID(c); got != "rid-2" {
		t.Fatalf("runtime id = %q, want rid-2", got)
	}
	select {
	case change := <-c.Changes():
		if change.Provider != "omp" || change.OldID != "rid-1" || change.NewID != "rid-2" {
			t.Fatalf("runtime change = %+v", change)
		}
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
	registerDeclared(t, c)

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
	registerDeclared(t, c)

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
	registerDeclared(t, c)

	ready, err := c.PrepareLease(t.Context(), "rid-1", "task-ok")
	if err != nil || !ready {
		t.Errorf("200: ready=%v err=%v, want ready=true", ready, err)
	}
	ready, err = c.PrepareLease(t.Context(), "rid-1", "task-not-ready")
	if err != nil || ready {
		t.Errorf("400: ready=%v err=%v, want ready=false without error (semantic)", ready, err)
	}
	_, err = c.PrepareLease(t.Context(), "rid-1", "task-gone")
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

func TestForwardStartTranslatesJobRuntimeToRealRuntime(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-real")
	stub.on("POST", "/api/daemon/tasks/t1/start", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "running"})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test",
		WithTaskRuntime(func(string) string { return "rid-real" }))
	registerDeclared(t, c)

	// The daemon names the job runtime it was handed at S6; the claim the
	// server holds belongs to the real runtime id.
	body := []byte(`{"capabilities":["x"],"runtime_id":"job-runtime","dispatched_at":"2026-10-12T10:54:12.016974Z"}`)
	code, _, err := c.Forward(t.Context(), scheduler.EPStart, "t1", body)
	if err != nil || code != 200 {
		t.Fatalf("Forward = %d, %v", code, err)
	}
	calls := stub.calls("POST", "/api/daemon/tasks/t1/start")
	if len(calls) != 1 {
		t.Fatalf("calls = %d", len(calls))
	}
	var got struct {
		Capabilities []string `json:"capabilities"`
		RuntimeID    string   `json:"runtime_id"`
		DispatchedAt string   `json:"dispatched_at"`
	}
	if err := json.Unmarshal(calls[0].Body, &got); err != nil {
		t.Fatalf("body not JSON: %s (%v)", calls[0].Body, err)
	}
	if got.RuntimeID != "rid-real" {
		t.Errorf("runtime_id = %q, want the real runtime id (the server's claim carries it)", got.RuntimeID)
	}
	if got.DispatchedAt != "2026-10-12T10:54:12.016974Z" {
		t.Errorf("dispatched_at = %q, want it verbatim (the claim generation)", got.DispatchedAt)
	}
	if len(got.Capabilities) != 1 || got.Capabilities[0] != "x" {
		t.Errorf("capabilities = %v, want the daemon's list verbatim", got.Capabilities)
	}
}

func TestForwardStartWithoutRuntimeIdentityIsVerbatim(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-real")
	stub.on("POST", "/api/daemon/tasks/t1/start", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "running"})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, c)

	// A start that names no runtime keeps the legacy shape byte-for-byte:
	// Foreman invents no field the daemon did not send.
	if _, _, err := c.Forward(t.Context(), scheduler.EPStart, "t1", []byte(`{}`)); err != nil {
		t.Fatalf("Forward: %v", err)
	}
	calls := stub.calls("POST", "/api/daemon/tasks/t1/start")
	if len(calls) != 1 || string(calls[0].Body) != `{}` {
		t.Fatalf("body = %s, want {} verbatim", calls[0].Body)
	}
}

// s29ProbeHeader is the caller header set the S29 probes send: one entry per
// field the contract rules on, multi-valued where order matters.
func s29ProbeHeader(contentType string) http.Header {
	return http.Header{
		"Authorization": {"Bearer mat_tasktoken"},
		"Content-Type":  {contentType},
		"Accept":        {"application/vnd.multica+json"},
		"User-Agent":    {"s29-probe/1.0"},
		"X-Probe-Multi": {"a", "b"},
	}
}

// multipartProbeBody is a byte-exact multipart/form-data body; the boundary
// is only meaningful next to the matching Content-Type.
func multipartProbeBody(boundary string) []byte {
	return []byte("--" + boundary + "\r\nContent-Disposition: form-data; name=\"f\"; filename=\"p.txt\"\r\n" +
		"Content-Type: text/plain\r\n\r\nhi\r\n--" + boundary + "--\r\n")
}

// singleCall returns the stub's only recorded call for the route.
func singleCall(t *testing.T, stub *stubUpstream, method, path string) recordedRequest {
	t.Helper()
	calls := stub.calls(method, path)
	if len(calls) != 1 {
		t.Fatalf("upstream calls to %s = %d, want 1", path, len(calls))
	}
	return calls[0]
}

// upstreamFields is a received header set with the field the transport
// generates for the forwarded body set aside: Content-Length belongs to the
// proxy's own connection (§1.2 exclusion set), so the caller's set is
// compared without it.
func upstreamFields(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, vv := range h {
		if k == "Content-Length" {
			continue
		}
		out[k] = append([]string(nil), vv...)
	}
	return out
}

// upstreamAuthority is the Host the stub upstream must see: the proxy's own
// address is never forwarded (§1.2 exclusion set).
func upstreamAuthority(srv *httptest.Server) string {
	return srv.URL[len("http://"):]
}

func TestPassthroughIsVerbatim(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("POST", "/api/upload-file", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"id":7}`))
	})
	var metrics fakeMetrics
	c := NewClient(testConfig(srv.URL), "mdt_test", WithMetrics(&metrics))

	body := multipartProbeBody("----s29probe")
	header := s29ProbeHeader("multipart/form-data; boundary=----s29probe")
	code, respHeader, resp, err := c.Passthrough(t.Context(), "POST", "/api/upload-file", "b=2&a=1", header, body)
	if err != nil || code != 201 {
		t.Fatalf("Passthrough = %d, %v", code, err)
	}
	if string(resp) != `{"id":7}` || respHeader.Get("Content-Type") != "application/json" {
		t.Errorf("response = %v %s, want the upstream verdict as-is", respHeader, resp)
	}
	got := singleCall(t, stub, "POST", "/api/upload-file")
	// The upstream sees exactly the caller's set: no field picked out, none
	// added (no client-library User-Agent/Accept-Encoding, no daemon identity
	// headers), values and their order untouched.
	if extra := upstreamFields(got.Header); len(extra) != len(header) {
		t.Errorf("upstream fields = %v, want exactly %v", extra, header)
	}
	for field, want := range header {
		if gotVals := got.Header.Values(field); !slices.Equal(gotVals, want) {
			t.Errorf("upstream %s = %v, want %v", field, gotVals, want)
		}
	}
	if string(got.Body) != string(body) || got.ContentLength != int64(len(body)) {
		t.Errorf("body = %d bytes (CL %d), want %d bytes", len(got.Body), got.ContentLength, len(body))
	}
	if got.RawQuery != "b=2&a=1" {
		t.Errorf("query = %q, want b=2&a=1", got.RawQuery)
	}
	if got.Host != upstreamAuthority(srv) {
		t.Errorf("upstream Host = %q, want the target authority %q", got.Host, upstreamAuthority(srv))
	}
	// The S29 forward is metered under the agent-api endpoint label.
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if len(metrics.forwards) != 1 || metrics.forwards[0] != EndpointAgentAPI {
		t.Errorf("forward metric = %v, want [%q]", metrics.forwards, EndpointAgentAPI)
	}
}

func TestForwardableHeaderAppliesExclusionSet(t *testing.T) {
	in := http.Header{
		"Accept":              {"application/json"},
		"Authorization":       {"Bearer mat_x"},
		"Connection":          {"X-Probe-Drop, keep-alive"},
		"Content-Length":      {"4096"},
		"Content-Type":        {`multipart/form-data; boundary=----s29probe`},
		"Host":                {"foreman.foreman.svc.cluster.local:8080"},
		"Keep-Alive":          {"timeout=5"},
		"Proxy-Authorization": {"Basic zzz"},
		"TE":                  {"trailers"},
		"Trailer":             {"X-Probe-Trailer"},
		"Transfer-Encoding":   {"chunked"},
		"Upgrade":             {"h2c"},
		"X-Probe-Drop":        {"nope"},
		"X-Probe-Multi":       {"a", "b"},
	}
	want := http.Header{
		"Accept":        {"application/json"},
		"Authorization": {"Bearer mat_x"},
		"Content-Type":  {`multipart/form-data; boundary=----s29probe`},
		"X-Probe-Multi": {"a", "b"},
	}
	got := forwardableHeader(in)
	if len(got) != len(want) {
		t.Fatalf("forwardable = %v, want exactly %v", got, want)
	}
	for field, wantVals := range want {
		if gotVals := got.Values(field); !slices.Equal(gotVals, wantVals) {
			t.Errorf("%s = %v, want %v", field, gotVals, wantVals)
		}
	}
	// The caller's set is not mutated on the way through.
	if !slices.Equal(in["Connection"], []string{"X-Probe-Drop, keep-alive"}) || in["Host"] == nil {
		t.Errorf("caller header set mutated: %v", in)
	}
}

func TestForwardableHeaderCanonicalizesNames(t *testing.T) {
	// Field-name case is not a fidelity item (§1.2 逐字段裁定): the copy
	// writes canonical names, so net/http's own lookups (User-Agent, framing
	// fields) see the same field the caller spelled differently.
	for _, tt := range []struct{ name, caller string }{
		{"upper", "User-Agent"},
		{"lower", "user-agent"},
		{"mixed", "uSeR-aGeNt"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := forwardableHeader(http.Header{
				tt.caller:       {"probe/1.0"},
				"content-type":  {"application/json"},
				"x-probe-multi": {"a", "b"},
			})
			want := http.Header{
				"User-Agent":    {"probe/1.0"},
				"Content-Type":  {"application/json"},
				"X-Probe-Multi": {"a", "b"},
			}
			if len(got) != len(want) {
				t.Fatalf("forwardable = %v, want exactly %v", got, want)
			}
			for field, wantVals := range want {
				if gotVals := got.Values(field); !slices.Equal(gotVals, wantVals) {
					t.Errorf("%s = %v, want %v", field, gotVals, wantVals)
				}
			}
		})
	}
	// Excluded fields are dropped however they are spelled.
	got := forwardableHeader(http.Header{
		"host":           {"caller.invalid"},
		"content-length": {"999"},
		"te":             {"trailers"},
		"connection":     {"close"},
	})
	if len(got) != 0 {
		t.Errorf("forwardable = %v, want the exclusion set dropped", got)
	}
}

func TestPassthroughAddsNoClientDefaults(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("POST", "/api/daemon/tasks/whatever/status", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "running"})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	// A caller that sends no User-Agent and no Accept-Encoding gets exactly
	// that upstream: the client library may not speak for it.
	header := http.Header{"Authorization": {"Bearer mat_probe"}}
	if _, _, _, err := c.Passthrough(t.Context(), "POST", "/api/daemon/tasks/whatever/status", "", header, multipartProbeBody("----s29probe")); err != nil {
		t.Fatalf("Passthrough: %v", err)
	}
	got := singleCall(t, stub, "POST", "/api/daemon/tasks/whatever/status")
	if got.Header.Get("User-Agent") != "" || got.Header.Get("Accept-Encoding") != "" {
		t.Errorf("upstream saw invented fields: %v", got.Header)
	}
	if extra := upstreamFields(got.Header); len(extra) != 1 {
		t.Errorf("upstream fields = %v, want only the Authorization the caller sent", extra)
	}
}

func TestPassthroughKeepsCallerEncodingNegotiation(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("GET", "/api/issues/42", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"id": "42"})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	header := http.Header{
		"Authorization":    {"Bearer mat_x"},
		"Accept-Encoding":  {"gzip"},
		"User-Agent":       {"probe/1.0"},
		"Cookie":           {"a=1", "b=2"},
		"X-Client-Version": {"caller-supplied"},
	}
	if _, _, _, err := c.Passthrough(t.Context(), "GET", "/api/issues/42", "", header, nil); err != nil {
		t.Fatalf("Passthrough: %v", err)
	}
	got := singleCall(t, stub, "GET", "/api/issues/42")
	for field, want := range header {
		if v := got.Header.Values(field); !slices.Equal(v, want) {
			t.Errorf("upstream %s = %v, want %v", field, v, want)
		}
	}
	// No Foreman-side identity fields ride along: the proxy neither adds its
	// own X-Client-* set nor its daemon credential.
	if caps := got.Header.Get("X-Client-Capabilities"); caps != "" {
		t.Errorf("X-Client-Capabilities = %q, want it absent (the caller sent none)", caps)
	}
}

func TestPassthroughKeepsUpstreamContentEncoding(t *testing.T) {
	srv, stub := newStubUpstream(t)
	gz := []byte{0x1f, 0x8b, 0x08, 0x00, 0xde, 0xad} // not valid gzip: bytes stay bytes
	stub.on("GET", "/api/blob", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)
		_, _ = w.Write(gz)
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	code, header, body, err := c.Passthrough(t.Context(), "GET", "/api/blob", "", http.Header{"Authorization": {"Bearer mat_x"}, "Accept-Encoding": {"gzip"}}, nil)
	if err != nil || code != 200 {
		t.Fatalf("Passthrough = %d, %v", code, err)
	}
	// The body is never decoded: the encoding field and the bytes it
	// describes travel together (§1.2 逐字段裁定).
	if v := header.Values("Content-Encoding"); !slices.Equal(v, []string{"gzip"}) {
		t.Errorf("Content-Encoding = %v, want gzip verbatim", v)
	}
	if !slices.Equal(body, gz) {
		t.Errorf("body = %v, want the upstream bytes untouched", body)
	}
}

func TestPassthroughDropsExclusionSetOnTheWire(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("POST", "/api/issues/42/comments", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(204)
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	header := http.Header{
		"Authorization": {"Bearer mat_x"},
		"Connection":    {"X-Probe-Drop"},
		"TE":            {"trailers"},
		"Trailer":       {"X-Probe-Trailer"},
		"X-Probe-Drop":  {"nope"},
		"X-Probe-Multi": {"a", "b"},
	}
	if _, _, _, err := c.Passthrough(t.Context(), "POST", "/api/issues/42/comments", "", header, []byte(`{"body":"hi"}`)); err != nil {
		t.Fatalf("Passthrough: %v", err)
	}
	got := singleCall(t, stub, "POST", "/api/issues/42/comments")
	for _, field := range []string{"Connection", "TE", "Trailer", "X-Probe-Drop"} {
		if v := got.Header.Values(field); len(v) != 0 {
			t.Errorf("excluded field %s reached the upstream: %v", field, v)
		}
	}
	if v := got.Header.Values("X-Probe-Multi"); !slices.Equal(v, []string{"a", "b"}) {
		t.Errorf("X-Probe-Multi = %v, want [a b]", v)
	}
	if got.Host != upstreamAuthority(srv) {
		t.Errorf("upstream Host = %q, want %q", got.Host, upstreamAuthority(srv))
	}
}

func TestPassthroughKeepsResponseHeadersVerbatim(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("GET", "/api/attachments/7/download", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		h := w.Header()
		h.Set("Content-Type", "application/octet-stream")
		h.Set("Content-Disposition", "attachment; filename=probe.bin")
		h.Set("Cache-Control", "no-store")
		h.Set("ETag", `"v1"`)
		h.Add("X-Probe-Resp", "a")
		h.Add("X-Probe-Resp", "b")
		h.Add("Set-Cookie", "a=1")
		h.Add("Set-Cookie", "b=2")
		h.Set("Connection", "close")
		w.WriteHeader(200)
		_, _ = w.Write([]byte{0x00, 0x01, 0xff, 0xfe})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	code, header, body, err := c.Passthrough(t.Context(), "GET", "/api/attachments/7/download", "", http.Header{"Authorization": {"Bearer mat_x"}}, nil)
	if err != nil || code != 200 {
		t.Fatalf("Passthrough = %d, %v", code, err)
	}
	for field, want := range map[string][]string{
		"Content-Type":        {"application/octet-stream"},
		"Content-Disposition": {"attachment; filename=probe.bin"},
		"Cache-Control":       {"no-store"},
		"ETag":                {`"v1"`},
		"X-Probe-Resp":        {"a", "b"},
		"Set-Cookie":          {"a=1", "b=2"},
	} {
		if got := header.Values(field); !slices.Equal(got, want) {
			t.Errorf("%s = %v, want %v", field, got, want)
		}
	}
	for _, field := range []string{"Connection", "Content-Length", "Transfer-Encoding", "Trailer"} {
		if v := header.Values(field); len(v) != 0 {
			t.Errorf("excluded response field %s survived: %v", field, v)
		}
	}
	if !slices.Equal(body, []byte{0x00, 0x01, 0xff, 0xfe}) {
		t.Errorf("response body = %v, want the upstream bytes", body)
	}
}

// The exclusion set's transport half: fields the proxy's own connection
// decides (framing, trailers, a Date it did not receive) never reach the
// caller.
func TestPassthroughReproducesNoFramingOrTrailers(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("GET", "/api/plain", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header()["Date"] = nil // the upstream sends no Date
		w.Header().Set("Trailer", "X-Probe-Trailer")
		w.Header().Set("X-Probe-Note", "late")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("plain"))
		w.Header().Set("X-Probe-Trailer", "trailer-value")
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	code, header, body, err := c.Passthrough(t.Context(), "GET", "/api/plain", "", http.Header{"Authorization": {"Bearer mat_x"}}, nil)
	if err != nil || code != 200 || string(body) != "plain" {
		t.Fatalf("Passthrough = %d %q, %v", code, body, err)
	}
	if v := header.Values("Trailer"); len(v) != 0 {
		t.Errorf("Trailer field survived: %v", v)
	}
	if v := header.Values("X-Probe-Trailer"); len(v) != 0 {
		t.Errorf("trailer value reproduced: %v", v)
	}
	if v := header.Values("Date"); len(v) != 0 {
		t.Errorf("Date = %v, want none: the upstream sent none and the proxy invents none", v)
	}
	// An ordinary end-to-end field set before the status line still travels.
	if v := header.Values("X-Probe-Note"); !slices.Equal(v, []string{"late"}) {
		t.Errorf("X-Probe-Note = %v, want it forwarded", v)
	}
}

func TestPassthroughDoesNotFollowRedirects(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("GET", "/api/moved", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Location", "https://elsewhere.example/api/moved")
		w.WriteHeader(302)
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	code, header, _, err := c.Passthrough(t.Context(), "GET", "/api/moved", "", http.Header{"Authorization": {"Bearer mat_x"}}, nil)
	if err != nil || code != 302 {
		t.Fatalf("Passthrough = %d, %v; want the upstream 302 handed to the caller", code, err)
	}
	// Location travels verbatim; the caller decides whether to follow.
	if got := header.Get("Location"); got != "https://elsewhere.example/api/moved" {
		t.Errorf("Location = %q, want it verbatim", got)
	}
}

func TestPassthroughNoContentTypeFallback(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("POST", "/api/issues/42/comments", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(201)
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")

	// A caller that declares no Content-Type gets none upstream: a fallback
	// would put the proxied request on a different upstream branch than the
	// same direct request (§1.2 逐字段裁定).
	if _, _, _, err := c.Passthrough(t.Context(), "POST", "/api/issues/42/comments", "", http.Header{"Authorization": {"Bearer mat_x"}}, []byte(`{"body":"hi"}`)); err != nil {
		t.Fatalf("Passthrough: %v", err)
	}
	got := singleCall(t, stub, "POST", "/api/issues/42/comments")
	if got.Header.Get("Content-Type") != "" {
		t.Errorf("Content-Type = %q, want it absent", got.Header.Get("Content-Type"))
	}
	if string(got.Body) != `{"body":"hi"}` {
		t.Errorf("body = %s", got.Body)
	}
}

func TestPassthroughMetricCarriesCallerVisibleCode(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("GET", "/api/boom", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 500, map[string]string{"error": "boom"})
	})
	stub.on("GET", "/api/rejected", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 400, map[string]string{"error": "bad"})
	})
	var metrics fakeMetrics
	c := NewClient(testConfig(srv.URL), "mdt_test", WithMetrics(&metrics))

	if code, _, _, _ := c.Passthrough(t.Context(), "GET", "/api/boom", "", http.Header{"Authorization": {"Bearer mat_x"}}, nil); code != 500 {
		t.Fatalf("upstream 500 = %d", code)
	}
	if code, _, _, _ := c.Passthrough(t.Context(), "GET", "/api/rejected", "", http.Header{"Authorization": {"Bearer mat_x"}}, nil); code != 400 {
		t.Fatalf("upstream 400 = %d", code)
	}
	// An unreachable upstream is the third shape: the caller gets 502 and so
	// does the metric, never a zero.
	down := NewClient(testConfig("http://127.0.0.1:9"), "mdt_test", WithMetrics(&metrics))
	if _, _, _, err := down.Passthrough(t.Context(), "GET", "/api/boom", "", http.Header{"Authorization": {"Bearer mat_x"}}, nil); err == nil {
		t.Fatal("unreachable upstream must report an error")
	}
	metrics.mu.Lock()
	got := append([]int(nil), metrics.codes...)
	metrics.mu.Unlock()
	// Upstream 5xx converges to the 502 the caller sees; the upstream's own
	// code is not the label (observability.md).
	if !slices.Equal(got, []int{502, 400, 502}) {
		t.Errorf("metric codes = %v, want [502 400 502]", got)
	}
}

// A body the upstream never finishes (truncated/dropped stream) is a
// transport failure: the caller gets 502, so the agent-api metric must say
// 502 rather than the upstream's status line.
func TestPassthroughCountsTruncatedBodyAs502(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		// Promise 64 bytes, deliver 5, then drop the connection.
		_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 64\r\n\r\nshort")
		_ = buf.Flush()
		_ = conn.Close()
	}))
	t.Cleanup(srv.Close)

	var metrics fakeMetrics
	c := NewClient(testConfig(srv.URL), "mdt_test", WithMetrics(&metrics))
	if _, _, _, err := c.Passthrough(t.Context(), "GET", "/api/truncated", "", http.Header{"Authorization": {"Bearer mat_x"}}, nil); err == nil {
		t.Fatal("a truncated upstream body must surface as an error")
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if !slices.Equal(metrics.codes, []int{http.StatusBadGateway}) {
		t.Errorf("metric codes = %v, want [502] (the code the caller sees)", metrics.codes)
	}
}

// A DefaultTransport replaced by a foreign RoundTripper must not turn into a
// startup panic, and the fallback keeps the connection defaults this path
// relies on.
func TestPassthroughTransportFallsBackOnForeignDefault(t *testing.T) {
	saved := http.DefaultTransport
	http.DefaultTransport = roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("foreign default transport")
	})
	t.Cleanup(func() { http.DefaultTransport = saved })

	tr, ok := passthroughTransport().(*http.Transport)
	if !ok {
		t.Fatalf("passthroughTransport = %T, want *http.Transport", passthroughTransport())
	}
	if !tr.DisableCompression {
		t.Error("the fallback must not negotiate compression")
	}
	if tr.Proxy == nil || !tr.ForceAttemptHTTP2 || tr.DialContext == nil {
		t.Errorf("fallback lost the defaults: proxy=%v http2=%v dial=%v", tr.Proxy != nil, tr.ForceAttemptHTTP2, tr.DialContext != nil)
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

	code, _, _, err := c.Passthrough(t.Context(), "GET", "/api/issues/a%2Fb", "", http.Header{"Authorization": {"Bearer mat_tasktoken"}}, nil)
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
	registerDeclared(t, c)

	_, err := c.Heartbeat(t.Context(), "rid-1")
	var se *StatusError
	if !errors.As(err, &se) || se.Code != 401 {
		t.Fatalf("Heartbeat = %v, want a 401 StatusError (the loop's backoff depends on it)", err)
	}
}

func TestRebuildSignalSurvivesFailedReregister(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	c := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, c)

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

	if _, err := c.Heartbeat(t.Context(), "rid-1"); err == nil {
		t.Fatal("heartbeat must fail while re-registration fails")
	}
	// The register failure cleared the cached id but kept the rebuild fact:
	// the next successful register must still fire C15 + the runtime-set
	// signal.
	failRegister.Store(false)
	if err := c.Register(t.Context()); err != nil {
		t.Fatalf("second Register: %v", err)
	}
	if got := firstRuntimeID(c); got != "rid-2" {
		t.Fatalf("runtime id = %q", got)
	}
	if got := orphanCalls.Load(); got != 1 {
		t.Errorf("C15 calls = %d, want 1 (runtime was rebuilt)", got)
	}
	select {
	case change := <-c.Changes():
		if change.NewID != "rid-2" {
			t.Errorf("runtime change = %+v", change)
		}
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
	c := NewClient(testConfig(srv.URL), "mdt_test",
		WithTaskRuntime(func(string) string { return "rid-real" }))
	registerDeclared(t, c)

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
	if err := c.RegisterWithDiscovery(t.Context(), testDiscovery()); err != nil {
		t.Fatalf("RegisterWithDiscovery: %v", err)
	}

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
	if err := c.RegisterWithDiscovery(t.Context(), testDiscovery()); err != nil {
		t.Fatalf("RegisterWithDiscovery: %v", err)
	}
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
	registerDeclared(t, c)
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
	mu           sync.Mutex
	forwards     []string
	codes        []int
	claimErrors  []string
	authFails    []string
	wsConnected  map[string]int
	wsReconnect  []string
	catalogCodes []string
	catalogSpans []float64
}

func (m *fakeMetrics) Forward(endpoint string, code int, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.forwards = append(m.forwards, endpoint)
	m.codes = append(m.codes, code)
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

func (m *fakeMetrics) ModelCatalogRelay(code string, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.catalogCodes = append(m.catalogCodes, code)
	m.catalogSpans = append(m.catalogSpans, seconds)
}

// catalogCodeCount counts the recorded relay outcomes per code (locked).
func (m *fakeMetrics) catalogCodeCount(code string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, recorded := range m.catalogCodes {
		if recorded == code {
			n++
		}
	}
	return n
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

// roundTripperFunc adapts a function to http.RoundTripper (test seam for a
// replaced http.DefaultTransport).
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestFirstDeclarationDoesNotRecoverOrphans pins §1.1 C15: C15 belongs to a
// rebuilt runtime, never to the rows a first declaration creates.
func TestFirstDeclarationDoesNotRecoverOrphans(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("POST", "/api/daemon/runtimes/rid-1/recover-orphans", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		t.Error("a first declaration must not run C15")
		writeJSONStub(w, 200, map[string]int{"orphaned": 0})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, c)

	if got := len(stub.calls("POST", "/api/daemon/runtimes/rid-1/recover-orphans")); got != 0 {
		t.Fatalf("C15 calls = %d, want 0 on a first declaration", got)
	}
}

// TestPreflightSendsSingleC1 pins §1.1 C1's single startup trigger: once the
// startup declaration has been answered, the claim loop's preflight must not
// declare again (a second C1 could move ids and orphan the startup discovery).
func TestPreflightSendsSingleC1(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("GET", "/api/daemon/workspaces", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, []map[string]string{{"id": "ws-1", "name": "ws-one"}})
	})
	stub.on("POST", "/api/tokens/current/renew", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{"renewed": true})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, c)

	if err := c.Preflight(t.Context()); err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if got := len(stub.calls("POST", "/api/daemon/register")); got != 1 {
		t.Fatalf("C1 calls = %d, want exactly the startup one", got)
	}
}

// TestDeclareDefersTheSingleC1: Declare caches the startup discovery without a
// wire call, and the first preflight carries it.
func TestDeclareDefersTheSingleC1(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	stub.on("GET", "/api/daemon/workspaces", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, []map[string]string{{"id": "ws-1", "name": "ws-one"}})
	})
	stub.on("POST", "/api/tokens/current/renew", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{"renewed": true})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")
	c.Declare(testDiscovery())

	if got := len(stub.calls("POST", "/api/daemon/register")); got != 0 {
		t.Fatalf("Declare must not touch the wire, got %d C1 calls", got)
	}
	if err := c.Preflight(t.Context()); err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if got := len(stub.calls("POST", "/api/daemon/register")); got != 1 {
		t.Fatalf("C1 calls = %d, want 1 (the preflight carries the declaration)", got)
	}
	if got := firstRuntimeID(c); got != "rid-1" {
		t.Errorf("runtime id = %q", got)
	}
}

// TestRebuildSkipsReRegisterForAStaleID: when the id is already replaced by a
// concurrent path, a rebuild must not write another C1 that cannot move
// anything (that write was the WS 404 loop's registration storm).
func TestRebuildSkipsReRegisterForAStaleID(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stubRegister(stub, "rid-1")
	c := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, c)
	calls := len(stub.calls("POST", "/api/daemon/register"))

	fresh, err := c.rebuildRuntime(t.Context(), "rid-already-gone")
	if err != nil {
		t.Fatalf("rebuildRuntime: %v", err)
	}
	if fresh != "" {
		t.Errorf("fresh id = %q, want none for an unknown runtime", fresh)
	}
	if got := len(stub.calls("POST", "/api/daemon/register")); got != calls {
		t.Fatalf("C1 calls = %d, want no re-register for a stale id (%d)", got, calls)
	}
}

// TestFailedRebuildIsRememberedPerRuntime pins §1.1 C15's granularity: one
// runtime's failed rebuild must not make every other row look rebuilt on the
// next successful C1 (each of those would emit a C15 call and a
// `runtime.rebuilt` line).
func TestFailedRebuildIsRememberedPerRuntime(t *testing.T) {
	srv, stub := newStubUpstream(t)
	ids := map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	var failRegister atomic.Bool
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		if failRegister.Load() {
			writeJSONStub(w, 503, map[string]string{"error": "boom"})
			return
		}
		answers := make([]map[string]string, 0, len(ids))
		for provider, id := range ids {
			answers = append(answers, map[string]string{"id": id, "name": "n-" + provider, "provider": provider, "status": "online"})
		}
		writeJSONStub(w, 200, map[string]any{
			"runtimes": answers, "repos": []map[string]string{}, "repos_version": "v1", "settings": nil,
		})
	})
	var c15 []string
	for _, id := range []string{"rid-omp", "rid-zz"} {
		id := id
		stub.on("POST", "/api/daemon/runtimes/"+id+"/recover-orphans", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
			c15 = append(c15, id)
			writeJSONStub(w, 200, map[string]int{"orphaned": 0})
		})
	}
	logs := &bytes.Buffer{}
	c := NewClient(testConfig(srv.URL), "mdt_test", WithLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	disc := registry.Discovery{Runtimes: []registry.DiscoveredRuntime{
		{Name: "n-omp", Type: "omp", Version: "1"},
		{Name: "n-zz", Type: "zz", Version: "2"},
	}}
	if err := c.RegisterWithDiscovery(t.Context(), disc); err != nil {
		t.Fatalf("RegisterWithDiscovery: %v", err)
	}
	drainChanges(c)

	// The omp row is gone and its re-register fails: only omp is marked lost.
	failRegister.Store(true)
	if _, err := c.rebuildRuntime(t.Context(), "rid-omp"); err == nil {
		t.Fatal("the rebuild must surface the register failure")
	}
	failRegister.Store(false)
	if err := c.Register(t.Context()); err != nil {
		t.Fatalf("second Register: %v", err)
	}

	if len(c15) != 1 || c15[0] != "rid-omp" {
		t.Fatalf("C15 calls = %v, want only the runtime whose row was lost", c15)
	}
	if got := strings.Count(logs.String(), "runtime.rebuilt"); got != 1 {
		t.Errorf("runtime.rebuilt lines = %d, want 1 (per runtime, not per declaration)", got)
	}
}

// TestSuccessfulRegisterKeepsAConcurrentLostMark pins §1.1 C15 under a race: a
// rebuild that fails while another C1 is in flight marks its own provider lost,
// and the successful C1 consumes only the marks it started with. A global clear
// would silently drop that row's C15 (`recover-orphans` + `runtime.rebuilt`)
// even though the row came back — under the same id — and needs the rebuild
// signal.
func TestSuccessfulRegisterKeepsAConcurrentLostMark(t *testing.T) {
	srv, stub := newStubUpstream(t)
	ids := map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	var fail atomic.Bool
	var hold atomic.Bool
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		if hold.CompareAndSwap(true, false) {
			// Hold this C1 inside the handler: its ledger snapshot is already
			// taken, and the rebuild below writes the mark it must not clear.
			entered <- struct{}{}
			<-release
		}
		if fail.Load() {
			writeJSONStub(w, 503, map[string]string{"error": "boom"})
			return
		}
		answers := make([]map[string]string, 0, len(ids))
		for provider, id := range ids {
			answers = append(answers, map[string]string{"id": id, "name": "n-" + provider, "provider": provider, "status": "online"})
		}
		writeJSONStub(w, 200, map[string]any{
			"runtimes": answers, "repos": []map[string]string{}, "repos_version": "v1", "settings": nil,
		})
	})
	var c15mu sync.Mutex
	var c15 []string
	stub.on("POST", "/api/daemon/runtimes/rid-omp/recover-orphans", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		c15mu.Lock()
		c15 = append(c15, "rid-omp")
		c15mu.Unlock()
		writeJSONStub(w, 200, map[string]int{"orphaned": 0})
	})
	logs := &lockedBuffer{}
	c := NewClient(testConfig(srv.URL), "mdt_test", WithLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	disc := registry.Discovery{Runtimes: []registry.DiscoveredRuntime{
		{Name: "n-omp", Type: "omp", Version: "1"},
		{Name: "n-zz", Type: "zz", Version: "2"},
	}}
	if err := c.RegisterWithDiscovery(t.Context(), disc); err != nil {
		t.Fatalf("RegisterWithDiscovery: %v", err)
	}
	drainChanges(c)

	hold.Store(true)
	inFlight := make(chan error, 1)
	go func() { inFlight <- c.Register(t.Context()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight register never reached the server")
	}
	// omp's own row is proved gone and its rebuild fails: only omp is marked.
	fail.Store(true)
	if _, err := c.rebuildRuntime(t.Context(), "rid-omp"); err == nil {
		t.Fatal("the rebuild must surface the register failure")
	}
	fail.Store(false)
	close(release)
	if err := <-inFlight; err != nil {
		t.Fatalf("the in-flight Register: %v", err)
	}

	// omp answers with the id it already had: the mark still owes a C15.
	if err := c.Register(t.Context()); err != nil {
		t.Fatalf("third Register: %v", err)
	}
	c15mu.Lock()
	got := append([]string(nil), c15...)
	c15mu.Unlock()
	if len(got) != 1 || got[0] != "rid-omp" {
		t.Fatalf("C15 calls = %v, want only the runtime whose rebuild failed", got)
	}
	if n := strings.Count(logs.String(), "runtime.rebuilt"); n != 1 {
		t.Errorf("runtime.rebuilt lines = %d, want 1", n)
	}
}
