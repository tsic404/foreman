package proxy

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/tsic404/foreman/internal/registry"
)

// newTestRigWS builds a rig with the S30 WS endpoint enabled.
func newTestRigWS(t *testing.T) (*testRig, *WSHandler) {
	t.Helper()
	rig := newTestRig(t)
	h := NewWSHandler(rig.issuer, rig.reg, rig.metrics)
	rig.server.Close()
	rig.handler = NewFakeServer(rig.issuer, rig.reg, rig.sched, rig.client, h, rig.metrics)
	rig.server = httptest.NewServer(rig.handler)
	t.Cleanup(rig.server.Close)
	return rig, h
}

func wsDial(t *testing.T, serverURL, token string, runtimeIDs []string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	u := "ws" + strings.TrimPrefix(serverURL, "http") + "/api/daemon/ws"
	if len(runtimeIDs) > 0 {
		u += "?runtime_ids=" + strings.Join(runtimeIDs, "&runtime_ids=")
	}
	hdr := http.Header{}
	if token != "" {
		hdr.Set("Authorization", "Bearer "+token)
	}
	return websocket.Dial(context.Background(), u, &websocket.DialOptions{HTTPHeader: hdr})
}

func readFrame(t *testing.T, conn *websocket.Conn) wsFrame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	var frame wsFrame
	if err := json.Unmarshal(data, &frame); err != nil {
		t.Fatalf("frame: %v", err)
	}
	return frame
}

func writeHeartbeat(t *testing.T, conn *websocket.Conn, runtimeID string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"runtime_id": runtimeID, "supports_batch_import": true})
	frame, _ := json.Marshal(wsFrame{Type: FrameDaemonHeartbeat, Payload: payload})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
		t.Fatalf("write heartbeat: %v", err)
	}
}

func TestWSHandlerHandshakeAuth(t *testing.T) {
	rig, _ := newTestRigWS(t)
	rig.claimTask(t, "t-ws")
	tok := rig.token(t, "t-ws")
	jobRID := rig.registerDaemon(t, "t-ws", tok)

	// No credential → 401.
	_, resp, err := wsDial(t, rig.server.URL, "", []string{jobRID})
	if err == nil || resp == nil || resp.StatusCode != 401 {
		t.Errorf("no token: err=%v status=%v, want 401", err, resp.StatusCode)
	}
	// Non-fmj_ credential: the routing rules (§2.1 rule 2) are
	// credential-based, so the request passes through to the real server
	// verbatim rather than being handled by the local WS endpoint.
	rig.stub.on("GET", "/api/daemon/ws", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":"runtime_ids or user identity required"}`))
	})
	_, resp, err = wsDial(t, rig.server.URL, "mat_x", []string{jobRID})
	if err == nil || resp == nil || resp.StatusCode != 400 {
		t.Errorf("mat_ token: err=%v status=%v, want the upstream's 400 verbatim", err, resp.StatusCode)
	}
	if got := rig.stub.calls("GET", "/api/daemon/ws"); len(got) != 1 || got[0].Header.Get("Authorization") != "Bearer mat_x" {
		t.Fatalf("the WS request must pass through with the caller credential: %+v", got)
	}
	// Missing runtime_ids → 400.
	_, resp, err = wsDial(t, rig.server.URL, tok, nil)
	if err == nil || resp == nil || resp.StatusCode != 400 {
		t.Errorf("missing runtime_ids: err=%v status=%v, want 400", err, resp.StatusCode)
	}
	// Foreign runtime_ids → 403.
	_, resp, err = wsDial(t, rig.server.URL, tok, []string{"other-runtime"})
	if err == nil || resp == nil || resp.StatusCode != 403 {
		t.Errorf("foreign runtime: err=%v status=%v, want 403", err, resp.StatusCode)
	}
	// Valid handshake → connected.
	conn, _, err := wsDial(t, rig.server.URL, tok, []string{jobRID})
	if err != nil {
		t.Fatalf("valid dial: %v", err)
	}
	_ = conn.Close(websocket.StatusNormalClosure, "done")
}

func TestWSHandlerHeartbeatAckPinned(t *testing.T) {
	rig, _ := newTestRigWS(t)
	rig.claimTask(t, "t-wsack")
	tok := rig.token(t, "t-wsack")
	jobRID := rig.registerDaemon(t, "t-wsack", tok)

	conn, _, err := wsDial(t, rig.server.URL, tok, []string{jobRID})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	// The on-connect catch-up push fires first (task not yet delivered).
	first := readFrame(t, conn)
	if first.Type != FrameDaemonTaskAvailable {
		t.Fatalf("first frame = %s, want the catch-up %s", first.Type, FrameDaemonTaskAvailable)
	}

	writeHeartbeat(t, conn, jobRID)
	ack := readFrame(t, conn)
	if ack.Type != FrameDaemonHeartbeatAck {
		t.Fatalf("ack type = %s", ack.Type)
	}
	// The ack payload is pinned: exactly runtime_id + status=ok (S-30).
	var payload map[string]string
	if err := json.Unmarshal(ack.Payload, &payload); err != nil {
		t.Fatalf("ack payload: %v", err)
	}
	if len(payload) != 2 || payload["runtime_id"] != jobRID || payload["status"] != "ok" {
		t.Errorf("ack payload = %s, want pinned {runtime_id, status:ok}", ack.Payload)
	}
	rt, _ := rig.reg.RuntimeByID(jobRID)
	if rt.LastHeartbeatAt.IsZero() {
		t.Error("WS ack must refresh the shared heartbeat clock (scenario #6)")
	}
}

func TestWSHandlerRuntimeGoneAck(t *testing.T) {
	rig, _ := newTestRigWS(t)
	rig.claimTask(t, "t-wsgone")
	tok := rig.token(t, "t-wsgone")
	jobRID := rig.registerDaemon(t, "t-wsgone", tok)

	conn, _, err := wsDial(t, rig.server.URL, tok, []string{jobRID})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")
	_ = readFrame(t, conn) // catch-up push

	writeHeartbeat(t, conn, "deleted-runtime")
	ack := readFrame(t, conn)
	if ack.Type != FrameDaemonHeartbeatAck {
		t.Fatalf("ack type = %s", ack.Type)
	}
	var payload map[string]any
	_ = json.Unmarshal(ack.Payload, &payload)
	if payload["runtime_gone"] != true || payload["status"] != "runtime_gone" {
		t.Errorf("runtime_gone ack = %s (scenario #12)", ack.Payload)
	}
}

func TestWSHandlerTaskAvailablePush(t *testing.T) {
	rig, h := newTestRigWS(t)
	rig.claimTask(t, "t-wspush")
	tok := rig.token(t, "t-wspush")
	jobRID := rig.registerDaemon(t, "t-wspush", tok)

	conn, _, err := wsDial(t, rig.server.URL, tok, []string{jobRID})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")
	_ = readFrame(t, conn) // catch-up push

	h.NotifyTaskAvailable("fm-t-wspush", "t-wspush")
	frame := readFrame(t, conn)
	if frame.Type != FrameDaemonTaskAvailable {
		t.Fatalf("push = %s", frame.Type)
	}
	var payload map[string]string
	_ = json.Unmarshal(frame.Payload, &payload)
	if payload["task_id"] != "t-wspush" {
		t.Errorf("push payload = %s", frame.Payload)
	}

	// Terminal closes the connection (S30).
	h.CloseJob("fm-t-wspush")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := conn.Read(ctx); err == nil {
		t.Error("connection must close on terminal/teardown")
	}
}

func TestRegisterDoesNotWakeASettledTask(t *testing.T) {
	rig, _ := newTestRigWS(t)
	rig.claimTask(t, "t-settled")
	tok := rig.token(t, "t-settled")
	jobRID := rig.registerDaemon(t, "t-settled", tok)

	conn, _, err := wsDial(t, rig.server.URL, tok, []string{jobRID})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")
	_ = readFrame(t, conn) // catch-up push

	if _, _, err := rig.reg.MarkTerminal("t-settled", registry.ResultCompleted, "", time.Time{}); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	code, body := rig.call(t, "POST", "/api/daemon/register", tok, []byte(`{
		"daemon_id": "fm-t-settled",
		"runtimes": [{"name": "foreman-job", "type": "omp"}]
	}`))
	if code != 200 {
		t.Fatalf("re-register = %d: %s", code, body)
	}
	// The entry is terminal: waking the daemon would only produce an empty
	// claim batch.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("a settled entry must not emit task_available")
	}
}

func TestWSRouteAbsentWhenDisabled(t *testing.T) {
	rig := newTestRig(t) // ws == nil → route not registered
	rig.claimTask(t, "t-nows")
	tok := rig.token(t, "t-nows")

	code, _ := rig.call(t, "GET", "/api/daemon/ws", tok, nil)
	if code != 404 {
		t.Errorf("WS dial with FOREMAN_WS_ENABLED=false = %d, want 404 (not 5xx)", code)
	}
}

// combinedStub serves the HTTP register endpoint and the WS endpoint on one
// server, so the subscriber dials it exactly as it would the real server.
type combinedStub struct {
	mu     sync.Mutex
	dials  int
	lastQ  url.Values
	frames chan wsFrame
	conn   *websocket.Conn
}

func newCombinedStub(t *testing.T) (*httptest.Server, *combinedStub) {
	t.Helper()
	stub := &combinedStub{frames: make(chan wsFrame, 16)}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/daemon/register", func(w http.ResponseWriter, r *http.Request) {
		writeJSONStub(w, 200, map[string]any{
			"runtimes": []map[string]string{{"id": "rid-real", "name": "foreman-omp", "provider": "omp", "status": "online"}},
			"repos":    []map[string]string{}, "repos_version": "v1", "settings": nil,
		})
	})
	mux.HandleFunc("/api/daemon/ws", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer mdt_test" {
			w.WriteHeader(401)
			return
		}
		stub.mu.Lock()
		stub.dials++
		stub.lastQ = r.URL.Query()
		stub.mu.Unlock()
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		stub.mu.Lock()
		stub.conn = conn
		stub.mu.Unlock()
		_, _, _ = conn.Read(r.Context()) // block until the peer goes away
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, stub
}

func (s *combinedStub) dialCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dials
}

func (s *combinedStub) push(t *testing.T, frame wsFrame) {
	t.Helper()
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		t.Fatal("no subscriber connection")
	}
	raw, _ := json.Marshal(frame)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatalf("push: %v", err)
	}
}

func (s *combinedStub) closeConn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.Close(websocket.StatusAbnormalClosure, "boom")
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestWSSubscriberLifecycle(t *testing.T) {
	srv, stub := newCombinedStub(t)
	wakeCh := make(chan struct{}, 4)
	var metrics fakeMetrics
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	sub := NewWSSubscriber(client, func() { wakeCh <- struct{}{} }, &metrics)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()

	// C1 runs before the WS dial (ADR-010 startup order) and runtime_ids is
	// the real runtime id (mandatory, C-20).
	waitFor(t, "ws dial", func() bool { return stub.dialCount() == 1 })
	waitFor(t, "ws.connected", func() bool { return metrics.wsCount(WSSideServer) == 1 })
	stub.mu.Lock()
	if got := stub.lastQ["runtime_ids"]; len(got) != 1 || got[0] != "rid-real" {
		t.Errorf("dial runtime_ids = %v, want the real runtime id", got)
	}
	stub.mu.Unlock()

	// daemon:task_available triggers an immediate claim wakeup.
	payload, _ := json.Marshal(map[string]string{"task_id": "t1"})
	stub.push(t, wsFrame{Type: FrameDaemonTaskAvailable, Payload: payload})
	select {
	case <-wakeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("task_available did not wake the claim loop")
	}

	// A dropped connection reconnects with backoff (§1.5/ADR-010).
	stub.closeConn()
	waitFor(t, "ws reconnect", func() bool { return stub.dialCount() >= 2 })
	waitFor(t, "ws.reconnect metric", func() bool { return metrics.reconnectCount(WSSideServer) >= 1 })
}

// TestWSSubscriberProfileChangeTriggersRefresh pins the §1.6 event face: a
// daemon:runtime_profiles_changed frame naming this workspace notifies the
// refresher exactly once, while another workspace's frame is ignored.
func TestWSSubscriberProfileChangeTriggersRefresh(t *testing.T) {
	srv, stub := newCombinedStub(t)
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	notified := make(chan struct{}, 4)
	sub := NewWSSubscriber(client, func() {}, nil,
		WithProfileChangeNotify(func() { notified <- struct{}{} }))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()
	waitFor(t, "ws dial", func() bool { return stub.dialCount() == 1 })

	// Another workspace's change is not this instance's.
	foreign, _ := json.Marshal(map[string]string{"workspace_id": "ws-other"})
	stub.push(t, wsFrame{Type: FrameDaemonRuntimeProfilesChanged, Payload: foreign})
	select {
	case <-notified:
		t.Fatal("a foreign workspace's profile change must not trigger a refresh")
	case <-time.After(200 * time.Millisecond):
	}
	// The matching frame triggers exactly one notification (the refresher
	// coalesces duplicates).
	matching, _ := json.Marshal(map[string]string{"workspace_id": "ws-1"})
	stub.push(t, wsFrame{Type: FrameDaemonRuntimeProfilesChanged, Payload: matching})
	select {
	case <-notified:
	case <-time.After(5 * time.Second):
		t.Fatal("a matching profile change did not trigger the refresh")
	}
}

// TestWSSubscriberIgnoresWorkspacesChanged pins ADR-049 decision 5: the
// discovery set is fixed for the process lifetime, so a
// daemon:workspaces_changed frame on the user-scoped connection is received
// and ignored — it must not refresh profiles, wake a claim, or drop the
// connection.
func TestWSSubscriberIgnoresWorkspacesChanged(t *testing.T) {
	srv, stub := newCombinedStub(t)
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	profilesChanged := make(chan struct{}, 4)
	wake := make(chan struct{}, 4)
	sub := NewWSSubscriber(client, func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}, nil, WithProfileChangeNotify(func() { profilesChanged <- struct{}{} }))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()
	waitFor(t, "ws dial", func() bool { return stub.dialCount() == 1 })

	payload, _ := json.Marshal(map[string]string{"workspace_id": "ws-1"})
	stub.push(t, wsFrame{Type: FrameDaemonWorkspacesChanged, Payload: payload})
	select {
	case <-profilesChanged:
		t.Fatal("workspaces_changed must not trigger a profile refresh")
	case <-wake:
		t.Fatal("workspaces_changed must not wake a claim")
	case <-time.After(200 * time.Millisecond):
	}
	// The connection stays up: no reconnect follows the ignored frame.
	if got := stub.dialCount(); got != 1 {
		t.Fatalf("dials = %d after an ignored workspaces_changed frame, want 1", got)
	}
}

// TestWSFrameWorkspaceIDReadsBothShapes keeps the invalidation hint tolerant:
// upstream may put workspace_id in the payload or at the envelope level.
func TestWSFrameWorkspaceIDReadsBothShapes(t *testing.T) {
	if got := (wsFrame{Payload: json.RawMessage(`{"workspace_id":"payload-ws"}`)}).workspaceID(); got != "payload-ws" {
		t.Errorf("payload workspace_id = %q", got)
	}
	if got := (wsFrame{WorkspaceID: "envelope-ws"}).workspaceID(); got != "envelope-ws" {
		t.Errorf("envelope workspace_id = %q", got)
	}
	if got := (wsFrame{Payload: json.RawMessage(`{}`)}).workspaceID(); got != "" {
		t.Errorf("absent workspace_id = %q", got)
	}
}

// TestWSSubscriberHandshake404BacksOff: a persistent handshake 404 must not
// spin a re-registration storm — the subscriber waits out the §1.5 backoff.
func TestWSSubscriberHandshake404BacksOff(t *testing.T) {
	var dials atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/daemon/register", func(w http.ResponseWriter, r *http.Request) {
		writeJSONStub(w, 200, map[string]any{
			"runtimes": []map[string]string{{"id": "rid-real", "name": "foreman-omp", "provider": "omp", "status": "online"}},
			"repos":    []map[string]string{}, "repos_version": "v1", "settings": nil,
		})
	})
	mux.HandleFunc("/api/daemon/ws", func(w http.ResponseWriter, r *http.Request) {
		dials.Add(1)
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"error":"runtime not found"}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := NewClient(testConfig(srv.URL), "mdt_test")
	sub := NewWSSubscriber(client, func() {}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()

	// 1.6s at a 1s-starting backoff: at most 2 dials. A zero-backoff loop
	// would produce dozens.
	time.Sleep(1600 * time.Millisecond)
	if got := dials.Load(); got > 2 {
		t.Fatalf("dials = %d in 1.6s, want ≤ 2 (backoff enforced)", got)
	}
	cancel()
}

// TestWSSubscriberRetriesOnHandshake403: a 403 handshake at runtime is the
// credential failure face of §1.4 — the subscriber backs off and retries,
// never judging it a configuration error (§1.5). Only the startup self-check
// fails fast; the running loop keeps dialing.
func TestWSSubscriberRetriesOnHandshake403(t *testing.T) {
	var dials atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/daemon/ws", func(w http.ResponseWriter, _ *http.Request) {
		dials.Add(1)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"token does not match daemon_id"}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := NewClient(testConfig(srv.URL), "mdt_test")
	// The declaration normally comes from the startup probe; seeding it
	// directly keeps this test on the handshake branch alone.
	client.refs = []RuntimeRef{{ID: "rid-real", Provider: "omp", Name: "foreman-omp"}}
	sub := NewWSSubscriber(client, func() {}, nil)
	sub.waitSteps = []time.Duration{time.Millisecond}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for dials.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := dials.Load(); got < 3 {
		t.Errorf("dials = %d, want >= 3 (a 403 handshake must be retried, not fatal)", got)
	}
	select {
	case err := <-done:
		t.Fatalf("Run stopped on a 403 handshake: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancel, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

// TestWSSubscriberDialTimeout: a peer that accepts TCP but never answers
// the upgrade must not wedge the subscriber (§1.5 control-plane timeout).
func TestWSSubscriberDialTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	var accepts atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			defer conn.Close() // hang: never answer the upgrade
		}
	}()

	cfg := testConfig("http://" + ln.Addr().String())
	client := NewClient(cfg, "mdt_test")
	// Registering against the hanging server is impossible; seed the declared
	// runtime set directly so the subscriber proceeds to the dial.
	client.refs = []RuntimeRef{{ID: "rid-real", Provider: "omp", Name: "foreman-omp"}}
	sub := NewWSSubscriber(client, func() {}, nil)
	sub.dialTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()

	// Each dial must give up after ~100ms, so several attempts pile up; a
	// wedged dial would pin accepts at 1.
	waitFor(t, "timed-out dials", func() bool { return accepts.Load() >= 2 })
	cancel()
}

// wsGoneStub answers the WS handshake with a runtime-gone 404 and the register
// endpoint with a programmable runtime id per provider. With rotate=true every
// register answers a fresh id (the "id moves on every C1" shape the design
// leaves unverified); with accept=true a dial for the current id is accepted and
// held, so live connections can be counted.
type wsGoneStub struct {
	mu        sync.Mutex
	runtimeID string
	dials     []string // runtime_ids query values, in dial order
	registers int
	rotate    bool
	accept    bool
	// dialScript, when set, decides the handshake per dial *in dial order*
	// (true = answer the runtime-gone 404): a test can dictate stale episodes
	// and healthy connections instead of relying on the stub's current id.
	dialScript []bool
	live       int
	peakLive   int
}

func newWSGoneStub(t *testing.T, runtimeID string) (*httptest.Server, *wsGoneStub) {
	t.Helper()
	stub := &wsGoneStub{runtimeID: runtimeID}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/daemon/register", func(w http.ResponseWriter, r *http.Request) {
		stub.mu.Lock()
		stub.registers++
		if stub.rotate {
			stub.runtimeID += "-next"
		}
		id := stub.runtimeID
		stub.mu.Unlock()
		writeJSONStub(w, 200, map[string]any{
			"runtimes": []map[string]string{{"id": id, "name": "foreman-omp", "provider": "omp", "status": "online"}},
			"repos":    []map[string]string{}, "repos_version": "v1", "settings": nil,
		})
	})
	mux.HandleFunc("/api/daemon/ws", func(w http.ResponseWriter, r *http.Request) {
		want := r.URL.Query().Get("runtime_ids")
		stub.mu.Lock()
		stub.dials = append(stub.dials, want)
		accept := stub.accept && want == stub.runtimeID
		if idx := len(stub.dials) - 1; idx < len(stub.dialScript) {
			accept = !stub.dialScript[idx]
		}
		if accept {
			stub.live++
			if stub.live > stub.peakLive {
				stub.peakLive = stub.live
			}
		}
		stub.mu.Unlock()
		if !accept {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"runtime not found"}`))
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "closing")
		defer func() {
			stub.mu.Lock()
			stub.live--
			stub.mu.Unlock()
		}()
		_, _, _ = conn.Read(r.Context()) // hold until the peer goes away
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, stub
}

// liveConnections / peakConnections report the observed connection counts.
func (s *wsGoneStub) liveConnections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.live
}

func (s *wsGoneStub) peakConnections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peakLive
}

// setAccept lets a dial for the current id succeed (the "row dialable again"
// shape a parked provider must come back from).
func (s *wsGoneStub) setAccept(accept bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accept = accept
}

// setRotate makes every register answer a fresh id (the "id moves on every C1"
// shape the design leaves unverified).
func (s *wsGoneStub) setRotate(rotate bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rotate = rotate
}

// setDialScript scripts the handshake per dial, in dial order (true = the
// runtime-gone 404), so a test can interleave stale episodes and healthy
// connections.
func (s *wsGoneStub) setDialScript(script []bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dialScript = append([]bool(nil), script...)
}

func (s *wsGoneStub) summary() (dials []string, registers int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.dials...), s.registers
}

func (s *wsGoneStub) setRuntimeID(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runtimeID = id
}

// TestWSSubscriberParksWhenRuntimeCannotBeRebuilt pins blocking finding #2: a
// handshake 404 that re-registering cannot move must not re-dial the same id in
// a loop — the stream backs off, then parks until the runtime set changes.
func TestWSSubscriberParksWhenRuntimeCannotBeRebuilt(t *testing.T) {
	srv, stub := newWSGoneStub(t, "rid-real")
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	drainChanges(client)
	sub := NewWSSubscriber(client, func() {}, nil)
	sub.dialTimeout = 50 * time.Millisecond
	sub.waitSteps = []time.Duration{time.Millisecond}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()

	// The id never moves, so the dials must stop after the stale-rebuild limit
	// instead of running away.
	waitFor(t, "the stream to park", func() bool {
		dials, _ := stub.summary()
		return len(dials) >= staleRebuildLimit
	})
	time.Sleep(200 * time.Millisecond)
	dials, registers := stub.summary()
	if len(dials) > staleRebuildLimit {
		t.Fatalf("dials = %d (%v), want the stream to park at %d", len(dials), dials, staleRebuildLimit)
	}
	if registers > staleRebuildLimit+1 {
		t.Errorf("register calls = %d, want at most one per stale rebuild", registers)
	}
}

// TestWSSubscriberFollowsTheRebuiltRuntimeID pins the other half: when the row
// does move, the stream reconnects with the fresh id.
func TestWSSubscriberFollowsTheRebuiltRuntimeID(t *testing.T) {
	srv, stub := newWSGoneStub(t, "rid-real")
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	drainChanges(client)
	sub := NewWSSubscriber(client, func() {}, nil)
	sub.dialTimeout = 50 * time.Millisecond
	sub.waitSteps = []time.Duration{time.Millisecond}

	// The row dies and the rebuild answers a fresh id: the stream must retire
	// and reconnect under it, not keep dialing the dead one.
	stub.setRuntimeID("rid-real-2")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()

	waitFor(t, "a dial against the rebuilt id", func() bool {
		dials, _ := stub.summary()
		for _, got := range dials {
			if got == "rid-real-2" {
				return true
			}
		}
		return false
	})
}

// TestWSSubscriberKeepsOneConnectionPerRuntimeOnIDMove pins blocking finding
// #1: while a stream is already serving, a runtime-id move must leave exactly
// one live connection per provider — the replaced stream's late outcome must
// not unregister its replacement (which would orphan the new cancel and stack a
// second, unreclaimable connection per move).
func TestWSSubscriberKeepsOneConnectionPerRuntimeOnIDMove(t *testing.T) {
	srv, stub := newWSGoneStub(t, "rid-real")
	stub.setAccept(true)
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	drainChanges(client)
	sub := NewWSSubscriber(client, func() {}, nil)
	sub.dialTimeout = 50 * time.Millisecond
	sub.waitSteps = []time.Duration{time.Millisecond}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()

	// The first stream is up and serving.
	waitFor(t, "the first connection", func() bool { return stub.liveConnections() == 1 })

	// A move driven by another component (what a heartbeat/claim 404 does).
	firstMove := len(stub.dials)
	stub.mu.Lock()
	stub.runtimeID = "rid-real-2"
	stub.mu.Unlock()
	if _, err := client.rebuildRuntime(t.Context(), "rid-real"); err != nil {
		t.Fatalf("rebuildRuntime: %v", err)
	}
	waitFor(t, "the replacement connection", func() bool {
		dials, _ := stub.summary()
		return len(dials) > firstMove && client.RuntimeIDs()[0] == "rid-real-2" && stub.liveConnections() == 1
	})

	// A second move must not stack another connection either.
	firstMove = len(stub.dials)
	stub.mu.Lock()
	stub.runtimeID = "rid-real-3"
	stub.mu.Unlock()
	if _, err := client.rebuildRuntime(t.Context(), "rid-real-2"); err != nil {
		t.Fatalf("rebuildRuntime: %v", err)
	}
	waitFor(t, "the second replacement connection", func() bool {
		dials, _ := stub.summary()
		return len(dials) > firstMove && stub.liveConnections() == 1
	})
	time.Sleep(200 * time.Millisecond)
	if live := stub.liveConnections(); live != 1 {
		t.Fatalf("live connections = %d, want exactly one per provider (§1.2 C-20)", live)
	}
	if peak := stub.peakConnections(); peak != 1 {
		t.Fatalf("peak connections = %d, want no overlap between the old and the new stream", peak)
	}
	dials, _ := stub.summary()
	if len(dials) != 3 {
		t.Fatalf("dials = %v, want one per id (no runaway stream)", dials)
	}
}

// TestWSSubscriberRateLimitsRepeatedIDMoves pins finding #2: when the id moves
// on every register, the retire path takes the same §1.5 rate limit and budget as
// the stale path instead of dialing + registering at RTT speed.
func TestWSSubscriberRateLimitsRepeatedIDMoves(t *testing.T) {
	srv, stub := newWSGoneStub(t, "rid-real")
	stub.mu.Lock()
	stub.rotate = true
	stub.mu.Unlock()
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	drainChanges(client)
	sub := NewWSSubscriber(client, func() {}, nil)
	sub.dialTimeout = 50 * time.Millisecond
	sub.waitSteps = []time.Duration{time.Millisecond}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()

	waitFor(t, "the stream to give up", func() bool {
		dials, _ := stub.summary()
		return len(dials) >= staleRebuildLimit
	})
	// The parked stream's own rebuild (a register) may still be in flight when
	// the last dial is observed: let it land before measuring, then require the
	// counts to freeze over the next window.
	time.Sleep(300 * time.Millisecond)
	dials, registers := stub.summary()
	time.Sleep(300 * time.Millisecond)
	after, afterRegisters := stub.summary()
	// The bound: parking stops the loop instead of dialing at RTT speed. One
	// extra dial can slip in from a stream that was already in flight.
	if len(after) > staleRebuildLimit+1 {
		t.Fatalf("dials = %d (%v), want the moves to stop at the stale-rebuild limit", len(after), after)
	}
	if len(after) != len(dials) || afterRegisters != registers {
		t.Fatalf("still churning after parking: dials %d -> %d, registers %d -> %d",
			len(dials), len(after), registers, afterRegisters)
	}
}

// TestWSSubscriberRearmsAParkedRuntime pins finding #5: parking is bounded — a
// runtime that becomes dialable again under the same id reconnects after the
// park window instead of staying at zero connections until a restart.
func TestWSSubscriberRearmsAParkedRuntime(t *testing.T) {
	srv, stub := newWSGoneStub(t, "rid-real")
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	drainChanges(client)
	sub := NewWSSubscriber(client, func() {}, nil)
	sub.dialTimeout = 50 * time.Millisecond
	sub.waitSteps = []time.Duration{time.Millisecond}
	sub.parkRetry = 100 * time.Millisecond
	sub.now = time.Now

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()

	parked := func() int {
		dials, _ := stub.summary()
		return len(dials)
	}
	waitFor(t, "the stream to park", func() bool { return parked() >= staleRebuildLimit })
	// The row becomes dialable again under the same id.
	stub.setAccept(true)
	time.Sleep(60 * time.Millisecond)
	if stub.liveConnections() != 0 {
		t.Fatal("the provider must stay parked until the retry window expires")
	}
	waitFor(t, "the park retry to reconnect", func() bool { return stub.liveConnections() == 1 })
}

// gateMetrics holds the first WS disconnect of a test at the metrics frame: it
// is the step a replaced stream passes right before it reports its outcome, so
// holding it lets the test place that stale outcome exactly where it wants it.
type gateMetrics struct {
	fakeMetrics
	once    sync.Once
	held    chan struct{}
	release chan struct{}
}

func newGateMetrics() *gateMetrics {
	return &gateMetrics{held: make(chan struct{}), release: make(chan struct{})}
}

func (m *gateMetrics) WSDisconnect(side string) {
	m.fakeMetrics.WSDisconnect(side)
	first := false
	m.once.Do(func() { first = true })
	if !first {
		return
	}
	close(m.held)
	select {
	case <-m.release:
	case <-time.After(10 * time.Second): // never wedge the suite if this frame moves
	}
}

// TestWSSubscriberKeepsOneConnectionWhenTheIDComesBack pins the identity rule:
// an outcome names the stream *instance*, never its (provider, id). When an id
// moves away and back (A→B→A) the replaced stream still names id A, and a
// (provider, id) match would unregister the fresh stream for A, drop its cancel
// and leave the provider with a second, unreclaimable connection (C-20).
func TestWSSubscriberKeepsOneConnectionWhenTheIDComesBack(t *testing.T) {
	srv, stub := newWSGoneStub(t, "rid-a")
	stub.setAccept(true)
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	drainChanges(client)
	gate := newGateMetrics()
	sub := NewWSSubscriber(client, func() {}, gate)
	sub.dialTimeout = 50 * time.Millisecond
	sub.waitSteps = []time.Duration{time.Millisecond}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()

	dials := func() []string {
		got, _ := stub.summary()
		return got
	}
	// The first stream serves id A. The move to B replaces it, and the gate
	// holds the replaced stream at its disconnect frame, so its outcome is
	// still pending when the id comes back.
	waitFor(t, "the first connection", func() bool { return stub.liveConnections() == 1 })
	stub.setRuntimeID("rid-b")
	if _, err := client.rebuildRuntime(t.Context(), "rid-a"); err != nil {
		t.Fatalf("rebuildRuntime(rid-a): %v", err)
	}
	waitFor(t, "the replacement connection for rid-b", func() bool {
		got := dials()
		return len(got) == 2 && got[1] == "rid-b" && stub.liveConnections() == 1
	})
	select {
	case <-gate.held:
	case <-time.After(5 * time.Second):
		t.Fatal("the replaced stream never reached its disconnect frame")
	}

	// Back to A: a fresh stream takes the id the replaced stream still names.
	stub.setRuntimeID("rid-a")
	if _, err := client.rebuildRuntime(t.Context(), "rid-b"); err != nil {
		t.Fatalf("rebuildRuntime(rid-b): %v", err)
	}
	waitFor(t, "the connection under the earlier id", func() bool {
		got := dials()
		return len(got) == 3 && got[2] == "rid-a" && stub.liveConnections() == 1
	})

	// Release the replaced stream: its outcome names (omp, rid-a) and belongs
	// to a stream that is gone, so it must be dropped on identity.
	close(gate.release)
	time.Sleep(300 * time.Millisecond)
	if live := stub.liveConnections(); live != 1 {
		t.Fatalf("live connections = %d, want exactly one per provider (§1.2 C-20)", live)
	}
	if got := dials(); len(got) != 3 {
		t.Fatalf("dials = %v, want one per id move and no stacked stream", got)
	}
}

// TestWSSubscriberResetsTheStaleBudgetOnAHealthyConnection pins the budget's
// scope: staleRebuildLimit bounds *consecutive* rebuild failures. A healthy
// connection ends the streak, so stale episodes separated by healthy
// connections must never park a runtime whose connection is fine — only three
// failures in a row do.
func TestWSSubscriberResetsTheStaleBudgetOnAHealthyConnection(t *testing.T) {
	srv, stub := newWSGoneStub(t, "rid-a")
	stub.setAccept(true)
	stub.setRotate(true) // every rebuild moves the id: one stale episode per move
	stub.setDialScript([]bool{true, false, true, true, false})
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	drainChanges(client)
	sub := NewWSSubscriber(client, func() {}, nil)
	sub.dialTimeout = 50 * time.Millisecond
	sub.waitSteps = []time.Duration{time.Millisecond}
	sub.parkRetry = time.Hour // a park must not heal on its own inside the test

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()

	// Dial 1 is stale, dial 2 is a healthy connection: the streak is over
	// before the next episode starts.
	waitFor(t, "the healthy connection after the first stale episode", func() bool {
		d, _ := stub.summary()
		return len(d) == 2 && stub.liveConnections() == 1
	})
	// Move the id out from under the healthy connection: two stale dials follow,
	// the second of which is the third episode of this runtime's life.
	if _, err := client.rebuildRuntime(t.Context(), client.RuntimeIDs()[0]); err != nil {
		t.Fatalf("rebuildRuntime: %v", err)
	}
	// With a reset streak both stale dials retire into a fresh, healthy
	// connection; a lifetime-cumulative budget parks the runtime at the third
	// failure instead.
	waitFor(t, "the healthy connection after the consecutive stale dials", func() bool {
		d, _ := stub.summary()
		return len(d) >= 5 && stub.liveConnections() == 1
	})
	// One more move must still be served. A parked provider is skipped until its
	// retry window expires — an hour here — so a parked runtime never comes back.
	if _, err := client.rebuildRuntime(t.Context(), client.RuntimeIDs()[0]); err != nil {
		t.Fatalf("rebuildRuntime: %v", err)
	}
	waitFor(t, "the reconnect after the final move", func() bool {
		d, _ := stub.summary()
		return len(d) >= 6 && stub.liveConnections() == 1
	})
}

// TestCancelledStreamDoesNotSpendTheRuntimeProgression pins who owns the shared
// §1.5 progression: a stream that was replaced (its ctx cancelled) is gone, so
// the next dial's wait must still be the progression's first step — it belongs
// to the replacement, not to the dead stream.
func TestCancelledStreamDoesNotSpendTheRuntimeProgression(t *testing.T) {
	srv, stub := newWSGoneStub(t, "rid-real")
	stub.setAccept(true)
	client := NewClient(testConfig(srv.URL), "mdt_test")
	ref := RuntimeRef{ID: "rid-real", Provider: "omp", Name: "foreman-omp"}
	client.refs = []RuntimeRef{ref}
	metrics := &fakeMetrics{}
	sub := NewWSSubscriber(client, func() {}, metrics)
	sub.dialTimeout = 50 * time.Millisecond
	sub.waitSteps = []time.Duration{7 * time.Millisecond, 9 * time.Millisecond}
	state := &runtimeStreamState{backoff: sub.newReconnectBackoff()}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan streamOutcome, 1)
	closed := make(chan struct{})
	var closedOnce sync.Once
	onClosed := func() { closedOnce.Do(func() { close(closed) }) }
	go func() { done <- sub.iterate(ctx, ref, state, onClosed) }()

	waitFor(t, "the connection", func() bool { return stub.liveConnections() == 1 })
	cancel() // what reconcile does when it replaces a stream
	select {
	case got := <-done:
		if got != streamDone {
			t.Fatalf("outcome = %v, want streamDone for a cancelled stream", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the cancelled stream never finished")
	}
	if wait := state.nextBackoff(); wait != 7*time.Millisecond {
		t.Fatalf("the next reconnect wait = %s, want the progression's first step: the cancelled stream must not spend it", wait)
	}
	// A cancelled stream's closed connection is what arms its replacement.
	select {
	case <-closed:
	default:
		t.Fatal("a cancelled stream must report its connection closed")
	}
	if got := len(metrics.wsReconnect); got != 0 {
		t.Fatalf("WSReconnect counted %d shutdown, want none", got)
	}
}

// TestWSProfileChangeFrameLiteral pins the wire name of the §1.6 trigger: it is
// an upstream protocol string, so the test spells it out instead of reusing the
// constant both sides are built from — a typo in the constant must fail here.
func TestWSProfileChangeFrameLiteral(t *testing.T) {
	if got := FrameDaemonRuntimeProfilesChanged; got != "daemon:runtime_profiles_changed" {
		t.Fatalf("profile change frame = %q, want the upstream literal daemon:runtime_profiles_changed", got)
	}

	srv, stub := newCombinedStub(t)
	client := NewClient(testConfig(srv.URL), "mdt_test")
	registerDeclared(t, client)
	notified := make(chan struct{}, 4)
	sub := NewWSSubscriber(client, func() {}, nil,
		WithProfileChangeNotify(func() { notified <- struct{}{} }))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()
	waitFor(t, "ws dial", func() bool { return stub.dialCount() == 1 })

	payload, _ := json.Marshal(map[string]string{"workspace_id": "ws-1"})
	stub.push(t, wsFrame{Type: "daemon:runtime_profiles_changed", Payload: payload})
	select {
	case <-notified:
	case <-time.After(5 * time.Second):
		t.Fatal("the literal frame name did not trigger the refresh")
	}
}
