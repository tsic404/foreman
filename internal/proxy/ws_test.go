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
	// Non-fmj_ credential: the routing rules (§1.2 rule 2) are
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
	// The ack payload is pinned: exactly runtime_id + status=ok (§1.2 S30).
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
	sub := NewWSSubscriber(client, func() { wakeCh <- struct{}{} }, &metrics)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = sub.Run(ctx) }()

	// C1 runs before the WS dial (ADR-010 startup order) and runtime_ids is
	// the real runtime id (mandatory, §1.1 C20).
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

	// A dropped connection reconnects with backoff (§4/ADR-010).
	stub.closeConn()
	waitFor(t, "ws reconnect", func() bool { return stub.dialCount() >= 2 })
	waitFor(t, "ws.reconnect metric", func() bool { return metrics.reconnectCount(WSSideServer) >= 1 })
}

// TestWSSubscriberHandshake404BacksOff: a persistent handshake 404 must not
// spin a re-registration storm — the subscriber waits out the §4 backoff.
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

// TestWSSubscriberDialTimeout: a peer that accepts TCP but never answers
// the upgrade must not wedge the subscriber (§4 control-plane timeout).
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
	// Register against the hanging server is impossible; seed the runtime id
	// directly so the subscriber proceeds to the dial.
	client.runtimeID = "rid-real"
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
