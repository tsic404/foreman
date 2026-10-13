package injectproxy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// awaitRows waits until exactly want rows exist for (path, direction) and
// returns them. Rows are appended after their bytes are forwarded (§4.1 puts
// forwarding first), so a test that has just received a response polls
// instead of assuming the row is already visible.
func (h *harness) awaitRows(path, direction string, want int) []record {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows := h.rowsWith(path, direction)
		if len(rows) >= want {
			return rows
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("%s/%s rows = %d, want %d: %+v", path, direction, len(rows), want, rows)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// awaitKind waits until exactly want rows of one kind exist for a path.
func (h *harness) awaitKind(path, kind string, want int) []record {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows := h.rowsKind(path, kind)
		if len(rows) >= want {
			return rows
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("%s/%s rows = %d, want %d: %+v", path, kind, len(rows), want, rows)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// rowsWith selects the rows of one path and direction; a request row and its
// recorded response body row share the path, so direction is what separates
// them (§4.3).
func (h *harness) rowsWith(path, direction string) []record {
	h.t.Helper()
	var out []record
	for _, row := range h.rows() {
		if row.Path == path && row.Direction == direction {
			out = append(out, row)
		}
	}
	return out
}

// rowsKind selects the rows of one path and kind (http vs ws-chunk).
func (h *harness) rowsKind(path, kind string) []record {
	h.t.Helper()
	var out []record
	for _, row := range h.rows() {
		if row.Path == path && row.Kind == kind {
			out = append(out, row)
		}
	}
	return out
}

// settleWindow bounds a negative assertion's settle: long enough for an
// already-issued upstream write to land, short enough not to slow the suite.
const settleWindow = 150 * time.Millisecond

// awaitNoUpstreamRequests asserts the upstream was never hit. A negative needs
// a settle: the caller's synthetic response says nothing about whether the
// upstream goroutine has finished appending (§4.1's "an injected request never
// reaches the server" is only meaningful once it had the chance to).
func (h *harness) awaitNoUpstreamRequests() {
	h.t.Helper()
	deadline := time.Now().Add(settleWindow)
	for {
		if got := h.upstream.received(); len(got) != 0 {
			h.t.Fatalf("upstream saw %d requests, want none: %q", len(got), got)
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// awaitUpstreamRequests asserts the upstream was hit exactly want times, after
// the same settle.
func (h *harness) awaitUpstreamRequests(want int) {
	h.t.Helper()
	deadline := time.Now().Add(settleWindow)
	for {
		if got := h.upstream.received(); len(got) != want {
			h.t.Fatalf("upstream saw %d requests, want %d: %q", len(got), want, got)
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// awaitNoRows asserts no row matches. Rows are appended after the bytes they
// describe are forwarded, so a row's absence is only evidence once the exchange
// that would have written it is over: same settle as above.
func (h *harness) awaitNoRows(path, direction string) {
	h.t.Helper()
	deadline := time.Now().Add(settleWindow)
	for {
		if rows := h.rowsWith(path, direction); len(rows) != 0 {
			h.t.Fatalf("%s/%s rows = %+v, want none", path, direction, rows)
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// bodyFiles lists the files in the record directory's bodies/ subtree.
func (h *harness) bodyFiles() []string {
	h.t.Helper()
	entries, err := os.ReadDir(filepath.Join(h.dir, bodiesDir))
	if err != nil {
		h.t.Fatalf("read bodies dir: %v", err)
	}
	var out []string
	for _, entry := range entries {
		out = append(out, entry.Name())
	}
	return out
}

func (h *harness) post(path, body string) string {
	h.t.Helper()
	resp, err := http.Post(h.control.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		h.t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("POST %s body: %v", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		h.t.Fatalf("POST %s = %d: %s", path, resp.StatusCode, data)
	}
	return string(data)
}

func (h *harness) state() string {
	h.t.Helper()
	resp, err := http.Get(h.control.URL + "/inject/state")
	if err != nil {
		h.t.Fatalf("GET /inject/state: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return string(data)
}

func sha256Hex(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

func cannedOK(body string) string {
	return fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
}

// TestTransparencyRelaysBytesVerbatim is §4.1's core: every request byte
// reaches the server unchanged, every response byte reaches the caller
// unchanged, and the record face says nothing was injected.
func TestTransparencyRelaysBytesVerbatim(t *testing.T) {
	upstream := startRawUpstream(t, echoRequest(""))
	h := startHarness(t, upstream)

	body := `{"probe":"transparency"}`
	request := "POST /api/daemon/tasks/claim?runtime_ids=r-1&x=2 HTTP/1.1\r\n" +
		"Host: test-multica-access\r\n" +
		"Authorization: Bearer mdt_supersecret1234\r\n" +
		"X-Client-Capabilities: skill-bundles-v1,coalesced-comments-v1,rpc-v1\r\n" +
		"X-Odd-CASE: MiXeD\r\n" +
		"X-Duplicate: one\r\n" +
		"X-Duplicate: two\r\n" +
		fmt.Sprintf("Content-Length: %d\r\n", len(body)) +
		"\r\n" + body
	// The upstream echoes the request verbatim as the response body, so the
	// expected response bytes are known exactly.
	want := fmt.Sprintf("HTTP/1.1 200 OK\r\nX-Upstream: raw\r\nContent-Length: %d\r\n\r\n%s", len(request), request)

	conn := h.dial()
	write(t, conn, request)
	if got := string(readN(t, conn, len(want), 5*time.Second)); got != want {
		t.Fatalf("response bytes differ:\n got %q\nwant %q", got, want)
	}

	received := upstream.received()
	if len(received) != 1 {
		t.Fatalf("upstream saw %d requests, want 1", len(received))
	}
	if string(received[0]) != request {
		t.Fatalf("request bytes differ:\n got %q\nwant %q", received[0], request)
	}

	requests := h.awaitRows("/api/daemon/tasks/claim", directionForemanToServer, 1)
	if len(requests) != 1 {
		t.Fatalf("request rows = %d, want 1: %+v", len(requests), requests)
	}
	row := requests[0]
	if row.Method != "POST" || row.Kind != kindHTTP || row.Status != 200 || row.Injected {
		t.Fatalf("request row = %+v", row)
	}
	if row.AuthorizationPrefix != "mdt_" {
		t.Fatalf("authorization_prefix = %q, want mdt_", row.AuthorizationPrefix)
	}
	if got := row.Headers["X-Client-Capabilities"]; got != "skill-bundles-v1,coalesced-comments-v1,rpc-v1" {
		t.Fatalf("recorded capabilities = %q", got)
	}
	if row.BodySHA256 != sha256Hex(body) || row.BodyPath == "" {
		t.Fatalf("request body record = %q/%q, want sha of %q", row.BodySHA256, row.BodyPath, body)
	}
	if h.blob(row.BodyPath) != body {
		t.Fatalf("recorded request body = %q, want %q", h.blob(row.BodyPath), body)
	}

	responses := h.awaitRows("/api/daemon/tasks/claim", directionServerToForeman, 1)
	if len(responses) != 1 {
		t.Fatalf("response rows = %d, want 1: %+v", len(responses), responses)
	}
	response := responses[0]
	if response.Method != "" || response.Status != 200 {
		t.Fatalf("response row = %+v (method must be omitted)", response)
	}
	if response.BodySHA256 != sha256Hex(request) || response.BodyPath == "" {
		t.Fatalf("response body record = %+v", response)
	}
	if h.blob(response.BodyPath) != request {
		t.Fatal("recorded response body is not the response bytes")
	}

	for _, row := range h.rows() {
		if row.Injected {
			t.Fatalf("zero-injection run recorded an injected row: %+v", row)
		}
	}
}

func TestTransparencyKeepsConnectionReusable(t *testing.T) {
	upstream := startRawUpstream(t, respondWith(cannedOK("ok")))
	h := startHarness(t, upstream)

	conn := h.dial()
	for i := range 2 {
		request := fmt.Sprintf("GET /api/daemon/tasks/t-%d/status HTTP/1.1\r\nHost: h\r\n\r\n", i)
		write(t, conn, request)
		if got := string(readN(t, conn, len(cannedOK("ok")), 5*time.Second)); got != cannedOK("ok") {
			t.Fatalf("exchange %d response = %q", i, got)
		}
	}
	if received := upstream.received(); len(received) != 2 {
		t.Fatalf("upstream saw %d requests, want 2 on one client connection", len(received))
	}
	// §4.1's connection mapping: one client connection, one upstream
	// connection — not one per exchange.
	if conns := upstream.connections(); conns != 1 {
		t.Fatalf("upstream accepted %d connections, want 1", conns)
	}
	h.awaitRows("/api/daemon/tasks/t-1/status", directionAgentToServer, 1)
	if rows := h.rows(); len(rows) != 2 {
		t.Fatalf("rows = %d, want 2: %+v", len(rows), rows)
	}
}

func TestTransparencyRelaysChunkedBodies(t *testing.T) {
	upstream := startRawUpstream(t, respondWith("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n7\r\npayload\r\n0\r\n\r\n"))
	h := startHarness(t, upstream)

	chunkedBody := "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"
	request := "POST /api/daemon/tasks/claim HTTP/1.1\r\n" +
		"Host: h\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"\r\n" + chunkedBody
	response := "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n7\r\npayload\r\n0\r\n\r\n"

	conn := h.dial()
	write(t, conn, request)
	if got := string(readN(t, conn, len(response), 5*time.Second)); got != response {
		t.Fatalf("chunked response bytes differ:\n got %q\nwant %q", got, response)
	}
	received := upstream.received()
	if len(received) != 1 || string(received[0]) != request {
		t.Fatalf("chunked request bytes differ: %q", received)
	}

	responses := h.awaitRows("/api/daemon/tasks/claim", directionServerToForeman, 1)
	if len(responses) != 1 {
		t.Fatalf("response rows = %d, want 1", len(responses))
	}
	// The recorded body is the decoded payload: assertions diff JSON, not
	// chunk framing (§4.3).
	if got := h.blob(responses[0].BodyPath); got != "payload" {
		t.Fatalf("recorded response body = %q, want the decoded payload", got)
	}
	requests := h.awaitRows("/api/daemon/tasks/claim", directionAgentToServer, 1)
	if len(requests) != 1 || h.blob(requests[0].BodyPath) != "hello world" {
		t.Fatalf("recorded request body = %+v", requests)
	}
}

// TestInjectionTerminal5xx mirrors TC-tech-forward-02: the terminal path is
// answered locally during the window, forwarded afterwards, and both rows
// carry the same request body.
func TestInjectionTerminal5xx(t *testing.T) {
	upstream := startRawUpstream(t, respondWith(cannedOK("done")))
	h := startHarness(t, upstream)

	state := h.post("/inject", `{"mode":"terminal-5xx","path_regex":"^/api/daemon/tasks/[^/]+/(complete|fail|cancel-ack)$","status":503,"duration_s":300}`)
	if !strings.Contains(state, `"mode":"terminal-5xx"`) || !strings.Contains(state, `"status":503`) {
		t.Fatalf("injection state = %s", state)
	}

	body := `{"result":"completed"}`
	terminal := "POST /api/daemon/tasks/t-1/complete HTTP/1.1\r\nHost: h\r\n" +
		"Authorization: Bearer mdt_supersecret1234\r\n" +
		fmt.Sprintf("Content-Length: %d\r\n", len(body)) + "\r\n" + body
	injected := fmt.Sprintf("HTTP/1.1 503 %s\r\nContent-Length: 0\r\n\r\n", http.StatusText(503))

	conn := h.dial()
	write(t, conn, terminal)
	if got := string(readN(t, conn, len(injected), 5*time.Second)); got != injected {
		t.Fatalf("injected response = %q, want %q", got, injected)
	}
	h.awaitNoUpstreamRequests()

	rows := h.awaitRows("/api/daemon/tasks/t-1/complete", directionForemanToServer, 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1: %+v", len(rows), rows)
	}
	if !rows[0].Injected || rows[0].Status != 503 || rows[0].BodySHA256 != sha256Hex(body) {
		t.Fatalf("injected row = %+v", rows[0])
	}

	// The window is path-scoped: other traffic keeps flowing.
	claim := "POST /api/daemon/tasks/claim HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\n{}"
	other := h.dial()
	write(t, other, claim)
	if got := string(readN(t, other, len(cannedOK("done")), 5*time.Second)); got != cannedOK("done") {
		t.Fatalf("non-matching request during the window = %q", got)
	}

	// Reset: the same request now reaches the server and keeps its body.
	h.post("/inject/reset", "")
	after := h.dial()
	write(t, after, terminal)
	if got := string(readN(t, after, len(cannedOK("done")), 5*time.Second)); got != cannedOK("done") {
		t.Fatalf("post-reset response = %q", got)
	}
	rows = h.awaitRows("/api/daemon/tasks/t-1/complete", directionForemanToServer, 2)
	if len(rows) != 2 {
		t.Fatalf("rows after reset = %d, want 2", len(rows))
	}
	if rows[1].Injected || rows[1].BodySHA256 != rows[0].BodySHA256 {
		t.Fatalf("post-reset row = %+v, want injected=false and the same body", rows[1])
	}
}

func TestInjectionUpstream5xx(t *testing.T) {
	upstream := startRawUpstream(t, respondWith(cannedOK("passthrough")))
	h := startHarness(t, upstream)

	h.post("/inject", `{"mode":"upstream-5xx","path_regex":"^/api/daemon/tasks/whatever/status$","status":500}`)

	// The S29 probe shape: a caller credential the injector cannot classify.
	body := `{"probe":"s29"}`
	probe := "POST /api/daemon/tasks/whatever/status HTTP/1.1\r\nHost: h\r\n" +
		"Authorization: Bearer notatoken\r\n" +
		fmt.Sprintf("Content-Length: %d\r\n", len(body)) + "\r\n" + body
	injected := fmt.Sprintf("HTTP/1.1 500 %s\r\nContent-Length: 0\r\n\r\n", http.StatusText(500))

	conn := h.dial()
	write(t, conn, probe)
	if got := string(readN(t, conn, len(injected), 5*time.Second)); got != injected {
		t.Fatalf("injected response = %q, want %q", got, injected)
	}
	h.awaitNoUpstreamRequests()
	rows := h.awaitRows("/api/daemon/tasks/whatever/status", directionAgentToServer, 1)
	if len(rows) != 1 || !rows[0].Injected || rows[0].Status != 500 {
		t.Fatalf("injected rows = %+v", rows)
	}
	if rows[0].AuthorizationPrefix != prefixOther {
		t.Fatalf("authorization_prefix = %q, want %q", rows[0].AuthorizationPrefix, prefixOther)
	}
	if rows[0].BodySHA256 != sha256Hex(body) {
		t.Fatalf("body sha = %q", rows[0].BodySHA256)
	}
}

// TestInjectionExpiresByDuration drives the window with the injector's own
// clock instead of wall time: the window is a product behaviour, and a test
// that raced it would flake under load rather than report a defect.
func TestInjectionExpiresByDuration(t *testing.T) {
	upstream := startRawUpstream(t, respondWith(cannedOK("ok")))
	h := startHarness(t, upstream)

	h.post("/inject", `{"mode":"upstream-5xx","path_regex":"^/expiring$","status":500,"duration_s":30}`)
	injected := fmt.Sprintf("HTTP/1.1 500 %s\r\nContent-Length: 0\r\n\r\n", http.StatusText(500))
	conn := h.dial()
	write(t, conn, "GET /expiring HTTP/1.1\r\nHost: h\r\n\r\n")
	if got := string(readN(t, conn, len(injected), 5*time.Second)); got != injected {
		t.Fatalf("response during the window = %q, want %q", got, injected)
	}
	h.awaitNoUpstreamRequests()

	// Past the window: the same request is forwarded again.
	h.clock.advance(31 * time.Second)
	if state := h.state(); !strings.Contains(state, `"mode":"none"`) {
		t.Fatalf("state after the window = %s, want none", state)
	}
	after := h.dial()
	write(t, after, "GET /expiring HTTP/1.1\r\nHost: h\r\n\r\n")
	if got := string(readN(t, after, len(cannedOK("ok")), 5*time.Second)); got != cannedOK("ok") {
		t.Fatalf("response after the window = %q, want it forwarded", got)
	}
	h.awaitUpstreamRequests(1)
}

// TestInjectionExpectContinueCloses covers a request that is waiting for the
// interim response when the fault lands: the injected status answers it and
// the connection ends, because the unread body could not be told apart from
// the next request head.
func TestInjectionExpectContinueCloses(t *testing.T) {
	upstream := startRawUpstream(t, respondWith(cannedOK("ok")))
	h := startHarness(t, upstream)

	h.post("/inject", `{"mode":"terminal-5xx","path_regex":"^/upload$","status":503}`)

	conn := h.dial()
	write(t, conn, "POST /upload HTTP/1.1\r\nHost: h\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\n")
	want := fmt.Sprintf("HTTP/1.1 503 %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", http.StatusText(503))
	if got := string(readN(t, conn, len(want), 5*time.Second)); got != want {
		t.Fatalf("injected response = %q, want %q", got, want)
	}
	if rest := readUntilClose(t, conn, 2*time.Second); len(rest) != 0 {
		t.Fatalf("connection carried %q after the injected response", rest)
	}
	h.awaitNoUpstreamRequests()
}

func TestTransparencyExpectContinueFlow(t *testing.T) {
	upstream := startRawUpstream(t, respondWith(cannedOK("accepted")))
	h := startHarness(t, upstream)

	body := `{"probe":"expect"}`
	conn := h.dial()
	write(t, conn, "POST /api/daemon/tasks/claim HTTP/1.1\r\nHost: h\r\n"+
		"Expect: 100-continue\r\n"+
		fmt.Sprintf("Content-Length: %d\r\n", len(body))+"\r\n")
	interim := "HTTP/1.1 100 Continue\r\n\r\n"
	if got := string(readN(t, conn, len(interim), 5*time.Second)); got != interim {
		t.Fatalf("interim response = %q, want %q", got, interim)
	}
	write(t, conn, body)
	if got := string(readN(t, conn, len(cannedOK("accepted")), 5*time.Second)); got != cannedOK("accepted") {
		t.Fatalf("final response = %q", got)
	}

	received := upstream.received()
	if len(received) != 1 || !strings.HasSuffix(string(received[0]), body) {
		t.Fatalf("upstream did not receive the body after the interim response: %q", received)
	}
	rows := h.awaitRows("/api/daemon/tasks/claim", directionAgentToServer, 1)
	if len(rows) != 1 || rows[0].BodySHA256 != sha256Hex(body) {
		t.Fatalf("rows = %+v", rows)
	}
}

// TestWSDropCoversTunnelsAndUpgrades is TC-tech-ws-01 step 3's mechanism: the
// open tunnel dies, upgrades are refused without an HTTP answer, HTTP keeps
// flowing, and the window's end lets the next upgrade through.
func TestWSDropCoversTunnelsAndUpgrades(t *testing.T) {
	upstream := startRawUpstream(t, wsUpstream)
	h := startHarness(t, upstream)

	upgrade := "GET /api/daemon/ws?runtime_ids=r-1 HTTP/1.1\r\nHost: h\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Authorization: Bearer mdt_supersecret1234\r\n\r\n"
	handshake := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"

	conn := h.dial()
	write(t, conn, upgrade)
	if got := string(readN(t, conn, len(handshake), 5*time.Second)); got != handshake {
		t.Fatalf("handshake = %q, want %q", got, handshake)
	}

	// Push a task_available frame and read it back through the tunnel.
	push := `{"type":"daemon:task_available","payload":{"task_id":"t-1"}}`
	write(t, conn, push)
	if got := string(readN(t, conn, len(push), 5*time.Second)); got != push {
		t.Fatalf("tunnel echo = %q, want %q", got, push)
	}

	// §4.3: one row per byte segment, raw bytes on disk, no frame parsing.
	chunks := h.awaitKind("/api/daemon/ws", kindWSChunk, 2)
	var uplink, downlink []record
	for _, row := range chunks {
		switch row.Direction {
		case directionForemanToServer:
			uplink = append(uplink, row)
		case directionServerToForeman:
			downlink = append(downlink, row)
		default:
			t.Fatalf("ws-chunk row with direction %q", row.Direction)
		}
	}
	if len(uplink) == 0 {
		t.Fatal("no F→S ws-chunk rows")
	}
	if len(downlink) == 0 {
		t.Fatal("no S→F ws-chunk rows")
	}
	for _, row := range chunks {
		if row.BytesPath == "" || row.Method != "" {
			t.Fatalf("ws-chunk row = %+v", row)
		}
	}
	// The tester finds the push by matching raw bytes (T_push in ws-01 step 2).
	found := false
	for _, row := range downlink {
		if strings.Contains(h.blob(row.BytesPath), "task_available") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("no S→F byte segment carries the task_available push")
	}

	// Inject the drop: the established tunnel goes away.
	h.post("/inject", `{"mode":"ws-drop","duration_s":120}`)
	if rest := readUntilClose(t, conn, 5*time.Second); len(rest) != 0 {
		t.Fatalf("dropped tunnel delivered %q", rest)
	}

	// A new upgrade inside the window is refused with a closed connection and
	// no HTTP response at all (§4.2).
	refused := h.dial()
	write(t, refused, upgrade)
	if got := readUntilClose(t, refused, 5*time.Second); len(got) != 0 {
		t.Fatalf("refused upgrade received %q, want no HTTP response", got)
	}
	refusals := h.awaitKind("/api/daemon/ws", kindHTTP, 2)
	if len(refusals) != 2 {
		t.Fatalf("upgrade rows = %+v, want the handshake and the refusal", refusals)
	}
	if refusals[0].Injected || refusals[0].Status != 101 {
		t.Fatalf("handshake row = %+v", refusals[0])
	}
	if !refusals[1].Injected || refusals[1].Status != 0 {
		t.Fatalf("refusal row = %+v, want injected without an HTTP status", refusals[1])
	}
	h.awaitUpstreamRequests(1)

	// HTTP is untouched by the WS window.
	plain := h.dial()
	write(t, plain, "GET /api/daemon/tasks/t-1/status HTTP/1.1\r\nHost: h\r\n\r\n")
	if got := string(readN(t, plain, len(cannedOK("ok")), 5*time.Second)); got != cannedOK("ok") {
		t.Fatalf("HTTP during the ws window = %q", got)
	}

	// After the window the next upgrade succeeds.
	h.post("/inject/reset", "")
	revived := h.dial()
	write(t, revived, upgrade)
	if got := string(readN(t, revived, len(handshake), 5*time.Second)); got != handshake {
		t.Fatalf("post-reset handshake = %q", got)
	}
}

// TestTokensNeverLeaveTheirPrefix is §4.1.4: no credential reaches a record
// file or a log line in clear.
func TestTokensNeverLeaveTheirPrefix(t *testing.T) {
	upstream := startRawUpstream(t, respondWith(cannedOK("ok")))
	h := startHarness(t, upstream)

	daemonToken := "mdt_supersecretvalue1234567890"
	taskToken := "mat_tasktoken0987654321"
	for _, request := range []string{
		"POST /api/daemon/tasks/claim HTTP/1.1\r\nHost: h\r\nAuthorization: Bearer " + daemonToken + "\r\nContent-Length: 2\r\n\r\n{}",
		"GET /api/daemon/tasks/t-1/status HTTP/1.1\r\nHost: h\r\nAuthorization: Bearer " + taskToken + "\r\n\r\n",
	} {
		conn := h.dial()
		write(t, conn, request)
		readN(t, conn, len(cannedOK("ok")), 5*time.Second)
	}

	h.awaitRows("/api/daemon/tasks/claim", directionForemanToServer, 1)
	h.awaitRows("/api/daemon/tasks/t-1/status", directionAgentToServer, 1)
	h.awaitRows("/api/daemon/tasks/claim", directionServerToForeman, 1)
	prefixes := map[string]string{}
	for _, row := range h.rows() {
		if row.Kind != kindHTTP || row.Direction == directionServerToForeman {
			continue
		}
		prefixes[row.Path] = row.AuthorizationPrefix
	}
	if prefixes["/api/daemon/tasks/claim"] != "mdt_" {
		t.Fatalf("claim prefix = %q, want mdt_", prefixes["/api/daemon/tasks/claim"])
	}
	if prefixes["/api/daemon/tasks/t-1/status"] != "mat_" {
		t.Fatalf("status prefix = %q, want mat_", prefixes["/api/daemon/tasks/t-1/status"])
	}

	var files []string
	err := filepath.WalkDir(h.dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk record dir: %v", err)
	}
	if len(files) < 3 {
		t.Fatalf("record dir holds %d files, want records plus bodies", len(files))
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, token := range []string{daemonToken, taskToken} {
			if strings.Contains(string(data), token) {
				t.Fatalf("token leaked into %s", path)
			}
		}
	}
	for _, token := range []string{daemonToken, taskToken} {
		if strings.Contains(h.logs.String(), token) {
			t.Fatalf("token leaked into the log: %s", h.logs.String())
		}
	}
}

// TestResponseBodyFaceIsConfigurationDriven pins §4.3's "the response-body
// face is a deployment setting": the same exchange is recorded or not
// depending on what the configuration says, including "status codes only".
func TestResponseBodyFaceIsConfigurationDriven(t *testing.T) {
	claimOnly := mustRegexp(t, DefaultRecordResponseBodies)
	allTasks := mustRegexp(t, `^/api/daemon/tasks/`)
	for name, tc := range map[string]struct {
		patterns []*regexp.Regexp
		record   map[string]bool
	}{
		"contract default": {
			patterns: []*regexp.Regexp{claimOnly},
			record:   map[string]bool{"/api/daemon/tasks/claim": true, "/api/daemon/tasks/whatever/status": false},
		},
		"widened": {
			patterns: []*regexp.Regexp{allTasks},
			record:   map[string]bool{"/api/daemon/tasks/claim": true, "/api/daemon/tasks/whatever/status": true},
		},
		"status codes only": {
			patterns: nil,
			record:   map[string]bool{"/api/daemon/tasks/claim": false, "/api/daemon/tasks/whatever/status": false},
		},
	} {
		t.Run(name, func(t *testing.T) {
			upstream := startRawUpstream(t, respondWith(cannedOK("body-bytes")))
			patterns := tc.patterns
			h := startHarness(t, upstream, func(c *Config) { c.RecordResponseBodies = patterns })

			for path := range tc.record {
				conn := h.dial()
				write(t, conn, "GET "+path+" HTTP/1.1\r\nHost: h\r\n\r\n")
				readN(t, conn, len(cannedOK("body-bytes")), 5*time.Second)
				// The request row carries the status in every case.
				rows := h.awaitRows(path, directionAgentToServer, 1)
				if len(rows) != 1 || rows[0].Status != 200 {
					t.Fatalf("%s request row = %+v", path, rows)
				}
			}
			for path, want := range tc.record {
				if want {
					rows := h.awaitRows(path, directionServerToForeman, 1)
					if h.blob(rows[0].BodyPath) != "body-bytes" {
						t.Fatalf("%s response body = %q", path, h.blob(rows[0].BodyPath))
					}
					continue
				}
				h.awaitNoRows(path, directionServerToForeman)
			}
		})
	}
}

// TestTransparencyDoesNotRetryOrCache pins the two halves of §4.1 that a
// regression could add silently: a failing upstream is relayed as it is,
// exactly once, and a repeated request is forwarded again rather than answered
// from memory.
func TestTransparencyDoesNotRetryOrCache(t *testing.T) {
	failed := "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 3\r\n\r\nbad"
	upstream := startRawUpstream(t, respondWith(failed))
	h := startHarness(t, upstream)

	request := "POST /api/daemon/tasks/t-1/complete HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\n{}"
	conn := h.dial()
	for i := range 2 {
		write(t, conn, request)
		if got := string(readN(t, conn, len(failed), 5*time.Second)); got != failed {
			t.Fatalf("exchange %d response = %q, want %q", i, got, failed)
		}
	}
	// Two client requests, two upstream requests: a third would be a retry, a
	// single one a cached answer.
	h.awaitRows("/api/daemon/tasks/t-1/complete", directionAgentToServer, 2)
	h.awaitUpstreamRequests(2)
	if conns := upstream.connections(); conns != 1 {
		t.Fatalf("upstream accepted %d connections, want 1", conns)
	}
	for _, row := range h.rowsFor("/api/daemon/tasks/t-1/complete") {
		if row.Status != 500 || row.Injected {
			t.Fatalf("row = %+v, want the 500 relayed as it arrived", row)
		}
	}
}

// TestTransparencyForwardsStrayBlankLines covers the one place a byte-exact
// relay could silently drop input: the blank lines RFC 9112 §2.2 lets a server
// ignore in front of a request line. §4.1 has no exception for them.
func TestTransparencyForwardsStrayBlankLines(t *testing.T) {
	upstream := startRawUpstream(t, respondWith(cannedOK("ok")))
	h := startHarness(t, upstream)

	request := "GET /api/daemon/tasks/t-1/status HTTP/1.1\r\nHost: h\r\n\r\n"
	conn := h.dial()
	write(t, conn, "\r\n"+request)

	// The harness's upstream reads one blank-line-terminated block at a time,
	// so the forwarded CRLF is its own block: byte evidence, not a status.
	deadline := time.Now().Add(5 * time.Second)
	var received [][]byte
	for {
		received = upstream.received()
		if len(received) >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(received) != 2 || string(received[0]) != "\r\n" || string(received[1]) != request {
		t.Fatalf("upstream saw %q, want the stray CRLF then the request", received)
	}
}

// TestNoBodyRequestRecordsEmptyHash covers the record face's empty-body shape
// and its disk footprint: no body, no file — not even an unreferenced one.
func TestNoBodyRequestRecordsEmptyHash(t *testing.T) {
	upstream := startRawUpstream(t, respondWith(cannedOK("ok")))
	h := startHarness(t, upstream)

	conn := h.dial()
	write(t, conn, "GET /api/daemon/tasks/t-1/status HTTP/1.1\r\nHost: h\r\n\r\n")
	readN(t, conn, len(cannedOK("ok")), 5*time.Second)

	rows := h.awaitRows("/api/daemon/tasks/t-1/status", directionAgentToServer, 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
	if rows[0].BodySHA256 != sha256Hex("") || rows[0].BodyPath != "" {
		t.Fatalf("body record = %q/%q, want sha256(\"\") and no file", rows[0].BodySHA256, rows[0].BodyPath)
	}
	if files := h.bodyFiles(); len(files) != 0 {
		t.Fatalf("bodies dir holds %q, want no file for a bodyless request", files)
	}
}

// TestEveryRequestLeavesARow is the record face's forensic promise: a request
// that was seen and never answered (here: no upstream at all) still has a row,
// with its status absent rather than invented.
func TestEveryRequestLeavesARow(t *testing.T) {
	upstream := startRawUpstream(t, respondWith(cannedOK("ok")))

	// A port nothing listens on: the relay cannot reach an upstream.
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadURL := &url.URL{Scheme: "http", Host: dead.Addr().String()}
	dead.Close()
	h := startHarness(t, upstream, func(c *Config) { c.Upstream = deadURL })

	conn := h.dial()
	write(t, conn, "POST /api/daemon/tasks/t-9/complete HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\n{}")
	readUntilClose(t, conn, 5*time.Second)

	rows := h.awaitRows("/api/daemon/tasks/t-9/complete", directionAgentToServer, 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want one row for the unanswered request", rows)
	}
	if rows[0].Status != 0 || rows[0].Injected || rows[0].Method != "POST" {
		t.Fatalf("row = %+v, want the request with no status and no injection", rows[0])
	}
	if files := h.bodyFiles(); len(files) != 0 {
		t.Fatalf("bodies dir holds %q, want no body file for a request that was never sent", files)
	}
}
