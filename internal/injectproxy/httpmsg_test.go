package injectproxy

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func parseRequest(t *testing.T, raw string) *messageHead {
	t.Helper()
	head, err := parseRequestHead([]byte(raw))
	if err != nil {
		t.Fatalf("parseRequestHead(%q): %v", raw, err)
	}
	return head
}

func TestParseRequestHead(t *testing.T) {
	req := parseRequest(t, "POST /api/daemon/tasks/claim?runtime_ids=r-1 HTTP/1.1\r\n"+
		"Host: test-multica-access\r\n"+
		"Authorization: Bearer mdt_secretvalue\r\n"+
		"X-Client-Capabilities: skill-bundles-v1,coalesced-comments-v1,rpc-v1\r\n"+
		"Content-Length: 17\r\n"+
		"\r\n")

	if req.method != "POST" || req.proto != "HTTP/1.1" {
		t.Fatalf("request line = %s %s %s", req.method, req.target, req.proto)
	}
	if got := req.requestPath(); got != "/api/daemon/tasks/claim" {
		t.Fatalf("path = %q, want the query stripped", got)
	}
	if got := req.first("host"); got != "test-multica-access" {
		t.Fatalf("host = %q", got)
	}
	if got := req.first("X-Client-Capabilities"); got != "skill-bundles-v1,coalesced-comments-v1,rpc-v1" {
		t.Fatalf("capabilities = %q", got)
	}
	if got := authPrefix(req); got != "mdt_" {
		t.Fatalf("credential prefix = %q, want mdt_", got)
	}
	if got := directionFor(authPrefix(req)); got != directionForemanToServer {
		t.Fatalf("direction = %q, want %q", got, directionForemanToServer)
	}
	if !req.keepAlive() {
		t.Fatal("HTTP/1.1 without Connection: close must keep the connection")
	}
	framing, length, err := req.requestFraming()
	if err != nil || framing != bodyContentLength || length != 17 {
		t.Fatalf("framing = %v/%d/%v, want content-length 17", framing, length, err)
	}
	if req.isWebSocket() {
		t.Fatal("plain request classified as a WS upgrade")
	}
	if !bytes.Equal(req.raw, []byte("POST /api/daemon/tasks/claim?runtime_ids=r-1 HTTP/1.1\r\n"+
		"Host: test-multica-access\r\n"+
		"Authorization: Bearer mdt_secretvalue\r\n"+
		"X-Client-Capabilities: skill-bundles-v1,coalesced-comments-v1,rpc-v1\r\n"+
		"Content-Length: 17\r\n"+
		"\r\n")) {
		t.Fatalf("raw head was not kept verbatim: %q", req.raw)
	}
}

func TestParseRequestHeadVariants(t *testing.T) {
	// Absolute-form target, no credential, HTTP/1.0, chunked body.
	req := parseRequest(t, "POST http://test-multica-access/api/daemon/ws?x=1 HTTP/1.0\r\n"+
		"Transfer-Encoding: chunked\r\n"+
		"Connection: keep-alive\r\n"+
		"\r\n")
	if got := req.requestPath(); got != "/api/daemon/ws" {
		t.Fatalf("absolute-form path = %q", got)
	}
	if got := authPrefix(req); got != prefixOther {
		t.Fatalf("credential prefix = %q, want %q", got, prefixOther)
	}
	if got := directionFor(authPrefix(req)); got != directionAgentToServer {
		t.Fatalf("direction = %q, want %q", got, directionAgentToServer)
	}
	if !req.keepAlive() {
		t.Fatal("HTTP/1.0 with Connection: keep-alive must keep the connection")
	}
	if framing, _, err := req.requestFraming(); err != nil || framing != bodyChunked {
		t.Fatalf("framing = %v/%v, want chunked", framing, err)
	}

	// WS upgrade with Expect and a close request.
	ws := parseRequest(t, "GET /api/daemon/ws?runtime_ids=r-1 HTTP/1.1\r\n"+
		"Connection: keep-alive, Upgrade\r\n"+
		"upgrade: WebSocket\r\n"+
		"Expect: 100-continue\r\n"+
		"Authorization: Bearer mat_tasktoken\r\n"+
		"\r\n")
	if !ws.isWebSocket() || !ws.hasUpgrade() {
		t.Fatalf("WS upgrade not detected: %+v", ws.headers)
	}
	if !ws.expectContinue() {
		t.Fatal("Expect: 100-continue not detected")
	}
	if got := authPrefix(ws); got != "mat_" {
		t.Fatalf("credential prefix = %q, want mat_", got)
	}

	closing := parseRequest(t, "GET /x HTTP/1.1\r\nConnection: close\r\n\r\n")
	if closing.keepAlive() {
		t.Fatal("Connection: close must end the connection")
	}
}

func TestParseRequestHeadRejectsMalformed(t *testing.T) {
	for name, raw := range map[string]string{
		"no protocol":        "GET /x\r\n\r\n",
		"bad header line":    "GET /x HTTP/1.1\r\nnot-a-header\r\n\r\n",
		"empty head":         "",
		"negative length":    "POST /x HTTP/1.1\r\nContent-Length: -1\r\n\r\n",
		"non-numeric length": "POST /x HTTP/1.1\r\nContent-Length: 12a\r\n\r\n",
	} {
		head, err := parseRequestHead([]byte(raw))
		if err == nil {
			if _, _, ferr := head.requestFraming(); ferr == nil {
				t.Fatalf("%s: expected an error", name)
			}
		}
	}
}

func TestParseResponseHeadFraming(t *testing.T) {
	for name, tc := range map[string]struct {
		raw     string
		method  string
		framing bodyFraming
		length  int64
	}{
		"content-length": {"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n", "GET", bodyContentLength, 5},
		"chunked":        {"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n", "GET", bodyChunked, 0},
		"until close":    {"HTTP/1.1 200 OK\r\n\r\n", "GET", bodyUntilClose, 0},
		"204":            {"HTTP/1.1 204 No Content\r\n\r\n", "GET", bodyNone, 0},
		"304":            {"HTTP/1.1 304 Not Modified\r\nContent-Length: 5\r\n\r\n", "GET", bodyNone, 0},
		"interim":        {"HTTP/1.1 100 Continue\r\n\r\n", "POST", bodyNone, 0},
		"head request":   {"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n", "HEAD", bodyNone, 0},
	} {
		head, err := parseResponseHead([]byte(tc.raw))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		framing, length, err := head.responseFraming(tc.method)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if framing != tc.framing || length != tc.length {
			t.Fatalf("%s: framing = %v/%d, want %v/%d", name, framing, length, tc.framing, tc.length)
		}
	}

	head, err := parseResponseHead([]byte("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if head.status != 503 || head.reason != "Service Unavailable" {
		t.Fatalf("status line = %d %q", head.status, head.reason)
	}
	if framing, length, err := head.responseFraming("POST"); err != nil || framing != bodyContentLength || length != 0 {
		t.Fatalf("zero-length response framing = %v/%d/%v, want content-length 0", framing, length, err)
	}
}

func TestReadRequestHeadKeepsStrayBlankLines(t *testing.T) {
	r := bufio.NewReaderSize(strings.NewReader("\r\n\r\nGET /x HTTP/1.1\r\nHost: h\r\n\r\n"), 16)
	leading, head, err := readRequestHead(r)
	if err != nil {
		t.Fatalf("readRequestHead: %v", err)
	}
	if string(leading) != "\r\n\r\n" {
		t.Fatalf("leading bytes = %q, want the two stray blank lines", leading)
	}
	req, err := parseRequestHead(head)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if req.method != "GET" || req.target != "/x" {
		t.Fatalf("head = %q", head)
	}
}

func TestReadLineHandlesLongLines(t *testing.T) {
	long := strings.Repeat("a", 3*readerBufferSize)
	r := bufio.NewReaderSize(strings.NewReader("X-Long: "+long+"\r\n\r\n"), readerBufferSize)
	head, err := readHead(r)
	if err != nil {
		t.Fatalf("readHead: %v", err)
	}
	req, err := parseRequestHead([]byte("GET /x HTTP/1.1\r\n" + string(head)))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := req.first("X-Long"); got != long {
		t.Fatalf("long header value length = %d, want %d", len(got), len(long))
	}
}

func TestReadHeadRefusesOversizedHeads(t *testing.T) {
	huge := strings.Repeat("X-Filler: "+strings.Repeat("b", 1024)+"\r\n", maxHeadBytes/1024+2)
	r := bufio.NewReaderSize(strings.NewReader(huge), readerBufferSize)
	if _, err := readHead(r); err == nil {
		t.Fatal("oversized head accepted")
	}
}

// TestReadHeadRefusesAnUnterminatedLine is the memory-bound case: one line
// that never ends must be refused while it arrives, not after the whole head
// has been accumulated.
func TestReadHeadRefusesAnUnterminatedLine(t *testing.T) {
	long := strings.Repeat("a", maxHeadBytes+1)
	r := bufio.NewReaderSize(strings.NewReader("GET /x HTTP/1.1\r\nX-Long: "+long), readerBufferSize)
	_, err := readHead(r)
	if !errors.Is(err, errLineTooLong) {
		t.Fatalf("err = %v, want %v", err, errLineTooLong)
	}
}

// TestReadLineRefusesLongChunkFraming covers the same cap on the chunked-body
// path: a chunk-size line is framing, not payload.
func TestReadLineRefusesLongChunkFraming(t *testing.T) {
	long := strings.Repeat("a", maxChunkLineBytes+1)
	src := bufio.NewReaderSize(strings.NewReader(long+"\r\n"), readerBufferSize)
	rec, _ := testRecorder(t)
	sink := rec.NewBodySink()
	var dst bytes.Buffer
	err := relayBody(&dst, src, bodyChunked, 0, sink, make([]byte, copyBufferSize))
	if !errors.Is(err, errLineTooLong) {
		t.Fatalf("err = %v, want %v", err, errLineTooLong)
	}
	sink.Close()
}

func TestRelayBodyContentLength(t *testing.T) {
	rec, _ := testRecorder(t)
	sink := rec.NewBodySink()
	body := []byte(`{"probe":"transparency"}`)
	src := bufio.NewReader(strings.NewReader(string(body) + "TRAILING"))
	var dst bytes.Buffer

	if err := relayBody(&dst, src, bodyContentLength, int64(len(body)), sink, make([]byte, copyBufferSize)); err != nil {
		t.Fatalf("relayBody: %v", err)
	}
	if dst.String() != string(body) {
		t.Fatalf("relayed %q, want %q", dst.String(), body)
	}
	if rest, _ := src.ReadString('g'); rest != "TRAILING" {
		t.Fatalf("relay consumed past the body: %q", rest)
	}
	sha, rel := sink.Close()
	sum := sha256.Sum256(body)
	if sha != hex.EncodeToString(sum[:]) || rel == "" {
		t.Fatalf("sink = %q/%q", sha, rel)
	}
}

func TestRelayBodyChunkedKeepsRawBytesAndDecodesForTheRecord(t *testing.T) {
	rec, dir := testRecorder(t)
	sink := rec.NewBodySink()
	raw := "5\r\nhello\r\n6\r\n world\r\n0\r\nX-Trailer: v\r\n\r\n"
	src := bufio.NewReader(strings.NewReader(raw + "NEXT"))
	var dst bytes.Buffer

	if err := relayBody(&dst, src, bodyChunked, 0, sink, make([]byte, copyBufferSize)); err != nil {
		t.Fatalf("relayBody: %v", err)
	}
	if dst.String() != raw {
		t.Fatalf("relayed %q, want the raw framing %q", dst.String(), raw)
	}
	if rest, _ := src.ReadString('T'); rest != "NEXT" {
		t.Fatalf("relay consumed past the body: %q", rest)
	}
	sha, rel := sink.Close()
	sum := sha256.Sum256([]byte("hello world"))
	if sha != hex.EncodeToString(sum[:]) {
		t.Fatalf("decoded body sha = %s, want %s", sha, hex.EncodeToString(sum[:]))
	}
	stored, err := readRecordFile(dir, rel)
	if err != nil {
		t.Fatalf("read recorded body: %v", err)
	}
	if stored != "hello world" {
		t.Fatalf("recorded body = %q, want the decoded payload", stored)
	}
}

func TestRelayBodyRejectsBadChunkSize(t *testing.T) {
	rec, _ := testRecorder(t)
	sink := rec.NewBodySink()
	src := bufio.NewReader(strings.NewReader("zz\r\nhello\r\n0\r\n\r\n"))
	var dst bytes.Buffer
	if err := relayBody(&dst, src, bodyChunked, 0, sink, make([]byte, copyBufferSize)); err == nil {
		t.Fatal("malformed chunk size accepted")
	}
	sink.Close()
}

func TestDrainBufferedForwardsAndRecords(t *testing.T) {
	r := bufio.NewReaderSize(strings.NewReader("buffered bytes"), 64)
	if _, err := r.Peek(14); err != nil {
		t.Fatalf("peek: %v", err)
	}
	var dst bytes.Buffer
	var recorded [][]byte
	if err := drainBuffered(r, &dst, func(b []byte) { recorded = append(recorded, append([]byte(nil), b...)) }); err != nil {
		t.Fatalf("drainBuffered: %v", err)
	}
	if dst.String() != "buffered bytes" {
		t.Fatalf("forwarded %q", dst.String())
	}
	if len(recorded) != 1 || string(recorded[0]) != "buffered bytes" {
		t.Fatalf("recorded %q", recorded)
	}
	if r.Buffered() != 0 {
		t.Fatalf("reader still holds %d bytes", r.Buffered())
	}
}

func TestWriteSyntheticStatus(t *testing.T) {
	var buf bytes.Buffer
	if err := writeSyntheticStatus(&buf, 503, false); err != nil {
		t.Fatalf("writeSyntheticStatus: %v", err)
	}
	if got := buf.String(); got != "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n" {
		t.Fatalf("response = %q", got)
	}

	buf.Reset()
	if err := writeSyntheticStatus(&buf, 500, true); err != nil {
		t.Fatalf("writeSyntheticStatus: %v", err)
	}
	if got := buf.String(); !strings.Contains(got, "HTTP/1.1 500 Internal Server Error\r\n") || !strings.Contains(got, "Connection: close\r\n") {
		t.Fatalf("response = %q", got)
	}
}
