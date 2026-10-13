package injectproxy

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Test harness: a scripted raw upstream, the injector's data plane on an
// ephemeral port, its control plane over httptest, and raw TCP clients. Every
// assertion is made on bytes, so transparency is checked at the level §4.1
// talks about.

// rawUpstream is a TCP server that records the bytes of every request it
// receives and answers with whatever the test's handler writes. Its request
// reader is deliberately independent of the injector's parser: a shared bug
// must not make both sides agree.
type rawUpstream struct {
	listener net.Listener

	mu       sync.Mutex
	requests [][]byte
	accepted int

	handler func(conn net.Conn, raw []byte)
	conns   map[net.Conn]struct{}
	wg      sync.WaitGroup
}

func startRawUpstream(t *testing.T, handler func(conn net.Conn, raw []byte)) *rawUpstream {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("upstream listen: %v", err)
	}
	up := &rawUpstream{listener: listener, handler: handler, conns: map[net.Conn]struct{}{}}

	up.wg.Add(1)
	go func() {
		defer up.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			up.mu.Lock()
			up.conns[conn] = struct{}{}
			up.accepted++
			up.mu.Unlock()
			up.wg.Add(1)
			go func() {
				defer up.wg.Done()
				defer conn.Close()
				up.serve(conn)
			}()
		}
	}()

	t.Cleanup(func() {
		listener.Close()
		up.mu.Lock()
		for conn := range up.conns {
			conn.Close()
		}
		up.mu.Unlock()
		up.wg.Wait()
	})
	return up
}

func (up *rawUpstream) serve(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		raw, err := readTestRequest(conn, reader)
		if err != nil {
			return
		}
		up.mu.Lock()
		up.requests = append(up.requests, raw)
		up.mu.Unlock()
		up.handler(conn, raw)
	}
}

func (up *rawUpstream) received() [][]byte {
	up.mu.Lock()
	defer up.mu.Unlock()
	out := make([][]byte, len(up.requests))
	copy(out, up.requests)
	return out
}

// connections reports how many upstream connections were accepted: §4.1 pins
// one upstream connection per client connection, so a relay that redialed per
// request would show up here.
func (up *rawUpstream) connections() int {
	up.mu.Lock()
	defer up.mu.Unlock()
	return up.accepted
}

// readTestRequest reads one request off the wire: head, then the body its
// framing announces. An Expect: 100-continue head is answered with the interim
// response first, exactly like a real server.
func readTestRequest(conn net.Conn, reader *bufio.Reader) ([]byte, error) {
	var raw []byte
	for {
		line, err := reader.ReadString('\n')
		raw = append(raw, line...)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(line) == "" {
			break
		}
	}
	head := string(raw)
	lower := strings.ToLower(head)
	if strings.Contains(lower, "expect: 100-continue") {
		if _, err := io.WriteString(conn, "HTTP/1.1 100 Continue\r\n\r\n"); err != nil {
			return nil, err
		}
	}
	if strings.Contains(lower, "transfer-encoding: chunked") {
		for !bytes.HasSuffix(raw, []byte("0\r\n\r\n")) {
			line, err := reader.ReadString('\n')
			raw = append(raw, line...)
			if err != nil {
				return nil, err
			}
		}
		return raw, nil
	}
	if i := strings.Index(lower, "content-length:"); i >= 0 {
		value := head[i+len("content-length:"):]
		if j := strings.IndexAny(value, "\r\n"); j >= 0 {
			value = value[:j]
		}
		length, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return nil, err
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(reader, body); err != nil {
			return nil, err
		}
		raw = append(raw, body...)
	}
	return raw, nil
}

// respondWith answers every request with one fixed raw response.
func respondWith(response string) func(net.Conn, []byte) {
	return func(conn net.Conn, _ []byte) {
		io.WriteString(conn, response)
	}
}

// echoRequest answers every request with a response whose body is the request
// verbatim: any byte the relay changed shows up in the comparison.
func echoRequest(extraHeaders string) func(net.Conn, []byte) {
	return func(conn net.Conn, raw []byte) {
		head := "HTTP/1.1 200 OK\r\nX-Upstream: raw\r\n" + extraHeaders +
			"Content-Length: " + strconv.Itoa(len(raw)) + "\r\n\r\n"
		io.WriteString(conn, head)
		conn.Write(raw)
	}
}

// wsUpstream answers a WS upgrade with 101 and then echoes every byte back,
// so a tunnel can be exercised in both directions. Plain HTTP requests get a
// canned answer: an HTTP request must not be affected by a WS window.
func wsUpstream(conn net.Conn, raw []byte) {
	if !strings.Contains(strings.ToLower(string(raw)), "upgrade: websocket") {
		io.WriteString(conn, cannedOK("ok"))
		return
	}
	io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
	io.Copy(conn, conn)
}

// harness runs the injector against a raw upstream.
type harness struct {
	t        *testing.T
	cfg      Config
	injector *Injector
	recorder *Recorder
	logs     *bytes.Buffer
	dataAddr string
	control  *httptest.Server
	dir      string
	upstream *rawUpstream
	clock    *testClock
}

// testClock is the harness's time source. It starts at wall time and moves
// only when a test says so, so a duration window is exercised by advancing it
// rather than by racing a real one. The injector gets it before any goroutine
// starts, which is why advancing it is not a data race.
type testClock struct{ nanos atomic.Int64 }

func (c *testClock) now() time.Time          { return time.Unix(0, c.nanos.Load()) }
func (c *testClock) advance(d time.Duration) { c.nanos.Add(int64(d)) }

func startHarness(t *testing.T, upstream *rawUpstream, mutate ...func(*Config)) *harness {
	t.Helper()
	dir := t.TempDir()
	logs := &bytes.Buffer{}
	log := NewLogger(logs, slog.LevelDebug, "injectproxy")
	recorder, err := OpenRecorder(dir, log)
	if err != nil {
		t.Fatalf("OpenRecorder: %v", err)
	}

	upstreamURL, err := url.Parse("http://" + upstream.listener.Addr().String())
	if err != nil {
		t.Fatalf("parse upstream: %v", err)
	}
	cfg := Config{
		Upstream:             upstreamURL,
		DataAddr:             "127.0.0.1:0",
		ControlAddr:          "127.0.0.1:0",
		RecordDir:            dir,
		RecordResponseBodies: []*regexp.Regexp{regexp.MustCompile(DefaultRecordResponseBodies)},
		LogLevel:             slog.LevelDebug,
	}
	for _, m := range mutate {
		m(&cfg)
	}

	clock := &testClock{}
	clock.nanos.Store(time.Now().UnixNano())
	injector := NewInjector()
	injector.now = clock.now
	server := NewServer(cfg, injector, recorder, log)
	listener, err := net.Listen("tcp", cfg.DataAddr)
	if err != nil {
		t.Fatalf("data plane listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.Serve(ctx, listener)
	}()

	control := httptest.NewServer(NewControl(injector, recorder, log).Handler())
	t.Cleanup(func() {
		control.Close()
		cancel()
		<-done
		recorder.Close()
	})

	return &harness{
		t:        t,
		cfg:      cfg,
		injector: injector,
		recorder: recorder,
		logs:     logs,
		dataAddr: listener.Addr().String(),
		control:  control,
		dir:      dir,
		upstream: upstream,
		clock:    clock,
	}
}

// rows reads the whole record face.
func (h *harness) rows() []record {
	h.t.Helper()
	rows, err := h.recorder.Read(time.Time{}, 0)
	if err != nil {
		h.t.Fatalf("read records: %v", err)
	}
	return rows
}

// rowsFor reads the rows of one path.
func (h *harness) rowsFor(path string) []record {
	h.t.Helper()
	var out []record
	for _, row := range h.rows() {
		if row.Path == path {
			out = append(out, row)
		}
	}
	return out
}

// blob reads one recorded body or byte segment.
func (h *harness) blob(rel string) string {
	h.t.Helper()
	data, err := readRecordFile(h.dir, rel)
	if err != nil {
		h.t.Fatalf("read blob %q: %v", rel, err)
	}
	return data
}

// dial opens a client connection to the data plane.
func (h *harness) dial() net.Conn {
	h.t.Helper()
	conn, err := net.Dial("tcp", h.dataAddr)
	if err != nil {
		h.t.Fatalf("dial data plane: %v", err)
	}
	h.t.Cleanup(func() { conn.Close() })
	return conn
}

// readN reads exactly n bytes with a deadline: a relay that adds, drops or
// reorders bytes fails here instead of hanging.
func readN(t *testing.T, conn net.Conn, n int, timeout time.Duration) []byte {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})
	buf := make([]byte, n)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read %d bytes: %v", n, err)
	}
	return buf
}

// readUntilClose drains a connection until the peer closes it.
func readUntilClose(t *testing.T, conn net.Conn, timeout time.Duration) []byte {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})
	data, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("drain connection: %v", err)
	}
	return data
}

func write(t *testing.T, conn net.Conn, data string) {
	t.Helper()
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetWriteDeadline(time.Time{})
	if _, err := io.WriteString(conn, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func mustRegexp(t *testing.T, pattern string) *regexp.Regexp {
	t.Helper()
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("regexp %q: %v", pattern, err)
	}
	return re
}
