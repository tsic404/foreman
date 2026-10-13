package injectproxy

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Server is the transparent data plane: one listener, one goroutine per
// connection, one upstream connection per client connection. Nothing is
// pooled, cached or retried — a client connection maps to exactly the
// upstream connection its bytes travel on (§4.1).
type Server struct {
	cfg      Config
	injector *Injector
	recorder *Recorder
	log      *slog.Logger
	dialer   *net.Dialer

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

// NewServer builds the data plane.
func NewServer(cfg Config, injector *Injector, recorder *Recorder, log *slog.Logger) *Server {
	return &Server{
		cfg:      cfg,
		injector: injector,
		recorder: recorder,
		log:      log,
		dialer:   &net.Dialer{Timeout: upstreamDialTimeout, KeepAlive: 30 * time.Second},
		conns:    map[net.Conn]struct{}{},
	}
}

// Listen opens the data-plane listener; the caller logs the bound address.
func ListenData(cfg Config) (net.Listener, error) {
	listener, err := net.Listen("tcp", cfg.DataAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", cfg.DataAddr, err)
	}
	return listener, nil
}

// Serve accepts data-plane connections until ctx ends.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			listener.Close()
			s.closeConns()
		case <-stop:
		}
	}()

	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if !s.track(conn) {
			conn.Close()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.untrack(conn)
			defer conn.Close()
			s.serveConn(ctx, conn)
		}()
	}
}

func (s *Server) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[conn] = struct{}{}
	return true
}

func (s *Server) untrack(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, conn)
}

func (s *Server) closeConns() {
	s.mu.Lock()
	s.closed = true
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()
	for _, conn := range conns {
		conn.Close()
	}
}

// serveConn runs one client connection through as many exchanges as both
// sides keep it alive for.
func (s *Server) serveConn(ctx context.Context, client net.Conn) {
	sess := &session{server: s, client: client, clientR: bufio.NewReaderSize(client, readerBufferSize)}
	defer sess.close()

	for {
		leading, head, err := readRequestHead(sess.clientR)
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				s.log.Debug("request head read ended", "err", err)
			}
			return
		}
		req, err := parseRequestHead(head)
		if err != nil {
			s.log.Warn("malformed request head; closing connection", "err", err)
			return
		}
		if !sess.handleRequest(ctx, req, leading) {
			return
		}
	}
}

// session is one client connection and its upstream counterpart.
type session struct {
	server  *Server
	client  net.Conn
	clientR *bufio.Reader
	up      net.Conn
	upR     *bufio.Reader
	reqBuf  []byte
	respBuf []byte
}

func (sess *session) close() {
	if sess.up != nil {
		sess.up.Close()
	}
}

func (sess *session) upstream(ctx context.Context) (net.Conn, *bufio.Reader, error) {
	if sess.up != nil {
		return sess.up, sess.upR, nil
	}
	cfg := sess.server.cfg
	conn, err := sess.server.dialer.DialContext(ctx, "tcp", cfg.Upstream.Host)
	if err != nil {
		return nil, nil, err
	}
	if cfg.Upstream.Scheme == "https" {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: cfg.Upstream.Hostname()})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, nil, err
		}
		conn = tlsConn
	}
	sess.up, sess.upR = conn, bufio.NewReaderSize(conn, readerBufferSize)
	return sess.up, sess.upR, nil
}

// reqScratch and respScratch are the relay buffers of this connection: the
// request body relay and the response relay may run at the same time (a
// client can still be uploading while the server has answered), so they never
// share a buffer.
func (sess *session) reqScratch() []byte {
	if sess.reqBuf == nil {
		sess.reqBuf = make([]byte, copyBufferSize)
	}
	return sess.reqBuf
}

func (sess *session) respScratch() []byte {
	if sess.respBuf == nil {
		sess.respBuf = make([]byte, copyBufferSize)
	}
	return sess.respBuf
}

// handleRequest routes one request: refuse it (ws-drop), answer it with the
// injected status, or relay it verbatim. leading is whatever preceded the
// request head on the wire and is relayed with it. It reports false when the
// connection is done.
func (sess *session) handleRequest(ctx context.Context, req *messageHead, leading []byte) bool {
	s := sess.server
	path := req.requestPath()
	prefix := authPrefix(req)

	if req.isWebSocket() && s.injector.WSDropActive(path) {
		row := requestRow(time.Now(), directionFor(prefix), req.method, path, prefix, recordHeaders(req))
		row.Injected = true
		s.recorder.Append(row)
		s.log.Info("ws upgrade refused by injection", "path", path, "authorization_prefix", prefix)
		// §4.2: refuse by dropping the connection — no HTTP response, so the
		// caller cannot mistake this for a 404 "runtime not found".
		return false
	}

	ex := newExchange(s, requestRow(time.Now(), directionFor(prefix), req.method, path, prefix, recordHeaders(req)))
	defer ex.finish()

	framing, length, err := req.requestFraming()
	if err != nil {
		s.log.Warn("invalid request framing; closing connection", "path", path, "err", err)
		return false
	}
	if status, injected := s.injector.HTTP5xxStatus(path); injected {
		ex.row.Injected = true
		if req.expectContinue() {
			// The caller is holding its body back waiting for 100-continue;
			// answering with the final status means it will never send it,
			// and a body byte that raced in could not be told apart from the
			// next request head — so the connection ends here.
			ex.finishBody()
			ex.markStatus(status)
			if err := writeSyntheticStatus(sess.client, status, true); err != nil {
				return false
			}
			s.log.Info("request injected", "method", req.method, "path", path, "status", status)
			return false
		}
		// The request is consumed but never forwarded (§4.2); its body still
		// reaches the record face, forward-02 compares it across the window.
		if err := relayBody(io.Discard, sess.clientR, framing, length, ex.sink, sess.reqScratch()); err != nil {
			return false
		}
		ex.finishBody()
		ex.markStatus(status)
		if err := writeSyntheticStatus(sess.client, status, false); err != nil {
			return false
		}
		s.log.Info("request injected", "method", req.method, "path", path, "status", status)
		return req.keepAlive()
	}

	upstream, upstreamR, err := sess.upstream(ctx)
	if err != nil {
		s.log.Error("upstream dial failed", "upstream", s.cfg.Upstream.String(), "err", err)
		return false
	}
	// The bytes that preceded the request head travel with it, in order.
	if len(leading) > 0 {
		if _, err := upstream.Write(leading); err != nil {
			s.log.Debug("upstream write failed", "path", path, "err", err)
			return false
		}
	}
	if _, err := upstream.Write(req.raw); err != nil {
		s.log.Debug("upstream write failed", "path", path, "err", err)
		return false
	}
	hasBody := hasRequestBody(framing, length)
	if !hasBody {
		ex.finishBody()
	} else if !req.expectContinue() {
		ex.startBodyRelay(sess, framing, length)
	}

	resp, ok := sess.readResponse(upstreamR, req, ex, framing, length)
	if !ok {
		return false
	}
	// §4.3: the row carries the response status, and it is written before the
	// response reaches the caller so a record read right after a probe sees it.
	ex.markStatus(resp.status)

	if resp.status == 101 && req.hasUpgrade() {
		return sess.runTunnel(req, resp)
	}
	if _, err := sess.client.Write(resp.raw); err != nil {
		return false
	}
	respFraming, respLength, err := resp.responseFraming(req.method)
	if err != nil {
		s.log.Warn("invalid response framing", "path", path, "err", err)
		return false
	}
	recordBody := s.cfg.RecordsResponseBody(path)
	var sink *bodySink
	if recordBody {
		sink = s.recorder.NewBodySink()
	}
	respAt := time.Now()
	if err := relayBody(sess.client, upstreamR, respFraming, respLength, sink, sess.respScratch()); err != nil {
		if sink != nil {
			sink.Close()
		}
		s.log.Debug("response relay ended", "path", path, "err", err)
		return false
	}
	if recordBody {
		if sha, rel := sink.Close(); rel != "" {
			s.recorder.Append(responseRow(respAt, path, resp.status, sha, rel))
		}
	}
	return sess.canReuse(req, resp, ex, hasBody, respFraming)
}

// readResponse reads the response head, relaying interim 1xx answers on the
// way. A 100 Continue is also the signal that starts the caller's body relay
// when the request announced Expect: 100-continue.
func (sess *session) readResponse(upstreamR *bufio.Reader, req *messageHead, ex *exchange, framing bodyFraming, length int64) (*messageHead, bool) {
	s := sess.server
	for {
		raw, err := readHead(upstreamR)
		if err != nil {
			s.log.Debug("upstream ended before answering", "path", req.requestPath(), "err", err)
			return nil, false
		}
		resp, err := parseResponseHead(raw)
		if err != nil {
			s.log.Warn("malformed response head from upstream", "err", err)
			return nil, false
		}
		if resp.status < 200 && resp.status != 101 {
			if _, err := sess.client.Write(resp.raw); err != nil {
				return nil, false
			}
			if resp.status == 100 {
				ex.startBodyRelay(sess, framing, length)
			}
			continue
		}
		return resp, true
	}
}

// canReuse decides whether the connection carries another exchange. It does
// not when either side asked to close, when the response has no length of its
// own, or when request bytes were left unconsumed — bytes the caller may
// still send cannot be told apart from the next request head, and §4.1
// forbids guessing at or rewriting them.
func (sess *session) canReuse(req, resp *messageHead, ex *exchange, hasBody bool, respFraming bodyFraming) bool {
	if !req.keepAlive() || !resp.keepAlive() || respFraming == bodyUntilClose {
		return false
	}
	return !hasBody || ex.isBodyDone()
}

// runTunnel takes over after a 101 switched protocols. From here the bytes
// are no longer HTTP: each read segment of either direction is relayed
// verbatim and recorded as a ws-chunk row (§4.3, no frame parsing). An
// upgraded connection never returns to HTTP.
func (sess *session) runTunnel(req, resp *messageHead) bool {
	s := sess.server
	path := req.requestPath()
	direction := directionFor(authPrefix(req))

	tunnel := &tunnel{client: sess.client, upstream: sess.up}
	deregister := s.injector.RegisterTunnel(path, func() {
		s.log.Info("ws connection dropped by injection", "path", path)
		tunnel.Close()
	})
	defer deregister()

	// Register before the handshake answer reaches the caller: a revocation
	// landing in between must still find this connection, not only the ones
	// that finished upgrading.
	if _, err := sess.client.Write(resp.raw); err != nil {
		return false
	}
	s.log.Info("ws tunnel established", "path", path, "authorization_prefix", authPrefix(req))

	record := func(dir string, data []byte) {
		sink := s.recorder.NewChunkSink()
		sink.Write(data)
		if _, rel := sink.Close(); rel != "" {
			s.recorder.Append(chunkRow(time.Now(), dir, path, rel))
		}
	}
	// Bytes buffered while the handshake was read are tunnel bytes, not
	// handshake leftovers.
	if err := drainBuffered(sess.clientR, tunnel.upstream, func(b []byte) { record(direction, b) }); err != nil {
		tunnel.Close()
		return false
	}
	if err := drainBuffered(sess.upR, tunnel.client, func(b []byte) { record(directionServerToForeman, b) }); err != nil {
		tunnel.Close()
		return false
	}

	done := make(chan struct{}, 2)
	go func() { sess.pump(tunnel.upstream, tunnel.client, direction, record); done <- struct{}{} }()
	go func() {
		sess.pump(tunnel.client, tunnel.upstream, directionServerToForeman, record)
		done <- struct{}{}
	}()
	<-done
	tunnel.Close()
	<-done
	s.log.Info("ws tunnel closed", "path", path)
	return false
}

// pump relays one direction of an upgraded connection, one record per read.
func (sess *session) pump(dst, src net.Conn, direction string, record func(string, []byte)) {
	buf := make([]byte, copyBufferSize)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
			record(direction, buf[:n])
		}
		if err != nil {
			return
		}
	}
}

// tunnel is an upgraded connection pair; Close drops both ends and is safe to
// call from the injection control path and from the relay at the same time.
type tunnel struct {
	client   net.Conn
	upstream net.Conn
	once     sync.Once
}

// Close tears the tunnel down.
func (t *tunnel) Close() {
	t.once.Do(func() {
		t.client.Close()
		t.upstream.Close()
	})
}

// exchange tracks one HTTP exchange's §4.3 row. The row needs two facts —
// the response status and the request body — which arrive in either order
// (a server may answer before reading the whole body), so whichever lands
// last writes the row.
type exchange struct {
	server *Server
	row    record
	sink   *bodySink

	mu          sync.Mutex
	status      int
	hasStatus   bool
	bodyStarted bool
	bodyDone    bool
	closing     bool
	written     bool
}

func newExchange(server *Server, row record) *exchange {
	return &exchange{server: server, row: row, sink: server.recorder.NewBodySink()}
}

// startBodyRelay relays the request body to the upstream. With
// Expect: 100-continue it is called only once the server asked for it — a
// body sent before that would be traffic the caller never offered.
func (ex *exchange) startBodyRelay(sess *session, framing bodyFraming, length int64) {
	ex.mu.Lock()
	if ex.bodyStarted {
		ex.mu.Unlock()
		return
	}
	ex.bodyStarted = true
	ex.mu.Unlock()

	go func() {
		err := relayBody(sess.up, sess.clientR, framing, length, ex.sink, sess.reqScratch())
		if err != nil && !errors.Is(err, io.EOF) {
			ex.server.log.Debug("request body relay ended", "path", ex.row.Path, "err", err)
		}
		ex.finishBody()
	}()
}

// markStatus publishes the response status.
func (ex *exchange) markStatus(status int) {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	ex.status, ex.hasStatus = status, true
	ex.writeLocked()
}

// finishBody marks the request body consumed (or absent).
func (ex *exchange) finishBody() {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	ex.bodyDone = true
	ex.writeLocked()
}

func (ex *exchange) isBodyDone() bool {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	return ex.bodyDone
}

// finish closes the exchange. It runs once the connection is done with the
// request — normally after the status arrived, but also when the exchange
// died half-way (upstream dial failure, an upstream that closed before
// answering). The record face is evidence: a request that was seen and never
// answered leaves a row with its status absent rather than invented. A body
// relay still in flight keeps the row open: closing the body file under it
// would record a truncated body.
func (ex *exchange) finish() {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	ex.closing = true
	if ex.bodyStarted && !ex.bodyDone {
		return
	}
	ex.bodyDone = true
	ex.writeLocked()
}

func (ex *exchange) writeLocked() {
	if ex.written || !ex.bodyDone {
		return
	}
	if !ex.hasStatus && !ex.closing {
		return
	}
	ex.written = true
	row := ex.row
	row.Status = ex.status
	if ex.sink != nil {
		row.BodySHA256, row.BodyPath = ex.sink.Close()
	}
	ex.server.recorder.Append(row)
}
