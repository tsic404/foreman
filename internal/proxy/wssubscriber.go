package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/coder/websocket"
)

// WSSubscriber is the C20 subscription to the server's daemon WS (ADR-010):
// it connects after C1 registered the real runtime, turns
// daemon:task_available pushes into immediate claim iterations, and
// reconnects with §4 backoff on loss. It is an accelerator only — the
// polling claim loop keeps running while the WS is down.
type WSSubscriber struct {
	client  *Client
	wake    func()
	metrics Metrics
	log     *slog.Logger
	backoff *backoff

	// dialTimeout bounds the WS handshake (§4 control-plane 30s); without it
	// a peer that accepts TCP but stalls the upgrade would wedge the
	// subscriber forever.
	dialTimeout time.Duration
}

// NewWSSubscriber builds the subscriber. wake is ClaimLoop.Wake.
func NewWSSubscriber(client *Client, wake func(), metrics Metrics) *WSSubscriber {
	return &WSSubscriber{
		client:      client,
		wake:        wake,
		metrics:     orNoopMetrics(metrics),
		log:         slog.Default().With("component", "ws-subscriber"),
		backoff:     newBackoff(),
		dialTimeout: controlTimeout,
	}
}

// Run maintains the subscription until ctx ends. It is never started when
// FOREMAN_WS_ENABLED=false (ADR-010 degradation switch).
func (s *WSSubscriber) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		s.iterate(ctx)
	}
}

// iterate runs one connect → read → reconnect cycle.
func (s *WSSubscriber) iterate(ctx context.Context) {
	rid, err := s.client.ensureRuntime(ctx)
	if err != nil {
		s.waitReconnect(ctx, err)
		return
	}
	u := wsURL(s.client.cfg.ServerURL) + "/api/daemon/ws?runtime_ids=" + url.QueryEscape(rid)
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+s.client.token)
	hdr.Set("X-Client-Platform", "daemon")
	hdr.Set("X-Client-Version", s.client.cfg.Version)
	hdr.Set("X-Client-OS", "linux")
	hdr.Set("X-Client-Capabilities", ClientCapabilities)
	dialCtx, cancel := context.WithTimeout(ctx, s.dialTimeout)
	conn, resp, err := websocket.Dial(dialCtx, u, &websocket.DialOptions{HTTPHeader: hdr})
	cancel() // the dial context only bounds the handshake, not the connection
	if err != nil {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		switch {
		case code == 400:
			// §1.1 failure table: a handshake 400 means this implementation
			// sent a bad URL — log and stop retrying; polling carries on.
			s.log.Error("ws handshake rejected (bad request); staying on polling", "status", code)
			s.waitForRuntimeChange(ctx)
		case code == 404:
			// A handshake 404 only re-registers when the body proves the
			// runtime row is gone (same rule as withRuntime); either way the
			// retry waits out the §4 backoff — never a registration storm.
			if handshakeSaysRuntimeGone(resp) {
				s.log.Warn("ws handshake: runtime not found; re-registering")
				s.client.noteRuntimeGone()
			} else {
				s.log.Warn("ws handshake returned 404 without a runtime marker")
			}
			s.waitReconnect(ctx, err)
		default:
			s.waitReconnect(ctx, err)
		}
		return
	}
	s.backoff.reset()
	s.metrics.WSConnect(WSSideServer)
	s.log.Info("ws.connected", "side", WSSideServer, "runtime_id", rid)
	s.readLoop(ctx, conn)
	_ = conn.Close(websocket.StatusNormalClosure, "closing")
	s.metrics.WSDisconnect(WSSideServer)
	s.metrics.WSReconnect(WSSideServer)
	s.log.Info("ws.reconnect", "side", WSSideServer, "runtime_id", rid)
	// Reconnect promptly on a runtime-set change, else with backoff.
	select {
	case <-ctx.Done():
	case <-s.client.RuntimeSet():
		s.log.Info("runtime set changed; reconnecting ws")
	case <-time.After(s.backoff.next()):
	}
}

// readLoop consumes frames until the connection drops; task_available
// coalesces into a single claim wakeup (proxy.md §claim 循环).
func (s *WSSubscriber) readLoop(ctx context.Context, conn *websocket.Conn) {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var frame wsFrame
		if err := json.Unmarshal(data, &frame); err != nil {
			s.log.Debug("ws frame undecodable", "err", err)
			continue
		}
		if frame.Type == FrameDaemonTaskAvailable {
			s.wake()
		}
	}
}

// handshakeSaysRuntimeGone checks the failed handshake body for the
// server's "runtime not found" marker (§1.1 failure table).
func handshakeSaysRuntimeGone(resp *http.Response) bool {
	if resp == nil || resp.Body == nil {
		return false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return err == nil && containsRuntimeNotFound(body)
}

func (s *WSSubscriber) waitReconnect(ctx context.Context, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	wait := s.backoff.next()
	s.log.Warn("ws connect failed", "err", err, "retry_in", wait)
	select {
	case <-ctx.Done():
	case <-time.After(wait):
	}
}

// waitForRuntimeChange parks the subscriber until the runtime set changes
// (a handshake 400 is an implementation bug — no blind retries, §1.1).
func (s *WSSubscriber) waitForRuntimeChange(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-s.client.RuntimeSet():
	}
}
