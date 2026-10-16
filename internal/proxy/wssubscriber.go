package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// WSSubscriber is the C20 subscription to the server's daemon WS (ADR-010): N
// declared real runtimes take N connections, one per runtime id, each with its
// own dial loop and backoff (§1.1 C20). It turns daemon:task_available pushes
// into immediate claim iterations. It is an accelerator only — the polling
// claim loop keeps running while a connection is down.
type WSSubscriber struct {
	client  *Client
	wake    func()
	metrics Metrics
	log     *slog.Logger

	// dialTimeout bounds the WS handshake (§4 control-plane 30s); without it
	// a peer that accepts TCP but stalls the upgrade would wedge the
	// subscriber forever.
	dialTimeout time.Duration
	// waitSteps overrides the reconnect progression (tests use near-zero
	// waits); production leaves it nil and takes the §4 1s→30s backoff.
	waitSteps []time.Duration
	// parkRetry bounds how long one parked runtime waits before the supervisor
	// arms its retry again — §4 is "back off and reconnect", not "stop" — so a
	// row that becomes dialable again under the same id returns on its own.
	parkRetry time.Duration
	// now is the clock (overridable in tests).
	now func() time.Time
}

// parkRetryInterval is the production park window (one §4 backoff ceiling).
const parkRetryInterval = 30 * time.Second

// reconnectBackoff is one stream's progression: the §4 exponential backoff, or
// a scripted list in tests.
type reconnectBackoff struct {
	next  func() time.Duration
	reset func()
}

// newReconnectBackoff builds the progression of one stream.
func (s *WSSubscriber) newReconnectBackoff() reconnectBackoff {
	if len(s.waitSteps) == 0 {
		b := newBackoff()
		return reconnectBackoff{next: b.next, reset: b.reset}
	}
	steps := s.waitSteps
	step := 0
	return reconnectBackoff{
		next: func() time.Duration {
			delay := steps[len(steps)-1]
			if step < len(steps) {
				delay = steps[step]
			}
			step++
			return delay
		},
		reset: func() { step = 0 },
	}
}

// NewWSSubscriber builds the subscriber. wake is ClaimLoop.Wake.
func NewWSSubscriber(client *Client, wake func(), metrics Metrics) *WSSubscriber {
	return &WSSubscriber{
		client:      client,
		wake:        wake,
		metrics:     orNoopMetrics(metrics),
		log:         slog.Default().With("component", "ws-subscriber"),
		dialTimeout: controlTimeout,
		parkRetry:   parkRetryInterval,
		now:         time.Now,
	}
}

// streamOutcome tells the supervisor why one per-runtime stream ended.
type streamOutcome int

const (
	// streamRetry reconnects the same runtime after the backoff already waited.
	streamRetry streamOutcome = iota
	// streamRetired means this id is gone and a fresh one is in place: the
	// supervisor restarts the provider once the change notification arrives.
	streamRetired
	// streamParked means dialing this id cannot succeed any more (the row is
	// gone and re-registering did not move it). The stream stops instead of
	// hammering the handshake; the supervisor un-parks on the next runtime-set
	// change, which is exactly when dialing can work again (§4 退避面).
	streamParked
	// streamDone means the context ended.
	streamDone
)

// staleRebuildLimit bounds how often one stream may re-register for an id the
// server keeps refusing before it parks and waits for the set to move.
const staleRebuildLimit = 3

// wsSubscription is one supervised connection goroutine. epoch is the stream
// instance's monotonic identity: an id that moves away and back (A→B→A) is a
// *new* stream under an already used name, so the two must stay
// distinguishable when the replaced one reports its outcome.
type wsSubscription struct {
	runtimeID string
	epoch     uint64
	cancel    context.CancelFunc
}

// providerStreamState survives stream replacement: the reconnect progression
// and the stale-rebuild budget belong to the runtime, so an id that keeps
// moving cannot reset the backoff by restarting the stream. A replaced stream
// keeps running until it notices its cancel, so it and its replacement reach
// these fields from two goroutines: every access goes through the mutex.
type providerStreamState struct {
	mu            sync.Mutex
	backoff       reconnectBackoff
	staleRebuilds int
}

// nextBackoff advances this runtime's §4 progression and returns the wait.
func (st *providerStreamState) nextBackoff() time.Duration {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.backoff.next()
}

// noteStaleRebuild counts one failed stale rebuild and reports the total.
func (st *providerStreamState) noteStaleRebuild() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.staleRebuilds++
	return st.staleRebuilds
}

// rearm clears the failure streak: the §4 progression and the stale-rebuild
// budget both start over. It runs whenever the streak ends — a dial succeeded,
// or a parked runtime's retry window expired — so staleRebuildLimit bounds
// *consecutive* failures, never a runtime's lifetime (§1.1 C20, §4).
func (st *providerStreamState) rearm() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.staleRebuilds = 0
	st.backoff.reset()
}

// streamResult is what a finished stream reports back to the supervisor. epoch
// names the stream instance — never (provider, id), which an id that moved away
// and back still satisfies.
type streamResult struct {
	provider  string
	runtimeID string
	epoch     uint64
	outcome   streamOutcome
}

// Run keeps one connection per declared real runtime until ctx ends. A runtime
// whose id moved is reconnected on its own; the other connections stay up
// (§7 多 runtime 的失败面按 runtime 粒度). It is never started when
// FORMANET_WS_ENABLED=false (ADR-010 degradation switch).
func (s *WSSubscriber) Run(ctx context.Context) error {
	changes := s.client.Changes()
	connections := map[string]wsSubscription{}
	// parked holds the moment each provider's dialing is armed again. It is
	// keyed by provider, not by id: an id that moves on every register is the
	// same pathology as one that never moves, and only a bounded window (§4's
	// 30s ceiling) keeps either from dialing at RTT speed. The retry comes back
	// on its own when the window expires.
	parked := map[string]time.Time{}
	// states carries the per-runtime reconnect progression and stale-rebuild
	// budget across stream replacements (one id move must not reset either).
	states := map[string]*providerStreamState{}
	// epochs numbers the stream instances: an id may be reused, a stream
	// instance never is (it is what the outcome is matched against).
	var epochs uint64
	stopAll := func() {
		for provider, sub := range connections {
			sub.cancel()
			delete(connections, provider)
		}
	}
	defer stopAll()
	outcomes := make(chan streamResult, 8)
	reconcile := func() {
		declared := map[string]RuntimeRef{}
		for _, ref := range s.client.RuntimeRefs() {
			if ref.ID != "" {
				declared[ref.Provider] = ref
			}
		}
		for provider, until := range parked {
			_, declaredNow := declared[provider]
			if !declaredNow || !s.now().Before(until) {
				// The provider left or the retry window expired: arm it again
				// with a fresh budget.
				delete(parked, provider)
				if state, ok := states[provider]; ok {
					state.rearm()
				}
			}
		}
		for provider := range states {
			if _, ok := declared[provider]; !ok {
				delete(states, provider)
			}
		}
		for provider, sub := range connections {
			ref, ok := declared[provider]
			if !ok || ref.ID != sub.runtimeID {
				sub.cancel()
				delete(connections, provider)
			}
		}
		for provider, ref := range declared {
			if _, ok := connections[provider]; ok {
				continue
			}
			if _, parking := parked[provider]; parking {
				continue
			}
			state, ok := states[provider]
			if !ok {
				state = &providerStreamState{backoff: s.newReconnectBackoff()}
				states[provider] = state
			}
			subCtx, cancel := context.WithCancel(ctx)
			epochs++
			epoch := epochs
			connections[provider] = wsSubscription{runtimeID: ref.ID, epoch: epoch, cancel: cancel}
			go func(ref RuntimeRef, state *providerStreamState, epoch uint64) {
				result := s.listen(subCtx, ref, state, epoch)
				select {
				case outcomes <- result:
				case <-ctx.Done():
				}
			}(ref, state, epoch)
		}
	}
	for {
		reconcile()
		select {
		case <-ctx.Done():
			return nil
		case result := <-outcomes:
			if result.outcome == streamParked {
				// Parking is a statement about the provider, not about one id:
				// the rebuild budget is shared across replacements, so even a
				// replaced stream's park outcome must hold the provider back
				// for the window (an id that moves on every register is the
				// same pathology as one that never moves). Idempotent: an
				// already parked provider keeps its expiry.
				if _, already := parked[result.provider]; !already {
					parked[result.provider] = s.now().Add(s.parkRetry)
				}
			}
			// Unregister by identity: the outcome names the stream instance,
			// not (provider, id). An id that moved away and back names a
			// different stream, and matching it here would delete that live
			// connection and orphan its cancel (§1.1 C20).
			sub, ok := connections[result.provider]
			if !ok || sub.epoch != result.epoch {
				continue
			}
			delete(connections, result.provider)
		case <-changes:
			// Just wake up: parking is left to its own window, so a provider
			// whose id moves on every register cannot re-arm by moving.
		case <-time.After(heartbeatSupervisorInterval):
		}
	}
}

// listen drives one runtime's connection: dial → read → reconnect, with its own
// backoff (1s→30s, §4). The progression and the stale-rebuild budget live in
// state, which outlives a stream replacement — an id that keeps moving must not
// reset the rate limit by starting a fresh stream. epoch is this instance's own
// identity and travels back with the outcome.
func (s *WSSubscriber) listen(ctx context.Context, ref RuntimeRef, state *providerStreamState, epoch uint64) streamResult {
	for {
		outcome := s.iterate(ctx, ref, state)
		if outcome != streamRetry {
			return streamResult{provider: ref.Provider, runtimeID: ref.ID, epoch: epoch, outcome: outcome}
		}
	}
}

// iterate runs one connect → read → reconnect cycle for a runtime, returning
// the reason the loop must stop (or streamRetry to go around again).
func (s *WSSubscriber) iterate(ctx context.Context, ref RuntimeRef, state *providerStreamState) streamOutcome {
	runtimeID := ref.ID
	u := wsURL(s.client.cfg.ServerURL) + "/api/daemon/ws?runtime_ids=" + url.QueryEscape(runtimeID)
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+s.client.token)
	hdr.Set("X-Client-Platform", "daemon")
	hdr.Set("X-Client-Version", s.client.CLIVersion())
	hdr.Set("X-Client-OS", "linux")
	hdr.Set("X-Client-Capabilities", ClientCapabilities)
	dialCtx, cancel := context.WithTimeout(ctx, s.dialTimeout)
	conn, resp, err := websocket.Dial(dialCtx, u, &websocket.DialOptions{HTTPHeader: hdr})
	cancel() // the dial context only bounds the handshake, not the connection
	if err != nil {
		if ctx.Err() != nil {
			return streamDone
		}
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		switch {
		case code == http.StatusBadRequest:
			// §1.1 failure table: a handshake 400 means this implementation
			// sent a bad URL — log and stop dialing this id; polling carries
			// on, and the next runtime-set change gives it another chance.
			s.log.Error("ws handshake rejected (bad request); staying on polling", "status", code, "runtime_id", runtimeID)
			return streamParked
		case code == http.StatusNotFound && handshakeSaysRuntimeGone(resp):
			// The runtime row is gone: rebuild it (C1) and reconnect under the
			// fresh id. A row that cannot be moved parks this stream instead of
			// re-dialing in a loop (the design leaves server-side upsert
			// unverified, so the same id may come straight back).
			s.log.Warn("ws handshake: runtime not found; re-registering", "runtime_id", runtimeID)
			fresh, rerr := s.client.rebuildRuntime(ctx, runtimeID)
			if rerr != nil {
				s.log.Warn("ws re-register failed", "runtime_id", runtimeID, "err", rerr)
			}
			attempts := state.noteStaleRebuild()
			if attempts >= staleRebuildLimit {
				s.log.Warn("ws reconnect parked until the runtime set moves",
					"runtime_id", runtimeID, "attempts", attempts)
				return streamParked
			}
			// Retire and stale take the same rate limit: the id moved, so the
			// next dial targets the fresh one — after the §4 wait, so a server
			// that hands out a new id on every register cannot be dialed at RTT
			// speed (and cannot be re-registered at that speed either).
			s.waitReconnect(ctx, err, state)
			if rerr == nil && fresh != "" && fresh != runtimeID {
				return streamRetired
			}
		default:
			s.waitReconnect(ctx, err, state)
		}
		return streamRetry
	}
	// A connection is the streak's end: both the stale-rebuild budget and the
	// §4 progression start over (the limit bounds consecutive failures only).
	state.rearm()
	s.metrics.WSConnect(WSSideServer)
	s.log.Info("ws.connected", "side", WSSideServer, "runtime_id", runtimeID)
	s.readLoop(ctx, conn)
	_ = conn.Close(websocket.StatusNormalClosure, "closing")
	s.metrics.WSDisconnect(WSSideServer)
	s.metrics.WSReconnect(WSSideServer)
	s.log.Info("ws.reconnect", "side", WSSideServer, "runtime_id", runtimeID)
	// A replaced stream must not spend the runtime's progression: the select
	// below evaluates its operands even once the cancel arrived, and that step
	// belongs to the replacement's first dial (§4).
	if ctx.Err() != nil {
		return streamDone
	}
	wait := state.nextBackoff()
	select {
	case <-ctx.Done():
		return streamDone
	case <-time.After(wait):
	}
	return streamRetry
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

func (s *WSSubscriber) waitReconnect(ctx context.Context, err error, state *providerStreamState) {
	// A stream that is already gone waits for nobody: both the sleep and the
	// §4 step it would spend belong to the replacement.
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return
	}
	wait := state.nextBackoff()
	s.log.Warn("ws connect failed", "err", err, "retry_in", wait)
	select {
	case <-ctx.Done():
	case <-time.After(wait):
	}
}
