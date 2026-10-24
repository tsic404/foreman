// TC-tech-multi-11 / AC-102: the store's own answer bound is the only timeout
// evidence, and a caller's budget never shares that role.
//
// The store runs every operation on a bound it owns; the caller's context only
// ends that caller's wait. So a store that answers inside its bound but slower
// than a caller budget opens no outage, an aborted wait opens none either, and
// a store that stalls past its own bound is still adjudicated unavailable even
// when every call that reached it carried a shorter budget (§10 共享状态不可用
// 的判定; ADR-054 决策 1–4).
package acceptance

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// storeGate is a controllable TCP face in front of miniredis. A scenario uses
// it to hold every answer past a caller's budget, or to stop answering
// entirely; the production store keeps running on its real clock, which is
// what the evidence boundary is about.
type storeGate struct {
	// open is closed while the gate forwards; swapping in a fresh channel
	// stalls every connection.
	open chan struct{}
	// answers is closed while answers may reach their clients; swapping in a
	// fresh channel withholds them, so a caller's budget ends first by
	// construction instead of racing a fixed delay.
	answers chan struct{}

	mu     sync.Mutex
	ln     net.Listener
	conns  []net.Conn
	closed bool
	// watched counts forwarded answers per marker found in the request that
	// produced them or in the answer itself, so a scenario can wait for the
	// operation it observes to conclude instead of sleeping for a guess.
	watched map[string]int
}

func newStoreGate(t *testing.T) *storeGate {
	t.Helper()
	g := &storeGate{
		open:    make(chan struct{}),
		answers: make(chan struct{}),
	}
	close(g.open)
	close(g.answers)
	t.Cleanup(g.close)
	return g
}

// serve starts the gate in front of upstream and returns its address.
func (g *storeGate) serve(t *testing.T, upstream string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("store gate listen: %v", err)
	}
	g.mu.Lock()
	g.ln = ln
	g.mu.Unlock()
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", upstream)
			if err != nil {
				_ = client.Close()
				continue
			}
			g.track(client, server)
			exchange := &gateExchange{}
			go g.pipe(server, client, false, exchange)
			go g.pipe(client, server, true, exchange)
		}
	}()
	return ln.Addr().String()
}

func (g *storeGate) track(conns ...net.Conn) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.conns = append(g.conns, conns...)
}

// gateExchange is one client–store connection pair's request bytes read since
// the last forwarded answer. Redis answers each connection's requests in
// order, so those bytes identify the command an answer belongs to.
type gateExchange struct {
	mu  sync.Mutex
	req []byte
}

func (e *gateExchange) record(b []byte) {
	e.mu.Lock()
	e.req = append(e.req, b...)
	e.mu.Unlock()
}

func (e *gateExchange) take() []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	req := e.req
	e.req = nil
	return req
}

// pipe forwards one direction. answers marks the upstream → client direction,
// the only one the answer delay applies to.
func (g *storeGate) pipe(dst, src net.Conn, answers bool, exchange *gateExchange) {
	defer func() {
		_ = dst.Close()
		_ = src.Close()
	}()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			var request []byte
			if answers {
				request = exchange.take()
			}
			g.await()
			if answers {
				g.awaitAnswers()
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
			if answers {
				g.noteAnswer(request, buf[:n])
			} else {
				exchange.record(buf[:n])
			}
		}
		if err != nil {
			return
		}
	}
}

func (g *storeGate) await() {
	g.mu.Lock()
	open := g.open
	g.mu.Unlock()
	<-open
}

func (g *storeGate) awaitAnswers() {
	g.mu.Lock()
	answers := g.answers
	g.mu.Unlock()
	<-answers
}

// holdAnswers withholds every answer from its client until releaseAnswers.
func (g *storeGate) holdAnswers() {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.answers:
		g.answers = make(chan struct{})
	default:
	}
}

// releaseAnswers lets the held answers reach their clients.
func (g *storeGate) releaseAnswers() {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.answers:
	default:
		close(g.answers)
	}
}

// watch starts counting the answers that carry marker in their request or
// answer bytes.
func (g *storeGate) watch(marker string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.watched == nil {
		g.watched = make(map[string]int)
	}
	if _, ok := g.watched[marker]; !ok {
		g.watched[marker] = 0
	}
}

// noteAnswer counts one forwarded answer against every watched marker present
// in the request that produced it or in the answer itself.
func (g *storeGate) noteAnswer(request, answer []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for marker := range g.watched {
		if bytes.Contains(request, []byte(marker)) || bytes.Contains(answer, []byte(marker)) {
			g.watched[marker]++
		}
	}
}

// awaitAnswer blocks until the gate has forwarded the answer to the operation
// carrying marker: the store operation has concluded, so the assertions that
// follow read a settled availability face.
func (g *storeGate) awaitAnswer(t *testing.T, marker string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		g.mu.Lock()
		seen := g.watched[marker]
		g.mu.Unlock()
		if seen > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the gate forwarded no answer for %q within %s", marker, timeout)
		}
		time.Sleep(time.Millisecond)
	}
}

// stall stops forwarding in both directions: nothing reaches the store and
// nothing comes back.
func (g *storeGate) stall() {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.open:
		g.open = make(chan struct{})
	default:
	}
}

// resume reopens the gate; anything held at it is forwarded then.
func (g *storeGate) resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.open:
	default:
		close(g.open)
	}
}

func (g *storeGate) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	g.closed = true
	if g.ln != nil {
		_ = g.ln.Close()
	}
	for _, conn := range g.conns {
		_ = conn.Close()
	}
	select {
	case <-g.open:
	default:
		close(g.open)
	}
	select {
	case <-g.answers:
	default:
		close(g.answers)
	}
}

// awaitEventCount waits for the availability transition lines to reach count.
func (tw *twoInstances) awaitEventCount(t *testing.T, count int, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		lines := tw.stateEventLines()
		if len(lines) >= count {
			return lines
		}
		if time.Now().After(deadline) {
			t.Fatalf("availability events stayed at %v; want %d lines", lines, count)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestTechMulti11CallerBudgetBoundary(t *testing.T) {
	const (
		// The store's own answer bound, deliberately much larger than the
		// caller budgets the scenario uses below.
		storeBound   = 800 * time.Millisecond
		callerBudget = 40 * time.Millisecond
	)
	gate := newStoreGate(t)
	gate.watch("task-short")
	gate.watch("task-abort")
	tw := newTwoInstances(t, twoInstanceOptions{
		callTimeout: storeBound,
		storeURL:    func(upstream string) string { return "redis://" + gate.serve(t, upstream) },
		leaderCfg:   smallLeaderCfg(2 * time.Second),
	})
	tw.declareAll(t)
	tw.a.startLeader(t)

	// Warm the pool before any timing assertion: a cold connection's handshake
	// is not part of what this case measures.
	if err := tw.coord.DropQueue(context.Background(), "ws-1", "task-warm"); err != nil {
		t.Fatalf("warm-up store operation: %v", err)
	}
	baseline := tw.stateEventLines()

	// Segment one: the store answers inside its own bound but the caller's
	// budget ends first. The call ends with its own deadline, the store keeps
	// running its operation, and no availability event appears.
	gate.holdAnswers()
	short, cancelShort := context.WithTimeout(context.Background(), callerBudget)
	defer cancelShort()
	started := time.Now()
	if err := tw.coord.DropQueue(short, "ws-1", "task-short"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("slow store under a short caller budget = %v, want context.DeadlineExceeded", err)
	}
	if took := time.Since(started); took > 4*callerBudget {
		t.Fatalf("the call waited %s, past its own %s budget", took, callerBudget)
	}
	// Release the answer and wait for it to cross the gate: the store
	// operation has concluded, and a caller that gave up did not leave its
	// conclusion in doubt.
	gate.releaseAnswers()
	gate.awaitAnswer(t, "task-short", 2*time.Second)
	if !tw.coord.Available() || tw.coord.DownFor() != 0 {
		t.Fatalf("a caller budget moved the outage face: available=%v downFor=%s",
			tw.coord.Available(), tw.coord.DownFor())
	}
	if lines := tw.stateEventLines(); len(lines) != len(baseline) {
		t.Fatalf("a caller budget added availability events: %v -> %v", baseline, lines)
	}

	// Segment two: a caller that aborts while the store is working on its
	// answer. Same rule — the wait ends, the verdict does not move.
	abort, cancelAbort := context.WithCancel(context.Background())
	gate.holdAnswers()
	go func() {
		time.Sleep(3 * callerBudget)
		cancelAbort()
	}()
	if err := tw.coord.DropQueue(abort, "ws-1", "task-abort"); !errors.Is(err, context.Canceled) {
		t.Fatalf("aborted store call = %v, want context.Canceled", err)
	}
	gate.releaseAnswers()
	gate.awaitAnswer(t, "task-abort", 2*time.Second)
	if !tw.coord.Available() || tw.coord.DownFor() != 0 {
		t.Fatalf("an aborted call moved the outage face: available=%v downFor=%s",
			tw.coord.Available(), tw.coord.DownFor())
	}
	if lines := tw.stateEventLines(); len(lines) != len(baseline) {
		t.Fatalf("an aborted call added availability events: %v -> %v", baseline, lines)
	}

	// The four verdicts the contract names stay unchanged while the store is
	// slow: a healthy store inside its bound is still available, a task is
	// still claimed and enqueued, a token is still issued, and this instance
	// is still the last one.
	if last, err := tw.a.currentLeader().LastInstance(context.Background()); err != nil || !last {
		t.Fatalf("last-instance verdict against a slow but healthy store = %v, %v; want true, nil", last, err)
	}
	stopClaim := startConcurrency01ClaimLoop(t, tw.a)
	defer stopClaim()
	tw.a.upstream.enqueueTask("task-slow-store")
	tw.waitForTaskJob(t, "task-slow-store", 15*time.Second)
	if token := tw.token(t, tw.a, "task-slow-store"); token == "" {
		t.Fatal("token issuance produced no token")
	}
	if !tw.coord.Available() {
		t.Fatal("a slow but healthy store was read as unavailable")
	}
	if lines := tw.stateEventLines(); len(lines) != len(baseline) {
		t.Fatalf("a slow but healthy store moved the availability events: %v -> %v", baseline, lines)
	}

	// Segment three: the store stalls past its own bound while every call that
	// reaches it carries a short budget. The short calls end at their own
	// budgets, and the outage still appears inside the store's bound — a
	// short-budget caller must not exempt a stalled store.
	stopClaim()
	// Background lease traffic would make the bound below ambiguous, so the
	// shutdown caller runs alone against the stalled store.
	leader := tw.a.currentLeader()
	tw.a.stopLeader(t)
	gate.stall()
	stallStarted := time.Now()
	shortA, cancelA := context.WithTimeout(context.Background(), callerBudget)
	defer cancelA()
	if _, err := leader.LastInstance(shortA); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled store under a short caller budget = %v, want context.DeadlineExceeded", err)
	}
	shortReturn := time.Since(stallStarted)
	shortB, cancelB := context.WithTimeout(context.Background(), callerBudget)
	defer cancelB()
	if err := tw.coord.DropQueue(shortB, "ws-1", "task-stalled"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled store, second short call = %v, want context.DeadlineExceeded", err)
	}

	lines := tw.awaitEventCount(t, len(baseline)+1, storeBound+2*time.Second)
	unavailableAt := time.Since(stallStarted)
	if last := lines[len(lines)-1]; last != "unavailable" {
		t.Fatalf("availability events after the stall = %v, want an unavailable transition", lines)
	}
	if unavailableAt > storeBound+500*time.Millisecond {
		t.Fatalf("the outage appeared %s after the stall, past the store's own %s bound", unavailableAt, storeBound)
	}
	if unavailableAt <= shortReturn {
		t.Fatalf("the outage appeared at %s, before the short-budget call returned at %s", unavailableAt, shortReturn)
	}
	if tw.coord.Available() {
		t.Fatal("a stalled store is still read as available")
	}

	// Recovery: the first successful operation closes the interval, and the
	// recovered event precedes the next claim.
	gate.resume()
	deadline := time.Now().Add(storeBound + 3*time.Second)
	for !tw.coord.Available() || len(tw.stateEventLines()) < len(baseline)+2 {
		if time.Now().After(deadline) {
			t.Fatalf("the store did not recover; events=%v", tw.stateEventLines())
		}
		if err := tw.coord.DropQueue(context.Background(), "ws-1", "task-recover"); err != nil {
			time.Sleep(20 * time.Millisecond)
		}
	}
	lines = tw.stateEventLines()
	if len(lines) != len(baseline)+2 {
		t.Fatalf("availability events after recovery = %v, want the baseline plus unavailable and recovered", lines)
	}
	if text := lines[len(lines)-1]; !strings.HasPrefix(text, "recovered") {
		t.Fatalf("the last availability event = %q, want a recovered transition", text)
	}

	// The control plane comes back and claims again; recovery precedes it.
	tw.a.startLeader(t)
	stopAfter := startConcurrency01ClaimLoop(t, tw.a)
	defer stopAfter()
	tw.a.upstream.enqueueTask("task-after-recovery")
	tw.waitForTaskJob(t, "task-after-recovery", 15*time.Second)
	if got := tw.stateEventLines(); len(got) != len(lines) {
		t.Fatalf("the claim after recovery moved the availability events: %v -> %v", lines, got)
	}
}
