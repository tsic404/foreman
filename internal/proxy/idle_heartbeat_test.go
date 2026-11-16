package proxy

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func idleLoopWithCapture(t *testing.T) (*ClaimLoop, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	l := &ClaimLoop{
		cfg:      Config{ClaimInterval: time.Second, ClaimIntervalIdle: time.Second},
		log:      slog.New(slog.NewJSONHandler(buf, nil)).With("component", "claim-loop"),
		Capacity: func() int { return 0 },
	}
	return l, buf
}

// TestNoteIdleRoundEmitsOnceThenThrottles is the log-volume contract: the
// first idle round states the loop is alive, and the following rounds inside
// the interval stay silent — otherwise a 30s idle cadence would emit thousands
// of lines a day and bury everything else.
func TestNoteIdleRoundEmitsOnceThenThrottles(t *testing.T) {
	l, buf := idleLoopWithCapture(t)

	l.noteIdleRound()
	if got := strings.Count(buf.String(), `"msg":"claim.idle"`); got != 1 {
		t.Fatalf("first idle round produced %d claim.idle lines, want 1:\n%s", got, buf.String())
	}
	if !strings.Contains(buf.String(), `"capacity":0`) {
		t.Fatalf("the line should carry the capacity:\n%s", buf.String())
	}

	// Rounds inside the window must not emit.
	for i := 0; i < 50; i++ {
		l.noteIdleRound()
	}
	if got := strings.Count(buf.String(), `"msg":"claim.idle"`); got != 1 {
		t.Fatalf("throttled rounds added %d lines, want 0 extra", got-1)
	}
	// The counter is reset when a line is emitted, so the 50 silent rounds are
	// what accumulates (plus the one that just emitted and reset).
	if l.idleRounds != 50 {
		t.Fatalf("idleRounds = %d, want 50 (counting continues while silent)", l.idleRounds)
	}
}

// TestNoteIdleRoundResumesAfterInterval: once the window elapses the breadcrumb
// returns, so a long-idle process keeps proving it is alive.
func TestNoteIdleRoundResumesAfterInterval(t *testing.T) {
	l, buf := idleLoopWithCapture(t)

	l.noteIdleRound()
	// Pretend the previous note was written just over the interval ago.
	l.lastIdleNote = time.Now().Add(-idleHeartbeatInterval - time.Second)
	l.noteIdleRound()

	if got := strings.Count(buf.String(), `"msg":"claim.idle"`); got != 2 {
		t.Fatalf("claim.idle lines = %d, want 2 (one before the window, one after)", got)
	}
}

// TestReconcileLoopNotesRoundsAtLowRate: the convergence loop must leave the
// same kind of breadcrumb — a successful round writes nothing, so silence has
// to mean "quiet", never "wedged".
func TestReconcileLoopNotesRoundsAtLowRate(t *testing.T) {
	buf := &bytes.Buffer{}
	l := &ReconcileLoop{
		interval: time.Second,
		log:      slog.New(slog.NewJSONHandler(buf, nil)),
	}

	l.noteRound()
	if got := strings.Count(buf.String(), `"msg":"reconcile.round"`); got != 1 {
		t.Fatalf("first round produced %d reconcile.round lines, want 1:\n%s", got, buf.String())
	}
	for i := 0; i < 20; i++ {
		l.noteRound()
	}
	if got := strings.Count(buf.String(), `"msg":"reconcile.round"`); got != 1 {
		t.Fatalf("throttled rounds added lines: %d total", got)
	}
	if l.rounds != 20 {
		t.Fatalf("rounds = %d, want 20", l.rounds)
	}
}
