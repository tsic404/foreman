package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/observability"
)

func testSupervisor(t *testing.T, health *observability.Health) (*supervisor, context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return newSupervisor(slog.New(slog.NewJSONHandler(io.Discard, nil)), health, cancel), ctx, cancel
}

// drain waits for every component goroutine to return, with a bound.
func drain(t *testing.T, sup *supervisor) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if !sup.wait(waitCtx) {
		t.Fatal("components did not return within the drain budget")
	}
}

// TestSupervisorEscalatesComponentFailure pins the §1.5 escalation: a loop
// that stops with an error while the process is still meant to serve is a
// process-level failure — the root context is cancelled and the error is
// reported, so main exits non-zero and the Deployment restarts (the
// CrashLoopBackOff is the alarm). The liveness view follows the loop.
func TestSupervisorEscalatesComponentFailure(t *testing.T) {
	health := observability.NewHealth()
	sup, ctx, _ := testSupervisor(t, health)

	sup.run(ctx, "claim", func(context.Context) error { return errors.New("403 config error") })

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a component failure must cancel the root context")
	}
	err := sup.failure()
	if err == nil || !strings.Contains(err.Error(), "claim") || !strings.Contains(err.Error(), "403") {
		t.Fatalf("failure = %v, want the claim component's error", err)
	}
	drain(t, sup)
	if got := health.Stopped(); len(got) != 1 || got[0] != "claim" {
		t.Fatalf("health stopped = %v, want [claim]", got)
	}
}

// TestSupervisorKeepsGracefulShutdownQuiet: the ordinary cancellation path is
// not a failure — a component that returns nil (or an error after the context
// ended) leaves the process with no fatal error to report.
func TestSupervisorKeepsGracefulShutdownQuiet(t *testing.T) {
	health := observability.NewHealth()
	sup, ctx, cancel := testSupervisor(t, health)

	sup.run(ctx, "heartbeat", func(c context.Context) error {
		<-c.Done()
		return nil
	})
	sup.run(ctx, "recovery", func(c context.Context) error {
		<-c.Done()
		return errors.New("upstream closed during shutdown")
	})

	cancel()
	drain(t, sup)
	if err := sup.failure(); err != nil {
		t.Fatalf("failure = %v, want nil on a graceful shutdown", err)
	}
	if got := health.Stopped(); len(got) != 2 {
		t.Fatalf("health stopped = %v, want both components after shutdown", got)
	}
}
