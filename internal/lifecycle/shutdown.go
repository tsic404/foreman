package lifecycle

import (
	"context"
	"log/slog"
	"time"
)

// LastInstanceGate is the shared store's instance-count verdict: true only
// when this process is the only live instance.
type LastInstanceGate interface {
	LastInstance(ctx context.Context) (bool, error)
}

// Deregisterer is one served workspace identity's graceful-exit face. The
// implementation sends C14 carrying that identity's full real runtime set.
type Deregisterer interface {
	Deregister(ctx context.Context) error
	WorkspaceID() string
}

// DeregisterOnShutdown applies the shutdown narrowing of §4.6 优雅关闭: C14 is
// sent per served identity only when the shared store answered and this
// process is the last live instance. A survivor holds the logical identity,
// and an unreachable store cannot prove the instance count, so both cases send
// nothing and leave the row to the server's own expiry. A nil gate keeps the
// single-instance form, where no shared state exists to consult.
func DeregisterOnShutdown(ctx context.Context, gate LastInstanceGate, served []Deregisterer, log *slog.Logger) {
	if gate != nil {
		last, err := gate.LastInstance(ctx)
		if err != nil || !last {
			reason := "another instance is alive"
			if err != nil {
				reason = "shared state unavailable"
			}
			log.Info("daemon.deregister_skipped", "reason", reason)
			return
		}
	}
	for _, c := range served {
		if err := c.Deregister(ctx); err != nil {
			log.Warn("daemon.deregister_failed", "workspace_id", c.WorkspaceID(), "err", err.Error())
		}
	}
}

// ShutdownOptions is one process's graceful-exit face. Cancel stops the
// long-lived loops, Drain stops the HTTP listener, and Wait blocks until
// every loop returned or the shutdown budget expired.
type ShutdownOptions struct {
	Cancel func()
	// Gate is the shared store's last-instance verdict; nil on the
	// single-instance form.
	Gate   LastInstanceGate
	Served []Deregisterer
	Drain  func(context.Context)
	Wait   func(context.Context)
	Log    *slog.Logger
}

// Shutdown runs the exit sequence of §4.6 优雅关闭 and returns the instant it
// began, immediately before the loops were cancelled. The production order is
// kept: C14 is decided and sent before Wait, so an exit never delays
// deregistration behind a claim round still in flight.
func Shutdown(ctx context.Context, opts ShutdownOptions) time.Time {
	started := time.Now()
	if opts.Cancel != nil {
		opts.Cancel()
	}
	DeregisterOnShutdown(ctx, opts.Gate, opts.Served, opts.Log)
	if opts.Drain != nil {
		opts.Drain(ctx)
	}
	if opts.Wait != nil {
		opts.Wait(ctx)
	}
	return started
}
