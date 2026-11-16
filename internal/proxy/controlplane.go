package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// ControlPlane is one workspace's daemon-side loop set: C1 preflight + claim,
// C2 heartbeats (keeps the registered runtimes online on the server) and the
// dispatcher's convergence round. The resident daemons heartbeat to Foreman
// (S5); Foreman heartbeats to the multica server (C2) so the runtime it
// registered is not judged offline by a dispatch.
type ControlPlane struct {
	Claim     *ClaimLoop
	Heartbeat *HeartbeatLoop
	Reconcile *ReconcileLoop
}

// ControlPlaneOptions are the per-workspace construction inputs.
type ControlPlaneOptions struct {
	ProxyConfig Config
	Client      *Client
	Claimer     Claimer
	Registry    *registry.Registry
	Capacity    func() int
	Reconciler  Reconciler
	Interval    time.Duration
	Logger      *slog.Logger
}

// Reconciler converges in-flight tasks (the dispatcher's round).
type Reconciler interface {
	Reconcile(ctx context.Context) error
}

// NewControlPlane builds the loop set for one workspace.
func NewControlPlane(opts ControlPlaneOptions) *ControlPlane {
	interval := opts.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &ControlPlane{
		Claim:     NewClaimLoop(opts.ProxyConfig, opts.Client, opts.Claimer, opts.Capacity),
		Heartbeat: NewHeartbeatLoop(opts.Client, opts.ProxyConfig.LeaseRefreshInterval),
		Reconcile: NewReconcileLoop(opts.Reconciler, interval),
	}
}

// Component is one supervised loop.
type Component struct {
	Name string
	Run  func(context.Context) error
}

// Components renders the loop set in supervision order.
func (c *ControlPlane) Components() []Component {
	return []Component{
		{Name: "claim", Run: c.Claim.Run},
		{Name: "heartbeat", Run: c.Heartbeat.Run},
		{Name: "reconcile", Run: c.Reconcile.Run},
	}
}

// HeartbeatLoop sends C2 for every registered real runtime on a fixed cadence.
// It replaces v1's per-runtime heartbeat supervisor: there is one runtime per
// registered daemon, and the loop reconciles the set each tick.
type HeartbeatLoop struct {
	client   *Client
	interval time.Duration
	log      *slog.Logger
}

// NewHeartbeatLoop builds the loop.
func NewHeartbeatLoop(client *Client, interval time.Duration) *HeartbeatLoop {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	return &HeartbeatLoop{client: client, interval: interval, log: slog.Default().With("component", "heartbeat-loop")}
}

// Run heartbeats every runtime until ctx ends.
//
// A heartbeat failure is reported once per distinct cause rather than once per
// tick: a server that is down (or a credential that stopped being valid after a
// stack rebuild) would otherwise emit one WARN per runtime every 15s forever,
// burying everything else in the log. Recovery is logged too, so the operator
// can see the outage window.
func (l *HeartbeatLoop) Run(ctx context.Context) error {
	var lastErr string
	for {
		for _, id := range l.client.RuntimeIDs() {
			err := l.client.Heartbeat(ctx, id)
			if ctx.Err() != nil {
				return nil
			}
			switch {
			case err == nil:
				if lastErr != "" {
					l.log.Info("heartbeat recovered", "runtime_id", id)
					lastErr = ""
				}
			default:
				msg := err.Error()
				if msg == lastErr {
					continue
				}
				lastErr = msg
				var se *StatusError
				if errors.As(err, &se) && se.Code == http.StatusUnauthorized {
					// Not transient: the credential Foreman holds is no longer
					// accepted, so no amount of retrying fixes it.
					l.log.Error("heartbeat unauthorized: the server credential is no longer valid",
						"runtime_id", id, "err", err)
					continue
				}
				l.log.Warn("heartbeat failed", "runtime_id", id, "err", err)
			}
		}
		if !sleepOrDone(ctx, l.interval) {
			return nil
		}
	}
}

// ReconcileLoop runs the dispatcher's convergence round on a fixed cadence.
type ReconcileLoop struct {
	rec      Reconciler
	interval time.Duration
	log      *slog.Logger
	// rounds and lastNote drive the same low-rate idle breadcrumb the claim
	// loop uses: a convergence round that keeps succeeding logs nothing, so a
	// wedged reconciler and a healthy quiet one look identical at INFO.
	rounds   int
	lastNote time.Time
}

// NewReconcileLoop builds the loop.
func NewReconcileLoop(rec Reconciler, interval time.Duration) *ReconcileLoop {
	return &ReconcileLoop{
		rec:      rec,
		interval: interval,
		log:      slog.Default().With("component", "reconcile-loop"),
	}
}

// Run runs one round per interval until ctx ends.
func (l *ReconcileLoop) Run(ctx context.Context) error {
	for {
		if l.rec != nil {
			if err := l.rec.Reconcile(ctx); err != nil && ctx.Err() == nil {
				l.log.Warn("reconcile round failed", "err", err)
			}
		}
		l.noteRound()
		if !sleepOrDone(ctx, l.interval) {
			return nil
		}
	}
}

// noteRound emits a low-rate INFO breadcrumb. A convergence round that keeps
// succeeding writes nothing, so without this line a wedged reconciler is
// indistinguishable from a healthy quiet one at the default log level. Ten
// minutes matches the claim loop's cadence and stays readable.
func (l *ReconcileLoop) noteRound() {
	now := time.Now()
	l.rounds++
	if !l.lastNote.IsZero() && now.Sub(l.lastNote) < idleHeartbeatInterval {
		return
	}
	l.log.Info("reconcile.round", "rounds", l.rounds, "interval", l.interval.String())
	l.lastNote = now
	l.rounds = 0
}
