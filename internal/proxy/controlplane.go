package proxy

import (
	"context"
	"log/slog"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// ControlPlane is one workspace's daemon-side loop set: claim, daemon
// reconciliation and lease renewal. v2 has no heartbeat-to-server loop (the
// daemons themselves heartbeat to Foreman) and no WS subscription.
type ControlPlane struct {
	Claim     *ClaimLoop
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
		{Name: "reconcile", Run: c.Reconcile.Run},
	}
}

// ReconcileLoop runs the dispatcher's convergence round on a fixed cadence.
type ReconcileLoop struct {
	rec      Reconciler
	interval time.Duration
	log      *slog.Logger
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
		if !sleepOrDone(ctx, l.interval) {
			return nil
		}
	}
}