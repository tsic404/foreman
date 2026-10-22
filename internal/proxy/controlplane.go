package proxy

import (
	"context"
	"log/slog"

	"github.com/tsic404/foreman/internal/registry"
)

// ControlPlane is one workspace's daemon-side control-loop set: claim,
// heartbeat, lease keepalive, profile refresh and (when enabled) the WS
// subscription. cmd/foreman and the assembly-level acceptance tests build it
// through NewControlPlane, so the leader gating and the declaration
// publication are wired by the same code in production and under test
// (02-contracts.md §1.1 发出者口径, §0 术语「leader」).
type ControlPlane struct {
	Claim     *ClaimLoop
	Heartbeat *HeartbeatLoop
	Lease     *LeaseKeeper
	Profiles  *ProfileRefresher
	WS        *WSSubscriber

	wsEnabled bool
}

// ControlPlaneOptions are the per-workspace construction inputs.
type ControlPlaneOptions struct {
	ProxyConfig Config
	Client      *Client
	Scheduler   ClaimScheduler
	Registry    *registry.Registry
	LeaseHooks  LeaseHooks
	Reporter    *ModelCatalogReporter
	Metrics     Metrics
	Logger      *slog.Logger
	WorkspaceID string
	WSEnabled   bool
}

// NewControlPlane builds the loop set for one workspace.
func NewControlPlane(opts ControlPlaneOptions) *ControlPlane {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	claim := NewClaimLoop(opts.ProxyConfig, opts.Client, opts.Scheduler)
	heartbeat := NewHeartbeatLoop(opts.Client, opts.Reporter)
	lease := NewLeaseKeeper(opts.Client, opts.Registry, opts.LeaseHooks, opts.ProxyConfig.LeaseRefreshInterval,
		WithLeaseWorkspace(opts.WorkspaceID))
	refresher := NewProfileRefresher(opts.Client, WithProfileRefresherLogger(logger))
	plane := &ControlPlane{
		Claim:     claim,
		Heartbeat: heartbeat,
		Lease:     lease,
		Profiles:  refresher,
		wsEnabled: opts.WSEnabled,
	}
	if opts.WSEnabled {
		plane.WS = NewWSSubscriber(opts.Client, claim.Wake, opts.Metrics,
			WithProfileChangeNotify(refresher.Notify))
	}
	return plane
}

// SetLeaderGate wires the control-plane ownership check on the actions §1.1
// enumerates as holder-only: claim, heartbeat (ack consumption), WS
// subscription and the probe-backed profile refresh. Prepare-lease renewal is
// deliberately NOT gated: §1.1 lists 续租 among the requests a non-holder
// still forwards, and §10 requires a renewal cadence well below the
// control-plane lease timing.
func (c *ControlPlane) SetLeaderGate(g func() bool) {
	c.Claim.SetLeaderGate(g)
	c.Heartbeat.SetLeaderGate(g)
	c.Profiles.SetLeaderGate(g)
	if c.WS != nil {
		c.WS.SetLeaderGate(g)
	}
}

// SetDeclarationPublisher wires the post-C1 declaration writer: the holder
// publishes each workspace's registration identity so followers and a future
// takeover adopt it without re-registering (AC-77 沿用同一注册身份).
func (c *ControlPlane) SetDeclarationPublisher(fn func(context.Context, *Client) error) {
	c.Claim.SetRegisteredHook(fn)
}

// Component is one supervised loop.
type Component struct {
	Name string
	Run  func(context.Context) error
}

// Components renders the loop set in supervision order.
func (c *ControlPlane) Components() []Component {
	out := []Component{
		{Name: "claim", Run: c.Claim.Run},
		{Name: "heartbeat", Run: c.Heartbeat.Run},
		{Name: "lease", Run: c.Lease.Run},
	}
	if c.wsEnabled && c.WS != nil {
		out = append(out,
			Component{Name: "ws-subscriber", Run: c.WS.Run},
			Component{Name: "profile-refresh", Run: c.Profiles.Run},
		)
	}
	return out
}
