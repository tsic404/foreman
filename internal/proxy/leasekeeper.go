package proxy

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// LeaseClient is the lease surface of the fake client (C4/C13).
type LeaseClient interface {
	// PrepareLease extends the task's prepare lease (C4): ready=false is
	// the semantic 400 (task left the renewable pre-start state), not an
	// error; a deleted task returns scheduler.ErrTaskNotFound.
	PrepareLease(ctx context.Context, taskID string) (ready bool, err error)
}

// LeaseHooks are the scheduler's settlement entries the LeaseKeeper drives
// (proxy.md §prepare-lease 保活).
type LeaseHooks interface {
	// OnBootTimeout fails the task (failure-handling scenario #4).
	OnBootTimeout(ctx context.Context, taskID string) error
	// ConvergeLeaseRefused settles a C4-400 entry against the server (C13).
	ConvergeLeaseRefused(ctx context.Context, taskID string) error
	// OnTaskVanished drops an entry whose task the server deleted (#9).
	OnTaskVanished(ctx context.Context, taskID string) error
}

// LeaseKeeper renews prepare leases (C4) for tasks that are claimed but not
// yet started, every FOREMAN_LEASE_REFRESH_INTERVAL (must stay < the 45s
// server lease, §1.1 C3). It also enforces the boot deadline (scenario #4).
type LeaseKeeper struct {
	client   LeaseClient
	reg      *registry.Registry
	hooks    LeaseHooks
	interval time.Duration
	now      func() time.Time
	log      *slog.Logger
}

// NewLeaseKeeper builds the keeper; interval is cfg.LeaseRefreshInterval.
func NewLeaseKeeper(client LeaseClient, reg *registry.Registry, hooks LeaseHooks, interval time.Duration) *LeaseKeeper {
	return &LeaseKeeper{
		client:   client,
		reg:      reg,
		hooks:    hooks,
		interval: interval,
		now:      time.Now,
		log:      slog.Default().With("component", "lease-keeper"),
	}
}

// Run renews leases until ctx ends.
func (k *LeaseKeeper) Run(ctx context.Context) error {
	for {
		k.round(ctx)
		if !sleepOrDone(ctx, k.interval) {
			return nil
		}
	}
}

// round renews every leaseable entry once (proxy.md §prepare-lease 保活).
func (k *LeaseKeeper) round(ctx context.Context) {
	for _, e := range k.reg.List() {
		if !leaseable(e) {
			continue
		}
		ready, err := k.client.PrepareLease(ctx, e.TaskID)
		switch {
		case err == nil && ready:
			if k.now().After(e.BootDeadline) {
				if herr := k.hooks.OnBootTimeout(ctx, e.TaskID); herr != nil {
					k.log.Error("boot-timeout settlement failed", "task_id", e.TaskID, "err", herr)
				}
			}
		case err == nil && !ready:
			// ready=false (C4 400): the task left the renewable pre-start
			// state — converge via C13 (started / terminal / gone / retry).
			if cerr := k.hooks.ConvergeLeaseRefused(ctx, e.TaskID); cerr != nil {
				k.log.Debug("lease-refused convergence deferred", "task_id", e.TaskID, "err", cerr)
			}
		case errors.Is(err, scheduler.ErrTaskNotFound):
			if verr := k.hooks.OnTaskVanished(ctx, e.TaskID); verr != nil {
				k.log.Error("task-vanish settlement failed", "task_id", e.TaskID, "err", verr)
			}
		default:
			// Transient failure: next tick retries (§4 backoff envelope).
			k.log.Debug("prepare-lease failed", "task_id", e.TaskID, "err", err)
		}
	}
}

// leaseable reports whether the entry still needs prepare-lease renewals:
// claimed, not yet started, not terminal (proxy.md §prepare-lease 保活).
// Entries rebuilt from K8s after a restart carry neither payload nor boot
// deadline (§3.1/§3.2): leasing them would block the server's redispatch
// and a zero deadline would fake a boot timeout — the startup convergence
// owns them (failure-handling scenario #2).
func leaseable(e registry.TaskEntry) bool {
	if e.IsTerminal() || !e.StartedAt.IsZero() {
		return false
	}
	if e.BootDeadline.IsZero() || len(e.Payload) == 0 {
		return false
	}
	switch e.State {
	case registry.StatePending, registry.StateJobCreated,
		registry.StateDaemonRegistered, registry.StateDelivering:
		return true
	}
	return false
}
