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
	// PrepareLease extends the task's prepare lease (C4) on the real runtime
	// the task was dispatched under: ready=false is the semantic 400 (task
	// left the renewable pre-start state), not an error; a deleted task
	// returns scheduler.ErrTaskNotFound.
	PrepareLease(ctx context.Context, runtimeID, taskID string) (ready bool, err error)
}

// LeaseHooks are the scheduler's settlement entries the LeaseKeeper drives
// (S-7 + §10 租约不超时).
type LeaseHooks interface {
	// OnDeliveryUnconfirmed settles the S6 lost-response form: the entry
	// reached the boot bound on a committed delivery without a start fact, so
	// no delivery attempt was ever confirmed (§5,
	// ADR-032).
	OnDeliveryUnconfirmed(ctx context.Context, taskID string) error
	// OnBootTimeout fails the task (§4.4).
	OnBootTimeout(ctx context.Context, taskID string) error
	// ConvergeLeaseRefused settles a C4-400 entry against the server (C13).
	ConvergeLeaseRefused(ctx context.Context, taskID string) error
	// OnTaskVanished drops an entry whose task the server deleted (§4.6).
	OnTaskVanished(ctx context.Context, taskID string) error
}

// LeaseKeeper renews prepare leases (C4) for tasks that are claimed but not
// yet started, every FOREMAN_LEASE_REFRESH_INTERVAL (capped at 20s by the
// config guard, §10). It also enforces the pre-start terminal bound, the
// boot deadline (§4.4), with the S6 delivery attribution split
// (§5, ADR-032); the delivery confirmation window itself is not a
// bound here — it releases the delivery for another attempt on the claim
// path.
type LeaseKeeper struct {
	client   LeaseClient
	reg      *registry.Registry
	hooks    LeaseHooks
	interval time.Duration
	// workspace filters the round to one workspace's entries: each
	// (credential, daemon identity) pair renews only its own tasks (§1.4
	// 失败粒度含 workspace). Empty keeps the legacy global round.
	workspace string
	now       func() time.Time
	log       *slog.Logger
}

// LeaseKeeperOption customizes a LeaseKeeper.
type LeaseKeeperOption func(*LeaseKeeper)

// WithLeaseWorkspace binds the keeper to one workspace's entries.
func WithLeaseWorkspace(workspaceID string) LeaseKeeperOption {
	return func(k *LeaseKeeper) { k.workspace = workspaceID }
}

// NewLeaseKeeper builds the keeper; interval is cfg.LeaseRefreshInterval.
func NewLeaseKeeper(client LeaseClient, reg *registry.Registry, hooks LeaseHooks, interval time.Duration, opts ...LeaseKeeperOption) *LeaseKeeper {
	k := &LeaseKeeper{
		client:   client,
		reg:      reg,
		hooks:    hooks,
		interval: interval,
		now:      time.Now,
		log:      slog.Default().With("component", "lease-keeper"),
	}
	for _, opt := range opts {
		opt(k)
	}
	return k
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

// round renews every leaseable entry once (S-7 + §10 租约不超时).
func (k *LeaseKeeper) round(ctx context.Context) {
	for _, e := range k.reg.List() {
		if !leaseable(e) {
			continue
		}
		if k.workspace != "" && e.WorkspaceID != k.workspace {
			continue
		}
		if e.RealRuntimeID == "" {
			// No dispatch record (rebuilt entry): the startup convergence
			// owns it, and a lease against the wrong runtime would read as a
			// vanished task.
			continue
		}
		ready, err := k.client.PrepareLease(ctx, e.RealRuntimeID, e.TaskID)
		switch {
		case err == nil && ready:
			// The renewal restamped the server's lease: project the new
			// deadline (§4.1) and emit the observation line
			// the lease acceptance case judges. Info, not debug: that line
			// carries no level prefix there, i.e. it is read at the design's
			// default level — unlike daemon.heartbeat, whose debug level the
			// same file states explicitly.
			now := k.now()
			until := now.Add(scheduler.PrepareLeaseTTL)
			k.reg.SetLeaseKeepUntil(e.TaskID, until)
			k.log.Info("lease.renewed",
				"task_id", e.TaskID, "job_name", e.JobName,
				"lease_keep_until", registry.ProjectedTime(until))
			// One terminal bound for the pre-start forms: the boot deadline,
			// claim-relative — a zero deadline is "no upper bound", not an
			// expired one (§4.1, ADR-026), so the judgement holds only
			// on a written bound. The delivery confirmation window no longer
			// ends an entry: passing it with no start fact releases the
			// delivery for another attempt on the daemon's next claim (ADR-032).
			// The attribution splits on whether a delivery was ever committed:
			// a delivered entry that never started did not fail to boot.
			switch {
			case e.BootDeadline.IsZero() || !now.After(e.BootDeadline):
			case e.DeliveredAt.IsZero():
				if herr := k.hooks.OnBootTimeout(ctx, e.TaskID); herr != nil {
					k.log.Error("boot-timeout settlement failed", "task_id", e.TaskID, "err", herr)
				}
			default:
				if herr := k.hooks.OnDeliveryUnconfirmed(ctx, e.TaskID); herr != nil {
					k.log.Error("delivery-unconfirmed settlement failed", "task_id", e.TaskID, "err", herr)
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
			// Transient failure: the next tick retries within the §10 bound.
			k.log.Debug("prepare-lease failed", "task_id", e.TaskID, "err", err)
		}
	}
}

// leaseable reports whether the entry still needs prepare-lease renewals:
// claimed by this process, not yet started, not terminal (S-7 +
// §10 租约不超时). The claim's payload is the criterion — a rebuilt
// entry has none (F3) — never the boot bound: the bound is the boot face's
// own criterion, and it is absent both on rebuilt entries and when
// FOREMAN_JOB_BOOT_TIMEOUT=0, where the two would part ways (ADR-026: the two
// criteria never derive from each other). Leasing a rebuilt entry would block
// the server's redispatch; the startup convergence owns it (§4.3).
func leaseable(e registry.TaskEntry) bool {
	if e.IsTerminal() || !e.StartedAt.IsZero() {
		return false
	}
	if len(e.Payload) == 0 {
		return false
	}
	switch e.State {
	case registry.StatePending, registry.StateJobCreated,
		registry.StateDaemonRegistered, registry.StateDelivering:
		return true
	}
	return false
}
