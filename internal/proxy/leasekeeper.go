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
// (proxy.md §prepare-lease 保活).
type LeaseHooks interface {
	// OnDeliveryUnconfirmed settles the S6 lost-response form: the entry
	// reached the boot bound on a committed delivery without a start fact, so
	// no delivery attempt was ever confirmed (failure-handling scenario #18,
	// ADR-032).
	OnDeliveryUnconfirmed(ctx context.Context, taskID string) error
	// OnBootTimeout fails the task (failure-handling scenario #4).
	OnBootTimeout(ctx context.Context, taskID string) error
	// ConvergeLeaseRefused settles a C4-400 entry against the server (C13).
	ConvergeLeaseRefused(ctx context.Context, taskID string) error
	// OnTaskVanished drops an entry whose task the server deleted (#9).
	OnTaskVanished(ctx context.Context, taskID string) error
}

// LeaseKeeper renews prepare leases (C4) for tasks that are claimed but not
// yet started, every FOREMAN_LEASE_REFRESH_INTERVAL (must stay < the 45s
// server lease, §1.1 C3). It also enforces the pre-start terminal bound, the
// boot deadline (scenario #4), with the S6 delivery attribution split
// (scenario #18, ADR-032); the delivery confirmation window itself is not a
// bound here — it releases the delivery for another attempt on the claim
// path.
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
			// deadline (contract §3.1) and emit the observation line
			// protocol-lease.md 场景 A judges. Info, not debug: that line
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
			// expired one (contract §3.1, ADR-026), so the judgement holds only
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
			// Transient failure: next tick retries (§4 backoff envelope).
			k.log.Debug("prepare-lease failed", "task_id", e.TaskID, "err", err)
		}
	}
}

// leaseable reports whether the entry still needs prepare-lease renewals:
// claimed by this process, not yet started, not terminal (proxy.md
// §prepare-lease 保活). The claim's payload is the criterion — a rebuilt
// entry has none (F3) — never the boot bound: the bound is the boot face's
// own criterion, and it is absent both on rebuilt entries and when
// FOREMAN_JOB_BOOT_TIMEOUT=0, where the two would part ways (ADR-026: the two
// criteria never derive from each other). Leasing a rebuilt entry would block
// the server's redispatch; the startup convergence owns it (failure-handling
// scenario #2).
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
