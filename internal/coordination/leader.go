// Package coordination implements the control-plane side of ADR-046's stage
// two: the shared leader lease that grants the control-plane actions
// (registration, heartbeat, subscriptions, orphan reclaim, claim, periodic
// convergence), the bounded takeover window, and the instance liveness rows
// the shutdown path uses to narrow deregistration (02-contracts.md §0 术语
// leader, §4.6 优雅关闭, §10 控制面接管有界).
package coordination

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tsic404/foreman/internal/state"
)

// Environment variables of the coordination module.
const (
	// EnvLeaseTTL bounds the leader lease: the control plane is unavailable
	// for at most this long after the holder dies (before the takeover
	// window adds to it).
	EnvLeaseTTL = "FOREMAN_LEADER_LEASE_TTL"
	// EnvTakeoverWindow is the quiet period a new leader observes before it
	// runs reconciliation and starts claiming (§10 接管窗口内不产生新 claim).
	// It must be at least one renewal tick (a third of EnvLeaseTTL): the old
	// holder notices the loss on its next renewal, so a shorter window lets
	// its stale hold outlive the new holder's grant. LoadConfig rejects one.
	EnvTakeoverWindow = "FOREMAN_LEADER_TAKEOVER_WINDOW"
	// EnvInstanceTTL is the liveness-row retention used by the shutdown
	// narrowing: an instance is alive while its row exists.
	EnvInstanceTTL = "FOREMAN_INSTANCE_TTL"
	// EnvTaskLockTTL is the per-task write lock's lease upper bound. It must
	// cover the longest locked action (the claim chain incl. the Job-name
	// wait); a dead holder's lock expires after it and the convergence round
	// takes the action over (§4.2 写锁自带租约上界).
	EnvTaskLockTTL = "FOREMAN_TASK_LOCK_TTL"
)

const (
	defaultLeaseTTL = 10 * time.Second
	// defaultTakeoverWindow clears one renewal tick of the default lease
	// (10s/3, about 3.3s) so the old holder's stale hold clears inside the
	// quiet period; the relation, not this number, is what LoadConfig checks.
	defaultTakeoverWindow = 5 * time.Second
	defaultInstanceTTL    = 30 * time.Second
	defaultTaskLockTTL    = 120 * time.Second
)

// renewalTick is the lease renewal cadence. Run renews every tick, so a
// takeover window shorter than one tick can end before the old holder's next
// renewal clears its hold (§10 接管窗口内不产生新 claim).
func renewalTick(leaseTTL time.Duration) time.Duration {
	tick := leaseTTL / 3
	if tick < 100*time.Millisecond {
		tick = 100 * time.Millisecond
	}
	return tick
}

// Config is the coordination module's runtime configuration.
type Config struct {
	LeaseTTL       time.Duration
	TakeoverWindow time.Duration
	InstanceTTL    time.Duration
	TaskLockTTL    time.Duration
}

// LoadConfig reads the coordination configuration from env (os.Getenv when
// getenv is nil) and cross-checks the lease timings: a takeover window
// shorter than one renewal tick is a configuration error (§10 控制面接管有界).
func LoadConfig(getenv func(string) string) (Config, error) {
	cfg := Config{
		LeaseTTL:       defaultLeaseTTL,
		TakeoverWindow: defaultTakeoverWindow,
		InstanceTTL:    defaultInstanceTTL,
		TaskLockTTL:    defaultTaskLockTTL,
	}
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	if raw := getenv(EnvLeaseTTL); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return Config{}, errors.New(EnvLeaseTTL + " must be a positive duration")
		}
		cfg.LeaseTTL = d
	}
	if raw := getenv(EnvTakeoverWindow); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			return Config{}, errors.New(EnvTakeoverWindow + " must be a non-negative duration")
		}
		cfg.TakeoverWindow = d
	}
	if raw := getenv(EnvInstanceTTL); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return Config{}, errors.New(EnvInstanceTTL + " must be a positive duration")
		}
		cfg.InstanceTTL = d
	}
	if raw := getenv(EnvTaskLockTTL); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return Config{}, errors.New(EnvTaskLockTTL + " must be a positive duration")
		}
		cfg.TaskLockTTL = d
	}
	if tick := renewalTick(cfg.LeaseTTL); cfg.TakeoverWindow < tick {
		return Config{}, fmt.Errorf("%s (%s) must be >= one renewal tick %s of %s (%s): a shorter quiet period lets the old holder act after the new holder serves",
			EnvTakeoverWindow, cfg.TakeoverWindow, tick, EnvLeaseTTL, cfg.LeaseTTL)
	}
	return cfg, nil
}

// Leader owns this instance's control-plane lease. The control loops gate
// themselves on Held(); the holder change is observable through the lease row
// (LeadershipHolder) and the acquisition/release log events (§9.1 控制面与共享
// 状态事件).
type Leader struct {
	store state.CoordStore
	id    string
	cfg   Config
	log   *slog.Logger

	held    atomic.Bool
	crashed atomic.Bool
	// onAcquire runs after the takeover window and before the control plane
	// is granted; a failure releases the lease and retries (the acquire path
	// must not serve an unreconciled state).
	onAcquire func(context.Context) error
	onLost    func()
	metrics   Metrics
}

// Metrics is the coordination module's observable face (§9.2 租约持有者变更).
type Metrics interface {
	LeaderChange(result string)
}

// Option customizes a Leader.
type Option func(*Leader)

// WithMetrics wires the lease-change counter.
func WithMetrics(m Metrics) Option {
	return func(l *Leader) { l.metrics = m }
}

// WithLogger wires the structured logger.
func WithLogger(log *slog.Logger) Option {
	return func(l *Leader) { l.log = log }
}

// WithOnAcquire wires the takeover reconciliation hook (resync, queue sweep,
// slot reconcile). It must return before the control plane serves.
func WithOnAcquire(fn func(context.Context) error) Option {
	return func(l *Leader) { l.onAcquire = fn }
}

// WithOnLost wires the notification fired when the lease is lost (loops
// notice through Held() as well; the hook exists for observability).
func WithOnLost(fn func()) Option {
	return func(l *Leader) { l.onLost = fn }
}

// New builds the leader for one instance. The instance id is unique per
// process and names the lease owner, the task-lock owner and the liveness row.
func New(store state.CoordStore, instanceID string, cfg Config, opts ...Option) *Leader {
	l := &Leader{store: store, id: instanceID, cfg: cfg}
	for _, opt := range opts {
		opt(l)
	}
	if l.log == nil {
		l.log = slog.Default()
	}
	return l
}

// InstanceID is this process's identity.
func (l *Leader) InstanceID() string { return l.id }

// Crash parks the instance as a dead holder: the lease is left to expire (no
// graceful release), exactly like a killed process. It is the takeover
// scenario's kill switch.
func (l *Leader) Crash() {
	l.crashed.Store(true)
	l.held.Store(false)
}

// Held reports whether this instance owns the control plane. The control
// loops call it every iteration; a false answer parks them without exiting.
func (l *Leader) Held() bool { return l.held.Load() }

// Run acquires and renews the lease until ctx ends. The loop never returns a
// fatal error: an unreachable store parks the instance as a follower and the
// retry continues (§10 共享状态不可用).
func (l *Leader) Run(ctx context.Context) error {
	tick := renewalTick(l.cfg.LeaseTTL)
	for {
		if !l.held.Load() {
			ok, err := l.store.AcquireLeadership(ctx, l.id, l.cfg.LeaseTTL)
			if err == nil && ok {
				if !l.holdThroughWindow(ctx, tick) {
					if !l.crashed.Load() {
						_ = l.store.ReleaseLeadership(context.WithoutCancel(ctx), l.id)
					}
					if ctx.Err() != nil {
						return nil
					}
					if !sleep(ctx, tick) {
						return nil
					}
					continue
				}
				if l.onAcquire != nil {
					// The takeover makes destructive writes (counter
					// reconciliation, queue enqueue). Run it under a live
					// renewal: if the lease lapses, the reconcile context is
					// cancelled so the writes stop, and the grant is withheld
					// (§10 控制面接管有界/单持有).
					held, err := l.runUnderLease(ctx, tick, l.onAcquire)
					if !held {
						l.log.Warn("leader.grant_aborted", "instance_id", l.id, "reason", "lease lapsed during reconcile")
						_ = l.store.ReleaseLeadership(context.WithoutCancel(ctx), l.id)
						if !sleep(ctx, tick) {
							return nil
						}
						continue
					}
					if err != nil {
						l.log.Error("leader.reconcile_failed", "err", err.Error())
						_ = l.store.ReleaseLeadership(context.WithoutCancel(ctx), l.id)
						if !sleep(ctx, tick) {
							return nil
						}
						continue
					}
				}
				// The takeover work can outlive the lease: re-verify ownership
				// before serving. If it lapsed, another instance may have
				// lawfully acquired it, and this one must stay a follower
				// (§10 控制面接管有界/单持有).
				renewed, rerr := l.store.RenewLeadership(ctx, l.id, l.cfg.LeaseTTL)
				if rerr != nil || !renewed {
					l.log.Warn("leader.grant_lost", "instance_id", l.id, "err", errString(rerr))
					_ = l.store.ReleaseLeadership(context.WithoutCancel(ctx), l.id)
					if !sleep(ctx, tick) {
						return nil
					}
					continue
				}
				l.held.Store(true)
				if l.metrics != nil {
					l.metrics.LeaderChange("acquired")
				}
				l.log.Info("leader.acquired", "instance_id", l.id)
			} else if err != nil {
				l.log.Warn("leader.acquire_failed", "err", err.Error())
			}
		} else {
			ok, err := l.store.RenewLeadership(ctx, l.id, l.cfg.LeaseTTL)
			if err != nil || !ok {
				l.held.Store(false)
				if l.metrics != nil {
					l.metrics.LeaderChange("lost")
				}
				if l.onLost != nil {
					l.onLost()
				}
				l.log.Warn("leader.lost", "instance_id", l.id, "err", errString(err))
			}
		}
		if !sleep(ctx, tick) {
			if l.held.Load() && !l.crashed.Load() {
				_ = l.store.ReleaseLeadership(context.WithoutCancel(ctx), l.id)
			}
			return nil
		}
	}
}

// runUnderLease runs the takeover reconcile while renewing the lease in the
// background. A renewal failure cancels the reconcile's context (its store
// writes fail closed) and reports held=false, so the candidate never serves
// after another instance may have lawfully taken the lease.
func (l *Leader) runUnderLease(ctx context.Context, tick time.Duration, fn func(context.Context) error) (bool, error) {
	reconcileCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	lost := make(chan struct{})
	var lostOnce sync.Once
	var finished atomic.Bool
	go func() {
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		for {
			select {
			case <-reconcileCtx.Done():
				return
			case <-ticker.C:
				if finished.Load() {
					return
				}
				ok, err := l.store.RenewLeadership(reconcileCtx, l.id, l.cfg.LeaseTTL)
				if err != nil || !ok {
					lostOnce.Do(func() { close(lost) })
					cancel()
					return
				}
			}
		}
	}()
	err := fn(reconcileCtx)
	finished.Store(true)
	select {
	case <-lost:
		return false, err
	default:
		return true, err
	}
}

// holdThroughWindow observes the takeover quiet period while renewing the
// lease, so a long window cannot let the lease lapse before the holder is
// announced (§10 接管窗口内不产生新 claim；授予前必须仍持有租约).
func (l *Leader) holdThroughWindow(ctx context.Context, tick time.Duration) bool {
	if l.cfg.TakeoverWindow <= 0 {
		return true
	}
	deadline := time.Now().Add(l.cfg.TakeoverWindow)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return true
		}
		wait := tick
		if remaining < wait {
			wait = remaining
		}
		if !sleep(ctx, wait) {
			return false
		}
		if !l.renew(ctx) {
			return false
		}
	}
}

// renew extends the lease and reports whether this instance still holds it.
func (l *Leader) renew(ctx context.Context) bool {
	ok, err := l.store.RenewLeadership(ctx, l.id, l.cfg.LeaseTTL)
	if err != nil || !ok {
		l.log.Warn("leader.window_lost", "instance_id", l.id, "err", errString(err))
		return false
	}
	return true
}

// WaitHeld blocks until this instance holds the control plane or the timeout
// ends. The startup path uses it to decide whether to run the startup probe.
func (l *Leader) WaitHeld(ctx context.Context, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if l.Held() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		if !sleep(ctx, 100*time.Millisecond) {
			return false
		}
	}
}

// HeartbeatInstance refreshes this instance's liveness row until ctx ends.
func (l *Leader) HeartbeatInstance(ctx context.Context) error {
	interval := l.cfg.InstanceTTL / 3
	if interval < 200*time.Millisecond {
		interval = 200 * time.Millisecond
	}
	for {
		_ = l.store.HeartbeatInstance(ctx, l.id, l.cfg.InstanceTTL)
		if !sleep(ctx, interval) {
			return nil
		}
	}
}

// LastInstance reports whether this instance is the only live one. The
// shutdown path deregisters only in that case (§4.6 优雅关闭).
func (l *Leader) LastInstance(ctx context.Context) (bool, error) {
	if !l.store.Available() {
		return false, errors.New("shared state unavailable")
	}
	ids, err := l.store.ListInstances(ctx)
	if err != nil {
		return false, err
	}
	if len(ids) == 0 {
		return true, nil
	}
	return len(ids) == 1 && ids[0] == l.id, nil
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// InstanceIDFor derives the process instance id from a seed (the process
// start time and pid in production) so two instances never share a lease
// owner or a lock owner. Uniqueness is all that matters; the value is opaque.
func InstanceIDFor(seed string) string {
	if seed == "" {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return seed
}
