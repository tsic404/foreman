package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// heartbeatInterval is the C2 cadence (§1.1: every 15s per real runtime).
const heartbeatInterval = 15 * time.Second

// tokenRenewInterval is the C16 cadence (§1.1: every 72h, non-fatal).
const tokenRenewInterval = 72 * time.Hour

// isConfigError reports whether err is a 403 (§1.1 failure table: token ↔
// daemon_id/workspace_id mismatch). 403 is a configuration error: log it
// and stop instead of blindly retrying — a CrashLoopBackOff is the alarm.
func isConfigError(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Code == http.StatusForbidden
}

// ClaimScheduler is the scheduler's claim surface (task-mapping 内部结构).
type ClaimScheduler interface {
	// ClaimBudget is how many tasks may be claimed right now.
	ClaimBudget() int
	// OnClaim registers a claimed task and creates its Job; duplicate
	// claims (server redispatch) are settled inside (task-mapping §重复
	// claim), so the loop hands every claimed task over.
	OnClaim(ctx context.Context, task json.RawMessage) error
}

// ClaimLoop is the polling claim loop (proxy.md §claim 循环): it claims up
// to the scheduler's budget every FOREMAN_CLAIM_INTERVAL, idles at
// FOREMAN_CLAIM_INTERVAL_IDLE when no slot is free, and runs one immediate
// iteration on a WS wakeup (ADR-010). Errors back off per §4 (1s→30s,
// never exit) — except 403, which is a config error and stops the loop.
type ClaimLoop struct {
	cfg     Config
	client  *Client
	sched   ClaimScheduler
	log     *slog.Logger
	wake    chan struct{} // cap 1: the single consumer is this loop
	backoff *backoff
}

// NewClaimLoop builds the claim loop.
func NewClaimLoop(cfg Config, client *Client, sched ClaimScheduler) *ClaimLoop {
	return &ClaimLoop{
		cfg:     cfg,
		client:  client,
		sched:   sched,
		log:     slog.Default().With("component", "claim-loop"),
		wake:    make(chan struct{}, 1),
		backoff: newBackoff(),
	}
}

// Wake signals one immediate claim iteration (WSSubscriber's
// daemon:task_available). Non-blocking and coalescing: a pending signal
// means an iteration is already scheduled.
func (l *ClaimLoop) Wake() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// Run preflights (C1/C17/C18/C16) with §4 backoff, then serves the claim
// loop until ctx ends. A 403 ends the loop with an error: the credential
// is misbound (§1.1 failure table) and retrying cannot fix it.
func (l *ClaimLoop) Run(ctx context.Context) error {
	for {
		err := l.client.Preflight(ctx)
		if err == nil {
			break
		}
		if isConfigError(err) {
			l.log.Error("preflight rejected: token does not match daemon_id/workspace_id (config error, not retrying)", "err", err)
			return err
		}
		wait := l.backoff.next()
		l.log.Error("preflight failed", "err", err, "retry_in", wait)
		if !sleepOrDone(ctx, wait) {
			return nil
		}
	}
	l.backoff.reset()
	for {
		if err := l.iterate(ctx); err != nil {
			if isConfigError(err) {
				l.log.Error("claim rejected: 403 config error, not retrying", "err", err)
				return err
			}
			wait := l.backoff.next()
			l.log.Error("claim failed", "err", err, "backoff", wait)
			if !sleepOrDone(ctx, wait) {
				return nil
			}
			continue
		}
		l.backoff.reset()
		select {
		case <-ctx.Done():
			return nil
		case <-l.wake:
		case <-time.After(l.interval()):
		}
	}
}

// iterate runs one claim round: budget → C3 → OnClaim per task.
func (l *ClaimLoop) iterate(ctx context.Context) error {
	n := l.sched.ClaimBudget()
	if n == 0 {
		return nil
	}
	tasks, err := l.client.ClaimTasks(ctx, n)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if err := l.sched.OnClaim(ctx, task); err != nil {
			l.log.Error("on-claim failed", "err", err)
		}
	}
	return nil
}

// interval picks the poll cadence: the idle interval when no claim slot is
// free, the active interval otherwise (§5.1).
func (l *ClaimLoop) interval() time.Duration {
	if l.sched.ClaimBudget() == 0 {
		return l.cfg.ClaimIntervalIdle
	}
	return l.cfg.ClaimInterval
}

// HeartbeatLoop keeps the real runtime alive (C2 every 15s) and renews the
// server token (C16 every 72h, non-fatal). Failures back off per §4; a 403
// stops the loop (config error). The loop never exits on its own otherwise.
type HeartbeatLoop struct {
	client  *Client
	log     *slog.Logger
	backoff *backoff
	now     func() time.Time

	// lastRenewAttempt clocks the C16 cadence by attempt, not success: a
	// failing renew must not retry every 15s tick. Seeded at construction —
	// the preflight has just renewed.
	lastRenewAttempt time.Time
}

// NewHeartbeatLoop builds the heartbeat loop.
func NewHeartbeatLoop(client *Client) *HeartbeatLoop {
	return &HeartbeatLoop{
		client:           client,
		log:              slog.Default().With("component", "heartbeat-loop"),
		backoff:          newBackoff(),
		now:              time.Now,
		lastRenewAttempt: time.Now(),
	}
}

// Run heartbeats until ctx ends or a 403 marks the credential misbound.
func (l *HeartbeatLoop) Run(ctx context.Context) error {
	for {
		if err := l.client.Heartbeat(ctx); err != nil {
			if isConfigError(err) {
				l.log.Error("heartbeat rejected: 403 config error, not retrying", "err", err)
				return err
			}
			wait := l.backoff.next()
			l.log.Error("heartbeat failed", "err", err, "retry_in", wait)
			if !sleepOrDone(ctx, wait) {
				return nil
			}
			continue
		}
		l.backoff.reset()
		if l.now().Sub(l.lastRenewAttempt) >= tokenRenewInterval {
			l.lastRenewAttempt = l.now()
			if err := l.client.RenewToken(ctx); err != nil {
				l.log.Warn("token renew failed (non-fatal)", "err", err)
			}
		}
		if !sleepOrDone(ctx, heartbeatInterval) {
			return nil
		}
	}
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
