package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	mathrand "math/rand/v2"
	"time"

	"github.com/tsic404/foreman/internal/dispatch"
)

// Claimer is the dispatcher's claim surface.
type Claimer interface {
	OnClaim(ctx context.Context, workspaceID string, task json.RawMessage) error
}

// ClaimLoop polls the multica server for tasks (C-3) and hands each to the
// dispatcher. v2 has no per-runtime slot reservations and no WS wakeup: a
// daemon-driven claim pulls as many tasks as it has free capacity, and the
// loop's cadence is the only pacing.
//
// The loop never exits on a response code: a 401/403/404 is retried with the
// §1.5 backoff, and only the process context ends it.
type ClaimLoop struct {
	cfg    Config
	client *Client
	claim  Claimer
	// Capacity reports how many tasks Foreman may currently claim. A zero
	// answer idles the loop.
	Capacity func() int
	log      *slog.Logger
	backoff  *backoff
	// lastRenew is when the token was last renewed. Renewing is not a
	// heartbeat: the claim preflight runs every few seconds (and once per idle
	// poll), so renewing every round means dozens of calls a minute for a
	// credential that only needs extending occasionally.
	lastRenew time.Time
	// lastIdleNote and idleRounds drive the low-rate idle heartbeat: with the
	// default INFO level, a quiet system is indistinguishable from a wedged
	// loop because every claim-round line is DEBUG. One INFO every ten minutes
	// gives an operator a "the loop is still turning" breadcrumb without
	// flooding the log.
	lastIdleNote time.Time
	idleRounds   int
}

// tokenRenewInterval bounds how often the claim preflight extends the server
// credential. Five minutes keeps the token comfortably ahead of expiry without
// turning the preflight into a load source.
const tokenRenewInterval = 5 * time.Minute

// idleHeartbeatInterval is how often an idle claim loop states, at INFO, that
// it is still polling.
const idleHeartbeatInterval = 10 * time.Minute

// NewClaimLoop builds the loop. capacity returns the free task capacity.
func NewClaimLoop(cfg Config, client *Client, claim Claimer, capacity func() int) *ClaimLoop {
	return &ClaimLoop{
		cfg:      cfg,
		client:   client,
		claim:    claim,
		Capacity: capacity,
		log:      slog.Default().With("component", "claim-loop"),
		backoff:  newBackoff(),
	}
}

// Run serves the claim loop until ctx ends. Before each round it makes sure
// Foreman's current daemon set is registered to the server (C1): a claimed
// task can only be delivered to a daemon whose runtime the server knows, and a
// daemon that registers after startup is declared on the very next round.
func (l *ClaimLoop) Run(ctx context.Context) error {
	for {
		if err := l.preflight(ctx); err != nil {
			wait := l.backoff.next()
			l.log.Error("preflight failed", "err", err, "retry_in", wait)
			if !sleepOrDone(ctx, wait) {
				return nil
			}
			continue
		}
		l.backoff.reset()
		if err := l.iterate(ctx); err != nil {
			wait := l.backoff.next()
			l.log.Error("claim failed", "err", err, "retry_in", wait)
			if !sleepOrDone(ctx, wait) {
				return nil
			}
			continue
		}
		l.noteIdleRound()
		if !sleepOrDone(ctx, l.interval()) {
			return nil
		}
	}
}

// noteIdleRound emits a low-rate INFO breadcrumb while the loop has nothing to
// do. Without it a healthy idle system and a wedged loop look identical at the
// default log level, since every per-round line is DEBUG.
func (l *ClaimLoop) noteIdleRound() {
	now := time.Now()
	l.idleRounds++
	if !l.lastIdleNote.IsZero() && now.Sub(l.lastIdleNote) < idleHeartbeatInterval {
		return
	}
	l.log.Info("claim.idle", "rounds", l.idleRounds, "capacity", l.Capacity(),
		"interval", l.interval().String())
	l.lastIdleNote = now
	l.idleRounds = 0
}

// preflight registers the daemon set to the server (C1) when it is dirty and
// renews the server token (C16). A clean declaration (no daemon changed since
// the last C1) skips the register call and only renews the token.
func (l *ClaimLoop) preflight(ctx context.Context) error {
	if l.client.NeedsRegistration() {
		if err := l.client.Register(ctx); err != nil {
			if errors.Is(err, errNoDeclaredRuntime) {
				// No daemon registered to Foreman yet: defer. A later daemon
				// registration marks the declaration dirty and the next round
				// declares it.
				return nil
			}
			return err
		}
	}
	if l.lastRenew.IsZero() || time.Since(l.lastRenew) >= tokenRenewInterval {
		if err := l.client.RenewToken(ctx); err != nil {
			return err
		}
		l.lastRenew = time.Now()
	}
	return nil
}

// iterate runs one claim round.
func (l *ClaimLoop) iterate(ctx context.Context) error {
	n := l.Capacity()
	if n <= 0 {
		return nil
	}
	tasks, err := l.client.ClaimTasks(ctx, n)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if err := l.claim.OnClaim(ctx, l.cfg.WorkspaceID, task); err != nil {
			if err == dispatch.ErrNoDaemon {
				// No eligible daemon this round: the server's lease expiry is
				// the convergence, so the line is debug, not an error per task.
				l.log.Debug("claim_deferred", "reason", "no_daemon")
				continue
			}
			l.log.Error("on-claim failed", "err", err)
		}
	}
	return nil
}

// interval picks the poll cadence: the idle interval when no capacity is free.
func (l *ClaimLoop) interval() time.Duration {
	if l.Capacity() <= 0 {
		return l.cfg.ClaimIntervalIdle
	}
	return l.cfg.ClaimInterval
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// backoff is the §1.5 exponential retry policy: 1s doubling to 30s.
//
// Each step carries jitter. A multi-workspace deployment runs one claim loop
// per workspace, all started together; without jitter a server outage makes
// them retry in lockstep, and the recovery is met by a synchronized burst
// rather than a spread of light probes. The jitter is additive and capped at a
// quarter of the step, so the policy's ceiling still holds (30s becomes at
// most 37.5s).
type backoff struct {
	min, max time.Duration
	cur      time.Duration
	// rand returns a value in [0,1); nil disables jitter (tests that need a
	// exact schedule set it to a constant).
	rand func() float64
}

func newBackoff() *backoff {
	return &backoff{min: time.Second, max: 30 * time.Second, rand: mathrand.Float64}
}

func (b *backoff) next() time.Duration {
	if b.cur < b.min {
		b.cur = b.min
	} else {
		b.cur *= 2
		if b.cur > b.max {
			b.cur = b.max
		}
	}
	if b.rand == nil {
		return b.cur
	}
	return b.cur + time.Duration(float64(b.cur)*0.25*b.rand())
}

func (b *backoff) reset() { b.cur = 0 }
