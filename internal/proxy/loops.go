package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/scheduler"
)

// heartbeatInterval is the C2 cadence (§1.2: every 15s per real runtime).
const heartbeatInterval = 15 * time.Second

// tokenRenewInterval is the C16 cadence (§1.2: every 72h, non-fatal).
const tokenRenewInterval = 72 * time.Hour

// ClaimScheduler is the scheduler's claim surface.
type ClaimScheduler interface {
	// ClaimBudget is how many tasks may be claimed right now.
	ClaimBudget() int
	// ReserveSlots reserves up to want in-flight slots before the C3 call and
	// returns the reservation pairs actually taken (empty when the cap is
	// reached). A claim is only issued for a reserved pair (§10: claim 前必须
	// 持有预留), and each claimed task retires its own pair.
	ReserveSlots(ctx context.Context, want int) ([]scheduler.SlotReservation, error)
	// ReleaseSlots returns reservations C3 did not fill.
	ReleaseSlots(ctx context.Context, reserved []scheduler.SlotReservation)
	// OnClaimReserved registers a claimed task against an already-held
	// reservation; duplicate claims (server redispatch) are settled inside
	// (§4.3), so the loop hands every claimed task over.
	OnClaimReserved(ctx context.Context, task json.RawMessage, reserved scheduler.SlotReservation) error
}

// ClaimLoop is the polling claim loop (S-6 + §10): it claims up
// to the scheduler's budget every FOREMAN_CLAIM_INTERVAL, idles at
// FOREMAN_CLAIM_INTERVAL_IDLE when no slot is free, and runs one immediate
// iteration on a WS wakeup (ADR-010). Every error backs off per §1.5
// (1s→30s) and the loop never exits: at runtime a 401/403/404 is the §1.4
// credential failure face, not a configuration verdict (AC-86).
type ClaimLoop struct {
	cfg     Config
	client  *Client
	sched   ClaimScheduler
	log     *slog.Logger
	wake    chan struct{} // cap 1: the single consumer is this loop
	backoff *backoff
	// leader gates the control plane: only the lease holder claims
	// (02-contracts.md §1.1 发出者口径). Nil keeps the gate open (stage one).
	leader func() bool
	// onRegistered runs once the preflight's C1 succeeded: the leader stores
	// the registration identity for followers/takeovers (ADR-046 决策 2).
	onRegistered func(context.Context, *Client) error
}

// SetRegisteredHook wires the post-C1 declaration writer.
func (l *ClaimLoop) SetRegisteredHook(fn func(context.Context, *Client) error) { l.onRegistered = fn }

// leaderPollInterval is how often a parked control loop re-checks ownership.
const leaderPollInterval = 500 * time.Millisecond

// SetLeaderGate wires the control-plane ownership check.
func (l *ClaimLoop) SetLeaderGate(g func() bool) { l.leader = g }

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

// Run preflights (C1/C16) with §1.5 backoff, then serves the claim loop until
// ctx ends. No response code stops the loop: the startup self-check owns the
// configuration verdict, and runtime failures back off and retry (§1.4,
// §1.5, AC-86).
func (l *ClaimLoop) Run(ctx context.Context) error {
	for {
		if l.leader != nil && !l.leader() {
			if !sleepOrDone(ctx, leaderPollInterval) {
				return nil
			}
			continue
		}
		err := l.client.Preflight(ctx)
		if err == nil {
			break
		}
		wait := l.backoff.next()
		l.log.Error("preflight failed", "err", err, "retry_in", wait)
		if !sleepOrDone(ctx, wait) {
			return nil
		}
	}
	l.backoff.reset()
	if l.onRegistered != nil {
		for {
			if err := l.onRegistered(ctx, l.client); err == nil {
				break
			} else {
				l.log.Error("registration declaration not stored", "err", err)
			}
			if !sleepOrDone(ctx, l.backoff.next()) {
				return nil
			}
		}
	}
	for {
		if l.leader != nil && !l.leader() {
			if !sleepOrDone(ctx, leaderPollInterval) {
				return nil
			}
			continue
		}
		if err := l.iterate(ctx); err != nil {
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

// iterate runs one claim round: budget → reserve → C3 → OnClaimReserved per
// task, returning the reservations C3 did not fill.
func (l *ClaimLoop) iterate(ctx context.Context) error {
	n := l.sched.ClaimBudget()
	if n == 0 {
		return nil
	}
	reserved, err := l.sched.ReserveSlots(ctx, n)
	if err != nil {
		return err
	}
	if len(reserved) == 0 {
		return nil
	}
	tasks, err := l.client.ClaimTasks(ctx, len(reserved))
	if err != nil {
		l.sched.ReleaseSlots(ctx, reserved)
		return err
	}
	// Each claimed task retires exactly the pair reserved for it. A response
	// larger than max_tasks carries no pair of its own: the server's own bound
	// makes that a contract violation, and the task is handed over unpaired.
	paired := min(len(tasks), len(reserved))
	for i, task := range tasks {
		var res scheduler.SlotReservation
		if i < paired {
			res = reserved[i]
		}
		if err := l.sched.OnClaimReserved(ctx, task, res); err != nil {
			if errors.Is(err, scheduler.ErrUnnameableClaim) {
				// The scheduler counted it and logged at a bounded rate; the
				// server's lease expiry is the convergence. A per-attempt line
				// here would defeat that bound at the redispatch cadence.
				continue
			}
			l.log.Error("on-claim failed", "err", err)
		}
	}
	if unused := reserved[paired:]; len(unused) > 0 {
		l.sched.ReleaseSlots(ctx, unused)
	}
	return nil
}

// interval picks the poll cadence: the idle interval when no claim slot is
// free, the active interval otherwise.
func (l *ClaimLoop) interval() time.Duration {
	if l.sched.ClaimBudget() == 0 {
		return l.cfg.ClaimIntervalIdle
	}
	return l.cfg.ClaimInterval
}

// HeartbeatLoop keeps every declared real runtime alive: one independent C2
// stream per runtime, each with its own backoff — a failing runtime never
// touches the others (§1.4 多 runtime 的失败面按 runtime 粒度). C16 renews the
// server token every 72h and is non-fatal. A 401/403/404 backs off and retries
// (§1.5, AC-86); no response code stops the loop. The heartbeat ack is
// consumed in the same round it arrives (§1.3).
type HeartbeatLoop struct {
	client   *Client
	reporter *ModelCatalogReporter
	log      *slog.Logger
	now      func() time.Time

	// lastRenewAttempt clocks the C16 cadence by attempt, not success: a
	// failing renew must not retry every tick. Seeded at construction — the
	// preflight has just renewed.
	lastRenewAttempt time.Time
	// renewMu guards lastRenewAttempt: the per-runtime streams share one
	// token-renew clock.
	renewMu sync.Mutex

	// newBackoff builds one stream's retry progression (§1.5). Tests replace
	// it with a near-zero progression so a retry-count assertion does not
	// spend the real 1s→30s waits.
	newBackoff func() *backoff
	// leader gates the control plane: a non-holder opens no heartbeat stream
	// (§1.1 发出者口径). Nil keeps the gate open (stage one).
	leader func() bool
}

// SetLeaderGate wires the control-plane ownership check.
func (l *HeartbeatLoop) SetLeaderGate(g func() bool) { l.leader = g }

// NewHeartbeatLoop builds the heartbeat loop.
func NewHeartbeatLoop(client *Client, reporter *ModelCatalogReporter) *HeartbeatLoop {
	return &HeartbeatLoop{
		client:           client,
		reporter:         reporter,
		log:              slog.Default().With("component", "heartbeat-loop"),
		now:              time.Now,
		lastRenewAttempt: time.Now(),
		newBackoff:       newBackoff,
	}
}

// Run supervises one heartbeat goroutine per declared real runtime until ctx
// ends. A runtime whose id moved is restarted on its own; the other streams
// keep running (§1.4), and every failure inside a stream backs off and retries
// (§1.5) — no response code stops the loop.
func (l *HeartbeatLoop) Run(ctx context.Context) error {
	changes, unsubscribe := l.client.SubscribeChanges()
	defer unsubscribe()
	streams := map[string]heartbeatStream{}
	stopAll := func() {
		for provider, stream := range streams {
			stream.cancel()
			delete(streams, provider)
		}
	}
	defer stopAll()
	// One slot per stream would be ideal, but reconcile may add streams later;
	// the send below is ctx-guarded instead, so a stream that exits after Run
	// returned can never block on a full channel.
	started := make(chan error, 8)
	reconcile := func() {
		declared := map[string]RuntimeRef{}
		for _, ref := range l.client.RuntimeRefs() {
			if ref.ID != "" {
				declared[ref.Key()] = ref
			}
		}
		for key, stream := range streams {
			ref, ok := declared[key]
			if !ok || ref.ID != stream.runtimeID {
				stream.cancel()
				delete(streams, key)
			}
		}
		for key, ref := range declared {
			if _, ok := streams[key]; ok {
				continue
			}
			streamCtx, cancel := context.WithCancel(ctx)
			streams[key] = heartbeatStream{runtimeID: ref.ID, cancel: cancel}
			go func(runtimeID string) {
				err := l.stream(streamCtx, runtimeID)
				select {
				case started <- err:
				case <-ctx.Done():
				}
			}(ref.ID)
		}
	}
	for {
		if l.leader != nil && !l.leader() {
			stopAll()
			if !sleepOrDone(ctx, leaderPollInterval) {
				return nil
			}
			continue
		}
		reconcile()
		select {
		case <-ctx.Done():
			return nil
		case err := <-started:
			if err != nil {
				return err
			}
		case <-changes:
		case <-time.After(heartbeatSupervisorInterval):
		}
	}
}

// heartbeatStream is one supervised per-runtime heartbeat goroutine.
type heartbeatStream struct {
	runtimeID string
	cancel    context.CancelFunc
}

// heartbeatSupervisorInterval re-reads the runtime set even without a change
// notification (a dropped notification only delays the resync).
const heartbeatSupervisorInterval = 5 * time.Second

// stream heartbeats one runtime until ctx ends. A nil return means the stream
// was retired (runtime replaced) or the context ended; every error backs off
// and retries (§1.5).
func (l *HeartbeatLoop) stream(ctx context.Context, runtimeID string) error {
	backoff := l.newBackoff()
	// The ack body's runtime_id is advisory: the real server's ack does not
	// reliably echo it, so a mismatch is worth one event per stream — never a
	// re-key (§1.3).
	ackMismatchWarned := false
	for {
		ack, err := l.client.Heartbeat(ctx, runtimeID)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, errAckUnparsable) {
				// The pending request is unrecoverable (delivered once); the
				// server converges it by timeout. Keep the cadence.
				l.log.Error("heartbeat ack unreadable; continuing without pending work", "runtime_id", runtimeID, "err", err)
			} else {
				wait := backoff.next()
				l.log.Error("heartbeat failed", "runtime_id", runtimeID, "err", err, "retry_in", wait)
				if !sleepOrDone(ctx, wait) {
					return nil
				}
				continue
			}
		} else {
			backoff.reset()
			// §1.3 回显不取数: the ack's runtime identity is advisory. Missing,
			// empty and pointing at another runtime are the same defect — one
			// warning per heartbeat stream, and the stream's own runtime keeps
			// the slot and the C21 path.
			if ack.RuntimeID != runtimeID && !ackMismatchWarned {
				ackMismatchWarned = true
				requestID := ""
				if ack.PendingModelList != nil {
					requestID = ack.PendingModelList.ID
				}
				l.log.Warn("model.catalog_ack_mismatch", "request_id", requestID,
					"runtime_id", runtimeID, "ack_runtime_id", ack.RuntimeID)
			}
			if ack.PendingModelList != nil && l.reporter != nil {
				// The command belongs to the runtime this stream heartbeats —
				// the slot key and the C21 {rid} both come from here. Keying by
				// the ack body would send the command to another runtime's slot
				// (or, when the body carries nothing, to none at all).
				l.reporter.OnAck(ctx, runtimeID, ack.PendingModelList.ID)
			}
		}
		l.maybeRenewToken(ctx)
		if !sleepOrDone(ctx, heartbeatInterval) {
			return nil
		}
	}
}

func (l *HeartbeatLoop) maybeRenewToken(ctx context.Context) {
	l.renewMu.Lock()
	due := l.now().Sub(l.lastRenewAttempt) >= tokenRenewInterval
	if due {
		l.lastRenewAttempt = l.now()
	}
	l.renewMu.Unlock()
	if !due {
		return
	}
	if err := l.client.RenewToken(ctx); err != nil {
		l.log.Warn("token renew failed (non-fatal)", "err", err)
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
