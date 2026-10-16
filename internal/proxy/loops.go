package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
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

// HeartbeatLoop keeps every declared real runtime alive: one independent C2
// stream per runtime, each with its own backoff — a failing runtime never
// touches the others (§7 多 runtime 的失败面按 runtime 粒度). C16 renews the
// server token every 72h and is non-fatal. A 403 stops the loop (config error).
// The heartbeat ack is consumed in the same round it arrives (§1.1「C2 ack
// 消费」第 3 条).
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
}

// NewHeartbeatLoop builds the heartbeat loop.
func NewHeartbeatLoop(client *Client, reporter *ModelCatalogReporter) *HeartbeatLoop {
	return &HeartbeatLoop{
		client:           client,
		reporter:         reporter,
		log:              slog.Default().With("component", "heartbeat-loop"),
		now:              time.Now,
		lastRenewAttempt: time.Now(),
	}
}

// Run supervises one heartbeat goroutine per declared real runtime until ctx
// ends or a 403 marks the credential misbound. A runtime whose id moved is
// restarted on its own; the other streams keep running (§7).
func (l *HeartbeatLoop) Run(ctx context.Context) error {
	changes := l.client.Changes()
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
				declared[ref.Provider] = ref
			}
		}
		for provider, stream := range streams {
			ref, ok := declared[provider]
			if !ok || ref.ID != stream.runtimeID {
				stream.cancel()
				delete(streams, provider)
			}
		}
		for provider, ref := range declared {
			if _, ok := streams[provider]; ok {
				continue
			}
			streamCtx, cancel := context.WithCancel(ctx)
			streams[provider] = heartbeatStream{runtimeID: ref.ID, cancel: cancel}
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
// was retired (runtime replaced); a non-nil one is a 403 config error and stops
// the loop.
func (l *HeartbeatLoop) stream(ctx context.Context, runtimeID string) error {
	backoff := newBackoff()
	// The ack body's runtime_id is advisory: the real server's ack does not
	// reliably echo it, so a mismatch is worth one event per stream — never a
	// re-key (§1.1「C2 ack 消费」).
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
			} else if isConfigError(err) {
				l.log.Error("heartbeat rejected: 403 config error, not retrying", "runtime_id", runtimeID, "err", err)
				return err
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
			if ack.PendingModelList != nil && l.reporter != nil {
				// The command belongs to the runtime this stream heartbeats —
				// the slot key and the C21 {rid} both come from here. Keying by
				// the ack body would send the command to another runtime's slot
				// (or, when the body carries nothing, to none at all).
				if ack.RuntimeID != "" && ack.RuntimeID != runtimeID && !ackMismatchWarned {
					ackMismatchWarned = true
					l.log.Warn("model.catalog_ack_mismatch", "request_id", ack.PendingModelList.ID,
						"runtime_id", runtimeID, "ack_runtime_id", ack.RuntimeID)
				}
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
