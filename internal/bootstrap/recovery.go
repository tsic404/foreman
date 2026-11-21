package bootstrap

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scope"
)

// SourceAttach installs one recovered source's discoveries: it declares them on
// that source's clients and starts its control loops. The healthy sources are
// already serving and are never re-attached. It reports whether the attach
// happened; false means the lease was lost first, so the caller retries.
type SourceAttach func(sourceID string, discoveries map[string]registry.Discovery) bool

// SourceLeaderGate reports whether this instance still holds the control-plane
// lease. A probe creates a registration Job and its daemon registers an
// identity, so probing and attaching are holder-only actions
// (02-contracts §1.1 发出者口径, V2-C06). A nil gate is the single-instance
// deployment with no coordination face.
type SourceLeaderGate func() bool

// sourceLeaderPoll is how often the recovery loop re-checks a parked tenure.
// It matches the control loops' leader poll: the lease is lost on a renewal
// tick, so a shorter cadence adds no safety.
const sourceLeaderPoll = 500 * time.Millisecond

// SourceRetryBackoff is the bounded per-source retry schedule: 1s, 2s, 4s, 8s,
// 16s, then 30s. A source that recovers must join service without a process
// restart, while an unreachable one must not retry at the probe cadence.
func SourceRetryBackoff(attempt int) time.Duration {
	const base, maximum = time.Second, 30 * time.Second
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 6 {
		return maximum
	}
	delay := base << (attempt - 1)
	if delay > maximum {
		return maximum
	}
	return delay
}

// RetryFailedSources re-probes every source whose startup discovery failed and
// attaches each source the moment its probe succeeds. A failure stays confined
// to its source: the healthy sources keep serving while the failed one backs
// off and recovers (02-contracts V2-C06 故障粒度、按来源对账). It returns when
// every source recovered or ctx ends; the caller keeps the component alive
// until shutdown so a recovered source does not end the loop's liveness row.
func RetryFailedSources(
	ctx context.Context,
	prober StartupProber,
	clients []*proxy.Client,
	failed []string,
	held SourceLeaderGate,
	backoff func(attempt int) time.Duration,
	log *slog.Logger,
	attach SourceAttach,
) {
	if len(failed) == 0 {
		return
	}
	if backoff == nil {
		backoff = SourceRetryBackoff
	}
	if log == nil {
		log = slog.Default()
	}
	var wg sync.WaitGroup
	for _, sourceID := range failed {
		sourceID := sourceID
		if len(clientsOfSource(clients, sourceID)) == 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			attempt := 0
			for {
				if !parkUntilLeader(ctx, held) {
					return
				}
				// The in-flight probe runs under the tenure: losing the lease
				// cancels it, so a follower stops creating registration Jobs
				// instead of finishing the probe for the new holder.
				probeCtx, stopWatch, leaseLost := watchLease(ctx, held)
				found, err := prober.RunStartupProbeInSource(probeCtx, sourceID)
				lost := leaseLost()
				stopWatch()
				if ctx.Err() != nil {
					return
				}
				if lost {
					log.Warn("runtime.source_recovery_deferred",
						"source_id", sourceID, "reason", "leader_lease_lost")
					continue
				}
				attempt++
				switch {
				case err == nil:
					discoveries := make(map[string]registry.Discovery, len(found))
					for workspaceID, discovery := range found {
						discoveries[scope.Key(sourceID, workspaceID)] = discovery
					}
					if len(discoveries) == 0 {
						// The server answers but observes no workspace. That
						// is the probe's declaration answer, not a transport
						// failure, so re-probing cannot change it; the
						// unregistered/discovered split is reported upstream.
						log.Warn("runtime.source_recovered_without_workspace",
							"source_id", sourceID, "attempt", attempt)
						return
					}
					log.Info("runtime.source_recovered",
						"source_id", sourceID, "workspaces", len(discoveries), "attempt", attempt)
					if held != nil && !held() {
						log.Warn("runtime.source_recovery_deferred",
							"source_id", sourceID, "reason", "leader_lease_lost_before_attach")
						continue
					}
					if attach(sourceID, discoveries) {
						return
					}
					log.Warn("runtime.source_recovery_deferred",
						"source_id", sourceID, "reason", "attach_deferred")
					continue
				case err != nil:
					log.Warn("runtime.source_recovery_pending",
						"source_id", sourceID, "attempt", attempt, "err", err.Error())
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff(attempt)):
				}
			}
		}()
	}
	wg.Wait()
}

// parkUntilLeader blocks while this instance is a follower. It returns false
// only when ctx ends: a lost tenure is temporary, and this process may hold
// the lease again later.
func parkUntilLeader(ctx context.Context, held SourceLeaderGate) bool {
	if held == nil {
		return true
	}
	for {
		if held() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(sourceLeaderPoll):
		}
	}
}

// watchLease derives the probe's context from the current tenure and reports
// whether the tenure ended. The watcher exits when the probe finishes
// (stopWatch) or the tenant loses the lease.
func watchLease(ctx context.Context, held SourceLeaderGate) (context.Context, func(), func() bool) {
	if held == nil {
		return ctx, func() {}, func() bool { return false }
	}
	probeCtx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	lost := false
	markLost := func() {
		mu.Lock()
		lost = true
		mu.Unlock()
		cancel()
	}
	if !held() {
		markLost()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(sourceLeaderPoll)
		defer ticker.Stop()
		for {
			select {
			case <-probeCtx.Done():
				return
			case <-ticker.C:
				if !held() {
					markLost()
					return
				}
			}
		}
	}()
	stop := func() {
		cancel()
		<-done
	}
	leaseLost := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return lost
	}
	return probeCtx, stop, leaseLost
}

// clientsOfSource lists the clients one source owns.
func clientsOfSource(clients []*proxy.Client, sourceID string) []*proxy.Client {
	out := make([]*proxy.Client, 0, len(clients))
	for _, client := range clients {
		if client.SourceID() == sourceID {
			out = append(out, client)
		}
	}
	return out
}
