// Package tenure binds holder-only side effects to the control-plane lease.
// The contract allows only the current leader to create external state:
// discovery probes, C1 registration, declaration publication, attach and
// claim dispatch (02-contracts §1.1 发出者口径, V2-C06; 04-architecture
// §6/§8). Bound cancels an in-flight effect when the tenure ends, and Holds
// re-checks at the effect boundary so a lost tenure cannot hand a result to
// the next holder.
package tenure

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Gate reports whether this instance currently holds the control-plane lease.
// A nil gate is the single-instance form with no coordination face: it is
// always held.
type Gate func() bool

// PollInterval is how often a bound operation re-checks the tenure. The lease
// is lost on a renewal tick, so a shorter cadence adds no safety; it matches
// the control loops' leader poll.
const PollInterval = 500 * time.Millisecond

// ErrLost reports that the tenure ended before or during a holder-only
// effect. Callers park until the gate is held again instead of treating it as
// a transport failure.
var ErrLost = errors.New("control-plane lease lost")

// Holds reports whether g is held now. A nil gate is open.
func Holds(g Gate) bool { return g == nil || g() }

// Check returns ErrLost when the tenure is not held.
func Check(g Gate) error {
	if Holds(g) {
		return nil
	}
	return ErrLost
}

// Bound derives a context cancelled the moment the tenure ends and reports
// whether that happened. The returned stop releases the watcher and must be
// called after the effect returns. A nil gate returns the parent context and
// no-op functions.
func Bound(ctx context.Context, g Gate) (context.Context, func(), func() bool) {
	if g == nil {
		return ctx, func() {}, func() bool { return false }
	}
	bound, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	lost := false
	markLost := func() {
		mu.Lock()
		lost = true
		mu.Unlock()
		cancel()
	}
	if !g() {
		markLost()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(PollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-bound.Done():
				return
			case <-ticker.C:
				if !g() {
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
	tenureLost := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return lost
	}
	return bound, stop, tenureLost
}

// Await parks until the gate is held or ctx ends. It reports whether the
// caller may proceed.
func Await(ctx context.Context, g Gate) bool {
	if g == nil {
		return true
	}
	for !g() {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(PollInterval):
		}
	}
	return true
}

// Watch calls release the moment the tenure ends, for the long-lived resources
// a holder keeps open between effects (heartbeat streams, WS subscriptions).
// The returned stop ends the watcher; release is never called after stop
// returns. A nil gate watches nothing.
func Watch(ctx context.Context, g Gate, release func()) func() {
	if g == nil {
		return func() {}
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(PollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !g() {
					release()
					return
				}
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}
