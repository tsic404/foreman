package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/tsic404/foreman/internal/observability"
)

// supervisor runs the process's long-lived loops. A loop that returns an error
// while the process is still meant to serve is a process-level failure, not a
// log line: the loops stop only on a credential/identity defect (§1.5), which
// no retry can fix, so the honest signal is a non-zero exit the Deployment
// restarts — a CrashLoopBackOff is the alarm. The supervisor also owns the
// liveness view: a component is registered while it runs, so /foreman/healthz
// stops answering ok the moment a critical loop is gone (§9.3).
type supervisor struct {
	log    *slog.Logger
	health *observability.Health
	stop   context.CancelFunc
	wg     sync.WaitGroup
	fatal  chan error
}

// newSupervisor builds the supervisor; stop cancels the process's root context
// and is called when a component fails.
func newSupervisor(log *slog.Logger, health *observability.Health, stop context.CancelFunc) *supervisor {
	return &supervisor{
		log:    log,
		health: health,
		stop:   stop,
		fatal:  make(chan error, 1),
	}
}

// run starts one component. A nil return is the ordinary shutdown path (the
// root context ended); an error with a live context cancels the root context
// and is reported by failure.
func (s *supervisor) run(ctx context.Context, component string, f func(context.Context) error) {
	stopped := s.health.Running(component)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer stopped()
		err := f(ctx)
		if err == nil || ctx.Err() != nil {
			return
		}
		s.log.Error("component.failed", "component", component, "err", err.Error())
		// Record before cancelling: the main goroutine reads failure only
		// after the root context is done, so the value is already there.
		select {
		case s.fatal <- fmt.Errorf("%s: %w", component, err):
		default:
		}
		s.stop()
	}()
}

// failure returns the component error that ended the process, if any. It is
// read after the root context is done.
func (s *supervisor) failure() error {
	select {
	case err := <-s.fatal:
		return err
	default:
		return nil
	}
}

// wait blocks until every component returned, or the shutdown budget (ctx)
// expired first; it reports which happened.
func (s *supervisor) wait(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
