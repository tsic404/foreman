package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/bootstrap"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scope"
	"github.com/tsic404/foreman/internal/tenure"
)

// sourceDiscovery is the per-source declaration probe both the publication
// hook and the failed-source recovery drive.
type sourceDiscovery interface {
	DiscoverSource(context.Context, string) (map[string]registry.Discovery, error)
}

// declarationSync is the single coordinator for one source's declaration. The
// publication hook and the recovery loop register the same upstream daemon
// identity, so they share one per-source attempt slot instead of racing. A
// publication attempt keeps retrying until it lands, and a repeat kick asks
// for one extra pass instead of starting a second loop.
type declarationSync struct {
	ctx      context.Context
	discover sourceDiscovery
	attach   func(string, map[string]registry.Discovery) bool
	served   func(string) bool
	held     tenure.Gate
	backoff  func(int) time.Duration
	log      *slog.Logger

	mu    sync.Mutex
	slots map[string]*declarationSlot
}

// declarationSlot owns one source's exclusive attempt. apply serializes the
// probe and its C1 commit; running/again coalesce publication kicks.
type declarationSlot struct {
	apply   sync.Mutex
	running bool
	again   bool
}

func newDeclarationSync(
	ctx context.Context,
	discover sourceDiscovery,
	served func(string) bool,
	held tenure.Gate,
	attach func(string, map[string]registry.Discovery) bool,
	log *slog.Logger,
	backoff func(int) time.Duration,
) *declarationSync {
	if log == nil {
		log = slog.Default()
	}
	if backoff == nil {
		backoff = bootstrap.SourceRetryBackoff
	}
	return &declarationSync{
		ctx: ctx, discover: discover, attach: attach, served: served,
		held: held, backoff: backoff, log: log,
		slots: map[string]*declarationSlot{},
	}
}

// Kick schedules convergence for a source whose published facts changed. It
// returns immediately: publication must not wait on a probe.
func (s *declarationSync) Kick(sourceID string) {
	slot := s.slot(sourceID)
	s.mu.Lock()
	if slot.running {
		slot.again = true
		s.mu.Unlock()
		return
	}
	slot.running = true
	s.mu.Unlock()
	go s.converge(sourceID, slot)
}

// Prober routes the startup-failure recovery through the same attempt slot, so
// a publication and a recovery never register one source at the same time.
func (s *declarationSync) Prober(inner bootstrap.StartupProber) bootstrap.StartupProber {
	return coordinatedProber{sync: s, inner: inner}
}

func (s *declarationSync) slot(sourceID string) *declarationSlot {
	s.mu.Lock()
	defer s.mu.Unlock()
	slot, ok := s.slots[sourceID]
	if !ok {
		slot = &declarationSlot{}
		s.slots[sourceID] = slot
	}
	return slot
}

func (s *declarationSync) converge(sourceID string, slot *declarationSlot) {
	defer func() {
		s.mu.Lock()
		slot.running = false
		again := slot.again
		slot.again = false
		s.mu.Unlock()
		if again {
			s.Kick(sourceID)
		}
	}()
	attempt, stale := 0, 0
	for {
		if s.ctx.Err() != nil {
			return
		}
		if s.served(sourceID) {
			s.log.Info("runtime.redeclare_skipped", "source_id", sourceID,
				"reason", "a served declaration is never re-registered; the next start resolves the new pointer")
			return
		}
		found, err := s.exclusive(s.ctx, sourceID, func(ctx context.Context) (map[string]registry.Discovery, error) {
			if s.served(sourceID) {
				return nil, nil
			}
			return s.discover.DiscoverSource(ctx, sourceID)
		})
		if s.ctx.Err() != nil {
			return
		}
		switch {
		case err == nil && len(found) == 0:
			s.log.Info("runtime.redeclare_empty", "source_id", sourceID,
				"detail", "the published facts still do not declare a runtime")
			return
		case err == nil:
			if s.attach(sourceID, rekeyDiscoveries(sourceID, found)) {
				s.log.Info("runtime.redeclared", "source_id", sourceID, "workspaces", len(found))
				return
			}
			s.log.Warn("runtime.redeclare_deferred", "source_id", sourceID,
				"reason", "leader_lease_lost_before_attach")
		case errors.Is(err, tenure.ErrLost):
			s.log.Warn("runtime.redeclare_deferred", "source_id", sourceID,
				"reason", "leader_lease_lost")
		case errors.Is(err, errDeclarationStale):
			stale++
			if stale <= 3 {
				continue
			}
			s.log.Warn("runtime.redeclare_stale", "source_id", sourceID, "attempt", stale)
		default:
			s.log.Warn("runtime.redeclare_pending", "source_id", sourceID,
				"attempt", attempt+1, "err", err.Error())
		}
		attempt++
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(s.backoff(attempt)):
		}
	}
}

// exclusive runs one probe and its registration commit while holding the
// source's slot. Callers never share a registration attempt.
func (s *declarationSync) exclusive(
	ctx context.Context,
	sourceID string,
	probe func(context.Context) (map[string]registry.Discovery, error),
) (map[string]registry.Discovery, error) {
	slot := s.slot(sourceID)
	slot.apply.Lock()
	defer slot.apply.Unlock()
	if !tenure.Await(ctx, s.held) {
		return nil, tenure.ErrLost
	}
	return probe(ctx)
}

type coordinatedProber struct {
	sync  *declarationSync
	inner bootstrap.StartupProber
}

func (p coordinatedProber) RunStartupProbeInSource(ctx context.Context, sourceID string) (map[string]registry.Discovery, error) {
	return p.sync.exclusive(ctx, sourceID, func(ctx context.Context) (map[string]registry.Discovery, error) {
		return p.inner.RunStartupProbeInSource(ctx, sourceID)
	})
}

// rekeyDiscoveries turns a source's workspace-keyed declaration into the
// scope key the attach path resolves.
func rekeyDiscoveries(sourceID string, found map[string]registry.Discovery) map[string]registry.Discovery {
	rekeyed := make(map[string]registry.Discovery, len(found))
	for workspaceID, declaration := range found {
		rekeyed[scope.Key(sourceID, workspaceID)] = declaration
	}
	return rekeyed
}

// planeScopes records which scopes already own a control-plane set. Starting a
// second set would double claims and heartbeats for one workspace.
type planeScopes struct {
	mu      sync.Mutex
	started map[string]bool
}

func newPlaneScopes() *planeScopes {
	return &planeScopes{started: map[string]bool{}}
}

// Claim reports whether this call owns the scope's one plane start.
func (p *planeScopes) Claim(client *proxy.Client) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := client.ScopeKey()
	if p.started[key] {
		return false
	}
	p.started[key] = true
	return true
}

// Served reports whether any scope of the source already owns a plane set.
func (p *planeScopes) Served(sourceID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key := range p.started {
		if owner, _ := scope.Split(key); owner == sourceID {
			return true
		}
	}
	return false
}
