package proxy

import (
	"context"
	"errors"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// CatalogProbe is the control channel to the catalog-probe Jobs: build/wait/
// delete. The scheduler owns it (K8s objects); proxy only ever calls it and
// shares state with it through the probe session table.
type CatalogProbe interface {
	// StartProbe builds the probe Job and its credential Secret; mode fixes the
	// session's command face, and realRuntimeID names the runtime a
	// command-driven probe relays for (registry.ProbeMode).
	StartProbe(ctx context.Context, requestID, realRuntimeID, workspaceID string, mode registry.ProbeMode, budget time.Duration) (scheduler.ProbeHandle, error)
	// AwaitProbeRelayed waits until the probe's result has landed (startup
	// discovery) or been forwarded (command driven), or the deadline passes.
	AwaitProbeRelayed(ctx context.Context, h scheduler.ProbeHandle, deadline time.Time) (nodeName string, relayed bool, relayedCode int, err error)
	// DeleteProbe tears the probe down: session first, then Secret, then Job.
	DeleteProbe(ctx context.Context, h scheduler.ProbeHandle) error
}

// ProbeCatalogSource is the 1.x catalog source: it runs one command-driven
// catalog probe Job per request and lets that Job relay the result. It is
// retained for the pinned 1.x acceptance suite; the 2.0 production wiring uses
// the restricted discovery executor, which never creates a probe Job.
type ProbeCatalogSource struct {
	probes CatalogProbe
}

// NewProbeCatalogSource wraps a probe control channel as a catalog source.
func NewProbeCatalogSource(probes CatalogProbe) *ProbeCatalogSource {
	return &ProbeCatalogSource{probes: probes}
}

// MinimumBudget implements catalogBudgetSource: a probe Job needs build and
// command-delivery time before it can relay anything.
func (s *ProbeCatalogSource) MinimumBudget(timeout time.Duration) time.Duration {
	return probeMinBudget(timeout)
}

// Catalog implements CatalogSource.
func (s *ProbeCatalogSource) Catalog(ctx context.Context, request CatalogRequest) (CatalogAnswer, error) {
	if s.probes == nil {
		return CatalogAnswer{}, errors.New("catalog probe control channel is not wired")
	}
	budget := time.Until(request.Deadline)
	if budget <= 0 {
		return CatalogAnswer{}, errRelayExhausted
	}
	probe, err := s.probes.StartProbe(ctx, request.RequestID, request.RuntimeID,
		request.WorkspaceID, registry.ProbeCommandDriven, budget)
	if err != nil {
		return CatalogAnswer{}, err
	}
	defer s.probes.DeleteProbe(context.WithoutCancel(ctx), probe)
	node, relayed, code, err := s.probes.AwaitProbeRelayed(ctx, probe, request.Deadline)
	if err != nil {
		return CatalogAnswer{}, err
	}
	if !relayed {
		return CatalogAnswer{}, errors.New("catalog probe did not relay a result")
	}
	return CatalogAnswer{Relayed: true, Code: code, Node: node}, nil
}
