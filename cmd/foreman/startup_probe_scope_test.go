package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scope"
)

type startupProbeStub struct {
	bySource map[string]map[string]registry.Discovery
	fail     map[string]error
}

func (s *startupProbeStub) RunStartupProbeInSource(_ context.Context, sourceID string) (map[string]registry.Discovery, error) {
	if err := s.fail[sourceID]; err != nil {
		return nil, err
	}
	return s.bySource[sourceID], nil
}

// TestRunStartupProbesKeepsTheHealthySource pins the no-coordination startup
// path: a source probe failure is logged and the other source still starts.
func TestRunStartupProbesKeepsTheHealthySource(t *testing.T) {
	clients := []*proxy.Client{
		proxy.NewClient(proxy.Config{SourceID: "src-b", WorkspaceID: "ws-shared", DaemonID: "foreman-b", Version: "0.1.0"}, "token"),
		proxy.NewClient(proxy.Config{SourceID: "src-a", WorkspaceID: "ws-shared", DaemonID: "foreman-a", Version: "0.1.0"}, "token"),
	}
	prober := &startupProbeStub{
		bySource: map[string]map[string]registry.Discovery{
			"src-a": {"ws-shared": {CLIVersion: "probe-a"}},
		},
		fail: map[string]error{"src-b": errors.New("src-b is unreachable")},
	}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	out, err := runStartupProbes(context.Background(), prober, clients, log)
	if err != nil {
		t.Fatalf("one source's failure aborted startup: %v", err)
	}
	if _, ok := out[scope.Key("src-a", "ws-shared")]; !ok {
		t.Fatalf("discoveries = %v, want the healthy source", out)
	}
}

// TestRunStartupProbesFailsWhenNothingWasObserved keeps the fail-fast for the
// all-failed case.
func TestRunStartupProbesFailsWhenNothingWasObserved(t *testing.T) {
	clients := []*proxy.Client{
		proxy.NewClient(proxy.Config{SourceID: "src-a", WorkspaceID: "ws-shared", DaemonID: "foreman-a", Version: "0.1.0"}, "token"),
	}
	prober := &startupProbeStub{fail: map[string]error{"src-a": errors.New("down")}}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	if out, err := runStartupProbes(context.Background(), prober, clients, log); err == nil {
		t.Fatalf("every probe failed but startup returned %v", out)
	}
}
