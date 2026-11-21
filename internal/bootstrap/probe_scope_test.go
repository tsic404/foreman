package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/coordination"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scope"
)

type probeStub struct {
	mu       sync.Mutex
	bySource map[string]map[string]registry.Discovery
	fail     map[string]error
	calls    []string
}

func (p *probeStub) RunStartupProbeInSource(_ context.Context, sourceID string) (map[string]registry.Discovery, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, sourceID)
	if err := p.fail[sourceID]; err != nil {
		return nil, err
	}
	return p.bySource[sourceID], nil
}

func scopedProbeClient(sourceID, workspaceID string) *proxy.Client {
	return proxy.NewClient(proxy.Config{
		SourceID:    sourceID,
		WorkspaceID: workspaceID,
		DaemonID:    "foreman-" + sourceID,
		Version:     "0.1.0",
	}, "token")
}

func heldLeader(t *testing.T) *coordination.Leader {
	t.Helper()
	leader := coordination.New(heldLeaseStore{}, "holder", bootstrapTestLeaseConfig())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = leader.Run(ctx) }()
	if !leader.WaitHeld(ctx, 5*time.Second) {
		t.Fatal("the holder never acquired the lease")
	}
	return leader
}

// TestSourceProbeFailureDoesNotAbortTheOtherSource pins the failure domain:
// one registered server's probe failure is logged and skipped, and the
// healthy source's declaration still reaches startup. The previously failing
// source being first in the client list must not change the outcome.
func TestSourceProbeFailureDoesNotAbortTheOtherSource(t *testing.T) {
	for _, order := range [][]string{{"src-a", "src-b"}, {"src-b", "src-a"}} {
		t.Run(order[0]+"_first", func(t *testing.T) {
			clients := []*proxy.Client{
				scopedProbeClient(order[0], "ws-shared"),
				scopedProbeClient(order[1], "ws-shared"),
			}
			prober := &probeStub{
				bySource: map[string]map[string]registry.Discovery{
					"src-a": {"ws-shared": {CLIVersion: "probe-a"}},
				},
				fail: map[string]error{
					"src-b": errors.New("src-b is unreachable"),
				},
			}
			leader := heldLeader(t)
			store := &flakyDeclarationStore{rows: map[string][]byte{}}
			log := slog.New(slog.NewJSONHandler(io.Discard, nil))

			out, _, err := StartupDiscoveriesDetailed(context.Background(), leader, store, prober,
				clients, time.Second, log)
			if err != nil {
				t.Fatalf("one source's failure aborted startup: %v", err)
			}
			if _, ok := out[scope.Key("src-a", "ws-shared")]; !ok {
				t.Fatalf("discoveries = %v, want the healthy src-a declaration", out)
			}
			if _, ok := out[scope.Key("src-b", "ws-shared")]; ok {
				t.Fatalf("discoveries = %v, want no declaration for the failed source", out)
			}
			if len(prober.calls) != 2 {
				t.Fatalf("probe calls = %v, want the other source attempted after the failure", prober.calls)
			}
		})
	}
}

// TestEverySourceProbeFailureStillAborts pins the remaining fail-fast: when
// nothing was published and every probe failed, Foreman must not declare an
// empty runtime set.
func TestEverySourceProbeFailureStillAborts(t *testing.T) {
	prober := &probeStub{
		fail: map[string]error{
			"src-a": errors.New("src-a is unreachable"),
			"src-b": errors.New("src-b is unreachable"),
		},
	}
	leader := heldLeader(t)
	store := &flakyDeclarationStore{rows: map[string][]byte{}}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	out, _, err := StartupDiscoveriesDetailed(context.Background(), leader, store, prober,
		[]*proxy.Client{scopedProbeClient("src-a", "ws-shared"), scopedProbeClient("src-b", "ws-shared")},
		time.Second, log)
	if err == nil {
		t.Fatalf("all probes failed but startup returned %v", out)
	}
}

// TestAPublishedDeclarationSurvivesAnotherSourceProbeFailure pins the partial
// success case: an adopted declaration is a result, so a failing source must
// not turn the whole round into an abort.
func TestAPublishedDeclarationSurvivesAnotherSourceProbeFailure(t *testing.T) {
	prober := &probeStub{fail: map[string]error{"src-b": errors.New("src-b is unreachable")}}
	leader := heldLeader(t)
	store := &flakyDeclarationStore{rows: map[string][]byte{
		scope.Key("src-a", "ws-shared"): bootstrapTestRecord(t, "ws-shared", "rid-a"),
	}}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	out, _, err := StartupDiscoveriesDetailed(context.Background(), leader, store, prober,
		[]*proxy.Client{scopedProbeClient("src-a", "ws-shared"), scopedProbeClient("src-b", "ws-shared")},
		time.Second, log)
	if err != nil {
		t.Fatalf("the failing source aborted an adopted round: %v", err)
	}
	if _, ok := out[scope.Key("src-a", "ws-shared")]; !ok {
		t.Fatalf("discoveries = %v, want the adopted src-a declaration", out)
	}
}
