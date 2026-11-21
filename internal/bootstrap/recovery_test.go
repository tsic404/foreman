package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scope"
)

// recoveryProber fails a source a programmed number of times before answering
// with its discovery, so the retry path is observable.
type recoveryProber struct {
	mu       sync.Mutex
	calls    map[string]int
	failures map[string]int
	found    map[string]map[string]registry.Discovery
}

func (p *recoveryProber) RunStartupProbeInSource(_ context.Context, sourceID string) (map[string]registry.Discovery, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[sourceID]++
	if p.calls[sourceID] <= p.failures[sourceID] {
		return nil, errors.New(sourceID + " is unreachable")
	}
	return p.found[sourceID], nil
}

func (p *recoveryProber) callCount(sourceID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[sourceID]
}

// TestFailedSourceIsReprobedUntilItRecovers pins R5: a source whose startup
// probe failed is re-probed with backoff and attached the moment it answers.
// The healthy source is never probed or attached again, and the attach carries
// only the recovered source's source/workspace key (V2-C06 按来源退避与对账).
func TestFailedSourceIsReprobedUntilItRecovers(t *testing.T) {
	clients := []*proxy.Client{
		proxy.NewClient(proxy.Config{SourceID: "src-a", WorkspaceID: "ws-shared", DaemonID: "foreman-a", Version: "0.1.0"}, "token-a"),
		proxy.NewClient(proxy.Config{SourceID: "src-b", WorkspaceID: "ws-shared", DaemonID: "foreman-b", Version: "0.1.0"}, "token-b"),
	}
	prober := &recoveryProber{
		calls:    map[string]int{},
		failures: map[string]int{"src-b": 1},
		found: map[string]map[string]registry.Discovery{
			"src-b": {"ws-shared": {CLIVersion: "probe-b"}},
		},
	}
	var attachedSources []string
	var attachedDiscoveries map[string]registry.Discovery
	RetryFailedSources(context.Background(), prober, clients, []string{"src-b"},
		nil,
		func(int) time.Duration { return time.Millisecond },
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		func(sourceID string, found map[string]registry.Discovery) bool {
			attachedSources = append(attachedSources, sourceID)
			attachedDiscoveries = found
			return true
		})

	if len(attachedSources) != 1 || attachedSources[0] != "src-b" {
		t.Fatalf("attached sources = %v, want exactly the recovered src-b", attachedSources)
	}
	if got := prober.callCount("src-b"); got != 2 {
		t.Fatalf("src-b probe attempts = %d, want 2 (one failure, one recovery)", got)
	}
	if got := prober.callCount("src-a"); got != 0 {
		t.Fatalf("src-a probe attempts = %d, want 0 (healthy sources are not re-probed)", got)
	}
	recovered, ok := attachedDiscoveries[scope.Key("src-b", "ws-shared")]
	if !ok || recovered.CLIVersion != "probe-b" {
		t.Fatalf("attached discoveries = %v, want src-b/ws-shared", attachedDiscoveries)
	}
}

// TestSourceRetryBackoffIsBounded pins the retry schedule: the first five
// attempts double from one second and every later attempt stays at the 30s
// cap, so a source that stays down cannot hammer its server.
func TestSourceRetryBackoffIsBounded(t *testing.T) {
	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 30 * time.Second, 30 * time.Second,
	}
	for i, expected := range want {
		if got := SourceRetryBackoff(i + 1); got != expected {
			t.Fatalf("SourceRetryBackoff(%d) = %v, want %v", i+1, got, expected)
		}
	}
}
