package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/bootstrap"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scope"
)

// recoveringProbe fails each source a programmed number of times before
// answering with its discovery, so the startup failure and the recovery
// retry are separate observable attempts.
type recoveringProbe struct {
	mu       sync.Mutex
	calls    map[string]int
	failures map[string]int
	bySource map[string]map[string]registry.Discovery
}

func (p *recoveringProbe) RunStartupProbeInSource(_ context.Context, sourceID string) (map[string]registry.Discovery, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[sourceID]++
	if p.calls[sourceID] <= p.failures[sourceID] {
		return nil, errors.New(sourceID + " is unreachable")
	}
	return p.bySource[sourceID], nil
}

// TestUnregisteredWorkspaceIsNotARetryTarget pins the failure/declaration
// split the recovery loop depends on: a source whose probe answered but whose
// workspace was not registered is not a failed source. Retrying it would never
// change the answer (that split is a declaration fact, 02-contracts §10).
func TestUnregisteredWorkspaceIsNotARetryTarget(t *testing.T) {
	clients := []*proxy.Client{
		proxy.NewClient(proxy.Config{SourceID: "src-a", WorkspaceID: "ws-shared", DaemonID: "foreman-a", Version: "0.1.0"}, "token-a"),
		proxy.NewClient(proxy.Config{SourceID: "src-c", WorkspaceID: "ws-other", DaemonID: "foreman-c", Version: "0.1.0"}, "token-c"),
	}
	prober := &startupProbeStub{
		bySource: map[string]map[string]registry.Discovery{
			"src-a": {"ws-shared": {CLIVersion: "probe-a"}},
			"src-c": {},
		},
	}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	discoveries, failed, err := runStartupProbesDetailed(context.Background(), prober, clients, log)
	if err != nil {
		t.Fatalf("runStartupProbesDetailed: %v", err)
	}
	if _, ok := discoveries[scope.Key("src-a", "ws-shared")]; !ok {
		t.Fatalf("discoveries = %v, want the registered src-a workspace", discoveries)
	}
	if len(failed) != 0 {
		t.Fatalf("failed sources = %v, want none: src-c answered without a registration", failed)
	}
}

// TestRecoveredSourceRegistersAndClaims pins R5 end to end at the assembly
// seam: B fails its startup probe, A serves, B recovers without restarting A,
// and the attach path declares only B, starts only B's planes, and B then
// registers (C1) and picks up a task (C3) against its own server.
func TestRecoveredSourceRegistersAndClaims(t *testing.T) {
	srv, upstream := newScopedRuntimeUpstream(t)
	registerTwoRuntimes(upstream, [2]string{"rid-b1", "rid-b2"})
	upstream.on("POST", "/api/daemon/tasks/claim", func(w http.ResponseWriter, _ []byte) {
		writeScopedJSON(w, 200, map[string]any{
			"tasks": []json.RawMessage{json.RawMessage(`{"id":"task-b"}`)},
		})
	})

	clients := []*proxy.Client{
		proxy.NewClient(proxy.Config{SourceID: "src-a", WorkspaceID: "ws-shared", DaemonID: "foreman-a", Version: "0.1.0"}, "token-a"),
		proxy.NewClient(proxy.Config{SourceID: "src-b", WorkspaceID: "ws-shared", ServerURL: srv.URL, DaemonID: "foreman-b", Version: "0.1.0"}, "token-b"),
	}
	prober := &recoveringProbe{
		calls:    map[string]int{},
		failures: map[string]int{"src-b": 2},
		bySource: map[string]map[string]registry.Discovery{
			"src-a": {"ws-shared": {CLIVersion: "probe-a"}},
			"src-b": {"ws-shared": twoRuntimeDiscovery()},
		},
	}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	discoveries, failed, err := runStartupProbesDetailed(context.Background(), prober, clients, log)
	if err != nil {
		t.Fatalf("startup: %v", err)
	}
	if _, ok := discoveries[scope.Key("src-a", "ws-shared")]; !ok || len(discoveries) != 1 {
		t.Fatalf("startup discoveries = %v, want only the healthy src-a", discoveries)
	}
	if len(failed) != 1 || failed[0] != "src-b" {
		t.Fatalf("failed sources = %v, want [src-b]", failed)
	}

	// The server is still down for the recovery's first attempt; the same
	// process must back off and re-probe, then attach it.
	var started []*proxy.Client
	bootstrap.RetryFailedSources(context.Background(), prober, clients, failed,
		nil,
		func(int) time.Duration { return time.Millisecond }, log,
		func(sourceID string, found map[string]registry.Discovery) bool {
			return attachRecoveredSource(clients, sourceID, found, nil, func(targets []*proxy.Client) {
				started = append(started, targets...)
			})
		})

	if len(started) != 1 || started[0].SourceID() != "src-b" {
		var sources []string
		for _, c := range started {
			sources = append(sources, c.SourceID())
		}
		t.Fatalf("started planes for %v, want only the recovered src-b (the healthy src-a must not be re-declared)", sources)
	}
	if ids := clients[0].RuntimeIDs(); len(ids) != 0 {
		t.Fatalf("src-a runtime ids = %v; the healthy source was touched by the recovery", ids)
	}

	// The recovered client runs the production preflight (C1) and C3 claim.
	ctx := context.Background()
	if err := started[0].Preflight(ctx); err != nil {
		t.Fatalf("recovered source preflight: %v", err)
	}
	if ids := started[0].RuntimeIDs(); len(ids) != 2 {
		t.Fatalf("recovered source runtime ids = %v, want its two C1 answers", ids)
	}
	claim, err := started[0].ClaimTasks(ctx, 1)
	if err != nil {
		t.Fatalf("recovered source claim: %v", err)
	}
	if len(claim.Tasks) != 1 || string(claim.Tasks[0]) != `{"id":"task-b"}` {
		t.Fatalf("recovered source claimed %s, want the server's task", claim.Tasks)
	}
	if calls := upstream.calls("POST", "/api/daemon/register"); len(calls) != 1 {
		t.Fatalf("C1 calls = %d, want exactly one declaration from the recovered source", len(calls))
	}
	if calls := upstream.calls("POST", "/api/daemon/tasks/claim"); len(calls) != 1 {
		t.Fatalf("C3 calls = %d, want exactly one claim from the recovered source", len(calls))
	}
}

// TestAttachRecoveryRequiresAHeldLease pins the attach boundary: starting the
// control loops is a holder-only action, so a lease lost before the attach
// must leave the clients undeclared and the planes unstarted.
func TestAttachRecoveryRequiresAHeldLease(t *testing.T) {
	clients := []*proxy.Client{
		proxy.NewClient(proxy.Config{SourceID: "src-b", WorkspaceID: "ws-shared", DaemonID: "foreman-b", Version: "0.1.0"}, "token-b"),
	}
	found := map[string]registry.Discovery{
		scope.Key("src-b", "ws-shared"): twoRuntimeDiscovery(),
	}
	held := false
	var started []*proxy.Client
	if ok := attachRecoveredSource(clients, "src-b", found, func() bool { return held },
		func(targets []*proxy.Client) { started = append(started, targets...) }); ok {
		t.Fatal("attach proceeded while the lease was not held")
	}
	if len(started) != 0 {
		t.Fatalf("started planes = %d while the lease was not held, want none", len(started))
	}
	if got := clients[0].DeclarationRecord(); len(got.Runtimes) != 0 {
		t.Fatalf("the client was declared while the lease was not held: %+v", got)
	}

	held = true
	if ok := attachRecoveredSource(clients, "src-b", found, func() bool { return held },
		func(targets []*proxy.Client) { started = append(started, targets...) }); !ok {
		t.Fatal("attach was refused while the lease was held")
	}
	if len(started) != 1 || started[0].SourceID() != "src-b" {
		t.Fatalf("started planes = %v, want the recovered source", started)
	}
}
