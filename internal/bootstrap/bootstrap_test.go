package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/coordination"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/state"
)

// TestMergeDiscoveriesAdoptsPublishedWorkspacesPerWorkspace pins AC-77's
// per-workspace rule: a workspace whose identity is already published is
// adopted from its stored record (its server-side runtime ids are kept and the
// probe's discovery is ignored), while a workspace with no record takes the
// probe's discovery. That is what stops a partial publication from
// re-declaring and re-registering the already-published identities.
func TestMergeDiscoveriesAdoptsPublishedWorkspacesPerWorkspace(t *testing.T) {
	newClient := func(ws string) *proxy.Client {
		return proxy.NewClient(proxy.Config{WorkspaceID: ws, DaemonID: "foreman", Version: "0.1.0"}, "token")
	}
	clientA, clientB := newClient("ws-a"), newClient("ws-b")
	runtimeA := registry.DiscoveredRuntime{Name: "foreman-a", Type: "omp", Version: "0.6.1"}
	recordA := proxy.DeclarationRecord{
		WorkspaceID: "ws-a",
		CLIVersion:  "0.6.1",
		Runtimes:    []registry.DiscoveredRuntime{runtimeA},
		RuntimeIDs:  map[string]string{registry.RuntimeIdentityKey("omp", ""): "rid-a"},
	}
	probedB := registry.Discovery{
		CLIVersion: "0.6.2",
		Runtimes:   []registry.DiscoveredRuntime{{Name: "foreman-b", Type: "omp", Version: "0.6.2"}},
	}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	out := MergeDiscoveries([]*proxy.Client{clientA, clientB},
		map[string]proxy.DeclarationRecord{"ws-a": recordA},
		map[string]registry.Discovery{"ws-a": {CLIVersion: "probe-a", Runtimes: []registry.DiscoveredRuntime{runtimeA}}, "ws-b": probedB},
		log)

	idsA := clientA.RuntimeIDs()
	if len(idsA) != 1 || idsA[0] != "rid-a" {
		t.Fatalf("ws-a runtime ids = %v, want the published id rid-a (no re-declaration)", idsA)
	}
	if got := out["ws-a"].CLIVersion; got != "0.6.1" {
		t.Fatalf("ws-a discovery cli_version = %q, want the published 0.6.1, not the probe body", got)
	}
	if idsB := clientB.RuntimeIDs(); len(idsB) != 0 {
		t.Fatalf("ws-b runtime ids = %v, want none (the probe discovery is declared by main, not adopted)", idsB)
	}
	if got := out["ws-b"].CLIVersion; got != "0.6.2" {
		t.Fatalf("ws-b discovery cli_version = %q, want the probe's 0.6.2", got)
	}
}

// TestLoadDeclarationsListsOnlyMissingWorkspaces keeps the per-workspace
// distinction: a published workspace is a record, an absent or unusable one is
// missing, and a follower keeps waiting for the latter without touching the
// former.
func TestLoadDeclarationsListsOnlyMissingWorkspaces(t *testing.T) {
	clientA := proxy.NewClient(proxy.Config{WorkspaceID: "ws-a", DaemonID: "foreman", Version: "0.1.0"}, "token")
	clientB := proxy.NewClient(proxy.Config{WorkspaceID: "ws-b", DaemonID: "foreman", Version: "0.1.0"}, "token")
	raw, err := json.Marshal(proxy.DeclarationRecord{
		WorkspaceID: "ws-a",
		CLIVersion:  "0.6.1",
		Runtimes:    []registry.DiscoveredRuntime{{Name: "foreman-a", Type: "omp", Version: "0.6.1"}},
		RuntimeIDs:  map[string]string{registry.RuntimeIdentityKey("omp", ""): "rid-a"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	store := &fakeDeclarationStore{rows: map[string][]byte{"ws-a": raw}}
	records, missing, err := LoadDeclarations(context.Background(), store, []*proxy.Client{clientA, clientB})
	if err != nil {
		t.Fatalf("LoadDeclarations: %v", err)
	}
	if _, ok := records["ws-a"]; !ok {
		t.Fatal("ws-a was not read")
	}
	if len(missing) != 1 || missing[0] != "ws-b" {
		t.Fatalf("missing = %v, want [ws-b]", missing)
	}
}

// fakeDeclarationStore is the declaration face LoadDeclarations needs.
type fakeDeclarationStore struct {
	state.CoordStore
	rows map[string][]byte
}

func (f *fakeDeclarationStore) GetDeclaration(_ context.Context, ws string) ([]byte, bool, error) {
	raw, ok := f.rows[ws]
	return raw, ok, nil
}

// flakyDeclarationStore fails the first failuresLeft declaration reads.
type flakyDeclarationStore struct {
	state.CoordStore
	mu           sync.Mutex
	failuresLeft int
	rows         map[string][]byte
}

func (f *flakyDeclarationStore) GetDeclaration(_ context.Context, ws string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failuresLeft > 0 {
		f.failuresLeft--
		return nil, false, errors.New("store down")
	}
	raw, ok := f.rows[ws]
	return raw, ok, nil
}

// noLeaseStore never grants the lease (the follower's read face).
type noLeaseStore struct{ state.CoordStore }

func (noLeaseStore) AcquireLeadership(context.Context, string, time.Duration) (bool, error) {
	return false, nil
}

// heldLeaseStore grants and renews the lease (the holder's face).
type heldLeaseStore struct{ state.CoordStore }

func (heldLeaseStore) AcquireLeadership(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

func (heldLeaseStore) RenewLeadership(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

func (heldLeaseStore) ReleaseLeadership(context.Context, string) error { return nil }

func bootstrapTestClient(t *testing.T, ws string) *proxy.Client {
	t.Helper()
	return proxy.NewClient(proxy.Config{WorkspaceID: ws, DaemonID: "foreman", Version: "0.1.0"}, "token")
}

func bootstrapTestRecord(t *testing.T, ws, runtimeID string) []byte {
	t.Helper()
	raw, err := json.Marshal(proxy.DeclarationRecord{
		WorkspaceID: ws,
		CLIVersion:  "0.6.1",
		Runtimes:    []registry.DiscoveredRuntime{{Name: "foreman-" + ws, Type: "omp", Version: "0.6.1"}},
		RuntimeIDs:  map[string]string{registry.RuntimeIdentityKey("omp", ""): runtimeID},
	})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return raw
}

func bootstrapTestLeaseConfig() coordination.Config {
	return coordination.Config{
		LeaseTTL:       100 * time.Millisecond,
		TakeoverWindow: 0,
		InstanceTTL:    time.Second,
		TaskLockTTL:    time.Second,
	}
}

// TestStartupDiscoveriesWaitsThroughDeclarationReadErrors pins the startup
// error path: a failed declaration read is not an empty declaration set. A
// follower keeps waiting and adopts once the store answers, instead of
// returning an empty discovery that main would report as "probe declared
// nothing".
func TestStartupDiscoveriesWaitsThroughDeclarationReadErrors(t *testing.T) {
	client := bootstrapTestClient(t, "ws-a")
	store := &flakyDeclarationStore{
		failuresLeft: 1,
		rows:         map[string][]byte{"ws-a": bootstrapTestRecord(t, "ws-a", "rid-a")},
	}
	leader := coordination.New(noLeaseStore{}, "follower", bootstrapTestLeaseConfig())
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	out, err := StartupDiscoveries(context.Background(), leader, store, nil, []*proxy.Client{client}, time.Second, log)
	if err != nil {
		t.Fatalf("StartupDiscoveries: %v", err)
	}
	if _, ok := out["ws-a"]; !ok {
		t.Fatal("a declaration read error was turned into an empty declaration set")
	}
	if ids := client.RuntimeIDs(); len(ids) != 1 || ids[0] != "rid-a" {
		t.Fatalf("runtime ids = %v, want the adopted rid-a", ids)
	}
}

// TestStartupDiscoveriesDoesNotAdoptAnEmptySetOnPersistentErrors pins the
// negative face: with the store persistently down a follower keeps waiting
// (until the caller's context ends) rather than adopting nothing.
func TestStartupDiscoveriesDoesNotAdoptAnEmptySetOnPersistentErrors(t *testing.T) {
	client := bootstrapTestClient(t, "ws-a")
	store := &flakyDeclarationStore{failuresLeft: 1 << 30, rows: map[string][]byte{}}
	leader := coordination.New(noLeaseStore{}, "follower", bootstrapTestLeaseConfig())
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	out, err := StartupDiscoveries(ctx, leader, store, nil, []*proxy.Client{client}, time.Second, log)
	if err == nil {
		t.Fatalf("returned nil error with %d entries on a persistent store failure", len(out))
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the caller's deadline (the follower keeps waiting)", err)
	}
}

// TestStartupDiscoveriesHolderFailsFastOnDeclarationReadErrors pins the holder
// branch: the holder cannot reconcile against a store it cannot read, so it
// fails fast instead of starting a probe over a false empty set.
func TestStartupDiscoveriesHolderFailsFastOnDeclarationReadErrors(t *testing.T) {
	client := bootstrapTestClient(t, "ws-a")
	store := &flakyDeclarationStore{failuresLeft: 1 << 30, rows: map[string][]byte{}}
	leader := coordination.New(heldLeaseStore{}, "holder", bootstrapTestLeaseConfig())
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = leader.Run(ctx) }()
	if !leader.WaitHeld(ctx, 5*time.Second) {
		t.Fatal("the holder never acquired the lease")
	}

	if _, err := StartupDiscoveries(ctx, leader, store, nil, []*proxy.Client{client}, time.Second, log); err == nil {
		t.Fatal("the holder did not fail fast on a declaration read error")
	} else if !strings.Contains(err.Error(), "store down") {
		t.Fatalf("err = %v, want the store failure", err)
	}
}
