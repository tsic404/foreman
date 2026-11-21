package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/bootstrap"
	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/coordination"
	"github.com/tsic404/foreman/internal/discovery"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/release"
	"github.com/tsic404/foreman/internal/scope"
	"github.com/tsic404/foreman/internal/state"
	"github.com/tsic404/foreman/internal/tenure"
)

// mainTenureStore is the shared coordination store for the cold-start takeover
// assertions: a real lease, not a hand-flipped boolean.
func mainTenureStore(t *testing.T) (state.CoordStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	st, err := state.New(state.Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    time.Hour,
	})
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	return st.(state.CoordStore), mr
}

func waitForMainTenure(t *testing.T, leader *coordination.Leader, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for leader.Held() != want {
		if time.Now().After(deadline) {
			t.Fatalf("leader tenure never became %v", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type discoverySourceSpec struct {
	source    string
	url       string
	workspace string
	daemon    string
}

// newTenureDiscovery builds the production discovery seam over in-memory
// stores, one verified omp release and one configured profile per source.
func newTenureDiscovery(t *testing.T, gate tenure.Gate, specs ...discoverySourceSpec) (*controlPlaneDiscovery, *proxy.ClientSet) {
	t.Helper()
	ctx := context.Background()
	catStore := catalog.NewMemoryStore()
	catService, err := catalog.New(catalog.Config{Store: catStore, Upstream: catalogUpstreamStub{}})
	if err != nil {
		t.Fatalf("catalog.New: %v", err)
	}
	cache := release.NewMemoryArtifactCache()
	relStore := release.NewMemoryStore()
	relService, err := release.New(release.Config{
		Store: relStore, Verifier: release.Verifier{Fetcher: release.NewHTTPFetcher()},
		Artifacts: cache, TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("release.New: %v", err)
	}
	configService, err := agentconfig.New(agentconfig.Config{Store: agentconfig.NewMemoryStore()})
	if err != nil {
		t.Fatalf("agentconfig.New: %v", err)
	}
	discoveryService, err := startupTestDiscovery(t,
		discovery.AdapterRegistration{Adapter: &boundTrustedAdapter{trustedAdapter: trustedAdapter{name: "omp"}}},
	)
	if err != nil {
		t.Fatalf("discovery.New: %v", err)
	}
	if _, err := discoveryService.VerifyAdapter(ctx, "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	clients := make([]*proxy.Client, 0, len(specs))
	for _, spec := range specs {
		if err := catStore.PutSource(ctx, catalog.Source{ID: catalog.ServerID(spec.source), URL: spec.url}); err != nil {
			t.Fatalf("PutSource: %v", err)
		}
		putTenureRelease(t, relStore, cache, spec.source, "omp", []byte("verified-omp-binary"))
		if _, err := configService.Write(ctx, agentconfig.Scope{
			ServerID: spec.source, WorkspaceID: spec.workspace, Backend: "omp", Profile: "primary",
		}, agentconfig.WriteRequest{
			SecretRef: "omp-primary", FileName: "config.json",
			Config: json.RawMessage(`{"provider":"omp"}`),
		}, "ops"); err != nil {
			t.Fatalf("config Write: %v", err)
		}
		clients = append(clients, proxy.NewClient(proxy.Config{
			ServerURL:   spec.url,
			SourceID:    spec.source,
			WorkspaceID: spec.workspace,
			DaemonID:    spec.daemon,
			Version:     "test",
		}, "mdt_"+spec.workspace))
	}
	clientSet, err := proxy.NewClientSet(clients...)
	if err != nil {
		t.Fatalf("NewClientSet: %v", err)
	}
	return newControlPlaneDiscovery(
		catService, relService, discoveryService, configService, clientSet,
		slog.New(slog.NewTextHandler(io.Discard, nil)), gate,
	), clientSet
}

// putTenureRelease publishes one verified Agent release for a source. The
// definition id is per source so two sources keep distinct rows.
func putTenureRelease(t *testing.T, store *release.MemoryStore, cache *release.MemoryArtifactCache, serverID, adapter string, body []byte) {
	t.Helper()
	ctx := context.Background()
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	platform := release.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}
	artifact := release.Artifact{
		Platform: platform, URL: "https://mirror.test/omp",
		Digest: digest, Size: int64(len(body)),
	}
	if err := cache.Put(ctx, serverID, digest, body); err != nil {
		t.Fatalf("cache.Put: %v", err)
	}
	definition := release.Definition{
		ID: "omp-" + serverID, Kind: release.KindAgent, Adapter: adapter, ServerID: serverID,
		RequiredPlatforms: []release.Platform{platform},
		Current: &release.Release{
			Version: "1.0.0", Generation: 1, Artifacts: []release.Artifact{artifact},
		},
		Verified: map[string]release.VerifiedVersion{
			"1.0.0": {
				Artifacts: []release.Artifact{artifact},
				Checks: []release.Verification{{
					Platform: platform, OK: true, Digest: digest, Size: int64(len(body)),
				}},
				At: time.Now(),
			},
		},
	}
	if err := store.PutDefinition(ctx, definition); err != nil {
		t.Fatalf("PutDefinition: %v", err)
	}
}

// TestColdStartC1IsCancelledWhenTheTenureEnds pins R8: the cold-start
// registration is bound to the source's tenure. An old holder already inside
// a blocked C1 loses its lease, its request is cancelled, it returns no
// declaration and registers no runtime; only the new holder registers and
// publishes that source.
func TestColdStartC1IsCancelledWhenTheTenureEnds(t *testing.T) {
	store, mr := mainTenureStore(t)
	leaderCfg := coordination.Config{
		LeaseTTL:       300 * time.Millisecond,
		TakeoverWindow: 100 * time.Millisecond,
		InstanceTTL:    time.Second,
		TaskLockTTL:    time.Second,
	}
	oldLeader := coordination.New(store, "instance-old", leaderCfg)
	newLeader := coordination.New(store, "instance-new", leaderCfg)

	entered := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	release := make(chan struct{})
	var oldCancelled atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/daemon/register" {
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var body struct {
			DaemonID string `json:"daemon_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.DaemonID == "foreman-old" {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
				oldCancelled.Store(true)
				select {
				case cancelled <- struct{}{}:
				default:
				}
				return
			case <-release:
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"runtimes": []map[string]any{{
				"id": "rid-omp", "name": "omp", "provider": "omp",
				"status": "online", "profile_id": "primary",
			}},
		})
	}))
	defer server.Close()

	oldCtx, stopOld := context.WithCancel(context.Background())
	defer stopOld()
	oldRun := make(chan struct{})
	go func() { defer close(oldRun); _ = oldLeader.Run(oldCtx) }()
	waitForMainTenure(t, oldLeader, true)

	oldDiscovery, oldSet := newTenureDiscovery(t, oldLeader.Held, discoverySourceSpec{
		source: "atlas", url: server.URL, workspace: "ws-a", daemon: "foreman-old",
	})
	oldClient := oldSet.All()[0]

	type discoveryResult struct {
		out map[string]registry.Discovery
		err error
	}
	result := make(chan discoveryResult, 1)
	go func() {
		out, err := oldDiscovery.Discover(context.Background())
		result <- discoveryResult{out: out, err: err}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the old holder never entered the cold-start C1")
	}

	// The old holder is inside RegisterWithDiscovery now. End its tenure.
	mr.SetError("shared state outage")
	waitForMainTenure(t, oldLeader, false)
	stopOld()
	select {
	case <-oldRun:
	case <-time.After(5 * time.Second):
		t.Fatal("the old holder's lease loop did not stop")
	}

	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
	}
	// Release any request the missing fence would leave blocked, so a revert
	// is observable as a completed registration rather than a wedged test.
	close(release)

	res := <-result
	if !oldCancelled.Load() {
		t.Fatal("losing the tenure did not cancel the old holder's cold-start C1")
	}
	if !errors.Is(res.err, tenure.ErrLost) {
		t.Fatalf("old Discover error = %v, want tenure.ErrLost", res.err)
	}
	if len(res.out) != 0 {
		t.Fatalf("old Discover returned %+v after losing the tenure, want no declaration", res.out)
	}
	if ids := oldClient.RuntimeIDs(); len(ids) != 0 {
		t.Fatalf("old holder completed registration with runtime ids %v, want none", ids)
	}

	// The new holder takes the lease row the outage removed.
	mr.Del("foreman:leader")
	mr.SetError("")
	newCtx, stopNew := context.WithCancel(context.Background())
	defer stopNew()
	go func() { _ = newLeader.Run(newCtx) }()
	waitForMainTenure(t, newLeader, true)

	newDiscovery, newSet := newTenureDiscovery(t, newLeader.Held, discoverySourceSpec{
		source: "atlas", url: server.URL, workspace: "ws-a", daemon: "foreman-new",
	})
	out, err := newDiscovery.Discover(context.Background())
	if err != nil {
		t.Fatalf("new holder Discover: %v", err)
	}
	declaration, ok := out[scope.Key("atlas", "ws-a")]
	if !ok || len(declaration.Runtimes) != 1 || declaration.Runtimes[0].ProfileID != "primary" {
		t.Fatalf("new holder declarations = %+v, want the verified omp runtime", out)
	}
	newClient := newSet.All()[0]
	if ids := newClient.RuntimeIDs(); len(ids) != 1 || ids[0] != "rid-omp" {
		t.Fatalf("new holder runtime ids = %v, want [rid-omp]", ids)
	}
	// Production publishes the declaration right after C1; only the holder
	// that completed the registration may do it.
	if err := bootstrap.PublishDeclaration(context.Background(), store, newClient.ScopeKey(), newClient); err != nil {
		t.Fatalf("PublishDeclaration: %v", err)
	}
	raw, published, err := store.GetDeclaration(context.Background(), newClient.ScopeKey())
	if err != nil {
		t.Fatalf("GetDeclaration: %v", err)
	}
	if !published {
		t.Fatal("the new holder did not publish the recovered source")
	}
	var record proxy.DeclarationRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("decode declaration: %v", err)
	}
	if len(record.Runtimes) != 1 {
		t.Fatalf("published runtimes = %+v, want one", record.Runtimes)
	}
}

// TestColdStartStopsBeforeTheNextSourceWhenTheTenureEnds pins the per-source
// boundary: a lease lost while one source registers must not let the next
// source's C1 run, nor hand the first source's declaration back as this
// holder's result.
func TestColdStartStopsBeforeTheNextSourceWhenTheTenureEnds(t *testing.T) {
	var held atomic.Bool
	held.Store(true)
	var secondRegistrations atomic.Int32

	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/daemon/register" {
			t.Errorf("unexpected first-source path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// The tenure ends while the first source's C1 is in flight.
		held.Store(false)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"runtimes": []map[string]any{{
				"id": "rid-a", "name": "omp", "provider": "omp",
				"status": "online", "profile_id": "primary",
			}},
		})
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/daemon/register" {
			secondRegistrations.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"runtimes": []map[string]any{{
					"id": "rid-b", "name": "omp", "provider": "omp",
					"status": "online", "profile_id": "primary",
				}},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer second.Close()

	discoveryService, clientSet := newTenureDiscovery(t, held.Load,
		discoverySourceSpec{source: "atlas", url: first.URL, workspace: "ws-a", daemon: "foreman-a"},
		discoverySourceSpec{source: "borealis", url: second.URL, workspace: "ws-b", daemon: "foreman-b"},
	)
	out, err := discoveryService.Discover(context.Background())
	if !errors.Is(err, tenure.ErrLost) {
		t.Fatalf("Discover error = %v, want tenure.ErrLost", err)
	}
	if len(out) != 0 {
		t.Fatalf("Discover returned %+v after losing the tenure, want no declaration", out)
	}
	if got := secondRegistrations.Load(); got != 0 {
		t.Fatalf("the second source registered %d time(s) after the tenure ended, want 0", got)
	}
	for _, client := range clientSet.All() {
		if client.SourceID() == "borealis" {
			if ids := client.RuntimeIDs(); len(ids) != 0 {
				t.Fatalf("second source runtime ids = %v, want none", ids)
			}
		}
	}
}
