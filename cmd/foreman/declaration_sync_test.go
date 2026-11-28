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
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/bootstrap"
	"github.com/tsic404/foreman/internal/discovery"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/release"
)

// ompPayload is a probe payload that reports the given version and the model
// listing surface the adapter parser depends on.
func ompPayload(version string) []byte {
	return []byte("#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then echo 'omp/" + version + "'; exit 0; fi\n" +
		"if [ \"$1\" = \"models\" ] && [ \"$2\" = \"ls\" ] && [ \"$3\" = \"--help\" ]; then echo '--json --no-extensions'; exit 0; fi\n" +
		"exit 1\n")
}

// verifiedReleaseSetService publishes a definition with several already
// verified versions, so a test can move the pointer without a fetcher.
func verifiedReleaseSetService(t *testing.T, serverID, adapter, current string, versions map[string][]byte) *release.Service {
	t.Helper()
	cache := release.NewMemoryArtifactCache()
	store := release.NewMemoryStore()
	service, err := release.New(release.Config{
		Store: store, Verifier: release.Verifier{Fetcher: release.NewHTTPFetcher()},
		Artifacts: cache, TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("release.New: %v", err)
	}
	platform := release.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}
	definition := release.Definition{
		ID: "omp", Kind: release.KindAgent, Adapter: adapter, ServerID: serverID,
		RequiredPlatforms: []release.Platform{platform},
		Verified:          map[string]release.VerifiedVersion{},
	}
	for version, body := range versions {
		sum := sha256.Sum256(body)
		digest := hex.EncodeToString(sum[:])
		artifact := release.Artifact{
			Platform: platform, URL: "https://mirror.test/omp-" + version,
			Digest: digest, Size: int64(len(body)),
		}
		definition.Verified[version] = release.VerifiedVersion{
			Artifacts: []release.Artifact{artifact},
			Checks: []release.Verification{{
				Platform: platform, OK: true, Digest: digest, Size: int64(len(body)),
			}},
			At: time.Now(),
		}
		if err := cache.Put(context.Background(), serverID, digest, body); err != nil {
			t.Fatalf("cache.Put: %v", err)
		}
		if version == current {
			definition.Current = &release.Release{
				Version: version, Generation: 1, Artifacts: []release.Artifact{artifact},
			}
		}
	}
	if err := store.PutDefinition(context.Background(), definition); err != nil {
		t.Fatalf("PutDefinition: %v", err)
	}
	return service
}

// declarationHarness is the production assembly a publication or a recovery
// drives: the real control-plane discovery, the shared coordinator, and the
// plane claim the attach path uses.
type declarationHarness struct {
	client       *proxy.Client
	clientSet    *proxy.ClientSet
	release      *release.Service
	discovery    *discovery.Service
	discoverer   *controlPlaneDiscovery
	declarations *declarationSync
	scopes       *planeScopes
	started      []string
	startedMu    sync.Mutex
}

func newDeclarationHarness(t *testing.T, serverURL string, releases *release.Service, adapters ...discovery.AdapterRegistration) *declarationHarness {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := proxy.NewClient(proxy.Config{
		SourceID: "atlas", ServerURL: serverURL, WorkspaceID: "ws-a",
		DaemonID: "foreman-ws-a", Version: "test",
	}, "mdt_ws-a")
	clientSet, err := proxy.NewClientSet(client)
	if err != nil {
		t.Fatalf("NewClientSet: %v", err)
	}
	catalogService, err := startupTestCatalog(t, serverURL)
	if err != nil {
		t.Fatalf("catalog.New: %v", err)
	}
	configService, err := agentconfig.New(agentconfig.Config{Store: agentconfig.NewMemoryStore()})
	if err != nil {
		t.Fatalf("agentconfig.New: %v", err)
	}
	if _, err := configService.Write(context.Background(), agentconfig.Scope{
		ServerID: "atlas", WorkspaceID: "ws-a", Backend: "omp", Profile: "primary",
	}, agentconfig.WriteRequest{
		SecretRef: "omp-primary", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp"}`),
	}, "ops"); err != nil {
		t.Fatalf("config Write: %v", err)
	}
	if len(adapters) == 0 {
		adapters = []discovery.AdapterRegistration{{Adapter: discovery.NewOMPAdapter("")}}
	}
	discoveryService, err := startupTestDiscovery(t, adapters...)
	if err != nil {
		t.Fatalf("discovery.New: %v", err)
	}
	discoverer := newControlPlaneDiscovery(
		catalogService, releases, discoveryService, configService, clientSet, log, nil,
	)
	h := &declarationHarness{
		client: client, clientSet: clientSet, release: releases, discovery: discoveryService,
		discoverer: discoverer, scopes: newPlaneScopes(),
	}
	attach := func(sourceID string, found map[string]registry.Discovery) bool {
		return attachRecoveredSource(clientSet.All(), sourceID, found, nil, h.startPlanes)
	}
	h.declarations = newDeclarationSync(context.Background(), discoverer, h.scopes.Served, nil, attach, log,
		func(int) time.Duration { return time.Millisecond })
	releases.SetPublishHook(h.declarations.Kick)
	return h
}

func (h *declarationHarness) startPlanes(targets []*proxy.Client) {
	h.startedMu.Lock()
	defer h.startedMu.Unlock()
	for _, client := range targets {
		if h.scopes.Claim(client) {
			h.started = append(h.started, client.ScopeKey())
		}
	}
}

func (h *declarationHarness) startedScopes() []string {
	h.startedMu.Lock()
	defer h.startedMu.Unlock()
	return append([]string(nil), h.started...)
}

func (h *declarationHarness) publish(t *testing.T, version string) {
	t.Helper()
	view, _, err := h.release.Get(context.Background(), "omp")
	if err != nil {
		t.Fatalf("release.Get: %v", err)
	}
	if _, err := h.release.Publish(context.Background(), "omp", version,
		view.CheckGeneration, view.CurrentGeneration, "test"); err != nil {
		t.Fatalf("release.Publish %s: %v", version, err)
	}
}

func (h *declarationHarness) waitRegistered(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(h.client.RuntimeIDs()) > 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("runtime ids = %v, want a registration", h.client.RuntimeIDs())
}

// TestPublishedDeclarationRecoversAfterRegistrationFailure: a cold-start
// source has no declaration, the administrator publishes, and the first C1 is
// refused. The publication path must keep retrying until the upstream accepts
// it, and a repeat publication of the same target is the operator's retry
// signal, not an unrecoverable no-op.
func TestPublishedDeclarationRecoversAfterRegistrationFailure(t *testing.T) {
	var mu sync.Mutex
	attempts, accepted := 0, 0
	var acceptedVersions []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/daemon/register" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var body struct {
			Runtimes []struct {
				Version string `json:"version"`
			} `json:"runtimes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode register body: %v", err)
		}
		mu.Lock()
		attempts++
		fail := attempts <= 2
		mu.Unlock()
		if fail {
			http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
			return
		}
		version := ""
		if len(body.Runtimes) > 0 {
			version = body.Runtimes[0].Version
		}
		mu.Lock()
		accepted++
		acceptedVersions = append(acceptedVersions, version)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"runtimes": []map[string]any{{
				"id": "rid-omp", "name": "omp", "provider": "omp",
				"status": "online", "profile_id": "primary",
			}},
		})
	}))
	defer server.Close()

	releases := verifiedReleaseSetService(t, "atlas", "omp", "1.0.0",
		map[string][]byte{"1.0.0": ompPayload("1.0.0")})
	h := newDeclarationHarness(t, server.URL, releases)
	h.publish(t, "1.0.0")
	h.waitRegistered(t, 3*time.Second)

	if got := h.client.Declaration().Runtimes; len(got) != 1 || got[0].Version != "1.0.0" {
		t.Fatalf("declaration = %+v, want the published 1.0.0 runtime", got)
	}
	if scopes := h.startedScopes(); len(scopes) != 1 {
		t.Fatalf("started plane scopes = %v, want exactly one", scopes)
	}
	mu.Lock()
	gotAttempts, gotAccepted := attempts, accepted
	gotVersions := append([]string(nil), acceptedVersions...)
	mu.Unlock()
	if gotAttempts != 3 || gotAccepted != 1 {
		t.Fatalf("register attempts = %d, accepted = %d, want 3 and 1", gotAttempts, gotAccepted)
	}
	if len(gotVersions) != 1 || gotVersions[0] != "1.0.0" {
		t.Fatalf("accepted versions = %v, want [1.0.0]", gotVersions)
	}

	// Re-publishing the same target must not duplicate the C1 or the loops.
	h.publish(t, "1.0.0")
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	gotAttempts, gotAccepted = attempts, accepted
	mu.Unlock()
	if gotAttempts != 3 || gotAccepted != 1 {
		t.Fatalf("repeat publish re-registered: attempts = %d, accepted = %d", gotAttempts, gotAccepted)
	}
	if scopes := h.startedScopes(); len(scopes) != 1 {
		t.Fatalf("repeat publish started another plane set: %v", scopes)
	}
}

// gatedBoundAdapter delays its first bound probe, so a publication can land
// while a recovery attempt still holds the source's probe slot.
type gatedBoundAdapter struct {
	trustedAdapter
	first   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (a *gatedBoundAdapter) ProbeBound(ctx context.Context, binaryPath string) (string, error) {
	body, err := os.ReadFile(binaryPath)
	if err != nil {
		return "", err
	}
	if a.first.CompareAndSwap(true, false) {
		select {
		case a.entered <- struct{}{}:
		default:
		}
		select {
		case <-a.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	for _, line := range strings.Split(string(body), "\n") {
		if index := strings.Index(line, "omp/"); index >= 0 {
			version := strings.SplitN(line[index+len("omp/"):], "'", 2)[0]
			version = strings.TrimRight(strings.TrimSpace(version), " ;")
			return version, nil
		}
	}
	return "", errors.New("payload reports no version")
}

// TestRecoveryAndPublicationShareOneRegistrationSlot: a recovery attempt reads
// the old release and then blocks in its probe; a new publication lands and
// registers the new version first. The stale attempt must not overwrite it,
// and only one C1 and one plane set may exist afterwards.
func TestRecoveryAndPublicationShareOneRegistrationSlot(t *testing.T) {
	var mu sync.Mutex
	var accepted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/daemon/register" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var body struct {
			Runtimes []struct {
				Version string `json:"version"`
			} `json:"runtimes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode register body: %v", err)
		}
		version := ""
		if len(body.Runtimes) > 0 {
			version = body.Runtimes[0].Version
		}
		mu.Lock()
		accepted = append(accepted, version)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"runtimes": []map[string]any{{
				"id": "rid-omp", "name": "omp", "provider": "omp",
				"status": "online", "profile_id": "primary",
			}},
		})
	}))
	defer server.Close()

	releases := verifiedReleaseSetService(t, "atlas", "omp", "1.0.0", map[string][]byte{
		"1.0.0": ompPayload("1.0.0"),
		"2.0.0": ompPayload("2.0.0"),
	})
	adapter := &gatedBoundAdapter{
		trustedAdapter: trustedAdapter{name: "omp"},
		entered:        make(chan struct{}, 1),
		release:        make(chan struct{}),
	}
	adapter.first.Store(true)
	h := newDeclarationHarness(t, server.URL, releases,
		discovery.AdapterRegistration{Adapter: adapter})

	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		bootstrap.RetryFailedSources(context.Background(), h.declarations.Prober(h.discoverer),
			h.clientSet.All(), []string{"atlas"}, nil,
			func(int) time.Duration { return time.Millisecond },
			slog.New(slog.NewTextHandler(io.Discard, nil)),
			func(sourceID string, found map[string]registry.Discovery) bool {
				return attachRecoveredSource(h.clientSet.All(), sourceID, found, nil, h.startPlanes)
			})
	}()
	select {
	case <-adapter.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the recovery attempt never reached its bound probe")
	}

	// The publication lands while the recovery is blocked on the old release.
	h.publish(t, "2.0.0")
	close(adapter.release)
	select {
	case <-recoveryDone:
	case <-time.After(3 * time.Second):
		t.Fatal("the recovery loop did not finish")
	}
	h.waitRegistered(t, 3*time.Second)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	gotAccepted := append([]string(nil), accepted...)
	mu.Unlock()
	if len(gotAccepted) != 1 || gotAccepted[0] != "2.0.0" {
		t.Fatalf("accepted registrations = %v, want exactly one 2.0.0", gotAccepted)
	}
	if got := h.client.Declaration().Runtimes; len(got) != 1 || got[0].Version != "2.0.0" {
		t.Fatalf("declaration = %+v, want the newer 2.0.0 (no rollback)", got)
	}
	if scopes := h.startedScopes(); len(scopes) != 1 {
		t.Fatalf("started plane scopes = %v, want exactly one", scopes)
	}
}
