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
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/discovery"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/release"
)

type catalogUpstreamStub struct{}

func (catalogUpstreamStub) ListWorkspaces(context.Context, catalog.Source) ([]catalog.Workspace, error) {
	return nil, nil
}

func (catalogUpstreamStub) WorkspaceCatalog(context.Context, catalog.Source, string) (catalog.WorkspaceCatalog, error) {
	return catalog.WorkspaceCatalog{}, nil
}

func (catalogUpstreamStub) ReadAgent(context.Context, catalog.Source, string, string) (catalog.Agent, bool, error) {
	return catalog.Agent{}, false, nil
}

func (catalogUpstreamStub) SetAgentRuntime(context.Context, catalog.Source, string, string, string, string) error {
	return nil
}

func (catalogUpstreamStub) BindingWriteSupported(catalog.Source) (bool, string) {
	return false, "not supported"
}

type trustedAdapter struct {
	name string
}

func (a trustedAdapter) Name() string                { return a.name }
func (a trustedAdapter) Kinds() []discovery.Kind     { return []discovery.Kind{discovery.KindModels} }
func (a trustedAdapter) Probe(context.Context) error { return nil }
func (a trustedAdapter) Enumerate(context.Context, discovery.Call) (discovery.Outcome, error) {
	return discovery.Outcome{Models: []discovery.Model{{ID: "m1", Provider: a.name}}}, nil
}

type boundTrustedAdapter struct {
	trustedAdapter
	boundErr error
	version  string
	probed   []byte
}

func (a *boundTrustedAdapter) ProbeBound(_ context.Context, binaryPath string) (string, error) {
	body, err := os.ReadFile(binaryPath)
	if err != nil {
		return "", err
	}
	a.probed = body
	if a.boundErr != nil {
		return "", a.boundErr
	}
	if string(body) != "verified-omp-binary" {
		return "", errors.New("the bound probe did not receive the verified payload")
	}
	if a.version == "" {
		return "1.0.0", nil
	}
	return a.version, nil
}

func startupTestClient(t *testing.T, serverURL string) *proxy.Client {
	t.Helper()
	return proxy.NewClient(proxy.Config{
		ServerURL: serverURL, WorkspaceID: "ws-a",
		DaemonID: "foreman-ws-a", Version: "test",
	}, "mdt_ws-a")
}

func startupTestCatalog(t *testing.T, serverURL string) (*catalog.Service, error) {
	t.Helper()
	store := catalog.NewMemoryStore()
	service, err := catalog.New(catalog.Config{Store: store, Upstream: catalogUpstreamStub{}})
	if err != nil {
		return nil, err
	}
	if err := store.PutSource(context.Background(), catalog.Source{
		ID: "atlas", URL: serverURL,
	}); err != nil {
		return nil, err
	}
	return service, nil
}

func startupTestDiscovery(t *testing.T, adapters ...discovery.AdapterRegistration) (*discovery.Service, error) {
	t.Helper()
	return discovery.New(discovery.Config{
		Store:    discovery.NewMemoryStore(),
		Adapters: adapters,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

// verifiedReleaseService builds an Agent definition whose current release is
// backed by a matching verification record and cached bytes.
func verifiedReleaseService(t *testing.T, serverID, adapter string, body []byte, verified bool) *release.Service {
	t.Helper()
	return verifiedReleaseServiceVersion(t, serverID, adapter, "1.0.0", body, verified)
}

// verifiedReleaseServiceVersion is verifiedReleaseService with an explicit
// candidate version.
func verifiedReleaseServiceVersion(t *testing.T, serverID, adapter, version string, body []byte, verified bool) *release.Service {
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
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	platform := release.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}
	artifact := release.Artifact{
		Platform: platform, URL: "https://mirror.test/omp",
		Digest: digest, Size: int64(len(body)),
	}
	definition := release.Definition{
		ID: "omp", Kind: release.KindAgent, Adapter: adapter, ServerID: serverID,
		RequiredPlatforms: []release.Platform{platform},
		Current: &release.Release{
			Version: version, Generation: 1, Artifacts: []release.Artifact{artifact},
		},
	}
	if verified {
		definition.Verified = map[string]release.VerifiedVersion{
			version: {
				Artifacts: []release.Artifact{artifact},
				Checks: []release.Verification{{
					Platform: platform, OK: true, Digest: digest, Size: int64(len(body)),
				}},
				At: time.Now(),
			},
		}
		if err := cache.Put(context.Background(), serverID, digest, body); err != nil {
			t.Fatalf("cache.Put: %v", err)
		}
	}
	if err := store.PutDefinition(context.Background(), definition); err != nil {
		t.Fatalf("PutDefinition: %v", err)
	}
	return service
}

// TestControlPlaneDiscoveryWithoutVerifiedDeclarations: a cold start with no
// published CLI leaves every workspace unserved, returns no error, and never
// enters a probe-Job path.
func TestControlPlaneDiscoveryWithoutVerifiedDeclarations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("a zero-declaration startup must not register a runtime")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client := startupTestClient(t, server.URL)
	clientSet, err := proxy.NewClientSet(client)
	if err != nil {
		t.Fatalf("NewClientSet: %v", err)
	}
	catalogService, err := startupTestCatalog(t, server.URL)
	if err != nil {
		t.Fatalf("catalog.New: %v", err)
	}
	releaseService, err := release.New(release.Config{
		Store: release.NewMemoryStore(), Verifier: release.Verifier{Fetcher: release.NewHTTPFetcher()},
		TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("release.New: %v", err)
	}
	configService, err := agentconfig.New(agentconfig.Config{Store: agentconfig.NewMemoryStore()})
	if err != nil {
		t.Fatalf("agentconfig.New: %v", err)
	}
	discoveryService, err := startupTestDiscovery(t)
	if err != nil {
		t.Fatalf("discovery.New: %v", err)
	}
	discoverer := newControlPlaneDiscovery(
		catalogService, releaseService, discoveryService, configService, clientSet,
		slog.New(slog.NewTextHandler(io.Discard, nil)), nil,
	)
	out, err := discoverer.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("discoveries = %+v, want none", out)
	}
	if ids := client.RuntimeIDs(); len(ids) != 0 {
		t.Fatalf("runtime ids = %v, want none", ids)
	}
}

type countingDiscoverer struct {
	calls int
	out   map[string]registry.Discovery
}

func (c *countingDiscoverer) Discover(context.Context) (map[string]registry.Discovery, error) {
	c.calls++
	return c.out, nil
}

// TestProductionStartupResolvesThroughTheControlPlaneSeam: the production
// assembly takes a discoverer, not a scheduler or Job client. A cold start
// with zero declarations therefore returns an empty set without creating a
// probe Job.
func TestProductionStartupResolvesThroughTheControlPlaneSeam(t *testing.T) {
	discoverer := &countingDiscoverer{out: map[string]registry.Discovery{}}
	out, err := resolveStartupDiscoveries(
		context.Background(), nil, nil, discoverer, nil, time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("resolveStartupDiscoveries: %v", err)
	}
	if discoverer.calls != 1 {
		t.Fatalf("discover calls = %d, want 1", discoverer.calls)
	}
	if len(out) != 0 {
		t.Fatalf("discoveries = %+v, want none", out)
	}
}

// TestControlPlaneDiscoveryRegistersVerifiedRuntime: a published release with
// a matching verification record, a successful bound probe of its exact
// digest, and a registered configuration scope is declared through the
// in-process registration path.
func TestControlPlaneDiscoveryRegistersVerifiedRuntime(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/daemon/register" {
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode register body: %v", err)
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
	client := startupTestClient(t, server.URL)
	clientSet, err := proxy.NewClientSet(client)
	if err != nil {
		t.Fatalf("NewClientSet: %v", err)
	}
	catalogService, err := startupTestCatalog(t, server.URL)
	if err != nil {
		t.Fatalf("catalog.New: %v", err)
	}
	releaseService := verifiedReleaseService(t, "atlas", "omp", []byte("verified-omp-binary"), true)
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
	adapter := &boundTrustedAdapter{trustedAdapter: trustedAdapter{name: "omp"}}
	discoveryService, err := startupTestDiscovery(t,
		discovery.AdapterRegistration{Adapter: adapter},
	)
	if err != nil {
		t.Fatalf("discovery.New: %v", err)
	}
	if _, err := discoveryService.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	discoverer := newControlPlaneDiscovery(
		catalogService, releaseService, discoveryService, configService, clientSet,
		slog.New(slog.NewTextHandler(io.Discard, nil)), nil,
	)
	out, err := discoverer.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	declaration, ok := out["ws-a"]
	if !ok || len(declaration.Runtimes) != 1 || declaration.Runtimes[0].Name != "omp" ||
		declaration.Runtimes[0].Version != "1.0.0" ||
		declaration.Runtimes[0].ProfileID != "primary" {
		t.Fatalf("discoveries = %+v, want the verified omp runtime", out)
	}
	if string(adapter.probed) != "verified-omp-binary" {
		t.Fatalf("bound probe bytes = %q, want the verified payload", adapter.probed)
	}
	if ids := client.RuntimeIDs(); len(ids) != 1 || ids[0] != "rid-omp" {
		t.Fatalf("runtime ids = %v, want the registered rid-omp", ids)
	}
}

// TestControlPlaneDiscoveryRequiresTheExactBoundVersion: the payload's own
// --version must equal the candidate version. A correct digest and platform
// do not authorize a different or unparseable version upstream.
func TestControlPlaneDiscoveryRequiresTheExactBoundVersion(t *testing.T) {
	cases := []struct {
		name     string
		declared string
		reported string
		want     bool
	}{
		{name: "matching", declared: "2.0.0", reported: "omp/2.0.0", want: true},
		{name: "mismatch", declared: "2.0.0", reported: "omp/1.0.0"},
		{name: "unparseable", declared: "2.0.0", reported: "omp/not-a-version"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var registrations atomic.Int32
			var registeredVersion atomic.Value
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
				if len(body.Runtimes) > 0 {
					registeredVersion.Store(body.Runtimes[0].Version)
				}
				registrations.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"runtimes": []map[string]any{{
						"id": "rid-omp", "name": "omp", "provider": "omp",
						"status": "online", "profile_id": "primary",
					}},
				})
			}))
			defer server.Close()

			global := filepath.Join(t.TempDir(), "omp")
			globalScript := "#!/bin/sh\n" +
				"if [ \"$1\" = \"--version\" ]; then echo 'omp/9.9.9'; exit 0; fi\n" +
				"if [ \"$1\" = \"models\" ] && [ \"$2\" = \"ls\" ] && [ \"$3\" = \"--help\" ]; then echo '--json --no-extensions'; exit 0; fi\n" +
				"if [ \"$1\" = \"models\" ] && [ \"$2\" = \"ls\" ] && [ \"$3\" = \"--json\" ]; then echo '[]'; exit 0; fi\n" +
				"exit 1\n"
			if err := os.WriteFile(global, []byte(globalScript), 0o500); err != nil {
				t.Fatalf("write global omp: %v", err)
			}
			payload := []byte("#!/bin/sh\n" +
				"if [ \"$1\" = \"--version\" ]; then echo '" + tc.reported + "'; exit 0; fi\n" +
				"if [ \"$1\" = \"models\" ] && [ \"$2\" = \"ls\" ] && [ \"$3\" = \"--help\" ]; then echo '--json --no-extensions'; exit 0; fi\n" +
				"exit 1\n")

			client := startupTestClient(t, server.URL)
			clientSet, err := proxy.NewClientSet(client)
			if err != nil {
				t.Fatalf("NewClientSet: %v", err)
			}
			catalogService, err := startupTestCatalog(t, server.URL)
			if err != nil {
				t.Fatalf("catalog.New: %v", err)
			}
			releaseService := verifiedReleaseServiceVersion(t, "atlas", "omp", tc.declared, payload, true)
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
			discoveryService, err := startupTestDiscovery(t,
				discovery.AdapterRegistration{Adapter: discovery.NewOMPAdapter(global)},
			)
			if err != nil {
				t.Fatalf("discovery.New: %v", err)
			}
			if _, err := discoveryService.VerifyAdapter(context.Background(), "omp"); err != nil {
				t.Fatalf("VerifyAdapter: %v", err)
			}
			discoverer := newControlPlaneDiscovery(
				catalogService, releaseService, discoveryService, configService, clientSet,
				slog.New(slog.NewTextHandler(io.Discard, nil)), nil,
			)
			out, err := discoverer.Discover(context.Background())
			if err != nil {
				t.Fatalf("Discover: %v", err)
			}
			if tc.want {
				if got := registrations.Load(); got != 1 {
					t.Fatalf("registrations = %d, want 1", got)
				}
				version, _ := registeredVersion.Load().(string)
				if version != tc.declared {
					t.Fatalf("registered version = %q, want %q", version, tc.declared)
				}
				declaration, ok := out["ws-a"]
				if !ok || len(declaration.Runtimes) != 1 || declaration.Runtimes[0].Version != tc.declared {
					t.Fatalf("discoveries = %+v, want version %s", out, tc.declared)
				}
				return
			}
			if got := registrations.Load(); got != 0 {
				t.Fatalf("registrations = %d, want none for %s", got, tc.name)
			}
			if len(out) != 0 {
				t.Fatalf("discoveries = %+v, want none for %s", out, tc.name)
			}
			if ids := client.RuntimeIDs(); len(ids) != 0 {
				t.Fatalf("runtime ids = %v, want none for %s", ids, tc.name)
			}
		})
	}
}

// TestControlPlaneDiscoveryRejectsAnUnverifiedRelease: a trusted global
// adapter does not authorize a Current pointer whose version has no matching
// verification record.
func TestControlPlaneDiscoveryRejectsAnUnverifiedRelease(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("an unverified release must not register a runtime")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client := startupTestClient(t, server.URL)
	clientSet, err := proxy.NewClientSet(client)
	if err != nil {
		t.Fatalf("NewClientSet: %v", err)
	}
	catalogService, err := startupTestCatalog(t, server.URL)
	if err != nil {
		t.Fatalf("catalog.New: %v", err)
	}
	releaseService := verifiedReleaseService(t, "atlas", "omp", []byte("verified-omp-binary"), false)
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
	discoveryService, err := startupTestDiscovery(t,
		discovery.AdapterRegistration{Adapter: &boundTrustedAdapter{trustedAdapter: trustedAdapter{name: "omp"}}},
	)
	if err != nil {
		t.Fatalf("discovery.New: %v", err)
	}
	if _, err := discoveryService.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	discoverer := newControlPlaneDiscovery(
		catalogService, releaseService, discoveryService, configService, clientSet,
		slog.New(slog.NewTextHandler(io.Discard, nil)), nil,
	)
	out, err := discoverer.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("discoveries = %+v, want none for an unverified release", out)
	}
	if ids := client.RuntimeIDs(); len(ids) != 0 {
		t.Fatalf("runtime ids = %v, want none", ids)
	}
}

// TestControlPlaneDiscoveryRejectsAFailedBoundProbe: the global adapter probe
// succeeds, but the exact published digest cannot prove its command surface,
// so the release is not declared.
func TestControlPlaneDiscoveryRejectsAFailedBoundProbe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("a failed bound probe must not register a runtime")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client := startupTestClient(t, server.URL)
	clientSet, err := proxy.NewClientSet(client)
	if err != nil {
		t.Fatalf("NewClientSet: %v", err)
	}
	catalogService, err := startupTestCatalog(t, server.URL)
	if err != nil {
		t.Fatalf("catalog.New: %v", err)
	}
	releaseService := verifiedReleaseService(t, "atlas", "omp", []byte("verified-omp-binary"), true)
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
	adapter := &boundTrustedAdapter{
		trustedAdapter: trustedAdapter{name: "omp"},
		boundErr:       errors.New("published digest lacks the model command"),
	}
	discoveryService, err := startupTestDiscovery(t,
		discovery.AdapterRegistration{Adapter: adapter},
	)
	if err != nil {
		t.Fatalf("discovery.New: %v", err)
	}
	if _, err := discoveryService.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	discoverer := newControlPlaneDiscovery(
		catalogService, releaseService, discoveryService, configService, clientSet,
		slog.New(slog.NewTextHandler(io.Discard, nil)), nil,
	)
	out, err := discoverer.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("discoveries = %+v, want none after a failed bound probe", out)
	}
}

// TestControlPlaneDiscoveryDeclaresOnlyConfiguredProfiles: a profile without
// a registered configuration revision is not declared, and the empty profile
// is not assumed.
func TestControlPlaneDiscoveryDeclaresOnlyConfiguredProfiles(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/daemon/register" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"runtimes": []map[string]any{{
				"id": "rid-omp", "name": "omp", "provider": "omp",
				"status": "online", "profile_id": "secondary",
			}},
		})
	}))
	defer server.Close()
	client := startupTestClient(t, server.URL)
	clientSet, err := proxy.NewClientSet(client)
	if err != nil {
		t.Fatalf("NewClientSet: %v", err)
	}
	catalogService, err := startupTestCatalog(t, server.URL)
	if err != nil {
		t.Fatalf("catalog.New: %v", err)
	}
	releaseService := verifiedReleaseService(t, "atlas", "omp", []byte("verified-omp-binary"), true)
	configService, err := agentconfig.New(agentconfig.Config{Store: agentconfig.NewMemoryStore()})
	if err != nil {
		t.Fatalf("agentconfig.New: %v", err)
	}
	if _, err := configService.Write(context.Background(), agentconfig.Scope{
		ServerID: "atlas", WorkspaceID: "ws-a", Backend: "omp", Profile: "secondary",
	}, agentconfig.WriteRequest{
		SecretRef: "omp-secondary", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp"}`),
	}, "ops"); err != nil {
		t.Fatalf("config Write: %v", err)
	}
	discoveryService, err := startupTestDiscovery(t,
		discovery.AdapterRegistration{Adapter: &boundTrustedAdapter{trustedAdapter: trustedAdapter{name: "omp"}}},
	)
	if err != nil {
		t.Fatalf("discovery.New: %v", err)
	}
	if _, err := discoveryService.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	discoverer := newControlPlaneDiscovery(
		catalogService, releaseService, discoveryService, configService, clientSet,
		slog.New(slog.NewTextHandler(io.Discard, nil)), nil,
	)
	out, err := discoverer.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	declaration, ok := out["ws-a"]
	if !ok || len(declaration.Runtimes) != 1 || declaration.Runtimes[0].ProfileID != "secondary" {
		t.Fatalf("discoveries = %+v, want only the configured secondary profile", out)
	}
}

// TestReleaseClaimFactsLeavesUnboundVersionsUnknownAndFreezesConfig: the
// management pointers are not execution evidence, so the CLI facts stay
// unknown; the configuration revision is frozen onto the attempt and the
// first projection uses that same revision after a rotation.
func TestReleaseClaimFactsLeavesUnboundVersionsUnknownAndFreezesConfig(t *testing.T) {
	configService, err := agentconfig.New(agentconfig.Config{Store: agentconfig.NewMemoryStore()})
	if err != nil {
		t.Fatalf("agentconfig.New: %v", err)
	}
	scope := agentconfig.Scope{
		ServerID: "atlas", WorkspaceID: "ws-a", Backend: "omp", Profile: "primary",
	}
	if _, err := configService.Write(context.Background(), scope, agentconfig.WriteRequest{
		SecretRef: "omp-primary", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp"}`),
	}, "ops"); err != nil {
		t.Fatalf("config Write: %v", err)
	}
	facts := releaseClaimFacts{
		configs:  configService,
		sourceID: func(context.Context, string) (catalog.ServerID, bool) { return "atlas", true },
	}.Facts(context.Background(), "ws-a", "agent-1", "omp\x00primary", "attempt-1")
	if facts.CLIKnown || facts.MulticaVersion != "" || len(facts.MulticaDigests) != 0 {
		t.Fatalf("Multica facts = %+v, want unknown rather than the management pointer", facts)
	}
	if facts.AgentKnown || facts.AgentVersion != "" || len(facts.AgentDigests) != 0 {
		t.Fatalf("Agent facts = %+v, want unknown rather than the management pointer", facts)
	}
	if !facts.ConfigKnown || facts.ConfigRevision != 1 {
		t.Fatalf("config facts = %+v, want the frozen revision 1", facts)
	}
	if _, err := configService.Write(context.Background(), scope, agentconfig.WriteRequest{
		SecretRef: "omp-primary", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp","rotated":true}`),
	}, "ops"); err != nil {
		t.Fatalf("rotate config: %v", err)
	}
	projection, err := configService.Project(context.Background(), scope, "attempt-1")
	if err != nil {
		t.Fatalf("Project after rotation: %v", err)
	}
	if projection.Revision != facts.ConfigRevision {
		t.Fatalf("projection revision = %d, want the claim's %d", projection.Revision, facts.ConfigRevision)
	}
}

type failingRevisionStore struct {
	*agentconfig.MemoryStore
	fail atomic.Bool
}

func (s *failingRevisionStore) GetRevision(ctx context.Context, scope agentconfig.Scope) (agentconfig.Revision, bool, error) {
	if s.fail.Load() {
		return agentconfig.Revision{}, false, errors.New("configuration store is unavailable")
	}
	return s.MemoryStore.GetRevision(ctx, scope)
}

// TestReleaseClaimFactsReportsLookupFailureAsUnknown: a store error is not a
// known "no configuration" answer.
func TestReleaseClaimFactsReportsLookupFailureAsUnknown(t *testing.T) {
	store := &failingRevisionStore{MemoryStore: agentconfig.NewMemoryStore()}
	configService, err := agentconfig.New(agentconfig.Config{Store: store})
	if err != nil {
		t.Fatalf("agentconfig.New: %v", err)
	}
	scope := agentconfig.Scope{
		ServerID: "atlas", WorkspaceID: "ws-a", Backend: "omp", Profile: "primary",
	}
	if _, err := configService.Write(context.Background(), scope, agentconfig.WriteRequest{
		SecretRef: "omp-primary", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp"}`),
	}, "ops"); err != nil {
		t.Fatalf("config Write: %v", err)
	}
	store.fail.Store(true)
	facts := releaseClaimFacts{
		configs:  configService,
		sourceID: func(context.Context, string) (catalog.ServerID, bool) { return "atlas", true },
	}.Facts(context.Background(), "ws-a", "agent-1", "omp\x00primary", "attempt-1")
	if facts.ConfigKnown {
		t.Fatalf("config facts = %+v, want unknown after a lookup failure", facts)
	}
}

var _ discovery.Adapter = trustedAdapter{}
