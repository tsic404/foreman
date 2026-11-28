package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/catalog/catalogtest"
	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/environment/environmenttest"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
)

func environmentTestLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func cmdEnvironmentRegistration(id string) environment.Registration {
	return environment.Registration{
		ID:             id,
		Endpoint:       "build-a.example.test:7443",
		TLSRef:         "ref/" + id,
		StateRoot:      "/srv/rsbx/" + id,
		TrustDomain:    "build",
		AllowedSources: []string{"atlas"},
		Capabilities:   []string{"nix"},
		Concurrency:    2,
	}
}

func TestNewAdminRootSplitsEnvironmentFromManagementPaths(t *testing.T) {
	managementAdmin := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("management"))
	})
	environmentAdmin := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("environment"))
	})
	rulesAdmin := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("rules"))
	})
	root := newAdminRoot(managementAdmin, environmentAdmin, rulesAdmin)
	for _, tc := range []struct {
		path string
		want string
	}{
		{path: "/admin/environments", want: "environment"},
		{path: "/admin/environments/build-a", want: "environment"},
		{path: "/admin/environments/build-a/slots", want: "environment"},
		{path: "/admin/environments/audit", want: "environment"},
		{path: "/admin/rules", want: "rules"},
		{path: "/admin/rules/preview", want: "rules"},
		{path: "/admin/readiness", want: "rules"},
		{path: "/admin/servers", want: "management"},
		{path: "/admin/overview", want: "management"},
		{path: "/admin/audit", want: "management"},
		{path: "/admin/cli/releases", want: "management"},
		{path: "/admin/discovery/requests", want: "management"},
	} {
		rec := httptest.NewRecorder()
		root.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != http.StatusOK || rec.Body.String() != tc.want {
			t.Errorf("GET %s = %d %q, want %q", tc.path, rec.Code, rec.Body.String(), tc.want)
		}
	}
}

func TestNewEnvironmentServiceFailsClosedWithoutAProbe(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "environments.json")
	getenv := func(key string) string {
		switch key {
		case EnvEnvironmentStateFile:
			return stateFile
		case EnvEnvironmentTrustDomains:
			return "build"
		}
		return ""
	}
	service, err := newEnvironmentService(getenv, nil, environmentTestLogger())
	if err != nil {
		t.Fatalf("newEnvironmentService: %v", err)
	}
	view, err := service.Register(t.Context(), cmdEnvironmentRegistration("build-a"), "test")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if view.Verified || view.Schedulable {
		t.Fatalf("a target verified without a probe: %+v", view)
	}
	if got := service.CountRegistered(t.Context()); got != 0 {
		t.Fatalf("CountRegistered = %d, want 0", got)
	}
	if _, err := os.Stat(stateFile); err != nil {
		t.Fatalf("the failed registration was not persisted: %v", err)
	}
}

func TestNewEnvironmentServiceRefusesRegistrationsWithoutTrustDomains(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "environments.json")
	getenv := func(key string) string {
		if key == EnvEnvironmentStateFile {
			return stateFile
		}
		return ""
	}
	service, err := newEnvironmentService(getenv, nil, environmentTestLogger())
	if err != nil {
		t.Fatalf("newEnvironmentService: %v", err)
	}
	_, err = service.Register(t.Context(), cmdEnvironmentRegistration("build-a"), "test")
	if err == nil || !strings.Contains(err.Error(), "not an allowed data-residency domain") {
		t.Fatalf("Register without FOREMAN_ENVIRONMENT_TRUST_DOMAINS = %v, want a residency refusal", err)
	}
	if got := service.CountRegistered(t.Context()); got != 0 {
		t.Fatalf("CountRegistered = %d, want 0", got)
	}
}

func TestNewEnvironmentServiceVerifiesThroughTheConfiguredHelper(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "environments.json")
	probePath := filepath.Join(t.TempDir(), "probe.sh")
	script := "#!/bin/sh\n" +
		"printf '%s' '{\"tree_id\":\"tree-1\",\"identity\":\"sha256:one\",\"capabilities\":[\"nix\"],\"capacity\":8,\"version\":\"rsbxd-1\"}'\n"
	if err := os.WriteFile(probePath, []byte(script), 0o755); err != nil {
		t.Fatalf("write probe helper: %v", err)
	}
	getenv := func(key string) string {
		switch key {
		case EnvEnvironmentStateFile:
			return stateFile
		case EnvEnvironmentProbe:
			return probePath
		case EnvEnvironmentTrustDomains:
			return "build"
		default:
			return ""
		}
	}
	service, err := newEnvironmentService(getenv, nil, environmentTestLogger())
	if err != nil {
		t.Fatalf("newEnvironmentService: %v", err)
	}
	view, err := service.Register(t.Context(), cmdEnvironmentRegistration("build-a"), "test")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if !view.Verified || !view.Schedulable {
		t.Fatalf("view = %+v, want a verified environment", view)
	}
	if got := service.CountRegistered(t.Context()); got != 1 {
		t.Fatalf("CountRegistered = %d, want 1", got)
	}

	reopened, err := newEnvironmentService(func(key string) string {
		switch key {
		case EnvEnvironmentStateFile:
			return stateFile
		case EnvEnvironmentTrustDomains:
			return "build"
		}
		return ""
	}, nil, environmentTestLogger())
	if err != nil {
		t.Fatalf("reopen newEnvironmentService: %v", err)
	}
	restored, err := reopened.Get(t.Context(), "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	if !restored.Verified || restored.Verification.TreeID != "tree-1" {
		t.Fatalf("restored view = %+v, want the verified registration", restored)
	}
}

// The overview count is a set cardinality over the caller's grant. A
// workspace credential for one source sees one environment, a credential that
// resolves to no source sees zero, and two scopes on the same source do not
// double count.
func TestOverviewEnvironmentCountFollowsTheCallerGrant(t *testing.T) {
	ctx := context.Background()
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return environment.ProbeResult{
			TreeID:       "tree-" + target.Endpoint,
			Identity:     "sha256:" + target.Endpoint,
			Capabilities: []string{"nix"},
			Capacity:     8,
		}, nil
	}
	environmentService, err := environment.New(environment.Config{
		Probe:               probe,
		AllowedTrustDomains: []string{"build"},
		Logger:              environmentTestLogger(),
	})
	if err != nil {
		t.Fatalf("environment.New: %v", err)
	}
	regA := cmdEnvironmentRegistration("build-a")
	regA.AllowedSources = []string{"atlas"}
	regB := cmdEnvironmentRegistration("build-b")
	regB.Endpoint = "build-b.example.test:7443"
	regB.StateRoot = "/srv/rsbx/build-b"
	regB.AllowedSources = []string{"harbor"}
	for _, reg := range []environment.Registration{regA, regB} {
		if _, err := environmentService.Register(ctx, reg, "test"); err != nil {
			t.Fatalf("environment Register %s: %v", reg.ID, err)
		}
	}

	upstream := catalogtest.NewUpstream()
	catalogService, err := catalog.New(catalog.Config{
		Upstream:    upstream,
		Credentials: catalogtest.Credentials{},
		EnvironmentCount: func(ctx context.Context, grant catalog.Grant) int {
			return environmentService.CountRegisteredForGrant(ctx, environmentGrantFromCatalog(grant))
		},
		Logger: environmentTestLogger(),
	})
	if err != nil {
		t.Fatalf("catalog.New: %v", err)
	}
	for _, id := range []string{"atlas", "harbor"} {
		if _, err := catalogService.Register(ctx, catalog.Registration{
			ServerID: id, URL: "https://" + id + ".example.test", CredentialRef: id,
		}, "test"); err != nil {
			t.Fatalf("catalog Register %s: %v", id, err)
		}
	}

	cases := []struct {
		name  string
		grant catalog.Grant
		want  int
	}{
		{name: "deployment wide", grant: catalog.Grant{}, want: 2},
		{name: "atlas credential", grant: catalog.Grant{Scopes: []catalog.Scope{{ServerID: "atlas", WorkspaceID: "ws-core"}}}, want: 1},
		{name: "harbor credential", grant: catalog.Grant{Scopes: []catalog.Scope{{ServerID: "harbor", WorkspaceID: "ws-core"}}}, want: 1},
		{name: "no resolved source", grant: catalog.Grant{Scopes: []catalog.Scope{}}, want: 0},
		{name: "two atlas scopes", grant: catalog.Grant{Scopes: []catalog.Scope{
			{ServerID: "atlas", WorkspaceID: "ws-core"},
			{ServerID: "atlas", WorkspaceID: "ws-other"},
		}}, want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			view, err := catalogService.Overview(ctx, tc.grant)
			if err != nil {
				t.Fatalf("Overview: %v", err)
			}
			if !view.Counts.EnvironmentsAvailable {
				t.Fatal("environments read as unavailable with the registry wired")
			}
			if view.Counts.Environments != tc.want {
				t.Fatalf("environments = %d, want %d", view.Counts.Environments, tc.want)
			}
		})
	}
}

func TestNewEnvironmentAdminResolvesAWorkspaceCredentialToItsSource(t *testing.T) {
	ctx := context.Background()
	upstream := catalogtest.NewUpstream()
	upstream.Workspaces = []catalog.Workspace{{ID: "ws-core", Name: "core"}}
	catalogService, err := catalog.New(catalog.Config{
		Upstream:    upstream,
		Credentials: catalogtest.Credentials{},
		Clock:       func() time.Time { return time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC) },
		Logger:      environmentTestLogger(),
	})
	if err != nil {
		t.Fatalf("catalog.New: %v", err)
	}
	if _, err := catalogService.Register(ctx, catalog.Registration{
		ServerID: "atlas", URL: "https://atlas.example.test", CredentialRef: "atlas",
	}, "test"); err != nil {
		t.Fatalf("catalog Register: %v", err)
	}
	if _, err := catalogService.Sync(ctx, "atlas"); err != nil {
		t.Fatalf("catalog Sync: %v", err)
	}

	probe := environmenttest.NewProbe()
	probe.Result = environment.ProbeResult{
		TreeID: "tree-1", Identity: "sha256:one", Capabilities: []string{"nix"}, Capacity: 8,
	}
	environmentService, err := environment.New(environment.Config{
		Probe: probe, AllowedTrustDomains: []string{"build"}, Logger: environmentTestLogger(),
	})
	if err != nil {
		t.Fatalf("environment.New: %v", err)
	}
	if _, err := environmentService.Register(ctx, cmdEnvironmentRegistration("build-a"), "test"); err != nil {
		t.Fatalf("environment Register: %v", err)
	}
	admin, err := newEnvironmentAdmin(environmentService, catalogService)
	if err != nil {
		t.Fatalf("newEnvironmentAdmin: %v", err)
	}
	gate := observability.NewCredentialGate([]observability.OpsCredential{{
		Token:  "ws-token",
		Scopes: []observability.Scope{{WorkspaceID: "ws-core"}},
	}})
	handler := gate.Wrap(admin)

	request := func(method, path string, authenticated bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		if authenticated {
			req.Header.Set("Authorization", "Bearer ws-token")
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	if rec := request(http.MethodGet, "/admin/environments", false); rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET without a credential = %d, want 401", rec.Code)
	}
	rec := request(http.MethodGet, "/admin/environments", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET with the workspace credential = %d %s, want 200", rec.Code, rec.Body.String())
	}
	var views []environment.EnvironmentView
	if err := json.Unmarshal(rec.Body.Bytes(), &views); err != nil {
		t.Fatalf("decode environments: %v", err)
	}
	if len(views) != 1 || views[0].ID != "build-a" {
		t.Fatalf("environments = %+v, want build-a through its source", views)
	}
	if rec := request(http.MethodDelete, "/admin/environments/build-a", true); rec.Code != http.StatusForbidden {
		t.Fatalf("DELETE with the workspace credential = %d, want 403", rec.Code)
	}
}

// TestFixedDeliveryGateWaitsForStartedSlot pins the production gate the S-6
// handler calls: a frozen attempt's payload is held until the supervisor has
// committed the started phase on the shared ledger.
func TestFixedDeliveryGateWaitsForStartedSlot(t *testing.T) {
	ctx := t.Context()
	probe := environmenttest.NewProbe()
	probe.Result = environment.ProbeResult{
		TreeID: "tree-1", Identity: "sha256:tree-1", Capabilities: []string{"nix"},
		Capacity: 2, Version: "rsbxd-test",
	}
	service, err := environment.New(environment.Config{
		Probe:               probe,
		AllowedTrustDomains: []string{"build"},
	})
	if err != nil {
		t.Fatalf("environment.New: %v", err)
	}
	if _, err := service.Register(ctx, cmdEnvironmentRegistration("build-a"), "test"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := service.AcquireSlot(ctx, "build-a", "slot-1"); err != nil {
		t.Fatalf("AcquireSlot: %v", err)
	}
	if _, err := service.BeginSlot(ctx, "build-a", "slot-1", "claim-1"); err != nil {
		t.Fatalf("BeginSlot: %v", err)
	}

	raw, err := json.Marshal(attempt.Binding{AttemptID: "task-1", EnvironmentID: "build-a", SlotID: "slot-1"})
	if err != nil {
		t.Fatalf("encode the frozen binding: %v", err)
	}
	entry := registry.TaskEntry{FixedAttempt: raw}
	gate := newFixedDeliveryGate(service)
	ready, err := gate(ctx, entry)
	if err != nil || ready {
		t.Fatalf("gate before started = (%v, %v), want a plain not-yet", ready, err)
	}
	if _, err := service.BindSlot(ctx, "build-a", "slot-1", "claim-1", "session-1"); err != nil {
		t.Fatalf("BindSlot: %v", err)
	}
	if ready, err := gate(ctx, entry); err != nil || ready {
		t.Fatalf("gate after bind = (%v, %v), want still not-yet", ready, err)
	}
	if _, err := service.MarkSlotStarted(ctx, "build-a", "slot-1", "claim-1"); err != nil {
		t.Fatalf("MarkSlotStarted: %v", err)
	}
	if ready, err := gate(ctx, entry); err != nil || !ready {
		t.Fatalf("gate after started = (%v, %v), want the payload released", ready, err)
	}

	bad := registry.TaskEntry{FixedAttempt: json.RawMessage("{")}
	if _, err := gate(ctx, bad); err == nil {
		t.Fatal("gate accepted an unreadable frozen binding")
	}
}

// TestPodCloseObserverKeepsUnknownAndClosedApart pins the trusted observer:
// only terminal Pods prove a close, and an empty match is unknown rather than
// permission to return capacity.
func TestPodCloseObserverKeepsUnknownAndClosedApart(t *testing.T) {
	ctx := t.Context()
	entry := registry.TaskEntry{JobName: "fm-task-1", JobNamespace: "foreman"}
	if closed, err := newPodCloseObserver(fake.NewSimpleClientset(), "foreman")(ctx, entry, attempt.Binding{}); err == nil || closed {
		t.Fatalf("no matching pod = (%v, %v), want unknown", closed, err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "fm-task-1-x", Namespace: "foreman",
			Labels: map[string]string{"job-name": "fm-task-1"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	observer := newPodCloseObserver(fake.NewSimpleClientset(pod), "foreman")
	if closed, err := observer(ctx, entry, attempt.Binding{}); err != nil || closed {
		t.Fatalf("running pod = (%v, %v), want still open", closed, err)
	}
	pod.Status.Phase = corev1.PodSucceeded
	observer = newPodCloseObserver(fake.NewSimpleClientset(pod), "foreman")
	if closed, err := observer(ctx, entry, attempt.Binding{}); err != nil || !closed {
		t.Fatalf("terminal pod = (%v, %v), want closed", closed, err)
	}
}
