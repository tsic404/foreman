package environment_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/environment/environmenttest"
)

type adminFixture struct {
	service *environment.Service
	handler http.Handler
	rules   *environment.MemoryRuleReferences
}

func newAdminFixture(t *testing.T) *adminFixture {
	t.Helper()
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return environment.ProbeResult{
			TreeID:       "tree-" + target.StateRoot,
			Identity:     "sha256:" + target.StateRoot,
			Capabilities: []string{"nix"},
			Capacity:     8,
			Version:      "rsbxd-test",
		}, nil
	}
	rules := environment.NewMemoryRuleReferences()
	service, err := environment.New(environment.Config{
		Probe:               probe,
		AllowedTrustDomains: []string{"build"},
		RuleReferences:      rules,
		Clock:               testClock,
		Logger:              slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	handler, err := environment.NewAdminHandler(environment.AdminConfig{
		Service: service,
		Grant: func(r *http.Request) (environment.Grant, bool) {
			switch r.Header.Get("Authorization") {
			case "Bearer wide":
				return environment.Grant{DeploymentWide: true}, true
			case "Bearer atlas":
				return environment.Grant{Sources: []string{"atlas"}}, true
			default:
				return environment.Grant{}, false
			}
		},
		Actor: func(*http.Request) string { return "ops-test" },
	})
	if err != nil {
		t.Fatalf("NewAdminHandler: %v", err)
	}
	return &adminFixture{service: service, handler: handler, rules: rules}
}

func adminRequest(t *testing.T, handler http.Handler, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func decodeEnvironment(t *testing.T, rec *httptest.ResponseRecorder) environment.EnvironmentView {
	t.Helper()
	var view environment.EnvironmentView
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("decode environment view: %v (%s)", err, rec.Body.String())
	}
	return view
}

// The management surface fails closed: no credential is 401, and a
// workspace-bound credential may read but never register, edit, drain, retire
// or read the deployment-wide audit.
func TestAdminRequiresACredentialAndADeploymentGrantForMutations(t *testing.T) {
	fixture := newAdminFixture(t)

	if rec := adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET without a credential = %d, want 401", rec.Code)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments", "atlas", nil); rec.Code != http.StatusOK {
		t.Fatalf("GET with a workspace credential = %d, want 200", rec.Code)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments/audit", "atlas", nil); rec.Code != http.StatusForbidden {
		t.Fatalf("GET audit with a workspace credential = %d, want 403", rec.Code)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments/audit", "wide", nil); rec.Code != http.StatusOK {
		t.Fatalf("GET audit with the deployment credential = %d, want 200", rec.Code)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments/retired", "atlas", nil); rec.Code != http.StatusOK {
		t.Fatalf("GET retired with a workspace credential = %d, want 200", rec.Code)
	}

	reg := registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2)
	if rec := adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments", "atlas", reg); rec.Code != http.StatusForbidden {
		t.Fatalf("POST with a workspace credential = %d, want 403", rec.Code)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodDelete, "/admin/environments/build-a", "atlas", nil); rec.Code != http.StatusForbidden {
		t.Fatalf("DELETE with a workspace credential = %d, want 403", rec.Code)
	}
}

// ENV-04: a target is registered, verified, edited, re-verified, drained and
// resumed through the management API; before verification it is not a
// candidate.
func TestAdminRegistersAndDrivesTheLifecycle(t *testing.T) {
	fixture := newAdminFixture(t)
	reg := registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2)
	reg.AllowedSources = []string{"atlas"}

	rec := adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments", "wide", reg)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST register = %d %s, want 201", rec.Code, rec.Body.String())
	}
	created := decodeEnvironment(t, rec)
	if !created.Verified || !created.Schedulable || created.Phase != string(environment.PhaseActive) {
		t.Fatalf("registered view = %+v, want an active, verified environment", created)
	}
	if candidates := fixture.service.Candidates(t.Context(), environment.CandidateFilter{}); len(candidates) != 1 {
		t.Fatalf("Candidates = %+v, want the registered environment", candidates)
	}

	if rec := adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments", "wide", nil); rec.Code != http.StatusOK {
		t.Fatalf("GET list = %d, want 200", rec.Code)
	} else {
		var views []environment.EnvironmentView
		if err := json.Unmarshal(rec.Body.Bytes(), &views); err != nil {
			t.Fatalf("decode list: %v", err)
		}
		if len(views) != 1 || views[0].ID != "build-a" {
			t.Fatalf("list = %+v, want build-a", views)
		}
	}
	if rec := adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments/build-a", "atlas", nil); rec.Code != http.StatusOK {
		t.Fatalf("GET build-a with its source credential = %d, want 200", rec.Code)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments/missing", "wide", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("GET missing = %d, want 404", rec.Code)
	}

	bad := reg
	bad.ID = "Bad_ID"
	if rec := adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments", "wide", bad); rec.Code != http.StatusBadRequest {
		t.Fatalf("POST an invalid id = %d, want 400", rec.Code)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments", "wide", map[string]any{"id": "build-b"}); rec.Code != http.StatusBadRequest {
		t.Fatalf("POST a partial registration = %d, want 400", rec.Code)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodPatch, "/admin/environments/build-a", "wide", map[string]any{"unknown_field": "x"}); rec.Code != http.StatusBadRequest {
		t.Fatalf("PATCH an unknown field = %d, want 400", rec.Code)
	}

	rec = adminRequest(t, fixture.handler, http.MethodPatch, "/admin/environments/build-a", "wide",
		map[string]any{"endpoint": "build-b.example.test:7443"})
	if rec.Code != http.StatusOK {
		t.Fatalf("PATCH endpoint = %d %s, want 200", rec.Code, rec.Body.String())
	}
	stale := decodeEnvironment(t, rec)
	if stale.Schedulable || stale.Verification.State != string(environment.VerificationStale) {
		t.Fatalf("edited view = %+v, want a stale, unschedulable environment", stale)
	}

	rec = adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments/build-a/verify", "wide", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST verify = %d %s, want 200", rec.Code, rec.Body.String())
	}
	if verified := decodeEnvironment(t, rec); !verified.Verified || !verified.Schedulable {
		t.Fatalf("verified view = %+v, want active", verified)
	}

	rec = adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments/build-a/drain", "wide", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST drain = %d %s, want 200", rec.Code, rec.Body.String())
	}
	if drained := decodeEnvironment(t, rec); drained.Phase != string(environment.PhaseDraining) {
		t.Fatalf("drained view = %+v, want draining", drained)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments/build-a/resume", "wide", nil); rec.Code != http.StatusOK {
		t.Fatalf("POST resume = %d %s, want 200", rec.Code, rec.Body.String())
	}
}

// ENV-04: active slots, isolated slots, unconfirmed cleanups and rule
// references each refuse retirement with 409 and name the blocker; once the
// blocker is cleared, retirement removes the registration only.
func TestAdminRetirementIsBlockedUntilEveryReferenceIsCleared(t *testing.T) {
	fixture := newAdminFixture(t)
	reg := registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2)
	reg.AllowedSources = []string{"atlas"}
	if rec := adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments", "wide", reg); rec.Code != http.StatusCreated {
		t.Fatalf("POST register = %d %s", rec.Code, rec.Body.String())
	}
	if _, err := fixture.service.AcquireSlot(t.Context(), "build-a", "slot-1"); err != nil {
		t.Fatalf("AcquireSlot: %v", err)
	}

	rec := adminRequest(t, fixture.handler, http.MethodDelete, "/admin/environments/build-a", "wide", nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("DELETE with an active slot = %d, want 409", rec.Code)
	}
	var blocked struct {
		Blockers []string `json:"blockers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &blocked); err != nil {
		t.Fatalf("decode blockers: %v", err)
	}
	if len(blocked.Blockers) == 0 || !strings.Contains(strings.Join(blocked.Blockers, "; "), "active slot") {
		t.Fatalf("blockers = %+v, want the active slot named", blocked.Blockers)
	}

	if _, err := fixture.service.ReleaseSlot(t.Context(), "build-a", "slot-1", false); err != nil {
		t.Fatalf("ReleaseSlot unconfirmed: %v", err)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments/build-a/slots", "wide", nil); rec.Code != http.StatusOK {
		t.Fatalf("GET slots = %d, want 200", rec.Code)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodDelete, "/admin/environments/build-a", "wide", nil); rec.Code != http.StatusConflict {
		t.Fatalf("DELETE with an isolated slot = %d, want 409", rec.Code)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments/build-a/slots/slot-1/confirm", "wide", nil); rec.Code != http.StatusOK {
		t.Fatalf("POST confirm slot cleanup = %d %s, want 200", rec.Code, rec.Body.String())
	}
	if rec := adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments/build-a/slots/missing/confirm", "wide", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("POST confirm a missing slot = %d, want 404", rec.Code)
	}

	if err := fixture.service.AddPendingCleanup(t.Context(), "build-a", "cleanup-1", "worktree:attempt-1"); err != nil {
		t.Fatalf("AddPendingCleanup: %v", err)
	}
	rec = adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments/build-a/cleanups", "wide", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET cleanups = %d, want 200", rec.Code)
	}
	var cleanups []environment.CleanupView
	if err := json.Unmarshal(rec.Body.Bytes(), &cleanups); err != nil {
		t.Fatalf("decode cleanups: %v", err)
	}
	if len(cleanups) != 1 || cleanups[0].ID != "cleanup-1" {
		t.Fatalf("cleanups = %+v, want cleanup-1", cleanups)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodDelete, "/admin/environments/build-a", "wide", nil); rec.Code != http.StatusConflict {
		t.Fatalf("DELETE with an unconfirmed cleanup = %d, want 409", rec.Code)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments/build-a/cleanups/cleanup-1/confirm", "wide", nil); rec.Code != http.StatusOK {
		t.Fatalf("POST confirm cleanup = %d %s, want 200", rec.Code, rec.Body.String())
	}
	if rec := adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments/build-a/cleanups/missing/confirm", "wide", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("POST confirm a missing cleanup = %d, want 404", rec.Code)
	}

	fixture.rules.Add("build-a", "rule-core-dev")
	if rec := adminRequest(t, fixture.handler, http.MethodDelete, "/admin/environments/build-a", "wide", nil); rec.Code != http.StatusConflict {
		t.Fatalf("DELETE with a rule reference = %d, want 409", rec.Code)
	}
	fixture.rules.Remove("build-a", "rule-core-dev")

	rec = adminRequest(t, fixture.handler, http.MethodDelete, "/admin/environments/build-a", "wide", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE after every blocker is cleared = %d %s, want 200", rec.Code, rec.Body.String())
	}
	if rec := adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments/build-a", "wide", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("GET after retirement = %d, want 404", rec.Code)
	}
	rec = adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments/retired", "atlas", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET retired = %d, want 200", rec.Code)
	}
	var tombstones []environment.TombstoneView
	if err := json.Unmarshal(rec.Body.Bytes(), &tombstones); err != nil {
		t.Fatalf("decode tombstones: %v", err)
	}
	if len(tombstones) != 1 || tombstones[0].ID != "build-a" || tombstones[0].TreeID != "tree-/srv/rsbx/build-a" {
		t.Fatalf("tombstones = %+v, want build-a's tree", tombstones)
	}
}

// The list and read routes crop to the credential's source grant; a row
// outside the grant is absent, not empty.
func TestAdminReadsAreScopedToTheCredentialSource(t *testing.T) {
	fixture := newAdminFixture(t)
	for _, reg := range []environment.Registration{
		{ID: "build-a", Endpoint: "build-a.example.test:7443", TLSRef: "ref/build-a", StateRoot: "/srv/rsbx/build-a", TrustDomain: "build", AllowedSources: []string{"atlas"}, Capabilities: []string{"nix"}, Concurrency: 2},
		{ID: "build-b", Endpoint: "build-b.example.test:7443", TLSRef: "ref/build-b", StateRoot: "/srv/rsbx/build-b", TrustDomain: "build", AllowedSources: []string{"harbor"}, Capabilities: []string{"nix"}, Concurrency: 2},
	} {
		if rec := adminRequest(t, fixture.handler, http.MethodPost, "/admin/environments", "wide", reg); rec.Code != http.StatusCreated {
			t.Fatalf("POST %s = %d %s", reg.ID, rec.Code, rec.Body.String())
		}
	}
	rec := adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments", "atlas", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET list = %d, want 200", rec.Code)
	}
	var views []environment.EnvironmentView
	if err := json.Unmarshal(rec.Body.Bytes(), &views); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(views) != 1 || views[0].ID != "build-a" {
		t.Fatalf("scoped list = %+v, want only build-a", views)
	}
	if rec := adminRequest(t, fixture.handler, http.MethodGet, "/admin/environments/build-b", "atlas", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("GET build-b with the atlas credential = %d, want 404", rec.Code)
	}
}

func TestAdminConstructionNeedsTheServiceGrantAndActor(t *testing.T) {
	service := newService(t, environmenttest.NewProbe())
	if _, err := environment.NewAdminHandler(environment.AdminConfig{}); err == nil {
		t.Fatal("NewAdminHandler accepted an empty configuration")
	}
	if _, err := environment.NewAdminHandler(environment.AdminConfig{Service: service}); err == nil {
		t.Fatal("NewAdminHandler accepted a surface with no grant resolver")
	}
	if _, err := environment.NewAdminHandler(environment.AdminConfig{
		Service: service,
		Grant:   func(*http.Request) (environment.Grant, bool) { return environment.Grant{}, true },
	}); err == nil {
		t.Fatal("NewAdminHandler accepted a surface with no actor resolver")
	}
}

// Removing a restored environment's trust domain from the allowlist makes
// verify and resume refuse it. That policy answer must reach the caller as a
// non-5xx conflict with a reason the operator can act on, and it must not
// probe the target or change the record.
func TestAdminMapsTheTrustDomainRefusalToConflict(t *testing.T) {
	store := environment.NewMemoryStore()
	env := environment.Environment{
		ID: "build-a",
		Target: environment.Target{
			Endpoint:       "build-a.example.test:7443",
			TLSRef:         "ref/build-a",
			StateRoot:      "/srv/rsbx/build-a",
			TrustDomain:    "build",
			AllowedSources: []string{"atlas"},
			Capabilities:   []string{"nix"},
			Concurrency:    2,
		},
		Phase:      environment.PhaseActive,
		Registered: true,
		Verification: environment.Verification{
			State:      environment.VerificationOK,
			TreeID:     "tree-a",
			VerifiedAt: testNow,
		},
		Slots:    map[string]environment.Slot{},
		Cleanups: map[string]environment.Cleanup{},
		Revision: 1,
	}
	if err := store.PutEnvironment(t.Context(), env); err != nil {
		t.Fatalf("seed the store: %v", err)
	}
	probe := environmenttest.NewProbe()
	service, err := environment.New(environment.Config{
		Store:               store,
		Probe:               probe,
		AllowedTrustDomains: []string{"lab"},
		Clock:               testClock,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	handler, err := environment.NewAdminHandler(environment.AdminConfig{
		Service: service,
		Grant: func(*http.Request) (environment.Grant, bool) {
			return environment.Grant{DeploymentWide: true}, true
		},
		Actor: func(*http.Request) string { return "ops-test" },
	})
	if err != nil {
		t.Fatalf("NewAdminHandler: %v", err)
	}

	for _, path := range []string{
		"/admin/environments/build-a/verify",
		"/admin/environments/build-a/resume",
	} {
		rec := adminRequest(t, handler, http.MethodPost, path, "", nil)
		if rec.Code != http.StatusConflict {
			t.Fatalf("POST %s = %d %s, want 409", path, rec.Code, rec.Body.String())
		}
		body := rec.Body.String()
		if !strings.Contains(body, "data-residency") || strings.Contains(body, "environment operation failed") {
			t.Fatalf("POST %s body = %s, want the policy reason", path, body)
		}
	}
	if calls := probe.CallCount(); calls != 0 {
		t.Fatalf("a refused verification probed the target %d time(s)", calls)
	}
	view, err := service.Get(t.Context(), "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get after the refusals: %v", err)
	}
	if view.Phase != string(environment.PhaseActive) || view.ActiveSlots != 0 ||
		view.Verification.State != string(environment.VerificationOK) {
		t.Fatalf("a refused policy call changed the record: %+v", view)
	}
}
