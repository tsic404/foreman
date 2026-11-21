package discovery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testDiscoveryAdmin(t *testing.T, service *Service, wide, ok bool) *AdminHandler {
	t.Helper()
	handler, err := NewAdminHandler(AdminConfig{
		Service: service,
		Authorize: func(*http.Request) (bool, bool) {
			return wide, ok
		},
		Actor: func(*http.Request) string { return "test-ops" },
	})
	if err != nil {
		t.Fatalf("NewAdminHandler: %v", err)
	}
	return handler
}

func serveDiscoveryAdmin(handler http.Handler, method, target, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	handler.ServeHTTP(rec, request)
	return rec
}

// TestDiscoveryAdminKeepsTheFourClassesAndStatusMapping pins the route an
// operator calls: a verified adapter can answer, an unknown adapter is
// unsupported, and the catalog never turns that into an empty success.
func TestDiscoveryAdminKeepsTheFourClassesAndStatusMapping(t *testing.T) {
	adapter := &fakeAdapter{name: "omp", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		return Outcome{Models: []Model{{ID: "m1"}}}, nil
	}}
	service := newService(t, AdapterRegistration{Adapter: adapter})
	handler := testDiscoveryAdmin(t, service, true, true)

	verify := serveDiscoveryAdmin(handler, http.MethodPost, "/admin/discovery/adapters/omp/verify", "")
	if verify.Code != http.StatusOK || !strings.Contains(verify.Body.String(), `"trusted":true`) {
		t.Fatalf("verify = %d %s", verify.Code, verify.Body.String())
	}
	okRequest := serveDiscoveryAdmin(handler, http.MethodPost, "/admin/discovery/requests", `{
		"server_id":"atlas","workspace_id":"core","runtime_id":"rt-omp",
		"kind":"models","adapter":"omp","config_revision":1
	}`)
	if okRequest.Code != http.StatusOK || !strings.Contains(okRequest.Body.String(), "succeeded_nonempty") {
		t.Fatalf("discovery request = %d %s", okRequest.Code, okRequest.Body.String())
	}
	unsupported := serveDiscoveryAdmin(handler, http.MethodPost, "/admin/discovery/requests", `{
		"server_id":"atlas","workspace_id":"core","runtime_id":"rt-codex",
		"kind":"models","adapter":"codex","config_revision":1
	}`)
	if unsupported.Code != http.StatusOK || !strings.Contains(unsupported.Body.String(), "unsupported") {
		t.Fatalf("unsupported request = %d %s", unsupported.Code, unsupported.Body.String())
	}
	models := serveDiscoveryAdmin(handler, http.MethodGet,
		"/admin/discovery/models?server_id=atlas&workspace_id=core&runtime_id=rt-codex", "")
	if models.Code != http.StatusOK {
		t.Fatalf("models = %d %s", models.Code, models.Body.String())
	}
	if strings.Contains(models.Body.String(), `"available":true`) {
		t.Fatalf("unsupported adapter answered an available catalog: %s", models.Body.String())
	}
	bad := serveDiscoveryAdmin(handler, http.MethodPost, "/admin/discovery/requests", `{
		"server_id":"atlas","workspace_id":"core","runtime_id":"rt-omp","kind":"models"
	}`)
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("missing adapter = %d %s, want 400", bad.Code, bad.Body.String())
	}
	list := serveDiscoveryAdmin(handler, http.MethodGet, "/admin/discovery/requests", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list = %d %s", list.Code, list.Body.String())
	}
}

func TestDiscoveryAdminRequiresAWideCredential(t *testing.T) {
	service, err := New(Config{Store: NewMemoryStore(), Clock: func() time.Time { return testNow }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for name, handler := range map[string]*AdminHandler{
		"unauthenticated": testDiscoveryAdmin(t, service, false, false),
		"scoped":          testDiscoveryAdmin(t, service, false, true),
	} {
		rec := serveDiscoveryAdmin(handler, http.MethodGet, "/admin/discovery/adapters", "")
		want := http.StatusForbidden
		if name == "unauthenticated" {
			want = http.StatusUnauthorized
		}
		if rec.Code != want {
			t.Errorf("%s = %d, want %d", name, rec.Code, want)
		}
	}
}

type mutableCLIResolver struct {
	adapter string
	digest  string
	ok      bool
}

func (r *mutableCLIResolver) AdapterForPlatform(context.Context, string, string, string) (string, string, bool) {
	return r.adapter, r.digest, r.ok
}

func (r *mutableCLIResolver) CurrentDigest(context.Context, string, string, string) (string, bool) {
	return r.digest, r.ok
}

type mutableConfigResolver struct {
	revision uint64
	known    bool
}

func (r *mutableConfigResolver) Revision(context.Context, string, string, string, string) (uint64, bool) {
	return r.revision, r.known
}

type mutableBindingResolver struct {
	binding Binding
	ok      bool
}

func (r *mutableBindingResolver) RuntimeBinding(context.Context, string, string, string) (Binding, bool) {
	return r.binding, r.ok
}

// TestAdminModelsUsesTheCurrentRuntimeBinding: the console's four-field model
// query resolves the adapter, platform, digest and revision server-side, and
// a CLI or configuration rotation makes the old catalog unavailable.
func TestAdminModelsUsesTheCurrentRuntimeBinding(t *testing.T) {
	adapter := &fakeAdapter{name: "omp", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		return Outcome{Models: []Model{{ID: "m1", Provider: "omp"}}}, nil
	}}
	service := newService(t, AdapterRegistration{Adapter: adapter})
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	if _, err := service.Submit(context.Background(), SubmitInput{
		ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
		Profile: "primary", Kind: KindModels, Adapter: "omp",
		CLIDigest: "d1", ConfigRevision: 1, Platform: "linux/amd64",
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	clis := &mutableCLIResolver{adapter: "omp", digest: "d1", ok: true}
	configs := &mutableConfigResolver{revision: 1, known: true}
	bindings := &mutableBindingResolver{
		binding: Binding{Adapter: "omp", Platform: "linux/amd64"}, ok: true,
	}
	handler, err := NewAdminHandler(AdminConfig{
		Service: service, CLIs: clis, Configs: configs, Bindings: bindings,
		Authorize: func(*http.Request) (bool, bool) { return true, true },
		Actor:     func(*http.Request) string { return "test-ops" },
	})
	if err != nil {
		t.Fatalf("NewAdminHandler: %v", err)
	}
	query := "/admin/discovery/models?server_id=atlas&workspace_id=core&runtime_id=rt-omp&profile=primary"
	available := serveDiscoveryAdmin(handler, http.MethodGet, query, "")
	if available.Code != http.StatusOK || !strings.Contains(available.Body.String(), `"available":true`) {
		t.Fatalf("initial models = %d %s, want available", available.Code, available.Body.String())
	}
	configs.revision = 2
	rotated := serveDiscoveryAdmin(handler, http.MethodGet, query, "")
	if rotated.Code != http.StatusOK || strings.Contains(rotated.Body.String(), `"available":true`) {
		t.Fatalf("models after config rotation = %d %s, want unavailable", rotated.Code, rotated.Body.String())
	}
	configs.revision = 1
	clis.digest = "d2"
	rotated = serveDiscoveryAdmin(handler, http.MethodGet, query, "")
	if rotated.Code != http.StatusOK || strings.Contains(rotated.Body.String(), `"available":true`) {
		t.Fatalf("models after CLI rotation = %d %s, want unavailable", rotated.Code, rotated.Body.String())
	}
	clis.digest = "d1"
	bindings.ok = false
	unknown := serveDiscoveryAdmin(handler, http.MethodGet, query, "")
	if unknown.Code != http.StatusOK || strings.Contains(unknown.Body.String(), `"available":true`) ||
		!strings.Contains(unknown.Body.String(), "unknown") {
		t.Fatalf("models without a binding = %d %s, want an explicit unknown", unknown.Code, unknown.Body.String())
	}
}

// TestAdminModelsDoesNotTreatMissingPlatformAsAWildcard: an explicit adapter
// without a platform or binding cannot match every platform.
func TestAdminModelsDoesNotTreatMissingPlatformAsAWildcard(t *testing.T) {
	adapter := &fakeAdapter{name: "omp", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		return Outcome{Models: []Model{{ID: "m1", Provider: "omp"}}}, nil
	}}
	service := newService(t, AdapterRegistration{Adapter: adapter})
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	if _, err := service.Submit(context.Background(), SubmitInput{
		ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
		Profile: "primary", Kind: KindModels, Adapter: "omp",
		CLIDigest: "d1", ConfigRevision: 1, Platform: "linux/amd64",
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	handler, err := NewAdminHandler(AdminConfig{
		Service:   service,
		CLIs:      &mutableCLIResolver{adapter: "omp", digest: "d1", ok: true},
		Configs:   &mutableConfigResolver{revision: 1, known: true},
		Authorize: func(*http.Request) (bool, bool) { return true, true },
		Actor:     func(*http.Request) string { return "test-ops" },
	})
	if err != nil {
		t.Fatalf("NewAdminHandler: %v", err)
	}
	view := serveDiscoveryAdmin(handler, http.MethodGet,
		"/admin/discovery/models?server_id=atlas&workspace_id=core&runtime_id=rt-omp&profile=primary&adapter=omp", "")
	if view.Code != http.StatusOK || strings.Contains(view.Body.String(), `"available":true`) {
		t.Fatalf("models without a platform = %d %s, want unavailable", view.Code, view.Body.String())
	}
}
