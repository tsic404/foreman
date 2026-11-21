package agentconfig

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testAdminHandler(t *testing.T, service *Service, wide, ok bool) *AdminHandler {
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

func serveConfigAdmin(handler http.Handler, method, target, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	handler.ServeHTTP(rec, request)
	return rec
}

// TestAgentConfigAdminMapsValidationAndProjection pins the write and
// projection statuses without ever returning the submitted content.
func TestAgentConfigAdminMapsValidationAndProjection(t *testing.T) {
	service, err := New(Config{
		Store: NewMemoryStore(),
		Clock: func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	handler := testAdminHandler(t, service, true, true)
	const marker = "fictional-admin-secret"
	write := serveConfigAdmin(handler, http.MethodPut, "/admin/agent-configs/atlas/core/omp", `{
		"profile":"primary","secret_ref":"omp-primary-config","file_name":"config.json",
		"config":{"provider":"omp","token":"`+marker+`"}
	}`)
	if write.Code != http.StatusOK {
		t.Fatalf("write = %d %s", write.Code, write.Body.String())
	}
	if strings.Contains(write.Body.String(), marker) {
		t.Fatalf("write response echoes the content: %s", write.Body.String())
	}
	invalid := serveConfigAdmin(handler, http.MethodPut, "/admin/agent-configs/atlas/core/omp", `{
		"secret_ref":"ref","file_name":"config.json","config":[]
	}`)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid write = %d %s, want 400", invalid.Code, invalid.Body.String())
	}
	projection := serveConfigAdmin(handler, http.MethodGet,
		"/admin/agent-configs/atlas/core/omp/projection?profile=primary&attempt_id=attempt-1", "")
	if projection.Code != http.StatusOK {
		t.Fatalf("projection = %d %s", projection.Code, projection.Body.String())
	}
	if !strings.Contains(projection.Body.String(), `"read_only":true`) {
		t.Fatalf("projection is not read-only: %s", projection.Body.String())
	}
	second := serveConfigAdmin(handler, http.MethodGet,
		"/admin/agent-configs/atlas/core/omp/projection?profile=primary&attempt_id=attempt-2", "")
	if second.Code != http.StatusOK {
		t.Fatalf("second attempt projection = %d, want 200", second.Code)
	}
	audit := serveConfigAdmin(handler, http.MethodGet, "/admin/agent-configs/audit", "")
	if audit.Code != http.StatusOK || strings.Contains(audit.Body.String(), marker) {
		t.Fatalf("audit = %d %s", audit.Code, audit.Body.String())
	}
}

func TestAgentConfigAdminRequiresAWideCredential(t *testing.T) {
	service := testService(t)
	for name, handler := range map[string]*AdminHandler{
		"unauthenticated": testAdminHandler(t, service, false, false),
		"scoped":          testAdminHandler(t, service, false, true),
	} {
		rec := serveConfigAdmin(handler, http.MethodGet, "/admin/agent-configs", "")
		want := http.StatusForbidden
		if name == "unauthenticated" {
			want = http.StatusUnauthorized
		}
		if rec.Code != want {
			t.Errorf("%s = %d, want %d", name, rec.Code, want)
		}
	}
}
