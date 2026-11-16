package observability_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/observability"
)

func opsServer(t *testing.T, cfg observability.OpsConfig) *httptest.Server {
	t.Helper()
	cfg.Credentials = []observability.OpsCredential{{Token: "ops-tok"}}
	h := observability.NewOpsHandler(nil, nil, cfg)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url, token string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 512)
	n, _ := resp.Body.Read(buf)
	return resp.StatusCode, string(buf[:n])
}

// Every ops route must be reachable and must refuse an unauthenticated call.
func TestOpsSurfaceRequiresCredential(t *testing.T) {
	srv := opsServer(t, observability.OpsConfig{Version: "test"})
	for _, path := range []string{"/foreman/healthz", "/foreman/version", "/foreman/tasks"} {
		if code, _ := get(t, srv.URL+path, ""); code != http.StatusUnauthorized {
			t.Errorf("%s without token = %d, want 401", path, code)
		}
		if code, _ := get(t, srv.URL+path, "wrong"); code != http.StatusUnauthorized {
			t.Errorf("%s with wrong token = %d, want 401", path, code)
		}
		if code, _ := get(t, srv.URL+path, "ops-tok"); code != http.StatusOK {
			t.Errorf("%s with token = %d, want 200", path, code)
		}
	}
}

// A nil RegisterSource keeps the route and answers 404; a wired one reports the
// discovery view. Losing this wiring (as the v2 rewrite did) makes the route a
// permanent 404 that no operator can use.
func TestRegisterSourceRoute(t *testing.T) {
	srv := opsServer(t, observability.OpsConfig{Version: "test"})
	if code, _ := get(t, srv.URL+"/foreman/register-source", "ops-tok"); code != http.StatusNotFound {
		t.Fatalf("nil source = %d, want 404", code)
	}

	srv2 := opsServer(t, observability.OpsConfig{
		Version: "test",
		RegisterSource: func() map[string]any {
			return map[string]any{"ws-1": []any{map[string]any{"runtime_id": "rt-1"}}}
		},
	})
	code, body := get(t, srv2.URL+"/foreman/register-source", "ops-tok")
	if code != http.StatusOK {
		t.Fatalf("source = %d, want 200", code)
	}
	if !strings.Contains(body, "ws-1") || !strings.Contains(body, "rt-1") {
		t.Fatalf("body = %s, want the discovery view", body)
	}
}

// A stopped component must fail the ops healthz. Without the Health field wired
// the ops face answers the "always ok" shape, contradicting the daemon face.
func TestOpsHealthzReflectsStoppedComponent(t *testing.T) {
	h := observability.NewHealth()
	stop := h.Running("claim-loop")

	srv := opsServer(t, observability.OpsConfig{Version: "test", Health: h})
	if code, body := get(t, srv.URL+"/foreman/healthz", "ops-tok"); code != http.StatusOK {
		t.Fatalf("healthy = %d (%s), want 200", code, body)
	}

	stop()
	code, body := get(t, srv.URL+"/foreman/healthz", "ops-tok")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("after stop = %d (%s), want 503", code, body)
	}
	if !strings.Contains(body, "claim-loop") {
		t.Fatalf("body = %s, want the stopped component named", body)
	}
}
