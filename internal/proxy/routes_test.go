package proxy_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// testRoutes is every route the daemon face mounts, with a path whose
// placeholders are filled so the mux can match it. The guard runs before any
// routing decision, so these requests never reach a handler body.
var testRoutes = []struct{ method, path string }{
	{"POST", "/api/daemon/register"},
	{"POST", "/api/daemon/deregister"},
	{"POST", "/api/daemon/heartbeat"},
	{"POST", "/api/daemon/tasks/claim"},
	{"GET", "/api/daemon/tasks/t-1/status"},
	{"POST", "/api/daemon/tasks/t-1/start"},
	{"POST", "/api/daemon/tasks/t-1/progress"},
	{"POST", "/api/daemon/tasks/t-1/messages"},
	{"POST", "/api/daemon/tasks/t-1/usage"},
	{"POST", "/api/daemon/tasks/t-1/complete"},
	{"POST", "/api/daemon/tasks/t-1/fail"},
	{"POST", "/api/daemon/tasks/t-1/cancel-ack"},
	{"POST", "/api/daemon/tasks/t-1/session"},
	{"POST", "/api/daemon/tasks/t-1/wait-local-directory"},
	{"POST", "/api/daemon/tasks/t-1/context-receipt"},
	{"POST", "/api/daemon/runtimes/r-1/tasks/t-1/prepare-lease"},
	{"POST", "/api/daemon/runtimes/r-1/tasks/t-1/skill-bundles/resolve"},
	{"POST", "/api/daemon/runtimes/r-1/recover-orphans"},
	{"GET", "/api/daemon/workspaces"},
	{"GET", "/api/daemon/workspaces/ws-1/repos"},
	{"GET", "/api/daemon/workspaces/ws-1/runtime-profiles"},
	{"POST", "/api/tokens/current/renew"},
	{"GET", "/api/workspaces"},
}

// TestEveryDaemonRouteRequiresCredential is the single guard every endpoint
// depends on. A route added without passing through it would be reachable
// unauthenticated, and no per-route test would notice — so this walks the
// whole mount table instead of trusting one example.
func TestEveryDaemonRouteRequiresCredential(t *testing.T) {
	srv, _ := newServer(t)
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	for _, rt := range testRoutes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			for _, header := range []string{"", "Bearer wrong-token"} {
				req, err := http.NewRequest(rt.method, httpSrv.URL+rt.path, bytes.NewReader([]byte(`{}`)))
				if err != nil {
					t.Fatalf("build request: %v", err)
				}
				if header != "" {
					req.Header.Set("Authorization", header)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("do: %v", err)
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusUnauthorized {
					t.Fatalf("auth=%q status = %d, want 401", header, resp.StatusCode)
				}
			}
		})
	}
}

// TestDaemonRoutesDoNotLeakExistenceToUnauthenticated: an unauthenticated call
// must be refused identically whether or not the path or task exists.
func TestDaemonRoutesDoNotLeakExistence(t *testing.T) {
	srv, _ := newServer(t)
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	for _, path := range []string{
		"/api/daemon/tasks/real-task/start",
		"/api/daemon/tasks/no-such-task/start",
		"/api/daemon/nonexistent/route",
	} {
		req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+path, bytes.NewReader([]byte(`{}`)))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s without credential = %d, want 401 (no existence signal)", path, resp.StatusCode)
		}
	}
}

// TestMalformedBearerStillRefused: an Authorization header that is present but
// not a usable bearer credential must be refused, not treated as anonymous.
func TestMalformedBearerStillRefused(t *testing.T) {
	srv, _ := newServer(t)
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	for _, header := range []string{"Bearer", "Bearer ", "Basic xyz", "token abc"} {
		req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/api/daemon/heartbeat", bytes.NewReader([]byte(`{}`)))
		req.Header.Set("Authorization", header)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("header %q = %d, want 401", header, resp.StatusCode)
		}
	}
}

// TestRecoverOrphansAnswersZeroes pins S-18's contract: a long-lived daemon
// reaps its own run directories, so Foreman reports nothing to recover. The
// shape matters because the daemon parses both counters.
func TestRecoverOrphansAnswersZeroes(t *testing.T) {
	srv, _ := newServer(t)
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/api/daemon/runtimes/r-1/recover-orphans", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	body := strings.ReplaceAll(string(raw), " ", "")
	for _, want := range []string{`"orphaned":0`, `"retried":0`} {
		if !strings.Contains(body, want) {
			t.Fatalf("S-18 body missing %s: %s", want, body)
		}
	}
}
