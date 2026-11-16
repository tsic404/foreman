package proxy_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// registerDaemonWith posts an S-1 registration declaring the given runtimes
// and returns the daemon ids the registry assigned.
func registerDaemonWith(t *testing.T, srv *proxy.FakeServer, url, token, daemonID string, runtimes []map[string]string) []string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"daemon_id": daemonID, "workspace_id": "ws-1", "runtimes": runtimes,
	})
	req, _ := http.NewRequest(http.MethodPost, url+"/api/daemon/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register status %d", resp.StatusCode)
	}
	var out struct {
		Runtimes []struct {
			ID string `json:"id"`
		} `json:"runtimes"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	ids := make([]string, 0, len(out.Runtimes))
	for _, rt := range out.Runtimes {
		ids = append(ids, rt.ID)
	}
	return ids
}

// TestDeregisterMarksEveryDaemonRuntimeOffline: a daemon that declared several
// runtimes deregisters as one identity, so every one of its runtimes must go
// offline. Marking only the first leaves the rest looking online until the
// heartbeat sweep notices — long enough for dispatch to hand them a task the
// daemon can no longer start.
func TestDeregisterMarksEveryDaemonRuntimeOffline(t *testing.T) {
	srv, reg := newServer(t)
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	ids := registerDaemonWith(t, srv, httpSrv.URL, testToken, "worker-1", []map[string]string{
		{"name": "worker-1", "type": "omp", "status": "online"},
		{"name": "worker-1", "type": "codex", "status": "online"},
	})
	if len(ids) != 2 {
		t.Fatalf("registered %d runtimes, want 2", len(ids))
	}

	req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/api/daemon/deregister", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("deregister: %v", err)
	}
	resp.Body.Close()

	for _, id := range ids {
		rt, ok := reg.RuntimeByID(id)
		if !ok {
			t.Fatalf("runtime %s vanished from the registry", id)
		}
		if rt.Online {
			t.Fatalf("runtime %s still online after its daemon deregistered", id)
		}
	}
}

// TestDeregisterOnlyTouchesItsOwnDaemon: deregistration is per credential, so
// another daemon's runtimes must be untouched.
func TestDeregisterOnlyTouchesItsOwnDaemon(t *testing.T) {
	srv, reg := newServer(t)
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	// The second credential belongs to worker-b.
	mine := registerDaemonWith(t, srv, httpSrv.URL, testToken, "worker-1", []map[string]string{
		{"name": "worker-1", "type": "omp", "status": "online"},
	})
	if len(mine) != 1 {
		t.Fatalf("registered %d, want 1", len(mine))
	}
	// Another daemon, registered directly into the registry (a different
	// credential path is not needed to prove isolation of the sweep).
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "other-rt", DaemonID: "worker-b", WorkspaceID: "ws-1",
		Provider: "omp", Name: "worker-b", Online: true, RegisteredAt: reg.Now(),
	})

	req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/api/daemon/deregister", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("deregister: %v", err)
	}
	resp.Body.Close()

	if rt, ok := reg.RuntimeByID("other-rt"); !ok || !rt.Online {
		t.Fatal("deregistering one daemon marked another daemon's runtime offline")
	}
}
