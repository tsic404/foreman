package proxy_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"github.com/tsic404/foreman/internal/registry"
	"testing"
)

func postWithToken(t *testing.T, url, path, token, body string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+path, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// TestReRegisterAfterDeregisterRestoresOnline pins the symmetry of the
// lifecycle: deregistration takes every runtime offline, and a daemon that
// comes back must be dispatchable again. If C-1 did not reset the flag, a
// restarted daemon would stay invisible until the heartbeat sweep ran and the
// operator would see "registered" with no work arriving.
func TestReRegisterAfterDeregisterRestoresOnline(t *testing.T) {
	srv, reg := newServer(t)
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	runtimes := `[{"name":"worker-1","type":"omp","status":"online"},
	              {"name":"worker-1","type":"codex","status":"online"}]`
	register := func() []string {
		body, _ := json.Marshal(map[string]any{
			"daemon_id": "worker-1", "workspace_id": "ws-1",
			"runtimes": json.RawMessage(runtimes),
		})
		req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/api/daemon/register", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+testToken)
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

	ids := register()
	if len(ids) != 2 {
		t.Fatalf("register returned %d runtimes, want 2", len(ids))
	}

	if code := postWithToken(t, httpSrv.URL, "/api/daemon/deregister", testToken, `{}`); code != http.StatusOK {
		t.Fatalf("deregister status %d", code)
	}
	for _, id := range ids {
		if rt, ok := reg.RuntimeByID(id); !ok || rt.Online {
			t.Fatalf("runtime %s still online after deregister", id)
		}
	}

	// The daemon comes back with the same declaration.
	again := register()
	if len(again) != len(ids) {
		t.Fatalf("re-register returned %d runtimes, want %d", len(again), len(ids))
	}
	for _, id := range again {
		rt, ok := reg.RuntimeByID(id)
		if !ok {
			t.Fatalf("runtime %s missing after re-register", id)
		}
		if !rt.Online {
			t.Fatalf("runtime %s still offline after re-registering", id)
		}
	}
}

// TestHeartbeatRequiresOwnRuntime: a heartbeat may only refresh a runtime the
// presenting credential owns. Otherwise one daemon could keep another's row
// alive — masking a dead daemon from the sweep.
func TestHeartbeatRequiresOwnRuntime(t *testing.T) {
	srv, reg := newServer(t)
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	// A runtime belonging to a different daemon identity.
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "other-rt", DaemonID: "someone-else", WorkspaceID: "ws-1",
		Provider: "omp", Name: "someone-else", Online: true,
	})

	body, _ := json.Marshal(map[string]any{"runtime_id": "other-rt"})
	if code := postWithToken(t, httpSrv.URL, "/api/daemon/heartbeat", testToken, string(body)); code != http.StatusNotFound {
		t.Fatalf("foreign heartbeat = %d, want 404", code)
	}
	if rt, ok := reg.RuntimeByID("other-rt"); !ok || !rt.LastHeartbeatAt.IsZero() {
		t.Fatal("a foreign daemon's runtime heartbeat was refreshed")
	}
}

// TestHeartbeatRejectsMalformedBody: the daemon retries on a 4xx, so a bad
// shape must be refused rather than silently accepted as "some runtime".
func TestHeartbeatRejectsMalformedBody(t *testing.T) {
	srv, _ := newServer(t)
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	for _, body := range []string{`{}`, `{"runtime_id":""}`, `not json`} {
		if code := postWithToken(t, httpSrv.URL, "/api/daemon/heartbeat", testToken, body); code != http.StatusBadRequest {
			t.Fatalf("body %q = %d, want 400", body, code)
		}
	}
}
