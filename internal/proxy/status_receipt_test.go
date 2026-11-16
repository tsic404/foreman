package proxy_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/daemonauth"
	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// idleDispatcher is enough for the status and receipt routes.
type idleDispatcher struct{}

func (idleDispatcher) OnClaim(context.Context, string, json.RawMessage) error { return nil }
func (idleDispatcher) Deliver(context.Context, string, string, []string, int) (dispatch.Delivery, error) {
	return dispatch.Delivery{}, nil
}
func (idleDispatcher) OnReport(context.Context, dispatch.Endpoint, registry.TaskEntry, []byte, string) (int, []byte, error) {
	return http.StatusOK, nil, nil
}
func (idleDispatcher) OnTaskVanished(context.Context, string) error { return nil }
func (idleDispatcher) OnBootTimeout(context.Context, string) error  { return nil }

// twoDaemonRig builds a daemon face serving two credentials in the same
// workspace, so cross-daemon visibility can be probed on one WS.
func twoDaemonRig(t *testing.T, opts ...proxy.ServerOption) (*registry.Registry, string, string) {
	t.Helper()
	store, err := daemonauth.Load(func(k string) string {
		switch k {
		case daemonauth.EnvDaemonTokens:
			return "worker-1=tok-a,worker-b=tok-b"
		case daemonauth.EnvDaemonTokenWorkspaces:
			return "worker-1=ws-1,worker-b=ws-1"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("creds: %v", err)
	}
	// The first pair's daemon id comes from the expanded placeholder; read it
	// back instead of hard-coding a value the macro decides.
	credA, err := store.Verify("tok-a")
	if err != nil {
		t.Fatalf("verify tok-a: %v", err)
	}
	firstDaemonID := credA.DaemonID
	reg := registry.New()
	// The first credential's daemon id is the expanded placeholder, so the
	// runtime bound to it must carry that same id.
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "rt-a", DaemonID: firstDaemonID, WorkspaceID: "ws-1",
		Provider: "omp", Name: "first", Online: true, RegisteredAt: reg.Now(),
	})
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "rt-b", DaemonID: "worker-b", WorkspaceID: "ws-1",
		Provider: "omp", Name: "worker-b", Online: true, RegisteredAt: reg.Now(),
	})
	httpSrv := httptest.NewServer(proxy.NewFakeServer(store, reg, idleDispatcher{}, opts...))
	t.Cleanup(httpSrv.Close)
	return reg, httpSrv.URL, firstDaemonID
}

func putTask(t *testing.T, reg *registry.Registry, id, daemonID, runtimeID string, state registry.State) {
	t.Helper()
	if err := reg.Put(registry.TaskEntry{
		TaskID: id, WorkspaceID: "ws-1", DaemonID: daemonID,
		DaemonRuntimeID: runtimeID, RuntimeID: "srv-" + runtimeID, State: state,
	}); err != nil {
		t.Fatalf("put %s: %v", id, err)
	}
}

// TestTaskStatusCannotReadAnotherDaemonsTask: C-13 answers one task's status
// to the daemon that holds it. Every other task-scoped route checks the daemon
// binding; a status read that only checks the workspace would let one daemon
// in a shared workspace observe another's progress and result.
func TestTaskStatusCannotReadAnotherDaemonsTask(t *testing.T) {
	reg, url, _ := twoDaemonRig(t)
	putTask(t, reg, "task-b", "worker-b", "rt-b", registry.StateRunning)

	req, _ := http.NewRequest(http.MethodGet, url+"/api/daemon/tasks/task-b/status", nil)
	req.Header.Set("Authorization", "Bearer tok-a") // worker-a's credential
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var out map[string]string
		_ = json.NewDecoder(resp.Body).Decode(&out)
		t.Fatalf("worker-a read worker-b's task status (%v); a task-scoped route must check the daemon binding", out)
	}
}

// TestTaskStatusReportsOwnTask: the owning daemon still gets the status, and
// the value is the lifecycle state before terminal and the result after.
func TestTaskStatusReportsOwnTask(t *testing.T) {
	reg, url, ownerID := twoDaemonRig(t)
	putTask(t, reg, "task-a", ownerID, "rt-a", registry.StateRunning)

	req, _ := http.NewRequest(http.MethodGet, url+"/api/daemon/tasks/task-a/status", nil)
	req.Header.Set("Authorization", "Bearer tok-a")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["status"] != "running" {
		t.Fatalf("status = %q, want running", out["status"])
	}
}

// TestTaskStatusUnknownIs404: an unknown id and another workspace's id answer
// the same way, so the route cannot be used to enumerate task ids.
func TestTaskStatusUnknownIs404(t *testing.T) {
	reg, url, ownerID := twoDaemonRig(t)
	putTask(t, reg, "task-a", ownerID, "rt-a", registry.StateRunning)
	// A task in a workspace this credential cannot see.
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-elsewhere", WorkspaceID: "ws-other", DaemonID: ownerID,
		DaemonRuntimeID: "rt-a", State: registry.StateRunning,
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	for _, id := range []string{"no-such-task", "task-elsewhere"} {
		req, _ := http.NewRequest(http.MethodGet, url+"/api/daemon/tasks/"+id+"/status", nil)
		req.Header.Set("Authorization", "Bearer tok-a")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s = %d, want 404", id, resp.StatusCode)
		}
	}
}

// TestContextReceiptWithoutSeamIs404: the upstream shape for "this server does
// not accept receipts" — the daemon treats it as a one-time capability probe
// and never retries.
func TestContextReceiptWithoutSeamIs404(t *testing.T) {
	reg, url, ownerID := twoDaemonRig(t)
	putTask(t, reg, "task-a", ownerID, "rt-a", registry.StateRunning)

	if code := postWithToken(t, url, "/api/daemon/tasks/task-a/context-receipt", "tok-a", `{}`); code != http.StatusNotFound {
		t.Fatalf("receipt without seam = %d, want 404", code)
	}
}

// TestContextReceiptForwardsVerbatim: with the seam wired the route is a plain
// passthrough — status and body come back as the upstream gave them.
func TestContextReceiptForwardsVerbatim(t *testing.T) {
	reg, url, ownerID := twoDaemonRig(t, proxy.WithTaskForwarder(
		func(_ context.Context, taskID string, body []byte) (int, []byte, error) {
			if taskID != "task-a" {
				t.Errorf("forward saw task %q, want task-a", taskID)
			}
			return http.StatusOK, []byte(`{"accepted":true,"echo":` + string(body) + `}`), nil
		}))
	putTask(t, reg, "task-a", ownerID, "rt-a", registry.StateRunning)

	req, _ := http.NewRequest(http.MethodPost, url+"/api/daemon/tasks/task-a/context-receipt", strings.NewReader(`{"k":1}`))
	req.Header.Set("Authorization", "Bearer tok-a")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 200)
	n, _ := resp.Body.Read(buf)
	body := string(buf[:n])
	if !strings.Contains(body, `"accepted":true`) || !strings.Contains(body, `"k":1`) {
		t.Fatalf("receipt body not forwarded verbatim: %s", body)
	}
}
