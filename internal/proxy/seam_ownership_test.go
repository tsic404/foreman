package proxy_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tsic404/foreman/internal/daemonauth"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// seamRig builds a daemon face serving two daemons in one workspace, with the
// forwarding seams wired so their arguments can be inspected.
type seamRecorder struct {
	skillTask   string
	receiptTask string
	leaseTask   string
	leaseWS     string
	profileWS   string
}

func seamRig(t *testing.T, rec *seamRecorder) (*registry.Registry, string) {
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
	credA, err := store.Verify("tok-a")
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	reg := registry.New()
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "rt-a", DaemonID: credA.DaemonID, WorkspaceID: "ws-1",
		Provider: "omp", Name: "a", Online: true, RegisteredAt: reg.Now(),
	})
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "rt-b", DaemonID: "worker-b", WorkspaceID: "ws-1",
		Provider: "omp", Name: "b", Online: true, RegisteredAt: reg.Now(),
	})
	// A running task held by daemon B.
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-b", WorkspaceID: "ws-1", DaemonID: "worker-b",
		DaemonRuntimeID: "rt-b", RuntimeID: "srv-rt-b", State: registry.StateRunning,
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	srv := proxy.NewFakeServer(store, reg, idleDispatcher{},
		proxy.WithSkillResolver(func(_ context.Context, taskID string, _ []byte) (int, []byte, error) {
			rec.skillTask = taskID
			return http.StatusOK, []byte(`{"skills":[]}`), nil
		}),
		proxy.WithTaskForwarder(func(_ context.Context, taskID string, _ []byte) (int, []byte, error) {
			rec.receiptTask = taskID
			return http.StatusOK, []byte(`{}`), nil
		}),
		proxy.WithLeaseForwarder(func(_ context.Context, workspaceID, taskID string) (bool, error) {
			rec.leaseWS, rec.leaseTask = workspaceID, taskID
			return true, nil
		}),
		proxy.WithWorkspaceForwarders(
			func(context.Context, string) (int, []byte, error) { return http.StatusOK, []byte(`{}`), nil },
			func(_ context.Context, ws string) (int, []byte, error) {
				rec.profileWS = ws
				return http.StatusOK, []byte(`[]`), nil
			},
		))
	httpSrv := httptest.NewServer(srv)
	t.Cleanup(httpSrv.Close)
	return reg, httpSrv.URL
}

// TestSkillBundleResolveRefusesForeignTask: S-26 resolves under the task's own
// runtime, so a daemon that does not hold the task must not be able to point
// the resolve at it and read the bundle set back.
func TestSkillBundleResolveRefusesForeignTask(t *testing.T) {
	rec := &seamRecorder{}
	_, url := seamRig(t, rec)

	code := postWithToken(t, url, "/api/daemon/runtimes/rt-a/tasks/task-b/skill-bundles/resolve", "tok-a", `{}`)
	if code != http.StatusNotFound {
		t.Fatalf("foreign task resolve = %d, want 404", code)
	}
	if rec.skillTask != "" {
		t.Fatalf("resolver was reached with task %q; it must not run for a foreign task", rec.skillTask)
	}
}

// TestSkillBundleResolveAllowsUnboundTask: after a Foreman restart the registry
// is empty while daemons keep running, and the resolver answers
// ErrUnboundTask → 404 itself. Blocking at the face would break that path.
func TestSkillBundleResolveAllowsUnboundTask(t *testing.T) {
	rec := &seamRecorder{}
	_, url := seamRig(t, rec)

	code := postWithToken(t, url, "/api/daemon/runtimes/rt-a/tasks/unknown/skill-bundles/resolve", "tok-a", `{}`)
	if code != http.StatusOK {
		t.Fatalf("unbound task resolve = %d, want 200 (the resolver decides)", code)
	}
	if rec.skillTask != "unknown" {
		t.Fatalf("resolver saw %q, want the unbound id", rec.skillTask)
	}
}

// TestContextReceiptRefusesForeignTask: posting a receipt for another daemon's
// task would let one daemon forge or overwrite the victim's receipt upstream.
func TestContextReceiptRefusesForeignTask(t *testing.T) {
	rec := &seamRecorder{}
	_, url := seamRig(t, rec)

	code := postWithToken(t, url, "/api/daemon/tasks/task-b/context-receipt", "tok-a", `{"k":1}`)
	if code != http.StatusNotFound {
		t.Fatalf("foreign receipt = %d, want 404", code)
	}
	if rec.receiptTask != "" {
		t.Fatalf("forwarder reached with %q; it must not run for a foreign task", rec.receiptTask)
	}
}

// TestPrepareLeaseRefusesForeignTask: owning a runtime does not authorize
// renewing another daemon's task lease through it.
func TestPrepareLeaseRefusesForeignTask(t *testing.T) {
	rec := &seamRecorder{}
	_, url := seamRig(t, rec)

	code := postWithToken(t, url, "/api/daemon/runtimes/rt-a/tasks/task-b/prepare-lease", "tok-a", `{}`)
	if code != http.StatusNotFound {
		t.Fatalf("foreign lease = %d, want 404", code)
	}
	if rec.leaseTask != "" {
		t.Fatalf("lease forwarder reached with %q; it must not renew a foreign task", rec.leaseTask)
	}
}

// TestRuntimeProfilesUsesCredentialWorkspace: {ws} is caller-controlled, so a
// daemon asking for another workspace's profiles must be refused; and when it
// asks for its own, the forward must carry the credential's workspace.
func TestRuntimeProfilesUsesCredentialWorkspace(t *testing.T) {
	rec := &seamRecorder{}
	_, url := seamRig(t, rec)

	if code := getWithToken(t, url+"/api/daemon/workspaces/ws-other/runtime-profiles", "tok-a"); code != http.StatusNotFound {
		t.Fatalf("foreign workspace = %d, want 404", code)
	}
	if rec.profileWS != "" {
		t.Fatalf("forwarder reached with %q; it must not read another workspace", rec.profileWS)
	}

	if code := getWithToken(t, url+"/api/daemon/workspaces/ws-1/runtime-profiles", "tok-a"); code != http.StatusOK {
		t.Fatalf("own workspace = %d, want 200", code)
	}
	if rec.profileWS != "ws-1" {
		t.Fatalf("forwarded workspace = %q, want ws-1", rec.profileWS)
	}
}

func getWithToken(t *testing.T, url, token string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// TestSettledTaskOwnershipAlsoHeld: after a task settles, the live row is gone
// but the settled index keeps its owner. A daemon that never held the task must
// not be able to resolve bundles for it or post a receipt on it — the settle
// does not transfer ownership.
func TestSettledTaskOwnershipAlsoHeld(t *testing.T) {
	rec := &seamRecorder{}
	reg, url := seamRig(t, rec)

	// Settle task-b as its owner, so the live row disappears and only the
	// retained owner remains.
	if _, _, err := reg.MarkTerminal("task-b", registry.ResultCompleted, "", reg.Now()); err != nil {
		t.Fatalf("settle: %v", err)
	}
	// Reclaim removes the live row; the settled index keeps the owner.
	if err := reg.Delete("task-b"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, live := reg.Get("task-b"); live {
		t.Fatal("precondition: the row should be gone")
	}
	if owner := reg.OwnerOf("task-b"); owner != "worker-b" {
		t.Fatalf("precondition: owner = %q, want worker-b", owner)
	}

	if code := postWithToken(t, url, "/api/daemon/runtimes/rt-a/tasks/task-b/skill-bundles/resolve", "tok-a", `{}`); code != http.StatusNotFound {
		t.Fatalf("foreign resolve on a settled task = %d, want 404", code)
	}
	if rec.skillTask != "" {
		t.Fatalf("resolver reached with %q for a task another daemon owned", rec.skillTask)
	}
	if code := postWithToken(t, url, "/api/daemon/tasks/task-b/context-receipt", "tok-a", `{}`); code != http.StatusNotFound {
		t.Fatalf("foreign receipt on a settled task = %d, want 404", code)
	}
	if rec.receiptTask != "" {
		t.Fatalf("receipt forwarder reached with %q for a task another daemon owned", rec.receiptTask)
	}
}
