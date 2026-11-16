package dispatch_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// TestReconcileSkipsForeignWorkspaceLeases counts the actual C-4 calls a
// workspace-A reconciler makes: walking the shared task table, it must not ask
// the server about workspace B's lease, because its client is bound to A.
func TestReconcileSkipsForeignWorkspaceLeases(t *testing.T) {
	var leaseCalls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "prepare-lease") {
			atomic.AddInt32(&leaseCalls, 1)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	reg := registry.New()
	client := proxy.NewClient(proxy.Config{
		ServerURL: srv.URL, WorkspaceID: "ws-a", DaemonID: "foreman-a", Version: "test",
	}, "tok")
	disp := dispatch.New(dispatch.Config{WorkspaceID: "ws-a"}, reg, client, nil)

	reg.PutRuntime(registry.Runtime{
		RuntimeID: "b1", DaemonID: "b1", WorkspaceID: "ws-b",
		Provider: "omp", Name: "b1", Online: true, RegisteredAt: reg.Now(),
	})
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-b", WorkspaceID: "ws-b", DaemonID: "b1",
		DaemonRuntimeID: "b1", RuntimeID: "srv-b1",
		State: registry.StateDelivering, ClaimedAt: reg.Now(),
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	if err := disp.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if n := atomic.LoadInt32(&leaseCalls); n != 0 {
		t.Fatalf("workspace-A reconciler issued %d lease call(s) for workspace-B's task", n)
	}
}
