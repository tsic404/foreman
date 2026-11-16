package dispatch_test

import (
	"testing"

	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// workspaceIdleRig builds two dispatchers over one shared registry, mirroring
// main.go: one dispatcher per workspace, all reading the same runtime table.
func workspaceIdleRig(t *testing.T) (wsA, wsB *dispatch.Dispatcher, reg *registry.Registry) {
	t.Helper()
	reg = registry.New()
	mk := func(ws string) *dispatch.Dispatcher {
		client := proxy.NewClient(proxy.Config{
			ServerURL: "http://127.0.0.1:1", WorkspaceID: ws,
			DaemonID: "foreman-" + ws, Version: "test",
		}, "tok")
		return dispatch.New(dispatch.Config{WorkspaceID: ws}, reg, client, nil)
	}
	return mk("ws-a"), mk("ws-b"), reg
}

// TestIdleDaemonsCountsOnlyItsWorkspace is the capacity contract: a claim loop
// asks "how many tasks may I take now", and the answer must be what this
// workspace can actually dispatch. Counting the whole registry would let a
// workspace with no daemon claim work it can never hand out.
func TestIdleDaemonsCountsOnlyItsWorkspace(t *testing.T) {
	wsA, wsB, reg := workspaceIdleRig(t)

	// Two idle daemons in ws-b only.
	for _, id := range []string{"b1", "b2"} {
		reg.PutRuntime(registry.Runtime{
			RuntimeID: id, DaemonID: id, WorkspaceID: "ws-b",
			Provider: "omp", Name: id, Online: true, RegisteredAt: reg.Now(),
		})
	}

	if got := wsB.IdleDaemons(); got != 2 {
		t.Fatalf("ws-b IdleDaemons = %d, want 2 (its own daemons)", got)
	}
	if got := wsA.IdleDaemons(); got != 0 {
		t.Fatalf("ws-a IdleDaemons = %d, want 0 — it has no daemon of its own, so it must claim nothing", got)
	}
}

// TestIdleDaemonsIgnoresBusyAndOffline: the count is "can take a task now".
func TestIdleDaemonsIgnoresBusyAndOffline(t *testing.T) {
	_, wsB, reg := workspaceIdleRig(t)

	reg.PutRuntime(registry.Runtime{
		RuntimeID: "b1", DaemonID: "b1", WorkspaceID: "ws-b",
		Provider: "omp", Name: "b1", Online: true, RegisteredAt: reg.Now(),
	})
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "b2", DaemonID: "b2", WorkspaceID: "ws-b",
		Provider: "omp", Name: "b2", Online: false, RegisteredAt: reg.Now(),
	})
	if got := wsB.IdleDaemons(); got != 1 {
		t.Fatalf("IdleDaemons = %d, want 1 (the offline row does not count)", got)
	}

	// Make the only online daemon busy.
	if err := reg.Put(registry.TaskEntry{
		TaskID: "t1", WorkspaceID: "ws-b", DaemonID: "b1",
		DaemonRuntimeID: "b1", RuntimeID: "srv-b1", State: registry.StateRunning,
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got := wsB.IdleDaemons(); got != 0 {
		t.Fatalf("IdleDaemons = %d with the only daemon busy, want 0", got)
	}
}
