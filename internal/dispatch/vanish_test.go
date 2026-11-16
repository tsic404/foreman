package dispatch_test

import (
	"context"
	"testing"

	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// vanishRig builds a dispatcher with a live task bound to an online daemon.
func vanishRig(t *testing.T) (*dispatch.Dispatcher, *registry.Registry) {
	t.Helper()
	reg := registry.New()
	client := proxy.NewClient(proxy.Config{
		ServerURL:   "http://127.0.0.1:1", // nothing here should be contacted
		WorkspaceID: "ws-1",
		DaemonID:    "foreman-ws-1",
		Version:     "test",
	}, "tok")
	disp := dispatch.New(dispatch.Config{}, reg, client, nil)

	reg.PutRuntime(registry.Runtime{
		RuntimeID: "worker-1", DaemonID: "worker-1", WorkspaceID: "ws-1",
		Provider: "omp", Name: "worker-1", Online: true,
	})
	e := registry.TaskEntry{
		TaskID: "task-v", WorkspaceID: "ws-1", DaemonID: "worker-1",
		DaemonRuntimeID: "worker-1", RuntimeID: "srv-rt-1",
		State: registry.StateRunning,
	}
	if err := reg.Put(e); err != nil {
		t.Fatalf("put: %v", err)
	}
	return disp, reg
}

// TestOnTaskVanishedDropsEntryWithoutVerdict: the server deleted the task, so
// there is no result to report — the entry disappears without a terminal mark,
// and the daemon slot is released.
func TestOnTaskVanishedDropsEntryWithoutVerdict(t *testing.T) {
	disp, reg := vanishRig(t)

	if err := disp.OnTaskVanished(context.Background(), "task-v"); err != nil {
		t.Fatalf("vanish: %v", err)
	}

	if _, live := reg.Get("task-v"); live {
		t.Fatal("entry still live after the server deleted the task")
	}
	// Not terminal: no verdict was invented for a task we never judged. The
	// done index keeps the id so a re-dispatch is caught, and the retained
	// owner keeps a later report attributable.
	if !reg.IsDone("ws-1", "task-v") {
		t.Fatal("task not remembered as handled; a redelivery would re-run it")
	}
	if owner := reg.OwnerOf("task-v"); owner != "worker-1" {
		t.Fatalf("OwnerOf = %q, want worker-1 (the daemon that held it)", owner)
	}
	if reg.Inflight() != 0 {
		t.Fatalf("inflight = %d, the daemon slot should be released", reg.Inflight())
	}
}

// TestOnTaskVanishedIsIdempotent: the caller retries on the next reconcile
// round; the second call must be a no-op, not a panic or a metric bump.
func TestOnTaskVanishedIsIdempotent(t *testing.T) {
	disp, _ := vanishRig(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := disp.OnTaskVanished(ctx, "task-v"); err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
	}
}

// TestOnTaskVanishedUnknownTaskIsQuiet: a task Foreman never bound (or already
// forgot after a restart) must not error — the caller treats nil as handled.
func TestOnTaskVanishedUnknownTaskIsQuiet(t *testing.T) {
	disp, _ := vanishRig(t)

	if err := disp.OnTaskVanished(context.Background(), "never-seen"); err != nil {
		t.Fatalf("unknown task errored: %v", err)
	}
}

// TestOnTaskVanishedReleasesDaemonForNewClaims is the operational point of the
// whole path: a vanished task's daemon must be immediately claimable again —
// otherwise one deleted task permanently removes a daemon from the pool until
// restart.
func TestOnTaskVanishedReleasesDaemonForNewClaims(t *testing.T) {
	disp, reg := vanishRig(t)

	if got := disp.IdleDaemons(); got != 0 {
		t.Fatalf("precondition: busy daemon counted idle (%d)", got)
	}
	if err := disp.OnTaskVanished(context.Background(), "task-v"); err != nil {
		t.Fatalf("vanish: %v", err)
	}
	if got := disp.IdleDaemons(); got != 1 {
		t.Fatalf("after vanish IdleDaemons = %d, want 1 (slot released)", got)
	}
	_ = reg
}
