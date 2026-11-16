package dispatch_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// concurrentRig builds one dispatcher over a registry holding two daemons in
// the same workspace, each with a task waiting.
func concurrentRig(t *testing.T) (*dispatch.Dispatcher, *registry.Registry) {
	t.Helper()
	reg := registry.New()
	client := proxy.NewClient(proxy.Config{
		ServerURL: "http://127.0.0.1:1", WorkspaceID: "ws-1",
		DaemonID: "foreman-ws-1", Version: "test",
	}, "tok")
	disp := dispatch.New(dispatch.Config{WorkspaceID: "ws-1"}, reg, client, nil)

	for _, d := range []string{"worker-a", "worker-b"} {
		reg.PutRuntime(registry.Runtime{
			RuntimeID: d, DaemonID: d, WorkspaceID: "ws-1",
			Provider: "omp", Name: d, Online: true, RegisteredAt: reg.Now(),
		})
		payload, _ := json.Marshal(map[string]any{"id": "task-" + d, "runtime_id": d})
		if err := reg.Put(registry.TaskEntry{
			TaskID: "task-" + d, WorkspaceID: "ws-1", DaemonID: d,
			DaemonRuntimeID: d, RuntimeID: "srv-" + d,
			State: registry.StateDaemonRegistered, ClaimedAt: reg.Now(),
			Payload: payload,
		}); err != nil {
			t.Fatalf("put %s: %v", d, err)
		}
	}
	return disp, reg
}

// TestConcurrentDeliveriesEachGetOwnTask: two daemons polling at the same
// moment must each receive their own task — not both get the same one, and not
// one get nothing because the other's delivery was mid-flight.
func TestConcurrentDeliveriesEachGetOwnTask(t *testing.T) {
	disp, _ := concurrentRig(t)

	type result struct {
		daemon string
		task   string
	}
	results := make(chan result, 2)

	var wg sync.WaitGroup
	for _, d := range []string{"worker-a", "worker-b"} {
		d := d
		wg.Add(1)
		go func() {
			defer wg.Done()
			delivery, err := disp.Deliver(context.Background(), "ws-1", d, []string{d}, 1)
			if err != nil {
				t.Errorf("%s deliver: %v", d, err)
				return
			}
			if !delivery.Delivered || len(delivery.Tasks) != 1 {
				t.Errorf("%s got delivered=%v tasks=%d", d, delivery.Delivered, len(delivery.Tasks))
				return
			}
			var payload struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(delivery.Tasks[0], &payload)
			results <- result{daemon: d, task: payload.ID}
		}()
	}
	wg.Wait()
	close(results)

	seen := map[string]string{}
	for r := range results {
		if prev, dup := seen[r.task]; dup {
			t.Fatalf("%s and %s both received %s", prev, r.daemon, r.task)
		}
		seen[r.task] = r.daemon
		if want := "task-" + r.daemon; r.task != want {
			t.Fatalf("%s received %s, want %s", r.daemon, r.task, want)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("only %d deliveries completed, want 2", len(seen))
	}
}

// TestDeliverHandsOverOneTaskEvenWhenMoreAreQueued pins the documented batch
// shape: the resident daemon runs one task at a time, so a claim for more than
// one still yields a single task and the rest wait for the next poll.
func TestDeliverHandsOverOneTaskEvenWhenMoreAreQueued(t *testing.T) {
	disp, reg := concurrentRig(t)

	// Queue a second task for worker-a.
	payload, _ := json.Marshal(map[string]any{"id": "task-a2", "runtime_id": "worker-a"})
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-a2", WorkspaceID: "ws-1", DaemonID: "worker-a",
		DaemonRuntimeID: "worker-a", RuntimeID: "srv-worker-a",
		State: registry.StateDaemonRegistered, ClaimedAt: reg.Now(),
		Payload: payload,
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	delivery, err := disp.Deliver(context.Background(), "ws-1", "worker-a", []string{"worker-a"}, 5)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if len(delivery.Tasks) != 1 {
		t.Fatalf("delivered %d tasks for maxTasks=5, want 1", len(delivery.Tasks))
	}
	// The second task is still waiting, not lost.
	if _, ok := reg.Get("task-a2"); !ok {
		t.Fatal("the queued second task vanished")
	}
}

// TestDeliverRefusesForeignRuntime: a claim naming a runtime the daemon does
// not own must be refused, or a task would run on the wrong agent CLI.
func TestDeliverRefusesForeignRuntime(t *testing.T) {
	disp, _ := concurrentRig(t)

	if _, err := disp.Deliver(context.Background(), "ws-1", "worker-a", []string{"worker-b"}, 1); err == nil {
		t.Fatal("accepted a claim naming another daemon's runtime")
	}
}
