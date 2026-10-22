// TC-tech-multi-07 implements AC-75's queue face: a task is enqueued once, a
// concurrent/alternating daemon take on two instances yields exactly one
// delivery, an unconfirmed take returns the row to the takeable state after
// the confirmation window, and a start fact moves the row out of the queue.
package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

func TestTechMulti07SingleEnqueueSingleDispatch(t *testing.T) {
	window := 300 * time.Millisecond
	tw := newTwoInstances(t, twoInstanceOptions{deliveryWindow: window})
	tw.declareAll(t)
	tw.a.startLeader(t)

	tw.a.upstream.enqueueTask("task-7")
	tw.startClaims(t)
	tw.awaitClaimedJob(t, "task-7")

	// §5 单次入队: exactly one queue row, and a repeated claim of the same
	// task does not create a second one.
	tw.waitQueueDepth(t, 1)
	if err := tw.a.sched.OnClaimFor(context.Background(), "ws-1", deliveryPayload("task-7")); err != nil {
		t.Fatalf("duplicate claim: %v", err)
	}
	tw.waitQueueDepth(t, 1)

	token := tw.token(t, tw.a, "task-7")
	tw.registerVia(t, tw.a, "task-7", token)

	// §5 单次派发: two instances take at the same instant; exactly one wins.
	first, second := tw.concurrentClaims(t, "task-7", token)
	if len(first)+len(second) != 1 {
		e, _ := tw.entry(t, "task-7")
		t.Fatalf("concurrent takes delivered %d+%d tasks, want exactly one; first=%s second=%s row_deliveries=%d row_delivered_at=%v row_state=%s",
			len(first), len(second), first, second, e.Deliveries, e.DeliveredAt, e.State)
	}
	winner := first
	loserHarness := tw.b
	if len(second) == 1 {
		winner = second
		loserHarness = tw.a
	}
	// The row stays in the queue (the delivery is not confirmed yet).
	tw.waitQueueDepth(t, 1)

	// The confirmation window passes with no start fact: the take lease
	// expires and the daemon's next claim — routed to the other instance —
	// re-takes the same row with the same payload.
	time.Sleep(window + multiSharedWindowPad)
	redelivered := tw.claimVia(t, loserHarness, "task-7", token)
	if len(redelivered) != 1 {
		t.Fatalf("re-take delivered %d tasks, want the released payload once", len(redelivered))
	}
	if got, want := deliveryBodyComparable(t, redelivered[0]), deliveryBodyComparable(t, winner[0]); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("re-delivered body = %v, want the first delivery's %v", got, want)
	}
	entry, ok := tw.entry(t, "task-7")
	if !ok || entry.Deliveries != 2 {
		t.Fatalf("shared row deliveries = %+v, want 2 commits", entry.Deliveries)
	}

	// The start fact confirms the delivery: the row leaves the queue and is
	// never dispatched again (§5 确认 ⇒ 条目定论、移出队列).
	tw.startVia(t, tw.a, "task-7", token)
	tw.waitQueueDepth(t, 0)
	if again := tw.claimVia(t, tw.b, "task-7", token); len(again) != 0 {
		t.Fatalf("post-confirmation claim delivered %d tasks, want none", len(again))
	}
	entry, ok = tw.entry(t, "task-7")
	if !ok || entry.StartedAt.IsZero() || entry.State != registry.StateRunning {
		t.Fatalf("shared row after start = %+v, want running with a start fact", entry)
	}
}

// waitQueueDepth blocks until the shared queue holds n rows.
func (tw *twoInstances) waitQueueDepth(t *testing.T, n int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		depth, err := tw.coord.QueueDepth(context.Background())
		if err == nil && depth == n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	depth, _ := tw.coord.QueueDepth(context.Background())
	t.Fatalf("queue depth = %d, want %d", depth, n)
}

// concurrentClaims fires one S-6 request through each instance at the same
// moment. The two bodies are the delivered task lists.
func (tw *twoInstances) concurrentClaims(t *testing.T, taskID, token string) ([]json.RawMessage, []json.RawMessage) {
	t.Helper()
	// s6 performs one claim and returns the raw task list (nil on failure).
	s6 := func(h *harness) ([]json.RawMessage, error) {
		e, ok := h.reg.GetInWorkspace("ws-1", taskID)
		if !ok {
			return nil, fmt.Errorf("entry %s missing", taskID)
		}
		body, err := json.Marshal(map[string]any{
			"daemon_id":   "fm-" + taskID,
			"runtime_ids": []string{e.JobRuntimeID},
			"max_tasks":   4,
		})
		if err != nil {
			return nil, err
		}
		code, raw := h.call("POST", "/api/daemon/tasks/claim", token, body)
		if code != 200 {
			return nil, fmt.Errorf("S6 = %d (%s)", code, raw)
		}
		var resp struct {
			Tasks []json.RawMessage `json:"tasks"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			return nil, err
		}
		return resp.Tasks, nil
	}
	results := make([][]json.RawMessage, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(2)
	for i, h := range []*harness{tw.a, tw.b} {
		go func(i int, h *harness) {
			defer wg.Done()
			<-start
			results[i], errs[i] = s6(h)
		}(i, h)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent claim %d: %v", i, err)
		}
	}
	if len(results[0]) > 1 || len(results[1]) > 1 {
		t.Fatalf("one take delivered %d/%d tasks, want at most one per instance", len(results[0]), len(results[1]))
	}
	return results[0], results[1]
}
