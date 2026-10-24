// TC-tech-concurrency-01 / AC-02 (all three clauses): FOREMAN_MAX_INFLIGHT_JOBS
// bounds the task Jobs Foreman holds at once (cap); every task beyond it stays
// queued on the server and Foreman claims only what fits, prefetching nothing
// (excess queue; 02-contracts.md §10; 01-scope.md NG4); and each task has
// exactly one active Job (uniqueness). The claim batch is pinned to
// n = min(MAX_INFLIGHT − Inflight, CLAIM_BATCH_MAX) on both arms. The scenario
// drives the production claim loop against a server-side queue and reads the
// Job count through the injected JobClient seam, so no cluster is involved.
package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

const (
	concurrency01MaxInflight = 2
	// concurrency01ClaimInterval shortens the production loop's cadence for
	// bounded waits; the batch is the budget's property, not the cadence's.
	concurrency01ClaimInterval = 10 * time.Millisecond
)

// concurrency01Tasks is the TC's trigger batch: five tasks against a two-slot
// cap, so at least three must wait server-side at any instant.
var concurrency01Tasks = []string{"task-1", "task-2", "task-3", "task-4", "task-5"}

func TestTechConcurrency01InflightCapLeavesExcessQueued(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.rebuildScheduler(t, func(cfg *scheduler.Config) {
		cfg.MaxInflightJobs = concurrency01MaxInflight
	})
	for _, taskID := range concurrency01Tasks {
		h.upstream.enqueueTask(taskID)
	}

	maxJobs, maxInflight := sampleConcurrency01(t, h)
	startConcurrency01ClaimLoop(t, h)

	// The cap fills: exactly two of the five tasks are dispatched, the other
	// three stay queued on the server.
	awaitCondition(t, "two tasks in flight", func() bool {
		return h.reg.Inflight() == concurrency01MaxInflight
	})
	awaitCondition(t, "three tasks queued server-side", func() bool {
		return h.upstream.tasksInStatus(taskStatusQueued) == 3
	})
	// The Job face lags the registry count above: OnClaim registers the entry
	// and places the Job a step later, so waiting on Inflight() alone samples
	// a fact the assertion below then re-reads. Wait on the asserted count
	// itself; an overshoot stays the assertion's to report.
	awaitCondition(t, "both claimed tasks' Jobs placed", func() bool {
		return h.liveTaskJobs() >= concurrency01MaxInflight
	})
	if got := h.sched.ClaimBudget(); got != 0 {
		t.Fatalf("ClaimBudget() = %d with %d in flight, want 0", got, h.reg.Inflight())
	}
	if got := h.liveTaskJobs(); got != concurrency01MaxInflight {
		t.Fatalf("live task Jobs = %d, want %d", got, concurrency01MaxInflight)
	}
	if got := h.upstream.tasksInStatus(taskStatusDispatched); got != concurrency01MaxInflight {
		t.Fatalf("server-side dispatched = %d, want %d", got, concurrency01MaxInflight)
	}
	// The empty index yields the whole free window in one request: with
	// n = min(MAX_INFLIGHT − 0, CLAIM_BATCH_MAX) that is 2, not one-at-a-time.
	batches := concurrency01ClaimBatches(t, h)
	if len(batches) != 1 || batches[0] != concurrency01MaxInflight {
		t.Fatalf("claim batches at the empty index = %v, want exactly [%d]", batches, concurrency01MaxInflight)
	}

	// Every task with a Job is one the server dispatched, and every task
	// without one is still queued — the Job face and the server's queue
	// agree task by task.
	for _, taskID := range concurrency01Tasks {
		hasJob := h.jobs.jobCreated("fm-" + taskID)
		status := h.upstream.taskStatus(taskID)
		switch {
		case hasJob && status != taskStatusDispatched:
			t.Fatalf("task %s has a Job but server-side status = %q, want %q", taskID, status, taskStatusDispatched)
		case !hasJob && status != taskStatusQueued:
			t.Fatalf("task %s has no Job but server-side status = %q, want %q", taskID, status, taskStatusQueued)
		}
	}

	// Foreman never prefetches: with no free slot it issues no further claim.
	claimsWhenFull := len(h.upstream.calls("POST", "/api/daemon/tasks/claim"))
	time.Sleep(10 * concurrency01ClaimInterval)
	if got := len(h.upstream.calls("POST", "/api/daemon/tasks/claim")); got != claimsWhenFull {
		t.Fatalf("claim requests grew from %d to %d with no free slot — Foreman prefetched", claimsWhenFull, got)
	}

	// Each settlement frees exactly one slot, and the next claim asks for
	// exactly that many tasks: one. The queue drains one task per completion
	// and the cap never overshoots.
	for h.upstream.tasksInStatus(taskStatusQueued) > 0 {
		queuedBefore := h.upstream.tasksInStatus(taskStatusQueued)
		claimedBefore := h.upstream.tasksInStatus(taskStatusDispatched)
		inflightBefore := h.reg.Inflight()
		batchesBefore := len(concurrency01ClaimBatches(t, h))
		settleOneInflight(t, h)
		awaitCondition(t, "the freed slot is refilled from the server queue", func() bool {
			return h.upstream.tasksInStatus(taskStatusQueued) == queuedBefore-1 &&
				h.upstream.tasksInStatus(taskStatusDispatched) == claimedBefore+1
		})
		awaitCondition(t, "the cap is full again", func() bool {
			return h.reg.Inflight() == concurrency01MaxInflight
		})
		freeAfterSettle := concurrency01MaxInflight - (inflightBefore - 1)
		refill := concurrency01ClaimBatches(t, h)[batchesBefore:]
		if len(refill) != 1 || refill[0] != freeAfterSettle {
			t.Fatalf("after settling one of %d in flight the claim batch = %v, want [%d] (the observed free slots)",
				inflightBefore, refill, freeAfterSettle)
		}
	}

	// Every hand-out must have landed as a Job before the last tasks settle:
	// the server marks a task dispatched when it hands it out, a step before
	// the claim's Job exists, so the queue reads empty while one claim is
	// still mid-flight. An overshoot stays the assertion's to report.
	awaitCondition(t, "every dispatched task's Job placed", func() bool {
		return len(h.createdTaskJobNames()) >= len(concurrency01Tasks)
	})

	// The last two in-flight tasks settle without a refill: the queue is
	// empty, so every task has run and nothing is left waiting.
	for {
		ids := inflightTaskIDs(h)
		if len(ids) == 0 {
			break
		}
		for _, taskID := range ids {
			if err := h.sched.SettleTerminal(t.Context(), taskID, string(registry.ResultCompleted)); err != nil {
				t.Fatalf("SettleTerminal(%s): %v", taskID, err)
			}
		}
	}

	awaitCondition(t, "all five tasks terminal", func() bool {
		if h.reg.Inflight() != 0 {
			return false
		}
		for _, taskID := range concurrency01Tasks {
			entry, ok := h.reg.Get(taskID)
			if !ok || !entry.IsTerminal() {
				return false
			}
		}
		return true
	})
	if got := h.upstream.tasksInStatus(taskStatusQueued); got != 0 {
		t.Fatalf("server-side queued = %d after every task ran, want 0", got)
	}
	if got := h.upstream.tasksInStatus(taskStatusDispatched); got != len(concurrency01Tasks) {
		t.Fatalf("server-side dispatched = %d, want %d", got, len(concurrency01Tasks))
	}

	// One Job per task: the queue drained without a duplicate claim, which is
	// the same-task uniqueness the TC's step 4 samples with jq group_by.
	jobsByTask := h.createdTaskJobNames()
	if len(jobsByTask) != len(concurrency01Tasks) {
		t.Fatalf("distinct task Job names = %d (%v), want %d", len(jobsByTask), jobsByTask, len(concurrency01Tasks))
	}
	for _, taskID := range concurrency01Tasks {
		if n := jobsByTask["fm-"+taskID]; n != 1 {
			t.Fatalf("Job fm-%s created %d times, want exactly 1", taskID, n)
		}
	}

	// The cap held for the whole run, not only at the sampled checkpoints.
	if got := maxJobs.Load(); got > concurrency01MaxInflight {
		t.Fatalf("instantaneous task Jobs peaked at %d, above the cap %d", got, concurrency01MaxInflight)
	}
	if got := maxInflight.Load(); got > concurrency01MaxInflight {
		t.Fatalf("Registry.Inflight peaked at %d, above the cap %d", got, concurrency01MaxInflight)
	}
}

// TestTechConcurrency01ClaimBatchClampBinds pins the second arm of
// n = min(MAX_INFLIGHT − Inflight, CLAIM_BATCH_MAX): with five free slots and
// a batch cap of two, the loop still asks for two at a time. A per-one
// implementation (batches 1,1,1,1,1) and one that drops the batch clamp
// (first batch 5) both fail the exact sequence below.
func TestTechConcurrency01ClaimBatchClampBinds(t *testing.T) {
	const (
		clampMaxInflight = 5
		clampBatchMax    = 2
		clampTasks       = 7
	)
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.rebuildScheduler(t, func(cfg *scheduler.Config) {
		cfg.MaxInflightJobs = clampMaxInflight
		cfg.ClaimBatchMax = clampBatchMax
	})
	taskIDs := make([]string, 0, clampTasks)
	for i := 1; i <= clampTasks; i++ {
		taskID := fmt.Sprintf("task-%d", i)
		taskIDs = append(taskIDs, taskID)
		h.upstream.enqueueTask(taskID)
	}

	maxJobs, _ := sampleConcurrency01(t, h)
	stop := startConcurrency01ClaimLoop(t, h)

	awaitCondition(t, "five tasks in flight", func() bool {
		return h.reg.Inflight() == clampMaxInflight
	})
	if got := h.upstream.tasksInStatus(taskStatusQueued); got != clampTasks-clampMaxInflight {
		t.Fatalf("server-side queued = %d, want %d", got, clampTasks-clampMaxInflight)
	}
	// min(5−0, 2)=2, min(5−2, 2)=2, min(5−4, 2)=1.
	want := []int{2, 2, 1}
	if got := concurrency01ClaimBatches(t, h); !slices.Equal(got, want) {
		t.Fatalf("claim batches = %v, want %v (the batch clamp binds at 2)", got, want)
	}

	// Two slots freed together are claimed in one batch of two. The loop is
	// stopped across the settlements so both land before its next iteration,
	// making the refill batch a property of the budget, not of timing.
	stop()
	settleOneInflight(t, h)
	settleOneInflight(t, h)
	if got := h.sched.ClaimBudget(); got != 2 {
		t.Fatalf("ClaimBudget() = %d with two freed slots, want 2", got)
	}
	startConcurrency01ClaimLoop(t, h)
	awaitCondition(t, "the two freed slots are refilled", func() bool {
		return h.reg.Inflight() == clampMaxInflight
	})
	// The refill's Jobs land a step after the registry rows the wait above
	// covers, and the per-task create counts asserted below are that Job face.
	awaitCondition(t, "every refilled claim's Job placed", func() bool {
		return len(h.createdTaskJobNames()) >= clampTasks
	})
	refill := concurrency01ClaimBatches(t, h)[len(want):]
	if !slices.Equal(refill, []int{2}) {
		t.Fatalf("refill batch = %v, want [2] (two free slots, batch cap 2)", refill)
	}
	if got := h.upstream.tasksInStatus(taskStatusQueued); got != 0 {
		t.Fatalf("server-side queued = %d after the refill, want 0", got)
	}
	if got := h.upstream.tasksInStatus(taskStatusDispatched); got != clampTasks {
		t.Fatalf("server-side dispatched = %d, want %d", got, clampTasks)
	}
	if got := maxJobs.Load(); got > clampMaxInflight {
		t.Fatalf("instantaneous task Jobs peaked at %d, above the cap %d", got, clampMaxInflight)
	}
	jobsByTask := h.createdTaskJobNames()
	for _, taskID := range taskIDs {
		if n := jobsByTask["fm-"+taskID]; n != 1 {
			t.Fatalf("Job fm-%s created %d times, want exactly 1", taskID, n)
		}
	}
}

// startConcurrency01ClaimLoop runs the production claim loop until the
// returned stop function cancels it.
func startConcurrency01ClaimLoop(t *testing.T, h *harness) func() {
	cancel, wait := startConcurrency01ClaimLoopDetached(t, h)
	var once sync.Once
	return func() { once.Do(func() { cancel(); wait() }) }
}

// startConcurrency01ClaimLoopDetached is the loop above with its stop split
// into cancel and wait, for scenarios that pin the exit ordering.
func startConcurrency01ClaimLoopDetached(t *testing.T, h *harness) (cancel func(), wait func()) {
	t.Helper()
	loopCfg := h.cfg
	loopCfg.ClaimInterval = concurrency01ClaimInterval
	loopCfg.ClaimIntervalIdle = concurrency01ClaimInterval
	loop := proxy.NewClaimLoop(loopCfg, h.client, h.sched)
	ctx, cancelCtx := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = loop.Run(ctx)
	}()
	var once sync.Once
	cancel = func() { once.Do(cancelCtx) }
	wait = func() { <-done }
	t.Cleanup(func() { cancel(); wait() })
	return cancel, wait
}

// sampleConcurrency01 records the two independent projections of "in flight"
// for the whole run: the Job objects the injected client holds and the
// registry count the budget is computed from.
func sampleConcurrency01(t *testing.T, h *harness) (maxJobs, maxInflight *atomic.Int64) {
	t.Helper()
	maxJobs = &atomic.Int64{}
	maxInflight = &atomic.Int64{}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if n := int64(h.liveTaskJobs()); n > maxJobs.Load() {
				maxJobs.Store(n)
			}
			if n := int64(h.reg.Inflight()); n > maxInflight.Load() {
				maxInflight.Store(n)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})
	return maxJobs, maxInflight
}

// liveTaskJobs is the task Job count at one instant: created Jobs whose
// delete has not landed yet. Catalog-probe Jobs carry no task and are named
// with the probe prefix, so they never enter this count (the TC's own
// selector excludes them by the task-id label).
func (h *harness) liveTaskJobs() int {
	h.jobs.mu.Lock()
	defer h.jobs.mu.Unlock()
	deleted := make(map[string]bool, len(h.jobs.deletedJobs))
	for _, name := range h.jobs.deletedJobs {
		deleted[name] = true
	}
	live := 0
	for _, job := range h.jobs.createdJobs {
		if strings.HasPrefix(job.Name, "fm-catalog-") || deleted[job.Name] {
			continue
		}
		live++
	}
	return live
}

// createdTaskJobNames counts the task Jobs the injected client created, by
// name: one entry per create, so a duplicate claim would show up as 2.
func (h *harness) createdTaskJobNames() map[string]int {
	h.jobs.mu.Lock()
	defer h.jobs.mu.Unlock()
	counts := make(map[string]int, len(h.jobs.createdJobs))
	for _, job := range h.jobs.createdJobs {
		if strings.HasPrefix(job.Name, "fm-catalog-") {
			continue
		}
		counts[job.Name]++
	}
	return counts
}

// settleOneInflight ends one non-terminal task the way the completion path
// does (terminal write + object delete), freeing exactly one slot. The
// completion protocol itself is another TC's subject.
func settleOneInflight(t *testing.T, h *harness) {
	t.Helper()
	for _, entry := range h.reg.List() {
		if entry.IsTerminal() {
			continue
		}
		if err := h.sched.SettleTerminal(t.Context(), entry.TaskID, string(registry.ResultCompleted)); err != nil {
			t.Fatalf("SettleTerminal(%s): %v", entry.TaskID, err)
		}
		return
	}
	t.Fatal("no in-flight task to settle")
}

// inflightTaskIDs lists the non-terminal tasks in the registry.
func inflightTaskIDs(h *harness) []string {
	var ids []string
	for _, entry := range h.reg.List() {
		if !entry.IsTerminal() {
			ids = append(ids, entry.TaskID)
		}
	}
	return ids
}

// awaitCondition waits for one observable condition of the scenario, failing
// the test when it does not hold within the bounded window.
func awaitCondition(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// concurrency01ClaimBatches reads the max_tasks of every C3 request the loop
// sent, in order.
func concurrency01ClaimBatches(t *testing.T, h *harness) []int {
	t.Helper()
	var batches []int
	for _, req := range h.upstream.calls("POST", "/api/daemon/tasks/claim") {
		var body struct {
			MaxTasks int `json:"max_tasks"`
		}
		if err := json.Unmarshal(req.Body, &body); err != nil {
			t.Fatalf("claim request body %s: %v", req.Body, err)
		}
		batches = append(batches, body.MaxTasks)
	}
	return batches
}
