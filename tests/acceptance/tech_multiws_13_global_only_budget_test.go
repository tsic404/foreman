// TC-tech-multiws-13 implements AC-100: with a single budget level — the
// per-workspace cap unset, or set equal to the global cap — cross-workspace
// claim isolation does not hold. One workspace fills the shared pool and holds
// it while another workspace's claim does not continue (no in-flight entry, its
// task stays on the server's queue). Releasing one slot lets the second
// workspace claim exactly one, and both draw from the same pool.
package acceptance

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// multiWS13GlobalCap is the pool size the scenario fills; the per-workspace cap
// forms are 0 (unset) and this value (its largest legal setting).
const multiWS13GlobalCap = 2

// TestTechMultiWS13GlobalOnlyBudget judges the process-local counters. The two
// valuation forms are the scenario's own repetition: the per-workspace level is
// switched off in one and cannot bind first in the other.
func TestTechMultiWS13GlobalOnlyBudget(t *testing.T) {
	for _, form := range multiWS13CapForms() {
		t.Run(form.name, func(t *testing.T) {
			multiWSGlobalOnlyBudget(t, newMultiWS(t, "ws-a", "ws-b"), form.perWorkspaceCap)
		})
	}
}

// TestTechMultiWS13GlobalOnlyBudgetShared judges the same scenario on the
// stage-two counters: the budgets and the reservations are read from and taken
// in the shared store, so the pool's single level is a different code path.
func TestTechMultiWS13GlobalOnlyBudgetShared(t *testing.T) {
	for _, form := range multiWS13CapForms() {
		t.Run(form.name, func(t *testing.T) {
			multiWSGlobalOnlyBudget(t, newMultiWSShared(t, "ws-a", "ws-b"), form.perWorkspaceCap)
		})
	}
}

// multiWS13CapForm is one per-workspace cap setting the scenario repeats.
type multiWS13CapForm struct {
	name            string
	perWorkspaceCap int
}

// multiWS13CapForms is the TC's "repeat once with the per-workspace cap equal
// to the global cap" pair.
func multiWS13CapForms() []multiWS13CapForm {
	return []multiWS13CapForm{
		{name: "per-workspace cap unset", perWorkspaceCap: 0},
		{name: "per-workspace cap equals the global cap", perWorkspaceCap: multiWS13GlobalCap},
	}
}

// multiWSGlobalOnlyBudget drives one fixture (ws-a floods, ws-b queues) through
// the scenario; m is the two-workspace fixture of either counter form.
func multiWSGlobalOnlyBudget(t *testing.T, m *multiWS, perWorkspaceCap int) {
	t.Helper()
	const (
		globalCap = multiWS13GlobalCap
		// queuedClaimWindow spans several claim intervals: long enough that a
		// loop which would claim issues its C3 request.
		queuedClaimWindow = 20 * multiWSClaimInterval
	)
	m.rebuildScheduler(func(cfg *scheduler.Config) {
		cfg.MaxInflightJobs = globalCap
		cfg.MaxInflightJobsPerWorkspace = perWorkspaceCap
	})

	for _, workspaceID := range []string{"ws-a", "ws-b"} {
		m.upstream[workspaceID].setRuntimeID("omp", "", "rid-"+workspaceID)
		m.clients[workspaceID].Declare(registry.Discovery{
			CLIVersion: "0.6.1",
			Runtimes:   []registry.DiscoveredRuntime{{Name: "foreman-" + workspaceID, Type: "omp", Version: "0.6.1"}},
		})
	}
	// A carries exactly the tasks that fill the pool, so its in-flight is this
	// scenario's tasks and nothing waits behind them. B carries two, so
	// "exactly one" after the release is observable: the second must stay on
	// the server's queue.
	aTasks := []string{"task-a1", "task-a2"}
	bTasks := []string{"task-b1", "task-b2"}
	for _, taskID := range aTasks {
		m.upstream["ws-a"].enqueueTaskFor(taskID, "ws-a")
	}
	for _, taskID := range bTasks {
		m.upstream["ws-b"].enqueueTaskFor(taskID, "ws-b")
	}

	// A fills the shared pool through its own production claim loop.
	stopA := m.startClaimLoopFor("ws-a")
	awaitCondition(t, "A fills the pool", func() bool {
		for _, taskID := range aTasks {
			if m.upstream["ws-a"].taskStatus(taskID) != taskStatusDispatched {
				return false
			}
		}
		return true
	})
	awaitCondition(t, "A's claims are registered", func() bool {
		for _, taskID := range aTasks {
			if _, ok := m.reg.Get(taskID); !ok || !m.jobs.jobCreated("fm-"+taskID) {
				return false
			}
		}
		return true
	})
	stopA()

	if got := m.upstream["ws-a"].tasksInStatus(taskStatusQueued); got != 0 {
		t.Fatalf("A has %d queued tasks after filling the pool, want 0 (its in-flight is this scenario's tasks only)", got)
	}
	if got := m.reg.InflightFor("ws-a"); got != globalCap {
		t.Fatalf("A's in-flight = %d, want %d", got, globalCap)
	}
	if got := m.reg.Inflight(); got != globalCap {
		t.Fatalf("global in-flight = %d after A fills the pool, want %d", got, globalCap)
	}
	if got := m.sched.ClaimBudget(); got != 0 {
		t.Fatalf("global claim budget with the pool full = %d, want 0", got)
	}
	// Only the global level is in force: B's own allowance (two free
	// per-workspace slots in the equal-cap form) buys it nothing while A
	// holds every slot.
	if got := m.sched.ClaimBudgetFor("ws-b"); got != 0 {
		t.Fatalf("B claim budget under A's full pool = %d, want 0 (only the global level is in force)", got)
	}
	if m.coord != nil {
		if used, err := m.coord.Slots(t.Context(), ""); err != nil || used != globalCap {
			t.Fatalf("shared global slots = %d (err %v), want %d", used, err, globalCap)
		}
	}

	// B's claim loop runs while the pool is full and must not continue: the
	// server keeps both tasks queued and B registers nothing.
	stopB := m.startClaimLoopFor("ws-b")
	defer stopB()
	awaitCondition(t, "B's claim loop preflighted", func() bool {
		return len(m.upstream["ws-b"].calls("POST", "/api/daemon/register")) > 0
	})
	time.Sleep(queuedClaimWindow)
	if got := m.upstream["ws-b"].tasksInStatus(taskStatusQueued); got != len(bTasks) {
		t.Fatalf("B's queued tasks while A holds the pool = %d, want %d (they must stay on the server's queue)", got, len(bTasks))
	}
	if got := len(m.upstream["ws-b"].calls("POST", "/api/daemon/tasks/claim")); got != 0 {
		t.Fatalf("B issued %d claim requests while the pool was full — its claim continued", got)
	}
	if got := m.reg.InflightFor("ws-b"); got != 0 {
		t.Fatalf("B's in-flight while A holds the pool = %d, want 0", got)
	}
	for _, taskID := range bTasks {
		if _, ok := m.reg.Get(taskID); ok {
			t.Fatalf("entry %s registered while the pool was full", taskID)
		}
		if m.jobs.jobCreated("fm-" + taskID) {
			t.Fatalf("Job fm-%s created while the pool was full", taskID)
		}
	}

	// A's task reaches terminal: one slot of the shared pool is released.
	if _, _, err := m.reg.MarkTerminal("task-a1", registry.ResultCompleted, "", time.Now()); err != nil {
		t.Fatalf("MarkTerminal(task-a1): %v", err)
	}
	if err := m.sched.ReconcileSlots(t.Context()); err != nil {
		t.Fatalf("ReconcileSlots: %v", err)
	}

	awaitCondition(t, "B claims the freed slot", func() bool {
		for _, taskID := range bTasks {
			if m.upstream["ws-b"].taskStatus(taskID) != taskStatusDispatched {
				continue
			}
			if _, ok := m.reg.Get(taskID); ok && m.jobs.jobCreated("fm-"+taskID) {
				return true
			}
		}
		return false
	})
	// A settled task is not re-dispatched by the freed slot, and the second B
	// task must stay queued: one slot means one claim.
	time.Sleep(queuedClaimWindow)
	if got := m.upstream["ws-b"].tasksInStatus(taskStatusDispatched); got != 1 {
		t.Fatalf("B dispatched %d tasks after one slot freed, want exactly 1", got)
	}
	if got := m.upstream["ws-b"].tasksInStatus(taskStatusQueued); got != len(bTasks)-1 {
		t.Fatalf("B's remaining queued tasks = %d, want %d (the pool is full again)", got, len(bTasks)-1)
	}
	claims := m.upstream["ws-b"].calls("POST", "/api/daemon/tasks/claim")
	if len(claims) != 1 {
		t.Fatalf("B issued %d claim requests after the release, want exactly 1", len(claims))
	}
	var claimBody struct {
		MaxTasks int `json:"max_tasks"`
	}
	if err := json.Unmarshal(claims[0].Body, &claimBody); err != nil {
		t.Fatalf("decode B's claim request: %v", err)
	}
	if claimBody.MaxTasks != 1 {
		t.Errorf("B's claim request asked for max_tasks=%d, want 1 (the one free global slot)", claimBody.MaxTasks)
	}
	if got := m.reg.InflightFor("ws-b"); got != 1 {
		t.Fatalf("B's in-flight after its claim = %d, want 1", got)
	}
	if got := m.reg.InflightFor("ws-a"); got != globalCap-1 {
		t.Fatalf("A's in-flight after releasing one slot = %d, want %d", got, globalCap-1)
	}
	if got := m.reg.Inflight(); got != globalCap {
		t.Fatalf("global in-flight after B's claim = %d, want back at %d (one shared pool)", got, globalCap)
	}
	if got := m.sched.ClaimBudget(); got != 0 {
		t.Fatalf("global claim budget after B's claim = %d, want 0", got)
	}
	if got := m.sched.ClaimBudgetFor("ws-b"); got != 0 {
		t.Fatalf("B claim budget with the pool full again = %d, want 0", got)
	}
	if m.coord != nil {
		if used, err := m.coord.Slots(t.Context(), ""); err != nil || used != globalCap {
			t.Fatalf("shared global slots after B's claim = %d (err %v), want %d", used, err, globalCap)
		}
		if perWorkspaceCap > 0 {
			if used, err := m.coord.Slots(t.Context(), "ws-b"); err != nil || used != 1 {
				t.Fatalf("shared ws-b slots after its claim = %d (err %v), want 1", used, err)
			}
		}
	}
}
