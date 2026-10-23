// TC-tech-multiws-05 implements AC-85: the in-flight budget has two levels —
// the global cap and each workspace's own cap. With the per-workspace cap
// active and below the global cap, one workspace's flood never stops another
// workspace's claim.
package acceptance

import (
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

func TestTechMultiWS05TwoLevelBudget(t *testing.T) {
	m := newMultiWS(t, "ws-a", "ws-b")
	m.rebuildScheduler(func(cfg *schedulerConfig) {
		cfg.MaxInflightJobs = 2
		cfg.MaxInflightJobsPerWorkspace = 1
	})

	// A's flood fills A's own cap. B keeps its own free slot even though the
	// global budget is shared.
	m.claim("ws-a", "task-a1", "rid-a")
	if got := m.sched.ClaimBudgetFor("ws-a"); got != 0 {
		t.Errorf("A budget after A's own claim = %d, want 0 (per-workspace cap)", got)
	}
	if got := m.sched.ClaimBudgetFor("ws-b"); got != 1 {
		t.Errorf("B budget under A's flood = %d, want 1 (A must not consume B's share)", got)
	}
	if got := m.sched.ClaimBudget(); got != 1 {
		t.Errorf("global budget = %d, want 1 free of 2", got)
	}

	// B takes its slot; both levels are now exhausted.
	m.claim("ws-b", "task-b1", "rid-b")
	if got := m.sched.ClaimBudget(); got != 0 {
		t.Errorf("global budget with two in flight = %d, want 0", got)
	}
	if got := m.sched.ClaimBudgetFor("ws-a"); got != 0 {
		t.Errorf("A budget with global full = %d, want 0", got)
	}

	// A's verdict releases only A's slot; B stays at its own cap.
	m.reg.MarkTerminal("task-a1", registry.ResultCompleted, "", time.Now())
	if got := m.sched.ClaimBudgetFor("ws-a"); got != 1 {
		t.Errorf("A budget after A's verdict = %d, want 1", got)
	}
	if got := m.sched.ClaimBudgetFor("ws-b"); got != 0 {
		t.Errorf("B budget after A's verdict = %d, want 0 (B is at its own cap)", got)
	}
	if got := m.sched.ClaimBudgetFor("ws-b"); got > m.sched.ClaimBudget() {
		t.Errorf("workspace budget %d exceeds the global budget %d", got, m.sched.ClaimBudget())
	}
}

// TestTechMultiWS05TwoLevelBudgetShared judges the same two-level budget on the
// stage-two form: the counters live in the shared store, so the read face and
// the slot reservation are separate code. With A at its own cap, its budget
// must read zero — the global free slot alone would otherwise read as A's
// share, which is exactly what a lost per-workspace term looks like.
func TestTechMultiWS05TwoLevelBudgetShared(t *testing.T) {
	m := newMultiWSShared(t, "ws-a", "ws-b")
	m.rebuildScheduler(func(cfg *schedulerConfig) {
		cfg.MaxInflightJobs = 2
		cfg.MaxInflightJobsPerWorkspace = 1
	})

	m.claim("ws-a", "task-a1", "rid-a")
	if got := m.sched.ClaimBudgetFor("ws-a"); got != 0 {
		t.Errorf("A budget at its own cap = %d, want 0 (shared per-workspace counter)", got)
	}
	if got := m.sched.ClaimBudgetFor("ws-b"); got != 1 {
		t.Errorf("B budget under A's flood = %d, want 1 (A must not consume B's share)", got)
	}
	if got := m.sched.ClaimBudget(); got != 1 {
		t.Errorf("global budget = %d, want 1 free of 2", got)
	}
}
