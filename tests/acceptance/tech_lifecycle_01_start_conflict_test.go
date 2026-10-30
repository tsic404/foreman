// TC-tech-lifecycle-01 (AC-03, AC-15): the four server-side facts behind an
// S8 409 each converge on their declared branch — running adopts and keeps the
// Job, a terminal status settles to the server's verdict, a deleted task is
// released without a report, and a still-undelivered claim is failed as stale
// — while the daemon always receives the server's original 409.
package acceptance

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// tcLifecycle01Delivered drives one task to the delivered state through the
// daemon surface: the claim builds the Job, its daemon registers, S6 hands the
// payload over and leaves the entry delivering.
func tcLifecycle01Delivered(t *testing.T, h *harness, taskID string) string {
	t.Helper()
	if err := h.sched.OnClaim(t.Context(), deliveryPayload(taskID)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, taskID)
	registerTaskDaemon(t, h, taskID, token)
	claimTask(t, h, taskID, token)
	if e, ok := h.reg.Get(taskID); !ok || e.State != registry.StateDelivering {
		t.Fatalf("entry = %+v (%v), want the delivered state", e, ok)
	}
	return token
}

// tcLifecycle01StartConflict sends the daemon's S8 start to a server that
// answers 409 and returns the entry the branch left behind. The daemon keeps
// the server's own answer (§2.5).
func tcLifecycle01StartConflict(t *testing.T, h *harness, taskID, token string) registry.TaskEntry {
	t.Helper()
	h.server.setForwardCode(http.StatusConflict)
	code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/start", token, []byte(`{}`))
	if code != http.StatusConflict {
		t.Fatalf("S8 = %d (%s), want the server's 409 passed through", code, body)
	}
	if got := len(h.server.forwarded(scheduler.EPStart)); got != 1 {
		t.Fatalf("start forwards = %d, want the daemon's one start", got)
	}
	e, ok := h.reg.Get(taskID)
	if !ok {
		t.Fatalf("entry %s disappeared while the 409 branch converged", taskID)
	}
	return e
}

// tcLifecycle01AssertNoRedispatch asserts the branch ended the dispatch loop:
// the server re-dispatching the same task builds no second Job.
func tcLifecycle01AssertNoRedispatch(t *testing.T, h *harness, taskID string) {
	t.Helper()
	if err := h.sched.OnClaim(t.Context(), deliveryPayload(taskID)); err != nil {
		t.Fatalf("re-claim of %s: %v", taskID, err)
	}
	if n := h.jobs.jobCreatedCount("fm-" + taskID); n != 1 {
		t.Errorf("Jobs created for %s = %d, want the original one only (no dispatch loop)", taskID, n)
	}
}

// tcLifecycle01AssertNoTerminalReport asserts the branch sent no terminal
// report of its own: the server's facts decide, Foreman never invents one.
func tcLifecycle01AssertNoTerminalReport(t *testing.T, h *harness) {
	t.Helper()
	for _, ep := range []scheduler.Endpoint{scheduler.EPComplete, scheduler.EPFail, scheduler.EPCancelAck} {
		if forwarded := h.server.forwarded(ep); len(forwarded) != 0 {
			t.Errorf("%s forwards = %+v, want none (no false terminal)", ep, forwarded)
		}
	}
}

// tcLifecycle01AssertNoTerminalMetric asserts no verdict was written at all.
func tcLifecycle01AssertNoTerminalMetric(t *testing.T, h *harness) {
	t.Helper()
	for _, result := range []string{"failed", "completed", "cancelled"} {
		if n := h.metrics.terminalCount(result); n != 0 {
			t.Errorf("terminal metric %s = %d, want none", result, n)
		}
	}
}

// Face 1 — running: a start whose 2xx was lost (transient failure → daemon
// retry) reaches a task the server already runs. Adopting is the only safe
// branch: failing here would kill a live agent, so the start fact is recorded
// and the Job survives (顺序规则 3/4).
func TestLifecycle01StartConflictRunningAdopts(t *testing.T) {
	h := newDeliveryHarness(t)
	token := tcLifecycle01Delivered(t, h, "task-1")
	h.server.setStatus("task-1", "running")

	e := tcLifecycle01StartConflict(t, h, "task-1", token)
	if e.StartedAt.IsZero() {
		t.Error("started_at = zero, want the C13 observation instant")
	}
	if e.IsTerminal() {
		t.Errorf("entry settled %s/%s, want the adopted non-terminal entry", e.State, e.Result)
	}
	if e.State != registry.StateDelivering {
		t.Errorf("state = %s, want delivering (adoption is an observation, not a report)", e.State)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job of an adopted running task was deleted")
	}
	tcLifecycle01AssertNoTerminalReport(t, h)
	tcLifecycle01AssertNoTerminalMetric(t, h)
	tcLifecycle01AssertNoRedispatch(t, h, "task-1")
}

// Face 2 — terminal: the server already holds the verdict, so the 409 settles
// the entry to that verdict and nothing is reported again (§4.2).
func TestLifecycle01StartConflictTerminalSettlesToServer(t *testing.T) {
	h := newDeliveryHarness(t)
	token := tcLifecycle01Delivered(t, h, "task-1")
	h.server.setStatus("task-1", "completed")

	e := tcLifecycle01StartConflict(t, h, "task-1", token)
	if e.State != registry.StateTerminal || e.Result != registry.ResultCompleted {
		t.Fatalf("entry = %s/%s, want terminal/completed (the server's verdict)", e.State, e.Result)
	}
	if !e.IsDraining() {
		t.Error("entry did not enter drain after settling to the server's verdict")
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job was not deleted after settling to the server's verdict")
	}
	if n := h.metrics.terminalCount("completed"); n != 1 {
		t.Errorf("terminal metric completed = %d, want one (the server's result)", n)
	}
	tcLifecycle01AssertNoTerminalReport(t, h)
	tcLifecycle01AssertNoRedispatch(t, h, "task-1")
}

// Face 3 — the server deleted the task: the branch applies the vanish
// disposition — terminal with no result written, the Job deleted, no report
// for a task nobody holds (§4.6).
func TestLifecycle01StartConflictVanishedReleases(t *testing.T) {
	h := newDeliveryHarness(t)
	token := tcLifecycle01Delivered(t, h, "task-1")
	h.server.setStatusDeleted("task-1")

	e := tcLifecycle01StartConflict(t, h, "task-1", token)
	if e.State != registry.StateTerminal {
		t.Fatalf("state = %s, want terminal after the vanish disposition", e.State)
	}
	if e.Result != "" || e.FailureReason != "" {
		t.Fatalf("entry result/reason = %q/%q, want the no-result verdict", e.Result, e.FailureReason)
	}
	if !e.IsDraining() {
		t.Error("entry did not enter drain after the vanish disposition")
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job of the vanished task was not deleted")
	}
	tcLifecycle01AssertNoTerminalReport(t, h)
	tcLifecycle01AssertNoTerminalMetric(t, h)
	tcLifecycle01AssertNoRedispatch(t, h, "task-1")
}

// Face 4 — still undelivered (queued / dispatched): the claim the start was
// built on is gone, so the task is failed on the daemon's behalf with the
// stale-claim attribution; the Job and its credential go with it.
func TestLifecycle01StartConflictBeforeDeliveryFailsStaleClaim(t *testing.T) {
	for _, status := range []string{"queued", taskStatusDispatched} {
		t.Run(status, func(t *testing.T) {
			h := newDeliveryHarness(t)
			token := tcLifecycle01Delivered(t, h, "task-1")
			h.server.setStatus("task-1", status)

			e := tcLifecycle01StartConflict(t, h, "task-1", token)
			if e.State != registry.StateTerminal || e.Result != registry.ResultFailed {
				t.Fatalf("entry = %s/%s, want terminal/failed", e.State, e.Result)
			}
			if e.FailureReason != scheduler.FailureReasonStaleClaim {
				t.Errorf("failure_reason = %q, want %q", e.FailureReason, scheduler.FailureReasonStaleClaim)
			}
			if !e.IsDraining() {
				t.Error("entry did not enter drain after the stale-claim failure")
			}
			if !h.jobs.jobDeleted("fm-task-1") {
				t.Error("the Job was not deleted after the stale-claim failure")
			}
			fails := h.server.forwarded(scheduler.EPFail)
			if len(fails) != 1 || !bytes.Contains(fails[0].Body, []byte(`"failure_reason":"stale_claim"`)) {
				t.Errorf("fail forwards = %+v, want one stale_claim report", fails)
			}
			if n := h.metrics.terminalCount("failed"); n != 1 {
				t.Errorf("terminal metric failed = %d, want one", n)
			}
			// The credential follows the Job at reclaim time (§6.4).
			if err := h.sched.OnJobTerminated(t.Context(), "fm-task-1",
				scheduler.JobTerminationReasonJobTerminated, time.Now(), 0); err != nil {
				t.Fatalf("OnJobTerminated: %v", err)
			}
			if !h.jobs.secretDeleted("fm-task-1-cred") {
				t.Errorf("deleted Secrets = %v, want fm-task-1-cred", h.jobs.deletedSecretNames())
			}
			tcLifecycle01AssertNoRedispatch(t, h, "task-1")
		})
	}
}
