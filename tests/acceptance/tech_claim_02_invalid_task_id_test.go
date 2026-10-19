// TC-tech-claim-02 (AC-65): a claim whose task id cannot compose a legal Job
// name must still leave a reclaimable mapping entry. The name face is
// template-expressed (ADR-034), so the render-time final check can fail for a
// legal but unusable task id (the invalid_task_id path). The entry is settled
// terminal + draining without any object, and the only path that ever reclaims
// such an entry is the Job-name lookup — an empty JobName would leave the
// registry's byJob index without a key and leak the row forever. The
// identity-derived names are not legal object names here, so the reclaim must
// not issue Secret/Job reads for them either. This drives the real builder,
// because the canned one's ResolveName always succeeds and cannot exercise the
// face.
package acceptance

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestClaimInvalidTaskIDLeavesReclaimableEntry(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)

	// An uppercase task id is legal as a payload id but renders a name that is
	// not DNS-1123 — the template's format is validated at startup, the value
	// only at render time.
	payload := json.RawMessage(`{
		"id": "TASK-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1"
	}`)
	err := h.sched.OnClaim(t.Context(), payload)
	if err == nil {
		t.Fatal("OnClaim accepted an id that cannot compose a legal Job name")
	}
	if !errors.Is(err, jobbuilder.ErrInvalidTaskID) {
		t.Fatalf("OnClaim error = %v, want ErrInvalidTaskID", err)
	}

	h.jobs.mu.Lock()
	createdJobs, createdSecrets := len(h.jobs.createdJobs), len(h.jobs.createdSecrets)
	h.jobs.mu.Unlock()
	if createdJobs != 0 || createdSecrets != 0 {
		t.Fatalf("objects created for an unresolvable name: jobs=%d secrets=%d", createdJobs, createdSecrets)
	}

	entry, ok := h.reg.Get("TASK-1")
	if !ok {
		t.Fatal("entry missing: the failed claim must stay queryable until it is reclaimed")
	}
	if entry.State != registry.StateTerminal || entry.Result != registry.ResultFailed {
		t.Fatalf("entry = %+v, want terminal/failed", entry)
	}
	if entry.FailureReason != scheduler.FailureReasonInvalidTaskID {
		t.Errorf("failure_reason = %q, want %q", entry.FailureReason, scheduler.FailureReasonInvalidTaskID)
	}
	// The regression: the entry must be indexed under a non-empty Job name, and
	// that name is the identity name — no object exists to read one from.
	if entry.JobName == "" {
		t.Fatal("empty JobName: the entry can never be found by its Job name")
	}
	if entry.JobName != entry.DaemonID || entry.DaemonID != "fm-TASK-1" {
		t.Errorf("JobName/DaemonID = %q/%q, want the identity name fm-TASK-1", entry.JobName, entry.DaemonID)
	}
	if indexed, ok := h.reg.ByJob(entry.JobName); !ok || indexed.TaskID != "TASK-1" {
		t.Fatalf("ByJob(%q) = %+v/%v, want the entry", entry.JobName, indexed, ok)
	}

	// The server is told why, on the fail endpoint (C10).
	forwards := h.server.forwarded(scheduler.EPFail)
	if len(forwards) != 1 {
		t.Fatalf("fail forwards = %d, want one", len(forwards))
	}
	var body map[string]string
	if err := json.Unmarshal(forwards[0].Body, &body); err != nil {
		t.Fatalf("fail body: %v", err)
	}
	if body["failure_reason"] != scheduler.FailureReasonInvalidTaskID {
		t.Errorf("forwarded failure_reason = %q, want %q", body["failure_reason"], scheduler.FailureReasonInvalidTaskID)
	}

	// The drain path finds the entry by that name and reclaims it; the terminal
	// record survives in the done index. The identity-derived names are not
	// legal object names here, so the reclaim must not issue any Secret/Job
	// read or delete for them.
	h.jobs.mu.Lock()
	getsBefore, jobDeletesBefore, secretDeletesBefore := len(h.jobs.getJobs), len(h.jobs.deletedJobs), len(h.jobs.deletedSecrets)
	h.jobs.mu.Unlock()
	if err := h.sched.OnJobTerminated(t.Context(), entry.JobName,
		scheduler.JobTerminationReasonJobTerminated, time.Time{}, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	h.jobs.mu.Lock()
	gets, jobDeletes, secretDeletes := len(h.jobs.getJobs), len(h.jobs.deletedJobs), len(h.jobs.deletedSecrets)
	h.jobs.mu.Unlock()
	if gets != getsBefore || jobDeletes != jobDeletesBefore || secretDeletes != secretDeletesBefore {
		t.Fatalf("objectless reclaim touched K8s objects: GetJob %d→%d, DeleteJob %d→%d, DeleteSecret %d→%d",
			getsBefore, gets, jobDeletesBefore, jobDeletes, secretDeletesBefore, secretDeletes)
	}
	if _, ok := h.reg.Get("TASK-1"); ok {
		t.Fatal("entry survived the reclaim")
	}
	if !h.reg.IsDone("TASK-1") {
		t.Fatal("terminal record lost by the reclaim")
	}
}
