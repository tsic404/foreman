// TC-tech-multiws-01 implements AC-81: two workspaces in one deployment run
// their claim → delivery → terminal → reclaim chains side by side, and the
// external projections partition by workspace.
package acceptance

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestTechMultiWS01ParallelChainsDoNotCross(t *testing.T) {
	m := newMultiWS(t, "ws-a", "ws-b")

	entryA := m.claim("ws-a", "task-a1", "rid-a")
	entryB := m.claim("ws-b", "task-b1", "rid-b")
	if entryA.WorkspaceID != "ws-a" || entryB.WorkspaceID != "ws-b" {
		t.Fatalf("entries = %q/%q, want ws-a/ws-b", entryA.WorkspaceID, entryB.WorkspaceID)
	}
	if entryA.DaemonID == entryB.DaemonID {
		t.Fatalf("two workspaces share daemon identity %q (F2)", entryA.DaemonID)
	}

	// The workspace identity is on the Job object, not only in memory.
	for _, e := range []registry.TaskEntry{entryA, entryB} {
		job := jobByWorkspace(t, m, e.JobName, e.WorkspaceID)
		if job == nil {
			t.Fatalf("Job %s not created", e.JobName)
		}
		if got := job.Labels[registry.LabelWorkspaceID]; got != e.WorkspaceID {
			t.Errorf("Job %s workspace label = %q, want %q", e.JobName, got, e.WorkspaceID)
		}
	}

	// Each daemon registers (S4) before delivery is legal.
	for _, e := range []registry.TaskEntry{entryA, entryB} {
		if _, ok := m.reg.MarkDaemonRegistered(e.DaemonID, time.Now()); !ok {
			t.Fatalf("MarkDaemonRegistered(%s) refused", e.DaemonID)
		}
	}

	// Each daemon receives only its own workspace's task; the runtime id is
	// rewritten to its own job runtime.
	for _, e := range []registry.TaskEntry{entryA, entryB} {
		tasks, err := m.sched.Deliver(e.DaemonID, e.JobRuntimeID, 1)
		if err != nil {
			t.Fatalf("Deliver(%s): %v", e.DaemonID, err)
		}
		if len(tasks) != 1 {
			t.Fatalf("Deliver(%s) returned %d tasks, want 1", e.DaemonID, len(tasks))
		}
		var body struct {
			ID          string `json:"id"`
			WorkspaceID string `json:"workspace_id"`
			RuntimeID   string `json:"runtime_id"`
		}
		if err := json.Unmarshal(tasks[0], &body); err != nil {
			t.Fatalf("decode delivered body: %v", err)
		}
		if body.ID != e.TaskID || body.WorkspaceID != e.WorkspaceID || body.RuntimeID != e.JobRuntimeID {
			t.Errorf("delivered body = %+v, want task/workspace/runtime %s/%s/%s",
				body, e.TaskID, e.WorkspaceID, e.JobRuntimeID)
		}
	}

	// Both chains reach terminal independently; each Job is deleted.
	for _, e := range []registry.TaskEntry{entryA, entryB} {
		e, _ = m.reg.Get(e.TaskID)
		code, body, err := m.sched.OnReport(t.Context(), scheduler.EPComplete, e,
			[]byte(`{"status":"completed"}`), "")
		if err != nil || code/100 != 2 {
			t.Fatalf("complete %s = %d/%s err=%v", e.TaskID, code, body, err)
		}
		settled, ok := m.reg.Get(e.TaskID)
		if !ok || !settled.IsTerminal() || settled.Result != registry.ResultCompleted {
			t.Errorf("entry %s after complete = %+v, want terminal/completed", e.TaskID, settled)
		}
	}

	// Projection partition: the list answers one workspace's entries and the
	// single-task route treats another workspace's id as absent (§4.1).
	ops := observability.NewOpsHandler(observability.NewMetrics(), m.reg, nil, observability.OpsConfig{
		WorkspaceIDs: []string{"ws-a", "ws-b"},
	})
	for _, workspaceID := range []string{"ws-a", "ws-b"} {
		req := httptest.NewRequest(http.MethodGet, "/foreman/tasks?workspace="+workspaceID, nil)
		rec := httptest.NewRecorder()
		ops.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("list %s = %d: %s", workspaceID, rec.Code, rec.Body)
		}
		var entries []struct {
			TaskID      string `json:"task_id"`
			WorkspaceID string `json:"workspace_id"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &entries); err != nil {
			t.Fatalf("decode list: %v", err)
		}
		for _, e := range entries {
			if e.WorkspaceID != workspaceID {
				t.Errorf("workspace %s list leaked %s of %s", workspaceID, e.TaskID, e.WorkspaceID)
			}
		}
	}

	// A multi-workspace deployment has no default dimension: an unscoped
	// projection would answer across the partition (§4.1).
	req := httptest.NewRequest(http.MethodGet, "/foreman/tasks", nil)
	rec := httptest.NewRecorder()
	ops.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("unscoped list = %d, want 400", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/foreman/tasks/task-b1?workspace=ws-a", nil)
	rec = httptest.NewRecorder()
	ops.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("cross-workspace single-task = %d, want 404", rec.Code)
	}
}

func jobByWorkspace(t *testing.T, m *multiWS, name, workspaceID string) *batchv1Job {
	t.Helper()
	m.jobs.mu.Lock()
	defer m.jobs.mu.Unlock()
	for _, job := range m.jobs.createdJobs {
		if job.Name == name && job.Labels[registry.LabelWorkspaceID] == workspaceID {
			return job
		}
	}
	return nil
}

// TestTechMultiWS01TerminalReplayUsesPersistedWorkspace covers the restart
// replay face of AC-81/§10: a terminal report's carrier outlives its entry,
// and the workspace persisted with the report — not the index, which no
// longer holds the task — selects the credential. Replaying through another
// workspace's client would cross the partition ADR-047 draws.
func TestTechMultiWS01TerminalReplayUsesPersistedWorkspace(t *testing.T) {
	m := newMultiWS(t, "ws-a", "ws-b")
	e := m.claim("ws-a", "task-a-replay", "rid-a")
	body := []byte(`{"error":"job missing"}`)
	if err := m.pending.Enqueue(t.Context(), e.TaskID, e.JobName, e.DaemonID, e.WorkspaceID,
		scheduler.EPFail, body, scheduler.FailureReasonJobMissing); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// A process exit discards the node-local queue; the carrier is what a
	// restarted process lists.
	restarted, err := scheduler.NewPendingReports(t.TempDir(),
		scheduler.WithPendingReportsCarrier(scheduler.NewSecretCarrierStore(m.jobs)))
	if err != nil {
		t.Fatalf("NewPendingReports(restarted): %v", err)
	}
	if err := restarted.Restore(t.Context()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !restarted.Queued(e.TaskID) {
		t.Fatalf("the carrier did not rebuild the queued report")
	}

	// The credential is reclaimed before the replay lands: the entry is gone,
	// so the queued workspace is the only routing key.
	if err := m.reg.Delete(e.TaskID); err != nil {
		t.Fatalf("reg.Delete: %v", err)
	}
	remaining, err := restarted.Drain(t.Context(), m.sched)
	if err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v; want the report delivered", remaining, err)
	}

	path := "/api/daemon/tasks/" + e.TaskID + "/fail"
	if calls := m.upstream["ws-a"].calls(http.MethodPost, path); len(calls) != 1 {
		t.Errorf("ws-a replay calls = %d, want 1", len(calls))
	}
	if calls := m.upstream["ws-b"].calls(http.MethodPost, path); len(calls) != 0 {
		t.Errorf("ws-b saw %d replay calls, want 0 (cross-workspace credential)", len(calls))
	}
}
