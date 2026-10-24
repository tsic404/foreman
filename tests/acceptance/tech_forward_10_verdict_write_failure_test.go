// TC-tech-forward-10 (AC-16) drives the write-ahead order's failure face: the
// terminal report is queued before the verdict is written, so a verdict write
// that fails withdraws the entry it was written ahead of. The caller sees the
// error, the queue and the carrier hold nothing afterwards, and the recovery
// drain never forwards a report this instance never decided — the daemon's
// retry is what carries it once the store recovers.
package acceptance

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
	"github.com/tsic404/foreman/internal/state"
)

// verdictFailingStore programs the stage-one/shared terminal write: CommitTerminal
// answers the programmed error, everything else passes through to the store.
// The explicit delegations disambiguate the methods both embedded interfaces
// declare.
type verdictFailingStore struct {
	state.Store
	state.CoordStore

	err error
}

func (s *verdictFailingStore) Shared() bool { return true }

func (s *verdictFailingStore) CommitTerminal(ctx context.Context, ws, taskID string, result registry.Result, reason string, completedAt, drainDeadline time.Time) (registry.TaskEntry, bool, error) {
	if s.err != nil {
		return registry.TaskEntry{}, false, s.err
	}
	return s.CoordStore.CommitTerminal(ctx, ws, taskID, result, reason, completedAt, drainDeadline)
}

func (s *verdictFailingStore) GetEntry(ctx context.Context, ws, taskID string) (registry.TaskEntry, bool, error) {
	return s.CoordStore.GetEntry(ctx, ws, taskID)
}

func (s *verdictFailingStore) ListEntries(ctx context.Context) ([]registry.TaskEntry, error) {
	return s.CoordStore.ListEntries(ctx)
}

func (s *verdictFailingStore) DeleteEntry(ctx context.Context, ws, taskID string) error {
	return s.CoordStore.DeleteEntry(ctx, ws, taskID)
}

func (s *verdictFailingStore) ListSettled(ctx context.Context) ([]registry.TaskEntry, error) {
	return s.CoordStore.ListSettled(ctx)
}

func (s *verdictFailingStore) ListRevoked(ctx context.Context) (map[string]map[string]time.Time, error) {
	return s.CoordStore.ListRevoked(ctx)
}

// TestForward10VerdictWriteFailureWithdrawsTheWriteAheadEntry: the terminal
// write fails after the report was queued. §10's order is one intent — enqueue,
// verdict, forward — so the entry goes with the verdict it preceded; the drain
// must not put a terminal verdict on the server while this instance still holds
// the task as running.
func TestForward10VerdictWriteFailureWithdrawsTheWriteAheadEntry(t *testing.T) {
	store, _ := multi08Store(t)
	failing := &verdictFailingStore{Store: store, CoordStore: store.(state.CoordStore)}
	h := newHarnessWithSharedState(t, false, failing)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	claimTask(t, h, "task-1", token)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}

	// The verdict write fails: the caller is told to retry (never a 2xx), and
	// the queue keeps nothing of the report it had written ahead.
	failing.err = errors.New("shared state unavailable")
	report := []byte(`{"result":"ok"}`)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, report); code != http.StatusBadGateway {
		t.Fatalf("S13 with a failing verdict write = %d (%s), want 502", code, body)
	}
	if entry, ok := h.reg.Get("task-1"); !ok || entry.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want the pre-report non-terminal entry", entry, ok)
	}
	if got := h.pending.Len(); got != 0 {
		t.Fatalf("pending reports = %d, want the entry withdrawn with the failed verdict", got)
	}
	if got := h.metrics.pendingReportsDepth(); got != 0 {
		t.Fatalf("foreman_pending_reports = %d, want 0", got)
	}
	if got := len(h.jobs.carrierSecrets()); got != 0 {
		t.Fatalf("carrier entries = %d, want none", got)
	}

	// The recovery drain has nothing to replay: the server must not hold a
	// verdict for a task this instance never decided.
	if remaining, err := h.sched.DrainPendingReports(t.Context()); err != nil || remaining != 0 {
		t.Fatalf("DrainPendingReports = %d, %v; want an empty queue", remaining, err)
	}
	if got := len(h.server.forwarded(scheduler.EPComplete)); got != 0 {
		t.Fatalf("complete forwards = %d, want the undecided report never sent", got)
	}

	// The daemon's retry walks the whole order again once the store is back:
	// one terminal write, the queue empty, the Job torn down.
	failing.err = nil
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, report); code != http.StatusOK {
		t.Fatalf("S13 retry = %d (%s), want 200", code, body)
	}
	if got := h.pending.Len(); got != 0 {
		t.Fatalf("pending reports after the retry = %d, want 0", got)
	}
	if got := h.server.acceptedFor(scheduler.EPComplete); got != 1 {
		t.Fatalf("accepted complete writes = %d, want exactly 1", got)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job must be deleted once the terminal report landed")
	}
}
