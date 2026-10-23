// TC-tech-forward-07 (AC-16) drives the queue's unbacked face: a terminal
// report whose carrier write does not land — the payload is beyond the
// carrier object's single-object bound, or the cluster refuses the write —
// stays in process memory only. The queue keeps it, the error face names the
// reason, no carrier object exists, and the task's Job is kept while the
// report is undelivered. Process exit loses the entry: the rebuilt queue is
// empty and equals the carrier list, which is the declared loss face. An
// identifier the queue's file grammar rejects has no disk face even to wait
// for, so that in-memory copy is the queue itself and counts as one.
package acceptance

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// capturePendingLogs points the default logger at a readable buffer. The queue
// binds that logger at construction, so this must run before the harness is
// built; the queue's own error events are this scenario's judgement face.
func capturePendingLogs(t *testing.T) *syncBuffer {
	t.Helper()
	logs := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return logs
}

// assertUndeliveredEvent fails unless the queue reported one undelivered
// report for the task under the given reason, with the workspace the task
// belongs to (contract §9.1: task-face rows carry the workspace).
func assertUndeliveredEvent(t *testing.T, logs *syncBuffer, taskID, reason string) {
	t.Helper()
	for _, line := range bytes.Split(logs.Bytes(), []byte("\n")) {
		var fields map[string]any
		if json.Unmarshal(line, &fields) != nil {
			continue
		}
		if fields["msg"] == "task.forward_undelivered" &&
			fields["task_id"] == taskID && fields["reason"] == reason {
			if fields["workspace_id"] != "ws-1" {
				t.Fatalf("task.forward_undelivered workspace_id = %v, want ws-1: %v", fields["workspace_id"], fields)
			}
			return
		}
	}
	t.Fatalf("no task.forward_undelivered event for %s with reason %q:\n%s", taskID, reason, logs.Bytes())
}

// assertUnbackedQueueEntry is the in-process face both unbacked shapes share:
// the report is still queued, no carrier object holds it, the error face names
// the reason, and the Job waits for its report.
func assertUnbackedQueueEntry(t *testing.T, h *harness, logs *syncBuffer, reason string) {
	t.Helper()
	if got := h.pending.Len(); got != 1 {
		t.Fatalf("pending reports = %d, want 1: the report stays queued in process", got)
	}
	if got := h.metrics.pendingReportsDepth(); got != 1 {
		t.Fatalf("foreman_pending_reports = %d, want 1", got)
	}
	if got := len(h.jobs.carrierSecrets()); got != 0 {
		t.Fatalf("carriers = %d, want 0: a report without a landed carrier must leave no carrier object", got)
	}
	assertUndeliveredEvent(t, logs, "task-1", reason)
	if h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job was deleted while its terminal report was still undelivered")
	}
}

// assertLostOnProcessExit is the loss face: the unbacked entry dies with the
// process. The restart keeps the cluster objects and drops the node-local
// queue directory, exactly a reschedule.
func assertLostOnProcessExit(t *testing.T, h *harness) {
	t.Helper()
	_, pending, sched := restartOverCluster(t, h)
	if err := sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if !pending.Rebuilt() {
		t.Fatal("the carrier list did not complete; the queue state is not authoritative")
	}
	if got, carriers := pending.Len(), len(h.jobs.carrierSecrets()); got != 0 || carriers != 0 {
		t.Fatalf("rebuilt queue depth = %d, carriers = %d; want 0 and 0 — an unbacked entry must not survive the process", got, carriers)
	}
}

// carrierBoundReportBytes is a report body beyond the platform's single-object
// bound (a Secret holds 1 MiB) and well under the local HTTP face's bound, so
// the daemon's report is forwardable but no carrier can hold it.
const carrierBoundReportBytes = 2 << 20

// oversizedReport returns a terminal report body no carrier object can hold.
func oversizedReport() []byte {
	return []byte(`{"result":"` + strings.Repeat("a", carrierBoundReportBytes) + `"}`)
}

func TestForward07CarrierBoundReportStaysInProcessOnly(t *testing.T) {
	logs := capturePendingLogs(t)
	h := newDeliveryHarness(t)
	h.useRealBuilder(t)
	queueTerminalReport(t, h, oversizedReport())

	assertUnbackedQueueEntry(t, h, logs, "pending report exceeds the carrier object bound")
	assertLostOnProcessExit(t, h)
}

func TestForward07RefusedCarrierWriteStaysInProcessOnly(t *testing.T) {
	logs := capturePendingLogs(t)
	h := newDeliveryHarness(t)
	h.useRealBuilder(t)
	h.jobs.setCreateCarrierErr(errors.New("apiserver refused the carrier write"))
	queueTerminalReport(t, h, []byte(`{"result":"ok"}`))

	assertUnbackedQueueEntry(t, h, logs, "pending-report carrier write failed")
	assertLostOnProcessExit(t, h)
}

// forward07UnnameableTaskID is an id the queue's file grammar rejects, so no
// queue file can ever name it. The carrier's label face accepts it and the Job
// name face does not, so the claim fails before any Job exists: the report this
// id can have is its claim failure, and that report lives in memory only.
const forward07UnnameableTaskID = "task.abcdef123456"

// deliveredEvents counts the queue's delivered log rows for one task.
func deliveredEvents(logs *syncBuffer, taskID string) int {
	n := 0
	for _, line := range bytes.Split(logs.Bytes(), []byte("\n")) {
		var fields map[string]any
		if json.Unmarshal(line, &fields) != nil {
			continue
		}
		if fields["msg"] == "task.forward_delivered" && fields["task_id"] == taskID {
			n++
		}
	}
	return n
}

// TestForward07UnnameableTaskIDStaysCountedInProcess is the third unbacked
// shape: the identifier itself cannot name a queue file. Such a report has no
// disk face to wait for, so the in-memory copy is its queue — the pending
// count reads 1 rather than 0, and the entry reads as queued while it is
// undelivered. A drain then delivers it exactly once and clears both.
func TestForward07UnnameableTaskIDStaysCountedInProcess(t *testing.T) {
	logs := capturePendingLogs(t)
	h := newDeliveryHarness(t)
	h.useRealBuilder(t)
	h.jobs.setCreateCarrierErr(errors.New("apiserver refused the carrier write"))
	h.server.setForwardCode(503)

	// The claim itself fails on the id (no legal Job name), so the failure
	// report is the only report this task ever produces.
	if err := h.sched.OnClaim(t.Context(), deliveryPayload(forward07UnnameableTaskID)); err == nil {
		t.Fatal("OnClaim: want the invalid-task-id failure")
	}
	if jobs := h.jobs.liveJobs(); jobs != 0 {
		t.Fatalf("live Jobs = %d, want 0: the claim never produced one", jobs)
	}
	if got := h.pending.Len(); got != 1 {
		t.Fatalf("pending reports = %d, want 1: the in-memory copy is this id's whole queue", got)
	}
	if got := h.metrics.pendingReportsDepth(); got != 1 {
		t.Fatalf("foreman_pending_reports = %d, want 1", got)
	}
	if !h.pending.Queued(forward07UnnameableTaskID) {
		t.Fatal("the undelivered report read as delivered: the settlement gate is open")
	}
	if got := len(h.jobs.carrierSecrets()); got != 0 {
		t.Fatalf("carriers = %d, want 0: the carrier write was refused", got)
	}
	assertUndeliveredEvent(t, logs, forward07UnnameableTaskID, "pending-report carrier write failed")

	// The upstream and the carrier come back: one drain lands the carrier,
	// delivers the report, and settles the entry.
	h.jobs.setCreateCarrierErr(nil)
	h.server.setForwardCode(0)
	remaining, err := h.pending.Drain(t.Context(), h.sched)
	if err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v; want the staged report delivered", remaining, err)
	}
	if h.pending.Queued(forward07UnnameableTaskID) || h.pending.Len() != 0 {
		t.Fatalf("after the drain: Queued=%v Len=%d, want false/0",
			h.pending.Queued(forward07UnnameableTaskID), h.pending.Len())
	}
	if got := h.metrics.pendingReportsDepth(); got != 0 {
		t.Fatalf("foreman_pending_reports = %d, want 0", got)
	}
	if got := len(h.jobs.carrierSecrets()); got != 0 {
		t.Fatalf("carriers after delivery = %d, want 0", got)
	}
	if got := deliveredEvents(logs, forward07UnnameableTaskID); got != 1 {
		t.Fatalf("deliveries = %d, want exactly 1", got)
	}
}
