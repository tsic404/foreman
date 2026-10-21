// TC-tech-forward-09 (AC-16) drives the pending queue's retention bound at
// admission: with the queue already full, a new task's terminal report is
// refused and dropped on the alarm face (§4.6, ADR-046 decision 6). The
// task's terminal write still lands, no carrier object holds the report, the
// refused report's only wire face is its direct forward (the status-coded
// counter records that attempt and nothing after it), and a rebuilt process
// never replays it — the loss face is the queue entry that never existed.
package acceptance

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// forward09Harness is the delivery fixture over the production forward client
// with two production seams narrowed for the scenario: the pending queue's
// retention bound (so one queued report reaches it) and the §10 terminal
// retry budget (one attempt, so a refused report's direct forward returns
// without waiting out the deployment budget).
func forward09Harness(t *testing.T) *harness {
	t.Helper()
	h := buildHarnessWith(t, false, deliveryConfirmWindow, nil, true, false, harnessOptions{
		clientOpts:  []proxy.Option{proxy.WithTerminalRetryWaits([]time.Duration{0})},
		pendingOpts: []scheduler.PendingReportsOption{scheduler.WithPendingReportsMaxEntries(1)},
	})
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	// The restart reads the Job back, so the scenario needs the production
	// Job (labels + annotations), not the canned one.
	h.useRealBuilder(t)
	return h
}

// forward09TerminalReport drives one task from claim to its terminal report
// while the upstream refuses terminal forwards with 503. The report is
// written ahead of the forward; whether it lands in the queue is the
// scenario's variable.
func forward09TerminalReport(t *testing.T, h *harness, taskID string, report []byte) {
	t.Helper()
	if err := h.sched.OnClaim(t.Context(), deliveryPayload(taskID)); err != nil {
		t.Fatalf("OnClaim(%s): %v", taskID, err)
	}
	token := issueJobToken(t, h, taskID)
	registerTaskDaemon(t, h, taskID, token)
	claimTask(t, h, taskID, token)
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8(%s) = %d (%s), want 200", taskID, code, body)
	}
	h.upstream.setLifecycleStatus(string(scheduler.EPComplete), http.StatusServiceUnavailable)
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/complete", token, report); code != http.StatusBadGateway {
		t.Fatalf("S13(%s) with a down upstream = %d (%s), want 502", taskID, code, body)
	}
}

// forward09Completes counts the terminal forwards the stub upstream observed
// for taskID: the wire face of "the refused report is forwarded once, never
// replayed".
func forward09Completes(h *harness, taskID string) int {
	path := "/api/daemon/tasks/" + taskID + "/complete"
	n := 0
	for _, req := range h.upstream.all() {
		if req.Path == path {
			n++
		}
	}
	return n
}

// assertForwardDroppedEvent fails unless the queue logged one drop for taskID
// with the given reason and bound: the admission refusal's alarm face.
func assertForwardDroppedEvent(t *testing.T, logs *syncBuffer, taskID, reason string, bound int) {
	t.Helper()
	for _, line := range bytes.Split(logs.Bytes(), []byte("\n")) {
		var fields map[string]any
		if json.Unmarshal(line, &fields) != nil {
			continue
		}
		if fields["msg"] != "task.forward_dropped" || fields["task_id"] != taskID || fields["reason"] != reason {
			continue
		}
		if got, ok := fields["bound"].(float64); !ok || int(got) != bound {
			t.Fatalf("drop event for %s carries bound %v, want %d:\n%s", taskID, fields["bound"], bound, line)
		}
		return
	}
	t.Fatalf("no task.forward_dropped event for %s with reason %q:\n%s", taskID, reason, logs.Bytes())
}

func TestForward09QueueBoundRefusalDropsTheReport(t *testing.T) {
	logs := capturePendingLogs(t)
	h := forward09Harness(t)

	// task-1 fills the queue: its report is written ahead, its forward fails,
	// so the entry stays queued and carried. The bound is reached here.
	forward09TerminalReport(t, h, "task-1", []byte(`{"result":"ok"}`))
	if got := h.pending.Len(); got != 1 {
		t.Fatalf("pending reports after the first report = %d, want 1", got)
	}
	if got := len(h.jobs.carrierSecrets()); got != 1 {
		t.Fatalf("carriers after the first report = %d, want 1", got)
	}
	failedBefore := h.metrics.forwardCount(string(scheduler.EPComplete), http.StatusServiceUnavailable)
	acceptedBefore := h.metrics.forwardCount(string(scheduler.EPComplete), http.StatusOK)
	if failedBefore != 1 || acceptedBefore != 0 {
		t.Fatalf("complete forwards before the refused report = %d failed / %d accepted, want 1/0",
			failedBefore, acceptedBefore)
	}

	// task-2's report meets the reached bound: it is refused at admission and
	// dropped, but the terminal write it precedes still lands.
	forward09TerminalReport(t, h, "task-2", []byte(`{"result":"ok","note":"refused"}`))

	// The verdict is the report's, written before the forward and kept even
	// though the queue refused the entry.
	entry, ok := h.reg.Get("task-2")
	if !ok || !entry.IsTerminal() || entry.Result != registry.ResultCompleted {
		t.Fatalf("entry = %+v, %v; want the retained completed verdict", entry, ok)
	}

	// Only task-1's entry is held and carried: the refused report neither
	// joins the queue nor reaches a carrier.
	if got := h.pending.Len(); got != 1 {
		t.Fatalf("pending reports after the refusal = %d, want 1 (the first report only)", got)
	}
	if h.pending.Queued("task-2") {
		t.Fatal("the refused report is held by the queue")
	}
	if got := h.metrics.pendingReportsDepth(); got != 1 {
		t.Fatalf("foreman_pending_reports = %d, want 1", got)
	}
	carriers := h.jobs.carrierSecrets()
	if len(carriers) != 1 || carriers[0].Labels[registry.LabelTaskID] != "task-1" {
		t.Fatalf("carriers after the refusal = %d, want the first task's only", len(carriers))
	}
	// The report never landed, so nothing proves the server gave the task up.
	if h.jobs.jobDeleted("fm-task-2") {
		t.Fatal("the Job was deleted while its terminal report was still undelivered")
	}

	// The drop's own error event names the bound and the reason, and the
	// status-coded counter records exactly the direct attempt — one refused
	// forward, no accepted one, no replay.
	assertForwardDroppedEvent(t, logs, "task-2", "pending report queue bound reached", 1)
	if got := forward09Completes(h, "task-2"); got != 1 {
		t.Fatalf("complete requests for the refused task = %d, want the single direct attempt", got)
	}
	if got := h.metrics.forwardCount(string(scheduler.EPComplete), http.StatusServiceUnavailable); got != failedBefore+1 {
		t.Fatalf("foreman_forward_total{endpoint=complete,code=503} = %d, want %d", got, failedBefore+1)
	}
	if got := h.metrics.forwardCount(string(scheduler.EPComplete), http.StatusOK); got != acceptedBefore {
		t.Fatalf("accepted complete forwards = %d, want %d: the refusal never queues a delivery", got, acceptedBefore)
	}

	// The rebuilt process restores only the carried entry, and no convergence
	// round replays the refused report.
	reg, pending, sched := restartOverCluster(t, h)
	if err := sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if !pending.Rebuilt() {
		t.Fatal("the carrier list did not complete; the queue state is not authoritative")
	}
	if got, carriers := pending.Len(), len(h.jobs.carrierSecrets()); got != 1 || carriers != 1 {
		t.Fatalf("rebuilt depth = %d, carriers = %d; want 1 and 1", got, carriers)
	}
	if pending.Queued("task-2") {
		t.Fatal("the refused report reappeared in the rebuilt queue")
	}
	// The server still holds the refused task; the scenario judges replay, not
	// the vanish path the rebuild's C13 round would otherwise take.
	h.upstream.setTaskStatus("task-2", "running")
	h.upstream.setLifecycleStatus(string(scheduler.EPComplete), 0)
	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		reg, queueObjects{jobs: h.jobs}, h.serverClient, sched)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	for round := 1; round <= 2; round++ {
		if err := rec.Reconcile(t.Context()); err != nil {
			t.Fatalf("Reconcile round %d: %v", round, err)
		}
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("rebuilt queue depth after two rounds = %d, want 0 (the carried report drained)", got)
	}
	if got := forward09Completes(h, "task-2"); got != 1 {
		t.Fatalf("complete requests for the refused task after the restart = %d, want the single pre-restart attempt (no replay)", got)
	}
	if got := h.metrics.forwardCount(string(scheduler.EPComplete), http.StatusServiceUnavailable); got != failedBefore+1 {
		t.Fatalf("refused forward count after the restart = %d, want %d (no replay)", got, failedBefore+1)
	}
	if got := h.metrics.forwardCount(string(scheduler.EPComplete), http.StatusOK); got != acceptedBefore+1 {
		t.Fatalf("accepted complete forwards after the restart = %d, want %d (the carried report's replay only)", got, acceptedBefore+1)
	}
}
