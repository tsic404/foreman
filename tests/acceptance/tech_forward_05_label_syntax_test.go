// TC-tech-forward-05 (AC-16) label-syntax face: the carrier locates a report
// by its task-id label, whose grammar accepts "." and "_", while the
// node-local queue names files after the id and does not. Such an id can never
// name a Job, so the report it produces is the claim's invalid-name verdict —
// exactly the entry whose only durability face is the carrier.
package acceptance

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// labelSyntaxTaskID carries both characters the queue's file-name grammar
// rejects while staying a legal label value — the carrier's locator face
// (§3, §6.5). It also cannot render a DNS-1123 Job name, so the only report
// it can produce is the invalid-name verdict of its claim.
const labelSyntaxTaskID = "task.1_a"

// queueInvalidNameReport drives one claim whose id cannot name a Job: the
// entry settles failed with the invalid-name attribution and its report is
// written ahead into the queue. The upstream stays down, so the report must
// remain queued instead of being delivered.
func queueInvalidNameReport(t *testing.T, h *harness) {
	t.Helper()
	if err := h.sched.OnClaim(t.Context(), deliveryPayload(labelSyntaxTaskID)); !errors.Is(err, jobbuilder.ErrInvalidTaskID) {
		t.Fatalf("OnClaim = %v, want the invalid-name refusal", err)
	}
	e, ok := h.reg.Get(labelSyntaxTaskID)
	if !ok || !e.IsTerminal() || e.Result != registry.ResultFailed ||
		e.FailureReason != scheduler.FailureReasonInvalidTaskID {
		t.Fatalf("entry = %+v, %v; want terminal failed with the invalid-name attribution", e, ok)
	}
	if got := h.server.acceptedFor(scheduler.EPFail); got != 0 {
		t.Fatalf("accepted fail writes = %d, want 0 while the upstream is down", got)
	}
	if got := len(h.server.forwarded(scheduler.EPFail)); got != 1 {
		t.Fatalf("fail forwards = %d, want the one failed verdict report", got)
	}
}

// TestForward05LabelSyntaxCarrierSurvivesForemanRestart: the carrier rides the
// raw id on its task-id label, so a "." / "_" id keeps its cross-process face
// even though the queue file grammar rejects it (§3, §6.5). The rebuilt queue
// holds the carrier entry and replays its body verbatim within two rounds.
func TestForward05LabelSyntaxCarrierSurvivesForemanRestart(t *testing.T) {
	h := newDeliveryHarness(t)
	h.useRealBuilder(t)
	h.server.setForwardCode(503)
	queueInvalidNameReport(t, h)

	carriers := h.jobs.carrierSecrets()
	if len(carriers) != 1 {
		t.Fatalf("carriers = %d, want 1: the id is a legal label value and must keep its carrier", len(carriers))
	}
	if got := carriers[0].Labels[registry.LabelTaskID]; got != labelSyntaxTaskID {
		t.Fatalf("carrier task-id label = %q, want the raw %q", got, labelSyntaxTaskID)
	}
	if got := string(carriers[0].Data["failure_reason"]); got != scheduler.FailureReasonInvalidTaskID {
		t.Fatalf("carrier attribution = %q, want %q", got, scheduler.FailureReasonInvalidTaskID)
	}
	enqueued := append([]byte(nil), carriers[0].Data["body"]...)
	if len(enqueued) == 0 {
		t.Fatal("the carrier holds no report body")
	}

	reg, pending, sched := restartOverCluster(t, h)
	if err := sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	// AC-16's rebuild judgement: the depth does not fall to zero and equals
	// the carrier count.
	if got, want := pending.Len(), len(h.jobs.carrierSecrets()); got != want || got == 0 {
		t.Fatalf("rebuilt depth = %d, want %d (the carrier count)", got, want)
	}
	if got := h.metrics.pendingReportsDepth(); got != pending.Len() {
		t.Fatalf("foreman_pending_reports = %d, want %d", got, pending.Len())
	}
	if !pending.Queued(labelSyntaxTaskID) {
		t.Fatal("the restored report does not read as queued")
	}

	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		reg, queueObjects{jobs: h.jobs}, h.server, sched)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	h.server.setForwardCode(0)
	for round := 1; round <= 2 && pending.Len() > 0; round++ {
		if err := rec.Reconcile(t.Context()); err != nil {
			t.Fatalf("Reconcile round %d: %v", round, err)
		}
	}

	if got := pending.Len(); got != 0 {
		t.Fatalf("pending reports after two rounds = %d, want 0", got)
	}
	if got := len(h.jobs.carrierSecrets()); got != 0 {
		t.Fatalf("carriers after settlement = %d, want 0", got)
	}
	if got := h.server.acceptedFor(scheduler.EPFail); got != 1 {
		t.Fatalf("accepted fail writes = %d, want exactly 1", got)
	}
	if applied := h.server.appliedTerminalWrites(labelSyntaxTaskID); len(applied) != 1 ||
		!bytes.Equal(applied[0].Body, enqueued) {
		t.Fatalf("applied writes = %+v, want the enqueued body %s", applied, enqueued)
	}
	// The failed report and its one replay: a third forward that landed would
	// mean the replay re-decided the verdict.
	if got := len(h.server.forwarded(scheduler.EPFail)); got != 2 {
		t.Fatalf("fail forwards = %d, want 2 (the failed report and its one replay)", got)
	}
	if events := h.events("task.failed_compensated"); len(events) != 1 {
		t.Fatalf("compensation events = %d, want only the pre-restart verdict", len(events))
	}
}

// TestForward05LabelSyntaxCarrierWriteIsRetriedInProcess: an id the file
// grammar rejects has no node-local file, so its carrier write is the only
// durability face it can get. A refused write stays staged and the next drain
// retries it; the report's process exit must then find the carrier (§10,
// ADR-046 decision 2).
func TestForward05LabelSyntaxCarrierWriteIsRetriedInProcess(t *testing.T) {
	h := newDeliveryHarness(t)
	h.useRealBuilder(t)
	h.server.setForwardCode(503)
	h.jobs.setCreateCarrierErr(errors.New("apiserver refused the carrier write"))
	queueInvalidNameReport(t, h)

	if got := len(h.jobs.carrierSecrets()); got != 0 {
		t.Fatalf("carriers = %d, want 0: the programmed write failed", got)
	}
	// No durability face yet, but the entry is queued: the id cannot name a
	// file and the carrier write did not land, so the in-memory copy is the
	// report's whole queue face — depth and membership read it.
	if got := h.pending.Len(); got != 1 {
		t.Fatalf("pending reports = %d, want 1 before a carrier lands", got)
	}
	if !h.pending.Queued(labelSyntaxTaskID) {
		t.Fatal("a staged report whose id cannot name a queue file must read as queued")
	}

	h.jobs.setCreateCarrierErr(nil)
	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		h.reg, queueObjects{jobs: h.jobs}, h.server, h.sched)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile (carrier retry): %v", err)
	}
	// The upstream is still down, so the round's only durable progress is the
	// retried carrier write.
	carriers := h.jobs.carrierSecrets()
	if len(carriers) != 1 {
		t.Fatalf("carriers after the retry round = %d, want the retried write to land", len(carriers))
	}
	if got := h.pending.Len(); got != 1 {
		t.Fatalf("durable depth after the retry = %d, want 1", got)
	}
	if got := h.metrics.pendingReportsDepth(); got != 1 {
		t.Fatalf("foreman_pending_reports = %d, want 1", got)
	}
	if got := h.server.acceptedFor(scheduler.EPFail); got != 0 {
		t.Fatalf("accepted fail writes during the retry round = %d, want 0", got)
	}
	if got := len(h.server.forwarded(scheduler.EPFail)); got != 2 {
		t.Fatalf("fail forwards = %d, want 2 (the failed report and the round's attempt)", got)
	}
	enqueued := append([]byte(nil), carriers[0].Data["body"]...)
	if len(enqueued) == 0 {
		t.Fatal("the retried carrier holds no report body")
	}

	reg, pending, sched := restartOverCluster(t, h)
	if err := sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if got, want := pending.Len(), len(h.jobs.carrierSecrets()); got != want || got == 0 {
		t.Fatalf("rebuilt depth = %d, want %d (the carrier count)", got, want)
	}

	rec, err = recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		reg, queueObjects{jobs: h.jobs}, h.server, sched)
	if err != nil {
		t.Fatalf("recovery.New (restarted): %v", err)
	}
	h.server.setForwardCode(0)
	for round := 1; round <= 2 && pending.Len() > 0; round++ {
		if err := rec.Reconcile(t.Context()); err != nil {
			t.Fatalf("Reconcile round %d: %v", round, err)
		}
	}

	if got := pending.Len(); got != 0 {
		t.Fatalf("pending reports after two rounds = %d, want 0", got)
	}
	if got := len(h.jobs.carrierSecrets()); got != 0 {
		t.Fatalf("carriers after settlement = %d, want 0", got)
	}
	if got := h.server.acceptedFor(scheduler.EPFail); got != 1 {
		t.Fatalf("accepted fail writes = %d, want exactly 1", got)
	}
	if applied := h.server.appliedTerminalWrites(labelSyntaxTaskID); len(applied) != 1 ||
		!bytes.Equal(applied[0].Body, enqueued) {
		t.Fatalf("applied writes = %+v, want the enqueued body %s", applied, enqueued)
	}
	if got := len(h.server.forwarded(scheduler.EPFail)); got != 3 {
		t.Fatalf("fail forwards = %d, want 3 (the failed report, the retry attempt, and its one replay)", got)
	}
	if events := h.events("task.failed_compensated"); len(events) != 1 {
		t.Fatalf("compensation events = %d, want only the pre-restart verdict", len(events))
	}
}
