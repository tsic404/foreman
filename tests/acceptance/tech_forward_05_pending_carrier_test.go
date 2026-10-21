// TC-tech-forward-05 (AC-16) drives the cross-process face of the pending
// terminal-report queue: the entry lands on its cluster carrier before the
// forward, a Foreman restart rebuilds the queue from the carrier, and the
// recovery loop replays the enqueued body within two rounds. The rebuilt
// entry keeps the report's verdict — a completed task must not be turned into
// a synthesized failure — and the carrier disappears only after the report
// settled. The carrier-list window is the negative face: while the list
// fails, no round settles anything and no synthetic terminal appears.
package acceptance

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// restartOverCluster rebuilds the process-local components over the same
// cluster fake: a fresh registry and a fresh pending queue (its node-local
// directory is gone with the process), exactly what a rescheduled Foreman
// starts from. The carriers and Jobs are the cluster's. The forward seam is
// the one the harness itself runs on (h.serverClient), so a replay from the
// rebuilt process reaches the same wire and the same counter a live process
// would — wiring the recorder stub instead would leave every replay
// assertion blind.
func restartOverCluster(t *testing.T, h *harness) (*registry.Registry, *scheduler.PendingReports, *scheduler.Scheduler) {
	t.Helper()
	reg := registry.New(time.Now, registry.WithDeliveryConfirmTimeout(deliveryConfirmWindow))
	pending, err := scheduler.NewPendingReports(t.TempDir(),
		scheduler.WithPendingReportsGauge(h.metrics.PendingReports),
		scheduler.WithPendingReportsCarrier(scheduler.NewSecretCarrierStore(h.jobs)))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	sched, err := scheduler.New(h.schedCfg, reg, h.jobs, h.builder, h.serverClient, h.metrics,
		scheduler.WithPendingReports(pending),
		scheduler.WithRevoker(noRevoker{}),
		scheduler.WithProbeRegistry(h.probes),
		scheduler.WithLogger(slog.New(slog.NewJSONHandler(h.logs, nil))),
		scheduler.WithRuntimeIdentity(h.client.RuntimeIdentity))
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	return reg, pending, sched
}

// queueTerminalReport drives one task to the queued-report state: claimed,
// started, then a terminal report the upstream refuses with 503 until the
// daemon's retry face is exhausted.
func queueTerminalReport(t *testing.T, h *harness, report []byte) {
	t.Helper()
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	claimTask(t, h, "task-1", token)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != 200 {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	h.server.setForwardCode(503)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, report); code != 502 {
		t.Fatalf("S13 with a down upstream = %d (%s), want 502", code, body)
	}
}

func TestForward05PendingCarrierSurvivesForemanRestart(t *testing.T) {
	h := newDeliveryHarness(t)
	// The rebuild reads the Job back, so the scenario needs the production
	// Job (labels + annotations), not the canned one.
	h.useRealBuilder(t)
	report := []byte(`{"result":"ok"}`)
	queueTerminalReport(t, h, report)

	// The entry is queued and carried; the Job waits for the report.
	if got := h.pending.Len(); got != 1 {
		t.Fatalf("pending reports = %d, want 1", got)
	}
	carriers := h.jobs.carrierSecrets()
	if len(carriers) != 1 {
		t.Fatalf("carriers = %d, want 1", len(carriers))
	}
	if got := carriers[0].Data["body"]; !bytes.Equal(got, report) {
		t.Fatalf("carrier body = %s, want the enqueued %s", got, report)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job was deleted before its terminal report landed")
	}

	// Restart: new registry, new queue directory, same cluster objects.
	reg, pending, sched := restartOverCluster(t, h)
	if err := sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	// AC-16's rebuild judgement: the count does not fall to zero and equals
	// the carrier count.
	if got, want := pending.Len(), len(h.jobs.carrierSecrets()); got != want || got == 0 {
		t.Fatalf("rebuilt depth = %d, want %d (the carrier count)", got, want)
	}
	if got := h.metrics.pendingReportsDepth(); got != pending.Len() {
		t.Fatalf("foreman_pending_reports = %d, want %d", got, pending.Len())
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

	// Two rounds converge: the enqueued body is replayed once, the Job goes
	// only after the report landed, and the carrier settles with the entry.
	if got := pending.Len(); got != 0 {
		t.Fatalf("pending reports after two rounds = %d, want 0", got)
	}
	if got := h.server.acceptedFor(scheduler.EPComplete); got != 1 {
		t.Fatalf("accepted complete writes = %d, want exactly 1", got)
	}
	forwards := h.server.forwarded(scheduler.EPComplete)
	// acceptedFor counts settled writes; the forward count is what catches a
	// duplicate replay of the same terminal.
	if got := len(forwards); got != 2 {
		t.Fatalf("complete forwards = %d, want 2 (the failed report and its one replay)", got)
	}
	if last := forwards[len(forwards)-1]; !bytes.Equal(last.Body, report) {
		t.Fatalf("replayed body = %s, want the enqueued %s", last.Body, report)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job must be deleted once the replayed report landed")
	}
	if got := len(h.jobs.carrierSecrets()); got != 0 {
		t.Fatalf("carriers after settlement = %d, want 0", got)
	}
	// The verdict is the replayed report's, not a synthesized failure. The
	// live row may already be reclaimed (the fake reports every pod gone), so
	// the retained terminal record is the judgement face (§4.3).
	entry, ok := reg.Settled("task-1")
	if !ok || entry.Result != registry.ResultCompleted {
		t.Fatalf("settled record = %+v, %v; want a completed verdict, not a synthesized failure", entry, ok)
	}
	if events := h.events("task.failed_compensated"); len(events) != 0 {
		t.Fatalf("synthesized failure events = %d, want none for a completed task", len(events))
	}
}

func TestForward05CarrierListFailureEmitsNoSyntheticTerminal(t *testing.T) {
	h := newDeliveryHarness(t)
	h.useRealBuilder(t)
	report := []byte(`{"result":"ok"}`)
	queueTerminalReport(t, h, report)

	reg, pending, sched := restartOverCluster(t, h)
	// The carrier list is unavailable for the whole window: the rebuild is
	// incomplete, and a failed list must not read as an empty queue.
	h.jobs.setListSecretsErr(errors.New("apiserver unavailable"))
	if err := sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("depth with a failed list = %d, want 0 (nothing restored)", got)
	}

	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		reg, queueObjects{jobs: h.jobs}, h.server, sched)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	h.server.setForwardCode(0)
	for round := 1; round <= 2; round++ {
		if err := rec.Reconcile(t.Context()); err != nil {
			t.Fatalf("Reconcile round %d: %v", round, err)
		}
	}
	// No settlement of any kind: no synthetic terminal, no Job deletion, no
	// terminal write.
	if entry, ok := reg.Get("task-1"); !ok || entry.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want it unsettled while the queue is not rebuilt", entry, ok)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job was deleted during the carrier-list window")
	}
	if got := h.server.acceptedFor(scheduler.EPComplete); got != 0 {
		t.Fatalf("terminal writes during the list window = %d, want 0", got)
	}
	if got := len(h.server.forwarded(scheduler.EPComplete)); got != 1 {
		t.Fatalf("complete forwards during the list window = %d, want the one failed report", got)
	}
	if events := h.events("task.failed_compensated"); len(events) != 0 {
		t.Fatalf("synthesized failure events = %d, want none", len(events))
	}
	if got := len(h.jobs.carrierSecrets()); got != 1 {
		t.Fatalf("carriers = %d, want the unsettled one kept", got)
	}

	// The list recovers: the queue rebuilds, the replay lands, the carrier
	// settles.
	h.jobs.setListSecretsErr(nil)
	// The first round rebuilds the queue (the depth was zero before it), so
	// the two rounds are unconditional here.
	for round := 1; round <= 2; round++ {
		if err := rec.Reconcile(t.Context()); err != nil {
			t.Fatalf("Reconcile after recovery round %d: %v", round, err)
		}
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("pending reports after the list recovered = %d, want 0", got)
	}
	if got := h.server.acceptedFor(scheduler.EPComplete); got != 1 {
		t.Fatalf("accepted complete writes = %d, want exactly 1", got)
	}
	if got := len(h.server.forwarded(scheduler.EPComplete)); got != 2 {
		t.Fatalf("complete forwards after the list recovered = %d, want 2 (the failed report and its one replay)", got)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job must be deleted after the replay landed")
	}
	if got := len(h.jobs.carrierSecrets()); got != 0 {
		t.Fatalf("carriers after settlement = %d, want 0", got)
	}
	entry, ok := reg.Settled("task-1")
	if !ok || entry.Result != registry.ResultCompleted {
		t.Fatalf("settled record = %+v, %v; want a completed verdict", entry, ok)
	}
}

// TestForward05ReplayedFailureKeepsItsAttribution: a synthetic failure verdict
// queued before a process exit is replayed with the attribution it was decided
// with (§4.1). The rebuilt entry has no in-memory verdict, and the body is not
// the source — an inbound failure body carries the same key — so the
// attribution must ride the queue entry and its carrier.
func TestForward05ReplayedFailureKeepsItsAttribution(t *testing.T) {
	h := newDeliveryHarness(t)
	h.useRealBuilder(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	// The boot bound settles the task on the daemon's behalf while the
	// upstream is unreachable, so the verdict's report stays queued.
	h.server.setForwardCode(503)
	if err := h.sched.OnBootTimeout(t.Context(), "task-1"); err != nil {
		t.Fatalf("OnBootTimeout: %v", err)
	}

	if got := h.pending.Len(); got != 1 {
		t.Fatalf("pending reports = %d, want 1", got)
	}
	carriers := h.jobs.carrierSecrets()
	if len(carriers) != 1 {
		t.Fatalf("carriers = %d, want 1", len(carriers))
	}
	enqueued := append([]byte(nil), carriers[0].Data["body"]...)
	if got := string(carriers[0].Data["failure_reason"]); got != scheduler.FailureReasonJobBootTimeout {
		t.Fatalf("carrier attribution = %q, want %q", got, scheduler.FailureReasonJobBootTimeout)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job was deleted before its terminal report landed")
	}

	// Restart: the rebuilt entry carries no verdict; the carrier is its only
	// terminal source.
	reg, pending, sched := restartOverCluster(t, h)
	if err := sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if got := pending.Len(); got != 1 {
		t.Fatalf("rebuilt depth = %d, want 1", got)
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
	if got := h.server.acceptedFor(scheduler.EPFail); got != 1 {
		t.Fatalf("accepted fail writes = %d, want exactly 1", got)
	}
	forwards := h.server.forwarded(scheduler.EPFail)
	// acceptedFor counts settled writes; the forward count is what catches a
	// duplicate replay of the same terminal.
	if got := len(forwards); got != 2 {
		t.Fatalf("fail forwards = %d, want 2 (the failed report and its one replay)", got)
	}
	if last := forwards[len(forwards)-1]; !bytes.Equal(last.Body, enqueued) {
		t.Fatalf("replayed body = %s, want the enqueued %s", last.Body, enqueued)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job must be deleted once the replayed report landed")
	}
	if got := len(h.jobs.carrierSecrets()); got != 0 {
		t.Fatalf("carriers after settlement = %d, want 0", got)
	}
	entry, ok := reg.Settled("task-1")
	if !ok || entry.Result != registry.ResultFailed || entry.FailureReason != scheduler.FailureReasonJobBootTimeout {
		t.Fatalf("settled record = %+v, %v; want failed with the queued attribution", entry, ok)
	}
	// The replay restores the pre-restart verdict; it must not decide a
	// second one.
	if events := h.events("task.failed_compensated"); len(events) != 1 {
		t.Fatalf("compensation events = %d, want only the pre-restart verdict", len(events))
	}
}

// TestForward05RegistrationRefusedDuringCarrierListFailureSettlesNothing: the
// proxy's refused-registration path calls FailJob outside any recovery round.
// After a restart whose carrier list fails, that path must not settle a task
// whose carrier holds a completed report — the exact shape AC-16/§4.3
// eliminate. Once the list recovers, the queued report is the only verdict.
func TestForward05RegistrationRefusedDuringCarrierListFailureSettlesNothing(t *testing.T) {
	h := newDeliveryHarness(t)
	h.useRealBuilder(t)
	report := []byte(`{"result":"ok"}`)
	queueTerminalReport(t, h, report)

	reg, pending, sched := restartOverCluster(t, h)
	h.jobs.setListSecretsErr(errors.New("apiserver unavailable"))
	if err := sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if pending.Rebuilt() {
		t.Fatal("the queue reads as rebuilt after a failed carrier list")
	}

	// The daemon registers against the restarted scheduler with a runtime set
	// its rebuilt identity cannot match; the handler refuses and compensates
	// through FailJob.
	issuer, err := proxy.JobTokenIssuer(testKey, reg, h.probes, auth.DefaultJobTokenTTL, nil)
	if err != nil {
		t.Fatalf("JobTokenIssuer: %v", err)
	}
	token, err := issuer.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	handler := proxy.NewFakeServer(issuer, reg, sched, h.client, nil, h.metrics,
		proxy.WithProbeRegistry(h.probes),
		proxy.WithSkillBundles(h.bundles),
		proxy.WithServerLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))))
	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/daemon/register", bytes.NewReader([]byte(`{
		"daemon_id": "fm-task-1",
		"runtimes": [{"name": "foreman-zz", "type": "zz", "version": "3.1.4"}]
	}`)))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("S4 = %d, want the refused-registration 409", resp.StatusCode)
	}

	// Nothing settled: the queued completed report still owns the verdict.
	if e, ok := reg.Get("task-1"); !ok || e.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want it unsettled while the queue is not rebuilt", e, ok)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job was deleted during the carrier-list window")
	}
	if got := len(h.server.forwarded(scheduler.EPFail)); got != 0 {
		t.Fatalf("synthesized fail forwards = %d, want none", got)
	}
	if got := len(h.server.forwarded(scheduler.EPComplete)); got != 1 {
		t.Fatalf("complete forwards during the list window = %d, want the one failed report", got)
	}
	if got := len(h.jobs.carrierSecrets()); got != 1 {
		t.Fatalf("carriers = %d, want the unsettled one kept", got)
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("depth with a failed list = %d, want 0 (nothing restored)", got)
	}

	// The list recovers: the completed report is replayed and no synthetic
	// terminal is ever emitted.
	h.jobs.setListSecretsErr(nil)
	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		reg, queueObjects{jobs: h.jobs}, h.server, sched)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	h.server.setForwardCode(0)
	for round := 1; round <= 2; round++ {
		if err := rec.Reconcile(t.Context()); err != nil {
			t.Fatalf("Reconcile after recovery round %d: %v", round, err)
		}
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("pending reports after the list recovered = %d, want 0", got)
	}
	if got := h.server.acceptedFor(scheduler.EPComplete); got != 1 {
		t.Fatalf("accepted complete writes = %d, want exactly 1", got)
	}
	if got := len(h.server.forwarded(scheduler.EPComplete)); got != 2 {
		t.Fatalf("complete forwards after the list recovered = %d, want 2 (the failed report and its one replay)", got)
	}
	if got := len(h.server.forwarded(scheduler.EPFail)); got != 0 {
		t.Fatalf("synthesized fail forwards = %d, want none", got)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job must be deleted after the replay landed")
	}
	if got := len(h.jobs.carrierSecrets()); got != 0 {
		t.Fatalf("carriers after settlement = %d, want 0", got)
	}
	entry, ok := reg.Settled("task-1")
	if !ok || entry.Result != registry.ResultCompleted {
		t.Fatalf("settled record = %+v, %v; want a completed verdict", entry, ok)
	}
}
