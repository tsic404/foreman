package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// newQueueScheduler builds the scheduler over a supplied pending-report queue:
// the replay scenarios need the real queue (its disk and carrier faces), not
// the fixture's recording stand-in.
func newQueueScheduler(t *testing.T, f *fixture, q PendingReportQueue, logw io.Writer) *Scheduler {
	t.Helper()
	cfg, err := LoadConfig(testEnv(nil))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	s, err := New(cfg, f.reg, f.jobs, f.builder, f.server, f.metrics,
		WithClock(func() time.Time { return f.now }),
		WithPendingReports(q),
		WithRevoker(f.revoker),
		WithProbeRegistry(registry.NewProbeRegistry()),
		WithLogger(slog.New(slog.NewJSONHandler(logw, nil))),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// settleCancelledTimeout drives the cancel path's timeout branch: the window
// expired without a daemon confirmation, so the verdict is written and the Job
// deleted, and the confirmation becomes Foreman's debt.
func settleCancelledTimeout(t *testing.T, s *Scheduler, f *fixture, taskID string) registry.TaskEntry {
	t.Helper()
	if err := s.SettleCancelled(context.Background(), taskID); err != nil {
		t.Fatalf("SettleCancelled: %v", err)
	}
	e, ok := f.reg.Get(taskID)
	if !ok {
		t.Fatalf("entry %s vanished", taskID)
	}
	if e.Result != registry.ResultCancelled {
		t.Fatalf("result = %q, want cancelled", e.Result)
	}
	if !e.CancelAckOwed {
		t.Fatal("the confirmation debt was not recorded on the entry")
	}
	return e
}

// countLogEvents counts the JSON log lines with the given msg.
func countLogEvents(t *testing.T, logs *bytes.Buffer, msg string) int {
	t.Helper()
	n := 0
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("decode log line %s: %v", line, err)
		}
		if event["msg"] == msg {
			n++
		}
	}
	return n
}

// The timeout branch settles without a report and leaves the confirmation to
// Foreman, but only a positive termination fact may release it: the
// job_terminated verdict carries it, and it is sent exactly once, byte for
// byte as the endpoint's minimal confirmation.
func TestOnJobTerminatedSynthesizesTheCancelConfirmationOnce(t *testing.T) {
	f := newFixture(t)
	var logs bytes.Buffer
	s := newQueueScheduler(t, f, f.pending, &logs)
	f.claim(t, "task-1")
	settleCancelledTimeout(t, s, f, "task-1")

	if got := len(f.server.forwards); got != 0 {
		t.Fatalf("forwards before the termination fact = %d, want 0", got)
	}

	finished := f.now.Add(-10 * time.Second)
	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonJobTerminated, finished, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	forwards := f.server.forwards
	if len(forwards) != 1 {
		t.Fatalf("forwards = %+v, want exactly one cancel-ack", forwards)
	}
	if forwards[0].ep != EPCancelAck {
		t.Fatalf("endpoint = %q, want cancel-ack", forwards[0].ep)
	}
	if forwards[0].body != `{"process_cleanup_confirmed":true}` {
		t.Fatalf("body = %s, want the endpoint's minimal confirmation", forwards[0].body)
	}
	if forwards[0].taskID != "task-1" || forwards[0].workspaceID != "ws-1" {
		t.Fatalf("routing = task %q workspace %q, want the task's own workspace", forwards[0].taskID, forwards[0].workspaceID)
	}

	event := decodeLogEvent(t, &logs, "task.cancel_ack_synthesized")
	if event["task_id"] != "task-1" || event["job_name"] != "fm-task-1" {
		t.Fatalf("event = %+v, want the task and Job identity", event)
	}
	if event["pod_terminated_at"] != finished.UTC().Format(time.RFC3339) {
		t.Fatalf("pod_terminated_at = %v, want the kubelet fact", event["pod_terminated_at"])
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("the entry must be reclaimed once the confirmation was emitted")
	}

	// A repeated reclaim (a retried teardown) sends nothing: the debt died with
	// the entry, so at most one delivery is ever owed.
	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonJobTerminated, finished, 0); err != nil {
		t.Fatalf("repeated OnJobTerminated: %v", err)
	}
	if got := len(f.server.forwards); got != 1 {
		t.Fatalf("forwards after the repeated reclaim = %d, want 1", got)
	}
}

// The drain upper bound closes the credential window without any termination
// fact: pods still alive, a node unreachable, an informer lost. There is no
// "the process is dead" evidence on that path, so no confirmation is sent.
func TestOnJobTerminatedDoesNotSynthesizeOnDrainTimeout(t *testing.T) {
	f := newFixture(t)
	var logs bytes.Buffer
	s := newQueueScheduler(t, f, f.pending, &logs)
	f.claim(t, "task-1")
	settleCancelledTimeout(t, s, f, "task-1")

	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonDrainTimeout, time.Time{}, 2); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if got := len(f.server.forwards); got != 0 {
		t.Fatalf("forwards = %+v, want none on the drain_timeout path", f.server.forwards)
	}
	if events := countLogEvents(t, &logs, "task.cancel_ack_synthesized"); events != 0 {
		t.Fatalf("task.cancel_ack_synthesized events = %d, want 0", events)
	}
}

// The daemon's own confirmation keeps its body and closes the window: the
// compensation never replaces or repeats it.
func TestDaemonCancelAckIsNeverDisplacedByTheCompensation(t *testing.T) {
	f := newFixture(t)
	var logs bytes.Buffer
	s := newQueueScheduler(t, f, f.pending, &logs)
	e := f.claim(t, "task-1")

	ack := []byte(`{"status":"cancelled","reason":"user_cancel"}`)
	if code, body, err := s.OnReport(context.Background(), EPCancelAck, e, ack, ""); err != nil || code != 200 {
		t.Fatalf("OnReport(cancel-ack) = %d (%s), %v; want 200", code, body, err)
	}
	forwards := f.server.forwards
	if len(forwards) != 1 || forwards[0].body != string(ack) {
		t.Fatalf("forwards = %+v, want the daemon's own body verbatim", forwards)
	}
	settled, ok := f.reg.Get("task-1")
	if !ok || settled.CancelAckOwed {
		t.Fatalf("entry = %+v, %v; want no confirmation debt after the daemon's ack", settled, ok)
	}

	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonJobTerminated, f.now, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if got := len(f.server.forwards); got != 1 {
		t.Fatalf("forwards after the reclaim = %d, want still the daemon's one", got)
	}
	if f.server.forwards[0].body != string(ack) {
		t.Fatalf("body = %s, want the daemon's body, never the compensation", f.server.forwards[0].body)
	}
}

// A confirmation that could not be forwarded is written ahead of the entry's
// removal: a restart rebuilds it from its carrier and delivers it once. 2xx
// settles the entry for good.
func TestSyntheticConfirmationSurvivesRestartAndReplaysOnce(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	carriers := newMemCarrierStore()
	q, err := NewPendingReports(dir, WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	logw := io.Discard
	if err := q.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	s := newQueueScheduler(t, f, q, logw)
	f.claim(t, "task-1")
	settleCancelledTimeout(t, s, f, "task-1")

	// The upstream is unreachable for the whole retry budget: the confirmation
	// stays queued instead of being lost with the entry.
	f.server.forwardErr = errors.New("upstream down")
	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonJobTerminated, f.now, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if got := q.Len(); got != 1 {
		t.Fatalf("queued confirmations = %d, want 1", got)
	}
	if _, ok := carriers.carriers["task-1"]; !ok {
		t.Fatal("the confirmation has no cluster carrier: a process exit would lose it")
	}

	// Restart: a fresh queue over the same state restores the entry and the
	// convergence round delivers it.
	f.server.forwardErr = nil
	restarted, err := NewPendingReports(dir, WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	if err := restarted.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	s2 := newQueueScheduler(t, f, restarted, logw)
	before := len(f.server.forwards)
	if remaining, err := restarted.Drain(context.Background(), s2); err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v; want a delivered queue", remaining, err)
	}
	forwards := f.server.forwards[before:]
	if len(forwards) != 1 {
		t.Fatalf("replayed forwards = %+v, want exactly one", forwards)
	}
	if forwards[0].ep != EPCancelAck || forwards[0].body != `{"process_cleanup_confirmed":true}` {
		t.Fatalf("replay = %+v, want the confirmation verbatim", forwards[0])
	}
	if got := restarted.Len(); got != 0 {
		t.Fatalf("queue after the replay = %d, want 0", got)
	}

	// Nothing is delivered twice once the 2xx landed.
	if remaining, err := restarted.Drain(context.Background(), s2); err != nil || remaining != 0 {
		t.Fatalf("second Drain = %d, %v; want an empty queue", remaining, err)
	}
	if got := len(f.server.forwards) - before; got != 1 {
		t.Fatalf("forwards after the second drain = %d, want 1", got)
	}
}

// 404 means the task is gone server-side: the confirmation is settled, not
// retried.
func TestSyntheticConfirmationIsDroppedOnTaskGone(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	carriers := newMemCarrierStore()
	q, err := NewPendingReports(dir, WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := q.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	s := newQueueScheduler(t, f, q, io.Discard)
	f.claim(t, "task-1")
	settleCancelledTimeout(t, s, f, "task-1")

	f.server.forwardErr = errors.New("upstream down")
	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonJobTerminated, f.now, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if got := q.Len(); got != 1 {
		t.Fatalf("queued confirmations = %d, want 1", got)
	}

	f.server.forwardErr = nil
	f.server.forwardCode = 404
	if remaining, err := q.Drain(context.Background(), s); err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v; want the 404 settled", remaining, err)
	}
	if got := q.Len(); got != 0 {
		t.Fatalf("queue after the 404 = %d, want 0", got)
	}
}

// The confirmation's carrier is the only face that survives a reschedule, so
// the sweep must read it as held: membership is the sweep's question, and a
// synthesized entry is a member — only the Job gate treats it differently.
// Enqueue → one sweep round → carrier still there → restart replays it.
func TestSyntheticConfirmationIsNotSweptBeforeDelivery(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	carriers := newMemCarrierStore()
	q, err := NewPendingReports(dir, WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := q.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	s := newQueueScheduler(t, f, q, io.Discard)
	f.claim(t, "task-1")
	settleCancelledTimeout(t, s, f, "task-1")

	f.server.forwardErr = errors.New("upstream down")
	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonJobTerminated, f.now, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if got := q.Len(); got != 1 {
		t.Fatalf("queued confirmations = %d, want 1", got)
	}

	// The sweep's criterion is membership, not the Job gate: the undelivered
	// confirmation holds its carrier.
	name := carrierSecretName("task-1")
	deleted, err := q.SweepCarrier(context.Background(), "task-1", name)
	if err != nil {
		t.Fatalf("SweepCarrier: %v", err)
	}
	if deleted {
		t.Fatal("the sweep deleted the carrier of an undelivered confirmation")
	}
	if _, ok := carriers.carriers["task-1"]; !ok {
		t.Fatal("the carrier is gone after one sweep round")
	}

	// Restart: the fresh process rebuilds the entry from that carrier and the
	// convergence round delivers it exactly once.
	f.server.forwardErr = nil
	restarted, err := NewPendingReports(dir, WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	if err := restarted.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	s2 := newQueueScheduler(t, f, restarted, io.Discard)
	before := len(f.server.forwards)
	if remaining, err := restarted.Drain(context.Background(), s2); err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v; want the confirmation delivered", remaining, err)
	}
	forwards := f.server.forwards[before:]
	if len(forwards) != 1 || forwards[0].body != `{"process_cleanup_confirmed":true}` {
		t.Fatalf("replayed forwards = %+v, want the confirmation verbatim", forwards)
	}

	// Delivered: the entry is settled, so the sweep now takes the carrier.
	deleted, err = restarted.SweepCarrier(context.Background(), "task-1", name)
	if err != nil {
		t.Fatalf("SweepCarrier after the delivery: %v", err)
	}
	if !deleted {
		t.Fatal("the settled carrier was not swept")
	}
	if _, ok := carriers.carriers["task-1"]; ok {
		t.Fatal("the settled carrier is still there")
	}
}

// A cancellation whose claim never produced an object is the "no pod ever
// existed" shape: there is no termination fact to wait for, so the debt is
// settled on the objectless reclaim instead of dying with the entry.
func TestObjectlessCancelStillConfirmsTheCleanup(t *testing.T) {
	f := newFixture(t)
	var logs bytes.Buffer
	s := newQueueScheduler(t, f, f.pending, &logs)
	if err := f.reg.Put(registry.TaskEntry{
		TaskID: "task-1", JobName: "fm-task-1", DaemonID: "fm-task-1",
		WorkspaceID: "ws-1", State: registry.StateJobCreated,
		ClaimedAt: f.now, FailureReason: FailureReasonInvalidTaskID,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	settleCancelledTimeout(t, s, f, "task-1")

	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonNoObjects, time.Time{}, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	forwards := f.server.forwards
	if len(forwards) != 1 {
		t.Fatalf("forwards = %+v, want one cancel-ack", forwards)
	}
	if forwards[0].ep != EPCancelAck || forwards[0].body != `{"process_cleanup_confirmed":true}` {
		t.Fatalf("forward = %+v, want the confirmation verbatim", forwards[0])
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("the objectless entry survived its reclaim")
	}
	if got := countLogEvents(t, &logs, "task.cancel_ack_synthesized"); got != 1 {
		t.Fatalf("task.cancel_ack_synthesized events = %d, want 1", got)
	}
}

// While an undelivered confirmation sits in the queue, the reclaim's catch-up
// Job deletion still runs: the entry is a member of the queue but never holds
// the §10 gate, so the cancel path's 60s deletion bound does not move with it
// (§4.6 兜底一). The enqueue and the membership are asserted together — a
// predicate that reads the entry as "not held" would break the sweep instead.
func TestQueuedConfirmationDoesNotHoldTheJobGate(t *testing.T) {
	f := newFixture(t)
	s := newQueueScheduler(t, f, f.pending, io.Discard)
	f.claim(t, "task-1")
	settleCancelledTimeout(t, s, f, "task-1")

	// The forward does not land, so the confirmation stays queued across the
	// reclaim — the only window in which the gate can be observed.
	f.server.forwardErr = errors.New("upstream down")
	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonJobTerminated, f.now, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if !f.pending.HasQueuedEntry("task-1") {
		t.Fatal("the undelivered confirmation is not held by the queue")
	}
	if f.pending.Queued("task-1") {
		t.Fatal("a synthesized confirmation holds the Job gate")
	}
	deletes := 0
	for _, name := range f.jobs.deletedJobs {
		if name == "fm-task-1" {
			deletes++
		}
	}
	if deletes < 2 {
		t.Fatalf("Job deletions = %v, want the settle's and the reclaim's catch-up", f.jobs.deletedJobs)
	}
}

// The negative condition is "accepted by the server", not "received": a daemon
// confirmation that lands through the queue replay settles the debt, so the
// reclaim that follows sends nothing (§4.6 兜底一).
func TestDaemonConfirmationDeliveredByTheQueueSettlesTheDebt(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	carriers := newMemCarrierStore()
	q, err := NewPendingReports(dir, WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := q.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	s := newQueueScheduler(t, f, q, io.Discard)
	e := f.claim(t, "task-1")
	settleCancelledTimeout(t, s, f, "task-1")

	// The daemon's late confirmation arrives while the upstream is down: it is
	// held write-ahead, the debt is still recorded, and the server has not
	// taken anything yet.
	f.server.forwardErr = errors.New("upstream down")
	ack := []byte(`{"status":"cancelled"}`)
	if code, _, err := s.OnReport(context.Background(), EPCancelAck, e, ack, ""); err != nil || code != 502 {
		t.Fatalf("OnReport = %d, %v; want 502 with the confirmation queued", code, err)
	}
	if held, _ := f.reg.Get("task-1"); !held.CancelAckOwed {
		t.Fatal("the debt was dropped before the confirmation reached the server")
	}

	// The replay lands it: the server has accepted a confirmation, so the debt
	// is settled and the later reclaim sends nothing.
	f.server.forwardErr = nil
	if remaining, err := q.Drain(context.Background(), s); err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v; want the confirmation delivered", remaining, err)
	}
	if settled, _ := f.reg.Get("task-1"); settled.CancelAckOwed {
		t.Fatal("the debt survived the accepted confirmation")
	}
	before := len(f.server.forwards)
	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonJobTerminated, f.now, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if got := len(f.server.forwards) - before; got != 0 {
		t.Fatalf("forwards after the reclaim = %d, want 0 (the daemon's confirmation already landed)", got)
	}
}

// The retention bound acts on new entries: when it refuses the confirmation,
// the refusal is the alarm face and the direct forward is what is left (§4.6
// 兜底一 保留上界与失败面). The other task's entry stays where it was.
func TestSyntheticConfirmationFallsBackToTheDirectForwardAtTheBound(t *testing.T) {
	f := newFixture(t)
	q, err := NewPendingReports(t.TempDir(), WithPendingReportsMaxEntries(1))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := q.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if err := q.Enqueue(context.Background(), "task-other", "fm-task-other", "fm-task-other",
		"ws-1", EPFail, []byte(`{}`), ""); err != nil {
		t.Fatalf("fill the queue: %v", err)
	}
	s := newQueueScheduler(t, f, q, io.Discard)
	f.claim(t, "task-1")
	settleCancelledTimeout(t, s, f, "task-1")

	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonJobTerminated, f.now, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	forwards := f.server.forwards
	if len(forwards) != 1 || forwards[0].ep != EPCancelAck || forwards[0].body != `{"process_cleanup_confirmed":true}` {
		t.Fatalf("forwards = %+v, want the direct confirmation", forwards)
	}
	if got := q.Len(); got != 1 {
		t.Fatalf("queue = %d, want still the other task's entry", got)
	}
}
