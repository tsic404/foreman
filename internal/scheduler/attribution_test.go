package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/state"
)

// ---- the single platform-fact judgement (ADR-031) ----

func deadlineJob(name string, created time.Time) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.Time{Time: created}},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded",
		}}},
	}
}

func jobFailedWith(name, reason string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: reason,
		}}},
	}
}

// budgetJob is a Job that declares a deadline budget without a termination
// condition yet: the overdue wait signal's shape.
func budgetJob(name string, created time.Time, budget int64) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.Time{Time: created}},
		Spec:       batchv1.JobSpec{ActiveDeadlineSeconds: &budget},
	}
}

func deadlinePod(name string, created time.Time) corev1.Pod {
	pod := testPod(name, corev1.PodFailed)
	pod.CreationTimestamp = metav1.Time{Time: created}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "DeadlineExceeded"}},
	}}
	return pod
}

// TestPlatformTerminationFixedOrder pins §4.4's fixed judgement order: the
// Job's Failed condition first, the pod's container termination only on a
// miss. A nil Job is a miss, never an error, and the instance filter drops a
// reused name's predecessor pod.
func TestPlatformTerminationFixedOrder(t *testing.T) {
	since := testNow.Add(-time.Hour)
	tests := []struct {
		name   string
		job    *batchv1.Job
		pods   []corev1.Pod
		source string
		hit    bool
	}{
		{
			name:   "job condition wins over the pod",
			job:    deadlineJob("fm-a", since),
			pods:   []corev1.Pod{testPod("fm-a", corev1.PodFailed)},
			source: "job",
			hit:    true,
		},
		{
			name:   "pod container reason is the fallback",
			job:    jobFailedWith("fm-a", "BackoffLimitExceeded"),
			pods:   []corev1.Pod{deadlinePod("fm-a", since)},
			source: "pod",
			hit:    true,
		},
		{
			name:   "a missing Job does not stop the pod read",
			pods:   []corev1.Pod{deadlinePod("fm-a", since)},
			hit:    true,
			source: "pod",
		},
		{name: "no objects is a miss", job: nil},
		{
			name: "another reason is a miss",
			job:  jobFailedWith("fm-a", "BackoffLimitExceeded"),
		},
		{
			name: "a predecessor pod is filtered out",
			pods: []corev1.Pod{deadlinePod("fm-a", since.Add(-time.Second))},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source, hit := PlatformTermination(tt.job, tt.pods, since)
			if hit != tt.hit || source != tt.source {
				t.Fatalf("PlatformTermination = (%q, %v), want (%q, %v)", source, hit, tt.source, tt.hit)
			}
		})
	}
}

// TestAttributionEventValueLiterals pins the task.attribution_decided value
// set to its literals (§9.1, ADR-031 observation face): consumers read
// decision and fact_source by value, so a drifted constant would rewrite the
// event's meaning while every branch test still compares symbol to symbol.
func TestAttributionEventValueLiterals(t *testing.T) {
	for want, got := range map[string]string{
		"platform":           DecisionPlatform,
		"degraded":           DecisionDegraded,
		"read_error":         DecisionReadError,
		"c13_settled":        DecisionC13Settled,
		"daemon_self_report": DecisionDaemonSelfReport,
	} {
		if got != want {
			t.Errorf("decision value = %q, want the emitted literal %q", got, want)
		}
	}
	for want, got := range map[string]string{
		"job": FactSourceJob,
		"pod": FactSourcePod,
	} {
		if got != want {
			t.Errorf("fact_source value = %q, want the emitted literal %q", got, want)
		}
	}
}

// ---- the S14 attribution gate ----

// gateFixture is a claimed task with a Job in the cluster and a log sink the
// gate's events can be read from.
type gateFixture struct {
	*fixture
	logs *strings.Builder
}

func newGateFixture(t *testing.T) *gateFixture {
	t.Helper()
	f := newFixture(t)
	buf := &strings.Builder{}
	f.sched.log = slog.New(slog.NewJSONHandler(buf, nil)).With("component", "scheduler")
	return &gateFixture{fixture: f, logs: buf}
}

// stepClock advances the fixture clock by step on every scheduler reading.
// The gate's wait bound runs on that clock, the same source as the overdue
// signal, so a bounded wait expires in scheduler time while the poll cadence
// stays a real timer: no wall-clock sleep has to be waited out.
func (g *gateFixture) stepClock(step time.Duration) {
	g.sched.now = func() time.Time {
		now := g.now
		g.now = now.Add(step)
		return now
	}
}

// runningTask claims a task and programs the C13 answer the gate consults.
func (g *gateFixture) runningTask(t *testing.T, taskID, status string) registry.TaskEntry {
	t.Helper()
	e := g.claim(t, taskID)
	g.server.status = status
	g.jobs.existingJobs = map[string]*batchv1.Job{}
	return e
}

// events returns the gate's structured log records carrying msg.
func (g *gateFixture) events(msg string) []map[string]any {
	var found []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(g.logs.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record["msg"] == msg {
			found = append(found, record)
		}
	}
	return found
}

// TestOnReportAttributionGateDecidesPlatform covers §4.4's gate path (AC-08
// form A): the platform fact holds, C13 is not terminal, and the daemon's
// failure self-report is replaced by the compensation body — the decided
// failure reason and the session value included.
func TestOnReportAttributionGateDecidesPlatform(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.jobs.existingJobs[e.JobName] = deadlineJob(e.JobName, e.JobCreatedAt)

	daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error","session_id":"sess-1"}`)
	code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "sess-1")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}

	forwards := g.server.forwards
	if len(forwards) != 1 || forwards[0].ep != EPFail {
		t.Fatalf("forwards = %+v, want one fail report", forwards)
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(forwards[0].body), &body); err != nil {
		t.Fatalf("forward body: %v", err)
	}
	if body["failure_reason"] != FailureReasonJobDeadline {
		t.Fatalf("failure_reason = %q, want %q (the daemon body must be suppressed)", body["failure_reason"], FailureReasonJobDeadline)
	}
	if body["session_id"] != "sess-1" {
		t.Fatalf("session_id = %q, want the learning slot's raw value carried back", body["session_id"])
	}
	if body["error"] == "agent crashed" {
		t.Fatal("the daemon's failure text must be replaced by the compensation body")
	}

	settled, ok := g.reg.Get("task-1")
	if !ok || !settled.IsTerminal() || settled.Result != registry.ResultFailed || settled.FailureReason != FailureReasonJobDeadline {
		t.Fatalf("entry = %+v, want terminal failed with %q", settled, FailureReasonJobDeadline)
	}
	if string(settled.DecidedReport) != forwards[0].body {
		t.Fatalf("decided body = %q, want the forwarded compensation body %q", settled.DecidedReport, forwards[0].body)
	}
	if !g.jobs.deleted(e.JobName) {
		t.Fatal("the Job must be deleted once the compensation body landed")
	}

	decided := g.events("task.attribution_decided")
	if len(decided) != 1 {
		t.Fatalf("attribution_decided events = %+v, want one", decided)
	}
	if decided[0]["decision"] != "platform" || decided[0]["fact_source"] != "job" {
		t.Fatalf("attribution_decided = %+v, want decision=platform fact_source=job", decided[0])
	}
	if events := g.events("task.failed_compensated"); len(events) != 1 || events[0]["failure_reason"] != FailureReasonJobDeadline {
		t.Fatalf("failed_compensated events = %+v, want one with the deadline reason", events)
	}
}

// TestOnReportAttributionGateSettlementFailureFallsBackToDaemon covers the
// gate's error exit: the platform fact holds but the settlement write fails,
// so the daemon's own report is the verdict that stands. The event must name
// that decision exactly once — a platform event would describe a verdict
// whose write never landed.
func TestOnReportAttributionGateSettlementFailureFallsBackToDaemon(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.jobs.existingJobs[e.JobName] = deadlineJob(e.JobName, e.JobCreatedAt)

	armed, removed, restored := false, false, false
	var hookErr error
	g.jobs.onGetJob = func() { armed = true }
	// The evidence read precedes the settlement: the next clock read is
	// settleFailure's, whose MarkTerminal then finds no entry. The read after
	// that is OnReport's, once the gate has handed the report back as
	// undecided; the entry is back before the report's terminal write.
	g.sched.now = func() time.Time {
		switch {
		case armed && !removed:
			removed = true
			hookErr = g.reg.Delete("task-1")
		case removed && !restored:
			restored = true
			hookErr = g.reg.Put(e)
		}
		return g.now
	}

	daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error"}`)
	code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "sess-1")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if hookErr != nil {
		t.Fatalf("arming the failed settlement: %v", hookErr)
	}
	if !removed || !restored {
		t.Fatalf("the failed settlement never ran (removed=%v restored=%v)", removed, restored)
	}
	if events := g.events("task.settle_failed"); len(events) != 1 {
		t.Fatalf("settle_failed events = %+v, want one", events)
	}
	if events := g.events("task.failed_compensated"); len(events) != 0 {
		t.Fatalf("failed_compensated events = %+v, want none: the settlement did not land", events)
	}
	if forwards := g.server.forwards; len(forwards) != 1 || forwards[0].body != string(daemonBody) {
		t.Fatalf("forwards = %+v, want the daemon body %q", forwards, daemonBody)
	}
	decided := g.events("task.attribution_decided")
	if len(decided) != 1 {
		t.Fatalf("attribution_decided events = %+v, want exactly one", decided)
	}
	if decided[0]["decision"] != "daemon_self_report" {
		t.Fatalf("decision = %v, want %q", decided[0]["decision"], "daemon_self_report")
	}
	if _, ok := decided[0]["fact_source"]; ok {
		t.Fatalf("attribution_decided = %+v, want no fact_source on the daemon branch", decided[0])
	}
	if settled, ok := g.reg.Get("task-1"); !ok || settled.Result != registry.ResultFailed {
		t.Fatalf("entry = %+v, want the daemon report's failed verdict", settled)
	}
}

// TestOnReportAttributionGateWaitsForTheFact covers the wait face: the fact
// is not visible yet, the pod already carries a platform deletion, and the
// gate re-reads until the fact appears (bounded by attributionWait). The
// budget is larger than one poll interval so C13 still has room after the
// wait — the wait and the C13 re-check share it (§10).
func TestOnReportAttributionGateWaitsForTheFact(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.sched.attributionWait = 600 * time.Millisecond

	pod := testPod(e.JobName, corev1.PodRunning)
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	g.jobs.pods = []corev1.Pod{pod}

	reads := 0
	g.jobs.onGetJob = func() {
		reads++
		if reads > 1 {
			g.jobs.existingJobs[e.JobName] = deadlineJob(e.JobName, e.JobCreatedAt)
		}
	}

	code, _, err := g.sched.OnReport(context.Background(), EPFail, e, []byte(`{"error":"agent crashed"}`), "")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if reads < 2 {
		t.Fatalf("evidence reads = %d, want the gate to re-read while waiting", reads)
	}
	if got := g.server.forwards[0].body; !strings.Contains(got, FailureReasonJobDeadline) {
		t.Fatalf("forward body = %q, want the platform attribution", got)
	}
	decided := g.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != "platform" {
		t.Fatalf("attribution_decided = %+v, want decision=platform", decided)
	}
}

// TestOnReportAttributionGateWaitsForTheJobBudget covers the second wait
// signal: the instance Job declares a deadline budget and the apiserver
// creation instant plus that budget lies in the past, so the gate waits even
// without a pod deletion — and, with no fact, degrades visibly.
func TestOnReportAttributionGateWaitsForTheJobBudget(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.sched.attributionWait = 30 * time.Millisecond
	g.stepClock(10 * time.Millisecond)
	g.jobs.existingJobs[e.JobName] = budgetJob(e.JobName, e.JobCreatedAt, 60)

	reads := 0
	g.jobs.onGetJob = func() { reads++ }

	daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error"}`)
	code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if reads < 2 {
		t.Fatalf("evidence reads = %d, want the gate to wait on the overdue budget", reads)
	}
	if got := g.server.forwards[0].body; got != string(daemonBody) {
		t.Fatalf("forward body = %q, want the daemon's own body", got)
	}
	decided := g.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != "degraded" {
		t.Fatalf("attribution_decided = %+v, want decision=degraded", decided)
	}
}

// TestOnReportAttributionGateDegraded covers the bound's failure face: a wait
// signal held, the fact never appeared, so the self-report is released and
// the degradation is visible.
func TestOnReportAttributionGateDegraded(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.sched.attributionWait = 30 * time.Millisecond
	g.stepClock(10 * time.Millisecond)

	pod := testPod(e.JobName, corev1.PodRunning)
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	g.jobs.pods = []corev1.Pod{pod}

	daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error"}`)
	code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if got := g.server.forwards[0].body; got != string(daemonBody) {
		t.Fatalf("forward body = %q, want the daemon's own body", got)
	}
	settled, _ := g.reg.Get("task-1")
	if !settled.IsTerminal() || settled.Result != registry.ResultFailed || settled.FailureReason != "" {
		t.Fatalf("entry = %+v, want the daemon's own failed verdict", settled)
	}
	decided := g.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != "degraded" {
		t.Fatalf("attribution_decided = %+v, want decision=degraded", decided)
	}
}

// TestOnReportAttributionWaitBoundFollowsTheSchedulerClock pins the bound to
// the same clock as the overdue signal: a reading that is already past the
// bound in scheduler time ends the wait without sleeping out the bound in
// wall time. A wall-clock bound would spend the whole attributionWait here.
func TestOnReportAttributionWaitBoundFollowsTheSchedulerClock(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.sched.attributionWait = 3 * time.Second
	// Every reading lands past the bound the previous reading computed.
	g.stepClock(2 * g.sched.attributionWait)

	pod := testPod(e.JobName, corev1.PodRunning)
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	g.jobs.pods = []corev1.Pod{pod}

	daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error"}`)
	start := time.Now()
	code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("gate waited %s: the bound must read the scheduler clock, not the wall clock", elapsed)
	}
	if got := g.server.forwards[0].body; got != string(daemonBody) {
		t.Fatalf("forward body = %q, want the daemon's own body", got)
	}
	decided := g.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != "degraded" {
		t.Fatalf("attribution_decided = %+v, want decision=degraded", decided)
	}
}

// TestDefaultAttributionWaitStaysUnderTheDaemonControlPlaneTimeout pins the
// gate's wait upper bound (ADR-031): 15s as the implementation constant, and
// strictly under the Job daemon's 30s control-plane timeout so a gated
// response never lands in the daemon's retry face. The value is not a config
// item — a drift is a contract change, not a tuning choice.
func TestDefaultAttributionWaitStaysUnderTheDaemonControlPlaneTimeout(t *testing.T) {
	const daemonControlPlaneTimeout = 30 * time.Second
	if defaultAttributionWait != 15*time.Second {
		t.Fatalf("defaultAttributionWait = %v, want 15s", defaultAttributionWait)
	}
	if defaultAttributionWait >= daemonControlPlaneTimeout {
		t.Fatalf("defaultAttributionWait = %v, want < %v (the Job daemon's control-plane timeout)",
			defaultAttributionWait, daemonControlPlaneTimeout)
	}
}

// TestOnReportAttributionGateDaemonSelfReport: neither wait signal holds, so
// the report is forwarded at once and the attribution stays with the daemon.
func TestOnReportAttributionGateDaemonSelfReport(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.jobs.existingJobs[e.JobName] = jobFailedWith(e.JobName, "BackoffLimitExceeded")

	daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error"}`)
	start := time.Now()
	code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("gate waited %s with no wait signal", elapsed)
	}
	if got := g.server.forwards[0].body; got != string(daemonBody) {
		t.Fatalf("forward body = %q, want the daemon's own body", got)
	}
	decided := g.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != "daemon_self_report" {
		t.Fatalf("attribution_decided = %+v, want decision=daemon_self_report", decided)
	}
}

// TestOnReportAttributionGateReadError: a K8s read that fails is never a
// teardown sign — the report goes out as it arrived, with the failure
// recorded.
func TestOnReportAttributionGateReadError(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.jobs.getJobErr = errors.New("apiserver down")

	daemonBody := []byte(`{"error":"agent crashed"}`)
	code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if got := g.server.forwards[0].body; got != string(daemonBody) {
		t.Fatalf("forward body = %q, want the daemon's own body", got)
	}
	decided := g.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != "read_error" {
		t.Fatalf("attribution_decided = %+v, want decision=read_error", decided)
	}
}

// TestOnReportAttributionGateC13Settled: the server already holds a verdict,
// so the platform fact does not rewrite it — the daemon body is forwarded
// verbatim.
func TestOnReportAttributionGateC13Settled(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "completed")
	g.jobs.existingJobs[e.JobName] = deadlineJob(e.JobName, e.JobCreatedAt)

	daemonBody := []byte(`{"error":"agent crashed"}`)
	code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if got := g.server.forwards[0].body; got != string(daemonBody) {
		t.Fatalf("forward body = %q, want the daemon's own body", got)
	}
	decided := g.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != "c13_settled" {
		t.Fatalf("attribution_decided = %+v, want decision=c13_settled", decided)
	}
	if events := g.events("task.failed_compensated"); len(events) != 0 {
		t.Fatalf("failed_compensated events = %+v, want none for a settled task", events)
	}
}

// TestOnReportAttributionGateC13SpendsTheRemainingBudget pins §10's gate-path
// bound (ADR-031): the wait and the C13 re-check share one attributionWait
// budget, so a control plane that never answers cannot push the S14 round
// past it — the gate lands on its undecidable branch instead.
func TestOnReportAttributionGateC13SpendsTheRemainingBudget(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.sched.attributionWait = 700 * time.Millisecond
	// The deployment's lease shape: the terminal write must still fit on the
	// shared store after the gate's share is spent.
	g.sched.shared = &idleTaskLockStore{}
	g.sched.instanceID = "instance-1"

	// One wait signal holds and the fact shows up only on the second read:
	// one poll interval of the gate's share is gone before C13 is consulted.
	pod := testPod(e.JobName, corev1.PodRunning)
	deleted := metav1.Now()
	pod.DeletionTimestamp = &deleted
	g.jobs.pods = []corev1.Pod{pod}
	reads := 0
	g.jobs.onGetJob = func() {
		reads++
		if reads > 1 {
			g.jobs.existingJobs[e.JobName] = deadlineJob(e.JobName, e.JobCreatedAt)
		}
	}
	statusCalled := false
	g.server.onStatus = func() { statusCalled = true }
	g.server.statusBlocks = true

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error"}`)
	start := time.Now()
	code, _, err := g.sched.OnReport(ctx, EPFail, e, daemonBody, "")
	elapsed := time.Since(start)
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if !statusCalled {
		t.Fatal("C13 was never consulted: the fact was visible and the budget had room")
	}
	if elapsed > g.sched.attributionWait+250*time.Millisecond {
		t.Fatalf("S14 round took %s, want it inside the attribution budget %s", elapsed, g.sched.attributionWait)
	}
	if got := g.server.forwards[0].body; got != string(daemonBody) {
		t.Fatalf("forward body = %q, want the daemon's own body on the undecidable branch", got)
	}
	decided := g.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != "read_error" || decided[0]["error_source"] != "c13" {
		t.Fatalf("attribution_decided = %+v, want decision=read_error error_source=c13", decided)
	}
}

// TestOnReportAttributionGateSkipsC13WhenTheBudgetIsSpent: an evidence read
// the budget cuts off leaves nothing for C13 — the gate makes no call rather
// than opening a fresh control-plane timeout behind the read (§10, ADR-031).
// The fake read blocks until its context ends, so a read that went out on the
// request context instead of the gate's would outlive the budget and fail the
// elapsed assertion.
func TestOnReportAttributionGateSkipsC13WhenTheBudgetIsSpent(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.sched.attributionWait = 200 * time.Millisecond
	// The deployment's lease shape: the degraded terminal write must still
	// take the lease on the reserve, or the round ends 503 instead of 200.
	g.sched.shared = &idleTaskLockStore{}
	g.sched.instanceID = "instance-1"

	// The read never answers on its own: only the gate's budget ends it.
	blocked := make(chan struct{})
	defer close(blocked)
	g.jobs.blockGetJob = blocked
	statusCalled := false
	g.server.onStatus = func() { statusCalled = true }
	g.server.statusBlocks = true

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error"}`)
	start := time.Now()
	code, _, err := g.sched.OnReport(ctx, EPFail, e, daemonBody, "")
	elapsed := time.Since(start)
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if statusCalled {
		t.Fatal("C13 must not be consulted once the budget is spent")
	}
	if elapsed > g.sched.attributionWait+250*time.Millisecond {
		t.Fatalf("S14 round took %s, want the read cut at the attribution budget %s", elapsed, g.sched.attributionWait)
	}
	if got := g.server.forwards[0].body; got != string(daemonBody) {
		t.Fatalf("forward body = %q, want the daemon's own body on the undecidable branch", got)
	}
	decided := g.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != "read_error" || decided[0]["error_source"] != "platform_fact" {
		t.Fatalf("attribution_decided = %+v, want decision=read_error error_source=platform_fact", decided)
	}
}

// TestOnReportAttributionGateWaitFailureMapping pins what ends a wait that
// never produced a fact: the budget's own expiry is the degraded face (the
// instance could not observe), while a read that broke on its own is the
// environment face (read_error). Both leave the report to the daemon.
func TestOnReportAttributionGateWaitFailureMapping(t *testing.T) {
	tests := []struct {
		name        string
		arm         func(*gateFixture)
		decision    string
		errorSource string
	}{
		{
			name: "the budget ends the wait",
			arm: func(g *gateFixture) {
				// The fixture clock never reaches the bound, so the wall-clock
				// budget is what ends the wait.
				g.sched.attributionWait = 200 * time.Millisecond
			},
			decision: "degraded",
		},
		{
			name: "the re-read fails",
			arm: func(g *gateFixture) {
				g.sched.attributionWait = 600 * time.Millisecond
				reads := 0
				g.jobs.onGetJob = func() {
					reads++
					if reads == 2 {
						g.jobs.getJobErr = errors.New("apiserver down")
					}
				}
			},
			decision:    "read_error",
			errorSource: "platform_fact",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newGateFixture(t)
			e := g.runningTask(t, "task-1", "running")
			// The deployment's lease shape: the expired-budget exits must
			// write the verdict on the reserve, not fail the shared lease.
			g.sched.shared = &idleTaskLockStore{}
			g.sched.instanceID = "instance-1"
			pod := testPod(e.JobName, corev1.PodRunning)
			now := metav1.Now()
			pod.DeletionTimestamp = &now
			g.jobs.pods = []corev1.Pod{pod}
			tt.arm(g)

			daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error"}`)
			start := time.Now()
			code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "")
			elapsed := time.Since(start)
			if err != nil || code != 200 {
				t.Fatalf("OnReport = %d, %v", code, err)
			}
			if elapsed > g.sched.attributionWait+250*time.Millisecond {
				t.Fatalf("S14 round took %s, want it inside the attribution budget %s", elapsed, g.sched.attributionWait)
			}
			if got := g.server.forwards[0].body; got != string(daemonBody) {
				t.Fatalf("forward body = %q, want the daemon's own body", got)
			}
			decided := g.events("task.attribution_decided")
			if len(decided) != 1 || decided[0]["decision"] != tt.decision {
				t.Fatalf("attribution_decided = %+v, want decision=%s", decided, tt.decision)
			}
			if got, _ := decided[0]["error_source"].(string); got != tt.errorSource {
				t.Fatalf("error_source = %q, want %q", got, tt.errorSource)
			}
			settled, _ := g.reg.Get("task-1")
			if !settled.IsTerminal() || settled.Result != registry.ResultFailed || settled.FailureReason != "" {
				t.Fatalf("entry = %+v, want the daemon's own failed verdict", settled)
			}
		})
	}
}

// TestOnReportAttributionGateWritesTheVerdictOnAnExpiredBudget: the wait
// signal holds and the fact never appears, so the gate spends its share of
// attributionWait and releases the self-report as degraded. On the
// deployment's shared lease the caller's terminal write must still land: the
// gate stopped one reserve short, so the verdict, the queued report and the
// decision event all follow instead of failing closed on an expired deadline
// (§10, ADR-031).
func TestOnReportAttributionGateWritesTheVerdictOnAnExpiredBudget(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.sched.attributionWait = 400 * time.Millisecond
	// A free lease that still refuses an expired caller: the local mutex
	// shape cannot show this, the deployed shared store is exactly this.
	g.sched.shared = &idleTaskLockStore{}
	g.sched.instanceID = "instance-1"
	g.sched.taskLockTTL = time.Second
	pod := testPod(e.JobName, corev1.PodRunning)
	deleted := metav1.Now()
	pod.DeletionTimestamp = &deleted
	g.jobs.pods = []corev1.Pod{pod}

	daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error"}`)
	start := time.Now()
	code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "")
	elapsed := time.Since(start)
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v (a spent gate budget must not fail the write closed)", code, err)
	}
	if elapsed > g.sched.attributionWait+250*time.Millisecond {
		t.Fatalf("S14 round took %s, want it inside the attribution budget %s", elapsed, g.sched.attributionWait)
	}
	if got := g.server.forwards[0].body; got != string(daemonBody) {
		t.Fatalf("forward body = %q, want the daemon's own body", got)
	}
	queued := g.pending.reports
	if len(queued) != 1 || queued[0].ep != EPFail || queued[0].body != string(daemonBody) {
		t.Fatalf("pending reports = %+v, want the daemon's body queued for replay", queued)
	}
	settled, _ := g.reg.Get("task-1")
	if !settled.IsTerminal() || settled.Result != registry.ResultFailed || settled.FailureReason != "" {
		t.Fatalf("entry = %+v, want the daemon's own failed verdict", settled)
	}
	decided := g.events("task.attribution_decided")
	if len(decided) != 1 || decided[0]["decision"] != "degraded" {
		t.Fatalf("attribution_decided = %+v, want decision=degraded", decided)
	}
	if source, present := decided[0]["fact_source"]; present && source != "" {
		t.Fatalf("attribution_decided fact_source = %v, want none on the degraded branch", source)
	}
}

// TestTaskStatusWithinMakesNoCallOnASpentBudget pins C13's precheck: once the
// gate path's deadline has passed, the re-check makes no control-plane call
// at all (§10, ADR-031).
func TestTaskStatusWithinMakesNoCallOnASpentBudget(t *testing.T) {
	g := newGateFixture(t)
	called := false
	g.server.onStatus = func() { called = true }
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	if _, err := g.sched.taskStatusWithin(ctx, "task-1"); err == nil || !strings.Contains(err.Error(), "attribution budget exhausted") {
		t.Fatalf("taskStatusWithin err = %v, want the budget-exhausted refusal", err)
	}
	if called {
		t.Fatal("C13 must not be called once the budget is spent")
	}
}

// heldTaskLockStore models a task lease another instance holds: an
// acquisition is refused until the hold runs out, and the caller's own
// context ends the wait when it comes first. The embedded interface covers
// the faces this scenario never reaches.
type heldTaskLockStore struct {
	state.CoordStore
	until time.Time
}

func (s *heldTaskLockStore) LockTask(ctx context.Context, _, _, _ string, _ time.Duration) (bool, error) {
	for {
		if !time.Now().Before(s.until) {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (s *heldTaskLockStore) UnlockTask(context.Context, string, string, string) error { return nil }

func (s *heldTaskLockStore) Slots(context.Context, string) (int64, error) { return 0, nil }

// idleTaskLockStore is an available shared store in the deployment's shape:
// the lease is free, so any acquisition succeeds — but only while the
// caller's context still stands. An expired deadline is refused exactly as
// the real store refuses it, which is what the local mutex shape cannot show.
type idleTaskLockStore struct {
	state.CoordStore
}

func (s *idleTaskLockStore) LockTask(ctx context.Context, _, _, _ string, _ time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *idleTaskLockStore) UnlockTask(context.Context, string, string, string) error { return nil }

func (s *idleTaskLockStore) Slots(context.Context, string) (int64, error) { return 0, nil }

// TestOnReportAttributionGateCutsTheLockWaitAtTheBudget: another instance
// holds the task lease past the gate's budget. The gate path — lock wait
// included — is bounded by that budget, so the gate gives up on the lock and
// takes the decided-elsewhere exit: it neither waits the lease out nor
// settles the instance on a snapshot it does not own (§10, ADR-031). The
// caller writes nothing on that exit, so it must not take the lease again
// either — the round ends at the gate's budget, not at the lease's end.
func TestOnReportAttributionGateCutsTheLockWaitAtTheBudget(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.sched.attributionWait = 150 * time.Millisecond
	// The platform fact holds: a gate that reaches the lock inside the budget
	// would settle the instance with the platform verdict.
	g.jobs.existingJobs[e.JobName] = deadlineJob(e.JobName, e.JobCreatedAt)
	g.server.status = "running"
	// The lease outlives the gate's budget; the caller must not wait it out
	// for a write it does not perform.
	g.sched.shared = &heldTaskLockStore{until: time.Now().Add(600 * time.Millisecond)}
	g.sched.instanceID = "instance-1"
	g.sched.taskLockTTL = time.Second

	daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error"}`)
	start := time.Now()
	code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "")
	elapsed := time.Since(start)
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if elapsed > g.sched.attributionWait+250*time.Millisecond {
		t.Fatalf("S14 round took %s, want the gate budget %s to bound it, not the lease", elapsed, g.sched.attributionWait)
	}
	if decided := g.events("task.attribution_decided"); len(decided) != 0 {
		t.Fatalf("attribution_decided = %+v, want none: the gate never held the lock", decided)
	}
	if got := g.server.forwards[0].body; got != string(daemonBody) {
		t.Fatalf("forward body = %q, want the daemon's own body", got)
	}
	if settled, ok := g.reg.Get("task-1"); !ok || settled.IsTerminal() {
		t.Fatalf("entry = %+v, want a non-terminal entry: the gate must not decide without the lock", settled)
	}
}

// TestOnReportDecidedInstanceForwardsItsDecidedBody: an already-decided
// instance never re-enters the gate, and every terminal forward takes the
// registered body — the daemon's original body cannot become the first write.
func TestOnReportDecidedInstanceForwardsItsDecidedBody(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	decided := []byte(`{"error":"platform terminated","failure_reason":"job_deadline_exceeded"}`)
	if _, _, err := g.reg.MarkTerminal("task-1", registry.ResultFailed, FailureReasonJobDeadline, testNow); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if _, ok := g.reg.RegisterDecidedReport("task-1", decided); !ok {
		t.Fatal("RegisterDecidedReport: entry missing")
	}
	// A platform fact is visible, but the gate must not face a decided entry.
	g.jobs.existingJobs[e.JobName] = deadlineJob(e.JobName, e.JobCreatedAt)

	if _, _, err := g.sched.OnReport(context.Background(), EPFail, e, []byte(`{"error":"late daemon body"}`), ""); err != nil {
		t.Fatalf("OnReport: %v", err)
	}
	if got := g.server.forwards[0].body; got != string(decided) {
		t.Fatalf("forward body = %q, want the decided body %q", got, decided)
	}
	if events := g.events("task.attribution_decided"); len(events) != 0 {
		t.Fatalf("attribution_decided events = %+v, want none for a decided replay", events)
	}
}

// TestOnReportTerminal404SettlesWithoutResult: a first terminal report the
// server answers 404 decides in place with no result (§4.4) and cleans up.
func TestOnReportTerminal404SettlesWithoutResult(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	f.server.forwardCode = 404

	code, _, err := f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{"result":"ok"}`), "")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v (a terminal 404 is an idempotent success)", code, err)
	}
	settled, ok := f.reg.Get("task-1")
	if !ok || !settled.IsTerminal() || settled.Result != "" {
		t.Fatalf("entry = %+v, want terminal with no result", settled)
	}
	if !f.jobs.deleted(e.JobName) {
		t.Fatal("the Job must be deleted by the in-place decision")
	}
}

// TestOnReportTerminal404KeepsADecidedResult: an already-decided instance
// answers 200 without rewriting its result.
func TestOnReportTerminal404KeepsADecidedResult(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, "", testNow); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	f.server.forwardCode = 404
	// The inbound face reads the entry fresh, so the replay sees the verdict.
	e, _ = f.reg.Get("task-1")

	if _, _, err := f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{}`), ""); err != nil {
		t.Fatalf("OnReport: %v", err)
	}
	settled, _ := f.reg.Get("task-1")
	if settled.Result != registry.ResultCompleted {
		t.Fatalf("result = %q, want the decided %q", settled.Result, registry.ResultCompleted)
	}
}

// TestDeliverQueued404SettlesWithoutResult: a replay the server answers 404
// means the task no longer exists, so an undecided entry decides in place
// with no result and its objects go.
func TestDeliverQueued404SettlesWithoutResult(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	f.server.forwardCode = 404

	code, _, err := f.sched.DeliverQueued(context.Background(), EPFail, "task-1", "", []byte(`{"failure_reason":"job_failed"}`), "")
	if err != nil || code != 404 {
		t.Fatalf("DeliverQueued = %d, %v", code, err)
	}
	settled, ok := f.reg.Get("task-1")
	if !ok || !settled.IsTerminal() || settled.Result != "" {
		t.Fatalf("entry = %+v, want terminal with no result", settled)
	}
	if !f.jobs.deleted(e.JobName) {
		t.Fatal("the Job must be deleted by the replay's in-place decision")
	}
}

// TestDeliverQueued404KeepsAVerdictDecidedDuringReplay: the replay can stay
// in flight for the whole retry budget, and a settlement path may decide the
// instance in that window. The 404 branch re-reads the entry under the task
// lock, so the fresh verdict stands — the in-place decision must never clear
// a result another path has already written (§4.2, §4.4).
func TestDeliverQueued404KeepsAVerdictDecidedDuringReplay(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	f.server.forwardCode = 404
	decided := []byte(`{"error":"container ended without a terminal report","failure_reason":"job_failed"}`)
	f.server.onForward = func() {
		// The other path settles the instance while the replay is in flight.
		if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultFailed, FailureReasonJobFailed, testNow); err != nil {
			t.Fatalf("MarkTerminal: %v", err)
		}
		if _, ok := f.reg.RegisterDecidedReport("task-1", decided); !ok {
			t.Fatal("RegisterDecidedReport: entry missing")
		}
	}

	code, _, err := f.sched.DeliverQueued(context.Background(), EPFail, "task-1", "", []byte(`{"failure_reason":"job_failed"}`), "")
	if err != nil || code != 404 {
		t.Fatalf("DeliverQueued = %d, %v", code, err)
	}
	settled, ok := f.reg.Get("task-1")
	if !ok || !settled.IsTerminal() {
		t.Fatalf("entry = %+v, want the decided terminal entry", settled)
	}
	if settled.Result != registry.ResultFailed || settled.FailureReason != FailureReasonJobFailed {
		t.Fatalf("entry = result=%q reason=%q, want the verdict decided during the replay", settled.Result, settled.FailureReason)
	}
	if string(settled.DecidedReport) != string(decided) {
		t.Fatalf("decided body = %q, want %q", settled.DecidedReport, decided)
	}
	if !f.jobs.deleted(e.JobName) {
		t.Fatal("the Job must still be deleted by the replay's in-place decision")
	}
}

// TestDeliverQueuedUsesTheDecidedBody: the queue replay is not an inbound
// report — it sends the registered body and never re-runs the gate.
func TestDeliverQueuedUsesTheDecidedBody(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	decided := []byte(`{"error":"platform terminated","failure_reason":"job_deadline_exceeded"}`)
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultFailed, FailureReasonJobDeadline, testNow); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if _, ok := f.reg.RegisterDecidedReport("task-1", decided); !ok {
		t.Fatal("RegisterDecidedReport: entry missing")
	}

	code, _, err := f.sched.DeliverQueued(context.Background(), EPFail, "task-1", "", []byte(`{"error":"queued daemon body"}`), "")
	if err != nil || code != 200 {
		t.Fatalf("DeliverQueued = %d, %v", code, err)
	}
	if got := f.server.forwards[0].body; got != string(decided) {
		t.Fatalf("forward body = %q, want the decided body %q", got, decided)
	}
	if !f.jobs.deleted(e.JobName) {
		t.Fatal("the Job must be deleted once the replay landed")
	}
}

// TestOnReportAttributionGateDecidedDuringWait: the wait opens a window in
// which another path can settle the instance. Whichever exit the gate takes
// afterwards — the platform fact, an expired bound or a failed re-read — it
// forwards the already-decided body, leaves the verdict untouched and emits
// no decision event: a decided instance is never rewritten (§4.4, ADR-031).
func TestOnReportAttributionGateDecidedDuringWait(t *testing.T) {
	tests := []struct {
		name       string
		duringWait func(g *gateFixture, jobName string, created time.Time)
	}{
		{
			name: "the platform fact appears",
			duringWait: func(g *gateFixture, jobName string, created time.Time) {
				g.jobs.existingJobs[jobName] = deadlineJob(jobName, created)
			},
		},
		{
			name:       "the bound expires",
			duringWait: func(*gateFixture, string, time.Time) {},
		},
		{
			name: "the re-read fails",
			duringWait: func(g *gateFixture, _ string, _ time.Time) {
				g.jobs.getJobErr = errors.New("apiserver down")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newGateFixture(t)
			e := g.runningTask(t, "task-1", "running")
			g.sched.attributionWait = 30 * time.Millisecond
			g.stepClock(10 * time.Millisecond)
			pod := testPod(e.JobName, corev1.PodRunning)
			now := metav1.Now()
			pod.DeletionTimestamp = &now
			g.jobs.pods = []corev1.Pod{pod}

			decided := []byte(`{"error":"the other path decided","failure_reason":"job_failed"}`)
			reads := 0
			g.jobs.onGetJob = func() {
				reads++
				if reads != 2 {
					return
				}
				// The convergence path settles the instance while the gate
				// waits: a completed verdict and its decided body.
				if _, _, err := g.reg.MarkTerminal("task-1", registry.ResultCompleted, "", testNow); err != nil {
					t.Fatalf("MarkTerminal: %v", err)
				}
				if _, ok := g.reg.RegisterDecidedReport("task-1", decided); !ok {
					t.Fatal("RegisterDecidedReport: entry missing")
				}
				tt.duringWait(g, e.JobName, e.JobCreatedAt)
			}

			daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error"}`)
			code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "")
			if err != nil || code != 200 {
				t.Fatalf("OnReport = %d, %v", code, err)
			}
			if reads < 2 {
				t.Fatalf("evidence reads = %d, want the gate to wait", reads)
			}
			forwards := g.server.forwards
			if len(forwards) != 1 || forwards[0].body != string(decided) {
				t.Fatalf("forwards = %+v, want the already-decided body %q", forwards, decided)
			}
			settled, ok := g.reg.Get("task-1")
			if !ok || !settled.IsTerminal() || settled.Result != registry.ResultCompleted {
				t.Fatalf("entry = %+v, want the other path's completed verdict", settled)
			}
			if events := g.events("task.attribution_decided"); len(events) != 0 {
				t.Fatalf("attribution_decided events = %+v, want none for an already-decided instance", events)
			}
			if events := g.events("task.failed_compensated"); len(events) != 0 {
				t.Fatalf("failed_compensated events = %+v, want none", events)
			}
			if !g.jobs.deleted(e.JobName) {
				t.Fatal("the Job must be deleted once the decided body landed")
			}
		})
	}
}

// TestOnReportAttributionGateDecisionSupersededBeforeTheWrite: the gate reads
// an undecided instance and retires its lock, and another path settles the
// instance before the caller's terminal write. The gate's decision no longer
// stands, so nothing may announce it: the event names the decision the process
// applied, never one a later write displaced (§9.1, ADR-031).
func TestOnReportAttributionGateDecisionSupersededBeforeTheWrite(t *testing.T) {
	tests := []struct {
		name string
		// arm gives the gate its undecided exit: the decision the other path
		// then displaces.
		arm func(g *gateFixture, e registry.TaskEntry)
	}{
		{
			name: "neither wait signal holds",
			arm: func(g *gateFixture, e registry.TaskEntry) {
				g.jobs.existingJobs[e.JobName] = jobFailedWith(e.JobName, "BackoffLimitExceeded")
			},
		},
		{
			name: "the server already holds a verdict",
			arm: func(g *gateFixture, e registry.TaskEntry) {
				g.jobs.existingJobs[e.JobName] = deadlineJob(e.JobName, e.JobCreatedAt)
				g.server.status = "completed"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newGateFixture(t)
			e := g.runningTask(t, "task-1", "running")
			tt.arm(g, e)

			decided := []byte(`{"error":"the other path decided","failure_reason":"job_failed"}`)
			armed, fired := false, false
			var hookErr error
			g.jobs.onGetJob = func() { armed = true }
			// The evidence read precedes the gate's decision; the next clock
			// read is OnReport's, after the gate returned and before it takes
			// the terminal lock. Settling there lands in exactly that window.
			g.sched.now = func() time.Time {
				if armed && !fired {
					fired = true
					if _, _, err := g.reg.MarkTerminal("task-1", registry.ResultCompleted, "", testNow); err != nil {
						hookErr = err
					} else if _, ok := g.reg.RegisterDecidedReport("task-1", decided); !ok {
						hookErr = errors.New("RegisterDecidedReport: entry missing")
					}
				}
				return g.now
			}

			daemonBody := []byte(`{"error":"agent crashed","failure_reason":"agent_error"}`)
			code, _, err := g.sched.OnReport(context.Background(), EPFail, e, daemonBody, "")
			if err != nil || code != 200 {
				t.Fatalf("OnReport = %d, %v", code, err)
			}
			if hookErr != nil {
				t.Fatalf("the competing path failed to decide: %v", hookErr)
			}
			if !fired {
				t.Fatal("the competing decision never landed between the gate's read and the terminal write")
			}
			if forwards := g.server.forwards; len(forwards) != 1 || forwards[0].body != string(decided) {
				t.Fatalf("forwards = %+v, want the decided body %q", forwards, decided)
			}
			settled, ok := g.reg.Get("task-1")
			if !ok || settled.Result != registry.ResultCompleted {
				t.Fatalf("entry = %+v, want the other path's completed verdict", settled)
			}
			if events := g.events("task.attribution_decided"); len(events) != 0 {
				t.Fatalf("attribution_decided events = %+v, want none for a superseded decision", events)
			}
		})
	}
}

// TestOnReportStaleSnapshotDoesNotRewriteADecidedInstance: the entry the
// caller hands in may predate another path's verdict. The terminal write
// re-reads it under the task lock, so a decided instance keeps its result —
// including on the 404 face, where the in-place decision must not clear it.
func TestOnReportStaleSnapshotDoesNotRewriteADecidedInstance(t *testing.T) {
	tests := []struct {
		name        string
		forwardCode int
	}{
		{name: "the report lands", forwardCode: 200},
		{name: "the server answers 404", forwardCode: 404},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newGateFixture(t)
			e := g.runningTask(t, "task-1", "running")
			g.server.forwardCode = tt.forwardCode
			decided := []byte(`{"error":"the other path decided","failure_reason":"job_failed"}`)
			if _, _, err := g.reg.MarkTerminal("task-1", registry.ResultCompleted, "", testNow); err != nil {
				t.Fatalf("MarkTerminal: %v", err)
			}
			if _, ok := g.reg.RegisterDecidedReport("task-1", decided); !ok {
				t.Fatal("RegisterDecidedReport: entry missing")
			}
			// e is the pre-verdict snapshot the caller read.
			code, _, err := g.sched.OnReport(context.Background(), EPFail, e, []byte(`{"error":"agent crashed"}`), "")
			if err != nil || code != 200 {
				t.Fatalf("OnReport = %d, %v", code, err)
			}
			settled, _ := g.reg.Get("task-1")
			if settled.Result != registry.ResultCompleted {
				t.Fatalf("result = %q, want the decided %q", settled.Result, registry.ResultCompleted)
			}
			if got := g.server.forwards[0].body; got != string(decided) {
				t.Fatalf("forward body = %q, want the decided body %q", got, decided)
			}
			if events := g.events("task.attribution_decided"); len(events) != 0 {
				t.Fatalf("attribution_decided events = %+v, want none", events)
			}
		})
	}
}
