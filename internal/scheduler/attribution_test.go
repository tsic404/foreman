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
			source: FactSourceJob,
			hit:    true,
		},
		{
			name:   "pod container reason is the fallback",
			job:    jobFailedWith("fm-a", "BackoffLimitExceeded"),
			pods:   []corev1.Pod{deadlinePod("fm-a", since)},
			source: FactSourcePod,
			hit:    true,
		},
		{
			name:   "a missing Job does not stop the pod read",
			pods:   []corev1.Pod{deadlinePod("fm-a", since)},
			hit:    true,
			source: FactSourcePod,
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
	if decided[0]["decision"] != DecisionPlatform || decided[0]["fact_source"] != FactSourceJob {
		t.Fatalf("attribution_decided = %+v, want decision=platform fact_source=job", decided[0])
	}
	if events := g.events("task.failed_compensated"); len(events) != 1 || events[0]["failure_reason"] != FailureReasonJobDeadline {
		t.Fatalf("failed_compensated events = %+v, want one with the deadline reason", events)
	}
}

// TestOnReportAttributionGateWaitsForTheFact covers the wait face: the fact
// is not visible yet, the pod already carries a platform deletion, and the
// gate re-reads until the fact appears (bounded by attributionWait).
func TestOnReportAttributionGateWaitsForTheFact(t *testing.T) {
	g := newGateFixture(t)
	e := g.runningTask(t, "task-1", "running")
	g.sched.attributionWait = 200 * time.Millisecond

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
	if len(decided) != 1 || decided[0]["decision"] != DecisionPlatform {
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
	if len(decided) != 1 || decided[0]["decision"] != DecisionDegraded {
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
	if len(decided) != 1 || decided[0]["decision"] != DecisionDegraded {
		t.Fatalf("attribution_decided = %+v, want decision=degraded", decided)
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
	if len(decided) != 1 || decided[0]["decision"] != DecisionDaemonSelfReport {
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
	if len(decided) != 1 || decided[0]["decision"] != DecisionReadError {
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
	if len(decided) != 1 || decided[0]["decision"] != DecisionC13Settled {
		t.Fatalf("attribution_decided = %+v, want decision=c13_settled", decided)
	}
	if events := g.events("task.failed_compensated"); len(events) != 0 {
		t.Fatalf("failed_compensated events = %+v, want none for a settled task", events)
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

	code, _, err := f.sched.DeliverQueued(context.Background(), EPFail, "task-1", []byte(`{"failure_reason":"job_failed"}`), "")
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

	code, _, err := f.sched.DeliverQueued(context.Background(), EPFail, "task-1", []byte(`{"failure_reason":"job_failed"}`), "")
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

	code, _, err := f.sched.DeliverQueued(context.Background(), EPFail, "task-1", []byte(`{"error":"queued daemon body"}`), "")
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
