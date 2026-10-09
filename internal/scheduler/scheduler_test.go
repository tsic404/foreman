package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
)

var testNow = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

// ---- fakes ----

type fakeJobs struct {
	jobs []batchv1.Job

	createdJobs    []*batchv1.Job
	createdSecrets []*corev1.Secret
	deletedJobs    []string
	deletedSecrets []string

	createJobErr    error
	createSecretErr error
	deleteJobErr    error
}

func (f *fakeJobs) CreateJob(_ context.Context, job *batchv1.Job) error {
	if f.createJobErr != nil {
		return f.createJobErr
	}
	f.createdJobs = append(f.createdJobs, job)
	return nil
}

func (f *fakeJobs) DeleteJob(_ context.Context, name string) error {
	if f.deleteJobErr != nil {
		return f.deleteJobErr
	}
	f.deletedJobs = append(f.deletedJobs, name)
	return nil
}

func (f *fakeJobs) CreateSecret(_ context.Context, secret *corev1.Secret) error {
	if f.createSecretErr != nil {
		return f.createSecretErr
	}
	f.createdSecrets = append(f.createdSecrets, secret)
	return nil
}

func (f *fakeJobs) DeleteSecret(_ context.Context, name string) error {
	f.deletedSecrets = append(f.deletedSecrets, name)
	return nil
}

func (f *fakeJobs) ListJobs(context.Context) ([]batchv1.Job, error) { return f.jobs, nil }

func (f *fakeJobs) WatchJobs(context.Context, JobEventHandler) error { return nil }

type forwardCall struct {
	ep     Endpoint
	taskID string
	body   string
}

type fakeServer struct {
	status    string
	statusErr error
	onStatus  func() // test hook: fires inside TaskStatus (simulates mid-C13 races)

	forwardCode int
	forwardErr  error
	forwards    []forwardCall
}

func (f *fakeServer) TaskStatus(context.Context, string) (string, error) {
	if f.onStatus != nil {
		f.onStatus()
	}
	return f.status, f.statusErr
}

func (f *fakeServer) Forward(_ context.Context, ep Endpoint, taskID string, body []byte) (int, []byte, error) {
	f.forwards = append(f.forwards, forwardCall{ep, taskID, string(body)})
	if f.forwardErr != nil {
		return 0, nil, f.forwardErr
	}
	code := f.forwardCode
	if code == 0 {
		code = 200
	}
	return code, []byte(`{"status":"ok"}`), nil
}

type fakeBuilder struct {
	job    *batchv1.Job
	secret *corev1.Secret
	err    error
	got    jobbuilder.TaskEntry
	delay  time.Duration
}

func (f *fakeBuilder) Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, error) {
	f.got = e
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	if f.err != nil {
		return nil, nil, f.err
	}
	job := f.job
	if job == nil {
		job = &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "fm-" + e.TaskID}}
	}
	secret := f.secret
	if secret == nil {
		secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "fm-" + e.TaskID + "-cred"}}
	}
	return job, secret, nil
}

type fakeMetrics struct {
	claimed    int
	terminal   []string
	inflight   []int
	duplicates int
}

func (f *fakeMetrics) TaskClaimed()               { f.claimed++ }
func (f *fakeMetrics) TaskTerminal(result string) { f.terminal = append(f.terminal, result) }
func (f *fakeMetrics) JobBootSeconds(float64)     {}
func (f *fakeMetrics) JobCreateSeconds(float64)   {}
func (f *fakeMetrics) InflightJobs(n int)         { f.inflight = append(f.inflight, n) }
func (f *fakeMetrics) DuplicateDispatch()         { f.duplicates++ }

// ---- helpers ----

type fixture struct {
	reg     *registry.Registry
	jobs    *fakeJobs
	server  *fakeServer
	builder *fakeBuilder
	metrics *fakeMetrics
	sched   *Scheduler
	now     time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{
		jobs:    &fakeJobs{},
		server:  &fakeServer{},
		builder: &fakeBuilder{},
		metrics: &fakeMetrics{},
		now:     testNow,
	}
	// The registry shares the fixture clock: with the real clock its done
	// index (TTL 24h) would expire entries stamped at testNow and the suite
	// would rot with wall time.
	f.reg = registry.New(func() time.Time { return f.now })
	cfg, err := LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	s, err := New(cfg, f.reg, f.jobs, f.builder, f.server, f.metrics,
		WithClock(func() time.Time { return f.now }),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	f.sched = s
	return f
}

func claimPayload(id string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
		"id": %q,
		"agent_id": "agent-1",
		"issue_id": "issue-1",
		"issue_identifier": "MULTI-1",
		"workspace_id": "ws-1",
		"runtime_id": "real-runtime",
		"auth_token": "mat_secret",
		"agent": {"model": "x"}
	}`, id))
}

// claim runs OnClaim and returns the registered entry.
func (f *fixture) claim(t *testing.T, taskID string) registry.TaskEntry {
	t.Helper()
	if err := f.sched.OnClaim(context.Background(), claimPayload(taskID)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	e, ok := f.reg.Get(taskID)
	if !ok {
		t.Fatalf("task %s not registered", taskID)
	}
	return e
}

// ---- OnClaim ----

func TestOnClaimCreatesSecretJobAndEntry(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")

	if e.State != registry.StateJobCreated {
		t.Fatalf("state = %v, want job_created", e.State)
	}
	wantName := "fm-task-1"
	if e.JobName != wantName || e.DaemonID != wantName || e.JobNamespace != "multica-agents" {
		t.Fatalf("identity fields: %+v", e)
	}
	if e.JobRuntimeID == "" || e.JobRuntimeID == "real-runtime" {
		t.Fatalf("job_runtime_id = %q", e.JobRuntimeID)
	}
	if e.BootDeadline != testNow.Add(DefaultJobBootTimeout) {
		t.Fatalf("boot_deadline = %v", e.BootDeadline)
	}
	if len(f.jobs.createdSecrets) != 1 || f.jobs.createdSecrets[0].Name != wantName+"-cred" {
		t.Fatalf("secrets created: %+v", f.jobs.createdSecrets)
	}
	if len(f.jobs.createdJobs) != 1 || f.jobs.createdJobs[0].Name != wantName {
		t.Fatalf("jobs created: %+v", f.jobs.createdJobs)
	}
	// Stale-secret sweep happens before create (idempotent retry path).
	if len(f.jobs.deletedSecrets) != 1 || f.jobs.deletedSecrets[0] != wantName+"-cred" {
		t.Fatalf("stale secret sweep: %+v", f.jobs.deletedSecrets)
	}
	if f.builder.got.TaskID != "task-1" || f.builder.got.WorkspaceID != "ws-1" {
		t.Fatalf("builder got %+v", f.builder.got)
	}
	if f.metrics.claimed != 1 || f.metrics.inflight[len(f.metrics.inflight)-1] != 1 {
		t.Fatalf("metrics: %+v", f.metrics)
	}
}

func TestOnClaimSecretFailureFailsTaskWithoutJob(t *testing.T) {
	f := newFixture(t)
	f.jobs.createSecretErr = errors.New("rbac denied")

	err := f.sched.OnClaim(context.Background(), claimPayload("task-1"))
	if err == nil {
		t.Fatal("OnClaim error swallowed")
	}
	e, ok := f.reg.Get("task-1")
	if !ok || e.State != registry.StateTerminal || e.Result != registry.ResultFailed {
		t.Fatalf("entry = %+v, %v", e, ok)
	}
	if len(f.jobs.createdJobs) != 0 {
		t.Fatal("Job created despite Secret failure")
	}
	if len(f.server.forwards) != 1 || f.server.forwards[0].ep != EPFail {
		t.Fatalf("forwards = %+v", f.server.forwards)
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(f.server.forwards[0].body), &body); err != nil {
		t.Fatal(err)
	}
	if body["failure_reason"] != "job_create_failed" {
		t.Fatalf("failure_reason = %q", body["failure_reason"])
	}
}

func TestOnClaimInvalidTemplateFailsWithInvalidJobTemplate(t *testing.T) {
	f := newFixture(t)
	f.builder.err = &jobbuilder.ValidationError{
		Source:     "job template",
		Violations: []jobbuilder.Violation{{Path: "spec.template.spec.initContainers[1].volumeMounts[0].name", Rule: "C-9"}},
	}

	err := f.sched.OnClaim(context.Background(), claimPayload("task-1"))
	if err == nil {
		t.Fatal("OnClaim error swallowed")
	}
	e, ok := f.reg.Get("task-1")
	if !ok || e.State != registry.StateTerminal || e.Result != registry.ResultFailed {
		t.Fatalf("entry = %+v, %v", e, ok)
	}
	if len(f.jobs.createdJobs) != 0 || len(f.jobs.createdSecrets) != 0 {
		t.Fatalf("objects created for a rejected template: jobs=%d secrets=%d", len(f.jobs.createdJobs), len(f.jobs.createdSecrets))
	}
	if len(f.server.forwards) != 1 || f.server.forwards[0].ep != EPFail {
		t.Fatalf("forwards = %+v", f.server.forwards)
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(f.server.forwards[0].body), &body); err != nil {
		t.Fatal(err)
	}
	if body["failure_reason"] != FailureReasonInvalidJobTemplate {
		t.Fatalf("failure_reason = %q, want %q (failure-handling #0)", body["failure_reason"], FailureReasonInvalidJobTemplate)
	}
}

func TestOnClaimInvalidTaskIDFailsWithInvalidTaskID(t *testing.T) {
	f := newFixture(t)
	f.builder.err = fmt.Errorf("%w: %q does not yield a valid Job name", jobbuilder.ErrInvalidTaskID, "TASK-1")

	err := f.sched.OnClaim(context.Background(), claimPayload("task-1"))
	if err == nil {
		t.Fatal("OnClaim error swallowed")
	}
	if len(f.jobs.createdJobs) != 0 || len(f.jobs.createdSecrets) != 0 {
		t.Fatalf("objects created for an invalid task id: jobs=%d secrets=%d", len(f.jobs.createdJobs), len(f.jobs.createdSecrets))
	}
	if len(f.server.forwards) != 1 || f.server.forwards[0].ep != EPFail {
		t.Fatalf("forwards = %+v", f.server.forwards)
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(f.server.forwards[0].body), &body); err != nil {
		t.Fatal(err)
	}
	if body["failure_reason"] != FailureReasonInvalidTaskID {
		t.Fatalf("failure_reason = %q, want %q (failure-handling #0)", body["failure_reason"], FailureReasonInvalidTaskID)
	}
}

func TestOnClaimJobFailureRollsBackSecret(t *testing.T) {
	f := newFixture(t)
	f.jobs.createJobErr = errors.New("quota exceeded")

	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err == nil {
		t.Fatal("OnClaim error swallowed")
	}
	e, _ := f.reg.Get("task-1")
	if e.State != registry.StateTerminal || e.Result != registry.ResultFailed {
		t.Fatalf("entry = %+v", e)
	}
	// One pre-create sweep + one rollback delete.
	if len(f.jobs.deletedSecrets) != 2 {
		t.Fatalf("secret deletes = %+v", f.jobs.deletedSecrets)
	}
	if !f.reg.IsDone("task-1") {
		t.Fatal("done index not written")
	}
}

func TestDuplicateClaimRunningKeepsOriginal(t *testing.T) {
	f := newFixture(t)
	first := f.claim(t, "task-1")
	f.server.status = "running"

	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if len(f.jobs.createdJobs) != 1 {
		t.Fatalf("second Job created: %d", len(f.jobs.createdJobs))
	}
	e, _ := f.reg.Get("task-1")
	if e.JobName != first.JobName || e.State != registry.StateJobCreated {
		t.Fatalf("entry = %+v", e)
	}
	if f.metrics.duplicates != 1 {
		t.Fatalf("duplicates = %d", f.metrics.duplicates)
	}
}

func TestDuplicateClaimPreStartReplacesEntry(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "dispatched"

	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if len(f.jobs.createdJobs) != 2 {
		t.Fatalf("jobs created = %d, want 2", len(f.jobs.createdJobs))
	}
	if len(f.jobs.deletedJobs) != 1 || f.jobs.deletedJobs[0] != "fm-task-1" {
		t.Fatalf("old Job not deleted: %+v", f.jobs.deletedJobs)
	}
	e, _ := f.reg.Get("task-1")
	if e.Attempt != 2 || e.State != registry.StateJobCreated {
		t.Fatalf("entry = %+v", e)
	}
}

func TestDuplicateClaimTerminalOnServerDropsNewClaim(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "cancelled"

	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if len(f.jobs.createdJobs) != 1 {
		t.Fatal("second Job created for terminal task")
	}
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatal("running Job deleted for terminal-on-server task")
	}
}

func TestDuplicateClaimStatusUnknownKeepsOriginal(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.statusErr = errors.New("timeout")

	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if len(f.jobs.createdJobs) != 1 {
		t.Fatal("second Job created with unknown truth")
	}
}

func TestDuplicateClaimAgainstTerminalEntryNeverRebuilds(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	// Terminal report got a 500: entry lingers terminal, Job still up.
	f.server.forwardCode = 500
	e, _ := f.reg.Get("task-1")
	if _, _, err := f.sched.OnReport(context.Background(), EPFail, e, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	// Server never got the fail report and re-dispatches the task.
	f.server.status = "dispatched"
	f.server.forwardCode = 200

	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if len(f.jobs.createdJobs) != 1 {
		t.Fatal("second Job created for a task that already ran")
	}
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatal("terminal entry's Job deleted by duplicate handling")
	}
}

func TestOnClaimDropsTaskInDoneIndex(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	if _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, testNow); err != nil {
		t.Fatal(err)
	}
	if err := f.reg.Delete("task-1"); err != nil {
		t.Fatal(err)
	}

	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if len(f.jobs.createdJobs) != 1 {
		t.Fatal("Job re-created for a done task (invariant 4 broken)")
	}
	if f.metrics.duplicates != 1 {
		t.Fatalf("duplicates = %d", f.metrics.duplicates)
	}
}

// ---- Deliver ----

func TestDeliverHandsTaskOnceWithRuntimeRewrite(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	if _, ok := f.reg.MarkDaemonRegistered(e.DaemonID, testNow); !ok {
		t.Fatal("MarkDaemonRegistered failed")
	}
	// S4 registered the job runtime before the daemon can claim.
	f.reg.PutRuntime(registry.JobRuntime{
		JobRuntimeID: e.JobRuntimeID, DaemonID: e.DaemonID, TaskID: e.TaskID,
		Provider: "omp", Name: "foreman-job", Online: true,
	})

	// job_created is not deliverable: the daemon must register first.
	f2 := newFixture(t)
	e2 := f2.claim(t, "task-9")
	if got, err := f2.sched.Deliver(e2.DaemonID, e2.JobRuntimeID, 1); err != nil || len(got) != 0 {
		t.Fatalf("pre-register deliver = %v, %v", got, err)
	}

	got, err := f.sched.Deliver(e.DaemonID, e.JobRuntimeID, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("Deliver = %v, %v", got, err)
	}
	var delivered map[string]any
	if err := json.Unmarshal(got[0], &delivered); err != nil {
		t.Fatal(err)
	}
	if delivered["runtime_id"] != e.JobRuntimeID {
		t.Fatalf("runtime_id = %v", delivered["runtime_id"])
	}
	// Everything else passes through byte-identical.
	if delivered["auth_token"] != "mat_secret" || delivered["agent_id"] != "agent-1" {
		t.Fatalf("payload mangled: %v", delivered)
	}

	e, _ = f.reg.Get("task-1")
	if e.State != registry.StateDelivering || e.DeliveredAt.IsZero() {
		t.Fatalf("entry = %+v", e)
	}
	if rt, _ := f.reg.RuntimeByID(e.JobRuntimeID); rt.DeliveredTaskID != "task-1" {
		t.Fatalf("runtime delivered_task_id = %q", rt.DeliveredTaskID)
	}

	// At-most-once: the second claim gets nothing.
	got, err = f.sched.Deliver(e.DaemonID, e.JobRuntimeID, 1)
	if err != nil || len(got) != 0 {
		t.Fatalf("second Deliver = %v, %v", got, err)
	}
}

func TestDeliverRejectsStrangers(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	f.reg.MarkDaemonRegistered(e.DaemonID, testNow)

	if _, err := f.sched.Deliver("ghost", e.JobRuntimeID, 1); !errors.Is(err, ErrUnknownDaemon) {
		t.Fatalf("unknown daemon: %v", err)
	}
	if _, err := f.sched.Deliver(e.DaemonID, "rt-other", 1); !errors.Is(err, ErrRuntimeMismatch) {
		t.Fatalf("runtime mismatch: %v", err)
	}
	if got, err := f.sched.Deliver(e.DaemonID, e.JobRuntimeID, 0); err != nil || len(got) != 0 {
		t.Fatalf("zero budget: %v, %v", got, err)
	}
}

// ---- OnReport ----

func TestOnReportStartMarksRunning(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	e.State = registry.StateDelivering
	if err := f.reg.Put(e); err != nil {
		t.Fatal(err)
	}

	code, _, err := f.sched.OnReport(context.Background(), EPStart, e, []byte(`{}`))
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	e, _ = f.reg.Get("task-1")
	if e.State != registry.StateRunning || e.StartedAt != testNow {
		t.Fatalf("entry = %+v", e)
	}
	if f.server.forwards[0].ep != EPStart {
		t.Fatalf("forwards = %+v", f.server.forwards)
	}
}

func TestOnReportTerminalCleansUpAfterSuccess(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	e.State = registry.StateRunning
	f.reg.Put(e)

	code, _, err := f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{"x":1}`))
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("entry kept after terminal cleanup")
	}
	if !f.reg.IsDone("task-1") {
		t.Fatal("done index not written")
	}
	if len(f.jobs.deletedJobs) != 1 || f.jobs.deletedJobs[0] != "fm-task-1" {
		t.Fatalf("job deletes = %+v", f.jobs.deletedJobs)
	}
	if len(f.jobs.deletedSecrets) != 2 { // 1 stale sweep at claim + 1 terminal delete
		t.Fatalf("secret deletes = %+v", f.jobs.deletedSecrets)
	}
	if f.metrics.terminal[0] != "completed" {
		t.Fatalf("terminal metric = %v", f.metrics.terminal)
	}
}

func TestOnReportTerminal404StillCleansUp(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	e.State = registry.StateRunning
	f.reg.Put(e)
	f.server.forwardCode = 404

	code, _, err := f.sched.OnReport(context.Background(), EPFail, e, []byte(`{}`))
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("entry kept after 404 cleanup")
	}
	if len(f.jobs.deletedJobs) != 1 {
		t.Fatalf("job deletes = %+v", f.jobs.deletedJobs)
	}
}

func TestOnReportTerminalFailureKeepsObjects(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	e.State = registry.StateRunning
	f.reg.Put(e)
	f.server.forwardCode = 500

	// Without a wired PendingReports queue the daemon still gets 502
	// (proxy.md 转发 switch) and the terminal entry is retained.
	code, _, err := f.sched.OnReport(context.Background(), EPComplete, e, []byte(`{}`))
	if err != nil || code != 502 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	e, _ = f.reg.Get("task-1")
	if e.State != registry.StateTerminal || e.Result != registry.ResultCompleted {
		t.Fatalf("entry = %+v", e)
	}
	// The report did not land: objects must survive for the retry path.
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatal("Job deleted before terminal report succeeded")
	}
	if n := f.reg.Inflight(); n != 0 {
		t.Fatalf("Inflight = %d, terminal entries do not count", n)
	}
}

// ---- OnJobGone ----

func TestOnJobGoneCompensatesRunningTask(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	e.State = registry.StateRunning
	f.reg.Put(e)
	f.now = testNow.Add(2 * time.Minute) // past the visibility grace
	f.server.status = "running"

	if err := f.sched.OnJobGone(context.Background(), "fm-task-1"); err != nil {
		t.Fatalf("OnJobGone: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("entry kept after compensation")
	}
	if len(f.server.forwards) != 1 || f.server.forwards[0].ep != EPFail {
		t.Fatalf("forwards = %+v", f.server.forwards)
	}
	var body map[string]string
	json.Unmarshal([]byte(f.server.forwards[0].body), &body)
	if body["failure_reason"] != "job_missing" {
		t.Fatalf("failure_reason = %q", body["failure_reason"])
	}
	if !f.reg.IsDone("task-1") {
		t.Fatal("done index not written")
	}
}

func TestOnJobGoneWithinGracePeriodWaits(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1") // claimed at testNow, still fresh

	if err := f.sched.OnJobGone(context.Background(), "fm-task-1"); err != nil {
		t.Fatalf("OnJobGone: %v", err)
	}
	if len(f.server.forwards) != 0 || len(f.jobs.deletedJobs) != 0 {
		t.Fatal("grace period violated")
	}
}

func TestOnJobGoneTaskVanishedServerSide(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.now = testNow.Add(2 * time.Minute)
	f.server.statusErr = ErrTaskNotFound

	if err := f.sched.OnJobGone(context.Background(), "fm-task-1"); err != nil {
		t.Fatalf("OnJobGone: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("entry kept for a server-deleted task")
	}
	if len(f.server.forwards) != 0 {
		t.Fatal("terminal report sent for a vanished task (scenario #9 forbids it)")
	}
}

func TestOnJobGoneUnknownJobIsNoop(t *testing.T) {
	f := newFixture(t)
	if err := f.sched.OnJobGone(context.Background(), "fm-ghost"); err != nil {
		t.Fatalf("OnJobGone: %v", err)
	}
}

// ---- Rebuild + Reconcile ----

func jobObject(taskID string) batchv1.Job {
	return batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      "fm-" + taskID,
		Namespace: "multica-agents",
		Labels: map[string]string{
			registry.LabelManagedBy:   registry.ManagedByForeman,
			registry.LabelTaskID:      taskID,
			registry.LabelAgentID:     "agent-1",
			registry.LabelWorkspaceID: "ws-1",
		},
		Annotations: map[string]string{
			registry.AnnoDaemonID:     "fm-" + taskID,
			registry.AnnoJobRuntimeID: "rt-" + taskID,
			registry.AnnoClaimedAt:    testNow.Add(-time.Hour).Format(time.RFC3339),
			registry.AnnoAttempt:      "1",
		},
	}}
}

func TestRebuildRestoresIndexAndRuntime(t *testing.T) {
	f := newFixture(t)
	f.jobs.jobs = []batchv1.Job{jobObject("task-1")}

	if err := f.sched.Rebuild(context.Background()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	e, ok := f.reg.ByJob("fm-task-1")
	if !ok || e.State != registry.StateJobCreated {
		t.Fatalf("entry = %+v, %v", e, ok)
	}
	if e.JobRuntimeID != "rt-task-1" || e.DaemonID != "fm-task-1" {
		t.Fatalf("identity = %+v", e)
	}
	if _, ok := f.reg.RuntimeByDaemon("fm-task-1"); !ok {
		t.Fatal("runtime index not rebuilt")
	}
	// The rebuilt runtime's heartbeat clock is seeded at rebuild time; a zero
	// value would let the scenario-#6 suspect check misjudge a just-recovered
	// running Job as never-heartbeated.
	rt, ok := f.reg.RuntimeByID("rt-task-1")
	if !ok {
		t.Fatal("runtime not rebuilt by ID")
	}
	if !rt.LastHeartbeatAt.Equal(testNow) {
		t.Fatalf("LastHeartbeatAt = %v, want rebuild-time clock %v", rt.LastHeartbeatAt, testNow)
	}
}

func TestConvergeRunningKeepsJob(t *testing.T) {
	f := newFixture(t)
	f.jobs.jobs = []batchv1.Job{jobObject("task-1")}
	f.server.status = "running"

	if err := f.sched.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.sched.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, ok := f.reg.Get("task-1")
	if !ok || e.State != registry.StateRunning {
		t.Fatalf("entry = %+v, %v", e, ok)
	}
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatal("running Job deleted")
	}
}

func TestConvergePreStartReleasesForRedispatch(t *testing.T) {
	f := newFixture(t)
	f.jobs.jobs = []batchv1.Job{jobObject("task-1")}
	f.server.status = "dispatched"

	if err := f.sched.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.sched.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("pre-start entry kept after restart")
	}
	if len(f.jobs.deletedJobs) != 1 {
		t.Fatalf("job deletes = %+v", f.jobs.deletedJobs)
	}
	if len(f.server.forwards) != 0 {
		t.Fatal("report sent for a released task")
	}
}

func TestConvergeTerminalAndGoneCleanup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    string
		statusErr error
		wantDone  bool
	}{
		{"terminal forwarded", "completed", nil, true},
		{"task deleted server-side", "", ErrTaskNotFound, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.jobs.jobs = []batchv1.Job{jobObject("task-1")}
			f.server.status = tc.status
			f.server.statusErr = tc.statusErr

			if err := f.sched.Rebuild(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := f.sched.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, ok := f.reg.Get("task-1"); ok {
				t.Fatal("entry kept")
			}
			if len(f.jobs.deletedJobs) != 1 {
				t.Fatalf("job deletes = %+v", f.jobs.deletedJobs)
			}
			if got := f.reg.IsDone("task-1"); got != tc.wantDone {
				t.Fatalf("IsDone = %v, want %v", got, tc.wantDone)
			}
		})
	}
}

func TestConvergeStatusErrorKeepsEntry(t *testing.T) {
	f := newFixture(t)
	f.jobs.jobs = []batchv1.Job{jobObject("task-1")}
	f.server.statusErr = errors.New("server unreachable")

	if err := f.sched.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.sched.Reconcile(context.Background()); err == nil {
		t.Fatal("converge error swallowed")
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("entry dropped on a transient status error")
	}
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatal("Job deleted on a transient status error")
	}
}

type recordingReconciler struct{ calls int }

func (r *recordingReconciler) Reconcile(context.Context) error { r.calls++; return nil }

func TestReconcileDelegatesToRecoveryWhenWired(t *testing.T) {
	f := newFixture(t)
	rec := &recordingReconciler{}
	cfg, _ := LoadConfig(func(string) string { return "" })
	s, err := New(cfg, f.reg, f.jobs, f.builder, f.server, nil,
		WithReconciler(rec),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	f.reg.Put(registry.TaskEntry{TaskID: "task-1", JobName: "fm-task-1", State: registry.StateRunning})
	if err := s.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.calls != 1 {
		t.Fatalf("reconciler calls = %d", rec.calls)
	}
}

// ---- Run ----

func TestRunRebuildsThenBlocksUntilCancel(t *testing.T) {
	f := newFixture(t)
	f.jobs.jobs = []batchv1.Job{jobObject("task-1")}
	f.server.status = "running"

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.sched.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for {
		if _, ok := f.reg.Get("task-1"); ok {
			cancel()
			break
		}
		select {
		case <-deadline:
			t.Fatal("Run did not rebuild")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	if e, _ := f.reg.Get("task-1"); e.State != registry.StateRunning {
		t.Fatalf("entry = %+v", e)
	}
}

func TestConcurrentOnClaimSameTaskBuildsExactlyOneJob(t *testing.T) {
	f := newFixture(t)
	f.builder.delay = 20 * time.Millisecond // widen the CreateSecret race window

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
				t.Errorf("OnClaim: %v", err)
			}
		}()
	}
	wg.Wait()

	if n := len(f.jobs.createdJobs); n != 1 {
		t.Fatalf("jobs created = %d, want 1", n)
	}
	if n := len(f.jobs.createdSecrets); n != 1 {
		t.Fatalf("secrets created = %d, want 1", n)
	}
	for _, call := range f.server.forwards {
		if call.ep == EPFail {
			t.Fatal("concurrent claim failed a healthy task")
		}
	}
	if got := f.sched.claimMu.lenEntries(); got != 0 {
		t.Fatalf("claimMu entries = %d after all claims, want 0", got)
	}
}

func TestConvergeRunningPreservesConcurrentTransition(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	// The daemon's S8 start lands while Reconcile's C13 is in flight.
	concurrentStart := testNow.Add(90 * time.Second)
	f.server.status = "running"
	f.server.onStatus = func() { f.reg.MarkStarted("task-1", concurrentStart) }

	if err := f.sched.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, _ := f.reg.Get("task-1")
	if e.State != registry.StateRunning {
		t.Fatalf("state = %v", e.State)
	}
	if e.StartedAt != concurrentStart {
		t.Fatalf("concurrent start clobbered by stale snapshot: %v", e.StartedAt)
	}
	if e.LastStatusSeen != "running" {
		t.Fatalf("last_status_seen = %q", e.LastStatusSeen)
	}
}

func TestOnJobGoneFailedReportRetainsEntryUntilDelivered(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	e.State = registry.StateRunning
	f.reg.Put(e)
	f.now = testNow.Add(2 * time.Minute)
	f.server.status = "running"
	f.server.forwardCode = 500

	if err := f.sched.OnJobGone(context.Background(), "fm-task-1"); err != nil {
		t.Fatalf("OnJobGone: %v", err)
	}
	// The report did not land: the terminal entry and its handle must stay.
	e, ok := f.reg.Get("task-1")
	if !ok || e.State != registry.StateTerminal || e.Result != registry.ResultFailed {
		t.Fatalf("entry = %+v, %v", e, ok)
	}
	if len(f.jobs.deletedSecrets) != 1 { // only the claim-time stale sweep
		t.Fatalf("secret deleted before report landed: %+v", f.jobs.deletedSecrets)
	}

	// Server recovers: the next reconcile redelivers the report, then cleans up.
	f.server.forwardCode = 200
	if err := f.sched.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("entry kept after the pending report landed")
	}
	if n := len(f.server.forwards); n != 2 {
		t.Fatalf("forwards = %d, want 2 (initial + retry)", n)
	}
	if len(f.jobs.deletedJobs) != 1 || f.jobs.deletedJobs[0] != "fm-task-1" {
		t.Fatalf("job deletes = %+v", f.jobs.deletedJobs)
	}
}

// ---- ClaimBudget ----

func TestClaimBudget(t *testing.T) {
	f := newFixture(t)
	if got := f.sched.ClaimBudget(); got != 32 {
		t.Fatalf("ClaimBudget = %d, want 32", got)
	}
	for i := range 99 {
		f.reg.Put(registry.TaskEntry{
			TaskID: fmt.Sprintf("t%d", i), JobName: fmt.Sprintf("fm-t%d", i),
			DaemonID: fmt.Sprintf("fm-t%d", i), State: registry.StateRunning,
		})
	}
	if got := f.sched.ClaimBudget(); got != 1 {
		t.Fatalf("ClaimBudget = %d, want 1", got)
	}
	f.reg.Put(registry.TaskEntry{TaskID: "t100", JobName: "fm-t100", DaemonID: "fm-t100", State: registry.StateRunning})
	if got := f.sched.ClaimBudget(); got != 0 {
		t.Fatalf("ClaimBudget = %d, want 0", got)
	}
}

// ---- per-node soft cap (FOREMAN_MAX_JOBS_PER_NODE, ADR-006 节流) ----

// testIssuer satisfies jobbuilder.TokenIssuer (the real builder is wired so
// the affinity decision of a claim is exercised end to end).
type testIssuer struct{}

func (testIssuer) Issue(string, string, string, time.Duration) (string, error) {
	return "fmj_test.token", nil
}

// newCappedScheduler builds a scheduler over f's registry whose builder is
// the real jobbuilder, so Job objects carry the genuine affinity.
func newCappedScheduler(t *testing.T, f *fixture, maxJobsPerNode int, logw io.Writer) *Scheduler {
	t.Helper()
	jbCfg, err := jobbuilder.LoadConfig(envFrom(map[string]string{}))
	if err != nil {
		t.Fatalf("jobbuilder.LoadConfig: %v", err)
	}
	jbCfg.Issuer = testIssuer{}
	jbCfg.Nodes = f.reg
	jbCfg.Logger = slog.New(slog.NewJSONHandler(logw, nil))
	cfg, err := LoadConfig(envFrom(map[string]string{EnvMaxJobsPerNode: strconv.Itoa(maxJobsPerNode)}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	builder, err := jobbuilder.NewBuilder(jbCfg)
	if err != nil {
		t.Fatalf("jobbuilder.NewBuilder: %v", err)
	}
	s, err := New(cfg, f.reg, f.jobs, builder, f.server, f.metrics,
		WithClock(func() time.Time { return f.now }),
		WithLogger(slog.New(slog.NewJSONHandler(logw, nil))))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestJobCreatedLogsImageReference (AC-10): the job.created event carries the
// FOREMAN_JOB_IMAGE reference verbatim. With a movable tag there is no digest
// to audit, so the reference itself is the record of what the Job will run.
func TestJobCreatedLogsImageReference(t *testing.T) {
	f := newFixture(t)
	var buf bytes.Buffer
	sched := newCappedScheduler(t, f, 4, &buf)
	if err := sched.OnClaim(context.Background(), claimPayload("t1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	record := findLogRecord(t, buf.Bytes(), "job.created")
	if got := record["image"]; got != jobbuilder.DefaultJobImage {
		t.Errorf("job.created image = %v, want the reference %q", got, jobbuilder.DefaultJobImage)
	}
	if _, stale := record["image_digest"]; stale {
		t.Error("job.created still carries the removed image_digest field")
	}
}

// findLogRecord returns the first JSON log record whose msg equals event.
func findLogRecord(t *testing.T, raw []byte, event string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record["msg"] == event {
			return record
		}
	}
	t.Fatalf("no %s record in logs:\n%s", event, raw)
	return nil
}

// place seeds a live Job already placed on node: the state the reconcile
// round leaves behind after reading spec.nodeName.
func place(t *testing.T, f *fixture, taskID, issueID, node string) {
	t.Helper()
	e := registry.TaskEntry{
		TaskID: taskID, IssueID: issueID, JobName: "fm-" + taskID,
		DaemonID: "fm-" + taskID, JobRuntimeID: "rt-" + taskID,
		State: registry.StateRunning,
	}
	if err := f.reg.Put(e); err != nil {
		t.Fatalf("Put(%s): %v", taskID, err)
	}
	f.reg.SetNode(taskID, node)
}

// affinityNode returns the hostname the Job prefers, "" when it carries no
// reuse affinity.
func affinityNode(t *testing.T, job *batchv1.Job) string {
	t.Helper()
	aff := job.Spec.Template.Spec.Affinity
	if aff == nil || aff.NodeAffinity == nil {
		return ""
	}
	terms := aff.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) == 0 {
		return ""
	}
	expr := terms[0].Preference.MatchExpressions
	if len(expr) != 1 || len(expr[0].Values) != 1 {
		t.Fatalf("unexpected affinity expression: %+v", expr)
	}
	return expr[0].Values[0]
}

func TestRefreshNodeSaturationMarksNodesAtCap(t *testing.T) {
	f := newFixture(t)
	place(t, f, "t1", "issue-a", "node-a")
	place(t, f, "t2", "issue-b", "node-a")
	place(t, f, "t3", "issue-c", "node-b")
	sched := newCappedScheduler(t, f, 2, io.Discard)

	load := sched.RefreshNodeSaturation(context.Background())
	if load["node-a"] != 2 || load["node-b"] != 1 || len(load) != 2 {
		t.Fatalf("load = %v, want node-a=2 node-b=1", load)
	}
	if markers := f.reg.SaturatedNodes(); len(markers) != 1 || markers[0] != "node-a" {
		t.Fatalf("saturated = %v, want [node-a]", markers)
	}

	// A node that drops below the cap must lose its marker again.
	if _, err := f.reg.MarkTerminal("t1", registry.ResultCompleted, testNow); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if load = sched.RefreshNodeSaturation(context.Background()); load["node-a"] != 1 {
		t.Fatalf("load after terminal = %v, want node-a=1", load)
	}
	if markers := f.reg.SaturatedNodes(); len(markers) != 0 {
		t.Fatalf("saturated = %v, want none once the load dropped", markers)
	}
}

func TestRefreshNodeSaturationLogsLoadAndCap(t *testing.T) {
	f := newFixture(t)
	place(t, f, "t1", "issue-a", "node-a")
	place(t, f, "t2", "issue-b", "node-a")
	var buf bytes.Buffer
	sched := newCappedScheduler(t, f, 2, &buf)

	sched.RefreshNodeSaturation(context.Background())
	for _, want := range []string{
		`"msg":"node.saturation"`, `"node":"node-a"`,
		`"active_jobs":2`, `"max_jobs_per_node":2`, `"saturated":true`,
	} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log line %q misses %s", buf.String(), want)
		}
	}
}

func TestOnClaimDropsAffinityForSaturatedNode(t *testing.T) {
	for _, tc := range []struct {
		name         string
		maxPerNode   int
		wantAffinity string
	}{
		{"below the cap keeps the reuse affinity", 3, "node-a"},
		{"at the cap drops the reuse affinity", 2, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			// issue-1 (the claim payload's issue) ran on node-a, which
			// already holds two live Jobs.
			place(t, f, "t1", "issue-1", "node-a")
			place(t, f, "t2", "issue-b", "node-a")
			sched := newCappedScheduler(t, f, tc.maxPerNode, io.Discard)

			if err := sched.OnClaim(context.Background(), claimPayload("task-x")); err != nil {
				t.Fatalf("OnClaim: %v", err)
			}
			// The soft cap never blocks a Job: it only picks its placement.
			if len(f.jobs.createdJobs) != 1 {
				t.Fatalf("created Jobs = %d, want 1", len(f.jobs.createdJobs))
			}
			if got := affinityNode(t, f.jobs.createdJobs[0]); got != tc.wantAffinity {
				t.Errorf("affinity node = %q, want %q", got, tc.wantAffinity)
			}
		})
	}
}
