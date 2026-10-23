package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// ---- fakes ----

type fakeObjects struct {
	mu      sync.Mutex
	job     *batchv1.Job
	jobErr  error
	pods    []corev1.Pod
	podsErr error
	rounds  int
	jobGets int
}

func (f *fakeObjects) GetJob(context.Context, string) (*batchv1.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobGets++
	return f.job, f.jobErr
}

func (f *fakeObjects) jobGetsSeen() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.jobGets
}

func (f *fakeObjects) ListPods(context.Context) ([]corev1.Pod, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rounds++
	return f.pods, f.podsErr
}

func (f *fakeObjects) roundsSeen() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rounds
}

type fakeStatus struct {
	status string
	err    error
}

func (f *fakeStatus) TaskStatus(context.Context, string) (string, error) {
	return f.status, f.err
}

type failCall struct{ jobName, reason string }
type settleCall struct{ taskID, result string }
type adoptCall struct{ taskID, status string }
type reclaimCall struct {
	jobName string
	reason  string
	at      time.Time
	orphans int
}

// fakeSettler records every settlement the reconciler asks for. The
// termination evidence (ADR-017) is programmed per test through the
// terminated*/alivePods* knobs; the zero values mean "the Job is gone".
type fakeSettler struct {
	mu        sync.Mutex
	onJobGone []string
	failJobs  []failCall
	settles   []settleCall
	adopts    []adoptCall
	discards  []string
	continued []string
	vanished  []string
	sweeps    int
	reclaims  []reclaimCall
	syncs     int
	err       error
	// discardErrs is consumed one error per DiscardTask call; an empty queue
	// falls back to err.
	discardErrs []error

	// Job termination evidence.
	terminated       bool
	terminatedErr    error
	lastTerminatedAt time.Time
	lastTermErr      error
	alivePods        int
	alivePodsErr     error
	reclaimErr       error
	sweepErr         error
	// Evidence-call counters: the objectless reclaim must not read Job/pod
	// termination evidence at all.
	jobTerminatedCalls int
	lastPodCalls       int
	alivePodsCalls     int
	// DrainPendingReports' programmed answer.
	drainCalls     int
	drainRemaining int
	drainErr       error
	// RestorePendingReports' programmed answer.
	restoreCalls int
	restoreErr   error
}

func (f *fakeSettler) ReconcileSlots(context.Context) error { return nil }

func (f *fakeSettler) ContinueClaim(_ context.Context, taskID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.continued = append(f.continued, taskID)
	return f.err
}

func (f *fakeSettler) OnJobGone(_ context.Context, jobName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onJobGone = append(f.onJobGone, jobName)
	return f.err
}

func (f *fakeSettler) FailJob(_ context.Context, jobName, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failJobs = append(f.failJobs, failCall{jobName, reason})
	return f.err
}

func (f *fakeSettler) SettleTerminal(_ context.Context, taskID, result string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settles = append(f.settles, settleCall{taskID, result})
	return f.err
}

func (f *fakeSettler) AdoptRunning(_ context.Context, taskID, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adopts = append(f.adopts, adoptCall{taskID, status})
	return f.err
}

func (f *fakeSettler) DiscardTask(_ context.Context, taskID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.discards = append(f.discards, taskID)
	if len(f.discardErrs) > 0 {
		err := f.discardErrs[0]
		f.discardErrs = f.discardErrs[1:]
		if err != nil {
			return err
		}
	}
	return f.err
}

func (f *fakeSettler) OnTaskVanished(_ context.Context, taskID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.vanished = append(f.vanished, taskID)
	return f.err
}

func (f *fakeSettler) SweepOrphanSecrets(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sweeps++
	return f.sweepErr
}

func (f *fakeSettler) JobTerminated(context.Context, string, time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobTerminatedCalls++
	return f.terminated, f.terminatedErr
}

func (f *fakeSettler) LastPodTerminatedAt(context.Context, string, time.Time) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastPodCalls++
	return f.lastTerminatedAt, f.lastTermErr
}

func (f *fakeSettler) AlivePods(context.Context, string, time.Time) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alivePodsCalls++
	return f.alivePods, f.alivePodsErr
}

// evidenceCalls reports how often the reclaim read Job/pod termination
// evidence: the objectless path must answer zero.
func (f *fakeSettler) evidenceCalls() (jobTerminated, lastPod, alivePods int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.jobTerminatedCalls, f.lastPodCalls, f.alivePodsCalls
}

func (f *fakeSettler) OnJobTerminated(_ context.Context, jobName, reason string, at time.Time, orphans int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reclaims = append(f.reclaims, reclaimCall{jobName, reason, at, orphans})
	return f.reclaimErr
}

func (f *fakeSettler) SyncInflight() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs++
}

// DrainPendingReports stands in for the scheduler's queue drain: the queue is
// the scheduler's own (ADR-031), so the round reaches it here.
func (f *fakeSettler) DrainPendingReports(context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drainCalls++
	return f.drainRemaining, f.drainErr
}

// RestorePendingReports is the carrier-list gate: while it errors the round
// settles nothing (§4.3).
func (f *fakeSettler) RestorePendingReports(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restoreCalls++
	return f.restoreErr
}

func (f *fakeSettler) calls() (int, int, int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.onJobGone), len(f.failJobs), len(f.settles), len(f.adopts), len(f.discards)
}

// drains reports the reclaims and the vanish settlements, the two paths the
// drain semantics added (ADR-017).
func (f *fakeSettler) drains() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reclaims), len(f.vanished)
}

type fakeMetrics struct {
	mu   sync.Mutex
	last int
	seq  []int
}

func (m *fakeMetrics) HeartbeatSuspect(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.last = n
	m.seq = append(m.seq, n)
}

// ---- helpers ----

func testEntry(taskID string) registry.TaskEntry {
	jobName := "fm-" + taskID
	return registry.TaskEntry{
		TaskID:       taskID,
		JobName:      jobName,
		DaemonID:     jobName,
		JobRuntimeID: "rt-" + taskID,
		State:        registry.StateRunning,
		ClaimedAt:    time.Now().Add(-time.Hour),
		Payload:      json.RawMessage(`{"id":"` + taskID + `"}`),
	}
}

func newRegistry(t *testing.T, entries ...registry.TaskEntry) *registry.Registry {
	t.Helper()
	reg := registry.New(time.Now)
	for _, e := range entries {
		if err := reg.Put(e); err != nil {
			t.Fatalf("registry.Put(%s): %v", e.TaskID, err)
		}
	}
	return reg
}

func newTestReconciler(t *testing.T, reg *registry.Registry, objs ObjectClient, st StatusClient, set Settler, opts ...Option) *Reconciler {
	t.Helper()
	r, err := New(Config{ReconcileInterval: time.Minute}, reg, objs, st, set, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

func runningJob(name string) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func failedJob(name, conditionReason string) *batchv1.Job {
	job := runningJob(name)
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{{
		Type:   batchv1.JobFailed,
		Status: corev1.ConditionTrue,
		Reason: conditionReason,
	}}
	return job
}

func podFor(jobName string, mutate func(*corev1.Pod)) corev1.Pod {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   jobName + "-x",
			Labels: map[string]string{podJobNameLabel: jobName},
		},
	}
	if mutate != nil {
		mutate(&pod)
	}
	return pod
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// logRecord returns the first JSON record whose msg equals event.
func logRecord(t *testing.T, raw []byte, event string) map[string]any {
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

// ---- §4.3: restart rebuild ----------------------------------------------

func TestReconcileTerminalEntryRetriesCleanup(t *testing.T) {
	e := testEntry("t1")
	e.State = registry.StateTerminal
	e.Result = registry.ResultFailed
	reg := newRegistry(t, e)
	set := &fakeSettler{}

	r := newTestReconciler(t, reg, &fakeObjects{}, &fakeStatus{status: "failed"}, set)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.settles) != 1 || set.settles[0] != (settleCall{"t1", "failed"}) {
		t.Fatalf("settles = %+v, want one settle of t1/failed", set.settles)
	}
}

// 顺序与幂等规则 2: the object deletion of a terminal entry is retried for a
// bounded number of rounds, so a permanently failing cleanup cannot spin
// forever (the entry stays as the handle a restart retries with).
func TestReconcileTerminalCleanupRetriesAreBounded(t *testing.T) {
	e := testEntry("t1")
	e.State = registry.StateTerminal
	e.Result = registry.ResultFailed
	reg := newRegistry(t, e)
	set := &fakeSettler{err: errors.New("k8s delete failed")}

	var logs bytes.Buffer
	r := newTestReconciler(t, reg, &fakeObjects{}, &fakeStatus{status: "failed"}, set,
		WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))))

	for i := range cleanupRetryLimit {
		if err := r.Reconcile(context.Background()); err == nil {
			t.Fatalf("round %d: want the deletion error surfaced", i+1)
		}
	}
	// The budget is spent: the deletion is not attempted again, and the
	// round reports no error for an entry that is already settled.
	for range 3 {
		if err := r.Reconcile(context.Background()); err != nil {
			t.Fatalf("round after the budget: %v", err)
		}
	}
	if len(set.settles) != cleanupRetryLimit {
		t.Fatalf("settles = %d, want exactly %d retries", len(set.settles), cleanupRetryLimit)
	}
	if !bytes.Contains(logs.Bytes(), []byte("task.cleanup_abandoned")) {
		t.Fatalf("missing the abandonment event in %s", logs.String())
	}
}

func TestReconcileJobMissingHandsOverToCompensation(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	// GetJob returning nil models the object that vanished.
	r := newTestReconciler(t, reg, &fakeObjects{}, &fakeStatus{status: "running"}, set)

	// The first sighting of a missing Job only starts the visibility window.
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.onJobGone) != 0 {
		t.Fatalf("onJobGone = %v, want no compensation on the first missing sighting", set.onJobGone)
	}
	// Still inside the window, even though the task was claimed long ago.
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.onJobGone) != 0 {
		t.Fatalf("onJobGone = %v, want none inside the visibility window", set.onJobGone)
	}

	r.jobGoneWindow = 0 // the visibility window has passed
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.onJobGone) != 1 || set.onJobGone[0] != "fm-t1" {
		t.Fatalf("onJobGone = %v, want [fm-t1]", set.onJobGone)
	}
}

func TestReconcileMissingJobClockResetsWhenItReappears(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	objs := &fakeObjects{} // the Job is invisible this round
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, set)

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	objs.job = runningJob("fm-t1") // a transient API blip: the Job is back
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(r.missing) != 0 {
		t.Fatalf("missing = %v, want the window forgotten once the Job is visible", r.missing)
	}
}

// A release that failed halfway must be finished by the next round: the Job
// may already be gone, and re-deriving the intent from the objects would
// read it as an unexpectedly missing Job.
func TestReconcileReleaseSurvivesAPartialDeletion(t *testing.T) {
	e := testEntry("t1")
	e.Payload = nil // rebuilt entry: nothing to deliver
	e.State = registry.StateJobCreated
	reg := newRegistry(t, e)
	set := &fakeSettler{discardErrs: []error{errors.New("secret delete failed")}}
	objs := &fakeObjects{job: runningJob("fm-t1")}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "dispatched"}, set)

	if err := r.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile: want the deletion error surfaced")
	}
	if _, ok := reg.Get("t1"); !ok {
		t.Fatal("the entry must stay live as the retry handle")
	}

	// The Job delete succeeded meanwhile: only the Secret is left to retry.
	objs.job = nil
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.discards) != 2 {
		t.Fatalf("discards = %v, want the discard retried", set.discards)
	}
	if len(set.onJobGone) != 0 || len(set.failJobs) != 0 {
		t.Fatalf("onJobGone=%v failJobs=%v, want no missing-Job compensation for an intentional release",
			set.onJobGone, set.failJobs)
	}
}

func TestReconcileRebuiltPreStartEntryIsReleased(t *testing.T) {
	e := testEntry("t1")
	e.Payload = nil // rebuilt from the Job object face, which carries no payload
	reg := newRegistry(t, e)
	set := &fakeSettler{}

	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")}, &fakeStatus{status: "dispatched"}, set)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.discards) != 1 || set.discards[0] != "t1" {
		t.Fatalf("discards = %v, want [t1]", set.discards)
	}
}

func TestReconcileLivePreStartEntryIsLeftAlone(t *testing.T) {
	reg := newRegistry(t, testEntry("t1")) // still holds its payload
	set := &fakeSettler{}

	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")}, &fakeStatus{status: "dispatched"}, set)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, _, _, _, discards := set.calls(); discards != 0 {
		t.Fatalf("discards = %d, want 0 (a live pre-start task must be kept)", discards)
	}
}

// ADR-026 决策 5: the discard row's 成立面 is started_at == 0. An entry
// adopted through C13 has lost its payload with the restart, so it still
// answers the payload test; a later queued/dispatched answer (the server
// re-queues or the status jitters) must not send it back to that row — the
// Job survives and the entry converges on the Job/Pod facts.
func TestReconcileAdoptedEntryOnStatusFallbackIsKept(t *testing.T) {
	for _, status := range []string{"queued", "dispatched"} {
		t.Run(status, func(t *testing.T) {
			e := testEntry("t1")
			e.Payload = nil // rebuilt entry: nothing to deliver
			e.StartedAt = time.Now().Add(-time.Minute)
			reg := newRegistry(t, e)
			set := &fakeSettler{}

			r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")}, &fakeStatus{status: status}, set)
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if gone, fails, settles, adopts, discards := set.calls(); gone+fails+settles+adopts+discards != 0 {
				t.Fatalf("settlements = gone:%d fail:%d terminal:%d adopt:%d discard:%d, want none for an adopted entry",
					gone, fails, settles, adopts, discards)
			}
			got, ok := reg.Get("t1")
			if !ok {
				t.Fatal("the adopted entry disappeared from the index")
			}
			if got.StartedAt.IsZero() {
				t.Fatal("started_at was cleared")
			}
		})
	}
}

func TestReconcileRunningTaskIsAdopted(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}

	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")}, &fakeStatus{status: "running"}, set)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.adopts) != 1 || set.adopts[0] != (adoptCall{"t1", "running"}) {
		t.Fatalf("adopts = %v, want [t1/running]", set.adopts)
	}
}

// A rebuilt entry has no payload — the Job object face carries identity and
// trigger context only (F3) — so a daemon-held status is the post-delivery
// evidence the adoption face needs (§4.3, ADR-026 决策结果 1/3).
func TestReconcileRebuiltWaitingEntryIsAdopted(t *testing.T) {
	e := testEntry("t1")
	e.Payload = nil // rebuilt: the Job object face carries no payload
	e.State = registry.StateJobCreated
	reg := newRegistry(t, e)
	set := &fakeSettler{}

	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")}, &fakeStatus{status: "waiting_local_directory"}, set)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.adopts) != 1 || set.adopts[0] != (adoptCall{"t1", "waiting_local_directory"}) {
		t.Fatalf("adopts = %v, want [t1/waiting_local_directory]", set.adopts)
	}
}

// An in-flight entry still holds its payload: waiting_local_directory is the
// normal pre-start face of its own delivery, not adoption evidence.
func TestReconcileLiveWaitingEntryIsLeftAlone(t *testing.T) {
	reg := newRegistry(t, testEntry("t1")) // still holds its payload
	set := &fakeSettler{}

	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")}, &fakeStatus{status: "waiting_local_directory"}, set)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, _, _, adopts, _ := set.calls(); adopts != 0 {
		t.Fatalf("adopts = %d, want 0 (an in-flight entry keeps waiting)", adopts)
	}
}

// A value outside the C13 domain is kept and reported: the disposition table
// has no branch for it and widening the domain is a design change
// (ADR-026 决策结果 3).
func TestReconcileUnclassifiedStatusIsReportedAndKept(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	var logs bytes.Buffer
	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")}, &fakeStatus{status: "paused"}, set,
		WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))))

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := reg.Get("t1"); !ok {
		t.Fatal("an unclassified status must keep the entry")
	}
	if _, fails, settles, adopts, discards := set.calls(); fails+settles+adopts+discards != 0 {
		t.Fatalf("calls = fails %d, settles %d, adopts %d, discards %d; want no disposition", fails, settles, adopts, discards)
	}
	record := logRecord(t, logs.Bytes(), "task.status_unclassified")
	if got := record["level"]; got != "ERROR" {
		t.Errorf("level = %v, want ERROR (an Info/Warn downgrade must fail the test)", got)
	}
	if got := record["task_id"]; got != "t1" {
		t.Errorf("task_id = %v, want t1", got)
	}
	if got := record["job_name"]; got != "fm-t1" {
		t.Errorf("job_name = %v, want fm-t1", got)
	}
	if got := record["daemon_id"]; got != "fm-t1" {
		t.Errorf("daemon_id = %v, want fm-t1", got)
	}
	if got := record["status"]; got != "paused" {
		t.Errorf("status = %v, want paused", got)
	}
}

func TestReconcileTerminalStatusSettles(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}

	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")}, &fakeStatus{status: "completed"}, set)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.settles) != 1 || set.settles[0] != (settleCall{"t1", "completed"}) {
		t.Fatalf("settles = %+v, want one settle of t1/completed", set.settles)
	}
}

// ---- §4.6: task deleted server-side -------------------------------------

// A task the server deleted settles terminal with an empty result and
// enters drain: no report is sent, but the Job is deleted and the credential
// then follows the Job (ADR-017 path ③).
func TestReconcileDeletedTaskVanishes(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}

	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")},
		&fakeStatus{err: scheduler.ErrTaskNotFound}, set)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.vanished) != 1 || set.vanished[0] != "t1" {
		t.Fatalf("vanished = %v, want [t1]", set.vanished)
	}
	if _, _, _, _, discards := set.calls(); discards != 0 {
		t.Fatalf("discards = %d, want 0 (a deleted task still drains)", discards)
	}
}

// ---- §4.6: the Job ended without a report -------------------------------

func TestReconcileJobEndedWaitsThenFails(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	objs := &fakeObjects{
		job:  failedJob("fm-t1", "BackoffLimitExceeded"),
		pods: []corev1.Pod{podFor("fm-t1", func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed })},
	}
	status := &fakeStatus{status: "running"}
	r := newTestReconciler(t, reg, objs, status, set)

	// Round one records the sighting; the daemon's report window holds.
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, fails, _, _, _ := set.calls(); fails != 0 {
		t.Fatalf("failJobs = %d, want 0 on the recording round", fails)
	}
	// Still inside the window: the compensation must keep waiting.
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, fails, _, _, _ := set.calls(); fails != 0 {
		t.Fatalf("failJobs = %d, want 0 inside the daemon-report window", fails)
	}

	// The window has passed: the next round compensates with job_failed.
	r.recheckWindow = 0
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.failJobs) != 1 || set.failJobs[0] != (failCall{"fm-t1", scheduler.FailureReasonJobFailed}) {
		t.Fatalf("failJobs = %+v, want job_failed", set.failJobs)
	}
}

func TestReconcileDaemonReportWinsInsideTheWindow(t *testing.T) {
	e := testEntry("t1")
	reg := newRegistry(t, e)
	set := &fakeSettler{}
	objs := &fakeObjects{job: failedJob("fm-t1", "BackoffLimitExceeded")}
	status := &fakeStatus{status: "running"}
	r := newTestReconciler(t, reg, objs, status, set)

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// The daemon's terminal report landed: the entry is terminal and the
	// server knows it — the ended path must not synthesize a fail.
	e.State, e.Result = registry.StateTerminal, registry.ResultCompleted
	if err := reg.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	r.recheckWindow = 0
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, fails, _, _, _ := set.calls(); fails != 0 {
		t.Fatalf("failJobs = %d, want 0 (the daemon reported first)", fails)
	}
	if len(set.settles) != 1 || set.settles[0].result != "completed" {
		t.Fatalf("settles = %+v, want the terminal entry settled as completed", set.settles)
	}
}

func TestReconcileDeadlineExceededIsImmediate(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	objs := &fakeObjects{job: failedJob("fm-t1", "DeadlineExceeded")}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, set)

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.failJobs) != 1 || set.failJobs[0] != (failCall{"fm-t1", scheduler.FailureReasonJobDeadline}) {
		t.Fatalf("failJobs = %+v, want job_deadline_exceeded with no window", set.failJobs)
	}
}

func TestReconcileEvictedPodIsNamed(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	objs := &fakeObjects{
		job: failedJob("fm-t1", "BackoffLimitExceeded"),
		pods: []corev1.Pod{podFor("fm-t1", func(p *corev1.Pod) {
			p.Status.Phase = corev1.PodFailed
			p.Status.Reason = "Evicted"
		})},
	}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, set)

	if err := r.Reconcile(context.Background()); err != nil { // records the evidence
		t.Fatalf("Reconcile: %v", err)
	}
	r.recheckWindow = 0
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.failJobs) != 1 || set.failJobs[0].reason != scheduler.FailureReasonJobEvicted {
		t.Fatalf("failJobs = %+v, want job_evicted", set.failJobs)
	}
}

func TestReconcileUnknownPodIsEvicted(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	objs := &fakeObjects{
		job:  runningJob("fm-t1"),
		pods: []corev1.Pod{podFor("fm-t1", func(p *corev1.Pod) { p.Status.Phase = corev1.PodUnknown })},
	}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, set)

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	r.recheckWindow = 0
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.failJobs) != 1 || set.failJobs[0].reason != scheduler.FailureReasonJobEvicted {
		t.Fatalf("failJobs = %+v, want job_evicted (场景 #11)", set.failJobs)
	}
}

// A re-run reuses the Job name (fm-<task_id>), so the list face can still
// carry the predecessor's corpse pod. Its eviction and its node belong to the
// predecessor: neither may name this instance's end reason nor its placement
// (ADR-017 instance filter).
func TestReconcilePredecessorPodDoesNotDecideTheInstance(t *testing.T) {
	e := testEntry("t1")
	e.JobCreatedAt = time.Now()
	reg := newRegistry(t, e)
	set := &fakeSettler{}
	objs := &fakeObjects{
		job: runningJob("fm-t1"),
		pods: []corev1.Pod{
			podFor("fm-t1", func(p *corev1.Pod) {
				p.CreationTimestamp = metav1.NewTime(e.JobCreatedAt.Add(-time.Minute))
				p.Spec.NodeName = "company-02"
				p.Status.Phase = corev1.PodUnknown
			}),
			podFor("fm-t1", func(p *corev1.Pod) {
				p.CreationTimestamp = metav1.NewTime(e.JobCreatedAt)
				p.Spec.NodeName = "company-01"
				p.Status.Phase = corev1.PodRunning
			}),
		},
	}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, set)
	r.recheckWindow = 0

	// Two rounds: the first only records an end sighting, the second is where
	// the eviction would be compensated.
	for round := 1; round <= 2; round++ {
		if err := r.Reconcile(context.Background()); err != nil {
			t.Fatalf("Reconcile round %d: %v", round, err)
		}
	}
	if _, fails, _, _, _ := set.calls(); fails != 0 {
		t.Fatalf("failJobs = %d, want 0 (the predecessor's pod is not this instance)", fails)
	}
	got, ok := reg.Get("t1")
	if !ok {
		t.Fatal("entry t1 vanished")
	}
	if got.NodeName != "company-01" {
		t.Fatalf("node_name = %q, want company-01 from the current instance's pod", got.NodeName)
	}
}

// ---- §4.6: server-side cancel -------------------------------------------

func TestReconcileCancelledWaitsForAckThenReleases(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")},
		&fakeStatus{status: "cancelled"}, set)

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, _, _, _, discards := set.calls(); discards != 0 {
		t.Fatalf("discards = %d, want 0 while the daemon may still ack", discards)
	}

	// Still inside the ack timeout: nothing may be removed yet.
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, _, _, _, discards := set.calls(); discards != 0 {
		t.Fatalf("discards = %d, want 0 before the ack timeout", discards)
	}

	// The ack timeout has passed: the entry settles as cancelled without a
	// fail report, and the Job is deleted — the verdict starts the drain
	// (ADR-017 path ④).
	r.cancelAckTimeout = 0
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.settles) != 1 || set.settles[0] != (settleCall{"t1", "cancelled"}) {
		t.Fatalf("settles = %+v, want one settle of t1/cancelled", set.settles)
	}
	if _, fails, _, _, _ := set.calls(); fails != 0 {
		t.Fatalf("failJobs = %d, want no fail report for a cancelled task", fails)
	}
}

func TestReconcileCancelledPrunedWhenDaemonAcks(t *testing.T) {
	e := testEntry("t1")
	reg := newRegistry(t, e)
	set := &fakeSettler{}
	status := &fakeStatus{status: "cancelled"}
	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")}, status, set)

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(r.cancels) != 1 {
		t.Fatalf("cancels = %d, want the first sighting recorded", len(r.cancels))
	}
	// The daemon acked: the entry is terminal and gone from the index.
	e.State, e.Result = registry.StateTerminal, registry.ResultCancelled
	if err := reg.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := reg.Delete(e.TaskID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(r.cancels) != 0 {
		t.Fatalf("cancels = %d, want the stale sighting pruned", len(r.cancels))
	}
	if _, _, _, _, discards := set.calls(); discards != 0 {
		t.Fatalf("discards = %d, want 0 (the ack path owns the cleanup)", discards)
	}
}

// A cancelled observation that falls back to a pre-delivery status must drop
// the cancel deadline (02-contracts §4.6): a stale timestamp makes
// nextDeadline() return a past instant — the loop's wait goes to zero and
// spins — and lets the next cancelled observation settle on the old window
// instead of the daemon's fresh cancel-ack window.
func TestReconcileStatusFallbackClearsCancelDeadline(t *testing.T) {
	for _, status := range []string{"queued", "dispatched"} {
		t.Run(status, func(t *testing.T) {
			reg := newRegistry(t, testEntry("t1"))
			set := &fakeSettler{}
			st := &fakeStatus{status: "cancelled"}
			r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")}, st, set)
			r.cancelAckTimeout = 50 * time.Millisecond

			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			first, ok := r.cancels["t1"]
			if !ok {
				t.Fatal("the cancelled sighting was not recorded")
			}
			if next, ok := r.nextDeadline(); !ok || !next.Equal(first.Add(r.cancelAckTimeout)) {
				t.Fatalf("nextDeadline = %v, %v; want the cancel deadline", next, ok)
			}

			// The server falls back to a pre-delivery status: the entry is
			// kept and the confirmation window is dropped with the
			// observation.
			time.Sleep(r.cancelAckTimeout + 10*time.Millisecond)
			st.status = status
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if _, ok := r.cancels["t1"]; ok {
				t.Fatal("the stale cancel deadline survived the status fallback")
			}
			if _, ok := r.nextDeadline(); ok {
				t.Fatal("nextDeadline() still reports the stale cancel deadline")
			}
			if _, _, settles, _, discards := set.calls(); settles+discards != 0 {
				t.Fatalf("settles=%d discards=%d, want the entry kept", settles, discards)
			}

			// A later cancellation opens a fresh window: the expired stale
			// timestamp would have settled it immediately.
			st.status = "cancelled"
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if len(set.settles) != 0 || len(set.discards) != 0 {
				t.Fatalf("settles=%+v discards=%v, want a fresh cancel-ack window", set.settles, set.discards)
			}
			if _, ok := r.cancels["t1"]; !ok {
				t.Fatal("the fresh cancelled sighting was not recorded")
			}
		})
	}
}

// ---- §4.6: heartbeat suspect --------------------------------------------

func TestSuspectSignalTracksStaleHeartbeats(t *testing.T) {
	e := testEntry("t1")
	reg := newRegistry(t, e)
	reg.PutRuntime(registry.JobRuntime{
		JobRuntimeID:    e.JobRuntimeID,
		DaemonID:        e.DaemonID,
		LastHeartbeatAt: time.Now().Add(-defaultSuspectThreshold - time.Second),
	})
	set := &fakeSettler{}
	metrics := &fakeMetrics{}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")},
		&fakeStatus{status: "running"}, set, WithMetrics(metrics), WithLogger(logger))

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if metrics.last != 1 {
		t.Fatalf("foreman_heartbeat_suspect = %d, want 1", metrics.last)
	}
	if _, fails, _, _, discards := set.calls(); fails+discards != 0 {
		t.Fatalf("fails=%d discards=%d, want no action on a suspect task", fails, discards)
	}
	if !bytes.Contains(logs.Bytes(), []byte("task.heartbeat_suspect")) {
		t.Fatalf("missing task.heartbeat_suspect event in %s", logs.String())
	}

	// The partition heals: two fresh heartbeats clear the flag.
	logs.Reset()
	reg.TouchHeartbeat(e.JobRuntimeID, time.Now())
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if metrics.last != 0 {
		t.Fatalf("foreman_heartbeat_suspect = %d, want 0 after recovery", metrics.last)
	}
	if bytes.Contains(logs.Bytes(), []byte("task.heartbeat_suspect")) {
		t.Fatalf("suspect event repeated after recovery: %s", logs.String())
	}
}

func TestSuspectIgnoresUnregisteredDaemons(t *testing.T) {
	reg := newRegistry(t, testEntry("t1")) // no runtime row: still booting
	metrics := &fakeMetrics{}
	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")},
		&fakeStatus{status: "running"}, &fakeSettler{}, WithMetrics(metrics))

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if metrics.last != 0 {
		t.Fatalf("foreman_heartbeat_suspect = %d, want 0 (the boot deadline owns pre-registration)", metrics.last)
	}
}

// ---- §4.6/§10: durable report queue -------------------------------------

// TestReconcileDrainsPendingReports pins the round's queue call: the queue is
// the scheduler's own (ADR-031), so the recovery loop reaches it through the
// settlement surface once per round, and a failing drain never fails the
// round.
func TestReconcileDrainsPendingReports(t *testing.T) {
	reg := newRegistry(t)
	set := &fakeSettler{drainRemaining: 1, drainErr: errors.New("upstream down")}
	r := newTestReconciler(t, reg, &fakeObjects{}, &fakeStatus{}, set)

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if set.drainCalls != 1 {
		t.Fatalf("drain calls = %d, want 1", set.drainCalls)
	}
}

// TestReconcileSettlesNothingWhileTheCarrierListFails pins §4.3: a failed
// carrier list is not an empty queue, so the round settles nothing (a
// terminal entry waits, and no synthetic terminal can be emitted). The known
// queue entries still replay — that is delivery, not a settlement decision —
// and the next round retries the list.
func TestReconcileSettlesNothingWhileTheCarrierListFails(t *testing.T) {
	entry := testEntry("t1")
	entry.State = registry.StateTerminal
	entry.Result = registry.ResultCompleted
	reg := newRegistry(t, entry)
	set := &fakeSettler{restoreErr: errors.New("list secrets: apiserver unavailable")}
	r := newTestReconciler(t, reg, &fakeObjects{}, &fakeStatus{}, set)

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if set.restoreCalls != 1 {
		t.Fatalf("restore calls = %d, want 1", set.restoreCalls)
	}
	if len(set.settles) != 0 {
		t.Fatalf("settles = %v, want none while the queue is not rebuilt", set.settles)
	}
	if set.drainCalls != 1 {
		t.Fatalf("drain calls = %d, want the known entries replayed", set.drainCalls)
	}

	set.restoreErr = nil
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile retry: %v", err)
	}
	if len(set.settles) != 1 || set.settles[0].taskID != "t1" {
		t.Fatalf("settles after the retry = %v, want the terminal entry", set.settles)
	}
}

// ---- error handling -----------------------------------------------------

func TestReconcileSkipsRoundWhenK8sIsUnreachable(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	objs := &fakeObjects{podsErr: errors.New("api server down")}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, set)

	err := r.Reconcile(context.Background())
	if err == nil {
		t.Fatal("Reconcile: want an error when the K8s API is unreachable")
	}
	if _, _, _, _, discards := set.calls(); discards != 0 {
		t.Fatalf("discards = %d, want no settlement on a blind round", discards)
	}
}

func TestReconcileStatusErrorLeavesEntryAlone(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")},
		&fakeStatus{err: errors.New("server 503")}, set)

	if err := r.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile: want the C13 error surfaced")
	}
	if _, _, _, _, discards := set.calls(); discards != 0 {
		t.Fatalf("discards = %d, want no compensation without a C13 truth", discards)
	}
}

func TestReconcileContinuesAfterOneEntryFails(t *testing.T) {
	broken := testEntry("t1")
	broken.State = registry.StateTerminal
	broken.Result = registry.ResultFailed
	reg := newRegistry(t, broken, testEntry("t2"))
	set := &fakeSettler{err: errors.New("k8s delete failed")}
	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t2")}, &fakeStatus{status: "running"}, set)

	if err := r.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile: want the first error surfaced")
	}
	if len(set.adopts) != 1 || set.adopts[0] != (adoptCall{"t2", "running"}) {
		t.Fatalf("adopts = %v, want [t2/running] — one broken entry must not stop the round", set.adopts)
	}
}

// ---- the loop -----------------------------------------------------------

func TestRunConvergesUntilContextIsDone(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")},
		&fakeStatus{status: "running"}, set)
	r.cfg.ReconcileInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	waitFor(t, "repeated rounds", func() bool {
		_, _, _, adopts, _ := set.calls()
		return adopts >= 2
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop on context cancellation")
	}
}

func TestRunWakesOnWatcherNotification(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")},
		&fakeStatus{status: "running"}, set, WithWatcher(&fakeWatcher{}))
	r.cfg.ReconcileInterval = time.Hour // only the watcher can trigger round two

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	waitFor(t, "a watcher-triggered round", func() bool {
		_, _, _, adopts, _ := set.calls()
		return adopts >= 2
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestRunDrivesDeferredDeadlineWithoutWatcher(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}
	objs := &fakeObjects{job: failedJob("fm-t1", "BackoffLimitExceeded")}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, set)
	// Only the deferred deadline may wake the loop: a settlement that waited
	// for the next interval would never be observed here.
	r.cfg.ReconcileInterval = time.Hour
	r.recheckWindow = 30 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	waitFor(t, "the deferred deadline to compensate", func() bool {
		_, fails, _, _, _ := set.calls()
		return fails == 1
	})
}

type fakeWatcher struct{}

func (w *fakeWatcher) Watch(ctx context.Context, notify func()) error {
	notify()
	<-ctx.Done()
	return nil
}

// ---- node placement (input of the per-node soft cap) ----

func TestReconcileRecordsPodNodePlacement(t *testing.T) {
	e := testEntry("t1")
	e.IssueID = "issue-t1"
	e.PriorSessionID = "/state/home/.multica/pi-sessions/sess-1.jsonl"
	reg := newRegistry(t, e)
	objs := &fakeObjects{
		job:  runningJob("fm-t1"),
		pods: []corev1.Pod{podFor("fm-t1", func(p *corev1.Pod) { p.Spec.NodeName = "company-02" })},
	}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, &fakeSettler{})

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got, _ := reg.Get("t1")
	if got.NodeName != "company-02" {
		t.Fatalf("node_name = %q, want company-02", got.NodeName)
	}
	// Placement feeds the per-node cap and the terminal report, never the
	// session index: only S13/S14 teach it (ADR-015).
	if last := reg.LastNodeForSession(e.WorkspaceID, registry.SessionKey(e.PriorSessionID)); last != "" {
		t.Fatalf("LastNodeForSession = %q, want empty before a terminal report", last)
	}
}

// A Job rebuilds its pod after an eviction or a node drain. The placement fact
// follows the current pod: the per-node cap counts the entry on the node it now
// occupies, and the learning face names the node the session was written on.
func TestReconcileFollowsRebuiltPodToAnotherNode(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	objs := &fakeObjects{
		job: runningJob("fm-t1"),
		pods: []corev1.Pod{podFor("fm-t1", func(p *corev1.Pod) {
			p.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
			p.Spec.NodeName = "company-01"
		})},
	}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, &fakeSettler{})

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if load := reg.ActiveJobsByNode(); load["company-01"] != 1 || len(load) != 1 {
		t.Fatalf("ActiveJobsByNode = %v, want company-01=1", load)
	}

	// The controller's replacement pod lands on another node.
	objs.mu.Lock()
	objs.pods = []corev1.Pod{podFor("fm-t1", func(p *corev1.Pod) {
		p.CreationTimestamp = metav1.NewTime(time.Now())
		p.Spec.NodeName = "company-02"
	})}
	objs.mu.Unlock()

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile after the rebuild: %v", err)
	}
	got, _ := reg.Get("t1")
	if got.NodeName != "company-02" {
		t.Fatalf("node_name = %q, want company-02 from the rebuilt pod", got.NodeName)
	}
	load := reg.ActiveJobsByNode()
	if load["company-01"] != 0 || load["company-02"] != 1 {
		t.Fatalf("ActiveJobsByNode = %v, want the entry counted on company-02 only", load)
	}
}

// A rebuild leaves a window in which the predecessor pod is gone and the
// replacement is not scheduled yet, so the round observes no placement at all.
// An empty observation is not a write source (§3, §4.5): the recorded node
// survives, keeping the per-node load and the terminal report's learning face
// on the last real placement.
func TestReconcileKeepsNodeWhileRebuiltPodIsPending(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	objs := &fakeObjects{
		job:  runningJob("fm-t1"),
		pods: []corev1.Pod{podFor("fm-t1", func(p *corev1.Pod) { p.Spec.NodeName = "company-01" })},
	}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, &fakeSettler{})

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got, _ := reg.Get("t1"); got.NodeName != "company-01" {
		t.Fatalf("node_name = %q, want company-01 after the first placement", got.NodeName)
	}

	// The controller's replacement pod has no node yet.
	objs.mu.Lock()
	objs.pods = []corev1.Pod{podFor("fm-t1", nil)}
	objs.mu.Unlock()

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile during the rebuild: %v", err)
	}
	if got, _ := reg.Get("t1"); got.NodeName != "company-01" {
		t.Fatalf("node_name = %q, want company-01 retained while the replacement is unscheduled", got.NodeName)
	}
	if load := reg.ActiveJobsByNode(); load["company-01"] != 1 || len(load) != 1 {
		t.Fatalf("ActiveJobsByNode = %v, want company-01=1 retained", load)
	}
}

func TestReconcileLeavesNodeEmptyWhilePodIsPending(t *testing.T) {
	e := testEntry("t1")
	reg := newRegistry(t, e)
	objs := &fakeObjects{
		job:  runningJob("fm-t1"),
		pods: []corev1.Pod{podFor("fm-t1", nil)}, // no spec.nodeName yet
	}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, &fakeSettler{})

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got, _ := reg.Get("t1"); got.NodeName != "" {
		t.Fatalf("node_name = %q, want empty for an unscheduled pod", got.NodeName)
	}
}

// The session index expires on the convergence round alone (ADR-015): the
// sweep runs before the round looks at live entries, so an index with nothing
// in flight still converges instead of keeping a stale key until the next
// claim anywhere.
func TestReconcileExpiresSessionIndexEntries(t *testing.T) {
	now := time.Now()
	reg := registry.New(func() time.Time { return now })
	reg.LearnSession("ws-1", "sess-stale", "company-02")
	now = now.Add(2 * time.Hour)
	reg.LearnSession("ws-1", "sess-fresh", "company-03")

	r, err := New(Config{ReconcileInterval: time.Minute, AgentStateTTL: time.Hour},
		reg, &fakeObjects{}, &fakeStatus{}, &fakeSettler{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := reg.LastNodeForSession("ws-1", "sess-stale"); got != "" {
		t.Fatalf("expired entry still points at %q", got)
	}
	if got := reg.LastNodeForSession("ws-1", "sess-fresh"); got != "company-03" {
		t.Fatalf("fresh entry = %q, want company-03", got)
	}
}

// TestReconcileCleanupProceedsWhenPodsUnreadable pins the degraded round: a
// failed pod read is not a teardown sign (§4.4), so only the end-reason
// judgement of live entries waits — terminal Job deletion and the drain
// closure keep moving.
func TestReconcileCleanupProceedsWhenPodsUnreadable(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	if _, _, err := reg.MarkTerminal("t1", registry.ResultCompleted, "", time.Time{}); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	set := &fakeSettler{terminated: true}
	objs := &fakeObjects{podsErr: errors.New("api server down")}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, set)

	if err := r.Reconcile(context.Background()); err == nil {
		t.Fatal("a blind round must still report the pod-read error")
	}
	set.mu.Lock()
	reclaims := append([]reclaimCall(nil), set.reclaims...)
	set.mu.Unlock()
	if len(reclaims) != 1 || reclaims[0].jobName != "fm-t1" {
		t.Fatalf("reclaims = %+v, want the terminal entry reclaimed despite the blind pod read", reclaims)
	}
}

// TestReclaimObjectlessEntrySkipsJobEvidence pins the invalid-task-id drain:
// no Job/Secret ever existed, so the identity-derived names may be illegal
// selectors and there is no termination fact to read. The entry is reclaimed
// with the objectless reason instead.
func TestReclaimObjectlessEntrySkipsJobEvidence(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	if _, _, err := reg.MarkTerminal("t1", registry.ResultFailed, scheduler.FailureReasonInvalidTaskID, time.Time{}); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	set := &fakeSettler{}
	objs := &fakeObjects{jobErr: errors.New("invalid label value")}
	r := newTestReconciler(t, reg, objs, &fakeStatus{status: "running"}, set)

	_ = r.Reconcile(context.Background())

	set.mu.Lock()
	reclaims := append([]reclaimCall(nil), set.reclaims...)
	set.mu.Unlock()
	if len(reclaims) != 1 {
		t.Fatalf("reclaims = %+v, want one", reclaims)
	}
	if reclaims[0].reason != scheduler.JobTerminationReasonNoObjects {
		t.Errorf("reclaim reason = %q, want %q", reclaims[0].reason, scheduler.JobTerminationReasonNoObjects)
	}
	// The branch must sit before every evidence read: moving any of them ahead
	// of it turns this red.
	terminated, lastPod, alive := set.evidenceCalls()
	if terminated != 0 || lastPod != 0 || alive != 0 {
		t.Errorf("termination evidence reads = JobTerminated:%d LastPodTerminatedAt:%d AlivePods:%d, want none",
			terminated, lastPod, alive)
	}
	if got := objs.jobGetsSeen(); got != 0 {
		t.Errorf("GetJob calls = %d, want none for an objectless entry", got)
	}
}
