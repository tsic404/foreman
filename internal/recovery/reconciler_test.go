package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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
}

func (f *fakeObjects) GetJob(context.Context, string) (*batchv1.Job, error) {
	return f.job, f.jobErr
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
	adopts    []string
	discards  []string
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
	// DrainPendingReports' programmed answer.
	drainCalls     int
	drainRemaining int
	drainErr       error
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

func (f *fakeSettler) AdoptRunning(_ context.Context, taskID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adopts = append(f.adopts, taskID)
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
	return f.terminated, f.terminatedErr
}

func (f *fakeSettler) LastPodTerminatedAt(context.Context, string, time.Time) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastTerminatedAt, f.lastTermErr
}

func (f *fakeSettler) AlivePods(context.Context, string, time.Time) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.alivePods, f.alivePodsErr
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
	e.Payload = nil // rebuilt from K8s: the payload never left process memory
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

func TestReconcileRunningTaskIsAdopted(t *testing.T) {
	reg := newRegistry(t, testEntry("t1"))
	set := &fakeSettler{}

	r := newTestReconciler(t, reg, &fakeObjects{job: runningJob("fm-t1")}, &fakeStatus{status: "running"}, set)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(set.adopts) != 1 || set.adopts[0] != "t1" {
		t.Fatalf("adopts = %v, want [t1]", set.adopts)
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
	if len(set.adopts) != 1 || set.adopts[0] != "t2" {
		t.Fatalf("adopts = %v, want [t2] — one broken entry must not stop the round", set.adopts)
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
	if last := reg.LastNodeForSession(registry.SessionKey(e.PriorSessionID)); last != "" {
		t.Fatalf("LastNodeForSession = %q, want empty before a terminal report", last)
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
	reg.LearnSession("sess-stale", "company-02")
	now = now.Add(2 * time.Hour)
	reg.LearnSession("sess-fresh", "company-03")

	r, err := New(Config{ReconcileInterval: time.Minute, AgentStateTTL: time.Hour},
		reg, &fakeObjects{}, &fakeStatus{}, &fakeSettler{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := reg.LastNodeForSession("sess-stale"); got != "" {
		t.Fatalf("expired entry still points at %q", got)
	}
	if got := reg.LastNodeForSession("sess-fresh"); got != "company-03" {
		t.Fatalf("fresh entry = %q, want company-03", got)
	}
}
