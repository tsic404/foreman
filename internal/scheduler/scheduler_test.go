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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
)

var testNow = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

// testDrainTimeout is the fixture's FOREMAN_JOB_DRAIN_TIMEOUT: the credential
// reclaim upper bound written at the alive → draining transition (ADR-017).
const testDrainTimeout = 2 * time.Minute

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
	deleteSecretErr error
	getJobErr       error
	listPodsErr     error

	// existingJobs models Jobs already in the cluster: CreateJob answers
	// AlreadyExists for their names and GetJob returns them (terminating,
	// with a deletionTimestamp, for the names listed in terminatingJobs).
	existingJobs    map[string]*batchv1.Job
	terminatingJobs map[string]bool
	onGetJob        func()
	onCreateJob     func()

	// secrets and pods back ListSecrets/ListPods: the orphan-Secret sweep
	// and the Job-termination evidence read them.
	secrets []corev1.Secret
	pods    []corev1.Pod

	// onSecretCreate / onDeleteSecret observe the probe teardown order.
	onSecretCreate func()
	onDeleteSecret func()
}

// createJobCreationTimestamp is what the fake apiserver stamps on a created
// Job: the scheduler reads it as job_created_at (the pod instance filter's
// basis), so it must be non-zero and equal for every instance of a name.
var createJobCreationTimestamp = testNow.Add(-time.Hour)

func (f *fakeJobs) CreateJob(_ context.Context, job *batchv1.Job) (*batchv1.Job, error) {
	// onCreateJob runs before the answer: it lets a test land a concurrent
	// registry write (e.g. a prepare-lease renewal) inside the claim's
	// create window.
	if f.onCreateJob != nil {
		f.onCreateJob()
	}
	if f.createJobErr != nil {
		return nil, f.createJobErr
	}
	if _, ok := f.existingJobs[job.Name]; ok {
		return nil, apierrors.NewAlreadyExists(schema.GroupResource{Group: "batch", Resource: "jobs"}, job.Name)
	}
	f.createdJobs = append(f.createdJobs, job)
	created := job.DeepCopy()
	created.CreationTimestamp = metav1.Time{Time: createJobCreationTimestamp}
	return created, nil
}

func (f *fakeJobs) DeleteJob(_ context.Context, name string) error {
	if f.deleteJobErr != nil {
		return f.deleteJobErr
	}
	f.deletedJobs = append(f.deletedJobs, name)
	return nil
}

// GetJob answers from existingJobs: a name listed in terminatingJobs comes
// back with a deletionTimestamp (the foreground deletion window). onGetJob
// runs first — on error probes too — so a test can release the name, trip
// the wait deadline, or clear getJobErr.
func (f *fakeJobs) GetJob(_ context.Context, name string) (*batchv1.Job, error) {
	if f.onGetJob != nil {
		f.onGetJob()
	}
	if f.getJobErr != nil {
		return nil, f.getJobErr
	}
	job, ok := f.existingJobs[name]
	if !ok {
		return nil, nil
	}
	out := job.DeepCopy()
	if f.terminatingJobs[name] {
		out.DeletionTimestamp = &metav1.Time{Time: testNow}
	}
	return out, nil
}

func (f *fakeJobs) CreateSecret(_ context.Context, secret *corev1.Secret) error {
	if f.onSecretCreate != nil {
		f.onSecretCreate()
	}
	if f.createSecretErr != nil {
		return f.createSecretErr
	}
	f.createdSecrets = append(f.createdSecrets, secret)
	return nil
}

func (f *fakeJobs) DeleteSecret(_ context.Context, name string) error {
	if f.onDeleteSecret != nil {
		f.onDeleteSecret()
	}
	if f.deleteSecretErr != nil {
		return f.deleteSecretErr
	}
	f.deletedSecrets = append(f.deletedSecrets, name)
	return nil
}

// deleted / secretDeleted report whether a name went through DeleteJob /
// DeleteSecret.
func (f *fakeJobs) deleted(name string) bool {
	for _, got := range f.deletedJobs {
		if got == name {
			return true
		}
	}
	return false
}

func (f *fakeJobs) secretDeleted(name string) bool {
	for _, got := range f.deletedSecrets {
		if got == name {
			return true
		}
	}
	return false
}

func (f *fakeJobs) ListJobs(context.Context) ([]batchv1.Job, error) { return f.jobs, nil }

func (f *fakeJobs) ListSecrets(context.Context) ([]corev1.Secret, error) { return f.secrets, nil }

// ListPods applies the selector the scheduler passes (job-name=<job>), so the
// tests exercise the same ownership key the informer-based path uses.
func (f *fakeJobs) ListPods(_ context.Context, labelSelector string) ([]corev1.Pod, error) {
	if f.listPodsErr != nil {
		return nil, f.listPodsErr
	}
	selector, err := labels.Parse(labelSelector)
	if err != nil {
		return nil, err
	}
	var out []corev1.Pod
	for i := range f.pods {
		if selector.Matches(labels.Set(f.pods[i].Labels)) {
			out = append(out, f.pods[i])
		}
	}
	return out, nil
}

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
	onForward   func() // test hook: fires inside Forward (simulates an in-flight round)
	forwards    []forwardCall
}

func (f *fakeServer) TaskStatus(context.Context, string) (string, error) {
	if f.onStatus != nil {
		f.onStatus()
	}
	return f.status, f.statusErr
}

func (f *fakeServer) Forward(_ context.Context, ep Endpoint, taskID string, body []byte) (int, []byte, error) {
	if f.onForward != nil {
		f.onForward()
	}
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
	job       *batchv1.Job
	secret    *corev1.Secret
	tokenHash string
	err       error
	got       jobbuilder.TaskEntry
	delay     time.Duration

	// Catalog-probe face: the recorded call and the injected failure.
	probeRequestID string
	probeRuntimeID string
	probeWorkspace string
	probeBudget    time.Duration
	probeJob       *batchv1.Job
	probeSecret    *corev1.Secret
	probeTokenHash string
	probeErr       error
}

func (f *fakeBuilder) Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, string, error) {
	f.got = e
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	if f.err != nil {
		return nil, nil, "", f.err
	}
	job := f.job
	if job == nil {
		job = &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "fm-" + e.TaskID}}
	}
	secret := f.secret
	if secret == nil {
		secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "fm-" + e.TaskID + "-cred"}}
	}
	hash := f.tokenHash
	if hash == "" {
		hash = "hash-of-" + e.TaskID
	}
	return job, secret, hash, nil
}

// BuildCatalogProbe records the probe call and renders the probe objects a
// real builder would: Job fm-catalog-<requestID> with its credential Secret.
func (f *fakeBuilder) BuildCatalogProbe(requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, error) {
	f.probeRequestID, f.probeRuntimeID = requestID, jobRuntimeID
	f.probeWorkspace, f.probeBudget = workspaceID, budget
	if f.probeErr != nil {
		return nil, nil, "", f.probeErr
	}
	name := "fm-catalog-" + requestID
	job := f.probeJob
	if job == nil {
		job = &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	secret := f.probeSecret
	if secret == nil {
		secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-cred"}}
	}
	hash := f.probeTokenHash
	if hash == "" {
		hash = "hash-of-" + name
	}
	return job, secret, hash, nil
}

type fakeMetrics struct {
	claimed     int
	terminal    []string
	inflight    []int
	duplicates  int
	reclaim     []string
	drainSecond []float64
}

func (f *fakeMetrics) TaskClaimed()               { f.claimed++ }
func (f *fakeMetrics) TaskTerminal(result string) { f.terminal = append(f.terminal, result) }
func (f *fakeMetrics) JobBootSeconds(float64)     {}
func (f *fakeMetrics) JobCreateSeconds(float64)   {}
func (f *fakeMetrics) InflightJobs(n int)         { f.inflight = append(f.inflight, n) }
func (f *fakeMetrics) DuplicateDispatch()         { f.duplicates++ }
func (f *fakeMetrics) CredReclaimed(reason string, drainSeconds float64) {
	f.reclaim = append(f.reclaim, reason)
	f.drainSecond = append(f.drainSecond, drainSeconds)
}

// fakeRevoker records the digests the credential reclaim revokes.
type fakeRevoker struct {
	revoked []string
}

func (f *fakeRevoker) Revoke(tokenHash string) { f.revoked = append(f.revoked, tokenHash) }

// ---- helpers ----

type fixture struct {
	reg     *registry.Registry
	jobs    *fakeJobs
	server  *fakeServer
	builder *fakeBuilder
	metrics *fakeMetrics
	revoker *fakeRevoker
	sched   *Scheduler
	pending *fakePendingReports
	now     time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{
		jobs:    &fakeJobs{},
		server:  &fakeServer{},
		builder: &fakeBuilder{},
		metrics: &fakeMetrics{},
		revoker: &fakeRevoker{},
		pending: &fakePendingReports{},
		now:     testNow,
	}
	// The registry shares the fixture clock: with the real clock its done
	// index (TTL 24h) would expire entries stamped at testNow and the suite
	// would rot with wall time.
	f.reg = registry.New(func() time.Time { return f.now }, registry.WithDrainTimeout(testDrainTimeout))
	cfg, err := LoadConfig(testEnv(nil))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	s, err := New(cfg, f.reg, f.jobs, f.builder, f.server, f.metrics,
		WithClock(func() time.Time { return f.now }),
		WithPendingReports(f.pending),
		WithRevoker(f.revoker),
		WithProbeRegistry(registry.NewProbeRegistry()),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	f.sched = s
	return f
}

// testPod is a pod of one Job instance: it carries the ownership label the
// termination evidence selects on and a creationTimestamp the instance filter
// keeps.
func testPod(jobName string, phase corev1.PodPhase) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              jobName + "-pod",
			Labels:            map[string]string{podJobNameLabel: jobName},
			CreationTimestamp: metav1.Time{Time: testNow},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
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

// resumeClaimPayload is the resume form of claimPayload: the payload names the
// session file it continues, which is the soft affinity key source (ADR-015).
func resumeClaimPayload(id, priorSession string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
		"id": %q,
		"agent_id": "agent-1",
		"issue_id": "issue-1",
		"issue_identifier": "MULTI-1",
		"workspace_id": "ws-1",
		"runtime_id": "real-runtime",
		"auth_token": "mat_secret",
		"prior_session_id": %q,
		"agent": {"model": "x"}
	}`, id, priorSession))
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
	// The claim stamped the server's 45s prepare lease (§1.1 C3), so the
	// projected lease_keep_until (§3.1) starts out live — never the zero
	// value the field would otherwise carry into /foreman/tasks.
	if want := testNow.Add(PrepareLeaseTTL); !e.LeaseKeepUntil.Equal(want) {
		t.Fatalf("lease_keep_until = %v, want %v", e.LeaseKeepUntil, want)
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

// TestClaimKeepsLeaseRenewedDuringJobCreate guards the claim path's second
// registry write against a lost update: the claim's snapshot predates the Job
// create — up to jobNameWait long when a terminating predecessor still holds
// the name — so re-Putting that snapshot wholesale would revert
// lease_keep_until behind the LeaseKeeper's last renewal, i.e. behind exactly
// the advance test-cases/protocol-lease.md 场景 A asserts.
func TestClaimKeepsLeaseRenewedDuringJobCreate(t *testing.T) {
	f := newFixture(t)
	renewed := testNow.Add(2 * time.Minute)
	f.jobs.onCreateJob = func() { f.reg.SetLeaseKeepUntil("task-1", renewed) }

	e := f.claim(t, "task-1")

	if e.State != registry.StateJobCreated {
		t.Fatalf("state = %v, want job_created", e.State)
	}
	if !e.JobCreatedAt.Equal(createJobCreationTimestamp) {
		t.Fatalf("job_created_at = %v, want %v", e.JobCreatedAt, createJobCreationTimestamp)
	}
	if !e.LeaseKeepUntil.Equal(renewed) {
		t.Fatalf("lease_keep_until = %v, want the concurrent renewal %v", e.LeaseKeepUntil, renewed)
	}
}

// TestClaimRollsBackObjectsWhenEntryLeftDuringJobCreate covers the create
// window's failure branch: once the entry is settled or reclaimed while the
// Job is being created, the fresh Secret and Job have no owner left, and
// nothing else reclaims an entry-less Job (failure-handling #16 skips Jobs
// that exist, ListJobs is read only by the startup rebuild) — so the claim
// must delete both before returning its error.
func TestClaimRollsBackObjectsWhenEntryLeftDuringJobCreate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		leave func(f *fixture)
	}{
		{"settled", func(f *fixture) {
			if _, err := f.reg.MarkTerminal("task-1", registry.ResultCancelled, time.Time{}); err != nil {
				t.Errorf("MarkTerminal: %v", err)
			}
		}},
		{"reclaimed", func(f *fixture) {
			if err := f.reg.Delete("task-1"); err != nil {
				t.Errorf("Delete: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.jobs.onCreateJob = func() { tc.leave(f) }

			err := f.sched.OnClaim(context.Background(), claimPayload("task-1"))
			if err == nil {
				t.Fatal("OnClaim must fail once the entry left the registry")
			}
			if len(f.jobs.deletedJobs) != 1 || f.jobs.deletedJobs[0] != "fm-task-1" {
				t.Errorf("deletedJobs = %v, want the entry-less Job rolled back", f.jobs.deletedJobs)
			}
			// The stale-secret sweep runs before the create, the rollback
			// after it: the credential Secret is deleted twice in this path.
			if len(f.jobs.deletedSecrets) != 2 ||
				f.jobs.deletedSecrets[0] != "fm-task-1-cred" || f.jobs.deletedSecrets[1] != "fm-task-1-cred" {
				t.Errorf("secret rollback: %v", f.jobs.deletedSecrets)
			}
			if _, ok := f.reg.Get("task-1"); ok && tc.name == "reclaimed" {
				t.Error("reclaimed entry was resurrected")
			}
		})
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

// A pre-start replacement deletes the old Job with Foreground, so the name
// (fm-<task_id>) stays taken until the cascade finishes. The replacement
// must wait for it instead of swallowing AlreadyExists as a restart replay:
// the entry would otherwise bind to a Job about to disappear and the task
// would converge to job_missing (AC-09: no task lost).
func TestDuplicateClaimWaitsForTerminatingJobName(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "dispatched"
	f.jobs.existingJobs = map[string]*batchv1.Job{"fm-task-1": {ObjectMeta: metav1.ObjectMeta{Name: "fm-task-1"}}}
	f.jobs.terminatingJobs = map[string]bool{"fm-task-1": true}
	probes := 0
	f.jobs.onGetJob = func() {
		probes++
		if probes >= 2 {
			delete(f.jobs.existingJobs, "fm-task-1") // the cascade finished
		}
	}

	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if len(f.jobs.createdJobs) != 2 {
		t.Fatalf("jobs created = %d, want 2", len(f.jobs.createdJobs))
	}
	if len(f.server.forwards) != 0 {
		t.Fatalf("task reported failed on a name collision: %+v", f.server.forwards)
	}
	e, ok := f.reg.Get("task-1")
	if !ok || e.State != registry.StateJobCreated || e.Attempt != 2 {
		t.Fatalf("entry = %+v", e)
	}
}

// A name that never frees (stuck pod, unresponsive node) must not become a
// false failure: the claim is dropped and the server re-dispatches the task
// once the lease lapses (failure-handling #1/#2).
func TestClaimDefersWhileJobNameStillHeld(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "dispatched"
	f.jobs.existingJobs = map[string]*batchv1.Job{"fm-task-1": {ObjectMeta: metav1.ObjectMeta{Name: "fm-task-1"}}}
	f.jobs.terminatingJobs = map[string]bool{"fm-task-1": true}
	f.jobs.onGetJob = func() { f.now = f.now.Add(jobNameWait + time.Second) }

	err := f.sched.OnClaim(context.Background(), claimPayload("task-1"))
	if !errors.Is(err, errJobNameHeld) {
		t.Fatalf("OnClaim err = %v, want errJobNameHeld", err)
	}
	if len(f.server.forwards) != 0 {
		t.Fatalf("deferred claim reported a failure: %+v", f.server.forwards)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("deferred claim kept the entry: its lease would be renewed, never re-dispatched")
	}
	if len(f.jobs.createdJobs) != 1 {
		t.Fatalf("jobs created = %d, want 1 (the first claim's)", len(f.jobs.createdJobs))
	}
}

// The other meaning of AlreadyExists: the Job is already there and not
// terminating (restart replay). It is kept and the claim proceeds to
// job_created without waiting.
func TestClaimKeepsReplayedJobOnAlreadyExists(t *testing.T) {
	f := newFixture(t)
	f.jobs.createJobErr = apierrors.NewAlreadyExists(schema.GroupResource{Group: "batch", Resource: "jobs"}, "fm-task-1")
	f.jobs.existingJobs = map[string]*batchv1.Job{"fm-task-1": {ObjectMeta: metav1.ObjectMeta{Name: "fm-task-1"}}}

	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	e, ok := f.reg.Get("task-1")
	if !ok || e.State != registry.StateJobCreated {
		t.Fatalf("entry = %+v", e)
	}
	if len(f.server.forwards) != 0 {
		t.Fatalf("replay reported a failure: %+v", f.server.forwards)
	}
}

// A transient read failure on the name probe must not become a task
// failure: the truth is unknown, so the claim keeps polling, hits the
// bound, and defers (dropped entry → server re-dispatch). Reporting
// job_create_failed here would settle a task that is still perfectly
// resumable (AC-09: 0 task lost).
func TestClaimProbeErrorDoesNotFailTask(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "dispatched"
	f.jobs.existingJobs = map[string]*batchv1.Job{"fm-task-1": {ObjectMeta: metav1.ObjectMeta{Name: "fm-task-1"}}}
	f.jobs.terminatingJobs = map[string]bool{"fm-task-1": true}
	f.jobs.getJobErr = errors.New("apiserver unavailable")
	f.jobs.onGetJob = func() { f.now = f.now.Add(jobNameWait + time.Second) }

	err := f.sched.OnClaim(context.Background(), claimPayload("task-1"))
	if !errors.Is(err, errJobNameHeld) {
		t.Fatalf("OnClaim err = %v, want errJobNameHeld", err)
	}
	if len(f.server.forwards) != 0 {
		t.Fatalf("probe failure reported a task failure: %+v", f.server.forwards)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("deferred claim kept the entry: its lease would be renewed, never re-dispatched")
	}
}

// The other half of the unknown-truth rule: once the probe recovers and the
// name is free, the claim creates its Job without waiting out a poll
// interval (GetJob's nil,nil means the object is gone).
func TestClaimProbeErrorClearsAndJobIsCreated(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "dispatched"
	f.jobs.existingJobs = map[string]*batchv1.Job{"fm-task-1": {ObjectMeta: metav1.ObjectMeta{Name: "fm-task-1"}}}
	f.jobs.terminatingJobs = map[string]bool{"fm-task-1": true}
	f.jobs.getJobErr = errors.New("apiserver unavailable")
	probes := 0
	f.jobs.onGetJob = func() {
		probes++
		if probes >= 2 {
			f.jobs.getJobErr = nil
			delete(f.jobs.existingJobs, "fm-task-1")
		}
	}

	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if len(f.jobs.createdJobs) != 2 {
		t.Fatalf("jobs created = %d, want 2", len(f.jobs.createdJobs))
	}
	if len(f.server.forwards) != 0 {
		t.Fatalf("recovered claim reported a failure: %+v", f.server.forwards)
	}
	e, ok := f.reg.Get("task-1")
	if !ok || e.State != registry.StateJobCreated || e.Attempt != 2 {
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
	// The rewrite is retained on the entry: the ops preview reads the entry,
	// so its payload must be the delivered form, not the claim-time one.
	var stored map[string]any
	if err := json.Unmarshal(e.Payload, &stored); err != nil {
		t.Fatalf("entry payload: %v", err)
	}
	if stored["runtime_id"] != e.JobRuntimeID {
		t.Fatalf("entry runtime_id = %v, want %q", stored["runtime_id"], e.JobRuntimeID)
	}
	if stored["auth_token"] != "mat_secret" || stored["agent_id"] != "agent-1" {
		t.Fatalf("entry payload mangled: %v", stored)
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

// A refused start (409 状态不允许, §1.2 S8 + 05-modules/proxy.md §转发)
// settles the task instead of leaving it running: failed/stale_claim,
// reported on the daemon's behalf, objects deleted — and the done index
// stops the re-dispatched task from building another Job whose start would
// be refused the same way.
func TestOnReportStart409SettlesStaleClaim(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "t-409")
	e.State = registry.StateDelivering
	if err := f.reg.Put(e); err != nil {
		t.Fatal(err)
	}
	f.server.forwardCode = 409
	f.server.status = "dispatched" // C13: the claim is still the startable one

	code, _, err := f.sched.OnReport(context.Background(), EPStart, e, []byte(`{}`))
	if err != nil || code != 409 {
		t.Fatalf("OnReport = %d, %v (the daemon keeps the server's 409)", code, err)
	}
	if e, ok := f.reg.Get("t-409"); !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Fatalf("entry = %+v, %v; want a retained draining entry", e, ok)
	}
	if !f.reg.IsDone("t-409") {
		t.Fatal("the terminal write must land in the done index")
	}
	if f.hasJob("fm-t-409") {
		t.Fatal("Job must be deleted after the refused start")
	}
	if got := f.metrics.terminal; len(got) != 1 || got[0] != "failed" {
		t.Fatalf("terminal metric = %v, want one failed", got)
	}
	if len(f.server.forwards) != 2 {
		t.Fatalf("forwards = %+v, want start + fail", f.server.forwards)
	}
	if fail := f.server.forwards[1]; fail.ep != EPFail ||
		!strings.Contains(fail.body, `"failure_reason":"stale_claim"`) {
		t.Fatalf("fail forward = %+v, want a stale_claim report", fail)
	}

	// The dispatch loop ends here: no second Job for the re-dispatched task.
	if err := f.sched.OnClaim(context.Background(), claimPayload("t-409")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if len(f.jobs.createdJobs) != 1 {
		t.Fatalf("created jobs = %+v, want no Job for a settled task", f.jobs.createdJobs)
	}
}

// 顺序规则 3: a 409 says "not startable", not "not running" — a start whose
// 200 was lost (transient 502 → daemon retry) must not have its task killed.
// C13 = running adopts the entry and keeps the Job.
func TestOnReportStart409RunningAdoptsEntry(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "t-adopt")
	e.State = registry.StateDelivering
	f.reg.Put(e)
	f.server.forwardCode = 409
	f.server.status = "running"

	code, _, err := f.sched.OnReport(context.Background(), EPStart, e, []byte(`{}`))
	if err != nil || code != 409 {
		t.Fatalf("OnReport = %d, %v (the daemon keeps the server's 409)", code, err)
	}
	got, ok := f.reg.Get("t-adopt")
	if !ok || got.State != registry.StateRunning || got.StartedAt.IsZero() {
		t.Fatalf("entry = %+v, want running (the lost 200 must not fail the task)", got)
	}
	if !f.hasJob("fm-t-adopt") {
		t.Fatal("the Job of an adopted running task must be kept")
	}
	if len(f.server.forwards) != 1 || f.server.forwards[0].ep != EPStart {
		t.Fatalf("forwards = %+v, want the start only (no synthesized fail)", f.server.forwards)
	}
	if got := f.metrics.terminal; len(got) != 0 {
		t.Fatalf("terminal metric = %v, want none (顺序规则 4: 不发假终态)", got)
	}
}

// C13 = terminal settles the entry to the server's answer: the report the
// server already holds stands, so no second (false) terminal is sent.
func TestOnReportStart409TerminalSettlesToServer(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "t-done")
	e.State = registry.StateDelivering
	f.reg.Put(e)
	f.server.forwardCode = 409
	f.server.status = "completed"

	code, _, err := f.sched.OnReport(context.Background(), EPStart, e, []byte(`{}`))
	if err != nil || code != 409 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if e, ok := f.reg.Get("t-done"); !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Fatalf("entry = %+v, %v; want a retained draining entry", e, ok)
	}
	if !f.reg.IsDone("t-done") {
		t.Fatal("the terminal write must land in the done index")
	}
	if f.hasJob("fm-t-done") {
		t.Fatal("Job must be deleted after settling to the server's terminal state")
	}
	if got := f.metrics.terminal; len(got) != 1 || got[0] != "completed" {
		t.Fatalf("terminal metric = %v, want one completed (the server's result)", got)
	}
	if len(f.server.forwards) != 1 {
		t.Fatalf("forwards = %+v, want the start only", f.server.forwards)
	}
}

// C13 = 404: the server deleted the task — the objects go and no terminal is
// synthesized for a task nobody holds (failure-handling 场景 #9).
func TestOnReportStart409VanishedReleases(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "t-gone")
	e.State = registry.StateDelivering
	f.reg.Put(e)
	f.server.forwardCode = 409
	f.server.statusErr = ErrTaskNotFound

	code, _, err := f.sched.OnReport(context.Background(), EPStart, e, []byte(`{}`))
	if err != nil || code != 409 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	if e, ok := f.reg.Get("t-gone"); !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Fatalf("entry = %+v, %v; want a retained draining entry", e, ok)
	}
	if f.hasJob("fm-t-gone") {
		t.Fatal("Job must be deleted with the vanished task")
	}
	if len(f.server.forwards) != 1 {
		t.Fatalf("forwards = %+v, want the start only (no report for a deleted task)", f.server.forwards)
	}
	if got := f.metrics.terminal; len(got) != 0 {
		t.Fatalf("terminal metric = %v, want none", got)
	}
}

// The start round owns the entry: a boot deadline that fires while the S8
// answer is still outstanding must not settle the task and delete its Job
// (failure-handling §判据模型 — the in-flight 2xx is the decisive fact).
func TestBootTimeoutWaitsForInFlightStart(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "t-race")
	e.State = registry.StateDelivering
	f.reg.Put(e)

	entered := make(chan struct{})
	release := make(chan struct{})
	f.server.onForward = func() {
		close(entered)
		<-release
	}

	startDone := make(chan struct{})
	go func() {
		defer close(startDone)
		if _, _, err := f.sched.OnReport(context.Background(), EPStart, e, []byte(`{}`)); err != nil {
			t.Errorf("OnReport: %v", err)
		}
	}()
	<-entered

	timeoutDone := make(chan struct{})
	go func() {
		defer close(timeoutDone)
		if err := f.sched.OnBootTimeout(context.Background(), "t-race"); err != nil {
			t.Errorf("OnBootTimeout: %v", err)
		}
	}()
	select {
	case <-timeoutDone:
		t.Fatal("the boot deadline settled a task whose S8 answer was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	<-startDone
	<-timeoutDone

	got, ok := f.reg.Get("t-race")
	if !ok || got.State != registry.StateRunning || got.StartedAt.IsZero() {
		t.Fatalf("entry = %+v, want running", got)
	}
	if !f.hasJob("fm-t-race") {
		t.Fatal("the Job must survive a boot deadline that lost the race to the start")
	}
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatalf("job deletes = %+v, want none", f.jobs.deletedJobs)
	}
	if len(f.server.forwards) != 1 || f.server.forwards[0].ep != EPStart {
		t.Fatalf("forwards = %+v, want the start only", f.server.forwards)
	}
}

// A late start on an entry that already settled only carries the server's
// 409 back: no transition is left and the settled result stands.
func TestOnReportStart409OnSettledEntryKeepsResult(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "t-late")
	if _, err := f.reg.MarkTerminal("t-late", registry.ResultCompleted, testNow); err != nil {
		t.Fatal(err)
	}
	f.server.forwardCode = 409

	code, _, err := f.sched.OnReport(context.Background(), EPStart, e, []byte(`{}`))
	if err != nil || code != 409 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}
	got, ok := f.reg.Get("t-late")
	if !ok || got.State != registry.StateTerminal || got.Result != registry.ResultCompleted {
		t.Fatalf("entry = %+v, want the settled result kept", got)
	}
	if len(f.server.forwards) != 1 || f.server.forwards[0].ep != EPStart {
		t.Fatalf("forwards = %+v, want the start forward only", f.server.forwards)
	}
	if got := f.metrics.terminal; len(got) != 0 {
		t.Fatalf("terminal metric = %v, want no write for a late start", got)
	}
	if !f.hasJob("fm-t-late") {
		t.Fatal("a settled entry's Job is not touched by a late start")
	}
}

// A transient start failure (5xx → 502) leaves the entry pre-start: the
// running write waits for a 2xx, so the daemon's retry can still start the
// task (05-modules/proxy.md §转发 transient branch).
func TestOnReportStartTransientKeepsEntryPreStart(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "t-5xx")
	e.State = registry.StateDelivering
	if err := f.reg.Put(e); err != nil {
		t.Fatal(err)
	}
	f.server.forwardCode = 503

	code, resp, err := f.sched.OnReport(context.Background(), EPStart, e, []byte(`{}`))
	if err != nil || code != 502 || !strings.Contains(string(resp), "upstream unavailable") {
		t.Fatalf("OnReport = %d %s, %v", code, resp, err)
	}
	got, ok := f.reg.Get("t-5xx")
	if !ok || got.State != registry.StateDelivering || !got.StartedAt.IsZero() {
		t.Fatalf("entry = %+v, want it untouched by the failed forward", got)
	}

	// The daemon's retry reaches a healthy server: delivering → running.
	f.server.forwardCode = 200
	if code, _, err = f.sched.OnReport(context.Background(), EPStart, got, []byte(`{}`)); err != nil || code != 200 {
		t.Fatalf("retried OnReport = %d, %v", code, err)
	}
	if got, _ = f.reg.Get("t-5xx"); got.State != registry.StateRunning || got.StartedAt.IsZero() {
		t.Fatalf("entry after the retry = %+v, want running", got)
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
	if e, ok := f.reg.Get("task-1"); !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Fatalf("entry = %+v, %v; want a retained draining entry", e, ok)
	}
	if !f.reg.IsDone("task-1") {
		t.Fatal("done index not written")
	}
	if len(f.jobs.deletedJobs) != 1 || f.jobs.deletedJobs[0] != "fm-task-1" {
		t.Fatalf("job deletes = %+v", f.jobs.deletedJobs)
	}
	if len(f.jobs.deletedSecrets) != 1 { // only the claim-time stale sweep
		t.Fatalf("secret deletes = %+v, want the Secret kept until the reclaim", f.jobs.deletedSecrets)
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
	if e, ok := f.reg.Get("task-1"); !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Fatalf("entry = %+v, %v; want a retained draining entry", e, ok)
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

	// The daemon still gets 502 (proxy.md 转发 switch) and the terminal
	// entry keeps its objects: the report is in the durable queue now, not
	// dropped.
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
	if e, ok := f.reg.Get("task-1"); !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Fatalf("entry = %+v, %v; want a retained draining entry", e, ok)
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
	if e, ok := f.reg.Get("task-1"); !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Fatalf("entry = %+v, %v; want a retained draining entry", e, ok)
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
		// The vanish settlement is a verdict too: it writes the done index
		// (the drain's start) so a late terminal replay stays idempotent.
		{"task deleted server-side", "", ErrTaskNotFound, true},
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
			if e, ok := f.reg.Get("task-1"); !ok || !e.IsTerminal() || !e.IsDraining() {
				t.Fatalf("entry = %+v, %v; want a retained draining entry", e, ok)
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
	cfg, _ := LoadConfig(testEnv(nil))
	s, err := New(cfg, f.reg, f.jobs, f.builder, f.server, nil,
		WithReconciler(rec),
		WithPendingReports(&fakePendingReports{}),
		WithRevoker(&fakeRevoker{}),
		WithProbeRegistry(registry.NewProbeRegistry()),
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

// Contract §4: without a durable queue a terminal report could only be
// logged and dropped, so a scheduler cannot be built without one.
func TestNewRequiresPendingReports(t *testing.T) {
	f := newFixture(t)
	cfg, _ := LoadConfig(testEnv(nil))
	_, err := New(cfg, f.reg, f.jobs, f.builder, f.server, nil,
		WithRevoker(&fakeRevoker{}),
		WithProbeRegistry(registry.NewProbeRegistry()),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if err == nil {
		t.Fatal("New without a pending-report queue: want an error")
	}
}

// ADR-017: the credential reclaim must be able to revoke the digest it
// recorded, so a scheduler cannot be built without a revoker — a no-op one
// would silently reopen the same-name reuse face.
func TestNewRequiresRevoker(t *testing.T) {
	f := newFixture(t)
	cfg, _ := LoadConfig(testEnv(nil))
	_, err := New(cfg, f.reg, f.jobs, f.builder, f.server, nil,
		WithPendingReports(&fakePendingReports{}),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if err == nil {
		t.Fatal("New without a revoker: want an error")
	}
}

// The catalog probe's identity, command delivery and result routing all read
// the probe session table, so a scheduler without one could never register a
// probe: it is a wiring error, not a mode.
func TestNewRequiresProbeRegistry(t *testing.T) {
	f := newFixture(t)
	cfg, _ := LoadConfig(testEnv(nil))
	_, err := New(cfg, f.reg, f.jobs, f.builder, f.server, nil,
		WithPendingReports(&fakePendingReports{}),
		WithRevoker(&fakeRevoker{}),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if err == nil {
		t.Fatal("New without a probe session table: want an error")
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
	if got := f.sched.taskMu.lenEntries(); got != 0 {
		t.Fatalf("taskMu entries = %d after all claims, want 0", got)
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
	p := f.withPendingReports()
	e := f.claim(t, "task-1")
	e.State = registry.StateRunning
	f.reg.Put(e)
	f.now = testNow.Add(2 * time.Minute)
	f.server.status = "running"
	f.server.forwardCode = 500

	if err := f.sched.OnJobGone(context.Background(), "fm-task-1"); err != nil {
		t.Fatalf("OnJobGone: %v", err)
	}
	// The report did not land: it is in the durable queue, and the terminal
	// entry keeps its objects until a drain delivers it.
	e, ok := f.reg.Get("task-1")
	if !ok || e.State != registry.StateTerminal || e.Result != registry.ResultFailed {
		t.Fatalf("entry = %+v, %v", e, ok)
	}
	if len(p.reports) != 1 || p.reports[0].taskID != "task-1" || p.reports[0].ep != EPFail {
		t.Fatalf("pending reports = %+v, want the compensated fail report", p.reports)
	}
	if len(f.jobs.deletedSecrets) != 1 { // only the claim-time stale sweep
		t.Fatalf("secret deleted before report landed: %+v", f.jobs.deletedSecrets)
	}
	if !f.hasJob("fm-task-1") {
		t.Fatal("Job deleted before the queued report landed")
	}

	// The drain landed the report: the next round cleans up. The scheduler
	// never forwards it a second time — the queue owns the redelivery.
	p.deliver("task-1")
	if err := f.sched.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e, ok := f.reg.Get("task-1"); !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Fatalf("entry = %+v, %v; want a retained draining entry", e, ok)
	}
	if n := len(f.server.forwards); n != 1 {
		t.Fatalf("forwards = %d, want 1 (initial attempt only)", n)
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

func (testIssuer) IssueProbe(string, string, string, time.Duration) (string, error) {
	return "fmj_test.probe", nil
}

func (testIssuer) Hash(token string) string { return "hash(" + token + ")" }

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
	cfg, err := LoadConfig(testEnv(map[string]string{EnvMaxJobsPerNode: strconv.Itoa(maxJobsPerNode)}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	builder, err := jobbuilder.NewBuilder(jbCfg)
	if err != nil {
		t.Fatalf("jobbuilder.NewBuilder: %v", err)
	}
	s, err := New(cfg, f.reg, f.jobs, builder, f.server, f.metrics,
		WithClock(func() time.Time { return f.now }),
		WithPendingReports(&fakePendingReports{}),
		WithRevoker(&fakeRevoker{}),
		WithProbeRegistry(registry.NewProbeRegistry()),
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
	// The claim resumes this session, learned on node-a.
	const priorSession = "/state/home/.multica/pi-sessions/sess-1.jsonl"
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
			f.reg.LearnSession(registry.SessionKey(priorSession), "node-a")
			// node-a already holds two live Jobs.
			place(t, f, "t1", "issue-a", "node-a")
			place(t, f, "t2", "issue-b", "node-a")
			sched := newCappedScheduler(t, f, tc.maxPerNode, io.Discard)

			if err := sched.OnClaim(context.Background(), resumeClaimPayload("task-x", priorSession)); err != nil {
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

// TestOnClaimWithoutIssueContext (AC-23): an autopilot-triggered payload has no
// issue context and must still reach the Job path — it used to fail because the
// invariant check demanded non-empty issue annotations.
func TestOnClaimWithoutIssueContext(t *testing.T) {
	f := newFixture(t)
	var buf bytes.Buffer
	sched := newCappedScheduler(t, f, 4, &buf)

	payload := json.RawMessage(`{
		"id": "t1",
		"agent_id": "agent-1",
		"workspace_id": "ws-1",
		"autopilot_run_id": "run-1",
		"runtime_id": "real-runtime",
		"auth_token": "mat_secret"
	}`)
	if err := sched.OnClaim(context.Background(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if len(f.jobs.createdJobs) != 1 {
		t.Fatalf("created Jobs = %d, want 1", len(f.jobs.createdJobs))
	}
	annos := f.jobs.createdJobs[0].Annotations
	if annos[registry.AnnoAutopilotRunID] != "run-1" {
		t.Errorf("autopilot-run-id = %q, want run-1", annos[registry.AnnoAutopilotRunID])
	}
	for _, key := range []string{registry.AnnoIssueID, registry.AnnoIssueIdentifier, registry.AnnoChatSessionID} {
		if value, present := annos[key]; present {
			t.Errorf("annotation %q = %q, want the key absent", key, value)
		}
	}
	e, ok := f.reg.Get("t1")
	if !ok || e.State != registry.StateJobCreated {
		t.Fatalf("entry = %+v, %v", e, ok)
	}
	if e.AutopilotRunID != "run-1" || e.IssueID != "" || e.ChatSessionID != "" {
		t.Fatalf("entry trigger context = %+v", e)
	}
	// The claim log carries the entry anchors, so an operator can find a task
	// that has no issue (observability.md task.claimed).
	record := findLogRecord(t, buf.Bytes(), "task.claimed")
	if record["autopilot_run_id"] != "run-1" || record["chat_session_id"] != "" || record["issue_identifier"] != "" {
		t.Errorf("task.claimed trigger context = %v", record)
	}
}
