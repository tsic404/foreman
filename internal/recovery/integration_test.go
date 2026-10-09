package recovery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// These tests wire the real scheduler and the real reconciler through their
// shared seams (the composition root's job): they catch integration bugs the
// unit tests of either module cannot, above all the ordering between the
// terminal write and the K8s deletion.

type lateBound struct{ r *Reconciler }

func (l *lateBound) Reconcile(ctx context.Context) error { return l.r.Reconcile(ctx) }

func schedConfig() scheduler.Config {
	return scheduler.Config{
		JobNamespace:    "multica-agents",
		MaxInflightJobs: 10,
		ClaimBatchMax:   5,
		JobBootTimeout:  time.Minute,
	}
}

func fakeJobName(taskID string) string { return "fm-" + taskID }

// setupIntegration wires the composition root: the real scheduler and the
// real reconciler over the real durable queue. dir empty means a fresh temp
// directory; tests that need to break the queue open it themselves.
func setupIntegration(t *testing.T, server *fakeServerClient, dir string, opts ...PendingReportsOption) (*registry.Registry, *fakeJobClient, *scheduler.Scheduler, *PendingReports, func(ObjectClient) *Reconciler) {
	t.Helper()
	return setupIntegrationWithMetrics(t, server, dir, nil, opts...)
}

// setupIntegrationWithMetrics is setupIntegration with the observability
// implementation wired into the scheduler, so a test can read the gauge off
// the exposition the scrape endpoint serves rather than off the seam.
func setupIntegrationWithMetrics(t *testing.T, server *fakeServerClient, dir string, metrics scheduler.Metrics, opts ...PendingReportsOption) (*registry.Registry, *fakeJobClient, *scheduler.Scheduler, *PendingReports, func(ObjectClient) *Reconciler) {
	t.Helper()
	if dir == "" {
		dir = t.TempDir()
	}
	pending, err := NewPendingReports(dir, opts...)
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	reg := registry.New(time.Now)
	jobs := newFakeJobClient()
	lb := &lateBound{}
	sched, err := scheduler.New(schedConfig(), reg, jobs, stubBuilder{}, server, metrics,
		scheduler.WithReconciler(lb),
		scheduler.WithPendingReports(pending))
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	return reg, jobs, sched, pending, func(objs ObjectClient) *Reconciler {
		rec, err := New(Config{ReconcileInterval: 30 * time.Second}, reg, objs, server, sched,
			WithMetrics(&fakeMetrics{}),
			WithPendingReports(pending, server))
		if err != nil {
			t.Fatalf("recovery.New: %v", err)
		}
		lb.r = rec
		return rec
	}
}

// Scenario #3: the Job failed and its daemon never reported — the task is
// failed on the daemon's behalf and the objects are removed.
func TestIntegrationFailedJobIsCompensated(t *testing.T) {
	server := newFakeServerClient(map[string]string{"t1": "running"})
	reg, jobs, sched, _, newReconciler := setupIntegration(t, server, "")
	ctx := context.Background()

	claim := json.RawMessage(`{"id":"t1","agent_id":"a1","issue_id":"i1","issue_identifier":"TSI-1","workspace_id":"w1"}`)
	if err := sched.OnClaim(ctx, claim); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	entry, ok := reg.Get("t1")
	if !ok {
		t.Fatal("the claim did not register the task")
	}
	if !jobs.has(fakeJobName("t1")) || !jobs.hasSecret(fakeJobName("t1")+"-cred") {
		t.Fatal("OnClaim did not create the Job and its Secret")
	}
	reg.MarkDaemonRegistered(entry.DaemonID, time.Now())
	if _, err := sched.Deliver(entry.DaemonID, entry.JobRuntimeID, 1); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	entry, _ = reg.Get("t1")
	if _, _, err := sched.OnReport(ctx, scheduler.EPStart, entry, nil); err != nil {
		t.Fatalf("OnReport(start): %v", err)
	}

	objs := &fakeObjects{
		job: failedJob(fakeJobName("t1"), "BackoffLimitExceeded"),
		pods: []corev1.Pod{podFor(fakeJobName("t1"), func(p *corev1.Pod) {
			p.Status.Phase = corev1.PodFailed
		})},
	}
	rec := newReconciler(objs)

	// First round only records the sighting: the daemon gets its window.
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := server.forwarded(scheduler.EPFail); len(got) != 0 {
		t.Fatalf("fail forwards = %+v, want none inside the daemon-report window", got)
	}

	// The daemon's report window has passed.
	rec.recheckWindow = 0
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	fails := server.forwarded(scheduler.EPFail)
	if len(fails) != 1 || fails[0].reason != scheduler.FailureReasonJobFailed {
		t.Fatalf("fail forwards = %+v, want one job_failed report", fails)
	}
	if !reg.IsDone("t1") {
		t.Fatal("the task must be settled terminal after the compensation")
	}
	if _, ok := reg.Get("t1"); ok {
		t.Fatal("the mapping must be dropped after the objects are removed")
	}
	if jobs.has(fakeJobName("t1")) || jobs.hasSecret(fakeJobName("t1")+"-cred") {
		t.Fatal("the Job and its Secret must be deleted after the report lands")
	}
}

// Scenario #10 on the compensation path (AC-16): a synthesized fail report
// whose forward does not land is queued durably (gauge +1), holds the
// objects back until the drain delivers it, and reaches the server exactly
// once.
func TestIntegrationCompensatedReportIsQueuedAndDrained(t *testing.T) {
	server := newFakeServerClient(map[string]string{"t1": "running"})
	metrics := observability.NewMetrics()
	reg, jobs, sched, pending, newReconciler := setupIntegration(t, server, "",
		WithPendingReportsGauge(metrics.PendingReports))
	ctx := context.Background()

	claim := json.RawMessage(`{"id":"t1","agent_id":"a1","issue_id":"i1","issue_identifier":"TSI-1","workspace_id":"w1"}`)
	if err := sched.OnClaim(ctx, claim); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}

	objs := &fakeObjects{
		job: failedJob(fakeJobName("t1"), "BackoffLimitExceeded"),
		pods: []corev1.Pod{podFor(fakeJobName("t1"), func(p *corev1.Pod) {
			p.Status.Phase = corev1.PodFailed
		})},
	}
	rec := newReconciler(objs)
	WithPendingReports(pending, server)(rec)

	// First round only records the sighting: the daemon gets its window.
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := server.forwards(); len(got) != 0 {
		t.Fatalf("forwards = %+v, want none inside the daemon-report window", got)
	}

	// The window has passed and the upstream is down: the report exhausts
	// its forward budget and lands in the durable queue.
	rec.recheckWindow = 0
	server.setForwardCode(503)
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := pending.Len(); got != 1 {
		t.Fatalf("queued reports = %d, want 1", got)
	}
	if got := scrapeGauge(t, metrics.Handler(), "foreman_pending_reports"); got != 1 {
		t.Fatalf("foreman_pending_reports = %v, want 1", got)
	}
	entry, ok := reg.Get("t1")
	if !ok || !entry.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want a retained terminal entry", entry, ok)
	}
	if !jobs.has(fakeJobName("t1")) || !jobs.hasSecret(fakeJobName("t1")+"-cred") {
		t.Fatal("the objects must stay while the report is queued")
	}

	// Still down one round later: the failed drain keeps the report queued
	// and the objects held back.
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := pending.Len(); got != 1 {
		t.Fatalf("queued reports after a failed drain = %d, want 1", got)
	}
	if !jobs.has(fakeJobName("t1")) {
		t.Fatal("the Job must survive a failed drain")
	}

	// The upstream recovers: one round drains the report and, with the queue
	// empty again, cleans the objects up — the server takes the terminal
	// report exactly once.
	server.setForwardCode(0)
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("queued reports after the drain = %d, want 0", got)
	}
	if got := scrapeGauge(t, metrics.Handler(), "foreman_pending_reports"); got != 0 {
		t.Fatalf("foreman_pending_reports = %v, want 0", got)
	}
	if got := server.accepted(scheduler.EPFail); got != 1 {
		t.Fatalf("accepted fail reports = %d, want exactly 1 terminal write", got)
	}
	if _, ok := reg.Get("t1"); ok {
		t.Fatal("the mapping must be dropped once the report landed")
	}
	if jobs.has(fakeJobName("t1")) || jobs.hasSecret(fakeJobName("t1")+"-cred") {
		t.Fatal("the Job and its Secret must be deleted once the report landed")
	}
}

// 错误处理表「待发队列目录不可写」on the compensation path: a report whose
// write fails is not lost — it stays staged, the gauge does not grow, the
// objects are kept, and one drain lands it (exactly once) once the queue
// directory is writable again.
func TestIntegrationStagedReportSurvivesWriteFailure(t *testing.T) {
	server := newFakeServerClient(map[string]string{"t1": "running"})
	queuePath := filepath.Join(t.TempDir(), "pending-reports")
	metrics := observability.NewMetrics()
	reg, jobs, sched, pending, newReconciler := setupIntegration(t, server, queuePath,
		WithPendingReportsGauge(metrics.PendingReports))
	ctx := context.Background()

	// Break the queue directory: every write now fails (ENOTDIR), exactly
	// like an unwritable state root.
	if err := os.RemoveAll(queuePath); err != nil {
		t.Fatalf("remove queue dir: %v", err)
	}
	if err := os.WriteFile(queuePath, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("break queue dir: %v", err)
	}

	claim := json.RawMessage(`{"id":"t1","agent_id":"a1","issue_id":"i1","issue_identifier":"TSI-1","workspace_id":"w1"}`)
	if err := sched.OnClaim(ctx, claim); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	objs := &fakeObjects{
		job: failedJob(fakeJobName("t1"), "BackoffLimitExceeded"),
		pods: []corev1.Pod{podFor(fakeJobName("t1"), func(p *corev1.Pod) {
			p.Status.Phase = corev1.PodFailed
		})},
	}
	rec := newReconciler(objs)

	// First round only records the sighting: the daemon gets its window.
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// The window has passed and the upstream is down: the report cannot be
	// written and must be staged instead of dropped.
	rec.recheckWindow = 0
	server.setForwardCode(503)
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("durable queue depth = %d, want 0 (the write failed)", got)
	}
	if got := scrapeGauge(t, metrics.Handler(), "foreman_pending_reports"); got != 0 {
		t.Fatalf("foreman_pending_reports = %v, want 0 (a failed write must not grow the gauge)", got)
	}
	if !pending.Queued("t1") {
		t.Fatal("a staged report must still count as undelivered")
	}
	entry, ok := reg.Get("t1")
	if !ok || !entry.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want a retained terminal entry", entry, ok)
	}
	if !jobs.has(fakeJobName("t1")) || !jobs.hasSecret(fakeJobName("t1")+"-cred") {
		t.Fatal("the objects must stay while the report is staged")
	}

	// A later round with the directory still broken keeps both the staged
	// report and the objects.
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !pending.Queued("t1") || !jobs.has(fakeJobName("t1")) {
		t.Fatal("a failing write must neither drop the report nor free the objects")
	}

	// The directory is writable again and the upstream recovers: one round
	// lands the staged write, delivers it, and cleans up.
	if err := os.Remove(queuePath); err != nil {
		t.Fatalf("remove queue blocker: %v", err)
	}
	if err := os.MkdirAll(queuePath, 0o700); err != nil {
		t.Fatalf("restore queue dir: %v", err)
	}
	server.setForwardCode(0)
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if pending.Queued("t1") {
		t.Fatal("the staged report must be gone once it landed")
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("durable queue depth = %d, want 0 after the drain", got)
	}
	if got := server.accepted(scheduler.EPFail); got != 1 {
		t.Fatalf("accepted fail reports = %d, want exactly 1 terminal write", got)
	}
	if _, ok := reg.Get("t1"); ok {
		t.Fatal("the mapping must be dropped once the report landed")
	}
	if jobs.has(fakeJobName("t1")) || jobs.hasSecret(fakeJobName("t1")+"-cred") {
		t.Fatal("the Job and its Secret must be deleted once the report landed")
	}
}

// Scenario #2: a rebuilt pre-start entry (payload lost with the restart) is
// released so the server re-dispatches the task — no report, no zombie.
func TestIntegrationRebuiltPreStartEntryIsReleased(t *testing.T) {
	server := newFakeServerClient(map[string]string{"t1": "dispatched"})
	reg, jobs, _, _, newReconciler := setupIntegration(t, server, "")
	ctx := context.Background()

	e := testEntry("t1")
	e.Payload = nil // rebuilt from K8s
	e.State = registry.StateJobCreated
	if err := reg.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := jobs.CreateJob(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: e.JobName}}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := jobs.CreateSecret(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: e.JobName + "-cred"}}); err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}

	rec := newReconciler(&fakeObjects{job: runningJob(e.JobName)})
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := server.forwards(); len(got) != 0 {
		t.Fatalf("forwards = %+v, want none for a task the server will re-dispatch", got)
	}
	if _, ok := reg.Get("t1"); ok {
		t.Fatal("the released entry must leave the live index")
	}
	if jobs.has(e.JobName) || jobs.hasSecret(e.JobName+"-cred") {
		t.Fatal("the Job and its Secret must be deleted to release the task")
	}
}

// Scenario #8: a server-side cancellation waits for the daemon's cancel-ack
// and then removes the objects without a fail report.
func TestIntegrationCancelWaitsForAckThenReleases(t *testing.T) {
	server := newFakeServerClient(map[string]string{"t1": "cancelled"})
	reg, jobs, sched, _, newReconciler := setupIntegration(t, server, "")
	ctx := context.Background()

	claim := json.RawMessage(`{"id":"t1","agent_id":"a1","issue_id":"i1","issue_identifier":"TSI-1","workspace_id":"w1"}`)
	if err := sched.OnClaim(ctx, claim); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	reg.MarkStarted("t1", time.Now())

	rec := newReconciler(&fakeObjects{job: runningJob("fm-t1")})
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := server.forwards(); len(got) != 0 {
		t.Fatalf("forwards = %+v, want none while the daemon may still ack", got)
	}
	if !jobs.has("fm-t1") {
		t.Fatal("the Job must be kept until the cancel-ack timeout")
	}

	// The cancel-ack timeout has passed.
	rec.cancelAckTimeout = 0
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := server.forwards(); len(got) != 0 {
		t.Fatalf("forwards = %+v, want no fail report for a cancelled task", got)
	}
	if _, ok := reg.Get("t1"); ok {
		t.Fatal("the cancelled task must leave the live index")
	}
	if jobs.has("fm-t1") || jobs.hasSecret("fm-t1-cred") {
		t.Fatal("the Job and its Secret must be removed after the ack timeout")
	}
}

// ---- fakes for the wired modules ----

type stubBuilder struct{}

func (stubBuilder) Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, error) {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: fakeJobName(e.TaskID)}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: fakeJobName(e.TaskID) + "-cred"}}, nil
}

type fakeJobClient struct {
	mu      sync.Mutex
	jobs    map[string]*batchv1.Job
	secrets map[string]*corev1.Secret
}

func newFakeJobClient() *fakeJobClient {
	return &fakeJobClient{jobs: map[string]*batchv1.Job{}, secrets: map[string]*corev1.Secret{}}
}

func (c *fakeJobClient) CreateJob(_ context.Context, job *batchv1.Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.jobs[job.GetName()] = job
	return nil
}

func (c *fakeJobClient) DeleteJob(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.jobs, name)
	return nil
}

func (c *fakeJobClient) GetJob(_ context.Context, name string) (*batchv1.Job, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.jobs[name], nil
}

func (c *fakeJobClient) CreateSecret(_ context.Context, secret *corev1.Secret) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.secrets[secret.GetName()] = secret
	return nil
}

func (c *fakeJobClient) DeleteSecret(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.secrets, name)
	return nil
}

func (c *fakeJobClient) ListJobs(context.Context) ([]batchv1.Job, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]batchv1.Job, 0, len(c.jobs))
	for _, job := range c.jobs {
		out = append(out, *job)
	}
	return out, nil
}

func (c *fakeJobClient) WatchJobs(ctx context.Context, _ scheduler.JobEventHandler) error {
	<-ctx.Done()
	return nil
}

func (c *fakeJobClient) has(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.jobs[name]
	return ok
}

func (c *fakeJobClient) hasSecret(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.secrets[name]
	return ok
}

type forwardedCall struct {
	ep     scheduler.Endpoint
	taskID string
	reason string
	code   int
}

type fakeServerClient struct {
	mu          sync.Mutex
	statuses    map[string]string
	calls       []forwardedCall
	forwardCode int // 0 → 200
}

func newFakeServerClient(statuses map[string]string) *fakeServerClient {
	if statuses == nil {
		statuses = map[string]string{}
	}
	return &fakeServerClient{statuses: statuses}
}

func (s *fakeServerClient) TaskStatus(_ context.Context, taskID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, ok := s.statuses[taskID]
	if !ok {
		return "", scheduler.ErrTaskNotFound
	}
	return status, nil
}

func (s *fakeServerClient) Forward(_ context.Context, ep scheduler.Endpoint, taskID string, body []byte) (int, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	call := forwardedCall{ep: ep, taskID: taskID}
	var payload struct {
		FailureReason string `json:"failure_reason"`
	}
	if err := json.Unmarshal(body, &payload); err != nil && len(body) > 0 {
		return 0, nil, err
	}
	call.reason = payload.FailureReason
	code := s.forwardCode
	if code == 0 {
		code = 200
	}
	call.code = code
	s.calls = append(s.calls, call)
	return code, []byte(`{}`), nil
}

// setForwardCode programs the answer of every following forward (0 → 200).
func (s *fakeServerClient) setForwardCode(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forwardCode = code
}

// accepted counts the forwards the server took as a terminal write (2xx).
func (s *fakeServerClient) accepted(ep scheduler.Endpoint) int {
	n := 0
	for _, call := range s.forwards() {
		if call.ep == ep && call.code >= 200 && call.code < 300 {
			n++
		}
	}
	return n
}

// scrapeGauge reads one gauge from the /metrics exposition.
func scrapeGauge(t *testing.T, h http.Handler, name string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		value, ok := strings.CutPrefix(line, name+" ")
		if !ok {
			continue
		}
		got, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			t.Fatalf("%s exposition %q: %v", name, line, err)
		}
		return got
	}
	t.Fatalf("metric %s missing from the exposition", name)
	return 0
}

func (s *fakeServerClient) forwards() []forwardedCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]forwardedCall(nil), s.calls...)
}

func (s *fakeServerClient) forwarded(ep scheduler.Endpoint) []forwardedCall {
	var out []forwardedCall
	for _, call := range s.forwards() {
		if call.ep == ep {
			out = append(out, call)
		}
	}
	return out
}
