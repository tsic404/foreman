package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// probeFixture builds a scheduler with an explicit probe session table and a
// log buffer, so the probe lifecycle can be observed end to end.
type probeFixture struct {
	reg     *registry.Registry
	probes  *registry.ProbeRegistry
	jobs    *fakeJobs
	builder *fakeBuilder
	server  *fakeServer
	metrics *fakeMetrics
	revoker *fakeRevoker
	sched   *Scheduler
	logs    *bytes.Buffer
	cfg     Config
}

func newProbeFixture(t *testing.T, cfg Config) *probeFixture {
	t.Helper()
	return newProbeFixtureWithClock(t, cfg, time.Now)
}

// newProbeFixtureWithClock is newProbeFixture with an injectable clock, so the
// startup window's boundary can be driven deterministically.
func newProbeFixtureWithClock(t *testing.T, cfg Config, now func() time.Time) *probeFixture {
	t.Helper()
	logs := &bytes.Buffer{}
	p := &probeFixture{
		reg:     registry.New(now),
		probes:  registry.NewProbeRegistry(),
		jobs:    &fakeJobs{},
		builder: &fakeBuilder{},
		server:  &fakeServer{},
		metrics: &fakeMetrics{},
		revoker: &fakeRevoker{},
		logs:    logs,
		cfg:     cfg,
	}
	sched, err := New(cfg, p.reg, p.jobs, p.builder, p.server, p.metrics,
		WithClock(now),
		WithPendingReports(&fakePendingReports{}),
		WithRevoker(p.revoker),
		WithProbeRegistry(p.probes),
		WithLogger(slog.New(slog.NewJSONHandler(logs, nil))),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.sched = sched
	return p
}

func probeConfig(t *testing.T) Config {
	t.Helper()
	cfg := Config{
		JobNamespace:        "test-ns",
		WorkspaceID:         "ws-test",
		MaxInflightJobs:     10,
		ClaimBatchMax:       32,
		JobBootTimeout:      300 * time.Second,
		JobDrainTimeout:     120 * time.Second,
		StartupProbeTimeout: 3 * time.Second,
		ModelCatalogEnabled: true,
		ModelCatalogTimeout: 45 * time.Second,
		ReconcileInterval:   30 * time.Second,
	}
	return cfg
}

// events returns the parsed log lines carrying one msg value.
func (p *probeFixture) events(msg string) []map[string]any {
	var found []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(p.logs.String()), "\n") {
		if line == "" {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			continue
		}
		if fields["msg"] == msg {
			found = append(found, fields)
		}
	}
	return found
}

func TestStartProbeSessionBeforeObjects(t *testing.T) {
	p := newProbeFixture(t, probeConfig(t))
	var order []string
	p.jobs.onSecretCreate = func() {
		if _, ok := p.probes.ProbeByJob("fm-catalog-req-1"); !ok {
			t.Error("session must exist before the Secret is created")
		}
		order = append(order, "secret")
	}
	p.jobs.onCreateJob = func() {
		order = append(order, "job")
	}

	handle, err := p.sched.StartProbe(t.Context(), "req-1", "rid-real", registry.ProbeCommandDriven, 30*time.Second)
	if err != nil {
		t.Fatalf("StartProbe: %v", err)
	}
	if handle.JobName != "fm-catalog-req-1" || handle.JobRuntime == "" {
		t.Fatalf("handle = %+v", handle)
	}
	if len(order) != 2 || order[0] != "secret" || order[1] != "job" {
		t.Fatalf("creation order = %v, want Secret before Job", order)
	}
	session, ok := p.probes.ProbeByJob(handle.JobName)
	if !ok {
		t.Fatal("probe session missing")
	}
	if session.RequestID != "req-1" || session.SelfMintedID != "" || session.RealRuntimeID != "rid-real" {
		t.Errorf("session = %+v", session)
	}
	if session.JobRuntime != handle.JobRuntime || session.TokenHash == "" {
		t.Errorf("session identity fields = %+v", session)
	}
	if p.builder.probeWorkspace != "ws-test" || p.builder.probeBudget != 30*time.Second {
		t.Errorf("builder got workspace %q budget %s", p.builder.probeWorkspace, p.builder.probeBudget)
	}
}

func TestStartProbeStartupModeMintsItsOwnCommand(t *testing.T) {
	p := newProbeFixture(t, probeConfig(t))
	handle, err := p.sched.StartProbe(t.Context(), "startup-abc", "", registry.ProbeStartupDiscovery, 30*time.Second)
	if err != nil {
		t.Fatalf("StartProbe: %v", err)
	}
	session, _ := p.probes.ProbeByJob(handle.JobName)
	if session.SelfMintedID != "startup-abc" || session.RequestID != "" || session.RealRuntimeID != "" {
		t.Fatalf("startup session = %+v", session)
	}
}

func TestStartProbeRollsBackOnJobFailure(t *testing.T) {
	p := newProbeFixture(t, probeConfig(t))
	p.jobs.createJobErr = errors.New("apiserver down")

	_, err := p.sched.StartProbe(t.Context(), "req-2", "rid-real", registry.ProbeCommandDriven, 30*time.Second)
	if err == nil {
		t.Fatal("StartProbe must fail when the Job cannot be created")
	}
	if _, ok := p.probes.ProbeByJob("fm-catalog-req-2"); ok {
		t.Error("rollback must remove the session (the credential's validity face)")
	}
	if !p.jobs.deleted("fm-catalog-req-2") {
		t.Error("rollback must delete the Job")
	}
	if !p.jobs.secretDeleted("fm-catalog-req-2-cred") {
		t.Error("rollback must delete the Secret")
	}
}

func TestAwaitProbeRelayedReadsSessionResult(t *testing.T) {
	p := newProbeFixture(t, probeConfig(t))
	handle, err := p.sched.StartProbe(t.Context(), "req-3", "rid-real", registry.ProbeCommandDriven, 30*time.Second)
	if err != nil {
		t.Fatalf("StartProbe: %v", err)
	}
	done := make(chan struct {
		node    string
		relayed bool
		code    int
		err     error
	}, 1)
	go func() {
		node, relayed, code, err := p.sched.AwaitProbeRelayed(t.Context(), handle, time.Now().Add(2*time.Second))
		done <- struct {
			node    string
			relayed bool
			code    int
			err     error
		}{node, relayed, code, err}
	}()

	p.probes.UpdateProbe(handle.JobName, func(session *registry.ProbeSession) {
		session.NodeName = "node-a"
		session.RelayedAt = time.Now()
		session.RelayedCode = 200
	})
	select {
	case got := <-done:
		if got.err != nil || !got.relayed || got.code != 200 || got.node != "node-a" {
			t.Fatalf("AwaitProbeRelayed = %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AwaitProbeRelayed did not observe the result")
	}
}

func TestAwaitProbeRelayedStopsAtDeadline(t *testing.T) {
	p := newProbeFixture(t, probeConfig(t))
	handle, err := p.sched.StartProbe(t.Context(), "req-4", "rid-real", registry.ProbeCommandDriven, 30*time.Second)
	if err != nil {
		t.Fatalf("StartProbe: %v", err)
	}
	node, relayed, code, err := p.sched.AwaitProbeRelayed(t.Context(), handle, time.Now().Add(50*time.Millisecond))
	if err != nil || relayed || code != 0 || node != "" {
		t.Fatalf("expired wait = %q %v %d %v", node, relayed, code, err)
	}
}

func TestDeleteProbeRemovesSessionFirst(t *testing.T) {
	p := newProbeFixture(t, probeConfig(t))
	handle, err := p.sched.StartProbe(t.Context(), "req-5", "rid-real", registry.ProbeCommandDriven, 30*time.Second)
	if err != nil {
		t.Fatalf("StartProbe: %v", err)
	}
	p.jobs.onDeleteSecret = func() {
		if _, ok := p.probes.ProbeByJob(handle.JobName); ok {
			t.Error("the session is the credential's validity face: it must go first")
		}
	}
	if err := p.sched.DeleteProbe(t.Context(), handle); err != nil {
		t.Fatalf("DeleteProbe: %v", err)
	}
	if _, ok := p.probes.ProbeByJob(handle.JobName); ok {
		t.Error("session must be removed")
	}
	if !p.jobs.secretDeleted(handle.JobName+"-cred") || !p.jobs.deleted(handle.JobName) {
		t.Error("Secret and Job must both be deleted")
	}
	// Idempotent: a second teardown is a no-op, not an error.
	if err := p.sched.DeleteProbe(t.Context(), handle); err != nil {
		t.Fatalf("second DeleteProbe: %v", err)
	}
}

func TestDeleteProbeAttemptsEveryStepOnFailure(t *testing.T) {
	p := newProbeFixture(t, probeConfig(t))
	handle, err := p.sched.StartProbe(t.Context(), "req-6", "rid-real", registry.ProbeCommandDriven, 30*time.Second)
	if err != nil {
		t.Fatalf("StartProbe: %v", err)
	}
	p.jobs.deleteSecretErr = errors.New("apiserver down")
	if err := p.sched.DeleteProbe(t.Context(), handle); err == nil {
		t.Fatal("a failed step must surface as an error")
	}
	if !p.jobs.deleted(handle.JobName) {
		t.Error("a failed Secret deletion must not short-circuit the Job deletion")
	}
}

func TestSweepStaleProbesTwoFaces(t *testing.T) {
	p := newProbeFixture(t, probeConfig(t))
	old := metav1.NewTime(time.Now().Add(-2 * p.sched.staleProbeGrace()))
	fresh := metav1.NewTime(time.Now())
	probeJob := func(name string, created metav1.Time) batchv1.Job {
		return batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name: name, CreationTimestamp: created,
			Labels: map[string]string{registry.LabelRole: registry.RoleCatalogProbe},
		}}
	}
	taskJob := probeJob("fm-task-1", old)
	taskJob.Labels[registry.LabelTaskID] = "task-1"
	probeOld, probeFresh := probeJob("fm-catalog-old", old), probeJob("fm-catalog-fresh", fresh)
	p.jobs.jobs = []batchv1.Job{probeOld, probeFresh, taskJob}
	p.jobs.existingJobs = map[string]*batchv1.Job{
		probeOld.Name: &probeOld, probeFresh.Name: &probeFresh,
	}
	probeSecret := func(name string, created metav1.Time) corev1.Secret {
		return corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: name, CreationTimestamp: created,
			Labels: map[string]string{registry.LabelRole: registry.RoleCatalogProbe},
		}}
	}
	taskSecret := probeSecret("fm-task-1-cred", old)
	taskSecret.Labels[registry.LabelTaskID] = "task-1"
	p.jobs.secrets = []corev1.Secret{
		probeSecret("fm-catalog-old-cred", old),    // same-name Job exists → skipped here, removed with the Job face
		probeSecret("fm-catalog-fresh-cred", old),  // Job exists (fresh) → kept
		probeSecret("fm-catalog-orphan-cred", old), // no Job, stale → deleted
		probeSecret("fm-catalog-orphan-fresh-cred", fresh),
		taskSecret,
		probeSecret("fm-catalog-nosuffix", old),
	}
	if err := p.sched.SweepStaleProbes(t.Context()); err != nil {
		t.Fatalf("SweepStaleProbes: %v", err)
	}
	if !p.jobs.deleted("fm-catalog-old") {
		t.Error("a stale probe Job must be swept")
	}
	if !p.jobs.secretDeleted("fm-catalog-old-cred") {
		t.Error("the stale probe Job's Secret must be deleted with it")
	}
	if p.jobs.deleted("fm-catalog-fresh") || p.jobs.secretDeleted("fm-catalog-fresh-cred") {
		t.Errorf("fresh probe objects must survive the grace window: jobs=%v secrets=%v",
			p.jobs.deletedJobs, p.jobs.deletedSecrets)
	}
	if p.jobs.deleted("fm-task-1") || p.jobs.secretDeleted("fm-task-1-cred") {
		t.Error("objects carrying a task-id belong to the task face")
	}
	if !p.jobs.secretDeleted("fm-catalog-orphan-cred") {
		t.Error("an orphan probe Secret past the grace window must be swept")
	}
	if p.jobs.secretDeleted("fm-catalog-orphan-fresh-cred") {
		t.Error("an orphan probe Secret inside the grace window is the Secret-before-Job window")
	}
	if p.jobs.secretDeleted("fm-catalog-nosuffix") {
		t.Error("a Secret without the -cred suffix cannot name a Job and must be skipped")
	}
}

// registerProbe simulates the probe daemon's S4: it fills the session's startup
// face exactly like the fake server's probe branch does.
func (p *probeFixture) registerProbe(t *testing.T, jobName string) {
	t.Helper()
	p.probes.UpdateProbe(jobName, func(session *registry.ProbeSession) {
		session.RegisteredAt = time.Now()
		session.CLIVersion = "9.9.9"
		session.Runtimes = []registry.DiscoveredRuntime{{Name: "n-omp", Type: "omp", Version: "9.9.9"}}
		session.FailedProfiles = []json.RawMessage{json.RawMessage(`{"profile_id":"p1"}`)}
	})
}

func TestRunStartupProbeCollectsRegistration(t *testing.T) {
	cfg := probeConfig(t)
	cfg.StartupProbeTimeout = 2 * time.Second
	p := newProbeFixture(t, cfg)
	p.jobs.onCreateJob = func() {
		name := "fm-catalog-" + p.builder.probeRequestID
		p.registerProbe(t, name)
		p.probes.UpdateProbe(name, func(session *registry.ProbeSession) {
			session.RelayedAt = time.Now()
			session.RelayedCode = 200
		})
	}

	discovery, err := p.sched.RunStartupProbe(t.Context())
	if err != nil {
		t.Fatalf("RunStartupProbe: %v", err)
	}
	if discovery.CLIVersion != "9.9.9" || len(discovery.Runtimes) != 1 || discovery.Runtimes[0].Type != "omp" {
		t.Fatalf("discovery = %+v", discovery)
	}
	if len(p.probes.ListProbes()) != 0 {
		t.Error("the startup probe must be torn down before C1")
	}
	if len(p.jobs.deletedJobs) != 1 || !strings.HasPrefix(p.jobs.deletedJobs[0], "fm-catalog-startup-") {
		t.Errorf("probe Job not deleted: %v", p.jobs.deletedJobs)
	}
	if !p.jobs.secretDeleted(p.jobs.deletedJobs[0] + "-cred") {
		t.Errorf("probe Secret not deleted: %v", p.jobs.deletedSecrets)
	}
}

func TestRunStartupProbeUnregisteredIsFatal(t *testing.T) {
	cfg := probeConfig(t)
	cfg.StartupProbeTimeout = 700 * time.Millisecond
	p := newProbeFixture(t, cfg)

	_, err := p.sched.RunStartupProbe(t.Context())
	if !errors.Is(err, ErrStartupProbeUnregistered) {
		t.Fatalf("err = %v, want ErrStartupProbeUnregistered (exit non-zero)", err)
	}
	events := p.events("startup.probe_failed")
	if len(events) != 1 {
		t.Fatalf("startup.probe_failed events = %d, want 1", len(events))
	}
	if events[0]["reason"] != ProbeFailurePodNotStarted {
		t.Errorf("reason = %v, want the pod-not-started class (no pod ever appeared)", events[0]["reason"])
	}
	if len(p.probes.ListProbes()) != 0 {
		t.Error("a failed startup probe must still be cleaned up")
	}
}

func TestRunStartupProbeDaemonNotRegisteredClass(t *testing.T) {
	cfg := probeConfig(t)
	cfg.StartupProbeTimeout = 700 * time.Millisecond
	p := newProbeFixture(t, cfg)
	p.jobs.onCreateJob = func() {
		// The pod is up but its daemon never registers.
		p.jobs.pods = []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{
			Name:   "probe-pod",
			Labels: map[string]string{"job-name": "fm-catalog-" + p.builder.probeRequestID},
		}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}}
	}

	if _, err := p.sched.RunStartupProbe(t.Context()); !errors.Is(err, ErrStartupProbeUnregistered) {
		t.Fatalf("err = %v, want the fatal verdict", err)
	}
	events := p.events("startup.probe_failed")
	if len(events) != 1 || events[0]["reason"] != ProbeFailureDaemonNotRegistered {
		t.Fatalf("events = %+v, want one daemon_not_registered failure", events)
	}
	if events[0]["pod_status"] != "Running" {
		t.Errorf("pod_status = %v, want the observed phase", events[0]["pod_status"])
	}
}

func TestRunStartupProbeDiscoveryNotLandedIsNotFatal(t *testing.T) {
	cfg := probeConfig(t)
	cfg.StartupProbeTimeout = 700 * time.Millisecond
	p := newProbeFixture(t, cfg)
	p.jobs.onCreateJob = func() {
		p.registerProbe(t, "fm-catalog-"+p.builder.probeRequestID)
	}

	discovery, err := p.sched.RunStartupProbe(t.Context())
	if err != nil {
		t.Fatalf("a landing timeout must not stop startup: %v", err)
	}
	if len(discovery.Runtimes) != 1 {
		t.Fatalf("discovery = %+v (C1 still declares the registration)", discovery)
	}
	events := p.events("startup.probe_failed")
	if len(events) != 1 || events[0]["reason"] != ProbeFailureDiscoveryNotLanded {
		t.Fatalf("events = %+v, want the fourth class and no exit", events)
	}
}

func TestRunStartupProbeCatalogDisabledCompletesAtRegistration(t *testing.T) {
	cfg := probeConfig(t)
	cfg.ModelCatalogEnabled = false
	cfg.StartupProbeTimeout = 5 * time.Second
	p := newProbeFixture(t, cfg)
	p.jobs.onCreateJob = func() {
		p.registerProbe(t, "fm-catalog-"+p.builder.probeRequestID)
	}

	start := time.Now()
	discovery, err := p.sched.RunStartupProbe(t.Context())
	if err != nil {
		t.Fatalf("RunStartupProbe: %v", err)
	}
	if len(discovery.Runtimes) != 1 {
		t.Fatalf("discovery = %+v", discovery)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("orchestration waited %s for a discovery the disabled route never needs", elapsed)
	}
	if events := p.events("startup.probe_failed"); len(events) != 0 {
		t.Fatalf("a disabled catalog route is a supported degradation, not a failure: %+v", events)
	}
}

// TestRunStartupProbePodRanThenExitedClass covers the third failure class: a
// pod that started and then exited (the template-credential failure shape) is
// "daemon never registered", not "pod never started".
func TestRunStartupProbePodRanThenExitedClass(t *testing.T) {
	cfg := probeConfig(t)
	cfg.StartupProbeTimeout = 600 * time.Millisecond
	p := newProbeFixture(t, cfg)
	p.jobs.onCreateJob = func() {
		p.jobs.pods = []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{
			Name:   "probe-pod",
			Labels: map[string]string{"job-name": "fm-catalog-" + p.builder.probeRequestID},
		}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}}
	}

	_, err := p.sched.RunStartupProbe(t.Context())
	if !errors.Is(err, ErrStartupProbeUnregistered) {
		t.Fatalf("err = %v, want the fatal verdict", err)
	}
	events := p.events("startup.probe_failed")
	if len(events) != 1 || events[0]["reason"] != ProbeFailureDaemonNotRegistered {
		t.Fatalf("events = %+v, want the daemon-not-registered class for a pod that ran and exited", events)
	}
}

// TestRunStartupProbeExitCarriesReason pins §9.1's requirement that
// the exit record reports the same reason class the event does.
func TestRunStartupProbeExitCarriesReason(t *testing.T) {
	cfg := probeConfig(t)
	cfg.StartupProbeTimeout = 500 * time.Millisecond
	p := newProbeFixture(t, cfg)

	_, err := p.sched.RunStartupProbe(t.Context())
	var failure *StartupProbeFailure
	if !errors.As(err, &failure) {
		t.Fatalf("err = %v, want a *StartupProbeFailure", err)
	}
	if failure.Reason != ProbeFailurePodNotStarted {
		t.Errorf("reason = %q, want the classification the event carries", failure.Reason)
	}
	if failure.JobName == "" {
		t.Error("the failure must name the probe Job")
	}
	events := p.events("startup.probe_failed")
	if len(events) != 1 || events[0]["reason"] != failure.Reason {
		t.Errorf("event reason %v != exit reason %q", events[0]["reason"], failure.Reason)
	}
}

// TestRunStartupProbeCountsABoundaryRegistration pins the window horizon: a
// registration landing in the window's final poll interval still counts as
// "a runtime registered within the window".
func TestRunStartupProbeCountsABoundaryRegistration(t *testing.T) {
	cfg := probeConfig(t)
	cfg.StartupProbeTimeout = time.Second
	// The landing face is covered elsewhere; this case isolates the window's
	// registration horizon.
	cfg.ModelCatalogEnabled = false
	var clock atomic.Int64
	start := time.Now()
	clock.Store(start.UnixNano())
	p := newProbeFixtureWithClock(t, cfg, func() time.Time { return time.Unix(0, clock.Load()) })

	p.jobs.onCreateJob = func() {
		// The registration lands after the loop's last in-window read and
		// before the deadline: the final read must still see it.
		name := "fm-catalog-" + p.builder.probeRequestID
		go func() {
			time.Sleep(80 * time.Millisecond)
			p.registerProbe(t, name)
			clock.Store(time.Now().Add(2 * time.Second).UnixNano())
		}()
	}

	discovery, err := p.sched.RunStartupProbe(t.Context())
	if err != nil {
		t.Fatalf("a boundary registration must count as within the window: %v", err)
	}
	if len(discovery.Runtimes) != 1 {
		t.Fatalf("discovery = %+v", discovery)
	}
	if events := p.events("startup.probe_failed"); len(events) != 0 {
		t.Fatalf("no failure event expected: %+v", events)
	}
}

// TestProbeNodeTrackerStopsWithTheProcess pins the shutdown face: the tracker
// is detached from the caller (the HTTP request dies before the probe lands)
// but not from the process. Cancelling Run's context must stop the 1s ListPods
// poll instead of letting it run to the probe budget.
func TestProbeNodeTrackerStopsWithTheProcess(t *testing.T) {
	p := newProbeFixture(t, probeConfig(t))
	ctx, cancel := context.WithCancel(t.Context())
	runDone := make(chan struct{})
	go func() {
		_ = p.sched.Run(ctx)
		close(runDone)
	}()
	waitUntil(t, "Run to install its lifecycle", func() bool {
		p.sched.lifecycleMu.Lock()
		defer p.sched.lifecycleMu.Unlock()
		return p.sched.lifecycle != nil
	})

	if _, err := p.sched.StartProbe(t.Context(), "startup-track", "", registry.ProbeStartupDiscovery, 30*time.Second); err != nil {
		t.Fatalf("StartProbe: %v", err)
	}
	waitUntil(t, "the tracker's first poll", func() bool { return p.jobs.listPodsCalls.Load() > 0 })

	cancel()
	<-runDone
	settled := p.jobs.listPodsCalls.Load()
	time.Sleep(3 * probePodPollInterval)
	if got := p.jobs.listPodsCalls.Load(); got != settled {
		t.Fatalf("ListPods calls grew after shutdown: %d -> %d", settled, got)
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
