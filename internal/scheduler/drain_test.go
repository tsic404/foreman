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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// newLoggingScheduler builds a scheduler over f's seams with a log buffer the
// test can read (the fixture discards logs).
func newLoggingScheduler(t *testing.T, f *fixture, logw io.Writer) *Scheduler {
	t.Helper()
	cfg, err := LoadConfig(testEnv(nil))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	s, err := New(cfg, f.reg, f.jobs, f.builder, f.server, f.metrics,
		WithClock(func() time.Time { return f.now }),
		WithPendingReports(&fakePendingReports{}),
		WithRevoker(f.revoker),
		WithProbeRegistry(registry.NewProbeRegistry()),
		WithLogger(slog.New(slog.NewJSONHandler(logw, nil))),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// job_created_at is the apiserver's creationTimestamp of the accepted Job
// object, not a local clock reading: it is the instance-filter basis that
// separates this Job's pods from a previous instance's (§4.1).
func TestOnClaimTakesJobCreatedAtFromTheApiserver(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	if !e.JobCreatedAt.Equal(createJobCreationTimestamp) {
		t.Fatalf("job_created_at = %s, want the apiserver's %s", e.JobCreatedAt, createJobCreationTimestamp)
	}
	if e.JobCreatedAt.Equal(e.ClaimedAt) {
		t.Fatal("job_created_at must not be the Foreman claim time")
	}
}

// The ownership + instance filter is the single evidence surface: a pod from
// a reused name's predecessor (created before this Job instance) must not
// count, or the termination verdict of the new Job would be decided by the
// old one's pod (ADR-017).
func TestJobPodsFiltersPredecessorInstances(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	since := e.JobCreatedAt

	stale := testPod("fm-task-1", corev1.PodRunning)
	stale.Name = "fm-task-1-stale"
	stale.CreationTimestamp = metav1.Time{Time: since.Add(-time.Minute)}
	fresh := testPod("fm-task-1", corev1.PodSucceeded)
	f.jobs.pods = []corev1.Pod{stale, fresh}

	pods, err := f.sched.jobPods(context.Background(), "fm-task-1", since)
	if err != nil {
		t.Fatalf("jobPods: %v", err)
	}
	if len(pods) != 1 || pods[0].Name != fresh.Name {
		t.Fatalf("jobPods = %+v, want only the current instance's pod", pods)
	}
	// The stale pod is Running; without the filter it would keep the Job
	// alive forever.
	terminated, err := f.sched.JobTerminated(context.Background(), "fm-task-1", since)
	if err != nil || !terminated {
		t.Fatalf("JobTerminated = %v, %v; want true for the current instance", terminated, err)
	}
	alive, err := f.sched.AlivePods(context.Background(), "fm-task-1", since)
	if err != nil || alive != 0 {
		t.Fatalf("AlivePods = %d, %v; want 0", alive, err)
	}
}

// A pod list that fails is not a verdict: the caller must see the error and
// keep waiting instead of reading the empty set as "terminated".
func TestJobTerminationEvidenceSurfacesListErrors(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.jobs.listPodsErr = errors.New("apiserver unavailable")

	if _, err := f.sched.JobTerminated(context.Background(), "fm-task-1", f.now); err == nil {
		t.Fatal("JobTerminated: want the list error surfaced")
	}
	if _, err := f.sched.LastPodTerminatedAt(context.Background(), "fm-task-1", f.now); err == nil {
		t.Fatal("LastPodTerminatedAt: want the list error surfaced")
	}
	if _, err := f.sched.AlivePods(context.Background(), "fm-task-1", f.now); err == nil {
		t.Fatal("AlivePods: want the list error surfaced")
	}
}

// The termination fact comes from the kubelet's finishedAt; with no such fact
// (the pod objects are gone) it is zero, and the reclaim records the
// observation moment in the observed clock domain instead (§10).
func TestLastPodTerminatedAtReadsKubeletFact(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	since := e.JobCreatedAt
	finished := f.now.Add(-30 * time.Second)

	pod := testPod("fm-task-1", corev1.PodSucceeded)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "agent",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.Time{Time: finished}}},
	}}
	f.jobs.pods = []corev1.Pod{pod}

	at, err := f.sched.LastPodTerminatedAt(context.Background(), "fm-task-1", since)
	if err != nil {
		t.Fatalf("LastPodTerminatedAt: %v", err)
	}
	if !at.Equal(finished) {
		t.Fatalf("pod terminated at = %s, want %s", at, finished)
	}

	// The pod object is gone: no k8s fact exists.
	f.jobs.pods = nil
	at, err = f.sched.LastPodTerminatedAt(context.Background(), "fm-task-1", since)
	if err != nil {
		t.Fatalf("LastPodTerminatedAt without pods: %v", err)
	}
	if !at.IsZero() {
		t.Fatalf("pod terminated at = %s, want the zero value (no k8s fact)", at)
	}
}

// The cred.reclaimed evidence carries the clock domain with the value: a
// kubelet fact is the node clock, a degraded fact the observation moment, and
// the drain upper bound claims no fact at all (§10, ADR-017).
func TestCredReclaimedEvidenceCarriesClockDomain(t *testing.T) {
	f := newFixture(t)
	var logs bytes.Buffer
	s := newLoggingScheduler(t, f, &logs)
	f.claim(t, "task-1")
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	finished := f.now.Add(-20 * time.Second)

	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonJobTerminated, finished, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	event := decodeLogEvent(t, &logs, "cred.reclaimed")
	if event["pod_terminated_at_source"] != PodTerminatedAtSourceK8s {
		t.Fatalf("source = %v, want %q", event["pod_terminated_at_source"], PodTerminatedAtSourceK8s)
	}
	if event["pod_terminated_at"] != finished.UTC().Format(time.RFC3339) {
		t.Fatalf("pod_terminated_at = %v, want %s", event["pod_terminated_at"], finished.UTC().Format(time.RFC3339))
	}
	if event["reason"] != JobTerminationReasonJobTerminated {
		t.Fatalf("reason = %v, want %q", event["reason"], JobTerminationReasonJobTerminated)
	}

	// No kubelet fact: the value degrades to the observation moment and says
	// so.
	f2 := newFixture(t)
	var logs2 bytes.Buffer
	s2 := newLoggingScheduler(t, f2, &logs2)
	f2.claim(t, "task-2")
	if _, _, err := f2.reg.MarkTerminal("task-2", registry.ResultCompleted, "", f2.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if err := s2.OnJobTerminated(context.Background(), "fm-task-2",
		JobTerminationReasonJobTerminated, time.Time{}, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	event = decodeLogEvent(t, &logs2, "cred.reclaimed")
	if event["pod_terminated_at_source"] != PodTerminatedAtSourceObserved {
		t.Fatalf("source = %v, want %q", event["pod_terminated_at_source"], PodTerminatedAtSourceObserved)
	}
	if event["pod_terminated_at"] == "" {
		t.Fatal("an observed fact must carry the observation moment")
	}

	// The upper bound closed the window: no termination fact is claimed.
	f3 := newFixture(t)
	var logs3 bytes.Buffer
	s3 := newLoggingScheduler(t, f3, &logs3)
	f3.claim(t, "task-3")
	if _, _, err := f3.reg.MarkTerminal("task-3", registry.ResultFailed, "", f3.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if err := s3.OnJobTerminated(context.Background(), "fm-task-3",
		JobTerminationReasonDrainTimeout, time.Time{}, 2); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	event = decodeLogEvent(t, &logs3, "cred.reclaimed")
	if event["pod_terminated_at"] != "" {
		t.Fatalf("pod_terminated_at = %v, want the empty value on the drain_timeout path", event["pod_terminated_at"])
	}
	if event["orphan_pods"] != float64(2) {
		t.Fatalf("orphan_pods = %v, want 2", event["orphan_pods"])
	}
}

// decodeLogEvent returns the first JSON log line with the given msg.
func decodeLogEvent(t *testing.T, logs *bytes.Buffer, msg string) map[string]any {
	t.Helper()
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("decode log line %s: %v", line, err)
		}
		if event["msg"] == msg {
			return event
		}
	}
	t.Fatalf("no %s event in %s", msg, logs.String())
	return nil
}

// The reclaim's Job-deletion catch-up carries the §10 gate: while a
// terminal report is still queued the Job must not be deleted — a deleted Job
// must never stand in for a delivered report — so it is left to its
// ttlSecondsAfterFinished (§8.1). The credential is reclaimed either way: its
// bound does not wait for the report.
func TestReclaimGatesTheJobDeletionOnTheQueuedReport(t *testing.T) {
	f := newFixture(t)
	p := f.withPendingReports()
	f.claim(t, "task-1")
	f.jobs.deletedSecrets = nil
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultFailed, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPFail, []byte(`{"failure_reason":"job_missing"}`), ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := f.sched.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonDrainTimeout, time.Time{}, 1); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatalf("deletedJobs = %v, want the Job left to ttlSecondsAfterFinished while the report is queued", f.jobs.deletedJobs)
	}
	if f.hasJob("fm-task-1") != true {
		t.Fatal("the Job object must survive the reclaim while its report is queued")
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("the credential reclaim must not wait for the report")
	}
	if len(f.revoker.revoked) != 1 || len(f.jobs.deletedSecrets) != 1 {
		t.Fatalf("revoked=%v secrets=%v, want the credential reclaimed", f.revoker.revoked, f.jobs.deletedSecrets)
	}

	// The report landed: the next reclaim catch-up deletes the Job (idempotent
	// for the ordinary path, where the verdict already did).
	f2 := newFixture(t)
	f2.claim(t, "task-2")
	if _, _, err := f2.reg.MarkTerminal("task-2", registry.ResultCompleted, "", f2.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if err := f2.sched.OnJobTerminated(context.Background(), "fm-task-2",
		JobTerminationReasonJobTerminated, f2.now, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if len(f2.jobs.deletedJobs) != 1 || f2.jobs.deletedJobs[0] != "fm-task-2" {
		t.Fatalf("deletedJobs = %v, want the catch-up deletion", f2.jobs.deletedJobs)
	}
}

// A rebuilt entry has no token digest (it never leaves process memory), so
// the reclaim cannot revoke it — the degradation must be visible instead of
// silent (ADR-017 已知边界).
func TestReclaimWithoutDigestIsVisible(t *testing.T) {
	f := newFixture(t)
	var logs bytes.Buffer
	s := newLoggingScheduler(t, f, &logs)
	if err := f.reg.Put(registry.TaskEntry{
		TaskID: "task-1", JobName: "fm-task-1", DaemonID: "fm-task-1",
		State: registry.StateTerminal, JobPhase: registry.JobPhaseDraining,
		DrainDeadline: f.now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := s.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonJobTerminated, f.now, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	decodeLogEvent(t, &logs, "cred.revoke_unavailable")
	if len(f.revoker.revoked) != 0 {
		t.Fatalf("revoked = %v, want none without a digest", f.revoker.revoked)
	}
}

// The orphan-Secret sweep demands both halves of the criterion (failure-
// handling 场景 #16): no registry entry (any state) AND an explicit Job
// NotFound. Either half alone deletes a Secret that is still needed.
func TestSweepOrphanSecretsCriterion(t *testing.T) {
	f := newFixture(t)

	// A live entry (pending, Job not created yet): the Secret must stay.
	f.claim(t, "task-live")
	// An entry in drain: the Secret must stay too — the reclaim owns it.
	f.claim(t, "task-draining")
	if _, _, err := f.reg.MarkTerminal("task-draining", registry.ResultFailed, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	// A genuine orphan: no entry, no Job.
	f.jobs.secrets = []corev1.Secret{
		secretObject("fm-task-live-cred", "task-live"),
		secretObject("fm-task-draining-cred", "task-draining"),
		secretObject("fm-task-orphan-cred", "task-orphan"),
	}
	// The orphan's Job is gone; the drained task's Job is still there.
	f.jobs.existingJobs = map[string]*batchv1.Job{
		"fm-task-draining": {ObjectMeta: metav1.ObjectMeta{Name: "fm-task-draining"}},
	}
	// The claims above ran their own delete-then-create sweep: only the
	// round's deletions matter here.
	f.jobs.deletedSecrets = nil

	if err := f.sched.SweepOrphanSecrets(context.Background()); err != nil {
		t.Fatalf("SweepOrphanSecrets: %v", err)
	}
	if len(f.jobs.deletedSecrets) != 1 || f.jobs.deletedSecrets[0] != "fm-task-orphan-cred" {
		t.Fatalf("deleted secrets = %v, want only the orphan", f.jobs.deletedSecrets)
	}
}

// A Secret whose name and task-id label disagree is not a design product:
// the creation path writes both from one task id. Deleting by label would hit
// a same-named in-flight credential, so both positive criteria hold and the
// Secret still survives (TC-tech-auth-06 负向).
func TestSweepOrphanSecretsSkipsNameLabelMismatch(t *testing.T) {
	f := newFixture(t)

	// Paired with a genuine orphan in the same round: its deletion proves the
	// sweep ran, so the survivor is attributable to the mismatch alone.
	f.jobs.secrets = []corev1.Secret{
		secretObject("fm-task-orphan-cred", "task-orphan"),
		secretObject("fm-task-other-cred", "task-orphan"),
	}

	if err := f.sched.SweepOrphanSecrets(context.Background()); err != nil {
		t.Fatalf("SweepOrphanSecrets: %v", err)
	}
	if len(f.jobs.deletedSecrets) != 1 || f.jobs.deletedSecrets[0] != "fm-task-orphan-cred" {
		t.Fatalf("deleted secrets = %v, want only the name-label-consistent orphan", f.jobs.deletedSecrets)
	}
}

// The carrier domain is disjoint from the credential domain: a role-labelled
// Secret is never a credential candidate even though it carries a task-id
// label, and a carrier is swept only once its entry settled (§6.5).
func TestSweepSecretDomainsKeepCarriersOutOfTheCredentialCriterion(t *testing.T) {
	f := newFixture(t)
	f.pending.queued = map[string]bool{"task-pending": true}
	secret := func(name, taskID, role string) corev1.Secret {
		labels := map[string]string{
			registry.LabelManagedBy: registry.ManagedByForeman,
			registry.LabelTaskID:    taskID,
		}
		if role != "" {
			labels[registry.LabelRole] = role
		}
		return corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
	}
	f.jobs.secrets = []corev1.Secret{
		secret("fm-task-orphan-cred", "task-orphan", ""),
		secret("fm-carrier-pending", "task-pending", registry.RolePendingReport),
		secret("fm-carrier-settled", "task-settled", registry.RolePendingReport),
		secret("fm-catalog-probe-cred", "probe-1", registry.RoleCatalogProbe),
	}

	if err := f.sched.SweepOrphanSecrets(context.Background()); err != nil {
		t.Fatalf("SweepOrphanSecrets: %v", err)
	}
	// The credential domain runs first, then the carrier domain.
	want := []string{"fm-task-orphan-cred", "fm-carrier-settled"}
	if len(f.jobs.deletedSecrets) != len(want) {
		t.Fatalf("deleted secrets = %v, want %v", f.jobs.deletedSecrets, want)
	}
	for i, name := range want {
		if f.jobs.deletedSecrets[i] != name {
			t.Fatalf("deleted secrets = %v, want %v", f.jobs.deletedSecrets, want)
		}
	}
}

// A ListJobs that fails is not a NotFound: the criterion demands an explicit
// absence, so the Secret survives the round.
func TestSweepOrphanSecretsKeepsSecretOnAPIError(t *testing.T) {
	f := newFixture(t)
	f.jobs.secrets = []corev1.Secret{secretObject("fm-task-orphan-cred", "task-orphan")}
	f.jobs.listJobsErr = errors.New("apiserver unavailable")

	if err := f.sched.SweepOrphanSecrets(context.Background()); err == nil {
		t.Fatal("SweepOrphanSecrets: want the read error surfaced")
	}
	if len(f.jobs.deletedSecrets) != 0 {
		t.Fatalf("deleted secrets = %v, want none without an explicit absence", f.jobs.deletedSecrets)
	}
}

// Job deletion carries the Background propagation policy explicitly: the
// batch/v1 default (OrphanDependents) would keep the pods alive without a
// SIGTERM, so the termination evidence would stay false forever and every
// reclaim would fall to the drain upper bound (ADR-017).
func TestJobDeletionUsesBackgroundPropagation(t *testing.T) {
	opts := jobDeleteOptions()
	if opts.PropagationPolicy == nil || *opts.PropagationPolicy != metav1.DeletePropagationBackground {
		t.Fatalf("propagation policy = %v, want Background", opts.PropagationPolicy)
	}
}

func secretObject(name, taskID string) corev1.Secret {
	return corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: name,
		Labels: map[string]string{
			registry.LabelManagedBy: registry.ManagedByForeman,
			registry.LabelTaskID:    taskID,
		},
	}}
}
