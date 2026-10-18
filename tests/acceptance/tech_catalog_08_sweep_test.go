// TC-tech-catalog-08 / AC-31: the stale-probe sweep works off the two discovery
// faces (the Job list and the Secret list) under one grace period and one
// ownership exclusion. A stale probe Job takes its Secret with it; a stale
// Secret whose Job is gone is removed on its own; nothing fresh is touched; and
// anything carrying a task-id belongs to the task face — the probe sweep never
// reaches across the boundary, even when someone injects the probe role onto a
// task object (ADR-019 符合性确认 7; 03-acceptance.md「模型目录」).
package acceptance

import (
	"slices"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/registry"
)

func TestCatalog08StaleProbeSweepCoversBothFaces(t *testing.T) {
	h := newHarness(t, true)
	// The grace is one catalog budget plus a reconciliation round on each side:
	// derive the stale timestamps from the running config instead of pinning it.
	grace := h.schedCfg.ModelCatalogTimeout + 2*h.schedCfg.ReconcileInterval
	stale := metav1.NewTime(time.Now().Add(-grace - time.Minute))
	withinGrace := metav1.NewTime(time.Now().Add(-grace / 2))
	fresh := metav1.NewTime(time.Now())

	probeLabels := map[string]string{registry.LabelRole: registry.RoleCatalogProbe}
	// The reverse face: a task object with the probe role injected is still a
	// task object — the task-id label is what owns it.
	taskLabels := map[string]string{
		registry.LabelRole:   registry.RoleCatalogProbe,
		registry.LabelTaskID: "task-1",
	}
	probeJob := func(name string, created metav1.Time, labels map[string]string) batchv1.Job {
		return batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, CreationTimestamp: created}}
	}
	probeSecret := func(name string, created metav1.Time, labels map[string]string) corev1.Secret {
		return corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, CreationTimestamp: created}}
	}

	// Face 1: a stale probe Job with its Secret.
	staleJob := probeJob("fm-catalog-stale", stale, probeLabels)
	staleJobSecret := probeSecret("fm-catalog-stale-cred", stale, probeLabels)
	// Face 2: the Job is gone, the Secret is stale.
	orphanSecret := probeSecret("fm-catalog-orphan-cred", stale, probeLabels)
	// Inside the grace: old enough to look abandoned, young enough to keep.
	withinJob := probeJob("fm-catalog-recent", withinGrace, probeLabels)
	withinSecret := probeSecret("fm-catalog-recent-cred", withinGrace, probeLabels)
	// A live Job owns its Secret: the "Secret written, Job not yet created"
	// window must survive even when the Secret itself is old.
	liveJob := probeJob("fm-catalog-live", fresh, probeLabels)
	liveSecret := probeSecret("fm-catalog-live-cred", stale, probeLabels)
	// The task face with the probe role injected.
	taskJob := probeJob("fm-task-1", stale, taskLabels)
	taskSecret := probeSecret("fm-task-1-cred", stale, taskLabels)

	h.jobs.programListed(
		[]batchv1.Job{staleJob, withinJob, liveJob, taskJob},
		[]corev1.Secret{staleJobSecret, orphanSecret, withinSecret, liveSecret, taskSecret},
	)
	h.jobs.programGetJob(&liveJob)
	// The swept Job's probe session goes with it: the credential stops being
	// valid the moment the remnant does.
	if err := h.probes.PutProbe(registry.ProbeSession{JobName: staleJob.Name, JobRuntime: "rt-stale", RequestID: "req-stale"}); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}

	if err := h.sched.SweepStaleProbes(t.Context()); err != nil {
		t.Fatalf("SweepStaleProbes: %v", err)
	}

	jobsDeleted := h.jobs.deletedJobNames()
	slices.Sort(jobsDeleted)
	if !slices.Equal(jobsDeleted, []string{"fm-catalog-stale"}) {
		t.Errorf("Jobs deleted = %v, want the stale probe Job alone", jobsDeleted)
	}
	secretsDeleted := h.jobs.deletedSecretNames()
	slices.Sort(secretsDeleted)
	if !slices.Equal(secretsDeleted, []string{"fm-catalog-orphan-cred", "fm-catalog-stale-cred"}) {
		t.Errorf("Secrets deleted = %v, want the stale Job's Secret and the orphan", secretsDeleted)
	}
	if _, ok := h.probes.ProbeByJob(staleJob.Name); ok {
		t.Error("the swept probe Job's session must be removed with it")
	}
}
