// TC-tech-catalog-01 / AC-31: the probe remnant sweep is a round-level action
// (§6.5 探针残留清扫), so it is not tied to startup. A probe teardown that
// failed during the run leaves the Job and its Secret behind; the next
// convergence round clears both, with no restart in between. The entry index
// is empty here — probe remnants are found by their labels, never by the task
// index.
package acceptance

import (
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
)

func TestCatalog01ProbeRemnantSweptByTheReconcileRound(t *testing.T) {
	h := newHarness(t, true)
	// The grace is one catalog budget plus a reconciliation round on each
	// side; derive the stale timestamp from the running config.
	grace := h.schedCfg.ModelCatalogTimeout + 2*h.schedCfg.ReconcileInterval
	stale := metav1.NewTime(time.Now().Add(-grace - time.Minute))
	labels := map[string]string{registry.LabelRole: registry.RoleCatalogProbe}

	remnantJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "fm-catalog-remnant", Labels: labels, CreationTimestamp: stale,
	}}
	remnantSecret := corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: "fm-catalog-remnant-cred", Labels: labels, CreationTimestamp: stale,
	}}
	h.jobs.programListed([]batchv1.Job{remnantJob}, []corev1.Secret{remnantSecret})
	if err := h.probes.PutProbe(registry.ProbeSession{
		JobName: remnantJob.Name, JobRuntime: "rt-remnant", RequestID: "req-remnant",
	}); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}

	// No Scheduler.Start: this is a running process whose earlier teardown
	// failed, so only the periodic round can clear the remnant.
	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Minute},
		h.reg, queueObjects{jobs: h.jobs}, h.server, h.sched,
	)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !h.jobs.jobDeleted(remnantJob.Name) {
		t.Error("the stale probe Job survived the round")
	}
	if !h.jobs.secretDeleted(remnantSecret.Name) {
		t.Error("the stale probe Secret survived the round")
	}
	if _, ok := h.probes.ProbeByJob(remnantJob.Name); ok {
		t.Error("the swept probe's session must go with it, so the token fails closed")
	}
}
