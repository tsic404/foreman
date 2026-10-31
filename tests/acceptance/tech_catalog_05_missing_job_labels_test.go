// TC-tech-catalog-05 implements the probe's label face (AC-32): §8.1 accepts a
// deployment template whose Job metadata declares no labels — that shape is
// outside the four refusal classes — and §8.4's label face still writes the
// probe block, managed-by among it, on both sides. The startup probe renders
// over this template, so a nil label map used to crash the process instead of
// starting.
package acceptance

import (
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

// probeTemplateWithoutJobLabels writes a legal Job whose metadata carries only
// a name: no Job-side label block at all.
const probeTemplateWithoutJobLabels = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
spec:
  backoffLimit: 0
  template:
    metadata:
      labels: { cost-center: platform }
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
          command: ["/usr/bin/dumb-init", "--", "/usr/local/bin/multica"]
          args: ["daemon", "start", "--foreground", "--profile", "foreman-job"]
          env: []
          volumeMounts:
            - { name: cred, mountPath: /home/agent/.multica/profiles/foreman-job/config.json, subPath: config.json, readOnly: true }
      volumes:
        - name: cred-src
          secret: { secretName: fm-<task_id>-cred, defaultMode: 0400 }
`

func TestCatalog05StartupProbeTemplateWithoutJobLabels(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilderWithTemplate(t, writeTemplateFile(t, probeTemplateWithoutJobLabels))
	// The probe must register, or the startup window closes with a failure
	// instead of handing back the Job it rendered.
	h.jobs.onCreateJob = func(job *batchv1Job) {
		token := h.builder.probeToken(job.Name)
		h.call("POST", "/api/daemon/register", token, []byte(`{
			"daemon_id": "`+job.Name+`",
			"cli_version": "0.6.1",
			"runtimes": [{"name": "foreman-omp", "type": "omp", "version": "0.6.1"}]
		}`))
	}
	// §8.1: the template passes the startup gate and the probe renders — no
	// refusal path, no crash.
	if _, err := h.sched.RunStartupProbe(t.Context()); err != nil {
		t.Fatalf("RunStartupProbe over a template without Job labels: %v", err)
	}
	probes := h.jobs.probeJobsCreated()
	if len(probes) != 1 {
		t.Fatalf("probe Jobs = %v, want exactly one", probes)
	}
	job := h.createdJob(t, probes[0])

	// §8.4 label face: the probe block lands even though the template wrote
	// none, and the workspace identity is the probe credential's.
	wantLabels := map[string]string{
		labelAppNameOwnership:     "foreman-job",
		registry.LabelManagedBy:   "foreman",
		registry.LabelRole:        registry.RoleCatalogProbe,
		labelDaemonIDOwnership:    probes[0],
		registry.LabelWorkspaceID: h.schedCfg.WorkspaceID,
	}
	if len(job.Labels) != len(wantLabels) {
		t.Fatalf("probe Job labels = %v, want exactly %v", job.Labels, wantLabels)
	}
	for key, want := range wantLabels {
		if job.Labels[key] != want {
			t.Errorf("probe Job label %q = %q, want %q", key, job.Labels[key], want)
		}
	}
	if _, hasTask := job.Labels[registry.LabelTaskID]; hasTask {
		t.Errorf("probe Job labels = %v, want no task id", job.Labels)
	}

	// Pod side: the probe's own three keys plus the workspace identity, the
	// template's out-of-domain key kept.
	podLabels := job.Spec.Template.Labels
	if podLabels[labelAppNameOwnership] != "foreman-job" ||
		podLabels[registry.LabelRole] != registry.RoleCatalogProbe ||
		podLabels[labelDaemonIDOwnership] != probes[0] ||
		podLabels[registry.LabelWorkspaceID] != h.schedCfg.WorkspaceID {
		t.Errorf("probe pod labels = %v, want the probe block and workspace identity", podLabels)
	}
	if podLabels["cost-center"] != "platform" {
		t.Errorf("probe pod labels = %v, want the template's own label kept", podLabels)
	}
	if _, hasManagedBy := podLabels[registry.LabelManagedBy]; hasManagedBy {
		t.Errorf("probe pod labels = %v, want no managed-by on a pod", podLabels)
	}
}
