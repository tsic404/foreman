// TC-tech-catalog-09 / AC-64 implements the write face boundary of the probe
// render as a negative: a deployment template that carries its own image
// references — the published repository name reads "foreman-job" — and its own
// pull credential keeps all three references verbatim on the probe Job, while
// the two literals still take the probe's own values on the faces the write
// face declares (03-acceptance.md「模型目录」; contracts §8.4 第 4 条).
package acceptance

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// literalFaceTemplate writes the two probe faces as their task-side literals
// and its own image references as plain values: the shape a whole-object
// literal rename corrupts by turning the repository name into the probe
// profile's.
const literalFaceTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
  labels: { cost-center: foreman-job }
spec:
  backoffLimit: 0
  template:
    metadata:
      labels: { cost-center: foreman-job }
    spec:
      restartPolicy: Never
      imagePullSecrets:
        - { name: foreman-job-pull }
      initContainers:
        - name: prepare
          image: ghcr.io/tsic404/foreman-job:v0.1.0
          command: ["/bin/sh", "-ec"]
          args: ["install -d /home/agent/.multica/profiles/foreman-job"]
          volumeMounts:
            - { name: home, mountPath: /home/agent }
      containers:
        - name: agent
          image: ghcr.io/tsic404/foreman-job:v0.1.0
          command: ["/usr/bin/dumb-init", "--", "/usr/local/bin/multica"]
          args: ["daemon", "start", "--foreground", "--profile", "foreman-job"]
          env:
            - { name: MULTICA_AGENT_RUNTIME_NAME, value: "foreman-job" }
            - { name: MULTICA_SERVER_URL, value: "http://somewhere-else:1234" }
          volumeMounts:
            - { name: cred, mountPath: /home/agent/.multica/profiles/foreman-job/config.json, subPath: config.json, readOnly: true }
      volumes:
        - name: home
          hostPath: { path: /var/lib/foreman/home, type: DirectoryOrCreate }
        - name: cred
          emptyDir: { sizeLimit: 16Mi }
        - name: cred-src
          secret: { secretName: fm-<task_id>-cred, defaultMode: 0400 }
`

func TestCatalog09ProbeRenderKeepsTemplateLiteralFaces(t *testing.T) {
	const literalImage = "ghcr.io/tsic404/foreman-job:v0.1.0"

	h := newHarness(t, false)
	h.useRealBuilderWithTemplate(t, writeTemplateFile(t, literalFaceTemplate))
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
	if _, err := h.sched.RunStartupProbe(t.Context()); err != nil {
		t.Fatalf("RunStartupProbe: %v", err)
	}

	probes := h.jobs.probeJobsCreated()
	if len(probes) != 1 {
		t.Fatalf("probe Jobs = %v, want exactly one", probes)
	}
	job := h.createdJob(t, probes[0])
	pod := job.Spec.Template.Spec

	// The template's own image references and pull credential: verbatim, the
	// literal rename does not reach them.
	for _, container := range pod.InitContainers {
		if container.Image != literalImage {
			t.Errorf("init container %q image = %q, want the template's verbatim %q",
				container.Name, container.Image, literalImage)
		}
	}
	for _, container := range pod.Containers {
		if container.Image != literalImage {
			t.Errorf("container %q image = %q, want the template's verbatim %q",
				container.Name, container.Image, literalImage)
		}
	}
	if len(pod.ImagePullSecrets) != 1 || pod.ImagePullSecrets[0].Name != "foreman-job-pull" {
		t.Errorf("imagePullSecrets = %v, want the template's own pull credential", pod.ImagePullSecrets)
	}

	// The two declared faces still take the probe's values: the daemon's start
	// line and the profile directory beside it, the volume's Secret reference.
	agent := pod.Containers[0]
	if got := strings.Join(agent.Args, " "); !strings.Contains(got, "--profile foreman-catalog") {
		t.Errorf("probe args = %q, want the probe profile", got)
	}
	if got := mountPathOf(t, agent.VolumeMounts, "cred"); got != "/home/agent/.multica/profiles/foreman-catalog/config.json" {
		t.Errorf("cred mount = %q, want the probe profile path", got)
	}
	if got := strings.Join(pod.InitContainers[0].Args, " "); !strings.Contains(got, "profiles/foreman-catalog") {
		t.Errorf("prepare args = %q, want the probe profile directory", got)
	}
	credSrc := volumeByName(t, pod.Volumes, "cred-src")
	if credSrc.Secret == nil || credSrc.Secret.SecretName != probes[0]+"-cred" {
		t.Errorf("cred-src = %+v, want the probe's Secret", credSrc.VolumeSource)
	}

	// Positions off the face keep the template's own text.
	if got := envValue(agent.Env, "MULTICA_AGENT_RUNTIME_NAME"); got != "foreman-job" {
		t.Errorf("MULTICA_AGENT_RUNTIME_NAME = %q, want the template's own %q", got, "foreman-job")
	}
	if got := job.Spec.Template.Labels["cost-center"]; got != "foreman-job" {
		t.Errorf("pod label cost-center = %q, want the template's own %q", got, "foreman-job")
	}
	// The credential carrier is the probe's own: role label, no task face.
	cred := h.createdSecret(t, probes[0]+"-cred")
	if cred.Labels[registry.LabelRole] != registry.RoleCatalogProbe {
		t.Errorf("probe Secret labels = %v, want the catalog-probe role", cred.Labels)
	}
	if _, hasTask := cred.Labels[registry.LabelTaskID]; hasTask {
		t.Errorf("probe Secret labels = %v, want no task-id label", cred.Labels)
	}
}

// mountPathOf returns the mount path of the named volume mount.
func mountPathOf(t *testing.T, mounts []corev1.VolumeMount, name string) string {
	t.Helper()
	for _, mount := range mounts {
		if mount.Name == name {
			return mount.MountPath
		}
	}
	t.Fatalf("mount %q not found", name)
	return ""
}
