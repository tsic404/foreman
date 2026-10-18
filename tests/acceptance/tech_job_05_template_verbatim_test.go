// TC-tech-job-05 implements AC-27's verbatim assertion for the custom-template
// window: a template that used to be refused wholesale (privileged sidecar,
// rewritten command/args, two containers, hostNetwork, its own
// serviceAccountName and parallelism, a node socket mount) starts and renders,
// and the only differences between the template and the Job are the A1 writes.
package acceptance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// customWindowTemplate is the design case's adversarial template: every shape
// it carries is the deployment's own, and none of it is Foreman's to change.
const customWindowTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
  labels: { cost-center: platform }
spec:
  backoffLimit: 0
  parallelism: 2
  ttlSecondsAfterFinished: 120
  template:
    metadata:
      labels: { app.kubernetes.io/name: foreman-job }
    spec:
      restartPolicy: Never
      serviceAccountName: custom-sa
      hostNetwork: true
      nodeSelector: { disktype: ssd }
      priorityClassName: batch-low
      affinity:
        nodeAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              preference:
                matchExpressions:
                  - { key: kubernetes.io/hostname, operator: In, values: ["<preferred node>"] }
      initContainers:
        - name: bootstrap
          image: registry.corp.example/bootstrap@sha256:1111111111111111111111111111111111111111111111111111111111111111
          command: ["/bin/sh", "-ec"]
          args: ["install -d -o 1000 -g 1000 /home/agent/bin"]
          securityContext: { runAsUser: 0, runAsNonRoot: false, allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"], add: ["CHOWN"] } }
          volumeMounts: [{ name: home, mountPath: /home/agent }]
      containers:
        - name: dind
          image: docker:29-dind@sha256:2222222222222222222222222222222222222222222222222222222222222222
          securityContext: { privileged: true }
          volumeMounts: [{ name: docker-sock, mountPath: /var/run/docker.sock }]
          env: []
        - name: agent
          image: <job-image>
          command: ["/bin/sh", "-ec"]
          args: ["exec /usr/local/bin/multica daemon start --foreground --profile foreman-job"]
          env:
            - { name: HOME, value: "/home/agent" }
            - { name: LOG_LEVEL, value: "debug" }
            - { name: HTTP_PROXY, value: "http://proxy.corp:3128" }
          volumeMounts:
            - { name: cred, mountPath: /home/agent/.multica/profiles/foreman-job/config.json, subPath: config.json, readOnly: true }
      volumes:
        - { name: home, hostPath: { path: /var/lib/foreman/home, type: DirectoryOrCreate } }
        - { name: docker-sock, hostPath: { path: /var/run/docker.sock } }
        - { name: cred-src, secret: { secretName: fm-<task_id>-cred, defaultMode: 0400 } }
        - { name: cred, emptyDir: { sizeLimit: 16Mi } }
`

func TestJob05TemplateVerbatimCustomWindow(t *testing.T) {
	templatePath := writeTemplateFile(t, customWindowTemplate)

	h := newHarness(t, false)
	h.useRealBuilderWithTemplate(t, templatePath)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	claim := func(taskID string) {
		t.Helper()
		payload := json.RawMessage(`{
			"id": "` + taskID + `", "agent_id": "agent-1", "workspace_id": "ws-1",
			"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1"
		}`)
		if err := h.sched.OnClaim(t.Context(), payload); err != nil {
			t.Fatalf("OnClaim(%s): %v", taskID, err)
		}
	}
	claim("task-1")
	job := h.createdJob(t, "fm-task-1")
	pod := job.Spec.Template.Spec

	// The template's own shape, verbatim.
	if got := containerNames(pod.Containers); len(got) != 2 || got[0] != "dind" || got[1] != "agent" {
		t.Fatalf("containers = %v, want the template's two entries in order", got)
	}
	if len(pod.InitContainers) != 1 || pod.InitContainers[0].Name != "bootstrap" {
		t.Fatalf("initContainers = %v, want the template's bootstrap", containerNames(pod.InitContainers))
	}
	if pod.Containers[0].SecurityContext == nil || pod.Containers[0].SecurityContext.Privileged == nil ||
		!*pod.Containers[0].SecurityContext.Privileged {
		t.Errorf("dind securityContext = %+v, want the template's privileged sidecar", pod.Containers[0].SecurityContext)
	}
	if !pod.HostNetwork || pod.ServiceAccountName != "custom-sa" {
		t.Errorf("hostNetwork/serviceAccountName = %v/%q, want the template's", pod.HostNetwork, pod.ServiceAccountName)
	}
	if job.Spec.Parallelism == nil || *job.Spec.Parallelism != 2 {
		t.Errorf("parallelism = %v, want the template's 2", job.Spec.Parallelism)
	}
	if job.Labels["cost-center"] != "platform" {
		t.Errorf("labels = %v, want the template's own label kept", job.Labels)
	}
	agent := pod.Containers[1]
	if strings.Join(agent.Command, " ") != "/bin/sh -ec" ||
		strings.Join(agent.Args, " ") != "exec /usr/local/bin/multica daemon start --foreground --profile foreman-job" {
		t.Errorf("agent command/args = %v %v, want the template's rewritten pair", agent.Command, agent.Args)
	}
	if socket := volumeByName(t, pod.Volumes, "docker-sock"); socket.HostPath == nil || socket.HostPath.Path != "/var/run/docker.sock" {
		t.Errorf("docker-sock = %+v, want the template's node socket", socket.VolumeSource)
	}
	if pod.NodeSelector["disktype"] != "ssd" {
		t.Errorf("nodeSelector = %v, want the template's (no §5.1 env is applied)", pod.NodeSelector)
	}
	if got := envValue(agent.Env, "HTTP_PROXY"); got != "http://proxy.corp:3128" {
		t.Errorf("agent HTTP_PROXY = %q, want the template's", got)
	}
	if got := envValue(agent.Env, "LOG_LEVEL"); got != "debug" {
		t.Errorf("agent LOG_LEVEL = %q, want the template's", got)
	}
	if bootstrap := pod.InitContainers[0]; !strings.HasPrefix(bootstrap.Image, "registry.corp.example/bootstrap@sha256:") {
		t.Errorf("bootstrap image = %q, want the template's", bootstrap.Image)
	}

	// The A1 writes: the only differences allowed.
	if got := envValue(pod.Containers[0].Env, "MULTICA_SERVER_URL"); got != "http://foreman.foreman.svc.cluster.local:8080" {
		t.Errorf("containers[0] MULTICA_SERVER_URL = %q, want Foreman's address", got)
	}
	if daemonID := envByName(t, pod.Containers[0].Env, "MULTICA_DAEMON_ID"); daemonID.ValueFrom == nil ||
		daemonID.ValueFrom.FieldRef == nil || daemonID.ValueFrom.FieldRef.FieldPath != "metadata.labels['foreman.tsic.top/daemon-id']" {
		t.Errorf("containers[0] MULTICA_DAEMON_ID = %+v, want the daemon-id label fieldRef", daemonID)
	}
	if got := envValue(pod.Containers[0].Env, "MULTICA_DAEMON_MAX_CONCURRENT_TASKS"); got != "1" {
		t.Errorf("containers[0] MULTICA_DAEMON_MAX_CONCURRENT_TASKS = %q, want 1", got)
	}
	for _, entry := range agent.Env {
		if strings.HasPrefix(entry.Name, "MULTICA_") {
			t.Errorf("containers[1] carries the injected key %q: A1.1 targets containers[0] only", entry.Name)
		}
	}
	if job.Name != "fm-task-1" || job.Namespace != "multica-agents" {
		t.Errorf("job %q/%q, want fm-task-1/multica-agents", job.Namespace, job.Name)
	}
	credSrc := volumeByName(t, pod.Volumes, "cred-src")
	if credSrc.Secret == nil || credSrc.Secret.SecretName != "fm-task-1-cred" {
		t.Errorf("cred-src = %+v, want the composite literal fm-task-1-cred", credSrc.VolumeSource)
	}
	if job.Labels[registry.LabelTaskID] != "task-1" || job.Annotations[registry.AnnoDaemonID] != "fm-task-1" {
		t.Errorf("per-task keys = %v/%v", job.Labels, job.Annotations)
	}
	if job.Spec.Template.Labels[registry.LabelTaskID] != "task-1" || job.Spec.Template.Labels["app.kubernetes.io/name"] != "foreman-job" {
		t.Errorf("pod labels = %v, want the two §3.2 labels", job.Spec.Template.Labels)
	}
	if _, present := job.Spec.Template.Labels["app.kubernetes.io/managed-by"]; present {
		t.Error("pod labels carry managed-by: that key is Job-side only")
	}

	// No registered placeholder survives; the template's own unregistered
	// literals would be allowed, this template has none.
	rendered := canonicalJob(job)
	for _, placeholder := range registeredPlaceholders {
		if strings.Contains(rendered, placeholder) {
			t.Errorf("placeholder %s survived into the Job:\n%s", placeholder, rendered)
		}
	}

	// A1.5 does not fire without a policy file: containers[0] keeps exactly the
	// two fields the template wrote.
	if resources, present := pod.Containers[0].Resources.Requests["cpu"]; present && !resources.IsZero() {
		t.Errorf("containers[0] resources = %+v, want the template's (no policy injection)", pod.Containers[0].Resources)
	}
	if len(pod.Containers[0].Resources.Requests) != 0 || len(pod.Containers[0].Resources.Limits) != 0 {
		t.Errorf("containers[0] resources = %+v, want none", pod.Containers[0].Resources)
	}
	security := pod.Containers[0].SecurityContext
	if security.Privileged == nil || !*security.Privileged || security.Capabilities != nil {
		t.Errorf("containers[0] securityContext = %+v, want the template's privileged flag and nothing else", security)
	}

	// A1.3: a second task renders from the same template without carrying the
	// first task's values.
	claim("task-2")
	second := h.createdJob(t, "fm-task-2")
	if got := volumeByName(t, second.Spec.Template.Spec.Volumes, "cred-src").Secret.SecretName; got != "fm-task-2-cred" {
		t.Errorf("second Job cred-src = %q, want fm-task-2-cred", got)
	}
	if rendered := canonicalJob(second); strings.Contains(rendered, "task-1") {
		t.Errorf("the second Job carries the first task's identity:\n%s", rendered)
	}
}

// TestJob05NodeLiteralOutsideRecognitionSite (AC-27): <preferred node> is the
// substitution set's one exception — a template that writes it outside the
// recognition site keeps it verbatim. The Job still builds; whether that
// position can take the literal is the apiserver's call at creation
// (03-contracts §5.4「占位符替换」第 4 条), never a Build failure and never a
// silent rewrite.
func TestJob05NodeLiteralOutsideRecognitionSite(t *testing.T) {
	const manifest = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
spec:
  template:
    spec:
      restartPolicy: Never
      affinity:
        nodeAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              preference:
                matchExpressions:
                  - { key: kubernetes.io/hostname, operator: In, values: ["<preferred node>"] }
      containers:
        - name: agent
          image: <job-image>
          args: ["daemon", "start", "--node", "<preferred node>"]
`
	templatePath := writeTemplateFile(t, manifest)

	h := newHarness(t, false)
	h.useRealBuilderWithTemplate(t, templatePath)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	payload := json.RawMessage(`{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1"
	}`)
	if err := h.sched.OnClaim(t.Context(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	job := h.createdJob(t, "fm-task-1")
	pod := job.Spec.Template.Spec

	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %v, want the template's one", containerNames(pod.Containers))
	}
	args := strings.Join(pod.Containers[0].Args, " ")
	if !strings.Contains(args, "<preferred node>") {
		t.Errorf("agent args = %q, want the literal verbatim", args)
	}
	// No resume session: the recognition item goes with the structures it
	// empties, and the deployment's own position is untouched.
	if pod.Affinity != nil {
		t.Errorf("affinity = %+v, want the recognition item dropped", pod.Affinity)
	}
}

// registeredPlaceholders is the substitution set of 05-modules/job-template.md
// 「占位符与回归基线」.
var registeredPlaceholders = []string{
	"<task_id>", "<agent_id>", "<workspace_id>", "<issue_id>", "<issue-identifier>",
	"<autopilot_run_id>", "<chat_session_id>", "<job runtime uuid>", "<RFC3339>",
	"<job-image>", "<job-name>", "<daemon-id>", "<agent-name>", "<preferred node>",
}

// writeTemplateFile writes a template manifest where the builder reads it.
func writeTemplateFile(t *testing.T, manifest string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "job-overlay.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write template: %v", err)
	}
	return path
}

func containerNames(containers []corev1.Container) []string {
	names := make([]string, 0, len(containers))
	for _, container := range containers {
		names = append(names, container.Name)
	}
	return names
}

func envByName(t *testing.T, env []corev1.EnvVar, name string) corev1.EnvVar {
	t.Helper()
	for _, entry := range env {
		if entry.Name == name {
			return entry
		}
	}
	t.Fatalf("env %q not found", name)
	return corev1.EnvVar{}
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, entry := range env {
		if entry.Name == name {
			return entry.Value
		}
	}
	return ""
}
