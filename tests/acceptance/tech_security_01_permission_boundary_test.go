// TC-tech-security-01 pins the manifest half of AC-13's permission and image
// boundary on the reference-template window: the rendered task Job has no
// privilege escalation, no host network/PID, no runtime socket, exactly one
// business container, a non-root agent with the fixed uid, the prepare
// capability set verbatim, and every container image equal to the deployment's
// own FOREMAN_JOB_IMAGE reference (no digest assembly). The design case's
// steps 3/4 — the runtime `id`/`stat` and /proc/mounts checks inside a live pod
// — run from tests/acceptance/tech-security-01-cluster.sh.
package acceptance

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// tcSecurity01DeployedJobImage reads the reference the deployment writes into
// every Job: FOREMAN_JOB_IMAGE in deploy/30-foreman.yaml.
func tcSecurity01DeployedJobImage(t *testing.T) string {
	t.Helper()
	documents := yamlDocuments(t, filepath.Join("..", "..", "deploy", "30-foreman.yaml"))
	for _, document := range documents {
		jsonBytes, err := utilyaml.ToJSON(document)
		if err != nil {
			t.Fatalf("decode deploy/30-foreman.yaml: %v", err)
		}
		var probe struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(jsonBytes, &probe); err != nil {
			t.Fatalf("decode deploy/30-foreman.yaml: %v", err)
		}
		if probe.Kind != "Deployment" {
			continue
		}
		var deployment appsv1.Deployment
		if err := json.Unmarshal(jsonBytes, &deployment); err != nil {
			t.Fatalf("decode the foreman Deployment: %v", err)
		}
		if len(deployment.Spec.Template.Spec.Containers) == 0 {
			t.Fatal("the foreman Deployment declares no container")
		}
		for _, env := range deployment.Spec.Template.Spec.Containers[0].Env {
			if env.Name != "FOREMAN_JOB_IMAGE" {
				continue
			}
			if env.Value == "" {
				t.Fatalf("FOREMAN_JOB_IMAGE is not a literal value: the deployment's reference must be readable (AC-13)")
			}
			return env.Value
		}
		t.Fatal("the foreman Deployment declares no FOREMAN_JOB_IMAGE")
	}
	t.Fatal("deploy/30-foreman.yaml carries no Deployment")
	return ""
}

func TestSecurity01PermissionBoundary(t *testing.T) {
	image := tcSecurity01DeployedJobImage(t)
	h := newHarness(t, false)
	h.jobImage = image
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
	job := h.createdJob(t, "fm-task-1")
	pod := job.Spec.Template.Spec

	// Privilege shape: no host namespaces, no privileged container, no
	// container-runtime socket.
	if pod.HostNetwork {
		t.Error("hostNetwork = true, want the pod network")
	}
	if pod.HostPID {
		t.Error("hostPID = true, want the pod PID namespace")
	}
	for _, container := range append(append([]corev1.Container{}, pod.InitContainers...), pod.Containers...) {
		if container.SecurityContext != nil && container.SecurityContext.Privileged != nil &&
			*container.SecurityContext.Privileged {
			t.Errorf("container %q is privileged", container.Name)
		}
	}
	for _, volume := range pod.Volumes {
		if volume.HostPath == nil {
			continue
		}
		if !strings.HasPrefix(volume.HostPath.Path, "/var/lib/foreman") {
			t.Errorf("volume %q hostPath = %q, want a path under /var/lib/foreman", volume.Name, volume.HostPath.Path)
		}
		if strings.Contains(volume.HostPath.Path, "docker.sock") || strings.Contains(volume.HostPath.Path, "containerd") {
			t.Errorf("volume %q mounts the container runtime socket: %q", volume.Name, volume.HostPath.Path)
		}
	}

	// Container counts: one business container (native sidecars live in
	// initContainers) and at least one init container.
	if got := len(pod.Containers); got != 1 {
		t.Errorf("containers = %d, want exactly one business container", got)
	}
	if len(pod.InitContainers) == 0 {
		t.Error("initContainers is empty, want the template's prepare/omp entries")
	}

	// The agent's fixed non-root identity (AC-13's user face).
	agent := tcJob09Container(t, pod.Containers, "agent")
	if agent.SecurityContext == nil || agent.SecurityContext.RunAsNonRoot == nil || !*agent.SecurityContext.RunAsNonRoot {
		t.Error("agent runAsNonRoot is not true")
	}
	if agent.SecurityContext == nil || agent.SecurityContext.RunAsUser == nil || *agent.SecurityContext.RunAsUser != 1000 {
		t.Errorf("agent runAsUser = %v, want 1000", agent.SecurityContext)
	}

	// The prepare capability set verbatim: drop ALL plus the two ownership and
	// metadata capabilities — a superset is a design change, not implementation
	// freedom (AC-13).
	prepare := tcJob09Container(t, pod.InitContainers, "prepare")
	if prepare.SecurityContext == nil || prepare.SecurityContext.Capabilities == nil {
		t.Fatal("prepare declares no capability set")
	}
	caps := prepare.SecurityContext.Capabilities
	if got, want := tcCapabilities(caps.Drop), []corev1.Capability{"ALL"}; !tcCapabilitySlicesEqual(got, want) {
		t.Errorf("prepare drop = %v, want %v", got, want)
	}
	if got, want := tcCapabilities(caps.Add), []corev1.Capability{"CHOWN", "DAC_OVERRIDE"}; !tcCapabilitySlicesEqual(got, want) {
		t.Errorf("prepare add = %v, want %v verbatim", got, want)
	}

	// The ownership write itself (ADR-011 / AC-13): prepare chowns the three
	// trees whose contents cross task boundaries — the home volume, the
	// profile directory inside it and the workspaces tree. This literal is the
	// only in-repo assertion that goes red when a chown target leaves the
	// template; the published-template/fixture byte comparison cannot see it,
	// because both copies would move together.
	if len(prepare.Args) != 1 ||
		!strings.Contains(prepare.Args[0], "chown -R 1000:1000 /home/agent /state/workspaces /credout") {
		t.Errorf("prepare args = %v, want the ownership write over /home/agent, /state/workspaces and /credout (ADR-011 / AC-13)", prepare.Args)
	}

	// Pod-level hardening and the pull credential.
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Error("automountServiceAccountToken is not false")
	}
	if len(pod.ImagePullSecrets) == 0 {
		t.Error("imagePullSecrets is empty, want the registry pull credential")
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("backoffLimit = %v, want 0", job.Spec.BackoffLimit)
	}
	if pod.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q, want Never", pod.RestartPolicy)
	}

	// Image references: every container equals the deployment's reference
	// verbatim — no digest concatenation (ADR-012).
	for _, container := range append(append([]corev1.Container{}, pod.InitContainers...), pod.Containers...) {
		if container.Image != image {
			t.Errorf("container %q image = %q, want the deployment reference %q verbatim", container.Name, container.Image, image)
		}
	}
}

func tcCapabilities(caps []corev1.Capability) []corev1.Capability {
	return append([]corev1.Capability(nil), caps...)
}

func tcCapabilitySlicesEqual(got, want []corev1.Capability) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
