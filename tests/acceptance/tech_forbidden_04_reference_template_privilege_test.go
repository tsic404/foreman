// TC-tech-forbidden-04 (F4): the reference template carries no privilege
// escalation, no host namespaces and no container-runtime socket; the agent
// container is non-root with the fixed uid, and the prepare container's
// capability set is the minimal one. The policy capability axis only appends —
// it cannot re-enable privileged mode or a host namespace.
package acceptance

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// tcForbidden04NoRuntimeSocket asserts no mount path or host path is a
// container-runtime socket.
func tcForbidden04NoRuntimeSocket(t *testing.T, label string, mounts []corev1.VolumeMount, volumes []corev1.Volume) {
	t.Helper()
	sockets := []string{"docker.sock", "containerd.sock", "crio.sock", "podman.sock", "k3s.sock"}
	for _, mount := range mounts {
		for _, socket := range sockets {
			if strings.Contains(mount.MountPath, socket) {
				t.Errorf("%s mounts a runtime socket at %s", label, mount.MountPath)
			}
		}
	}
	for _, volume := range volumes {
		if volume.HostPath == nil {
			continue
		}
		for _, socket := range sockets {
			if strings.Contains(volume.HostPath.Path, socket) {
				t.Errorf("%s carries a runtime socket hostPath %s", label, volume.HostPath.Path)
			}
		}
	}
}

// tcForbidden04AssertPod checks the pod-level privilege face.
func tcForbidden04AssertPod(t *testing.T, label string, pod corev1.PodSpec) {
	t.Helper()
	if pod.HostNetwork || pod.HostPID || pod.HostIPC {
		t.Errorf("%s: host namespace in use (network=%v pid=%v ipc=%v)",
			label, pod.HostNetwork, pod.HostPID, pod.HostIPC)
	}
	if pod.SecurityContext == nil {
		t.Errorf("%s: no pod securityContext", label)
		return
	}
	if pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot {
		t.Errorf("%s: pod is not runAsNonRoot", label)
	}
	if pod.SecurityContext.RunAsUser == nil || *pod.SecurityContext.RunAsUser != 1000 {
		t.Errorf("%s: pod runAsUser = %v, want 1000", label, pod.SecurityContext.RunAsUser)
	}
	for _, container := range append(append([]corev1.Container{}, pod.InitContainers...), pod.Containers...) {
		if container.SecurityContext != nil && container.SecurityContext.Privileged != nil &&
			*container.SecurityContext.Privileged {
			t.Errorf("%s: container %s is privileged", label, container.Name)
		}
		if container.SecurityContext != nil && container.SecurityContext.RunAsUser != nil &&
			*container.SecurityContext.RunAsUser == 0 && container.Name == "agent" {
			t.Errorf("%s: the agent container runs as root", label)
		}
		for _, port := range container.Ports {
			if port.HostPort != 0 {
				t.Errorf("%s: container %s publishes hostPort %d", label, container.Name, port.HostPort)
			}
		}
	}
	tcForbidden04NoRuntimeSocket(t, label, nil, pod.Volumes)
}

func TestForbidden04ReferenceTemplateHasNoPrivilegePath(t *testing.T) {
	template := decodeJob(t, []byte(publishedTemplateContent(t)))
	tcForbidden04AssertPod(t, "reference template", template.Spec.Template.Spec)

	agent := tcJob09Container(t, template.Spec.Template.Spec.Containers, "agent")
	if agent.SecurityContext == nil {
		t.Fatal("the agent container has no securityContext")
	}
	security := agent.SecurityContext
	if security.RunAsNonRoot == nil || !*security.RunAsNonRoot ||
		security.RunAsUser == nil || *security.RunAsUser != 1000 {
		t.Errorf("agent identity = runAsNonRoot %v runAsUser %v, want true/1000",
			security.RunAsNonRoot, security.RunAsUser)
	}
	if security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation {
		t.Error("the agent container allows privilege escalation")
	}
	if security.Capabilities == nil || !tcCapabilitySlicesEqual(security.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Errorf("agent capabilities.drop = %+v, want [ALL]", security.Capabilities)
	}
	if len(security.Capabilities.Add) != 0 {
		t.Errorf("agent capabilities.add = %v, want none", security.Capabilities.Add)
	}
	tcForbidden04NoRuntimeSocket(t, "agent container", agent.VolumeMounts, template.Spec.Template.Spec.Volumes)

	prepare := tcJob09Container(t, template.Spec.Template.Spec.InitContainers, "prepare")
	if prepare.SecurityContext == nil || prepare.SecurityContext.Capabilities == nil {
		t.Fatal("the prepare container has no capabilities")
	}
	caps := prepare.SecurityContext.Capabilities
	if !tcCapabilitySlicesEqual(caps.Drop, []corev1.Capability{"ALL"}) {
		t.Errorf("prepare capabilities.drop = %v, want [ALL]", caps.Drop)
	}
	if len(caps.Add) != 2 || !tcCapabilitySlicesEqual(caps.Add, []corev1.Capability{"CHOWN", "DAC_OVERRIDE"}) {
		t.Errorf("prepare capabilities.add = %v, want [CHOWN DAC_OVERRIDE]", caps.Add)
	}
	if prepare.SecurityContext.AllowPrivilegeEscalation == nil || *prepare.SecurityContext.AllowPrivilegeEscalation {
		t.Error("the prepare container allows privilege escalation")
	}

	// The policy axis only appends capabilities: a hit must not turn the
	// privileged flag or a host namespace on.
	h := newHarness(t, false)
	policyPath := writePolicyFile(t, `
agents:
  agent-1:
    capPolicy: agent
    capabilities:
      add: ["SYS_ADMIN"]
`)
	h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
	tcPrepareRuntimes(t, h)
	tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
	rendered := h.createdJob(t, "fm-task-1")
	tcForbidden04AssertPod(t, "policy-rendered Job", rendered.Spec.Template.Spec)
	renderedAgent := tcJob09Container(t, rendered.Spec.Template.Spec.Containers, "agent")
	if renderedAgent.SecurityContext == nil || renderedAgent.SecurityContext.Privileged != nil {
		t.Errorf("the policy render set privileged: %+v", renderedAgent.SecurityContext)
	}
	if renderedAgent.SecurityContext.Capabilities == nil ||
		!tcCapabilitySlicesEqual(renderedAgent.SecurityContext.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Errorf("the policy render changed the drop set: %+v", renderedAgent.SecurityContext.Capabilities)
	}
	if len(renderedAgent.SecurityContext.Capabilities.Add) != 1 ||
		renderedAgent.SecurityContext.Capabilities.Add[0] != "SYS_ADMIN" {
		t.Errorf("the policy hit did not append its capability: %v", renderedAgent.SecurityContext.Capabilities.Add)
	}
}
