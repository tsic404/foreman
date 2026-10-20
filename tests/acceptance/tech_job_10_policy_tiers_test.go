// TC-tech-job-10 implements AC-41: the policy table's four tiers, the three
// axes' mutual independence, the miss shapes (unlisted agent / axis field
// absent / no usable definition) and the exact-equality keyspace. The design
// case's three minimal controls stand in for the 4x4x4 grid; the keyspace
// negatives kill both case/prefix normalization and UUID matching. The
// implementation drives the real builder through the acceptance harness, so
// the assertions read the rendered Job and the policy.applied event the
// production path emits.
package acceptance

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/scheduler"
)

// The reference template's containers[0] and pod nodeSelector, the values
// every miss must keep.
var (
	tcPolicyAxisTemplateRequests = map[string]string{"cpu": "1", "memory": "2Gi"}
	tcPolicyAxisTemplateLimits   = map[string]string{"cpu": "4", "memory": "8Gi"}
)

// tcPolicyAxisUUID is the UUID-shaped key the keyspace negatives use: a
// platform-side identifier the policy table must never match.
const tcPolicyAxisUUID = "3f0c1b02-9a4d-4a5e-8c7b-1f2e3d4c5b6a"

// TestJob10AxisTiersAreIndependent is AC-41's minimal three-control set: each
// control declares one axis and leaves the other two at None, so the control
// proves both that the declared axis is written and that the other two keep
// the template's values.
func TestJob10AxisTiersAreIndependent(t *testing.T) {
	t.Run("capability axis only", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  omp:
    capPolicy: projectWithAgent
    capabilities:
      add: ["SYS_ADMIN"]
projects:
  proj-1:
    capabilities:
      add: ["NET_ADMIN"]
`)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
		h.rebuildScheduler(t, func(c *scheduler.Config) { c.Project = tcPolicyAxisProjectIdentity{} })
		jobName := tcPolicyAxisClaim(t, h, "task-1", "omp", "proj-1")
		job := h.createdJob(t, jobName)
		pod := job.Spec.Template.Spec

		// The capability axis is the only face written.
		_, add := tcPolicyAxisCapabilities(job)
		tcPolicyAxisWantSet(t, "containers[0].securityContext.capabilities.add", add, "SYS_ADMIN", "NET_ADMIN")
		tcPolicyAxisWantResources(t, pod.Containers[0].Resources, tcPolicyAxisTemplateRequests, tcPolicyAxisTemplateLimits)
		tcPolicyAxisWantNodeSelector(t, pod, map[string]string{"kubernetes.io/os": "linux"})

		ev := tcPolicyAxisEvent(t, h, "task-1")
		tcPolicyAxisWantField(t, ev, "resource_policy", "none")
		tcPolicyAxisWantField(t, ev, "cap_policy", "project_with_agent")
		tcPolicyAxisWantField(t, ev, "node_selector_policy", "none")
		tcPolicyAxisWantField(t, ev, "resource_source", "no_definition")
		tcPolicyAxisWantField(t, ev, "cap_source", "direct")
		tcPolicyAxisWantField(t, ev, "node_selector_source", "no_definition")
		tcPolicyAxisWantList(t, ev, "template_written_faces", "capabilities_add")
	})

	t.Run("resource axis only", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  omp:
    resourcePolicy: projectWithAgent
    resources:
      requests: { cpu: "1" }
projects:
  proj-1:
    resources:
      requests: { memory: 1Gi }
`)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
		h.rebuildScheduler(t, func(c *scheduler.Config) { c.Project = tcPolicyAxisProjectIdentity{} })
		jobName := tcPolicyAxisClaim(t, h, "task-1", "omp", "proj-1")
		job := h.createdJob(t, jobName)
		pod := job.Spec.Template.Spec

		// The resource axis is the whole block, and it is the only face.
		tcPolicyAxisWantResources(t, pod.Containers[0].Resources,
			map[string]string{"cpu": "1", "memory": "1Gi"}, nil)
		tcPolicyAxisWantSet(t, "containers[0].securityContext.capabilities.drop", tcPolicyAxisDrop(job), "ALL")
		if add := tcPolicyAxisAdd(job); len(add) != 0 {
			t.Errorf("containers[0] capabilities.add = %v, want none (the template has none)", add)
		}
		tcPolicyAxisWantNodeSelector(t, pod, map[string]string{"kubernetes.io/os": "linux"})

		ev := tcPolicyAxisEvent(t, h, "task-1")
		tcPolicyAxisWantField(t, ev, "resource_policy", "project_with_agent")
		tcPolicyAxisWantField(t, ev, "cap_policy", "none")
		tcPolicyAxisWantField(t, ev, "node_selector_policy", "none")
		tcPolicyAxisWantField(t, ev, "resource_source", "direct")
		tcPolicyAxisWantField(t, ev, "cap_source", "no_definition")
		tcPolicyAxisWantField(t, ev, "node_selector_source", "no_definition")
		tcPolicyAxisWantList(t, ev, "template_written_faces", "resources")
	})

	t.Run("nodeSelector axis only", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  omp:
    nodeSelectorPolicy: agent
    nodeSelector: { disktype: ssd }
`)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
		jobName := tcPolicyAxisClaim(t, h, "task-1", "omp", "")
		job := h.createdJob(t, jobName)
		pod := job.Spec.Template.Spec

		// The nodeSelector axis merges key by key; the containers stay the
		// template's.
		tcPolicyAxisWantNodeSelector(t, pod, map[string]string{"kubernetes.io/os": "linux", "disktype": "ssd"})
		tcPolicyAxisWantResources(t, pod.Containers[0].Resources, tcPolicyAxisTemplateRequests, tcPolicyAxisTemplateLimits)
		tcPolicyAxisWantSet(t, "containers[0].securityContext.capabilities.drop", tcPolicyAxisDrop(job), "ALL")
		if add := tcPolicyAxisAdd(job); len(add) != 0 {
			t.Errorf("containers[0] capabilities.add = %v, want none (the template has none)", add)
		}

		ev := tcPolicyAxisEvent(t, h, "task-1")
		tcPolicyAxisWantField(t, ev, "resource_policy", "none")
		tcPolicyAxisWantField(t, ev, "cap_policy", "none")
		tcPolicyAxisWantField(t, ev, "node_selector_policy", "agent")
		tcPolicyAxisWantField(t, ev, "node_selector_source", "direct")
		tcPolicyAxisWantList(t, ev, "template_written_faces", "node_selector")
		// A hard constraint wins outright over the soft affinity item
		// (ADR-036): the cause is the policy, not saturation.
		tcPolicyAxisWantField(t, ev, "soft_affinity_suppressed", "policy")
	})
}

// TestJob10MissKeepsTemplateAndConfiguredTier covers AC-41's miss shapes: an
// unlisted agent reads every axis None, and a listed row whose axis has no
// usable definition keeps the template's value while still recording the
// configured tier — a miss never rewrites the tier to none.
func TestJob10MissKeepsTemplateAndConfiguredTier(t *testing.T) {
	t.Run("unlisted agent", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  other:
    resourcePolicy: agent
    resources:
      requests: { cpu: "9" }
`)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
		jobName := tcPolicyAxisClaim(t, h, "task-1", "omp", "")
		job := h.createdJob(t, jobName)
		pod := job.Spec.Template.Spec

		tcPolicyAxisWantResources(t, pod.Containers[0].Resources, tcPolicyAxisTemplateRequests, tcPolicyAxisTemplateLimits)
		tcPolicyAxisWantSet(t, "containers[0].securityContext.capabilities.drop", tcPolicyAxisDrop(job), "ALL")
		tcPolicyAxisWantNodeSelector(t, pod, map[string]string{"kubernetes.io/os": "linux"})

		ev := tcPolicyAxisEvent(t, h, "task-1")
		tcPolicyAxisWantField(t, ev, "resource_policy", "none")
		tcPolicyAxisWantField(t, ev, "cap_policy", "none")
		tcPolicyAxisWantField(t, ev, "node_selector_policy", "none")
		tcPolicyAxisWantField(t, ev, "resource_source", "no_definition")
		tcPolicyAxisWantField(t, ev, "cap_source", "no_definition")
		tcPolicyAxisWantField(t, ev, "node_selector_source", "no_definition")
		tcPolicyAxisWantList(t, ev, "template_written_faces")
	})

	t.Run("row present, axis field absent", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  omp:
    nodeSelector: { disktype: ssd }
`)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
		jobName := tcPolicyAxisClaim(t, h, "task-1", "omp", "")
		job := h.createdJob(t, jobName)

		// The definition is present but no tier references it: every axis
		// reads None and the template survives verbatim.
		tcPolicyAxisWantResources(t, job.Spec.Template.Spec.Containers[0].Resources,
			tcPolicyAxisTemplateRequests, tcPolicyAxisTemplateLimits)
		tcPolicyAxisWantNodeSelector(t, job.Spec.Template.Spec, map[string]string{"kubernetes.io/os": "linux"})

		ev := tcPolicyAxisEvent(t, h, "task-1")
		tcPolicyAxisWantField(t, ev, "node_selector_policy", "none")
		tcPolicyAxisWantField(t, ev, "node_selector_source", "no_definition")
		tcPolicyAxisWantList(t, ev, "template_written_faces")
	})

	t.Run("tiers configured without definitions", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  omp:
    resourcePolicy: agent
    capPolicy: projectWithAgent
    nodeSelectorPolicy: project
`)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
		h.rebuildScheduler(t, func(c *scheduler.Config) { c.Project = tcPolicyAxisProjectIdentity{} })
		// No definition on either side: the axis does not inject, but the tier
		// it declares is what the event records — the miss is silent and never
		// fails the task (ADR-035 decision 5).
		job := h.createdJob(t, tcPolicyAxisClaim(t, h, "task-1", "omp", "proj-1"))
		pod := job.Spec.Template.Spec

		// No definition on either side: the axis does not inject, but the
		// tier it declares is what the event records.
		tcPolicyAxisWantResources(t, pod.Containers[0].Resources, tcPolicyAxisTemplateRequests, tcPolicyAxisTemplateLimits)
		tcPolicyAxisWantSet(t, "containers[0].securityContext.capabilities.drop", tcPolicyAxisDrop(job), "ALL")
		tcPolicyAxisWantNodeSelector(t, pod, map[string]string{"kubernetes.io/os": "linux"})

		ev := tcPolicyAxisEvent(t, h, "task-1")
		tcPolicyAxisWantField(t, ev, "resource_policy", "agent")
		tcPolicyAxisWantField(t, ev, "cap_policy", "project_with_agent")
		tcPolicyAxisWantField(t, ev, "node_selector_policy", "project")
		tcPolicyAxisWantField(t, ev, "resource_source", "no_definition")
		tcPolicyAxisWantField(t, ev, "cap_source", "no_definition")
		tcPolicyAxisWantField(t, ev, "node_selector_source", "no_definition")
		tcPolicyAxisWantList(t, ev, "template_written_faces")
	})
}

// TestJob10KeysAreExactNames is AC-41's keyspace negative: the table's keys are
// agent/project names compared by exact equality, so a case or prefix variant
// and a UUID-shaped key never hit — and when the platform's authority is
// reachable and answers absent, the same keys refuse startup instead of
// degrading into a permanent silent miss.
func TestJob10KeysAreExactNames(t *testing.T) {
	t.Run("variant and uuid keys never hit at render time", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  OMP:
    capPolicy: agent
    capabilities:
      add: ["SYS_ADMIN"]
  omp-gpu:
    capPolicy: agent
    capabilities:
      add: ["SYS_ADMIN"]
  `+tcPolicyAxisUUID+`:
    capPolicy: agent
    capabilities:
      add: ["SYS_ADMIN"]
`)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, func(cfg *jobbuilder.Config) {
			// The authority answers resolved for every key: the startup gate
			// passes, so what the render does is the only thing left to judge.
			cfg.NameResolver = tcPolicyAxisResolvedResolver{}
		})
		// The payload's agent is named "omp" and its agent id is the UUID key:
		// neither the case variant, nor the prefix variant, nor a UUID match
		// may reach the definition (AC-41: 键比较 = 全等).
		payload, err := json.Marshal(map[string]any{
			"id": "task-1", "agent_id": tcPolicyAxisUUID, "workspace_id": "ws-1",
			"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1",
			"agent": map[string]any{"name": "omp"},
		})
		if err != nil {
			t.Fatalf("marshal claim: %v", err)
		}
		tcPolicyAxisClaimRaw(t, h, payload)
		job := h.createdJob(t, "fm-task-1")

		// The authority was reachable and resolved every key at startup, so the
		// render's non-hit is the exact-name comparison itself — not a
		// degradation that hid the miss.
		resolutions := h.events("policy.name_resolution")
		if len(resolutions) != 3 {
			t.Errorf("policy.name_resolution events = %d, want one per key (OMP, omp-gpu, uuid)", len(resolutions))
		}
		for _, resolution := range resolutions {
			if conclusion, _ := resolution["conclusion"].(string); conclusion != "resolved" {
				t.Errorf("policy.name_resolution %v = %q, want resolved", resolution["name"], conclusion)
			}
		}

		_, add := tcPolicyAxisCapabilities(job)
		if len(add) != 0 {
			t.Errorf("containers[0].securityContext.capabilities.add = %v, want none: no key matches exactly", add)
		}
		tcPolicyAxisWantResources(t, job.Spec.Template.Spec.Containers[0].Resources,
			tcPolicyAxisTemplateRequests, tcPolicyAxisTemplateLimits)

		ev := tcPolicyAxisEvent(t, h, "task-1")
		// The agent's name is unlisted (only the variants and the UUID are
		// keys), so the axis reads None — but the definition is still not
		// reached, which is what the render shows.
		tcPolicyAxisWantField(t, ev, "cap_policy", "none")
		tcPolicyAxisWantField(t, ev, "cap_source", "no_definition")
		tcPolicyAxisWantList(t, ev, "template_written_faces")
	})

	t.Run("authority answering absent refuses startup", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  OMP:
    capPolicy: agent
    capabilities:
      add: ["SYS_ADMIN"]
  `+tcPolicyAxisUUID+`:
    capPolicy: agent
    capabilities:
      add: ["SYS_ADMIN"]
`)
		cfg, logs := tcPolicyAxisConfig(t, deployedTemplatePath(t), policyPath, func(cfg *jobbuilder.Config) {
			// The platform is reachable and its authority answers "no such
			// name" for every key: a configuration error, not a degradation.
			cfg.NameResolver = tcPolicyAxisAbsentResolver{}
		})

		builder, err := jobbuilder.NewBuilder(cfg)
		if err == nil {
			t.Fatalf("NewBuilder accepted keys the authority reports absent: %+v", builder)
		}
		if !errors.Is(err, jobbuilder.ErrInvalidPolicy) {
			t.Errorf("NewBuilder error = %v, want ErrInvalidPolicy", err)
		}
		log := logs.String()
		for _, want := range []string{"policy.name_resolution", string(jobbuilder.NameAbsent), "OMP", tcPolicyAxisUUID} {
			if !strings.Contains(log, want) {
				t.Errorf("the refusal log must carry %q: %s", want, log)
			}
		}
	})
}

// --- helpers shared by TC-tech-job-10…14 ---

// tcPolicyAxisProjectIdentity is the platform-side project-identity seam
// (ADR-048): the claim payload's top-level project_id carries the platform
// identifier, and the read-only lookup knows nothing — an empty answer is
// "none", never an error.
type tcPolicyAxisProjectIdentity struct{}

func (tcPolicyAxisProjectIdentity) FromPayload(task json.RawMessage) string {
	var head struct {
		ProjectID string `json:"project_id"`
	}
	if err := json.Unmarshal(task, &head); err != nil {
		return ""
	}
	return strings.TrimSpace(head.ProjectID)
}

func (tcPolicyAxisProjectIdentity) ByIssue(context.Context, string) (string, error) {
	return "", nil
}

// tcPolicyAxisAbsentResolver is a reachable authority that answers "no such
// name" for every key.
type tcPolicyAxisAbsentResolver struct{}

func (tcPolicyAxisAbsentResolver) ResolveName(context.Context, string, string) (jobbuilder.NameResolution, error) {
	return jobbuilder.NameResolution{Conclusion: jobbuilder.NameAbsent}, nil
}

// tcPolicyAxisResolvedResolver is a reachable authority that resolves every
// key; the runtime comparison is still the exact-name one.
type tcPolicyAxisResolvedResolver struct{}

// ResolveName resolves every key; a project key answers the name itself as its
// platform identifier, which keeps the runtime index addressable from a claim
// carrying the same identifier.
func (tcPolicyAxisResolvedResolver) ResolveName(_ context.Context, kind, name string) (jobbuilder.NameResolution, error) {
	identifier := ""
	if kind == jobbuilder.PolicyKeyProject {
		identifier = projectIDFor(name)
	}
	return jobbuilder.NameResolution{Conclusion: jobbuilder.NameResolved, Identifier: identifier}, nil
}

// tcPolicyAxisClaimRaw hands the scheduler one claim payload and declares the
// discovery the stub upstream answers.
func tcPolicyAxisClaimRaw(t *testing.T, h *harness, payload []byte) {
	t.Helper()
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	if err := h.sched.OnClaim(t.Context(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
}

// tcPolicyAxisClaim hands the scheduler one claim carrying the two policy
// identity inputs, and returns the Job name the reference template renders.
func tcPolicyAxisClaim(t *testing.T, h *harness, taskID, agentName, projectName string) string {
	t.Helper()
	payload := map[string]any{
		"id": taskID, "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1",
		"agent": map[string]any{"name": agentName},
	}
	if projectName != "" {
		payload["project_id"] = projectIDFor(projectName)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal claim: %v", err)
	}
	tcPolicyAxisClaimRaw(t, h, raw)
	return "fm-" + taskID
}

// tcPolicyAxisEvent returns the policy.applied event of one task.
func tcPolicyAxisEvent(t *testing.T, h *harness, taskID string) map[string]any {
	t.Helper()
	for _, ev := range h.events("policy.applied") {
		if id, _ := ev["task_id"].(string); id == taskID {
			return ev
		}
	}
	t.Fatalf("no policy.applied event for task %q: %v", taskID, h.events("policy.applied"))
	return nil
}

// tcPolicyAxisField reads one string field of an event.
func tcPolicyAxisField(t *testing.T, ev map[string]any, field string) string {
	t.Helper()
	raw, ok := ev[field]
	if !ok {
		t.Fatalf("policy.applied has no field %q: %v", field, ev)
	}
	value, _ := raw.(string)
	return value
}

// tcPolicyAxisWantField asserts one string field of an event.
func tcPolicyAxisWantField(t *testing.T, ev map[string]any, field, want string) {
	t.Helper()
	if got := tcPolicyAxisField(t, ev, field); got != want {
		t.Errorf("policy.applied %s = %q, want %q", field, got, want)
	}
}

// tcPolicyAxisList reads one list field of an event; a null field is an empty
// list, not a missing one.
func tcPolicyAxisList(t *testing.T, ev map[string]any, field string) []string {
	t.Helper()
	raw, ok := ev[field]
	if !ok {
		t.Fatalf("policy.applied has no field %q: %v", field, ev)
	}
	if raw == nil {
		return nil
	}
	items, ok := raw.([]any)
	if !ok {
		t.Fatalf("policy.applied %s = %v, want a list", field, raw)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			t.Fatalf("policy.applied %s carries %v, want strings", field, item)
		}
		out = append(out, text)
	}
	return out
}

// tcPolicyAxisWantList asserts one list field of an event, element by element
// in order (the faces have a documented order).
func tcPolicyAxisWantList(t *testing.T, ev map[string]any, field string, want ...string) {
	t.Helper()
	got := tcPolicyAxisList(t, ev, field)
	if len(got) != len(want) {
		t.Errorf("policy.applied %s = %v, want %v", field, got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("policy.applied %s = %v, want %v", field, got, want)
			return
		}
	}
}

// tcPolicyAxisSet turns a list into a set (AC-43 judges the capability axis as
// a set; order is not part of any assertion).
func tcPolicyAxisSet(list []string) map[string]bool {
	set := make(map[string]bool, len(list))
	for _, item := range list {
		set[item] = true
	}
	return set
}

// tcPolicyAxisWantSet asserts set equality between a got list and a want list.
func tcPolicyAxisWantSet(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	gotSet, wantSet := tcPolicyAxisSet(got), tcPolicyAxisSet(want)
	for item := range wantSet {
		if !gotSet[item] {
			t.Errorf("%s = %v, want the set %v (missing %q)", what, got, want, item)
			return
		}
	}
	for item := range gotSet {
		if !wantSet[item] {
			t.Errorf("%s = %v, want the set %v (unexpected %q)", what, got, want, item)
			return
		}
	}
}

// tcPolicyAxisDrop returns containers[0]'s capability drop list.
func tcPolicyAxisDrop(job *batchv1.Job) []string {
	if len(job.Spec.Template.Spec.Containers) == 0 ||
		job.Spec.Template.Spec.Containers[0].SecurityContext == nil ||
		job.Spec.Template.Spec.Containers[0].SecurityContext.Capabilities == nil {
		return nil
	}
	raw := job.Spec.Template.Spec.Containers[0].SecurityContext.Capabilities.Drop
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		out = append(out, string(item))
	}
	return out
}

// tcPolicyAxisAdd returns containers[0]'s capability add list.
func tcPolicyAxisAdd(job *batchv1.Job) []string {
	if len(job.Spec.Template.Spec.Containers) == 0 ||
		job.Spec.Template.Spec.Containers[0].SecurityContext == nil ||
		job.Spec.Template.Spec.Containers[0].SecurityContext.Capabilities == nil {
		return nil
	}
	raw := job.Spec.Template.Spec.Containers[0].SecurityContext.Capabilities.Add
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		out = append(out, string(item))
	}
	return out
}

// tcPolicyAxisCapabilities returns containers[0]'s two capability lists.
func tcPolicyAxisCapabilities(job *batchv1.Job) (drop []string, add []string) {
	return tcPolicyAxisDrop(job), tcPolicyAxisAdd(job)
}

// tcPolicyAxisRequirements renders one expected resource block.
func tcPolicyAxisRequirements(requests, limits map[string]string) corev1.ResourceRequirements {
	want := corev1.ResourceRequirements{}
	if len(requests) > 0 {
		want.Requests = corev1.ResourceList{}
		for key, value := range requests {
			want.Requests[corev1.ResourceName(key)] = resource.MustParse(value)
		}
	}
	if len(limits) > 0 {
		want.Limits = corev1.ResourceList{}
		for key, value := range limits {
			want.Limits[corev1.ResourceName(key)] = resource.MustParse(value)
		}
	}
	return want
}

// tcPolicyAxisWantResources asserts containers[0].resources equals one
// definition verbatim: the axis is one replacement block, so a side the
// definition omits must not survive from the template.
func tcPolicyAxisWantResources(t *testing.T, got corev1.ResourceRequirements, requests, limits map[string]string) {
	t.Helper()
	want := tcPolicyAxisRequirements(requests, limits)
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal resources: %v", err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal resources: %v", err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Errorf("containers[0].resources = %s, want %s", gotJSON, wantJSON)
	}
}

// tcPolicyAxisWantNodeSelector asserts the pod template's nodeSelector exactly:
// the axis merges key by key, so every other key must survive.
func tcPolicyAxisWantNodeSelector(t *testing.T, pod corev1.PodSpec, want map[string]string) {
	t.Helper()
	if len(pod.NodeSelector) != len(want) {
		t.Errorf("pod nodeSelector = %v, want %v", pod.NodeSelector, want)
		return
	}
	for key, value := range want {
		if pod.NodeSelector[key] != value {
			t.Errorf("pod nodeSelector = %v, want %v", pod.NodeSelector, want)
			return
		}
	}
}
