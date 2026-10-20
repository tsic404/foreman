// TC-tech-job-11 implements AC-42: the resource axis's whole-block
// replacement, the projectWithAgent merge (agent wins on a shared key, with
// the overridden keys as the refutable face), the empty/absent definition
// misses, and the priority rule that identity unavailability outranks a
// single-side definition even when the agent's side is present. The render
// layer is driven through the harness's real builder, so each case hands the
// TaskEntry the identity inputs the design case names.
package acceptance

import (
	"testing"

	batchv1 "k8s.io/api/batch/v1"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// tcPolicyAxisBuildJob renders one task through the harness's real builder
// without going through a claim: the identity inputs are the scenario's, so a
// case can construct "identity unavailable" the way the design case does.
func tcPolicyAxisBuildJob(t *testing.T, h *harness, e jobbuilder.TaskEntry) *batchv1.Job {
	t.Helper()
	if e.TaskID == "" {
		e.TaskID = "task-1"
	}
	if e.AgentID == "" {
		e.AgentID = "agent-1"
	}
	if e.WorkspaceID == "" {
		e.WorkspaceID = "ws-1"
	}
	if e.JobRuntimeID == "" {
		e.JobRuntimeID = "rid-1"
	}
	if e.ClaimedAt.IsZero() {
		e.ClaimedAt = testClaimTime
	}
	if e.Attempt == 0 {
		e.Attempt = 1
	}
	job, _, _, err := h.builder.Build(e)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return job
}

// TestJob11AgentTierWritesDefinitionVerbatim is AC-42's whole-block
// replacement: the definition is one block, so the side it omits must not
// survive from the template. (The design case's one-way k8s default — a
// limits-only definition defaulting requests to limits — is the apiserver's
// admission default; there is no apiserver here, so the block is asserted
// verbatim.)
func TestJob11AgentTierWritesDefinitionVerbatim(t *testing.T) {
	cases := []struct {
		name           string
		definition     string
		wantRequests   map[string]string
		wantLimits     map[string]string
		overriddenKeys []string
	}{
		{
			name:         "requests only",
			definition:   "      requests: { cpu: 250m, memory: 512Mi }\n",
			wantRequests: map[string]string{"cpu": "250m", "memory": "512Mi"},
		},
		{
			name:       "limits only",
			definition: "      limits: { cpu: \"2\", memory: 1Gi }\n",
			wantLimits: map[string]string{"cpu": "2", "memory": "1Gi"},
		},
		{
			name:         "both sides",
			definition:   "      requests: { cpu: 250m }\n      limits: { cpu: \"2\" }\n",
			wantRequests: map[string]string{"cpu": "250m"},
			wantLimits:   map[string]string{"cpu": "2"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policyPath := writePolicyFile(t, "agents:\n  omp:\n    resourcePolicy: agent\n    resources:\n"+tc.definition)
			h := newHarness(t, false)
			h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
			job := tcPolicyAxisBuildJob(t, h, jobbuilder.TaskEntry{TaskID: "task-1", AgentName: "omp"})
			container := job.Spec.Template.Spec.Containers[0]

			tcPolicyAxisWantResources(t, container.Resources, tc.wantRequests, tc.wantLimits)

			ev := tcPolicyAxisEvent(t, h, "task-1")
			tcPolicyAxisWantField(t, ev, "resource_policy", "agent")
			tcPolicyAxisWantField(t, ev, "resource_source", "direct")
			tcPolicyAxisWantList(t, ev, "overridden_keys", tc.overriddenKeys...)
			tcPolicyAxisWantList(t, ev, "template_written_faces", "resources")
		})
	}
}

// TestJob11ProjectWithAgentMergePrefersAgentOnSharedKey is AC-42's merge: the
// two definitions merge key by key, a shared key takes the agent's value, a
// key only one side gives survives, and every shared key is named in
// overridden_keys — which makes the merge refutable against the project's
// values.
func TestJob11ProjectWithAgentMergePrefersAgentOnSharedKey(t *testing.T) {
	policyPath := writePolicyFile(t, `
agents:
  omp:
    resourcePolicy: projectWithAgent
    resources:
      requests: { cpu: "1" }
      limits:   { cpu: "8", memory: 4Gi }
projects:
  proj-1:
    resources:
      requests: { cpu: 500m, memory: 1Gi }
      limits:   { cpu: "2" }
`)
	h := newHarness(t, false)
	h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
	job := tcPolicyAxisBuildJob(t, h, jobbuilder.TaskEntry{
		TaskID: "task-1", AgentName: "omp", ProjectID: projectIDFor("proj-1"), ProjectSource: jobbuilder.ProjectSourcePayload,
	})

	// The shared keys take the agent's side (cpu 1, not 500m; limits cpu 8,
	// not 2) and the project-only key (memory requests) survives.
	tcPolicyAxisWantResources(t, job.Spec.Template.Spec.Containers[0].Resources,
		map[string]string{"cpu": "1", "memory": "1Gi"},
		map[string]string{"cpu": "8", "memory": "4Gi"})

	ev := tcPolicyAxisEvent(t, h, "task-1")
	tcPolicyAxisWantField(t, ev, "resource_policy", "project_with_agent")
	tcPolicyAxisWantField(t, ev, "resource_source", "direct")
	tcPolicyAxisWantField(t, ev, "project_source", "payload")
	tcPolicyAxisWantList(t, ev, "overridden_keys", "resources.limits:cpu", "resources.requests:cpu")
	tcPolicyAxisWantList(t, ev, "template_written_faces", "resources")
}

// TestJob11EmptyOrMissingDefinitionKeepsTemplate is AC-42's miss set: an empty
// definition body and a single tier whose side has no definition both read
// no_definition and leave the template's block in place.
func TestJob11EmptyOrMissingDefinitionKeepsTemplate(t *testing.T) {
	t.Run("empty definition body", func(t *testing.T) {
		policyPath := writePolicyFile(t, "agents:\n  omp:\n    resourcePolicy: agent\n    resources: {}\n")
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
		job := tcPolicyAxisBuildJob(t, h, jobbuilder.TaskEntry{TaskID: "task-1", AgentName: "omp"})

		tcPolicyAxisWantResources(t, job.Spec.Template.Spec.Containers[0].Resources,
			tcPolicyAxisTemplateRequests, tcPolicyAxisTemplateLimits)
		ev := tcPolicyAxisEvent(t, h, "task-1")
		tcPolicyAxisWantField(t, ev, "resource_policy", "agent")
		tcPolicyAxisWantField(t, ev, "resource_source", "no_definition")
		tcPolicyAxisWantList(t, ev, "template_written_faces")
	})

	t.Run("project tier without a project definition", func(t *testing.T) {
		policyPath := writePolicyFile(t, "agents:\n  omp:\n    resourcePolicy: project\n")
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
		job := tcPolicyAxisBuildJob(t, h, jobbuilder.TaskEntry{
			TaskID: "task-1", AgentName: "omp", ProjectID: projectIDFor("proj-1"), ProjectSource: jobbuilder.ProjectSourcePayload,
		})

		tcPolicyAxisWantResources(t, job.Spec.Template.Spec.Containers[0].Resources,
			tcPolicyAxisTemplateRequests, tcPolicyAxisTemplateLimits)
		ev := tcPolicyAxisEvent(t, h, "task-1")
		tcPolicyAxisWantField(t, ev, "resource_policy", "project")
		tcPolicyAxisWantField(t, ev, "resource_source", "no_definition")
		tcPolicyAxisWantList(t, ev, "template_written_faces")
	})

	t.Run("projectWithAgent with no definition on either side", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  omp:
    resourcePolicy: projectWithAgent
projects:
  proj-1:
    capabilities:
      add: ["NET_ADMIN"]
`)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
		job := tcPolicyAxisBuildJob(t, h, jobbuilder.TaskEntry{
			TaskID: "task-1", AgentName: "omp", ProjectID: projectIDFor("proj-1"), ProjectSource: jobbuilder.ProjectSourcePayload,
		})

		tcPolicyAxisWantResources(t, job.Spec.Template.Spec.Containers[0].Resources,
			tcPolicyAxisTemplateRequests, tcPolicyAxisTemplateLimits)
		ev := tcPolicyAxisEvent(t, h, "task-1")
		tcPolicyAxisWantField(t, ev, "resource_policy", "project_with_agent")
		tcPolicyAxisWantField(t, ev, "resource_source", "no_definition")
		tcPolicyAxisWantList(t, ev, "template_written_faces")
	})
}

// TestJob11ProjectWithAgentSingleSideIsDirect is AC-42's one-sided form with
// the identity available: the present side is written as direct, and the axis
// never falls back to a different tier's definition.
func TestJob11ProjectWithAgentSingleSideIsDirect(t *testing.T) {
	t.Run("project side only", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  omp:
    resourcePolicy: projectWithAgent
projects:
  proj-1:
    resources:
      requests: { memory: 1Gi }
`)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
		job := tcPolicyAxisBuildJob(t, h, jobbuilder.TaskEntry{
			TaskID: "task-1", AgentName: "omp", ProjectID: projectIDFor("proj-1"), ProjectSource: jobbuilder.ProjectSourcePayload,
		})

		tcPolicyAxisWantResources(t, job.Spec.Template.Spec.Containers[0].Resources,
			map[string]string{"memory": "1Gi"}, nil)
		ev := tcPolicyAxisEvent(t, h, "task-1")
		tcPolicyAxisWantField(t, ev, "resource_source", "direct")
		tcPolicyAxisWantList(t, ev, "overridden_keys")
		tcPolicyAxisWantList(t, ev, "template_written_faces", "resources")
	})

	t.Run("agent side only", func(t *testing.T) {
		policyPath := writePolicyFile(t, `
agents:
  omp:
    resourcePolicy: projectWithAgent
    resources:
      limits: { cpu: "6" }
projects:
  proj-1:
    capabilities:
      add: ["NET_ADMIN"]
`)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
		job := tcPolicyAxisBuildJob(t, h, jobbuilder.TaskEntry{
			TaskID: "task-1", AgentName: "omp", ProjectID: projectIDFor("proj-1"), ProjectSource: jobbuilder.ProjectSourcePayload,
		})

		tcPolicyAxisWantResources(t, job.Spec.Template.Spec.Containers[0].Resources,
			nil, map[string]string{"cpu": "6"})
		ev := tcPolicyAxisEvent(t, h, "task-1")
		tcPolicyAxisWantField(t, ev, "resource_source", "direct")
		tcPolicyAxisWantList(t, ev, "overridden_keys")
		tcPolicyAxisWantList(t, ev, "template_written_faces", "resources")
	})
}

// TestJob11NoIdentityOutranksTheAgentSide is AC-42's priority rule: without the
// project identity the projectWithAgent axis degrades to no_identity and does
// not inject, even though the agent's own definition is present.
func TestJob11NoIdentityOutranksTheAgentSide(t *testing.T) {
	policyPath := writePolicyFile(t, `
agents:
  omp:
    resourcePolicy: projectWithAgent
    resources:
      requests: { cpu: "3" }
projects:
  proj-1:
    resources:
      requests: { cpu: 500m }
`)
	h := newHarness(t, false)
	h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
	job := tcPolicyAxisBuildJob(t, h, jobbuilder.TaskEntry{TaskID: "task-1", AgentName: "omp"})

	tcPolicyAxisWantResources(t, job.Spec.Template.Spec.Containers[0].Resources,
		tcPolicyAxisTemplateRequests, tcPolicyAxisTemplateLimits)
	ev := tcPolicyAxisEvent(t, h, "task-1")
	tcPolicyAxisWantField(t, ev, "resource_policy", "project_with_agent")
	tcPolicyAxisWantField(t, ev, "resource_source", "no_identity")
	tcPolicyAxisWantField(t, ev, "project_source", "none")
	tcPolicyAxisWantList(t, ev, "template_written_faces")
}
