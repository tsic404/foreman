// TC-tech-job-12 implements AC-43's implementation-layer face: the capability
// axis appends the definition's `add` list to the container's own list, keeps
// `drop` and every other securityContext field verbatim, unions and dedupes the
// projectWithAgent pair (set equality only — the result's order is not part of
// the assertion), and leaves the container untouched on None or a miss.
// AC-43's "requirement achieved" half (a real mount inside the container)
// needs a cluster and stays QA-side.
package acceptance

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// tcJob12Template is a template whose containers[0] already carries an add
// entry, so the append half of AC-43 is observable: the policy adds to the
// template's list, it never replaces it.
const tcJob12Template = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
spec:
  template:
    spec:
      restartPolicy: Never
      nodeSelector: { kubernetes.io/os: linux }
      containers:
        - name: agent
          image: <job-image>
          securityContext:
            runAsUser: 1000
            runAsNonRoot: true
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: ["ALL"], add: ["CHOWN"] }
`

// TestJob12HitAppendsAndKeepsDropVerbatim is AC-43's hit form: the definition's
// capabilities join the template's own add list, and the drop list plus every
// other securityContext field stay exactly the template's.
func TestJob12HitAppendsAndKeepsDropVerbatim(t *testing.T) {
	templatePath := writeTemplateFile(t, tcJob12Template)
	h := newHarness(t, false)

	// The policy-free render is the byte-for-byte reference the hit is
	// measured against.
	h.useRealBuilderWithPolicy(t, templatePath, "", nil)
	baseline := tcPolicyAxisBuildJob(t, h, jobbuilder.TaskEntry{TaskID: "task-base", AgentName: "omp"})

	policyPath := writePolicyFile(t, `
agents:
  omp:
    capPolicy: agent
    capabilities:
      add: ["NET_ADMIN", "SYS_ADMIN"]
`)
	h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
	job := tcPolicyAxisBuildJob(t, h, jobbuilder.TaskEntry{TaskID: "task-hit", AgentName: "omp"})

	baseContainer := baseline.Spec.Template.Spec.Containers[0]
	container := job.Spec.Template.Spec.Containers[0]

	// The definition's set joins the template's entry; no entry is lost or
	// duplicated.
	_, add := tcPolicyAxisCapabilities(job)
	tcPolicyAxisWantSet(t, "containers[0].securityContext.capabilities.add", add, "CHOWN", "NET_ADMIN", "SYS_ADMIN")
	if len(add) != 3 {
		t.Errorf("capabilities.add = %v, want three entries (the template's plus the two added, deduped)", add)
	}

	// drop and the rest of securityContext are byte-for-byte the template's.
	tcJob12WantSecurityContextUnchanged(t, container.SecurityContext, baseContainer.SecurityContext)

	ev := tcPolicyAxisEvent(t, h, "task-hit")
	tcPolicyAxisWantField(t, ev, "cap_policy", "agent")
	tcPolicyAxisWantField(t, ev, "cap_source", "direct")
	tcPolicyAxisWantList(t, ev, "template_written_faces", "capabilities_add")
}

// TestJob12ProjectWithAgentUnionsAndDedupes is AC-43's merge: the two add
// lists become one set — an entry both sides name appears once — and the drop
// list still never participates.
func TestJob12ProjectWithAgentUnionsAndDedupes(t *testing.T) {
	policyPath := writePolicyFile(t, `
agents:
  omp:
    capPolicy: projectWithAgent
    capabilities:
      add: ["SYS_ADMIN", "CHOWN"]
projects:
  proj-1:
    capabilities:
      add: ["NET_ADMIN", "SYS_ADMIN"]
`)
	h := newHarness(t, false)
	h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
	h.rebuildScheduler(t, func(c *schedulerConfig) { c.Project = tcPolicyAxisProjectIdentity{} })
	jobName := tcPolicyAxisClaim(t, h, "task-1", "omp", "proj-1")
	job := h.createdJob(t, jobName)

	// Set equality is the judgement (order is not asserted); the length check
	// is the dedupe face.
	_, add := tcPolicyAxisCapabilities(job)
	tcPolicyAxisWantSet(t, "containers[0].securityContext.capabilities.add", add, "NET_ADMIN", "SYS_ADMIN", "CHOWN")
	if len(add) != 3 {
		t.Errorf("capabilities.add = %v, want the three-name union with SYS_ADMIN deduped", add)
	}
	tcPolicyAxisWantSet(t, "containers[0].securityContext.capabilities.drop", tcPolicyAxisDrop(job), "ALL")

	ev := tcPolicyAxisEvent(t, h, "task-1")
	tcPolicyAxisWantField(t, ev, "cap_policy", "project_with_agent")
	tcPolicyAxisWantField(t, ev, "cap_source", "direct")
	tcPolicyAxisWantList(t, ev, "template_written_faces", "capabilities_add")
}

// TestJob12NoneOrMissLeavesCapabilitiesUntouched is AC-43's negative half: with
// the axis at None, the agent unlisted, or the definition empty, the
// container's capability surface is the template's, verbatim (the reference
// template's {drop: ["ALL"]} and no add).
func TestJob12NoneOrMissLeavesCapabilitiesUntouched(t *testing.T) {
	cases := []struct {
		name   string
		policy string
	}{
		{
			name:   "unlisted agent",
			policy: "agents:\n  other:\n    capPolicy: agent\n    capabilities: { add: [\"SYS_ADMIN\"] }\n",
		},
		{
			name:   "axis field absent",
			policy: "agents:\n  omp:\n    capabilities: { add: [\"SYS_ADMIN\"] }\n",
		},
		{
			name:   "axis at None",
			policy: "agents:\n  omp:\n    capPolicy: None\n    capabilities: { add: [\"SYS_ADMIN\"] }\n",
		},
		{
			name:   "empty definition body",
			policy: "agents:\n  omp:\n    capPolicy: agent\n    capabilities: { add: [] }\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, false)
			h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), "", nil)
			baseline := tcPolicyAxisBuildJob(t, h, jobbuilder.TaskEntry{TaskID: "task-base", AgentName: "omp"})

			policyPath := writePolicyFile(t, tc.policy)
			h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, nil)
			job := tcPolicyAxisBuildJob(t, h, jobbuilder.TaskEntry{TaskID: "task-miss", AgentName: "omp"})

			baseContainer := baseline.Spec.Template.Spec.Containers[0]
			container := job.Spec.Template.Spec.Containers[0]
			if !tcJob12SecurityContextEqual(t, container.SecurityContext, baseContainer.SecurityContext) {
				t.Errorf("containers[0].securityContext = %+v, want the template's %+v",
					container.SecurityContext, baseContainer.SecurityContext)
			}

			ev := tcPolicyAxisEvent(t, h, "task-miss")
			tcPolicyAxisWantField(t, ev, "cap_source", "no_definition")
			tcPolicyAxisWantList(t, ev, "template_written_faces")
		})
	}
}

// tcJob12WantSecurityContextUnchanged asserts that a hit changed nothing but
// the add list: drop is the template's element for element, and every other
// field is equal once add is set aside.
func tcJob12WantSecurityContextUnchanged(t *testing.T, got, base *corev1.SecurityContext) {
	t.Helper()
	if got == nil || base == nil {
		t.Fatalf("securityContext = %+v, want the template's %+v", got, base)
	}
	if got.Capabilities == nil || base.Capabilities == nil {
		t.Fatalf("capabilities = %+v, want the template's %+v", got.Capabilities, base.Capabilities)
	}
	if len(got.Capabilities.Drop) != len(base.Capabilities.Drop) {
		t.Errorf("capabilities.drop = %v, want the template's %v", got.Capabilities.Drop, base.Capabilities.Drop)
	} else {
		for i := range base.Capabilities.Drop {
			if got.Capabilities.Drop[i] != base.Capabilities.Drop[i] {
				t.Errorf("capabilities.drop = %v, want the template's %v", got.Capabilities.Drop, base.Capabilities.Drop)
			}
		}
	}
	if !tcJob12SecurityContextEqual(t, tcJob12WithoutAdd(got), tcJob12WithoutAdd(base)) {
		t.Errorf("securityContext (add aside) = %s, want the template's %s",
			tcJob12JSON(t, tcJob12WithoutAdd(got)), tcJob12JSON(t, tcJob12WithoutAdd(base)))
	}
}

// tcJob12SecurityContextEqual compares two securityContexts by their JSON
// rendering, so every field — including the ones this suite does not name —
// participates.
func tcJob12SecurityContextEqual(t *testing.T, got, want *corev1.SecurityContext) bool {
	t.Helper()
	return tcJob12JSON(t, got) == tcJob12JSON(t, want)
}

// tcJob12WithoutAdd returns a copy with the capability add list emptied.
func tcJob12WithoutAdd(sc *corev1.SecurityContext) *corev1.SecurityContext {
	if sc == nil {
		return nil
	}
	clone := sc.DeepCopy()
	if clone.Capabilities != nil {
		clone.Capabilities.Add = nil
	}
	return clone
}

// tcJob12JSON renders one securityContext as JSON.
func tcJob12JSON(t *testing.T, sc *corev1.SecurityContext) string {
	t.Helper()
	raw, err := json.Marshal(sc)
	if err != nil {
		t.Fatalf("marshal securityContext: %v", err)
	}
	return string(raw)
}
