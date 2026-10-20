// TC-tech-node-selector-01 implements AC-53 on the real jobbuilder: the
// nodeSelector axis merges key-by-key into the pod template (a shared key
// takes the policy's value, the template's other keys survive), the
// projectWithAgent tier merges the two definitions with the agent winning a
// shared key, a miss leaves the field byte-identical to the template's, and a
// hit suppresses the template's <preferred node> item with
// soft_affinity_suppressed=policy while the item without a hit still follows
// the four existing rules. The negative half: the policy never writes an
// affinity item and never introduces a required affinity.
package acceptance

import (
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

// tcNodeTemplate carries a template-written nodeSelector (one key the policy
// shares, one key it does not) and the <preferred node> recognition item.
const tcNodeTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
spec:
  template:
    spec:
      restartPolicy: Never
      nodeSelector:
        disktype: ssd
        zone: template-zone
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
`

// tcNodeNoAffinityTemplate writes no affinity at all, so "the policy never
// creates one" is observable.
const tcNodeNoAffinityTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
spec:
  template:
    spec:
      restartPolicy: Never
      nodeSelector: { disktype: ssd }
      containers:
        - name: agent
          image: <job-image>
`

// tcNodeAgentPolicy declares the agent tier: the definition's shared key wins
// over the template's, and rack is new.
const tcNodeAgentPolicy = `
agents:
  agent-1:
    nodeSelectorPolicy: agent
    nodeSelector:
      disktype: nvme
      rack: r1
`

// tcNodeProjectWithAgentPolicy declares both sides: agent wins disktype, the
// project's zone survives, rack is the agent's alone.
const tcNodeProjectWithAgentPolicy = `
agents:
  agent-1:
    nodeSelectorPolicy: projectWithAgent
    nodeSelector:
      disktype: agent-nvme
      rack: r1
projects:
  proj-x:
    nodeSelector:
      disktype: project-ssd
      zone: project-zone
`

// tcSelectorEqual compares two nodeSelector maps key by key.
func tcSelectorEqual(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return true
}

func TestNodeSelector01Axis(t *testing.T) {
	t.Run("agent-hit-merges-key-by-key", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcNodeTemplate)
		policyPath := writePolicyFile(t, tcNodeAgentPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		tcPrepareRuntimes(t, h)

		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		pod := h.createdJob(t, "fm-task-1").Spec.Template.Spec
		want := map[string]string{"disktype": "nvme", "zone": "template-zone", "rack": "r1"}
		if !tcSelectorEqual(pod.NodeSelector, want) {
			t.Errorf("nodeSelector = %v, want %v (policy key wins, template's other keys survive)", pod.NodeSelector, want)
		}
		event := tcSinglePolicyEvent(t, h)
		if !tcHasFace(event, "node_selector") {
			t.Errorf("template_written_faces = %v, want node_selector", tcTemplateFaces(event))
		}
		if got := tcEventString(event, "node_selector_source"); got != "direct" {
			t.Errorf("node_selector_source = %q, want direct", got)
		}
		if got := tcEventString(event, "soft_affinity_suppressed"); got != "policy" {
			t.Errorf("soft_affinity_suppressed = %q, want policy", got)
		}
		// The item went with the emptied structures: the template wrote nothing
		// else under affinity.
		if pod.Affinity != nil {
			t.Errorf("affinity = %+v, want the recognition item dropped", pod.Affinity)
		}
	})

	t.Run("projectWithAgent-merges-both-definitions", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcNodeTemplate)
		policyPath := writePolicyFile(t, tcNodeProjectWithAgentPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		identity := &countingProjectIdentity{}
		h.rebuildScheduler(t, func(c *schedulerConfig) { c.Project = identity })
		tcPrepareRuntimes(t, h)

		body := tcBaseClaim("task-1")
		body.ProjectID = projectIDFor("proj-x")
		tcClaimOnClaim(t, h, body)

		pod := h.createdJob(t, "fm-task-1").Spec.Template.Spec
		want := map[string]string{"disktype": "agent-nvme", "zone": "project-zone", "rack": "r1"}
		if !tcSelectorEqual(pod.NodeSelector, want) {
			t.Errorf("nodeSelector = %v, want %v (agent wins the shared key, the project's other key survives)", pod.NodeSelector, want)
		}
		event := tcSinglePolicyEvent(t, h)
		if got := tcEventString(event, "project_source"); got != "payload" {
			t.Errorf("project_source = %q, want payload", got)
		}
		if !tcHasString(event, "overridden_keys", "node_selector:disktype") {
			t.Errorf("overridden_keys = %v, want node_selector:disktype", event["overridden_keys"])
		}
		if got := identity.lookupCount(); got != 0 {
			t.Errorf("reverse lookups = %d, want none (the payload carried the project identifier)", got)
		}
	})

	t.Run("miss-leaves-the-field-verbatim", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcNodeTemplate)
		policyPath := writePolicyFile(t, tcNodeAgentPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		tcPrepareRuntimes(t, h)

		body := tcBaseClaim("task-1")
		body.Agent = tcAgentObject{Name: "agent-x"}
		tcClaimOnClaim(t, h, body)

		pod := h.createdJob(t, "fm-task-1").Spec.Template.Spec
		templatePod := decodeJob(t, []byte(tcNodeTemplate)).Spec.Template.Spec
		if !tcSelectorEqual(pod.NodeSelector, templatePod.NodeSelector) {
			t.Errorf("nodeSelector = %v, want the template's %v byte-identical", pod.NodeSelector, templatePod.NodeSelector)
		}
		event := tcSinglePolicyEvent(t, h)
		if got := tcEventString(event, "node_selector_source"); got != "no_definition" {
			t.Errorf("node_selector_source = %q, want no_definition", got)
		}
		if faces := tcTemplateFaces(event); len(faces) != 0 {
			t.Errorf("template_written_faces = %v, want none on a miss", faces)
		}
	})

	t.Run("suppression-and-the-four-rules", func(t *testing.T) {
		// Without the hit the item is filled with the learned node.
		plainTemplate := writeTemplateFile(t, tcNodeTemplate)
		h := newHarness(t, false)
		h.useRealBuilderWithTemplate(t, plainTemplate)
		tcPrepareRuntimes(t, h)
		h.reg.LearnSession(registry.SessionKey("sessions/sess-1"), "node-a")
		body := tcBaseClaim("task-1")
		body.PriorSessionID = "sessions/sess-1"
		tcClaimOnClaim(t, h, body)

		pod := h.createdJob(t, "fm-task-1").Spec.Template.Spec
		if pod.Affinity == nil || pod.Affinity.NodeAffinity == nil {
			t.Fatalf("affinity = %+v, want the recognition item kept", pod.Affinity)
		}
		terms := pod.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
		if len(terms) != 1 || len(terms[0].Preference.MatchExpressions) != 1 ||
			len(terms[0].Preference.MatchExpressions[0].Values) != 1 ||
			terms[0].Preference.MatchExpressions[0].Values[0] != "node-a" {
			t.Errorf("preferred item = %+v, want its value filled with the learned node-a", terms)
		}
		if got := tcEventString(tcSinglePolicyEvent(t, h), "soft_affinity_suppressed"); got != "none" {
			t.Errorf("soft_affinity_suppressed = %q, want none without a hit", got)
		}

		// With the hit, the same session's item is gone and the cause is policy.
		hitTemplate := writeTemplateFile(t, tcNodeTemplate)
		policyPath := writePolicyFile(t, tcNodeAgentPolicy)
		hit := newHarness(t, false)
		hit.useRealBuilderWithPolicy(t, hitTemplate, policyPath, nil)
		tcPrepareRuntimes(t, hit)
		hit.reg.LearnSession(registry.SessionKey("sessions/sess-1"), "node-a")
		hitBody := tcBaseClaim("task-1")
		hitBody.PriorSessionID = "sessions/sess-1"
		tcClaimOnClaim(t, hit, hitBody)

		hitPod := hit.createdJob(t, "fm-task-1").Spec.Template.Spec
		if hitPod.Affinity != nil {
			t.Errorf("affinity = %+v, want the item deleted with the emptied structures", hitPod.Affinity)
		}
		if got := tcEventString(tcSinglePolicyEvent(t, hit), "soft_affinity_suppressed"); got != "policy" {
			t.Errorf("soft_affinity_suppressed = %q, want policy", got)
		}
	})

	t.Run("policy-creates-no-affinity", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcNodeNoAffinityTemplate)
		policyPath := writePolicyFile(t, tcNodeAgentPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		tcPrepareRuntimes(t, h)

		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		pod := h.createdJob(t, "fm-task-1").Spec.Template.Spec
		if pod.Affinity != nil {
			t.Errorf("affinity = %+v, want none: the axis writes nodeSelector, never an affinity item", pod.Affinity)
		}
		want := map[string]string{"disktype": "nvme", "rack": "r1"}
		if !tcSelectorEqual(pod.NodeSelector, want) {
			t.Errorf("nodeSelector = %v, want %v", pod.NodeSelector, want)
		}
		if faces := tcTemplateFaces(tcSinglePolicyEvent(t, h)); len(faces) != 1 || faces[0] != "node_selector" {
			t.Errorf("template_written_faces = %v, want exactly [node_selector]", faces)
		}
	})
}
