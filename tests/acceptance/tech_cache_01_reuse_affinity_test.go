// TC-tech-cache-01 covers AC-12's Foreman-side drive path and AC-26's reverse
// assertion: an agent's continuation carries exactly one soft-affinity item for
// the node its terminal report taught, while a different agent's task on the
// same issue carries no block (issue_id is not a key). The node-side cache
// evidence (the bare .repos object's creation instant against the second pod's
// start) runs from tests/acceptance/tech-cache-01-cluster.sh.
package acceptance

import (
	"testing"

	"github.com/tsic404/foreman/internal/scheduler"
)

func TestCache01ReuseAffinityAndIssueKeyRemoval(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	h.rebuildScheduler(t, func(c *scheduler.Config) { c.MaxJobsPerNode = 4 })

	// Task #1: agent-1 on the issue. The terminal report is the learning face;
	// its session key is the node-reuse key.
	tcClaimOnClaim(t, h, tcClaimPayloadBody{
		ID: "task-1", AgentID: "agent-1", WorkspaceID: "ws-1", RuntimeID: "rid-zz",
		IssueID: "issue-1", IssueIdentifier: "TSI-1",
		Agent: tcAgentObject{Name: "agent-1"},
	})
	h.reg.SetNode("task-1", "node-a")
	tcCacheRunTask(t, h, "task-1", "/state/home/.multica/pi-sessions/sess-42.jsonl")

	// Task #2: the same agent's continuation on the same issue lands in the
	// single-item shape pointing at task #1's node.
	tcClaimOnClaim(t, h, tcClaimPayloadBody{
		ID: "task-2", AgentID: "agent-1", WorkspaceID: "ws-1", RuntimeID: "rid-zz",
		IssueID: "issue-1", IssueIdentifier: "TSI-1", PriorSessionID: "sess-42",
		Agent: tcAgentObject{Name: "agent-1"},
	})
	tcCacheAssertAffinityNode(t, h.createdJob(t, "fm-task-2"), "node-a")

	// Task #3: a different agent on the same issue, no prior session. The
	// issue key is not an affinity key, so the block must be absent — the
	// reverse assertion of AC-26.
	tcClaimOnClaim(t, h, tcClaimPayloadBody{
		ID: "task-3", AgentID: "agent-2", WorkspaceID: "ws-1", RuntimeID: "rid-zz",
		IssueID: "issue-1", IssueIdentifier: "TSI-1",
		Agent: tcAgentObject{Name: "agent-2"},
	})
	third := h.createdJob(t, "fm-task-3")
	if terms := tcCachePreferredTerms(third); terms != nil {
		t.Errorf("a different agent's same-issue task carries affinity %+v, want none (issue_id is not a key)", terms)
	}
	if third.Spec.Template.Spec.Affinity != nil {
		t.Errorf("affinity = %+v, want the block absent", third.Spec.Template.Spec.Affinity)
	}
}
