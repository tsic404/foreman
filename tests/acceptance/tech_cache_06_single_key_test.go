// TC-tech-cache-06 implements AC-26's single-key semantics in the four shapes
// the design case fixes: the issue key does not participate (a different
// agent's same-issue task has no block, while the same agent's continuation
// still has exactly one item), a saturated node suppresses the item, switching
// reuse off suppresses it with the key still resolvable, and an expired index
// entry suppresses it after the TTL prune. Each negative shape carries its own
// positive control so a missing key and a suppressed item are distinguishable.
// The live-Job face of the four shapes is
// tests/acceptance/tech-cache-06-cluster.sh.
package acceptance

import (
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestCache06SingleKeySemantics(t *testing.T) {
	t.Run("issue-key-does-not-participate", func(t *testing.T) {
		h := newHarness(t, false)
		h.useRealBuilder(t)
		tcPrepareRuntimes(t, h)
		h.rebuildScheduler(t, func(c *scheduler.Config) { c.MaxJobsPerNode = 4 })

		tcClaimOnClaim(t, h, tcClaimPayloadBody{
			ID: "task-1", AgentID: "agent-1", WorkspaceID: "ws-1", RuntimeID: "rid-zz",
			IssueID: "issue-1", IssueIdentifier: "TSI-1",
			Agent: tcAgentObject{Name: "agent-1"},
		})
		h.reg.SetNode("task-1", "node-a")
		tcCacheRunTask(t, h, "task-1", "/state/home/.multica/pi-sessions/sess-1.jsonl")

		// 1a: a different agent on the same issue — no block.
		tcClaimOnClaim(t, h, tcClaimPayloadBody{
			ID: "task-b1", AgentID: "agent-b", WorkspaceID: "ws-1", RuntimeID: "rid-zz",
			IssueID: "issue-1", IssueIdentifier: "TSI-1",
			Agent: tcAgentObject{Name: "agent-b"},
		})
		other := h.createdJob(t, "fm-task-b1")
		if other.Spec.Template.Spec.Affinity != nil {
			t.Errorf("a different agent's same-issue task has affinity %+v, want none", other.Spec.Template.Spec.Affinity)
		}

		// 1b: the same agent's continuation still carries exactly one item.
		tcClaimOnClaim(t, h, tcClaimPayloadBody{
			ID: "task-a2", AgentID: "agent-1", WorkspaceID: "ws-1", RuntimeID: "rid-zz",
			IssueID: "issue-1", IssueIdentifier: "TSI-1", PriorSessionID: "sess-1",
			Agent: tcAgentObject{Name: "agent-1"},
		})
		tcCacheAssertAffinityNode(t, h.createdJob(t, "fm-task-a2"), "node-a")
	})

	t.Run("saturated-node-suppresses", func(t *testing.T) {
		h := newHarness(t, false)
		h.useRealBuilder(t)
		tcPrepareRuntimes(t, h)
		h.rebuildScheduler(t, func(c *scheduler.Config) { c.MaxJobsPerNode = 1 })

		// One live task already sits on node-a: with a cap of 1 the node is
		// saturated at the next claim.
		tcClaimOnClaim(t, h, tcClaimPayloadBody{
			ID: "task-1", AgentID: "agent-1", WorkspaceID: "ws-1", RuntimeID: "rid-zz",
			IssueID: "issue-1", IssueIdentifier: "TSI-1",
			Agent: tcAgentObject{Name: "agent-1"},
		})
		h.reg.SetNode("task-1", "node-a")
		h.reg.LearnSession(registry.SessionKey("sess-sat"), "node-a")

		tcClaimOnClaim(t, h, tcClaimPayloadBody{
			ID: "task-2", AgentID: "agent-1", WorkspaceID: "ws-1", RuntimeID: "rid-zz",
			IssueID: "issue-1", IssueIdentifier: "TSI-1", PriorSessionID: "sess-sat",
			Agent: tcAgentObject{Name: "agent-1"},
		})
		// The key is resolvable; only the cap explains the drop.
		if got := h.reg.LastNodeForSession(registry.SessionKey("sess-sat")); got != "node-a" {
			t.Fatalf("session index = %q, want node-a (the positive control)", got)
		}
		if affinity := h.createdJob(t, "fm-task-2").Spec.Template.Spec.Affinity; affinity != nil {
			t.Errorf("affinity = %+v, want the item suppressed and the block collapsed", affinity)
		}
		saturation := h.events("node.saturation")
		if len(saturation) != 1 {
			t.Fatalf("node.saturation events = %d, want 1", len(saturation))
		}
		if got, _ := saturation[0]["node"].(string); got != "node-a" {
			t.Errorf("saturated node = %q, want node-a", got)
		}
		if got, _ := saturation[0]["max_jobs_per_node"].(float64); got != 1 {
			t.Errorf("max_jobs_per_node = %v, want 1", saturation[0]["max_jobs_per_node"])
		}
	})

	t.Run("reuse-switch-off", func(t *testing.T) {
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), "", func(c *jobbuilder.Config) {
			c.PreferNodeReuse = false
		})
		tcPrepareRuntimes(t, h)
		h.rebuildScheduler(t, func(c *scheduler.Config) { c.MaxJobsPerNode = 4 })

		h.reg.LearnSession(registry.SessionKey("sess-off"), "node-a")
		if got := h.reg.LastNodeForSession(registry.SessionKey("sess-off")); got != "node-a" {
			t.Fatalf("session index = %q, want node-a (the key is resolvable; the switch is the only suppressant)", got)
		}
		tcClaimOnClaim(t, h, tcClaimPayloadBody{
			ID: "task-1", AgentID: "agent-1", WorkspaceID: "ws-1", RuntimeID: "rid-zz",
			IssueID: "issue-1", IssueIdentifier: "TSI-1", PriorSessionID: "sess-off",
			Agent: tcAgentObject{Name: "agent-1"},
		})
		if affinity := h.createdJob(t, "fm-task-1").Spec.Template.Spec.Affinity; affinity != nil {
			t.Errorf("affinity = %+v, want none with FOREMAN_PREFER_NODE_REUSE=false", affinity)
		}
	})

	t.Run("expired-index-entry", func(t *testing.T) {
		h := newHarness(t, false)
		h.useRealBuilder(t)
		tcPrepareRuntimes(t, h)
		h.rebuildScheduler(t, func(c *scheduler.Config) { c.MaxJobsPerNode = 4 })

		h.reg.LearnSession(registry.SessionKey("sess-ttl"), "node-a")
		time.Sleep(5 * time.Millisecond)
		if dropped := h.reg.PruneExpiredSessions(time.Millisecond); dropped != 1 {
			t.Fatalf("pruned entries = %d, want the one expired entry", dropped)
		}
		if got := h.reg.LastNodeForSession(registry.SessionKey("sess-ttl")); got != "" {
			t.Fatalf("session index = %q after expiry, want the entry gone", got)
		}
		tcClaimOnClaim(t, h, tcClaimPayloadBody{
			ID: "task-1", AgentID: "agent-1", WorkspaceID: "ws-1", RuntimeID: "rid-zz",
			IssueID: "issue-1", IssueIdentifier: "TSI-1", PriorSessionID: "sess-ttl",
			Agent: tcAgentObject{Name: "agent-1"},
		})
		if affinity := h.createdJob(t, "fm-task-1").Spec.Template.Spec.Affinity; affinity != nil {
			t.Errorf("affinity = %+v, want none for an expired index entry", affinity)
		}
	})
}
