// TC-tech-cache-05 implements AC-25's Foreman-side chain: the learning face is
// the terminal report body's session_id (S16 is not one), both sides pass
// through the same SessionKey normalization, and a chat-entry continuation —
// a task with no issue context — gets exactly one preferred-affinity item
// naming the learned node (weight 100, hostname In [node]) when the reuse
// switch is on and the node is below its cap. The pod-landing face of the
// design case (step 4b) needs a live cluster and runs from
// tests/acceptance/tech-cache-05-cluster.sh.
package acceptance

import (
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// tcCachePreferredTerms returns the rendered Job's preferred-affinity items,
// nil when the Job has no affinity block.
func tcCachePreferredTerms(job *batchv1.Job) []corev1.PreferredSchedulingTerm {
	affinity := job.Spec.Template.Spec.Affinity
	if affinity == nil || affinity.NodeAffinity == nil {
		return nil
	}
	return affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
}

// tcCacheAssertAffinityNode asserts the single-item shape AC-25 fixes.
func tcCacheAssertAffinityNode(t *testing.T, job *batchv1.Job, node string) {
	t.Helper()
	terms := tcCachePreferredTerms(job)
	if len(terms) != 1 {
		t.Fatalf("preferred items = %d, want exactly one: %+v", len(terms), terms)
	}
	if terms[0].Weight != 100 {
		t.Errorf("preferred weight = %d, want 100", terms[0].Weight)
	}
	expressions := terms[0].Preference.MatchExpressions
	if len(expressions) != 1 {
		t.Fatalf("matchExpressions = %+v, want exactly one", expressions)
	}
	if expressions[0].Key != "kubernetes.io/hostname" || expressions[0].Operator != corev1.NodeSelectorOpIn ||
		len(expressions[0].Values) != 1 || expressions[0].Values[0] != node {
		t.Errorf("matchExpression = %+v, want hostname In [%s]", expressions[0], node)
	}
}

// tcCacheChatClaim is one chat-entry claim: no issue context, a chat session,
// optionally resuming a learned session.
func tcCacheChatClaim(taskID, chatID, priorSession string) tcClaimPayloadBody {
	return tcClaimPayloadBody{
		ID: taskID, AgentID: "agent-1", WorkspaceID: "ws-1", RuntimeID: "rid-zz",
		ChatSessionID: chatID, PriorSessionID: priorSession,
		Agent: tcAgentObject{Name: "agent-1"},
	}
}

// tcCacheRunTask drives one claimed task to its terminal report. The terminal
// body carries the session id the learning face reads.
func tcCacheRunTask(t *testing.T, h *harness, taskID, sessionID string) {
	t.Helper()
	token := issueJobToken(t, h, taskID)
	registerTaskDaemon(t, h, taskID, token)
	claimTask(t, h, taskID, token)
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/start", token, []byte(`{}`)); code != 200 {
		t.Fatalf("S8 %s = %d (%s), want 200", taskID, code, body)
	}
	body := []byte(`{"result":"ok"}`)
	if sessionID != "" {
		body = []byte(`{"result":"ok","session_id":"` + sessionID + `"}`)
	}
	if code, raw := h.call("POST", "/api/daemon/tasks/"+taskID+"/complete", token, body); code != 200 {
		t.Fatalf("S13 %s = %d (%s), want 200", taskID, code, raw)
	}
}

func TestCache05SessionResumeAffinity(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	h.rebuildScheduler(t, func(c *schedulerConfig) { c.MaxJobsPerNode = 4 })

	// Task #1: a chat-entry first run. Its terminal report teaches the index
	// the node its session file lives on; the path spelling is the learning
	// side's input, the bare id the payload side's.
	tcClaimOnClaim(t, h, tcCacheChatClaim("task-1", "chat-9", ""))
	h.reg.SetNode("task-1", "node-b")

	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	claimTask(t, h, "task-1", token)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != 200 {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	// S16 is not a learning face (ADR-015): its body never feeds the index.
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/session", token,
		[]byte(`{"session_id":"/state/home/.multica/pi-sessions/s16-only.jsonl"}`)); code != 200 {
		t.Fatalf("S16 = %d (%s), want 200", code, body)
	}
	if got := h.reg.LastNodeForSession("ws-1", registry.SessionKey("s16-only")); got != "" {
		t.Errorf("S16 taught the index node %q, want no learning outside S13/S14", got)
	}
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token,
		[]byte(`{"result":"ok","session_id":"/state/home/.multica/pi-sessions/chat-sess.jsonl"}`)); code != 200 {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}
	if got := h.reg.LastNodeForSession("ws-1", registry.SessionKey("chat-sess")); got != "node-b" {
		t.Fatalf("learning face recorded node %q, want node-b (path and bare spellings share one key)", got)
	}

	// Task #2: the same chat session's continuation, no issue context.
	tcClaimOnClaim(t, h, tcCacheChatClaim("task-2", "chat-9", "chat-sess"))
	entry, ok := h.reg.Get("task-2")
	if !ok {
		t.Fatal("task-2 is not in the registry")
	}
	if entry.IssueID != "" || entry.IssueIdentifier != "" {
		t.Errorf("the continuation carried issue context: %q / %q", entry.IssueID, entry.IssueIdentifier)
	}
	if entry.ChatSessionID != "chat-9" {
		t.Errorf("chat_session_id = %q, want chat-9", entry.ChatSessionID)
	}
	job := h.createdJob(t, "fm-task-2")
	tcCacheAssertAffinityNode(t, job, "node-b")
	if required := job.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution; required != nil {
		t.Errorf("required affinity = %+v, want none (soft affinity only)", required)
	}
}
