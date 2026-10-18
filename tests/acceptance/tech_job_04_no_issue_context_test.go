// TC-tech-job-04 implements AC-23 / AC-24 for both no-issue-context entries:
// an autopilot claim and a chat claim take the same path as an issue task. The
// scenario feeds the payloads a real trigger produces and asserts the Job is
// created inside the claim budget, the daemon claims the same task id, the Job
// and Secret are gone after the verdict, and the annotation block follows the
// entry anchor with both issue keys absent. The real trigger and the cluster
// claim → Job latency run from tech-job-04-cluster.sh; this file's 5s bound is
// the in-process claim → build budget (a memory Job client cannot observe the
// apiserver round trip), never a substitute for that cluster measurement.
package acceptance

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestJob04NoIssueContext(t *testing.T) {
	scenarios := []struct {
		name        string
		payload     string
		anchorKey   string
		anchorValue string
		otherKey    string
	}{
		{
			name: "autopilot",
			payload: `{"id":"task-1","agent_id":"agent-1","workspace_id":"ws-1",
				"runtime_id":"rid-zz","autopilot_run_id":"run-1"}`,
			anchorKey:   registry.AnnoAutopilotRunID,
			anchorValue: "run-1",
			otherKey:    registry.AnnoChatSessionID,
		},
		{
			name: "chat",
			payload: `{"id":"task-1","agent_id":"agent-1","workspace_id":"ws-1",
				"runtime_id":"rid-zz","chat_session_id":"chat-1"}`,
			anchorKey:   registry.AnnoChatSessionID,
			anchorValue: "chat-1",
			otherKey:    registry.AnnoAutopilotRunID,
		},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			h := newHarness(t, false)
			h.useRealBuilder(t)
			tcPrepareRuntimes(t, h)

			// Step 2: the Job appears inside the claim-to-create budget. The
			// claim is synchronous, so the elapsed time is that budget.
			begin := time.Now()
			if err := h.sched.OnClaim(t.Context(), json.RawMessage(sc.payload)); err != nil {
				t.Fatalf("OnClaim: %v", err)
			}
			created := time.Since(begin)
			if created > 5*time.Second {
				t.Fatalf("in-process claim → build took %s, want ≤5s (the cluster claim → Job latency is tech-job-04-cluster.sh)", created)
			}
			entry, ok := h.reg.Get("task-1")
			if !ok {
				t.Fatal("the claimed task is not in the registry")
			}
			if entry.IssueID != "" || entry.IssueIdentifier != "" {
				t.Fatalf("the payload carried issue context: %q / %q", entry.IssueID, entry.IssueIdentifier)
			}
			if entry.FailureReason != "" {
				t.Fatalf("failure_reason = %q, want none (the template-refusal path is retired)", entry.FailureReason)
			}
			job := h.createdJob(t, "fm-task-1")

			// Step 3: the conditional annotation block (AC-24). The issue keys
			// are absent, the scenario's own anchor is exact, the other
			// entry's anchor is absent, and the four identity keys are filled.
			for _, key := range []string{registry.AnnoIssueID, registry.AnnoIssueIdentifier} {
				if got, present := job.Annotations[key]; present {
					t.Errorf("annotation %q = %q, want it absent (no issue context)", key, got)
				}
			}
			if got := job.Annotations[sc.anchorKey]; got != sc.anchorValue {
				t.Errorf("annotation %q = %q, want %q", sc.anchorKey, got, sc.anchorValue)
			}
			if got, present := job.Annotations[sc.otherKey]; present {
				t.Errorf("annotation %q = %q, want it absent (the other entry did not trigger)", sc.otherKey, got)
			}
			for _, key := range []string{
				registry.AnnoDaemonID, registry.AnnoJobRuntimeID,
				registry.AnnoClaimedAt, registry.AnnoAttempt,
			} {
				if key == registry.AnnoDaemonID {
					if got := job.Annotations[key]; got != "fm-task-1" {
						t.Errorf("%s = %q, want the identity name", key, got)
					}
					continue
				}
				if got := job.Annotations[key]; got == "" {
					t.Errorf("%s = empty, want non-empty (identity keys are unconditional)", key)
				}
			}
			if got := job.Annotations[registry.AnnoAttempt]; got != "1" {
				t.Errorf("attempt = %q, want 1 (first claim)", got)
			}
			if _, err := time.Parse(time.RFC3339, job.Annotations[registry.AnnoClaimedAt]); err != nil {
				t.Errorf("claimed-at %q is not RFC3339: %v", job.Annotations[registry.AnnoClaimedAt], err)
			}
			for key, value := range job.Annotations {
				if strings.HasPrefix(key, "foreman.tsic.top/") && value == "" {
					t.Errorf("annotation %q is an empty placeholder, want the key absent", key)
				}
			}
			// First run (no prior session): the soft-affinity key cannot
			// resolve, so no affinity block is injected (ADR-015).
			if job.Spec.Template.Spec.Affinity != nil {
				t.Errorf("affinity = %+v, want nil on a first run", job.Spec.Template.Spec.Affinity)
			}

			// Step 4: the daemon claims the same task id and the chain runs to
			// its terminal verdict.
			token := issueJobToken(t, h, "task-1")
			registerTaskDaemon(t, h, "task-1", token)
			if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
				t.Fatalf("S6 claim = %s, want the same task id", got)
			}
			if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != 200 {
				t.Fatalf("S8 = %d (%s), want 200", code, body)
			}
			if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, []byte(`{"result":"ok"}`)); code != 200 {
				t.Fatalf("S13 = %d (%s), want 200", code, body)
			}
			for _, event := range []string{"task.claimed", "job.created", "task.delivered", "task.started", "task.terminal"} {
				if got := h.events(event); len(got) != 1 {
					t.Errorf("%s events = %d, want 1 (one task chain)", event, len(got))
				}
			}

			// Step 5: the verdict removes the Job; closing the drain removes
			// the credential Secret (AC-23's cleanup face, AC-30's order).
			if !h.jobs.jobDeleted("fm-task-1") {
				t.Error("the Job must be deleted with the terminal verdict")
			}
			if err := h.sched.OnJobTerminated(t.Context(), "fm-task-1",
				scheduler.JobTerminationReasonJobTerminated, time.Now(), 0); err != nil {
				t.Fatalf("OnJobTerminated: %v", err)
			}
			secretGone := false
			for _, name := range h.jobs.deletedSecrets {
				if name == "fm-task-1-cred" {
					secretGone = true
				}
			}
			if !secretGone {
				t.Errorf("deleted Secrets = %v, want the per-task Secret reclaimed", h.jobs.deletedSecrets)
			}
			if _, exists := h.reg.Get("task-1"); exists {
				t.Error("the reclaimed entry is still live")
			}
		})
	}
}
