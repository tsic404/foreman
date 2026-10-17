// TC-tech-ownership-01 implements AC-51 (ADR-034 decision 7): Foreman owns the
// foreman.tsic.top/ domain and the two app.kubernetes.io keys. A template that
// pre-writes them cannot keep a value: the domain is cleared and rewritten per
// side, the Job side carries both app.kubernetes.io keys with Foreman's fixed
// values, the pod side carries the three-label set (name, task id, daemon id —
// no managed-by), and every out-of-domain key a deployer wrote survives
// verbatim. Controller keys are the apiserver's, never Foreman's.
package acceptance

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// The two §3.2 keys the read side (registry) does not index: the Job-side app
// name and the pod-side daemon identity. They are asserted as literals, the
// same way the other rendering TCs assert template keys.
const (
	labelAppNameOwnership  = "app.kubernetes.io/name"
	labelDaemonIDOwnership = "foreman.tsic.top/daemon-id"
)

// ownershipTemplate pre-writes every key Foreman owns, on both sides, plus
// out-of-domain keys that must survive and a static trigger-context key that
// the conditional block has to delete for a task without that context.
const ownershipTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
  labels:
    app.kubernetes.io/name: not-foreman
    app.kubernetes.io/managed-by: someone-else
    foreman.tsic.top/task-id: "static-task"
    foreman.tsic.top/agent-id: "static-agent"
    foreman.tsic.top/workspace-id: "static-workspace"
    cost-center: platform
  annotations:
    foreman.tsic.top/daemon-id: "static-daemon"
    foreman.tsic.top/job-runtime-id: "static-runtime"
    foreman.tsic.top/claimed-at: "1999-01-01T00:00:00Z"
    foreman.tsic.top/attempt: "9"
    foreman.tsic.top/issue-id: "static-issue"
    foreman.tsic.top/issue-identifier: "STATIC-1"
    example.com/note: keep-me
spec:
  template:
    metadata:
      labels:
        app.kubernetes.io/name: not-foreman
        app.kubernetes.io/managed-by: someone-else
        foreman.tsic.top/task-id: "static-task"
        foreman.tsic.top/daemon-id: "static-daemon"
        cost-center: platform
      annotations:
        foreman.tsic.top/issue-identifier: "STATIC-1"
        example.com/pod-note: keep-me
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
          env: []
          volumeMounts: []
`

func TestOwnership01DomainClearedAndRewritten(t *testing.T) {
	templatePath := writeTemplateFile(t, ownershipTemplate)
	h := newHarness(t, false)
	h.useRealBuilderWithTemplate(t, templatePath)

	// Task 1 carries an issue context, so the two issue annotations are
	// rewritten with the task's own values.
	withIssue := json.RawMessage(`{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1"
	}`)
	if err := h.sched.OnClaim(t.Context(), withIssue); err != nil {
		t.Fatalf("OnClaim(task-1): %v", err)
	}
	job := h.createdJob(t, "fm-task-1")
	entry, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("entry task-1 missing after the claim")
	}

	// Job side: the §3.2 set with Foreman's values, the template's same-name
	// values discarded, the deployer's own key kept.
	wantLabels := map[string]string{
		labelAppNameOwnership:     "foreman-job",
		registry.LabelManagedBy:   "foreman",
		registry.LabelTaskID:      "task-1",
		registry.LabelAgentID:     "agent-1",
		registry.LabelWorkspaceID: "ws-1",
		"cost-center":             "platform",
	}
	if len(job.Labels) != len(wantLabels) {
		t.Fatalf("job labels = %v, want exactly %v", job.Labels, wantLabels)
	}
	for key, want := range wantLabels {
		if job.Labels[key] != want {
			t.Errorf("job label %q = %q, want %q", key, job.Labels[key], want)
		}
	}

	wantAnnotations := map[string]string{
		registry.AnnoDaemonID:        "fm-task-1",
		registry.AnnoJobRuntimeID:    entry.JobRuntimeID,
		registry.AnnoIssueID:         "issue-1",
		registry.AnnoIssueIdentifier: "TSI-1",
		"example.com/note":           "keep-me",
	}
	if got := job.Annotations[registry.AnnoClaimedAt]; got == "" || got == "1999-01-01T00:00:00Z" {
		t.Errorf("claimed-at = %q, want the task's claim time", got)
	}
	if got := job.Annotations[registry.AnnoAttempt]; got != "1" {
		t.Errorf("attempt = %q, want 1", got)
	}
	for key, want := range wantAnnotations {
		if job.Annotations[key] != want {
			t.Errorf("job annotation %q = %q, want %q", key, job.Annotations[key], want)
		}
	}
	if len(job.Annotations) != len(wantAnnotations)+2 {
		t.Errorf("job annotations = %v, want the set plus claimed-at/attempt", job.Annotations)
	}

	// Pod side: exactly three Foreman labels (no managed-by — that key is
	// Job-side only, so the template's pod-side value is discarded, not kept)
	// plus the deployer's own key.
	podLabels := job.Spec.Template.Labels
	wantPodLabels := map[string]string{
		labelAppNameOwnership:  "foreman-job",
		registry.LabelTaskID:   "task-1",
		labelDaemonIDOwnership: "fm-task-1",
		"cost-center":          "platform",
	}
	if len(podLabels) != len(wantPodLabels) {
		t.Fatalf("pod labels = %v, want exactly %v", podLabels, wantPodLabels)
	}
	for key, want := range wantPodLabels {
		if podLabels[key] != want {
			t.Errorf("pod label %q = %q, want %q", key, podLabels[key], want)
		}
	}
	// The pod side carries no annotations of Foreman's: the template's
	// foreman.tsic.top/issue-identifier is cleared, the deployer's stays.
	podAnnotations := job.Spec.Template.Annotations
	if len(podAnnotations) != 1 || podAnnotations["example.com/pod-note"] != "keep-me" {
		t.Errorf("pod annotations = %v, want only the deployer's own key", podAnnotations)
	}

	// Controller keys belong to the apiserver: Foreman writes none of them,
	// and the one on the Job carries the Job's own name.
	for _, key := range []string{"job-name", "controller-uid", "batch.kubernetes.io/job-name", "batch.kubernetes.io/controller-uid"} {
		if got, ok := job.Labels[key]; ok {
			t.Errorf("job label %q = %q, want no controller key from Foreman", key, got)
		}
		if got, ok := podLabels[key]; ok {
			t.Errorf("pod label %q = %q, want no controller key from Foreman", key, got)
		}
	}

	// Task 2 has no issue context: the template's static issue keys must be
	// deleted, not left pointing at another task.
	withoutIssue := json.RawMessage(`{
		"id": "task-2", "agent_id": "agent-2", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "chat_session_id": "chat-1"
	}`)
	if err := h.sched.OnClaim(t.Context(), withoutIssue); err != nil {
		t.Fatalf("OnClaim(task-2): %v", err)
	}
	second := h.createdJob(t, "fm-task-2")
	for _, key := range []string{registry.AnnoIssueID, registry.AnnoIssueIdentifier} {
		if value, ok := second.Annotations[key]; ok {
			t.Errorf("annotation %q = %q, want the key absent without an issue context", key, value)
		}
	}
	if second.Annotations[registry.AnnoChatSessionID] != "chat-1" {
		t.Errorf("chat-session-id = %q, want the payload's entry anchor",
			second.Annotations[registry.AnnoChatSessionID])
	}
	if got := second.Annotations[registry.AnnoDaemonID]; got != "fm-task-2" {
		t.Errorf("daemon-id = %q, want the second task's identity name", got)
	}

	// The rendered objects carry no empty-string placeholder anywhere in the
	// domain (§3.2: an empty string is indistinguishable from a lost key).
	rendered := canonicalJob(second)
	for _, key := range []string{
		registry.AnnoIssueID, registry.AnnoIssueIdentifier,
		registry.AnnoAutopilotRunID, registry.AnnoChatSessionID,
	} {
		if strings.Contains(rendered, `"`+key+`":""`) {
			t.Errorf("rendered Job carries an empty %q placeholder:\n%s", key, rendered)
		}
	}
}

// TestOwnership01ProbeKeepsItsOwnDomain: the probe's label block is Foreman's
// write face too — the task-id label never appears (it would make a probe a
// task Job), and the probe's daemon identity is written even when the template
// omits it.
func TestOwnership01ProbeKeepsItsOwnDomain(t *testing.T) {
	templatePath := writeTemplateFile(t, ownershipTemplate)
	h := newHarness(t, true)
	h.useRealBuilderWithTemplate(t, templatePath)

	job, _, _, err := h.builder.BuildCatalogProbe("req-1", "runtime-1", "ws-1", 45*time.Second)
	if err != nil {
		t.Fatalf("BuildCatalogProbe: %v", err)
	}
	if _, ok := job.Labels[registry.LabelTaskID]; ok {
		t.Errorf("probe labels = %v, want no task-id", job.Labels)
	}
	if job.Labels[registry.LabelRole] != registry.RoleCatalogProbe {
		t.Errorf("probe role label = %q, want %q", job.Labels[registry.LabelRole], registry.RoleCatalogProbe)
	}
	if job.Labels[labelDaemonIDOwnership] != job.Name {
		t.Errorf("probe daemon-id = %q, want the probe Job name %q", job.Labels[labelDaemonIDOwnership], job.Name)
	}
	if job.Labels[registry.LabelManagedBy] != "foreman" {
		t.Errorf("probe managed-by = %q, want the sweep selector's value", job.Labels[registry.LabelManagedBy])
	}
	for key, value := range job.Annotations {
		if strings.HasPrefix(key, "foreman.tsic.top/") {
			t.Errorf("probe annotation %q = %q, want the domain empty", key, value)
		}
	}
	if _, ok := job.Spec.Template.Labels[registry.LabelManagedBy]; ok {
		t.Errorf("probe pod labels = %v, want no managed-by", job.Spec.Template.Labels)
	}
}
