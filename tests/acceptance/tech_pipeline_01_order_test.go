// TC-tech-pipeline-01 implements AC-52's render-order assertions on the real
// jobbuilder: the name composition runs before the generic substitution (so a
// <job-name> outside metadata.name equals the final display name, while
// metadata.name writing <job-name> is refused at startup), the substitution
// runs before the policy injection (so a policy definition's <...> literal
// lands verbatim), the injection runs before the <preferred node> conditional
// block (so a nodeSelector hit deletes the item and collapses what it empties),
// and the forced ownership writes run last (so neither the template nor the
// policy can overwrite Foreman's own keys). The generic substitution rewrites
// strings in place and never builds structure; it leaves <preferred node> and
// the name-face variables alone outside their own recognition sites.
package acceptance

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
)

// tcPipelineNameTemplate carries <job-name> outside metadata.name and a name
// format whose rendering is knowable: metadata.name is the format, the pod
// annotation and the container args read the final name.
const tcPipelineNameTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<issue-segment>-<task-key>
  annotations:
    example.com/expected-name: "<job-name>"
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
          args: ["run", "--name", "<job-name>", "--agent", "<agent-name>"]
`

// tcPipelineNodeTemplate carries both the <preferred node> recognition item and
// a template-written nodeSelector key, so the injection-vs-conditional-block
// order is observable.
const tcPipelineNodeTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
spec:
  template:
    spec:
      restartPolicy: Never
      nodeSelector: { disktype: ssd }
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

// tcPipelineLiteralPolicy writes a <...> literal into the nodeSelector axis's
// definition, which must reach the product verbatim.
const tcPipelineLiteralPolicy = `
agents:
  agent-1:
    nodeSelectorPolicy: agent
    nodeSelector:
      example.com/note: "<agent-name>"
`

// tcPipelineOwnershipTemplate pre-writes Foreman's own keys on both sides and
// the A1.1 env keys, so the forced ownership writes are the only writer left.
const tcPipelineOwnershipTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
  labels:
    foreman.tsic.top/task-id: static-wrong
    foreman.tsic.top/daemon-id: static-wrong
    example.com/keep: kept
  annotations:
    foreman.tsic.top/daemon-id: static-wrong
    foreman.tsic.top/issue-id: static-wrong
spec:
  template:
    metadata:
      labels:
        foreman.tsic.top/task-id: static-wrong
        foreman.tsic.top/daemon-id: static-wrong
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
          env:
            - { name: MULTICA_SERVER_URL, value: "http://evil.example" }
            - { name: MULTICA_DAEMON_MAX_CONCURRENT_TASKS, value: "99" }
            - { name: KEEP, value: "kept" }
`

// tcPipelineOwnershipPolicy injects both axis faces into containers[0], so the
// policy writes and the ownership writes land in the same build.
const tcPipelineOwnershipPolicy = `
agents:
  agent-1:
    resourcePolicy: agent
    resources:
      limits: { cpu: "2" }
    capPolicy: agent
    capabilities:
      add: ["NET_ADMIN"]
`

// tcPipelineStructureTemplate exercises the substitution deep inside lists and
// maps and writes the literals the generic walk must not consume.
const tcPipelineStructureTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
  annotations:
    example.com/literals: "<issue-segment> <agent-segment> <preferred node>"
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
          args: ["--task", "<task_id>", "--agent", "<agent-name>"]
          env:
            - { name: KEEP, value: "<workspace_id>" }
`

// tcClaimPayloadBody is the minimal claim payload the pipeline scenarios need:
// the head fields the scheduler indexes plus the agent object and the optional
// structured project key.
type tcClaimPayloadBody struct {
	ID              string          `json:"id"`
	AgentID         string          `json:"agent_id"`
	WorkspaceID     string          `json:"workspace_id"`
	RuntimeID       string          `json:"runtime_id"`
	IssueID         string          `json:"issue_id,omitempty"`
	IssueIdentifier string          `json:"issue_identifier,omitempty"`
	ChatSessionID   string          `json:"chat_session_id,omitempty"`
	PriorSessionID  string          `json:"prior_session_id,omitempty"`
	Agent           tcAgentObject   `json:"agent"`
	Project         *tcProjectValue `json:"project,omitempty"`
}

type tcAgentObject struct {
	Name string `json:"name"`
}

type tcProjectValue struct {
	Name string `json:"name"`
}

// tcBaseClaim is the issue-triggered claim every pipeline scenario starts from.
func tcBaseClaim(taskID string) tcClaimPayloadBody {
	return tcClaimPayloadBody{
		ID:              taskID,
		AgentID:         "agent-1",
		WorkspaceID:     "ws-1",
		RuntimeID:       "rid-omp",
		IssueID:         "issue-1",
		IssueIdentifier: "TSI-1",
		Agent:           tcAgentObject{Name: "agent-1"},
	}
}

// tcClaimPayload marshals one claim body.
func tcClaimPayload(tb testing.TB, body tcClaimPayloadBody) json.RawMessage {
	tb.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		tb.Fatalf("marshal claim payload: %v", err)
	}
	return raw
}

// tcClaimOnClaim runs one claim through the real scheduler.
func tcClaimOnClaim(t *testing.T, h *harness, body tcClaimPayloadBody) {
	t.Helper()
	if err := h.sched.OnClaim(t.Context(), tcClaimPayload(t, body)); err != nil {
		t.Fatalf("OnClaim(%s): %v", body.ID, err)
	}
}

// tcPrepareRuntimes mirrors the deployment precondition the real claim path
// needs: the declared runtimes the Job's provider is chosen from.
func tcPrepareRuntimes(t *testing.T, h *harness) {
	t.Helper()
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
}

// tcSinglePolicyEvent returns the one policy.applied event of the process.
func tcSinglePolicyEvent(t *testing.T, h *harness) map[string]any {
	t.Helper()
	events := h.events("policy.applied")
	if len(events) != 1 {
		t.Fatalf("policy.applied events = %d, want 1: %v", len(events), events)
	}
	return events[0]
}

// tcEventString reads one string field of a log event.
func tcEventString(event map[string]any, key string) string {
	value, _ := event[key].(string)
	return value
}

// tcTemplateFaces reads the template_written_faces list of a policy.applied
// event.
func tcTemplateFaces(event map[string]any) []string {
	raw, _ := event["template_written_faces"].([]any)
	out := make([]string, 0, len(raw))
	for _, entry := range raw {
		if face, ok := entry.(string); ok {
			out = append(out, face)
		}
	}
	return out
}

// tcHasFace reports whether template_written_faces carries one face.
func tcHasFace(event map[string]any, face string) bool {
	for _, entry := range tcTemplateFaces(event) {
		if entry == face {
			return true
		}
	}
	return false
}

// tcHasString reports whether one list field of an event carries a value.
func tcHasString(event map[string]any, key, value string) bool {
	raw, _ := event[key].([]any)
	for _, entry := range raw {
		if s, ok := entry.(string); ok && s == value {
			return true
		}
	}
	return false
}

func TestPipeline01RenderOrder(t *testing.T) {
	t.Run("job-name-outside-metadata-name-is-the-final-name", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcPipelineNameTemplate)
		h := newHarness(t, false)
		h.useRealBuilderWithTemplate(t, templatePath)
		tcPrepareRuntimes(t, h)

		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		job := h.createdJob(t, "fm-tsi-1-task1")
		if job.Name != "fm-tsi-1-task1" {
			t.Errorf("metadata.name = %q, want the rendered format fm-tsi-1-task1", job.Name)
		}
		// The name composition runs before the substitution: <job-name> is the
		// final display name everywhere it appears outside metadata.name.
		if got := job.Annotations["example.com/expected-name"]; got != job.Name {
			t.Errorf("annotation expected-name = %q, want the final name %q", got, job.Name)
		}
		args := job.Spec.Template.Spec.Containers[0].Args
		if got := args[2]; got != job.Name {
			t.Errorf("args <job-name> = %q, want the final name %q", got, job.Name)
		}
		// <agent-name> is the payload's raw agent.name, not the normalized
		// <agent-segment>.
		if got := args[4]; got != "agent-1" {
			t.Errorf("args <agent-name> = %q, want the raw payload name agent-1", got)
		}
	})

	t.Run("job-name-inside-metadata-name-refuses-startup", func(t *testing.T) {
		templatePath := writeTemplateFile(t, `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<job-name>
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`)
		cfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
		if err != nil {
			t.Fatalf("jobbuilder.LoadConfig: %v", err)
		}
		cfg.TemplatePath = templatePath
		if _, err := jobbuilder.NewBuilder(cfg); !errors.Is(err, jobbuilder.ErrInvalidJobNameFormat) {
			t.Fatalf("NewBuilder error = %v, want ErrInvalidJobNameFormat (a self-referential name format is refused)", err)
		}
	})

	t.Run("policy-literal-lands-verbatim", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcPipelineNodeTemplate)
		policyPath := writePolicyFile(t, tcPipelineLiteralPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		tcPrepareRuntimes(t, h)

		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		job := h.createdJob(t, "fm-task-1")
		selector := job.Spec.Template.Spec.NodeSelector
		// The substitution runs before the injection: the policy's own <...>
		// literal was not in the template copy the walk saw, so it survives.
		if got := selector["example.com/note"]; got != "<agent-name>" {
			t.Errorf("nodeSelector example.com/note = %q, want the policy's literal <agent-name> verbatim", got)
		}
		if got := selector["disktype"]; got != "ssd" {
			t.Errorf("nodeSelector disktype = %q, want the template's ssd kept", got)
		}
		event := tcSinglePolicyEvent(t, h)
		if !tcHasFace(event, "node_selector") {
			t.Errorf("template_written_faces = %v, want node_selector", tcTemplateFaces(event))
		}
	})

	t.Run("node-selector-hit-collapses-the-preferred-item", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcPipelineNodeTemplate)
		policyPath := writePolicyFile(t, tcPipelineLiteralPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		tcPrepareRuntimes(t, h)
		// A learned session with a usable node: without the policy injection the
		// item would have been filled with node-a, so the deletion proves the
		// injection ran before the conditional block.
		h.reg.LearnSession(registry.SessionKey("sessions/sess-1"), "node-a")

		body := tcBaseClaim("task-1")
		body.PriorSessionID = "sessions/sess-1"
		tcClaimOnClaim(t, h, body)
		job := h.createdJob(t, "fm-task-1")
		if job.Spec.Template.Spec.Affinity != nil {
			t.Errorf("affinity = %+v, want the recognition item dropped and the emptied structures collapsed", job.Spec.Template.Spec.Affinity)
		}
		event := tcSinglePolicyEvent(t, h)
		if got := tcEventString(event, "soft_affinity_suppressed"); got != "policy" {
			t.Errorf("soft_affinity_suppressed = %q, want policy", got)
		}
	})

	t.Run("foreman-owned-keys-win", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcPipelineOwnershipTemplate)
		policyPath := writePolicyFile(t, tcPipelineOwnershipPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		tcPrepareRuntimes(t, h)

		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		job := h.createdJob(t, "fm-task-1")
		pod := job.Spec.Template.Spec

		if got := job.Labels[registry.LabelTaskID]; got != "task-1" {
			t.Errorf("Job label task-id = %q, want Foreman's task-1", got)
		}
		if _, present := job.Labels["foreman.tsic.top/daemon-id"]; present {
			t.Errorf("Job label daemon-id = %q, want Foreman's domain cleared (Job-side daemon-id lives in the annotation)", job.Labels["foreman.tsic.top/daemon-id"])
		}
		if got := job.Annotations[registry.AnnoDaemonID]; got != "fm-task-1" {
			t.Errorf("Job annotation daemon-id = %q, want fm-task-1", got)
		}
		if got := job.Annotations[registry.AnnoIssueID]; got != "issue-1" {
			t.Errorf("Job annotation issue-id = %q, want the claim's issue-1", got)
		}
		if got := job.Spec.Template.Labels[registry.LabelTaskID]; got != "task-1" {
			t.Errorf("pod label task-id = %q, want Foreman's task-1", got)
		}
		if got := job.Spec.Template.Labels["foreman.tsic.top/daemon-id"]; got != "fm-task-1" {
			t.Errorf("pod label daemon-id = %q, want fm-task-1", got)
		}
		if got := job.Labels["example.com/keep"]; got != "kept" {
			t.Errorf("out-of-domain label = %q, want the template's kept", got)
		}

		env := pod.Containers[0].Env
		if got := envValue(env, "MULTICA_SERVER_URL"); got != "http://foreman.foreman.svc.cluster.local:8080" {
			t.Errorf("MULTICA_SERVER_URL = %q, want Foreman's address, not the template's", got)
		}
		if got := envValue(env, "MULTICA_DAEMON_MAX_CONCURRENT_TASKS"); got != "1" {
			t.Errorf("MULTICA_DAEMON_MAX_CONCURRENT_TASKS = %q, want Foreman's 1, not the template's 99", got)
		}
		if got := envValue(env, "KEEP"); got != "kept" {
			t.Errorf("KEEP = %q, want the template's kept", got)
		}

		// The policy's own writes landed, but Foreman's keys did not move.
		if got := pod.Containers[0].Resources.Limits.Cpu().String(); got != "2" {
			t.Errorf("containers[0] cpu limit = %q, want the policy's 2", got)
		}
		event := tcSinglePolicyEvent(t, h)
		for _, face := range []string{"resources", "capabilities_add"} {
			if !tcHasFace(event, face) {
				t.Errorf("template_written_faces = %v, want %s", tcTemplateFaces(event), face)
			}
		}
	})

	t.Run("substitution-builds-no-structure", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcPipelineStructureTemplate)
		h := newHarness(t, false)
		h.useRealBuilderWithTemplate(t, templatePath)
		tcPrepareRuntimes(t, h)

		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		job := h.createdJob(t, "fm-task-1")
		pod := job.Spec.Template.Spec

		// The substitution rewrites strings in place: the template's structure
		// (containers, volumes, arg lists, absent affinity/selector) survives
		// the walk exactly.
		templateJob := decodeJob(t, []byte(tcPipelineStructureTemplate))
		templatePod := templateJob.Spec.Template.Spec
		if len(pod.Containers) != len(templatePod.Containers) {
			t.Fatalf("containers = %d, want the template's %d", len(pod.Containers), len(templatePod.Containers))
		}
		if len(pod.Volumes) != len(templatePod.Volumes) {
			t.Errorf("volumes = %d, want the template's %d", len(pod.Volumes), len(templatePod.Volumes))
		}
		for i := range pod.Containers {
			if len(pod.Containers[i].Args) != len(templatePod.Containers[i].Args) {
				t.Errorf("container %d args = %d entries, want the template's %d", i, len(pod.Containers[i].Args), len(templatePod.Containers[i].Args))
			}
		}
		if pod.Affinity != nil || pod.NodeSelector != nil {
			t.Errorf("affinity/nodeSelector = %+v/%v, want no structure the template did not write", pod.Affinity, pod.NodeSelector)
		}

		// The substitution did visit deep string values.
		container := pod.Containers[0]
		if got := container.Args[1]; got != "task-1" {
			t.Errorf("args <task_id> = %q, want task-1", got)
		}
		if got := container.Args[3]; got != "agent-1" {
			t.Errorf("args <agent-name> = %q, want agent-1", got)
		}
		if got := envValue(container.Env, "KEEP"); got != "ws-1" {
			t.Errorf("env KEEP = %q, want the substituted ws-1", got)
		}

		// It does not consume <preferred node> or the name-face variables
		// outside metadata.name.
		literals := job.Annotations["example.com/literals"]
		for _, literal := range []string{"<issue-segment>", "<agent-segment>", "<preferred node>"} {
			if !strings.Contains(literals, literal) {
				t.Errorf("annotation literals = %q, want %s verbatim", literals, literal)
			}
		}
	})
}
