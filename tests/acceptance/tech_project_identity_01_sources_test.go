// TC-tech-project-identity-01 implements AC-54 across the scheduler seam: the
// claim payload's structured project key is the preferred source
// (project_source=payload and no reverse lookup at all), a missing key with an
// issue id goes through one read-only ByIssue attempt (project_source=api), no
// issue or no project name reads project_source=none with no failure event, and
// a failing or slow lookup logs project.lookup_failed (with the issue id) and
// lets the Job be created. A task whose declared tiers never read the project
// side performs no lookup. The reverse lookup is cached per issue id.
package acceptance

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

// tcProjectIdentity is the platform-side project-identity seam the scenarios
// install through scheduler.Config.Project. FromPayload reads the claim
// payload's structured project key ({"project":{"name":...}}); ByIssue answers
// the read-only reverse lookup, recording every issue id it was called with.
type tcProjectIdentity struct {
	apiName string
	apiErr  error
	block   bool

	mu          sync.Mutex
	issueCalls  []string
	payloadSeen int
}

func (p *tcProjectIdentity) FromPayload(task json.RawMessage) string {
	p.mu.Lock()
	p.payloadSeen++
	p.mu.Unlock()
	var head struct {
		Project struct {
			Name string `json:"name"`
		} `json:"project"`
	}
	if err := json.Unmarshal(task, &head); err != nil {
		return ""
	}
	return head.Project.Name
}

func (p *tcProjectIdentity) ByIssue(ctx context.Context, issueID string) (string, error) {
	p.mu.Lock()
	p.issueCalls = append(p.issueCalls, issueID)
	block, apiName, apiErr := p.block, p.apiName, p.apiErr
	p.mu.Unlock()
	if block {
		<-ctx.Done()
		return "", ctx.Err()
	}
	return apiName, apiErr
}

// calls returns the issue ids ByIssue was called with, in order.
func (p *tcProjectIdentity) calls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.issueCalls...)
}

// tcProjectPolicy reads the project side on the nodeSelector axis: without a
// project identity the axis degrades to no_identity and the template stays
// untouched; with one it injects the project's own definition.
const tcProjectPolicy = `
agents:
  agent-1:
    nodeSelectorPolicy: project
    nodeSelector:
      example.com/project: from-project
projects:
  proj-x:
    nodeSelector:
      example.com/project: from-project
`

// tcProjectAgentOnlyPolicy reads no project side: resourcePolicy agent only.
const tcProjectAgentOnlyPolicy = `
agents:
  agent-1:
    resourcePolicy: agent
    resources:
      limits: { cpu: "2" }
`

// tcProjectTemplate is the projection target: one container, no nodeSelector.
const tcProjectTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`

// tcPolicyEventFor returns the policy.applied event of one task.
func tcPolicyEventFor(t *testing.T, h *harness, taskID string) map[string]any {
	t.Helper()
	for _, event := range h.events("policy.applied") {
		if task, _ := event["task_id"].(string); task == taskID {
			return event
		}
	}
	t.Fatalf("no policy.applied event for task %s: %v", taskID, h.events("policy.applied"))
	return nil
}

// tcLookupFailures returns the process's project.lookup_failed events.
func tcLookupFailures(h *harness) []map[string]any {
	return h.events("project.lookup_failed")
}

func TestProjectIdentity01Sources(t *testing.T) {
	t.Run("payload-key-wins-without-lookup", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcProjectTemplate)
		policyPath := writePolicyFile(t, tcProjectPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		identity := &tcProjectIdentity{apiName: "must-not-be-called"}
		h.rebuildScheduler(t, func(c *schedulerConfig) { c.Project = identity })
		tcPrepareRuntimes(t, h)

		body := tcBaseClaim("task-1")
		body.Project = &tcProjectValue{Name: "proj-x"}
		tcClaimOnClaim(t, h, body)

		event := tcPolicyEventFor(t, h, "task-1")
		if got := tcEventString(event, "project_source"); got != "payload" {
			t.Errorf("project_source = %q, want payload", got)
		}
		if calls := identity.calls(); len(calls) != 0 {
			t.Errorf("ByIssue calls = %v, want none on the payload branch", calls)
		}
		if failures := tcLookupFailures(h); len(failures) != 0 {
			t.Errorf("project.lookup_failed events = %v, want none on the payload branch", failures)
		}
		pod := h.createdJob(t, "fm-task-1").Spec.Template.Spec
		if got := pod.NodeSelector["example.com/project"]; got != "from-project" {
			t.Errorf("nodeSelector = %v, want the project definition injected", pod.NodeSelector)
		}
	})

	t.Run("missing-key-looks-up-by-issue", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcProjectTemplate)
		policyPath := writePolicyFile(t, tcProjectPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		identity := &tcProjectIdentity{apiName: "proj-x"}
		h.rebuildScheduler(t, func(c *schedulerConfig) { c.Project = identity })
		tcPrepareRuntimes(t, h)

		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		event := tcPolicyEventFor(t, h, "task-1")
		if got := tcEventString(event, "project_source"); got != "api" {
			t.Errorf("project_source = %q, want api", got)
		}
		if calls := identity.calls(); len(calls) != 1 || calls[0] != "issue-1" {
			t.Errorf("ByIssue calls = %v, want exactly [issue-1]", calls)
		}
		if failures := tcLookupFailures(h); len(failures) != 0 {
			t.Errorf("project.lookup_failed events = %v, want none on a successful lookup", failures)
		}
		pod := h.createdJob(t, "fm-task-1").Spec.Template.Spec
		if got := pod.NodeSelector["example.com/project"]; got != "from-project" {
			t.Errorf("nodeSelector = %v, want the project definition injected", pod.NodeSelector)
		}
	})

	t.Run("no-issue-or-no-project-reads-none", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcProjectTemplate)
		policyPath := writePolicyFile(t, tcProjectPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		identity := &tcProjectIdentity{apiName: ""}
		h.rebuildScheduler(t, func(c *schedulerConfig) { c.Project = identity })
		tcPrepareRuntimes(t, h)

		// No issue id at all: the reverse lookup cannot even be addressed.
		body := tcBaseClaim("task-1")
		body.IssueID = ""
		body.IssueIdentifier = ""
		tcClaimOnClaim(t, h, body)

		// An issue id but the platform reports no project name.
		second := tcBaseClaim("task-2")
		second.IssueID = "issue-none"
		second.IssueIdentifier = ""
		tcClaimOnClaim(t, h, second)

		for _, taskID := range []string{"task-1", "task-2"} {
			event := tcPolicyEventFor(t, h, taskID)
			if got := tcEventString(event, "project_source"); got != "none" {
				t.Errorf("task %s project_source = %q, want none", taskID, got)
			}
			if got := tcEventString(event, "node_selector_source"); got != "no_identity" {
				t.Errorf("task %s node_selector_source = %q, want no_identity", taskID, got)
			}
			pod := h.createdJob(t, "fm-"+taskID).Spec.Template.Spec
			if pod.NodeSelector != nil {
				t.Errorf("task %s nodeSelector = %v, want the template untouched", taskID, pod.NodeSelector)
			}
		}
		// Only task-2 had an issue id to look up.
		if calls := identity.calls(); len(calls) != 1 || calls[0] != "issue-none" {
			t.Errorf("ByIssue calls = %v, want exactly [issue-none]", calls)
		}
		if failures := tcLookupFailures(h); len(failures) != 0 {
			t.Errorf("project.lookup_failed events = %v, want none (no project is not a failure)", failures)
		}
	})

	t.Run("lookup-failure-warns-and-continues", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcProjectTemplate)
		policyPath := writePolicyFile(t, tcProjectPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		identity := &tcProjectIdentity{apiErr: errors.New("platform unreachable")}
		h.rebuildScheduler(t, func(c *schedulerConfig) { c.Project = identity })
		tcPrepareRuntimes(t, h)

		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		// The task continues: the Job exists.
		pod := h.createdJob(t, "fm-task-1").Spec.Template.Spec
		if pod.NodeSelector != nil {
			t.Errorf("nodeSelector = %v, want the template untouched on a failed lookup", pod.NodeSelector)
		}
		event := tcPolicyEventFor(t, h, "task-1")
		if got := tcEventString(event, "project_source"); got != "none" {
			t.Errorf("project_source = %q, want none", got)
		}
		if got := tcEventString(event, "node_selector_source"); got != "no_identity" {
			t.Errorf("node_selector_source = %q, want no_identity", got)
		}
		failures := tcLookupFailures(h)
		if len(failures) != 1 {
			t.Fatalf("project.lookup_failed events = %v, want exactly one", failures)
		}
		if got := tcEventString(failures[0], "issue_id"); got != "issue-1" {
			t.Errorf("project.lookup_failed issue_id = %q, want issue-1", got)
		}
		if got := tcEventString(failures[0], "task_id"); got != "task-1" {
			t.Errorf("project.lookup_failed task_id = %q, want task-1", got)
		}
	})

	t.Run("no-project-tier-performs-no-lookup", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcProjectTemplate)
		policyPath := writePolicyFile(t, tcProjectAgentOnlyPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		identity := &tcProjectIdentity{apiName: "must-not-be-called"}
		h.rebuildScheduler(t, func(c *schedulerConfig) { c.Project = identity })
		tcPrepareRuntimes(t, h)

		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		event := tcPolicyEventFor(t, h, "task-1")
		if got := tcEventString(event, "project_source"); got != "none" {
			t.Errorf("project_source = %q, want none", got)
		}
		if got := tcEventString(event, "resource_source"); got != "direct" {
			t.Errorf("resource_source = %q, want direct (the agent tier applied)", got)
		}
		if calls := identity.calls(); len(calls) != 0 {
			t.Errorf("ByIssue calls = %v, want none when no axis reads the project side", calls)
		}
		if failures := tcLookupFailures(h); len(failures) != 0 {
			t.Errorf("project.lookup_failed events = %v, want none", failures)
		}
	})

	t.Run("lookup-is-bounded-and-the-job-is-created", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcProjectTemplate)
		policyPath := writePolicyFile(t, tcProjectPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		identity := &tcProjectIdentity{block: true}
		h.rebuildScheduler(t, func(c *schedulerConfig) {
			c.Project = identity
			c.ProjectLookupTimeout = 80 * time.Millisecond
		})
		tcPrepareRuntimes(t, h)

		start := time.Now()
		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		elapsed := time.Since(start)

		h.createdJob(t, "fm-task-1") // the claim still produced the Job
		if elapsed > 3*time.Second {
			t.Errorf("claim took %s, want it bounded well under the test's window", elapsed)
		}
		event := tcPolicyEventFor(t, h, "task-1")
		if got := tcEventString(event, "project_source"); got != "none" {
			t.Errorf("project_source = %q, want none after the timeout", got)
		}
		failures := tcLookupFailures(h)
		if len(failures) != 1 {
			t.Fatalf("project.lookup_failed events = %v, want exactly one", failures)
		}
		if got := tcEventString(failures[0], "issue_id"); got != "issue-1" {
			t.Errorf("project.lookup_failed issue_id = %q, want issue-1", got)
		}
	})

	t.Run("lookup-is-cached-per-issue", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcProjectTemplate)
		policyPath := writePolicyFile(t, tcProjectPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, nil)
		identity := &tcProjectIdentity{apiName: "proj-x"}
		h.rebuildScheduler(t, func(c *schedulerConfig) { c.Project = identity })
		tcPrepareRuntimes(t, h)

		for _, taskID := range []string{"task-1", "task-2"} {
			body := tcBaseClaim(taskID)
			body.IssueID = "issue-cache"
			body.IssueIdentifier = ""
			tcClaimOnClaim(t, h, body)
			if got := tcEventString(tcPolicyEventFor(t, h, taskID), "project_source"); got != "api" {
				t.Errorf("task %s project_source = %q, want api", taskID, got)
			}
		}
		if calls := identity.calls(); len(calls) != 1 || calls[0] != "issue-cache" {
			t.Errorf("ByIssue calls = %v, want exactly one call for issue-cache (the short-TTL cache)", calls)
		}
	})
}
