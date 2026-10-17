// TC-tech-name-02 implements AC-49's normalization and boundary face
// (ADR-034 decision 4): each segment source normalizes by one rule — lowercase,
// runs of non-[a-z0-9] collapse to '-', trim '-', truncate to the budget, trim
// a trailing '-', then fall back — the task key is the last 12 characters of
// the dashes-stripped task id, and every produced name is DNS-1123 and fits
// the 63-character limit.
package acceptance

import (
	"bytes"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/observability"
)

// name02Template writes the readable format the table is resolved against.
const name02Template = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<issue-segment>-<agent-segment>-<task-key>
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`

// name02TaskID is the table's default task id: its compact tail is
// "555555555555".
const name02TaskID = "11111111-2222-3333-4444-555555555555"

// name02DNS1123 is the label shape every produced name must match.
var name02DNS1123 = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// name02Builder builds the production Builder over the readable template, so
// the table can read ResolveName's answers without producing objects.
func name02Builder(t *testing.T, templatePath string) *jobbuilder.Builder {
	t.Helper()
	cfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cfg.TemplatePath = templatePath
	cfg.JobImage = harnessJobImage
	cfg.Issuer = stubIssuer{}
	cfg.Logger = observability.NewLogger(&bytes.Buffer{}, slog.LevelInfo, "")
	builder, err := jobbuilder.NewBuilder(cfg)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	return builder
}

// name02Entry is one row of the normalization table: the raw segment sources,
// the task id and the name the documented rule renders.
type name02Entry struct {
	scenario   string
	issueID    string
	identifier string
	agentName  string
	taskID     string
	want       string
}

// TestName02NormalizationBoundaries (AC-49): each shape's produced name is
// exactly the documented normalization of its inputs — case folding, separator
// collapsing, non-ASCII degradation to the fallback segment, budget truncation
// with the trailing separator dropped, empty segments falling back, and the
// task key's 12-character compact tail. Every name is DNS-1123 and ≤63.
func TestName02NormalizationBoundaries(t *testing.T) {
	builder := name02Builder(t, writeTemplateFile(t, name02Template))

	entries := []name02Entry{
		{
			scenario:   "case folding and separator collapsing",
			identifier: "TSI 123", agentName: "OMP Bot", taskID: name02TaskID,
			want: "fm-tsi-123-omp-bot-555555555555",
		},
		{
			scenario:   "punctuation and dashes collapse into one separator",
			identifier: "Foo--Bar__Baz", agentName: "ops/team", taskID: name02TaskID,
			want: "fm-foo-bar-baz-ops-team-555555555555",
		},
		{
			scenario:   "all non-ASCII segments degrade to the fallback segments",
			identifier: "日本語", agentName: "агент", taskID: name02TaskID,
			want: "fm-task-agent-555555555555",
		},
		{
			scenario:   "non-ASCII inside an ASCII name collapses in place",
			identifier: "café", agentName: "Müller", taskID: name02TaskID,
			want: "fm-caf-m-ller-555555555555",
		},
		{
			scenario:   "over-long issue truncates to 20 then drops the exposed dash",
			identifier: strings.Repeat("a", 19) + "-bbb", agentName: "OMP Bot", taskID: name02TaskID,
			want: "fm-" + strings.Repeat("a", 19) + "-omp-bot-555555555555",
		},
		{
			scenario:   "over-long agent truncates to 24",
			identifier: "TSI-123", agentName: strings.Repeat("B", 30), taskID: name02TaskID,
			want: "fm-tsi-123-" + strings.Repeat("b", 24) + "-555555555555",
		},
		{
			scenario:   "no issue context falls back to the task segment",
			identifier: "", agentName: "OMP Bot", taskID: name02TaskID,
			want: "fm-task-omp-bot-555555555555",
		},
		{
			scenario: "empty identifier falls back to the issue id",
			issueID:  "PROJ-9", identifier: "", agentName: "OMP Bot", taskID: name02TaskID,
			want: "fm-proj-9-omp-bot-555555555555",
		},
		{
			scenario:   "empty agent name falls back to the agent segment",
			identifier: "TSI-123", agentName: "", taskID: name02TaskID,
			want: "fm-tsi-123-agent-555555555555",
		},
		{
			scenario:   "empty issue and agent both fall back",
			identifier: "", agentName: "", taskID: name02TaskID,
			want: "fm-task-agent-555555555555",
		},
		{
			scenario:   "task key is the last 12 of the dashes-stripped id",
			identifier: "TSI-123", agentName: "OMP Bot", taskID: "aaaa-bbbb-cccc-dddd-eeee-ffff",
			want: "fm-tsi-123-omp-bot-ddddeeeeffff",
		},
		{
			scenario:   "a short task id keeps its compact form",
			identifier: "TSI-123", agentName: "OMP Bot", taskID: "task-1",
			want: "fm-tsi-123-omp-bot-task1",
		},
	}

	for _, tc := range entries {
		t.Run(tc.scenario, func(t *testing.T) {
			entry := jobbuilder.TaskEntry{
				TaskID:          tc.taskID,
				AgentID:         "agent-1",
				WorkspaceID:     "ws-1",
				IssueID:         tc.issueID,
				IssueIdentifier: tc.identifier,
				AgentName:       tc.agentName,
			}
			got, err := builder.ResolveName(entry)
			if err != nil {
				t.Fatalf("ResolveName: %v", err)
			}
			if got != tc.want {
				t.Errorf("ResolveName = %q, want %q", got, tc.want)
			}
			if len(got) > 63 {
				t.Errorf("name %q is %d characters, over the 63 limit", got, len(got))
			}
			if !name02DNS1123.MatchString(got) {
				t.Errorf("name %q is not DNS-1123", got)
			}
		})
	}

	// Uniqueness: the mandatory task segment separates two tasks that share
	// every other input.
	first, err := builder.ResolveName(jobbuilder.TaskEntry{TaskID: name02TaskID, IssueIdentifier: "TSI-123", AgentName: "OMP Bot"})
	if err != nil {
		t.Fatalf("ResolveName(first): %v", err)
	}
	second, err := builder.ResolveName(jobbuilder.TaskEntry{TaskID: "aaaa-bbbb-cccc-dddd-eeee-ffff", IssueIdentifier: "TSI-123", AgentName: "OMP Bot"})
	if err != nil {
		t.Fatalf("ResolveName(second): %v", err)
	}
	if first == second {
		t.Errorf("two task ids produced the same name %q: the task key must make them unique", first)
	}
}

// TestName02EndToEndMatchesResolveName (AC-49): the object Build produces
// carries exactly the name ResolveName answers, so the table's fast path and
// the render path cannot drift.
func TestName02EndToEndMatchesResolveName(t *testing.T) {
	templatePath := writeTemplateFile(t, name02Template)
	builder := name02Builder(t, templatePath)

	entry := jobbuilder.TaskEntry{
		TaskID: name02TaskID, AgentID: "agent-1", WorkspaceID: "ws-1",
		JobRuntimeID: "rid-1", IssueID: "issue-1",
		IssueIdentifier: "TSI 123", AgentName: "OMP Bot",
	}
	resolved, err := builder.ResolveName(entry)
	if err != nil {
		t.Fatalf("ResolveName: %v", err)
	}
	const want = "fm-tsi-123-omp-bot-555555555555"
	if resolved != want {
		t.Fatalf("ResolveName = %q, want %q", resolved, want)
	}

	h := newHarness(t, false)
	h.useRealBuilderWithTemplate(t, templatePath)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	if err := h.sched.OnClaim(t.Context(), name01Claim(name02TaskID, "TSI 123", "OMP Bot")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	job := h.createdJob(t, resolved)
	if job.Name != resolved {
		t.Errorf("created Job name = %q, want ResolveName's answer %q", job.Name, resolved)
	}
	if h.jobs.jobCreated("fm-" + name02TaskID) {
		t.Errorf("a Job was created under the identity name %q: the template's format must win", "fm-"+name02TaskID)
	}
}
