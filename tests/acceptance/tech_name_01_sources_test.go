// TC-tech-name-01 implements AC-48's name-source face (ADR-034): the
// template's metadata.name is the Job display name's only source, Foreman only
// falls back to the identity name fm-<task_id> when the template writes none,
// and a template whose name is not a legal format refuses startup instead of
// silently renaming the Job.
package acceptance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/observability"
)

// name01ReadableTemplate expresses the name format of AC-48's first criterion:
// fm-<issue-segment>-<agent-segment>-<task-key>.
const name01ReadableTemplate = `
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

// name01NamelessTemplate writes no metadata at all; name01EmptyNameTemplate
// writes metadata.name as the empty string. Both are "the template expresses
// no name format" and must render the fallback name.
const name01NamelessTemplate = `
apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`

const name01EmptyNameTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: ""
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`

// name01Manifest is a minimal legal Job whose metadata.name is the given name
// format, so a scenario can put one format under the startup gate.
func name01Manifest(format string) string {
	return `
apiVersion: batch/v1
kind: Job
metadata:
  name: ` + format + `
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`
}

// name01Claim is the claim payload the scenario dispatches: the agent name and
// the issue identifier are the two segment sources of the name face.
func name01Claim(taskID, issueIdentifier, agentName string) json.RawMessage {
	return json.RawMessage(`{
		"id": "` + taskID + `", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1",
		"issue_identifier": "` + issueIdentifier + `",
		"agent": {"name": "` + agentName + `"}
	}`)
}

// name01TaskID is the scenario task id: the task key is its last 12 characters
// after the dashes are removed.
const name01TaskID = "11111111-2222-3333-4444-555555555555"

// TestName01TemplateNameFormat (AC-48): the template's format wins. The
// created Job's metadata.name is the rendered value — normalized
// issue_identifier, normalized agent.name, the task key — and Foreman never
// overwrites it with the identity name.
func TestName01TemplateNameFormat(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilderWithTemplate(t, writeTemplateFile(t, name01ReadableTemplate))
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	if err := h.sched.OnClaim(t.Context(), name01Claim(name01TaskID, "TSI 123", "OMP Bot")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}

	// issue = "tsi-123", agent = "omp-bot", task-key = "555555555555".
	const want = "fm-tsi-123-omp-bot-555555555555"
	job := h.createdJob(t, want)
	if job.Name != want {
		t.Errorf("Job name = %q, want the template's rendered value %q", job.Name, want)
	}
	if job.Name == "fm-"+name01TaskID {
		t.Errorf("Job name = %q: the identity name was written where the template's format belongs", job.Name)
	}
	if job.Labels["app.kubernetes.io/managed-by"] != "foreman" {
		t.Errorf("Job labels = %v, want Foreman's ownership write alongside the name", job.Labels)
	}
}

// TestName01FallbackWhenTemplateWritesNoName (AC-48): a template that writes
// no metadata.name — absent or empty — renders the identity name fm-<task_id>,
// so the legacy template's behaviour is unchanged.
func TestName01FallbackWhenTemplateWritesNoName(t *testing.T) {
	for name, manifest := range map[string]string{
		"absent": name01NamelessTemplate,
		"empty":  name01EmptyNameTemplate,
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, false)
			h.useRealBuilderWithTemplate(t, writeTemplateFile(t, manifest))
			h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
			h.declare(declaredRuntimes())

			if err := h.sched.OnClaim(t.Context(), name01Claim("task-1", "TSI 123", "OMP Bot")); err != nil {
				t.Fatalf("OnClaim: %v", err)
			}
			want := "fm-task-1"
			job := h.createdJob(t, want)
			if job.Name != want {
				t.Errorf("Job name = %q, want the fallback identity name %q", job.Name, want)
			}
			if got := job.Annotations["foreman.tsic.top/daemon-id"]; got != want {
				t.Errorf("daemon-id annotation = %q, want the fallback name %q (the name is the identity here)", got, want)
			}
		})
	}
}

// TestName01IllegalFormatRefusesStartup (AC-48): every name that leaves the
// character-level closure — a static name, a raw-value variable, a registered
// placeholder outside the name face, an unregistered variable, an over-long
// worst case, a minimum rendering that is not DNS-1123 — refuses startup with
// ErrInvalidJobNameFormat. The refusal log carries the failure class and the
// template's content hash, and no Builder exists, so the illegal template can
// never be served with a silently renamed Job.
func TestName01IllegalFormatRefusesStartup(t *testing.T) {
	cases := []struct {
		name   string
		format string
	}{
		{name: "static name", format: "fm-static-name"},
		{name: "raw value variable", format: "fm-<issue-identifier>-<task-key>"},
		{name: "job-name variable", format: "fm-<job-name>-<task-key>"},
		{name: "unregistered variable", format: "fm-<bogus>-<task-key>"},
		{name: "over long worst case", format: "fm-<issue-segment>-<agent-segment>-<task-key>-<task_id>"},
		{name: "minimum rendering is not dns1123", format: "-<task-key>"},
		{name: "uppercase literal", format: "FM-<task-key>"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemplateFile(t, name01Manifest(tc.format))
			logs := &bytes.Buffer{}
			cfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			cfg.TemplatePath = path
			cfg.Logger = observability.NewLogger(logs, slog.LevelInfo, "")

			// The call main makes before it serves: an error here is the
			// non-zero exit (CrashLoopBackOff), never a renamed Job.
			builder, err := jobbuilder.NewBuilder(cfg)
			if err == nil {
				t.Fatalf("NewBuilder accepted the illegal name format %q: %+v", tc.format, builder)
			}
			if !errors.Is(err, jobbuilder.ErrInvalidJobNameFormat) {
				t.Errorf("NewBuilder error = %v, want ErrInvalidJobNameFormat", err)
			}

			// Negative: no Builder ⇒ no Build ⇒ no Job. The illegal path must
			// never silently serve with a renamed object.
			if builder != nil {
				t.Errorf("NewBuilder returned a Builder alongside the error: the illegal template would serve")
			}

			log := logs.String()
			if !strings.Contains(log, "名字格式非法") {
				t.Errorf("the refusal log must name the class 名字格式非法: %s", log)
			}
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read template: %v", err)
			}
			sum := sha256.Sum256(content)
			want := "模板内容哈希 sha256=" + hex.EncodeToString(sum[:])
			if !strings.Contains(log, want) {
				t.Errorf("the refusal log must carry %s: %s", want, log)
			}
		})
	}
}
