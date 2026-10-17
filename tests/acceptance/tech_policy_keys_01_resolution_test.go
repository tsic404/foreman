// TC-tech-policy-keys-01 implements AC-55: the policy table's keys are names
// compared by exact equality only (a case or prefix variant, and a UUID-shaped
// key that names no object, never hit at render time and leave the template
// untouched), and the startup name resolution reports one policy.name_resolution
// event per key — resolved continues, absent and ambiguous refuse startup with
// ErrInvalidPolicy, unavailable warns and the boot continues with the runtime
// match still exact-name — all inside one bounded window shared by the batch,
// with no repeat on later builds.
package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// tcNameResolver is the platform-side name-resolution seam the scenarios
// install through jobbuilder.Config.NameResolver.
type tcNameResolver struct {
	conclusion jobbuilder.NameConclusion
	err        error
	block      bool

	mu   sync.Mutex
	keys []string
}

func (r *tcNameResolver) ResolveName(ctx context.Context, kind, name string) (jobbuilder.NameConclusion, error) {
	r.mu.Lock()
	r.keys = append(r.keys, kind+"/"+name)
	r.mu.Unlock()
	if r.block {
		<-ctx.Done()
		return "", ctx.Err()
	}
	return r.conclusion, r.err
}

// calledKeys returns the keys the resolver was asked about.
func (r *tcNameResolver) calledKeys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.keys...)
}

// tcResolutionPolicy carries one key of each section, so the batch order and
// the per-key events are both observable.
const tcResolutionPolicy = `
agents:
  agent-1:
    nodeSelectorPolicy: agent
    nodeSelector: { example.com/pool: p1 }
projects:
  proj-x:
    nodeSelector: { example.com/pool: p1 }
`

// tcResolutionTemplate is the projection target of the harness subtests.
const tcResolutionTemplate = `
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

// tcBuilderConfig builds a Config pointed at the test's template and policy
// files, with the module's log lines collected into a buffer.
func tcBuilderConfig(t *testing.T, templatePath, policyPath string, mutate func(*jobbuilder.Config)) (jobbuilder.Config, *bytes.Buffer) {
	t.Helper()
	cfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("jobbuilder.LoadConfig: %v", err)
	}
	cfg.TemplatePath = templatePath
	cfg.PolicyPath = policyPath
	logs := &bytes.Buffer{}
	cfg.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg, logs
}

// tcLogEvents parses the JSON log lines of one buffer carrying msg.
func tcLogEvents(logs *bytes.Buffer, msg string) []map[string]any {
	var found []map[string]any
	for _, line := range bytes.Split(logs.Bytes(), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			continue
		}
		if fields["msg"] == msg {
			found = append(found, fields)
		}
	}
	return found
}

// tcNameResolutionEvents parses the policy.name_resolution events of a buffer.
func tcNameResolutionEvents(logs *bytes.Buffer) []map[string]any {
	return tcLogEvents(logs, "policy.name_resolution")
}

// tcResolutionConclusion is one expected per-key conclusion.
type tcResolutionConclusion struct {
	kind       string
	name       string
	conclusion string
	level      string
}

// tcAssertResolutions asserts the batch's per-key events against the expected
// conclusions, key for key and in order.
func tcAssertResolutions(t *testing.T, events []map[string]any, want []tcResolutionConclusion) {
	t.Helper()
	if len(events) != len(want) {
		t.Fatalf("policy.name_resolution events = %v, want %d", events, len(want))
	}
	for i, expected := range want {
		event := events[i]
		if got := tcEventString(event, "kind"); got != expected.kind {
			t.Errorf("event %d kind = %q, want %q", i, got, expected.kind)
		}
		if got := tcEventString(event, "name"); got != expected.name {
			t.Errorf("event %d name = %q, want %q", i, got, expected.name)
		}
		if got := tcEventString(event, "conclusion"); got != expected.conclusion {
			t.Errorf("event %d conclusion = %q, want %q", i, got, expected.conclusion)
		}
		if got := tcEventString(event, "level"); got != expected.level {
			t.Errorf("event %d level = %q, want %q", i, got, expected.level)
		}
	}
}

// tcResolutionBatch is the two-key batch every direct-builder subtest resolves.
var tcResolutionBatch = []tcResolutionConclusion{
	{kind: "agent", name: "agent-1", conclusion: "resolved", level: "INFO"},
	{kind: "project", name: "proj-x", conclusion: "resolved", level: "INFO"},
}

func TestPolicyKeys01Resolution(t *testing.T) {
	t.Run("resolved-continues", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcResolutionTemplate)
		policyPath := writePolicyFile(t, tcResolutionPolicy)
		resolver := &tcNameResolver{conclusion: jobbuilder.NameResolved}
		cfg, logs := tcBuilderConfig(t, templatePath, policyPath, func(c *jobbuilder.Config) { c.NameResolver = resolver })
		if _, err := jobbuilder.NewBuilder(cfg); err != nil {
			t.Fatalf("NewBuilder: %v, want startup to continue on resolved", err)
		}
		tcAssertResolutions(t, tcNameResolutionEvents(logs), tcResolutionBatch)
		if keys := resolver.calledKeys(); len(keys) != 2 {
			t.Errorf("resolver keys = %v, want one call per key", keys)
		}
	})

	t.Run("absent-refuses-startup", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcResolutionTemplate)
		policyPath := writePolicyFile(t, tcResolutionPolicy)
		cfg, logs := tcBuilderConfig(t, templatePath, policyPath, func(c *jobbuilder.Config) {
			c.NameResolver = &tcNameResolver{conclusion: jobbuilder.NameAbsent}
		})
		_, err := jobbuilder.NewBuilder(cfg)
		if !errors.Is(err, jobbuilder.ErrInvalidPolicy) {
			t.Fatalf("NewBuilder error = %v, want ErrInvalidPolicy", err)
		}
		want := []tcResolutionConclusion{
			{kind: "agent", name: "agent-1", conclusion: "absent", level: "INFO"},
			{kind: "project", name: "proj-x", conclusion: "absent", level: "INFO"},
		}
		tcAssertResolutions(t, tcNameResolutionEvents(logs), want)
	})

	t.Run("ambiguous-refuses-startup", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcResolutionTemplate)
		policyPath := writePolicyFile(t, tcResolutionPolicy)
		cfg, logs := tcBuilderConfig(t, templatePath, policyPath, func(c *jobbuilder.Config) {
			c.NameResolver = &tcNameResolver{conclusion: jobbuilder.NameAmbiguous}
		})
		_, err := jobbuilder.NewBuilder(cfg)
		if !errors.Is(err, jobbuilder.ErrInvalidPolicy) {
			t.Fatalf("NewBuilder error = %v, want ErrInvalidPolicy", err)
		}
		want := []tcResolutionConclusion{
			{kind: "agent", name: "agent-1", conclusion: "ambiguous", level: "INFO"},
			{kind: "project", name: "proj-x", conclusion: "ambiguous", level: "INFO"},
		}
		tcAssertResolutions(t, tcNameResolutionEvents(logs), want)
	})

	t.Run("uuid-shaped-key-absent-refuses-startup", func(t *testing.T) {
		const uuid = "11111111-1111-1111-1111-111111111111"
		templatePath := writeTemplateFile(t, tcResolutionTemplate)
		policyPath := writePolicyFile(t, "\nagents:\n  "+uuid+":\n    nodeSelectorPolicy: agent\n    nodeSelector: { example.com/pool: p1 }\n")
		cfg, logs := tcBuilderConfig(t, templatePath, policyPath, func(c *jobbuilder.Config) {
			c.NameResolver = &tcNameResolver{conclusion: jobbuilder.NameAbsent}
		})
		_, err := jobbuilder.NewBuilder(cfg)
		if !errors.Is(err, jobbuilder.ErrInvalidPolicy) {
			t.Fatalf("NewBuilder error = %v, want ErrInvalidPolicy (a UUID-shaped key is not a name)", err)
		}
		want := []tcResolutionConclusion{{kind: "agent", name: uuid, conclusion: "absent", level: "INFO"}}
		tcAssertResolutions(t, tcNameResolutionEvents(logs), want)
	})

	t.Run("unavailable-warns-and-continues", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcResolutionTemplate)
		policyPath := writePolicyFile(t, tcResolutionPolicy)
		cfg, logs := tcBuilderConfig(t, templatePath, policyPath, nil) // nil resolver: no platform query at all
		if _, err := jobbuilder.NewBuilder(cfg); err != nil {
			t.Fatalf("NewBuilder: %v, want startup to continue on unavailable", err)
		}
		want := []tcResolutionConclusion{
			{kind: "agent", name: "agent-1", conclusion: "unavailable", level: "WARN"},
			{kind: "project", name: "proj-x", conclusion: "unavailable", level: "WARN"},
		}
		tcAssertResolutions(t, tcNameResolutionEvents(logs), want)
	})

	t.Run("timed-out-window-converges-on-unavailable", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcResolutionTemplate)
		policyPath := writePolicyFile(t, tcResolutionPolicy)
		cfg, logs := tcBuilderConfig(t, templatePath, policyPath, func(c *jobbuilder.Config) {
			c.NameResolver = &tcNameResolver{block: true}
			c.PolicyNameResolutionTimeout = 60 * time.Millisecond
		})
		start := time.Now()
		if _, err := jobbuilder.NewBuilder(cfg); err != nil {
			t.Fatalf("NewBuilder: %v, want startup to continue when the window exhausts", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("startup resolution took %s, want it bounded by the window", elapsed)
		}
		want := []tcResolutionConclusion{
			{kind: "agent", name: "agent-1", conclusion: "unavailable", level: "WARN"},
			{kind: "project", name: "proj-x", conclusion: "unavailable", level: "WARN"},
		}
		tcAssertResolutions(t, tcNameResolutionEvents(logs), want)
	})

	t.Run("one-event-per-key-one-batch-per-process", func(t *testing.T) {
		templatePath := writeTemplateFile(t, tcResolutionTemplate)
		policyPath := writePolicyFile(t, tcResolutionPolicy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, func(c *jobbuilder.Config) {
			c.NameResolver = &tcNameResolver{conclusion: jobbuilder.NameResolved}
		})
		tcPrepareRuntimes(t, h)

		tcAssertResolutions(t, h.events("policy.name_resolution"), tcResolutionBatch)
		// Later builds do not resolve again: the batch runs once per process.
		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		tcClaimOnClaim(t, h, tcBaseClaim("task-2"))
		tcAssertResolutions(t, h.events("policy.name_resolution"), tcResolutionBatch)
	})

	t.Run("runtime-match-stays-exact-name", func(t *testing.T) {
		// Two keys, one of them UUID-shaped: with the platform unreachable both
		// conclude unavailable, the boot continues, and the runtime comparison
		// is still exact-name — the name key hits, the variants and the
		// UUID-shaped key (which names no object) never do.
		const uuid = "11111111-1111-1111-1111-111111111111"
		policy := "\nagents:\n  agent-1:\n    nodeSelectorPolicy: agent\n    nodeSelector: { example.com/pool: p1 }\n  " +
			uuid + ":\n    nodeSelectorPolicy: agent\n    nodeSelector: { example.com/pool: p2 }\n"
		templatePath := writeTemplateFile(t, tcResolutionTemplate)
		policyPath := writePolicyFile(t, policy)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, templatePath, policyPath, func(c *jobbuilder.Config) {
			c.NameResolver = &tcNameResolver{conclusion: jobbuilder.NameUnavailable}
		})
		tcPrepareRuntimes(t, h)

		for _, event := range h.events("policy.name_resolution") {
			if got := tcEventString(event, "conclusion"); got != "unavailable" {
				t.Errorf("boot conclusion = %q, want unavailable", got)
			}
		}

		// The exact name hits and injects the agent's own definition.
		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		hit := tcPolicyEventFor(t, h, "task-1")
		if got := tcEventString(hit, "node_selector_source"); got != "direct" {
			t.Errorf("exact-name hit node_selector_source = %q, want direct", got)
		}
		if got := h.createdJob(t, "fm-task-1").Spec.Template.Spec.NodeSelector["example.com/pool"]; got != "p1" {
			t.Errorf("exact-name hit nodeSelector pool = %q, want p1", got)
		}

		for i, agentName := range []string{"Agent-1", "agent-10"} {
			taskID := "task-variant-" + string(rune('a'+i))
			body := tcBaseClaim(taskID)
			body.Agent = tcAgentObject{Name: agentName}
			tcClaimOnClaim(t, h, body)
			event := tcPolicyEventFor(t, h, taskID)
			if got := tcEventString(event, "node_selector_source"); got != "no_definition" {
				t.Errorf("agent %q node_selector_source = %q, want no_definition", agentName, got)
			}
			pod := h.createdJob(t, "fm-"+taskID).Spec.Template.Spec
			if pod.NodeSelector != nil {
				t.Errorf("agent %q nodeSelector = %v, want the template untouched", agentName, pod.NodeSelector)
			}
		}
		if got := h.createdJob(t, "fm-task-variant-a").Spec.Template.Spec.NodeSelector["example.com/pool"]; got == "p2" {
			t.Errorf("the UUID-shaped key injected p2, want it to name no object")
		}
	})
}
