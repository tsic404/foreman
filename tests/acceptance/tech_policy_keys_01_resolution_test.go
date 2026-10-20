// TC-tech-policy-keys-01 implements AC-55 over the production platform adapter
// (internal/platform.ProjectQueries): the policy table's keys are names
// compared by exact equality only, the startup resolution reports one
// policy.name_resolution event per key with the four documented conclusions,
// a resolved project key is matched at runtime through the platform project
// identifier it resolves to (never through the name), an unavailable
// resolution downgrades the project section instead of refusing the boot, and
// two project keys resolving to the same identifier refuse startup. The keys'
// conclusions depend on the platform's authoritative answer, not on the key's
// literal shape.
package acceptance

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/platform"
)

// platformProject is one row of the platform stub's project list: the
// configured project name (title) and its opaque identifier.
type platformProject struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

// tcPolicyKeysServer is the platform stub one resolution scenario runs
// against: the workspace project list, its answer status, and a per-call
// recorder. It answers only the two read-only project-identity queries.
type tcPolicyKeysServer struct {
	mu       sync.Mutex
	projects []platformProject
	status   int
	calls    int
	gate     chan struct{}
}

func (s *tcPolicyKeysServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.calls++
	projects, status, gate := append([]platformProject(nil), s.projects...), s.status, s.gate
	s.mu.Unlock()
	if gate != nil {
		<-gate
	}
	if status != 0 && status != http.StatusOK {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":"projects unavailable"}`))
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"projects": projects, "total": len(projects)})
}

func (s *tcPolicyKeysServer) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// tcPolicyKeysResolutionPolicy carries one key of each section, so the batch
// order and the per-key events are both observable.
const tcPolicyKeysResolutionPolicy = `
agents:
  agent-1:
    nodeSelectorPolicy: agent
    nodeSelector: { example.com/pool: p1 }
projects:
  proj-x:
    nodeSelector: { example.com/pool: project-pool }
`

// tcPolicyKeysTemplate is the projection target of the harness scenarios.
const tcPolicyKeysTemplate = `
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

// tcPolicyKeysConfig builds the production builder over one policy file with
// the production platform adapter wired as its name resolver.
func tcPolicyKeysConfig(t *testing.T, templatePath, policyPath string, adapter *platform.ProjectQueries) (jobbuilder.Config, *bytes.Buffer) {
	t.Helper()
	cfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("jobbuilder.LoadConfig: %v", err)
	}
	cfg.TemplatePath = templatePath
	cfg.PolicyPath = policyPath
	cfg.JobImage = harnessJobImage
	cfg.JobNamespace = "multica-agents"
	cfg.Issuer = stubIssuer{}
	cfg.NameResolver = adapter
	logs := &bytes.Buffer{}
	cfg.Logger = observability.NewLogger(logs, slog.LevelInfo, "")
	return cfg, logs
}

// tcPolicyKeysServerFor starts the resolution stub for one scenario.
func tcPolicyKeysServerFor(t *testing.T, server *tcPolicyKeysServer) *platform.ProjectQueries {
	t.Helper()
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	return platform.NewProjectQueries(httpServer.URL, "ws-1", "mul_test-token")
}

// tcPolicyKeysConclusion is one expected per-key conclusion.
type tcPolicyKeysConclusion struct {
	kind       string
	name       string
	conclusion string
	level      string
}

// tcPolicyKeysAssertResolutions asserts the batch's per-key events against the
// expected conclusions, key for key and in order.
func tcPolicyKeysAssertResolutions(t *testing.T, events []map[string]any, want []tcPolicyKeysConclusion) {
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

func TestPolicyKeys01Resolution(t *testing.T) {
	t.Run("project-key-resolves-and-hits-by-identifier", func(t *testing.T) {
		server := &tcPolicyKeysServer{projects: []platformProject{{ID: "pid-proj-x", Title: "proj-x"}}}
		adapter := tcPolicyKeysServerFor(t, server)
		cfg, logs := tcPolicyKeysConfig(t, writeTemplateFile(t, tcPolicyKeysTemplate),
			writePolicyFile(t, tcPolicyKeysResolutionPolicy), adapter)
		if _, err := jobbuilder.NewBuilder(cfg); err != nil {
			t.Fatalf("NewBuilder: %v, want startup to continue", err)
		}
		// The agent section has no anchored workspace-wide query and reads the
		// documented unavailable degradation; the project key resolves.
		tcPolicyKeysAssertResolutions(t, tcPolicyKeysEvents(logs), []tcPolicyKeysConclusion{
			{kind: "agent", name: "agent-1", conclusion: "unavailable", level: "WARN"},
			{kind: "project", name: "proj-x", conclusion: "resolved", level: "INFO"},
		})
		if got := server.callCount(); got != 1 {
			t.Errorf("project queries = %d, want one per project key", got)
		}
	})

	t.Run("project-key-absent-refuses-startup", func(t *testing.T) {
		server := &tcPolicyKeysServer{}
		adapter := tcPolicyKeysServerFor(t, server)
		cfg, logs := tcPolicyKeysConfig(t, writeTemplateFile(t, tcPolicyKeysTemplate),
			writePolicyFile(t, tcPolicyKeysResolutionPolicy), adapter)
		_, err := jobbuilder.NewBuilder(cfg)
		if !errors.Is(err, jobbuilder.ErrInvalidPolicy) {
			t.Fatalf("NewBuilder error = %v, want ErrInvalidPolicy", err)
		}
		want := []tcPolicyKeysConclusion{
			{kind: "agent", name: "agent-1", conclusion: "unavailable", level: "WARN"},
			{kind: "project", name: "proj-x", conclusion: "absent", level: "WARN"},
		}
		tcPolicyKeysAssertResolutions(t, tcPolicyKeysEvents(logs), want)
	})

	t.Run("project-key-ambiguous-title-refuses-startup", func(t *testing.T) {
		server := &tcPolicyKeysServer{projects: []platformProject{
			{ID: "pid-1", Title: "proj-x"},
			{ID: "pid-2", Title: "proj-x"},
		}}
		adapter := tcPolicyKeysServerFor(t, server)
		cfg, logs := tcPolicyKeysConfig(t, writeTemplateFile(t, tcPolicyKeysTemplate),
			writePolicyFile(t, tcPolicyKeysResolutionPolicy), adapter)
		if _, err := jobbuilder.NewBuilder(cfg); !errors.Is(err, jobbuilder.ErrInvalidPolicy) {
			t.Fatalf("NewBuilder error = %v, want ErrInvalidPolicy", err)
		}
		events := tcPolicyKeysEvents(logs)
		if len(events) != 2 || tcEventString(events[1], "conclusion") != "ambiguous" {
			t.Errorf("events = %v, want the project key ambiguous", events)
		}
	})

	t.Run("unavailable-project-query-warns-and-degrades", func(t *testing.T) {
		server := &tcPolicyKeysServer{status: http.StatusServiceUnavailable}
		adapter := tcPolicyKeysServerFor(t, server)
		cfg, logs := tcPolicyKeysConfig(t, writeTemplateFile(t, tcPolicyKeysTemplate),
			writePolicyFile(t, tcPolicyKeysResolutionPolicy), adapter)
		builder, err := jobbuilder.NewBuilder(cfg)
		if err != nil {
			t.Fatalf("NewBuilder: %v, want startup to continue on unavailable", err)
		}
		tcPolicyKeysAssertResolutions(t, tcPolicyKeysEvents(logs), []tcPolicyKeysConclusion{
			{kind: "agent", name: "agent-1", conclusion: "unavailable", level: "WARN"},
			{kind: "project", name: "proj-x", conclusion: "unavailable", level: "WARN"},
		})
		// The project section has no identifier index, so a task carrying any
		// identifier reads no_definition for the project side — there is no
		// fallback to name matching.
		job, _, _, err := builder.Build(jobbuilder.TaskEntry{
			TaskID: "task-1", AgentID: "agent-1", WorkspaceID: "ws-1",
			JobRuntimeID: "rid-1", ClaimedAt: testClaimTime, Attempt: 1,
			AgentName: "agent-1", ProjectID: "pid-proj-x", ProjectSource: jobbuilder.ProjectSourcePayload,
		})
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		if got := job.Spec.Template.Spec.NodeSelector["example.com/pool"]; got == "project-pool" {
			t.Errorf("the project definition was injected without an identifier mapping")
		}
	})

	t.Run("timed-out-window-converges-on-unavailable", func(t *testing.T) {
		gate := make(chan struct{})
		server := &tcPolicyKeysServer{gate: gate}
		defer close(gate)
		adapter := tcPolicyKeysServerFor(t, server)
		cfg, logs := tcPolicyKeysConfig(t, writeTemplateFile(t, tcPolicyKeysTemplate),
			writePolicyFile(t, tcPolicyKeysResolutionPolicy), adapter)
		cfg.PolicyNameResolutionTimeout = 80 * time.Millisecond
		start := time.Now()
		if _, err := jobbuilder.NewBuilder(cfg); err != nil {
			t.Fatalf("NewBuilder: %v, want startup to continue when the window exhausts", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("startup resolution took %s, want it bounded by the window", elapsed)
		}
		for _, event := range tcPolicyKeysEvents(logs) {
			if got := tcEventString(event, "conclusion"); got != "unavailable" {
				t.Errorf("conclusion = %q, want unavailable past the window", got)
			}
		}
	})

	t.Run("two-project-keys-with-one-identifier-refuse-startup", func(t *testing.T) {
		server := &tcPolicyKeysServer{projects: []platformProject{
			{ID: "pid-shared", Title: "first"},
			{ID: "pid-shared", Title: "second"},
		}}
		adapter := tcPolicyKeysServerFor(t, server)
		policy := "\nagents:\n  agent-1:\n    nodeSelectorPolicy: agent\n    nodeSelector: { example.com/pool: p1 }\n" +
			"projects:\n  first:\n    nodeSelector: { example.com/pool: a }\n" +
			"  second:\n    nodeSelector: { example.com/pool: b }\n"
		cfg, logs := tcPolicyKeysConfig(t, writeTemplateFile(t, tcPolicyKeysTemplate),
			writePolicyFile(t, policy), adapter)
		if _, err := jobbuilder.NewBuilder(cfg); !errors.Is(err, jobbuilder.ErrInvalidPolicy) {
			t.Fatalf("NewBuilder error = %v, want ErrInvalidPolicy", err)
		}
		if log := logs.String(); !strings.Contains(log, "pid-shared") {
			t.Errorf("the refusal must name the shared identifier: %s", log)
		}
	})

	t.Run("uuid-shaped-key-conclusion-follows-the-authority", func(t *testing.T) {
		// A key whose literal is UUID-shaped is a name like any other: the
		// platform's authoritative answer decides, never the shape.
		const uuid = "11111111-1111-1111-1111-111111111111"
		policy := "\nprojects:\n  " + uuid + ":\n    nodeSelector: { example.com/pool: p1 }\n"
		t.Run("authority-answers-present", func(t *testing.T) {
			server := &tcPolicyKeysServer{projects: []platformProject{{ID: "pid-uuid", Title: uuid}}}
			adapter := tcPolicyKeysServerFor(t, server)
			cfg, logs := tcPolicyKeysConfig(t, writeTemplateFile(t, tcPolicyKeysTemplate),
				writePolicyFile(t, policy), adapter)
			if _, err := jobbuilder.NewBuilder(cfg); err != nil {
				t.Fatalf("NewBuilder: %v, want the UUID-shaped key to resolve", err)
			}
			events := tcPolicyKeysEvents(logs)
			if len(events) != 1 || tcEventString(events[0], "conclusion") != "resolved" {
				t.Errorf("events = %v, want the UUID-shaped key resolved", events)
			}
		})
		t.Run("authority-answers-absent", func(t *testing.T) {
			server := &tcPolicyKeysServer{}
			adapter := tcPolicyKeysServerFor(t, server)
			cfg, _ := tcPolicyKeysConfig(t, writeTemplateFile(t, tcPolicyKeysTemplate),
				writePolicyFile(t, policy), adapter)
			if _, err := jobbuilder.NewBuilder(cfg); !errors.Is(err, jobbuilder.ErrInvalidPolicy) {
				t.Fatalf("NewBuilder error = %v, want ErrInvalidPolicy for the absent key", err)
			}
		})
	})

	t.Run("one-event-per-key-one-batch-per-process", func(t *testing.T) {
		server := &tcPolicyKeysServer{projects: []platformProject{{ID: "pid-proj-x", Title: "proj-x"}}}
		adapter := tcPolicyKeysServerFor(t, server)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, writeTemplateFile(t, tcPolicyKeysTemplate),
			writePolicyFile(t, tcPolicyKeysResolutionPolicy), func(c *jobbuilder.Config) {
				c.NameResolver = adapter
			})
		tcPrepareRuntimes(t, h)

		tcPolicyKeysAssertResolutions(t, h.events("policy.name_resolution"), []tcPolicyKeysConclusion{
			{kind: "agent", name: "agent-1", conclusion: "unavailable", level: "WARN"},
			{kind: "project", name: "proj-x", conclusion: "resolved", level: "INFO"},
		})
		// Later builds do not resolve again: the batch runs once per process.
		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		tcClaimOnClaim(t, h, tcBaseClaim("task-2"))
		tcPolicyKeysAssertResolutions(t, h.events("policy.name_resolution"), []tcPolicyKeysConclusion{
			{kind: "agent", name: "agent-1", conclusion: "unavailable", level: "WARN"},
			{kind: "project", name: "proj-x", conclusion: "resolved", level: "INFO"},
		})
		if got := server.callCount(); got != 1 {
			t.Errorf("project queries = %d, want one for the whole process", got)
		}
	})

	t.Run("project-definition-hits-only-through-the-identifier", func(t *testing.T) {
		// The harness platform resolves proj-x to pid-proj-x. A claim carrying
		// that identifier injects the project definition; a claim carrying the
		// name itself does not (the name is not a runtime key).
		const policy = `
agents:
  agent-1:
    nodeSelectorPolicy: project
projects:
  proj-x:
    nodeSelector: { example.com/pool: project-pool }
`
		server := &tcPolicyKeysServer{projects: []platformProject{{ID: "pid-proj-x", Title: "proj-x"}}}
		adapter := tcPolicyKeysServerFor(t, server)
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, writeTemplateFile(t, tcPolicyKeysTemplate),
			writePolicyFile(t, policy), func(c *jobbuilder.Config) {
				c.NameResolver = adapter
			})
		h.rebuildScheduler(t, func(c *schedulerConfig) { c.Project = adapter })
		tcPrepareRuntimes(t, h)

		hit := tcBaseClaim("task-1")
		hit.ProjectID = "pid-proj-x"
		tcClaimOnClaim(t, h, hit)
		if got := tcEventString(tcPolicyEventFor(t, h, "task-1"), "node_selector_source"); got != "direct" {
			t.Errorf("identifier identity node_selector_source = %q, want direct", got)
		}
		if got := h.createdJob(t, "fm-task-1").Spec.Template.Spec.NodeSelector["example.com/pool"]; got != "project-pool" {
			t.Errorf("nodeSelector pool = %q, want the project definition", got)
		}

		byName := tcBaseClaim("task-2")
		byName.ProjectID = "proj-x"
		tcClaimOnClaim(t, h, byName)
		if got := tcEventString(tcPolicyEventFor(t, h, "task-2"), "node_selector_source"); got != "no_definition" {
			t.Errorf("name identity node_selector_source = %q, want no_definition", got)
		}
		if got := h.createdJob(t, "fm-task-2").Spec.Template.Spec.NodeSelector["example.com/pool"]; got != "" {
			t.Errorf("nodeSelector pool = %q, want the template untouched", got)
		}
	})

	t.Run("unavailable-resolution-leaves-the-agent-match-exact-name", func(t *testing.T) {
		// The platform is down: the boot continues, the agent section still
		// matches by exact name, and the project section injects nothing.
		server := &tcPolicyKeysServer{status: http.StatusServiceUnavailable}
		adapter := tcPolicyKeysServerFor(t, server)
		policy := "\nagents:\n  agent-1:\n    nodeSelectorPolicy: agent\n    nodeSelector: { example.com/pool: p1 }\n" +
			"  " + "11111111-1111-1111-1111-111111111111" + ":\n    nodeSelectorPolicy: agent\n    nodeSelector: { example.com/pool: p2 }\n"
		h := newHarness(t, false)
		h.useRealBuilderWithPolicy(t, writeTemplateFile(t, tcPolicyKeysTemplate),
			writePolicyFile(t, policy), func(c *jobbuilder.Config) {
				c.NameResolver = adapter
			})
		tcPrepareRuntimes(t, h)

		for _, event := range h.events("policy.name_resolution") {
			if got := tcEventString(event, "conclusion"); got != "unavailable" {
				t.Errorf("boot conclusion = %q, want unavailable", got)
			}
		}

		tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
		if got := tcEventString(tcPolicyEventFor(t, h, "task-1"), "node_selector_source"); got != "direct" {
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
			if got := tcEventString(tcPolicyEventFor(t, h, taskID), "node_selector_source"); got != "no_definition" {
				t.Errorf("agent %q node_selector_source = %q, want no_definition", agentName, got)
			}
			if pod := h.createdJob(t, "fm-"+taskID).Spec.Template.Spec; pod.NodeSelector != nil {
				t.Errorf("agent %q nodeSelector = %v, want the template untouched", agentName, pod.NodeSelector)
			}
		}
	})
}

// tcPolicyKeysEvents parses the policy.name_resolution events of a log buffer.
func tcPolicyKeysEvents(logs *bytes.Buffer) []map[string]any {
	var found []map[string]any
	for _, line := range bytes.Split(logs.Bytes(), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			continue
		}
		if fields["msg"] == "policy.name_resolution" {
			found = append(found, fields)
		}
	}
	return found
}
