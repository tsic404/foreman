package jobbuilder

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/observability"
)

// policyConfig points a Config at one policy file and captures its logs.
func policyConfig(t *testing.T, manifest string) (Config, *strings.Builder) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write policy: %v", err)
	}
	logs := &strings.Builder{}
	cfg := testConfig()
	cfg.PolicyPath = path
	cfg.Logger = observability.NewLogger(logs, slog.LevelInfo, "")
	return cfg, logs
}

// loadPolicy loads one policy file, failing the test on any error.
func loadPolicy(t *testing.T, manifest string) (Policy, *strings.Builder) {
	t.Helper()
	cfg, logs := policyConfig(t, manifest)
	policy, err := LoadPolicy(cfg)
	if err != nil {
		t.Fatalf("LoadPolicy: %v", err)
	}
	return policy, logs
}

// loadPolicyWithResolver loads one policy file with the platform name
// resolution seam installed, so resolved project keys produce the runtime
// identifier index.
func loadPolicyWithResolver(t *testing.T, manifest string, resolver PolicyNameResolver) (Policy, *strings.Builder) {
	t.Helper()
	cfg, logs := policyConfig(t, manifest)
	cfg.NameResolver = resolver
	policy, err := LoadPolicy(cfg)
	if err != nil {
		t.Fatalf("LoadPolicy: %v", err)
	}
	return policy, logs
}

// TestLoadPolicyMissingFileIsLegalDefault: an absent policy file is not an
// error — all three axes read None — but it is not silent either: the warn and
// the declaration sentence are the deployment's only signal that no axis is
// configured.
func TestLoadPolicyMissingFileIsLegalDefault(t *testing.T) {
	logs := &strings.Builder{}
	cfg := testConfig()
	cfg.PolicyPath = filepath.Join(t.TempDir(), "absent.yaml")
	cfg.Logger = observability.NewLogger(logs, slog.LevelInfo, "")

	policy, err := LoadPolicy(cfg)
	if err != nil {
		t.Fatalf("LoadPolicy on a missing file: %v", err)
	}
	if len(policy.agents) != 0 || len(policy.projects) != 0 {
		t.Errorf("policy = %+v, want an empty table", policy)
	}
	ev := policy.Evaluate(TaskEntry{TaskID: testTaskID, AgentName: "omp"})
	if ev.resourcePolicy != AxisNone || ev.capPolicy != AxisNone || ev.nodeSelectorPolicy != AxisNone {
		t.Errorf("tiers = %s/%s/%s, want all None", ev.resourcePolicy, ev.capPolicy, ev.nodeSelectorPolicy)
	}
	if ev.resourceSource != SourceNoDefinition || ev.capSource != SourceNoDefinition || ev.nodeSelectorSource != SourceNoDefinition {
		t.Errorf("sources = %s/%s/%s, want all no_definition",
			ev.resourceSource, ev.capSource, ev.nodeSelectorSource)
	}
	out := logs.String()
	if !strings.Contains(out, policyEffectStatement) {
		t.Errorf("startup log misses the declaration %q:\n%s", policyEffectStatement, out)
	}
	if !strings.Contains(out, "policy file missing") {
		t.Errorf("a missing policy file must warn:\n%s", out)
	}
}

// TestLoadPolicyRefusalClasses: unreadable, unparseable, unknown key and a
// definition writing drop all refuse startup — the deployment interface says a
// present file is meant as written.
func TestLoadPolicyRefusalClasses(t *testing.T) {
	cases := []struct {
		name     string
		manifest string
		wantHash bool
	}{
		{
			name:     "unknown key",
			manifest: "agents:\n  omp:\n    resourcePolicy: agent\n    resorces: {}\n",
			wantHash: true,
		},
		{
			name:     "definition writes drop",
			manifest: "agents:\n  omp:\n    capPolicy: agent\n    capabilities:\n      drop: [\"ALL\"]\n",
			wantHash: true,
		},
		{
			name:     "bad axis literal",
			manifest: "agents:\n  omp:\n    capPolicy: sometimes\n",
			wantHash: true,
		},
		{
			name:     "unparseable quantity",
			manifest: "agents:\n  omp:\n    resourcePolicy: agent\n    resources:\n      limits: { cpu: not-a-quantity }\n",
			wantHash: true,
		},
		{
			name:     "not yaml",
			manifest: "agents: [this is not a mapping\n",
			wantHash: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, logs := policyConfig(t, tc.manifest)
			_, err := LoadPolicy(cfg)
			if err == nil {
				t.Fatal("LoadPolicy accepted a broken file, want a refusal")
			}
			if !errors.Is(err, ErrInvalidPolicy) {
				t.Errorf("error = %v, want ErrInvalidPolicy", err)
			}
			out := logs.String()
			if got := strings.Contains(out, policyHashLinePrefix); got != tc.wantHash {
				t.Errorf("hash line presence = %v, want %v:\n%s", got, tc.wantHash, out)
			}
		})
	}

	t.Run("unreadable file", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "policy.yaml")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		logs := &strings.Builder{}
		cfg := testConfig()
		cfg.PolicyPath = dir
		cfg.Logger = observability.NewLogger(logs, slog.LevelInfo, "")
		if _, err := LoadPolicy(cfg); err == nil {
			t.Fatal("LoadPolicy accepted an unreadable file, want a refusal")
		}
		out := logs.String()
		if strings.Contains(out, policyHashLinePrefix) {
			t.Errorf("an unreadable file cannot carry a hash line:\n%s", out)
		}
		if !strings.Contains(out, "哈希不可得") {
			t.Errorf("the unreadable path must say the hash is out of reach:\n%s", out)
		}
	})
}

// TestLoadPolicyEmptyDocument: the shipped skeleton is comment-only — it decodes
// to an empty policy and must not crash-loop a deployment that applied it
// unedited.
func TestLoadPolicyEmptyDocument(t *testing.T) {
	policy, _ := loadPolicy(t, "# no axes configured\n")
	if len(policy.agents) != 0 || len(policy.projects) != 0 {
		t.Errorf("policy = %+v, want an empty table", policy)
	}
}

// TestLoadPolicyDeadConfigurationWarns: a definition no axis references is
// visible but never blocks the boot.
func TestLoadPolicyDeadConfigurationWarns(t *testing.T) {
	_, logs := loadPolicy(t, `
agents:
  omp:
    resourcePolicy: None
    resources:
      limits: { cpu: "2" }
projects:
  foreman:
    nodeSelector: { disktype: ssd }
`)
	out := logs.String()
	if !strings.Contains(out, "agents.omp.resources") {
		t.Errorf("dead agent definition not reported:\n%s", out)
	}
	if !strings.Contains(out, "projects.foreman.nodeSelector") {
		t.Errorf("dead project definition not reported:\n%s", out)
	}
}

// resolutionStub answers the startup name resolution from a fixed table and
// records every key it was asked about. A resolved project key gets
// "id:"+name unless the test names another identifier.
type resolutionStub struct {
	conclusions map[string]NameConclusion
	identifiers map[string]string
	err         error
	calls       []string
}

func (s *resolutionStub) ResolveName(_ context.Context, kind, name string) (NameResolution, error) {
	s.calls = append(s.calls, kind+":"+name)
	if s.err != nil {
		return NameResolution{}, s.err
	}
	conclusion := NameResolved
	if c, ok := s.conclusions[name]; ok {
		conclusion = c
	}
	identifier := ""
	if kind == PolicyKeyProject && conclusion == NameResolved {
		identifier = "id:" + name
		if id, ok := s.identifiers[name]; ok {
			identifier = id
		}
	}
	return NameResolution{Conclusion: conclusion, Identifier: identifier}, nil
}

// TestLoadPolicyNameResolution: the four conclusions per key. absent and
// ambiguous refuse the boot; unavailable only warns; a nil resolver means the
// platform offers no such query, which is the same unavailable — and every key
// is recorded as its own policy.name_resolution event.
func TestLoadPolicyNameResolution(t *testing.T) {
	const manifest = `
agents:
  omp:
    resourcePolicy: agent
projects:
  foreman:
    nodeSelector: { disktype: ssd }
`
	t.Run("resolved", func(t *testing.T) {
		cfg, logs := policyConfig(t, manifest)
		stub := &resolutionStub{}
		cfg.NameResolver = stub
		if _, err := LoadPolicy(cfg); err != nil {
			t.Fatalf("LoadPolicy: %v", err)
		}
		if len(stub.calls) != 2 {
			t.Fatalf("resolver calls = %v, want one per key", stub.calls)
		}
		for _, key := range stub.calls {
			if !strings.Contains(logs.String(), `"conclusion":"resolved"`) {
				t.Fatalf("missing a resolved conclusion for %s:\n%s", key, logs.String())
			}
		}
	})

	t.Run("absent refuses startup", func(t *testing.T) {
		cfg, logs := policyConfig(t, manifest)
		cfg.NameResolver = &resolutionStub{conclusions: map[string]NameConclusion{"omp": NameAbsent}}
		_, err := LoadPolicy(cfg)
		if err == nil || !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("LoadPolicy = %v, want an ErrInvalidPolicy refusal", err)
		}
		if !strings.Contains(logs.String(), `"conclusion":"absent"`) {
			t.Errorf("the absent conclusion is not recorded:\n%s", logs.String())
		}
	})

	t.Run("ambiguous refuses startup", func(t *testing.T) {
		cfg, _ := policyConfig(t, manifest)
		cfg.NameResolver = &resolutionStub{conclusions: map[string]NameConclusion{"foreman": NameAmbiguous}}
		if _, err := LoadPolicy(cfg); err == nil {
			t.Fatal("LoadPolicy accepted an ambiguous key")
		}
	})

	t.Run("unavailable warns and continues", func(t *testing.T) {
		cfg, logs := policyConfig(t, manifest)
		cfg.NameResolver = &resolutionStub{conclusions: map[string]NameConclusion{"omp": NameUnavailable}}
		policy, err := LoadPolicy(cfg)
		if err != nil {
			t.Fatalf("LoadPolicy: %v", err)
		}
		if !strings.Contains(logs.String(), `"conclusion":"unavailable"`) {
			t.Errorf("the unavailable conclusion is not recorded:\n%s", logs.String())
		}
		// The runtime match stays exact-name: the key resolved as unavailable
		// still hits its row by name.
		ev := policy.Evaluate(TaskEntry{TaskID: testTaskID, AgentName: "omp"})
		if ev.resourcePolicy != AxisAgent {
			t.Errorf("resource tier = %s, want agent", ev.resourcePolicy)
		}
	})

	t.Run("no resolver reads as unavailable", func(t *testing.T) {
		cfg, logs := policyConfig(t, manifest)
		if _, err := LoadPolicy(cfg); err != nil {
			t.Fatalf("LoadPolicy: %v", err)
		}
		if !strings.Contains(logs.String(), `"conclusion":"unavailable"`) {
			t.Errorf("a platform without the query must record unavailable:\n%s", logs.String())
		}
	})

	t.Run("resolver error reads as unavailable", func(t *testing.T) {
		cfg, logs := policyConfig(t, manifest)
		cfg.NameResolver = &resolutionStub{err: errors.New("platform down")}
		if _, err := LoadPolicy(cfg); err != nil {
			t.Fatalf("LoadPolicy: %v", err)
		}
		if !strings.Contains(logs.String(), `"conclusion":"unavailable"`) {
			t.Errorf("an unreachable platform must record unavailable:\n%s", logs.String())
		}
	})

	t.Run("window exhaustion converges on unavailable", func(t *testing.T) {
		cfg, logs := policyConfig(t, manifest)
		cfg.NameResolver = &resolutionStub{err: context.DeadlineExceeded}
		cfg.PolicyNameResolutionTimeout = time.Millisecond
		if _, err := LoadPolicy(cfg); err != nil {
			t.Fatalf("LoadPolicy: %v", err)
		}
		if !strings.Contains(logs.String(), `"conclusion":"unavailable"`) {
			t.Errorf("an exhausted window must record unavailable:\n%s", logs.String())
		}
	})
}

// TestLoadPolicyProjectIdentifierIndex: the project section is matched
// through the startup name → identifier index (ADR-048 decision 2/3), so the
// configured name never selects a definition at runtime; two keys resolving to
// one identifier refuse startup, and a resolved answer without an identifier
// degrades to the documented unavailable class.
func TestLoadPolicyProjectIdentifierIndex(t *testing.T) {
	const manifest = `
agents:
  omp:
    resourcePolicy: project
projects:
  rffmpeg:
    resources:
      limits: { gpu.intel.com/i915: "1" }
`
	t.Run("resolved name hits only through its identifier", func(t *testing.T) {
		policy, _ := loadPolicyWithResolver(t, manifest, &resolutionStub{
			identifiers: map[string]string{"rffmpeg": "proj-rffmpeg"},
		})
		if _, ok := policy.projectsByID["proj-rffmpeg"]; !ok {
			t.Fatalf("project index entries = %v, want proj-rffmpeg", policy.projectsByID)
		}
		hit := policy.Evaluate(TaskEntry{TaskID: testTaskID, AgentName: "omp", ProjectID: "proj-rffmpeg"})
		if hit.resourceSource != SourceDirect || hit.resources == nil {
			t.Errorf("identifier identity = %s/%+v, want direct with the definition", hit.resourceSource, hit.resources)
		}
		byName := policy.Evaluate(TaskEntry{TaskID: testTaskID, AgentName: "omp", ProjectID: "rffmpeg"})
		if byName.resourceSource != SourceNoDefinition || byName.resources != nil {
			t.Errorf("name identity = %s/%+v, want no_definition: the name is not a runtime key",
				byName.resourceSource, byName.resources)
		}
	})

	t.Run("two keys resolving to one identifier refuse startup", func(t *testing.T) {
		const two = `
agents:
  omp:
    resourcePolicy: project
projects:
  first:
    resources: { limits: { cpu: "1" } }
  second:
    resources: { limits: { cpu: "2" } }
`
		cfg, logs := policyConfig(t, two)
		cfg.NameResolver = &resolutionStub{identifiers: map[string]string{
			"first": "same-project", "second": "same-project",
		}}
		_, err := LoadPolicy(cfg)
		if !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("LoadPolicy error = %v, want ErrInvalidPolicy", err)
		}
		if !strings.Contains(logs.String(), "same-project") {
			t.Errorf("the refusal must name the shared identifier:\n%s", logs.String())
		}
	})

	t.Run("resolved project without an identifier degrades to unavailable", func(t *testing.T) {
		stub := &resolutionStub{conclusions: map[string]NameConclusion{"rffmpeg": NameResolved}}
		// Drop the stub's default identifier to model a malformed platform
		// answer.
		policy, logs := loadPolicyWithResolver(t, manifest, noIdentifierProjectResolver{stub})
		if len(policy.projectsByID) != 0 {
			t.Errorf("project index = %v, want none without an identifier", policy.projectsByID)
		}
		if !strings.Contains(logs.String(), `"conclusion":"unavailable"`) {
			t.Errorf("a resolved answer without an identifier must read unavailable:\n%s", logs.String())
		}
	})
}

// noIdentifierProjectResolver strips a project key's identifier, modelling a
// platform answer that says resolved but carries no identity.
type noIdentifierProjectResolver struct{ inner *resolutionStub }

func (r noIdentifierProjectResolver) ResolveName(ctx context.Context, kind, name string) (NameResolution, error) {
	resolution, err := r.inner.ResolveName(ctx, kind, name)
	resolution.Identifier = ""
	return resolution, err
}

// TestPolicyEvaluateAxes: the three tiers, their merge rules, the miss sources
// and the identity precedence — one table over the axes this change owns.
func TestPolicyEvaluateAxes(t *testing.T) {
	const manifest = `
agents:
  omp:
    resourcePolicy: projectWithAgent
    capPolicy: agent
    nodeSelectorPolicy: projectWithAgent
    resources:
      requests: { cpu: "2" }
      limits: { cpu: "4", memory: 8Gi }
    capabilities:
      add: ["SYS_ADMIN", "NET_ADMIN"]
    nodeSelector: { disktype: ssd }
  plain:
    capPolicy: None
    capabilities:
      add: ["SYS_ADMIN"]
projects:
  foreman:
    resources:
      requests: { cpu: "1", memory: 2Gi }
      limits: { cpu: "2" }
    capabilities:
      add: ["SYS_ADMIN", "DAC_OVERRIDE"]
    nodeSelector: { kubernetes.io/os: linux, disktype: hdd }
`
	policy, _ := loadPolicyWithResolver(t, manifest, &resolutionStub{})
	projectID := "id:foreman"
	if _, ok := policy.projectsByID[projectID]; !ok {
		t.Fatal("the resolved project key produced no runtime index entry")
	}

	t.Run("projectWithAgent merges with the agent winning", func(t *testing.T) {
		ev := policy.Evaluate(TaskEntry{TaskID: testTaskID, AgentName: "omp", ProjectID: projectID, ProjectSource: ProjectSourcePayload})
		if ev.resourcePolicy != AxisProjectWithAgent || ev.resourceSource != SourceDirect {
			t.Fatalf("resource axis = %s/%s, want projectWithAgent/direct", ev.resourcePolicy, ev.resourceSource)
		}
		if got := ev.resources.Requests.Cpu().String(); got != "2" {
			t.Errorf("requests.cpu = %s, want the agent's 2", got)
		}
		if got := ev.resources.Requests.Memory().String(); got != "2Gi" {
			t.Errorf("requests.memory = %s, want the project's 2Gi kept", got)
		}
		if got := ev.resources.Limits.Cpu().String(); got != "4" {
			t.Errorf("limits.cpu = %s, want the agent's 4", got)
		}
		if got := ev.resources.Limits.Memory().String(); got != "8Gi" {
			t.Errorf("limits.memory = %s, want the agent's 8Gi", got)
		}
		wantOverrides := []string{"node_selector:disktype", "resources.limits:cpu", "resources.requests:cpu"}
		if strings.Join(ev.overriddenKeys, ",") != strings.Join(wantOverrides, ",") {
			t.Errorf("overridden keys = %v, want %v", ev.overriddenKeys, wantOverrides)
		}
		if len(ev.capabilityAdd) != 2 || ev.capabilityAdd[0] != "SYS_ADMIN" {
			t.Errorf("capability add = %v, want the agent's two", ev.capabilityAdd)
		}
		if ev.nodeSelector["kubernetes.io/os"] != "linux" || ev.nodeSelector["disktype"] != "ssd" {
			t.Errorf("nodeSelector = %v, want the merged map", ev.nodeSelector)
		}
		if ev.projectSource != ProjectSourcePayload {
			t.Errorf("project source = %s, want payload", ev.projectSource)
		}
		if faces := strings.Join(ev.writtenFaces(), ","); faces != "resources,capabilities_add,node_selector" {
			t.Errorf("written faces = %s", faces)
		}
		if ev.suppressed != SuppressionPolicy {
			t.Errorf("soft affinity suppression = %s, want policy", ev.suppressed)
		}
	})

	t.Run("project identity unavailable outranks the agent side", func(t *testing.T) {
		ev := policy.Evaluate(TaskEntry{TaskID: testTaskID, AgentName: "omp"})
		if ev.resourceSource != SourceNoIdentity {
			t.Errorf("resource source = %s, want no_identity even though the agent definition exists", ev.resourceSource)
		}
		if ev.resources != nil {
			t.Errorf("resources = %+v, want none injected", ev.resources)
		}
	})

	t.Run("projectWithAgent with one side present takes that side", func(t *testing.T) {
		ev := policy.Evaluate(TaskEntry{TaskID: testTaskID, AgentName: "omp", ProjectID: "not-in-the-index"})
		if ev.nodeSelectorSource != SourceDirect {
			t.Errorf("nodeSelector axis = %s, want direct (the agent side exists)", ev.nodeSelectorSource)
		}
		if ev.nodeSelector["disktype"] != "ssd" {
			t.Errorf("nodeSelector = %v, want the agent's definition", ev.nodeSelector)
		}
	})

	t.Run("projectWithAgent with neither side present is a miss", func(t *testing.T) {
		ev := policy.Evaluate(TaskEntry{TaskID: testTaskID, AgentName: "plain", ProjectID: projectID})
		if ev.capSource != SourceNoDefinition {
			t.Errorf("cap source = %s, want no_definition", ev.capSource)
		}
	})

	t.Run("projectWithAgent capability merge survives absent sides", func(t *testing.T) {
		// A missing definition side is an empty list: the merge must read it as
		// one instead of dereferencing the absent definition.
		merged, overridden := mergeCapabilities(nil, &capabilityDefinition{add: []string{"SYS_ADMIN"}})
		if len(merged) != 1 || merged[0] != "SYS_ADMIN" || len(overridden) != 0 {
			t.Errorf("mergeCapabilities(nil, agent) = %v/%v, want the agent side alone", merged, overridden)
		}
		merged, overridden = mergeCapabilities(nil, nil)
		if merged != nil || overridden != nil {
			t.Errorf("mergeCapabilities(nil, nil) = %v/%v, want both empty", merged, overridden)
		}
		merged, overridden = mergeCapabilities(
			&capabilityDefinition{add: []string{"SYS_ADMIN", "DAC_OVERRIDE"}},
			&capabilityDefinition{add: []string{"SYS_ADMIN"}})
		if len(merged) != 2 || len(overridden) != 1 || overridden[0] != "capabilities:SYS_ADMIN" {
			t.Errorf("mergeCapabilities(both) = %v/%v, want the union with the shared key reported", merged, overridden)
		}
	})

	t.Run("unlisted agent keeps every tier None", func(t *testing.T) {
		ev := policy.Evaluate(TaskEntry{TaskID: testTaskID, AgentName: "ghost", ProjectID: projectID})
		if ev.resourcePolicy != AxisNone || ev.capPolicy != AxisNone || ev.nodeSelectorPolicy != AxisNone {
			t.Errorf("tiers = %s/%s/%s, want None/None/None", ev.resourcePolicy, ev.capPolicy, ev.nodeSelectorPolicy)
		}
		if len(ev.writtenFaces()) != 0 {
			t.Errorf("written faces = %v, want none", ev.writtenFaces())
		}
		if ev.suppressed != SuppressionNone {
			t.Errorf("suppression = %s, want none", ev.suppressed)
		}
	})

	t.Run("agent identity unavailable", func(t *testing.T) {
		ev := policy.Evaluate(TaskEntry{TaskID: testTaskID, ProjectID: projectID})
		if ev.capSource != SourceNoIdentity {
			t.Errorf("cap source = %s, want no_identity without an agent name", ev.capSource)
		}
	})

	t.Run("keys compare by exact equality", func(t *testing.T) {
		ev := policy.Evaluate(TaskEntry{TaskID: testTaskID, AgentName: "OMP", ProjectID: projectID})
		if ev.capPolicy != AxisNone || len(ev.capabilityAdd) != 0 {
			t.Errorf("a case variant hit the agent row: %s/%v", ev.capPolicy, ev.capabilityAdd)
		}
	})
}

// TestRequiresProjectIdentity: the reverse lookup must stay out of the claim
// path when no declared tier reads the project side.
func TestRequiresProjectIdentity(t *testing.T) {
	policy, _ := loadPolicy(t, `
agents:
  agent-only:
    resourcePolicy: agent
  project-side:
    resourcePolicy: project
  merged:
    nodeSelectorPolicy: projectWithAgent
projects:
  foreman:
    nodeSelector: { disktype: ssd }
`)
	builder := &Builder{policy: policy}
	cases := map[string]bool{
		"agent-only":   false,
		"project-side": true,
		"merged":       true,
		"ghost":        false,
	}
	for agent, want := range cases {
		if got := builder.RequiresProjectIdentity(TaskEntry{AgentName: agent}); got != want {
			t.Errorf("RequiresProjectIdentity(%q) = %v, want %v", agent, got, want)
		}
	}
}

// TestApplyPolicy: the three write faces. Resources are replaced as one block,
// capabilities are appended with the drop list untouched, nodeSelector is
// merged key by key, and initContainers stay out of the whole face.
func TestApplyPolicy(t *testing.T) {
	t.Run("resources replace the block", func(t *testing.T) {
		job := policyJobFixture()
		applyPolicy(job, Evaluation{
			resources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: mustQuantity(t, "2")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: mustQuantity(t, "4")},
			},
		})
		resources := job.Spec.Template.Spec.Containers[0].Resources
		if got := resources.Requests.Cpu().String(); got != "2" {
			t.Errorf("requests.cpu = %s, want 2", got)
		}
		if _, ok := resources.Requests[corev1.ResourceMemory]; ok {
			t.Errorf("requests = %v, want the template's memory dropped by the whole-block replacement", resources.Requests)
		}
		if got := job.Spec.Template.Spec.InitContainers[0].Resources.Requests.Cpu().String(); got != "20m" {
			t.Errorf("initContainers resources = %s, want the template's untouched", got)
		}
	})

	t.Run("capabilities append and keep drop", func(t *testing.T) {
		job := policyJobFixture()
		applyPolicy(job, Evaluation{capabilityAdd: []string{"SYS_ADMIN", "NET_ADMIN", "CHOWN"}})
		capabilities := job.Spec.Template.Spec.Containers[0].SecurityContext.Capabilities
		want := []corev1.Capability{"CHOWN", "SYS_ADMIN", "NET_ADMIN"}
		if got := capabilities.Add; len(got) != len(want) {
			t.Errorf("add = %v, want %v (appended once, already-present entry not duplicated)", got, want)
		} else {
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("add[%d] = %s, want %s", i, got[i], want[i])
				}
			}
		}
		if len(capabilities.Drop) != 1 || capabilities.Drop[0] != "ALL" {
			t.Errorf("drop = %v, want the template's [ALL] untouched", capabilities.Drop)
		}
		initCapabilities := job.Spec.Template.Spec.InitContainers[0].SecurityContext.Capabilities
		if len(initCapabilities.Add) != 1 || initCapabilities.Add[0] != "CHOWN" {
			t.Errorf("initContainers capabilities = %v, want the template's untouched", initCapabilities.Add)
		}
	})

	t.Run("nodeSelector merges key by key", func(t *testing.T) {
		job := policyJobFixture()
		applyPolicy(job, Evaluation{nodeSelector: map[string]string{"kubernetes.io/os": "windows", "disktype": "ssd"}})
		selector := job.Spec.Template.Spec.NodeSelector
		if selector["kubernetes.io/os"] != "windows" || selector["disktype"] != "ssd" || selector["zone"] != "a" {
			t.Errorf("nodeSelector = %v, want the merged map with the template's other keys kept", selector)
		}
	})

	t.Run("empty evaluation writes nothing", func(t *testing.T) {
		job := policyJobFixture()
		before := canonicalJSON(job)
		applyPolicy(job, Evaluation{})
		if after := canonicalJSON(job); after != before {
			t.Errorf("an empty evaluation changed the Job:\n%s\n%s", before, after)
		}
	})
}

// policyJobFixture is the typed Job the injection tests mutate: one container
// with its own resources, capability set and pod nodeSelector, plus an init
// container that must stay out of every write face.
func policyJobFixture() *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "fm-task", Namespace: "multica-agents"},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"zone": "a"},
					InitContainers: []corev1.Container{{
						Name:      "prepare",
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: mustQuantityRaw("20m")}},
						SecurityContext: &corev1.SecurityContext{
							Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"CHOWN"}},
						},
					}},
					Containers: []corev1.Container{{
						Name:      "agent",
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: mustQuantityRaw("2Gi")}},
						SecurityContext: &corev1.SecurityContext{
							Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"CHOWN"}},
						},
					}},
				},
			},
		},
	}
}

func mustQuantity(t *testing.T, value string) resource.Quantity {
	t.Helper()
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		t.Fatalf("ParseQuantity(%q): %v", value, err)
	}
	return quantity
}

// mustQuantityRaw is mustQuantity for fixture construction, where no testing.T
// is at hand: an unparseable literal is a test bug and must be loud.
func mustQuantityRaw(value string) resource.Quantity {
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		panic(err)
	}
	return quantity
}

// TestPolicyRefusesMultipleDocuments pins §8.5's "不可解析": the YAML reader
// keeps only the first document, so a second one must refuse startup rather
// than be dropped silently. Both documents are valid policies on their own:
// were the first invalid, strict decoding would refuse it with or without the
// document-count guard and the case would prove nothing.
func TestPolicyRefusesMultipleDocuments(t *testing.T) {
	cfg, _ := policyConfig(t, `
agents:
  alice:
    resourcePolicy: agent
---
projects:
  proj:
    nodeSelector:
      kubernetes.io/os: linux
`)
	policy, err := LoadPolicy(cfg)
	if err == nil {
		t.Fatalf("a two-document policy must refuse startup, got %+v", policy)
	}
	if !errors.Is(err, ErrInvalidPolicy) {
		t.Errorf("error = %v, want ErrInvalidPolicy", err)
	}
	if !strings.Contains(err.Error(), "must hold one YAML document, got 2") {
		t.Errorf("error = %v, want the multi-document refusal", err)
	}
}
