package scheduler

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/registry"
)

const (
	fixedTestImage  = "ghcr.io/tsic404/rsbx@sha256:" + "1111111111111111111111111111111111111111111111111111111111111111"
	fixedTestProxy  = "http://foreman.foreman.svc.cluster.local:8080"
	fixedTestBudget = int64(64 << 20)
	fixedMultica    = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	fixedAgent      = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
)

func fixedRegistryEntry() registry.TaskEntry {
	return registry.TaskEntry{
		TaskID: "task-v2-1", SourceID: "src-a", WorkspaceID: "ws-1",
		AgentID: "agent-1", DaemonID: "fm-task-v2-1", JobRuntimeID: "runtime-job-1",
		Attempt: 1, PriorSessionID: "session-resume",
	}
}

func completeEnvironmentBinding() EnvironmentBinding {
	return EnvironmentBinding{
		EnvironmentID: "build-a", SlotID: "task-v2-1", RuleID: "rule-1",
		Endpoint: "build-a.example.test:7443", TLSRef: "ref/build-a",
		StateRoot: "/srv/rsbx/build-a", EnvironmentGen: "tree-1",
		EnvironmentIdentity: "sha256:tree-1-identity",
		Platform:            "linux/amd64", AgentBackend: "codex",
		MulticaVersion: "0.6.1", MulticaDigest: fixedMultica,
		AgentVersion: "0.167.1", AgentDigest: fixedAgent,
	}
}

// TestFixedAttemptSpecFreezesTheBinderFacts pins V2-C03/§6: the claim turns
// the placement into the fixed Job's exact two-CLI/environment snapshot.
func TestFixedAttemptSpecFreezesTheBinderFacts(t *testing.T) {
	raw, fixed, err := fixedAttemptSpec(fixedRegistryEntry(), completeEnvironmentBinding(), fixedTestImage, fixedTestProxy, fixedTestBudget)
	if err != nil {
		t.Fatalf("fixedAttemptSpec: %v", err)
	}
	if !fixed {
		t.Fatal("a complete environment binding did not select the fixed Job")
	}
	var binding attempt.Binding
	if err := json.Unmarshal(raw, &binding); err != nil {
		t.Fatalf("decode the frozen binding: %v", err)
	}
	if err := binding.Validate(); err != nil {
		t.Fatalf("the frozen binding is not consumable: %v", err)
	}
	if binding.EnvironmentID != "build-a" || binding.Endpoint != "build-a.example.test:7443" ||
		binding.EnvironmentGen != "tree-1" || binding.SlotID != "task-v2-1" {
		t.Fatalf("binding = %+v, want the binder's environment facts", binding)
	}
	if binding.AttemptID != registry.AttemptID("task-v2-1", 1) {
		t.Errorf("attempt id = %q, want the claim's attempt id", binding.AttemptID)
	}
	if binding.SessionID != "" {
		t.Errorf("remote session id = %q, want no frozen remote session", binding.SessionID)
	}
	if binding.Image != "ghcr.io/tsic404/rsbx" || binding.ImageDigest != "sha256:"+strings.Repeat("1", 64) {
		t.Errorf("carrier = %s@%s, want the configured rsbx digest", binding.Image, binding.ImageDigest)
	}
	if binding.Platform != "linux/amd64" || binding.ProxyURL != fixedTestProxy {
		t.Errorf("client platform/proxy = %q/%q", binding.Platform, binding.ProxyURL)
	}
	if binding.CLIBudgetBytes != fixedTestBudget {
		t.Errorf("CLI budget = %d, want %d", binding.CLIBudgetBytes, fixedTestBudget)
	}
	for _, cli := range binding.CLIs {
		if !strings.HasPrefix(cli.DownloadURL, fixedTestProxy+"/cli/") {
			t.Errorf("%s CLI download URL %q is not on the Foreman proxy", cli.Kind, cli.DownloadURL)
		}
	}
	if binding.ResumeSessionID != "session-resume" {
		t.Errorf("resume session = %q, want the Multica session path", binding.ResumeSessionID)
	}
	if len(binding.CLIs) != 2 {
		t.Fatalf("binding has %d CLIs, want exactly two", len(binding.CLIs))
	}
	if agent, ok := binding.Agent(); !ok || agent.Backend != "codex" || agent.Version != "0.167.1" {
		t.Errorf("agent CLI = %+v, want the selected codex artifact", agent)
	}
}

// TestFixedAttemptSpecKeepsLegacyBinderOnTemplate pins compatibility: a
// binder that answers only reservation ids does not silently become a fixed
// claim with guessed placement.
func TestFixedAttemptSpecKeepsLegacyBinderOnTemplate(t *testing.T) {
	raw, fixed, err := fixedAttemptSpec(fixedRegistryEntry(), EnvironmentBinding{
		EnvironmentID: "build-a", SlotID: "task-v2-1", RuleID: "rule-1",
	}, fixedTestImage, fixedTestProxy, fixedTestBudget)
	if err != nil {
		t.Fatalf("legacy binder: %v", err)
	}
	if fixed || raw != nil {
		t.Fatalf("legacy binder selected the fixed Job: fixed=%v spec=%s", fixed, raw)
	}
}

// TestFixedAttemptSpecRefusesPartialPlacement pins the no-fallback rule: a
// binder that names an environment but not its endpoint refuses the claim.
func TestFixedAttemptSpecRefusesPartialPlacement(t *testing.T) {
	partial := completeEnvironmentBinding()
	partial.Endpoint = ""
	_, fixed, err := fixedAttemptSpec(fixedRegistryEntry(), partial, fixedTestImage, fixedTestProxy, fixedTestBudget)
	if err == nil {
		t.Fatal("partial placement was accepted")
	}
	if !fixed {
		t.Fatal("partial placement did not select the fixed path")
	}
	if !strings.Contains(err.Error(), "environment endpoint") {
		t.Fatalf("error = %v, want the missing endpoint", err)
	}
}

// TestFixedAttemptSpecNamesTheAttemptConfigSecret pins that the claim and the
// render agree on the one config projection Secret.
func TestFixedAttemptSpecNamesTheAttemptConfigSecret(t *testing.T) {
	binding := completeEnvironmentBinding()
	binding.ConfigRevision = 7
	binding.ConfigSecretRef = "ref/agent-config"
	binding.ConfigFileName = "config.toml"
	binding.ConfigDigest = "sha256:config"
	raw, fixed, err := fixedAttemptSpec(fixedRegistryEntry(), binding, fixedTestImage, fixedTestProxy, fixedTestBudget)
	if err != nil || !fixed {
		t.Fatalf("fixedAttemptSpec = (fixed=%v, err=%v)", fixed, err)
	}
	var spec attempt.Binding
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("decode the frozen binding: %v", err)
	}
	if want := agentconfig.SecretNameForAttempt(spec.AttemptID); spec.Config.SecretName != want {
		t.Errorf("projection Secret = %q, want %q", spec.Config.SecretName, want)
	}
	if spec.Config.FileName != "config.toml" || spec.Config.Revision != 7 {
		t.Errorf("config = %+v, want the frozen revision/file", spec.Config)
	}
}
