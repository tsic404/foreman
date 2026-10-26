// The agent-backend override entry (deploy/optional/agent-backend-mirror.sh)
// exists so a slow-egress deployment can point the shipped reference template
// at a cluster-reachable artifact mirror without forking the template. Its
// whole contract is "one line changes, everything else stays": the shipped
// ConfigMap stays byte-identical to the AC-20 baseline (25-job-template.yaml),
// and the rendered copy differs from it only in the omp init step's `base=`.
// A rewrite that touched anything else — or a script that silently rendered
// the template unchanged after the reference origin moved — would leave the
// deployer believing the mirror took effect while the Job still fetched from
// upstream.

package deploy

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	agentBackendMirrorScript    = "optional/agent-backend-mirror.sh"
	jobTemplateManifest         = "25-job-template.yaml"
	referenceAgentBackendOrigin = "https://github.com/can1357/oh-my-pi/releases/latest/download"
	agentBackendMirrorBase      = "http://ompmirror.multica-agents.svc:8080"
)

// TestAgentBackendMirrorRewritesOnlyTheOrigin holds the override to the one
// line it may change: same ConfigMap identity, same template bytes everywhere
// else, and the changed line is the init step's `base=` with the deployer's
// value. The checksum gate, the arch asset choice and the install path are
// therefore the reference template's, not a copy's.
func TestAgentBackendMirrorRewritesOnlyTheOrigin(t *testing.T) {
	shipped := decodeConfigMap(t, readFile(t, jobTemplateManifest))
	out, stderr, err := runAgentBackendMirror(t, map[string]string{
		"FOREMAN_AGENT_BACKEND_BASE": agentBackendMirrorBase,
	})
	if err != nil {
		t.Fatalf("the override entry failed:\n%s", stderr)
	}
	rendered := decodeConfigMap(t, out)
	if rendered.Name != shipped.Name || rendered.Namespace != shipped.Namespace {
		t.Errorf("rendered %s/%s, want %s/%s: the manifest is the shipped ConfigMap, not a second one",
			rendered.Namespace, rendered.Name, shipped.Namespace, shipped.Name)
	}

	want := shipped.Data["job-overlay.yaml"]
	got := rendered.Data["job-overlay.yaml"]
	if want == "" || got == "" {
		t.Fatalf("the override entry dropped job-overlay.yaml (shipped %d bytes, rendered %d bytes)", len(want), len(got))
	}
	wantLines, gotLines := strings.Split(want, "\n"), strings.Split(got, "\n")
	if len(wantLines) != len(gotLines) {
		t.Fatalf("the template changed shape: %d lines shipped, %d rendered", len(wantLines), len(gotLines))
	}
	var changed []int
	for i := range wantLines {
		if wantLines[i] != gotLines[i] {
			changed = append(changed, i)
		}
	}
	if len(changed) != 1 {
		t.Fatalf("the template differs on %d lines (%v), want exactly the omp origin line", len(changed), changed)
	}
	line := changed[0]
	if !strings.HasSuffix(strings.TrimSpace(wantLines[line]), "="+referenceAgentBackendOrigin) {
		t.Errorf("changed line %d is not the reference origin:\n%s", line+1, wantLines[line])
	}
	indent := wantLines[line][:len(wantLines[line])-len(strings.TrimLeft(wantLines[line], " \t"))]
	if want := indent + "base=" + agentBackendMirrorBase; gotLines[line] != want {
		t.Errorf("rendered origin line = %q, want %q", gotLines[line], want)
	}
}

// TestAgentBackendMirrorRefusesWithoutAnOrigin pins the fail-closed input
// handling: an empty base must not render the template (a no-op that looks
// like success would leave the Job downloading from upstream).
func TestAgentBackendMirrorRefusesWithoutAnOrigin(t *testing.T) {
	out, stderr, err := runAgentBackendMirror(t, nil)
	if err == nil {
		t.Fatalf("the override entry accepted a missing FOREMAN_AGENT_BACKEND_BASE:\n%s", out)
	}
	if out != "" {
		t.Errorf("the override entry rendered a manifest without an origin:\n%s", out)
	}
	if !strings.Contains(stderr, "FOREMAN_AGENT_BACKEND_BASE") {
		t.Errorf("the refusal does not name the missing input: %q", stderr)
	}
}

// TestAgentBackendMirrorRefusesShellSyntaxInTheOrigin keeps the value inside
// the init step's shell assignment. `base=<value>` is a shell word, and the
// same line also passes through awk: a value carrying a separator, quoting or
// substitution would change the script instead of the URL.
func TestAgentBackendMirrorRefusesShellSyntaxInTheOrigin(t *testing.T) {
	for _, base := range []string{
		"ftp://mirror.example/omp",
		"http://mirror.example/omp path",
		"http://mirror.example/omp&x",
		"http://mirror.example/omp;x",
		`http://mirror.example/omp$(x)`,
		`http://mirror.example/omp"x`,
		"http://mirror.example/omp'x",
		"http://mirror.example/omp|x",
	} {
		t.Run(base, func(t *testing.T) {
			out, stderr, err := runAgentBackendMirror(t, map[string]string{
				"FOREMAN_AGENT_BACKEND_BASE": base,
			})
			if err == nil {
				t.Fatalf("the override entry accepted %q and rendered:\n%s", base, out)
			}
			if out != "" {
				t.Errorf("the override entry rendered a manifest for %q:\n%s", base, out)
			}
			if stderr == "" {
				t.Errorf("the refusal of %q is silent", base)
			}
		})
	}
}

// TestAgentBackendMirrorFailsClosedWhenTheOriginMoves pins the drift guard: if
// a future reference template changes or drops the upstream origin, the entry
// refuses instead of emitting the template unchanged and reporting success.
func TestAgentBackendMirrorFailsClosedWhenTheOriginMoves(t *testing.T) {
	moved := strings.Replace(readFile(t, jobTemplateManifest),
		referenceAgentBackendOrigin, "https://mirror.example/pinned-release", 1)
	path := filepath.Join(t.TempDir(), jobTemplateManifest)
	if err := os.WriteFile(path, []byte(moved), 0o600); err != nil {
		t.Fatalf("write the moved template: %v", err)
	}
	out, stderr, err := runAgentBackendMirror(t, map[string]string{
		"FOREMAN_AGENT_BACKEND_BASE": agentBackendMirrorBase,
		"FOREMAN_JOB_TEMPLATE":       path,
	})
	if err == nil {
		t.Fatalf("the override entry rendered a template whose origin already moved:\n%s", out)
	}
	if out != "" {
		t.Errorf("the override entry emitted a manifest despite the moved origin:\n%s", out)
	}
	if !strings.Contains(stderr, "carries the reference origin") {
		t.Errorf("the drift refusal does not name the cause: %q", stderr)
	}
}

func runAgentBackendMirror(t *testing.T, env map[string]string) (stdout, stderr string, err error) {
	t.Helper()
	cmd := exec.Command("sh", agentBackendMirrorScript)
	// Start from PATH only: an inherited value of either input would make a
	// refusal subtest pass with the opposite of what it asserts.
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err = cmd.Run()
	return out.String(), errOut.String(), err
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(body)
}

func decodeConfigMap(t *testing.T, body string) corev1.ConfigMap {
	t.Helper()
	var configMap corev1.ConfigMap
	if err := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(body), 4096).Decode(&configMap); err != nil {
		t.Fatalf("decode the ConfigMap manifest: %v", err)
	}
	return configMap
}
