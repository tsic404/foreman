// TestJobImageSkipsPublishedVersions pins 验收判据 5's skip half: a version that
// is already published must not be rebuilt or re-pushed. The gate step is driven
// with a docker stub whose `buildx imagetools inspect` answers "exists" or
// "missing" per case, and every downstream build/push step is checked through
// its own `if:` expression — so the case fails both when the gate stops
// answering from the registry probe and when a downstream step loses the
// condition (the mutation this exists for: a gate that always says skip).
package release

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestJobImageSkipsPublishedVersions(t *testing.T) {
	requireTools(t, "bash", "git")
	top := workflowEnv(t, foremanJobWorkflow)
	steps := workflowSteps(t, foremanJobWorkflow)
	resolve := requireStep(t, steps, "Resolve the upstream daemon version", foremanJobWorkflow)
	gate := requireStep(t, steps, "Skip an already-published version", foremanJobWorkflow)
	publish := requireStep(t, steps, "Build and push the image", foremanJobWorkflow)

	// Everything that would build, push or verify must hang off the gate.
	downstream := []string{
		"Build the per-platform binary",
		"Build and push the image",
	}
	for _, name := range downstream {
		step := requireStep(t, steps, name, foremanJobWorkflow)
		if !strings.Contains(step.ifExpr, "steps.gate.outputs.skip") {
			t.Fatalf("%q carries no gate condition (if: %q): a published version would be rebuilt and re-pushed", name, step.ifExpr)
		}
	}

	for _, tc := range []struct {
		label     string
		force     string
		tagExists bool
		wantSkip  string
	}{
		{label: "version not published", force: "false", tagExists: false, wantSkip: "false"},
		{label: "version already published", force: "false", tagExists: true, wantSkip: "true"},
		{label: "published but forced", force: "true", tagExists: true, wantSkip: "false"},
		{label: "not published and forced", force: "true", tagExists: false, wantSkip: "false"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			dir := t.TempDir()
			log := filepath.Join(dir, "docker.log")
			stub(t, dir, "curl", "#!/bin/sh\necho \"https://github.com/multica-ai/multica/releases/tag/v0.6.3\"\n")
			stub(t, dir, "docker", dockerStub)

			provided := map[string]string{
				"inputs.version":  "",
				"inputs.force":    tc.force,
				"github.ref_type": "branch",
				"github.ref_name": "main",
			}
			resolved := runStep(t, resolve.run, dir, dir, mergeEnv(top, resolveEnv(t, resolve.env, provided, nil)))

			gateEnv := mergeEnv(top, resolveEnv(t, gate.env, provided, nil, resolved.outputs), map[string]string{
				"STUB_DOCKER_LOG": log,
				"STUB_TAG_EXISTS": boolFlag(tc.tagExists),
			})
			gateRun := runStep(t, gate.run, dir, dir, gateEnv)
			assertOutput(t, gateRun, "skip", tc.wantSkip)

			// The workflow's own conditions decide what runs next.
			for _, name := range downstream {
				step := requireStep(t, steps, name, foremanJobWorkflow)
				want := tc.wantSkip != "true"
				if got := evalCondition(t, step.ifExpr, provided, gateRun.outputs); got != want {
					t.Errorf("%q: if %q = %v for skip=%s, want %v", name, step.ifExpr, got, tc.wantSkip, want)
				}
			}

			// A run that skips must never reach the build: run the publish step
			// only when its own condition allows it, then read the docker log.
			if evalCondition(t, publish.ifExpr, provided, gateRun.outputs) {
				publishEnv := mergeEnv(top, resolveEnv(t, publish.env, nil, map[string]string{
					"MULTICA_CLI_URL_AMD64":    "u",
					"MULTICA_CLI_SHA256_AMD64": "s",
					"MULTICA_CLI_URL_ARM64":    "u",
					"MULTICA_CLI_SHA256_ARM64": "s",
				}, resolved.outputs), map[string]string{
					"STUB_DOCKER_LOG": log,
					// the push itself creates the tag, so the post-push probe
					// must find it
					"STUB_TAG_EXISTS": "1",
				})
				runStep(t, publish.run, dir, dir, publishEnv)
			}
			body, err := os.ReadFile(log)
			if err != nil && tc.wantSkip == "false" {
				t.Fatalf("the publish step never called buildx build: %v", err)
			}
			if tc.wantSkip == "true" && strings.Contains(string(body), "BUILD") {
				t.Errorf("a skipped run still called buildx build:\n%s", body)
			}
		})
	}
}

// dockerStub answers the registry probe from STUB_TAG_EXISTS and records every
// buildx build, so the test can tell "the tag was found" from "something was
// built".
const dockerStub = `#!/bin/sh
if [ "$1 $2" = "buildx imagetools" ]; then
  if [ "$STUB_TAG_EXISTS" = "1" ]; then
    printf 'sha256:'; printf '%064d' 0; printf '\n'
    exit 0
  fi
  echo "manifest unknown: tag does not exist" >&2
  exit 1
fi
if [ "$1 $2" = "buildx build" ]; then
  shift 2
  echo "BUILD $*" >> "$STUB_DOCKER_LOG"
  exit 0
fi
exit 0
`

func boolFlag(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

var conditionExpr = regexp.MustCompile(`^(.*?)\s*(!=|==)\s*'(.*)'$`)

// evalCondition evaluates the subset of Actions expressions the release
// workflows use in `if:` — a step output (or dispatch input) compared with a
// literal, and `always()`. Anything else fails the case: the skip must be
// decided by the gate's output, not by a form this test cannot see through.
func evalCondition(t *testing.T, expr string, provided, outputs map[string]string) bool {
	t.Helper()
	condition := strings.TrimSpace(expr)
	if condition == "always()" {
		return true
	}
	match := conditionExpr.FindStringSubmatch(condition)
	if match == nil {
		t.Fatalf("unsupported condition %q", expr)
	}
	left := evalRef(t, match[1], provided, outputs)
	switch match[2] {
	case "!=":
		return left != match[3]
	case "==":
		return left == match[3]
	}
	t.Fatalf("unsupported operator in condition %q", expr)
	return false
}

func evalRef(t *testing.T, ref string, provided, outputs map[string]string) string {
	t.Helper()
	reference := strings.TrimSpace(ref)
	reference = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(reference, "${{"), "}}"))
	if match := outputExpr.FindStringSubmatch(reference); match != nil {
		value, ok := outputs[match[1]]
		if !ok {
			t.Fatalf("condition reads %q, which the step it names did not produce", reference)
		}
		return value
	}
	if inputExpr.MatchString(reference) {
		value, ok := provided[reference]
		if !ok {
			t.Fatalf("condition reads %q, which the case does not provide", reference)
		}
		return value
	}
	t.Fatalf("unsupported condition reference %q", ref)
	return ""
}
