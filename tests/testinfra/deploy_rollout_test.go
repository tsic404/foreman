// Rollout coverage for deploy.sh. Its wait list names objects, and a name that
// no manifest declares is only observable against a cluster: kubectl answers
// NotFound, the script dies on that object, and every later object in the list
// is never waited for. The stub below answers every call the script makes up to
// the rollout, so the list can be read back and judged against the Deployments
// deploy.yaml actually declares.
package testinfra

import (
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const answeringKubectlStub = `#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$STUB_CALLS"
case $1 in
create)
	printf 'apiVersion: v1\nkind: Namespace\nmetadata:\n  name: multica-test\n'
	;;
apply)
	# The rendered manifests: the deployment, and — without an endpoint — the
	# local model backend. Which of them arrive is the round's answer.
	case ${3##*/} in
	deploy.yaml) cp "$3" "$STUB_RENDER" ;;
	model-backend.yaml) cp "$3" "$STUB_RENDER_MODEL" ;;
	esac
	;;
get)
	case $2 in
	service) printf '{"items":[]}' ;;
	secret) printf 'secret/test-multica-secrets\n' ;;
	*) exit 1 ;;
	esac
	;;
rollout) ;;
*) exit 1 ;;
esac
`

func TestDeployWaitsForEveryDeployedObject(t *testing.T) {
	run := runDeploy(t, answeringKubectlStub, noEndpointEnv())
	if run.err != nil {
		t.Fatalf("deploy.sh did not finish: %v\nstderr:\n%s", run.err, run.stderr)
	}
	if run.render == "" {
		t.Fatalf("deploy.sh never rendered a manifest\nstderr:\n%s", run.stderr)
	}

	waited, budgets := rolloutWait(t, run)
	judgeWaitList(t, waited, manifestDeployments(t, run.render, run.renderModel))

	// The model backend's init step downloads the model on the first rollout, so
	// its budget is the long one; every other object gets the standard budget. A
	// branch keyed to a name nothing declares would quietly apply the standard
	// timeout to the slow object instead of failing loudly.
	for name, want := range map[string]string{
		"postgres":            "300s",
		"test-multica-ollama": "900s",
		"backend":             "300s",
		"web":                 "300s",
		"proxy":               "300s",
	} {
		if got := budgets[name]; got != want {
			t.Errorf("deploy.sh waits for deploy/%s with --timeout=%s, want %s", name, got, want)
		}
	}
}

// TestDeployEndpointRoundWaitsOnlyForWhatItDeploys is the same invariant on the
// other model-source path: the endpoint serves the round, so the local backend
// is not applied — and waiting for an object no manifest declares would answer
// NotFound and kill the deployment entry on that name.
func TestDeployEndpointRoundWaitsOnlyForWhatItDeploys(t *testing.T) {
	run := runDeploy(t, answeringKubectlStub, endpointEnv())
	if run.err != nil {
		t.Fatalf("deploy.sh did not finish: %v\nstderr:\n%s", run.err, run.stderr)
	}
	if run.renderModel != "" {
		t.Errorf("the endpoint round applied a local model backend:\n%s", run.renderModel)
	}

	waited, budgets := rolloutWait(t, run)
	judgeWaitList(t, waited, manifestDeployments(t, run.render, run.renderModel))

	if slices.Contains(waited, "test-multica-ollama") {
		t.Errorf("deploy.sh waits for the local model backend the endpoint round does not deploy")
	}
	if want := map[string]string{
		"postgres": "300s",
		"backend":  "300s",
		"web":      "300s",
		"proxy":    "300s",
	}; !maps.Equal(budgets, want) {
		t.Errorf("the endpoint round's rollout budgets are %v, want %v", budgets, want)
	}
}

// rolloutWait is the wait list deploy.sh issued, with each object's budget.
func rolloutWait(t *testing.T, run deployRun) ([]string, map[string]string) {
	t.Helper()
	waited := []string{}
	budgets := map[string]string{}
	for _, call := range run.calls {
		fields := strings.Fields(call)
		if len(fields) < 3 || fields[0] != "rollout" || fields[1] != "status" {
			continue
		}
		name, ok := strings.CutPrefix(fields[2], "deploy/")
		if !ok {
			t.Fatalf("the rollout wait names %q, not a Deployment", fields[2])
		}
		waited = append(waited, name)
		for _, field := range fields[3:] {
			if budget, ok := strings.CutPrefix(field, "--timeout="); ok {
				budgets[name] = budget
			}
		}
	}
	if len(waited) == 0 {
		t.Fatal("deploy.sh never waited for a rollout; the stub answers every call, so nothing stopped it early")
	}
	return waited, budgets
}

// judgeWaitList asserts the wait list and the manifests agree both ways: every
// waited object is one this round applied, and every Deployment it applied is
// waited for.
func judgeWaitList(t *testing.T, waited []string, declared map[string]bool) {
	t.Helper()
	for _, name := range waited {
		if !declared[name] {
			t.Errorf("deploy.sh waits for deploy/%s, which no manifest declares: rollout status answers NotFound and the deployment entry dies on that object", name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(declared)) {
		if !slices.Contains(waited, name) {
			t.Errorf("deploy.sh never waits for deploy/%s, which the manifest declares", name)
		}
	}
}

// manifestDeployments is the set of Deployment names the given rendered
// manifests declare between them.
func manifestDeployments(t *testing.T, rendered ...string) map[string]bool {
	t.Helper()
	names := map[string]bool{}
	for _, manifest := range rendered {
		decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(manifest), 4096)
		for {
			var doc unstructured.Unstructured
			err := decoder.Decode(&doc)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("parse the rendered manifest: %v", err)
			}
			if doc.GetKind() == "Deployment" {
				names[doc.GetName()] = true
			}
		}
	}
	return names
}
