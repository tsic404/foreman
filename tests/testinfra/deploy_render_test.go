// Package testinfra pins the test cluster's render step. deploy.sh substitutes
// two operator-supplied values into the manifests' double-quoted scalars — the
// frontend entry origin and the model the local backend serves, the latter in
// its own manifest — so each has to pass sed's replacement grammar and YAML's
// escape rules and come back out as the operator wrote it. It also decides
// which manifests a round has at all: with the endpoint triple set the local
// backend is neither rendered nor applied.
package testinfra

import (
	"bytes"
	"errors"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	deployScript         = "../../testinfra/test-multica/deploy.sh"
	deployManifest       = "../../testinfra/test-multica/deploy.yaml"
	modelBackendManifest = "../../testinfra/test-multica/model-backend.yaml"
	entryPlaceholder     = "__ENTRY_URL__"
	modelPlaceholder     = "__MODEL__"
	defaultEntryURL      = "http://127.0.0.1:30300"
	defaultModel         = "qwen2.5:0.5b"
)

// endpointEnv is a complete endpoint triple with values that are obviously not
// a real deployment's: the credential in particular must never be one, and
// nothing here is ever read from the repository.
func endpointEnv() map[string]string {
	return map[string]string{
		"TEST_MULTICA_MODEL_BASE_URL": "https://endpoint.invalid/v1",
		"TEST_MULTICA_MODEL_ID":       "fake-model",
		"TEST_MULTICA_MODEL_API_KEY":  "fake-key",
	}
}

// noEndpointEnv clears the triple explicitly. The triple decides which model
// source the round has, so every path assertion must state it: an operator's
// exported endpoint would otherwise decide what a local-path test reads.
func noEndpointEnv() map[string]string {
	return map[string]string{
		"TEST_MULTICA_MODEL_BASE_URL":     "",
		"TEST_MULTICA_MODEL_ID":           "",
		"TEST_MULTICA_MODEL_API_KEY":      "",
		"TEST_MULTICA_MODEL_API_KEY_FILE": "",
	}
}

// TestDeployRendersEntryURLVerbatim covers the characters each layer would
// otherwise read as syntax: `&` is sed's whole-match reference, `|` the script
// delimiter, and `\`/`"` are YAML escapes inside a double-quoted scalar.
func TestDeployRendersEntryURLVerbatim(t *testing.T) {
	for _, tc := range []struct{ name, url string }{
		{"default", defaultEntryURL},
		{"ampersand", "http://h:30300/?a=1&b=2"},
		{"pipe", "http://h:30300/a|b"},
		{"backslash", `http://h:30300/a\b`},
		{"quote", `http://h:30300/a"b`},
		{"mixed", `http://h:30300/x?a=1&b=2|y\z"q`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rendered := renderManifest(t, tc.url)
			if strings.Contains(rendered, entryPlaceholder) {
				t.Errorf("the rendered manifest still carries %s — the value broke the substitution", entryPlaceholder)
			}
			want := map[string]string{
				"configmap/MULTICA_APP_URL":         tc.url,
				"configmap/FRONTEND_ORIGIN":         tc.url,
				"configmap/CORS_ALLOWED_ORIGINS":    tc.url,
				"deployment/test-multica/entry-url": tc.url,
			}
			if got := entrySites(t, rendered); !maps.Equal(got, want) {
				t.Errorf("rendered origin reads back as %q, want %q", got, want)
			}
		})
	}
}

// TestDeployDefaultRenderStaysLiteral pins the default path byte for byte. The
// default origin and model carry none of the characters the two escape layers
// touch, so every correct renderer — the pre-fix one included — produced
// exactly the placeholders replaced by those values; any other difference is a
// regression in what re-deploying this environment changes. Both manifests are
// pinned: deploy.yaml carries the origin, model-backend.yaml the model the
// local backend serves. The two rendered values are set explicitly: an
// operator's exported TEST_MULTICA_* must not decide what this test reads.
func TestDeployDefaultRenderStaysLiteral(t *testing.T) {
	run := runDeploy(t, renderOnlyKubectlStub, mergeEnv(noEndpointEnv(), map[string]string{
		"TEST_MULTICA_ENTRY_URL": defaultEntryURL,
		"TEST_MULTICA_MODEL":     defaultModel,
	}))
	replace := strings.NewReplacer(
		entryPlaceholder, defaultEntryURL,
		modelPlaceholder, defaultModel,
	)
	for _, tc := range []struct{ manifest, rendered string }{
		{deployManifest, run.render},
		{modelBackendManifest, run.renderModel},
	} {
		if tc.rendered == "" {
			t.Fatalf("deploy.sh never rendered %s", tc.manifest)
		}
		source, err := os.ReadFile(tc.manifest)
		if err != nil {
			t.Fatalf("read %s: %v", tc.manifest, err)
		}
		if want := replace.Replace(string(source)); tc.rendered != want {
			t.Errorf("%s's default render is no longer the literal substitution of %s and %s",
				tc.manifest, entryPlaceholder, modelPlaceholder)
		}
	}
}

// TestDeployRendersModelVerbatim pins the second rendered value where it is
// consumed: the model backend's pull step. ./reset.sh reads that value back
// from the Deployment to build the agent's selector, so a mangled render would
// bind a model the backend does not serve. An untagged override is normalized
// to the `latest` row the backend's list reports; anything else lands as
// written.
func TestDeployRendersModelVerbatim(t *testing.T) {
	for _, tc := range []struct{ name, override, want string }{
		{"tagged", "library/some-model:1.5b", "library/some-model:1.5b"},
		{"untagged", "some-model", "some-model:latest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := renderBoth(t, mergeEnv(noEndpointEnv(), map[string]string{"TEST_MULTICA_MODEL": tc.override}))

			if strings.Contains(run.renderModel, modelPlaceholder) {
				t.Fatalf("the rendered manifest still carries %s — the value broke the substitution", modelPlaceholder)
			}
			if got := backendModel(t, run.renderModel); got != tc.want {
				t.Errorf("the backend pulls %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDeployEnvOverridesInheritedValues pins the harness contract: a test value
// reaches deploy.sh even when the parent exports the same key, and the child
// sees one entry per key — the one the test set. Without it, an exported
// TEST_MULTICA_ENTRY_URL/TEST_MULTICA_MODEL decides what a test reads, and the
// byte-for-byte default render fails on an operator's machine state.
func TestDeployEnvOverridesInheritedValues(t *testing.T) {
	t.Setenv("TEST_MULTICA_ENTRY_URL", "http://inherited.example:30300")
	t.Setenv("TEST_MULTICA_MODEL", "inherited-model:1b")

	run := renderBoth(t, mergeEnv(noEndpointEnv(), map[string]string{
		"TEST_MULTICA_ENTRY_URL": "http://chosen.example:30300",
		"TEST_MULTICA_MODEL":     "chosen-model:2b",
	}))
	for _, rendered := range []string{run.render, run.renderModel} {
		if strings.Contains(rendered, "inherited") {
			t.Errorf("the render carries a value the test did not set — the child environment shadows instead of replacing")
		}
	}
	if got := entrySites(t, run.render)["configmap/MULTICA_APP_URL"]; got != "http://chosen.example:30300" {
		t.Errorf("the rendered origin reads back as %q, want the test's value", got)
	}
	if got := backendModel(t, run.renderModel); got != "chosen-model:2b" {
		t.Errorf("the backend pulls %q, want the test's model", got)
	}
}

// localBackendObjects are the objects the local model source consists of. The
// endpoint round must carry none of them: one model source per round, never two
// prerequisites.
var localBackendObjects = []struct{ kind, name string }{
	{"PersistentVolumeClaim", "ollama-models"},
	{"Service", "test-multica-ollama"},
	{"Deployment", "test-multica-ollama"},
}

// TestDeployEndpointRoundCarriesNoLocalBackend pins the model-source split. The
// triple names the endpoint that serves the round, so the local backend is not
// rendered at all — not its PVC, not its Service, not its Deployment. Without
// the triple the same objects are the round's model source and must be there:
// that is the control that keeps the assertion from passing vacuously.
func TestDeployEndpointRoundCarriesNoLocalBackend(t *testing.T) {
	t.Run("without the triple", func(t *testing.T) {
		run := renderBoth(t, noEndpointEnv())
		if run.renderModel == "" {
			t.Fatal("deploy.sh rendered no local backend without the endpoint triple — the round would have no model source")
		}
		for _, object := range localBackendObjects {
			if !manifestCarries(t, run.renderModel, object.kind, object.name) {
				t.Errorf("the rendered local backend carries no %s/%s", object.kind, object.name)
			}
		}
	})

	t.Run("with the triple", func(t *testing.T) {
		run := renderBoth(t, endpointEnv())
		if run.renderModel != "" {
			t.Errorf("deploy.sh rendered the local backend even though the endpoint serves the round:\n%s", run.renderModel)
		}
		for _, object := range localBackendObjects {
			if manifestCarries(t, run.render, object.kind, object.name) {
				t.Errorf("deploy.yaml declares %s/%s — the endpoint round must not carry the local backend", object.kind, object.name)
			}
		}
		if got := entrySites(t, run.render)["configmap/MULTICA_APP_URL"]; got != defaultEntryURL {
			t.Errorf("the endpoint round rendered the origin as %q, want %q — the model source must not disturb the rest of the manifest", got, defaultEntryURL)
		}
	})
}

// TestDeployRefusesAPartialEndpointTriple pins the fail-closed edge deploy.sh
// inherits with the triple: a half-set endpoint is an operator error, and
// falling back to the local backend would deploy a model source nobody chose
// while ./reset.sh refuses the same environment.
func TestDeployRefusesAPartialEndpointTriple(t *testing.T) {
	run := runDeploy(t, renderOnlyKubectlStub, map[string]string{
		"TEST_MULTICA_MODEL_BASE_URL":     "https://endpoint.invalid/v1",
		"TEST_MULTICA_MODEL_ID":           "",
		"TEST_MULTICA_MODEL_API_KEY":      "",
		"TEST_MULTICA_MODEL_API_KEY_FILE": "",
	})
	if run.err == nil {
		t.Fatal("deploy.sh deployed a round whose endpoint triple is incomplete")
	}
	if !strings.Contains(run.stderr, "TEST_MULTICA_MODEL_ID is unset") {
		t.Errorf("deploy.sh did not name the missing part of the triple:\n%s", run.stderr)
	}
	if run.render != "" {
		t.Errorf("deploy.sh rendered a manifest before settling the model source")
	}
}

// renderManifest runs deploy.sh with the given entry origin and every other
// test-multica value left at its lib.sh default.
func renderManifest(t *testing.T, entryURL string) string {
	t.Helper()
	return renderBoth(t, mergeEnv(noEndpointEnv(), map[string]string{"TEST_MULTICA_ENTRY_URL": entryURL})).render
}

// renderOnlyKubectlStub keeps whatever manifests deploy.sh rendered — before
// its first kubectl call, since a render-only stub never gets to an apply —
// and then stops the script.
const renderOnlyKubectlStub = `#!/bin/sh
for f in "$TMPDIR"/tmp.*/deploy.yaml; do [ -f "$f" ] && cp "$f" "$STUB_RENDER"; done
for f in "$TMPDIR"/tmp.*/model-backend.yaml; do [ -f "$f" ] && cp "$f" "$STUB_RENDER_MODEL"; done
exit 1
`

// renderBoth runs deploy.sh against a stub kubectl that keeps the manifests
// the script rendered and then fails, so the script stops at the first apply.
// `env` is applied on top of the parent environment, which is how an operator
// override reaches the render; a manifest the round does not have stays empty.
func renderBoth(t *testing.T, env map[string]string) deployRun {
	t.Helper()
	run := runDeploy(t, renderOnlyKubectlStub, env)
	if run.err == nil {
		t.Fatalf("deploy.sh ran past the render step; the stub kubectl should have stopped it")
	}
	if run.render == "" {
		t.Fatalf("deploy.sh never rendered deploy.yaml\nstderr:\n%s", run.stderr)
	}
	return run
}

// deployRun is one deploy.sh invocation against a stub kubectl.
type deployRun struct {
	render      string   // deploy.yaml as deploy.sh rendered it, empty if it never got there
	renderModel string   // model-backend.yaml as deploy.sh rendered it, empty when the round has no local backend
	calls       []string // one line per kubectl invocation, its arguments joined by spaces
	stderr      string
	err         error // deploy.sh's exit status
}

// runDeploy runs deploy.sh with `stub` standing in for kubectl; how far the
// script gets is the stub's answer, and every call it makes is recorded for the
// caller to judge. `env` overrides a single value per key — see envWith.
func runDeploy(t *testing.T, stub string, env map[string]string) deployRun {
	t.Helper()
	dir := t.TempDir()
	stubDir := filepath.Join(dir, "bin")
	tmpDir := filepath.Join(dir, "tmp")
	for _, d := range []string{stubDir, tmpDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("create %s: %v", d, err)
		}
	}
	renderPath := filepath.Join(dir, "rendered.yaml")
	modelRenderPath := filepath.Join(dir, "rendered-model-backend.yaml")
	callsPath := filepath.Join(dir, "calls")
	if err := os.WriteFile(filepath.Join(stubDir, "kubectl"), []byte(stub), 0o755); err != nil {
		t.Fatalf("write the kubectl stub: %v", err)
	}
	script, err := filepath.Abs(deployScript)
	if err != nil {
		t.Fatalf("resolve %s: %v", deployScript, err)
	}
	cmd := exec.Command("sh", script)
	cmd.Env = envWith(map[string]string{
		"PATH":              stubDir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"TMPDIR":            tmpDir,
		"STUB_RENDER":       renderPath,
		"STUB_RENDER_MODEL": modelRenderPath,
		"STUB_CALLS":        callsPath,
		// One attempt: a stub that fails on purpose must stop the script, not
		// make it sleep through the retry budget.
		"TEST_MULTICA_KUBECTL_RETRIES": "1",
	}, env)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	run := deployRun{err: cmd.Run()}
	// The stderr is complete only after the wait above.
	run.stderr = stderr.String()
	if body, err := os.ReadFile(renderPath); err == nil {
		run.render = string(body)
	}
	if body, err := os.ReadFile(modelRenderPath); err == nil {
		run.renderModel = string(body)
	}
	if body, err := os.ReadFile(callsPath); err == nil {
		if calls := strings.TrimSpace(string(body)); calls != "" {
			run.calls = strings.Split(calls, "\n")
		}
	}
	return run
}

// mergeEnv is the override layers as one map, later layers winning.
func mergeEnv(layers ...map[string]string) map[string]string {
	merged := map[string]string{}
	for _, layer := range layers {
		maps.Copy(merged, layer)
	}
	return merged
}

// envWith is a child environment: the parent's variables with every key the
// override layers name replaced, not repeated. os/exec deduplicates Cmd.Env in
// favour of the last value, so appending works today — but the value the test
// then reads would ride on that rule rather than on a single entry per key.
func envWith(layers ...map[string]string) []string {
	overrides := map[string]string{}
	for _, layer := range layers {
		maps.Copy(overrides, layer)
	}
	env := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, replaced := overrides[key]; replaced {
			continue
		}
		env = append(env, entry)
	}
	for key, value := range overrides {
		env = append(env, key+"="+value)
	}
	return env
}

// findManifestObject returns one document of a rendered manifest by kind and
// name, or nil when that manifest does not carry it.
func findManifestObject(t *testing.T, rendered, kind, name string) *unstructured.Unstructured {
	t.Helper()
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(rendered), 4096)
	for {
		var doc unstructured.Unstructured
		err := decoder.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("parse the rendered manifest: %v", err)
		}
		if doc.GetKind() == kind && doc.GetName() == name {
			return &doc
		}
	}
	return nil
}

// manifestCarries reports whether one rendered manifest declares the object.
func manifestCarries(t *testing.T, rendered, kind, name string) bool {
	t.Helper()
	return findManifestObject(t, rendered, kind, name) != nil
}

// manifestObject returns one document of the rendered manifest by kind and
// name.
func manifestObject(t *testing.T, rendered, kind, name string) *unstructured.Unstructured {
	t.Helper()
	doc := findManifestObject(t, rendered, kind, name)
	if doc == nil {
		t.Fatalf("the rendered manifest carries no %s/%s", kind, name)
	}
	return doc
}

// backendModel reads the model the rendered model backend's pull step is
// configured with — the value ./reset.sh reads back to build the agent's
// selector. `rendered` is model-backend.yaml, not deploy.yaml.
func backendModel(t *testing.T, rendered string) string {
	t.Helper()
	backend := manifestObject(t, rendered, "Deployment", "test-multica-ollama")
	initContainers, ok, err := unstructured.NestedSlice(backend.Object, "spec", "template", "spec", "initContainers")
	if err != nil || !ok || len(initContainers) == 0 {
		t.Fatalf("the model backend carries no init container: %v", err)
	}
	pull, _ := initContainers[0].(map[string]any)
	env, _, _ := unstructured.NestedSlice(pull, "env")
	for _, entry := range env {
		entry, _ := entry.(map[string]any)
		if entry["name"] != "OLLAMA_MODEL" {
			continue
		}
		value, _ := entry["value"].(string)
		return value
	}
	t.Fatal("the model backend's pull step carries no OLLAMA_MODEL")
	return ""
}

// entrySites reads the origin back through a YAML parser from every place the
// manifests carry it: the backend ConfigMap's three keys and the pod template
// annotation that rolls the server when the origin moves.
func entrySites(t *testing.T, rendered string) map[string]string {
	t.Helper()
	sites := map[string]string{}
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(rendered), 4096)
	for {
		var doc unstructured.Unstructured
		err := decoder.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("parse the rendered manifest: %v", err)
		}
		switch doc.GetKind() {
		case "ConfigMap":
			for _, key := range []string{"MULTICA_APP_URL", "FRONTEND_ORIGIN", "CORS_ALLOWED_ORIGINS"} {
				if value, ok, _ := unstructured.NestedString(doc.Object, "data", key); ok {
					sites["configmap/"+key] = value
				}
			}
		case "Deployment":
			if value, ok, _ := unstructured.NestedString(doc.Object,
				"spec", "template", "metadata", "annotations", "test-multica/entry-url"); ok {
				sites["deployment/test-multica/entry-url"] = value
			}
		}
	}
	return sites
}
