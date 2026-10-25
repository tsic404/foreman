// The model backend's init step is a shell program embedded in
// model-backend.yaml. Its failure path is what keeps a dead engine from parking
// the pod in Init: the wait is bounded and the engine's own log reaches the pod
// log. The script is run here the way the container runs it — the manifest's
// own text, with a stub `ollama` on PATH.
package testinfra

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// stubOllama is the engine as the init script sees it: `serve` either dies on
// the spot or answers, `list` either fails or reports the model.
const (
	dyingOllamaStub = `#!/bin/sh
case $1 in
serve)
	echo "stub serve: binding 0.0.0.0:11434"
	echo "FATAL: listen tcp: address already in use" >&2
	exit 1
	;;
*) exit 1 ;;
esac
`
	answeringOllamaStub = `#!/bin/sh
case $1 in
serve) echo "stub serve: listening" ;;
list) printf 'NAME\tID\tSIZE\nqwen2.5:0.5b\tdeadbeef\t397MB\n' ;;
pull) echo "stub pull $2" ;;
*) exit 1 ;;
esac
`
)

func TestModelBackendInitBoundsTheServeWait(t *testing.T) {
	script := initContainerScript(t)

	t.Run("engine never answers", func(t *testing.T) {
		// The bound is lowered so the failure path runs in about a second; the
		// production value is the script's own default.
		stdout, stderr, err := runInitScript(t, script, dyingOllamaStub)
		if err == nil {
			t.Fatalf("the init step passed with a dead engine\nstdout:\n%s", stdout)
		}
		for _, want := range []string{
			"did not answer within 1s",
			"FATAL: listen tcp: address already in use",
		} {
			if !strings.Contains(stderr, want) {
				t.Errorf("the failure does not carry %q\nstderr:\n%s", want, stderr)
			}
		}
	})

	t.Run("engine answers", func(t *testing.T) {
		stdout, stderr, err := runInitScript(t, script, answeringOllamaStub)
		if err != nil {
			t.Fatalf("the init step failed with a healthy engine: %v\nstderr:\n%s", err, stderr)
		}
		if !strings.Contains(stdout, "model qwen2.5:0.5b already present") {
			t.Errorf("the init step did not find the model on the volume\nstdout:\n%s", stdout)
		}
	})
}

// initContainerScript is the model backend's pull step, exactly as
// model-backend.yaml embeds it.
func initContainerScript(t *testing.T) string {
	t.Helper()
	source, err := os.ReadFile(modelBackendManifest)
	if err != nil {
		t.Fatalf("read %s: %v", modelBackendManifest, err)
	}
	backend := manifestObject(t, string(source), "Deployment", "test-multica-ollama")
	initContainers, ok, err := unstructured.NestedSlice(backend.Object, "spec", "template", "spec", "initContainers")
	if err != nil || !ok || len(initContainers) == 0 {
		t.Fatalf("the model backend carries no init container: %v", err)
	}
	pull, _ := initContainers[0].(map[string]any)
	args, _, _ := unstructured.NestedStringSlice(pull, "args")
	if len(args) == 0 {
		t.Fatal("the model backend's pull step carries no command")
	}
	return args[0]
}

// runInitScript runs the init script with a stub `ollama` first on PATH and the
// pull step's own environment.
func runInitScript(t *testing.T, script, stub string) (string, string, error) {
	t.Helper()
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("create %s: %v", binDir, err)
	}
	ollama := filepath.Join(binDir, "ollama")
	if err := os.WriteFile(ollama, []byte(stub), 0o755); err != nil {
		t.Fatalf("write the ollama stub: %v", err)
	}
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = envWith(map[string]string{
		"PATH":             binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"OLLAMA_MODEL":     "qwen2.5:0.5b",
		"OLLAMA_WAIT":      "1",
		"OLLAMA_SERVE_LOG": filepath.Join(dir, "serve.log"),
	})
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}
