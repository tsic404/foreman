// Package testinfra pins the test cluster's render step. deploy.sh substitutes
// one operator-supplied origin into deploy.yaml's double-quoted scalars, so the
// value has to pass sed's replacement grammar and YAML's escape rules and come
// back out as the operator wrote it.
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
	deployScript     = "../../testinfra/test-multica/deploy.sh"
	deployManifest   = "../../testinfra/test-multica/deploy.yaml"
	entryPlaceholder = "__ENTRY_URL__"
	defaultEntryURL  = "http://127.0.0.1:30300"
)

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
// default origin carries none of the characters the two escape layers touch, so
// every correct renderer — the pre-fix one included — produced exactly the
// placeholder replaced by the value; any other difference is a regression in
// what re-deploying this environment changes.
func TestDeployDefaultRenderStaysLiteral(t *testing.T) {
	source, err := os.ReadFile(deployManifest)
	if err != nil {
		t.Fatalf("read %s: %v", deployManifest, err)
	}
	rendered := renderManifest(t, defaultEntryURL)
	if want := strings.ReplaceAll(string(source), entryPlaceholder, defaultEntryURL); rendered != want {
		t.Errorf("the default render is no longer the literal substitution of %s", entryPlaceholder)
	}
}

// renderManifest runs deploy.sh against a stub kubectl that keeps the manifest
// the script rendered and then fails, so the script stops at the first apply.
func renderManifest(t *testing.T, entryURL string) string {
	t.Helper()
	dir := t.TempDir()
	stubDir := filepath.Join(dir, "bin")
	tmpDir := filepath.Join(dir, "tmp")
	for _, d := range []string{stubDir, tmpDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("create %s: %v", d, err)
		}
	}
	rendered := filepath.Join(dir, "rendered.yaml")
	stub := "#!/bin/sh\n" +
		`for f in "$TMPDIR"/tmp.*/deploy.yaml; do cp "$f" "$STUB_RENDER"; done` + "\n" +
		"exit 1\n"
	if err := os.WriteFile(filepath.Join(stubDir, "kubectl"), []byte(stub), 0o755); err != nil {
		t.Fatalf("write the kubectl stub: %v", err)
	}
	script, err := filepath.Abs(deployScript)
	if err != nil {
		t.Fatalf("resolve %s: %v", deployScript, err)
	}
	cmd := exec.Command("sh", script)
	cmd.Env = append(os.Environ(),
		"PATH="+stubDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TMPDIR="+tmpDir,
		"STUB_RENDER="+rendered,
		// One attempt: the stub fails on purpose, and a retry only sleeps.
		"TEST_MULTICA_KUBECTL_RETRIES=1",
		"TEST_MULTICA_ENTRY_URL="+entryURL,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatalf("deploy.sh ran past the render step; the stub kubectl should have stopped it")
	}
	body, err := os.ReadFile(rendered)
	if err != nil {
		t.Fatalf("deploy.sh never rendered a manifest: %v\nstderr:\n%s", err, stderr.String())
	}
	return string(body)
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
