package jobbuilder

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// baselinePath is the AC-20 authority: the in-repo baseline below, whose Job
// document is the reference template the deployment publishes
// (deploy/25-job-template.yaml) — one copy each, same bytes — so editing
// the reference template means syncing this copy and the ConfigMap in the
// same change.
const baselinePath = "testdata/job-baseline.yaml"

// referenceTemplatePath writes the baseline's Job document — the reference
// template the Deployment mounts — to a file and returns its path, so a test
// builds through the same required-template gate the Deployment does.
func referenceTemplatePath(t *testing.T) string {
	t.Helper()
	docs := rawBaselineDocuments(t)
	if len(docs) != 2 {
		t.Fatalf("baseline must hold a Job and a Secret, got %d documents", len(docs))
	}
	path := filepath.Join(t.TempDir(), "job-overlay.yaml")
	if err := os.WriteFile(path, docs[0], 0o600); err != nil {
		t.Fatalf("write reference template: %v", err)
	}
	return path
}

// TestReferenceTemplateMatchesBaseline renders through the reference template
// and diffs the objects against the baseline: the AC-20 regression. Every
// shape field now comes from the template, so this diff — not a per-field
// assertion — is what holds the rendered Job to the reference shape.
func TestReferenceTemplateMatchesBaseline(t *testing.T) {
	entry := testEntry()
	cfg := testConfig()
	docs := baselineDocuments(t, entry, cfg.JobImage)
	if len(docs) != 2 {
		t.Fatalf("baseline must hold a Job and a Secret, got %d documents", len(docs))
	}
	wantJob := decodeDocument[batchv1.Job](t, docs[0])
	wantSecret := decodeDocument[corev1.Secret](t, docs[1])

	job, secret := mustBuild(t, cfg, entry)

	// apiVersion/kind come from the client-go scheme when the object is
	// created, status is never rendered: the baseline comparison covers
	// metadata + spec, the same projection TC-tech-job-01 diffs.
	if wantJob.APIVersion != "batch/v1" || wantJob.Kind != "Job" {
		t.Fatalf("baseline must declare the batch/v1 Job, got %s/%s", wantJob.APIVersion, wantJob.Kind)
	}
	if got, want := canonicalJSON(jobProjection(job)), canonicalJSON(jobProjection(&wantJob)); got != want {
		t.Errorf("reference-template Job does not match the AC-20 baseline\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
	if got, want := canonicalJSON(secret.ObjectMeta), canonicalJSON(wantSecret.ObjectMeta); got != want {
		t.Errorf("credential Secret metadata does not match the baseline\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
	if secret.Type != wantSecret.Type {
		t.Errorf("secret type = %q, want %q", secret.Type, wantSecret.Type)
	}
	token, ok := strings.CutPrefix(secret.StringData[configFileKey], `{"token":"`)
	if !ok {
		t.Fatalf("config.json is not the token document: %q", secret.StringData[configFileKey])
	}
	if token != "fmj_test.sig\"}" {
		t.Errorf("config.json = %q, want the issued Job Token", secret.StringData[configFileKey])
	}
	if wantSecret.StringData[configFileKey] == "" {
		t.Error("baseline Secret lost its config.json key")
	}
}

// jobProjection is the baseline view of a Job: metadata + spec with the
// conditional blocks removed — the trigger-context annotations and the
// node-reuse affinity — the same normalization TC-tech-job-01 performs with its
// normalize.jq before the diff. Both shapes are asserted on their own
// (TestBuildPreferredNodeAffinity, TestBuildTriggerContextAnnotations).
func jobProjection(job *batchv1.Job) map[string]any {
	metadata := *job.ObjectMeta.DeepCopy()
	for _, key := range triggerContextAnnotationKeys {
		delete(metadata.Annotations, key)
	}
	spec := *job.Spec.DeepCopy()
	spec.Template.Spec.Affinity = nil
	return map[string]any{"metadata": metadata, "spec": spec}
}

// baselineDocuments reads the baseline, rewrites the documented placeholders
// with the test entry's values, and splits the multi-document YAML.
func baselineDocuments(t *testing.T, entry TaskEntry, jobImage string) [][]byte {
	t.Helper()
	docs := rawBaselineDocuments(t)
	substitutions := map[string]string{
		"<task_id>":             entry.TaskID,
		"<agent_id>":            entry.AgentID,
		"<workspace_id>":        entry.WorkspaceID,
		"<job runtime uuid>":    entry.JobRuntimeID,
		"<RFC3339>":             entry.ClaimedAt.UTC().Format(time.RFC3339),
		"<job-image>":           jobImage,
		`"fmj_<payload>.<sig>"`: `"fmj_test.sig"`,
	}
	out := make([][]byte, 0, len(docs))
	for _, doc := range docs {
		text := string(doc)
		for placeholder, value := range substitutions {
			text = strings.ReplaceAll(text, placeholder, value)
		}
		out = append(out, []byte(text))
	}
	return out
}

// rawBaselineDocuments splits the baseline into its documents verbatim.
func rawBaselineDocuments(t *testing.T) [][]byte {
	t.Helper()
	raw, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatalf("read baseline: %v", err)
	}
	reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
	var docs [][]byte
	for {
		doc, err := reader.Read()
		if err != nil {
			break
		}
		if len(bytes.TrimSpace(doc)) > 0 {
			docs = append(docs, doc)
		}
	}
	return docs
}

func decodeDocument[T any](t *testing.T, doc []byte) T {
	t.Helper()
	var out T
	jsonBytes, err := utilyaml.ToJSON(doc)
	if err != nil {
		t.Fatalf("baseline document is not valid YAML: %v", err)
	}
	if err := json.Unmarshal(jsonBytes, &out); err != nil {
		t.Fatalf("baseline document does not decode: %v", err)
	}
	return out
}

// canonicalJSON renders an object as its canonical JSON form; comparing that
// instead of the Go structs keeps the diff readable and ignores
// serialization-only differences (resource.Quantity string caches).
func canonicalJSON(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return err.Error()
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, raw, "", "  "); err != nil {
		return string(raw)
	}
	return pretty.String()
}
