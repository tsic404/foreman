package jobbuilder

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// baselinePath is the AC-20 authority: a byte copy of the design repo's
// test-cases/_infra/job-baseline.yaml (docs/05-modules/job-template.md
// §Job Manifest). Editing the default template means syncing the design file
// and this copy in the same change.
const baselinePath = "testdata/job-baseline.yaml"

// TestDefaultTemplateMatchesBaseline renders with no overlay — the AC-20
// default deployment window — and diffs the objects against the baseline
// after placeholder substitution.
func TestDefaultTemplateMatchesBaseline(t *testing.T) {
	entry := testEntry()
	cfg := testConfig()
	docs := baselineDocuments(t, entry, cfg.imageRef())
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
		t.Errorf("default Job does not match the AC-20 baseline\n--- want ---\n%s\n--- got ---\n%s", want, got)
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

// jobProjection is the baseline view of a Job: metadata + spec.
func jobProjection(job *batchv1.Job) map[string]any {
	return map[string]any{"metadata": job.ObjectMeta, "spec": job.Spec}
}

// baselineDocuments reads the baseline, rewrites the documented placeholders
// with the test entry's values, and splits the multi-document YAML.
func baselineDocuments(t *testing.T, entry TaskEntry, jobImage string) [][]byte {
	t.Helper()
	raw, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatalf("read baseline: %v", err)
	}
	substitutions := map[string]string{
		"<task_id>":             entry.TaskID,
		"<agent_id>":            entry.AgentID,
		"<workspace_id>":        entry.WorkspaceID,
		"<issue_id>":            entry.IssueID,
		"<issue-identifier>":    entry.IssueIdentifier,
		"<job runtime uuid>":    entry.JobRuntimeID,
		"<RFC3339>":             entry.ClaimedAt.UTC().Format(time.RFC3339),
		"<job-image>":           jobImage,
		`"fmj_<payload>.<sig>"`: `"fmj_test.sig"`,
	}
	text := string(raw)
	for placeholder, value := range substitutions {
		text = strings.ReplaceAll(text, placeholder, value)
	}

	reader := utilyaml.NewYAMLReader(bufio.NewReader(strings.NewReader(text)))
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
