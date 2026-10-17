// TC-tech-job-01 implements AC-20's reference-template regression in the form
// this repo can execute: the ConfigMap the deployment publishes is the design
// authority's Job document byte for byte, its content self-proof holds (both
// <job-image> placeholders and the <preferred node> recognition item), and a
// task Job built by the real components against it matches the baseline after
// the design case's normalization. The design case's steps that need a cluster
// (apiserver defaults, node identity) stay QA-side.
package acceptance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/tsic404/foreman/internal/registry"
)

// authorityFixture is the AC-20 authority, the same byte copy of the design
// repo's test-cases/_infra/job-baseline.yaml the jobbuilder package tests
// against.
const authorityFixture = "../../internal/jobbuilder/testdata/job-baseline.yaml"

func TestJob01ReferenceTemplateMatchesBaseline(t *testing.T) {
	docs := authorityDocuments(t)
	if len(docs) != 2 {
		t.Fatalf("authority must hold a Job and a Secret, got %d documents", len(docs))
	}
	authority := string(docs[0])

	// Step 3a's window precondition: what the deployment publishes is the
	// authority's Job document, not a copy that drifted.
	published := publishedTemplateContent(t)
	if published != authority {
		t.Errorf("deploy/25-job-template.yaml is not the authority's Job document byte for byte\n--- authority ---\n%s\n--- published ---\n%s",
			authority, published)
	}

	// Step 3a's content self-proof: the two <job-image> placeholders and the
	// recognition item the soft-affinity cases need.
	templateJob := decodeJob(t, []byte(published))
	images := []string{
		templateJob.Spec.Template.Spec.Containers[0].Image,
		templateJob.Spec.Template.Spec.InitContainers[0].Image,
	}
	for i, image := range images {
		if image != "<job-image>" {
			t.Errorf("template container %d image = %q, want the verbatim <job-image>", i, image)
		}
	}
	if templateJob.Spec.Template.Spec.Affinity == nil || templateJob.Spec.Template.Spec.Affinity.NodeAffinity == nil {
		t.Fatal("the reference template must carry the <preferred node> recognition item")
	}
	terms := templateJob.Spec.Template.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 || terms[0].Weight != 100 {
		t.Fatalf("preferred terms = %+v, want exactly one of weight 100", terms)
	}
	expressions := terms[0].Preference.MatchExpressions
	if len(expressions) != 1 || expressions[0].Key != "kubernetes.io/hostname" ||
		expressions[0].Operator != corev1.NodeSelectorOpIn || len(expressions[0].Values) != 1 ||
		expressions[0].Values[0] != "<preferred node>" {
		t.Errorf("recognition item = %+v, want hostname In [<preferred node>]", expressions)
	}

	// The rendered Job, through the real components against the published
	// template: an issue-triggered task (the case's precondition).
	h := newHarness(t, false)
	h.useRealBuilder(t)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	payload := json.RawMessage(`{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1"
	}`)
	if err := h.sched.OnClaim(t.Context(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	entry, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("the claimed task is not in the registry")
	}
	job := h.createdJob(t, "fm-task-1")

	// Step 2: the normalized objects diff empty. The design case converges the
	// server-generated fields with kubectl-side jq; here the object never left
	// the builder, so only the two conditional blocks are removed — the same
	// normalization, minus the apiserver's defaults.
	wantJob := decodeJob(t, substituteAuthority(t, docs[0], entry))
	if got, want := canonicalJob(job), canonicalJob(&wantJob); got != want {
		t.Errorf("the rendered Job does not match the AC-20 baseline\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}

	// Step 3: the conditional blocks on their own. This task carries an issue
	// context and no resume session.
	for key, want := range map[string]string{
		registry.AnnoIssueID:         "issue-1",
		registry.AnnoIssueIdentifier: "TSI-1",
	} {
		if got := job.Annotations[key]; got != want {
			t.Errorf("annotation %q = %q, want %q", key, got, want)
		}
	}
	for _, key := range []string{registry.AnnoAutopilotRunID, registry.AnnoChatSessionID} {
		if got, present := job.Annotations[key]; present {
			t.Errorf("annotation %q = %q, want the key absent (no such context)", key, got)
		}
	}
	for key, value := range job.Annotations {
		if strings.HasPrefix(key, "foreman.tsic.top/") && value == "" {
			t.Errorf("annotation %q is an empty placeholder", key)
		}
	}
	// Step 3b: with no resume session the affinity item and the structures it
	// empties are gone; the two shapes (absent, or exactly one item) are the
	// only legal ones.
	if affinity := job.Spec.Template.Spec.Affinity; affinity != nil {
		if len(affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 1 {
			t.Errorf("affinity = %+v, want the key absent or one resolved item", affinity)
		}
	}

	// Step 4: the credential Secret is the baseline's second document, and the
	// Job references it.
	secret := h.createdSecret(t, "fm-task-1-cred")
	if secret.Type != corev1.SecretTypeOpaque {
		t.Errorf("secret type = %q, want Opaque", secret.Type)
	}
	for key, want := range map[string]string{
		"app.kubernetes.io/managed-by": "foreman",
		registry.LabelTaskID:           "task-1",
	} {
		if got := secret.Labels[key]; got != want {
			t.Errorf("secret label %q = %q, want %q", key, got, want)
		}
	}
	if !strings.HasPrefix(secret.StringData["config.json"], `{"token":"fmj_`) {
		t.Errorf("secret config.json = %q, want a Job Token", secret.StringData["config.json"])
	}
	credSrc := volumeByName(t, job.Spec.Template.Spec.Volumes, "cred-src")
	if credSrc.Secret == nil || credSrc.Secret.SecretName != secret.Name {
		t.Errorf("cred-src = %+v, want the per-task Secret %s", credSrc.VolumeSource, secret.Name)
	}
}

// authorityDocuments splits the AC-20 authority file into its documents.
func authorityDocuments(t *testing.T) [][]byte {
	t.Helper()
	return yamlDocuments(t, authorityFixture)
}

// publishedTemplateContent returns the job-overlay.yaml content the deployment
// publishes: what a cluster's ConfigMap foreman-job-template carries.
func publishedTemplateContent(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "25-job-template.yaml"))
	if err != nil {
		t.Fatalf("read deploy/25-job-template.yaml: %v", err)
	}
	jsonBytes, err := utilyaml.ToJSON(raw)
	if err != nil {
		t.Fatalf("decode deploy/25-job-template.yaml: %v", err)
	}
	var configMap corev1.ConfigMap
	if err := json.Unmarshal(jsonBytes, &configMap); err != nil {
		t.Fatalf("decode the template ConfigMap: %v", err)
	}
	content := configMap.Data["job-overlay.yaml"]
	if content == "" {
		t.Fatal("the template ConfigMap carries no job-overlay.yaml key")
	}
	return content
}

// substituteAuthority rewrites the authority's placeholders with the claimed
// entry's values — the design case's dynamic-field backfill.
func substituteAuthority(t *testing.T, doc []byte, entry registry.TaskEntry) []byte {
	t.Helper()
	substitutions := map[string]string{
		"<task_id>":          entry.TaskID,
		"<agent_id>":         entry.AgentID,
		"<workspace_id>":     entry.WorkspaceID,
		"<job runtime uuid>": entry.JobRuntimeID,
		"<RFC3339>":          entry.ClaimedAt.UTC().Format(time.RFC3339),
		"<job-image>":        harnessJobImage,
	}
	text := string(doc)
	for placeholder, value := range substitutions {
		if value == "" {
			t.Fatalf("the claimed entry carries no value for %s", placeholder)
		}
		text = strings.ReplaceAll(text, placeholder, value)
	}
	return []byte(text)
}

// decodeJob decodes one YAML document into a Job.
func decodeJob(t *testing.T, doc []byte) batchv1.Job {
	t.Helper()
	jsonBytes, err := utilyaml.ToJSON(doc)
	if err != nil {
		t.Fatalf("document is not valid YAML: %v", err)
	}
	var job batchv1.Job
	if err := json.Unmarshal(jsonBytes, &job); err != nil {
		t.Fatalf("document does not decode into a Job: %v", err)
	}
	return job
}

// volumeByName returns the named volume of a pod spec.
func volumeByName(t *testing.T, volumes []corev1.Volume, name string) corev1.Volume {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			return volume
		}
	}
	t.Fatalf("volume %q not found", name)
	return corev1.Volume{}
}

// canonicalJob renders the Job's metadata + spec with the two conditional
// blocks removed — the trigger-context annotations and the node-reuse affinity
// item, both asserted on their own above — as canonical JSON. That is the
// design case's normalize.jq minus the server-generated fields, which a Job
// read straight from the builder never carries.
func canonicalJob(job *batchv1.Job) string {
	raw, err := json.Marshal(job)
	if err != nil {
		return err.Error()
	}
	var view map[string]any
	if err := json.Unmarshal(raw, &view); err != nil {
		return err.Error()
	}
	metadata, _ := view["metadata"].(map[string]any)
	annotations, _ := metadata["annotations"].(map[string]any)
	for _, key := range []string{
		registry.AnnoIssueID, registry.AnnoIssueIdentifier,
		registry.AnnoAutopilotRunID, registry.AnnoChatSessionID,
	} {
		delete(annotations, key)
	}
	spec, _ := view["spec"].(map[string]any)
	podTemplate, _ := spec["template"].(map[string]any)
	podSpec, _ := podTemplate["spec"].(map[string]any)
	delete(podSpec, "affinity")
	canonical, err := json.Marshal(view)
	if err != nil {
		return err.Error()
	}
	return string(canonical)
}
