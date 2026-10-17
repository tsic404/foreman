package jobbuilder

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
)

// statefulSetShapedTemplate is the deployment shape P-006 was filed for: a
// privileged dind native sidecar holding the node's docker socket, an init
// installing into a shared nix store, a deployment-chosen command/args, a
// second container, and the omp config mounted where the deployment wants it.
// Every one of these used to be a startup refusal.
const statefulSetShapedTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: <job-name>
  namespace: multica-agents
  labels:
    app.kubernetes.io/name: foreman-job
    app.kubernetes.io/managed-by: foreman
    foreman.tsic.top/task-id: "<task_id>"
    foreman.tsic.top/agent-id: "<agent_id>"
    foreman.tsic.top/workspace-id: "<workspace_id>"
    cost-center: platform
  annotations:
    foreman.tsic.top/daemon-id: "<job-name>"
    foreman.tsic.top/job-runtime-id: "<job runtime uuid>"
    foreman.tsic.top/claimed-at: "<RFC3339>"
    foreman.tsic.top/attempt: "1"
    foreman.tsic.top/issue-id: "<issue_id>"
    foreman.tsic.top/issue-identifier: "<issue-identifier>"
    foreman.tsic.top/autopilot-run-id: "<autopilot_run_id>"
    foreman.tsic.top/chat-session-id: "<chat_session_id>"
spec:
  backoffLimit: 0
  completions: 1
  parallelism: 1
  ttlSecondsAfterFinished: 600
  activeDeadlineSeconds: 86400
  template:
    metadata:
      labels:
        app.kubernetes.io/name: foreman-job
        foreman.tsic.top/task-id: "<task_id>"
        cost-center: platform
    spec:
      restartPolicy: Never
      terminationGracePeriodSeconds: 30
      serviceAccountName: multica-omp
      automountServiceAccountToken: false
      imagePullSecrets:
        - name: registry-tsic
      initContainers:
        - name: nix-setup
          image: registry.corp.example/nix@sha256:2222222222222222222222222222222222222222222222222222222222222222
          command: ["/bin/sh", "-ec"]
          args: ["install -d -o 1000 -g 1000 /nix"]
          volumeMounts:
            - { name: nix, mountPath: /nix }
        - name: dind
          restartPolicy: Always
          image: docker:29-dind
          securityContext:
            privileged: true
          volumeMounts:
            - { name: docker-sock, mountPath: /var/run/docker.sock }
      containers:
        - name: agent
          image: <job-image>
          workingDir: /workspace
          command: ["/bin/sh", "-ec"]
          args: ["exec multica daemon start --foreground --profile foreman-job"]
          tty: true
          env:
            - { name: HOME, value: /home/agent }
            - { name: DOCKER_HOST, value: "unix:///var/run/docker.sock" }
            - { name: MULTICA_SERVER_URL, value: "http://somewhere-else:1234" }
            - { name: LOG_LEVEL, value: debug }
            - { name: LITERAL, value: "<not-registered>" }
          volumeMounts:
            - { name: docker-sock, mountPath: /var/run/docker.sock }
            - { name: omp, mountPath: /home/agent/.omp/agent }
            - { name: cred-src, mountPath: /cred, readOnly: true }
        - name: helper
          image: busybox
          command: ["sleep", "infinity"]
          env:
            - { name: MULTICA_SERVER_URL, value: "http://helper:1" }
      volumes:
        - name: docker-sock
          hostPath: { path: /var/run/docker.sock }
        - name: nix
          hostPath: { path: /nix, type: Directory }
        - name: omp
          emptyDir: {}
        - name: cred-src
          secret: { secretName: <job-name>-cred, defaultMode: 0400 }
`

// nodeAffinityTemplate carries the <preferred node> conditional block next to
// an affinity item the deployment owns.
const nodeAffinityTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: <job-name>
spec:
  template:
    spec:
      restartPolicy: Never
      affinity:
        nodeAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              preference:
                matchExpressions:
                  - { key: kubernetes.io/hostname, operator: In, values: ["<preferred node>"] }
            - weight: 10
              preference:
                matchExpressions:
                  - { key: topology.kubernetes.io/zone, operator: In, values: [zone-a] }
      containers:
        - name: agent
          image: <job-image>
`

// onlyPreferredNodeTemplate's affinity block is the conditional block alone:
// with no resolved node the whole block must disappear.
const onlyPreferredNodeTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: <job-name>
spec:
  template:
    spec:
      restartPolicy: Never
      affinity:
        nodeAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              preference:
                matchExpressions:
                  - { key: kubernetes.io/hostname, operator: In, values: ["<preferred node>"] }
      containers:
        - name: agent
          image: <job-image>
`

// templateConfig writes manifest where the Deployment mounts the template and
// returns the config plus the captured startup log. The log runs through the
// production chain (observability.NewLogger: JSON plus redaction, the handler
// main installs at the Deployment's info level): AC-29 / TC-tech-job-07 judge
// the deployed log text, so a redaction rule that rewrote a fixed literal must
// fail here rather than only in the cluster.
func templateConfig(t *testing.T, manifest string) (Config, *bytes.Buffer) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "job-overlay.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write template: %v", err)
	}
	logs := &bytes.Buffer{}
	cfg := testConfig()
	cfg.TemplatePath = path
	cfg.Issuer = &fakeIssuer{token: "fmj_test.sig"}
	cfg.Logger = observability.NewLogger(logs, slog.LevelInfo, "")
	return cfg, logs
}

// mustBuildTemplate renders e through the deployment-template path.
func mustBuildTemplate(t *testing.T, manifest string, e TaskEntry, mutate func(*Config)) (*batchv1.Job, *bytes.Buffer) {
	t.Helper()
	cfg, logs := templateConfig(t, manifest)
	if mutate != nil {
		mutate(&cfg)
	}
	job, _ := mustBuild(t, cfg, e)
	return job, logs
}

// ---- startup gate: two failure classes only (P-006 §A4) ----

// The startup log's fixed literals, pinned as text and not read from the
// production constants: the acceptance test greps them verbatim
// (test-cases/job-template.md TC-tech-job-07, AC-29), so a typo inside a
// constant is exactly the defect these assertions have to catch.
const (
	wantDeclaration = "Foreman 不对 Job 模板做任何校验，模板内容不构成安全边界"
	wantEffect      = "运行中 CM 变更不生效"
	wantHashPrefix  = "模板内容哈希 sha256="
	wantUnreadable  = "文件不可读"
	wantNoHash      = "模板内容哈希不可得"
)

// wantHashLine is the hash line AC-29 fixes for a template file's bytes.
func wantHashLine(content string) string {
	sum := sha256.Sum256([]byte(content))
	return wantHashPrefix + hex.EncodeToString(sum[:])
}

func TestLoadJobTemplateAbsent(t *testing.T) {
	cfg := testConfig()
	cfg.TemplatePath = filepath.Join(t.TempDir(), "job-overlay.yaml")
	logs := &bytes.Buffer{}
	cfg.Logger = observability.NewLogger(logs, slog.LevelInfo, "")

	template, present, err := LoadJobTemplate(cfg)
	if err != nil {
		t.Fatalf("a missing template is not an error: %v", err)
	}
	if present || template != nil {
		t.Fatalf("present=%v template=%v, want the default-template path", present, template)
	}
	if !strings.Contains(logs.String(), wantDeclaration) {
		t.Errorf("startup log must carry the declaration on the absent path: %s", logs.String())
	}
	if !strings.Contains(logs.String(), wantEffect) {
		t.Errorf("the absent path must still state that a ConfigMap edit needs a restart: %s", logs.String())
	}
	if strings.Contains(logs.String(), wantHashPrefix) {
		t.Errorf("the absent path has no content to hash: %s", logs.String())
	}
}

func TestLoadJobTemplateAcceptsCompleteJob(t *testing.T) {
	cfg, logs := templateConfig(t, statefulSetShapedTemplate)

	template, present, err := LoadJobTemplate(cfg)
	if err != nil {
		t.Fatalf("LoadJobTemplate: %v", err)
	}
	if !present || template == nil {
		t.Fatalf("present=%v template=%v, want the parsed template", present, template)
	}
	if template["apiVersion"] != "batch/v1" || template["kind"] != "Job" {
		t.Errorf("template header = %v/%v", template["apiVersion"], template["kind"])
	}
	log := logs.String()
	if !strings.Contains(log, wantDeclaration) {
		t.Errorf("startup log must carry the declaration: %s", log)
	}
	if !strings.Contains(log, wantHashLine(statefulSetShapedTemplate)) {
		t.Errorf("startup log must carry the template file's hash line: %s", log)
	}
	if !strings.Contains(log, wantEffect) {
		t.Errorf("startup log must state that a ConfigMap edit needs a restart: %s", log)
	}
}

// TestLoadJobTemplateUnreadable is failure class 1: the file exists but cannot
// be read (a directory is the portable stand-in for an unreadable path).
func TestLoadJobTemplateUnreadable(t *testing.T) {
	cfg := testConfig()
	cfg.TemplatePath = t.TempDir()
	logs := &bytes.Buffer{}
	cfg.Logger = observability.NewLogger(logs, slog.LevelInfo, "")

	template, present, err := LoadJobTemplate(cfg)
	if err == nil {
		t.Fatalf("unreadable template accepted: %v", template)
	}
	if present {
		t.Error("a failed read must not report the template as present")
	}
	if !strings.Contains(logs.String(), wantDeclaration) {
		t.Errorf("the unreadable refusal must carry the declaration: %s", logs.String())
	}
	if !strings.Contains(logs.String(), wantUnreadable) {
		t.Errorf("the unreadable refusal must name the class (文件不可读): %s", logs.String())
	}
	// Class 1 states the hash is out of reach and must not show a hash line.
	if !strings.Contains(logs.String(), wantNoHash) {
		t.Errorf("the unreadable refusal must state that no hash is available: %s", logs.String())
	}
	if strings.Contains(logs.String(), wantHashPrefix) {
		t.Errorf("an unreadable file has no hash to report: %s", logs.String())
	}
}

// TestLoadJobTemplateRejects is failure class 2: the content is not a
// batch/v1 Job. A bare manifest fragment lands here too — it has no
// apiVersion/kind, so it cannot be the complete template P-006 defines.
func TestLoadJobTemplateRejects(t *testing.T) {
	cases := []struct {
		name     string
		manifest string
		want     string
	}{
		{"unparsable YAML", "apiVersion: batch/v1\nkind: Job\nspec: [\n", "not parseable as YAML"},
		{"empty document", "# only a comment\n", "empty document"},
		{"manifest fragment", "spec:\n  backoffLimit: 0\n", "apiVersion"},
		{"wrong apiVersion", "apiVersion: apps/v1\nkind: Job\n", "apiVersion"},
		{"wrong kind", "apiVersion: batch/v1\nkind: Deployment\n", "kind Job"},
		{"misspelled field", "apiVersion: batch/v1\nkind: Job\nspec:\n  template:\n    spec:\n      contianers: []\n", "contianers"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, logs := templateConfig(t, tc.manifest)

			template, present, err := LoadJobTemplate(cfg)
			if err == nil {
				t.Fatalf("template accepted: %v", template)
			}
			if !present {
				t.Error("the file existed, so present must stay true")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q must name the failure (%q)", err, tc.want)
			}
			if !strings.Contains(logs.String(), wantDeclaration) {
				t.Errorf("the refusal log must carry the declaration: %s", logs.String())
			}
			// Class 2 is readable, so it keeps the hash line (AC-29).
			if !strings.Contains(logs.String(), wantHashLine(tc.manifest)) {
				t.Errorf("the refusal log must carry the template file's hash line: %s", logs.String())
			}
		})
	}
}

// ---- template passthrough + the A1 writes ----

func TestTemplatePassthroughShape(t *testing.T) {
	job, _ := mustBuildTemplate(t, statefulSetShapedTemplate, testEntry(), nil)
	spec := job.Spec.Template.Spec

	if len(spec.Containers) != 2 || spec.Containers[0].Name != "agent" || spec.Containers[1].Name != "helper" {
		t.Fatalf("containers = %v, want the template's two entries verbatim", containerNames(spec.Containers))
	}
	agent := spec.Containers[0]
	if strings.Join(agent.Command, " ") != "/bin/sh -ec" {
		t.Errorf("command = %v, want the template's", agent.Command)
	}
	if strings.Join(agent.Args, " ") != "exec multica daemon start --foreground --profile foreman-job" {
		t.Errorf("args = %v, want the template's", agent.Args)
	}
	if agent.WorkingDir != "/workspace" {
		t.Errorf("workingDir = %q, want the template's /workspace", agent.WorkingDir)
	}
	if got := mountPathByName(t, agent.VolumeMounts, "omp"); got != "/home/agent/.omp/agent" {
		t.Errorf("omp mountPath = %q, want the template's", got)
	}

	if len(spec.InitContainers) != 2 || spec.InitContainers[0].Name != "nix-setup" || spec.InitContainers[1].Name != "dind" {
		t.Fatalf("initContainers = %v, want the template's order verbatim", containerNames(spec.InitContainers))
	}
	dind := spec.InitContainers[1]
	if dind.RestartPolicy == nil || *dind.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Errorf("dind restartPolicy = %v, want the native sidecar Always", dind.RestartPolicy)
	}
	if dind.SecurityContext == nil || dind.SecurityContext.Privileged == nil || !*dind.SecurityContext.Privileged {
		t.Errorf("dind securityContext = %+v, want the template's privileged sidecar (F4 废弃位)", dind.SecurityContext)
	}

	if len(spec.Volumes) != 4 {
		t.Fatalf("volumes = %v, want the template's four", containerNames(nil))
	}
	if v := volumeByName(t, spec.Volumes, "docker-sock"); v.HostPath == nil || v.HostPath.Path != "/var/run/docker.sock" {
		t.Errorf("volume docker-sock = %+v, want the template's socket mount", v)
	}
	if v := volumeByName(t, spec.Volumes, "nix"); v.HostPath == nil || v.HostPath.Path != "/nix" {
		t.Errorf("volume nix = %+v, want the template's shared store", v)
	}
	if v := volumeByName(t, spec.Volumes, "cred-src"); v.Secret == nil || v.Secret.SecretName != testJobName+"-cred" {
		t.Errorf("volume cred-src = %+v, want the placeholder-resolved Secret %s-cred", v, testJobName)
	}
	if spec.ServiceAccountName != "multica-omp" || spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("pod spec = %+v, want the template's restartPolicy/serviceAccountName", spec)
	}
}

// TestTemplateInjectsA1Env (P-006 §A1.1): the three keys land on containers[0]
// — the first entry by writing order, never a container matched by name — a
// same-name entry is replaced whole, and every other container is untouched.
func TestTemplateInjectsA1Env(t *testing.T) {
	job, _ := mustBuildTemplate(t, statefulSetShapedTemplate, testEntry(), nil)
	agent := job.Spec.Template.Spec.Containers[0]

	if got := envByName(t, agent.Env, "MULTICA_SERVER_URL").Value; got != "http://foreman.foreman.svc.cluster.local:8080" {
		t.Errorf("MULTICA_SERVER_URL = %q, want Foreman's address (the template's value is overwritten)", got)
	}
	daemonID := envByName(t, agent.Env, "MULTICA_DAEMON_ID")
	if daemonID.ValueFrom == nil || daemonID.ValueFrom.FieldRef == nil ||
		daemonID.ValueFrom.FieldRef.FieldPath != "metadata.labels['job-name']" || daemonID.Value != "" {
		t.Errorf("MULTICA_DAEMON_ID = %+v, want the job-name fieldRef only", daemonID)
	}
	if got := envByName(t, agent.Env, "MULTICA_DAEMON_MAX_CONCURRENT_TASKS").Value; got != "1" {
		t.Errorf("MULTICA_DAEMON_MAX_CONCURRENT_TASKS = %q, want 1", got)
	}
	// The template's other env survives; only the three keys are Foreman's.
	for _, name := range []string{"HOME", "DOCKER_HOST", "LOG_LEVEL", "LITERAL"} {
		if envByName(t, agent.Env, name).Name != name {
			t.Errorf("env %q was dropped from the template", name)
		}
	}
	if got := envNames(agent.Env); len(got) != 7 {
		t.Errorf("env = %v, want the template's four plus the three injected", got)
	}
	if got := envByName(t, job.Spec.Template.Spec.Containers[1].Env, "MULTICA_SERVER_URL").Value; got != "http://helper:1" {
		t.Errorf("helper MULTICA_SERVER_URL = %q, containers[1] must stay untouched", got)
	}
}

// TestTemplateWithoutContainers: the injection is skipped, the Job is left for
// the apiserver to reject (job_create_failed on the scheduler side).
func TestTemplateWithoutContainers(t *testing.T) {
	manifest := "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: <job-name>\nspec:\n  template:\n    spec:\n      restartPolicy: Never\n"
	job, _ := mustBuildTemplate(t, manifest, testEntry(), nil)
	if len(job.Spec.Template.Spec.Containers) != 0 {
		t.Fatalf("containers = %v, want none", containerNames(job.Spec.Template.Spec.Containers))
	}
}

// TestTemplatePerTaskWrites (P-006 §A1.2): metadata, the §3.2 key set by side,
// and the domain clear that lets an empty TaskEntry field delete a template's
// static key.
func TestTemplatePerTaskWrites(t *testing.T) {
	entry := testEntry()
	job, _ := mustBuildTemplate(t, statefulSetShapedTemplate, entry, nil)

	if job.Name != testJobName || job.Namespace != DefaultJobNamespace {
		t.Errorf("job %q/%q, want %q/%q", job.Namespace, job.Name, DefaultJobNamespace, testJobName)
	}
	wantLabels := map[string]string{
		"app.kubernetes.io/name":        "foreman-job",
		"app.kubernetes.io/managed-by":  "foreman",
		"foreman.tsic.top/task-id":      testTaskID,
		"foreman.tsic.top/agent-id":     entry.AgentID,
		"foreman.tsic.top/workspace-id": entry.WorkspaceID,
		"cost-center":                   "platform",
	}
	if len(job.Labels) != len(wantLabels) {
		t.Fatalf("job labels = %v, want %v", job.Labels, wantLabels)
	}
	for key, want := range wantLabels {
		if job.Labels[key] != want {
			t.Errorf("label %q = %q, want %q", key, job.Labels[key], want)
		}
	}

	wantAnnotations := map[string]string{
		annoDaemonID:        testJobName,
		annoJobRuntimeID:    entry.JobRuntimeID,
		annoClaimedAt:       entry.ClaimedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
		annoAttempt:         "1",
		annoIssueID:         entry.IssueID,
		annoIssueIdentifier: entry.IssueIdentifier,
	}
	if len(job.Annotations) != len(wantAnnotations) {
		t.Fatalf("job annotations = %v, want %v", job.Annotations, wantAnnotations)
	}
	for key, want := range wantAnnotations {
		if job.Annotations[key] != want {
			t.Errorf("annotation %q = %q, want %q", key, job.Annotations[key], want)
		}
	}

	podLabels := job.Spec.Template.Labels
	if len(podLabels) != 3 {
		t.Fatalf("pod labels = %v, want the two §3.2 labels plus the template's own", podLabels)
	}
	if podLabels["app.kubernetes.io/name"] != "foreman-job" || podLabels["foreman.tsic.top/task-id"] != testTaskID {
		t.Errorf("pod labels = %v, want the two §3.2 labels", podLabels)
	}
	if podLabels["cost-center"] != "platform" {
		t.Errorf("pod labels = %v, the template's free label must survive", podLabels)
	}
	if _, ok := podLabels["app.kubernetes.io/managed-by"]; ok {
		t.Error("managed-by is a Job-side key; the pod side must not carry it")
	}
	if len(job.Spec.Template.Annotations) != 0 {
		t.Errorf("pod annotations = %v, want none", job.Spec.Template.Annotations)
	}
}

// TestTemplateConditionalTriggerContext: a template's static trigger-context
// keys are deleted when the entry has no such context (P-006 §A1.2 增删).
func TestTemplateConditionalTriggerContext(t *testing.T) {
	entry := testEntry()
	entry.IssueID, entry.IssueIdentifier = "", ""
	entry.AutopilotRunID = "b1c2d3e4-5f60-4a7b-8c9d-0e1f2a3b4c5d"

	job, _ := mustBuildTemplate(t, statefulSetShapedTemplate, entry, nil)

	for _, key := range []string{annoIssueID, annoIssueIdentifier, annoChatSessionID} {
		if value, present := job.Annotations[key]; present {
			t.Errorf("annotation %q = %q, want the template's static key deleted", key, value)
		}
	}
	if job.Annotations[annoAutopilotRunID] != entry.AutopilotRunID {
		t.Errorf("annotation %q = %q, want %q", annoAutopilotRunID, job.Annotations[annoAutopilotRunID], entry.AutopilotRunID)
	}
}

// TestTemplatePlaceholders (P-006 §A1.2): every registered placeholder resolves
// to its per-task value — inside larger strings too — and an unregistered
// <...> literal is left verbatim.
func TestTemplatePlaceholders(t *testing.T) {
	entry := testEntry()
	job, _ := mustBuildTemplate(t, statefulSetShapedTemplate, entry, nil)

	agent := job.Spec.Template.Spec.Containers[0]
	if agent.Image != DefaultJobImage {
		t.Errorf("agent image = %q, want the <job-image> resolved to FOREMAN_JOB_IMAGE %q", agent.Image, DefaultJobImage)
	}
	if got := envByName(t, agent.Env, "LITERAL").Value; got != "<not-registered>" {
		t.Errorf("unregistered placeholder = %q, want it verbatim", got)
	}
	if job.Labels[labelTaskID] != entry.TaskID || job.Annotations[annoJobRuntimeID] != entry.JobRuntimeID {
		t.Errorf("labels/annotations = %v/%v, want the entry's values", job.Labels, job.Annotations)
	}
	if got := job.Annotations[annoClaimedAt]; got != entry.ClaimedAt.UTC().Format("2006-01-02T15:04:05Z07:00") {
		t.Errorf("claimed-at = %q, want the claim time", got)
	}
	if job.Annotations[annoAttempt] != "1" {
		t.Errorf("attempt = %q, want 1", job.Annotations[annoAttempt])
	}
}

// TestTemplatePreferredNodePlaceholder (P-006 §A1.2): <preferred node> is a
// conditional block — resolved into the affinity item when a node exists, and
// dropped together with that item when none does. Affinity items the
// deployment owns are never touched.
func TestTemplatePreferredNodePlaceholder(t *testing.T) {
	entry := testEntry()

	job, _ := mustBuildTemplate(t, nodeAffinityTemplate, entry, func(cfg *Config) {
		cfg.Nodes = sessionNodeIndex("company-02")
	})
	terms := job.Spec.Template.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 2 {
		t.Fatalf("preferred terms = %+v, want the template's two", terms)
	}
	if got := terms[0].Preference.MatchExpressions[0].Values; len(got) != 1 || got[0] != "company-02" {
		t.Errorf("session item values = %v, want the resolved node", got)
	}
	if got := terms[1].Preference.MatchExpressions[0].Values; len(got) != 1 || got[0] != "zone-a" {
		t.Errorf("deployment item values = %v, want the template's own", got)
	}

	// No history for the issue: the item goes, the deployment's stays.
	job, _ = mustBuildTemplate(t, nodeAffinityTemplate, entry, nil)
	terms = job.Spec.Template.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 || terms[0].Preference.MatchExpressions[0].Values[0] != "zone-a" {
		t.Errorf("preferred terms = %+v, want only the deployment's item", terms)
	}

	// Reuse switched off behaves like no history.
	job, _ = mustBuildTemplate(t, nodeAffinityTemplate, entry, func(cfg *Config) {
		cfg.Nodes = sessionNodeIndex("company-02")
		cfg.PreferNodeReuse = false
	})
	terms = job.Spec.Template.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 || terms[0].Preference.MatchExpressions[0].Values[0] != "zone-a" {
		t.Errorf("preferred terms = %+v, want the placeholder item dropped", terms)
	}

	// The placeholder was the whole affinity block: the block disappears.
	job, _ = mustBuildTemplate(t, onlyPreferredNodeTemplate, entry, nil)
	if job.Spec.Template.Spec.Affinity != nil {
		t.Errorf("affinity = %+v, want none once its only item is dropped", job.Spec.Template.Spec.Affinity)
	}
	job, _ = mustBuildTemplate(t, onlyPreferredNodeTemplate, entry, func(cfg *Config) {
		cfg.Nodes = sessionNodeIndex("company-02")
	})
	if got := job.Spec.Template.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].
		Preference.MatchExpressions[0].Values[0]; got != "company-02" {
		t.Errorf("resolved node = %q, want company-02", got)
	}
}

// TestTemplateSessionNodeSpelling: the design corpus writes the node-reuse
// placeholder two ways — <preferred node> (P-006 §A1.2, 03-contracts §5.4) and
// <session node> (job-template.md's built-in manifest and placeholder table).
// They name one conditional block, so a template copied from either resolves
// the node and drops the item exactly the same way.
func TestTemplateSessionNodeSpelling(t *testing.T) {
	entry := testEntry()
	manifest := strings.ReplaceAll(onlyPreferredNodeTemplate, "<preferred node>", "<session node>")

	job, _ := mustBuildTemplate(t, manifest, entry, func(cfg *Config) {
		cfg.Nodes = sessionNodeIndex("company-02")
	})
	if got := job.Spec.Template.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].
		Preference.MatchExpressions[0].Values[0]; got != "company-02" {
		t.Errorf("resolved node = %q, want company-02", got)
	}

	job, _ = mustBuildTemplate(t, manifest, entry, nil)
	if job.Spec.Template.Spec.Affinity != nil {
		t.Errorf("affinity = %+v, want none once its only item is dropped", job.Spec.Template.Spec.Affinity)
	}
}

// duplicateEnvTemplate declares the injected key twice: the daemon reads the
// last entry (os.Getenv), so keeping either template copy would let it win.
const duplicateEnvTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: <job-name>
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
          env:
            - { name: MULTICA_SERVER_URL, value: "http://first:1" }
            - { name: HOME, value: /home/agent }
            - { name: MULTICA_SERVER_URL, value: "http://second:2" }
            - { name: MULTICA_DAEMON_ID, value: "template-daemon" }
            - { name: MULTICA_DAEMON_ID, valueFrom: { fieldRef: { fieldPath: metadata.name } } }
`

// misplacedNodePlaceholderTemplate keeps the node placeholder out of the
// preferred-affinity item, where the conditional deletion does not reach.
const misplacedNodePlaceholderTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: <job-name>
spec:
  template:
    spec:
      restartPolicy: Never
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - { key: kubernetes.io/hostname, operator: In, values: ["<preferred node>"] }
      containers:
        - name: agent
          image: <job-image>
`

// largeIntegerTemplate carries int64 values a float64 round trip would rewrite:
// 2^53+1 loses its low bit, and int64 max overflows the float64 conversion.
const largeIntegerTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: <job-name>
spec:
  activeDeadlineSeconds: 9223372036854775807
  template:
    spec:
      restartPolicy: Never
      terminationGracePeriodSeconds: 9007199254740993
      containers:
        - name: agent
          image: <job-image>
`

// conflictingMetadataTemplate writes its own per-task metadata and its own
// §3.2 keys: every one of them is Foreman's to overwrite or delete, so the
// assertions below fail if the A1.2 write is removed.
const conflictingMetadataTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-static
  namespace: static-ns
  labels:
    app.kubernetes.io/name: not-foreman
    app.kubernetes.io/managed-by: someone-else
    foreman.tsic.top/task-id: "static-task"
    foreman.tsic.top/agent-id: "static-agent"
    foreman.tsic.top/workspace-id: "static-workspace"
  annotations:
    foreman.tsic.top/daemon-id: "static-daemon"
    foreman.tsic.top/job-runtime-id: "static-runtime"
    foreman.tsic.top/claimed-at: "1999-01-01T00:00:00Z"
    foreman.tsic.top/attempt: "9"
    foreman.tsic.top/issue-id: "static-issue"
    foreman.tsic.top/issue-identifier: "STATIC-1"
spec:
  template:
    metadata:
      labels:
        app.kubernetes.io/name: not-foreman
        app.kubernetes.io/managed-by: someone-else
        foreman.tsic.top/task-id: "static-task"
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`

// TestTemplateInjectsA1EnvOnce (P-006 §A1.1): a template may declare an
// injected key twice; the daemon reads the last entry, so every copy must be
// replaced and exactly one authoritative entry must remain.
func TestTemplateInjectsA1EnvOnce(t *testing.T) {
	job, _ := mustBuildTemplate(t, duplicateEnvTemplate, testEntry(), nil)
	env := job.Spec.Template.Spec.Containers[0].Env

	if got := envNames(env); len(got) != 4 {
		t.Fatalf("env = %v, want HOME plus the three authoritative keys", got)
	}
	serverURLs := 0
	daemonIDs := 0
	for _, entry := range env {
		switch entry.Name {
		case "MULTICA_SERVER_URL":
			serverURLs++
			if entry.Value != "http://foreman.foreman.svc.cluster.local:8080" || entry.ValueFrom != nil {
				t.Errorf("MULTICA_SERVER_URL = %+v, want Foreman's address", entry)
			}
		case "MULTICA_DAEMON_ID":
			daemonIDs++
			if entry.Value != "" || entry.ValueFrom == nil || entry.ValueFrom.FieldRef == nil ||
				entry.ValueFrom.FieldRef.FieldPath != "metadata.labels['job-name']" {
				t.Errorf("MULTICA_DAEMON_ID = %+v, want the job-name fieldRef only", entry)
			}
		}
	}
	if serverURLs != 1 || daemonIDs != 1 {
		t.Errorf("MULTICA_SERVER_URL x%d, MULTICA_DAEMON_ID x%d, want one each", serverURLs, daemonIDs)
	}
}

// TestTemplateMisplacedNodePlaceholder (P-006 §A1.2): the conditional deletion
// covers the preferred-affinity item only. Anywhere else the placeholder has no
// value to write when no node resolves, so the Build fails instead of dropping
// the key or element that carried it.
func TestTemplateMisplacedNodePlaceholder(t *testing.T) {
	entry := testEntry()
	sessionSpelling := strings.Replace(misplacedNodePlaceholderTemplate, "<preferred node>", "<session node>", 1)

	for name, template := range map[string]string{
		"required node affinity": misplacedNodePlaceholderTemplate,
		"session node spelling":  sessionSpelling,
		"container args":         strings.Replace(statefulSetShapedTemplate, `"exec multica daemon start --foreground --profile foreman-job"`, `"exec multica daemon start --node <preferred node>"`, 1),
	} {
		cfg, _ := templateConfig(t, template)
		cfg.Issuer = &fakeIssuer{token: "fmj_test.sig"}
		builder, err := NewBuilder(cfg)
		if err != nil {
			t.Fatalf("%s: NewBuilder: %v", name, err)
		}
		job, secret, _, err := builder.Build(entry)
		if err == nil {
			t.Fatalf("%s: Build succeeded, want a fail-closed error (job=%v secret=%v)", name, job, secret)
		}
		if !strings.Contains(err.Error(), "preferred-affinity item") {
			t.Errorf("%s: error %q must name the misplaced placeholder", name, err)
		}
	}

	// With a node resolved the same template renders the node instead.
	cfg, _ := templateConfig(t, misplacedNodePlaceholderTemplate)
	cfg.Issuer = &fakeIssuer{token: "fmj_test.sig"}
	cfg.Nodes = sessionNodeIndex("company-02")
	builder, err := NewBuilder(cfg)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	job, _, _, err := builder.Build(entry)
	if err != nil {
		t.Fatalf("Build with a resolved node: %v", err)
	}
	if got := job.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.
		NodeSelectorTerms[0].MatchExpressions[0].Values[0]; got != "company-02" {
		t.Errorf("required node affinity value = %q, want the resolved node", got)
	}
}

// TestTemplateIntegerPrecision: the parsed map is written back per Build, so
// numbers must survive the round trip exactly (json.Number, not float64).
func TestTemplateIntegerPrecision(t *testing.T) {
	job, _ := mustBuildTemplate(t, largeIntegerTemplate, testEntry(), nil)

	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != math.MaxInt64 {
		t.Errorf("activeDeadlineSeconds = %v, want %d", job.Spec.ActiveDeadlineSeconds, int64(math.MaxInt64))
	}
	if got := job.Spec.Template.Spec.TerminationGracePeriodSeconds; got == nil || *got != 9007199254740993 {
		t.Errorf("terminationGracePeriodSeconds = %v, want 9007199254740993", got)
	}
}

// TestTemplateAuthoritativeMetadataWins (P-006 §A1.2): the template's own
// per-task values are overwritten or deleted, never trusted.
func TestTemplateAuthoritativeMetadataWins(t *testing.T) {
	entry := testEntry()
	job, _ := mustBuildTemplate(t, conflictingMetadataTemplate, entry, nil)

	if job.Name != entry.JobName || job.Namespace != DefaultJobNamespace {
		t.Errorf("job %q/%q, want %q/%q", job.Namespace, job.Name, DefaultJobNamespace, entry.JobName)
	}
	wantLabels := map[string]string{
		"app.kubernetes.io/name":        jobAppName,
		"app.kubernetes.io/managed-by":  managedByForeman,
		"foreman.tsic.top/task-id":      entry.TaskID,
		"foreman.tsic.top/agent-id":     entry.AgentID,
		"foreman.tsic.top/workspace-id": entry.WorkspaceID,
	}
	if len(job.Labels) != len(wantLabels) {
		t.Fatalf("job labels = %v, want %v", job.Labels, wantLabels)
	}
	for key, want := range wantLabels {
		if job.Labels[key] != want {
			t.Errorf("label %q = %q, want %q", key, job.Labels[key], want)
		}
	}
	wantAnnotations := map[string]string{
		annoDaemonID:        entry.JobName,
		annoJobRuntimeID:    entry.JobRuntimeID,
		annoClaimedAt:       entry.ClaimedAt.UTC().Format(time.RFC3339),
		annoAttempt:         strconv.Itoa(entry.Attempt),
		annoIssueID:         entry.IssueID,
		annoIssueIdentifier: entry.IssueIdentifier,
	}
	if len(job.Annotations) != len(wantAnnotations) {
		t.Fatalf("job annotations = %v, want %v", job.Annotations, wantAnnotations)
	}
	for key, want := range wantAnnotations {
		if job.Annotations[key] != want {
			t.Errorf("annotation %q = %q, want %q", key, job.Annotations[key], want)
		}
	}

	podLabels := job.Spec.Template.Labels
	if len(podLabels) != 3 || podLabels["app.kubernetes.io/name"] != jobAppName || podLabels[labelTaskID] != entry.TaskID {
		t.Errorf("pod labels = %v, want the two authoritative labels plus the template's own", podLabels)
	}
	// managed-by is a Job-side §3.2 key; on the pod side the template's own
	// value is not Foreman's to overwrite (P-006 §A2 不写).
	if podLabels["app.kubernetes.io/managed-by"] != "someone-else" {
		t.Errorf("pod managed-by = %q, want the template's value left alone", podLabels["app.kubernetes.io/managed-by"])
	}
}

func containerNames(containers []corev1.Container) []string {
	names := make([]string, 0, len(containers))
	for _, ct := range containers {
		names = append(names, ct.Name)
	}
	return names
}

func mountPathByName(t *testing.T, mounts []corev1.VolumeMount, name string) string {
	t.Helper()
	for _, mount := range mounts {
		if mount.Name == name {
			return mount.MountPath
		}
	}
	t.Fatalf("mount %q not found in %v", name, mountNames(mounts))
	return ""
}

func mountNames(mounts []corev1.VolumeMount) []string {
	names := make([]string, 0, len(mounts))
	for _, mount := range mounts {
		names = append(names, mount.Name)
	}
	return names
}

func envNames(env []corev1.EnvVar) []string {
	names := make([]string, 0, len(env))
	for _, entry := range env {
		names = append(names, entry.Name)
	}
	return names
}

func envByName(t *testing.T, env []corev1.EnvVar, name string) corev1.EnvVar {
	t.Helper()
	for _, entry := range env {
		if entry.Name == name {
			return entry
		}
	}
	t.Fatalf("env %q not found in %v", name, envNames(env))
	return corev1.EnvVar{}
}

func volumeByName(t *testing.T, volumes []corev1.Volume, name string) corev1.Volume {
	t.Helper()
	for _, vol := range volumes {
		if vol.Name == name {
			return vol
		}
	}
	t.Fatalf("volume %q not found", name)
	return corev1.Volume{}
}

// TestTemplateCopyPerBuild (P-006 §A1.3): the startup-parsed template is
// read-only, so a second task's Job carries only its own per-task values — the
// credential Secret reference above all.
func TestTemplateCopyPerBuild(t *testing.T) {
	cfg, _ := templateConfig(t, statefulSetShapedTemplate)
	builder, err := NewBuilder(cfg)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}

	first := testEntry()
	firstJob, _, _, err := builder.Build(first)
	if err != nil {
		t.Fatalf("Build first: %v", err)
	}
	second := testEntry()
	second.TaskID = "0f1e2d3c-4b5a-4968-8776-655443322110"
	second.JobName = registry.JobName(second.TaskID, second.IssueIdentifier, "")
	secondJob, secondSecret, _, err := builder.Build(second)
	if err != nil {
		t.Fatalf("Build second: %v", err)
	}

	wantSecond := second.JobName + "-cred"
	if got := volumeByName(t, secondJob.Spec.Template.Spec.Volumes, "cred-src").Secret.SecretName; got != wantSecond {
		t.Errorf("second Job cred-src = %q, want %q", got, wantSecond)
	}
	if secondSecret.Name != wantSecond || secondJob.Annotations[annoDaemonID] != second.JobName {
		t.Errorf("second Job/Secret = %q/%q, want the second task's", secondSecret.Name, secondJob.Annotations[annoDaemonID])
	}
	if got := canonicalJSON(secondJob); strings.Contains(got, first.TaskID) {
		t.Errorf("second Job carries the first task's values:\n%s", got)
	}
	if got := volumeByName(t, firstJob.Spec.Template.Spec.Volumes, "cred-src").Secret.SecretName; got != first.JobName+"-cred" {
		t.Errorf("first Job cred-src = %q, want it left alone by the second Build", got)
	}
	if got := canonicalJSON(firstJob); strings.Contains(got, second.TaskID) {
		t.Errorf("first Job was mutated by the second Build:\n%s", got)
	}
}
