package jobbuilder

import (
	"errors"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	testRequestID = "9c1f0b3a-4d2e-4f5a-8b6c-1d2e3f4a5b6c"
	testProbeName = "fm-catalog-" + testRequestID
	// testProbeRuntime is the probe's own job runtime id: the value
	// <job runtime uuid> takes in a probe render.
	testProbeRuntime = "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d"
	// testProbeImage carries "foreman-job" inside the reference: the renames
	// write only their declared faces (container command and arguments, mount
	// paths, volume Secret references) and a container's image is none of them,
	// so the reference must come back verbatim.
	testProbeImage = "ghcr.io/tsic404/foreman-job:v0.1.0"
	// testWorkspaceID is the workspace a probe token is scoped to.
	testWorkspaceID = "c20ff019-2980-4936-8cb0-22d79d694e71"
)

// probeTemplate is a deployment template written the way the probe render
// expects: the profile and the credential Secret as their task-side literals,
// the four consumed placeholders, and the task-identity and agent placeholders
// the probe must leave alone.
const probeTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
  namespace: other-namespace
  labels:
    app.kubernetes.io/name: foreman-job
    app.kubernetes.io/managed-by: foreman
    foreman.tsic.top/task-id: "<task_id>"
    cost-center: platform
  annotations:
    foreman.tsic.top/daemon-id: "fm-<task_id>"
    example.com/note: keep-me
    example.com/runtime: "<job runtime uuid>"
    example.com/probe-identity: "<job-name>|<daemon-id>"
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
      annotations:
        foreman.tsic.top/daemon-id: "fm-<task_id>"
        example.com/note: keep-me
    spec:
      restartPolicy: Never
      serviceAccountName: multica-omp
      automountServiceAccountToken: false
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
          command: ["/usr/bin/dumb-init", "--", "/usr/local/bin/multica"]
          args: ["daemon", "start", "--foreground", "--profile", "foreman-job"]
          env:
            - { name: MULTICA_SERVER_URL, value: "http://somewhere-else:1234" }
            - { name: MULTICA_DAEMON_HEARTBEAT_INTERVAL, value: "60s" }
            - { name: TASK_ANCHOR, value: "<task_id>" }
            - { name: AGENT_ANCHOR, value: "<agent-name>" }
          volumeMounts:
            - { name: cred, mountPath: /home/agent/.multica/profiles/foreman-job/config.json, subPath: config.json, readOnly: true }
      volumes:
        - name: cred-src
          secret: { secretName: fm-<task_id>-cred, defaultMode: 0400 }
`

// mustBuildProbe renders a probe with the fake issuer recording the probe
// token call.
func mustBuildProbe(t *testing.T, cfg Config, requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, *fakeIssuer) {
	t.Helper()
	issuer := &fakeIssuer{token: "fmj_probe.sig"}
	cfg.Issuer = issuer
	job, secret, tokenHash, err := newTestBuilder(t, cfg).BuildCatalogProbe(requestID, jobRuntimeID, workspaceID, budget)
	if err != nil {
		t.Fatalf("BuildCatalogProbe: %v", err)
	}
	if job == nil || secret == nil {
		t.Fatalf("BuildCatalogProbe returned nil objects (job=%v, secret=%v)", job, secret)
	}
	return job, secret, tokenHash, issuer
}

// probeTemplateConfig writes probeTemplate where the Deployment mounts the
// template and returns the config plus the recording issuer.
func probeTemplateConfig(t *testing.T) (Config, *fakeIssuer) {
	t.Helper()
	cfg, _ := templateConfig(t, probeTemplate)
	issuer := &fakeIssuer{token: "fmj_probe.sig"}
	cfg.Issuer = issuer
	cfg.JobImage = testProbeImage
	return cfg, issuer
}

// ---- the probe render over a deployment template ----

func TestBuildCatalogProbeTemplateRender(t *testing.T) {
	cfg, _ := probeTemplateConfig(t)
	budget := time.Minute
	job, secret, _, _ := mustBuildProbe(t, cfg, testRequestID, testProbeRuntime, testWorkspaceID, budget)

	// The probe's own identity overwrites the template's task identity.
	if job.Name != testProbeName {
		t.Errorf("job name = %q, want %q", job.Name, testProbeName)
	}
	if job.Namespace != cfg.JobNamespace {
		t.Errorf("namespace = %q, want the configured %q", job.Namespace, cfg.JobNamespace)
	}
	if job.Namespace == "other-namespace" {
		t.Errorf("namespace = %q, want the template's value overwritten", job.Namespace)
	}
	// The foreman.tsic.top/ domain is cleared before the probe block is
	// written: no task-id survives, out-of-domain keys do.
	if _, ok := job.Labels["foreman.tsic.top/task-id"]; ok {
		t.Error("probe Job kept the template's task-id label")
	}
	wantLabels := map[string]string{
		"app.kubernetes.io/name":       "foreman-job",
		"app.kubernetes.io/managed-by": "foreman",
		"foreman.tsic.top/role":        "catalog-probe",
		"foreman.tsic.top/daemon-id":   testProbeName,
		"cost-center":                  "platform",
	}
	if len(job.Labels) != len(wantLabels) {
		t.Fatalf("job labels = %v, want exactly %v", job.Labels, wantLabels)
	}
	for key, want := range wantLabels {
		if job.Labels[key] != want {
			t.Errorf("label %q = %q, want %q", key, job.Labels[key], want)
		}
	}
	for key, value := range job.Annotations {
		if strings.HasPrefix(key, foremanDomain) {
			t.Errorf("annotation %q = %q survives in the foreman domain", key, value)
		}
	}
	if job.Annotations["example.com/note"] != "keep-me" {
		t.Errorf("out-of-domain annotation = %v, want the template's own kept", job.Annotations)
	}
	// <job runtime uuid> is one of the four placeholders a probe consumes.
	if got := job.Annotations["example.com/runtime"]; got != testProbeRuntime {
		t.Errorf("<job runtime uuid> = %q, want %q", got, testProbeRuntime)
	}
	// <job-name> and <daemon-id> are two more: the probe's own final Job name
	// and its identity name — the same string on this path, since a probe is
	// identified by its Job name.
	if got := job.Annotations["example.com/probe-identity"]; got != testProbeName+"|"+testProbeName {
		t.Errorf("<job-name>|<daemon-id> = %q, want %q", got, testProbeName+"|"+testProbeName)
	}

	podLabels := job.Spec.Template.Labels
	if podLabels["foreman.tsic.top/role"] != "catalog-probe" || podLabels["app.kubernetes.io/name"] != "foreman-job" {
		t.Errorf("pod labels = %v, want the probe block's name and role", podLabels)
	}
	if _, ok := podLabels["foreman.tsic.top/task-id"]; ok {
		t.Error("probe pod kept the template's task-id label")
	}
	if podLabels["cost-center"] != "platform" {
		t.Errorf("pod labels = %v, want the template's out-of-domain label kept", podLabels)
	}
	for key, value := range job.Spec.Template.Annotations {
		if strings.HasPrefix(key, foremanDomain) {
			t.Errorf("pod annotation %q = %q survives in the foreman domain", key, value)
		}
	}

	// A1.1 writes the three daemon keys over the template's values.
	daemon := job.Spec.Template.Spec.Containers[0]
	if got := envByName(t, daemon.Env, "MULTICA_SERVER_URL").Value; got != foremanServerURL {
		t.Errorf("MULTICA_SERVER_URL = %q, want Foreman's %q", got, foremanServerURL)
	}
	daemonID := envByName(t, daemon.Env, "MULTICA_DAEMON_ID")
	if daemonID.ValueFrom == nil || daemonID.ValueFrom.FieldRef == nil ||
		daemonID.ValueFrom.FieldRef.FieldPath != "metadata.labels['foreman.tsic.top/daemon-id']" {
		t.Errorf("MULTICA_DAEMON_ID = %+v, want the daemon-id label fieldRef", daemonID)
	}
	if got := envByName(t, daemon.Env, "MULTICA_DAEMON_MAX_CONCURRENT_TASKS").Value; got != "1" {
		t.Errorf("MULTICA_DAEMON_MAX_CONCURRENT_TASKS = %q, want 1", got)
	}
	serverURLEntries := 0
	for _, entry := range daemon.Env {
		if entry.Name == "MULTICA_SERVER_URL" {
			serverURLEntries++
		}
	}
	if serverURLEntries != 1 {
		t.Errorf("MULTICA_SERVER_URL appears %d times, want one authoritative entry", serverURLEntries)
	}

	// A1.4 ③: the heartbeat value is Foreman's, not the template's 60s.
	heartbeats := 0
	for _, entry := range daemon.Env {
		if entry.Name != "MULTICA_DAEMON_HEARTBEAT_INTERVAL" {
			continue
		}
		heartbeats++
		if entry.Value != "15s" {
			t.Errorf("heartbeat = %q, want the fixed 15s", entry.Value)
		}
	}
	if heartbeats != 1 {
		t.Errorf("heartbeat key appears %d times, want one", heartbeats)
	}

	// A1.4 ④: the profile literal is renamed on its faces — the daemon's start
	// arguments and the mount path the profile directory sits at.
	if got := strings.Join(daemon.Args, " "); !strings.Contains(got, "--profile foreman-catalog") {
		t.Errorf("daemon args = %q, want the probe profile", got)
	}
	if got := mountPathByName(t, daemon.VolumeMounts, "cred"); got != "/home/agent/.multica/profiles/foreman-catalog/config.json" {
		t.Errorf("cred mount = %q, want the probe profile path", got)
	}
	credSrc := volumeByName(t, job.Spec.Template.Spec.Volumes, "cred-src")
	if credSrc.Secret == nil || credSrc.Secret.SecretName != testProbeName+"-cred" {
		t.Errorf("cred-src = %+v, want the probe's Secret", credSrc.VolumeSource)
	}
	if secret.Name != testProbeName+"-cred" || secret.Labels["foreman.tsic.top/role"] != "catalog-probe" {
		t.Errorf("secret = %s with labels %v, want the probe's credential Secret", secret.Name, secret.Labels)
	}

	// <job-image> is consumed; the rename ran first, so the reference's own
	// "foreman-job" survives.
	if daemon.Image != testProbeImage {
		t.Errorf("daemon image = %q, want the verbatim %q", daemon.Image, testProbeImage)
	}

	// Task-identity and agent placeholders are not the probe's to resolve.
	if got := envByName(t, daemon.Env, "TASK_ANCHOR").Value; got != "<task_id>" {
		t.Errorf("TASK_ANCHOR = %q, want the <task_id> literal left verbatim", got)
	}
	if got := envByName(t, daemon.Env, "AGENT_ANCHOR").Value; got != "<agent-name>" {
		t.Errorf("AGENT_ANCHOR = %q, want the <agent-name> literal left verbatim", got)
	}

	// No soft affinity: the item is deleted with the structures it empties.
	if job.Spec.Template.Spec.Affinity != nil {
		t.Errorf("affinity = %+v, want the item and its empty parents deleted", job.Spec.Template.Spec.Affinity)
	}

	// The TTL stays the template's; the deadline is the probe's own budget,
	// the one field Foreman writes on a probe beyond the A1 faces (§8.4).
	if *job.Spec.ActiveDeadlineSeconds != int64(budget/time.Second) || *job.Spec.TTLSecondsAfterFinished != 600 {
		t.Errorf("deadline/ttl = %d/%d, want the budget %d and the template's 600",
			*job.Spec.ActiveDeadlineSeconds, *job.Spec.TTLSecondsAfterFinished, int64(budget/time.Second))
	}
}

// probeLiteralTemplate is the shape the whole-object literal rename corrupted: a
// deployment template that carries its own image references (the published
// repository name reads "foreman-job") and expresses the profile and the
// credential as their task-side literals. A probe render may differ from it on
// the faces A1.4 ④ declares and nowhere else.
const probeLiteralTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
  labels: { cost-center: foreman-job }
spec:
  backoffLimit: 0
  template:
    metadata:
      labels: { cost-center: foreman-job }
    spec:
      restartPolicy: Never
      imagePullSecrets:
        - { name: foreman-job-pull }
      initContainers:
        - name: prepare
          image: ghcr.io/tsic404/foreman-job:v0.1.0
          command: ["/bin/sh", "-ec"]
          args: ["install -d /home/agent/.multica/profiles/foreman-job"]
          volumeMounts:
            - { name: home, mountPath: /home/agent }
      containers:
        - name: agent
          image: ghcr.io/tsic404/foreman-job:v0.1.0
          command: ["/usr/bin/dumb-init", "--", "/usr/local/bin/multica"]
          args: ["daemon", "start", "--foreground", "--profile", "foreman-job"]
          env:
            - { name: MULTICA_AGENT_RUNTIME_NAME, value: "foreman-job" }
            - { name: MULTICA_SERVER_URL, value: "http://somewhere-else:1234" }
          volumeMounts:
            - { name: cred, mountPath: /home/agent/.multica/profiles/foreman-job/config.json, subPath: config.json, readOnly: true }
        - name: relay
          image: ghcr.io/tsic404/foreman-job:v0.1.0
          command: ["/bin/sh", "-ec", "exec multica daemon start --foreground --profile foreman-job"]
          volumeMounts:
            - { name: home, mountPath: /home/agent }
      volumes:
        - name: home
          hostPath: { path: /var/lib/foreman/home, type: DirectoryOrCreate }
        - name: cred
          emptyDir: { sizeLimit: 16Mi }
        - name: cred-src
          secret: { secretName: fm-<task_id>-cred, defaultMode: 0400 }
        - name: projected-creds
          projected:
            sources:
              - secret: { name: fm-<task_id>-cred }
`

// TestWriteProbeDeadlineRoundsUp pins the probe deadline's whole-second
// representation (§8.4): activeDeadlineSeconds counts seconds, so a positive
// budget rounds up — a sub-second budget renders one second, never the zero
// that would kill the Job at creation — while a non-positive budget writes
// nothing and leaves the template's value in place.
func TestWriteProbeDeadlineRoundsUp(t *testing.T) {
	positive := []struct {
		name   string
		budget time.Duration
		want   int64
	}{
		{"sub-second rounds up", 500 * time.Millisecond, 1},
		{"fraction rounds up", 1500 * time.Millisecond, 2},
		{"whole seconds are exact", 45 * time.Second, 45},
	}
	for _, tc := range positive {
		job := &batchv1.Job{}
		writeProbeDeadline(job, tc.budget)
		if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != tc.want {
			t.Errorf("%s: deadline = %v, want %d", tc.name, job.Spec.ActiveDeadlineSeconds, tc.want)
		}
	}

	template := int64(86400)
	for _, budget := range []time.Duration{0, -time.Second} {
		job := &batchv1.Job{Spec: batchv1.JobSpec{ActiveDeadlineSeconds: &template}}
		writeProbeDeadline(job, budget)
		if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != template {
			t.Errorf("budget %s: the template's deadline was overwritten (%v)", budget, job.Spec.ActiveDeadlineSeconds)
		}
	}
}

// TestBuildCatalogProbeKeepsTemplateLiterals (AC-13's shape rule, AC-32): the
// two probe literals are renamed only on the faces A1.4 ④ declares, so a
// template's own image references — and every other position off those faces —
// reach the probe Job verbatim. A whole-object rename would turn the repository
// name into the probe profile's and the probe would pull an image that does not
// exist.
func TestBuildCatalogProbeKeepsTemplateLiterals(t *testing.T) {
	const literalImage = "ghcr.io/tsic404/foreman-job:v0.1.0"

	cfg, _ := templateConfig(t, probeLiteralTemplate)
	cfg.JobImage = testProbeImage
	job, _, _, _ := mustBuildProbe(t, cfg, testRequestID, testProbeRuntime, testWorkspaceID, time.Minute)
	podSpec := job.Spec.Template.Spec

	for _, container := range podSpec.InitContainers {
		if container.Image != literalImage {
			t.Errorf("init container %q image = %q, want the template's verbatim %q",
				container.Name, container.Image, literalImage)
		}
	}
	for _, container := range podSpec.Containers {
		if container.Image != literalImage {
			t.Errorf("container %q image = %q, want the template's verbatim %q",
				container.Name, container.Image, literalImage)
		}
	}
	if len(podSpec.ImagePullSecrets) != 1 || podSpec.ImagePullSecrets[0].Name != "foreman-job-pull" {
		t.Errorf("imagePullSecrets = %v, want the template's own pull credential", podSpec.ImagePullSecrets)
	}

	// The profile face: the daemon's start line and the mount path beside it.
	daemon := podSpec.Containers[0]
	if got := strings.Join(daemon.Args, " "); !strings.Contains(got, "--profile foreman-catalog") {
		t.Errorf("daemon args = %q, want the probe profile", got)
	}
	if got := strings.Join(podSpec.Containers[1].Command, " "); !strings.Contains(got, "--profile foreman-catalog") {
		t.Errorf("relay command = %q, want the probe profile", got)
	}
	if got := strings.Join(podSpec.InitContainers[0].Args, " "); !strings.Contains(got, "profiles/foreman-catalog") {
		t.Errorf("prepare args = %q, want the probe profile directory", got)
	}
	if got := mountPathByName(t, daemon.VolumeMounts, "cred"); got != "/home/agent/.multica/profiles/foreman-catalog/config.json" {
		t.Errorf("cred mount = %q, want the probe profile path", got)
	}

	// The credential face: every Secret reference a volume carries.
	credSrc := volumeByName(t, podSpec.Volumes, "cred-src")
	if credSrc.Secret == nil || credSrc.Secret.SecretName != testProbeName+"-cred" {
		t.Errorf("cred-src = %+v, want the probe's Secret", credSrc.VolumeSource)
	}
	projected := volumeByName(t, podSpec.Volumes, "projected-creds")
	if len(projected.Projected.Sources) != 1 || projected.Projected.Sources[0].Secret.Name != testProbeName+"-cred" {
		t.Errorf("projected-creds = %+v, want the probe's Secret", projected.VolumeSource)
	}

	// Off the face the template's own text survives: the runtime name env value
	// and the deployer's labels are the template's, not the probe's.
	if got := envByName(t, daemon.Env, "MULTICA_AGENT_RUNTIME_NAME").Value; got != "foreman-job" {
		t.Errorf("MULTICA_AGENT_RUNTIME_NAME = %q, want the template's own %q", got, "foreman-job")
	}
	if got := job.Labels["cost-center"]; got != "foreman-job" {
		t.Errorf("job label cost-center = %q, want the template's own %q", got, "foreman-job")
	}
	if got := job.Spec.Template.Labels["cost-center"]; got != "foreman-job" {
		t.Errorf("pod label cost-center = %q, want the template's own %q", got, "foreman-job")
	}
}

// ---- the token's lifetime ----

// TestBuildCatalogProbeTokenLifetime (ADR-017): the probe's Job Token must
// outlive the probe's own budget by the drain bound, or the daemon's shutdown
// requests land after the credential is gone.
func TestBuildCatalogProbeTokenLifetime(t *testing.T) {
	cases := []struct {
		name   string
		budget time.Duration
		drain  time.Duration
	}{
		{"contract default drain", 45 * time.Second, DefaultJobDrainTimeout},
		{"explicit drain bound", 15 * time.Second, 90 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.JobDrainTimeout = tc.drain
			_, _, _, issuer := mustBuildProbe(t, cfg, testRequestID, testProbeRuntime, testWorkspaceID, tc.budget)

			if want := tc.budget + tc.drain; issuer.probeTTL != want {
				t.Errorf("probe TTL = %s, want the budget %s plus the drain bound %s", issuer.probeTTL, tc.budget, tc.drain)
			}
			if issuer.probeJobName != testProbeName {
				t.Errorf("probe job name = %q, want %q", issuer.probeJobName, testProbeName)
			}
			if issuer.probeRequestID != testRequestID {
				t.Errorf("probe request id = %q, want %q", issuer.probeRequestID, testRequestID)
			}
			if issuer.probeWorkspaceID != testWorkspaceID {
				t.Errorf("probe workspace id = %q, want %q", issuer.probeWorkspaceID, testWorkspaceID)
			}
		})
	}
	if DefaultJobDrainTimeout != 120*time.Second || DefaultProbeHeartbeatInterval != 15*time.Second {
		t.Errorf("defaults = %s/%s, want the contract's 120s and 15s",
			DefaultJobDrainTimeout, DefaultProbeHeartbeatInterval)
	}
}

// ---- failure faces ----

// TestLoadConfigProbeTimeouts: the two values the probe render and its token
// lifetime read. The drain bound is the scheduler's env, with the same default
// and the same "zero is legal" rule; the heartbeat interval is Foreman's fixed
// value and has no env at all.
func TestLoadConfigProbeTimeouts(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.JobDrainTimeout != DefaultJobDrainTimeout || cfg.ProbeHeartbeatInterval != DefaultProbeHeartbeatInterval {
		t.Errorf("drain/heartbeat = %s/%s, want %s/%s",
			cfg.JobDrainTimeout, cfg.ProbeHeartbeatInterval, DefaultJobDrainTimeout, DefaultProbeHeartbeatInterval)
	}

	// The heartbeat interval is not deployer-tunable: the key's name in the
	// container is not a config entry.
	cfg, err = LoadConfig(envFrom(map[string]string{"MULTICA_DAEMON_HEARTBEAT_INTERVAL": "60s", EnvJobDrainTimeout: "0"}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.ProbeHeartbeatInterval != DefaultProbeHeartbeatInterval {
		t.Errorf("ProbeHeartbeatInterval = %s, want the fixed %s", cfg.ProbeHeartbeatInterval, DefaultProbeHeartbeatInterval)
	}
	if cfg.JobDrainTimeout != 0 {
		t.Errorf("JobDrainTimeout = %s, want the configured 0", cfg.JobDrainTimeout)
	}

	cfg, err = LoadConfig(envFrom(map[string]string{EnvJobDrainTimeout: "2m"}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.JobDrainTimeout != 2*time.Minute {
		t.Errorf("JobDrainTimeout = %s, want the configured 2m", cfg.JobDrainTimeout)
	}

	for _, raw := range []string{"-1s", "soon"} {
		if _, err := LoadConfig(envFrom(map[string]string{EnvJobDrainTimeout: raw})); err == nil {
			t.Errorf("%s = %q: err = nil, want a refusal", EnvJobDrainTimeout, raw)
		}
	}
}

func TestBuildCatalogProbeInvalidRequestID(t *testing.T) {
	builder := newTestBuilder(t, testConfig())
	for _, requestID := range []string{"", "Not_A_Label", strings.Repeat("a", 60)} {
		job, secret, tokenHash, err := builder.BuildCatalogProbe(requestID, testProbeRuntime, testWorkspaceID, time.Minute)
		if !errors.Is(err, ErrInvalidRequestID) {
			t.Errorf("request id %q: err = %v, want ErrInvalidRequestID", requestID, err)
		}
		if job != nil || secret != nil || tokenHash != "" {
			t.Errorf("request id %q: got objects (job=%v secret=%v hash=%q), want none", requestID, job, secret, tokenHash)
		}
	}
}

func TestBuildCatalogProbeRequiresIssuer(t *testing.T) {
	builder := newTestBuilder(t, testConfig())
	if _, _, _, err := builder.BuildCatalogProbe(testRequestID, testProbeRuntime, testWorkspaceID, time.Minute); err == nil {
		t.Fatal("BuildCatalogProbe without an issuer: err = nil, want a refusal")
	}
}

// TestBuildCatalogProbeTemplateCopy: the startup-parsed template is read-only,
// so a probe built after a task Job carries the probe's own Secret reference
// and never the task's.
func TestBuildCatalogProbeTemplateCopy(t *testing.T) {
	cfg, _ := probeTemplateConfig(t)
	builder, err := NewBuilder(cfg)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	if _, _, _, err := builder.Build(testEntry()); err != nil {
		t.Fatalf("Build: %v", err)
	}
	job, _, _, err := builder.BuildCatalogProbe(testRequestID, testProbeRuntime, testWorkspaceID, time.Minute)
	if err != nil {
		t.Fatalf("BuildCatalogProbe: %v", err)
	}
	rendered := canonicalJSON(job)
	if strings.Contains(rendered, testEntry().TaskID) {
		t.Errorf("probe Job carries the task's identity:\n%s", rendered)
	}
	if got := volumeByName(t, job.Spec.Template.Spec.Volumes, "cred-src").Secret.SecretName; got != testProbeName+"-cred" {
		t.Errorf("probe cred-src = %q, want %q", got, testProbeName+"-cred")
	}
}
