package jobbuilder

import (
	"errors"
	"slices"
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
	// testProbeImage carries "foreman-job" inside the reference: a probe render
	// renames that literal everywhere it appears *before* the image value lands,
	// so the reference must come back intact (AC-13's shape rule).
	testProbeImage = "ghcr.io/tsic404/foreman-job:v0.1.0"
	// testWorkspaceID is the workspace a probe token is scoped to.
	testWorkspaceID = "c20ff019-2980-4936-8cb0-22d79d694e71"
)

// probeTemplate is a deployment template written the way the probe render
// expects: the profile and the credential Secret as their task-side literals,
// the two consumed placeholders, and one <task_id> the probe must leave alone.
const probeTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: <job-name>
  namespace: other-namespace
  labels:
    app.kubernetes.io/name: foreman-job
    app.kubernetes.io/managed-by: foreman
    foreman.tsic.top/task-id: "<task_id>"
    cost-center: platform
  annotations:
    foreman.tsic.top/daemon-id: "<job-name>"
    example.com/note: keep-me
    example.com/runtime: "<job runtime uuid>"
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
        foreman.tsic.top/daemon-id: "<job-name>"
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
          volumeMounts:
            - { name: cred, mountPath: /home/agent/.multica/profiles/foreman-job/config.json, subPath: config.json, readOnly: true }
      volumes:
        - name: cred-src
          secret: { secretName: <job-name>-cred, defaultMode: 0400 }
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

// ---- CM-absent window: the built-in default probe Job ----

func TestBuildCatalogProbeDefaultShape(t *testing.T) {
	cfg := testConfig()
	budget := 45 * time.Second
	job, secret, tokenHash, issuer := mustBuildProbe(t, cfg, testRequestID, testProbeRuntime, testWorkspaceID, budget)

	if job.Name != testProbeName {
		t.Errorf("job name = %q, want %q", job.Name, testProbeName)
	}
	if job.Namespace != DefaultJobNamespace {
		t.Errorf("namespace = %q, want %q", job.Namespace, DefaultJobNamespace)
	}
	wantLabels := map[string]string{
		"app.kubernetes.io/name":       "foreman-job",
		"app.kubernetes.io/managed-by": "foreman",
		"foreman.tsic.top/role":        "catalog-probe",
	}
	if len(job.Labels) != len(wantLabels) {
		t.Fatalf("job labels = %v, want exactly %v (no task-id)", job.Labels, wantLabels)
	}
	for key, want := range wantLabels {
		if job.Labels[key] != want {
			t.Errorf("label %q = %q, want %q", key, job.Labels[key], want)
		}
	}
	if _, ok := job.Labels["foreman.tsic.top/task-id"]; ok {
		t.Error("probe Job carries foreman.tsic.top/task-id: it would enter the task mapping")
	}
	// A probe maps no task, so it carries none of the §3.2 annotations.
	if len(job.Annotations) != 0 {
		t.Errorf("annotations = %v, want none", job.Annotations)
	}

	spec := job.Spec
	if *spec.BackoffLimit != 0 || *spec.Completions != 1 || *spec.Parallelism != 1 {
		t.Errorf("backoff/completions/parallelism = %d/%d/%d, want 0/1/1",
			*spec.BackoffLimit, *spec.Completions, *spec.Parallelism)
	}
	if *spec.TTLSecondsAfterFinished != 300 {
		t.Errorf("ttlSecondsAfterFinished = %d, want the probe's fixed 300", *spec.TTLSecondsAfterFinished)
	}
	if *spec.ActiveDeadlineSeconds != int64(budget/time.Second) {
		t.Errorf("activeDeadlineSeconds = %d, want the remaining budget %d", *spec.ActiveDeadlineSeconds, int64(budget/time.Second))
	}

	pod := spec.Template
	wantPodLabels := map[string]string{
		"app.kubernetes.io/name": "foreman-job",
		"foreman.tsic.top/role":  "catalog-probe",
	}
	if len(pod.Labels) != len(wantPodLabels) {
		t.Fatalf("pod labels = %v, want exactly %v", pod.Labels, wantPodLabels)
	}
	for key, want := range wantPodLabels {
		if pod.Labels[key] != want {
			t.Errorf("pod label %q = %q, want %q", key, pod.Labels[key], want)
		}
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q, want Never", pod.Spec.RestartPolicy)
	}
	if pod.Spec.ServiceAccountName != "multica-omp" {
		t.Errorf("serviceAccountName = %q, want multica-omp", pod.Spec.ServiceAccountName)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Errorf("automountServiceAccountToken = %v, want false", pod.Spec.AutomountServiceAccountToken)
	}
	if pod.Spec.Affinity != nil {
		t.Errorf("affinity = %+v, want none: a probe has no soft affinity", pod.Spec.Affinity)
	}
	if len(pod.Spec.TopologySpreadConstraints) != 0 {
		t.Errorf("topologySpreadConstraints = %v, want none", pod.Spec.TopologySpreadConstraints)
	}

	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("containers = %v, want the daemon alone", containerNames(pod.Spec.Containers))
	}
	daemon := pod.Spec.Containers[0]
	if daemon.Name != "daemon" {
		t.Errorf("container name = %q, want daemon", daemon.Name)
	}
	if daemon.Image != cfg.JobImage || daemon.ImagePullPolicy != corev1.PullAlways {
		t.Errorf("daemon image/policy = %q/%q, want %q/Always", daemon.Image, daemon.ImagePullPolicy, cfg.JobImage)
	}
	if got := strings.Join(daemon.Command, " "); got != "/usr/bin/dumb-init -- /usr/local/bin/multica" {
		t.Errorf("daemon command = %v, want the dumb-init PID 1 prefix", daemon.Command)
	}
	if got := strings.Join(daemon.Args, " "); got != "daemon start --foreground --profile foreman-catalog" {
		t.Errorf("daemon args = %v, want the probe profile", daemon.Args)
	}
	if !daemon.TTY {
		t.Error("daemon tty = false, want true (the daemon logs to stderr when it is a terminal)")
	}
	wantRequests := map[corev1.ResourceName]string{corev1.ResourceCPU: "100m", corev1.ResourceMemory: "256Mi"}
	wantLimits := map[corev1.ResourceName]string{corev1.ResourceCPU: "1", corev1.ResourceMemory: "1Gi"}
	for name, want := range wantRequests {
		quantity := daemon.Resources.Requests[name]
		if got := quantity.String(); got != want {
			t.Errorf("daemon request %s = %s, want %s", name, got, want)
		}
	}
	for name, want := range wantLimits {
		quantity := daemon.Resources.Limits[name]
		if got := quantity.String(); got != want {
			t.Errorf("daemon limit %s = %s, want %s", name, got, want)
		}
	}

	// The 15 daemon keys verbatim, then the probe's heartbeat key.
	wantEnv := append(envNames(agentEnv()), "MULTICA_DAEMON_HEARTBEAT_INTERVAL")
	if got := envNames(daemon.Env); !slices.Equal(got, wantEnv) {
		t.Errorf("daemon env = %v, want the task daemon set plus the heartbeat key %v", got, wantEnv)
	}
	if got := envByName(t, daemon.Env, "MULTICA_DAEMON_HEARTBEAT_INTERVAL").Value; got != "15s" {
		t.Errorf("heartbeat = %q, want the fixed 15s", got)
	}

	// The credential is mounted single-file where the probe profile reads it.
	wantCredMount := "/home/agent/.multica/profiles/foreman-catalog/config.json"
	if got := mountPathByName(t, daemon.VolumeMounts, volumeCred); got != wantCredMount {
		t.Errorf("cred mount = %q, want %q", got, wantCredMount)
	}
	for _, mount := range daemon.VolumeMounts {
		if mount.Name == volumeCred && (mount.SubPath != configFileKey || !mount.ReadOnly) {
			t.Errorf("cred mount = %+v, want subPath %s readOnly", mount, configFileKey)
		}
	}
	if got := mountPathByName(t, daemon.VolumeMounts, volumeWorkspaces); got != "/state/workspaces" {
		t.Errorf("workspaces mount = %q, want /state/workspaces", got)
	}

	if len(pod.Spec.InitContainers) != 1 {
		t.Fatalf("initContainers = %v, want the prepare container", containerNames(pod.Spec.InitContainers))
	}
	prepare := pod.Spec.InitContainers[0]
	if prepare.Name != containerPrepare || prepare.Image != cfg.JobImage {
		t.Errorf("prepare = %q/%q, want %s/%s", prepare.Name, prepare.Image, containerPrepare, cfg.JobImage)
	}
	script := strings.Join(prepare.Args, "\n")
	if !strings.Contains(script, probeProfileDir) {
		t.Errorf("prepare script does not target the probe profile:\n%s", script)
	}
	if strings.Contains(script, profileDir) {
		t.Errorf("prepare script still targets the task profile:\n%s", script)
	}

	// Volumes: the node-local home, a probe-private workspaces emptyDir, and
	// the credential Secret under the probe's own name.
	home := volumeByName(t, pod.Spec.Volumes, volumeHome)
	if home.HostPath == nil || home.HostPath.Path != "/var/lib/foreman/home" {
		t.Errorf("home volume = %+v, want the state root's hostPath", home.VolumeSource)
	}
	for name, want := range map[string]string{volumeTmp: "1Gi", volumeCred: "16Mi", volumeWorkspaces: "1Gi"} {
		vol := volumeByName(t, pod.Spec.Volumes, name)
		if vol.EmptyDir == nil || vol.EmptyDir.SizeLimit.String() != want {
			t.Errorf("volume %s = %+v, want an emptyDir of %s", name, vol.VolumeSource, want)
		}
	}
	credSrc := volumeByName(t, pod.Spec.Volumes, volumeCredSrc)
	if credSrc.Secret == nil || credSrc.Secret.SecretName != testProbeName+"-cred" {
		t.Errorf("cred-src = %+v, want Secret %s-cred", credSrc.VolumeSource, testProbeName)
	}
	if credSrc.Secret.DefaultMode == nil || *credSrc.Secret.DefaultMode != 0o400 {
		t.Errorf("cred-src defaultMode = %v, want 0400", credSrc.Secret.DefaultMode)
	}

	// The Secret: one Job Token, probe-labelled, never task-labelled.
	if secret.Name != testProbeName+"-cred" || secret.Namespace != DefaultJobNamespace {
		t.Errorf("secret = %s/%s, want %s/%s", secret.Namespace, secret.Name, DefaultJobNamespace, testProbeName+"-cred")
	}
	if len(secret.Labels) != len(wantLabels) {
		t.Fatalf("secret labels = %v, want exactly %v", secret.Labels, wantLabels)
	}
	for key, want := range wantLabels {
		if secret.Labels[key] != want {
			t.Errorf("secret label %q = %q, want %q", key, secret.Labels[key], want)
		}
	}
	if secret.Type != corev1.SecretTypeOpaque {
		t.Errorf("secret type = %q, want Opaque", secret.Type)
	}
	if len(secret.StringData) != 1 || secret.StringData[configFileKey] != `{"token":"`+issuer.token+`"}` {
		t.Errorf("secret keys = %v, want the one Job Token under %s", secret.StringData, configFileKey)
	}
	if tokenHash != tokenDigest(issuer.token) {
		t.Errorf("token hash = %q, want the digest of the token the Secret carries", tokenHash)
	}
}

// ---- CM present: the probe render over a deployment template ----

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
	// <job runtime uuid> is one of the two placeholders a probe consumes.
	if got := job.Annotations["example.com/runtime"]; got != testProbeRuntime {
		t.Errorf("<job runtime uuid> = %q, want %q", got, testProbeRuntime)
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
		daemonID.ValueFrom.FieldRef.FieldPath != "metadata.labels['job-name']" {
		t.Errorf("MULTICA_DAEMON_ID = %+v, want the job-name fieldRef", daemonID)
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

	// A1.4 ④: the two task-side literals are renamed across the whole object.
	if got := strings.Join(daemon.Args, " "); !strings.Contains(got, "--profile foreman-catalog") {
		t.Errorf("daemon args = %q, want the probe profile", got)
	}
	if got := mountPathByName(t, daemon.VolumeMounts, volumeCred); got != "/home/agent/.multica/profiles/foreman-catalog/config.json" {
		t.Errorf("cred mount = %q, want the probe profile path", got)
	}
	credSrc := volumeByName(t, job.Spec.Template.Spec.Volumes, volumeCredSrc)
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

	// Task-identity placeholders are not the probe's to resolve.
	if got := envByName(t, daemon.Env, "TASK_ANCHOR").Value; got != "<task_id>" {
		t.Errorf("TASK_ANCHOR = %q, want the <task_id> literal left verbatim", got)
	}

	// No soft affinity: the item is deleted with the structures it empties.
	if job.Spec.Template.Spec.Affinity != nil {
		t.Errorf("affinity = %+v, want the item and its empty parents deleted", job.Spec.Template.Spec.Affinity)
	}

	// The probe consumes neither the template's deadline nor its TTL.
	if *job.Spec.ActiveDeadlineSeconds != 86400 || *job.Spec.TTLSecondsAfterFinished != 600 {
		t.Errorf("deadline/ttl = %d/%d, want the template's own values",
			*job.Spec.ActiveDeadlineSeconds, *job.Spec.TTLSecondsAfterFinished)
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
	if got := volumeByName(t, job.Spec.Template.Spec.Volumes, volumeCredSrc).Secret.SecretName; got != testProbeName+"-cred" {
		t.Errorf("probe cred-src = %q, want %q", got, testProbeName+"-cred")
	}
}
