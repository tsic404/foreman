package jobbuilder

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	testTaskID  = "3f6b1a2c-9d4e-4a5b-8c7d-0e1f2a3b4c5d"
	testJobName = "fm-" + testTaskID
	testDigest  = "sha256:4242424242424242424242424242424242424242424242424242424242424242"
)

var testClaimedAt = time.Date(2026, 10, 4, 8, 30, 0, 0, time.UTC)

type fakeIssuer struct {
	jobName     string
	taskID      string
	workspaceID string
	ttl         time.Duration
	token       string
	err         error
}

func (f *fakeIssuer) Issue(jobName, taskID, workspaceID string, ttl time.Duration) (string, error) {
	f.jobName, f.taskID, f.workspaceID, f.ttl = jobName, taskID, workspaceID, ttl
	if f.err != nil {
		return "", f.err
	}
	return f.token, nil
}

type fakeNodes map[string]string

func (n fakeNodes) LastNodeForIssue(issueID string) string { return n[issueID] }

func testConfig() Config {
	cfg, err := LoadConfig(envFrom(map[string]string{EnvJobImageDigest: testDigest}))
	if err != nil {
		panic(err)
	}
	return cfg
}

func testEntry() TaskEntry {
	return TaskEntry{
		TaskID:          testTaskID,
		AgentID:         "0a1b2c3d-4e5f-4a6b-9c8d-7e6f5a4b3c2d",
		IssueID:         "aa10a105-59cf-4b6c-964e-4f857b580bcf",
		IssueIdentifier: "MULTI-9000",
		WorkspaceID:     "c20ff019-2980-4936-8cb0-22d79d694e71",
		JobRuntimeID:    "5d4c3b2a-1e0f-4a9b-8c7d-6e5f4a3b2c1d",
		ClaimedAt:       testClaimedAt,
		Attempt:         1,
	}
}

func testPayload() json.RawMessage {
	return json.RawMessage(`{"id":"` + testTaskID + `","auth_token":"mat_secret","remote_mcp_daemon_token":"mcp_secret","agent":{"id":"a1"}}`)
}

func mustBuild(t *testing.T, cfg Config, e TaskEntry, payload json.RawMessage) (*batchv1.Job, *corev1.Secret) {
	t.Helper()
	issuer := &fakeIssuer{token: "fmj_test.sig"}
	cfg.Issuer = issuer
	job, secret, err := NewBuilder(cfg).Build(e, payload)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if job == nil || secret == nil {
		t.Fatalf("Build returned nil objects (job=%v, secret=%v)", job, secret)
	}
	return job, secret
}

func TestBuildJobMetadata(t *testing.T) {
	job, _ := mustBuild(t, testConfig(), testEntry(), testPayload())

	if job.Name != testJobName {
		t.Errorf("job name = %q, want %q", job.Name, testJobName)
	}
	if job.Namespace != DefaultJobNamespace {
		t.Errorf("namespace = %q, want %q", job.Namespace, DefaultJobNamespace)
	}
	wantLabels := map[string]string{
		"app.kubernetes.io/name":        "foreman-job",
		"app.kubernetes.io/managed-by":  "foreman",
		"foreman.tsic.top/task-id":      testTaskID,
		"foreman.tsic.top/agent-id":     "0a1b2c3d-4e5f-4a6b-9c8d-7e6f5a4b3c2d",
		"foreman.tsic.top/workspace-id": "c20ff019-2980-4936-8cb0-22d79d694e71",
	}
	if len(job.Labels) != len(wantLabels) {
		t.Fatalf("job labels = %v, want exactly %v", job.Labels, wantLabels)
	}
	for k, v := range wantLabels {
		if job.Labels[k] != v {
			t.Errorf("label %q = %q, want %q", k, job.Labels[k], v)
		}
	}
	wantAnnotations := map[string]string{
		"foreman.tsic.top/issue-id":         "aa10a105-59cf-4b6c-964e-4f857b580bcf",
		"foreman.tsic.top/issue-identifier": "MULTI-9000",
		"foreman.tsic.top/daemon-id":        testJobName,
		"foreman.tsic.top/job-runtime-id":   "5d4c3b2a-1e0f-4a9b-8c7d-6e5f4a3b2c1d",
		"foreman.tsic.top/claimed-at":       "2026-10-04T08:30:00Z",
		"foreman.tsic.top/attempt":          "1",
	}
	for k, v := range wantAnnotations {
		if job.Annotations[k] != v {
			t.Errorf("annotation %q = %q, want %q", k, job.Annotations[k], v)
		}
	}
}

func TestBuildJobSpec(t *testing.T) {
	job, _ := mustBuild(t, testConfig(), testEntry(), testPayload())
	spec := job.Spec

	if *spec.BackoffLimit != 0 || *spec.Completions != 1 || *spec.Parallelism != 1 {
		t.Errorf("backoff/completions/parallelism = %d/%d/%d, want 0/1/1",
			*spec.BackoffLimit, *spec.Completions, *spec.Parallelism)
	}
	if *spec.TTLSecondsAfterFinished != DefaultJobTTLSeconds {
		t.Errorf("ttlSecondsAfterFinished = %d, want %d", *spec.TTLSecondsAfterFinished, DefaultJobTTLSeconds)
	}
	if *spec.ActiveDeadlineSeconds != int64(DefaultTaskMaxDuration/time.Second) {
		t.Errorf("activeDeadlineSeconds = %d, want %d", *spec.ActiveDeadlineSeconds, int64(DefaultTaskMaxDuration/time.Second))
	}
}

func TestBuildPodSpec(t *testing.T) {
	job, _ := mustBuild(t, testConfig(), testEntry(), testPayload())
	pod := job.Spec.Template

	wantPodLabels := map[string]string{
		"app.kubernetes.io/name":   "foreman-job",
		"foreman.tsic.top/task-id": testTaskID,
	}
	if len(pod.Labels) != len(wantPodLabels) {
		t.Fatalf("pod labels = %v, want exactly %v", pod.Labels, wantPodLabels)
	}
	for k, v := range wantPodLabels {
		if pod.Labels[k] != v {
			t.Errorf("pod label %q = %q, want %q", k, pod.Labels[k], v)
		}
	}

	spec := pod.Spec
	if spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q, want Never", spec.RestartPolicy)
	}
	if *spec.TerminationGracePeriodSeconds != 30 {
		t.Errorf("terminationGracePeriodSeconds = %d, want 30", *spec.TerminationGracePeriodSeconds)
	}
	if spec.ServiceAccountName != "multica-omp" {
		t.Errorf("serviceAccountName = %q, want multica-omp", spec.ServiceAccountName)
	}
	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Errorf("automountServiceAccountToken = %v, want false", spec.AutomountServiceAccountToken)
	}
	sc := spec.SecurityContext
	if *sc.RunAsUser != 1000 || *sc.RunAsGroup != 1000 || *sc.FSGroup != 1000 || !*sc.RunAsNonRoot {
		t.Errorf("pod securityContext = %+v, want uid/gid/fsGroup 1000 + runAsNonRoot", sc)
	}
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("seccompProfile = %+v, want RuntimeDefault", sc.SeccompProfile)
	}
	if len(spec.ImagePullSecrets) != 1 || spec.ImagePullSecrets[0].Name != "registry-tsic" {
		t.Errorf("imagePullSecrets = %v, want [registry-tsic]", spec.ImagePullSecrets)
	}
	if spec.NodeSelector["kubernetes.io/os"] != "linux" {
		t.Errorf("nodeSelector = %v, want kubernetes.io/os=linux", spec.NodeSelector)
	}
	if len(spec.Tolerations) != 2 {
		t.Fatalf("tolerations = %v, want 2 entries", spec.Tolerations)
	}
	for i, key := range []string{"unreachable", "maybe_unreachable"} {
		tol := spec.Tolerations[i]
		if tol.Key != key || tol.Operator != corev1.TolerationOpExists || tol.Effect != corev1.TaintEffectNoExecute {
			t.Errorf("toleration[%d] = %+v, want key=%s Exists/NoExecute", i, tol, key)
		}
	}
	if spec.HostNetwork || spec.HostPID {
		t.Error("hostNetwork/hostPID must stay false (F4)")
	}

	if len(spec.TopologySpreadConstraints) != 1 {
		t.Fatalf("topologySpreadConstraints = %v, want 1 entry", spec.TopologySpreadConstraints)
	}
	tsc := spec.TopologySpreadConstraints[0]
	if tsc.MaxSkew != 3 || tsc.TopologyKey != "kubernetes.io/hostname" || tsc.WhenUnsatisfiable != corev1.ScheduleAnyway {
		t.Errorf("topologySpreadConstraint = %+v, want maxSkew=3 hostname ScheduleAnyway", tsc)
	}
	if tsc.LabelSelector.MatchLabels["app.kubernetes.io/name"] != "foreman-job" {
		t.Errorf("topologySpread labelSelector = %v", tsc.LabelSelector.MatchLabels)
	}
}

func TestBuildContainers(t *testing.T) {
	job, _ := mustBuild(t, testConfig(), testEntry(), testPayload())
	spec := job.Spec.Template.Spec

	if len(spec.Containers) != 1 {
		t.Fatalf("containers = %d, want exactly 1 (ADR-001)", len(spec.Containers))
	}
	if len(spec.InitContainers) != 1 {
		t.Fatalf("initContainers = %d, want 1", len(spec.InitContainers))
	}

	agent := spec.Containers[0]
	wantImage := DefaultJobImage + "@" + testDigest
	if agent.Image != wantImage {
		t.Errorf("agent image = %q, want %q", agent.Image, wantImage)
	}
	if agent.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("agent imagePullPolicy = %q", agent.ImagePullPolicy)
	}
	if agent.WorkingDir != "/home/agent" {
		t.Errorf("agent workingDir = %q", agent.WorkingDir)
	}
	if strings.Join(agent.Command, " ") != "/usr/local/bin/multica" || strings.Join(agent.Args, " ") != "daemon start --foreground" {
		t.Errorf("agent command/args = %v %v", agent.Command, agent.Args)
	}
	if !agent.TTY {
		t.Error("agent tty must be true so daemon logs go to stderr")
	}
	asc := agent.SecurityContext
	if *asc.RunAsUser != 1000 || !*asc.RunAsNonRoot || *asc.AllowPrivilegeEscalation || *asc.ReadOnlyRootFilesystem {
		t.Errorf("agent securityContext = %+v, want uid 1000 non-root no-escalation rw-rootfs", asc)
	}
	if len(asc.Capabilities.Drop) != 1 || asc.Capabilities.Drop[0] != "ALL" {
		t.Errorf("agent capabilities = %+v, want drop ALL", asc.Capabilities)
	}
	if agent.Resources.Requests.Cpu().String() != "1" || agent.Resources.Requests.Memory().String() != "2Gi" {
		t.Errorf("agent requests = %v", agent.Resources.Requests)
	}
	if agent.Resources.Limits.Cpu().String() != "4" || agent.Resources.Limits.Memory().String() != "8Gi" {
		t.Errorf("agent limits = %v", agent.Resources.Limits)
	}

	wantMounts := []string{"home", "cred", "workspaces", "tmp"}
	if len(agent.VolumeMounts) != len(wantMounts) {
		t.Fatalf("agent volumeMounts = %v, want %v", agent.VolumeMounts, wantMounts)
	}
	for i, name := range wantMounts {
		if agent.VolumeMounts[i].Name != name {
			t.Errorf("agent volumeMount[%d] = %q, want %q", i, agent.VolumeMounts[i].Name, name)
		}
	}

	init := spec.InitContainers[0]
	if init.Name != "prepare" || init.Image != wantImage {
		t.Errorf("init container = %q/%q, want prepare/%q", init.Name, init.Image, wantImage)
	}
	if strings.Join(init.Command, " ") != "/bin/sh -ec" {
		t.Errorf("init command = %v", init.Command)
	}
	wantScript := "cp /cred/config.json /home/agent/cred/config.json\nchmod 0400 /home/agent/cred/config.json\nchown -R 1000:1000 /home/agent /state/workspaces\n"
	if len(init.Args) != 1 || init.Args[0] != wantScript {
		t.Errorf("init args = %q, want %q", init.Args, wantScript)
	}
	isc := init.SecurityContext
	if *isc.RunAsUser != 0 || *isc.RunAsNonRoot || *isc.AllowPrivilegeEscalation || !*isc.ReadOnlyRootFilesystem {
		t.Errorf("init securityContext = %+v, want uid 0 root-only ro-rootfs no-escalation", isc)
	}
	if init.Resources.Requests.Cpu().String() != "20m" || init.Resources.Limits.Memory().String() != "128Mi" {
		t.Errorf("init resources = %v", init.Resources)
	}
	if len(init.VolumeMounts) != 4 || init.VolumeMounts[3].Name != "cred-src" || !init.VolumeMounts[3].ReadOnly {
		t.Errorf("init volumeMounts = %v, want cred-src readOnly", init.VolumeMounts)
	}
}

func TestBuildAgentEnv(t *testing.T) {
	job, _ := mustBuild(t, testConfig(), testEntry(), testPayload())
	env := job.Spec.Template.Spec.Containers[0].Env

	want := []struct {
		name      string
		value     string
		fieldPath string
	}{
		{"MULTICA_SERVER_URL", "http://foreman.foreman.svc.cluster.local:8080", ""},
		{"MULTICA_TASK_CONFIG_ROOT", "/home/agent/cred", ""},
		{"MULTICA_DAEMON_ID", "", "metadata.labels['job-name']"},
		{"MULTICA_DAEMON_DEVICE_NAME", "", "spec.nodeName"},
		{"MULTICA_AGENT_RUNTIME_NAME", "foreman-job", ""},
		{"MULTICA_DAEMON_MAX_CONCURRENT_TASKS", "1", ""},
		{"MULTICA_WORKSPACES_ROOT", "/state/workspaces", ""},
		{"TMPDIR", "/tmp", ""},
		{"TMP", "/tmp", ""},
		{"TEMP", "/tmp", ""},
		{"MULTICA_GC_ENABLED", "false", ""},
		{"MULTICA_DAEMON_AUTO_UPDATE", "false", ""},
		{"MULTICA_DAEMON_AUTO_RELOAD", "false", ""},
		{"MULTICA_OMP_PATH", "/usr/local/bin/omp", ""},
		{"HOME", "/home/agent", ""},
		{"LOG_LEVEL", "info", ""},
	}
	if len(env) != len(want) {
		t.Fatalf("env has %d entries, want exactly %d (contract §5.2)", len(env), len(want))
	}
	for i, w := range want {
		got := env[i]
		if got.Name != w.name {
			t.Errorf("env[%d].Name = %q, want %q", i, got.Name, w.name)
			continue
		}
		if w.fieldPath == "" {
			if got.Value != w.value || got.ValueFrom != nil {
				t.Errorf("env[%d] %s = %+v, want value %q", i, w.name, got, w.value)
			}
			continue
		}
		if got.ValueFrom == nil || got.ValueFrom.FieldRef == nil || got.ValueFrom.FieldRef.FieldPath != w.fieldPath {
			t.Errorf("env[%d] %s = %+v, want fieldRef %q", i, w.name, got, w.fieldPath)
		}
	}
}

func TestBuildVolumes(t *testing.T) {
	job, _ := mustBuild(t, testConfig(), testEntry(), testPayload())
	volumes := job.Spec.Template.Spec.Volumes

	if len(volumes) != 5 {
		t.Fatalf("volumes = %d, want 5", len(volumes))
	}
	byName := make(map[string]corev1.Volume, len(volumes))
	for _, v := range volumes {
		byName[v.Name] = v
	}
	for name, wantPath := range map[string]string{"home": "/var/lib/foreman/home", "workspaces": "/var/lib/foreman/workspaces"} {
		v := byName[name]
		if v.HostPath == nil || v.HostPath.Path != wantPath || *v.HostPath.Type != corev1.HostPathDirectoryOrCreate {
			t.Errorf("volume %q = %+v, want hostPath %s DirectoryOrCreate", name, v, wantPath)
		}
	}
	if v := byName["tmp"]; v.EmptyDir == nil || v.EmptyDir.SizeLimit == nil || v.EmptyDir.SizeLimit.String() != "2Gi" {
		t.Errorf("volume tmp = %+v, want emptyDir 2Gi", v)
	}
	if v := byName["cred"]; v.EmptyDir == nil || v.EmptyDir.SizeLimit == nil || v.EmptyDir.SizeLimit.String() != "16Mi" {
		t.Errorf("volume cred = %+v, want emptyDir 16Mi", v)
	}
	credSrc := byName["cred-src"]
	if credSrc.Secret == nil || credSrc.Secret.SecretName != testJobName+"-cred" || *credSrc.Secret.DefaultMode != 0o400 {
		t.Errorf("volume cred-src = %+v, want secret %s-cred mode 0400", credSrc, testJobName)
	}
}

func TestBuildIsolatedCacheMode(t *testing.T) {
	cfg := testConfig()
	cfg.RepoCacheMode = CacheModeIsolated
	job, _ := mustBuild(t, cfg, testEntry(), testPayload())

	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.HostPath != nil {
			t.Errorf("isolated mode: volume %q still has hostPath %q", v.Name, v.HostPath.Path)
		}
		if (v.Name == "home" || v.Name == "workspaces") && v.EmptyDir == nil {
			t.Errorf("isolated mode: volume %q must be emptyDir", v.Name)
		}
	}
}

func TestBuildPreferredNodeAffinity(t *testing.T) {
	entry := testEntry()

	cfg := testConfig()
	cfg.Nodes = fakeNodes{entry.IssueID: "company-02"}
	job, _ := mustBuild(t, cfg, entry, testPayload())
	affinity := job.Spec.Template.Spec.Affinity
	if affinity == nil || affinity.NodeAffinity == nil {
		t.Fatal("affinity missing for known node")
	}
	terms := affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 || terms[0].Weight != 100 {
		t.Fatalf("preferred terms = %+v, want 1 term weight 100", terms)
	}
	expr := terms[0].Preference.MatchExpressions
	if len(expr) != 1 || expr[0].Key != "kubernetes.io/hostname" || expr[0].Operator != corev1.NodeSelectorOpIn ||
		len(expr[0].Values) != 1 || expr[0].Values[0] != "company-02" {
		t.Errorf("affinity expression = %+v, want hostname In [company-02]", expr)
	}

	cfg.PreferNodeReuse = false
	job, _ = mustBuild(t, cfg, entry, testPayload())
	if job.Spec.Template.Spec.Affinity != nil {
		t.Error("affinity must be omitted when FOREMAN_PREFER_NODE_REUSE=false")
	}

	cfg = testConfig()
	cfg.Nodes = fakeNodes{}
	job, _ = mustBuild(t, cfg, entry, testPayload())
	if job.Spec.Template.Spec.Affinity != nil {
		t.Error("affinity must be omitted when no node history exists")
	}
}

func TestBuildActiveDeadlineDisabled(t *testing.T) {
	cfg := testConfig()
	cfg.TaskMaxDuration = 0
	job, _ := mustBuild(t, cfg, testEntry(), testPayload())
	if job.Spec.ActiveDeadlineSeconds != nil {
		t.Errorf("activeDeadlineSeconds = %v, want nil when FOREMAN_TASK_MAX_DURATION=0", *job.Spec.ActiveDeadlineSeconds)
	}
}

func TestBuildInvalidTaskID(t *testing.T) {
	for _, taskID := range []string{
		"",
		"UPPER-CASE",
		"has space",
		"underscore_ok_no",
		strings.Repeat("a", 62), // fm- + 62 chars exceeds the 63-char DNS-1123 label limit
	} {
		entry := testEntry()
		entry.TaskID = taskID
		job, secret, err := NewBuilder(testConfigWithIssuer()).Build(entry, testPayload())
		if err == nil {
			t.Errorf("taskID %q: expected error, got job=%v", taskID, job != nil)
		}
		if job != nil || secret != nil {
			t.Errorf("taskID %q: objects must be nil on error", taskID)
		}
	}
}

func TestBuildIssuerError(t *testing.T) {
	cfg := testConfig()
	cfg.Issuer = &fakeIssuer{err: errors.New("boom")}
	if _, _, err := NewBuilder(cfg).Build(testEntry(), testPayload()); err == nil {
		t.Fatal("expected issuer error to propagate")
	}
}

func TestBuildMissingIssuer(t *testing.T) {
	if _, _, err := NewBuilder(testConfig()).Build(testEntry(), testPayload()); err == nil {
		t.Fatal("expected error when Config.Issuer is unset")
	}
}

func TestBuildSecret(t *testing.T) {
	cfg := testConfig()
	issuer := &fakeIssuer{token: "fmj_payload.sig"}
	cfg.Issuer = issuer
	entry := testEntry()
	_, secret, err := NewBuilder(cfg).Build(entry, testPayload())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if issuer.jobName != testJobName || issuer.taskID != entry.TaskID || issuer.workspaceID != entry.WorkspaceID || issuer.ttl != cfg.JobTokenTTL {
		t.Errorf("Issue called with (%q, %q, %q, %s)", issuer.jobName, issuer.taskID, issuer.workspaceID, issuer.ttl)
	}
	if secret.Name != testJobName+"-cred" || secret.Namespace != DefaultJobNamespace {
		t.Errorf("secret %q/%q", secret.Namespace, secret.Name)
	}
	wantLabels := map[string]string{
		"app.kubernetes.io/managed-by": "foreman",
		"foreman.tsic.top/task-id":     testTaskID,
	}
	if len(secret.Labels) != len(wantLabels) {
		t.Fatalf("secret labels = %v, want exactly %v", secret.Labels, wantLabels)
	}
	for k, v := range wantLabels {
		if secret.Labels[k] != v {
			t.Errorf("secret label %q = %q, want %q", k, secret.Labels[k], v)
		}
	}
	if secret.Type != corev1.SecretTypeOpaque {
		t.Errorf("secret type = %q, want Opaque", secret.Type)
	}
	var config struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(secret.StringData["config.json"]), &config); err != nil {
		t.Fatalf("config.json is not valid JSON: %v", err)
	}
	if config.Token != "fmj_payload.sig" {
		t.Errorf("config.json token = %q, want the issued token", config.Token)
	}
}

func TestPayloadAnnotation(t *testing.T) {
	build := func(t *testing.T, payload json.RawMessage) map[string]string {
		t.Helper()
		job, _ := mustBuild(t, testConfig(), testEntry(), payload)
		return job.Annotations
	}

	t.Run("strips credential fields and base64-encodes the rest", func(t *testing.T) {
		annotations := build(t, testPayload())
		encoded, ok := annotations["foreman.tsic.top/payload"]
		if !ok {
			t.Fatal("payload annotation missing")
		}
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatalf("annotation is not valid base64: %v", err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatalf("annotation is not valid JSON: %v", err)
		}
		if _, ok := fields["auth_token"]; ok {
			t.Error("auth_token must be stripped")
		}
		if _, ok := fields["remote_mcp_daemon_token"]; ok {
			t.Error("remote_mcp_daemon_token must be stripped")
		}
		if _, ok := fields["id"]; !ok {
			t.Error("non-sensitive fields must survive")
		}
	})

	t.Run("omitted when a remaining field matches the server token shape", func(t *testing.T) {
		for _, payload := range []string{
			`{"id":"x","note":"use mdt_abc123"}`,
			`{"nested":{"token":"mul_xyz"}}`,
		} {
			if _, ok := build(t, json.RawMessage(payload))["foreman.tsic.top/payload"]; ok {
				t.Errorf("payload %s must drop the annotation", payload)
			}
		}
	})

	t.Run("omitted when the payload exceeds 128KB", func(t *testing.T) {
		payload := json.RawMessage(`{"id":"x","blob":"` + strings.Repeat("a", 200*1024) + `"}`)
		if _, ok := build(t, payload)["foreman.tsic.top/payload"]; ok {
			t.Error("oversized payload must drop the annotation")
		}
	})

	t.Run("omitted when the payload is missing or unparseable", func(t *testing.T) {
		for _, payload := range []json.RawMessage{nil, {}, json.RawMessage(`{not json`)} {
			if _, ok := build(t, payload)["foreman.tsic.top/payload"]; ok {
				t.Errorf("payload %q must drop the annotation", payload)
			}
		}
	})
}

func testConfigWithIssuer() Config {
	cfg := testConfig()
	cfg.Issuer = &fakeIssuer{token: "fmj_test.sig"}
	return cfg
}
