package jobbuilder

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const testSidecarDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
const testPrepareInitDigest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"

// parseOverlay turns overlay YAML into the raw map LoadAndValidateOverlay
// would produce from the ConfigMap key.
func parseOverlay(t *testing.T, manifest string) map[string]any {
	t.Helper()
	jsonBytes, err := utilyaml.ToJSON([]byte(manifest))
	if err != nil {
		t.Fatalf("overlay is not valid YAML: %v", err)
	}
	var overlay map[string]any
	if err := json.Unmarshal(jsonBytes, &overlay); err != nil {
		t.Fatalf("overlay is not a JSON object: %v", err)
	}
	return overlay
}

// overlayConfig writes the overlay where the Deployment mounts it and returns
// the matching config plus the captured startup log.
func overlayConfig(t *testing.T, manifest string) (Config, *bytes.Buffer) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "job-overlay.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write overlay: %v", err)
	}
	logs := &bytes.Buffer{}
	cfg := testConfig()
	cfg.OverlayPath = path
	cfg.Issuer = &fakeIssuer{token: "fmj_test.sig"}
	cfg.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	return cfg, logs
}

func TestLoadAndValidateOverlayAbsent(t *testing.T) {
	cfg := testConfig()
	cfg.OverlayPath = filepath.Join(t.TempDir(), "job-overlay.yaml")
	logs := &bytes.Buffer{}
	cfg.Logger = slog.New(slog.NewJSONHandler(logs, nil))

	overlay, present, err := LoadAndValidateOverlay(cfg)
	if err != nil {
		t.Fatalf("a missing overlay must fall back to the default template, got %v", err)
	}
	if present || overlay != nil {
		t.Fatalf("present=%v overlay=%v, want the default-template path", present, overlay)
	}
	if !strings.Contains(logs.String(), "job overlay absent") {
		t.Errorf("startup log must warn about the fallback, got %s", logs.String())
	}
}

func TestLoadAndValidateOverlayAccepts(t *testing.T) {
	cfg, logs := overlayConfig(t, `
spec:
  template:
    spec:
      priorityClassName: batch-low
`)
	overlay, present, err := LoadAndValidateOverlay(cfg)
	if err != nil {
		t.Fatalf("a legal overlay must be accepted: %v", err)
	}
	if !present || overlay == nil {
		t.Fatalf("present=%v overlay=%v, want the overlay map", present, overlay)
	}
	log := logs.String()
	if !strings.Contains(log, "overlay_sha256") {
		t.Errorf("startup log must record the overlay hash, got %s", log)
	}
	if !strings.Contains(log, "takes effect after a restart") {
		t.Errorf("startup log must state that a ConfigMap change needs a restart, got %s", log)
	}
}

// TestLoadAndValidateOverlayRejectsEmptyDocument: a ConfigMap key created
// empty holds no Job, so it refuses startup instead of silently becoming a
// no-op overlay.
func TestLoadAndValidateOverlayRejectsEmptyDocument(t *testing.T) {
	cfg, _ := overlayConfig(t, "# the key exists but carries no Job\n")
	builder, err := NewBuilder(cfg)
	if err == nil {
		t.Fatalf("an empty overlay must be refused, got builder %v", builder != nil)
	}
	if !strings.Contains(err.Error(), "empty document") {
		t.Errorf("refusal must explain the empty document, got %v", err)
	}
}

// TestInvariantCheckerOwnsDomainKeys exercises the stage-2 half of the C-5/A
// domain rule directly (the hook bypasses the raw-map scan, like an overlay
// that slipped through stage 1).
func TestInvariantCheckerOwnsDomainKeys(t *testing.T) {
	cases := []struct {
		name    string
		overlay string
		want    string
	}{
		{"pod annotation in the foreman domain", `spec:
  template:
    metadata:
      annotations:
        foreman.tsic.top/evil: "x"`, "spec.template.metadata.annotations[foreman.tsic.top/evil]"},
		{"reserved app key on the pod template", `spec:
  template:
    metadata:
      labels:
        app.kubernetes.io/managed-by: someone-else`, "spec.template.metadata.labels[app.kubernetes.io/managed-by]"},
		{"job label in the foreman domain", `metadata:
  labels:
    foreman.tsic.top/evil: "x"`, "metadata.labels[foreman.tsic.top/evil]"},
		{"job annotation in the foreman domain", `metadata:
  annotations:
    foreman.tsic.top/evil: "x"`, "metadata.annotations[foreman.tsic.top/evil]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.Issuer = &fakeIssuer{token: "fmj_test.sig"}
			cfg.Logger = discardLogger()

			job, secret, err := NewBuilderWithOverlay(cfg, parseOverlay(t, tc.overlay)).Build(testEntry())
			if err == nil {
				t.Fatalf("an overlay-owned reserved key must be rejected (job=%v secret=%v)", job != nil, secret != nil)
			}
			if job != nil || secret != nil {
				t.Fatal("Build must not produce objects for a rejected template")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("violation must name %q, got %v", tc.want, err)
			}
		})
	}
}

// TestVolumeSecretRefs pins the volume-source coverage of the C-2/F3 ban: the
// plain secret volume was the only carrier checked before.
func TestVolumeSecretRefs(t *testing.T) {
	localRef := func(name string) *corev1.LocalObjectReference {
		return &corev1.LocalObjectReference{Name: name}
	}
	cases := []struct {
		name string
		vol  corev1.Volume
		want []string
	}{
		{"secret", corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "s1"}}}, []string{"s1"}},
		{"projected", corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
				{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}},
				{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "s2"}}},
			}}}}, []string{"s2"}},
		{"csi", corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
			CSI: &corev1.CSIVolumeSource{NodePublishSecretRef: localRef("s3")}}}, []string{"s3"}},
		{"iscsi", corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
			ISCSI: &corev1.ISCSIVolumeSource{SecretRef: localRef("s4")}}}, []string{"s4"}},
		{"rbd", corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
			RBD: &corev1.RBDVolumeSource{SecretRef: localRef("s5")}}}, []string{"s5"}},
		{"flexVolume", corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
			FlexVolume: &corev1.FlexVolumeSource{SecretRef: localRef("s6")}}}, []string{"s6"}},
		{"azureFile", corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
			AzureFile: &corev1.AzureFileVolumeSource{SecretName: "s7"}}}, []string{"s7"}},
		{"cephfs", corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
			CephFS: &corev1.CephFSVolumeSource{SecretRef: localRef("s8")}}}, []string{"s8"}},
		{"cinder", corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
			Cinder: &corev1.CinderVolumeSource{SecretRef: localRef("s10")}}}, []string{"s10"}},
		{"scaleIO", corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
			ScaleIO: &corev1.ScaleIOVolumeSource{SecretRef: localRef("s11")}}}, []string{"s11"}},
		{"storageOS", corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
			StorageOS: &corev1.StorageOSVolumeSource{SecretRef: localRef("s12")}}}, []string{"s12"}},
		{"hostPath carries none", corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: "/var/lib/foreman/x"}}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			refs := volumeSecretRefs(tc.vol)
			names := make([]string, 0, len(refs))
			for _, ref := range refs {
				names = append(names, ref.name)
			}
			if !equalStrings(names, tc.want) {
				t.Errorf("volumeSecretRefs = %v, want %v", names, tc.want)
			}
		})
	}

	projected := corev1.Volume{Name: "v", VolumeSource: corev1.VolumeSource{
		Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
			{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "s"}}},
		}}}}
	if refs := volumeSecretRefs(projected); len(refs) != 1 || refs[0].path != "projected.sources[0].secret.name" {
		t.Errorf("projected ref path = %+v, want projected.sources[0].secret.name", refs)
	}
}

// TestStateRootTrailingSlashAccepted: FOREMAN_STATE_ROOT is operator input; a
// trailing slash must not turn the built-in hostPath volumes into a C-2
// violation that refuses startup.
func TestStateRootTrailingSlashAccepted(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{
		EnvStateRoot: "/var/lib/foreman/",
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.StateRoot != "/var/lib/foreman" {
		t.Errorf("StateRoot = %q, want the cleaned /var/lib/foreman", cfg.StateRoot)
	}

	cfg.Issuer = &fakeIssuer{token: "fmj_test.sig"}
	cfg.Logger = discardLogger()
	job, _ := mustBuild(t, cfg, testEntry())
	for _, vol := range job.Spec.Template.Spec.Volumes {
		if vol.Name == volumeHome {
			if vol.HostPath == nil || vol.HostPath.Path != "/var/lib/foreman/home" {
				t.Errorf("home hostPath = %+v, want /var/lib/foreman/home", vol.VolumeSource)
			}
		}
	}
}

// TestHostPathRuntimeRules pins the narrowed C-2 socket rule: docker.sock and
// containerd.sock are violations, while a runtime-looking path below the state
// root is only warned about (C-2 names the two sockets, nothing wider).
func TestHostPathRuntimeRules(t *testing.T) {
	t.Run("docker.sock is rejected", func(t *testing.T) {
		cfg, _ := overlayConfig(t, `spec:
  template:
    spec:
      volumes:
        - name: sock
          hostPath: { path: /var/lib/foreman/var/run/docker.sock, type: Socket }`)
		builder, err := NewBuilder(cfg)
		if err == nil {
			_, _, err = builder.Build(testEntry())
		}
		if err == nil || !strings.Contains(err.Error(), "docker.sock") {
			t.Fatalf("docker.sock hostPath must be rejected, got %v", err)
		}
	})

	t.Run("runtime-looking path below the state root is warned about only", func(t *testing.T) {
		cfg, logs := overlayConfig(t, `spec:
  template:
    spec:
      volumes:
        - name: cache
          hostPath: { path: /var/lib/foreman/containerd-cache, type: DirectoryOrCreate }`)
		job, _ := mustBuild(t, cfg, testEntry())
		if !strings.Contains(logs.String(), "outside 清单 C-2") {
			t.Errorf("expected a warning for the runtime-looking path, got %s", logs.String())
		}
		found := false
		for _, vol := range job.Spec.Template.Spec.Volumes {
			if vol.Name == "cache" {
				found = true
			}
		}
		if !found {
			t.Error("the overlay volume must survive the merge")
		}
	})
}

// validOverlay is the TC-tech-job-02 (AC-18) overlay: a native sidecar, a
// prepare-init entry, a strengthened agent securityContext, a new env, a PVC
// volume plus mount, labels and priorityClassName.
const validOverlay = `
spec:
  template:
    metadata:
      labels: { cost-center: "platform" }
      annotations: { example.com/team: "runtime" }
    spec:
      priorityClassName: batch-low
      containers:
        - name: agent
          imagePullPolicy: IfNotPresent
          env:
            - { name: HTTP_PROXY, value: "http://proxy.corp:3128" }
          securityContext:
            readOnlyRootFilesystem: true
          volumeMounts:
            - { name: models, mountPath: /opt/models, readOnly: true }
      initContainers:
        - name: prepare
          imagePullPolicy: IfNotPresent
        - name: wrapper-setup
          image: registry.corp.example/bootstrap@` + testPrepareInitDigest + `
          command: ["/bin/sh", "-ec"]
          args: ["install -d -o 1000 -g 1000 /home/agent/bin"]
          securityContext: { runAsUser: 0, runAsNonRoot: false, allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"], add: ["CHOWN", "DAC_OVERRIDE"] }, privileged: false }
          volumeMounts:
            - { name: home, mountPath: /home/agent }
        - name: log-shipper
          restartPolicy: Always
          image: registry.corp.example/fluent-bit@` + testSidecarDigest + `
          securityContext: { runAsNonRoot: true, allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } }
          volumeMounts:
            - { name: tmp, mountPath: /logs }
      volumes:
        - name: models
          persistentVolumeClaim: { claimName: shared-models, readOnly: true }
`

func TestBuildWithValidOverlay(t *testing.T) {
	cfg, _ := overlayConfig(t, validOverlay)
	job, _ := mustBuild(t, cfg, testEntry())
	pod := job.Spec.Template.Spec

	if job.Spec.Template.Labels["cost-center"] != "platform" {
		t.Errorf("free pod label did not survive the merge: %v", job.Spec.Template.Labels)
	}
	if job.Spec.Template.Annotations["example.com/team"] != "runtime" {
		t.Errorf("free pod annotation did not survive the merge: %v", job.Spec.Template.Annotations)
	}
	if pod.PriorityClassName != "batch-low" {
		t.Errorf("priorityClassName = %q, want batch-low", pod.PriorityClassName)
	}

	// containers stays a single business container; auxiliary containers are
	// appended init entries in overlay writing order after the built-in prepare.
	if len(pod.Containers) != 1 || pod.Containers[0].Name != containerAgent {
		t.Fatalf("containers = %v, want exactly [agent]", containerNames(pod.Containers))
	}
	wantInit := []string{containerPrepare, "wrapper-setup", "log-shipper"}
	if got := containerNames(pod.InitContainers); !equalStrings(got, wantInit) {
		t.Fatalf("initContainers = %v, want %v", got, wantInit)
	}

	wrapper := containerByName(t, pod.InitContainers, "wrapper-setup")
	if wrapper.RestartPolicy != nil {
		t.Errorf("prepare-init must carry no restartPolicy, got %q", *wrapper.RestartPolicy)
	}
	sc := wrapper.SecurityContext
	if sc.RunAsUser == nil || *sc.RunAsUser != 0 || sc.RunAsNonRoot == nil || *sc.RunAsNonRoot {
		t.Errorf("prepare-init root pair = %v/%v, want 0/false", sc.RunAsUser, sc.RunAsNonRoot)
	}
	if sc.Privileged == nil || *sc.Privileged {
		t.Errorf("prepare-init privileged must stay false, got %v", sc.Privileged)
	}
	if got := capabilityStrings(sc.Capabilities.Add); !equalStrings(got, []string{"CHOWN", "DAC_OVERRIDE"}) {
		t.Errorf("prepare-init capabilities.add = %v", got)
	}
	if got := mountNames(wrapper.VolumeMounts); !equalStrings(got, []string{volumeHome}) {
		t.Errorf("prepare-init mounts = %v, want [home]", got)
	}

	sidecar := containerByName(t, pod.InitContainers, "log-shipper")
	if sidecar.RestartPolicy == nil || *sidecar.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Errorf("native sidecar restartPolicy = %v, want Always", sidecar.RestartPolicy)
	}
	if got := mountNames(sidecar.VolumeMounts); !equalStrings(got, []string{volumeTmp}) {
		t.Errorf("sidecar mounts = %v, want [tmp] (the only shared exception)", got)
	}

	// agent and prepare carry the FOREMAN_JOB_IMAGE reference verbatim
	// (ADR-012); overlay-appended entries keep their digest pin (清单 C-8).
	if pod.Containers[0].Image != cfg.imageRef() || pod.InitContainers[0].Image != cfg.imageRef() {
		t.Errorf("agent/prepare image = %q/%q, want %q", pod.Containers[0].Image, pod.InitContainers[0].Image, cfg.imageRef())
	}
	if pod.Containers[0].Image != pod.InitContainers[0].Image {
		t.Errorf("agent image %q != prepare image %q, want one shared reference (清单 B)", pod.Containers[0].Image, pod.InitContainers[0].Image)
	}
	for _, ct := range []corev1.Container{wrapper, sidecar} {
		if !digestPinnedImage.MatchString(ct.Image) {
			t.Errorf("appended entry %s image %q is not digest-pinned (清单 C-8)", ct.Name, ct.Image)
		}
	}

	// §5.4「imagePullPolicy」段: the free field reaches both built-in
	// containers — the deployer's pinned-tag escape hatch depends on it.
	if pod.Containers[0].ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("agent imagePullPolicy = %q, want the overlay's IfNotPresent", pod.Containers[0].ImagePullPolicy)
	}
	if pod.InitContainers[0].ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("prepare imagePullPolicy = %q, want the overlay's IfNotPresent", pod.InitContainers[0].ImagePullPolicy)
	}

	// 清单 A: the 15 authoritative env keys survive and the overlay env is added.
	names := envNames(pod.Containers[0].Env)
	if len(names) != 16 {
		t.Errorf("agent env count = %d (%v), want 15 authoritative + HTTP_PROXY", len(names), names)
	}
	httpProxy := envByName(t, pod.Containers[0].Env, "HTTP_PROXY")
	if httpProxy.Value != "http://proxy.corp:3128" {
		t.Errorf("HTTP_PROXY = %q", httpProxy.Value)
	}
	if pod.Containers[0].SecurityContext.ReadOnlyRootFilesystem == nil || !*pod.Containers[0].SecurityContext.ReadOnlyRootFilesystem {
		t.Error("C-6 strengthening of readOnlyRootFilesystem did not take effect")
	}

	// PVC volume plus its mount.
	models := volumeByName(t, pod.Volumes, "models")
	if models.PersistentVolumeClaim == nil || models.PersistentVolumeClaim.ClaimName != "shared-models" {
		t.Errorf("models volume = %+v", models.VolumeSource)
	}
	if got := mountPathByName(t, pod.Containers[0].VolumeMounts, "models"); got != "/opt/models" {
		t.Errorf("models mountPath = %q", got)
	}

	// 清单 A: the authoritative fields are byte-identical to the default
	// template even with the overlay in place (AC-18 不变量不动).
	agent := pod.Containers[0]
	if !equalStrings(agent.Command, []string{agentCommand}) || !equalStrings(agent.Args, []string{"daemon", "start", "--foreground", "--profile", jobProfile}) {
		t.Errorf("agent entrypoint drifted: %v %v", agent.Command, agent.Args)
	}
	if !agent.TTY || agent.WorkingDir != agentWorkingDir {
		t.Errorf("agent tty/workingDir drifted: %v %q", agent.TTY, agent.WorkingDir)
	}
	if job.Labels[labelTaskID] != testEntry().TaskID || job.Labels[labelAgentID] != testEntry().AgentID {
		t.Errorf("identity labels drifted: %v", job.Labels)
	}
	credSrc := volumeByName(t, pod.Volumes, volumeCredSrc)
	if credSrc.Secret == nil || credSrc.Secret.SecretName != job.Name+secretCredSuffix {
		t.Errorf("cred-src volume drifted: %+v", credSrc.VolumeSource)
	}
	if path := mountPathByName(t, agent.VolumeMounts, volumeCred); path != mountCredFile {
		t.Errorf("cred mountPath = %q, want %q", path, mountCredFile)
	}
}

// TestOverlayNegativeMatrix is the AC-19 matrix: every illegal overlay must
// refuse startup with the offending field path in the rendered violations
// (test-cases/job-template.md TC-tech-job-03).
func TestOverlayNegativeMatrix(t *testing.T) {
	cases := []struct {
		name  string
		index int
		body  string
		want  []string
	}{
		{"variant-1", 1, `spec:
  template:
    spec:
      priorityClassName: "unclosed`, []string{"job-overlay.yaml"}},
		{"variant-2", 2, `apiVersion: apps/v1
kind: Deployment
spec:
  replicas: 3`, []string{"kind", "apiVersion"}},
		{"variant-3", 3, `spec:
  template:
    spec:
      containers:
        - name: agent
          command: ["/bin/sh"]`, []string{"spec.template.spec.containers[0].command"}},
		{"variant-4", 4, `spec:
  template:
    spec:
      containers:
        - name: sidecar-evil
          image: registry.corp.example/evil@` + testSidecarDigest, []string{"spec.template.spec.containers"}},
		{"variant-5", 5, `spec:
  template:
    spec:
      containers:
        - name: agent
          env:
            - { name: MULTICA_SERVER_URL, value: "http://multica.tsic.top" }`, []string{"env[MULTICA_SERVER_URL]"}},
		{"variant-6", 6, `spec:
  template:
    spec:
      containers:
        - name: agent
          env:
            - { name: MULTICA_FOO, value: "1" }`, []string{"env[MULTICA_FOO]"}},
		{"variant-7", 7, `metadata:
  labels:
    foreman.tsic.top/task-id: "x"`, []string{"metadata.labels[foreman.tsic.top/task-id]"}},
		{"variant-8", 8, `spec:
  template:
    spec:
      hostNetwork: true`, []string{"spec.template.spec.hostNetwork"}},
		{"variant-9", 9, `spec:
  template:
    spec:
      initContainers:
        - name: log-shipper
          restartPolicy: Always
          image: registry.corp.example/fluent-bit@` + testSidecarDigest + `
          securityContext: { allowPrivilegeEscalation: true, capabilities: { drop: ["ALL"] } }`, []string{"securityContext.allowPrivilegeEscalation"}},
		{"variant-10", 10, `spec:
  template:
    spec:
      volumes:
        - name: evil
          hostPath: { path: /etc, type: Directory }`, []string{"volumes[].hostPath.path"}},
		{"variant-11", 11, `spec:
  template:
    spec:
      volumes:
        - name: cred-leak
          secret: { secretName: foreman-secret }`, []string{"volumes[].secret.secretName"}},
		{"variant-12", 12, `spec:
  template:
    metadata:
      annotations:
        example.com/note: "token mdt_test123"`, []string{"(mdt|mul)_", "spec.template.metadata.annotations"}},
		{"variant-13", 13, `spec:
  template:
    spec:
      nodeSelector: { disktype: ssd }`, []string{"spec.template.spec.nodeSelector"}},
		{"variant-14", 14, `spec:
  template:
    spec:
      containers:
        - name: agent
          resources: { limits: { cpu: "8" } }`, []string{"spec.template.spec.containers[0].resources"}},
		{"variant-15", 15, `spec:
  template:
    spec:
      containers:
        - name: agent
          $patch: replace`, []string{"$patch"}},
		{"variant-16", 16, `spec:
  template:
    spec:
      initContainers:
        - name: log-shipper
          restartPolicy: Always
          image: registry.corp.example/fluent-bit@` + testSidecarDigest + `
          securityContext: { runAsNonRoot: true, allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } }
          volumeMounts:
            - { name: cred, mountPath: /creds }`, []string{"volumeMounts[].name"}},
		{"variant-17", 17, `spec:
  template:
    spec:
      initContainers:
        - name: log-shipper
          restartPolicy: Always
          image: fluent-bit:3.1`, []string{"initContainers[].image"}},
		{"variant-18", 18, `spec:
  template:
    spec:
      initContainers:
        - name: prepare
          command: ["/bin/sh"]`, []string{"spec.template.spec.initContainers[0]"}},
		{"variant-19", 19, `spec:
  template:
    spec:
      containers:
        - name: agent
          image: registry.corp.example/evil@` + testSidecarDigest, []string{"spec.template.spec.containers[0].image"}},
		{"variant-20", 20, `spec:
  template:
    spec:
      initContainers:
        - name: log-shipper
          restartPolicy: Always
          image: registry.corp.example/fluent-bit@` + testSidecarDigest + `
          securityContext: { runAsNonRoot: true, allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } }
          volumeMounts:
            - { name: home, mountPath: /home/agent }`, []string{"volumeMounts[].name"}},
		{"variant-21", 21, `spec:
  template:
    spec:
      ephemeralContainers:
        - name: debug
          image: busybox:1.36`, []string{"spec.template.spec.ephemeralContainers"}},
		{"variant-22", 22, `spec:
  template:
    spec:
      initContainers:
        - name: log-shipper
          restartPolicy: Always
          image: registry.corp.example/fluent-bit@` + testSidecarDigest + `
          securityContext: { runAsUser: 0, allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } }`, []string{"securityContext"}},
		{"variant-23", 23, `spec:
  suspend: true`, []string{"spec.suspend"}},
		{"variant-24", 24, `spec:
  template:
    spec:
      contianers:
        - name: agent`, []string{"contianers"}},
		{"variant-25", 25, `spec:
  template:
    spec:
      initContainers:
        - name: wrapper-setup
          restartPolicy: OnFailure
          image: registry.corp.example/bootstrap@` + testPrepareInitDigest, []string{"spec.template.spec.initContainers"}},
		{"variant-26", 26, `spec:
  template:
    spec:
      initContainers:
        - name: wrapper-setup
          image: registry.corp.example/bootstrap@` + testPrepareInitDigest + `
          securityContext: { runAsUser: 0, runAsNonRoot: false, allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] }, privileged: true }`, []string{"securityContext"}},
		{"variant-27", 27, `spec:
  template:
    spec:
      initContainers:
        - name: wrapper-setup
          image: registry.corp.example/bootstrap@` + testPrepareInitDigest + `
          securityContext: { runAsUser: 0, allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } }`, []string{"securityContext"}},
		{"variant-28", 28, `spec:
  template:
    spec:
      initContainers:
        - name: wrapper-setup
          image: registry.corp.example/bootstrap@` + testPrepareInitDigest + `
          securityContext: { runAsUser: 0, runAsNonRoot: false, allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } }
          volumeMounts:
            - { name: cred, mountPath: /creds }`, []string{"volumeMounts[].name"}},
		{"variant-29", 29, `spec:
  template:
    spec:
      initContainers:
        - name: wrapper-setup
          image: busybox:stable`, []string{"initContainers[].image"}},
		{"variant-30", 30, `spec:
  template:
    spec:
      initContainers:
        - name: log-shipper
          restartPolicy: Always
          image: registry.corp.example/fluent-bit@` + testSidecarDigest + `
          securityContext: { runAsNonRoot: true, allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"], add: ["SYS_ADMIN"] } }`, []string{"securityContext.capabilities.add"}},
		{"variant-31", 31, `spec:
  template:
    spec:
      initContainers:
        - name: wrapper-setup
          image: registry.corp.example/bootstrap@` + testPrepareInitDigest + `
          securityContext: { runAsUser: 0, runAsNonRoot: false, allowPrivilegeEscalation: false }`, []string{"securityContext.capabilities"}},
		{"variant-32", 32, `spec:
  template:
    metadata:
      annotations:
        foreman.tsic.top/evil: "x"`, []string{"spec.template.metadata.annotations[foreman.tsic.top/evil]"}},
		{"variant-33", 33, `spec:
  template:
    spec:
      volumes:
        - name: leak
          projected:
            sources:
              - secret: { name: foreman-secret }`, []string{"projected.sources[0].secret.name"}},
		{"variant-34", 34, `spec:
  template:
    spec:
      containers:
        - name: agent
          env:
            - name: MTOKEN
              valueFrom: { secretKeyRef: { name: foreman-secret, key: MULTICA_TOKEN } }`, []string{"env[MTOKEN].valueFrom.secretKeyRef.name"}},
		{"variant-35", 35, `spec:
  template:
    spec:
      containers:
        - name: agent
          envFrom:
            - secretRef: { name: foreman-secret }`, []string{"envFrom[0].secretRef.name"}},
		{"variant-36", 36, `spec:
  template:
    spec:
      initContainers:
        - name: prepare
          restartPolicy: Always`, []string{"spec.template.spec.initContainers[0].restartPolicy"}},
		{"variant-37", 37, `spec:
  template:
    spec:
      initContainers:
        - name: prepare
          image: registry.corp.example/evil@` + testSidecarDigest, []string{"spec.template.spec.initContainers[0].image"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, logs := overlayConfig(t, tc.body)
			builder, err := NewBuilder(cfg)
			if err == nil {
				job, secret, buildErr := builder.Build(testEntry())
				t.Fatalf("overlay #%d passed the startup gate (job=%v secret=%v buildErr=%v)", tc.index, job != nil, secret != nil, buildErr)
			}
			var invalid *ValidationError
			if !errors.As(err, &invalid) {
				t.Fatalf("overlay #%d must be rejected as a ValidationError, got %v", tc.index, err)
			}
			message := err.Error()
			for _, want := range tc.want {
				if !strings.Contains(message, want) {
					t.Errorf("overlay #%d log must contain %q, got:\n%s", tc.index, want, message)
				}
			}
			if !strings.Contains(logs.String(), "refusing to start") {
				t.Errorf("overlay #%d must log the startup refusal, got %s", tc.index, logs.String())
			}
		})
	}
}

// TestBuildTimeInvariantFallback drives the build-time hook: an overlay that
// bypassed the startup gate must fail Build with *ValidationError and produce
// no objects (TC-tech-job-03 构建期兜底).
func TestBuildTimeInvariantFallback(t *testing.T) {
	cfg := testConfig()
	cfg.Issuer = &fakeIssuer{token: "fmj_test.sig"}
	cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	overlay := parseOverlay(t, `spec:
  template:
    spec:
      initContainers:
        - name: log-shipper
          restartPolicy: Always
          image: registry.corp.example/fluent-bit@`+testSidecarDigest+`
          securityContext: { runAsNonRoot: true, allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } }
          volumeMounts:
            - { name: cred, mountPath: /creds }
`)

	job, secret, err := NewBuilderWithOverlay(cfg, overlay).Build(testEntry())
	if err == nil {
		t.Fatal("Build must reject an overlay that breaks the invariants")
	}
	if job != nil || secret != nil {
		t.Fatalf("Build must create no object on a rejected template (job=%v secret=%v)", job != nil, secret != nil)
	}
	var invalid *ValidationError
	if !errors.As(err, &invalid) {
		t.Fatalf("want *ValidationError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "volumeMounts[].name") {
		t.Errorf("fallback violation must name the broken clause, got %v", err)
	}
}

// TestOverlayMergeKeyFailureFailsClosed: an appended entry without a name
// cannot be merged; the refusal must be loud (ErrNoMergeKey), never a silent
// drop of the entry.
func TestOverlayMergeKeyFailureFailsClosed(t *testing.T) {
	cfg, _ := overlayConfig(t, `spec:
  template:
    spec:
      initContainers:
        - image: registry.corp.example/bootstrap@`+testPrepareInitDigest+`
`)
	builder, err := NewBuilder(cfg)
	if err == nil {
		_, _, err = builder.Build(testEntry())
	}
	if err == nil {
		t.Fatal("a nameless initContainers entry must be rejected")
	}
	if !strings.Contains(err.Error(), "name") && !strings.Contains(err.Error(), "merge key") {
		t.Errorf("refusal must mention the missing merge key, got %v", err)
	}
}

func containerNames(containers []corev1.Container) []string {
	names := make([]string, 0, len(containers))
	for _, ct := range containers {
		names = append(names, ct.Name)
	}
	return names
}

func containerByName(t *testing.T, containers []corev1.Container, name string) corev1.Container {
	t.Helper()
	for _, ct := range containers {
		if ct.Name == name {
			return ct
		}
	}
	t.Fatalf("container %q not found in %v", name, containerNames(containers))
	return corev1.Container{}
}

func mountNames(mounts []corev1.VolumeMount) []string {
	names := make([]string, 0, len(mounts))
	for _, mount := range mounts {
		names = append(names, mount.Name)
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

func capabilityStrings(caps []corev1.Capability) []string {
	out := make([]string, 0, len(caps))
	for _, cap := range caps {
		out = append(out, string(cap))
	}
	return out
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

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
