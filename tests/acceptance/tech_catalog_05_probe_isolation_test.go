// TC-tech-catalog-05 / AC-32: the catalog-probe Job's credential face and
// isolation face. The probe holds only its own Job Token, in its own Secret,
// under its own daemon profile; it never enters the task mapping table, never
// appears in /foreman/tasks and never consumes the in-flight budget. The two
// literal substitutions are judged where §8.4 pins them — the mount-path face
// and the volume-reference face (03-acceptance.md「模型目录」; contracts §6.3 /
// §8.4).
package acceptance

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
)

func TestCatalog05ProbeCredentialAndIsolationFaces(t *testing.T) {
	const requestID = "req-5"
	const probeName = "fm-catalog-" + requestID

	h := newHarness(t, true)
	h.useRealBuilder(t)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	// One task in flight is the reference the isolation faces are read against:
	// the probe must add neither a mapping row nor an in-flight count beside it.
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if got := h.reg.Inflight(); got != 1 {
		t.Fatalf("Inflight before the probe = %d, want the single claimed task", got)
	}
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	if _, err := h.sched.StartProbe(t.Context(), requestID, "rid-omp", registry.ProbeCommandDriven, h.schedCfg.ModelCatalogTimeout); err != nil {
		t.Fatalf("StartProbe: %v", err)
	}
	job := h.createdJob(t, probeName)
	cred := h.createdSecret(t, probeName+"-cred")
	podMeta := job.Spec.Template.ObjectMeta
	pod := job.Spec.Template.Spec

	// Credential face: the Secret is the probe's own carrier — one
	// config.json key holding a probe-role Job Token with no task face.
	if cred.Labels[registry.LabelRole] != registry.RoleCatalogProbe {
		t.Errorf("probe Secret labels = %v, want the catalog-probe role", cred.Labels)
	}
	for _, key := range []string{registry.LabelTaskID, registry.LabelAgentID, registry.LabelWorkspaceID} {
		if _, has := cred.Labels[key]; has {
			t.Errorf("probe Secret labels = %v, want no %s", cred.Labels, key)
		}
	}
	token, keys := probeCredentialToken(t, cred)
	if len(keys) != 1 || keys[0] != "config.json" {
		t.Errorf("probe Secret keys = %v, want only config.json", keys)
	}
	claims, err := h.issuer.Verify(token, auth.RequestScope{})
	if err != nil {
		t.Fatalf("verify the probe's Job Token: %v", err)
	}
	if claims.Role != auth.RoleCatalogProbe || claims.TaskID != "" {
		t.Errorf("probe token claims = role %q / task %q, want the catalog-probe role and no task", claims.Role, claims.TaskID)
	}
	// The env face carries no credential: no server-side token, no Secret
	// reference, and no token literal anywhere in a value.
	for _, container := range probeContainers(job) {
		for _, entry := range container.Env {
			if entry.Name == auth.EnvServerToken {
				t.Errorf("probe container %s carries the server credential %s", container.Name, entry.Name)
			}
			if entry.ValueFrom != nil && entry.ValueFrom.SecretKeyRef != nil {
				t.Errorf("probe container %s reads %s from a Secret", container.Name, entry.Name)
			}
			for _, marker := range []string{"fmj_", "mdt_", "mul_"} {
				if strings.Contains(entry.Value, marker) {
					t.Errorf("probe env %s carries a token (%s)", entry.Name, marker)
				}
			}
		}
	}

	// The pod carries the probe label block only: no task labels and no task
	// annotations.
	for _, labels := range []map[string]string{job.Labels, podMeta.Labels} {
		for _, key := range []string{registry.LabelTaskID, registry.LabelAgentID, registry.LabelWorkspaceID} {
			if _, has := labels[key]; has {
				t.Errorf("probe labels = %v, want no %s", labels, key)
			}
		}
	}
	if got := podMeta.Labels[registry.LabelRole]; got != registry.RoleCatalogProbe {
		t.Errorf("pod role label = %q, want %q", got, registry.RoleCatalogProbe)
	}
	if got := podMeta.Labels["foreman.tsic.top/daemon-id"]; got != probeName {
		t.Errorf("pod daemon-id label = %q, want the probe identity %q", got, probeName)
	}
	for key := range podMeta.Annotations {
		if strings.HasPrefix(key, "foreman.tsic.top/") {
			t.Errorf("probe pod annotation %q, want no task annotation", key)
		}
	}

	// Isolation: the probe session table owns the probe; the task mapping
	// table, the task projection and the in-flight budget do not.
	if _, ok := h.probes.ProbeByJob(probeName); !ok {
		t.Error("the probe session is missing from the probe table")
	}
	if _, ok := h.reg.ByDaemon(probeName); ok {
		t.Error("the probe entered the task mapping table")
	}
	if _, ok := h.reg.Get(requestID); ok {
		t.Error("the probe entered the task index")
	}
	if got := h.reg.Inflight(); got != 1 {
		t.Errorf("Inflight with the probe up = %d, want the single task: a probe never occupies the budget", got)
	}
	entries := projectedList(t, ops.URL)
	if len(entries) != 1 || entries[0]["task_id"] != "task-1" {
		t.Errorf("task projection = %v, want only task-1", entries)
	}
	for _, entry := range entries {
		if entry["job_name"] == probeName {
			t.Errorf("the task projection carries the probe: %v", entry)
		}
	}
	if got := opsInflight(t, ops.URL); got != 1 {
		t.Errorf("/foreman/healthz inflight = %d, want the single task", got)
	}

	// The profile literal's judgement point is the rendered mount-path face
	// (§8.4: not the command and args).
	agent := pod.Containers[0]
	if got := mountPathOf(t, agent.VolumeMounts, "cred"); got != "/home/agent/.multica/profiles/foreman-catalog/config.json" {
		t.Errorf("cred mount = %q, want the probe profile's config path", got)
	}
	for _, container := range probeContainers(job) {
		for _, mount := range container.VolumeMounts {
			if strings.Contains(mount.MountPath, "/profiles/foreman-job") {
				t.Errorf("container %s mount %q still carries the task profile literal", container.Name, mount.MountPath)
			}
		}
	}
	// The credential Secret literal's judgement point is the volume-reference
	// face (§8.4).
	credSrc := volumeByName(t, pod.Volumes, "cred-src")
	if credSrc.Secret == nil || credSrc.Secret.SecretName != probeName+"-cred" {
		t.Errorf("cred-src = %+v, want the probe's Secret", credSrc.VolumeSource)
	}
	for _, volume := range pod.Volumes {
		if volume.Secret == nil {
			continue
		}
		if volume.Secret.SecretName != probeName+"-cred" {
			t.Errorf("volume %s references Secret %q, want the probe's own", volume.Name, volume.Secret.SecretName)
		}
	}
}

// probeCredentialToken reads the token the probe Secret carries and the key set
// it was stored under (the builder writes StringData; a read-back object may
// carry either face).
func probeCredentialToken(t *testing.T, secret *corev1.Secret) (string, []string) {
	t.Helper()
	keys := make([]string, 0, len(secret.Data)+len(secret.StringData))
	for key := range secret.Data {
		keys = append(keys, key)
	}
	for key := range secret.StringData {
		if _, dup := secret.Data[key]; !dup {
			keys = append(keys, key)
		}
	}
	raw := secret.Data["config.json"]
	if len(raw) == 0 {
		raw = []byte(secret.StringData["config.json"])
	}
	var payload struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode the probe credential Secret: %v", err)
	}
	if payload.Token == "" {
		t.Fatal("the probe credential Secret carries no token")
	}
	return payload.Token, keys
}

// opsInflight reads the operator-visible in-flight count.
func opsInflight(t *testing.T, base string) int {
	t.Helper()
	resp, err := http.Get(base + "/foreman/healthz")
	if err != nil {
		t.Fatalf("GET /foreman/healthz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var health struct {
		Inflight int `json:"inflight"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("decode /foreman/healthz: %v", err)
	}
	return health.Inflight
}
