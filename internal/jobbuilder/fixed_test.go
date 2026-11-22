package jobbuilder

import (
	"encoding/json"
	"path"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/attempt"
)

// fixedEntry is a claim whose environment binder froze every placement fact.
func fixedEntry(t *testing.T) TaskEntry {
	t.Helper()
	entry := testEntry()
	entry.SourceID = "src-a"
	entry.AgentID = "agent-1"
	entry.EnvironmentID = "build-a"
	entry.EnvironmentSlotID = "task-v2-1"
	binding := attempt.Binding{
		AttemptID: "task-v2-1", TaskID: entry.TaskID, SourceID: entry.SourceID,
		WorkspaceID: entry.WorkspaceID, AgentID: entry.AgentID,
		DaemonID:     DaemonIDForInScope(entry.TaskID, entry.SourceID, entry.WorkspaceID),
		JobRuntimeID: entry.JobRuntimeID, EnvironmentID: entry.EnvironmentID,
		Endpoint: "build-a.example.test:7443", TLSRef: "ref/build-a",
		StateRoot: "/srv/rsbx/build-a", EnvironmentGen: "tree-1",
		SlotID: entry.EnvironmentSlotID,
		Image:  "ghcr.io/tsic404/rsbx", ImageDigest: "sha256:" + strings.Repeat("1", 64),
		Platform: "linux/amd64", ProxyURL: "http://foreman.foreman.svc.cluster.local:8080",
		CLIs: []attempt.CLI{
			{Kind: attempt.KindMultica, Version: "0.6.1", Platform: "linux/amd64",
				Digest:      "sha256:" + strings.Repeat("2", 64),
				DownloadURL: "http://foreman.foreman.svc.cluster.local:8080/cli/multica/0.6.1/linux/amd64"},
			{Kind: attempt.KindAgent, Backend: "codex", Version: "0.167.1", Platform: "linux/amd64",
				Digest:      "sha256:" + strings.Repeat("3", 64),
				DownloadURL: "http://foreman.foreman.svc.cluster.local:8080/cli/agent/0.167.1/linux/amd64?backend=codex"},
		},
	}
	raw, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("encode fixed binding: %v", err)
	}
	entry.FixedAttempt = raw
	return entry
}

// TestFixedRenderCarriesOnlyTheAttemptsTwoCLIs pins 04-architecture §6 ①:
// the fixed Job mounts one credential Secret, one Agent CLI/config and no
// deployer template or probe container.
func TestFixedRenderCarriesOnlyTheAttemptsTwoCLIs(t *testing.T) {
	entry := fixedEntry(t)
	job, secret := mustBuild(t, testConfig(), entry)
	spec := job.Spec.Template.Spec

	if len(spec.Containers) != 1 {
		t.Fatalf("fixed Job has %d containers, want exactly the one Agent container", len(spec.Containers))
	}
	if len(spec.InitContainers) != 1 || spec.InitContainers[0].Name != "job-runtime" {
		t.Fatalf("fixed Job init containers = %+v, want the one job-runtime carrier", spec.InitContainers)
	}
	container := spec.Containers[0]
	if container.Name != "agent" {
		t.Errorf("container name = %q, want the fixed agent container", container.Name)
	}
	if got := strings.Join(container.Command, " "); got != "/run/foreman/runtime/foreman-job" {
		t.Errorf("command = %q, want the supervisor copied from the Foreman runtime image", got)
	}
	if strings.Contains(strings.Join(container.Command, " "), "/usr/local/bin/foreman-job") {
		t.Error("the fixed Job still requires the old foreman-job entry inside the carrier")
	}
	if container.Image != "ghcr.io/tsic404/rsbx@sha256:"+strings.Repeat("1", 64) {
		t.Errorf("carrier image = %q, want the frozen rsbx digest", container.Image)
	}
	if container.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("carrier pull policy = %q, want IfNotPresent for a digest-pinned image", container.ImagePullPolicy)
	}
	if got := spec.NodeSelector["kubernetes.io/arch"]; got != "amd64" {
		t.Errorf("node arch selector = %q, want amd64 from the frozen platform", got)
	}
	for _, name := range []string{"probe", "catalog", "omp", "claude"} {
		for _, other := range spec.Containers {
			if other.Name == name {
				t.Errorf("fixed Job still carries a %q container: probing is not a Job responsibility", name)
			}
		}
	}

	raw, ok := secret.StringData[attemptSpecKey]
	if !ok {
		t.Fatalf("credential Secret has no %s: the fixed Job would start without a binding", attemptSpecKey)
	}
	var binding attempt.Binding
	if err := json.Unmarshal([]byte(raw), &binding); err != nil {
		t.Fatalf("decode the frozen binding: %v", err)
	}
	if len(binding.CLIs) != 2 {
		t.Fatalf("binding carries %d CLIs, want exactly two", len(binding.CLIs))
	}
	for _, cli := range binding.CLIs {
		if cli.Kind == attempt.KindAgent && cli.Backend != "codex" {
			t.Errorf("Agent CLI backend = %q, want the selected codex", cli.Backend)
		}
		if cli.Backend == "claude" || cli.Backend == "omp" {
			t.Errorf("binding carries unselected backend %q", cli.Backend)
		}
	}
	if got := job.Annotations[annoEnvironmentID]; got != "build-a" {
		t.Errorf("environment annotation = %q, want build-a", got)
	}
	if got := job.Annotations[annoEnvironmentGen]; got != "tree-1" {
		t.Errorf("generation annotation = %q, want tree-1", got)
	}
	if got := job.Annotations[annoSlotID]; got != "task-v2-1" {
		t.Errorf("slot annotation = %q, want task-v2-1", got)
	}
}

// TestFixedRenderMountsOnlyTheSelectedConfig pins the config projection: one
// read-only Secret for the attempt, named by the claim's attempt id.
func TestFixedRenderMountsOnlyTheSelectedConfig(t *testing.T) {
	entry := fixedEntry(t)
	var binding attempt.Binding
	if err := json.Unmarshal(entry.FixedAttempt, &binding); err != nil {
		t.Fatalf("decode fixed binding: %v", err)
	}
	binding.Config = attempt.Config{
		SecretRef: "ref/agent-config", SecretName: agentconfig.SecretNameForAttempt(binding.AttemptID),
		FileName: "config.toml", Revision: 7, Digest: "sha256:config",
	}
	raw, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("encode fixed binding: %v", err)
	}
	entry.FixedAttempt = raw

	job, _ := mustBuild(t, testConfig(), entry)
	spec := job.Spec.Template.Spec
	var agentConfig bool
	for _, volume := range spec.Volumes {
		if volume.Name != "agent-config" {
			continue
		}
		agentConfig = true
		if volume.Secret == nil || volume.Secret.SecretName != binding.Config.SecretName {
			t.Errorf("agent-config volume = %+v, want only the attempt's projection Secret", volume.Secret)
		}
	}
	if !agentConfig {
		t.Fatal("fixed Job does not mount the selected Agent configuration")
	}
}

// TestFixedRenderRefusesAnotherEnvironment pins the claim's reservation: a
// binding that disagrees with the recorded environment never reaches a Job.
func TestFixedRenderRefusesAnotherEnvironment(t *testing.T) {
	entry := fixedEntry(t)
	entry.EnvironmentID = "build-b"
	_, _, _, err := newTestBuilder(t, testConfigWithIssuer()).Build(entry)
	if err == nil {
		t.Fatal("a fixed binding naming another environment was rendered")
	}
	if !strings.Contains(err.Error(), "environment id") {
		t.Fatalf("error = %v, want the environment mismatch", err)
	}
}

// TestFixedRenderProvidesWritableAttemptState pins the local record's carrier:
// /run/foreman itself is image-owned, so the store lives under a dedicated
// emptyDir that fsGroup 1000 makes writable and an explicit env names it. The
// shared ledger remains the cross-Pod authority for Started/isolated facts.
func TestFixedRenderProvidesWritableAttemptState(t *testing.T) {
	entry := fixedEntry(t)
	job, _ := mustBuild(t, testConfig(), entry)
	spec := job.Spec.Template.Spec

	if spec.SecurityContext == nil || spec.SecurityContext.FSGroup == nil ||
		*spec.SecurityContext.FSGroup != 1000 {
		t.Fatalf("pod fsGroup = %+v, want 1000 so the mounted store is writable", spec.SecurityContext)
	}
	container := spec.Containers[0]
	if container.SecurityContext == nil || container.SecurityContext.RunAsUser == nil ||
		*container.SecurityContext.RunAsUser != 1000 {
		t.Fatalf("container runAsUser = %+v, want 1000", container.SecurityContext)
	}

	var stateVolume, stateMount bool
	for _, volume := range spec.Volumes {
		if volume.Name != "attempt-state" {
			continue
		}
		stateVolume = volume.EmptyDir != nil
		if volume.HostPath != nil {
			t.Fatalf("attempt state is node-local hostPath: %+v", volume.VolumeSource)
		}
	}
	for _, mount := range container.VolumeMounts {
		if mount.Name != "attempt-state" {
			continue
		}
		stateMount = mount.MountPath == fixedStateDir && !mount.ReadOnly
	}
	if !stateVolume || !stateMount {
		t.Fatalf("attempt-state volume=%v mount=%v, want a writable emptyDir at %s", stateVolume, stateMount, fixedStateDir)
	}
	store := envByName(t, container.Env, "FOREMAN_JOB_STORE_FILE")
	if store.Value != fixedStateFile {
		t.Fatalf("FOREMAN_JOB_STORE_FILE = %q, want %q", store.Value, fixedStateFile)
	}
	if path.Dir(store.Value) != fixedStateDir {
		t.Fatalf("store %q is not under the mounted %q", store.Value, fixedStateDir)
	}
}
