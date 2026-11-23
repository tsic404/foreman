package jobbuilder

import (
	"encoding/json"
	"math"
	"path"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/attempt"
)

// fixedEntry is a claim whose environment binder froze every placement fact.
func fixedEntry(t *testing.T) TaskEntry {
	t.Helper()
	entry := testEntry()
	return fixedEntryWith(t, entry, "build-a.example.test:7443", "ref/build-a")
}

// fixedEntryWith freezes the carrier endpoint under test.
func fixedEntryWith(t *testing.T, entry TaskEntry, endpoint, tlsRef string) TaskEntry {
	t.Helper()
	entry.SourceID = "src-a"
	entry.AgentID = "agent-1"
	entry.EnvironmentID = "build-a"
	entry.EnvironmentSlotID = "task-v2-1"
	binding := attempt.Binding{
		AttemptID: "task-v2-1", TaskID: entry.TaskID, SourceID: entry.SourceID,
		WorkspaceID: entry.WorkspaceID, AgentID: entry.AgentID,
		DaemonID:     DaemonIDForInScope(entry.TaskID, entry.SourceID, entry.WorkspaceID),
		JobRuntimeID: entry.JobRuntimeID, EnvironmentID: entry.EnvironmentID,
		Endpoint: endpoint, TLSRef: tlsRef,
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

// TestFixedRenderProjectsTheCarrierDevice pins the one device the carrier
// client cannot open a sandbox without. The reference render is the default
// Job-side adapter (V2-C29), so it has to carry the posture the carrier
// requires instead of deferring it to a deployer template the fixed path
// never reads.
func TestFixedRenderProjectsTheCarrierDevice(t *testing.T) {
	job, _ := mustBuild(t, testConfig(), fixedEntry(t))
	spec := job.Spec.Template.Spec

	fuse := volumeByName(t, spec.Volumes, "fuse")
	if fuse.HostPath == nil || fuse.HostPath.Path != "/dev/fuse" ||
		fuse.HostPath.Type == nil || *fuse.HostPath.Type != corev1.HostPathCharDev {
		t.Fatalf("fuse volume = %+v, want the /dev/fuse character device", fuse.VolumeSource)
	}
	if !hasMount(spec.Containers[0].VolumeMounts, "fuse", "/dev/fuse") {
		t.Fatalf("agent mounts = %+v, want /dev/fuse", spec.Containers[0].VolumeMounts)
	}
}

// TestFixedRenderProjectsOnlyTheSelectedEnvironmentTLS pins V2-C08's TLS
// material reference for the authenticated network channel: exactly the
// selected environment's material is mounted read-only under the reference
// the adapter resolves, and the adapter is told where it is.
func TestFixedRenderProjectsOnlyTheSelectedEnvironmentTLS(t *testing.T) {
	job, _ := mustBuild(t, testConfig(), fixedEntry(t))
	spec := job.Spec.Template.Spec

	tlsVolume := volumeByName(t, spec.Volumes, "env-tls")
	if tlsVolume.Secret == nil || tlsVolume.Secret.SecretName != "foreman-env-tls-build-a" {
		t.Fatalf("env-tls volume = %+v, want the selected environment's projected material", tlsVolume.VolumeSource)
	}
	if !hasMount(spec.Containers[0].VolumeMounts, "env-tls", "/run/foreman/tls/ref/build-a") {
		t.Fatalf("agent mounts = %+v, want the frozen reference's directory", spec.Containers[0].VolumeMounts)
	}
	if got := envByName(t, spec.Containers[0].Env, "FOREMAN_JOB_TLS_DIR").Value; got != "/run/foreman/tls" {
		t.Errorf("FOREMAN_JOB_TLS_DIR = %q, want the projected root", got)
	}
}

// TestFixedRenderProjectsOnlyTheSocket pins the local channel: a unix
// endpoint is node-local, so exactly that socket is projected at its own path
// and nothing else of the node tree is exposed.
func TestFixedRenderProjectsOnlyTheSocket(t *testing.T) {
	entry := fixedEntryWith(t, testEntry(), "unix:///var/lib/foreman/workspaces/.rsbxd/rsbxd.sock", "ref/build-a")
	job, _ := mustBuild(t, testConfig(), entry)
	spec := job.Spec.Template.Spec

	socket := volumeByName(t, spec.Volumes, "rsbxd-socket")
	if socket.HostPath == nil || socket.HostPath.Path != "/var/lib/foreman/workspaces/.rsbxd/rsbxd.sock" ||
		socket.HostPath.Type == nil || *socket.HostPath.Type != corev1.HostPathSocket {
		t.Fatalf("rsbxd-socket volume = %+v, want the socket file itself", socket.VolumeSource)
	}
	if !hasMount(spec.Containers[0].VolumeMounts, "rsbxd-socket", "/var/lib/foreman/workspaces/.rsbxd/rsbxd.sock") {
		t.Fatalf("agent mounts = %+v, want the socket path", spec.Containers[0].VolumeMounts)
	}
	for _, volume := range spec.Volumes {
		if volume.Name == "env-tls" {
			t.Fatal("a unix endpoint projected network TLS material")
		}
	}
	for _, env := range spec.Containers[0].Env {
		if env.Name == "FOREMAN_JOB_TLS_DIR" {
			t.Fatal("a unix endpoint made the adapter require TLS material")
		}
	}
}

// TestFixedRenderNeverProjectsTheSocketsNeighbour pins V2-C30's host boundary
// against the shape that motivated it: a socket whose directory also holds the
// container runtime's socket. Projecting the directory — even read-only — would
// let the privileged client connect to the runtime socket, so only the named
// socket may enter the Pod.
func TestFixedRenderNeverProjectsTheSocketsNeighbour(t *testing.T) {
	entry := fixedEntryWith(t, testEntry(), "unix:///run/rsbxd.sock", "ref/build-a")
	job, _ := mustBuild(t, testConfig(), entry)
	spec := job.Spec.Template.Spec

	socket := volumeByName(t, spec.Volumes, "rsbxd-socket")
	if socket.HostPath == nil || socket.HostPath.Path != "/run/rsbxd.sock" {
		t.Fatalf("rsbxd-socket volume = %+v, want the socket itself, not /run", socket.VolumeSource)
	}
	for _, volume := range spec.Volumes {
		if volume.HostPath == nil {
			continue
		}
		// /run holds containerd.sock on a typical node. Anything that mounts
		// the directory instead of the socket exposes that neighbour.
		if volume.HostPath.Path == "/run" {
			t.Fatalf("volume %q projects /run, which carries the container runtime's socket", volume.Name)
		}
		if volume.HostPath.Type != nil && *volume.HostPath.Type == corev1.HostPathDirectory {
			t.Fatalf("volume %q projects a host directory (%s): a directory mount carries every socket in it",
				volume.Name, volume.HostPath.Path)
		}
	}
	for _, mount := range spec.Containers[0].VolumeMounts {
		if mount.MountPath == "/run" {
			t.Fatalf("agent mounts /run (%+v): the runtime socket shares that directory", mount)
		}
	}
}

// TestFixedRenderRefusesARuntimeSocketEndpoint pins the other half: an
// endpoint that names a container runtime socket is refused outright rather
// than projected into the privileged container.
func TestFixedRenderRefusesARuntimeSocketEndpoint(t *testing.T) {
	for _, endpoint := range []string{
		"unix:///run/containerd/containerd.sock",
		"unix:///var/run/docker.sock",
	} {
		entry := fixedEntryWith(t, testEntry(), endpoint, "ref/build-a")
		if _, _, _, err := newTestBuilder(t, testConfigWithIssuer()).Build(entry); err == nil {
			t.Errorf("the fixed render accepted the runtime socket endpoint %q", endpoint)
		}
	}
}

// TestFixedRenderRefusesAnEndpointTheCarrierCannotOpen pins the fail-closed
// side: an unsupported endpoint never becomes a Pod that can only fail at the
// session step.
func TestFixedRenderRefusesAnEndpointTheCarrierCannotOpen(t *testing.T) {
	entry := fixedEntryWith(t, testEntry(), "tcp://build-a.example.test:7443", "ref/build-a")
	if _, _, _, err := newTestBuilder(t, testConfigWithIssuer()).Build(entry); err == nil {
		t.Fatal("the fixed render accepted an endpoint no carrier channel serves")
	}
}

func hasMount(mounts []corev1.VolumeMount, name, path string) bool {
	for _, mount := range mounts {
		if mount.Name == name && mount.MountPath == path {
			return true
		}
	}
	return false
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
		*container.SecurityContext.RunAsUser != 0 {
		t.Fatalf("container runAsUser = %+v, want 0 so the sandbox client can mount FUSE", container.SecurityContext)
	}
	if container.SecurityContext.Privileged == nil || !*container.SecurityContext.Privileged {
		t.Fatalf("container securityContext = %+v, want the carrier client's privileged posture", container.SecurityContext)
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

// TestFixedAttemptStateBudgetFitsALiveSession is the guard for the eviction a
// live session produced. The attempt-state volume is not record-only: the
// carrier keeps its sandbox and a verified snapshot of every program it runs
// locally under the same root, so a limit below the attempt's own download
// budget evicts the Pod mid-session. The 64Mi this replaces was smaller than a
// single Agent artifact.
//
// The expectations are absolute, not derived from the render's own constants:
// a test that recomputes the need from the same headroom and floor it is meant
// to guard stays green when either parameter is wrong, which is exactly the
// regression that slipped through here. Default budget (512Mi) plus the 512Mi
// headroom is 1Gi, under the 2Gi floor, so the volume is 2Gi; a 3Gi budget plus
// the headroom is 3584Mi, above the floor, so the volume is 3584Mi.
//
// Each case also pins the download budget the Pod actually receives. The
// volume alone does not catch a render that hands the container the wrong
// budget: a 3Gi binding whose env fell back to 512Mi would still size the
// volume from 3Gi and stay green here, while the preparation helper capped its
// downloads at 512Mi. The two figures are the byte counts those budgets mean,
// written out rather than read back from the render.
func TestFixedAttemptStateBudgetFitsALiveSession(t *testing.T) {
	cases := []struct {
		name       string
		budget     int64
		want       string
		wantBudget string
	}{
		{"default budget", 0, "2Gi", "536870912"},
		{"3Gi budget", 3 << 30, "3584Mi", "3221225472"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry := fixedEntryWithBudget(t, tc.budget)
			job, _ := mustBuild(t, testConfig(), entry)
			spec := job.Spec.Template.Spec

			limit := fixedAttemptStateLimit(t, spec.Volumes)
			want := resource.MustParse(tc.want)
			if limit.Cmp(want) != 0 {
				t.Errorf("attempt-state limit = %s, want exactly %s", limit.String(), tc.want)
			}
			if got := envByName(t, spec.Containers[0].Env, "FOREMAN_JOB_CLI_BUDGET_BYTES").Value; got != tc.wantBudget {
				t.Errorf("FOREMAN_JOB_CLI_BUDGET_BYTES = %q, want exactly %q", got, tc.wantBudget)
			}
			// The bound has to stay a bound: an emptyDir without a sizeLimit lets
			// one Job fill the node's disk, which fails the node, not the Job.
			for _, volume := range spec.Volumes {
				if volume.EmptyDir != nil && volume.EmptyDir.SizeLimit == nil {
					t.Errorf("volume %q is an unbounded emptyDir", volume.Name)
				}
			}
		})
	}
}

// TestFixedAttemptStateBudgetRefusesUnrepresentableBudget guards the other
// half of the sizing formula: the volume is the download budget plus the
// headroom, and an int64 sum past the range wraps negative, which the floor
// then clamps back to 2Gi. That would make a very large budget shrink the
// volume below what the default budget gets. The render must refuse such a
// budget instead of wrapping it.
//
// The accepted case only asserts that Build succeeds for the largest
// representable budget; it does not read the rendered volume back. The
// MaxInt64-byte volume at that point is a derivation from the formula, not a
// value this test checks — the volume assertion lives in the two-budget guard
// above.
func TestFixedAttemptStateBudgetRefusesUnrepresentableBudget(t *testing.T) {
	// 512Mi is the documented headroom; the largest budget the volume can
	// carry is MaxInt64 minus that headroom.
	const headroom = 512 << 20
	cases := []struct {
		name   string
		budget int64
		wantOK bool
	}{
		{"largest representable budget", math.MaxInt64 - headroom, true},
		{"first unrepresentable budget", math.MaxInt64 - headroom + 1, false},
		{"maximum int64 budget", math.MaxInt64, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry := fixedEntryWithBudget(t, tc.budget)
			_, _, _, err := newTestBuilder(t, testConfigWithIssuer()).Build(entry)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("budget %d: Build: %v, want a representable volume", tc.budget, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("budget %d: Build accepted an unrepresentable budget; the wrapped sum would clamp the volume to the floor", tc.budget)
			}
		})
	}
}

// fixedEntryWithBudget freezes a fixed binding whose download budget is
// budget. A zero budget leaves the binding unset so the contract default
// applies.
func fixedEntryWithBudget(t *testing.T, budget int64) TaskEntry {
	t.Helper()
	entry := fixedEntry(t)
	if budget <= 0 {
		return entry
	}
	var binding attempt.Binding
	if err := json.Unmarshal(entry.FixedAttempt, &binding); err != nil {
		t.Fatalf("decode the fixed binding: %v", err)
	}
	binding.CLIBudgetBytes = budget
	raw, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("encode the fixed binding: %v", err)
	}
	entry.FixedAttempt = raw
	return entry
}

func fixedAttemptStateLimit(t *testing.T, volumes []corev1.Volume) resource.Quantity {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name != "attempt-state" {
			continue
		}
		if volume.EmptyDir == nil || volume.EmptyDir.SizeLimit == nil {
			t.Fatalf("attempt-state volume = %+v, want a bounded emptyDir", volume.VolumeSource)
		}
		return *volume.EmptyDir.SizeLimit
	}
	t.Fatal("the fixed render names no attempt-state volume")
	return resource.Quantity{}
}

// TestFixedRuntimeMountMatchesScriptAndEntrypoint pins the handoff the init and
// agent containers make through the "runtime" emptyDir: the init script must
// write the supervisor where the agent container can see it. A literal
// /runtime target, or an agent without the mount, fails every task at init.
func TestFixedRuntimeMountMatchesScriptAndEntrypoint(t *testing.T) {
	job, _ := mustBuild(t, testConfig(), fixedEntry(t))
	spec := job.Spec.Template.Spec

	if len(spec.InitContainers) != 1 {
		t.Fatalf("init containers = %d, want the one job-runtime carrier", len(spec.InitContainers))
	}
	if len(spec.Containers) != 1 {
		t.Fatalf("containers = %d, want the one agent container", len(spec.Containers))
	}
	carrier := spec.InitContainers[0]
	agent := spec.Containers[0]

	var carrierMount, agentMount *corev1.VolumeMount
	for i := range carrier.VolumeMounts {
		if carrier.VolumeMounts[i].Name == "runtime" {
			carrierMount = &carrier.VolumeMounts[i]
		}
	}
	if carrierMount == nil {
		t.Fatalf("init container %q does not mount the runtime volume: %+v", carrier.Name, carrier.VolumeMounts)
	}
	for i := range agent.VolumeMounts {
		if agent.VolumeMounts[i].Name == "runtime" {
			agentMount = &agent.VolumeMounts[i]
		}
	}
	if agentMount == nil {
		t.Fatalf("agent container does not mount the runtime volume: %+v", agent.VolumeMounts)
	}
	if carrierMount.MountPath != agentMount.MountPath {
		t.Errorf("runtime mounted at %q for init ids %q for the agent, want one shared path",
			carrierMount.MountPath, agentMount.MountPath)
	}
	if agentMount.MountPath != fixedRuntimeDir {
		t.Errorf("agent runtime mount = %q, want %q", agentMount.MountPath, fixedRuntimeDir)
	}

	var runtimeVolume bool
	for _, volume := range spec.Volumes {
		if volume.Name == "runtime" {
			runtimeVolume = volume.EmptyDir != nil
		}
	}
	if !runtimeVolume {
		t.Fatalf("volumes = %+v, want the runtime emptyDir both containers share", spec.Volumes)
	}

	script := strings.Join(carrier.Args, "\n")
	if !strings.Contains(script, `"`+fixedRuntimeDir+`/$name"`) {
		t.Errorf("init script does not copy to %q: %s", fixedRuntimeDir, script)
	}
	if !strings.Contains(script, `"`+fixedRuntimeDir+`/foreman-job"`) {
		t.Errorf("init script does not require %q: %s", fixedRuntimeDir+"/foreman-job", script)
	}
	target := carrierMount.MountPath + "/foreman-job"
	if got := strings.Join(agent.Command, " "); got != target {
		t.Errorf("agent command = %q, want the init script's own target %q", got, target)
	}

	// Every adapter the supervisor starts must resolve inside the volume the
	// init container fills, or the Job fails with a missing helper instead of
	// running the attempt.
	copied := scriptCopyList(t, script)
	for _, name := range []string{
		"FOREMAN_JOB_CLI_HELPER", "FOREMAN_JOB_RSBX_HELPER", "FOREMAN_JOB_REGISTER_HELPER",
		"FOREMAN_JOB_RUNNER_HELPER", "FOREMAN_JOB_SLOT_HELPER",
	} {
		value, ok := envValue(agent.Env, name)
		if !ok {
			t.Errorf("agent env lacks %s; the supervisor would fall back to its own default path", name)
			continue
		}
		if !strings.HasPrefix(value, carrierMount.MountPath+"/") {
			t.Errorf("%s = %q, want a path inside the shared runtime volume %q", name, value, carrierMount.MountPath)
		}
		if !copied[path.Base(value)] {
			t.Errorf("the init script does not copy %s, which %s names: %s", path.Base(value), name, script)
		}
	}
}

// scriptCopyList is the set of binary names the init script's copy loop walks.
func scriptCopyList(t *testing.T, script string) map[string]bool {
	t.Helper()
	const marker = "for name in "
	start := strings.Index(script, marker)
	if start < 0 {
		t.Fatalf("the init script has no copy loop: %s", script)
	}
	rest := script[start+len(marker):]
	end := strings.Index(rest, ";")
	if end < 0 {
		t.Fatalf("the init script's copy loop is unterminated: %s", script)
	}
	names := map[string]bool{}
	for _, name := range strings.Fields(rest[:end]) {
		names[name] = true
	}
	return names
}

func envValue(env []corev1.EnvVar, name string) (string, bool) {
	for _, item := range env {
		if item.Name == name {
			return item.Value, true
		}
	}
	return "", false
}
