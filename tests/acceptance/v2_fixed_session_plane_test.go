// TC-JOB-11: the reference Job-side adapter carries the posture and the
// material the carrier needs to open a session. V2-AC24, V2-C08, V2-C29.
package acceptance

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/jobexec"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/rsbxendpoint"
)

const tcJob11Platform = "linux/amd64"

// TestTCJob11FixedRenderCarriesTheSessionPlane asserts the two halves a fixed
// attempt needs to reach a real rsbxd: the device and privilege the carrier
// client mounts its sandbox with, and the frozen endpoint/TLS material
// projection. It also pins that the management surface and the Job render
// accept the same endpoint forms, which is what makes a registered target
// usable at all.
func TestTCJob11FixedRenderCarriesTheSessionPlane(t *testing.T) {
	for _, tc := range []struct {
		name       string
		endpoint   string
		wantSecret string
		wantMount  string
	}{
		{
			name:       "authenticated network",
			endpoint:   "tls://build-a.example.test:7443",
			wantSecret: "foreman-env-tls-build-a",
			wantMount:  "/run/foreman/tls/ref/build-a",
		},
		{
			// The socket is mounted at its own path: projecting the directory
			// would carry a runtime socket that shares it into the privileged
			// container (V2-C30 second subitem).
			name:      "local socket",
			endpoint:  "unix:///var/lib/foreman/workspaces/.rsbxd/rsbxd.sock",
			wantMount: "/var/lib/foreman/workspaces/.rsbxd/rsbxd.sock",
		},
		{
			// The counterexample: /run also holds containerd.sock on a typical
			// node, so a directory projection would hand the runtime socket to
			// the sandbox client.
			name:      "local socket beside the runtime socket",
			endpoint:  "unix:///run/rsbxd.sock",
			wantMount: "/run/rsbxd.sock",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The management surface and the Job render share one parser: a
			// form one side refuses is a target no attempt can use.
			if _, err := rsbxendpoint.Parse(tc.endpoint); err != nil {
				t.Fatalf("the carrier refused the frozen endpoint %q: %v", tc.endpoint, err)
			}

			job := tcJob11Build(t, tc.endpoint)
			spec := job.Spec.Template.Spec

			fuse := tcJob11Volume(t, spec.Volumes, "fuse")
			if fuse.HostPath == nil || fuse.HostPath.Path != "/dev/fuse" ||
				fuse.HostPath.Type == nil || *fuse.HostPath.Type != corev1.HostPathCharDev {
				t.Fatalf("fuse volume = %+v, want the /dev/fuse device", fuse.VolumeSource)
			}
			agent := spec.Containers[0]
			if agent.SecurityContext == nil || agent.SecurityContext.Privileged == nil ||
				!*agent.SecurityContext.Privileged || agent.SecurityContext.RunAsUser == nil ||
				*agent.SecurityContext.RunAsUser != 0 {
				t.Fatalf("agent securityContext = %+v, want the carrier client's privileged, root posture", agent.SecurityContext)
			}
			if spec.HostNetwork || spec.HostPID || spec.HostIPC {
				t.Error("the fixed Job uses a host namespace")
			}
			for _, volume := range spec.Volumes {
				if volume.HostPath == nil {
					continue
				}
				for _, socket := range []string{"docker.sock", "containerd.sock", "k3s.sock"} {
					if strings.Contains(volume.HostPath.Path, socket) {
						t.Errorf("the fixed Job projects a runtime socket: %s", volume.HostPath.Path)
					}
				}
			}

			projected := tcJob11Volume(t, spec.Volumes, tcJob11ProjectionName(tc.wantSecret))
			if tc.wantSecret != "" && (projected.Secret == nil || projected.Secret.SecretName != tc.wantSecret) {
				t.Fatalf("TLS projection = %+v, want Secret %q", projected.VolumeSource, tc.wantSecret)
			}
			if !tcJob11HasMount(agent.VolumeMounts, tcJob11ProjectionName(tc.wantSecret), tc.wantMount) {
				t.Fatalf("agent mounts = %+v, want %q", agent.VolumeMounts, tc.wantMount)
			}
			if tc.wantSecret == "" && tcJob11MountsADirectoryOf(agent.VolumeMounts, tc.wantMount) {
				t.Fatalf("agent mounts = %+v, want the socket itself, not its directory", agent.VolumeMounts)
			}

			// The run and ops details read this same fact off the rendered
			// spec (ADR-002 consequence 1): a Job that runs privileged as root
			// must be presented as such, never as unprivileged.
			posture := registry.SandboxPostureOf(spec)
			if !posture.Known || !posture.Privileged || !posture.RunAsUserSet || posture.RunAsUser != 0 || posture.RunAsNonRoot {
				t.Fatalf("rendered posture = %+v, want the privileged root container the Job declares", posture)
			}
		})
	}
}

func tcJob11ProjectionName(secret string) string {
	if secret == "" {
		return "rsbxd-socket"
	}
	return "env-tls"
}

func tcJob11Build(t *testing.T, endpoint string) *batchv1.Job {
	t.Helper()
	job, _ := tcJob11Render(t, endpoint)
	return job
}

// tcJob11Render builds the fixed Job for one endpoint and returns the frozen
// binding the Job would run with, so a case can drive the adapter with exactly
// the input the render carries.
func tcJob11Render(t *testing.T, endpoint string) (*batchv1.Job, attempt.Binding) {
	t.Helper()
	builder, err := jobbuilder.NewBuilder(jobbuilder.Config{
		JobNamespace:    "multica-agents",
		JobImage:        "ghcr.io/tsic404/rsbx@sha256:" + strings.Repeat("1", 64),
		JobRuntimeImage: "kos-local/foreman:test",
		CLIBudgetBytes:  attempt.DefaultCLIBudgetBytes,
		JobTokenTTL:     time.Hour,
		TemplatePath:    deployedTemplatePath(t),
		Issuer:          stubIssuer{},
	})
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	entry := tcJob11Entry(t, endpoint)
	job, _, _, err := builder.Build(entry)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	var binding attempt.Binding
	if err := json.Unmarshal(entry.FixedAttempt, &binding); err != nil {
		t.Fatalf("decode the frozen binding: %v", err)
	}
	return job, binding
}

func tcJob11Entry(t *testing.T, endpoint string) jobbuilder.TaskEntry {
	t.Helper()
	entry := jobbuilder.TaskEntry{
		TaskID:            "task-job-11",
		AgentID:           "0a1b2c3d-4e5f-4a6b-9c8d-7e6f5a4b3c2d",
		WorkspaceID:       "c20ff019-2980-4936-8cb0-22d79d694e71",
		JobRuntimeID:      "5d4c3b2a-1e0f-4a9b-8c7d-6e5f4a3b2c1d",
		ClaimedAt:         time.Date(2026, 11, 25, 8, 30, 0, 0, time.UTC),
		Attempt:           1,
		EnvironmentID:     "build-a",
		EnvironmentSlotID: "task-job-11-1",
	}
	binding := attempt.Binding{
		AttemptID: "task-job-11-1", TaskID: entry.TaskID,
		WorkspaceID: entry.WorkspaceID, AgentID: entry.AgentID,
		DaemonID:     jobbuilder.DaemonIDForInScope(entry.TaskID, entry.SourceID, entry.WorkspaceID),
		JobRuntimeID: entry.JobRuntimeID, EnvironmentID: entry.EnvironmentID,
		Endpoint: endpoint, TLSRef: "ref/build-a",
		StateRoot: "/srv/rsbx/build-a", EnvironmentGen: "tree-1",
		EnvironmentIdentity: "identity-1", SlotID: entry.EnvironmentSlotID,
		Image: "ghcr.io/tsic404/rsbx", ImageDigest: "sha256:" + strings.Repeat("1", 64),
		Platform: tcJob11Platform, ProxyURL: "http://foreman.foreman.svc.cluster.local:8080",
		CLIs: []attempt.CLI{
			{Kind: attempt.KindMultica, Version: "0.11.1", Platform: tcJob11Platform,
				Digest:      "sha256:" + strings.Repeat("2", 64),
				DownloadURL: "http://foreman.foreman.svc.cluster.local:8080/cli/multica/0.11.1/linux/amd64"},
			{Kind: attempt.KindAgent, Backend: "omp", Version: "19.0.1", Platform: tcJob11Platform,
				Digest:      "sha256:" + strings.Repeat("3", 64),
				DownloadURL: "http://foreman.foreman.svc.cluster.local:8080/cli/agent/19.0.1/linux/amd64?backend=omp"},
		},
	}
	raw, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("encode the frozen binding: %v", err)
	}
	entry.FixedAttempt = raw
	return entry
}

func tcJob11Volume(t *testing.T, volumes []corev1.Volume, name string) corev1.Volume {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			return volume
		}
	}
	t.Fatalf("volume %q not found", name)
	return corev1.Volume{}
}

// tcJob11MountsADirectoryOf reports whether some mount shadows the socket's
// parent directory, which is how a runtime socket beside it would enter the
// Pod.
func tcJob11MountsADirectoryOf(mounts []corev1.VolumeMount, socketPath string) bool {
	dir := path.Dir(socketPath)
	for _, mount := range mounts {
		if mount.MountPath == dir {
			return true
		}
	}
	return false
}

func tcJob11HasMount(mounts []corev1.VolumeMount, name, path string) bool {
	for _, mount := range mounts {
		if mount.Name == name && mount.MountPath == path {
			return true
		}
	}
	return false
}

// TestTCJob11SessionRelaySocketFitsSunPath is the cross-layer check for the
// session plane: the render names the store file, the adapter derives the
// session root from it, and the carrier binds its relay under
// <root>/<attempt>/cache/session-<random>/broker/daemon.sock. That address is
// a unix socket path, so it must fit the kernel's 108-byte sun_path (107 bytes
// of path plus its terminator).
//
// A claim's attempt id is a task id plus the attempt number, so a UUID task id
// spends 38 bytes before the root. That shape is what the relay must survive.
func TestTCJob11SessionRelaySocketFitsSunPath(t *testing.T) {
	const sunPathLimit = 107
	// The shape a real claim produces, and the root the render names.
	taskID := "6f1e2b40-5c7a-4d2e-9b3a-1c8d7e6f5a4b"
	attemptID := registry.AttemptID(taskID, 1)

	job := tcJob11Build(t, "unix:///var/lib/foreman/workspaces/.rsbxd/rsbxd.sock")
	container := job.Spec.Template.Spec.Containers[0]
	sessionRoot := ""
	for _, env := range container.Env {
		if env.Name == "FOREMAN_JOB_STORE_FILE" {
			sessionRoot = filepath.Join(filepath.Dir(env.Value), "session")
		}
	}
	if sessionRoot == "" {
		t.Fatal("the fixed render names no state file, so the session root is untested")
	}

	// The rendered root is the production one; both relays must fit under it.
	settings := jobexec.Settings{SessionDir: sessionRoot}
	if err := settings.CheckAttemptSocketBudget(attemptID); err != nil {
		t.Errorf("relay under the rendered root: %v", err)
	}

	// The counterexample: the same attempt before the directory name was
	// bounded. Unshortened, the session relay is 109 bytes and the probe's is
	// 115, which is the EINVAL the QA run observed at bind time. If this stops
	// exceeding the limit the guard above has lost its teeth.
	for name, cache := range map[string]string{
		"session": filepath.Join(sessionRoot, attemptID, "cache"),
		"probe":   filepath.Join(sessionRoot, attemptID, "probe-cache"),
	} {
		relay := filepath.Join(cache, "session-"+"9999999999", "broker", "daemon.sock")
		if len(relay) <= sunPathLimit {
			t.Errorf("%s relay with an unbounded attempt directory is %d bytes, want over %d: the guard no longer guards anything",
				name, len(relay), sunPathLimit)
		}
	}
}

// TestTCJob11ReadyProbeSurvivesAStateRootTheCarrierCannotSee is the guard for
// the ready probe's observation: the fixed render names the deployer's state
// root as the frozen tree identity, and the carrier answers an unmatched path
// from the daemon's own filesystem. A probe that asks its sandbox to open that
// host path therefore refuses a session the carrier can open — the failure the
// cluster run hit, where the session never started because the probe exited 1
// on `test -d <state_root>`.
//
// The case runs the real adapter against the real rendered binding. The
// fixture stands in for the daemon's view: the state root named by the render
// does not exist here either, so a reintroduced directory check fails exactly
// as it did on the cluster.
func TestTCJob11ReadyProbeSurvivesAStateRootTheCarrierCannotSee(t *testing.T) {
	_, binding := tcJob11Render(t, "unix:///var/lib/foreman/workspaces/.rsbxd/rsbxd.sock")
	if _, err := os.Stat(binding.StateRoot); err == nil {
		t.Fatalf("the fixture has %s, so it cannot reproduce a state root the carrier cannot see", binding.StateRoot)
	}

	dir := t.TempDir()
	credential := filepath.Join(dir, "config.json")
	if err := os.WriteFile(credential, []byte(`{"token":"fmj_test"}`), 0o400); err != nil {
		t.Fatalf("write the Job credential: %v", err)
	}
	clis := filepath.Join(dir, "clis")
	home := filepath.Join(dir, "home")
	for _, path := range []string{clis, home} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("create %s: %v", path, err)
		}
	}
	// The carrier stands in for rsbx: it runs the one command it was given and
	// records its argv, which is the text the adapter actually rendered.
	argsFile := filepath.Join(dir, "probe-argv")
	carrier := filepath.Join(dir, "rsbx")
	body := "#!/bin/sh\n" +
		"if [ ! -f " + argsFile + " ]; then printf '%s\\n' \"$@\" >" + argsFile + "; fi\n" +
		"if [ \"$1\" = \"-config\" ]; then shift 2; fi\n" +
		"if [ \"$1\" = \"/bin/sh\" ]; then shift; exec /bin/sh \"$@\"; fi\n" +
		"exec sleep 60\n"
	if err := os.WriteFile(carrier, []byte(body), 0o755); err != nil {
		t.Fatalf("write the carrier: %v", err)
	}
	// The session root must leave room for the relay socket's AF_UNIX path.
	sessionRoot := tcJob11ShortTempDir(t, "fes-")

	// The Job face answers the verified tree identity the frozen binding
	// names; the generation is proven there, not by the probe.
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tree_id": binding.EnvironmentGen, "identity": binding.EnvironmentIdentity, "state": "ok",
		})
	}))
	t.Cleanup(control.Close)

	t.Setenv("FOREMAN_JOB_TOKEN_FILE", credential)
	t.Setenv("FOREMAN_JOB_CLI_DIR", clis)
	t.Setenv("FOREMAN_JOB_SESSION_DIR", filepath.Join(sessionRoot, "s"))
	t.Setenv("FOREMAN_JOB_RSBX_BIN", carrier)
	t.Setenv("FOREMAN_JOB_RSBX_ENTRY", `["/bin/sleep","60"]`)
	t.Setenv("FOREMAN_JOB_CONTROL_URL", control.URL)
	t.Setenv("FOREMAN_JOB_RSBX_SETTLE", "200ms")
	t.Setenv("FOREMAN_JOB_TLS_DIR", "")
	t.Setenv("HOME", home)

	raw, err := json.Marshal(map[string]any{"binding": binding})
	if err != nil {
		t.Fatalf("encode the ready request: %v", err)
	}
	answer, err := jobexec.Ready(context.Background(), raw)
	if err != nil {
		t.Fatalf("the ready probe refused a session over a state root the carrier cannot see: %v", err)
	}
	session, ok := answer.(attempt.Session)
	if !ok {
		t.Fatalf("Ready answered %T, want the session identity", answer)
	}
	if session.Generation != binding.EnvironmentGen {
		t.Errorf("session generation = %q, want the observed %q", session.Generation, binding.EnvironmentGen)
	}
	t.Cleanup(func() {
		evidence, closeErr := jobexec.Close(context.Background(), mustJSON(t, map[string]any{"session": session}))
		if closeErr != nil {
			t.Logf("close: %v", closeErr)
			return
		}
		if confirmed, _ := evidence.(attempt.CloseEvidence); confirmed.Confirmed {
			t.Error("close confirmed a remote close the carrier never reported")
		}
	})

	recorded, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("read the carrier argv: %v", err)
	}
	probe := string(recorded)
	if strings.Contains(probe, "test -d") || strings.Contains(probe, binding.StateRoot) {
		t.Fatalf("the ready probe asks its sandbox to open the registered host path:\n%s", probe)
	}
	if !strings.Contains(probe, "foreman-ready-") {
		t.Fatalf("the ready probe carries no marker:\n%s", probe)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return body
}

// tcJob11ShortTempDir allocates a directory short enough to leave room for the
// relay socket's AF_UNIX address under it.
func tcJob11ShortTempDir(t *testing.T, prefix string) string {
	t.Helper()
	base := os.TempDir()
	if info, err := os.Stat("/tmp"); err == nil && info.IsDir() && len("/tmp") < len(base) {
		base = "/tmp"
	}
	dir, err := os.MkdirTemp(base, prefix)
	if err != nil {
		t.Fatalf("create a short session root under %s: %v", base, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
