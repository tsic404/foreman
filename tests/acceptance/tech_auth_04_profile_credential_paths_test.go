// TC-tech-auth-04 (AC-21): the Job daemon's profile and credential paths on
// the reference-template render. The profile directory is the Job's own
// credential volume (ADR-059): prepare writes the Job Token into that volume
// with the fixed owner and mode, the agent container mounts the volume at the
// profile root, and the node's shared home face carries no profile directory
// and no token. The two live halves — the node state root after a run and the
// concurrent registration of two Jobs on one node — are the cluster window;
// this file judges the rendered shape.
package acceptance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

const (
	// tcAuth04ProfilesRoot is the mount path of the Job's credential volume:
	// the daemon resolves its fixed profile under it, so every profile
	// directory lives inside that volume.
	tcAuth04ProfilesRoot = "/home/agent/.multica/profiles"
	tcAuth04HomePath     = "/home/agent"
	tcAuth04HomeRoot     = "/var/lib/foreman/home"
	tcAuth04Workspace    = "ws-1"
	// Node-side state is partitioned per workspace (ADR-047): the rendered
	// home volume is the claimed task's workspace tree under the shared root.
	tcAuth04HomeVolume  = tcAuth04HomeRoot + "/" + tcAuth04Workspace
	tcAuth04StateVolume = "/var/lib/foreman/workspaces"
)

// tcAuth04EnvNames returns the env names in declaration order.
func tcAuth04EnvNames(env []corev1.EnvVar) []string {
	names := make([]string, 0, len(env))
	for _, entry := range env {
		names = append(names, entry.Name)
	}
	return names
}

// tcAuth04Env returns the named env entry.
func tcAuth04Env(t *testing.T, env []corev1.EnvVar, name string) corev1.EnvVar {
	t.Helper()
	for _, entry := range env {
		if entry.Name == name {
			return entry
		}
	}
	t.Fatalf("env %s is absent", name)
	return corev1.EnvVar{}
}

// tcAuth04Mount returns the named volume mount.
func tcAuth04Mount(t *testing.T, mounts []corev1.VolumeMount, name string) corev1.VolumeMount {
	t.Helper()
	for _, mount := range mounts {
		if mount.Name == name {
			return mount
		}
	}
	t.Fatalf("volumeMount %s is absent", name)
	return corev1.VolumeMount{}
}

// tcAuth04Volume returns the named volume.
func tcAuth04Volume(t *testing.T, volumes []corev1.Volume, name string) corev1.Volume {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			return volume
		}
	}
	t.Fatalf("volume %s is absent", name)
	return corev1.Volume{}
}

// tcAuth04Declaration extracts the fixed profile-scope declaration from the
// prepare script: the one line the reference template echoes into every task
// Job's startup log (§6.2). Returning the exact text lets the same sentence be
// looked up in the deployment documentation.
func tcAuth04Declaration(t *testing.T, script string) string {
	t.Helper()
	for _, line := range strings.Split(script, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "echo ") {
			continue
		}
		value := strings.TrimPrefix(trimmed, "echo ")
		value = strings.Trim(value, `"'`)
		if value != "" {
			return value
		}
	}
	t.Fatalf("the prepare script carries no declaration line:\n%s", script)
	return ""
}

func TestAuth04ProfileAndCredentialPaths(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
	tcClaimOnClaim(t, h, tcBaseClaim("task-2"))

	job := h.createdJob(t, "fm-task-1")
	second := h.createdJob(t, "fm-task-2")
	secret := h.createdSecret(t, "fm-task-1-cred")
	template := decodeJob(t, []byte(publishedTemplateContent(t)))
	templateAgent := tcJob09Container(t, template.Spec.Template.Spec.Containers, "agent")
	agent := tcJob09Container(t, job.Spec.Template.Spec.Containers, "agent")

	// Env: the template's key set, the injected three, no forbidden key.
	gotNames := tcAuth04EnvNames(agent.Env)
	wantNames := tcAuth04EnvNames(templateAgent.Env)
	if !slices.Equal(gotNames, wantNames) {
		t.Errorf("agent env keys = %v, want the template's %v", gotNames, wantNames)
	}
	seen := map[string]bool{}
	for _, name := range gotNames {
		if seen[name] {
			t.Errorf("env %s is declared more than once", name)
		}
		seen[name] = true
	}
	for _, forbidden := range []string{
		"MULTICA_TASK_CONFIG_ROOT", "MULTICA_AGENT_ID", "MULTICA_TASK_ID", "MULTICA_DAEMON_PORT",
	} {
		if slices.Contains(gotNames, forbidden) {
			t.Errorf("env %s must not appear in a Job container (task-identity / pinned-port key)", forbidden)
		}
	}
	server := tcAuth04Env(t, agent.Env, "MULTICA_SERVER_URL")
	if server.Value != jobbuilder.DefaultJobServerURL {
		t.Errorf("MULTICA_SERVER_URL = %q, want the Foreman address %q", server.Value, jobbuilder.DefaultJobServerURL)
	}
	daemonID := tcAuth04Env(t, agent.Env, "MULTICA_DAEMON_ID")
	if daemonID.ValueFrom == nil || daemonID.ValueFrom.FieldRef == nil ||
		daemonID.ValueFrom.FieldRef.FieldPath != "metadata.labels['foreman.tsic.top/daemon-id']" {
		t.Errorf("MULTICA_DAEMON_ID = %+v, want the pod-label fieldRef", daemonID)
	}
	if got := tcAuth04Env(t, agent.Env, "MULTICA_DAEMON_MAX_CONCURRENT_TASKS").Value; got != "1" {
		t.Errorf("MULTICA_DAEMON_MAX_CONCURRENT_TASKS = %q, want 1", got)
	}
	if got := tcAuth04Env(t, agent.Env, "HOME").Value; got != tcAuth04HomePath {
		t.Errorf("HOME = %q, want %q (the session root stays on the home volume)", got, tcAuth04HomePath)
	}

	// Startup face: the fixed profile, working directory at the root.
	if got := strings.Join(agent.Command, " "); got != "/usr/bin/dumb-init -- /usr/local/bin/multica" {
		t.Errorf("agent command = %q, want the init prefix and the CLI", got)
	}
	if got := strings.Join(agent.Args, " "); got != "daemon start --foreground --profile foreman-job" {
		t.Errorf("agent args = %q, want the fixed profile's daemon start", got)
	}
	if agent.WorkingDir != "/" {
		t.Errorf("agent workingDir = %q, want /", agent.WorkingDir)
	}

	// The profile face: the credential volume is mounted as the profile root,
	// writable (the daemon writes pid, log and profile-relative stores there),
	// and nothing else is mounted under the home volume that could carry a
	// profile directory of its own.
	credMount := tcAuth04Mount(t, agent.VolumeMounts, "cred")
	if credMount.MountPath != tcAuth04ProfilesRoot {
		t.Errorf("cred mount path = %q, want the profile root %q", credMount.MountPath, tcAuth04ProfilesRoot)
	}
	if credMount.SubPath != "" || credMount.ReadOnly {
		t.Errorf("cred mount = %+v, want a whole-volume, writable mount (ADR-059: no single-file, read-only face)", credMount)
	}
	for _, mount := range agent.VolumeMounts {
		if strings.HasPrefix(mount.MountPath, tcAuth04HomePath+"/") && mount.Name != "cred" {
			t.Errorf("unexpected mount under the home volume: %s", mount.MountPath)
		}
	}

	// The credential volume is the per-Job emptyDir; its Secret source keeps
	// the fixed mode.
	cred := tcAuth04Volume(t, job.Spec.Template.Spec.Volumes, "cred")
	if cred.EmptyDir == nil || cred.HostPath != nil {
		t.Errorf("cred volume = %+v, want an emptyDir bucket", cred)
	}
	credSrc := tcAuth04Volume(t, job.Spec.Template.Spec.Volumes, "cred-src")
	if credSrc.Secret == nil {
		t.Fatalf("cred-src volume = %+v, want a Secret source", credSrc)
	}
	if credSrc.Secret.SecretName != "fm-task-1-cred" {
		t.Errorf("cred-src secretName = %q, want fm-task-1-cred", credSrc.Secret.SecretName)
	}
	if credSrc.Secret.DefaultMode == nil || *credSrc.Secret.DefaultMode != 0o400 {
		t.Errorf("cred-src defaultMode = %v, want 0400", credSrc.Secret.DefaultMode)
	}

	// Home and workspaces are the node-shared volumes; the profile is not one
	// of their entries and no host path carries a credential.
	home := tcAuth04Volume(t, job.Spec.Template.Spec.Volumes, "home")
	if home.HostPath == nil || home.HostPath.Path != tcAuth04HomeVolume {
		t.Errorf("home volume = %+v, want hostPath %s", home, tcAuth04HomeVolume)
	}
	workspaces := tcAuth04Volume(t, job.Spec.Template.Spec.Volumes, "workspaces")
	if workspaces.HostPath == nil || workspaces.HostPath.Path != tcAuth04StateVolume {
		t.Errorf("workspaces volume = %+v, want hostPath %s", workspaces, tcAuth04StateVolume)
	}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.HostPath == nil {
			continue
		}
		if strings.Contains(volume.HostPath.Path, "cred") {
			t.Errorf("hostPath volume %s points at a credential path: %s", volume.Name, volume.HostPath.Path)
		}
	}

	// prepare fills the credential volume: the profile directory, the token as
	// a volume file with the fixed owner and mode, and the profile-scope
	// declaration in the Job's startup log.
	prepare := tcJob09Container(t, job.Spec.Template.Spec.InitContainers, "prepare")
	prepareCred := tcAuth04Mount(t, prepare.VolumeMounts, "cred")
	if prepareCred.MountPath != "/credout" {
		t.Errorf("prepare's credential mount = %q, want the write end /credout", prepareCred.MountPath)
	}
	script := strings.Join(prepare.Args, "\n")
	for _, want := range []string{
		"for d in /home/agent/.multica /home/agent/.multica/profiles; do",
		"mkdir -p /credout/foreman-job",
		"cp /cred/config.json /credout/foreman-job/config.json",
		"chmod 0400 /credout/foreman-job/config.json",
		"chown -R 1000:1000 /home/agent /state/workspaces /credout",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("prepare script does not contain %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, tcAuth04ProfilesRoot+"/foreman-job") {
		t.Errorf("prepare writes the profile directory on the home face:\n%s", script)
	}
	declaration := tcAuth04Declaration(t, script)
	readme, err := os.ReadFile(filepath.Join("..", "..", "deploy", "README.md"))
	if err != nil {
		t.Fatalf("read deploy/README.md: %v", err)
	}
	if !strings.Contains(string(readme), declaration) {
		t.Errorf("deploy/README.md does not carry the Job startup declaration %q", declaration)
	}
	if !strings.Contains(declaration, "per-Job") && !strings.Contains(declaration, "per-job") {
		t.Errorf("the declaration %q does not state the profile's per-Job scope", declaration)
	}
	if !strings.Contains(declaration, "not reused across tasks") {
		t.Errorf("the declaration %q does not state that profile state is not reused across tasks", declaration)
	}

	// The token lives in the Secret and nowhere in the Job object; the emptyDir
	// only receives it at run time, through prepare.
	var parsed struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(secret.StringData["config.json"]), &parsed); err != nil {
		t.Fatalf("Secret config.json: %v", err)
	}
	rendered, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	if strings.Contains(string(rendered), parsed.Token) {
		t.Error("the Job object carries the Job Token; only the Secret may")
	}

	// Two Jobs of one workspace, on one node by construction: each gets its
	// own credential volume at the same mount path, so their profile
	// directories cannot meet at the directory level.
	secondAgent := tcJob09Container(t, second.Spec.Template.Spec.Containers, "agent")
	secondCred := tcAuth04Mount(t, secondAgent.VolumeMounts, "cred")
	if secondCred.MountPath != tcAuth04ProfilesRoot {
		t.Errorf("second Job's cred mount = %q, want the profile root", secondCred.MountPath)
	}
	if got := tcAuth04Volume(t, second.Spec.Template.Spec.Volumes, "cred"); got.EmptyDir == nil {
		t.Errorf("second Job's cred volume = %+v, want its own emptyDir", got)
	}
	if got := tcAuth04Volume(t, second.Spec.Template.Spec.Volumes, "cred-src").Secret.SecretName; got != "fm-task-2-cred" {
		t.Errorf("second Job cred-src = %q, want fm-task-2-cred", got)
	}
	if got := tcAuth04Volume(t, second.Spec.Template.Spec.Volumes, "home").HostPath.Path; got != tcAuth04HomeVolume {
		t.Errorf("second Job home = %q, want the same workspace partition %q", got, tcAuth04HomeVolume)
	}
}
