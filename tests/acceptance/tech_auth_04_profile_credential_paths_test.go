// TC-tech-auth-04 (AC-21): the Job daemon's profile and credential paths on
// the reference-template render. The agent container's env key set is exactly
// the template's (the three injected keys replace same-name entries), with no
// task-identity key and no MULTICA_DAEMON_PORT; the startup arguments and the
// working directory are the fixed profile's; the Job Token exists only in the
// per-task Secret and the single-file credential mount (read-only, fixed mode),
// while the node-shared home face carries no token; the profile directory and
// the session directories both live on the home volume.
package acceptance

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

const (
	tcAuth04ProfileDir = "/home/agent/.multica/profiles/foreman-job"
	tcAuth04CredFile   = tcAuth04ProfileDir + "/config.json"
	tcAuth04HomePath   = "/home/agent"
	tcAuth04HomeRoot   = "/var/lib/foreman/home"
	tcAuth04Workspace  = "ws-1"
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

func TestAuth04ProfileAndCredentialPaths(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	tcClaimOnClaim(t, h, tcBaseClaim("task-1"))

	job := h.createdJob(t, "fm-task-1")
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

	// Credential: one single-file mount, read-only, on the profile file.
	var credMount *corev1.VolumeMount
	for i := range agent.VolumeMounts {
		mount := &agent.VolumeMounts[i]
		if mount.MountPath == tcAuth04CredFile {
			credMount = mount
		}
		if strings.HasPrefix(mount.MountPath, tcAuth04HomePath+"/") && mount.MountPath != tcAuth04CredFile {
			t.Errorf("unexpected mount under the home volume: %s", mount.MountPath)
		}
	}
	if credMount == nil {
		t.Fatalf("the agent container does not mount %s", tcAuth04CredFile)
	}
	if !credMount.ReadOnly || credMount.SubPath != "config.json" || credMount.Name != "cred" {
		t.Errorf("credential mount = %+v, want name=cred subPath=config.json readOnly", *credMount)
	}

	// The Secret source and its fixed mode; the credential bucket is an
	// emptyDir, not a node path.
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
	cred := tcAuth04Volume(t, job.Spec.Template.Spec.Volumes, "cred")
	if cred.EmptyDir == nil || cred.HostPath != nil {
		t.Errorf("cred volume = %+v, want an emptyDir bucket", cred)
	}

	// Home and workspaces are the node-shared volumes; the profile and the
	// session directories both resolve under home, so a resumed pod sees them.
	home := tcAuth04Volume(t, job.Spec.Template.Spec.Volumes, "home")
	if home.HostPath == nil || home.HostPath.Path != tcAuth04HomeVolume {
		t.Errorf("home volume = %+v, want hostPath %s", home, tcAuth04HomeVolume)
	}
	workspaces := tcAuth04Volume(t, job.Spec.Template.Spec.Volumes, "workspaces")
	if workspaces.HostPath == nil || workspaces.HostPath.Path != tcAuth04StateVolume {
		t.Errorf("workspaces volume = %+v, want hostPath %s", workspaces, tcAuth04StateVolume)
	}

	// The token lives in the Secret and nowhere else; the home face is empty
	// at render time (no token, no Secret reference).
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
	if strings.Contains(home.HostPath.Path, "cred") {
		t.Errorf("home hostPath %q looks like a credential path", home.HostPath.Path)
	}
}
