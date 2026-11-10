// TC-tech-auth-05 (AC-21): prepare self-heals the two node-face directory
// positions and the marker position, and fills the Job's credential volume with
// the profile directory. The reference template's prepare script runs here with
// its absolute paths mapped into a sandbox. `.multica` and its `profiles` child
// stay on the node face — the second is the agent container's mount target, so
// a regular file left at either one would fail the mount before any container
// of this Job runs; prepare runs first and is the only place that can heal
// them. The marker position heals from the directory shape, and the profile
// directory itself lives in the Job's credential volume, so a legacy profile
// directory on the node face is left alone (ADR-059 decision 9).
//
// The live-pod half of the marker shape is tech-auth-05-cluster.sh: it seeds
// the marker on the node's home hostPath and asserts the pod starts and the
// node face carries no profile directory or token after the run.
package acceptance

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// tcAuth05Sandbox rewrites the prepare script's absolute paths into one
// sandbox. The chown target is rewritten to the runner's own ids: only root may
// chown to another user, and the target value (1000:1000) is asserted by
// TC-tech-auth-04 / TC-tech-security-01. The second return value is the plain
// round; the third is the round as it behaves under the template's
// runAsUser: 0, where the 0400 credential copy may be replaced in place.
func tcAuth05Sandbox(t *testing.T, script string) (sandbox, scriptOut, rootScript, home, credout string) {
	t.Helper()
	sandbox = t.TempDir()
	home = filepath.Join(sandbox, "home", "agent")
	state := filepath.Join(sandbox, "state", "workspaces")
	cred := filepath.Join(sandbox, "cred")
	credout = filepath.Join(sandbox, "credout")
	for _, dir := range []string{home, state, cred, credout} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	if err := os.WriteFile(filepath.Join(cred, "config.json"), []byte(`{"token":"fmj_test"}`), 0o600); err != nil {
		t.Fatalf("write credential: %v", err)
	}
	replacer := strings.NewReplacer(
		"/home/agent", home,
		"/state/workspaces", state,
		"/credout", credout,
		"/cred", cred,
	)
	rewrite := func(text string) string {
		text = replacer.Replace(text)
		return strings.Replace(text, "chown -R 1000:1000",
			fmt.Sprintf("chown -R %d:%d", os.Getuid(), os.Getgid()), 1)
	}
	// The sandbox runs as the runner's uid, which cannot overwrite the 0400
	// copy the first round leaves behind; the pod's prepare runs as root.
	// Spelling that overwrite out as remove-then-copy keeps the round free of
	// the host cp's overwrite semantics.
	rootScript = rewrite(strings.Replace(script,
		"cp /cred/config.json /credout/foreman-job/config.json",
		"rm -f /credout/foreman-job/config.json && cp /cred/config.json /credout/foreman-job/config.json", 1))
	script = rewrite(script)
	return sandbox, script, rootScript, home, credout
}

// tcAuth05Run executes one prepare round in the sandbox and returns its output.
func tcAuth05Run(t *testing.T, sandbox, script string) string {
	t.Helper()
	cmd := exec.Command("sh", "-ec", script)
	cmd.Dir = sandbox
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("prepare script failed: %v\n%s", err, out)
	}
	return string(out)
}

// TestAuth05PrepareHealsFileOccupiedNodeFacePositions covers the two node-face
// positions a stale payload can leave as a regular file: `.multica` and the
// `profiles` mount target below it. Both must be directories when prepare
// exits, or the agent container's mount and the marker cleanup cannot be set
// up.
func TestAuth05PrepareHealsFileOccupiedNodeFacePositions(t *testing.T) {
	template := decodeJob(t, []byte(publishedTemplateContent(t)))
	prepare := tcJob09Container(t, template.Spec.Template.Spec.InitContainers, "prepare")
	if len(prepare.Args) == 0 {
		t.Fatal("the prepare container carries no script")
	}
	for _, position := range []string{".multica", filepath.Join(".multica", "profiles")} {
		t.Run(filepath.ToSlash(position), func(t *testing.T) {
			sandbox, script, _, home, credout := tcAuth05Sandbox(t, prepare.Args[0])
			occupied := filepath.Join(home, position)
			if err := os.MkdirAll(filepath.Dir(occupied), 0o755); err != nil {
				t.Fatalf("mkdir the position's parent: %v", err)
			}
			if err := os.WriteFile(occupied, []byte("stale"), 0o644); err != nil {
				t.Fatalf("seed the file-shaped position %s: %v", position, err)
			}

			tcAuth05Run(t, sandbox, script)

			for _, want := range []string{".multica", filepath.Join(".multica", "profiles")} {
				info, err := os.Stat(filepath.Join(home, want))
				if err != nil || !info.IsDir() {
					t.Errorf("prepare left %s a non-directory (err %v)", want, err)
				}
			}
			if _, err := os.Stat(filepath.Join(credout, "foreman-job", "config.json")); err != nil {
				t.Errorf("prepare did not fill the credential volume: %v", err)
			}
		})
	}
}

func TestAuth05PrepareHealsMarkerAndFillsTheCredentialVolume(t *testing.T) {
	template := decodeJob(t, []byte(publishedTemplateContent(t)))
	prepare := tcJob09Container(t, template.Spec.Template.Spec.InitContainers, "prepare")
	if len(prepare.Args) == 0 {
		t.Fatal("the prepare container carries no script")
	}
	sandbox, script, rootScript, home, credout := tcAuth05Sandbox(t, prepare.Args[0])

	// The marker position is occupied by a directory; the two directory
	// positions stay healable while they hold a legacy profile.
	if err := os.MkdirAll(filepath.Join(home, ".multica", "daemon_task_context.json"), 0o755); err != nil {
		t.Fatalf("seed the directory-shaped marker position: %v", err)
	}
	// A legacy profile directory on the shared home face is left to the
	// node-side reclaim rules, not rewritten by a task's prepare.
	legacy := filepath.Join(home, ".multica", "profiles", "foreman-job")
	if err := os.MkdirAll(legacy, 0o755); err != nil {
		t.Fatalf("seed the legacy profile directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "keep"), []byte("legacy"), 0o644); err != nil {
		t.Fatalf("seed the legacy profile content: %v", err)
	}

	out := tcAuth05Run(t, sandbox, script)

	if _, err := os.Stat(filepath.Join(home, ".multica", "daemon_task_context.json")); !os.IsNotExist(err) {
		t.Errorf("the marker path survived prepare (err %v)", err)
	}
	if raw, err := os.ReadFile(filepath.Join(legacy, "keep")); err != nil || string(raw) != "legacy" {
		t.Errorf("prepare rewrote the legacy profile directory on the home face: %q (%v)", raw, err)
	}
	for _, want := range []string{".multica", filepath.Join(".multica", "profiles")} {
		info, err := os.Stat(filepath.Join(home, want))
		if err != nil || !info.IsDir() {
			t.Errorf("%s is not a directory after prepare (err %v)", want, err)
		}
	}
	if _, err := os.Stat(filepath.Join(home, ".multica", "profiles", "foreman-job", "config.json")); !os.IsNotExist(err) {
		t.Errorf("prepare wrote a credential on the home face (err %v)", err)
	}

	profile := filepath.Join(credout, "foreman-job")
	info, err := os.Stat(profile)
	if err != nil || !info.IsDir() {
		t.Fatalf("prepare did not create the profile directory in the credential volume (err %v)", err)
	}
	configPath := filepath.Join(profile, "config.json")
	info, err = os.Stat(configPath)
	if err != nil {
		t.Fatalf("the credential copy is absent: %v", err)
	}
	if info.Mode().Perm() != 0o400 {
		t.Errorf("credential copy mode = %o, want 0400", info.Mode().Perm())
	}
	raw, err := os.ReadFile(configPath)
	if err != nil || string(raw) != `{"token":"fmj_test"}` {
		t.Errorf("credential copy = %q (%v), want the source bytes", raw, err)
	}

	// The Job's startup log carries the fixed profile-scope declaration.
	if !strings.Contains(out, "profile-relative state is per-Job and is not reused across tasks") {
		t.Errorf("prepare's startup output does not carry the declaration: %q", out)
	}

	// Idempotence: the healed marker survives a second round, which in the pod
	// runs as root.
	if out := tcAuth05Run(t, sandbox, rootScript); !strings.Contains(out, "profile-relative state is per-Job") {
		t.Errorf("a second prepare round dropped the declaration: %q", out)
	}
	if raw, err := os.ReadFile(configPath); err != nil || string(raw) != `{"token":"fmj_test"}` {
		t.Errorf("a second prepare round broke the credential copy: %q (%v)", raw, err)
	}
}
