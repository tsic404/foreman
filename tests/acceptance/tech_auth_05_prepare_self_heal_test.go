// TC-tech-auth-05 (AC-21): prepare self-heals anomalous file shapes. The
// reference template's prepare script is executed here with its absolute paths
// mapped into a sandbox, against the two shapes the design names: a directory
// position occupied by a file and the marker position occupied by a directory.
// The live-pod half is tests/acceptance/tech-auth-05-cluster.sh: it seeds the
// same two shapes on the node's home hostPath and asserts the pod starts.
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
		"cp /cred/config.json /credout/config.json",
		"rm -f /credout/config.json && cp /cred/config.json /credout/config.json", 1))
	script = rewrite(script)
	return sandbox, script, rootScript, home, credout
}

// tcAuth05Run executes one prepare round in the sandbox.
func tcAuth05Run(t *testing.T, sandbox, script string) {
	t.Helper()
	cmd := exec.Command("sh", "-ec", script)
	cmd.Dir = sandbox
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("prepare script failed: %v\n%s", err, out)
	}
}

func TestAuth05PrepareSelfHealsAnomalousShapes(t *testing.T) {
	template := decodeJob(t, []byte(publishedTemplateContent(t)))
	prepare := tcJob09Container(t, template.Spec.Template.Spec.InitContainers, "prepare")
	if len(prepare.Args) == 0 {
		t.Fatal("the prepare container carries no script")
	}
	sandbox, script, rootScript, home, credout := tcAuth05Sandbox(t, prepare.Args[0])

	// Shape 1: the profile directory position is occupied by a file.
	if err := os.MkdirAll(filepath.Join(home, ".multica"), 0o755); err != nil {
		t.Fatalf("mkdir .multica: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".multica", "profiles"), []byte("stale"), 0o600); err != nil {
		t.Fatalf("seed the file-shaped directory position: %v", err)
	}
	// Shape 2: the marker file position is occupied by a directory.
	if err := os.MkdirAll(filepath.Join(home, ".multica", "daemon_task_context.json"), 0o755); err != nil {
		t.Fatalf("seed the directory-shaped marker position: %v", err)
	}

	tcAuth05Run(t, sandbox, script)

	for _, dir := range []string{
		filepath.Join(home, ".multica"),
		filepath.Join(home, ".multica", "profiles"),
		filepath.Join(home, ".multica", "profiles", "foreman-job"),
	} {
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			t.Errorf("%s is not a directory after prepare (err %v)", dir, err)
		}
	}
	if _, err := os.Stat(filepath.Join(home, ".multica", "daemon_task_context.json")); !os.IsNotExist(err) {
		t.Errorf("the marker path survived prepare (err %v)", err)
	}
	info, err := os.Stat(filepath.Join(credout, "config.json"))
	if err != nil {
		t.Fatalf("the credential copy is absent: %v", err)
	}
	if info.Mode().Perm() != 0o400 {
		t.Errorf("credential copy mode = %o, want 0400", info.Mode().Perm())
	}
	raw, err := os.ReadFile(filepath.Join(credout, "config.json"))
	if err != nil || string(raw) != `{"token":"fmj_test"}` {
		t.Errorf("credential copy = %q (%v), want the source bytes", raw, err)
	}

	// Idempotence: the healed shapes survive a second round, which in the pod
	// runs as root.
	tcAuth05Run(t, sandbox, rootScript)
	if info, err := os.Stat(filepath.Join(home, ".multica", "profiles", "foreman-job")); err != nil || !info.IsDir() {
		t.Errorf("a second prepare round broke the profile directory: %v", err)
	}
}
