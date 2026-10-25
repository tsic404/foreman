// TC-tech-auth-05's cluster carrier, driven under a fake kubectl/curl so the
// workspace-partition path has executable assertions in the repo: the parts
// that are pure string work (template hostPath parsing, <workspace_id>
// expansion, the state file round trip into restore) would otherwise only be
// exercised by the next manual cluster window.
package acceptance

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// tcAuth05CarrierPath returns the live carrier next to this test file.
func tcAuth05CarrierPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the carrier from the test source")
	}
	return filepath.Join(filepath.Dir(file), "tech-auth-05-cluster.sh")
}

func tcAuth05WriteFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// tcAuth05FakeTools writes the two operator-side commands the carrier calls.
// kubectl answers "get" from JSON files and runs the node-side script it is
// handed for "debug"; curl answers one workspace-scoped task read.
func tcAuth05FakeTools(t *testing.T) string {
	t.Helper()
	bin := t.TempDir()
	kubectl := `#!/bin/sh
set -eu
cmd=""
for a in "$@"; do
	case $a in
	get|debug|exec) cmd=$a; break ;;
	esac
done
case $cmd in
get)
	for a in "$@"; do
		case $a in
		nodes) cat "$FAKE_NODES"; exit 0 ;;
		job) cat "$FAKE_JOB"; exit 0 ;;
		pods) cat "$FAKE_PODS"; exit 0 ;;
		esac
	done
	echo "fake kubectl: unhandled get: $*" >&2
	exit 1
	;;
debug)
	for last; do :; done
	exec sh -c "$last"
	;;
exec)
	cat "$FAKE_EXEC"
	;;
*)
	echo "fake kubectl: unhandled: $*" >&2
	exit 1
	;;
esac
`
	curl := `#!/bin/sh
set -eu
url=""
for a in "$@"; do
	case $a in
	http*) url=$a ;;
	esac
done
case $url in
*"/foreman/tasks/$FAKE_TASK?workspace=$FAKE_WS") ;;
*)
	echo "fake curl: unexpected task read: $url" >&2
	exit 22
	;;
esac
cat "$FAKE_TASK_JSON"
`
	for name, body := range map[string]string{"kubectl": kubectl, "curl": curl} {
		path := filepath.Join(bin, name)
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatalf("write fake %s: %v", name, err)
		}
	}
	return bin
}

func tcAuth05Env(bin string, overrides map[string]string) []string {
	merged := map[string]string{}
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i > 0 {
			merged[kv[:i]] = kv[i+1:]
		}
	}
	merged["PATH"] = bin + string(os.PathListSeparator) + merged["PATH"]
	for key, value := range overrides {
		merged[key] = value
	}
	env := make([]string, 0, len(merged))
	for key, value := range merged {
		env = append(env, key+"="+value)
	}
	sort.Strings(env)
	return env
}

func tcAuth05RunCarrier(t *testing.T, script, bin string, env map[string]string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command("sh", append([]string{script}, args...)...)
	cmd.Env = tcAuth05Env(bin, env)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return string(out), exitErr.ExitCode()
	}
	t.Fatalf("run carrier %v: %v", args, err)
	return "", 0
}

func tcAuth05MustRead(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

func tcAuth05MustReadTrimmed(t *testing.T, path string) string {
	t.Helper()
	return strings.TrimSpace(tcAuth05MustRead(t, path))
}

func tcAuth05Require(t *testing.T, ok bool, format string, args ...any) {
	t.Helper()
	if !ok {
		t.Fatalf(format, args...)
	}
}

// tcAuth05SeedPartition writes the two positions the carrier will turn into
// anomalies and the originals it must put back.
func tcAuth05SeedPartition(t *testing.T, partition, label string) {
	t.Helper()
	tcAuth05WriteFile(t, filepath.Join(partition, ".multica", "profiles", "foreman-job", "keep"), label+" profile")
	tcAuth05WriteFile(t, filepath.Join(partition, ".multica", "daemon_task_context.json"), label+" marker")
}

const tcAuth05PrepareArgs = `for d in /home/agent/.multica; do [ -d "$d" ] || { rm -f "$d"; mkdir -p "$d"; }; done; rm -rf /home/agent/.multica/daemon_task_context.json`

func tcAuth05WriteJob(t *testing.T, path, home string) {
	t.Helper()
	tcAuth05WriteFile(t, path, fmt.Sprintf(`{"spec":{"template":{"spec":{"volumes":[{"name":"home","hostPath":{"path":%q}}],"initContainers":[{"name":"prepare","args":[%q]}]}}}}`, home, tcAuth05PrepareArgs))
}

func tcAuth05WritePods(t *testing.T, path string, prepareExit int, agentRunning bool) {
	t.Helper()
	agentState := `{"waiting":{"reason":"ContainerCreating"}}`
	if agentRunning {
		agentState = `{"running":{}}`
	}
	tcAuth05WriteFile(t, path, fmt.Sprintf(`{"items":[{"metadata":{"name":"pod-1","creationTimestamp":"2026-01-01T00:00:00Z"},"spec":{"nodeName":"node-1"},"status":{"initContainerStatuses":[{"name":"prepare","state":{"terminated":{"exitCode":%d}}}],"containerStatuses":[{"name":"agent","state":%s}]}}]}`, prepareExit, agentState))
}

func tcAuth05CarrierBaseEnv(t *testing.T, template, stateDir, hostRoot, nodes string) map[string]string {
	t.Helper()
	return map[string]string{
		"FOREMAN_JOB_TEMPLATE": template,
		"STATE_DIR":            stateDir,
		"NODE_HOST_ROOT":       hostRoot,
		"FAKE_NODES":           nodes,
	}
}

func TestAuth05ClusterCarrierSelectsWorkspacePartition(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	script := tcAuth05CarrierPath(t)
	bin := tcAuth05FakeTools(t)
	hostRoot := t.TempDir()
	stateDir := filepath.Join(t.TempDir(), "state")
	workspace, otherWorkspace := "ws-under-test", "ws-other"
	partition := filepath.Join(hostRoot, "home", workspace)
	otherPartition := filepath.Join(hostRoot, "home", otherWorkspace)
	tcAuth05SeedPartition(t, partition, "under-test")
	tcAuth05SeedPartition(t, otherPartition, "other")

	// Block-map form: hostPath on its own line, path on the next mapping line.
	template := filepath.Join(t.TempDir(), "template.yaml")
	tcAuth05WriteFile(t, template, `volumes:
  - name: home
    hostPath:
      path: /home/<workspace_id>
      type: DirectoryOrCreate
  - name: workspaces
    hostPath: { path: /workspaces, type: DirectoryOrCreate }
`)
	nodes := filepath.Join(t.TempDir(), "nodes.json")
	tcAuth05WriteFile(t, nodes, `{"items":[{"metadata":{"name":"node-1"},"spec":{}}]}`)
	env := tcAuth05CarrierBaseEnv(t, template, stateDir, hostRoot, nodes)
	env["WORKSPACE_ID"] = workspace

	out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
	tcAuth05Require(t, code == 0, "prepare exited %d:\n%s", code, out)
	tcAuth05Require(t, strings.Contains(out, "state=1|"), "prepare did not move the existing profile aside:\n%s", out)

	profile := filepath.Join(partition, ".multica", "profiles", "foreman-job")
	info, err := os.Stat(profile)
	tcAuth05Require(t, err == nil && info.Mode().IsRegular(), "profile position is not a regular file: %v", err)
	marker := filepath.Join(partition, ".multica", "daemon_task_context.json")
	info, err = os.Stat(marker)
	tcAuth05Require(t, err == nil && info.IsDir(), "marker position is not a directory: %v", err)
	tcAuth05Require(t, tcAuth05MustRead(t, filepath.Join(marker, "nested", "marker")) == "anomaly\n",
		"marker nested anomaly content changed")

	profiles, _ := filepath.Glob(filepath.Join(partition, ".tc-auth-05-*-profile"))
	tcAuth05Require(t, len(profiles) == 1, "profile backup count = %d, want 1", len(profiles))
	info, err = os.Stat(profiles[0])
	tcAuth05Require(t, err == nil && info.IsDir(), "profile backup is not a directory: %v", err)
	tcAuth05Require(t, tcAuth05MustRead(t, filepath.Join(profiles[0], "keep")) == "under-test profile", "profile backup content changed")
	markers, _ := filepath.Glob(filepath.Join(partition, ".tc-auth-05-*-marker"))
	tcAuth05Require(t, len(markers) == 1, "marker backup count = %d, want 1", len(markers))
	tcAuth05Require(t, tcAuth05MustRead(t, markers[0]) == "under-test marker", "marker backup content changed")

	// The rendered partition is exactly the template prefix plus the caller's
	// workspace, and it is recorded for verify/restore.
	tcAuth05Require(t, tcAuth05MustReadTrimmed(t, filepath.Join(stateDir, "home")) == "/home/"+workspace,
		"state home is %q, want %q", tcAuth05MustReadTrimmed(t, filepath.Join(stateDir, "home")), "/home/"+workspace)
	tcAuth05Require(t, tcAuth05MustReadTrimmed(t, filepath.Join(stateDir, "workspace")) == workspace, "state workspace mismatch")
	tcAuth05Require(t, tcAuth05MustReadTrimmed(t, filepath.Join(stateDir, "host_root")) == hostRoot, "state host root mismatch")

	// The other workspace's partition and the legacy state-root home position
	// ($STATE_ROOT/home/.multica) are untouched.
	otherProfile := filepath.Join(otherPartition, ".multica", "profiles", "foreman-job")
	info, err = os.Stat(otherProfile)
	tcAuth05Require(t, err == nil && info.IsDir(), "other workspace profile was rewritten: %v", err)
	tcAuth05Require(t, tcAuth05MustRead(t, filepath.Join(otherProfile, "keep")) == "other profile", "other workspace profile content changed")
	tcAuth05Require(t, tcAuth05MustRead(t, filepath.Join(otherPartition, ".multica", "daemon_task_context.json")) == "other marker",
		"other workspace marker changed")
	if _, err := os.Stat(filepath.Join(hostRoot, "var", "lib", "foreman", "home", ".multica")); !os.IsNotExist(err) {
		t.Fatalf("carrier wrote the legacy state-root home position: %v", err)
	}

	// Verify reads the task in the same workspace, checks that the Job's home
	// volume is the prepared partition, and restores the tree.
	jobPath := filepath.Join(t.TempDir(), "job.json")
	podsPath := filepath.Join(t.TempDir(), "pods.json")
	tcAuth05WriteJob(t, jobPath, "/home/"+workspace)
	tcAuth05WritePods(t, podsPath, 0, true)
	taskPath := filepath.Join(t.TempDir(), "task.json")
	tcAuth05WriteFile(t, taskPath, fmt.Sprintf(`{"task_id":"task-1","workspace_id":%q,"job_name":"job-1"}`, workspace))
	execPath := filepath.Join(t.TempDir(), "exec.txt")
	tcAuth05WriteFile(t, execPath, "400 1000:1000 /home/agent/.multica/profiles/foreman-job/config.json\nprofile=dir\nmarker=absent\n")
	env["FAKE_JOB"] = jobPath
	env["FAKE_PODS"] = podsPath
	env["FAKE_TASK"] = "task-1"
	env["FAKE_WS"] = workspace
	env["FAKE_TASK_JSON"] = taskPath
	env["FAKE_EXEC"] = execPath

	// Rejection branches, using the same prepared window and fixtures: a Job
	// mounting another workspace's partition, a prepare that exited non-zero,
	// and an agent container that never started must each fail rather than
	// pass. The window survives every refusal, so the passing run below still
	// restores it.
	tcAuth05WriteJob(t, jobPath, "/home/"+otherWorkspace)
	out, code = tcAuth05RunCarrier(t, script, bin, env, "verify", "task-1")
	tcAuth05Require(t, code == 2, "mismatched Job home exited %d, want blocked:\n%s", code, out)
	tcAuth05Require(t, strings.Contains(out, "mounts home at"), "mismatched Job home was not named:\n%s", out)

	tcAuth05WriteJob(t, jobPath, "/home/"+workspace)
	tcAuth05WritePods(t, podsPath, 1, true)
	env["WAIT"] = "1"
	out, code = tcAuth05RunCarrier(t, script, bin, env, "verify", "task-1")
	tcAuth05Require(t, code == 1, "failed prepare exited %d, want FAIL:\n%s", code, out)
	tcAuth05Require(t, strings.Contains(out, "prepare did not complete successfully"), "failed prepare was not named:\n%s", out)

	tcAuth05WritePods(t, podsPath, 0, false)
	out, code = tcAuth05RunCarrier(t, script, bin, env, "verify", "task-1")
	tcAuth05Require(t, code == 1, "unstarted agent exited %d, want FAIL:\n%s", code, out)
	tcAuth05Require(t, strings.Contains(out, "the agent container never started"), "unstarted agent was not named:\n%s", out)

	tcAuth05WritePods(t, podsPath, 0, true)
	env["WAIT"] = "5"
	out, code = tcAuth05RunCarrier(t, script, bin, env, "verify", "task-1")
	tcAuth05Require(t, code == 0, "verify exited %d:\n%s", code, out)
	tcAuth05Require(t, strings.Contains(out, "PASS: TC-tech-auth-05"), "verify did not PASS:\n%s", out)

	info, err = os.Stat(profile)
	tcAuth05Require(t, err == nil && info.IsDir(), "profile not restored to a directory: %v", err)
	tcAuth05Require(t, tcAuth05MustRead(t, filepath.Join(profile, "keep")) == "under-test profile", "profile content not restored")
	tcAuth05Require(t, tcAuth05MustRead(t, marker) == "under-test marker", "marker not restored")
	if _, err := os.Stat(stateDir); !os.IsNotExist(err) {
		t.Fatalf("state dir survived a successful verify: %v", err)
	}
	tcAuth05Require(t, tcAuth05MustRead(t, filepath.Join(otherProfile, "keep")) == "other profile", "other workspace changed during restore")
}

func TestAuth05ClusterCarrierAcceptsInlineHostPath(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	script := tcAuth05CarrierPath(t)
	bin := tcAuth05FakeTools(t)
	hostRoot := t.TempDir()
	stateDir := filepath.Join(t.TempDir(), "state")
	workspace := "ws-inline"
	partition := filepath.Join(hostRoot, "srv", workspace)
	tcAuth05SeedPartition(t, partition, "inline")
	template := filepath.Join(t.TempDir(), "template.yaml")
	tcAuth05WriteFile(t, template, "volumes:\n  - name: home\n    hostPath: { path: /srv/<workspace_id>, type: DirectoryOrCreate }\n")
	nodes := filepath.Join(t.TempDir(), "nodes.json")
	tcAuth05WriteFile(t, nodes, `{"items":[{"metadata":{"name":"node-1"},"spec":{}}]}`)
	env := tcAuth05CarrierBaseEnv(t, template, stateDir, hostRoot, nodes)
	env["WORKSPACE_ID"] = workspace

	out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
	tcAuth05Require(t, code == 0, "prepare exited %d:\n%s", code, out)
	tcAuth05Require(t, tcAuth05MustReadTrimmed(t, filepath.Join(stateDir, "home")) == "/srv/"+workspace,
		"inline hostPath rendered %q", tcAuth05MustReadTrimmed(t, filepath.Join(stateDir, "home")))

	out, code = tcAuth05RunCarrier(t, script, bin, env, "restore")
	tcAuth05Require(t, code == 0, "restore exited %d:\n%s", code, out)
	info, err := os.Stat(filepath.Join(partition, ".multica", "profiles", "foreman-job"))
	tcAuth05Require(t, err == nil && info.IsDir(), "inline window profile not restored: %v", err)
	tcAuth05Require(t, tcAuth05MustRead(t, filepath.Join(partition, ".multica", "daemon_task_context.json")) == "inline marker",
		"inline window marker not restored")
}

func TestAuth05ClusterCarrierRejectsUnreadableHomeHostPath(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	script := tcAuth05CarrierPath(t)
	bin := tcAuth05FakeTools(t)
	nodes := filepath.Join(t.TempDir(), "nodes.json")
	tcAuth05WriteFile(t, nodes, `{"items":[{"metadata":{"name":"node-1"},"spec":{}}]}`)
	cases := []struct {
		name     string
		template string
	}{
		{"block_without_path", "volumes:\n  - name: home\n    hostPath:\n      type: DirectoryOrCreate\n  - name: workspaces\n    hostPath: { path: /workspaces, type: DirectoryOrCreate }\n"},
		{"relative_path", "volumes:\n  - name: home\n    hostPath: { path: home/<workspace_id>, type: DirectoryOrCreate }\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			template := filepath.Join(t.TempDir(), "template.yaml")
			tcAuth05WriteFile(t, template, tc.template)
			env := tcAuth05CarrierBaseEnv(t, template, filepath.Join(t.TempDir(), "state"), t.TempDir(), nodes)
			env["WORKSPACE_ID"] = "ws-1"
			out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
			tcAuth05Require(t, code == 2, "prepare exited %d, want blocked:\n%s", code, out)
			tcAuth05Require(t, strings.Contains(out, "cannot read the home volume's hostPath"),
				"unreadable hostPath did not get its own reason:\n%s", out)
			tcAuth05Require(t, !strings.Contains(out, "not partitioned by <workspace_id>"),
				"unreadable hostPath was reported as an unpartitioned template:\n%s", out)
		})
	}
}

func TestAuth05ClusterCarrierRejectsPlaceholderlessHomeHostPath(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	script := tcAuth05CarrierPath(t)
	bin := tcAuth05FakeTools(t)
	nodes := filepath.Join(t.TempDir(), "nodes.json")
	tcAuth05WriteFile(t, nodes, `{"items":[{"metadata":{"name":"node-1"},"spec":{}}]}`)
	template := filepath.Join(t.TempDir(), "template.yaml")
	tcAuth05WriteFile(t, template, "volumes:\n  - name: home\n    hostPath: { path: /srv/static, type: DirectoryOrCreate }\n")
	env := tcAuth05CarrierBaseEnv(t, template, filepath.Join(t.TempDir(), "state"), t.TempDir(), nodes)
	env["WORKSPACE_ID"] = "ws-1"

	out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
	tcAuth05Require(t, code == 2, "prepare exited %d, want blocked:\n%s", code, out)
	tcAuth05Require(t, strings.Contains(out, "not partitioned by <workspace_id>"), "missing partition verdict:\n%s", out)
}

func TestAuth05ClusterCarrierRequiresWorkspaceID(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	script := tcAuth05CarrierPath(t)
	bin := tcAuth05FakeTools(t)
	nodes := filepath.Join(t.TempDir(), "nodes.json")
	tcAuth05WriteFile(t, nodes, `{"items":[{"metadata":{"name":"node-1"},"spec":{}}]}`)
	template := filepath.Join(t.TempDir(), "template.yaml")
	tcAuth05WriteFile(t, template, "volumes:\n  - name: home\n    hostPath: { path: /home/<workspace_id>, type: DirectoryOrCreate }\n")

	t.Run("missing", func(t *testing.T) {
		env := tcAuth05CarrierBaseEnv(t, template, filepath.Join(t.TempDir(), "state"), t.TempDir(), nodes)
		out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
		tcAuth05Require(t, code == 2, "prepare exited %d, want blocked:\n%s", code, out)
		tcAuth05Require(t, strings.Contains(out, "WORKSPACE_ID is required"), "refusal did not name WORKSPACE_ID:\n%s", out)
		tcAuth05Require(t, strings.Contains(out, "home/<workspace_id>"), "refusal did not name the template partition shape:\n%s", out)
	})

	t.Run("not_one_path_segment", func(t *testing.T) {
		env := tcAuth05CarrierBaseEnv(t, template, filepath.Join(t.TempDir(), "state"), t.TempDir(), nodes)
		env["WORKSPACE_ID"] = "../escape"
		out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
		tcAuth05Require(t, code == 2, "prepare exited %d, want blocked:\n%s", code, out)
		tcAuth05Require(t, strings.Contains(out, "not a single path segment"), "unsafe id refusal missing:\n%s", out)
	})
}

func TestAuth05ClusterCarrierRestoreWithoutWindowKeepsStateDir(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	script := tcAuth05CarrierPath(t)
	bin := tcAuth05FakeTools(t)
	stateDir := t.TempDir()
	sentinel := filepath.Join(stateDir, "sentinel")
	tcAuth05WriteFile(t, sentinel, "keep")
	env := map[string]string{"STATE_DIR": stateDir}

	out, code := tcAuth05RunCarrier(t, script, bin, env, "restore")
	tcAuth05Require(t, code == 2, "restore exited %d, want blocked:\n%s", code, out)
	tcAuth05Require(t, strings.Count(out, "QA_BLOCKED:") == 1, "restore reported more than one blocker:\n%s", out)
	tcAuth05Require(t, strings.Contains(out, "no prepared window"), "restore did not name the missing window:\n%s", out)
	tcAuth05Require(t, !strings.Contains(out, "state file is kept"), "restore contradicted the missing window:\n%s", out)
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("restore removed the state dir without a window: %v", err)
	}
}
