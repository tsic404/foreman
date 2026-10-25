// TC-tech-auth-05's cluster carrier, driven under a fake kubectl/curl so the
// workspace-partition path has executable assertions in the repo: the parts
// that are pure string work (template hostPath parsing, <workspace_id>
// expansion, the state file round trip into restore) would otherwise only be
// exercised by the next manual cluster window.
package acceptance

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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

// tcAuth05FakeTools writes the operator-side commands the carrier calls.
// kubectl creates a debugging pod per "debug" (running the node-side script it
// is handed), then answers the pod's phase, log and deletion; its attach
// stream carries only the informational line unless FAKE_ATTACH_DROP_LAST
// asks for the truncated tail a fast container outruns. "get" also answers
// "nodes", "job" and "pods" from JSON files, "pods" from a sequence of reads
// when FAKE_PODS_SEQ names one, and "exec" from FAKE_EXEC after
// FAKE_EXEC_FAILS failures ("container not found", what a recycled container
// returns). curl answers one workspace-scoped task read, and chown stands in
// for the root-only ownership writes the node scripts make in production.
func tcAuth05FakeTools(t *testing.T) string {
	t.Helper()
	bin := t.TempDir()
	kubectl := `#!/bin/sh
set -eu
debug_dir=${FAKE_DEBUG_DIR:-$(dirname "$0")/debug}
mkdir -p "$debug_dir"
pod_arg() {
	prev=""
	for a in "$@"; do
		case $a in
		pod/*) printf '%s\n' "${a#pod/}"; return 0 ;;
		esac
		if [ "$prev" = "pod" ]; then printf '%s\n' "$a"; return 0; fi
		prev=$a
	done
}
cmd=""
for a in "$@"; do
	case $a in
	get|debug|logs|delete|exec|config) cmd=$a; break ;;
	esac
done
case $cmd in
config)
	printf '%s' "${FAKE_KUBE_NS:-}"
	;;
get)
	for a in "$@"; do
		case $a in
		nodes) cat "$FAKE_NODES"; exit 0 ;;
		job) cat "$FAKE_JOB"; exit 0 ;;
		pods)
			if [ -n "${FAKE_PODS_SEQ:-}" ]; then
				n=$(ls "$FAKE_PODS_SEQ" | grep -c '^[0-9][0-9]*\.json$' || true)
				i=$(cat "$FAKE_PODS_SEQ/counter" 2>/dev/null || echo 0)
				i=$((i + 1))
				[ "$i" -le "$n" ] || i=$n
				printf '%s\n' "$i" >"$FAKE_PODS_SEQ/counter"
				cat "$FAKE_PODS_SEQ/$(printf '%02d.json' "$i")"
				exit 0
			fi
			cat "$FAKE_PODS"; exit 0
			;;
		pod)
			name=$(pod_arg "$@")
			case " $* " in
			*status.phase*) cut -d' ' -f1 "$debug_dir/$name.state" ;;
			*) cut -d' ' -f2 "$debug_dir/$name.state" ;;
			esac
			exit 0
			;;
		esac
	done
	echo "fake kubectl: unhandled get: $*" >&2
	exit 1
	;;
debug)
	printf '%s\n' "$*" >>"$debug_dir/debug_calls"
	for last; do :; done
	node=""
	for a in "$@"; do
		case $a in node/*) node=${a#node/} ;; esac
	done
	c=$(cat "$debug_dir/counter" 2>/dev/null || echo 0)
	c=$((c + 1))
	printf '%s' "$c" >"$debug_dir/counter"
	name="node-debugger-$node-$c"
	rc=0
	out=$(sh -c "$last" 2>&1) || rc=$?
	printf '%s\n' "$out" >"$debug_dir/$name.log"
	if [ "$rc" -eq 0 ]; then
		printf 'Succeeded 0\n' >"$debug_dir/$name.state"
	else
		printf 'Failed %s\n' "$rc" >"$debug_dir/$name.state"
	fi
	if [ "${FAKE_DEBUG_HANG:-}" = "1" ] && [ "$c" -le "${FAKE_DEBUG_HANG_CALLS:-1}" ]; then
		printf 'Running 0\n' >"$debug_dir/$name.state"
		printf '%s\n' "$out" | sed '$d' >"$debug_dir/$name.log"
	fi
	echo "Creating debugging pod $name with container debugger on node $node."
	if [ "${FAKE_ATTACH_DROP_LAST:-}" = "1" ]; then
		printf '%s\n' "$out" | sed '$d'
	else
		printf '%s\n' "$out"
	fi
	exit 0
	;;
logs)
	name=$(pod_arg "$@")
	n=$(cat "$debug_dir/log_reads" 2>/dev/null || echo 0)
	n=$((n + 1))
	printf '%s' "$n" >"$debug_dir/log_reads"
	if [ -n "${FAKE_LOGS_FAIL_CALLS:-}" ] && [ "$n" -le "$FAKE_LOGS_FAIL_CALLS" ]; then
		echo "error: the log stream is unavailable" >&2
		exit 1
	fi
	cat "$debug_dir/$name.log"
	;;
delete)
	name=$(pod_arg "$@")
	[ "${FAKE_KEEP_POD_LOGS:-}" = "1" ] || rm -f "$debug_dir/$name.log"
	rm -f "$debug_dir/$name.state"
	;;
exec)
	if [ -n "${FAKE_EXEC_FAILS:-}" ]; then
		n=$(cat "$debug_dir/execs" 2>/dev/null || echo 0)
		n=$((n + 1))
		printf '%s' "$n" >"$debug_dir/execs"
		if [ "$n" -le "$FAKE_EXEC_FAILS" ]; then
			echo 'error: container not found ("agent")' >&2
			exit 1
		fi
	fi
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
	// The node scripts run as root inside the debugger pod in production, so
	// their ownership writes are outside the harness's rights. The shim runs
	// in their place: it records the requested owner instead of applying it,
	// which keeps the suite independent of the runner's chown rights and lets
	// the case assert what owner the window ended with. FAKE_CHOWN_FAIL makes
	// the write fail the way an unprivileged runner sees it, so the abort
	// path that a swallowed `chown ... || true` would hide stays covered.
	chown := `#!/bin/sh
set -eu
debug_dir=${FAKE_DEBUG_DIR:-$(dirname "$0")/debug}
mkdir -p "$debug_dir"
printf '%s\n' "$*" >>"$debug_dir/chown_calls"
if [ "${FAKE_CHOWN_FAIL:-}" = "1" ]; then
	echo "chown: changing ownership: Operation not permitted" >&2
	exit 1
fi
`
	for name, body := range map[string]string{"kubectl": kubectl, "curl": curl, "chown": chown} {
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

// tcAuth05WriteJob writes the reference template's Job document — the window
// shape `verify` reads — with its home hostPath pointed at the partition under
// test.
func tcAuth05WriteJob(t *testing.T, path, home string) {
	t.Helper()
	job := decodeJob(t, []byte(publishedTemplateContent(t)))
	for i := range job.Spec.Template.Spec.Volumes {
		if job.Spec.Template.Spec.Volumes[i].Name == "home" {
			job.Spec.Template.Spec.Volumes[i].HostPath.Path = home
		}
	}
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal the Job fixture: %v", err)
	}
	tcAuth05WriteFile(t, path, string(raw))
}

// tcAuth05PodsJSON renders one pod read: prepare's exit code and the agent
// container's state (running / terminated / anything else reads as waiting).
func tcAuth05PodsJSON(prepareExit int, agentState string) string {
	state := `{"waiting":{"reason":"ContainerCreating"}}`
	switch agentState {
	case "running":
		state = `{"running":{"startedAt":"2026-01-01T00:00:01Z"}}`
	case "terminated":
		state = `{"terminated":{"exitCode":0,"finishedAt":"2026-01-01T00:00:04Z"}}`
	}
	return fmt.Sprintf(`{"items":[{"metadata":{"name":"pod-1","creationTimestamp":"2026-01-01T00:00:00Z"},"spec":{"nodeName":"node-1"},"status":{"initContainerStatuses":[{"name":"prepare","state":{"terminated":{"exitCode":%d}}}],"containerStatuses":[{"name":"agent","state":%s}]}}]}`, prepareExit, state)
}

func tcAuth05WritePods(t *testing.T, path string, prepareExit int, agentRunning bool) {
	t.Helper()
	state := "waiting"
	if agentRunning {
		state = "running"
	}
	tcAuth05WriteFile(t, path, tcAuth05PodsJSON(prepareExit, state))
}

// tcAuth05HealPartition applies what the Job's prepare init container does to
// the two shapes: the profile position becomes a directory again and the
// marker path disappears. `verify` reads the healed tree from the node, so a
// window whose pod actually ran must heal it before the read.
func tcAuth05HealPartition(t *testing.T, partition string) {
	t.Helper()
	profile := filepath.Join(partition, ".multica", "profiles", "foreman-job")
	if err := os.RemoveAll(profile); err != nil {
		t.Fatalf("heal the profile position: %v", err)
	}
	if err := os.MkdirAll(profile, 0o755); err != nil {
		t.Fatalf("heal the profile position: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(partition, ".multica", "daemon_task_context.json")); err != nil {
		t.Fatalf("heal the marker position: %v", err)
	}
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
	// volume is the prepared partition, and restores the tree. The pod that
	// ran installed the window had prepare heal both shapes first, so the node
	// tree is healed before the judgement — `verify` reads the healed tree
	// from the node itself.
	tcAuth05HealPartition(t, partition)
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

// tcAuth05TemplateHomeNodePath renders the reference template's home volume
// hostPath for one workspace: the node-side partition the carrier will select
// when it reads the same template.
func tcAuth05TemplateHomeNodePath(t *testing.T, workspace string) string {
	t.Helper()
	job := decodeJob(t, []byte(publishedTemplateContent(t)))
	volume := volumeByName(t, job.Spec.Template.Spec.Volumes, "home")
	if volume.HostPath == nil {
		t.Fatal("the reference template's home volume carries no hostPath")
	}
	return strings.ReplaceAll(volume.HostPath.Path, "<workspace_id>", workspace)
}

// tcAuth05VerifiableWindow prepares a window and the fixtures `verify` needs:
// the state files prepare wrote, the reference template's Job mounting that
// partition, a running agent container and the exec observation the mounted
// credential file yields. The tree is healed first — the pod that ran the
// window had prepare heal both shapes before its agent container started.
func tcAuth05VerifiableWindow(t *testing.T) (script, bin string, env map[string]string, stateDir, partition string) {
	t.Helper()
	script = tcAuth05CarrierPath(t)
	bin = tcAuth05FakeTools(t)
	hostRoot := t.TempDir()
	stateDir = filepath.Join(t.TempDir(), "state")
	workspace := "ws-verify"
	nodeHome := tcAuth05TemplateHomeNodePath(t, workspace)
	partition = filepath.Join(hostRoot, strings.TrimPrefix(nodeHome, "/"))
	tcAuth05SeedPartition(t, partition, "verify")
	nodes := filepath.Join(t.TempDir(), "nodes.json")
	tcAuth05WriteFile(t, nodes, `{"items":[{"metadata":{"name":"node-1"},"spec":{}}]}`)
	// No FOREMAN_JOB_TEMPLATE: the carrier's default is the published
	// reference template next to this package.
	env = map[string]string{
		"STATE_DIR":      stateDir,
		"NODE_HOST_ROOT": hostRoot,
		"FAKE_NODES":     nodes,
		"WORKSPACE_ID":   workspace,
	}
	out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
	tcAuth05Require(t, code == 0, "prepare exited %d:\n%s", code, out)
	tcAuth05HealPartition(t, partition)

	jobPath := filepath.Join(t.TempDir(), "job.json")
	tcAuth05WriteJob(t, jobPath, nodeHome)
	podsPath := filepath.Join(t.TempDir(), "pods.json")
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
	return script, bin, env, stateDir, partition
}

// tcAuth05WritePodSequence writes the pod reads the carrier will see in order;
// the last read repeats once the sequence runs out.
func tcAuth05WritePodSequence(t *testing.T, dir string, reads ...string) string {
	t.Helper()
	for i, body := range reads {
		tcAuth05WriteFile(t, filepath.Join(dir, fmt.Sprintf("%02d.json", i+1)), body)
	}
	return dir
}

// The node-side command's output must come from the debugger pod's log: a
// command that exits in milliseconds outruns kubectl's attach upgrade, whose
// stream then loses the tail — including the state marker the install reads
// back.
func TestAuth05ClusterCarrierReadsStateMarkerFromTheDebuggerLog(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	script := tcAuth05CarrierPath(t)
	bin := tcAuth05FakeTools(t)
	hostRoot := t.TempDir()
	stateDir := filepath.Join(t.TempDir(), "state")
	workspace := "ws-marker"
	nodeHome := tcAuth05TemplateHomeNodePath(t, workspace)
	partition := filepath.Join(hostRoot, strings.TrimPrefix(nodeHome, "/"))
	tcAuth05SeedPartition(t, partition, "marker")
	nodes := filepath.Join(t.TempDir(), "nodes.json")
	tcAuth05WriteFile(t, nodes, `{"items":[{"metadata":{"name":"node-1"},"spec":{}}]}`)
	env := map[string]string{
		"STATE_DIR":             stateDir,
		"NODE_HOST_ROOT":        hostRoot,
		"FAKE_NODES":            nodes,
		"WORKSPACE_ID":          workspace,
		"FAKE_ATTACH_DROP_LAST": "1",
	}

	out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
	tcAuth05Require(t, code == 0, "prepare exited %d while the attach stream lost its tail:\n%s", code, out)
	tcAuth05Require(t, strings.Contains(out, "state=1|"), "the state marker did not survive the lost attach tail:\n%s", out)
	tcAuth05Require(t, tcAuth05MustReadTrimmed(t, filepath.Join(stateDir, "home")) == nodeHome,
		"the marker read back from the debugger log did not reach the state file")
}

// A reference-template task can finish its agent container in seconds; when it
// lands before an exec does, the window must still reach a verdict from the
// evidence that outlives the container — the rendered Job's credential mount
// and the healed tree on the node — instead of reporting a blocked window.
func TestAuth05ClusterCarrierJudgesAfterTheAgentContainerIsRecycled(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}

	t.Run("recycled", func(t *testing.T) {
		script, bin, env, stateDir, partition := tcAuth05VerifiableWindow(t)
		env["FAKE_PODS_SEQ"] = tcAuth05WritePodSequence(t, t.TempDir(),
			tcAuth05PodsJSON(0, "running"), tcAuth05PodsJSON(0, "terminated"))
		env["FAKE_EXEC_FAILS"] = "99"
		env["WAIT"] = "10"

		out, code := tcAuth05RunCarrier(t, script, bin, env, "verify", "task-1")
		tcAuth05Require(t, code == 0, "verify exited %d, want a verdict with the container gone:\n%s", code, out)
		tcAuth05Require(t, strings.Contains(out, "PASS: TC-tech-auth-05"), "verify did not PASS:\n%s", out)
		tcAuth05Require(t, strings.Contains(out, "ended before an exec landed"),
			"the verdict did not name the evidence it rested on:\n%s", out)
		tcAuth05Require(t, strings.Contains(out, "carries the read-only 0400 credential mount"),
			"the PASS did not name the rendered credential mount:\n%s", out)
		profile := filepath.Join(partition, ".multica", "profiles", "foreman-job")
		marker := filepath.Join(partition, ".multica", "daemon_task_context.json")
		tcAuth05Require(t, tcAuth05MustRead(t, filepath.Join(profile, "keep")) == "verify profile", "profile not restored")
		tcAuth05Require(t, tcAuth05MustRead(t, marker) == "verify marker", "marker not restored")
		if _, err := os.Stat(stateDir); !os.IsNotExist(err) {
			t.Fatalf("state dir survived a passing verify: %v", err)
		}
	})

	t.Run("unhealed_marker", func(t *testing.T) {
		script, bin, env, _, partition := tcAuth05VerifiableWindow(t)
		// prepare's marker rm did not take: the directory shaped obstacle is
		// still on the node when the tree is read.
		if err := os.MkdirAll(filepath.Join(partition, ".multica", "daemon_task_context.json"), 0o755); err != nil {
			t.Fatalf("recreate the marker obstacle: %v", err)
		}
		env["FAKE_PODS_SEQ"] = tcAuth05WritePodSequence(t, t.TempDir(),
			tcAuth05PodsJSON(0, "running"), tcAuth05PodsJSON(0, "terminated"))
		env["FAKE_EXEC_FAILS"] = "99"
		env["WAIT"] = "10"

		out, code := tcAuth05RunCarrier(t, script, bin, env, "verify", "task-1")
		tcAuth05Require(t, code == 1, "verify exited %d, want FAIL for the surviving marker:\n%s", code, out)
		tcAuth05Require(t, strings.Contains(out, "the marker path still exists on node-1"), "the surviving marker was not named:\n%s", out)
	})

	t.Run("exec_fails_while_running", func(t *testing.T) {
		script, bin, env, _, _ := tcAuth05VerifiableWindow(t)
		env["FAKE_EXEC_FAILS"] = "99"
		env["WAIT"] = "2"

		out, code := tcAuth05RunCarrier(t, script, bin, env, "verify", "task-1")
		tcAuth05Require(t, code == 2, "verify exited %d, want blocked while the container is up:\n%s", code, out)
		tcAuth05Require(t, strings.Contains(out, "while its agent container is running"), "the failed exec was not named:\n%s", out)
	})

	t.Run("wrong_mode", func(t *testing.T) {
		script, bin, env, _, _ := tcAuth05VerifiableWindow(t)
		execPath := filepath.Join(t.TempDir(), "exec-wrong-mode.txt")
		tcAuth05WriteFile(t, execPath, "300 1000:1000 /home/agent/.multica/profiles/foreman-job/config.json\nprofile=dir\nmarker=absent\n")
		env["FAKE_EXEC"] = execPath

		out, code := tcAuth05RunCarrier(t, script, bin, env, "verify", "task-1")
		tcAuth05Require(t, code == 1, "verify exited %d, want FAIL for the wrong credential mode:\n%s", code, out)
		tcAuth05Require(t, strings.Contains(out, "want 400 1000:1000"), "the wrong mode was not named:\n%s", out)
	})
}

// A node command can rewrite the node and still fail to report — the pod
// outlives NODE_WAIT or its log is lost. The node is then already dirty, so
// the carrier must register it before the command runs and restore it even
// when the install cannot name what it did.
func TestAuth05ClusterCarrierRestoresNodeWhenTheDebuggerCommandNeverReports(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	script := tcAuth05CarrierPath(t)
	bin := tcAuth05FakeTools(t)
	hostRoot := t.TempDir()
	stateDir := filepath.Join(t.TempDir(), "state")
	workspace := "ws-timeout"
	nodeHome := tcAuth05TemplateHomeNodePath(t, workspace)
	partition := filepath.Join(hostRoot, strings.TrimPrefix(nodeHome, "/"))
	tcAuth05SeedPartition(t, partition, "timeout")
	nodes := filepath.Join(t.TempDir(), "nodes.json")
	tcAuth05WriteFile(t, nodes, `{"items":[{"metadata":{"name":"node-1"},"spec":{}}]}`)
	env := map[string]string{
		"STATE_DIR":             stateDir,
		"NODE_HOST_ROOT":        hostRoot,
		"FAKE_NODES":            nodes,
		"WORKSPACE_ID":          workspace,
		"NODE_WAIT":             "1",
		"FAKE_DEBUG_HANG":       "1",
		"FAKE_DEBUG_HANG_CALLS": "1",
	}

	out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
	tcAuth05Require(t, code == 2, "prepare exited %d, want blocked when the install never reports:\n%s", code, out)
	tcAuth05Require(t, strings.Contains(out, "did not finish within 1s"),
		"the unfinished debugger command was not reported:\n%s", out)
	tcAuth05Require(t, strings.Contains(out, "cannot install the anomaly on node-1"),
		"the failed install was not named:\n%s", out)

	profile := filepath.Join(partition, ".multica", "profiles", "foreman-job")
	info, err := os.Stat(profile)
	tcAuth05Require(t, err == nil && info.IsDir(), "the profile position was left dirty: %v", err)
	tcAuth05Require(t, tcAuth05MustRead(t, filepath.Join(profile, "keep")) == "timeout profile",
		"the profile written before the install was not restored")
	marker := filepath.Join(partition, ".multica", "daemon_task_context.json")
	info, err = os.Stat(marker)
	tcAuth05Require(t, err == nil && info.Mode().IsRegular(), "the marker position was left dirty: %v", err)
	tcAuth05Require(t, tcAuth05MustRead(t, marker) == "timeout marker", "the marker was not restored")
	leftovers, _ := filepath.Glob(filepath.Join(partition, ".tc-auth-05-*"))
	tcAuth05Require(t, len(leftovers) == 0, "restore left node-side state behind: %v", leftovers)
	if _, err := os.Stat(stateDir); !os.IsNotExist(err) {
		t.Fatalf("the state dir survived the restore: %v", err)
	}
}

// The debugger pod's namespace is the caller's when named, else the kubeconfig
// context's — a flat `default` would turn every node command into a permission
// blocker for an operator whose context lives elsewhere.
func TestAuth05ClusterCarrierDebuggerNamespaceFollowsTheKubeconfigContext(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	cases := []struct {
		name       string
		kubeNS     string
		overrideNS string
		want       string
	}{
		{"from_the_context", "team-debug", "", "team-debug"},
		{"caller_override_wins", "team-debug", "explicit-ns", "explicit-ns"},
		{"no_context_namespace", "", "", "default"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			script := tcAuth05CarrierPath(t)
			bin := tcAuth05FakeTools(t)
			hostRoot := t.TempDir()
			stateDir := filepath.Join(t.TempDir(), "state")
			workspace := "ws-ns"
			nodeHome := tcAuth05TemplateHomeNodePath(t, workspace)
			partition := filepath.Join(hostRoot, strings.TrimPrefix(nodeHome, "/"))
			tcAuth05SeedPartition(t, partition, "ns")
			nodes := filepath.Join(t.TempDir(), "nodes.json")
			tcAuth05WriteFile(t, nodes, `{"items":[{"metadata":{"name":"node-1"},"spec":{}}]}`)
			env := map[string]string{
				"STATE_DIR":      stateDir,
				"NODE_HOST_ROOT": hostRoot,
				"FAKE_NODES":     nodes,
				"WORKSPACE_ID":   workspace,
				"FAKE_KUBE_NS":   tc.kubeNS,
			}
			if tc.overrideNS != "" {
				env["DEBUG_NS"] = tc.overrideNS
			}

			out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
			tcAuth05Require(t, code == 0, "prepare exited %d:\n%s", code, out)
			calls := tcAuth05MustRead(t, filepath.Join(bin, "debug", "debug_calls"))
			tcAuth05Require(t, strings.Contains(calls, "-n "+tc.want+" debug"),
				"the node debugger pod did not use namespace %q: %s", tc.want, calls)
		})
	}
}

// tcAuth05MutateJobFile applies mutate to the Job document verify reads.
func tcAuth05MutateJobFile(t *testing.T, path string, mutate func(t *testing.T, job *batchv1.Job)) {
	t.Helper()
	var job batchv1.Job
	if err := json.Unmarshal([]byte(tcAuth05MustRead(t, path)), &job); err != nil {
		t.Fatalf("decode the Job fixture: %v", err)
	}
	mutate(t, &job)
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal the mutated Job fixture: %v", err)
	}
	tcAuth05WriteFile(t, path, string(raw))
}

// tcAuth05PrepareArg replaces every occurrence of old in the prepare init
// container's script.
func tcAuth05PrepareArg(t *testing.T, job *batchv1.Job, old, replacement string) {
	t.Helper()
	for i := range job.Spec.Template.Spec.InitContainers {
		prepare := &job.Spec.Template.Spec.InitContainers[i]
		if prepare.Name != "prepare" {
			continue
		}
		if len(prepare.Args) == 0 {
			t.Fatal("the prepare init container carries no script argument")
		}
		prepare.Args[0] = strings.ReplaceAll(prepare.Args[0], old, replacement)
		return
	}
	t.Fatal("the reference Job carries no prepare init container")
}

// Every verify precondition is a refusal of a Job that is not the reference
// template; each branch exists so a healthy window is never judged on a shape
// the case does not cover. A branch that stops firing turns the matching
// subtest red rather than letting the window pass.
func TestAuth05ClusterCarrierRefusesNonReferenceJobShapes(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	cases := []struct {
		name   string
		want   string
		mutate func(t *testing.T, job *batchv1.Job)
	}{
		{
			"no_prepare_container",
			"carries no prepare init container",
			func(t *testing.T, job *batchv1.Job) {
				kept := job.Spec.Template.Spec.InitContainers[:0]
				for _, container := range job.Spec.Template.Spec.InitContainers {
					if container.Name != "prepare" {
						kept = append(kept, container)
					}
				}
				job.Spec.Template.Spec.InitContainers = kept
			},
		},
		{
			"no_three_path_heal",
			"does not carry the three-path heal",
			func(t *testing.T, job *batchv1.Job) {
				tcAuth05PrepareArg(t, job, `[ -d "$d" ] || { rm -f "$d"; mkdir -p "$d"; }`, `[ -d "$d" ] || true`)
			},
		},
		{
			"no_marker_rm",
			"does not carry the marker rm -rf",
			func(t *testing.T, job *batchv1.Job) {
				tcAuth05PrepareArg(t, job, "rm -rf /home/agent/.multica/daemon_task_context.json", "true")
			},
		},
		{
			"no_credential_mode",
			"does not set the credential copy to 0400",
			func(t *testing.T, job *batchv1.Job) {
				tcAuth05PrepareArg(t, job, "chmod 0400 /credout/config.json", "chmod 0644 /credout/config.json")
			},
		},
		{
			"no_credential_owner",
			"does not own the credential copy as 1000:1000",
			func(t *testing.T, job *batchv1.Job) {
				tcAuth05PrepareArg(t, job, "chown -R 1000:1000", "chown -R 0:0")
			},
		},
		{
			"credential_not_the_single_file_mount",
			"does not mount the credential as the single read-only file",
			func(t *testing.T, job *batchv1.Job) {
				for i := range job.Spec.Template.Spec.Containers {
					agent := &job.Spec.Template.Spec.Containers[i]
					if agent.Name != "agent" {
						continue
					}
					for j := range agent.VolumeMounts {
						if agent.VolumeMounts[j].Name == "cred" {
							agent.VolumeMounts[j].MountPath = "/home/agent/.multica/profiles/foreman-job/other.json"
							return
						}
					}
				}
				t.Fatal("the reference Job's agent container mounts no cred volume")
			},
		},
		{
			"credential_volume_not_prepares_emptydir",
			"credential volume is not the emptyDir",
			func(t *testing.T, job *batchv1.Job) {
				for i := range job.Spec.Template.Spec.Volumes {
					volume := &job.Spec.Template.Spec.Volumes[i]
					if volume.Name != "cred" {
						continue
					}
					volume.EmptyDir = nil
					volume.HostPath = &corev1.HostPathVolumeSource{Path: "/tmp/foreman-cred"}
					return
				}
				t.Fatal("the reference Job carries no cred volume")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			script, bin, env, stateDir, _ := tcAuth05VerifiableWindow(t)
			tcAuth05MutateJobFile(t, env["FAKE_JOB"], tc.mutate)

			out, code := tcAuth05RunCarrier(t, script, bin, env, "verify", "task-1")
			tcAuth05Require(t, code == 2, "verify exited %d, want blocked for %s:\n%s", code, tc.name, out)
			tcAuth05Require(t, strings.Contains(out, tc.want), "the refusal did not name %q:\n%s", tc.want, out)
			if _, err := os.Stat(stateDir); err != nil {
				t.Fatalf("a refused window dropped the prepared state: %v", err)
			}
		})
	}
}

// A node that never ran this workspace's task carries neither position yet, so
// the install has no backup to name — the journal is the only node-side record
// that it touched anything. `restore` runs as a separate process with its own
// RUN, so it must take the install's journal name from the state, not derive
// one; without that the node keeps the anomaly file and no record is left.
func TestAuth05ClusterCarrierRestoresPositionsThatWereAbsent(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	script := tcAuth05CarrierPath(t)
	bin := tcAuth05FakeTools(t)
	hostRoot := t.TempDir()
	stateDir := filepath.Join(t.TempDir(), "state")
	workspace := "ws-absent"
	nodeHome := tcAuth05TemplateHomeNodePath(t, workspace)
	partition := filepath.Join(hostRoot, strings.TrimPrefix(nodeHome, "/"))
	if err := os.MkdirAll(filepath.Join(partition, ".multica", "profiles"), 0o755); err != nil {
		t.Fatalf("seed the empty home partition: %v", err)
	}
	nodes := filepath.Join(t.TempDir(), "nodes.json")
	tcAuth05WriteFile(t, nodes, `{"items":[{"metadata":{"name":"node-1"},"spec":{}}]}`)
	env := map[string]string{
		"STATE_DIR":      stateDir,
		"NODE_HOST_ROOT": hostRoot,
		"FAKE_NODES":     nodes,
		"WORKSPACE_ID":   workspace,
	}

	out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
	tcAuth05Require(t, code == 0, "prepare exited %d:\n%s", code, out)
	tcAuth05Require(t, strings.Contains(out, "state=0||0|"),
		"the install did not report both originals absent:\n%s", out)
	profile := filepath.Join(partition, ".multica", "profiles", "foreman-job")
	info, err := os.Stat(profile)
	tcAuth05Require(t, err == nil && info.Mode().IsRegular(), "the profile anomaly was not installed: %v", err)

	out, code = tcAuth05RunCarrier(t, script, bin, env, "restore")
	tcAuth05Require(t, code == 0, "restore exited %d:\n%s", code, out)
	info, err = os.Stat(profile)
	tcAuth05Require(t, err == nil && info.IsDir(), "the profile position was not restored to a directory: %v", err)
	if _, err := os.Stat(filepath.Join(partition, ".multica", "daemon_task_context.json")); !os.IsNotExist(err) {
		t.Fatalf("the marker position survived restore: %v", err)
	}
	leftovers, _ := filepath.Glob(filepath.Join(partition, ".tc-auth-05-*"))
	tcAuth05Require(t, len(leftovers) == 0, "restore left node-side state behind: %v", leftovers)
	// Recreating the absent profile must still hand it to 1000:1000. In
	// production that chown runs as root inside the debugger pod; the shim
	// records the request in its place, so the case holds without the runner
	// being able to chown to another user.
	chowns := tcAuth05MustRead(t, filepath.Join(bin, "debug", "chown_calls"))
	tcAuth05Require(t, strings.TrimSpace(chowns) == "1000:1000 "+profile,
		"the recreated profile owner was %q, want 1000:1000 %s", chowns, profile)
}

// tcAuth05AbsentOriginalsWindow prepares a window on a home partition where
// neither position exists yet: the install finds no backup to name, so the
// node-side journal is the only record that it touched anything.
func tcAuth05AbsentOriginalsWindow(t *testing.T) (script, bin string, env map[string]string, stateDir, partition, profile string) {
	t.Helper()
	script = tcAuth05CarrierPath(t)
	bin = tcAuth05FakeTools(t)
	hostRoot := t.TempDir()
	stateDir = filepath.Join(t.TempDir(), "state")
	workspace := "ws-absent"
	nodeHome := tcAuth05TemplateHomeNodePath(t, workspace)
	partition = filepath.Join(hostRoot, strings.TrimPrefix(nodeHome, "/"))
	if err := os.MkdirAll(filepath.Join(partition, ".multica", "profiles"), 0o755); err != nil {
		t.Fatalf("seed the empty home partition: %v", err)
	}
	nodes := filepath.Join(t.TempDir(), "nodes.json")
	tcAuth05WriteFile(t, nodes, `{"items":[{"metadata":{"name":"node-1"},"spec":{}}]}`)
	env = map[string]string{
		"STATE_DIR":      stateDir,
		"NODE_HOST_ROOT": hostRoot,
		"FAKE_NODES":     nodes,
		"WORKSPACE_ID":   workspace,
	}
	out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
	tcAuth05Require(t, code == 0, "prepare exited %d:\n%s", code, out)
	tcAuth05Require(t, strings.Contains(out, "state=0||0|"),
		"the window did not report both originals absent:\n%s", out)
	profile = filepath.Join(partition, ".multica", "profiles", "foreman-job")
	return script, bin, env, stateDir, partition, profile
}

// The ownership write is the restore's only privileged step. When it fails the
// node script must abort before its success line, the restore must report the
// failed node, and the prepared state must survive for the operator's retry —
// swallowing the failure would turn a half-restored node into a clean verdict.
func TestAuth05ClusterCarrierBlocksWhenTheOwnershipWriteFails(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	script, bin, env, stateDir, _, profile := tcAuth05AbsentOriginalsWindow(t)
	env["FAKE_CHOWN_FAIL"] = "1"

	out, code := tcAuth05RunCarrier(t, script, bin, env, "restore")
	tcAuth05Require(t, code == 2, "restore exited %d, want blocked when the ownership write fails:\n%s", code, out)
	tcAuth05Require(t, strings.Contains(out, "restore failed on: node-1"),
		"the failed node was not named:\n%s", out)
	tcAuth05Require(t, strings.Contains(out, "the tree restore failed — the state file is kept"),
		"the kept state was not named:\n%s", out)
	chowns := tcAuth05MustRead(t, filepath.Join(bin, "debug", "chown_calls"))
	tcAuth05Require(t, strings.TrimSpace(chowns) == "1000:1000 "+profile,
		"the failing ownership write did not target 1000:1000 %s: %q", profile, chowns)
	if _, err := os.Stat(stateDir); err != nil {
		t.Fatalf("the state dir was dropped after a failed restore: %v", err)
	}

	// The kept state is what makes the retry possible; without the failure the
	// same window finishes and removes it.
	delete(env, "FAKE_CHOWN_FAIL")
	out, code = tcAuth05RunCarrier(t, script, bin, env, "restore")
	tcAuth05Require(t, code == 0, "the retry exited %d:\n%s", code, out)
	info, err := os.Stat(profile)
	tcAuth05Require(t, err == nil && info.IsDir(), "the retry did not restore the profile directory: %v", err)
	if _, err := os.Stat(stateDir); !os.IsNotExist(err) {
		t.Fatalf("the state dir survived the successful retry: %v", err)
	}
}

// The install journal is the only record that a window with no originals
// touched the node. Losing it leaves nothing to undo and nothing to prove a
// restore happened; the run must say so instead of reaching the success line.
func TestAuth05ClusterCarrierReportsUntouchedWhenTheInstallRecordIsGone(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	script, bin, env, stateDir, partition, profile := tcAuth05AbsentOriginalsWindow(t)
	journals, _ := filepath.Glob(filepath.Join(partition, ".tc-auth-05-*.journal"))
	tcAuth05Require(t, len(journals) == 1, "the install left %d journals, want 1: %v", len(journals), journals)
	if err := os.Remove(journals[0]); err != nil {
		t.Fatalf("drop the install journal: %v", err)
	}
	env["FAKE_KEEP_POD_LOGS"] = "1"

	out, code := tcAuth05RunCarrier(t, script, bin, env, "restore")
	tcAuth05Require(t, code == 0, "restore exited %d with nothing to undo:\n%s", code, out)
	// A successful restore is quiet by design, so the branch is visible in the
	// node script's own report: keep the pod's log past its deletion
	// (FAKE_KEEP_POD_LOGS) and read it. Before the fix this said "restored"
	// without restoring anything.
	logs, _ := filepath.Glob(filepath.Join(bin, "debug", "*.log"))
	tcAuth05Require(t, len(logs) == 1, "the restore pod's log was not kept: %v", logs)
	nodeReport := tcAuth05MustRead(t, logs[0])
	tcAuth05Require(t, strings.Contains(nodeReport, "the install never touched this node"),
		"restore claimed a restore it could not evidence: %s", nodeReport)
	info, err := os.Stat(profile)
	tcAuth05Require(t, err == nil && info.Mode().IsRegular(), "restore touched the window without a record: %v", err)
	if _, err := os.Stat(stateDir); !os.IsNotExist(err) {
		t.Fatalf("the state dir survived the untouched report: %v", err)
	}
}

// A log read that fails carries the only account of why; deleting the pod with
// it destroys that account. The pod is kept, the window is still undone, and
// the failure names the pod a human can read afterwards.
func TestAuth05ClusterCarrierKeepsTheDebuggerPodWhenItsLogCannotBeRead(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	script := tcAuth05CarrierPath(t)
	bin := tcAuth05FakeTools(t)
	hostRoot := t.TempDir()
	stateDir := filepath.Join(t.TempDir(), "state")
	workspace := "ws-logfail"
	nodeHome := tcAuth05TemplateHomeNodePath(t, workspace)
	partition := filepath.Join(hostRoot, strings.TrimPrefix(nodeHome, "/"))
	tcAuth05SeedPartition(t, partition, "logfail")
	nodes := filepath.Join(t.TempDir(), "nodes.json")
	tcAuth05WriteFile(t, nodes, `{"items":[{"metadata":{"name":"node-1"},"spec":{}}]}`)
	env := map[string]string{
		"STATE_DIR":            stateDir,
		"NODE_HOST_ROOT":       hostRoot,
		"FAKE_NODES":           nodes,
		"WORKSPACE_ID":         workspace,
		"FAKE_LOGS_FAIL_CALLS": "1",
	}

	out, code := tcAuth05RunCarrier(t, script, bin, env, "prepare")
	tcAuth05Require(t, code == 2, "prepare exited %d, want blocked when the log cannot be read:\n%s", code, out)
	tcAuth05Require(t, strings.Contains(out, "the pod is kept for inspection"),
		"the unreadable log did not name the kept pod:\n%s", out)
	if _, err := os.Stat(filepath.Join(bin, "debug", "node-debugger-node-1-1.log")); err != nil {
		t.Fatalf("the debugger pod was deleted with its unreadable log: %v", err)
	}
	profile := filepath.Join(partition, ".multica", "profiles", "foreman-job")
	info, err := os.Stat(profile)
	tcAuth05Require(t, err == nil && info.IsDir(), "the kept pod blocked the restore: %v", err)
	tcAuth05Require(t, tcAuth05MustRead(t, filepath.Join(profile, "keep")) == "logfail profile", "profile not restored")
	if _, err := os.Stat(stateDir); !os.IsNotExist(err) {
		t.Fatalf("the state dir survived the restore: %v", err)
	}
}
