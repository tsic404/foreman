// TC-tech-cache-02's live carrier under a fake kubectl/curl/date: the window is
// constructed by the pin (both tasks on one node) and the case's own criterion
// is 任务全部成功. The cases pin the verdicts:
//
//   - a task that settled failed after the window held ⇒ FAIL, carrying the
//     failed task's projection, the Job pod's container exit code and the pod
//     log (the daemon's refusal line), and never "unconstructible"/blocked;
//   - tasks on different nodes, or windows that do not overlap ⇒ blocked (the
//     case's precondition was not constructed);
//   - both completed with a clean fsck ⇒ PASS.
//
// The refused daemon is the product's shared-profile exclusion (ADR-059 裁定
// A): the carrier exists to make that observable, not to hide it as a block.
package acceptance

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func tcCache02CarrierPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the carrier from the test source")
	}
	return filepath.Join(filepath.Dir(file), "tech-cache-02-cluster.sh")
}

func tcCache02WriteExecutable(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// tcCache02FakeBin writes the commands the carrier calls. The debug shell runs
// against a per-node directory standing in for the node filesystem.
func tcCache02FakeBin(t *testing.T) string {
	t.Helper()
	bin := t.TempDir()
	tcCache02WriteExecutable(t, filepath.Join(bin, "kubectl"), `#!/bin/sh
set -u
case " $* " in
*" get pods "*)
	id=''
	for a in "$@"; do
		case "$a" in
		*foreman.tsic.top/task-id=*) id=${a##*task-id=} ;;
		esac
	done
	[ -n "$id" ] || { echo "fake kubectl: no task id in: $*" >&2; exit 99; }
	cat "$FAKE_PODS/$id.json" 2>/dev/null || echo '{"items":[]}'
	exit 0 ;;
*" logs deploy/foreman "*)
	cat "${FAKE_FOREMAN_LOG}"
	exit 0 ;;
*" logs "*)
	cat "${FAKE_POD_LOG}"
	exit 0 ;;
*" debug "*)
	node=''
	for a in "$@"; do
		case "$a" in
		node/*) node=${a#node/} ;;
		esac
	done
	[ -n "$node" ] || { echo "fake kubectl: no node in: $*" >&2; exit 99; }
	cmd=''
	while [ $# -gt 0 ]; do
		case $1 in
		-c)
			[ $# -ge 2 ] || { echo "fake kubectl: -c without a command: $*" >&2; exit 99; }
			cmd=$2
			shift 2
			break
			;;
		esac
		shift
	done
	[ -n "$cmd" ] || { echo "fake kubectl: no -c in: $*" >&2; exit 99; }
	root="$FAKE_STATE/$node/host"
	cmd=$(printf '%s' "$cmd" | sed "s#/host#$root#g")
	exec sh -c "$cmd" "$@" ;;
*)
	echo "fake kubectl: unexpected call: $*" >&2
	exit 99 ;;
esac
`)
	tcCache02WriteExecutable(t, filepath.Join(bin, "curl"), `#!/bin/sh
set -u
for a; do url=$a; done
id=${url##*/}
cat "$FAKE_TASKS/$id.json"
`)
	tcCache02WriteExecutable(t, filepath.Join(bin, "date"), `#!/bin/sh
printf '%s\n' "${FAKE_NOW:-1000}"
`)
	return bin
}

// tcCache02FakeCLI stands in for the fixture's multica wrapper: the carrier
// asks it for the failed task's server-side run, and an empty list is a clean
// "no record" answer.
func tcCache02FakeCLI(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "multica-test")
	tcCache02WriteExecutable(t, path, "#!/bin/sh\nprintf '[]\\n'\n")
	return path
}

func tcCache02Env(bin string, overrides map[string]string) []string {
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

// tcCache02Run drives the carrier and returns its combined output and exit
// code. WAIT_SECONDS is zero so a case that never settles blocks immediately
// instead of sleeping through the budget.
func tcCache02Run(t *testing.T, bin, state string, overrides map[string]string) (string, int) {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	env := map[string]string{
		"TASK1":                  "t1",
		"TASK2":                  "t2",
		"FOREMAN_URL":            "http://foreman",
		"FOREMAN_OPS_TOKEN":      "test-ops-token",
		"FOREMAN_JOB_NAMESPACE":  "multica-agents",
		"FOREMAN_STATE_ROOT":     "/var/lib/foreman",
		"NODE_DEBUG_IMAGE":       "fake",
		"FAKE_STATE":             state,
		"FAKE_TASKS":             filepath.Join(state, "tasks"),
		"FAKE_PODS":              filepath.Join(state, "pods"),
		"FAKE_FOREMAN_LOG":       filepath.Join(state, "foreman.log"),
		"FAKE_POD_LOG":           filepath.Join(state, "pod.log"),
		"FAKE_NOW":               "1000",
		"WAIT_SECONDS":           "0",
		"POLL_SECONDS":           "0",
		"FOREMAN_CACHE_EVIDENCE": filepath.Join(state, "evidence"),
		"FOREMAN_MULTICA_TEST":   tcCache02FakeCLI(t, state),
	}
	for key, value := range overrides {
		env[key] = value
	}
	cmd := exec.Command("sh", tcCache02CarrierPath(t))
	cmd.Env = tcCache02Env(bin, env)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return string(out), exitErr.ExitCode()
	}
	t.Fatalf("run carrier: %v", err)
	return "", 0
}

// tcCache02State writes the projections and the Foreman log: both tasks
// terminal on node-a, TASK1 started at 10:00:00 and settled at 10:00:20,
// TASK2's Job created at 10:00:05 and settled at 10:00:07. The values a case
// moves are the results and, for the failed one, the begin fact.
func tcCache02State(t *testing.T, task2 string) string {
	t.Helper()
	state := t.TempDir()
	for _, dir := range []string{"tasks", "pods"} {
		if err := os.MkdirAll(filepath.Join(state, dir), 0o755); err != nil {
			t.Fatalf("create %s: %v", dir, err)
		}
	}
	task1 := `{"task_id":"t1","agent_id":"a1","state":"terminal","result":"completed",` +
		`"node_name":"node-a","started_at":"2026-01-01T10:00:00Z",` +
		`"job_created_at":"2026-01-01T09:59:58Z"}`
	if err := os.WriteFile(filepath.Join(state, "tasks", "t1.json"), []byte(task1), 0o644); err != nil {
		t.Fatalf("write t1: %v", err)
	}
	if err := os.WriteFile(filepath.Join(state, "tasks", "t2.json"), []byte(task2), 0o644); err != nil {
		t.Fatalf("write t2: %v", err)
	}
	log := `{"msg":"task.terminal","task_id":"t1","ts":"2026-01-01T10:00:20Z"}` + "\n" +
		`{"msg":"task.terminal","task_id":"t2","ts":"2026-01-01T10:00:07Z"}` + "\n"
	if err := os.WriteFile(filepath.Join(state, "foreman.log"), []byte(log), 0o644); err != nil {
		t.Fatalf("write the Foreman log: %v", err)
	}
	if err := os.WriteFile(filepath.Join(state, "pod.log"),
		[]byte("daemon profile \"foreman-job\" is already running or starting\n"), 0o644); err != nil {
		t.Fatalf("write the pod log: %v", err)
	}
	return state
}

// tcCache02RefusedPod is the second Job's pod: its agent container ended with
// exit 1, which is what the refused daemon leaves behind.
const tcCache02RefusedPod = `{"items":[{"metadata":{"name":"fm-t2-abcde"},` +
	`"spec":{"containers":[{"name":"agent"}]},` +
	`"status":{"startTime":"2026-01-01T10:00:05Z","containerStatuses":[` +
	`{"name":"agent","state":{"terminated":{"exitCode":1,"reason":"Error"}}}]}}]}`

func TestCache02CarrierFailsWhenTheSecondJobIsRefused(t *testing.T) {
	bin := tcCache02FakeBin(t)
	task2 := `{"task_id":"t2","agent_id":"a1","state":"terminal","result":"failed",` +
		`"failure_reason":"job_failed","node_name":"node-a","started_at":"",` +
		`"job_created_at":"2026-01-01T10:00:05Z"}`
	state := tcCache02State(t, task2)
	if err := os.WriteFile(filepath.Join(state, "pods", "t2.json"), []byte(tcCache02RefusedPod), 0o644); err != nil {
		t.Fatalf("write the pod: %v", err)
	}
	// A failure Foreman judged itself is logged as task.failed_compensated,
	// not task.terminal: the refused daemon's Job takes that path, and the
	// carrier must read the same terminal instant from it.
	log := `{"msg":"task.terminal","task_id":"t1","ts":"2026-01-01T10:00:20Z"}` + "\n" +
		`{"msg":"task.failed_compensated","task_id":"t2","ts":"2026-01-01T10:00:07Z","failure_reason":"job_failed"}` + "\n"
	if err := os.WriteFile(filepath.Join(state, "foreman.log"), []byte(log), 0o644); err != nil {
		t.Fatalf("write the compensated terminal line: %v", err)
	}

	out, code := tcCache02Run(t, bin, state, nil)
	if code != 1 {
		t.Fatalf("carrier exit = %d, want 1 (a decidable FAIL):\n%s", code, out)
	}
	for _, want := range []string{
		"FAIL:",
		"任务全部成功",
		"TASK2=failed",
		`daemon profile "foreman-job" is already running or starting`,
		"container=agent exit=1",
		`"result":"failed"`,
		`"failure_reason":"job_failed"`,
		"server_run:",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the FAIL does not carry %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "QA_BLOCKED") {
		t.Errorf("a constructed window and a failed task were reported as blocked:\n%s", out)
	}
}

func TestCache02CarrierBlocksWhenTheTasksDidNotShareANode(t *testing.T) {
	bin := tcCache02FakeBin(t)
	task2 := `{"task_id":"t2","agent_id":"a1","state":"terminal","result":"failed",` +
		`"failure_reason":"job_failed","node_name":"node-b","started_at":"",` +
		`"job_created_at":"2026-01-01T10:00:05Z"}`
	state := tcCache02State(t, task2)

	out, code := tcCache02Run(t, bin, state, nil)
	if code != 2 {
		t.Fatalf("carrier exit = %d, want 2 (precondition not constructed):\n%s", code, out)
	}
	if !strings.Contains(out, "did not share a node") {
		t.Errorf("the block does not name the missing precondition:\n%s", out)
	}
}

func TestCache02CarrierBlocksWhenTheWindowsDidNotOverlap(t *testing.T) {
	bin := tcCache02FakeBin(t)
	// TASK2's Job was created after TASK1 had already settled: the two were
	// never in flight together, so a failure would not be this case's window.
	task2 := `{"task_id":"t2","agent_id":"a1","state":"terminal","result":"failed",` +
		`"failure_reason":"job_failed","node_name":"node-a","started_at":"",` +
		`"job_created_at":"2026-01-01T10:00:30Z"}`
	state := tcCache02State(t, task2)

	out, code := tcCache02Run(t, bin, state, nil)
	if code != 2 {
		t.Fatalf("carrier exit = %d, want 2 (window never constructed):\n%s", code, out)
	}
	if !strings.Contains(out, "not in flight together") {
		t.Errorf("the block does not name the missing window:\n%s", out)
	}
}

func TestCache02CarrierPassesWhenBothCompletedAndTheCacheIsClean(t *testing.T) {
	bin := tcCache02FakeBin(t)
	task2 := `{"task_id":"t2","agent_id":"a1","state":"terminal","result":"completed",` +
		`"node_name":"node-a","started_at":"2026-01-01T10:00:05Z",` +
		`"job_created_at":"2026-01-01T10:00:05Z"}`
	state := tcCache02State(t, task2)
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is required to build the fake bare cache")
	}
	repo := filepath.Join(state, "node-a", "host", "var", "lib", "foreman",
		"workspaces", ".repos", "ws", "repo.git")
	if err := os.MkdirAll(filepath.Dir(repo), 0o755); err != nil {
		t.Fatalf("create the cache tree: %v", err)
	}
	if out, err := exec.Command("git", "init", "-q", "--bare", repo).CombinedOutput(); err != nil {
		t.Fatalf("git init --bare: %v\n%s", err, out)
	}

	out, code := tcCache02Run(t, bin, state, nil)
	if code != 0 {
		t.Fatalf("carrier exit = %d, want 0:\n%s", code, out)
	}
	if !strings.Contains(out, "PASS: TC-tech-cache-02") {
		t.Errorf("the carrier does not report a pass:\n%s", out)
	}
}

// The refused pod can vanish between two looks: the first capture may hold the
// pod while its container has not written anything, the next one the exit code
// and the refusal. No capture may overwrite another, or the evidence the FAIL
// exists for would be lost to whichever look happened to be last.
func TestCache02CarrierUnionsPodCapturesAcrossPolls(t *testing.T) {
	bin := tcCache02FakeBin(t)
	task2 := `{"task_id":"t2","agent_id":"a1","state":"terminal","result":"failed",` +
		`"failure_reason":"job_failed","node_name":"node-a","started_at":"",` +
		`"job_created_at":"2026-01-01T10:00:05Z"}`
	state := tcCache02State(t, task2)
	evidence := filepath.Join(state, "evidence")
	if err := os.MkdirAll(evidence, 0o755); err != nil {
		t.Fatalf("create the evidence dir: %v", err)
	}
	first := "pod=fm-t2-abcde\ncontainer=agent exit= reason=\nlog agent:\n"
	second := "pod=fm-t2-abcde\ncontainer=agent exit=1 reason=Error\nlog agent:\n" +
		"daemon profile \"foreman-job\" is already running or starting\n"
	if err := os.WriteFile(filepath.Join(evidence, "t2.pod.0000000001"), []byte(first), 0o644); err != nil {
		t.Fatalf("write the first capture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(evidence, "t2.pod.0000000002"), []byte(second), 0o644); err != nil {
		t.Fatalf("write the second capture: %v", err)
	}
	// The pod is gone by the time the carrier judges, so no new capture is made.
	if err := os.WriteFile(filepath.Join(state, "pods", "t2.json"), []byte(`{"items":[]}`), 0o644); err != nil {
		t.Fatalf("write the empty pod list: %v", err)
	}

	out, code := tcCache02Run(t, bin, state, nil)
	if code != 1 {
		t.Fatalf("carrier exit = %d, want 1:\n%s", code, out)
	}
	for _, want := range []string{
		"container=agent exit=1",
		`daemon profile "foreman-job" is already running or starting`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the FAIL lost %q from an earlier capture:\n%s", want, out)
		}
	}
}
