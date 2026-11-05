// TC-tech-cache-03's cluster carrier under a fake kubectl/curl/date: the three
// judged states and the cleanup the carrier wraps around them. A task that met
// the break on its own node fails explicitly (PASS) or silently completed
// (FAIL); everything else — another node, a break installed after the task
// started, an unreadable instant, an unplaced task, an ambiguous discovery —
// blocks instead of deciding, and a blocked judgement names the node the break
// still holds so `restore <node>` can clean it.
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
	"time"
)

const (
	tcCache03ModesJournal  = ".foreman-tc-cache-03-workspaces-modes"
	tcCache03BreakAtMarker = ".foreman-tc-cache-03-break-at"
	// The break instant is read on the node's clock and the start on the
	// server's; a fixed second for each keeps the comparison under test.
	tcCache03BreakAt = int64(1700000100)
)

// tcCache03CarrierPath returns the live carrier next to this test file.
func tcCache03CarrierPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the carrier from the test source")
	}
	return filepath.Join(filepath.Dir(file), "tech-cache-03-cluster.sh")
}

func tcCache03WriteExecutable(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// tcCache03CarrierFakeBin writes the three commands the carrier calls. kubectl
// answers the discovery list and runs a node debug shell against a per-node
// directory standing in for the node filesystem; curl answers the projection of
// one task id; date is the node clock the break instant is read on, so a case
// can place the break before or after a task's start. Every other call site
// errors, so a new one surfaces instead of feeding the carrier an answer it
// never asked for.
func tcCache03CarrierFakeBin(t *testing.T) string {
	t.Helper()
	bin := t.TempDir()
	tcCache03WriteExecutable(t, filepath.Join(bin, "kubectl"), `#!/bin/sh
set -u
state=${FAKE_STATE:?}
case " $* " in
*" get pods "*)
	printf '%s' "${FAKE_NODES:-}"
	exit 0 ;;
*" debug "*)
	node=''
	for a in "$@"; do
		case "$a" in
		node/*) node=${a#node/} ;;
		esac
	done
	[ -n "$node" ] || { echo "fake kubectl: no node in: $*" >&2; exit 99; }
	if [ -n "${FAKE_DEBUG_FAIL_NODE:-}" ] && [ "$FAKE_DEBUG_FAIL_NODE" = "$node" ]; then
		echo "error: unable to create debug pod on $node" >&2
		exit 1
	fi
	if [ -n "${FAKE_DEBUG_EMPTY_NODE:-}" ] && [ "$FAKE_DEBUG_EMPTY_NODE" = "$node" ]; then
		# kubectl debug can exit 0 after a lost attach: the command ran, its
		# answer never came back. That is not evidence of anything.
		exit 0
	fi
	for last; do :; done
	root="$state/$node/host"
	mkdir -p "$root/tmp"
	cmd=$(printf '%s' "$last" | sed "s#/host#$root#g")
	exec sh -c "$cmd" ;;
*)
	echo "fake kubectl: unexpected call: $*" >&2
	exit 99 ;;
esac
`)
	tcCache03WriteExecutable(t, filepath.Join(bin, "curl"), `#!/bin/sh
set -u
for a; do url=$a; done
id=${url##*/}
f="${FAKE_TASKS:?}/$id.json"
[ -f "$f" ] || exit 22
cat "$f"
`)
	tcCache03WriteExecutable(t, filepath.Join(bin, "date"), `#!/bin/sh
printf '%s\n' "${FAKE_NOW:?FAKE_NOW is not set}"
`)
	// The fake node filesystem is a temp dir owned by the test user, so the
	// carrier's privilege drop is a no-op here: the real probe still sees the
	// modes the carrier installs.
	tcCache03WriteExecutable(t, filepath.Join(bin, "setpriv"), `#!/bin/sh
while [ $# -gt 0 ]; do
	case "$1" in
	--reuid=*|--regid=*|--clear-groups) shift ;;
	--reuid|--regid) shift 2 ;;
	*) break ;;
	esac
done
exec "$@"`)
	return bin
}

func tcCache03CarrierEnv(bin string, overrides map[string]string) []string {
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

// tcCache03CarrierRun drives one carrier action with the fake commands on PATH
// and returns its combined output with the exit code the verdict is read from.
func tcCache03CarrierRun(t *testing.T, bin, state string, overrides map[string]string, args ...string) (string, int) {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	env := map[string]string{
		"FOREMAN_URL":                    "http://foreman",
		"FOREMAN_OPS_TOKEN":              opsTestToken,
		"FOREMAN_JOB_NAMESPACE":          "multica-agents",
		"FOREMAN_STATE_ROOT":             "/var/lib/foreman",
		"NODE_DEBUG_IMAGE":               "fake",
		"FAKE_STATE":                     state,
		"FAKE_TASKS":                     filepath.Join(state, "tasks"),
		"FOREMAN_TC_CACHE_03_BROKEN_LOG": filepath.Join(state, "broken-nodes"),
		"FAKE_NOW":                       fmt.Sprint(tcCache03BreakAt),
	}
	for key, value := range overrides {
		env[key] = value
	}
	cmd := exec.Command("sh", append([]string{tcCache03CarrierPath(t)}, args...)...)
	cmd.Env = tcCache03CarrierEnv(bin, env)
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

func tcCache03StartedAt(epoch int64) string {
	return time.Unix(epoch, 0).UTC().Format(time.RFC3339Nano)
}

// tcCache03Node creates the node filesystem a debug shell resolves /host to:
// the state root and its workspaces tree. A case that wants the tree absent
// simply does not call it.
func tcCache03Node(t *testing.T, state, node string) string {
	t.Helper()
	ws := filepath.Join(state, node, "host", "var", "lib", "foreman", "workspaces")
	if err := os.MkdirAll(filepath.Join(ws, ".repos", "ws", "repo.git"), 0o755); err != nil {
		t.Fatalf("create %s: %v", ws, err)
	}
	// A case that keeps the break in place (or fails before restoring it)
	// leaves directories at 0555, which t.TempDir's RemoveAll cannot clean.
	t.Cleanup(func() {
		_ = filepath.WalkDir(filepath.Dir(filepath.Dir(ws)), func(path string, d os.DirEntry, err error) error {
			if err == nil && d.IsDir() {
				_ = os.Chmod(path, 0o755)
			}
			return nil
		})
	})
	return ws
}

func tcCache03Task(t *testing.T, state, id, node, result, startedAt string) {
	t.Helper()
	dir := filepath.Join(state, "tasks")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	body := fmt.Sprintf(`{"task_id":%q,"state":"terminal","result":%q,"node_name":%q,"started_at":%q}`,
		id, result, node, startedAt)
	if err := os.WriteFile(filepath.Join(dir, id+".json"), []byte(body), 0o644); err != nil {
		t.Fatalf("write task %s: %v", id, err)
	}
}

func tcCache03Mode(t *testing.T, ws string) os.FileMode {
	t.Helper()
	info, err := os.Stat(ws)
	if err != nil {
		t.Fatalf("stat %s: %v", ws, err)
	}
	return info.Mode().Perm()
}

func tcCache03HasMarker(t *testing.T, state, node, name string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(state, node, "host", "tmp", name))
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	t.Fatalf("stat %s on %s: %v", name, node, err)
	return false
}

func tcCache03Journal(t *testing.T, state string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(state, "broken-nodes"))
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	return strings.Join(strings.Fields(string(raw)), " ")
}

// A task that failed explicitly on the node the break holds is the PASS the
// tolerance rests on; the run also has to leave that node writable and the
// journal clean, or the next scenario inherits the break as an environment
// failure.
func TestCache03CarrierPassesWhenTheJudgedTaskFailedOnItsNode(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	ws := tcCache03Node(t, state, "node-a")
	tcCache03Task(t, state, "a", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt-100))
	tcCache03Task(t, state, "c", "node-a", "failed", tcCache03StartedAt(tcCache03BreakAt+100))

	out, code := tcCache03CarrierRun(t, bin, state, nil, "break", "a")
	if code != 0 {
		t.Fatalf("break exit = %d, want 0:\n%s", code, out)
	}
	if !strings.Contains(out, "workspaces-readonly") ||
		!strings.Contains(out, "root_mode=555") ||
		!strings.Contains(out, fmt.Sprintf("broken_at=%d", tcCache03BreakAt)) {
		t.Errorf("the break does not report the state and instant it installed:\n%s", out)
	}
	if !strings.Contains(out, "control=writable") || !strings.Contains(out, "allowed=0") {
		t.Errorf("the break does not prove the tree is unwritable to the task user:\n%s", out)
	}
	if mode := tcCache03Mode(t, ws); mode != 0o555 {
		t.Fatalf("workspaces mode = %04o after break, want 0555:\n%s", mode, out)
	}

	out, code = tcCache03CarrierRun(t, bin, state, nil, "verify-unwritable", "c")
	if code != 0 {
		t.Fatalf("verify exit = %d, want 0 (the task failed as the break requires):\n%s", code, out)
	}
	if !strings.Contains(out, "PASS: TC-tech-cache-03 step 2") {
		t.Errorf("carrier did not report PASS:\n%s", out)
	}
	if mode := tcCache03Mode(t, ws); mode != 0o755 {
		t.Errorf("workspaces mode = %04o after a passing judgement, want 0755:\n%s", mode, out)
	}
	if tcCache03HasMarker(t, state, "node-a", tcCache03ModesJournal) ||
		tcCache03HasMarker(t, state, "node-a", tcCache03BreakAtMarker) {
		t.Errorf("a marker survived the restore:\n%s", out)
	}
	if got := tcCache03Journal(t, state); got != "" {
		t.Errorf("the journal still names %q after the restore:\n%s", got, out)
	}
}

// The silent success AC-12 bans: the task completed although the tree was
// unwritable before it started. That is the one FAIL this step may report, and
// the node is still restored.
func TestCache03CarrierFailsWhenTheTaskCompletedOnItsNode(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	ws := tcCache03Node(t, state, "node-a")
	tcCache03Task(t, state, "a", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt-100))
	tcCache03Task(t, state, "b", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt+100))

	if out, code := tcCache03CarrierRun(t, bin, state, nil, "break", "a"); code != 0 {
		t.Fatalf("break exit = %d, want 0:\n%s", code, out)
	}
	out, code := tcCache03CarrierRun(t, bin, state, nil, "verify-unwritable", "b")
	if code != 1 {
		t.Fatalf("verify exit = %d, want 1 (a completion on a broken tree is the failure):\n%s", code, out)
	}
	if !strings.Contains(out, "did not fail explicitly with the cache tree unwritable") {
		t.Errorf("the failure does not name the judged shape:\n%s", out)
	}
	if strings.Contains(out, "PASS:") {
		t.Errorf("a silent completion reached the PASS line:\n%s", out)
	}
	if mode := tcCache03Mode(t, ws); mode != 0o755 {
		t.Errorf("workspaces mode = %04o after a FAIL, want the restore to leave 0755:\n%s", mode, out)
	}
}

// A task judged on a node the break does not hold never met the scenario: the
// carrier blocks, names the node the break still holds, and leaves it for
// `restore <node>` instead of deciding about the task. The restore clears both
// the mode and the journal line, so the next scenario does not inherit an
// unwritable tree.
func TestCache03CarrierBlocksAndNamesTheBrokenNode(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	broken := tcCache03Node(t, state, "node-a")
	other := tcCache03Node(t, state, "node-b")
	tcCache03Task(t, state, "a", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt-100))
	tcCache03Task(t, state, "b", "node-b", "completed", tcCache03StartedAt(tcCache03BreakAt+100))

	if out, code := tcCache03CarrierRun(t, bin, state, nil, "break", "a"); code != 0 {
		t.Fatalf("break exit = %d, want 0:\n%s", code, out)
	}
	out, code := tcCache03CarrierRun(t, bin, state, nil, "verify-unwritable", "b")
	if code != 2 {
		t.Fatalf("verify exit = %d, want 2 (the task met a writable tree):\n%s", code, out)
	}
	if !strings.Contains(out, "never met flat cache") {
		t.Errorf("the blocker does not name the missing break:\n%s", out)
	}
	if !strings.Contains(out, "node-a still carries the break — run: ") {
		t.Errorf("the blocker does not name the node the break holds:\n%s", out)
	}
	if mode := tcCache03Mode(t, broken); mode != 0o555 {
		t.Errorf("the broken node's mode = %04o, want the break kept until restore:\n%s", mode, out)
	}
	if mode := tcCache03Mode(t, other); mode != 0o755 {
		t.Errorf("the judged task's node was touched: mode = %04o, want 0755:\n%s", mode, out)
	}
	if got := tcCache03Journal(t, state); got != "node-a" {
		t.Errorf("journal = %q, want the broken node recorded:\n%s", got, out)
	}

	out, code = tcCache03CarrierRun(t, bin, state, nil, "restore", "node-a")
	if code != 0 {
		t.Fatalf("restore exit = %d, want 0:\n%s", code, out)
	}
	if mode := tcCache03Mode(t, broken); mode != 0o755 {
		t.Errorf("the broken node's mode = %04o after restore, want 0755:\n%s", mode, out)
	}
	if tcCache03HasMarker(t, state, "node-a", tcCache03ModesJournal) ||
		tcCache03HasMarker(t, state, "node-a", tcCache03BreakAtMarker) {
		t.Errorf("restore left a marker on the node:\n%s", out)
	}
	if got := tcCache03Journal(t, state); got != "" {
		t.Errorf("journal = %q after restore, want it empty:\n%s", got, out)
	}
}

// A break installed after the judged task started is not a scenario either:
// the task may have passed the cache write before the tree changed, so the run
// blocks instead of reading its completion as a silent success.
func TestCache03CarrierBlocksWhenTheBreakCameAfterTheTaskStarted(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	ws := tcCache03Node(t, state, "node-a")
	tcCache03Task(t, state, "a", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt+100))
	tcCache03Task(t, state, "late", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt-60))

	if out, code := tcCache03CarrierRun(t, bin, state, nil, "break", "a"); code != 0 {
		t.Fatalf("break exit = %d, want 0:\n%s", code, out)
	}
	out, code := tcCache03CarrierRun(t, bin, state, nil, "verify-unwritable", "late")
	if code != 2 {
		t.Fatalf("verify exit = %d, want 2 (the tree changed after the task started):\n%s", code, out)
	}
	if !strings.Contains(out, "not before task late's started_at") {
		t.Errorf("the blocker does not name the instant comparison:\n%s", out)
	}
	if strings.Contains(out, "FAIL:") {
		t.Errorf("a mid-flight break became a product failure:\n%s", out)
	}
	if mode := tcCache03Mode(t, ws); mode != 0o755 {
		t.Errorf("workspaces mode = %04o, want the blocked run's trap to restore it:\n%s", mode, out)
	}
}

// Without the node-side instant the two facts are incomparable, so the run
// blocks and still restores the tree it confirmed broken.
func TestCache03CarrierBlocksWhenTheBreakInstantIsUnreadable(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	ws := tcCache03Node(t, state, "node-a")
	tcCache03Task(t, state, "a", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt-100))
	tcCache03Task(t, state, "b", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt+100))

	if out, code := tcCache03CarrierRun(t, bin, state, nil, "break", "a"); code != 0 {
		t.Fatalf("break exit = %d, want 0:\n%s", code, out)
	}
	instant := filepath.Join(state, "node-a", "host", "tmp", tcCache03BreakAtMarker)
	if err := os.Remove(instant); err != nil {
		t.Fatalf("remove the break instant: %v", err)
	}
	out, code := tcCache03CarrierRun(t, bin, state, nil, "verify-unwritable", "b")
	if code != 2 {
		t.Fatalf("verify exit = %d, want 2 (no instant to compare):\n%s", code, out)
	}
	if !strings.Contains(out, "break instant on node-a is unreadable") {
		t.Errorf("the blocker does not name the unreadable instant:\n%s", out)
	}
	if mode := tcCache03Mode(t, ws); mode != 0o755 {
		t.Errorf("workspaces mode = %04o, want the trap to restore the confirmed break:\n%s", mode, out)
	}
}

// A task projection without started_at cannot be placed after the break; the
// run blocks rather than assuming the order it needs.
func TestCache03CarrierBlocksWhenTheTaskHasNoStartedAt(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	ws := tcCache03Node(t, state, "node-a")
	tcCache03Task(t, state, "a", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt-100))
	tcCache03Task(t, state, "b", "node-a", "completed", "")

	if out, code := tcCache03CarrierRun(t, bin, state, nil, "break", "a"); code != 0 {
		t.Fatalf("break exit = %d, want 0:\n%s", code, out)
	}
	out, code := tcCache03CarrierRun(t, bin, state, nil, "verify-unwritable", "b")
	if code != 2 {
		t.Fatalf("verify exit = %d, want 2 (no start fact to compare):\n%s", code, out)
	}
	if !strings.Contains(out, "task b has neither started_at nor job_created_at") {
		t.Errorf("the blocker does not name the missing fact:\n%s", out)
	}
	if mode := tcCache03Mode(t, ws); mode != 0o755 {
		t.Errorf("workspaces mode = %04o, want the trap to restore the confirmed break:\n%s", mode, out)
	}
}

// Several task nodes and no task to follow leave no single node to break:
// picking one is the root cause the run must not repeat.
func TestCache03CarrierBlocksOnAmbiguousDiscovery(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	tcCache03Node(t, state, "node-a")
	tcCache03Node(t, state, "node-b")

	out, code := tcCache03CarrierRun(t, bin, state, map[string]string{"FAKE_NODES": "node-a node-b"}, "break")
	if code != 2 {
		t.Fatalf("break exit = %d, want 2 (no deterministic single node):\n%s", code, out)
	}
	if !strings.Contains(out, "name 2 nodes (node-a node-b)") {
		t.Errorf("the blocker does not name both nodes:\n%s", out)
	}
}

// A named task pins the break to its own node: that node changes, the other
// stays untouched, and the journal records the one holding the break.
func TestCache03CarrierBreaksTheNamedTasksNode(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	broken := tcCache03Node(t, state, "node-a")
	other := tcCache03Node(t, state, "node-b")
	tcCache03Task(t, state, "a", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt-100))

	out, code := tcCache03CarrierRun(t, bin, state, nil, "break", "a")
	if code != 0 {
		t.Fatalf("break exit = %d, want 0:\n%s", code, out)
	}
	if !strings.Contains(out, "node=node-a action=break target=a") {
		t.Errorf("the break does not name the node it pinned:\n%s", out)
	}
	if mode := tcCache03Mode(t, broken); mode != 0o555 {
		t.Errorf("the named task's node mode = %04o, want 0555:\n%s", mode, out)
	}
	if mode := tcCache03Mode(t, other); mode != 0o755 {
		t.Errorf("the other node was modified: mode = %04o, want 0755:\n%s", mode, out)
	}
	if got := tcCache03Journal(t, state); got != "node-a" {
		t.Errorf("journal = %q, want node-a:\n%s", got, out)
	}
}

// A task that never landed met no tree: the run blocks before asking a node
// anything.
func TestCache03CarrierBlocksWhenTheTaskNeverLanded(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	tcCache03Node(t, state, "node-a")
	tcCache03Task(t, state, "d", "", "completed", tcCache03StartedAt(tcCache03BreakAt+100))

	out, code := tcCache03CarrierRun(t, bin, state, nil, "verify-unwritable", "d")
	if code != 2 {
		t.Fatalf("verify exit = %d, want 2 (no judged node):\n%s", code, out)
	}
	if !strings.Contains(out, "task d has no node_name") {
		t.Errorf("the blocker does not name the missing placement:\n%s", out)
	}
}

// With no break anywhere the blocker must not send the operator after a node
// that was never touched: the journal is empty, so no residue hint appears.
func TestCache03CarrierBlocksWithoutResidueWithoutABreak(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	tcCache03Node(t, state, "node-a")
	tcCache03Task(t, state, "b", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt+100))

	out, code := tcCache03CarrierRun(t, bin, state, nil, "verify-unwritable", "b")
	if code != 2 {
		t.Fatalf("verify exit = %d, want 2 (no break on the task's node):\n%s", code, out)
	}
	if strings.Contains(out, "still carries the break") {
		t.Errorf("the blocker invents a broken node:\n%s", out)
	}
}

// restore is the operator's cleanup entry: it works on a node the carrier no
// longer judges, and running it twice is harmless.
func TestCache03CarrierRestoreIsIdempotent(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	ws := tcCache03Node(t, state, "node-a")
	tcCache03Task(t, state, "a", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt-100))

	if out, code := tcCache03CarrierRun(t, bin, state, nil, "break", "a"); code != 0 {
		t.Fatalf("break exit = %d, want 0:\n%s", code, out)
	}
	if out, code := tcCache03CarrierRun(t, bin, state, nil, "restore", "node-a"); code != 0 {
		t.Fatalf("restore exit = %d, want 0:\n%s", code, out)
	}
	out, code := tcCache03CarrierRun(t, bin, state, nil, "restore", "node-a")
	if code != 0 {
		t.Fatalf("second restore exit = %d, want 0 (nothing left to undo):\n%s", code, out)
	}
	if !strings.Contains(out, "no-journal") {
		t.Errorf("the second restore does not report the writable fallback:\n%s", out)
	}
	if mode := tcCache03Mode(t, ws); mode != 0o755 {
		t.Errorf("workspaces mode = %04o after two restores, want 0755:\n%s", mode, out)
	}
}

// An explicit NODE naming another node than the task ran on is a construction
// error on both sides of the carrier: break refuses to install the state
// elsewhere (pin_node) and verify-unwritable refuses to judge a tree the task
// did not meet (pin_place). Neither falls back to NODE.
func TestCache03CarrierBlocksWhenNodeContradictsTheTaskLanding(t *testing.T) {
	t.Run("break", func(t *testing.T) {
		bin := tcCache03CarrierFakeBin(t)
		state := t.TempDir()
		ws := tcCache03Node(t, state, "node-a")
		tcCache03Node(t, state, "node-b")
		tcCache03Task(t, state, "a", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt-100))

		out, code := tcCache03CarrierRun(t, bin, state, map[string]string{"NODE": "node-b"}, "break", "a")
		if code != 2 {
			t.Fatalf("break exit = %d, want 2 (NODE names another node):\n%s", code, out)
		}
		if !strings.Contains(out, "a state action targets the task's node") {
			t.Errorf("the blocker does not name the contradicted landing:\n%s", out)
		}
		if mode := tcCache03Mode(t, ws); mode != 0o755 {
			t.Errorf("the task's node was touched: mode = %04o, want 0755:\n%s", mode, out)
		}
		if got := tcCache03Journal(t, state); got != "" {
			t.Errorf("journal = %q after a refused break, want it empty:\n%s", got, out)
		}
	})

	t.Run("verify-unwritable", func(t *testing.T) {
		bin := tcCache03CarrierFakeBin(t)
		state := t.TempDir()
		broken := tcCache03Node(t, state, "node-a")
		tcCache03Node(t, state, "node-b")
		tcCache03Task(t, state, "a", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt-100))
		tcCache03Task(t, state, "b", "node-a", "failed", tcCache03StartedAt(tcCache03BreakAt+100))

		if out, code := tcCache03CarrierRun(t, bin, state, nil, "break", "a"); code != 0 {
			t.Fatalf("break exit = %d, want 0:\n%s", code, out)
		}
		out, code := tcCache03CarrierRun(t, bin, state, map[string]string{"NODE": "node-b"}, "verify-unwritable", "b")
		if code != 2 {
			t.Fatalf("verify exit = %d, want 2 (NODE names another node than the task):\n%s", code, out)
		}
		if !strings.Contains(out, "the judged node is the task's placement") {
			t.Errorf("the blocker does not name the contradicted placement:\n%s", out)
		}
		if mode := tcCache03Mode(t, broken); mode != 0o555 {
			t.Errorf("the guard touched the broken node: mode = %04o, want 0555:\n%s", mode, out)
		}
		if got := tcCache03Journal(t, state); got != "node-a" {
			t.Errorf("journal = %q, want the broken node kept for restore:\n%s", got, out)
		}
	})
}

// One task node and no task to follow is still a deterministic target: the
// discovery succeeds instead of refusing every state action on a single-node
// cluster.
func TestCache03CarrierDiscoversTheOnlyTaskNode(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	only := tcCache03Node(t, state, "node-a")
	other := tcCache03Node(t, state, "node-b")

	out, code := tcCache03CarrierRun(t, bin, state, map[string]string{"FAKE_NODES": "node-a"}, "break")
	if code != 0 {
		t.Fatalf("break exit = %d, want 0 (one discovered node is the target):\n%s", code, out)
	}
	if !strings.Contains(out, "node=node-a action=break") {
		t.Errorf("the break does not name the discovered node:\n%s", out)
	}
	if mode := tcCache03Mode(t, only); mode != 0o555 {
		t.Errorf("the only task node mode = %04o, want 0555:\n%s", mode, out)
	}
	if mode := tcCache03Mode(t, other); mode != 0o755 {
		t.Errorf("the unlisted node was modified: mode = %04o, want 0755:\n%s", mode, out)
	}
	if got := tcCache03Journal(t, state); got != "node-a" {
		t.Errorf("journal = %q, want node-a:\n%s", got, out)
	}
}

// A restore whose node-side call fails must not pretend to have cleaned up: it
// blocks, keeps the journal line and leaves both markers for the retry.
func TestCache03CarrierBlocksWhenTheRestoreCannotReachTheNode(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	ws := tcCache03Node(t, state, "node-a")
	tcCache03Task(t, state, "a", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt-100))

	if out, code := tcCache03CarrierRun(t, bin, state, nil, "break", "a"); code != 0 {
		t.Fatalf("break exit = %d, want 0:\n%s", code, out)
	}
	out, code := tcCache03CarrierRun(t, bin, state, map[string]string{"FAKE_DEBUG_FAIL_NODE": "node-a"}, "restore", "node-a")
	if code != 2 {
		t.Fatalf("restore exit = %d, want 2 (the node-side call failed):\n%s", code, out)
	}
	if !strings.Contains(out, "cannot restore node-a") {
		t.Errorf("the blocker does not name the unreachable node:\n%s", out)
	}
	if mode := tcCache03Mode(t, ws); mode != 0o555 {
		t.Errorf("a failed restore changed the mode to %04o, want 0555:\n%s", mode, out)
	}
	if !tcCache03HasMarker(t, state, "node-a", tcCache03ModesJournal) ||
		!tcCache03HasMarker(t, state, "node-a", tcCache03BreakAtMarker) {
		t.Errorf("a failed restore dropped a marker:\n%s", out)
	}
	if got := tcCache03Journal(t, state); got != "node-a" {
		t.Errorf("journal = %q after a failed restore, want the node kept:\n%s", got, out)
	}

	out, code = tcCache03CarrierRun(t, bin, state, nil, "restore", "node-a")
	if code != 0 {
		t.Fatalf("retry exit = %d, want 0 once the node is reachable:\n%s", code, out)
	}
	if mode := tcCache03Mode(t, ws); mode != 0o755 {
		t.Errorf("workspaces mode = %04o after the retry, want 0755:\n%s", mode, out)
	}
	if got := tcCache03Journal(t, state); got != "" {
		t.Errorf("journal = %q after the retry, want it empty:\n%s", got, out)
	}
}

// An attach that exits 0 with no answer proves nothing: the restore must block
// and keep the journal, or a break whose cleanup never ran is reported done.
func TestCache03CarrierBlocksWhenTheRestoreAnswerIsEmpty(t *testing.T) {
	bin := tcCache03CarrierFakeBin(t)
	state := t.TempDir()
	ws := tcCache03Node(t, state, "node-a")
	tcCache03Task(t, state, "a", "node-a", "completed", tcCache03StartedAt(tcCache03BreakAt-100))

	if out, code := tcCache03CarrierRun(t, bin, state, nil, "break", "a"); code != 0 {
		t.Fatalf("break exit = %d, want 0:\n%s", code, out)
	}
	out, code := tcCache03CarrierRun(t, bin, state, map[string]string{"FAKE_DEBUG_EMPTY_NODE": "node-a"}, "restore", "node-a")
	if code != 2 {
		t.Fatalf("restore exit = %d, want 2 (no node-side answer):\n%s", code, out)
	}
	if !strings.Contains(out, "returned no output") {
		t.Errorf("the blocker does not name the missing answer:\n%s", out)
	}
	if mode := tcCache03Mode(t, ws); mode != 0o555 {
		t.Errorf("workspaces mode = %04o after an unanswered restore, want 0555:\n%s", mode, out)
	}
	if got := tcCache03Journal(t, state); got != "node-a" {
		t.Errorf("journal = %q after an unanswered restore, want the node kept:\n%s", got, out)
	}
}
