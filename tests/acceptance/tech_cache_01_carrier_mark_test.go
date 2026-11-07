// TC-tech-cache-01's node-side carrier under a fake kubectl/curl/date: the
// creation instant is the carrier's own mark inside the cache object, written
// after the first task settled and read against the second pod's start. The
// cases pin both directions — a mark inside the object before the pod started
// passes, a mark after it blocks (the window was not constructed), a missing or
// foreign mark blocks (nothing establishes the object's age) — because a
// carrier that guessed here would either pass a fresh clone or block every
// round, which is what the birth-time criterion did.
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
	"time"
)

func tcCache01CarrierPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the carrier from the test source")
	}
	return filepath.Join(filepath.Dir(file), "tech-cache-01-cluster.sh")
}

func tcCache01WriteExecutable(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// tcCache01FakeBin writes the commands the carrier calls. The debug shell runs
// against a per-node directory standing in for the node filesystem (the same
// substitution the cache-03 carrier test uses); date is the node clock the mark
// is written on, so a case can place it before or after the pod's start.
func tcCache01FakeBin(t *testing.T) string {
	t.Helper()
	bin := t.TempDir()
	tcCache01WriteExecutable(t, filepath.Join(bin, "kubectl"), `#!/bin/sh
set -u
case " $* " in
*" get pods "*)
	cat "${FAKE_PODS:?}"
	exit 0 ;;
*" debug "*)
	node=''
	for a in "$@"; do
		case "$a" in
		node/*) node=${a#node/} ;;
		esac
	done
	[ -n "$node" ] || { echo "fake kubectl: no node in: $*" >&2; exit 99; }
	for last; do :; done
	root="$FAKE_STATE/$node/host"
	cmd=$(printf '%s' "$last" | sed "s#/host#$root#g")
	exec sh -c "$cmd" ;;
*)
	echo "fake kubectl: unexpected call: $*" >&2
	exit 99 ;;
esac
`)
	tcCache01WriteExecutable(t, filepath.Join(bin, "curl"), `#!/bin/sh
set -u
for a; do url=$a; done
id=${url##*/}
cat "$FAKE_TASKS/$id.json"
`)
	tcCache01WriteExecutable(t, filepath.Join(bin, "date"), `#!/bin/sh
printf '%s\n' "${FAKE_NOW:?}"
`)
	return bin
}

func tcCache01Env(bin string, overrides map[string]string) []string {
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

// tcCache01Run drives one carrier action and returns its output and exit code.
func tcCache01Run(t *testing.T, bin, state string, overrides map[string]string, args ...string) (string, int) {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	env := map[string]string{
		"TASK1":                 "t1",
		"TASK2":                 "t2",
		"FOREMAN_URL":           "http://foreman",
		"FOREMAN_OPS_TOKEN":     "test-ops-token",
		"FOREMAN_JOB_NAMESPACE": "multica-agents",
		"FOREMAN_STATE_ROOT":    "/var/lib/foreman",
		"NODE_DEBUG_IMAGE":      "fake",
		"FAKE_STATE":            state,
		"FAKE_TASKS":            filepath.Join(state, "tasks"),
		"FAKE_PODS":             filepath.Join(state, "pods.json"),
		"FAKE_NOW":              "1000",
	}
	for key, value := range overrides {
		env[key] = value
	}
	cmd := exec.Command("sh", append([]string{tcCache01CarrierPath(t)}, args...)...)
	cmd.Env = tcCache01Env(bin, env)
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

// tcCache01State builds one node's cache tree, the first task's projection and
// the second task's pod. The pod's startTime is fixed; a case moves the mark
// around it.
func tcCache01State(t *testing.T, startEpoch int64) (state, repo string) {
	t.Helper()
	state = t.TempDir()
	repo = filepath.Join(state, "node-a", "host", "var", "lib", "foreman",
		"workspaces", ".repos", "ws", "repo.git")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatalf("create the cache object: %v", err)
	}
	tasks := filepath.Join(state, "tasks")
	if err := os.MkdirAll(tasks, 0o755); err != nil {
		t.Fatalf("create the task dir: %v", err)
	}
	task := `{"task_id":"t1","state":"terminal","result":"completed","node_name":"node-a"}`
	if err := os.WriteFile(filepath.Join(tasks, "t1.json"), []byte(task), 0o644); err != nil {
		t.Fatalf("write the first task: %v", err)
	}
	pods := `{"items":[{"metadata":{"name":"pod2"},"spec":{"nodeName":"node-a"},` +
		`"status":{"startTime":"` + time.Unix(startEpoch, 0).UTC().Format(time.RFC3339) + `"}}]}`
	if err := os.WriteFile(filepath.Join(state, "pods.json"), []byte(pods), 0o644); err != nil {
		t.Fatalf("write the second pod: %v", err)
	}
	return state, repo
}

func TestCache01CarrierMarkBeforeTheSecondPodPasses(t *testing.T) {
	bin := tcCache01FakeBin(t)
	state, repo := tcCache01State(t, 1200)

	out, code := tcCache01Run(t, bin, state, nil, "mark")
	if code != 0 {
		t.Fatalf("mark exit = %d, want 0:\n%s", code, out)
	}
	mark, err := os.ReadFile(filepath.Join(repo, ".foreman-cache-mark"))
	if err != nil {
		t.Fatalf("the mark is not inside the cache object: %v", err)
	}
	if got := strings.TrimSpace(string(mark)); got != "1000 t1" {
		t.Errorf("the mark is %q, want the node clock and the first task", got)
	}

	out, code = tcCache01Run(t, bin, state, nil, "check")
	if code != 0 {
		t.Fatalf("check exit = %d, want 0:\n%s", code, out)
	}
	if !strings.Contains(out, "PASS: TC-tech-cache-01") {
		t.Errorf("the carrier does not report a pass:\n%s", out)
	}
}

func TestCache01CarrierMarkAfterTheSecondPodBlocks(t *testing.T) {
	bin := tcCache01FakeBin(t)
	state, _ := tcCache01State(t, 900) // the pod starts before the mark is written
	if out, code := tcCache01Run(t, bin, state, nil, "mark"); code != 0 {
		t.Fatalf("mark exit = %d, want 0:\n%s", code, out)
	}
	out, code := tcCache01Run(t, bin, state, nil, "check")
	if code != 2 {
		t.Fatalf("check exit = %d, want the blocked code 2:\n%s", code, out)
	}
	if !strings.Contains(out, "reuse window was not constructed") {
		t.Errorf("the block does not say the mark missed the window:\n%s", out)
	}
}

func TestCache01CarrierRequiresItsOwnMark(t *testing.T) {
	bin := tcCache01FakeBin(t)
	for _, tc := range []struct {
		name string
		mark string
		want string
	}{
		{name: "no mark", mark: "", want: "carries no carrier mark"},
		{name: "another round's mark", mark: "1000 t9\n", want: "not TASK1=t1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, repo := tcCache01State(t, 1200)
			if tc.mark != "" {
				if err := os.WriteFile(filepath.Join(repo, ".foreman-cache-mark"), []byte(tc.mark), 0o644); err != nil {
					t.Fatalf("write the mark: %v", err)
				}
			}
			out, code := tcCache01Run(t, bin, state, nil, "check")
			if code != 2 {
				t.Fatalf("check exit = %d, want the blocked code 2:\n%s", code, out)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("the block does not name %q:\n%s", tc.want, out)
			}
		})
	}
}
