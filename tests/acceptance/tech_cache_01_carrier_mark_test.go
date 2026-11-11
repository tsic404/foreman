// TC-tech-cache-01's node-side carrier under a fake kubectl/curl/date: the
// creation instant is the carrier's own mark inside the cache object, written
// after the first task settled and read against the second pod's start. The
// cases pin all three verdicts — a mark inside the object before the pod
// started passes, a mark after it blocks (the upper bound places nothing), and
// a mark the harness wrote but the object no longer carries fails (the object
// was rebuilt inside the window, which AC-12 calls a failure). Only the round
// that never wrote a mark blocks: there nothing separates reuse from a rebuild.
//
// The judgement object is the witness's, never REPO_GIT's: the mark names
// exactly one repository (an unnamed multi-repo workspace, or a name matching
// none or several, blocks), and check only requires REPO_GIT to agree with that
// witness. Otherwise a sibling's stale mark would pass a rebuilt object, and an
// object the mark never touched would fail as a rebuild.
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
	# The container command is the argument after -c; arguments following it
	# are that command's own argv (the carrier passes REPO_GIT this way, so it
	# must stay out of the script text).
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
		"FOREMAN_CACHE_WITNESS": filepath.Join(state, "witness"),
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
	// Another workspace's object on the same node: the mark must never land
	// there — it is not this task's cache, and its mtime drives the selection
	// this carrier falls back to.
	other := filepath.Join(state, "node-a", "host", "var", "lib", "foreman",
		"workspaces", ".repos", "ws-other", "other.git")
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatalf("create the other workspace's cache: %v", err)
	}
	tasks := filepath.Join(state, "tasks")
	if err := os.MkdirAll(tasks, 0o755); err != nil {
		t.Fatalf("create the task dir: %v", err)
	}
	task := `{"task_id":"t1","state":"terminal","result":"completed","node_name":"node-a","workspace_id":"ws"}`
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

// tcCache01AddRepo creates another bare repository in the first task's
// workspace on the fake node — the "several caches per workspace" case.
func tcCache01AddRepo(t *testing.T, state, name string) string {
	t.Helper()
	repo := filepath.Join(state, "node-a", "host", "var", "lib", "foreman",
		"workspaces", ".repos", "ws", name)
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatalf("create %s: %v", repo, err)
	}
	return repo
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
	witness, err := os.ReadFile(filepath.Join(state, "witness"))
	if err != nil {
		t.Fatalf("the harness kept no witness: %v", err)
	}
	// The witness names the object as the debug container saw it, so `check`
	// judges the repository the harness marked and no other. In this harness the
	// fake node root replaces /host.
	wantWitness := "t1 1000 ws " + repo
	if got := strings.TrimSpace(string(witness)); got != wantWitness {
		t.Errorf("the witness is %q, want %q", got, wantWitness)
	}
	other := filepath.Join(state, "node-a", "host", "var", "lib", "foreman",
		"workspaces", ".repos", "ws-other", "other.git", ".foreman-cache-mark")
	if _, err := os.Stat(other); !os.IsNotExist(err) {
		t.Errorf("the mark reached another workspace's cache object (stat: %v)", err)
	}

	out, code = tcCache01Run(t, bin, state, nil, "check")
	if code != 0 {
		t.Fatalf("check exit = %d, want 0:\n%s", code, out)
	}
	if !strings.Contains(out, "PASS: TC-tech-cache-01") {
		t.Errorf("the carrier does not report a pass:\n%s", out)
	}

	// A name that agrees with the witness changes nothing: the witness already
	// decided the object.
	out, code = tcCache01Run(t, bin, state, map[string]string{"REPO_GIT": "repo.git"}, "check")
	if code != 0 {
		t.Fatalf("check exit = %d with REPO_GIT agreeing with the witness, want 0:\n%s", code, out)
	}
	if !strings.Contains(out, "PASS: TC-tech-cache-01") {
		t.Errorf("the agreeing name does not report a pass:\n%s", out)
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
	if !strings.Contains(out, "upper bound") {
		t.Errorf("the block does not say the upper bound places nothing:\n%s", out)
	}
}

// TestCache01CarrierRebuildInsideTheWindowFails is the failure face AC-12 asks
// for: the harness wrote a mark, and the object no longer carries it (or
// carries another task's) — the object was rebuilt during this task, which the
// design makes a failure rather than an undecidable case.
func TestCache01CarrierRebuildInsideTheWindowFails(t *testing.T) {
	bin := tcCache01FakeBin(t)
	for _, tc := range []struct {
		name string
		mark string
		want string
	}{
		{name: "the object lost the mark", mark: "", want: "was rebuilt during this task"},
		{name: "another task rebuilt it", mark: "1000 t9\n", want: "was rebuilt during this task"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, repo := tcCache01State(t, 1200)
			if out, code := tcCache01Run(t, bin, state, nil, "mark"); code != 0 {
				t.Fatalf("mark exit = %d, want 0:\n%s", code, out)
			}
			// The rebuild: the mark the harness wrote is gone or replaced.
			if err := os.Remove(filepath.Join(repo, ".foreman-cache-mark")); err != nil {
				t.Fatalf("remove the mark: %v", err)
			}
			if tc.mark != "" {
				if err := os.WriteFile(filepath.Join(repo, ".foreman-cache-mark"), []byte(tc.mark), 0o644); err != nil {
					t.Fatalf("write the foreign mark: %v", err)
				}
			}
			out, code := tcCache01Run(t, bin, state, nil, "check")
			if code != 1 {
				t.Fatalf("check exit = %d, want the failure code 1:\n%s", code, out)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("the failure does not say %q:\n%s", tc.want, out)
			}
		})
	}
}

// TestCache01CarrierWithoutWitnessBlocks keeps the undecidable case out of the
// verdict: this round never wrote a mark, so neither reuse nor a rebuild can
// be claimed.
func TestCache01CarrierWithoutWitnessBlocks(t *testing.T) {
	bin := tcCache01FakeBin(t)
	state, _ := tcCache01State(t, 1200)
	out, code := tcCache01Run(t, bin, state, nil, "check")
	if code != 2 {
		t.Fatalf("check exit = %d, want the blocked code 2:\n%s", code, out)
	}
	if !strings.Contains(out, "no harness witness") {
		t.Errorf("the block does not say the witness is missing:\n%s", out)
	}
}

// TestCache01CarrierMarkNamesOneRepository pins the rule that keeps the
// judgement on the object the task actually used: with several repositories in
// the workspace the mark must not be written blindly (a rebuilt object judged
// by a surviving sibling would pass), so an unnamed mark blocks and a named one
// touches only that repository.
func TestCache01CarrierMarkNamesOneRepository(t *testing.T) {
	bin := tcCache01FakeBin(t)
	state, repo := tcCache01State(t, 1200)
	sibling := tcCache01AddRepo(t, state, "sibling.git")

	out, code := tcCache01Run(t, bin, state, nil, "mark")
	if code != 2 {
		t.Fatalf("mark exit = %d, want the blocked code 2 for an unnamed multi-repo workspace:\n%s", code, out)
	}
	if !strings.Contains(out, "holds 2 cache repositories") {
		t.Errorf("the block does not say the workspace holds several repositories:\n%s", out)
	}

	out, code = tcCache01Run(t, bin, state, map[string]string{"REPO_GIT": "repo.git"}, "mark")
	if code != 0 {
		t.Fatalf("mark exit = %d with REPO_GIT naming one repository:\n%s", code, out)
	}
	if _, err := os.Stat(filepath.Join(repo, ".foreman-cache-mark")); err != nil {
		t.Errorf("the named repository carries no mark: %v", err)
	}
	if _, err := os.Stat(filepath.Join(sibling, ".foreman-cache-mark")); !os.IsNotExist(err) {
		t.Errorf("the mark reached a repository the task did not name (stat: %v)", err)
	}
}

// TestCache01CarrierMarkRefusesAnAmbiguousName pins the invariant the mark
// carries: it names one object. A name selecting two repositories must block —
// writing both leaves the survivor answering for a rebuilt object, and the
// witness would then name only the first line it read.
func TestCache01CarrierMarkRefusesAnAmbiguousName(t *testing.T) {
	bin := tcCache01FakeBin(t)
	for _, tc := range []struct {
		name     string
		siblings []string
		repoGit  string
		want     string
	}{
		{
			name:     "a partial name selecting two repositories",
			siblings: []string{"ab.git", "ac.git"},
			repoGit:  "a",
			want:     "no cache repository matching REPO_GIT=a",
		},
		{
			name:     "a suffix shared by two repositories",
			siblings: []string{"sibling.git"},
			repoGit:  "git",
			want:     "matches 2 cache repositories",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, repo := tcCache01State(t, 1200)
			repos := []string{repo}
			for _, name := range tc.siblings {
				repos = append(repos, tcCache01AddRepo(t, state, name))
			}
			out, code := tcCache01Run(t, bin, state, map[string]string{"REPO_GIT": tc.repoGit}, "mark")
			if code != 2 {
				t.Fatalf("mark exit = %d, want the blocked code 2 for an ambiguous name:\n%s", code, out)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("the block does not say %q:\n%s", tc.want, out)
			}
			if _, err := os.Stat(filepath.Join(state, "witness")); !os.IsNotExist(err) {
				t.Errorf("a blocked mark kept a witness (stat: %v)", err)
			}
			for _, addr := range repos {
				if _, err := os.Stat(filepath.Join(addr, ".foreman-cache-mark")); !os.IsNotExist(err) {
					t.Errorf("a blocked mark wrote %s (stat: %v)", filepath.Join(addr, ".foreman-cache-mark"), err)
				}
			}
		})
	}
}

// TestCache01CarrierCheckRefusesANameTheWitnessDidNotMark closes one direction
// of a false verdict: judging the repository REPO_GIT names instead of the
// witness's would read an object the mark never touched and call the missing
// mark a rebuild.
func TestCache01CarrierCheckRefusesANameTheWitnessDidNotMark(t *testing.T) {
	bin := tcCache01FakeBin(t)
	state, _ := tcCache01State(t, 1200)
	if out, code := tcCache01Run(t, bin, state, nil, "mark"); code != 0 {
		t.Fatalf("mark exit = %d, want 0:\n%s", code, out)
	}
	tcCache01AddRepo(t, state, "sibling.git") // appeared after the mark, never marked

	out, code := tcCache01Run(t, bin, state, map[string]string{"REPO_GIT": "sibling.git"}, "check")
	if code != 1 {
		t.Fatalf("check exit = %d, want the failure code 1 when the name and the witness disagree:\n%s", code, out)
	}
	if !strings.Contains(out, "does not name the object the harness marked") {
		t.Errorf("the failure does not name the disagreement:\n%s", out)
	}
	if strings.Contains(out, "was rebuilt during this task") {
		t.Errorf("an object the mark never touched was judged as a rebuild:\n%s", out)
	}
}

// TestCache01CarrierCheckNeverReadsAStaleSiblingMark closes the other
// direction: marking B and then A leaves B's old mark in place, so a check that
// followed REPO_GIT=B would read that stale mark and pass while A — the object
// the task used — was rebuilt inside the window.
func TestCache01CarrierCheckNeverReadsAStaleSiblingMark(t *testing.T) {
	bin := tcCache01FakeBin(t)
	state, repoA := tcCache01State(t, 1200)
	tcCache01AddRepo(t, state, "sibling.git")
	if out, code := tcCache01Run(t, bin, state,
		map[string]string{"REPO_GIT": "sibling.git", "FAKE_NOW": "900"}, "mark"); code != 0 {
		t.Fatalf("mark of the sibling exit = %d, want 0:\n%s", code, out)
	}
	if out, code := tcCache01Run(t, bin, state,
		map[string]string{"REPO_GIT": "repo.git", "FAKE_NOW": "1000"}, "mark"); code != 0 {
		t.Fatalf("mark of the task's repository exit = %d, want 0:\n%s", code, out)
	}
	// The object the task used was rebuilt inside the window; the sibling still
	// carries the mark of the earlier round, written before the pod started.
	if err := os.Remove(filepath.Join(repoA, ".foreman-cache-mark")); err != nil {
		t.Fatalf("remove the task object's mark: %v", err)
	}

	out, code := tcCache01Run(t, bin, state, map[string]string{"REPO_GIT": "sibling.git"}, "check")
	if code == 0 || strings.Contains(out, "PASS: TC-tech-cache-01") {
		t.Fatalf("check exit = %d: the stale sibling mark passed the criterion:\n%s", code, out)
	}
	if !strings.Contains(out, "does not name the object the harness marked") {
		t.Errorf("the failure does not name the disagreement:\n%s", out)
	}
}

// TestCache01CarrierKeepsTheRepositoryNameOutOfTheNodeScript: REPO_GIT travels
// as an argument to the node-side shell, so characters that shell would read as
// syntax stay data. Spliced into the script text they would be executed.
func TestCache01CarrierKeepsTheRepositoryNameOutOfTheNodeScript(t *testing.T) {
	bin := tcCache01FakeBin(t)
	state, _ := tcCache01State(t, 1200)
	pwned := filepath.Join(state, "pwned")
	payload := "x; touch " + pwned

	out, code := tcCache01Run(t, bin, state, map[string]string{"REPO_GIT": payload}, "mark")
	if code != 2 {
		t.Fatalf("mark exit = %d, want the blocked code 2 for a name that matches nothing:\n%s", code, out)
	}
	if !strings.Contains(out, "no cache repository matching REPO_GIT="+payload) {
		t.Errorf("the name did not travel as data:\n%s", out)
	}
	if _, err := os.Stat(pwned); !os.IsNotExist(err) {
		t.Errorf("the repository name was executed as shell syntax (stat: %v)", err)
	}
}
