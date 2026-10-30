// The scenario fixture behind the two user-perspective terminal-class cases
// (TC-uat-lifecycle-01, TC-uat-observability-01) is a shell script whose only
// cluster-facing acts are a task dispatch, a cancel, and a Job deadline
// patch. None of them is observable without a cluster, so uat-scenarios.sh
// runs here against stubs that answer the Foreman projection, the metrics
// exposition, the kubectl Job reads and the CLI's issue/run verbs; the stubs
// keep what each act wrote, which is what the cases judge.
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

const (
	uatScenarioMainAgent  = "foreman-test-agent"
	uatScenarioFaultAgent = "foreman-test-agent-failing"
	uatScenarioForemanURL = "http://foreman.test.local:8080"
	uatScenarioJobNS      = "multica-agents"
)

// uatScenarioScriptPath returns the live scenario builder next to this test.
func uatScenarioScriptPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the scenario builder from the test source")
	}
	return filepath.Join(filepath.Dir(file), "uat-scenarios.sh")
}

// uatScenarioStubs writes the operator-side commands the builder calls. The
// stubs share $FAKE_STATE: the CLI owns issue/run state, kubectl owns the Job
// patch, and curl serves the projection and the counters that state implies.
func uatScenarioStubs(t *testing.T, bin string) {
	t.Helper()
	cli := `#!/bin/sh
set -eu
S=${FAKE_STATE:?}
FAULT=${FAKE_FAULT_AGENT:-` + uatScenarioFaultAgent + `}
fail() { echo "fake multica-test: $*" >&2; exit 1; }

terminal() { # <task> <result> <reason>
	mkdir -p "$S/tasks/$1"
	printf 'terminal' >"$S/tasks/$1/phase"
	printf '%s' "$2" >"$S/tasks/$1/result"
	printf '%s' "$3" >"$S/tasks/$1/reason"
}

cmd=$1
shift
case $cmd in
issue)
	sub=$1
	shift
	case $sub in
	create)
		title=
		allow_duplicate=0
		while [ $# -gt 0 ]; do
			case $1 in
			--title) title=$2; shift 2 ;;
			--allow-duplicate) allow_duplicate=1; shift ;;
			*) shift ;;
			esac
		done
		# Every issue this fixture creates stays active for good (nothing
		# completes it), which is the state the real CLI rejects a repeated
		# title in: a builder that does not waive the guard cannot run twice.
		if [ "$allow_duplicate" -eq 0 ]; then
			for f in "$S"/issues/*.title; do
				[ -f "$f" ] || continue
				[ "$(cat "$f")" = "$title" ] || continue
				echo "fake multica-test: Active duplicate issue exists: $(basename "$f" .title) ($title). Set allow_duplicate=true or use --allow-duplicate to create another." >&2
				exit 1
			done
		fi
		n=$(cat "$S/issue_counter" 2>/dev/null || echo 0)
		n=$((n + 1))
		printf '%s' "$n" >"$S/issue_counter"
		id="issue-$n"
		mkdir -p "$S/issues"
		printf '%s' "$title" >"$S/issues/$id.title"
		case $title in
		*completed*) printf 'completed' >"$S/issues/$id.class" ;;
		*failed*) printf 'failed' >"$S/issues/$id.class" ;;
		*cancelled*) printf 'cancelled' >"$S/issues/$id.class" ;;
		*timeout*) printf 'timeout' >"$S/issues/$id.class" ;;
		*) printf 'unknown' >"$S/issues/$id.class" ;;
		esac
		printf '{"id":"%s"}\n' "$id"
		;;
	assign)
		id=$1
		shift
		agent=
		while [ $# -gt 0 ]; do
			case $1 in
			--to) agent=$2; shift 2 ;;
			*) shift ;;
			esac
		done
		cls=$(cat "$S/issues/$id.class")
		n=$(cat "$S/task_counter" 2>/dev/null || echo 0)
		n=$((n + 1))
		printf '%s' "$n" >"$S/task_counter"
		tid="task-$n"
		mkdir -p "$S/tasks/$tid"
		printf '%s' "$agent" >"$S/tasks/$tid/agent"
		printf '%s' "$cls" >"$S/tasks/$tid/class"
		printf '%s' "$tid" >"$S/issues/$id.run"
		printf 'job-%s' "$tid" >"$S/tasks/$tid/job"
		# A non-failed task starts by default; FAKE_QUEUED models a run the
		# scheduler still holds before any Job reports a start.
		phase=running
		if [ "${FAKE_QUEUED:-}" = "1" ] && [ "$cls" != failed ]; then
			phase=queued
		fi
		printf '%s' "$phase" >"$S/tasks/$tid/phase"
		if [ "$phase" = running ]; then
			printf '2026-01-01T00:00:00Z' >"$S/tasks/$tid/started_at"
		fi
		case $cls in
		completed) terminal "$tid" completed "" ;;
		# The failure agent's task is the daemon's own failure report, so its
		# attribution is empty; FAKE_FAILURE_REASON models a failure Foreman
		# decided instead, which the class must refuse.
		failed) terminal "$tid" failed "${FAKE_FAILURE_REASON:-}" ;;
		cancelled)
			quota=$(cat "$S/complete_before_cancel" 2>/dev/null || echo 0)
			if [ "$quota" -gt 0 ]; then
				printf '%s' "$((quota - 1))" >"$S/complete_before_cancel"
				terminal "$tid" completed ""
			fi
			;;
		*) : ;;
		esac
		printf '{}\n'
		;;
	runs)
		id=$1
		shift
		active=0
		for a in "$@"; do
			[ "$a" = "--active" ] && active=1
		done
		if [ ! -f "$S/issues/$id.run" ]; then
			printf '[]\n'
			exit 0
		fi
		run=$(cat "$S/issues/$id.run")
		phase=$(cat "$S/tasks/$run/phase" 2>/dev/null || echo running)
		if [ "$active" -eq 1 ] && [ "$phase" = terminal ]; then
			printf '[]\n'
		else
			printf '[{"id":"%s"}]\n' "$run"
		fi
		;;
	cancel-task)
		rid=$1
		phase=$(cat "$S/tasks/$rid/phase" 2>/dev/null || echo missing)
		printf '%s\t%s\n' "$rid" "$phase" >>"$S/cancels.log"
		case $phase in
		running | queued) : ;;
		*) fail "run $rid is not in flight (phase $phase)" ;;
		esac
		# The four shapes the entry must tell apart: the cancel took effect, the
		# run completed first (a lost race), the run failed on its own first
		# (empty attribution, another lost race), or Foreman decided a
		# cancelled run failed (a defect, never a race).
		quota=$(cat "$S/cancel_loses_race" 2>/dev/null || echo 0)
		if [ "$quota" -gt 0 ]; then
			printf '%s' "$((quota - 1))" >"$S/cancel_loses_race"
			terminal "$rid" completed ""
			printf '{}\n'
			exit 0
		fi
		quota=$(cat "$S/cancel_self_fail" 2>/dev/null || echo 0)
		if [ "$quota" -gt 0 ]; then
			printf '%s' "$((quota - 1))" >"$S/cancel_self_fail"
			terminal "$rid" failed ""
			printf '{}\n'
			exit 0
		fi
		if [ -n "${FAKE_CANCEL_ATTRIBUTED:-}" ]; then
			terminal "$rid" failed "$FAKE_CANCEL_ATTRIBUTED"
			printf '{}\n'
			exit 0
		fi
		terminal "$rid" cancelled ""
		printf '{}\n'
		;;
	*) fail "unhandled issue subcommand: $sub" ;;
	esac
	;;
agent)
	sub=$1
	shift
	case $sub in
	list)
		if [ "${FAKE_NO_FAULT_AGENT:-}" = "1" ]; then
			printf '[{"id":"agent-main","name":"%s","archived_at":null}]\n' ` + uatScenarioMainAgent + `
		else
			printf '[{"id":"agent-main","name":"%s","archived_at":null},{"id":"agent-fault","name":"%s","archived_at":null}]\n' ` + uatScenarioMainAgent + ` "$FAULT"
		fi
		;;
	tasks)
		aid=$1
		name=` + uatScenarioMainAgent + `
		[ "$aid" = agent-fault ] && name=$FAULT
		first=1
		printf '['
		for d in "$S"/tasks/*/; do
			[ -d "$d" ] || continue
			grep -qx "$name" "$d/agent" || continue
			tid=$(basename "$d")
			result=
			if [ "$(cat "$d/phase" 2>/dev/null || true)" = terminal ]; then
				result=$(cat "$d/result")
			fi
			[ "$first" -eq 0 ] && printf ','
			first=0
			printf '{"id":"%s","result":"%s"}' "$tid" "$result"
		done
		printf ']\n'
		;;
	*) fail "unhandled agent subcommand: $sub" ;;
	esac
	;;
*) fail "unhandled command: $cmd" ;;
esac
`
	kubectl := `#!/bin/sh
set -eu
S=${FAKE_STATE:?}
terminal() { # <task> <result> <reason>
	mkdir -p "$S/tasks/$1"
	printf 'terminal' >"$S/tasks/$1/phase"
	printf '%s' "$2" >"$S/tasks/$1/result"
	printf '%s' "$3" >"$S/tasks/$1/reason"
}
cmd=
tid=
for a in "$@"; do
	case $a in
	get | patch) [ -z "$cmd" ] && cmd=$a ;;
	foreman.tsic.top/task-id=*) tid=${a#foreman.tsic.top/task-id=} ;;
	esac
done
case $cmd in
get)
	[ -n "$tid" ] || exit 1
	[ -f "$S/tasks/$tid/job" ] || exit 0
	cat "$S/tasks/$tid/job"
	;;
patch)
	name=$(printf '%s\n' "$@" | awk '/^patch$/{getline; getline; print; exit}')
	body=$(printf '%s\n' "$@" | awk '/^-p$/{getline; print; exit}')
	printf '%s\t%s\n' "$name" "$body" >>"$S/patches.log"
	task=${name#job-}
	[ -d "$S/tasks/$task" ] || { echo "fake kubectl: no task for job $name" >&2; exit 1; }
	quota=$(cat "$S/complete_before_patch" 2>/dev/null || echo 0)
	if [ "$quota" -gt 0 ]; then
		printf '%s' "$((quota - 1))" >"$S/complete_before_patch"
		terminal "$task" completed ""
		exit 0
	fi
	reason=$(cat "$S/patch_reason" 2>/dev/null || echo job_deadline_exceeded)
	terminal "$task" failed "$reason"
	;;
*) echo "fake kubectl: unhandled: $*" >&2; exit 1 ;;
esac
`
	curlStub := `#!/bin/sh
set -eu
S=${FAKE_STATE:?}
url=
for a in "$@"; do
	case $a in
	http*) url=$a ;;
	esac
done
case $url in
*/foreman/tasks/*)
	tid=${url##*/}
	if [ ! -d "$S/tasks/$tid" ]; then
		echo "not found" >&2
		exit 22
	fi
	started=$(cat "$S/tasks/$tid/started_at" 2>/dev/null || echo "")
	phase=$(cat "$S/tasks/$tid/phase" 2>/dev/null || echo running)
	if [ "$phase" = terminal ]; then
		result=$(cat "$S/tasks/$tid/result")
		reason=$(cat "$S/tasks/$tid/reason" 2>/dev/null || echo "")
		printf '{"task_id":"%s","state":"terminal","result":"%s","failure_reason":"%s","started_at":"%s","daemon_registered_at":"%s"}\n' \
			"$tid" "$result" "$reason" "$started" "$started"
	else
		printf '{"task_id":"%s","state":"%s","started_at":"%s","daemon_registered_at":"%s"}\n' \
			"$tid" "$phase" "$started" "$started"
	fi
	;;
*/metrics)
	completed=0
	failed=0
	cancelled=0
	for d in "$S"/tasks/*/; do
		[ -d "$d" ] || continue
		[ "$(cat "$d/phase" 2>/dev/null || true)" = terminal ] || continue
		case $(cat "$d/result") in
		completed) completed=$((completed + 1)) ;;
		failed) failed=$((failed + 1)) ;;
		cancelled) cancelled=$((cancelled + 1)) ;;
		esac
	done
	printf 'foreman_tasks_total{result="completed"} %d\n' "$completed"
	printf 'foreman_tasks_total{result="failed"} %d\n' "$failed"
	printf 'foreman_tasks_total{result="cancelled"} %d\n' "$cancelled"
	;;
*) echo "fake curl: unexpected url $url" >&2; exit 22 ;;
esac
`
	for name, body := range map[string]string{
		"multica-test": cli,
		"kubectl":      kubectl,
		"curl":         curlStub,
	} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatalf("write fake %s: %v", name, err)
		}
	}
}

// uatScenarioSandbox lays out the paths the builder resolves: its own
// directory (the script and uat-lib.sh), the CLI wrapper's test-multica
// directory, the stub bin and the shared fake state.
type uatScenarioSandbox struct {
	dir    string
	bin    string
	state  string
	window string
	mcli   string
}

func uatScenarioNewSandbox(t *testing.T) uatScenarioSandbox {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skipf("host has no sh: %v", err)
	}
	dir := t.TempDir()
	box := uatScenarioSandbox{
		dir:    dir,
		bin:    filepath.Join(dir, "bin"),
		state:  filepath.Join(dir, "state"),
		window: filepath.Join(dir, "window"),
		mcli:   filepath.Join(dir, "testinfra", "test-multica"),
	}
	for _, d := range []string{box.bin, box.state, box.mcli, filepath.Join(dir, "home")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("create %s: %v", d, err)
		}
	}
	uatScenarioStubs(t, box.bin)
	// The CLI wrapper must sit where uat-lib's mcli resolves it.
	body, err := os.ReadFile(filepath.Join(box.bin, "multica-test"))
	if err != nil {
		t.Fatalf("read the stub CLI: %v", err)
	}
	if err := os.WriteFile(filepath.Join(box.mcli, "multica-test"), body, 0o755); err != nil {
		t.Fatalf("install the stub CLI: %v", err)
	}
	return box
}

// carrierDir copies the live builder and its library beside each other: the
// script resolves uat-lib.sh from its own directory.
func uatScenarioCarrierDir(t *testing.T, box uatScenarioSandbox) string {
	t.Helper()
	dir := filepath.Join(box.dir, "tests", "acceptance")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create %s: %v", dir, err)
	}
	for _, name := range []string{"uat-scenarios.sh", "uat-lib.sh"} {
		source := filepath.Join(filepath.Dir(uatScenarioScriptPath(t)), name)
		body, err := os.ReadFile(source)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		mode := os.FileMode(0o644)
		if name == "uat-scenarios.sh" {
			mode = 0o755
		}
		if err := os.WriteFile(filepath.Join(dir, name), body, mode); err != nil {
			t.Fatalf("copy %s: %v", name, err)
		}
	}
	return filepath.Join(dir, "uat-scenarios.sh")
}

func uatScenarioEnv(box uatScenarioSandbox, overrides map[string]string) []string {
	merged := map[string]string{}
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i > 0 {
			merged[kv[:i]] = kv[i+1:]
		}
	}
	merged["PATH"] = box.bin + string(os.PathListSeparator) + merged["PATH"]
	merged["HOME"] = filepath.Join(box.dir, "home")
	merged["TMPDIR"] = box.dir
	base := map[string]string{
		"FAKE_STATE":               box.state,
		"FAKE_FAULT_AGENT":         uatScenarioFaultAgent,
		"FOREMAN_URL":              uatScenarioForemanURL,
		"FOREMAN_JOB_NAMESPACE":    uatScenarioJobNS,
		"TEST_MULTICA_DIR":         box.mcli,
		"TEST_MULTICA_AGENT":       uatScenarioMainAgent,
		"TEST_MULTICA_FAULT_AGENT": uatScenarioFaultAgent,
		"SCENARIOS_STATE":          box.window,
		"SCENARIO_WAIT":            "60",
		"SCENARIO_START_WAIT":      "30",
	}
	for key, value := range base {
		merged[key] = value
	}
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

type uatScenarioResult struct {
	out  string
	code int
}

func uatScenarioRun(t *testing.T, box uatScenarioSandbox, script string, overrides map[string]string, args ...string) uatScenarioResult {
	t.Helper()
	cmd := exec.Command("sh", append([]string{script}, args...)...)
	cmd.Dir = box.dir
	cmd.Env = uatScenarioEnv(box, overrides)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return uatScenarioResult{out: string(out), code: 0}
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return uatScenarioResult{out: string(out), code: exitErr.ExitCode()}
	}
	t.Fatalf("run %s %v: %v", script, args, err)
	return uatScenarioResult{}
}

// uatScenarioLibProbe sources uat-lib.sh in a shell and returns what the probe
// printed. Every probe goes through here: a bare exec.Command inherits the
// host environment, and the library's SCENARIOS_STATE default resolves under
// $HOME — the probe would then read a window this test never wrote, so a
// judgement would pass or fail on the machine's leftovers instead of on the
// sandbox's window.
func uatScenarioLibProbe(t *testing.T, box uatScenarioSandbox, lib, probe string, args ...string) string {
	t.Helper()
	cmd := exec.Command("sh", append([]string{"-c", probe, "probe", lib}, args...)...)
	cmd.Env = append(os.Environ(),
		"SCENARIOS_STATE="+box.window,
		"TEST_MULTICA_DIR="+box.mcli,
	)
	out, err := cmd.CombinedOutput()
	uatScenarioRequire(t, err == nil, "probe failed: %v\n%s", err, out)
	return string(out)
}

// uatScenarioReadFile reads a stub artefact; a missing one is empty, which is
// how a call that never happened reads.
func uatScenarioReadFile(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.TrimSpace(string(body))
}

// uatScenarioState parses the window the builder wrote.
func uatScenarioState(t *testing.T, box uatScenarioSandbox) map[string]string {
	t.Helper()
	body := uatScenarioReadFile(t, box.window)
	values := map[string]string{}
	for _, line := range strings.Split(body, "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" {
			continue
		}
		values[key] = value
	}
	return values
}

func uatScenarioClasses(t *testing.T, box uatScenarioSandbox) map[string]string {
	t.Helper()
	classes := map[string]string{}
	entries, err := os.ReadDir(filepath.Join(box.state, "tasks"))
	if err != nil {
		if os.IsNotExist(err) {
			return classes
		}
		t.Fatalf("read the task directory: %v", err)
	}
	for _, entry := range entries {
		classes[entry.Name()] = uatScenarioReadFile(t, filepath.Join(box.state, "tasks", entry.Name(), "class"))
	}
	return classes
}

func uatScenarioTaskOfClass(t *testing.T, box uatScenarioSandbox, class string) string {
	t.Helper()
	for task, got := range uatScenarioClasses(t, box) {
		if got == class {
			return task
		}
	}
	t.Fatalf("no %s task was built", class)
	return ""
}

func uatScenarioRequire(t *testing.T, ok bool, format string, args ...any) {
	t.Helper()
	if !ok {
		t.Fatalf(format, args...)
	}
}

// TestUatScenariosPrepareBuildsEveryTerminalClass judges the fixture's product:
// one task per terminal class, each built by the mechanism its class needs —
// the failure agent for the failure, a server-side cancel for the cancelled
// run, a Job deadline compression for the platform timeout — and one window
// file that names all four together with the counters taken before them.
func TestUatScenariosPrepareBuildsEveryTerminalClass(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	script := uatScenarioCarrierDir(t, box)

	run := uatScenarioRun(t, box, script, nil, "prepare")
	uatScenarioRequire(t, run.code == 0, "prepare exited %d:\n%s", run.code, run.out)

	window := uatScenarioState(t, box)
	// The carriers only accept a window a successful prepare completed: the
	// status marker and the timestamp are that proof.
	uatScenarioRequire(t, window["STATUS"] == "complete", "the window status is %q, want complete:\n%s", window["STATUS"], run.out)
	uatScenarioRequire(t, window["PREPARED_AT"] != "", "the window carries no PREPARED_AT:\n%s", run.out)
	for _, key := range []string{"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"} {
		uatScenarioRequire(t, window[key] != "", "the window carries no %s task:\n%s", key, run.out)
	}
	for _, key := range []string{"COMPLETED_BEFORE", "FAILED_BEFORE", "CANCELLED_BEFORE"} {
		uatScenarioRequire(t, window[key] != "", "the window carries no %s baseline:\n%s", key, run.out)
	}

	// The failure class is the failure agent's: the fixture's guarantee, not a
	// mutated model on the working agent.
	classes := uatScenarioClasses(t, box)
	uatScenarioRequire(t, classes[window["FAILED"]] == "failed",
		"FAILED=%s is not the failed-class task: %v", window["FAILED"], classes)
	uatScenarioRequire(t, uatScenarioReadFile(t, filepath.Join(box.state, "tasks", window["FAILED"], "agent")) == uatScenarioFaultAgent,
		"the failed task did not run on the failure agent: %s",
		uatScenarioReadFile(t, filepath.Join(box.state, "tasks", window["FAILED"], "agent")))
	// §4.1: a daemon self-reported failure carries no attribution. A
	// Foreman-decided one (boot timeout, Job failure) would mean the fixture
	// collected an environment fault as the failure agent's own failure.
	uatScenarioRequire(t, uatScenarioReadFile(t, filepath.Join(box.state, "tasks", window["FAILED"], "reason")) == "",
		"the failed sample carries a failure attribution, want the daemon's empty form")

	// The cancelled class is a server-side cancel of a run that was in flight.
	cancels := strings.Split(uatScenarioReadFile(t, filepath.Join(box.state, "cancels.log")), "\n")
	uatScenarioRequire(t, len(cancels) == 1 && strings.TrimSpace(cancels[0]) != "",
		"want exactly one cancel call, got %q", cancels)
	field := strings.Split(cancels[0], "\t")
	uatScenarioRequire(t, len(field) == 2 && field[0] == window["CANCELLED"],
		"the cancel names %q, want the cancelled task %s", cancels[0], window["CANCELLED"])
	uatScenarioRequire(t, len(field) == 2 && field[1] == "running",
		"the cancel was issued while the run was %q, want running — the case is about an in-flight cancel", field[1])

	// The timeout class is the Job's own deadline: the patch must be the
	// platform field, and the settled attribution must be the deadline one.
	patches := strings.Split(uatScenarioReadFile(t, filepath.Join(box.state, "patches.log")), "\n")
	uatScenarioRequire(t, len(patches) == 1 && strings.TrimSpace(patches[0]) != "",
		"want exactly one Job patch, got %q", patches)
	patch := strings.Split(patches[0], "\t")
	uatScenarioRequire(t, len(patch) == 2 && patch[0] == "job-"+window["TIMEOUT"],
		"the patch names %q, want the timeout task's Job job-%s", patches[0], window["TIMEOUT"])
	uatScenarioRequire(t, strings.Contains(patch[1], `"activeDeadlineSeconds":1`),
		"the patch does not compress the platform deadline: %s", patch[1])
	uatScenarioRequire(t, uatScenarioReadFile(t, filepath.Join(box.state, "tasks", window["TIMEOUT"], "reason")) == "job_deadline_exceeded",
		"the timeout task settled without the deadline attribution")

	// The window is what the carriers read: one line per value, no extra task
	// ids hiding in it.
	for _, key := range []string{"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"} {
		uatScenarioRequire(t, strings.Count(uatScenarioReadFile(t, box.window), key+"=") == 1,
			"the window states %s more than once", key)
	}

	// Nothing the builder gave up on may still be in flight: the observability
	// case reads the in-flight gauge after this window, so a leftover would
	// fail INFLIGHT_ZERO=1 for a legal race.
	for task := range classes {
		phase := uatScenarioReadFile(t, filepath.Join(box.state, "tasks", task, "phase"))
		uatScenarioRequire(t, phase == "terminal", "task %s is still %q after the window settled", task, phase)
	}
}

// TestUatScenariosTimeoutRebuildsWhenTheTaskFinishedFirst judges the one race
// the compression can lose: the agent completes before the patch lands. That
// task never met the platform's deadline, so it is not a timeout sample — the
// builder must discard it and build another, and the window must name the
// rebuilt task, never the escaped one.
func TestUatScenariosTimeoutRebuildsWhenTheTaskFinishedFirst(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	if err := os.WriteFile(filepath.Join(box.state, "complete_before_patch"), []byte("1"), 0o644); err != nil {
		t.Fatalf("arm the first-attempt completion: %v", err)
	}
	script := uatScenarioCarrierDir(t, box)

	run := uatScenarioRun(t, box, script, nil, "timeout")
	uatScenarioRequire(t, run.code == 0, "timeout exited %d:\n%s", run.code, run.out)
	uatScenarioRequire(t, strings.Contains(run.out, "completed before the deadline acted"),
		"the builder did not report the discarded attempt:\n%s", run.out)

	task := strings.TrimSpace(strings.Split(strings.TrimSpace(run.out), "\n")[len(strings.Split(strings.TrimSpace(run.out), "\n"))-1])
	classes := uatScenarioClasses(t, box)
	count := 0
	for _, class := range classes {
		if class == "timeout" {
			count++
		}
	}
	uatScenarioRequire(t, count == 2, "want two timeout tasks (one discarded, one built), got %d: %v", count, classes)
	uatScenarioRequire(t, classes[task] == "timeout", "the printed task %q is not the rebuilt timeout task: %v", task, classes)
	uatScenarioRequire(t, uatScenarioReadFile(t, filepath.Join(box.state, "tasks", task, "reason")) == "job_deadline_exceeded",
		"the rebuilt task did not settle on the deadline attribution")
	for id := range classes {
		phase := uatScenarioReadFile(t, filepath.Join(box.state, "tasks", id, "phase"))
		uatScenarioRequire(t, phase == "terminal", "task %s is still %q after the rebuild", id, phase)
	}
}

// TestUatScenariosTimeoutRefusesAForeignAttribution judges the other side of
// the same branch: the Job was terminated by its deadline but Foreman
// attributed it to something else. That is a product defect, not a lost race,
// so it must fail the fixture instead of being retried away.
func TestUatScenariosTimeoutRefusesAForeignAttribution(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	if err := os.WriteFile(filepath.Join(box.state, "patch_reason"), []byte("job_failed"), 0o644); err != nil {
		t.Fatalf("arm the foreign attribution: %v", err)
	}
	script := uatScenarioCarrierDir(t, box)

	run := uatScenarioRun(t, box, script, nil, "timeout")
	uatScenarioRequire(t, run.code == 1, "the builder accepted a non-deadline attribution (exit %d):\n%s", run.code, run.out)
	uatScenarioRequire(t, strings.Contains(run.out, "want job_deadline_exceeded"),
		"the refusal does not name the expected attribution:\n%s", run.out)
}

// TestUatScenariosCancelledRebuildsWhenTheRunSettledFirst judges the cancel
// race: a run that finished before the server-side cancel landed never met the
// cancellation, so it is not a cancelled sample. The builder must notice the
// terminal state, discard it and build another.
func TestUatScenariosCancelledRebuildsWhenTheRunSettledFirst(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	if err := os.WriteFile(filepath.Join(box.state, "complete_before_cancel"), []byte("1"), 0o644); err != nil {
		t.Fatalf("arm the first-attempt completion: %v", err)
	}
	script := uatScenarioCarrierDir(t, box)

	run := uatScenarioRun(t, box, script, nil, "cancelled")
	uatScenarioRequire(t, run.code == 0, "cancelled exited %d:\n%s", run.code, run.out)
	uatScenarioRequire(t, strings.Contains(run.out, "settled before the cancel landed"),
		"the builder did not report the discarded attempt:\n%s", run.out)

	lines := strings.Split(strings.TrimSpace(run.out), "\n")
	task := strings.TrimSpace(lines[len(lines)-1])
	classes := uatScenarioClasses(t, box)
	count := 0
	for _, class := range classes {
		if class == "cancelled" {
			count++
		}
	}
	uatScenarioRequire(t, count == 2, "want two cancelled tasks (one discarded, one built), got %d: %v", count, classes)
	uatScenarioRequire(t, uatScenarioReadFile(t, filepath.Join(box.state, "tasks", task, "result")) == "cancelled",
		"the printed task %q is not the cancelled one", task)
}

// TestUatScenariosPrepareLeavesNoReadableWindowWhenItFails judges the window's
// freshness: a prepare that dies part-way must not leave the previous round's
// window readable, or both terminal-class carriers would judge last round's
// tasks and baselines as this round's work and pass without one sample.
func TestUatScenariosPrepareLeavesNoReadableWindowWhenItFails(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	previous := "STATUS=complete\nPREPARED_AT=2026-01-01T00:00:00Z\nCOMPLETED=task-1\nFAILED=task-2\n"
	if err := os.WriteFile(box.window, []byte(previous), 0o644); err != nil {
		t.Fatalf("seed the previous window: %v", err)
	}
	script := uatScenarioCarrierDir(t, box)

	run := uatScenarioRun(t, box, script, map[string]string{"FAKE_NO_FAULT_AGENT": "1"}, "prepare")
	uatScenarioRequire(t, run.code == 2, "prepare without the failure agent exited %d, want blocked:\n%s", run.code, run.out)

	window := uatScenarioState(t, box)
	uatScenarioRequire(t, window["STATUS"] == "partial",
		"a failed prepare left the window at %q, want the partial placeholder", window["STATUS"])
	for _, key := range []string{"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "COMPLETED_BEFORE", "FAILED_BEFORE", "CANCELLED_BEFORE"} {
		uatScenarioRequire(t, window[key] == "",
			"a failed prepare left the previous round's %s readable: %v", key, window)
	}

	lib := filepath.Join(filepath.Dir(uatScenarioScriptPath(t)), "uat-lib.sh")
	probe := `set -eu
. "$1"
if scenario_window_ready; then echo ready; else echo not-ready; fi
`
	cmd := exec.Command("sh", "-c", probe, "probe", lib)
	cmd.Env = append(os.Environ(), "SCENARIOS_STATE="+box.window)
	out, err := cmd.CombinedOutput()
	uatScenarioRequire(t, err == nil, "probe failed: %v\n%s", err, out)
	uatScenarioRequire(t, strings.Contains(string(out), "not-ready"),
		"a partial window read as ready:\n%s", out)
}

// TestUatScenariosPrepareRerunsInTheSameWorkspace judges the second round: the
// environment's issues stay active after the round that created them, so the
// CLI refuses another issue with the same title unless the create waives the
// duplicate guard. A builder that does not waive it builds one round per
// workspace and dies on the next; the second window must name four fresh
// tasks, never the first round's.
func TestUatScenariosPrepareRerunsInTheSameWorkspace(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	script := uatScenarioCarrierDir(t, box)

	first := uatScenarioRun(t, box, script, nil, "prepare")
	uatScenarioRequire(t, first.code == 0, "the first prepare exited %d:\n%s", first.code, first.out)
	firstWindow := uatScenarioState(t, box)
	uatScenarioRequire(t, firstWindow["STATUS"] == "complete",
		"the first window is %q, want complete:\n%s", firstWindow["STATUS"], first.out)

	second := uatScenarioRun(t, box, script, nil, "prepare")
	uatScenarioRequire(t, second.code == 0,
		"the second prepare in the same workspace exited %d:\n%s", second.code, second.out)
	secondWindow := uatScenarioState(t, box)
	uatScenarioRequire(t, secondWindow["STATUS"] == "complete",
		"the second window is %q, want complete:\n%s", secondWindow["STATUS"], second.out)
	uatScenarioRequire(t, secondWindow["PREPARED_AT"] != "",
		"the second window carries no PREPARED_AT:\n%s", second.out)

	classes := uatScenarioClasses(t, box)
	for key, class := range map[string]string{
		"COMPLETED": "completed",
		"FAILED":    "failed",
		"CANCELLED": "cancelled",
		"TIMEOUT":   "timeout",
	} {
		task := secondWindow[key]
		uatScenarioRequire(t, task != "", "the second window carries no %s task:\n%s", key, second.out)
		uatScenarioRequire(t, task != firstWindow[key],
			"the second round reused the first round's %s task %s:\n%s", key, task, second.out)
		uatScenarioRequire(t, classes[task] == class,
			"the second window's %s task %s is %q, want %s: %v", key, task, classes[task], class, classes)
	}
}

// TestUatScenariosPrepareHealsItsPartialWindowOnTheNextRun judges the other
// half of re-runnability: a prepare that dies part-way leaves the placeholder,
// never a readable window, and the next run in the same workspace completes it
// — the placeholder marks a failed round, it does not lock the workspace.
func TestUatScenariosPrepareHealsItsPartialWindowOnTheNextRun(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	previous := "STATUS=complete\nPREPARED_AT=2026-01-01T00:00:00Z\nCOMPLETED=task-previous-a\nFAILED=task-previous-b\n"
	if err := os.WriteFile(box.window, []byte(previous), 0o644); err != nil {
		t.Fatalf("seed the previous window: %v", err)
	}
	script := uatScenarioCarrierDir(t, box)

	failed := uatScenarioRun(t, box, script, map[string]string{"FAKE_NO_FAULT_AGENT": "1"}, "prepare")
	uatScenarioRequire(t, failed.code == 2, "the blocked prepare exited %d:\n%s", failed.code, failed.out)
	uatScenarioRequire(t, uatScenarioState(t, box)["STATUS"] == "partial",
		"the blocked prepare left %q, want the partial placeholder:\n%s", uatScenarioState(t, box)["STATUS"], failed.out)

	run := uatScenarioRun(t, box, script, nil, "prepare")
	uatScenarioRequire(t, run.code == 0,
		"prepare did not recover from the placeholder it left (exit %d):\n%s", run.code, run.out)
	window := uatScenarioState(t, box)
	uatScenarioRequire(t, window["STATUS"] == "complete",
		"the healed window is %q, want complete:\n%s", window["STATUS"], run.out)
	uatScenarioRequire(t, window["PREPARED_AT"] != "",
		"the healed window carries no PREPARED_AT:\n%s", run.out)

	classes := uatScenarioClasses(t, box)
	for _, key := range []string{"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"} {
		task := window[key]
		uatScenarioRequire(t, task != "",
			"the healed window carries no %s task:\n%s", key, run.out)
		uatScenarioRequire(t, !strings.HasPrefix(task, "task-previous-"),
			"the healed window still names the previous round's %s task: %v", key, window)
		uatScenarioRequire(t, classes[task] != "",
			"the healed window's %s task %s does not exist: %v", key, task, classes)
	}
}

// TestUatScenariosCancelledRebuildsWhenTheRunCompletedFirst judges the legal
// race the cancel can lose: the run finishes before the cancellation settles.
// That task never met the cancellation, so it is not a cancelled sample — the
// builder must discard it and build another, exactly like the timeout class.
func TestUatScenariosCancelledRebuildsWhenTheRunCompletedFirst(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	if err := os.WriteFile(filepath.Join(box.state, "cancel_loses_race"), []byte("1"), 0o644); err != nil {
		t.Fatalf("arm the lost race: %v", err)
	}
	script := uatScenarioCarrierDir(t, box)

	run := uatScenarioRun(t, box, script, nil, "cancelled")
	uatScenarioRequire(t, run.code == 0, "cancelled exited %d while the race was legal:\n%s", run.code, run.out)
	uatScenarioRequire(t, strings.Contains(run.out, "completed before the cancel took effect"),
		"the builder did not report the discarded attempt:\n%s", run.out)

	lines := strings.Split(strings.TrimSpace(run.out), "\n")
	task := strings.TrimSpace(lines[len(lines)-1])
	uatScenarioRequire(t, uatScenarioReadFile(t, filepath.Join(box.state, "tasks", task, "result")) == "cancelled",
		"the printed task %q is not the cancelled one", task)
	classes := uatScenarioClasses(t, box)
	count := 0
	for _, class := range classes {
		if class == "cancelled" {
			count++
		}
	}
	uatScenarioRequire(t, count == 2, "want two cancelled tasks (one discarded, one built), got %d: %v", count, classes)
}

// TestUatScenariosCancelledRebuildsWhenTheRunFailedOnItsOwn judges the other
// lost race: the run dies on its own (daemon report, empty attribution) before
// the cancellation lands. Also not a cancelled sample, also rebuilt.
func TestUatScenariosCancelledRebuildsWhenTheRunFailedOnItsOwn(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	if err := os.WriteFile(filepath.Join(box.state, "cancel_self_fail"), []byte("1"), 0o644); err != nil {
		t.Fatalf("arm the self-failure: %v", err)
	}
	script := uatScenarioCarrierDir(t, box)

	run := uatScenarioRun(t, box, script, nil, "cancelled")
	uatScenarioRequire(t, run.code == 0, "cancelled exited %d while the race was legal:\n%s", run.code, run.out)
	uatScenarioRequire(t, strings.Contains(run.out, "failed on its own before the cancel took effect"),
		"the builder did not report the discarded attempt:\n%s", run.out)
}

// TestUatScenariosCancelledRefusesAForemanDecidedFailure: a non-empty
// attribution on a cancelled run is Foreman deciding a cancelled task failed.
// That is a product defect, not a lost race, so it must fail instead of being
// retried away.
func TestUatScenariosCancelledRefusesAForemanDecidedFailure(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	script := uatScenarioCarrierDir(t, box)

	run := uatScenarioRun(t, box, script, map[string]string{"FAKE_CANCEL_ATTRIBUTED": "job_failed"}, "cancelled")
	uatScenarioRequire(t, run.code == 1, "the builder retried a Foreman-decided cancelled failure (exit %d):\n%s", run.code, run.out)
	uatScenarioRequire(t, strings.Contains(run.out, "Foreman decided a cancelled run failed"),
		"the refusal does not name the defect:\n%s", run.out)
}

// TestUatScenariosCancelledRefusesAPreStartRun judges the class's §4.6 face:
// the cancellation is confirmed by the daemon's status poll, so a run that
// never started was never cancelled that way. The builder must refuse the
// sample — and close the run it gave up on, or the queue keeps it in flight.
func TestUatScenariosCancelledRefusesAPreStartRun(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	script := uatScenarioCarrierDir(t, box)

	run := uatScenarioRun(t, box, script, map[string]string{
		"FAKE_QUEUED":           "1",
		"SCENARIO_START_WAIT":   "1",
		"SCENARIO_CANCEL_TRIES": "2",
		"SCENARIO_ABANDON_WAIT": "30",
	}, "cancelled")
	uatScenarioRequire(t, run.code == 1, "the builder collected a pre-start cancel (exit %d):\n%s", run.code, run.out)
	uatScenarioRequire(t, strings.Contains(run.out, "never started"),
		"the refusal does not say why:\n%s", run.out)

	classes := uatScenarioClasses(t, box)
	count := 0
	for _, class := range classes {
		if class == "cancelled" {
			count++
		}
	}
	uatScenarioRequire(t, count == 2, "want one task per attempt, got %d: %v", count, classes)
	for task := range classes {
		phase := uatScenarioReadFile(t, filepath.Join(box.state, "tasks", task, "phase"))
		uatScenarioRequire(t, phase == "terminal", "abandoned run %s is still %q — it must be closed", task, phase)
		uatScenarioRequire(t, uatScenarioReadFile(t, filepath.Join(box.state, "tasks", task, "result")) == "cancelled",
			"abandoned run %s was not settled by a cancel", task)
	}
}

// TestUatScenariosFailedRefusesWithoutTheFailureAgent: the failure class has
// exactly one construction. Without the agent the fixture blocks instead of
// quietly producing a sample some other way.
func TestUatScenariosFailedRefusesWithoutTheFailureAgent(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	script := uatScenarioCarrierDir(t, box)

	run := uatScenarioRun(t, box, script, map[string]string{"FAKE_NO_FAULT_AGENT": "1"}, "failed")
	uatScenarioRequire(t, run.code == 2, "want a blocked exit without the failure agent, got %d:\n%s", run.code, run.out)
	uatScenarioRequire(t, strings.Contains(run.out, uatScenarioFaultAgent),
		"the blocked message does not name the missing agent:\n%s", run.out)
}

// TestUatScenariosFailedRefusesAForemanDecidedFailure judges the sample's shape
// (§4.1): the failure class is the daemon's own report, so it carries no
// attribution, and only a failure Foreman decided for the daemon has one. A
// non-empty attribution on this class means the fixture collected an
// environment fault as the failure agent's failure, so the builder must fail
// instead of taking the sample.
func TestUatScenariosFailedRefusesAForemanDecidedFailure(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	script := uatScenarioCarrierDir(t, box)

	run := uatScenarioRun(t, box, script, map[string]string{"FAKE_FAILURE_REASON": "job_boot_timeout"}, "failed")
	uatScenarioRequire(t, run.code == 1,
		"the builder collected an attributed sample as the failure agent's own (exit %d):\n%s", run.code, run.out)
	uatScenarioRequire(t, strings.Contains(run.out, "is not the failure agent's own report"),
		"the refusal does not name the sample's shape:\n%s", run.out)
}

// TestUatScenarioStateResolvesForTheCarriers judges the window contract the
// two terminal-class carriers rely on: an explicit value wins, the recorded
// one is the default, and a missing window reads empty rather than failing the
// carrier's own argument check.
func TestUatScenarioStateResolvesForTheCarriers(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	complete := "STATUS=complete\nPREPARED_AT=2026-01-01T00:00:00Z\nCOMPLETED=task-7\nFAILED=task-8\n"
	if err := os.WriteFile(box.window, []byte(complete), 0o644); err != nil {
		t.Fatalf("write the window: %v", err)
	}
	lib := filepath.Join(filepath.Dir(uatScenarioScriptPath(t)), "uat-lib.sh")

	probe := `set -eu
. "$1"
printf 'ready=%s\n' "$(scenario_window_ready && echo yes || echo no)"
printf 'recorded=%s\n' "$(scenario_value "" COMPLETED)"
printf 'explicit=%s\n' "$(scenario_value task-9 COMPLETED)"
printf 'absent=%s\n' "$(scenario_value "" TIMEOUT)"
`
	text := uatScenarioLibProbe(t, box, lib, probe)
	for _, want := range []string{"ready=yes", "recorded=task-7", "explicit=task-9", "absent="} {
		uatScenarioRequire(t, strings.Contains(text, want+"\n"), "probe lacks %q:\n%s", want, text)
	}

	// The partial placeholder a failed prepare leaves behind must never read
	// as a window: neither ready nor with the previous round's ids.
	partial := "STATUS=partial\nPREPARED_AT=2026-01-02T00:00:00Z\n"
	if err := os.WriteFile(box.window, []byte(partial), 0o644); err != nil {
		t.Fatalf("write the partial window: %v", err)
	}
	text = uatScenarioLibProbe(t, box, lib, probe)
	uatScenarioRequire(t, strings.Contains(text, "ready=no\n"),
		"a partial window must not be ready:\n%s", text)
	uatScenarioRequire(t, strings.Contains(text, "recorded=\n"),
		"a partial window must carry no task ids:\n%s", text)

	// A complete marker without the timestamp is still not a window.
	if err := os.WriteFile(box.window, []byte("STATUS=complete\nCOMPLETED=task-7\n"), 0o644); err != nil {
		t.Fatalf("write the untimestamped window: %v", err)
	}
	text = uatScenarioLibProbe(t, box, lib, probe)
	uatScenarioRequire(t, strings.Contains(text, "ready=no\n"),
		"a window without PREPARED_AT must not be ready:\n%s", text)

	if err := os.Remove(box.window); err != nil {
		t.Fatalf("remove the window: %v", err)
	}
	text = uatScenarioLibProbe(t, box, lib, probe)
	uatScenarioRequire(t, strings.Contains(text, "ready=no\n"),
		"a missing window must not be ready:\n%s", text)
	uatScenarioRequire(t, strings.Contains(text, "recorded=\n"),
		"a missing window must read empty, not fail the carrier:\n%s", text)
}

// TestUatServerRunFindsTheFailureAgentsRun judges the lookup the lifecycle
// carrier uses for the failed sample: the run belongs to the failure agent, so
// a lookup restricted to the working agent would report the failed task as
// missing and the case would block instead of judging it.
func TestUatServerRunFindsTheFailureAgentsRun(t *testing.T) {
	box := uatScenarioNewSandbox(t)
	script := uatScenarioCarrierDir(t, box)
	run := uatScenarioRun(t, box, script, nil, "failed")
	uatScenarioRequire(t, run.code == 0, "failed exited %d:\n%s", run.code, run.out)
	// The builder reports observations on stdout and prints the task id last.
	lines := strings.Split(strings.TrimSpace(run.out), "\n")
	failed := strings.TrimSpace(lines[len(lines)-1])

	lib := filepath.Join(filepath.Dir(uatScenarioScriptPath(t)), "uat-lib.sh")
	probe := `set -eu
. "$1"
server_run "$2"
`
	cmd := exec.Command("sh", "-c", probe, "probe", lib, failed)
	cmd.Env = append(os.Environ(),
		"FAKE_STATE="+box.state,
		"FAKE_FAULT_AGENT="+uatScenarioFaultAgent,
		"TEST_MULTICA_DIR="+box.mcli,
		"TEST_MULTICA_AGENT="+uatScenarioMainAgent,
		"TEST_MULTICA_FAULT_AGENT="+uatScenarioFaultAgent,
	)
	out, err := cmd.CombinedOutput()
	uatScenarioRequire(t, err == nil, "server_run probe failed: %v\n%s", err, out)
	uatScenarioRequire(t, strings.Contains(string(out), fmt.Sprintf(`"id":"%s"`, failed)),
		"server_run did not find the failure agent's run %s: %s", failed, out)
}
