// TC-tech-job-09's cluster carrier under a fake kubectl: step 5 is the shell
// judgment AC-35 rests on ("向 1 号进程发终止信号后容器终止", no deadline), and
// until now no Go case executed it. The cases pin the verdict rather than its
// wording: a termination after the old fixed +15s sample passes, so does one
// inside the second before the pod is deleted, a container that never
// terminates fails at the guard, while everything unobserved — an ambiguous pod
// set, a vanished pod, a failed status read, a failed signal, an unparsable
// status, a guard outside [1, 3600] — blocks instead of denying it.
package acceptance

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// tcJob09CarrierPath returns the live carrier next to this test file.
func tcJob09CarrierPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the carrier from the test source")
	}
	return filepath.Join(filepath.Dir(file), "tech-job-09-cluster.sh")
}

func tcJob09CarrierWriteExecutable(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// tcJob09CarrierFakeKubectl writes the operator-side command the carrier
// calls: one task Job whose agent container reads running until
// FAKE_TERMINATE_AFTER seconds have passed since the first status read, then
// terminal. Knobs: FAKE_NO_RUNNING (no pod), FAKE_TWO_PODS (a second running
// pod), FAKE_GONE_AFTER (the pod disappears after n seconds), FAKE_FAIL_READS
// (n failing status reads), FAKE_BAD_JSON (a status payload jq cannot parse),
// FAKE_FAIL_SIGNAL (the kill exec fails). Every other call site errors, so a
// new one surfaces instead of feeding the carrier an empty answer.
func tcJob09CarrierFakeKubectl(t *testing.T) string {
	t.Helper()
	bin := t.TempDir()
	tcJob09CarrierWriteExecutable(t, filepath.Join(bin, "kubectl"), `#!/bin/sh
set -u
state=${FAKE_STATE:?}
job='{"spec":{"template":{"spec":{"containers":[{"name":"agent","command":["/usr/bin/dumb-init","--","/usr/local/bin/multica"],"args":["daemon","run"]}]}}}}'
running='{"metadata":{"name":"fm-task-1-abcde"},"status":{"containerStatuses":[{"name":"agent","state":{"running":{}}}]}}'
terminated='{"metadata":{"name":"fm-task-1-abcde"},"status":{"containerStatuses":[{"name":"agent","state":{"terminated":{"exitCode":0}}}]}}'

# The pod status runs on a decisecond clock, not on a poll count: the two knobs
# are seconds from the first status read, whole or fractional, so a case can
# place the termination inside the window before the platform deletes the pod.
# jq carries the sub-second reading (busybox date ignores %N).
now_ds() { jq -n 'now * 10 | floor'; }
to_ds() { jq -n --arg seconds "$1" '($seconds | tonumber * 10) | floor'; }
elapsed_ds() {
	now=$(now_ds)
	[ -f "$state/first-read" ] || echo "$now" >"$state/first-read"
	echo $(( now - $(cat "$state/first-read") ))
}
terminate_ds=$(to_ds "${FAKE_TERMINATE_AFTER:-0}")
gone_ds=$(to_ds "${FAKE_GONE_AFTER:-999999}")

case " $* " in
*" get jobs "*)
	echo fm-task-1 ;;
*" get job "*)
	printf '%s' "$job" ;;
*" exec "*" kill -TERM 1 "*)
	if [ "${FAKE_FAIL_SIGNAL:-0}" = 1 ]; then
		echo 'Error from server: error dialing backend: connection refused' >&2
		exit 1
	fi ;;
*" get pods "*jsonpath*)
	if [ "${FAKE_NO_RUNNING:-0}" = 1 ]; then
		:
	elif [ "${FAKE_TWO_PODS:-0}" = 1 ]; then
		printf 'fm-task-1-abcde fm-task-1-fghij'
	else
		printf 'fm-task-1-abcde'
	fi ;;
*" get pod "*)
	reads=$(( $(cat "$state/reads" 2>/dev/null || echo 0) + 1 ))
	echo "$reads" >"$state/reads"
	if [ "$reads" -le "${FAKE_FAIL_READS:-0}" ]; then
		echo 'Error from server: etcdserver: request timed out' >&2
		exit 1
	fi
	if [ "${FAKE_BAD_JSON:-0}" = 1 ]; then
		printf '{not json'
		exit 0
	fi
	age=$(elapsed_ds)
	if [ "$age" -ge "$gone_ds" ]; then
		case " $* " in
		*" --ignore-not-found "*) exit 0 ;;
		*) echo "Error from server (NotFound): pods \"fm-task-1-abcde\" not found" >&2; exit 1 ;;
		esac
	fi
	if [ "$age" -ge "$terminate_ds" ]; then
		echo "$age" >"$state/terminated-age"
		printf '%s' "$terminated"
	else
		printf '%s' "$running"
	fi ;;
*" exec "*)
	case "$*" in
	*"cat /proc/1/comm"*)
		printf 'dumb-init\n/usr/bin/dumb-init -- /usr/local/bin/multica daemon run\n' ;;
	*"daemon_pid="*)
		printf 'daemon_pid=42\npid1_child=/usr/local/bin/multica daemon run\ndaemon_child=/usr/local/bin/omp serve\nagent_pid=43\n' ;;
	*"z0="*)
		printf 'z0=0 m1=30 z1=0 m2=60 z2=0\n' ;;
	*)
		echo "fake kubectl: unexpected exec: $*" >&2
		exit 99 ;;
	esac ;;
*)
	echo "fake kubectl: unexpected call: $*" >&2
	exit 99 ;;
esac
`)
	return bin
}

func tcJob09CarrierEnv(bin string, overrides map[string]string) []string {
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

// tcJob09CarrierRun drives the carrier with the fake kubectl on PATH and
// returns its combined output with the exit code the verdict is read from.
func tcJob09CarrierRun(t *testing.T, bin, stateDir string, overrides map[string]string) (string, int) {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to drive the carrier")
	}
	env := map[string]string{
		"TASK_ID":                        "task-1",
		"FOREMAN_JOB_NAMESPACE":          "multica-agents",
		"FOREMAN_TERMINATION_HANG_GUARD": "120",
		"FAKE_STATE":                     stateDir,
		"FAKE_TERMINATE_AFTER":           "0",
		"FAKE_GONE_AFTER":                "999999",
		"FAKE_FAIL_READS":                "0",
		"FAKE_NO_RUNNING":                "0",
		"FAKE_TWO_PODS":                  "0",
		"FAKE_BAD_JSON":                  "0",
		"FAKE_FAIL_SIGNAL":               "0",
	}
	for key, value := range overrides {
		env[key] = value
	}
	cmd := exec.Command("sh", tcJob09CarrierPath(t))
	cmd.Env = tcJob09CarrierEnv(bin, env)
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

// tcJob09CarrierTerminationLine pins the observation that has to survive the
// verdict: every run names the measured latency and the configured guard, so a
// slow shutdown stays visible even though it is not judged.
var tcJob09CarrierTerminationLine = regexp.MustCompile(`agent termination: \+([0-9]+\.[0-9])s \(hang guard ([0-9]+)s\): \[\{"exitCode":0\}\]`)

// A shutdown that outlives the old carrier's single +15s sample still passes:
// AC-35 asks that the container terminate, not how fast. The fake keeps the
// container running for 15s of its own clock, so the case covers a shutdown the
// old sample would have rejected as "still running".
func TestJob09CarrierAcceptsTerminationAfterTheSample(t *testing.T) {
	bin := tcJob09CarrierFakeKubectl(t)
	out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{"FAKE_TERMINATE_AFTER": "15"})
	if code != 0 {
		t.Fatalf("carrier exit = %d, want 0 (termination is what AC-35 asks for):\n%s", code, out)
	}
	if !strings.Contains(out, "PASS: TC-tech-job-09") {
		t.Errorf("carrier did not report PASS:\n%s", out)
	}
	m := tcJob09CarrierTerminationLine.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("the termination observation must carry the measured latency and the terminal state:\n%s", out)
	}
	if m[2] != "120" {
		t.Errorf("hang guard = %s, want the default 120 the base env pins:\n%s", m[2], out)
	}
	latency, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		t.Fatalf("latency %q is not seconds: %v", m[1], err)
	}
	if latency < 15 {
		t.Errorf("termination observed at +%ss; the case must not stop before the old +15s sample:\n%s", m[1], out)
	}
}

// The platform deletes the task pod about a second after the container exits,
// so the terminated state lives only inside that window: a cadence coarser than
// the window samples before the termination and again after the deletion, and
// blocks on a container that did terminate. 0.3s and 1.3s are the measured
// window (finishedAt and deletionTimestamp after SIGTERM); the fake records its
// own reading of the terminating poll, so the printed latency is checked
// against it instead of a wall-clock guess.
func TestJob09CarrierObservesATerminationInsideTheDeletionWindow(t *testing.T) {
	bin := tcJob09CarrierFakeKubectl(t)
	state := t.TempDir()
	out, code := tcJob09CarrierRun(t, bin, state, map[string]string{
		"FAKE_TERMINATE_AFTER": "0.3",
		"FAKE_GONE_AFTER":      "1.3",
	})
	if code != 0 {
		t.Fatalf("carrier exit = %d, want 0 (the container terminated before the pod was deleted):\n%s", code, out)
	}
	if !strings.Contains(out, "PASS: TC-tech-job-09") {
		t.Errorf("carrier did not report PASS:\n%s", out)
	}
	m := tcJob09CarrierTerminationLine.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("the termination observation must carry the measured latency and the terminal state:\n%s", out)
	}
	latency, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		t.Fatalf("latency %q is not seconds: %v", m[1], err)
	}
	if latency < 0.3 {
		t.Errorf("termination observed at +%ss, before the fake's 0.3s termination; the observation must keep the sub-second latency:\n%s", m[1], out)
	}
	age, err := os.ReadFile(filepath.Join(state, "terminated-age"))
	if err != nil {
		t.Fatalf("the fake did not record the terminating poll's age: %v", err)
	}
	fakeAge, err := strconv.ParseFloat(strings.TrimSpace(string(age)), 64)
	if err != nil {
		t.Fatalf("fake age %q is not deciseconds: %v", age, err)
	}
	fakeAge /= 10
	if diff := latency - fakeAge; diff > 0.2 || diff < -0.2 {
		t.Errorf("carrier observed +%ss but the terminating poll was at %ss on the fake's clock; a whole-second reading is what this case exists to reject:\n%s", m[1], strconv.FormatFloat(fakeAge, 'f', 1, 64), out)
	}
}

// A container that never terminates is the one shape the bound exists for: it
// fails, and the running container never reads as terminated (an absent
// state.terminated deserialises as null, not as an empty array).
func TestJob09CarrierFailsWhenTheContainerNeverTerminates(t *testing.T) {
	bin := tcJob09CarrierFakeKubectl(t)
	out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{
		"FAKE_TERMINATE_AFTER":           "999999",
		"FOREMAN_TERMINATION_HANG_GUARD": "2",
	})
	if code != 1 {
		t.Fatalf("carrier exit = %d, want 1 (a running container must never pass):\n%s", code, out)
	}
	if !strings.Contains(out, "did not terminate within 2s of SIGTERM") {
		t.Errorf("the failure does not name the guard:\n%s", out)
	}
	if strings.Contains(out, "PASS:") {
		t.Errorf("a running container reached the PASS line:\n%s", out)
	}
}

// Two running pods under one task label leave no single container to judge:
// the carrier blocks and names both instead of summing them into a verdict.
func TestJob09CarrierBlocksOnTwoRunningPods(t *testing.T) {
	bin := tcJob09CarrierFakeKubectl(t)
	out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{"FAKE_TWO_PODS": "1"})
	if code != 2 {
		t.Fatalf("carrier exit = %d, want 2 (ambiguous observation object):\n%s", code, out)
	}
	if !strings.Contains(out, "more than one running pod carries task task-1: fm-task-1-abcde fm-task-1-fghij") {
		t.Errorf("the blocker does not name both pods:\n%s", out)
	}
}

// A pod deleted under the wait takes the terminal state with it, and object
// removal is not termination: the run blocks instead of denying AC-35.
func TestJob09CarrierBlocksWhenThePodDisappears(t *testing.T) {
	bin := tcJob09CarrierFakeKubectl(t)
	out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{
		"FAKE_TERMINATE_AFTER": "999999",
		"FAKE_GONE_AFTER":      "0",
	})
	if code != 2 {
		t.Fatalf("carrier exit = %d, want 2 (an unobserved terminal state is not a verdict):\n%s", code, out)
	}
	if !strings.Contains(out, "is gone with no agent terminal state observed") {
		t.Errorf("the blocker does not name the vanished pod:\n%s", out)
	}
	if strings.Contains(out, "PASS:") {
		t.Errorf("a vanished pod reached the PASS line:\n%s", out)
	}
}

// Nothing to signal means nothing to judge: the carrier blocks rather than
// reporting a termination it never observed.
func TestJob09CarrierBlocksWithoutARunningPod(t *testing.T) {
	bin := tcJob09CarrierFakeKubectl(t)
	out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{"FAKE_NO_RUNNING": "1"})
	if code != 2 {
		t.Fatalf("carrier exit = %d, want 2 (no pod to observe):\n%s", code, out)
	}
	if !strings.Contains(out, "no running pod carries task task-1") {
		t.Errorf("the blocker does not name the missing pod:\n%s", out)
	}
}

// A signal that never reached PID 1 leaves the termination unobserved: the run
// blocks like every other unobserved answer instead of failing AC-35.
func TestJob09CarrierBlocksWhenTheSignalFails(t *testing.T) {
	bin := tcJob09CarrierFakeKubectl(t)
	out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{
		"FAKE_FAIL_SIGNAL":     "1",
		"FAKE_TERMINATE_AFTER": "999999",
	})
	if code != 2 {
		t.Fatalf("carrier exit = %d, want 2 (an unsent signal is not a verdict):\n%s", code, out)
	}
	if !strings.Contains(out, "cannot signal PID 1 (kubectl exec failed)") {
		t.Errorf("the blocker does not name the failed signal:\n%s", out)
	}
	if strings.Contains(out, "FAIL:") {
		t.Errorf("a failed signal exec became a failure:\n%s", out)
	}
	if strings.Contains(out, "PASS:") {
		t.Errorf("a failed signal exec reached the PASS line:\n%s", out)
	}
}

// A status read that fails is an unobserved answer, not a vanished pod: the
// wait keeps polling and the later successful read still decides the verdict.
func TestJob09CarrierRetriesFailedStatusReads(t *testing.T) {
	bin := tcJob09CarrierFakeKubectl(t)
	out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{
		"FAKE_FAIL_READS":      "2",
		"FAKE_TERMINATE_AFTER": "0",
	})
	if code != 0 {
		t.Fatalf("carrier exit = %d, want 0 (a failed read is not a verdict):\n%s", code, out)
	}
	if !strings.Contains(out, "PASS: TC-tech-job-09") {
		t.Errorf("carrier did not report PASS:\n%s", out)
	}
}

// Reads that never succeed leave the question unobserved: the run blocks at the
// guard instead of recording the product as not terminating.
func TestJob09CarrierBlocksWhenStatusReadsNeverSucceed(t *testing.T) {
	bin := tcJob09CarrierFakeKubectl(t)
	out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{
		"FAKE_FAIL_READS":                "999999",
		"FOREMAN_TERMINATION_HANG_GUARD": "2",
	})
	if code != 2 {
		t.Fatalf("carrier exit = %d, want 2 (unreadable status is not a verdict):\n%s", code, out)
	}
	if !strings.Contains(out, "cannot read pod fm-task-1-abcde's status within 2s of SIGTERM") {
		t.Errorf("the blocker does not name the unreadable pod and the guard:\n%s", out)
	}
	if strings.Contains(out, "FAIL:") {
		t.Errorf("an unreadable status became a failure:\n%s", out)
	}
}

// A status payload jq cannot parse is an unobserved answer, not a termination:
// the run blocks and quotes what it read instead of recording a verdict.
func TestJob09CarrierBlocksWhenTheStatusDoesNotParse(t *testing.T) {
	bin := tcJob09CarrierFakeKubectl(t)
	out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{
		"FAKE_BAD_JSON":        "1",
		"FAKE_TERMINATE_AFTER": "999999",
	})
	if code != 2 {
		t.Fatalf("carrier exit = %d, want 2 (an unparsable status is not a verdict):\n%s", code, out)
	}
	if !strings.Contains(out, "cannot read pod fm-task-1-abcde's container status: {not json") {
		t.Errorf("the blocker does not name the pod and quote the unparsable status:\n%s", out)
	}
	if strings.Contains(out, "FAIL:") || strings.Contains(out, "PASS:") {
		t.Errorf("an unparsable status reached a verdict:\n%s", out)
	}
}

// A malformed guard is an operator input error, not a product failure: the run
// blocks before it signals anything and names the variable and the value.
func TestJob09CarrierBlocksOnAMalformedHangGuard(t *testing.T) {
	for _, tc := range []struct{ guard, want string }{
		{"2m", "FOREMAN_TERMINATION_HANG_GUARD must be whole seconds, got '2m'"},
		{"0", "FOREMAN_TERMINATION_HANG_GUARD must be at least 1 second, got '0'"},
	} {
		t.Run(tc.guard, func(t *testing.T) {
			bin := tcJob09CarrierFakeKubectl(t)
			out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{"FOREMAN_TERMINATION_HANG_GUARD": tc.guard})
			if code != 2 {
				t.Fatalf("carrier exit = %d, want 2 (bad input is not a verdict):\n%s", code, out)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("the blocker does not name the variable and the value:\n%s", out)
			}
		})
	}
}

// A guard above the ceiling is bad input too: it blocks before the cluster is
// touched. The container would keep running past the case's short termination,
// which is the shape whose integer comparison used to error and read as a FAIL.
func TestJob09CarrierBlocksOnAHangGuardAboveTheCeiling(t *testing.T) {
	for _, guard := range []string{"3601", "120000", "99999999999999999999"} {
		t.Run(guard, func(t *testing.T) {
			bin := tcJob09CarrierFakeKubectl(t)
			out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{
				"FOREMAN_TERMINATION_HANG_GUARD": guard,
				"FAKE_TERMINATE_AFTER":           "5",
			})
			if code != 2 {
				t.Fatalf("carrier exit = %d, want 2 (bad input is not a verdict):\n%s", code, out)
			}
			if !strings.Contains(out, "FOREMAN_TERMINATION_HANG_GUARD must be at most 3600 seconds, got '"+guard+"'") {
				t.Errorf("the blocker does not name the variable and the value:\n%s", out)
			}
			if strings.Contains(out, "FAIL:") || strings.Contains(out, "PASS:") {
				t.Errorf("an out-of-range guard reached a verdict:\n%s", out)
			}
		})
	}
}

// The promised range is closed at both ends: 1 and 3600 are accepted, so a
// `-ge` written where the script means `-gt` blocks an operator who fills in a
// value the header and the blocker text promise.
func TestJob09CarrierAcceptsTheHangGuardEndpoints(t *testing.T) {
	for _, guard := range []string{"1", "3600"} {
		t.Run(guard, func(t *testing.T) {
			bin := tcJob09CarrierFakeKubectl(t)
			out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{
				"FOREMAN_TERMINATION_HANG_GUARD": guard,
				"FAKE_TERMINATE_AFTER":           "0",
			})
			if code != 0 {
				t.Fatalf("carrier exit = %d, want 0 (a %ss guard is inside the promised range):\n%s", code, guard, out)
			}
			if !strings.Contains(out, "PASS: TC-tech-job-09") {
				t.Errorf("carrier did not report PASS:\n%s", out)
			}
			if !strings.Contains(out, "(hang guard "+guard+"s)") {
				t.Errorf("the observation does not render the guard as %ss:\n%s", guard, out)
			}
		})
	}
}

// Leading zeros are stripped to the canonical decimal form, so 010 is ten
// seconds in the observation — neither the raw 010 nor an octal eight.
func TestJob09CarrierReadsALeadingZeroGuardAsTenSeconds(t *testing.T) {
	bin := tcJob09CarrierFakeKubectl(t)
	out, code := tcJob09CarrierRun(t, bin, t.TempDir(), map[string]string{"FOREMAN_TERMINATION_HANG_GUARD": "010"})
	if code != 0 {
		t.Fatalf("carrier exit = %d, want 0 (010 is a valid guard):\n%s", code, out)
	}
	if !strings.Contains(out, "(hang guard 10s)") {
		t.Errorf("the observation does not render the guard canonically as 10s:\n%s", out)
	}
}
