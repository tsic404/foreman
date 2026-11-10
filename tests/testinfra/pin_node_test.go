// pin-node.sh is the fixture's placement lever: tech-cache-02 judges two
// independent tasks that ran concurrently on one node, and the Job template
// cannot build that window (its preferred node is soft and session-only, its
// spread constraint keeps independent pods apart). The entry writes the job
// policy's nodeSelector axis and must verify the instance actually loaded the
// file — the file is read once at startup, so an unloaded pin is a silent
// no-op that leaves the case blocked with no evidence of why. Neither the
// ConfigMap nor the startup hash is observable without a cluster, so the entry
// runs here against a kubectl stub that keeps what it was given.
package testinfra

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// pinKubectlStub answers the reads and writes pin-node.sh makes. The policy it
// is asked to write is kept, and the log line the entry verifies is derived
// from that same file — the startup hash of what was loaded, which is the fact
// the real Foreman prints.
const pinKubectlStub = `#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$STUB_KUBECTL_CALLS"
case " $* " in
*" get node "*" -o name "*)
	printf 'node/%s\n' "$3"
	;;
*" get deploy $TEST_MULTICA_INSTANCE -o jsonpath={.spec.selector.matchLabels} "*)
	printf '{"app.kubernetes.io/name":"foreman"}'
	;;
*" get deploy $TEST_MULTICA_INSTANCE -o jsonpath={.spec.replicas} "*)
	printf '%s' "${STUB_REPLICAS:-1}"
	;;
*" get pod "*" -o name "*)
	printf 'pod/foreman-new\n'
	;;
*" create configmap foreman-job-policy "*)
	from=
	for arg in "$@"; do
		case $arg in
		--from-file=policy.yaml=*) from=${arg#--from-file=policy.yaml=} ;;
		esac
	done
	[ -n "$from" ] || { echo "stub kubectl: no --from-file" >&2; exit 1; }
	cp "$from" "$STUB_STATE/policy.yaml"
	sha256sum "$from" | cut -d' ' -f1 >"$STUB_STATE/sum"
	printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: foreman-job-policy\n'
	;;
*" apply -f - "*)
	cat >/dev/null
	;;
*" rollout restart deploy/$TEST_MULTICA_INSTANCE "*|*" rollout status deploy/$TEST_MULTICA_INSTANCE "*)
	;;
*" logs pod/foreman-new "*)
	if [ -n "${STUB_LOG:-}" ]; then
		printf '%s\n' "$STUB_LOG"
	else
		printf 'level=INFO msg="策略文件内容哈希 sha256=%s" policy=/etc/foreman/policy/policy.yaml\n' \
			"$(cat "$STUB_STATE/sum")"
	fi
	;;
*" delete configmap foreman-job-policy "*)
	printf '%s\n' "$*" >>"$STUB_KUBECTL_CALLS"
	;;
*)
	printf 'stub kubectl: unexpected call: %s\n' "$*" >&2
	exit 1
	;;
esac
`

// pinCurlStub is the server's agent list: one unarchived agent, named by
// STUB_AGENT (the entry's own name comes from TEST_MULTICA_AGENT, which a test
// may point elsewhere).
const pinCurlStub = `#!/bin/sh
set -eu
for arg in "$@"; do
	case $arg in
	*://*/api/agents) printf '[{"id":"agent-main","name":"%s","archived_at":null}]' "${STUB_AGENT:-$TEST_MULTICA_AGENT}"; exit 0 ;;
	esac
done
printf 'stub curl: unexpected call: %s\n' "$*" >&2
exit 1
`

type pinRun struct {
	kubectl  string
	out      string
	err      error
	policy   string
	witness  string
	exitCode int
}

// runPinNode runs pin-node.sh in a sandbox whose cluster and server are the
// stubs above. STUB_LOG overrides the log the entry reads; STUB_REPLICAS sets
// the instance's replica count.
func runPinNode(t *testing.T, stubs map[string]string, args ...string) pinRun {
	t.Helper()
	return runPinNodeAt(t, t.TempDir(), stubs, args...)
}

// runPinNodeAt is runPinNode on a caller-owned sandbox so a test can observe
// what one invocation leaves behind (the stopped-instance witness) in the next.
func runPinNodeAt(t *testing.T, dir string, stubs map[string]string, args ...string) pinRun {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skipf("host has no sh: %v", err)
	}
	binDir := filepath.Join(dir, "bin")
	entryDir := filepath.Join(dir, "test-multica")
	homeDir := filepath.Join(dir, "home")
	stateDir := filepath.Join(dir, "state")
	profileDir := filepath.Join(homeDir, ".multica", "profiles", "test-multica")
	for _, d := range []string{binDir, entryDir, stateDir, profileDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("create %s: %v", d, err)
		}
	}
	writeExecutable(t, filepath.Join(binDir, "kubectl"), pinKubectlStub)
	writeExecutable(t, filepath.Join(binDir, "curl"), pinCurlStub)
	if out, ok := stubs["STUB_SHA256_OUT"]; ok {
		// The digest label alone is a tautology: the entry must refuse an
		// empty or malformed value instead of verifying against "sha256=".
		writeExecutable(t, filepath.Join(binDir, "sha256sum"), "#!/bin/sh\nprintf '%s' "+shellSingleQuote(out)+"\n")
	}
	for _, name := range []string{"pin-node.sh", "lib.sh"} {
		body, err := os.ReadFile(filepath.Join(libDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		writeExecutable(t, filepath.Join(entryDir, name), string(body))
	}
	profile := `{"server_url":"` + resetServerURL + `","app_url":"` + resetServerURL +
		`","token":"test-token","workspace_id":"` + resetWorkspaceID + `"}`
	if err := os.WriteFile(filepath.Join(profileDir, "config.json"), []byte(profile+"\n"), 0o600); err != nil {
		t.Fatalf("write the CLI profile: %v", err)
	}

	env := map[string]string{
		"STUB_KUBECTL_CALLS":           filepath.Join(dir, "kubectl.calls"),
		"STUB_STATE":                   stateDir,
		"TEST_MULTICA_PROFILE":         "test-multica",
		"TEST_MULTICA_SERVER_URL":      resetServerURL,
		"TEST_MULTICA_ENTRY_URL":       "http://entry.test.local:30300",
		"TEST_MULTICA_AGENT":           resetAgentName,
		"TEST_MULTICA_INSTANCE_NS":     "foreman",
		"TEST_MULTICA_INSTANCE":        "foreman",
		"TEST_MULTICA_KUBECTL_RETRIES": "1",
	}
	for key, value := range stubs {
		env[key] = value
	}
	cmd := exec.Command("sh", append([]string{filepath.Join(entryDir, "pin-node.sh")}, args...)...)
	cmd.Dir = dir
	cmd.Env = envWith(
		map[string]string{
			"PATH":   binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
			"HOME":   homeDir,
			"TMPDIR": dir,
		},
		env,
	)
	raw, err := cmd.CombinedOutput()
	run := pinRun{out: string(raw), err: err}
	if ee, ok := err.(*exec.ExitError); ok {
		run.exitCode = ee.ExitCode()
	}
	if body, readErr := os.ReadFile(filepath.Join(dir, "kubectl.calls")); readErr == nil {
		run.kubectl = string(body)
	}
	if body, readErr := os.ReadFile(filepath.Join(stateDir, "policy.yaml")); readErr == nil {
		run.policy = string(body)
	}
	if body, readErr := os.ReadFile(filepath.Join(homeDir, ".foreman-job-policy-witness")); readErr == nil {
		run.witness = string(body)
	}
	return run
}

// shellSingleQuote renders s as one POSIX shell word.
func shellSingleQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// TestPinNodeWritesTheAgentSelectorAndVerifiesItLoaded: the pin the case needs
// is a hard selector on the agent row — the policy axis that also suppresses
// the preferred-node item — and the entry reports success only after the rolled
// instance's log carries the loaded file's hash. A success that skipped the
// check would send the case into a blocked window with nothing to read.
func TestPinNodeWritesTheAgentSelectorAndVerifiesItLoaded(t *testing.T) {
	run := runPinNode(t, map[string]string{}, "node-a")
	if run.err != nil {
		t.Fatalf("pin-node.sh did not finish: %v\n%s", run.err, run.out)
	}
	for _, want := range []string{
		"agents:",
		`"` + resetAgentName + `":`,
		"nodeSelectorPolicy: agent",
		`kubernetes.io/hostname: "node-a"` + "\n",
	} {
		if !strings.Contains(run.policy, want) {
			t.Errorf("the written policy does not carry %q:\n%s", want, run.policy)
		}
	}
	for _, want := range []string{
		"rollout restart deploy/foreman",
		"rollout status deploy/foreman",
		"logs pod/foreman-new",
	} {
		if !strings.Contains(run.kubectl, want) {
			t.Errorf("the entry never ran %q — the pin would not be loaded:\n%s", want, run.kubectl)
		}
	}
	if !strings.Contains(run.out, "pinned agent") || !strings.Contains(run.out, "node-a") {
		t.Errorf("the report does not name the pin:\n%s", run.out)
	}
}

// TestPinNodeRefusesAPinTheInstanceDidNotLoad: the startup log is the evidence
// that the file was read. A log without the hash is a pin that is not in force,
// and reporting success there is exactly the silent no-op this entry exists to
// prevent — the case would then block on "did not share a node" with no cause.
func TestPinNodeRefusesAPinTheInstanceDidNotLoad(t *testing.T) {
	run := runPinNode(t, map[string]string{
		"STUB_LOG": "level=INFO msg=\"job policy file missing: 策略文件缺席，三轴按 None 读（合法默认）\"",
	}, "node-a")
	if run.err == nil {
		t.Fatalf("pin-node.sh reported success although the instance never loaded the policy:\n%s", run.out)
	}
	if !strings.Contains(run.out, "was not loaded") {
		t.Errorf("the refusal does not say the policy was not loaded:\n%s", run.out)
	}
}

// TestPinNodeOffRemovesThePolicyAndVerifiesTheRestore: the pin applies to every
// task of the test agent and suppresses the preferred-node item cache-05/06
// judge, so the restore must be as verified as the pin — deletion alone proves
// nothing about the loaded state.
func TestPinNodeOffRemovesThePolicyAndVerifiesTheRestore(t *testing.T) {
	run := runPinNode(t, map[string]string{
		"STUB_LOG": "level=WARN msg=\"job policy file missing: 策略文件缺席，三轴按 None 读（合法默认）\"",
	}, "--off")
	if run.err != nil {
		t.Fatalf("pin-node.sh --off did not finish: %v\n%s", run.err, run.out)
	}
	for _, want := range []string{"delete configmap foreman-job-policy", "logs pod/foreman-new"} {
		if !strings.Contains(run.kubectl, want) {
			t.Errorf("--off never ran %q:\n%s", want, run.kubectl)
		}
	}
}

// TestPinNodeOffRefusesAnUnrestoredInstance: deletion is not evidence. The
// policy is read at startup, so a restore that the rolled instance did not pick
// up leaves the preferred-node item suppressed and cache-05/06 judging nothing.
func TestPinNodeOffRefusesAnUnrestoredInstance(t *testing.T) {
	run := runPinNode(t, map[string]string{
		"STUB_LOG": "level=INFO msg=\"策略文件内容哈希 sha256=deadbeef\"",
	}, "--off")
	if run.err == nil {
		t.Fatalf("pin-node.sh --off reported success while the instance still runs a policy:\n%s", run.out)
	}
	if !strings.Contains(run.out, "was not loaded") {
		t.Errorf("the refusal does not say the policy state was not loaded:\n%s", run.out)
	}
}

// TestPinNodeRefusesAnAgentTheServerDoesNotHave: the policy key is resolved
// against the server's agents at startup and an absent key refuses the boot —
// a CrashLoopBackOff is a worse failure mode than the named refusal here.
func TestPinNodeRefusesAnAgentTheServerDoesNotHave(t *testing.T) {
	run := runPinNode(t, map[string]string{
		"TEST_MULTICA_AGENT": "some-other-agent",
		"STUB_AGENT":         resetAgentName,
	}, "node-a")
	if run.err == nil {
		t.Fatalf("pin-node.sh wrote a policy for an agent the server does not have:\n%s", run.out)
	}
	if !strings.Contains(run.out, "./reset.sh") {
		t.Errorf("the refusal does not name the entry that creates the agent:\n%s", run.out)
	}
}

// TestPinNodeRefusesANonDigest: the startup line carries "sha256=<digest>";
// an empty or malformed digest turns the load check into a match on the label
// alone, which is always true on a running instance. The entry must refuse it
// before writing anything the case would trust.
func TestPinNodeRefusesANonDigest(t *testing.T) {
	for name, out := range map[string]string{
		"empty":     "",
		"short":     "deadbeef",
		"nonhex":    strings.Repeat("z", 64),
		"uppercase": strings.Repeat("A", 64),
	} {
		t.Run(name, func(t *testing.T) {
			run := runPinNode(t, map[string]string{"STUB_SHA256_OUT": out}, "node-a")
			if run.err == nil {
				t.Fatalf("pin-node.sh accepted a non-digest sha256 (%q):\n%s", out, run.out)
			}
			if !strings.Contains(run.out, "sha256") {
				t.Errorf("the refusal does not name the sha256 it could not read:\n%s", run.out)
			}
		})
	}
}

// TestPinNodeStoppedInstanceLeavesAWitnessAndVerifyChecksIt: the documented
// order is ./reset.sh then ./pin-node.sh, so the instance is stopped when the
// pin is written and this entry cannot verify it. That path must not report
// success; it records the expected startup line and ./wire.sh (via --verify)
// compares it with the started instance's log.
func TestPinNodeStoppedInstanceLeavesAWitnessAndVerifyChecksIt(t *testing.T) {
	dir := t.TempDir()
	run := runPinNodeAt(t, dir, map[string]string{"STUB_REPLICAS": "0"}, "node-a")
	if run.err == nil {
		t.Fatalf("pin-node.sh reported success although the stopped instance cannot have loaded the policy:\n%s", run.out)
	}
	if run.exitCode != 3 {
		t.Errorf("exit code = %d, want 3 (written, not verified):\n%s", run.exitCode, run.out)
	}
	if !strings.Contains(run.witness, "expect=sha256=") || !strings.Contains(run.witness, "node=node-a") {
		t.Fatalf("the witness does not carry the expected startup line:\n%s", run.witness)
	}
	sum := strings.TrimPrefix(strings.SplitN(strings.TrimSpace(run.witness), "\n", 2)[0], "expect=sha256=")

	verified := runPinNodeAt(t, dir, map[string]string{
		"STUB_LOG": "level=INFO msg=\"策略文件内容哈希 sha256=" + sum + "\" policy=/etc/foreman/policy/policy.yaml",
	}, "--verify")
	if verified.err != nil {
		t.Fatalf("--verify rejected the startup line the witness expects: %v\n%s", verified.err, verified.out)
	}
	if verified.witness != "" {
		t.Errorf("--verify left the witness behind after a match:\n%s", verified.witness)
	}
}

// TestPinNodeVerifyRefusesAMismatch distinguishes "the witness was checked and
// did not match" from "there was nothing to check": the witness stays for the
// operator, and the exit code is non-zero.
func TestPinNodeVerifyRefusesAMismatch(t *testing.T) {
	dir := t.TempDir()
	run := runPinNodeAt(t, dir, map[string]string{"STUB_REPLICAS": "0"}, "node-a")
	if run.err == nil {
		t.Fatalf("the stopped-instance pin unexpectedly succeeded:\n%s", run.out)
	}
	mismatch := runPinNodeAt(t, dir, map[string]string{
		"STUB_LOG": "level=INFO msg=\"策略文件内容哈希 sha256=" + strings.Repeat("0", 64) + "\"",
	}, "--verify")
	if mismatch.err == nil {
		t.Fatalf("--verify accepted a startup line that does not carry the witness digest:\n%s", mismatch.out)
	}
	if mismatch.witness == "" {
		t.Errorf("--verify dropped the witness although it could not verify it")
	}
}

// TestPinNodeVerifyWithoutAWitnessIsANoOp: wire.sh calls --verify on every
// round; a round with no pin (or whose pin was written while the instance was
// running) has nothing pending and must not fail.
func TestPinNodeVerifyWithoutAWitnessIsANoOp(t *testing.T) {
	run := runPinNode(t, map[string]string{}, "--verify")
	if run.err != nil {
		t.Fatalf("--verify failed although no witness exists: %v\n%s", run.err, run.out)
	}
	if strings.Contains(run.kubectl, "get pod") {
		t.Errorf("--verify read the instance although nothing was pending:\n%s", run.kubectl)
	}
}

// TestPinNodeVerifyRefusesAnUnreadableWitness: a witness that exists but
// cannot be read is not "nothing pending". Treating the two as the same would
// skip the startup check silently — the exact failure mode this entry exists
// to prevent — so --verify must fail and must not print the no-pending line.
func TestPinNodeVerifyRefusesAnUnreadableWitness(t *testing.T) {
	dir := t.TempDir()
	home := filepath.Join(dir, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatalf("create the sandbox home: %v", err)
	}
	witness := filepath.Join(home, ".foreman-job-policy-witness")
	if err := os.WriteFile(witness, []byte("expect=sha256="+strings.Repeat("a", 64)+"\nnode=node-a\n"), 0o600); err != nil {
		t.Fatalf("write the witness: %v", err)
	}
	if err := os.Chmod(witness, 0o000); err != nil {
		t.Fatalf("make the witness unreadable: %v", err)
	}
	if _, err := os.ReadFile(witness); err == nil {
		t.Skip("this user can still read a 0000 file (running as root?); the unreadable case is not constructible")
	}

	run := runPinNodeAt(t, dir, map[string]string{}, "--verify")
	if run.err == nil {
		t.Fatalf("--verify reported success although the pending witness is unreadable:\n%s", run.out)
	}
	if strings.Contains(run.out, "no pending job-policy pin") {
		t.Errorf("--verify reports an unreadable witness as nothing pending:\n%s", run.out)
	}
	if !strings.Contains(run.out, "unreadable") {
		t.Errorf("the refusal does not say the witness is unreadable:\n%s", run.out)
	}
	if strings.Contains(run.kubectl, "get pod") {
		t.Errorf("--verify read the instance although the witness could not be read:\n%s", run.kubectl)
	}
}
