// The environment gate's fourth check follows the same precedence as the
// agent's model: a deployment-supplied endpoint is the source when its triple
// is set, and the local backend is probed only otherwise. The entries run
// against a live cluster, so this drives the real selfcheck.sh with stubs and
// judges which source it checked, not only its exit code.
package testinfra

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// selfcheckKubectlStub answers the reads the gate makes: the instance's access
// point, the local backend's model, and the backend's own list. LOCAL_MODEL is
// empty when the backend is not deployed.
const selfcheckKubectlStub = `#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$KUBECTL_LOG"
case " $* " in
*" -n $TEST_MULTICA_INSTANCE_NS get deploy $TEST_MULTICA_INSTANCE -o name "*)
	printf 'deployment.apps/%s\n' "$TEST_MULTICA_INSTANCE"
	;;
*" -n $TEST_MULTICA_INSTANCE_NS get deploy $TEST_MULTICA_INSTANCE -o jsonpath="*)
	case " $* " in
	*MULTICA_SERVER_URL*) printf '%s' "$TEST_MULTICA_ACCESS_URL" ;;
	*FOREMAN_JOB_IMAGE*) printf '%s' "$JOB_IMAGE" ;;
	*) printf '' ;;
	esac
	;;
*" get pod test-multica-job-daemon-version -o jsonpath="*)
	printf 'Succeeded'
	;;
*" logs test-multica-job-daemon-version "* | *" logs test-multica-job-daemon-version")
	printf '{"version":"%s"}' "${JOB_DAEMON_VERSION:-}"
	;;
*" delete pod test-multica-job-daemon-version "* | *" apply -f "*)
	;;
*" -n $TEST_MULTICA_NS get deploy $TEST_MULTICA_MODEL_DEPLOY -o jsonpath="*)
	printf '%s' "${LOCAL_MODEL:-}"
	;;
*" -n $TEST_MULTICA_NS get deploy $TEST_MULTICA_SCRIPTED_DEPLOY -o jsonpath="*)
	printf '%s' "${SCRIPTED_MODEL:-}"
	;;
*" exec deploy/$TEST_MULTICA_SCRIPTED_DEPLOY "*)
	[ -z "${SCRIPTED_PROBE_FAILS:-}" ] || {
		printf 'error: the scripted backend is not answering\n' >&2
		exit 1
	}
	printf '%s\n' "${SCRIPTED_MODEL:-}"
	;;
*" exec deploy/$TEST_MULTICA_MODEL_DEPLOY "*)
	[ -z "${OLLAMA_LIST_FAILS:-}" ] || {
		printf 'error: cannot connect to the ollama server\n' >&2
		exit 1
	}
	printf 'NAME\tID\tSIZE\n%s\tdeadbeef\t397MB\n' "$LOCAL_MODEL"
	;;
*)
	printf 'stub kubectl: unexpected call: %s\n' "$*" >&2
	exit 1
	;;
esac
`

// TestSelfcheckChecksTheSelectedModelSource pins the gate's precedence: the
// endpoint triple short-circuits the local probe, and the local backend — with
// a model, without one, or not serving — decides only when no endpoint is
// configured.
func TestSelfcheckChecksTheSelectedModelSource(t *testing.T) {
	endpoint := []string{
		"TEST_MULTICA_MODEL_BASE_URL=http://gateway.test:8000/v1",
		"TEST_MULTICA_MODEL_ID=model-a",
		"TEST_MULTICA_MODEL_API_KEY=sk-test",
	}

	t.Run("deployment endpoint wins", func(t *testing.T) {
		run := runSelfcheck(t, endpoint...)
		if run.exit != 0 {
			t.Fatalf("selfcheck exited %d with an endpoint configured\n--- output ---\n%s", run.exit, run.output)
		}
		want := "④ ok: the deployment supplies the model endpoint (endpoint foreman-test/model-a)"
		if !strings.Contains(run.output, want) {
			t.Errorf("output does not carry %q\n--- output ---\n%s", want, run.output)
		}
		if strings.Contains(run.kubectl, "get deploy test-multica-ollama") {
			t.Errorf("the endpoint gate probed the local backend it does not use:\n%s", run.kubectl)
		}
	})

	local := "TEST_MULTICA_MODEL_LOCAL_SOURCE=ollama"

	t.Run("local engine serves", func(t *testing.T) {
		run := runSelfcheck(t, local, "LOCAL_MODEL=qwen2.5:0.5b")
		if run.exit != 0 {
			t.Fatalf("selfcheck exited %d with a serving local backend\n--- output ---\n%s", run.exit, run.output)
		}
		want := "④ ok: model backend serves qwen2.5:0.5b"
		if !strings.Contains(run.output, want) {
			t.Errorf("output does not carry %q\n--- output ---\n%s", want, run.output)
		}
		if !strings.Contains(run.kubectl, "exec deploy/test-multica-ollama") {
			t.Errorf("the local gate did not probe the backend's list:\n%s", run.kubectl)
		}
	})

	t.Run("local engine not serving", func(t *testing.T) {
		run := runSelfcheck(t, local, "LOCAL_MODEL=qwen2.5:0.5b", "OLLAMA_LIST_FAILS=1")
		if run.exit != 2 {
			t.Fatalf("selfcheck exited %d, want the blocked code 2\n--- output ---\n%s", run.exit, run.output)
		}
		if want := "④ the model backend is not serving"; !strings.Contains(run.output, want) {
			t.Errorf("output does not carry %q\n--- output ---\n%s", want, run.output)
		}
	})

	t.Run("scripted backend serves", func(t *testing.T) {
		run := runSelfcheck(t)
		if run.exit != 0 {
			t.Fatalf("selfcheck exited %d with the scripted backend serving\n--- output ---\n%s", run.exit, run.output)
		}
		want := "④ ok: scripted backend serves " + scriptedModelID
		if !strings.Contains(run.output, want) {
			t.Errorf("output does not carry %q\n--- output ---\n%s", want, run.output)
		}
		if !strings.Contains(run.kubectl, "exec deploy/test-multica-scripted") {
			t.Errorf("the default gate did not probe the scripted backend it serves:\n%s", run.kubectl)
		}
		if strings.Contains(run.kubectl, "exec deploy/test-multica-ollama") {
			t.Errorf("the default gate probed the ollama engine the round does not serve:\n%s", run.kubectl)
		}
	})

	t.Run("scripted backend not answering", func(t *testing.T) {
		run := runSelfcheck(t, "SCRIPTED_PROBE_FAILS=1")
		if run.exit != 2 {
			t.Fatalf("selfcheck exited %d, want the blocked code 2\n--- output ---\n%s", run.exit, run.output)
		}
		if want := "④ the scripted backend is not serving"; !strings.Contains(run.output, want) {
			t.Errorf("output does not carry %q\n--- output ---\n%s", want, run.output)
		}
	})

	t.Run("no model source", func(t *testing.T) {
		run := runSelfcheck(t, "SCRIPTED_MODEL=")
		if run.exit != 2 {
			t.Fatalf("selfcheck exited %d, want the blocked code 2\n--- output ---\n%s", run.exit, run.output)
		}
		if want := "carries no SCRIPTED_MODEL_ID"; !strings.Contains(run.output, want) {
			t.Errorf("output does not carry %q\n--- output ---\n%s", want, run.output)
		}
	})
}

type selfcheckRun struct {
	output  string
	kubectl string
	exit    int
}

// runSelfcheck runs the real selfcheck.sh in a sandbox whose kubectl, curl and
// CLI stubs answer every check; `env` overrides the sandbox defaults.
func runSelfcheck(t *testing.T, env ...string) selfcheckRun {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skipf("host has no sh: %v", err)
	}
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	entryDir := filepath.Join(dir, "test-multica")
	homeDir := filepath.Join(dir, "home")
	profileDir := filepath.Join(homeDir, ".multica", "profiles", "test-multica")
	for _, d := range []string{binDir, entryDir, profileDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("create %s: %v", d, err)
		}
	}
	kubectlLog := filepath.Join(dir, "kubectl.calls")
	writeExecutable(t, filepath.Join(binDir, "kubectl"), selfcheckKubectlStub)
	writeExecutable(t, filepath.Join(binDir, "curl"),
		"#!/bin/sh\nprintf '%s' '{\"server_version\":\"v0.6.1\"}'\n")
	// `need multica` resolves the command; the runtime list itself goes through
	// the entry's own CLI wrapper beside it.
	writeExecutable(t, filepath.Join(binDir, "multica"), "#!/bin/sh\nexit 0\n")
	writeExecutable(t, filepath.Join(entryDir, "multica-test"), "#!/bin/sh\nprintf '[]'\n")
	for _, name := range []string{"selfcheck.sh", "lib.sh"} {
		body, err := os.ReadFile(filepath.Join(libDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		writeExecutable(t, filepath.Join(entryDir, name), string(body))
	}
	profile := `{"token":"test-token","workspace_id":"11111111-1111-4111-8111-111111111111"}`
	if err := os.WriteFile(filepath.Join(profileDir, "config.json"), []byte(profile+"\n"), 0o600); err != nil {
		t.Fatalf("write the CLI profile: %v", err)
	}

	values := map[string]string{
		"PATH":                            binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"HOME":                            homeDir,
		"TMPDIR":                          dir,
		"KUBECTL_LOG":                     kubectlLog,
		"LOCAL_MODEL":                     "",
		"SCRIPTED_MODEL":                  scriptedModelID,
		"JOB_IMAGE":                       "ghcr.io/tsic404/foreman-job:latest",
		"JOB_DAEMON_VERSION":              "0.8.0",
		"TEST_MULTICA_PROFILE":            "test-multica",
		"TEST_MULTICA_SERVER_URL":         "http://127.0.0.1:30080",
		"TEST_MULTICA_ACCESS_URL":         "http://test-multica-server.multica-test.svc:8080",
		"TEST_MULTICA_NS":                 "multica-test",
		"TEST_MULTICA_MODEL_DEPLOY":       "test-multica-ollama",
		"TEST_MULTICA_SCRIPTED_DEPLOY":    "test-multica-scripted",
		"TEST_MULTICA_MODEL_LOCAL_SOURCE": "scripted",
		"TEST_MULTICA_INSTANCE_NS":        "foreman",
		"TEST_MULTICA_INSTANCE":           "foreman",
		"TEST_MULTICA_KUBECTL_RETRIES":    "1",
		"TEST_MULTICA_MODEL_BASE_URL":     "",
		"TEST_MULTICA_MODEL_ID":           "",
		"TEST_MULTICA_MODEL_API_KEY":      "",
	}
	for _, kv := range env {
		key, value, _ := strings.Cut(kv, "=")
		values[key] = value
	}
	cmdEnv := make([]string, 0, len(values))
	for key, value := range values {
		cmdEnv = append(cmdEnv, key+"="+value)
	}

	cmd := exec.Command("sh", filepath.Join(entryDir, "selfcheck.sh"))
	cmd.Dir = dir
	cmd.Env = cmdEnv
	var combined bytes.Buffer
	cmd.Stdout, cmd.Stderr = &combined, &combined
	exit := 0
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("run selfcheck.sh: %v", err)
		}
		exit = exitErr.ExitCode()
	}
	kubectl, err := os.ReadFile(kubectlLog)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read the kubectl log: %v", err)
	}
	return selfcheckRun{output: combined.String(), kubectl: string(kubectl), exit: exit}
}

// TestSelfcheckRefusesADaemonBelowTheOmpContract pins gate ⑤: the Job image's
// daemon must carry the argv omp accepts. Up to v0.6.1 the daemon passes
// `--session <path>`, which omp reads as a resume of the empty file it was
// just given — every task then dies before its first turn. The gate reads the
// version out of the image's own binary, so a moving tag cannot pass it.
func TestSelfcheckRefusesADaemonBelowTheOmpContract(t *testing.T) {
	t.Run("the pre-0.6.2 daemon is refused with the root cause", func(t *testing.T) {
		run := runSelfcheck(t, "JOB_DAEMON_VERSION=0.6.1")
		if run.exit == 0 {
			t.Fatalf("selfcheck accepted a daemon that starts omp with --session\n--- output ---\n%s", run.output)
		}
		for _, want := range []string{
			"⑤ Job image",
			"carries multica 0.6.1",
			"--session <path>",
			"agent_error.process_failure",
		} {
			if !strings.Contains(run.output, want) {
				t.Errorf("the refusal does not name %q\n--- output ---\n%s", want, run.output)
			}
		}
	})

	t.Run("the floor itself passes", func(t *testing.T) {
		run := runSelfcheck(t, "JOB_DAEMON_VERSION=0.6.2")
		if run.exit != 0 {
			t.Fatalf("selfcheck refused the contract's floor version\n--- output ---\n%s", run.output)
		}
		if !strings.Contains(run.output, "⑤ ok: Job image") {
			t.Errorf("the passing gate is not reported\n--- output ---\n%s", run.output)
		}
	})

	t.Run("an image that does not answer is refused", func(t *testing.T) {
		run := runSelfcheck(t, "JOB_DAEMON_VERSION=")
		if run.exit == 0 {
			t.Fatalf("selfcheck accepted an unreadable Job image\n--- output ---\n%s", run.output)
		}
		if !strings.Contains(run.output, "cannot read the daemon version out of Job image") {
			t.Errorf("the refusal does not name the unreadable image\n--- output ---\n%s", run.output)
		}
	})
}
