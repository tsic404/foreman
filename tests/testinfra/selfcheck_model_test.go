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
default_web_digest=$(printf '%064d' 0 | tr 0 d)
default_server_digest=$(printf '%064d' 0 | tr 0 a)
case " $* " in
*" -n $TEST_MULTICA_INSTANCE_NS get deploy $TEST_MULTICA_INSTANCE -o name "*)
	printf 'deployment.apps/%s\n' "$TEST_MULTICA_INSTANCE"
	;;
*" -n $TEST_MULTICA_INSTANCE_NS get deploy $TEST_MULTICA_INSTANCE -o jsonpath="*)
	case " $* " in
	*MULTICA_SERVER_URL*) printf '%s' "$TEST_MULTICA_ACCESS_URL" ;;
	*FOREMAN_JOB_IMAGE*) printf '%s' "$JOB_IMAGE" ;;
	*FOREMAN_JOB_RUNTIME_IMAGE*) printf '%s' "${JOB_RUNTIME_IMAGE:-}" ;;
	*FOREMAN_JOB_CLI_BUDGET_BYTES*) printf '%s' "${JOB_CLI_BUDGET_BYTES:-}" ;;
	*) printf '' ;;
	esac
	;;
*" -n $TEST_MULTICA_NS get deploy $TEST_MULTICA_MODEL_DEPLOY -o jsonpath="*)
	printf '%s' "${LOCAL_MODEL:-}"
	;;
*" -n $TEST_MULTICA_NS get deploy $TEST_MULTICA_SCRIPTED_DEPLOY -o jsonpath="*)
	printf '%s' "${SCRIPTED_MODEL:-}"
	;;
*" get deploy backend -n $TEST_MULTICA_NS -o jsonpath="*)
	printf '%s' "${SERVER_IMAGE-ghcr.io/multica-ai/multica-backend:latest}"
	;;
*" get pod -n $TEST_MULTICA_NS "*)
	case " $* " in
	*component=web*)
		printf '{"items":[{"status":{"phase":"Running","containerStatuses":[{"name":"web","imageID":"sha256:%s"}]}}]}' "${WEB_DIGEST-$default_web_digest}"
		;;
	*)
		printf '{"items":[{"status":{"phase":"Running","containerStatuses":[{"name":"backend","imageID":"sha256:%s"}]}}]}' "${SERVER_DIGEST-$default_server_digest}"
		;;
	esac
	;;
*" exec -n $TEST_MULTICA_NS deploy/backend "*)
	printf '{"version":"%s"}' "${SERVER_IMAGE_VERSION-v0.11.1}"
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

// TestSelfcheckIdentifiesTheRunningServerArtifact pins gate ③. The test server
// follows upstream's `latest`, so there is no version literal to compare
// against: the gate judges the artifact that is running — the server's
// /api/config answer must be the version the image's own binary reports, and
// the pod must carry a digest the round can quote. Another release passes
// without touching the script; a disagreement or an unreadable face is
// QA_BLOCKED with the two values named.
func TestSelfcheckIdentifiesTheRunningServerArtifact(t *testing.T) {
	t.Run("a newer latest passes without a script change", func(t *testing.T) {
		digest := strings.Repeat("b", 64)
		run := runSelfcheck(t,
			"SERVER_VERSION=v0.12.3", "SERVER_IMAGE_VERSION=v0.12.3", "SERVER_DIGEST="+digest)
		if run.exit != 0 {
			t.Fatalf("selfcheck exited %d for a coherent v0.12.3 server\n--- output ---\n%s", run.exit, run.output)
		}
		want := "③ ok: server_version v0.12.3, image ghcr.io/multica-ai/multica-backend:latest @ sha256:" + digest
		if !strings.Contains(run.output, want) {
			t.Errorf("output does not carry %q\n--- output ---\n%s", want, run.output)
		}
	})

	t.Run("the server and the running image disagree", func(t *testing.T) {
		run := runSelfcheck(t, "SERVER_VERSION=v0.12.3", "SERVER_IMAGE_VERSION=v0.11.1")
		if run.exit != 2 {
			t.Fatalf("selfcheck exited %d for a server that does not describe its image, want the blocked code 2\n--- output ---\n%s", run.exit, run.output)
		}
		for _, want := range []string{
			"③ the server reports v0.12.3",
			"the running image's binary reports v0.11.1",
		} {
			if !strings.Contains(run.output, want) {
				t.Errorf("the refusal does not name %q\n--- output ---\n%s", want, run.output)
			}
		}
	})

	t.Run("a missing server_version is a data-surface mismatch", func(t *testing.T) {
		run := runSelfcheck(t, "SERVER_CONFIG={}")
		if run.exit != 2 {
			t.Fatalf("selfcheck exited %d for a config without server_version, want the blocked code 2\n--- output ---\n%s", run.exit, run.output)
		}
		if want := "carries no server_version field"; !strings.Contains(run.output, want) {
			t.Errorf("the refusal does not name %q\n--- output ---\n%s", want, run.output)
		}
	})

	t.Run("an empty server_version is a data-surface mismatch", func(t *testing.T) {
		run := runSelfcheck(t, `SERVER_CONFIG={"server_version":""}`)
		if run.exit != 2 {
			t.Fatalf("selfcheck exited %d for an empty server_version, want the blocked code 2\n--- output ---\n%s", run.exit, run.output)
		}
		if want := "reports an empty server_version"; !strings.Contains(run.output, want) {
			t.Errorf("the refusal does not name %q\n--- output ---\n%s", want, run.output)
		}
	})

	t.Run("a running pod without a digest is not an identity", func(t *testing.T) {
		run := runSelfcheck(t, "SERVER_DIGEST=")
		if run.exit != 2 {
			t.Fatalf("selfcheck exited %d for a pod without a digest, want the blocked code 2\n--- output ---\n%s", run.exit, run.output)
		}
		if want := "cannot read the running backend pod's digest"; !strings.Contains(run.output, want) {
			t.Errorf("the refusal does not name %q\n--- output ---\n%s", want, run.output)
		}
	})

	t.Run("an image that does not answer is refused", func(t *testing.T) {
		run := runSelfcheck(t, "SERVER_IMAGE_VERSION=")
		if run.exit != 2 {
			t.Fatalf("selfcheck exited %d for an unreadable backend image, want the blocked code 2\n--- output ---\n%s", run.exit, run.output)
		}
		if want := "cannot read the version out of the running backend image"; !strings.Contains(run.output, want) {
			t.Errorf("the refusal does not name %q\n--- output ---\n%s", want, run.output)
		}
	})

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
		"#!/bin/sh\nif [ -n \"${SERVER_CONFIG:-}\" ]; then printf '%s' \"$SERVER_CONFIG\"; else printf '{\"server_version\":\"%s\"}' \"${SERVER_VERSION:-v0.11.1}\"; fi\n")
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
		"JOB_IMAGE":                       "ghcr.io/tsic404/rsbx@sha256:" + strings.Repeat("c", 64),
		"JOB_RUNTIME_IMAGE":               "ghcr.io/tsic404/foreman:latest",
		"JOB_CLI_BUDGET_BYTES":            "536870912",
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
		"SERVER_IMAGE":                    "ghcr.io/multica-ai/multica-backend:latest",
		"SERVER_DIGEST":                   strings.Repeat("a", 64),
		"SERVER_IMAGE_VERSION":            "v0.11.1",
		"SERVER_VERSION":                  "v0.11.1",
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

// TestSelfcheckAssertsTheFrozenCLIDelivery pins gate ⑤. The Job runs the rsbx
// carrier and downloads both CLIs from the release registry, so no Job image
// carries a version to probe: the gate asserts the delivery state instead —
// the carrier is digest-pinned, the supervisor source is declared, and the
// per-attempt proxy budget is positive. It must not pull or run an image.
func TestSelfcheckAssertsTheFrozenCLIDelivery(t *testing.T) {
	carrier := "ghcr.io/tsic404/rsbx@sha256:" + strings.Repeat("c", 64)

	t.Run("the digest-pinned rsbx carrier passes", func(t *testing.T) {
		run := runSelfcheck(t)
		if run.exit != 0 {
			t.Fatalf("selfcheck exited %d for a frozen delivery\n--- output ---\n%s", run.exit, run.output)
		}
		want := "⑤ ok: CLI delivery frozen: carrier " + carrier +
			", runtime image ghcr.io/tsic404/foreman:latest, download budget 536870912 bytes (separate limiter, V2-C28)"
		if !strings.Contains(run.output, want) {
			t.Errorf("the passing gate does not carry %q\n--- output ---\n%s", want, run.output)
		}
	})

	t.Run("a moving tag is not a carrier artifact", func(t *testing.T) {
		run := runSelfcheck(t, "JOB_IMAGE=ghcr.io/tsic404/rsbx:latest")
		if run.exit != 2 {
			t.Fatalf("selfcheck exited %d for a tag-only carrier, want the blocked code 2\n--- output ---\n%s", run.exit, run.output)
		}
		if want := "not the rsbx carrier at a verified index digest"; !strings.Contains(run.output, want) {
			t.Errorf("the refusal does not name %q\n--- output ---\n%s", want, run.output)
		}
	})

	t.Run("the legacy Job image is not a carrier", func(t *testing.T) {
		run := runSelfcheck(t, "JOB_IMAGE=ghcr.io/tsic404/foreman-job@sha256:"+strings.Repeat("d", 64))
		if run.exit != 2 {
			t.Fatalf("selfcheck exited %d for the legacy Job image, want the blocked code 2\n--- output ---\n%s", run.exit, run.output)
		}
		if want := "not the rsbx carrier at a verified index digest"; !strings.Contains(run.output, want) {
			t.Errorf("the refusal does not name %q\n--- output ---\n%s", want, run.output)
		}
	})

	t.Run("the supervisor source must be declared", func(t *testing.T) {
		run := runSelfcheck(t, "JOB_RUNTIME_IMAGE=")
		if run.exit != 2 {
			t.Fatalf("selfcheck exited %d without a runtime image, want the blocked code 2\n--- output ---\n%s", run.exit, run.output)
		}
		if want := "carries no FOREMAN_JOB_RUNTIME_IMAGE"; !strings.Contains(run.output, want) {
			t.Errorf("the refusal does not name %q\n--- output ---\n%s", want, run.output)
		}
	})

	t.Run("the proxy budget must be a positive byte count", func(t *testing.T) {
		for _, value := range []string{"", "0", "512MB"} {
			run := runSelfcheck(t, "JOB_CLI_BUDGET_BYTES="+value)
			if run.exit != 2 {
				t.Fatalf("selfcheck exited %d for budget %q, want the blocked code 2\n--- output ---\n%s", run.exit, value, run.output)
			}
			if !strings.Contains(run.output, "FOREMAN_JOB_CLI_BUDGET_BYTES") {
				t.Errorf("the refusal does not name the budget for %q\n--- output ---\n%s", value, run.output)
			}
		}
	})
}
