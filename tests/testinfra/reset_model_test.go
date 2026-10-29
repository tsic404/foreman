// The reset entry writes two values onto the test agent that only a Job pod
// could otherwise notice were wrong: the model selector and the model
// backend's in-cluster address. Neither is observable without a cluster, so
// reset.sh runs here against stubs that answer every call up to the writes and
// keep the request bodies for judgement: the selector must be the untagged
// family omp resolves after discovery, and custom_env must carry the backend
// address and nothing else.
package testinfra

import (
	"bytes"
	"encoding/json"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	resetWorkspaceID = "11111111-1111-4111-8111-111111111111"
	resetAgentID     = "22222222-2222-4222-8222-222222222222"
	resetAgentName   = "foreman-test-agent"
	resetChatTitle   = "foreman-test-chat"
	resetRepoURL     = "https://github.com/tsic404/foreman"
	resetServerURL   = "http://server.test.local:8080"
	resetNamespace   = "multica-test"
	resetModelDeploy = "test-multica-ollama"
	resetBackendURL  = "http://test-multica-ollama.multica-test.svc:11434"
)

// resetKubectlStub answers the cluster reads reset.sh makes before the model
// writes. The instance under test is reported absent, which is the branch a
// sandbox without a deployment takes; the model backend answers with what
// deploy.sh rendered into its pull step.
const resetKubectlStub = `#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$STUB_KUBECTL_CALLS"
case " $* " in
*" -n $TEST_MULTICA_INSTANCE_NS get deploy $TEST_MULTICA_INSTANCE -o name "*)
	printf 'Error from server (NotFound): deployments.apps "%s" not found\n' "$TEST_MULTICA_INSTANCE" >&2
	exit 1
	;;
*" -n $TEST_MULTICA_NS get deploy $TEST_MULTICA_MODEL_DEPLOY "*)
	if [ -n "${STUB_MODEL_READ_ERROR:-}" ]; then
		printf '%s\n' "$STUB_MODEL_READ_ERROR" >&2
		exit 1
	fi
	printf '%s' "$STUB_MODEL"
	;;
*" exec deploy/postgres "*)
	# psql_exec: the wipe prints nothing the entry reads.
	;;
*" get nodes "*" -o json "*)
	# One ready schedulable node: the endpoint-less branch rolls back through a
	# one-shot carrier per node.
	printf '{"items":[{"metadata":{"name":"node-a"},"spec":{},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}'
	;;
*" apply -f "*)
	# The removal carrier's manifest is applied; no pod ever runs here.
	;;
*" get pod "*" -o jsonpath="*)
	printf 'Succeeded'
	;;
*" delete pod "*|*" delete secret "*|*" describe pod "*)
	;;
*)
	printf 'stub kubectl: unexpected call: %s\n' "$*" >&2
	exit 1
	;;
esac
`

// resetCurlStub is the test server: it records every request (method, URL,
// body) and answers the endpoints reset.sh reads on its way to the model
// writes. The two writes are stored as sent, so the readback checks that
// follow them in reset.sh read back what the script wrote.
const resetCurlStub = `#!/bin/sh
set -eu
method=GET
url=
body=
out=
while [ $# -gt 0 ]; do
	case $1 in
	-X) method=$2; shift 2 ;;
	-d) body=$2; shift 2 ;;
	-o) out=$2; shift 2 ;;
	-w|-H|-m) shift 2 ;;
	*://*) url=$1; shift ;;
	*) shift ;;
	esac
done
printf '%s\t%s\t%s\n' "$method" "$url" "$body" >>"$STUB_CALLS"
# The status-only read (curl -o /dev/null -w '%{http_code}') answers with the
# code alone, which is how reset.sh reads a live profile.
if [ "$out" = /dev/null ]; then
	printf '%s' 200
	exit 0
fi
path=${url#*://}
path=${path#*/}
case "$method $path" in
"GET api/config") printf '{}' ;;
"GET api/workspaces/$STUB_WORKSPACE_ID")
	printf '{"id":"%s","repos":[{"url":"%s"}]}' "$STUB_WORKSPACE_ID" "$STUB_REPO_URL"
	;;
"GET api/agents")
	printf '[{"id":"%s","name":"%s","archived_at":null}]' "$STUB_AGENT_ID" "$STUB_AGENT_NAME"
	;;
"GET api/agents/$STUB_AGENT_ID") cat "$STUB_STATE/model" ;;
"GET api/agents/$STUB_AGENT_ID/env") cat "$STUB_STATE/env" ;;
"GET api/chat/sessions")
	printf '[{"id":"chat-1","title":"%s"}]' "$STUB_CHAT_TITLE"
	;;
"PUT api/agents/$STUB_AGENT_ID")
	printf '%s' "$body" >"$STUB_STATE/model"
	printf '{}'
	;;
"PUT api/agents/$STUB_AGENT_ID/env")
	printf '%s' "$body" >"$STUB_STATE/env"
	printf '{}'
	;;
*)
	printf 'stub curl: unexpected call: %s %s\n' "$method" "$url" >&2
	exit 1
	;;
esac
`

// TestResetPinsTheUntaggedModelFamilyAndTheBackendAddress judges the agent
// writes reset.sh sends when no endpoint is configured. A tagged selector is
// the regression the live path cannot catch: omp validates it against a
// startup catalog a fresh Job pod does not have, so the failure only shows up
// as a dead task. The last case is the chain's floor: neither the endpoint
// triple nor a deployed local backend, so both fields are cleared.
func TestResetPinsTheUntaggedModelFamilyAndTheBackendAddress(t *testing.T) {
	for _, tc := range []struct {
		name, served, wantModel, wantInfo string
		wantEnv                           map[string]string
	}{
		{
			name:      "tagged model",
			served:    "qwen2.5:0.5b",
			wantModel: "ollama/qwen2.5",
			wantInfo:  "test-multica: agent foreman-test-agent → ollama/qwen2.5 (" + resetBackendURL + ")",
			wantEnv:   map[string]string{"OLLAMA_BASE_URL": resetBackendURL},
		},
		{
			name:      "latest tag",
			served:    "some-model:latest",
			wantModel: "ollama/some-model",
			wantInfo:  "test-multica: agent foreman-test-agent → ollama/some-model (" + resetBackendURL + ")",
			wantEnv:   map[string]string{"OLLAMA_BASE_URL": resetBackendURL},
		},
		{
			name:      "no local backend",
			served:    "",
			wantModel: "",
			wantInfo:  "test-multica: 未配置模型端点",
			wantEnv:   map[string]string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := runReset(t, resetOptions{served: tc.served})
			if run.err != nil {
				t.Fatalf("reset.sh did not finish: %v\n--- stderr ---\n%s", run.err, run.stderr)
			}
			if !strings.Contains(run.stderr, tc.wantInfo) {
				t.Errorf("reset.sh does not report %q\n--- stderr ---\n%s", tc.wantInfo, run.stderr)
			}

			modelWrites := run.requests("PUT", "/api/agents/"+resetAgentID)
			if len(modelWrites) != 1 {
				t.Fatalf("reset.sh sent %d model writes to /api/agents/%s, want 1", len(modelWrites), resetAgentID)
			}
			var model struct {
				Model string `json:"model"`
			}
			decodeResetBody(t, modelWrites[0], &model)
			if model.Model != tc.wantModel {
				t.Errorf("the model write carries %q, want %q", model.Model, tc.wantModel)
			}

			envWrites := run.requests("PUT", "/api/agents/"+resetAgentID+"/env")
			if len(envWrites) != 1 {
				t.Fatalf("reset.sh sent %d env writes to /api/agents/%s/env, want 1", len(envWrites), resetAgentID)
			}
			var env struct {
				CustomEnv map[string]string `json:"custom_env"`
			}
			decodeResetBody(t, envWrites[0], &env)
			if !maps.Equal(env.CustomEnv, tc.wantEnv) {
				t.Errorf("custom_env = %v, want exactly %v — the env endpoint replaces the map, so every branch states the whole map", env.CustomEnv, tc.wantEnv)
			}
		})
	}
}

// TestResetFailsWhenTheModelBackendReadFails judges the branch the read's own
// answer cannot cover: an error that is not NotFound is not evidence that no
// local backend is deployed. Falling to the chain's floor there would clear a
// working agent's model behind a green self-check, so reset.sh must exit
// non-zero and write nothing at all.
func TestResetFailsWhenTheModelBackendReadFails(t *testing.T) {
	run := runReset(t, resetOptions{
		served:         "qwen2.5:0.5b",
		modelReadError: `Error from server (Forbidden): deployments.apps "` + resetModelDeploy + `" is forbidden`,
	})
	if run.err == nil {
		t.Fatalf("reset.sh finished with status 0 after the model backend read failed\n--- stderr ---\n%s", run.stderr)
	}
	want := "cannot read Deployment " + resetNamespace + "/" + resetModelDeploy
	if !strings.Contains(run.stderr, want) {
		t.Errorf("reset.sh does not report the failed read (%q)\n--- stderr ---\n%s", want, run.stderr)
	}
	for _, path := range []string{"/api/agents/" + resetAgentID, "/api/agents/" + resetAgentID + "/env"} {
		if writes := run.requests("PUT", path); len(writes) != 0 {
			t.Errorf("reset.sh sent %d PUT %s after the read failed (%v) — a failed read must not be written as an empty model", len(writes), path, writes)
		}
	}
}

// TestResetFallsToTheFloorWhenTheModelBackendIsNotDeployed judges the other
// side of the same classification: NotFound is an answer — the backend was
// never deployed — so the chain's floor applies and reset.sh still succeeds.
func TestResetFallsToTheFloorWhenTheModelBackendIsNotDeployed(t *testing.T) {
	run := runReset(t, resetOptions{
		modelReadError: `Error from server (NotFound): deployments.apps "` + resetModelDeploy + `" not found`,
	})
	if run.err != nil {
		t.Fatalf("reset.sh did not finish: %v\n--- stderr ---\n%s", run.err, run.stderr)
	}
	modelWrites := run.requests("PUT", "/api/agents/"+resetAgentID)
	if len(modelWrites) != 1 {
		t.Fatalf("reset.sh sent %d model writes to /api/agents/%s, want 1", len(modelWrites), resetAgentID)
	}
	var model struct {
		Model string `json:"model"`
	}
	decodeResetBody(t, modelWrites[0], &model)
	if model.Model != "" {
		t.Errorf("the model write carries %q, want the cleared value", model.Model)
	}
	envWrites := run.requests("PUT", "/api/agents/"+resetAgentID+"/env")
	if len(envWrites) != 1 {
		t.Fatalf("reset.sh sent %d env writes to /api/agents/%s/env, want 1", len(envWrites), resetAgentID)
	}
	var env struct {
		CustomEnv map[string]string `json:"custom_env"`
	}
	decodeResetBody(t, envWrites[0], &env)
	if !maps.Equal(env.CustomEnv, map[string]string{}) {
		t.Errorf("custom_env = %v, want the cleared map", env.CustomEnv)
	}
}

// resetOptions are the cluster answers the stubs give reset.sh.
type resetOptions struct {
	// served is the model the stubbed model backend reports.
	served string
	// modelReadError, when set, makes every read of the model backend fail
	// with this message instead of answering.
	modelReadError string
}

// resetCall is one request the stubbed test server received.
type resetCall struct {
	method, path, body string
}

// resetRun is one reset.sh invocation against the stubs.
type resetRun struct {
	calls  []resetCall
	stderr string
	err    error
}

// requests is every recorded call with the given method and path.
func (r resetRun) requests(method, path string) []resetCall {
	matched := []resetCall{}
	for _, call := range r.calls {
		if call.method == method && call.path == path {
			matched = append(matched, call)
		}
	}
	return matched
}

// decodeResetBody reads one recorded request body.
func decodeResetBody(t *testing.T, call resetCall, into any) {
	t.Helper()
	if err := json.Unmarshal([]byte(call.body), into); err != nil {
		t.Fatalf("%s %s carried a body that is not JSON: %v\nbody: %s", call.method, call.path, err, call.body)
	}
}

// runReset runs the real reset.sh in a sandbox whose stubs answer every call
// before the model writes; `opts` is what the stubbed cluster reports.
func runReset(t *testing.T, opts resetOptions) resetRun {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skipf("host has no sh: %v", err)
	}
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	entryDir := filepath.Join(dir, "test-multica")
	homeDir := filepath.Join(dir, "home")
	stateDir := filepath.Join(dir, "state")
	profileConfig := filepath.Join(homeDir, ".multica", "profiles", "test-multica", "config.json")
	for _, d := range []string{binDir, entryDir, stateDir, filepath.Dir(profileConfig)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("create %s: %v", d, err)
		}
	}
	writeExecutable(t, filepath.Join(binDir, "kubectl"), resetKubectlStub)
	writeExecutable(t, filepath.Join(binDir, "curl"), resetCurlStub)
	// The entries resolve their own directory, so the sandbox carries the
	// script text: only the gate beside it is a stub.
	for _, name := range []string{"reset.sh", "lib.sh"} {
		body, err := os.ReadFile(filepath.Join(libDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		writeExecutable(t, filepath.Join(entryDir, name), string(body))
	}
	writeExecutable(t, filepath.Join(entryDir, "selfcheck.sh"), "#!/bin/sh\nexit 0\n")
	profile := `{"server_url":"` + resetServerURL + `","app_url":"` + resetServerURL +
		`","token":"test-token","workspace_id":"` + resetWorkspaceID + `"}`
	if err := os.WriteFile(profileConfig, []byte(profile+"\n"), 0o600); err != nil {
		t.Fatalf("write the CLI profile: %v", err)
	}

	callsPath := filepath.Join(dir, "curl.calls")
	cmd := exec.Command("sh", filepath.Join(entryDir, "reset.sh"))
	cmd.Dir = dir
	cmd.Env = envWith(
		map[string]string{
			"PATH":   binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
			"HOME":   homeDir,
			"TMPDIR": dir,
		},
		map[string]string{
			"STUB_CALLS":            callsPath,
			"STUB_KUBECTL_CALLS":    filepath.Join(dir, "kubectl.calls"),
			"STUB_STATE":            stateDir,
			"STUB_MODEL":            opts.served,
			"STUB_MODEL_READ_ERROR": opts.modelReadError,
			"STUB_AGENT_ID":         resetAgentID,
			"STUB_AGENT_NAME":       resetAgentName,
			"STUB_WORKSPACE_ID":     resetWorkspaceID,
			"STUB_REPO_URL":         resetRepoURL,
			"STUB_CHAT_TITLE":       resetChatTitle,
			// The scripts' own defaults are restated so an exported
			// TEST_MULTICA_* on the host cannot decide what this test reads.
			"TEST_MULTICA_PROFILE":      "test-multica",
			"TEST_MULTICA_SERVER_URL":   resetServerURL,
			"TEST_MULTICA_NS":           resetNamespace,
			"TEST_MULTICA_MODEL_DEPLOY": resetModelDeploy,
			"TEST_MULTICA_INSTANCE_NS":  "foreman",
			"TEST_MULTICA_INSTANCE":     "foreman",
			"TEST_MULTICA_AGENT":        resetAgentName,
			"TEST_MULTICA_CHAT":         resetChatTitle,
			"TEST_MULTICA_REPO":         resetRepoURL,
			"TEST_MULTICA_ENTRY_URL":    "http://entry.test.local:30300",
			// No endpoint triple: the local backend is the source this test
			// judges. Blanks, not absences, so an exported TEST_MULTICA_* on
			// the host cannot switch reset.sh to the endpoint branch.
			"TEST_MULTICA_MODEL_BASE_URL":     "",
			"TEST_MULTICA_MODEL_ID":           "",
			"TEST_MULTICA_MODEL_API_KEY":      "",
			"TEST_MULTICA_MODEL_API_KEY_FILE": "",
			// One attempt: an unexpected call is a broken stub, not a flaky
			// API server.
			"TEST_MULTICA_KUBECTL_RETRIES": "1",
		},
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	run := resetRun{err: cmd.Run()}
	run.stderr = stderr.String()
	run.calls = parseResetCalls(t, callsPath)
	return run
}

// parseResetCalls reads the request log: one method<TAB>url<TAB>body line per
// call, with the test server's address trimmed off the URL.
func parseResetCalls(t *testing.T, path string) []resetCall {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read the request log: %v", err)
	}
	calls := []resetCall{}
	for _, line := range strings.Split(strings.TrimSuffix(string(body), "\n"), "\n") {
		if line == "" {
			continue
		}
		method, rest, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("request log line %q is not method<TAB>url<TAB>body", line)
		}
		url, callBody, _ := strings.Cut(rest, "\t")
		calls = append(calls, resetCall{
			method: method,
			path:   strings.TrimPrefix(url, resetServerURL),
			body:   callBody,
		})
	}
	return calls
}
