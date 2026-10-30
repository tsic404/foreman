// The failure agent is what makes the failed terminal class reproducible: a
// second agent bound to the same runtime, deliberately without a model source,
// so a dispatched task ends failed on its own. Neither half is observable
// without a cluster, so the two entries run here against the stubs that answer
// the server calls: reset.sh must create the row and clear its model source on
// every round, and bind-agent.sh must bind it to the runtime the instance
// registered.
package testinfra

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	faultAgentName = "foreman-test-agent-failing"
	faultAgentID   = "agent-" + faultAgentName
)

// faultAgentList is the agent list a round after the first sees: both rows.
const faultAgentList = `[{"id":"` + resetAgentID + `","name":"` + resetAgentName + `","archived_at":null},` +
	`{"id":"` + faultAgentID + `","name":"` + faultAgentName + `","archived_at":null}]`

// TestResetClearsTheFailureAgentsModelOnEveryRound judges the failure
// guarantee: the row may survive from an earlier round (the wipe keeps agent
// rows), so every reset rewrites both model fields even though nothing else
// touches that agent. A leftover selector would turn it into a second working
// agent and the failed class would quietly disappear.
func TestResetClearsTheFailureAgentsModelOnEveryRound(t *testing.T) {
	for _, tc := range []struct {
		name       string
		agents     string
		wantCreate bool
	}{
		{name: "created when missing", agents: "", wantCreate: true},
		{name: "cleared when present", agents: faultAgentList, wantCreate: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := runReset(t, resetOptions{served: "qwen2.5:0.5b", agents: tc.agents})
			if run.err != nil {
				t.Fatalf("reset.sh did not finish: %v\n--- stderr ---\n%s", run.err, run.stderr)
			}

			creates := run.requests("POST", "/api/agents")
			if tc.wantCreate && len(creates) != 1 {
				t.Fatalf("reset.sh created %d agents, want 1 (the failure agent): %v", len(creates), creates)
			}
			if !tc.wantCreate && len(creates) != 0 {
				t.Fatalf("reset.sh recreated the failure agent: %v", creates)
			}
			if tc.wantCreate {
				var body struct {
					Name string `json:"name"`
				}
				decodeResetBody(t, creates[0], &body)
				if body.Name != faultAgentName {
					t.Fatalf("the created agent is %q, want the failure agent %q", body.Name, faultAgentName)
				}
			}

			modelWrites := run.requests("PUT", "/api/agents/"+faultAgentID)
			if len(modelWrites) != 1 {
				t.Fatalf("reset.sh sent %d model writes to the failure agent, want 1: %v", len(modelWrites), modelWrites)
			}
			var model struct {
				Model string `json:"model"`
			}
			decodeResetBody(t, modelWrites[0], &model)
			if model.Model != "" {
				t.Errorf("the failure agent's model is %q, want empty — its missing model source is the failure guarantee", model.Model)
			}

			envWrites := run.requests("PUT", "/api/agents/"+faultAgentID+"/env")
			if len(envWrites) != 1 {
				t.Fatalf("reset.sh sent %d env writes to the failure agent, want 1: %v", len(envWrites), envWrites)
			}
			var env struct {
				CustomEnv map[string]string `json:"custom_env"`
			}
			decodeResetBody(t, envWrites[0], &env)
			if len(env.CustomEnv) != 0 {
				t.Errorf("the failure agent's custom_env is %v, want empty", env.CustomEnv)
			}
		})
	}
}

// The bind entry's only cluster acts are the curl calls; their answers decide
// what the entry reads back, so the stub keeps the binding it was sent.
const bindCurlStub = `#!/bin/sh
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
	-H|-m) shift 2 ;;
	*://*) url=$1; shift ;;
	*) shift ;;
	esac
done
printf '%s\t%s\t%s\n' "$method" "$url" "$body" >>"$STUB_CALLS"
if [ "$out" = /dev/null ]; then
	printf '%s' 200
	exit 0
fi
path=${url#*://}
path=${path#*/}
case "$method $path" in
"GET api/agents") cat "$STUB_AGENTS" ;;
"GET api/runtimes") printf '[{"id":"rt-omp","provider":"omp","name":"omp-runtime"}]' ;;
"PUT api/agents/"*)
	id=${path#api/agents/}
	printf '%s' "$body" >"$STUB_STATE/put.$id"
	printf '{}'
	;;
"GET api/agents/"*)
	id=${path#api/agents/}
	printf '{"id":"%s","runtime_id":"%s","instructions":"%s"}' "$id" \
		"$(jq -r '.runtime_id // ""' "$STUB_STATE/put.$id" 2>/dev/null || true)" \
		"$(jq -r '.instructions // ""' "$STUB_STATE/put.$id" 2>/dev/null || true)"
	;;
"GET api/autopilots") printf '{"autopilots":[]}' ;;
"POST api/autopilots") printf '{"id":"ap-1"}' ;;
*)
	printf 'stub curl: unexpected call: %s %s\n' "$method" "$url" >&2
	exit 1
	;;
esac
`

// bindRun runs bind-agent.sh in a sandbox whose only cluster face is the curl
// stub above.
func bindRun(t *testing.T, agents string) (calls []resetCall, out string, err error) {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skipf("host has no sh: %v", err)
	}
	dir := t.TempDir()
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
	writeExecutable(t, filepath.Join(binDir, "curl"), bindCurlStub)
	for _, name := range []string{"bind-agent.sh", "lib.sh"} {
		body, err := os.ReadFile(filepath.Join(libDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		writeExecutable(t, filepath.Join(entryDir, name), string(body))
	}
	agentsPath := filepath.Join(dir, "agents.json")
	if err := os.WriteFile(agentsPath, []byte(agents), 0o644); err != nil {
		t.Fatalf("seed the agent list: %v", err)
	}
	profile := `{"server_url":"` + resetServerURL + `","app_url":"` + resetServerURL +
		`","token":"test-token","workspace_id":"` + resetWorkspaceID + `"}`
	if err := os.WriteFile(filepath.Join(profileDir, "config.json"), []byte(profile+"\n"), 0o600); err != nil {
		t.Fatalf("write the CLI profile: %v", err)
	}

	callsPath := filepath.Join(dir, "curl.calls")
	cmd := exec.Command("sh", filepath.Join(entryDir, "bind-agent.sh"), "--wait", "1")
	cmd.Dir = dir
	cmd.Env = envWith(
		map[string]string{
			"PATH":   binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
			"HOME":   homeDir,
			"TMPDIR": dir,
		},
		map[string]string{
			"STUB_CALLS":               callsPath,
			"STUB_STATE":               stateDir,
			"STUB_AGENTS":              agentsPath,
			"STUB_AGENT_ID":            resetAgentID,
			"TEST_MULTICA_PROFILE":     "test-multica",
			"TEST_MULTICA_SERVER_URL":  resetServerURL,
			"TEST_MULTICA_ENTRY_URL":   "http://entry.test.local:30300",
			"TEST_MULTICA_AGENT":       resetAgentName,
			"TEST_MULTICA_FAULT_AGENT": faultAgentName,
		},
	)
	raw, err := cmd.CombinedOutput()
	calls = parseResetCalls(t, callsPath)
	return calls, string(raw), err
}

// TestBindAgentBindsBothAgentRows: the failure agent's tasks only reach a Job
// through the runtime the instance registered, so the bind entry must cover
// both rows — binding only the working agent leaves the failed class stuck in
// the queue.
func TestBindAgentBindsBothAgentRows(t *testing.T) {
	agents := `[{"id":"agent-main","name":"` + resetAgentName + `","archived_at":null},` +
		`{"id":"agent-fault","name":"` + faultAgentName + `","archived_at":null}]`
	calls, out, err := bindRun(t, agents)
	if err != nil {
		t.Fatalf("bind-agent.sh did not finish: %v\n%s", err, out)
	}
	for _, id := range []string{"agent-main", "agent-fault"} {
		bound := 0
		for _, call := range calls {
			if call.method == "PUT" && call.path == "/api/agents/"+id {
				var body struct {
					RuntimeID string `json:"runtime_id"`
				}
				if err := json.Unmarshal([]byte(call.body), &body); err != nil {
					t.Fatalf("%s carried a body that is not JSON: %v", call.path, err)
				}
				if body.RuntimeID != "rt-omp" {
					t.Errorf("%s bound to %q, want rt-omp", call.path, body.RuntimeID)
				}
				bound++
			}
		}
		if bound != 1 {
			t.Errorf("bind-agent.sh sent %d bindings to %s, want 1\n%s", bound, id, out)
		}
	}
}

// TestBindAgentRefusesAMissingFailureAgent: an environment whose reset never
// created the failure agent is not the fixture, and binding silently would
// leave the failed class broken; the entry must name the missing object.
func TestBindAgentRefusesAMissingFailureAgent(t *testing.T) {
	agents := `[{"id":"agent-main","name":"` + resetAgentName + `","archived_at":null}]`
	_, out, err := bindRun(t, agents)
	if err == nil {
		t.Fatalf("bind-agent.sh accepted an environment without the failure agent:\n%s", out)
	}
	if !strings.Contains(out, faultAgentName) {
		t.Fatalf("the refusal does not name the missing agent %q:\n%s", faultAgentName, out)
	}
}
