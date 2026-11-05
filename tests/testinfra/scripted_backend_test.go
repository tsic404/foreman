// The scripted agent backend is what makes the in-flight window cases
// constructible: the served model declares the task's window itself (one
// warmup call, then `sleep` holds) instead of answering in prose, so the Job's
// agent has a real process that can be observed, cancelled, deadlined or
// carried across a fault. These entries pin
// the two halves that can be judged without a cluster: the manifest the round
// applies (one deployment, one service that points at it, the model id in a
// variable) and the server's decision (call for the documented prompt shape,
// final text for everything else, and the same answer in both wire forms).
package testinfra

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	scriptedManifest  = "../../testinfra/test-multica/scripted-backend.yaml"
	scriptedServer    = "../../testinfra/test-multica/scripted-agent.py"
	scriptedDeploy    = "test-multica-scripted"
	scriptedService   = "test-multica-scripted"
	scriptedModelID   = "fixture:deterministic"
	scriptedSleep     = "60"
	scriptedHoldTurns = 3
	scriptedWarmup    = "multica issue list --output json"
)

// scriptedDocuments decodes the manifest into its objects.
func scriptedDocuments(t *testing.T) map[string]*unstructured.Unstructured {
	t.Helper()
	raw, err := os.ReadFile(scriptedManifest)
	if err != nil {
		t.Fatalf("read %s: %v", scriptedManifest, err)
	}
	objects := map[string]*unstructured.Unstructured{}
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
	for {
		var doc unstructured.Unstructured
		err := decoder.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("parse %s: %v", scriptedManifest, err)
		}
		if doc.GetKind() == "" {
			continue
		}
		objects[doc.GetKind()+"/"+doc.GetName()] = &doc
	}
	return objects
}

// TestScriptedBackendManifestIsOneDeploymentOneService pins the manifest's
// shape: the service selects the deployment's pods and targets its port, and
// the model id is a rendered value the server reports — ./reset.sh and
// ./selfcheck.sh read it back from the applied Deployment, so a hard-coded
// second copy cannot disagree with what is served.
func TestScriptedBackendManifestIsOneDeploymentOneService(t *testing.T) {
	objects := scriptedDocuments(t)
	config, ok := objects["ConfigMap/test-multica-scripted-server"]
	if !ok {
		t.Fatalf("the scripted manifest declares no ConfigMap/test-multica-scripted-server: %v", keysOf(objects))
	}
	script, _, _ := unstructured.NestedString(config.Object, "data", "server.py")
	if !strings.Contains(script, "__SCRIPTED_SERVER_PY__") {
		t.Errorf("the ConfigMap carries no server placeholder — deploy.sh would inject the file into nothing:\n%s", script)
	}

	deploy, ok := objects["Deployment/"+scriptedDeploy]
	if !ok {
		t.Fatalf("the scripted manifest declares no Deployment/%s: %v", scriptedDeploy, keysOf(objects))
	}
	env, _, _ := unstructured.NestedSlice(deploy.Object, "spec", "template", "spec", "containers")
	if len(env) != 1 {
		t.Fatalf("the scripted Deployment runs %d containers, want exactly one model server", len(env))
	}
	container, _ := env[0].(map[string]any)
	vars, _, _ := unstructured.NestedSlice(container, "env")
	found := false
	for _, raw := range vars {
		entry, _ := raw.(map[string]any)
		if entry["name"] == "SCRIPTED_MODEL_ID" {
			found = true
			if entry["value"] != "__SCRIPTED_MODEL_ID__" {
				t.Errorf("SCRIPTED_MODEL_ID is %v, want the rendered placeholder", entry["value"])
			}
		}
	}
	if !found {
		t.Errorf("the scripted Deployment carries no SCRIPTED_MODEL_ID env — reset.sh reads the served id back from it")
	}
	window := map[string]string{
		"SCRIPTED_SLEEP":          "__SCRIPTED_SLEEP__",
		"SCRIPTED_HOLD_TURNS":     "__SCRIPTED_HOLD_TURNS__",
		"SCRIPTED_WARMUP_COMMAND": "__SCRIPTED_WARMUP__",
	}
	for _, raw := range vars {
		entry, _ := raw.(map[string]any)
		name, _ := entry["name"].(string)
		if want, ok := window[name]; ok {
			if entry["value"] != want {
				t.Errorf("%s is %v, want the rendered placeholder %q", name, entry["value"], want)
			}
			delete(window, name)
		}
	}
	for name := range window {
		t.Errorf("the scripted Deployment carries no %s env — the fixture's window would fall back to the server default", name)
	}

	// The pod template carries the served file's digest: without it a
	// code-only change leaves the running pod on the old script, and the
	// round would verify code that is not the code it deployed.
	digest, _, _ := unstructured.NestedString(deploy.Object,
		"spec", "template", "metadata", "annotations", "test-multica/scripted-digest")
	if digest != "__SCRIPTED_DIGEST__" {
		t.Errorf("the pod template annotation is %q, want the rendered digest placeholder", digest)
	}

	// The image is pinned like every other manifest: a floating tag would let a
	// node run a different server base than the round was verified against.
	containers, _, _ := unstructured.NestedSlice(deploy.Object, "spec", "template", "spec", "containers")
	if len(containers) == 1 {
		container, _ := containers[0].(map[string]any)
		if image, _ := container["image"].(string); !strings.Contains(image, "-alpine3.20") {
			t.Errorf("the scripted server image is %q, want a pinned python:3.12-alpine3.20 base", image)
		}
	}

	service, ok := objects["Service/"+scriptedService]
	if !ok {
		t.Fatalf("the scripted manifest declares no Service/%s: %v", scriptedService, keysOf(objects))
	}
	selector, _, _ := unstructured.NestedStringMap(service.Object, "spec", "selector")
	labels, _, _ := unstructured.NestedStringMap(deploy.Object, "spec", "template", "metadata", "labels")
	for key, value := range selector {
		if labels[key] != value {
			t.Errorf("the Service selects %s=%s, the Deployment's pods carry %s=%s", key, value, key, labels[key])
		}
	}
	ports, _, _ := unstructured.NestedSlice(service.Object, "spec", "ports")
	if len(ports) != 1 {
		t.Fatalf("the Service carries %d ports, want one", len(ports))
	}
	port, _ := ports[0].(map[string]any)
	if port["port"] != int64(8080) || port["targetPort"] != "http" {
		t.Errorf("the Service exposes %v→%v, want 8080→http", port["port"], port["targetPort"])
	}
}

func keysOf(objects map[string]*unstructured.Unstructured) []string {
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	return keys
}

// TestScriptedBackendHoldsItsDeclaredWindow is the server's own behavioural
// surface, driven over HTTP exactly as the Job's agent drives it: a
// conversation that has not spent its hold turns yet answers with the tool
// call carrying the fixture's `sleep`, and the turn after the hold answers
// with the final text. The prompt text is deliberately a runtime brief — the
// window is the fixture's declaration (SCRIPTED_SLEEP / SCRIPTED_HOLD_TURNS),
// never something parsed out of the task's instructions. Both wire forms are
// judged because the endpoint registration declares the dialect the agent
// uses, not the form a test happens to send.
func TestScriptedBackendHoldsItsDeclaredWindow(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skipf("host has no python3 to serve the scripted backend: %v", err)
	}
	base := startScriptedBackend(t)

	tool := map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        "bash",
			"description": "run a shell command",
			"parameters": map[string]any{
				"type":     "object",
				"required": []any{"command", "i"},
				"properties": map[string]any{
					"command": map[string]any{"type": "string"},
					"i":       map[string]any{"type": "string"},
				},
			},
		},
	}
	taskBrief := "You are running as a local coding agent for a Multica workspace. Your assigned issue ID is: 01a1abf2-0aab-719b-8d92-2a3e31c3c4cd"

	t.Run("streamed first turn holds the window", func(t *testing.T) {
		body := map[string]any{
			"model":  scriptedModelID,
			"stream": true,
			"tools":  []any{tool},
			"messages": []any{
				map[string]any{"role": "user", "content": taskBrief},
			},
		}
		events := postScriptedStream(t, base+"/v1/chat/completions", body)
		var call struct {
			Choices []struct {
				Delta struct {
					ToolCalls []struct {
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
			Usage struct {
				TotalTokens int `json:"total_tokens"`
			} `json:"usage"`
		}
		finished := ""
		var toolCall struct {
			Function struct {
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			} `json:"function"`
		}
		for _, event := range events {
			var chunk struct {
				Choices []struct {
					Delta struct {
						ToolCalls []struct {
							Function struct {
								Name      string `json:"name"`
								Arguments string `json:"arguments"`
							} `json:"function"`
						} `json:"tool_calls"`
					} `json:"delta"`
					FinishReason string `json:"finish_reason"`
				} `json:"choices"`
				Usage struct {
					TotalTokens int `json:"total_tokens"`
				} `json:"usage"`
			}
			if err := json.Unmarshal(event, &chunk); err != nil {
				t.Fatalf("chunk is not JSON: %v\n%s", err, event)
			}
			call = chunk
			if len(chunk.Choices) > 0 && len(chunk.Choices[0].Delta.ToolCalls) > 0 {
				toolCall = chunk.Choices[0].Delta.ToolCalls[0]
			}
			if len(chunk.Choices) > 0 && chunk.Choices[0].FinishReason != "" {
				finished = chunk.Choices[0].FinishReason
			}
		}
		if finished != "tool_calls" {
			t.Fatalf("the stream ended with %q, want tool_calls", finished)
		}
		if toolCall.Function.Name == "" {
			t.Fatalf("the stream carries no tool call: %+v", call)
		}
		fn := toolCall.Function
		if fn.Name != "bash" {
			t.Errorf("the call names tool %q, want the offered bash", fn.Name)
		}
		var args struct {
			Command string `json:"command"`
			I       string `json:"i"`
		}
		if err := json.Unmarshal([]byte(fn.Arguments), &args); err != nil {
			t.Fatalf("the call's arguments are not JSON: %v\n%s", err, fn.Arguments)
		}
		if args.Command != scriptedWarmup {
			t.Errorf("the first call runs %q, want the fixture's warmup %q", args.Command, scriptedWarmup)
		}
		if args.I == "" {
			t.Errorf("the call leaves the intent empty; the agent's tool schema requires it")
		}
		if call.Usage.TotalTokens == 0 {
			t.Errorf("the stream's terminal chunk carries no usage; the round's token count would be unanswerable")
		}
	})

	t.Run("the second turn holds the window with sleep", func(t *testing.T) {
		body := map[string]any{
			"model":  scriptedModelID,
			"stream": false,
			"tools":  []any{tool},
			"messages": []any{
				map[string]any{"role": "user", "content": taskBrief},
				map[string]any{"role": "assistant", "content": nil},
				map[string]any{"role": "tool", "tool_call_id": "call_1", "content": "[]"},
			},
		}
		var answer struct {
			Choices []struct {
				Message struct {
					ToolCalls []struct {
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"message"`
			} `json:"choices"`
		}
		postScriptedJSON(t, base+"/v1/chat/completions", body, &answer)
		if len(answer.Choices) != 1 || len(answer.Choices[0].Message.ToolCalls) != 1 {
			t.Fatalf("the second turn is not one tool call: %+v", answer.Choices)
		}
		var args struct {
			Command string `json:"command"`
		}
		if err := json.Unmarshal([]byte(answer.Choices[0].Message.ToolCalls[0].Function.Arguments), &args); err != nil {
			t.Fatalf("the call's arguments are not JSON: %v", err)
		}
		if args.Command != "sleep "+scriptedSleep {
			t.Errorf("the second call runs %q, want the fixture's `sleep %s`", args.Command, scriptedSleep)
		}
	})

	// The window is exactly HOLD_TURNS `sleep` calls: the warmup is turn 0 and
	// must not consume a hold turn, or the recovery round's HOLD_TURNS=1 would
	// hold for no time at all.
	t.Run("the window is exactly HOLD_TURNS sleeps", func(t *testing.T) {
		commandAtTurn := func(t *testing.T, turns int) (string, string) {
			t.Helper()
			messages := []any{map[string]any{"role": "user", "content": taskBrief}}
			for turn := 0; turn < turns; turn++ {
				messages = append(
					messages,
					map[string]any{"role": "assistant", "content": nil},
					map[string]any{"role": "tool", "tool_call_id": "call_1", "content": "[]"},
				)
			}
			body := map[string]any{
				"model":    scriptedModelID,
				"stream":   false,
				"tools":    []any{tool},
				"messages": messages,
			}
			var answer struct {
				Choices []struct {
					Message struct {
						Content   string `json:"content"`
						ToolCalls []struct {
							Function struct {
								Arguments string `json:"arguments"`
							} `json:"function"`
						} `json:"tool_calls"`
					} `json:"message"`
				} `json:"choices"`
			}
			postScriptedJSON(t, base+"/v1/chat/completions", body, &answer)
			if len(answer.Choices) != 1 {
				t.Fatalf("turn %d carries %d choices", turns, len(answer.Choices))
			}
			message := answer.Choices[0].Message
			if len(message.ToolCalls) == 0 {
				return "content", message.Content
			}
			var args struct {
				Command string `json:"command"`
			}
			if err := json.Unmarshal([]byte(message.ToolCalls[0].Function.Arguments), &args); err != nil {
				t.Fatalf("turn %d: the call's arguments are not JSON: %v", turns, err)
			}
			return "call", args.Command
		}

		if kind, command := commandAtTurn(t, 0); kind != "call" || command != scriptedWarmup {
			t.Errorf("turn 0 = %s %q, want the warmup call %q", kind, command, scriptedWarmup)
		}
		for turns := 1; turns <= scriptedHoldTurns; turns++ {
			if kind, command := commandAtTurn(t, turns); kind != "call" || command != "sleep "+scriptedSleep {
				t.Errorf("turn %d = %s %q, want `sleep %s`", turns, kind, command, scriptedSleep)
			}
		}
		if kind, command := commandAtTurn(t, scriptedHoldTurns+1); kind != "content" || command != "DONE" {
			t.Errorf("turn %d = %s %q, want the final DONE", scriptedHoldTurns+1, kind, command)
		}
	})

	t.Run("a one-turn window still holds one sleep", func(t *testing.T) {
		one := startScriptedBackend(t, "SCRIPTED_HOLD_TURNS=1")
		messages := []any{map[string]any{"role": "user", "content": taskBrief}}
		commandAt := func(t *testing.T, turns int) (string, string) {
			t.Helper()
			body := map[string]any{
				"model":    scriptedModelID,
				"stream":   false,
				"tools":    []any{tool},
				"messages": messages,
			}
			var answer struct {
				Choices []struct {
					Message struct {
						Content   string `json:"content"`
						ToolCalls []struct {
							Function struct {
								Arguments string `json:"arguments"`
							} `json:"function"`
						} `json:"tool_calls"`
					} `json:"message"`
				} `json:"choices"`
			}
			postScriptedJSON(t, one+"/v1/chat/completions", body, &answer)
			if len(answer.Choices) != 1 {
				t.Fatalf("turn %d carries %d choices", turns, len(answer.Choices))
			}
			message := answer.Choices[0].Message
			if len(message.ToolCalls) == 0 {
				return "content", message.Content
			}
			var args struct {
				Command string `json:"command"`
			}
			if err := json.Unmarshal([]byte(message.ToolCalls[0].Function.Arguments), &args); err != nil {
				t.Fatalf("turn %d: arguments are not JSON: %v", turns, err)
			}
			return "call", args.Command
		}
		for _, turn := range []int{0, 1} {
			kind, command := commandAt(t, turn)
			if turn == 0 && (kind != "call" || command != scriptedWarmup) {
				t.Fatalf("turn 0 = %s %q, want the warmup", kind, command)
			}
			if turn == 1 && (kind != "call" || command != "sleep "+scriptedSleep) {
				t.Fatalf("turn 1 = %s %q, want one `sleep %s` before the answer", kind, command, scriptedSleep)
			}
			messages = append(
				messages,
				map[string]any{"role": "assistant", "content": nil},
				map[string]any{"role": "tool", "tool_call_id": "call_1", "content": "[]"},
			)
		}
		if kind, command := commandAt(t, 2); kind != "content" || command != "DONE" {
			t.Errorf("turn 2 = %s %q, want the final DONE", kind, command)
		}
	})

	t.Run("the turn after the hold answers the tool result", func(t *testing.T) {
		messages := []any{map[string]any{"role": "user", "content": taskBrief}}
		for turn := 0; turn < scriptedHoldTurns+1; turn++ {
			messages = append(
				messages,
				map[string]any{"role": "assistant", "content": nil},
				map[string]any{"role": "tool", "tool_call_id": "call_1", "content": "(no output)"},
			)
		}
		body := map[string]any{
			"model":    scriptedModelID,
			"stream":   false,
			"tools":    []any{tool},
			"messages": messages,
		}
		var answer struct {
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
			Usage struct {
				TotalTokens int `json:"total_tokens"`
			} `json:"usage"`
		}
		postScriptedJSON(t, base+"/v1/chat/completions", body, &answer)
		if len(answer.Choices) != 1 || answer.Choices[0].FinishReason != "stop" {
			t.Fatalf("the second turn's choices are %+v, want one stop", answer.Choices)
		}
		if answer.Choices[0].Message.Content == "" {
			t.Errorf("the second turn carries no final text")
		}
		if answer.Usage.TotalTokens == 0 {
			t.Errorf("the second turn carries no usage")
		}
	})

	t.Run("a conversation past the hold gets no further call", func(t *testing.T) {
		messages := []any{map[string]any{"role": "user", "content": taskBrief}}
		for turn := 0; turn < scriptedHoldTurns+2; turn++ {
			messages = append(
				messages,
				map[string]any{"role": "assistant", "content": nil},
				map[string]any{"role": "tool", "tool_call_id": "call_1", "content": "(no output)"},
			)
		}
		body := map[string]any{
			"model":    scriptedModelID,
			"stream":   false,
			"tools":    []any{tool},
			"messages": messages,
		}
		var answer struct {
			Choices []struct {
				Message struct {
					Content   string `json:"content"`
					ToolCalls []any  `json:"tool_calls"`
				} `json:"message"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
		}
		postScriptedJSON(t, base+"/v1/chat/completions", body, &answer)
		if len(answer.Choices) != 1 {
			t.Fatalf("choices = %+v, want one", answer.Choices)
		}
		message := answer.Choices[0].Message
		if len(message.ToolCalls) != 0 {
			t.Errorf("a conversation past the hold produced another tool call: %+v", message.ToolCalls)
		}
		if message.Content != "DONE" {
			t.Errorf("the final answer is %q, want DONE", message.Content)
		}
	})
}

// startScriptedBackend starts the server on an ephemeral loopback port and
// returns its base URL. The process is killed with the test.
func startScriptedBackend(t *testing.T, extra ...string) string {
	t.Helper()
	cmd := exec.Command("python3", scriptedServer, "serve", "127.0.0.1:0")
	cmd.Env = append(append(os.Environ(),
		"SCRIPTED_SLEEP="+scriptedSleep,
		"SCRIPTED_HOLD_TURNS="+strconv.Itoa(scriptedHoldTurns),
		"SCRIPTED_WARMUP_COMMAND="+scriptedWarmup,
	), extra...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("pipe the server's stdout: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the scripted backend: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		if t.Failed() && stderr.Len() > 0 {
			t.Logf("scripted backend output:\n%s", stderr.String())
		}
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("the scripted backend printed no address: %v\n%s", err, stderr.String())
	}
	address := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "listening "))
	if _, port, err := net.SplitHostPort(address); err != nil || port == "" {
		t.Fatalf("the scripted backend announced %q, not host:port", line)
	}
	return "http://" + address
}

func postScriptedJSON(t *testing.T, url string, body map[string]any, into any) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode the request: %v", err)
	}
	response, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("post the request: %v", err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("the server answered HTTP %d: %s", response.StatusCode, raw)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("the answer is not JSON: %v\n%s", err, raw)
	}
}

// postScriptedStream posts a streamed request and returns its data events.
func postScriptedStream(t *testing.T, url string, body map[string]any) [][]byte {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode the request: %v", err)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	response, err := client.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("post the streamed request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(response.Body)
		t.Fatalf("the server answered HTTP %d: %s", response.StatusCode, raw)
	}
	events := [][]byte{}
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		line := scanner.Text()
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok || data == "[DONE]" {
			continue
		}
		events = append(events, []byte(data))
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read the stream: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("the stream carried no data events")
	}
	return events
}

// TestDeployScriptRendersTheScriptedServer pins the injection itself: the
// rendered ConfigMap's block scalar is the checkout's server file, indented for
// the block, and the model id placeholder is replaced. Two silently-empty
// results would leave a pod running no server at all.
func TestDeployScriptRendersTheScriptedServer(t *testing.T) {
	run := runDeploy(t, renderOnlyKubectlStub, mergeEnv(noEndpointEnv(), map[string]string{
		"TEST_MULTICA_ENTRY_URL":          defaultEntryURL,
		"TEST_MULTICA_MODEL_LOCAL_SOURCE": "scripted",
	}))
	if run.renderScripted == "" {
		t.Fatalf("deploy.sh rendered no scripted-backend.yaml on the default round\nstderr:\n%s", run.stderr)
	}
	server, err := os.ReadFile(scriptedServer)
	if err != nil {
		t.Fatalf("read the server file: %v", err)
	}
	want := make([]string, 0)
	for _, line := range strings.Split(strings.TrimSuffix(string(server), "\n"), "\n") {
		want = append(want, "    "+line)
	}
	if !strings.Contains(run.renderScripted, strings.Join(want, "\n")) {
		t.Errorf("the rendered ConfigMap does not carry the server file verbatim:\n%s", run.renderScripted)
	}
	if strings.Contains(run.renderScripted, "__SCRIPTED_SERVER_PY__") {
		t.Errorf("the rendered ConfigMap still carries the server placeholder")
	}
	if strings.Contains(run.renderScripted, "__SCRIPTED_MODEL_ID__") {
		t.Errorf("the rendered ConfigMap still carries the model id placeholder")
	}
	if strings.Contains(run.renderScripted, "__SCRIPTED_SLEEP__") {
		t.Errorf("the rendered ConfigMap still carries the window's sleep placeholder")
	}
	if strings.Contains(run.renderScripted, "__SCRIPTED_HOLD_TURNS__") {
		t.Errorf("the rendered ConfigMap still carries the window's hold-turns placeholder")
	}
	if strings.Contains(run.renderScripted, "__SCRIPTED_WARMUP__") {
		t.Errorf("the rendered ConfigMap still carries the warmup placeholder")
	}
	if !strings.Contains(run.renderScripted, scriptedModelID) {
		t.Errorf("the rendered Deployment does not carry the fixture model id %q", scriptedModelID)
	}
}
