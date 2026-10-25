// The test cluster's model endpoint is a deployment parameter: its triple is
// read from the environment (or a mounted credential file), registered as one
// provider in the node-local home the Job pods mount, and pointed at by the
// test agent's model. The entries run against a live cluster, so this suite
// drives the real shell helpers with a stubbed kubectl and no cluster: the
// carrier manifests and the document they carry are the observable surface.
package testinfra

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// modelsDocument is the part of models.yml this registration owns: one
// provider with one model. The credential is compared verbatim.
type modelsDocument struct {
	Providers map[string]struct {
		BaseURL string `json:"baseUrl"`
		APIKey  string `json:"apiKey"`
		API     string `json:"api"`
		Models  []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"models"`
	} `json:"providers"`
}

func decodeModelsDocument(t *testing.T, raw []byte) modelsDocument {
	t.Helper()
	var doc modelsDocument
	if err := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096).Decode(&doc); err != nil {
		t.Fatalf("decode models.yml: %v\n%s", err, raw)
	}
	return doc
}

// TestModelProviderDocumentIsOneProviderAndOneModel pins the document's shape:
// exactly one provider, its triple verbatim (including characters YAML would
// read as syntax), and one model whose id is the endpoint's id.
func TestModelProviderDocumentIsOneProviderAndOneModel(t *testing.T) {
	url := "http://gateway.test:8000/v1?a=1&b=2"
	key := `sk-test:"quoted" #not-a-comment`
	modelID := "model/slash-in-id"
	out := runLib(t, "model_provider_document\n",
		"TEST_MULTICA_MODEL_BASE_URL="+url,
		"TEST_MULTICA_MODEL_ID="+modelID,
		"TEST_MULTICA_MODEL_API_KEY="+key,
	)

	doc := decodeModelsDocument(t, []byte(out))
	if len(doc.Providers) != 1 {
		t.Fatalf("providers = %d, want exactly 1:\n%s", len(doc.Providers), out)
	}
	provider, ok := doc.Providers["foreman-test"]
	if !ok {
		t.Fatalf("provider `foreman-test` is absent:\n%s", out)
	}
	if provider.BaseURL != url {
		t.Errorf("baseUrl = %q, want %q", provider.BaseURL, url)
	}
	if provider.APIKey != key {
		t.Errorf("apiKey = %q, want %q", provider.APIKey, key)
	}
	if provider.API != "openai-completions" {
		t.Errorf("api = %q, want openai-completions", provider.API)
	}
	if len(provider.Models) != 1 || provider.Models[0].ID != modelID {
		t.Errorf("models = %+v, want one %q", provider.Models, modelID)
	}
	if got := provider.Models[0].Name; got != "foreman-test/"+modelID {
		t.Errorf("model name = %q, want the selector", got)
	}
}

// TestModelEndpointGating pins the three states of the deployment parameter:
// nothing configured is the baseline (no calls, no invented endpoint), a
// partial triple is an operator error, and a mounted credential file is the
// admitted alternative carrier.
func TestModelEndpointGating(t *testing.T) {
	t.Run("nothing configured is the baseline", func(t *testing.T) {
		out := runLib(t, "if model_endpoint_configured; then echo configured; else echo baseline; fi\n")
		if got := strings.TrimSpace(out); got != "baseline" {
			t.Errorf("gate = %q, want baseline", got)
		}
	})

	t.Run("partial triple fails closed", func(t *testing.T) {
		out, err := tryLib(t, "set -eu\nmodel_endpoint_configured\necho configured\n",
			"TEST_MULTICA_MODEL_BASE_URL=http://gateway.test/v1",
		)
		if err == nil {
			t.Fatalf("a partial triple passed the gate; output:\n%s", out)
		}
		if want := "TEST_MULTICA_MODEL_ID is unset"; !strings.Contains(out, want) {
			t.Errorf("stderr does not name the missing part %q:\n%s", want, out)
		}
	})

	t.Run("mounted credential file", func(t *testing.T) {
		dir := t.TempDir()
		keyFile := filepath.Join(dir, "model-api-key")
		if err := os.WriteFile(keyFile, []byte("file-secret\n"), 0o600); err != nil {
			t.Fatalf("write key file: %v", err)
		}
		out := runLib(t, "set -eu\nmodel_endpoint_configured\nmodel_api_key\n",
			"TEST_MULTICA_MODEL_BASE_URL=http://gateway.test/v1",
			"TEST_MULTICA_MODEL_ID=model-a",
			"TEST_MULTICA_MODEL_API_KEY_FILE="+keyFile,
		)
		if got := strings.TrimSpace(out); got != "file-secret" {
			t.Errorf("model_api_key = %q, want the file's value without its newline", got)
		}
	})

	t.Run("stray credential alone is partial", func(t *testing.T) {
		out, err := tryLib(t, "set -eu\nmodel_endpoint_configured\necho configured\n",
			"TEST_MULTICA_MODEL_API_KEY=sk-only",
		)
		if err == nil {
			t.Fatalf("a stray credential passed the gate; output:\n%s", out)
		}
		if want := "TEST_MULTICA_MODEL_BASE_URL is unset"; !strings.Contains(out, want) {
			t.Errorf("stderr does not name the missing part %q:\n%s", want, out)
		}
	})
}

// TestInstallModelProviderWritesOneRegistrationPerNode drives the real
// installer twice. The second run must reproduce the first byte for byte (one
// Secret applied in place, one file per schedulable node), the credential must
// live only in the Secret the carrier Pods mount, and unschedulable or unready
// nodes must be skipped.
func TestInstallModelProviderWritesOneRegistrationPerNode(t *testing.T) {
	const (
		workspace = "11111111-2222-3333-4444-555555555555"
		key       = "sk-carrier-secret"
	)
	first := installModelProvider(t, workspace)
	second := installModelProvider(t, workspace)

	firstByKind := byKind(t, first)
	secondByKind := byKind(t, second)
	for _, kind := range []string{"Secret", "Pod"} {
		if len(firstByKind[kind]) != len(secondByKind[kind]) {
			t.Fatalf("%s count changed between resets: %d then %d", kind, len(firstByKind[kind]), len(secondByKind[kind]))
		}
		for i := range firstByKind[kind] {
			if firstByKind[kind][i] != secondByKind[kind][i] {
				t.Errorf("second reset changed the %s manifest:\n--- first ---\n%s\n--- second ---\n%s",
					kind, firstByKind[kind][i], secondByKind[kind][i])
			}
		}
	}

	if got := len(firstByKind["Secret"]); got != 1 {
		t.Fatalf("Secret manifests = %d, want one provider registration", got)
	}
	var secret corev1.Secret
	decodeManifest(t, firstByKind["Secret"][0], &secret)
	raw := secret.Data["models.yml"]
	doc := decodeModelsDocument(t, raw)
	if len(doc.Providers) != 1 {
		t.Fatalf("the registered document holds %d providers, want one:\n%s", len(doc.Providers), raw)
	}
	provider := doc.Providers["foreman-test"]
	if provider.BaseURL != "http://gateway.test:8000/v1" || provider.APIKey != key || provider.Models[0].ID != "model-a" {
		t.Errorf("registered provider = %+v, want the supplied triple", provider)
	}

	pods := firstByKind["Pod"]
	if len(pods) != 2 {
		t.Fatalf("carrier Pods = %d, want one per ready schedulable node (2):\n%s", len(pods), strings.Join(pods, "\n"))
	}
	nodes := map[string]bool{}
	for _, raw := range pods {
		var pod corev1.Pod
		decodeManifest(t, raw, &pod)
		nodes[pod.Spec.NodeName] = true
		if strings.Contains(raw, key) {
			t.Errorf("the credential reached a carrier Pod's manifest:\n%s", raw)
		}
		var home, providerVolume bool
		for _, volume := range pod.Spec.Volumes {
			if volume.HostPath != nil && volume.HostPath.Path == "/var/lib/foreman/home/"+workspace {
				home = true
			}
			if volume.Secret != nil && volume.Secret.SecretName == "test-multica-model-provider" {
				providerVolume = true
			}
		}
		if !home {
			t.Errorf("Pod %s does not mount the workspace home:\n%s", pod.Name, raw)
		}
		if !providerVolume {
			t.Errorf("Pod %s does not mount the registration Secret:\n%s", pod.Name, raw)
		}
		script := strings.Join(pod.Spec.Containers[0].Args, " ")
		assertWriteCarrierScript(t, script)
		// `chown` hands the inode to uid 1000 before the `chmod`, so the latter
		// needs FOWNER (DAC_OVERRIDE covers read/write/execute, never chmod).
		// Without it the first reset fails with EPERM, and a later reset that
		// finds an already-1000-owned tree fails the same way.
		assertCarrierCapabilities(t, pod, raw, []corev1.Capability{"CHOWN", "DAC_OVERRIDE", "FOWNER"})
	}
	if !nodes["node-a"] || !nodes["node-b"] || nodes["node-unready"] || nodes["node-cordoned"] {
		t.Errorf("carrier nodes = %v, want both ready schedulable nodes only", nodes)
	}
}

// assertWriteCarrierScript pins the write carrier's body: the three inodes the
// Job's agent must own, the mode each one ends up with, and chown before
// chmod. This suite never executes the script (kubectl is a stub), so the
// rendered args are the only place a permission regression shows up — turning
// a `chmod 0700` into `0755` changes nothing else here.
func assertWriteCarrierScript(t *testing.T, script string) {
	t.Helper()
	if !strings.Contains(script, "dir=/node-home/.omp/agent") {
		t.Errorf("carrier script does not target the registration directory:\n%s", script)
	}
	chownAt := -1
	chmods := map[string]string{}
	chmodLines := 0
	for i, line := range strings.Split(script, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "chown":
			if chownAt >= 0 {
				t.Errorf("carrier script chowns more than once:\n%s", script)
			}
			chownAt = i
			want := []string{"1000:1000", "/node-home/.omp", `"$dir"`, `"$dir/models.yml"`}
			if !slices.Equal(fields[1:], want) {
				t.Errorf("chown = %q, want %q:\n%s", fields[1:], want, script)
			}
		case "chmod":
			chmodLines++
			if len(fields) != 3 {
				t.Errorf("chmod line = %q, want `chmod <mode> <target>`:\n%s", line, script)
				continue
			}
			chmods[fields[2]] = fields[1]
		}
	}
	if chownAt < 0 {
		t.Fatalf("carrier script never chowns the registration:\n%s", script)
	}
	wantModes := map[string]string{
		"/node-home/.omp":   "0700",
		`"$dir"`:            "0700",
		`"$dir/models.yml"`: "0600",
	}
	if !maps.Equal(chmods, wantModes) {
		t.Errorf("chmod modes = %v, want %v:\n%s", chmods, wantModes, script)
	}
	if chmodLines != len(wantModes) {
		t.Errorf("carrier script runs %d chmods, want one per inode (%d):\n%s", chmodLines, len(wantModes), script)
	}
	for i, line := range strings.Split(script, "\n") {
		if fields := strings.Fields(line); len(fields) > 0 && fields[0] == "chmod" {
			if i < chownAt {
				t.Errorf("chmod runs before the chown that hands the inode to 1000 (a later chmod then needs FOWNER):\n%s", script)
			}
			break
		}
	}
}

// assertCarrierCapabilities pins one carrier Pod's capability set: ALL dropped
// first, then exactly the ones its own script needs — no shared maximum.
func assertCarrierCapabilities(t *testing.T, pod corev1.Pod, raw string, want []corev1.Capability) {
	t.Helper()
	security := pod.Spec.Containers[0].SecurityContext
	if security == nil || security.Capabilities == nil {
		t.Fatalf("carrier Pod %s has no capability set:\n%s", pod.Name, raw)
	}
	if got := security.Capabilities.Add; !slices.Equal(got, want) {
		t.Errorf("carrier Pod %s adds %v, want exactly %v:\n%s", pod.Name, got, want, raw)
	}
	if len(security.Capabilities.Drop) != 1 || security.Capabilities.Drop[0] != "ALL" {
		t.Errorf("carrier Pod %s does not drop ALL first: %v", pod.Name, security.Capabilities.Drop)
	}
}

// installModelProvider runs the real installer once and returns the manifests
// it applied, as the stub kubectl captured them.
func installModelProvider(t *testing.T, workspace string) []string {
	t.Helper()
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	capture := filepath.Join(dir, "capture")
	home := filepath.Join(dir, "home")
	profileDir := filepath.Join(home, ".multica", "profiles", "test-multica")
	for _, d := range []string{binDir, capture, profileDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("create %s: %v", d, err)
		}
	}
	if err := os.WriteFile(filepath.Join(profileDir, "config.json"),
		[]byte(`{"workspace_id":"`+workspace+`"}`), 0o600); err != nil {
		t.Fatalf("write profile: %v", err)
	}
	writeExecutable(t, filepath.Join(binDir, "kubectl"), modelEndpointKubectlStub)

	out := runLib(t, "set -eu\ninstall_model_provider\n",
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"HOME="+home,
		"TEST_MULTICA_KUBECTL_RETRIES=1",
		"TEST_MULTICA_NS=multica-test",
		"TEST_MULTICA_INSTANCE_NS=foreman",
		"TEST_MULTICA_INSTANCE=foreman",
		"TEST_MULTICA_MODEL_BASE_URL=http://gateway.test:8000/v1",
		"TEST_MULTICA_MODEL_ID=model-a",
		"TEST_MULTICA_MODEL_API_KEY=sk-carrier-secret",
		"APPLY_DIR="+capture,
		"KUBECTL_LOG="+filepath.Join(dir, "kubectl.log"),
	)
	if strings.Contains(out, "unexpected call") {
		t.Fatalf("the installer made a call the stub does not model:\n%s", out)
	}
	entries, err := os.ReadDir(capture)
	if err != nil {
		t.Fatalf("read captured manifests: %v", err)
	}
	var applied []string
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join(capture, entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		applied = append(applied, string(raw))
	}
	return applied
}

// byKind groups captured manifests by their kind, in application order.
func byKind(t *testing.T, applied []string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, raw := range applied {
		var meta struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal([]byte(raw), &meta); err != nil {
			t.Fatalf("captured manifest is not JSON: %v\n%s", err, raw)
		}
		out[meta.Kind] = append(out[meta.Kind], raw)
	}
	return out
}

func decodeManifest(t *testing.T, raw string, into any) {
	t.Helper()
	if err := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(raw), 4096).Decode(into); err != nil {
		t.Fatalf("decode manifest: %v\n%s", err, raw)
	}
}

// runLib runs a shell body in the same `sh -eu` condition the entries use from
// lib.sh's directory, failing the test when the body exits non-zero.
func runLib(t *testing.T, body string, env ...string) string {
	t.Helper()
	out, err := tryLib(t, body, env...)
	if err != nil {
		t.Fatalf("shell helper failed: %v\n--- output ---\n%s", err, out)
	}
	return out
}

// tryLib runs the body and returns stdout and stderr together, so a failure
// names what the helper printed rather than only its exit status.
func tryLib(t *testing.T, body string, env ...string) (string, error) {
	t.Helper()
	shPath, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("host has no sh: %v", err)
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "entry.sh")
	if err := os.WriteFile(script, []byte(". ./lib.sh\n"+body), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}
	// A closed environment: the sandbox supplies every variable the helper may
	// read, so an ambient TEST_MULTICA_* from the host cannot decide a case.
	values := map[string]string{
		"PATH":   os.Getenv("PATH"),
		"HOME":   t.TempDir(),
		"TMPDIR": os.TempDir(),
	}
	for _, kv := range env {
		key, value, _ := strings.Cut(kv, "=")
		values[key] = value
	}
	cmdEnv := make([]string, 0, len(values))
	for key, value := range values {
		cmdEnv = append(cmdEnv, key+"="+value)
	}
	sort.Strings(cmdEnv)

	cmd := exec.Command(shPath, "-eu", script)
	cmd.Dir = libDir
	cmd.Env = cmdEnv
	var combined bytes.Buffer
	cmd.Stdout, cmd.Stderr = &combined, &combined
	runErr := cmd.Run()
	var exitErr *exec.ExitError
	if runErr != nil && !errors.As(runErr, &exitErr) {
		return combined.String(), runErr
	}
	return combined.String(), runErr
}

// modelEndpointKubectlStub answers the reads the installer makes and captures
// every applied manifest. The Secret's `data` is rendered from the file the
// installer hands `--from-file`, so the credential can be checked where it is
// supposed to live.
const modelEndpointKubectlStub = `#!/bin/sh
printf '%s\n' "$*" >>"$KUBECTL_LOG"
case " $* " in
*" get nodes "*" -o json "*)
	cat <<'JSON'
{"items":[
 {"metadata":{"name":"node-a"},"spec":{"unschedulable":false},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
 {"metadata":{"name":"node-b"},"spec":{},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
 {"metadata":{"name":"node-cordoned"},"spec":{"unschedulable":true},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
 {"metadata":{"name":"node-unready"},"spec":{},"status":{"conditions":[{"type":"Ready","status":"False"}]}}
]}
JSON
	exit 0
	;;
*" get deploy foreman -o name "*)
	printf 'deployment.apps/foreman\n'
	exit 0
	;;
*" get deploy foreman -o jsonpath="*)
	printf '/var/lib/foreman'
	exit 0
	;;
*" create secret "*)
	name= ns= from=
	prev=
	for arg in "$@"; do
		case $prev in
		secret) name=$arg ;;
		-n) ns=$arg ;;
		esac
		case $arg in
		--from-file=models.yml=*) from=${arg#--from-file=models.yml=} ;;
		esac
		prev=$arg
	done
	data=$(base64 <"$from" | tr -d '\n')
	printf '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"%s","namespace":"%s"},"data":{"models.yml":"%s"}}\n' "$name" "$ns" "$data"
	exit 0
	;;
*" apply -f "*)
	prev=
	for arg in "$@"; do
		if [ "$prev" = "-f" ]; then
			n=$(cat "$APPLY_DIR/../count" 2>/dev/null || echo 0)
			n=$((n + 1))
			printf '%s' "$n" >"$APPLY_DIR/../count"
			cp "$arg" "$APPLY_DIR/$(printf '%03d' "$n").json"
			exit 0
		fi
		prev=$arg
	done
	printf 'stub kubectl: apply without -f: %s\n' "$*" >&2
	exit 1
	;;
*" get pod "*" -o jsonpath="*)
	printf 'Succeeded'
	exit 0
	;;
*" delete pod "* | *" delete secret "* | *" describe pod "*)
	exit 0
	;;
esac
printf 'stub kubectl: unexpected call: %s\n' "$*" >&2
exit 1
`

// registerRun is what one endpoint-less `register_model_endpoint` produced.
type registerRun struct {
	output  string
	wsapi   string
	kubectl string
	applied []string
}

// TestRegisterModelEndpointWithoutEndpointClearsTheBaseline pins the
// missing-endpoint branch end to end: the statement is printed, no
// registration is written, the agent model is cleared and the rollback carrier
// runs on every schedulable node. The rollback itself creates one one-shot
// removal Pod per node — that is the accepted cost of not guessing from the
// agent row; "nothing applied" here means the registration plane (no Secret,
// no write carrier).
func TestRegisterModelEndpointWithoutEndpointClearsTheBaseline(t *testing.T) {
	run := runRegisterModelEndpoint(t)

	if !strings.Contains(run.output, "未配置模型端点") {
		t.Errorf("the endpoint-less reset does not state the missing input:\n%s", run.output)
	}
	for _, raw := range run.applied {
		var meta struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal([]byte(raw), &meta); err != nil {
			t.Fatalf("captured manifest is not JSON: %v\n%s", err, raw)
		}
		if meta.Kind == "Secret" {
			t.Errorf("an endpoint-less reset applied a provider Secret:\n%s", raw)
		}
	}
	for _, raw := range byKind(t, run.applied)["Pod"] {
		var pod corev1.Pod
		decodeManifest(t, raw, &pod)
		for _, volume := range pod.Spec.Volumes {
			if volume.Secret != nil && volume.Secret.SecretName == "test-multica-model-provider" {
				t.Errorf("an endpoint-less reset applied a write carrier:\n%s", raw)
			}
		}
	}
	assertRemovalCarriers(t, run.applied)
	if !strings.Contains(run.wsapi, `PUT /api/agents/agent-1 {"model":""}`) {
		t.Errorf("the agent model was not cleared; wsapi calls:\n%s", run.wsapi)
	}
	if !strings.Contains(run.kubectl, "delete secret test-multica-model-provider") {
		t.Errorf("the Secret was not deleted (with --ignore-not-found); kubectl calls:\n%s", run.kubectl)
	}
}

// TestRegisterModelEndpointClearsCarriersWithAnEmptyModel is the regression
// pin for the cross-round path: the agent row is recreated with an empty model
// (the namespace, and with it the database, is gone) while the node-local file
// survives. The rollback must not consult the agent row — it runs anyway, and
// it must not even read it.
func TestRegisterModelEndpointClearsCarriersWithAnEmptyModel(t *testing.T) {
	run := runRegisterModelEndpoint(t)

	if strings.Contains(run.wsapi, "GET ") {
		t.Errorf("the rollback consulted the agent row instead of the carriers:\n%s", run.wsapi)
	}
	assertRemovalCarriers(t, run.applied)
	if !strings.Contains(run.kubectl, "delete secret test-multica-model-provider") {
		t.Errorf("the stale Secret was not deleted; kubectl calls:\n%s", run.kubectl)
	}
	if !strings.Contains(run.wsapi, `PUT /api/agents/agent-1 {"model":""}`) {
		t.Errorf("the agent model was not cleared; wsapi calls:\n%s", run.wsapi)
	}
}

// assertRemovalCarriers requires one removal carrier per schedulable node,
// running exactly the file-removal script with DAC_OVERRIDE alone: unlinking
// inside the 0700 directory the write carrier handed to 1000 needs it (without
// it `rm -f` fails with EACCES), and the delete script never chowns or chmods,
// so no other capability belongs on it.
func assertRemovalCarriers(t *testing.T, applied []string) {
	t.Helper()
	pods := byKind(t, applied)["Pod"]
	if len(pods) != 2 {
		t.Fatalf("removal carriers = %d, want one per ready schedulable node (2):\n%s",
			len(pods), strings.Join(applied, "\n"))
	}
	for _, raw := range pods {
		var pod corev1.Pod
		decodeManifest(t, raw, &pod)
		script := strings.Join(pod.Spec.Containers[0].Args, " ")
		if want := "set -eu\nrm -f /node-home/.omp/agent/models.yml"; script != want {
			t.Errorf("carrier %s script = %q, want %q", pod.Name, script, want)
		}
		assertCarrierCapabilities(t, pod, raw, []corev1.Capability{"DAC_OVERRIDE"})
	}
}

// runRegisterModelEndpoint drives the real entry helper with a stubbed platform
// API (`wsapi`) and a stubbed kubectl, without supplying the endpoint triple.
// The agent id is the one the entry derived from `ensure_agent`; the recreated
// row's model is empty, which is exactly why the rollback cannot be gated on it.
func runRegisterModelEndpoint(t *testing.T) registerRun {
	t.Helper()
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	capture := filepath.Join(dir, "capture")
	home := filepath.Join(dir, "home")
	profileDir := filepath.Join(home, ".multica", "profiles", "test-multica")
	for _, d := range []string{binDir, capture, profileDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("create %s: %v", d, err)
		}
	}
	if err := os.WriteFile(filepath.Join(profileDir, "config.json"),
		[]byte(`{"workspace_id":"11111111-2222-3333-4444-555555555555"}`), 0o600); err != nil {
		t.Fatalf("write profile: %v", err)
	}
	writeExecutable(t, filepath.Join(binDir, "kubectl"), modelEndpointKubectlStub)

	wsapiLog := filepath.Join(dir, "wsapi.log")
	kubectlLog := filepath.Join(dir, "kubectl.log")
	body := `wsapi() {
	printf '%s %s %s\n' "$1" "$2" "${3:-}" >>"$WSAPI_LOG"
	if [ "$1" = GET ]; then printf '{"model":""}\n'; fi
	return 0
}
agent_id=agent-1
register_model_endpoint
`
	out := runLib(t, body,
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"HOME="+home,
		"TEST_MULTICA_KUBECTL_RETRIES=1",
		"TEST_MULTICA_NS=multica-test",
		"TEST_MULTICA_INSTANCE_NS=foreman",
		"TEST_MULTICA_INSTANCE=foreman",
		"APPLY_DIR="+capture,
		"KUBECTL_LOG="+kubectlLog,
		"WSAPI_LOG="+wsapiLog,
	)

	entries, err := os.ReadDir(capture)
	if err != nil {
		t.Fatalf("read applied manifests: %v", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	var applied []string
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join(capture, entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		applied = append(applied, string(raw))
	}
	readLog := func(path string) string {
		raw, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("read %s: %v", path, err)
		}
		return string(raw)
	}
	return registerRun{
		output:  out,
		wsapi:   readLog(wsapiLog),
		kubectl: readLog(kubectlLog),
		applied: applied,
	}
}
