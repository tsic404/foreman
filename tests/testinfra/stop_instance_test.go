// Package testinfra pins the behaviour of the test cluster entries' shell
// helpers (testinfra/test-multica/lib.sh). They run against a live cluster, so
// the only observable surface is what they print and which kubectl calls they
// make — this suite drives them with a stubbed kubectl and no cluster. The
// design repo leaves the test infrastructure's implementation out of scope
// (01-scope, 明确不做：测试基础设施的实现体); this is that implementation's own
// regression pin.
package testinfra

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const libDir = "../../testinfra/test-multica"

// kubectlStub answers the reads stop_instance makes from the fixture-derived
// REPLICAS (empty when the manifest leaves spec.replicas out, exactly what
// `jsonpath='{.spec.replicas}'` prints then) and records every scale call.
const kubectlStub = `#!/bin/sh
printf '%s\n' "$*" >>"$KUBECTL_LOG"
case " $* " in
*" scale "*) printf '%s\n' "$*" >>"$SCALE_LOG"; exit 0 ;;
*"jsonpath={.spec.replicas}"*) printf '%s' "$REPLICAS"; exit 0 ;;
*"jsonpath={.spec.selector.matchLabels}"*) printf '%s' '{"app.kubernetes.io/name":"foreman"}'; exit 0 ;;
*" get deploy "*" -o name "*) printf 'deployment.apps/foreman\n'; exit 0 ;;
*" get pod "*" -o name "*) exit 0 ;;
esac
printf 'stub kubectl: unexpected call: %s\n' "$*" >&2
exit 1
`

// TestStopInstanceScalesAnInstanceWithoutReplicas covers the field's absent
// case: Kubernetes defaults spec.replicas to 1, so a Deployment that does not
// write the field is running, and stop_instance must scale it down rather than
// report it as already stopped and leave the pods in place.
func TestStopInstanceScalesAnInstanceWithoutReplicas(t *testing.T) {
	for _, tc := range []struct {
		name           string
		fixture        string
		wantScaled     bool
		wantAlreadyOff bool
	}{
		{name: "replicas omitted", fixture: "instance-without-replicas.yaml", wantScaled: true},
		{name: "replicas zero", fixture: "instance-stopped.yaml", wantAlreadyOff: true},
		{name: "replicas two", fixture: "instance-running.yaml", wantScaled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replicas := fixtureReplicas(t, filepath.Join("testdata", tc.fixture))
			run := stopInstance(t, replicas)

			if got := strings.Contains(run.stderr, "is already stopped"); got != tc.wantAlreadyOff {
				t.Errorf("stderr says already stopped = %v, want %v\n%s", got, tc.wantAlreadyOff, run.stderr)
			}
			if got := strings.Contains(run.scaleLog, "scale deploy foreman --replicas=0"); got != tc.wantScaled {
				t.Errorf("scaled = %v, want %v (scale calls:\n%s)", got, tc.wantScaled, run.scaleLog)
			}
			if tc.wantScaled && !strings.Contains(run.stderr, "stopped foreman/foreman") {
				t.Errorf("stderr does not report the stop:\n%s", run.stderr)
			}
		})
	}
}

// fixtureReplicas is what `kubectl get deploy -o jsonpath='{.spec.replicas}'`
// prints for the manifest: the field's value, or empty when the manifest
// leaves it out. Read through the real Deployment type, so the fixture's shape
// decides the answer instead of the test restating it.
func fixtureReplicas(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	var deploy appsv1.Deployment
	if err := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096).Decode(&deploy); err != nil {
		t.Fatalf("decode fixture %s: %v", path, err)
	}
	if deploy.Spec.Replicas == nil {
		return ""
	}
	return strconv.Itoa(int(*deploy.Spec.Replicas))
}

type stopRun struct {
	stderr, scaleLog string
}

// stopInstance sources the real lib.sh and runs stop_instance against the
// stub, in the same `sh -eu` condition the entries run it in.
func stopInstance(t *testing.T, replicas string) stopRun {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skipf("host has no sh: %v", err)
	}
	dir := t.TempDir()
	stubDir := filepath.Join(dir, "bin")
	if err := os.Mkdir(stubDir, 0o755); err != nil {
		t.Fatalf("create stub dir: %v", err)
	}
	writeExecutable(t, filepath.Join(stubDir, "kubectl"), kubectlStub)
	script := filepath.Join(dir, "stop.sh")
	writeExecutable(t, script, ". ./lib.sh\nstop_instance\n")

	scaleLog := filepath.Join(dir, "scale.log")
	cmd := exec.Command("sh", "-eu", script)
	cmd.Dir = libDir
	cmd.Env = append(os.Environ(),
		"PATH="+stubDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"REPLICAS="+replicas,
		"KUBECTL_LOG="+filepath.Join(dir, "kubectl.log"),
		"SCALE_LOG="+scaleLog,
		"TEST_MULTICA_NS=multica-test",
		"TEST_MULTICA_INSTANCE_NS=foreman",
		"TEST_MULTICA_INSTANCE=foreman",
		// One attempt: an unexpected call is a broken stub, not a flaky API
		// server, and the retry budget would only delay the report.
		"TEST_MULTICA_KUBECTL_RETRIES=1",
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if err != nil {
		t.Fatalf("stop_instance failed: %v\n--- stdout ---\n%s\n--- stderr ---\n%s", err, stdout.String(), stderr.String())
	}
	scaled, readErr := os.ReadFile(scaleLog)
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatalf("read scale log: %v", readErr)
	}
	return stopRun{stderr: stderr.String(), scaleLog: string(scaled)}
}

func writeExecutable(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
