// The shared-state Redis is the deployment's single persistence gate, and
// redis-cli exits 0 even on NOAUTH / WRONGPASS, so the probe command text is
// the judgment point: it must gate on the reply, authenticate through the
// container env, and keep the credential off the process command line. The
// manifest is the only place that shape lives — §7 fixes the store's
// connection face and credential, not the probe's form — so this case reads
// deploy/35-shared-state.yaml and holds its probe fields to that shape.
package acceptance

import (
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// sharedStateProbeCommand is the reply-text gate both probes must carry. The
// client's exit code says nothing about authentication, so the probe succeeds
// only when the server answers exactly PONG.
const sharedStateProbeCommand = "redis-cli ping | grep -qx PONG"

func TestSharedStateProbesKeepCredentialOffTheCommandLine(t *testing.T) {
	container := sharedStateRedisContainer(t)

	t.Run("both probes gate on the reply text", func(t *testing.T) {
		want := []string{"sh", "-c", sharedStateProbeCommand}
		liveness := execProbeCommand(t, container.LivenessProbe, "livenessProbe")
		readiness := execProbeCommand(t, container.ReadinessProbe, "readinessProbe")
		if !slices.Equal(liveness, want) {
			t.Errorf("livenessProbe command = %q, want %q", liveness, want)
		}
		if !slices.Equal(readiness, want) {
			t.Errorf("readinessProbe command = %q, want %q", readiness, want)
		}
		if !slices.Equal(liveness, readiness) {
			t.Errorf("liveness and readiness probes disagree: %q vs %q", liveness, readiness)
		}
	})

	t.Run("the password reaches redis-cli through the environment", func(t *testing.T) {
		const envName = "REDISCLI_AUTH"
		var value string
		found := false
		for _, env := range container.Env {
			if env.Name != envName {
				continue
			}
			found = true
			value = env.Value
			if env.ValueFrom != nil {
				t.Errorf("%s reads from %+v, want the literal $(FOREMAN_STATE_PASSWORD) reference", envName, env.ValueFrom)
			}
		}
		if !found {
			t.Fatalf("container %q has no %s env: redis-cli would probe unauthenticated, and the reply gate would then fail every probe", container.Name, envName)
		}
		if want := "$(FOREMAN_STATE_PASSWORD)"; value != want {
			t.Errorf("%s = %q, want %q", envName, value, want)
		}
	})

	t.Run("the command line carries no credential", func(t *testing.T) {
		for _, probe := range []struct {
			name    string
			command []string
		}{
			{"livenessProbe", execProbeCommand(t, container.LivenessProbe, "livenessProbe")},
			{"readinessProbe", execProbeCommand(t, container.ReadinessProbe, "readinessProbe")},
		} {
			for _, argument := range probe.command {
				if strings.Contains(argument, "FOREMAN_STATE_PASSWORD") {
					t.Errorf("%s argument %q names the password variable: the credential belongs in the env only", probe.name, argument)
				}
				for _, field := range strings.Fields(argument) {
					if field == "-a" {
						t.Errorf("%s argument %q passes -a: redis-cli would put the password on the process command line", probe.name, argument)
					}
				}
			}
		}
	})
}

// execProbeCommand returns a probe's exec command, insisting the probe exists
// at all: a probe with no exec handler cannot gate on the Redis reply.
func execProbeCommand(t *testing.T, probe *corev1.Probe, name string) []string {
	t.Helper()
	if probe == nil || probe.Exec == nil {
		t.Fatalf("%s has no exec probe: the container would report healthy without Redis answering", name)
	}
	return probe.Exec.Command
}

// sharedStateRedisContainer decodes the first container out of the StatefulSet
// in deploy/35-shared-state.yaml.
func sharedStateRedisContainer(t *testing.T) corev1.Container {
	t.Helper()
	for _, doc := range yamlDocuments(t, filepath.Join("..", "..", "deploy", "35-shared-state.yaml")) {
		jsonBytes, err := utilyaml.ToJSON(doc)
		if err != nil {
			t.Fatalf("deploy/35-shared-state.yaml document is not valid YAML: %v", err)
		}
		var typeMeta metav1.TypeMeta
		if err := json.Unmarshal(jsonBytes, &typeMeta); err != nil {
			t.Fatalf("decode document type: %v", err)
		}
		if typeMeta.Kind != "StatefulSet" {
			continue
		}
		var statefulSet appsv1.StatefulSet
		if err := json.Unmarshal(jsonBytes, &statefulSet); err != nil {
			t.Fatalf("decode the StatefulSet: %v", err)
		}
		containers := statefulSet.Spec.Template.Spec.Containers
		if len(containers) == 0 {
			t.Fatal("deploy/35-shared-state.yaml StatefulSet carries no container")
		}
		return containers[0]
	}
	t.Fatal("deploy/35-shared-state.yaml carries no StatefulSet")
	return corev1.Container{}
}
