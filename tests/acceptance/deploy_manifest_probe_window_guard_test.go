// Repo-side guard, not a design case: it pins the shipped
// deploy/30-foreman.yaml value, not AC-36's exit face (registration arrival,
// non-zero exit, the four failure classes) — those stay with the design's
// runtime case. §10 fixes only the code-side derivation, so the deployment
// owns this window; the probe's init pays the same per-pod agent-backend
// download as a task, and a window equal to the boot bound leaves 30s over
// the measured 270s cold path. Registered in acceptanceFixtures, so the
// census does not count this file as that case's evidence.
package acceptance

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/scheduler"
)

func TestDeployManifestProbeWindowOutlastsBootBound(t *testing.T) {
	env := foremanContainerEnv(t)

	boot, isSet := manifestDuration(t, env, scheduler.EnvJobBootTimeout)
	if !isSet {
		boot = scheduler.DefaultJobBootTimeout
	}
	window, isSet := manifestDuration(t, env, scheduler.EnvStartupProbeTimeout)
	if !isSet {
		// LoadConfig derives an unset window from the boot bound, so reading
		// the manifest means applying the same fallback: an omitted key
		// ships the bare derivation and must not pass as a compliant value.
		window = boot
	}
	if boot <= 0 {
		t.Fatalf("%s = %v: the startup probe window has no boot bound to cover", scheduler.EnvJobBootTimeout, boot)
	}
	if window < 2*boot {
		t.Fatalf("%s = %v, want at least twice %s (%v): the probe downloads the agent backend itself, so a window that only covers one boot makes a half-rate link CrashLoop the whole deployment", scheduler.EnvStartupProbeTimeout, window, scheduler.EnvJobBootTimeout, boot)
	}
}

// foremanContainerEnv collects the foreman container's env from the shipped
// Deployment. A document the manifest cannot yield is a failure, never a
// skip: an empty env would make the assertion pass vacuously.
func foremanContainerEnv(t *testing.T) map[string]corev1.EnvVar {
	t.Helper()
	deployment := deployedForemanDeployment(t)
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name != "foreman" {
			continue
		}
		env := make(map[string]corev1.EnvVar, len(container.Env))
		for _, entry := range container.Env {
			env[entry.Name] = entry
		}
		return env
	}
	t.Fatalf("the Deployment declares no foreman container: the pin reads its env")
	return nil
}

// manifestDuration reads one env value as a duration. isSet is false for an
// absent key, which the manifest may leave to the code default; a value the
// pin cannot read — a valueFrom reference or a non-duration string — fails
// here, because calling it absent would hide whatever the deployment ships.
func manifestDuration(t *testing.T, env map[string]corev1.EnvVar, key string) (time.Duration, bool) {
	t.Helper()
	entry, ok := env[key]
	if !ok {
		return 0, false
	}
	if entry.ValueFrom != nil {
		t.Fatalf("%s is not a literal value: the pin holds the shipped window, and an indirect source has no value to hold", key)
	}
	d, err := time.ParseDuration(entry.Value)
	if err != nil {
		t.Fatalf("%s = %q is not a duration: %v", key, entry.Value, err)
	}
	return d, true
}
