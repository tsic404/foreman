package dispatch_test

import (
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/dispatch"
)

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := dispatch.LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.BootTimeout != time.Minute {
		t.Fatalf("boot default = %s, want 1m", cfg.BootTimeout)
	}
	if cfg.HeartbeatTimeout != time.Minute {
		t.Fatalf("heartbeat default = %s, want 1m", cfg.HeartbeatTimeout)
	}
	if cfg.ReconcileInterval != 30*time.Second {
		t.Fatalf("reconcile default = %s, want 30s", cfg.ReconcileInterval)
	}
}

func TestLoadConfigOverride(t *testing.T) {
	env := map[string]string{
		dispatch.EnvBootTimeout:       "45s",
		dispatch.EnvHeartbeatTimeout:  "2m",
		dispatch.EnvReconcileInterval: "10s",
		dispatch.EnvTaskMaxDuration:   "3h",
	}
	cfg, err := dispatch.LoadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.BootTimeout != 45*time.Second || cfg.HeartbeatTimeout != 2*time.Minute ||
		cfg.ReconcileInterval != 10*time.Second || cfg.TaskMaxDuration != 3*time.Hour {
		t.Fatalf("got boot=%s heartbeat=%s reconcile=%s max=%s",
			cfg.BootTimeout, cfg.HeartbeatTimeout, cfg.ReconcileInterval, cfg.TaskMaxDuration)
	}
}

// TestTaskMaxDurationIsReachableFromEnv is the regression guard for a config
// field that was read by Reconcile but never populated: every bound the
// dispatcher honours must be settable from the environment it documents.
func TestTaskMaxDurationIsReachableFromEnv(t *testing.T) {
	cfg, err := dispatch.LoadConfig(func(k string) string {
		if k == dispatch.EnvTaskMaxDuration {
			return "90m"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.TaskMaxDuration != 90*time.Minute {
		t.Fatalf("TaskMaxDuration = %s, want 90m (the env key must reach it)",
			cfg.TaskMaxDuration)
	}
	// Unset keeps the documented default: no bound.
	cfg, err = dispatch.LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("load default: %v", err)
	}
	if cfg.TaskMaxDuration != 0 {
		t.Fatalf("default TaskMaxDuration = %s, want 0 (unbounded)", cfg.TaskMaxDuration)
	}
}

// A bound silently falling back to "disabled" is worse than refusing to
// start, so a malformed or negative value must be an error.
func TestLoadConfigRejectsBadValues(t *testing.T) {
	for _, raw := range []string{"60", "abc", "-5s"} {
		env := map[string]string{dispatch.EnvBootTimeout: raw}
		if _, err := dispatch.LoadConfig(func(k string) string { return env[k] }); err == nil {
			t.Fatalf("boot %q should be rejected", raw)
		}
	}
	// A zero cadence would never reconcile, silently disabling the boot bound
	// and the offline sweep.
	for _, raw := range []string{"0s", "0"} {
		env := map[string]string{dispatch.EnvReconcileInterval: raw}
		if _, err := dispatch.LoadConfig(func(k string) string { return env[k] }); err == nil {
			t.Fatalf("reconcile %q should be rejected", raw)
		}
	}
}
