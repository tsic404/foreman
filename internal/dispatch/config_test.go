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
}

func TestLoadConfigOverride(t *testing.T) {
	env := map[string]string{
		dispatch.EnvBootTimeout:      "45s",
		dispatch.EnvHeartbeatTimeout: "2m",
	}
	cfg, err := dispatch.LoadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.BootTimeout != 45*time.Second || cfg.HeartbeatTimeout != 2*time.Minute {
		t.Fatalf("got boot=%s heartbeat=%s", cfg.BootTimeout, cfg.HeartbeatTimeout)
	}
}

// A bound silently falling back to "disabled" is worse than refusing to
// start, so a malformed or negative value must be an error.
func TestLoadConfigRejectsBadValues(t *testing.T) {
	for _, raw := range []string{"60", "abc", "-5s"} {
		env := map[string]string{dispatch.EnvBootTimeout: raw}
		if _, err := dispatch.LoadConfig(func(k string) string { return env[k] }); err == nil {
			t.Fatalf("%q should be rejected", raw)
		}
	}
}
