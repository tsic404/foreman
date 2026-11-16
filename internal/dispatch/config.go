package dispatch

import (
	"fmt"
	"time"
)

// Environment keys of the dispatcher's timing bounds.
const (
	// EnvBootTimeout bounds claim → daemon start. Zero (or unset) disables the
	// bound; a malformed value refuses startup rather than silently disabling
	// a protection the operator asked for. The bound is evaluated once per
	// reconcile round (30s), so a value below that cadence can only fire when
	// the round happens to land in the window — set it comfortably above 30s
	// to actually protect anything.
	EnvBootTimeout = "FOREMAN_JOB_BOOT_TIMEOUT"
	// EnvHeartbeatTimeout bounds how long a daemon may stay silent before the
	// sweep marks it offline. Zero disables the sweep.
	EnvHeartbeatTimeout = "FOREMAN_HEARTBEAT_TIMEOUT"
	// EnvReconcileInterval is the convergence round's cadence.
	EnvReconcileInterval = "FOREMAN_RECONCILE_INTERVAL"
	// EnvTaskMaxDuration bounds one task once it started. Zero (the default)
	// leaves it unbounded. Reaching the bound does not fail the task: the
	// round reads the server's own status (C-13) and aligns to it, because the
	// server owns the verdict for a run that is still going.
	EnvTaskMaxDuration = "FOREMAN_TASK_MAX_DURATION"
)

// defaultReconcileInterval is the documented convergence cadence.
const defaultReconcileInterval = 30 * time.Second

// LoadConfig reads the dispatcher's timing bounds from env, applying the
// documented defaults. An unparseable or negative value is an error: a bound
// silently falling back to "disabled" is worse than refusing to start.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		return Config{}, fmt.Errorf("dispatch: getenv is nil")
	}
	cfg := Config{
		BootTimeout:       time.Minute, // documented default: claim → start bound
		HeartbeatTimeout:  time.Minute, // four upstream heartbeat intervals
		ReconcileInterval: defaultReconcileInterval,
	}
	var err error
	if cfg.BootTimeout, err = durationEnv(getenv, EnvBootTimeout, cfg.BootTimeout); err != nil {
		return Config{}, err
	}
	if cfg.HeartbeatTimeout, err = durationEnv(getenv, EnvHeartbeatTimeout, cfg.HeartbeatTimeout); err != nil {
		return Config{}, err
	}
	if cfg.TaskMaxDuration, err = durationEnv(getenv, EnvTaskMaxDuration, cfg.TaskMaxDuration); err != nil {
		return Config{}, err
	}
	if cfg.ReconcileInterval, err = durationEnv(getenv, EnvReconcileInterval, cfg.ReconcileInterval); err != nil {
		return Config{}, err
	}
	// A zero cadence would mean "never reconcile", which silently disables the
	// boot bound and the offline sweep — refuse it rather than idle.
	if cfg.ReconcileInterval <= 0 {
		return Config{}, fmt.Errorf("%s: must be positive", EnvReconcileInterval)
	}
	return cfg, nil
}

// durationEnv parses one duration variable, keeping def when it is unset.
func durationEnv(getenv func(string) string, key string, def time.Duration) (time.Duration, error) {
	raw := getenv(key)
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("%s: must not be negative (%s)", key, raw)
	}
	return d, nil
}
