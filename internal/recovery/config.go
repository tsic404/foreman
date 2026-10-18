package recovery

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Environment variable names read by the recovery module.
const (
	EnvStateRoot         = "FOREMAN_STATE_ROOT"
	EnvReconcileInterval = "FOREMAN_RECONCILE_INTERVAL"
	// EnvAgentStateTTL is shared with foreman-gc: there it is the retention of
	// the node-local agent state, here the expiry window of the session index
	// (ADR-015 — one env keeps the two windows from drifting apart).
	EnvAgentStateTTL = "FOREMAN_AGENT_STATE_TTL"
)

// Default values applied when the environment variable is unset.
const (
	DefaultStateRoot = "/var/lib/foreman"

	// DefaultReconcileInterval is how often the convergence loop runs.
	DefaultReconcileInterval = 30 * time.Second

	// DefaultAgentStateTTL is 14 days, the upstream GC default the GC module
	// uses for the same env.
	DefaultAgentStateTTL = 336 * time.Hour
)

// PendingReportsDir resolves the durable queue directory:
// FOREMAN_STATE_ROOT/pending-reports (contract §10).
func PendingReportsDir(getenv func(string) string) string {
	if getenv == nil {
		getenv = os.Getenv
	}
	root := strings.TrimSpace(getenv(EnvStateRoot))
	if root == "" {
		root = DefaultStateRoot
	}
	return filepath.Join(root, "pending-reports")
}

// Config is the recovery module's runtime configuration. The dependency
// seams (Registry, Objects, Server, Settler, ...) are set on the Reconciler
// directly, not via env.
type Config struct {
	// ReconcileInterval is how often the reconciler converges the live
	// index with the server (FOREMAN_RECONCILE_INTERVAL).
	ReconcileInterval time.Duration
	// AgentStateTTL is the expiry window of the session index (ADR-015):
	// every round drops the learned entries older than it. 0 disables
	// expiry, so a key stays usable for the process's lifetime.
	AgentStateTTL time.Duration
}

// LoadConfig reads the recovery configuration from env (os.Getenv when
// getenv is nil) and applies the contract defaults.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{ReconcileInterval: DefaultReconcileInterval, AgentStateTTL: DefaultAgentStateTTL}
	if raw := strings.TrimSpace(getenv(EnvReconcileInterval)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvReconcileInterval, err)
		}
		if d <= 0 {
			return Config{}, fmt.Errorf("%s must be positive, got %s", EnvReconcileInterval, d)
		}
		cfg.ReconcileInterval = d
	}
	if raw := strings.TrimSpace(getenv(EnvAgentStateTTL)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvAgentStateTTL, err)
		}
		if d < 0 {
			return Config{}, fmt.Errorf("%s must not be negative, got %s", EnvAgentStateTTL, d)
		}
		cfg.AgentStateTTL = d
	}
	return cfg, nil
}
