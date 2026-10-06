package recovery

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Environment variable names from the contract config table (§5.1).
const (
	EnvStateRoot         = "FOREMAN_STATE_ROOT"
	EnvReconcileInterval = "FOREMAN_RECONCILE_INTERVAL"
)

// Contract defaults (§5.1).
const (
	DefaultStateRoot = "/var/lib/foreman"

	// DefaultReconcileInterval is how often the convergence loop runs.
	DefaultReconcileInterval = 30 * time.Second
)

// PendingReportsDir resolves the durable queue directory:
// FOREMAN_STATE_ROOT/pending-reports (contract §4).
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
}

// LoadConfig reads the recovery configuration from env (os.Getenv when
// getenv is nil) and applies the contract defaults.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{ReconcileInterval: DefaultReconcileInterval}
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
	return cfg, nil
}
