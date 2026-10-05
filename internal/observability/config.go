// Package observability provides Foreman's structured logging (log/slog
// JSON), the Prometheus metric set and its /metrics handler, and the
// cluster-internal ops endpoints (/foreman/*). Every other module depends
// on this one for instrumentation; it depends only on registry's read view.
// Design: docs/05-modules/observability.md, contracts §1.2 S25/§2/§5.1.
package observability

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// Environment variable names from the contract config table (§5.1).
const (
	EnvLogLevel       = "FOREMAN_LOG_LEVEL"
	EnvMetricsEnabled = "FOREMAN_METRICS_ENABLED"
)

// Config is the observability module's runtime configuration.
type Config struct {
	LogLevel       slog.Level
	MetricsEnabled bool
}

// LoadConfig reads the observability configuration from env (os.Getenv when
// getenv is nil) and applies the contract defaults.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{LogLevel: slog.LevelInfo, MetricsEnabled: true}

	if raw := strings.TrimSpace(getenv(EnvLogLevel)); raw != "" {
		level, err := parseLevel(raw)
		if err != nil {
			return Config{}, err
		}
		cfg.LogLevel = level
	}
	if raw := strings.TrimSpace(getenv(EnvMetricsEnabled)); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a bool, got %q", EnvMetricsEnabled, raw)
		}
		cfg.MetricsEnabled = enabled
	}
	return cfg, nil
}

// parseLevel maps the contract's FOREMAN_LOG_LEVEL vocabulary
// (debug|info|warn|error) onto slog levels.
func parseLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(raw) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("%s must be debug|info|warn|error, got %q", EnvLogLevel, raw)
	}
}
