// Package observability provides Foreman's structured logging (log/slog
// JSON), the Prometheus metric set and its /metrics handler, and the
// cluster-internal ops endpoints (/foreman/*). Every other module depends
// on this one for instrumentation; it depends only on registry's read view.
// Design: 02-contracts.md §9, S-25 (观测面与运维端点).
package observability

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Environment variable names of the observability module.
const (
	EnvLogLevel = "FOREMAN_LOG_LEVEL"
)

// Config is the observability module's runtime configuration.
type Config struct {
	LogLevel slog.Level
}

// LoadConfig reads the observability configuration from env (os.Getenv when
// getenv is nil) and applies the contract defaults.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{LogLevel: slog.LevelInfo}

	if raw := strings.TrimSpace(getenv(EnvLogLevel)); raw != "" {
		level, err := parseLevel(raw)
		if err != nil {
			return Config{}, err
		}
		cfg.LogLevel = level
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
