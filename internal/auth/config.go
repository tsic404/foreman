package auth

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Environment variable names from the contract config table.
const (
	EnvJobTokenKey     = "FOREMAN_JOB_TOKEN_KEY"
	EnvJobTokenTTL     = "FOREMAN_JOB_TOKEN_TTL"
	EnvTaskMaxDuration = "FOREMAN_TASK_MAX_DURATION"
	EnvServerToken     = "MULTICA_TOKEN"
)

const (
	// DefaultJobTokenTTL matches the contract default (24h).
	DefaultJobTokenTTL = 24 * time.Hour
	// DefaultTaskMaxDuration matches the contract default (86400s).
	DefaultTaskMaxDuration = 86400 * time.Second
)

// Config is the auth module's runtime configuration.
type Config struct {
	Key             []byte        // decoded FOREMAN_JOB_TOKEN_KEY
	TokenTTL        time.Duration // FOREMAN_JOB_TOKEN_TTL
	TaskMaxDuration time.Duration // FOREMAN_TASK_MAX_DURATION; 0 = disabled
}

// LoadConfig reads the auth configuration from env (os.Getenv when getenv is
// nil) and runs the startup self-check.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{TokenTTL: DefaultJobTokenTTL, TaskMaxDuration: DefaultTaskMaxDuration}

	keyHex := strings.TrimSpace(getenv(EnvJobTokenKey))
	if keyHex == "" {
		return Config{}, fmt.Errorf("%s is required", EnvJobTokenKey)
	}
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		return Config{}, fmt.Errorf("%s must be hex-encoded: %v", EnvJobTokenKey, err)
	}
	if len(key) < MinKeyBytes {
		return Config{}, fmt.Errorf("%w: %s decodes to %d bytes, need >= %d", ErrKeyTooShort, EnvJobTokenKey, len(key), MinKeyBytes)
	}
	cfg.Key = key

	if raw := strings.TrimSpace(getenv(EnvJobTokenTTL)); raw != "" {
		ttl, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvJobTokenTTL, err)
		}
		cfg.TokenTTL = ttl
	}

	// FOREMAN_TASK_MAX_DURATION is activeDeadlineSeconds, i.e. bare seconds.
	if raw := strings.TrimSpace(getenv(EnvTaskMaxDuration)); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be an integer number of seconds: %v", EnvTaskMaxDuration, err)
		}
		if secs < 0 {
			return Config{}, fmt.Errorf("%s must be >= 0", EnvTaskMaxDuration)
		}
		cfg.TaskMaxDuration = time.Duration(secs) * time.Second
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate runs the startup self-check: the token TTL must be positive and
// must cover the task max duration, so a running task never outlives its
// token; a zero max duration disables the task limit but never legitimizes
// a non-positive TTL. Failing validation must refuse startup.
func (c Config) Validate() error {
	if c.TokenTTL <= 0 {
		return fmt.Errorf("%s must be positive, got %s", EnvJobTokenTTL, c.TokenTTL)
	}
	if c.TaskMaxDuration > 0 && c.TokenTTL < c.TaskMaxDuration {
		return fmt.Errorf("%s (%s) must be >= %s (%s)", EnvJobTokenTTL, c.TokenTTL, EnvTaskMaxDuration, c.TaskMaxDuration)
	}
	return nil
}
