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
	// DefaultJobTokenTTL matches the contract default (24h7m = 300s boot +
	// 86400s task + 120s drain, §5.1): the token covers claim → Job boot →
	// task → the drain window that follows the task's verdict (ADR-017).
	DefaultJobTokenTTL = 24*time.Hour + 7*time.Minute
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

// Validate runs the intrinsic self-check: the token TTL must be positive and
// must at least cover the task itself. The full credential budget — boot and
// drain included — is checked by ValidateTokenBudget once the scheduler's
// timeouts are known; both are fail-fast configuration errors.
func (c Config) Validate() error {
	if c.TokenTTL <= 0 {
		return fmt.Errorf("%s must be positive, got %s", EnvJobTokenTTL, c.TokenTTL)
	}
	if c.TaskMaxDuration > 0 && c.TokenTTL < c.TaskMaxDuration {
		return fmt.Errorf("%s (%s) must be >= %s (%s)", EnvJobTokenTTL, c.TokenTTL, EnvTaskMaxDuration, c.TaskMaxDuration)
	}
	return nil
}

// ValidateTokenBudget runs the startup self-check of the credential budget
// (contract §5.1): the Job Token must outlive the whole Job, i.e.
//
//	FOREMAN_JOB_TOKEN_TTL >= boot + task + drain
//
// so it is still valid through claim → Job boot → task execution → the drain
// window that follows the task's verdict (ADR-017). A shorter TTL would
// expire credentials inside a live window, so it refuses startup instead of
// degrading silently; a zero task duration (no activeDeadlineSeconds) leaves
// the inequality as boot + drain.
func (c Config) ValidateTokenBudget(bootTimeout, drainTimeout time.Duration) error {
	budget := bootTimeout + c.TaskMaxDuration + drainTimeout
	if c.TokenTTL < budget {
		return fmt.Errorf("%s (%s) must cover FOREMAN_JOB_BOOT_TIMEOUT (%s) + %s (%s) + FOREMAN_JOB_DRAIN_TIMEOUT (%s) = %s",
			EnvJobTokenTTL, c.TokenTTL,
			bootTimeout,
			EnvTaskMaxDuration, c.TaskMaxDuration,
			drainTimeout,
			budget)
	}
	return nil
}
