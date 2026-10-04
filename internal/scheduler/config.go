package scheduler

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// Environment variable names from the contract config table (§5.1).
const (
	EnvMaxInflightJobs = "FOREMAN_MAX_INFLIGHT_JOBS"
	EnvClaimBatchMax   = "FOREMAN_CLAIM_BATCH_MAX"
	EnvJobBootTimeout  = "FOREMAN_JOB_BOOT_TIMEOUT"
)

// Contract defaults (§5.1).
const (
	DefaultMaxInflightJobs = 100
	DefaultClaimBatchMax   = 32
	DefaultJobBootTimeout  = 300 * time.Second

	// serverClaimBatchCap is the server-side claim batch ceiling
	// (contract §1.1 C3: max_tasks > 32 is truncated to 32).
	serverClaimBatchCap = 32
)

// Config is the scheduler module's runtime configuration. The dependency
// seams (Registry, Jobs, Builder, Server, Metrics, Reconciler) are set on
// Scheduler directly, not via env.
type Config struct {
	JobNamespace    string
	MaxInflightJobs int
	ClaimBatchMax   int
	JobBootTimeout  time.Duration
}

// LoadConfig reads the scheduler configuration from env (os.Getenv when
// getenv is nil) and applies the contract defaults.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{
		JobNamespace:    jobbuilder.DefaultJobNamespace,
		MaxInflightJobs: DefaultMaxInflightJobs,
		ClaimBatchMax:   DefaultClaimBatchMax,
		JobBootTimeout:  DefaultJobBootTimeout,
	}

	if v := strings.TrimSpace(getenv(jobbuilder.EnvJobNamespace)); v != "" {
		cfg.JobNamespace = v
	}
	if raw := strings.TrimSpace(getenv(EnvMaxInflightJobs)); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("%s must be a positive integer, got %q", EnvMaxInflightJobs, raw)
		}
		cfg.MaxInflightJobs = n
	}
	if raw := strings.TrimSpace(getenv(EnvClaimBatchMax)); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("%s must be a positive integer, got %q", EnvClaimBatchMax, raw)
		}
		// Claiming above the server ceiling is silently truncated there;
		// clamp here so the budget math and the wire agree.
		if n > serverClaimBatchCap {
			n = serverClaimBatchCap
		}
		cfg.ClaimBatchMax = n
	}
	if raw := strings.TrimSpace(getenv(EnvJobBootTimeout)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvJobBootTimeout, err)
		}
		if d <= 0 {
			return Config{}, fmt.Errorf("%s must be positive, got %s", EnvJobBootTimeout, d)
		}
		cfg.JobBootTimeout = d
	}
	return cfg, nil
}
