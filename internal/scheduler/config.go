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
	EnvMaxJobsPerNode  = "FOREMAN_MAX_JOBS_PER_NODE"
	EnvClaimBatchMax   = "FOREMAN_CLAIM_BATCH_MAX"
	EnvJobBootTimeout  = "FOREMAN_JOB_BOOT_TIMEOUT"
)

// Contract defaults (§5.1).
const (
	DefaultMaxInflightJobs = 100
	DefaultMaxJobsPerNode  = 4
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
	JobNamespace string
	// MaxJobsPerNode is the per-node soft cap of concurrent Jobs
	// (FOREMAN_MAX_JOBS_PER_NODE, ADR-006 §决策结果 3): a node at or over it
	// loses its reuse affinity for the next claim. It is never admission
	// control — no Job is queued, refused, or deleted on account of it.
	MaxJobsPerNode  int
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
		MaxJobsPerNode:  DefaultMaxJobsPerNode,
		MaxInflightJobs: DefaultMaxInflightJobs,
		ClaimBatchMax:   DefaultClaimBatchMax,
		JobBootTimeout:  DefaultJobBootTimeout,
	}

	if v := strings.TrimSpace(getenv(jobbuilder.EnvJobNamespace)); v != "" {
		cfg.JobNamespace = v
	}
	if raw := strings.TrimSpace(getenv(EnvMaxJobsPerNode)); raw != "" {
		// A cap below 1 would either disable node placement entirely or mean
		// "unlimited"; the contract gives neither, so refuse the value.
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("%s must be a positive integer, got %q", EnvMaxJobsPerNode, raw)
		}
		cfg.MaxJobsPerNode = n
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
