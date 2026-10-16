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
	EnvJobDrainTimeout = "FOREMAN_JOB_DRAIN_TIMEOUT"
	// EnvDeliveryConfirmTimeout names the S6 delivery confirmation window
	// (ADR-030): the bound after which a committed delivery without a start
	// fact is judged undelivered. It must stay under JobBootTimeout, so it
	// lives beside it rather than in the proxy's config.
	EnvDeliveryConfirmTimeout = "FOREMAN_DELIVERY_CONFIRM_TIMEOUT"
	EnvWorkspaceID            = "MULTICA_WORKSPACE_ID"
	// EnvStartupProbeTimeout names the startup probe window; the design fixes
	// its semantics (one Job-boot magnitude) but leaves the key to the
	// implementation (§5.1).
	EnvStartupProbeTimeout = "FOREMAN_STARTUP_PROBE_TIMEOUT"
	EnvModelCatalogEnabled = "FOREMAN_MODEL_CATALOG_ENABLED"
	EnvModelCatalogTimeout = "FOREMAN_MODEL_CATALOG_TIMEOUT"
	EnvReconcileInterval   = "FOREMAN_RECONCILE_INTERVAL"
)

// Contract defaults (§5.1).
const (
	DefaultMaxInflightJobs = 100
	DefaultMaxJobsPerNode  = 4
	DefaultClaimBatchMax   = 32
	DefaultJobBootTimeout  = 300 * time.Second
	// DefaultDeliveryConfirmTimeout is the S6 delivery confirmation window
	// (§5.1, ADR-030): comfortably above the upstream daemon's own claim
	// budget (seconds, plus its retry margin) and well under the boot bound,
	// so the lost-response form settles long before the boot window expires.
	DefaultDeliveryConfirmTimeout = 30 * time.Second
	// DefaultJobDrainTimeout is the credential-reclaim upper bound that
	// follows a task's verdict (FOREMAN_JOB_DRAIN_TIMEOUT, ADR-017). A
	// deployment that raises its template's terminationGracePeriodSeconds
	// must raise this too.
	DefaultJobDrainTimeout = 120 * time.Second
	// DefaultModelCatalogTimeout is one catalog probe's own budget (§5.1).
	DefaultModelCatalogTimeout = 45 * time.Second
	// DefaultModelCatalogEnabled keeps the catalog route on unless a
	// deployment turns it off explicitly (§5.1).
	DefaultModelCatalogEnabled = true
	// DefaultReconcileInterval is the convergence cadence (§5.1); the probe
	// sweep grace is derived from it.
	DefaultReconcileInterval = 30 * time.Second

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
	// DeliveryConfirmTimeout is the S6 delivery confirmation window
	// (FOREMAN_DELIVERY_CONFIRM_TIMEOUT, ADR-030): after a committed
	// delivery, an entry still without a start fact is judged undelivered.
	// Zero leaves the window unarmed, so that form converges through the boot
	// bound alone; a non-zero value must stay under JobBootTimeout.
	DeliveryConfirmTimeout time.Duration
	// JobDrainTimeout bounds the window between a task's verdict and the
	// credential reclaim (ADR-017). Zero means "do not wait": the reclaim
	// still lands on the next observation point.
	JobDrainTimeout time.Duration
	// WorkspaceID is the workspace the probe Job Token is bound to
	// (MULTICA_WORKSPACE_ID); the catalog probe has no task to carry it.
	WorkspaceID string
	// StartupProbeTimeout is the startup probe's wait window: the first probe
	// must register within it or Foreman exits non-zero (§5.1 启动探针窗口,
	// default = JobBootTimeout — one "schedule + pull + register" magnitude).
	StartupProbeTimeout time.Duration
	// ModelCatalogEnabled is the catalog route's path-level switch; with it
	// off the startup orchestration completes as soon as S4 registered.
	ModelCatalogEnabled bool
	// ModelCatalogTimeout is one catalog probe's own budget: the probe sweep
	// grace is derived from it (§5.1 FOREMAN_MODEL_CATALOG_TIMEOUT).
	ModelCatalogTimeout time.Duration
	// ReconcileInterval is the convergence cadence; the probe sweep grace
	// covers one round on each side (FOREMAN_RECONCILE_INTERVAL).
	ReconcileInterval time.Duration
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
		JobDrainTimeout: DefaultJobDrainTimeout,
		// Refined below, where the boot bound it must stay under is known.
		DeliveryConfirmTimeout: DefaultDeliveryConfirmTimeout,
		ModelCatalogEnabled:    DefaultModelCatalogEnabled,
		ModelCatalogTimeout:    DefaultModelCatalogTimeout,
		ReconcileInterval:      DefaultReconcileInterval,
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
	// The delivery confirmation window is the second pre-start bound, and its
	// whole point is to fire before the boot one: a window at or above
	// JobBootTimeout would leave the lost-response form settling as
	// job_boot_timeout again (ADR-030). Zero is legal by §5.1: it disarms the
	// convergence exit.
	if raw := strings.TrimSpace(getenv(EnvDeliveryConfirmTimeout)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvDeliveryConfirmTimeout, err)
		}
		if d < 0 {
			return Config{}, fmt.Errorf("%s must be >= 0, got %s", EnvDeliveryConfirmTimeout, d)
		}
		if d > 0 && d >= cfg.JobBootTimeout {
			return Config{}, fmt.Errorf("%s (%s) must be below %s (%s): the delivery confirmation window has to fire before the boot bound",
				EnvDeliveryConfirmTimeout, d, EnvJobBootTimeout, cfg.JobBootTimeout)
		}
		cfg.DeliveryConfirmTimeout = d
	}
	// Zero is a legal value here (近似旧语义: reclaim on the next observation
	// point), so only a negative one is refused.
	if raw := strings.TrimSpace(getenv(EnvJobDrainTimeout)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvJobDrainTimeout, err)
		}
		if d < 0 {
			return Config{}, fmt.Errorf("%s must be >= 0, got %s", EnvJobDrainTimeout, d)
		}
		cfg.JobDrainTimeout = d
	}
	if cfg.WorkspaceID = strings.TrimSpace(getenv(EnvWorkspaceID)); cfg.WorkspaceID == "" {
		return Config{}, fmt.Errorf("%s is required", EnvWorkspaceID)
	}
	// The startup window defaults to the task Job's boot bound: the same
	// "schedule + pull + register" magnitude, with no second magic number.
	cfg.StartupProbeTimeout = cfg.JobBootTimeout
	if raw := strings.TrimSpace(getenv(EnvStartupProbeTimeout)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvStartupProbeTimeout, err)
		}
		if d <= 0 {
			return Config{}, fmt.Errorf("%s must be positive, got %s", EnvStartupProbeTimeout, d)
		}
		cfg.StartupProbeTimeout = d
	}
	if raw := strings.TrimSpace(getenv(EnvModelCatalogEnabled)); raw != "" {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a boolean: %v", EnvModelCatalogEnabled, err)
		}
		cfg.ModelCatalogEnabled = b
	}
	if raw := strings.TrimSpace(getenv(EnvModelCatalogTimeout)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvModelCatalogTimeout, err)
		}
		if d <= 0 {
			return Config{}, fmt.Errorf("%s must be positive, got %s", EnvModelCatalogTimeout, d)
		}
		cfg.ModelCatalogTimeout = d
	}
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
