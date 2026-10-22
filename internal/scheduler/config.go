package scheduler

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// Environment variable names read by LoadConfig.
const (
	EnvMaxInflightJobs = "FOREMAN_MAX_INFLIGHT_JOBS"
	EnvMaxJobsPerNode  = "FOREMAN_MAX_JOBS_PER_NODE"
	EnvClaimBatchMax   = "FOREMAN_CLAIM_BATCH_MAX"
	EnvJobBootTimeout  = "FOREMAN_JOB_BOOT_TIMEOUT"
	EnvJobDrainTimeout = "FOREMAN_JOB_DRAIN_TIMEOUT"
	// EnvDeliveryConfirmTimeout names the S6 delivery confirmation window
	// (ADR-032): the span a committed delivery is withheld for, after which
	// the entry without a start fact releases it for one more attempt. Where
	// a boot bound is set it must stay under JobBootTimeout (0 = no bound),
	// so it lives beside it rather than in the proxy's config.
	EnvDeliveryConfirmTimeout = "FOREMAN_DELIVERY_CONFIRM_TIMEOUT"
	EnvWorkspaceID            = "MULTICA_WORKSPACE_ID"
	// EnvStartupProbeTimeout names the startup probe window; the design fixes
	// its semantics (one Job-boot magnitude, §10) but leaves the key to the
	// implementation.
	EnvStartupProbeTimeout     = "FOREMAN_STARTUP_PROBE_TIMEOUT"
	EnvMaxInflightPerWorkspace = "FOREMAN_MAX_INFLIGHT_JOBS_PER_WORKSPACE"
	EnvModelCatalogEnabled     = "FOREMAN_MODEL_CATALOG_ENABLED"
	EnvModelCatalogTimeout     = "FOREMAN_MODEL_CATALOG_TIMEOUT"
	EnvReconcileInterval       = "FOREMAN_RECONCILE_INTERVAL"
	// EnvProjectLookupTimeout bounds one read-only project reverse lookup by
	// issue id (ADR-048 decision 4: the timeout value belongs to the
	// implementation, and the claim → Job 5s P95 must hold).
	EnvProjectLookupTimeout = "FOREMAN_PROJECT_LOOKUP_TIMEOUT"
)

// Defaults applied when the environment leaves a scheduler key unset.
const (
	DefaultMaxInflightJobs = 100
	// DefaultMaxInflightPerWorkspace is the per-workspace cap of the two-level
	// in-flight budget (§10). Zero means "no separate per-workspace bound":
	// the global cap alone applies; a positive value must not exceed the
	// global cap.
	DefaultMaxInflightPerWorkspace = 0
	DefaultMaxJobsPerNode          = 4
	DefaultClaimBatchMax           = 32
	DefaultJobBootTimeout          = 300 * time.Second
	// DefaultDeliveryConfirmTimeout is the S6 delivery confirmation window
	// (§5, ADR-032): comfortably above the upstream daemon's own claim
	// budget (seconds, plus its retry margin), so a released delivery can
	// never race a response still in flight, and well under the boot bound,
	// which stays the entry's terminal bound.
	DefaultDeliveryConfirmTimeout = 30 * time.Second
	// DefaultJobDrainTimeout is the credential-reclaim upper bound that
	// follows a task's verdict (FOREMAN_JOB_DRAIN_TIMEOUT, ADR-017). A
	// deployment that raises its template's terminationGracePeriodSeconds
	// must raise this too.
	DefaultJobDrainTimeout = 120 * time.Second
	// DefaultModelCatalogTimeout is one catalog probe's own budget (§1.3).
	DefaultModelCatalogTimeout = 45 * time.Second
	// DefaultModelCatalogEnabled keeps the catalog route on unless a
	// deployment turns it off explicitly (§1.3).
	DefaultModelCatalogEnabled = true
	// DefaultReconcileInterval is the convergence cadence; the probe
	// sweep grace is derived from it.
	DefaultReconcileInterval = 30 * time.Second

	// DefaultProjectLookupTimeout keeps one project reverse lookup well inside
	// the claim → Job 5s P95 (ADR-048 decision 4): a lookup that outlives it
	// converges on project.lookup_failed, never on a slow Job create.
	DefaultProjectLookupTimeout = 2 * time.Second

	// serverClaimBatchCap is the server-side claim batch ceiling
	// (C-3: max_tasks > 32 is truncated to 32).
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
	// MaxInflightJobsPerWorkspace is the per-workspace in-flight cap
	// (FOREMAN_MAX_INFLIGHT_JOBS_PER_WORKSPACE, §10 两级预算); 0 disables the
	// workspace term and leaves the global cap alone.
	MaxInflightJobsPerWorkspace int
	ClaimBatchMax               int
	JobBootTimeout              time.Duration
	// DeliveryConfirmTimeout is the S6 delivery confirmation window
	// (FOREMAN_DELIVERY_CONFIRM_TIMEOUT, ADR-032): a committed delivery still
	// without a start fact once it elapses releases the payload for one more
	// attempt on the daemon's next claim. Zero leaves the window unarmed —
	// the delivery is committed once and the boot bound owns the entry; a
	// non-zero value must stay under JobBootTimeout when that bound is set
	// (0 = no bound).
	DeliveryConfirmTimeout time.Duration
	// JobDrainTimeout bounds the window between a task's verdict and the
	// credential reclaim (ADR-017). Zero means "do not wait": the reclaim
	// still lands on the next observation point.
	JobDrainTimeout time.Duration
	// WorkspaceID is the legacy single-workspace identity (MULTICA_WORKSPACE_ID);
	// multi-workspace deployments carry the set in Workspaces and every probe
	// names its own workspace explicitly.
	WorkspaceID string
	// Workspaces is the configured workspace set (configuration order). The
	// scheduler reads it for the probe orchestration's registration scope and
	// for the credential-reclaim/sweep partition.
	Workspaces []string
	// StartupProbeTimeout is the startup probe's wait window: the first probe
	// must register within it or Foreman exits non-zero (§10 启动探针窗口,
	// default = JobBootTimeout — one "schedule + pull + register" magnitude).
	StartupProbeTimeout time.Duration
	// ModelCatalogEnabled is the catalog route's path-level switch; with it
	// off the startup orchestration completes as soon as S4 registered.
	ModelCatalogEnabled bool
	// ModelCatalogTimeout is one catalog probe's own budget: the probe sweep
	// grace is derived from it (§1.3).
	ModelCatalogTimeout time.Duration
	// ReconcileInterval is the convergence cadence; the probe sweep grace
	// covers one round on each side (FOREMAN_RECONCILE_INTERVAL).
	ReconcileInterval time.Duration
	// Project is the platform-side project-identity seam (ADR-048 decision 1):
	// the payload's top-level project identifier and the read-only lookup by
	// issue id. Nil means the platform offers neither, so every task reads
	// project_source=none and the project tiers stay no_identity.
	Project ProjectIdentity
	// ProjectLookupTimeout bounds one reverse lookup so the claim → Job budget
	// holds (FOREMAN_PROJECT_LOOKUP_TIMEOUT). Zero falls back to the default.
	ProjectLookupTimeout time.Duration
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
		// Refined below, once the boot bound it must stay under (if any) is known.
		DeliveryConfirmTimeout: DefaultDeliveryConfirmTimeout,
		ModelCatalogEnabled:    DefaultModelCatalogEnabled,
		ModelCatalogTimeout:    DefaultModelCatalogTimeout,
		ReconcileInterval:      DefaultReconcileInterval,

		ProjectLookupTimeout: DefaultProjectLookupTimeout,
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
	if raw := strings.TrimSpace(getenv(EnvMaxInflightPerWorkspace)); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("%s must be a positive integer, got %q", EnvMaxInflightPerWorkspace, raw)
		}
		cfg.MaxInflightJobsPerWorkspace = n
	}
	if cfg.MaxInflightJobsPerWorkspace > cfg.MaxInflightJobs {
		return Config{}, fmt.Errorf("%s (%d) must not exceed %s (%d): the per-workspace cap is one level of the global budget",
			EnvMaxInflightPerWorkspace, cfg.MaxInflightJobsPerWorkspace, EnvMaxInflightJobs, cfg.MaxInflightJobs)
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
		// 0 = no upper bound (排障用): the claim writes no boot_deadline
		// and the boot judgement never holds (§4.2, ADR-026). Only a
		// negative bound is meaningless.
		if d < 0 {
			return Config{}, fmt.Errorf("%s must be >= 0, got %s", EnvJobBootTimeout, d)
		}
		cfg.JobBootTimeout = d
	}
	// The delivery confirmation window is the second pre-start bound, and its
	// whole point is to fire before the boot one: a window at or above
	// JobBootTimeout would leave the lost-response form settling as
	// job_boot_timeout again (ADR-032). Zero is legal here: it disarms the
	// convergence exit. A boot bound switched off (0 = no upper bound, ADR-026)
	// leaves the ordering premise without a target and constrains nothing.
	if raw := strings.TrimSpace(getenv(EnvDeliveryConfirmTimeout)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvDeliveryConfirmTimeout, err)
		}
		if d < 0 {
			return Config{}, fmt.Errorf("%s must be >= 0, got %s", EnvDeliveryConfirmTimeout, d)
		}
		if d > 0 && cfg.JobBootTimeout > 0 && d >= cfg.JobBootTimeout {
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
	// Optional: the user-scoped credential form fills Workspaces (and this
	// single-workspace fallback) from the startup discovery (ADR-049). The
	// deployment shape is validated by workspace.LoadDeployment.
	cfg.WorkspaceID = strings.TrimSpace(getenv(EnvWorkspaceID))
	// The startup window defaults to the task Job's boot bound: the same
	// "schedule + pull + register" magnitude, with no second magic number
	// (§10 启动探针窗口). A boot bound switched off (0 = no upper bound)
	// leaves nothing to derive from, and a zero window would fail the probe
	// before it could register — the default magnitude stands in.
	cfg.StartupProbeTimeout = cfg.JobBootTimeout
	if cfg.StartupProbeTimeout == 0 {
		cfg.StartupProbeTimeout = DefaultJobBootTimeout
	}
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
	if raw := strings.TrimSpace(getenv(EnvProjectLookupTimeout)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvProjectLookupTimeout, err)
		}
		if d <= 0 {
			return Config{}, fmt.Errorf("%s must be positive, got %s", EnvProjectLookupTimeout, d)
		}
		cfg.ProjectLookupTimeout = d
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
