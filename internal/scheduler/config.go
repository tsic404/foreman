package scheduler

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
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
	// SourceWorkspaces maps a stable Multica server id to its configured
	// workspace ids. A source-scoped probe takes its admission workspace from
	// here; an empty map keeps the legacy single-source answer.
	SourceWorkspaces map[string][]string
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
	// DispatchGate decides whether the exact C3 request that returned a task
	// carried its real runtime. It answers a fact about that request's frozen
	// declaration, never about the agent's current binding or the live
	// registration set: an edit or refresh that lands while the claim is in
	// flight must not revoke a confirmation (02-contracts V2-C02 已确认归原
	// runtime).
	//
	// known=false means no evidence about this runtime (the registered-source
	// legacy shape or an undeclared runtime set) and the claim proceeds.
	// known=true, owned=false refuses the claim with an error — the task is
	// settled as a visible refusing claim, not silently dropped, because the
	// upstream did confirm it to someone (04-architecture.md §3.1).
	DispatchGate func(ctx context.Context, workspaceID, agentID, realRuntimeID string, claimedRuntimeIDs []string) (owned, known bool)
	// DeliveryGate decides whether the S-6 payload may be handed to the
	// daemon yet. A fixed attempt carries a shared reservation, and the
	// supervisor commits "started" only after it verified the registration
	// and won the execution claim; until then the payload stays with the
	// server and the daemon receives an empty batch. A legacy entry without a
	// frozen binding keeps the old behaviour.
	//
	// false is a plain "not yet", not an error: the daemon keeps polling.
	// An unreadable gate fails the claim closed with ErrStoreUnavailable, so
	// a payload is never handed over against an unknown ledger.
	DeliveryGate func(ctx context.Context, entry registry.TaskEntry) (bool, error)
	// ClaimFacts freezes the release and configuration facts one attempt
	// resolves at claim time. The impact preview reads these facts instead of
	// inferring a version from StartedAt (02-contracts V2-C05). A nil seam
	// leaves every fact unknown.
	ClaimFacts ClaimFactsFunc
	// EnvironmentBinder resolves the published environment rules for one
	// claim and reserves the selected environment's slot. Preview and real
	// dispatch share the resolver behind it; only the reservation differs
	// (02-contracts V2-C01). A nil seam keeps the pre-environment path.
	EnvironmentBinder EnvironmentBinder
	// JobImage is the configured rsbx carrier reference. The fixed attempt
	// freezes its digest; a reference without one refuses the claim (V2-C08).
	JobImage string
	// JobServerURL is the Foreman proxy origin frozen into the attempt's CLI
	// download addresses.
	JobServerURL string
	// CLIBudgetBytes is the per-attempt CLI download budget.
	CLIBudgetBytes int64
}

// ClaimFacts is one claim's release snapshot. The *Known flags separate a
// genuine empty fact from a fact the deployment could not resolve.
type ClaimFacts struct {
	MulticaVersion string
	MulticaDigests map[string]string
	AgentVersion   string
	AgentDigests   map[string]string
	ConfigRevision uint64
	CLIKnown       bool
	AgentKnown     bool
	ConfigKnown    bool
}

// ClaimFactsFunc resolves one claim's release facts. attemptID is the stable
// per-attempt configuration identity (registry.AttemptID).
type ClaimFactsFunc func(ctx context.Context, workspaceID, agentID, runtimeIdentity, attemptID string) ClaimFacts

func cloneDigests(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
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
