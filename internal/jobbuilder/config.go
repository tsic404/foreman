package jobbuilder

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Environment variable names from the deployment manifest that this module
// reads. The Job-shape tuning keys are gone by design: ADR-030 deleted the
// four template-only keys, and the resource / nodeSelector five followed once
// ADR-028 / ADR-035 took over those axes. Foreman reads none of them — the
// template is the only shape source, so an arbitrary value, an illegal one
// included, is ignored and never refuses startup.
const (
	EnvJobNamespace        = "FOREMAN_JOB_NAMESPACE"
	EnvJobImage            = "FOREMAN_JOB_IMAGE"
	EnvJobTokenTTL         = "FOREMAN_JOB_TOKEN_TTL"
	EnvPreferNodeReuse     = "FOREMAN_PREFER_NODE_REUSE"
	EnvJobDrainTimeout     = "FOREMAN_JOB_DRAIN_TIMEOUT"
	EnvPolicyNameResolveIn = "FOREMAN_POLICY_NAME_RESOLUTION_TIMEOUT"
	// EnvJobServerURL is the address a Job daemon dials: this Foreman's own
	// Service. The default is the reference deployment's address; renaming the
	// Service without setting this key breaks F5 silently at the Job side.
	EnvJobServerURL = "FOREMAN_JOB_SERVER_URL"
)

// Contract defaults.
const (
	DefaultJobNamespace = "multica-agents"
	DefaultJobImage     = "ghcr.io/tsic404/foreman-job:latest"
	// DefaultJobServerURL is the reference deployment's Job-side entry point
	// (§8.2 A1.1): the Service a Job daemon's MULTICA_SERVER_URL resolves to.
	DefaultJobServerURL = "http://foreman.foreman.svc.cluster.local:8080"

	// DefaultJobTokenTTL is the §6.1 value 24h7m = boot 300s + task 86400s +
	// drain 120s. This module signs the token, while auth runs the startup
	// budget check on the same env: the two must stay equal, and
	// TestLoadConfigDefaults pins them against auth's constant — a second
	// default that drifted lower would pass the self-check and still expire
	// inside the drain window (the 401 this change removes). The value is
	// duplicated as a literal on purpose: jobbuilder depends on registry only,
	// so the pin is a test concern, not a new production dependency.
	DefaultJobTokenTTL = 24*time.Hour + 7*time.Minute

	// DefaultJobDrainTimeout is the credential-reclaim upper bound that
	// follows a task's verdict (§6.4, ADR-017). It is also the scheduler
	// module's default, read from the same env: a probe's Job Token must
	// outlive the probe's Job by this bound.
	DefaultJobDrainTimeout = 120 * time.Second

	// DefaultProbeHeartbeatInterval is the probe daemon's S5 heartbeat
	// interval (§8.4 固定值): Foreman writes the key, never the template, and
	// the startup self-check keeps FOREMAN_MODEL_CATALOG_TIMEOUT at three
	// times this value at least.
	DefaultProbeHeartbeatInterval = 15 * time.Second

	// DefaultPolicyNameResolutionTimeout bounds the startup name resolution
	// of the policy table (§8.5: the window belongs to the deployment
	// interface, its key name to the implementation). Keys past the
	// window conclude unavailable, which only warns.
	DefaultPolicyNameResolutionTimeout = 5 * time.Second
)

// Config is the jobbuilder module's runtime configuration. Issuer, Nodes and
// Metrics are dependency seams, not env values: set them after LoadConfig. The
// Job's shape is not configured here — it comes from the template (ADR-030).
type Config struct {
	JobNamespace string
	JobImage     string
	JobTokenTTL  time.Duration
	// ServerURL is FOREMAN_JOB_SERVER_URL: the address written into every Job
	// daemon's MULTICA_SERVER_URL (§8.2 A1.1). Empty falls back to
	// DefaultJobServerURL.
	ServerURL string
	// PreferNodeReuse is FOREMAN_PREFER_NODE_REUSE: false deletes the
	// template's <preferred node> item instead of filling it in.
	PreferNodeReuse bool

	// JobDrainTimeout is FOREMAN_JOB_DRAIN_TIMEOUT: a task's credential-reclaim
	// bound, and the window past its budget that a probe's Job Token still has
	// to cover (ADR-017).
	JobDrainTimeout time.Duration
	// ProbeHeartbeatInterval is the value Foreman writes into the probe's
	// MULTICA_DAEMON_HEARTBEAT_INTERVAL (§8.4 固定值): it fixes how late a
	// server command can reach a probe, so it is not deployer-tunable.
	ProbeHeartbeatInterval time.Duration

	// TemplatePath is where job-overlay.yaml is mounted (02-contracts.md §8.1).
	// The Deployment fixes it; §8.1 has no env switch on purpose, so only
	// tests point it elsewhere.
	TemplatePath string

	// PolicyPath is where policy.yaml is mounted (02-contracts.md §8.5): the
	// optional ConfigMap foreman-job-policy. Like the template path it has no
	// env switch — the mount is the deployment's contract — so only tests
	// point it elsewhere.
	PolicyPath string
	// NameResolver is the platform-side name-resolution seam ADR-048 leaves to
	// the platform. Nil means the platform offers no such query: every policy
	// key concludes unavailable (warn, boot continues; the project section
	// then has no identifier index and reads as no definition).
	NameResolver PolicyNameResolver
	// PolicyNameResolutionTimeout is FOREMAN_POLICY_NAME_RESOLUTION_TIMEOUT:
	// the bounded window the startup resolution of all policy keys shares.
	PolicyNameResolutionTimeout time.Duration
	// WorkspaceIDs is the configured workspace set. A policy file's flat
	// sections are the legacy single-workspace form: with more than one
	// workspace configured they are ambiguous and refuse startup (§8.5 键空间
	// = workspace × 名字).
	WorkspaceIDs []string
	// Workspaces is the startup discovery result (§8.5 分区键的启动期解析):
	// every discovered workspace's identifier and the display name C-17
	// reported for it. The policy file's partition keys resolve against this
	// set — identifier exact match first, then display name exact match. Empty
	// falls back to WorkspaceIDs without display names, so a hand-built Config
	// keeps the identifier form working (name keys then have no match).
	Workspaces []WorkspaceRef

	Issuer TokenIssuer
	Nodes  NodeIndex
	// Metrics is the policy-face observability seam (§9.2): the per-axis hit
	// counter and the startup name-resolution conclusions. Nil degrades to
	// no-ops, exactly like the other seams.
	Metrics Metrics
	Logger  *slog.Logger
}

// WorkspaceRef is one workspace of the startup discovery result (§8.5 分区键的
// 启动期解析): the identifier the platform uses and the display name it
// reported. The policy file's partition keys resolve against this set; nothing
// else in the builder needs the name.
type WorkspaceRef struct {
	ID   string
	Name string
}

// Metrics is the policy-face observability seam (§9.2). The observability
// module provides the Prometheus implementation; nil degrades to no-ops.
type Metrics interface {
	// PolicyHit records one axis evaluation by axis (resources /
	// capabilities_add / node_selector) and source (direct / no_definition /
	// no_identity).
	PolicyHit(axis, source string)
	// PolicyNameResolution records one startup key's conclusion (resolved /
	// absent / ambiguous / unavailable, plus conflict for a partition key
	// another key already claimed).
	PolicyNameResolution(conclusion string)
}

type noopMetrics struct{}

func (noopMetrics) PolicyHit(string, string)    {}
func (noopMetrics) PolicyNameResolution(string) {}

func (c Config) metrics() Metrics {
	if c.Metrics == nil {
		return noopMetrics{}
	}
	return c.Metrics
}

// logger falls back to the process default so the module logs through the
// same handler main installed (§9.1).
func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

// JobServerURL is the injected Job-side address, with the reference
// deployment's value standing in for a Config built without LoadConfig.
func (c Config) JobServerURL() string {
	if c.ServerURL != "" {
		return c.ServerURL
	}
	return DefaultJobServerURL
}

// probeHeartbeat is the heartbeat interval the probe Job carries. A Config
// built without LoadConfig has no loaded value, and an empty interval would
// render "0s" — the daemon would then spin on its heartbeat — so the fixed
// contract value stands in.
func (c Config) probeHeartbeat() time.Duration {
	if c.ProbeHeartbeatInterval <= 0 {
		return DefaultProbeHeartbeatInterval
	}
	return c.ProbeHeartbeatInterval
}

// policyPath is where policy.yaml is read from; the mount path is the fixed
// deployment interface, and only tests point Config.PolicyPath elsewhere.
func (c Config) policyPath() string {
	if c.PolicyPath != "" {
		return c.PolicyPath
	}
	return PolicyPath
}

// workspaceRefs is the partition-key resolution set: the discovery result when
// given, else the configured ids (name keys then have no match — the
// identifier form keeps working for hand-built Configs).
func (c Config) workspaceRefs() []WorkspaceRef {
	if len(c.Workspaces) > 0 {
		return c.Workspaces
	}
	refs := make([]WorkspaceRef, 0, len(c.WorkspaceIDs))
	for _, id := range c.WorkspaceIDs {
		refs = append(refs, WorkspaceRef{ID: id})
	}
	return refs
}

// policyNameResolutionWindow is the bounded window the startup name resolution
// shares: an unset or non-positive value falls back to the contract default, so
// a hand-built Config cannot make the window unbounded.
func (c Config) policyNameResolutionWindow() time.Duration {
	if c.PolicyNameResolutionTimeout <= 0 {
		return DefaultPolicyNameResolutionTimeout
	}
	return c.PolicyNameResolutionTimeout
}

// LoadConfig reads the jobbuilder configuration from env (os.Getenv when
// getenv is nil) and applies the contract defaults.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{
		JobNamespace:    DefaultJobNamespace,
		JobImage:        DefaultJobImage,
		JobTokenTTL:     DefaultJobTokenTTL,
		ServerURL:       DefaultJobServerURL,
		PreferNodeReuse: true,
		TemplatePath:    TemplatePath,
		PolicyPath:      PolicyPath,

		JobDrainTimeout:             DefaultJobDrainTimeout,
		ProbeHeartbeatInterval:      DefaultProbeHeartbeatInterval,
		PolicyNameResolutionTimeout: DefaultPolicyNameResolutionTimeout,
	}

	if v := strings.TrimSpace(getenv(EnvJobNamespace)); v != "" {
		cfg.JobNamespace = v
	}
	if v := strings.TrimSpace(getenv(EnvJobServerURL)); v != "" {
		cfg.ServerURL = v
	}
	// FOREMAN_JOB_IMAGE is a full image reference, taken verbatim (ADR-012):
	// `<registry>/<repo>[:<tag>][@sha256:<digest>]` — shape is not a predicate.
	if v := strings.TrimSpace(getenv(EnvJobImage)); v != "" {
		cfg.JobImage = v
	}
	if raw := strings.TrimSpace(getenv(EnvJobTokenTTL)); raw != "" {
		ttl, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvJobTokenTTL, err)
		}
		if ttl <= 0 {
			return Config{}, fmt.Errorf("%s must be positive, got %s", EnvJobTokenTTL, ttl)
		}
		cfg.JobTokenTTL = ttl
	}
	// Zero is legal (the reclaim lands on the next observation point), so only
	// a negative bound is refused — the same rule the scheduler module applies
	// to this env.
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
	if raw := strings.TrimSpace(getenv(EnvPreferNodeReuse)); raw != "" {
		reuse, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a boolean: %v", EnvPreferNodeReuse, err)
		}
		cfg.PreferNodeReuse = reuse
	}
	// The resolution window must stay bounded: a zero or negative value would
	// make every key conclude unavailable instantly, which is a configuration
	// mistake, not a mode.
	if raw := strings.TrimSpace(getenv(EnvPolicyNameResolveIn)); raw != "" {
		window, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvPolicyNameResolveIn, err)
		}
		if window <= 0 {
			return Config{}, fmt.Errorf("%s must be positive, got %s", EnvPolicyNameResolveIn, window)
		}
		cfg.PolicyNameResolutionTimeout = window
	}
	return cfg, nil
}
