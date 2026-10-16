package jobbuilder

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Environment variable names from the contract config table (§5.1).
const (
	EnvJobNamespace        = "FOREMAN_JOB_NAMESPACE"
	EnvJobImage            = "FOREMAN_JOB_IMAGE"
	EnvJobImagePullSecrets = "FOREMAN_JOB_IMAGE_PULL_SECRETS"
	EnvJobTokenTTL         = "FOREMAN_JOB_TOKEN_TTL"
	EnvTaskMaxDuration     = "FOREMAN_TASK_MAX_DURATION"
	EnvJobTTLSeconds       = "FOREMAN_JOB_TTL_SECONDS"
	EnvStateRoot           = "FOREMAN_STATE_ROOT"
	EnvRepoCacheMode       = "FOREMAN_REPO_CACHE_MODE"
	EnvPreferNodeReuse     = "FOREMAN_PREFER_NODE_REUSE"
	EnvJobCPURequest       = "FOREMAN_JOB_CPU_REQUEST"
	EnvJobMemoryRequest    = "FOREMAN_JOB_MEMORY_REQUEST"
	EnvJobCPULimit         = "FOREMAN_JOB_CPU_LIMIT"
	EnvJobMemoryLimit      = "FOREMAN_JOB_MEMORY_LIMIT"
	EnvJobNodeSelector     = "FOREMAN_JOB_NODE_SELECTOR"
	EnvJobTolerations      = "FOREMAN_JOB_TOLERATIONS"
	EnvJobDrainTimeout     = "FOREMAN_JOB_DRAIN_TIMEOUT"
)

// Contract defaults (§5.1).
const (
	DefaultJobNamespace  = "multica-agents"
	DefaultJobImage      = "ghcr.io/tsic404/foreman-job:latest"
	DefaultJobTTLSeconds = 600
	DefaultStateRoot     = "/var/lib/foreman"

	// DefaultJobTokenTTL is the §5.1 value 24h7m = boot 300s + task 86400s +
	// drain 120s. This module signs the token, while auth runs the startup
	// budget check on the same env: the two must stay equal, and
	// TestLoadConfigDefaults pins them against auth's constant — a second
	// default that drifted lower would pass the self-check and still expire
	// inside the drain window (the 401 this change removes). The value is
	// duplicated as a literal on purpose: 04-architecture.md's module
	// dependency table lists jobbuilder → registry only, so the pin is a test
	// concern, not a new production dependency.
	DefaultJobTokenTTL     = 24*time.Hour + 7*time.Minute
	DefaultTaskMaxDuration = 86400 * time.Second

	// DefaultJobDrainTimeout is the credential-reclaim upper bound that
	// follows a task's verdict (§5.1, ADR-017). It is also the scheduler
	// module's default, read from the same env: a probe's Job Token must
	// outlive the probe's Job by this bound.
	DefaultJobDrainTimeout = 120 * time.Second

	// DefaultProbeHeartbeatInterval is the probe daemon's S5 heartbeat
	// interval (§5.1 固定值): Foreman writes the key, never the template, and
	// the startup self-check keeps FOREMAN_MODEL_CATALOG_TIMEOUT at three
	// times this value at least.
	DefaultProbeHeartbeatInterval = 15 * time.Second
)

// Cache modes for FOREMAN_REPO_CACHE_MODE (ADR-005).
const (
	CacheModeShared   = "shared"
	CacheModeIsolated = "isolated"
)

var (
	defaultCPURequest    = resource.MustParse("1")
	defaultMemoryRequest = resource.MustParse("2Gi")
	defaultCPULimit      = resource.MustParse("4")
	defaultMemoryLimit   = resource.MustParse("8Gi")

	defaultTolerations = []corev1.Toleration{
		{Key: "unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
		{Key: "maybe_unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
	}
)

// Config is the jobbuilder module's runtime configuration. Issuer and Nodes
// are dependency seams, not env values: set them after LoadConfig.
type Config struct {
	JobNamespace     string
	JobImage         string
	ImagePullSecrets []string
	JobTokenTTL      time.Duration
	TaskMaxDuration  time.Duration // 0 renders no activeDeadlineSeconds
	JobTTLSeconds    int32
	StateRoot        string
	RepoCacheMode    string
	PreferNodeReuse  bool
	CPURequest       resource.Quantity
	MemoryRequest    resource.Quantity
	CPULimit         resource.Quantity
	MemoryLimit      resource.Quantity
	NodeSelector     map[string]string
	Tolerations      []corev1.Toleration

	// JobDrainTimeout is FOREMAN_JOB_DRAIN_TIMEOUT: a task's credential-reclaim
	// bound, and the window past its budget that a probe's Job Token still has
	// to cover (ADR-017).
	JobDrainTimeout time.Duration
	// ProbeHeartbeatInterval is the value Foreman writes into the probe's
	// MULTICA_DAEMON_HEARTBEAT_INTERVAL (§5.1 固定值): it fixes how late a
	// server command can reach a probe, so it is not deployer-tunable.
	ProbeHeartbeatInterval time.Duration

	// TemplatePath is where the optional job-overlay.yaml is mounted
	// (03-contracts.md §5.4). The Deployment fixes it; §5.4 has no env switch
	// on purpose, so only tests point it elsewhere.
	TemplatePath string

	Issuer TokenIssuer
	Nodes  NodeIndex
	Logger *slog.Logger
}

// logger falls back to the process default so the module logs through the
// same handler main installed (observability §结构化日志).
func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
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

// LoadConfig reads the jobbuilder configuration from env (os.Getenv when
// getenv is nil) and applies the contract defaults.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{
		JobNamespace:     DefaultJobNamespace,
		JobImage:         DefaultJobImage,
		ImagePullSecrets: []string{"registry-tsic"},
		JobTokenTTL:      DefaultJobTokenTTL,
		TaskMaxDuration:  DefaultTaskMaxDuration,
		JobTTLSeconds:    DefaultJobTTLSeconds,
		StateRoot:        DefaultStateRoot,
		RepoCacheMode:    CacheModeShared,
		PreferNodeReuse:  true,
		CPURequest:       defaultCPURequest,
		MemoryRequest:    defaultMemoryRequest,
		CPULimit:         defaultCPULimit,
		MemoryLimit:      defaultMemoryLimit,
		NodeSelector:     map[string]string{"kubernetes.io/os": "linux"},
		Tolerations:      defaultTolerations,
		TemplatePath:     TemplatePath,

		JobDrainTimeout:        DefaultJobDrainTimeout,
		ProbeHeartbeatInterval: DefaultProbeHeartbeatInterval,
	}

	if v := strings.TrimSpace(getenv(EnvJobNamespace)); v != "" {
		cfg.JobNamespace = v
	}
	// FOREMAN_JOB_IMAGE is a full image reference, taken verbatim (ADR-012):
	// `<registry>/<repo>[:<tag>][@sha256:<digest>]` — shape is not a predicate.
	if v := strings.TrimSpace(getenv(EnvJobImage)); v != "" {
		cfg.JobImage = v
	}

	if raw := strings.TrimSpace(getenv(EnvJobImagePullSecrets)); raw != "" {
		var names []string
		if err := json.Unmarshal([]byte(raw), &names); err != nil {
			return Config{}, fmt.Errorf("%s must be a JSON array of strings: %v", EnvJobImagePullSecrets, err)
		}
		cfg.ImagePullSecrets = names
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
	// Bare seconds, same convention as the auth module.
	if raw := strings.TrimSpace(getenv(EnvTaskMaxDuration)); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be an integer number of seconds: %v", EnvTaskMaxDuration, err)
		}
		if secs < 0 {
			return Config{}, fmt.Errorf("%s must be >= 0", EnvTaskMaxDuration)
		}
		// time.Duration(secs)*time.Second overflows to negative past this bound,
		// which would silently drop activeDeadlineSeconds.
		if int64(secs) > math.MaxInt64/int64(time.Second) {
			return Config{}, fmt.Errorf("%s is too large: %d seconds overflows a time.Duration", EnvTaskMaxDuration, secs)
		}
		cfg.TaskMaxDuration = time.Duration(secs) * time.Second
	}
	if raw := strings.TrimSpace(getenv(EnvJobTTLSeconds)); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be an integer number of seconds: %v", EnvJobTTLSeconds, err)
		}
		if secs < 0 {
			return Config{}, fmt.Errorf("%s must be >= 0", EnvJobTTLSeconds)
		}
		if int64(secs) > math.MaxInt32 {
			return Config{}, fmt.Errorf("%s must fit into int32, got %d", EnvJobTTLSeconds, secs)
		}
		cfg.JobTTLSeconds = int32(secs)
	}
	if v := strings.TrimSpace(getenv(EnvStateRoot)); v != "" {
		cfg.StateRoot = path.Clean(v)
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
	if v := strings.TrimSpace(getenv(EnvRepoCacheMode)); v != "" {
		if v != CacheModeShared && v != CacheModeIsolated {
			return Config{}, fmt.Errorf("%s must be %q or %q, got %q", EnvRepoCacheMode, CacheModeShared, CacheModeIsolated, v)
		}
		cfg.RepoCacheMode = v
	}
	if raw := strings.TrimSpace(getenv(EnvPreferNodeReuse)); raw != "" {
		reuse, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a boolean: %v", EnvPreferNodeReuse, err)
		}
		cfg.PreferNodeReuse = reuse
	}

	quantities := []struct {
		env  string
		dest *resource.Quantity
	}{
		{EnvJobCPURequest, &cfg.CPURequest},
		{EnvJobMemoryRequest, &cfg.MemoryRequest},
		{EnvJobCPULimit, &cfg.CPULimit},
		{EnvJobMemoryLimit, &cfg.MemoryLimit},
	}
	for _, q := range quantities {
		raw := strings.TrimSpace(getenv(q.env))
		if raw == "" {
			continue
		}
		parsed, err := resource.ParseQuantity(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a resource quantity: %v", q.env, err)
		}
		*q.dest = parsed
	}

	if raw := strings.TrimSpace(getenv(EnvJobNodeSelector)); raw != "" {
		var selector map[string]string
		if err := json.Unmarshal([]byte(raw), &selector); err != nil {
			return Config{}, fmt.Errorf("%s must be a JSON object: %v", EnvJobNodeSelector, err)
		}
		cfg.NodeSelector = selector
	}
	if raw := strings.TrimSpace(getenv(EnvJobTolerations)); raw != "" {
		var tolerations []corev1.Toleration
		if err := json.Unmarshal([]byte(raw), &tolerations); err != nil {
			return Config{}, fmt.Errorf("%s must be a JSON array of tolerations: %v", EnvJobTolerations, err)
		}
		cfg.Tolerations = tolerations
	}
	return cfg, nil
}
