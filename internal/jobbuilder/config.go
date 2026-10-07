package jobbuilder

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path"
	"regexp"
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
	EnvJobImageDigest      = "FOREMAN_JOB_IMAGE_DIGEST"
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
)

// Contract defaults (§5.1).
const (
	DefaultJobNamespace    = "multica-agents"
	DefaultJobImage        = "ghcr.io/tsic404/foreman-job"
	DefaultJobTokenTTL     = 24 * time.Hour
	DefaultTaskMaxDuration = 86400 * time.Second
	DefaultJobTTLSeconds   = 600
	DefaultStateRoot       = "/var/lib/foreman"
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

	// The digest pins the Job image (04-architecture §镜像权威); the test
	// suite asserts the rendered reference keeps this exact shape.
	imageDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// Config is the jobbuilder module's runtime configuration. Issuer and Nodes
// are dependency seams, not env values: set them after LoadConfig.
type Config struct {
	JobNamespace     string
	JobImage         string
	JobImageDigest   string
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

	// OverlayPath is where the optional job-overlay.yaml is mounted
	// (03-contracts.md §5.4). The Deployment fixes it; §5.4 has no env switch
	// on purpose, so only tests point it elsewhere.
	OverlayPath string

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
		OverlayPath:      OverlayPath,
	}

	if v := strings.TrimSpace(getenv(EnvJobNamespace)); v != "" {
		cfg.JobNamespace = v
	}
	if v := strings.TrimSpace(getenv(EnvJobImage)); v != "" {
		cfg.JobImage = v
	}

	digest := strings.TrimSpace(getenv(EnvJobImageDigest))
	if digest == "" {
		return Config{}, fmt.Errorf("%s is required", EnvJobImageDigest)
	}
	if !imageDigestPattern.MatchString(digest) {
		return Config{}, fmt.Errorf("%s must match %q, got %q", EnvJobImageDigest, imageDigestPattern, digest)
	}
	cfg.JobImageDigest = digest

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
