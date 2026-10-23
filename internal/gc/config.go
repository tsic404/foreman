// Package gc implements foreman-gc, the node-local cache collector that
// runs as a DaemonSet (one pod per node) over the hostPath state root.
// Design: 02-contracts.md §8.8 (节点状态清理), §10 (缓存与节点状态限额),
// ADR-005 (node-local hostPath), ADR-006 (shared .repos).
package gc

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/tsic404/foreman/internal/observability"
)

// Environment variable names of the node-cleanup component.
const (
	EnvStateRoot     = "FOREMAN_STATE_ROOT"
	EnvCacheTTL      = "FOREMAN_CACHE_TTL"
	EnvTaskDirTTL    = "FOREMAN_TASK_DIR_TTL"
	EnvCacheMaxBytes = "FOREMAN_CACHE_MAX_BYTES"
	EnvGCInterval    = "FOREMAN_GC_INTERVAL"
	EnvGCQuiesce     = "FOREMAN_GC_QUIESCE"

	// EnvAgentStateTTL / EnvAgentMemoryTTL cover the node-local agent state
	// outside the workspaces tree: profile-relative session/transcript stores,
	// pi-sessions and the codex session stores (state), and hermes memory
	// (memory). 0 turns the rule off.
	EnvAgentStateTTL  = "FOREMAN_AGENT_STATE_TTL"
	EnvAgentMemoryTTL = "FOREMAN_AGENT_MEMORY_TTL"

	// EnvSkillCacheTTL / EnvSkillCacheMaxBytes bound the node-side skill
	// bundle cache the upstream daemon keeps under the workspaces tree. The
	// GC prefix keeps them apart from the proxy's in-process resolve cache
	// settings, which share the FOREMAN_SKILL_CACHE_* namespace.
	EnvSkillCacheTTL      = "FOREMAN_GC_SKILL_CACHE_TTL"
	EnvSkillCacheMaxBytes = "FOREMAN_GC_SKILL_CACHE_MAX_BYTES"

	// EnvNodeName carries the node the pod runs on (downward API
	// spec.nodeName); it is the `node` field of the cache.gc log event.
	EnvNodeName = "NODE_NAME"
)

// Deployment defaults.
const (
	DefaultStateRoot = "/var/lib/foreman"
	DefaultCacheTTL  = 720 * time.Hour
	// DefaultTaskDirTTL covers both task directories and orphan task roots
	// (§8.8: 7 days).
	DefaultTaskDirTTL    = 168 * time.Hour
	DefaultCacheMaxBytes = 50 << 30 // 50Gi
	DefaultGCInterval    = 6 * time.Hour
	DefaultGCQuiesce     = 30 * time.Minute
	// DefaultAgentStateTTL / DefaultAgentMemoryTTL align with the upstream GC
	// defaults (14 days for sessions/transcripts, 90 days for memory).
	DefaultAgentStateTTL  = 336 * time.Hour
	DefaultAgentMemoryTTL = 2160 * time.Hour
	// DefaultSkillCacheTTL matches the bare-cache retention: both are
	// re-fetchable node caches.
	DefaultSkillCacheTTL = 720 * time.Hour
	// DefaultSkillCacheMaxBytes is the per-workspace ceiling. Bundle files are
	// small, so a working set sits far below it; a reclaim only costs a
	// re-fetch, never a task failure.
	DefaultSkillCacheMaxBytes int64 = 10 << 30 // 10Gi
)

// Config is the gc module's runtime configuration.
type Config struct {
	// StateRoot is the state root as seen by this process: the DaemonSet
	// mounts the node's ${FOREMAN_STATE_ROOT} at /state and passes /state.
	StateRoot string
	// Node is the node this instance cleans; empty outside the DaemonSet.
	Node string
	// CacheTTL is the bare-cache retention (FOREMAN_CACHE_TTL).
	CacheTTL time.Duration
	// TaskDirTTL is the task-directory and orphan task-root retention
	// (FOREMAN_TASK_DIR_TTL).
	TaskDirTTL time.Duration
	// CacheMaxBytes caps the total size of .repos; the excess is reclaimed
	// LRU-first (FOREMAN_CACHE_MAX_BYTES).
	CacheMaxBytes int64
	// Quiesce skips a round when the state root was written within it
	// (FOREMAN_GC_QUIESCE): a round must never race a running Job.
	Quiesce time.Duration
	// AgentStateTTL is the retention for node-local agent sessions and
	// transcripts (FOREMAN_AGENT_STATE_TTL); 0 disables the rule.
	AgentStateTTL time.Duration
	// AgentMemoryTTL is the retention for node-local agent memory
	// (FOREMAN_AGENT_MEMORY_TTL); 0 disables the rule.
	AgentMemoryTTL time.Duration
	// SkillCacheTTL is the retention for the node-side skill bundle cache
	// (FOREMAN_GC_SKILL_CACHE_TTL).
	SkillCacheTTL time.Duration
	// SkillCacheMaxBytes caps one workspace's skill bundle cache; the excess
	// is reclaimed LRU-first (FOREMAN_GC_SKILL_CACHE_MAX_BYTES).
	SkillCacheMaxBytes int64
	// Interval is the round cadence (FOREMAN_GC_INTERVAL).
	Interval time.Duration
	// LogLevel comes from FOREMAN_LOG_LEVEL.
	LogLevel slog.Level
}

// LoadConfig reads the gc configuration from env (os.Getenv when getenv is
// nil) and applies the contract defaults.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	obs, err := observability.LoadConfig(getenv)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		StateRoot:      DefaultStateRoot,
		CacheTTL:       DefaultCacheTTL,
		TaskDirTTL:     DefaultTaskDirTTL,
		CacheMaxBytes:  DefaultCacheMaxBytes,
		Quiesce:        DefaultGCQuiesce,
		Interval:       DefaultGCInterval,
		AgentStateTTL:  DefaultAgentStateTTL,
		AgentMemoryTTL: DefaultAgentMemoryTTL,

		SkillCacheTTL:      DefaultSkillCacheTTL,
		SkillCacheMaxBytes: DefaultSkillCacheMaxBytes,
		LogLevel:           obs.LogLevel,
	}
	if v := strings.TrimSpace(getenv(EnvStateRoot)); v != "" {
		cfg.StateRoot = v
	}
	cfg.Node = strings.TrimSpace(getenv(EnvNodeName))
	if v := strings.TrimSpace(getenv(EnvCacheTTL)); v != "" {
		d, err := positiveDuration(EnvCacheTTL, v)
		if err != nil {
			return Config{}, err
		}
		cfg.CacheTTL = d
	}
	if v := strings.TrimSpace(getenv(EnvTaskDirTTL)); v != "" {
		d, err := positiveDuration(EnvTaskDirTTL, v)
		if err != nil {
			return Config{}, err
		}
		cfg.TaskDirTTL = d
	}
	if v := strings.TrimSpace(getenv(EnvCacheMaxBytes)); v != "" {
		n, err := byteSize(EnvCacheMaxBytes, v)
		if err != nil {
			return Config{}, err
		}
		cfg.CacheMaxBytes = n
	}
	if v := strings.TrimSpace(getenv(EnvGCQuiesce)); v != "" {
		d, err := nonNegativeDuration(EnvGCQuiesce, v)
		if err != nil {
			return Config{}, err
		}
		cfg.Quiesce = d
	}
	if v := strings.TrimSpace(getenv(EnvGCInterval)); v != "" {
		d, err := positiveDuration(EnvGCInterval, v)
		if err != nil {
			return Config{}, err
		}
		cfg.Interval = d
	}
	// The agent-state rules accept 0 (rule off); negative values are rejected
	// as a configuration error, like the quiesce window.
	if v := strings.TrimSpace(getenv(EnvAgentStateTTL)); v != "" {
		d, err := nonNegativeDuration(EnvAgentStateTTL, v)
		if err != nil {
			return Config{}, err
		}
		cfg.AgentStateTTL = d
	}
	if v := strings.TrimSpace(getenv(EnvAgentMemoryTTL)); v != "" {
		d, err := nonNegativeDuration(EnvAgentMemoryTTL, v)
		if err != nil {
			return Config{}, err
		}
		cfg.AgentMemoryTTL = d
	}
	// The skill bundle cache is a live cache like the bare repos: a zero
	// retention would wipe it every round, so it takes a positive value.
	if v := strings.TrimSpace(getenv(EnvSkillCacheTTL)); v != "" {
		d, err := positiveDuration(EnvSkillCacheTTL, v)
		if err != nil {
			return Config{}, err
		}
		cfg.SkillCacheTTL = d
	}
	if v := strings.TrimSpace(getenv(EnvSkillCacheMaxBytes)); v != "" {
		n, err := byteSize(EnvSkillCacheMaxBytes, v)
		if err != nil {
			return Config{}, err
		}
		cfg.SkillCacheMaxBytes = n
	}
	return cfg, nil
}

func positiveDuration(key, raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %v", key, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %s", key, d)
	}
	return d, nil
}

func nonNegativeDuration(key, raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %v", key, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("%s must not be negative, got %s", key, d)
	}
	return d, nil
}

// byteSize parses the contract's quantity syntax (50Gi, 512Mi, plain bytes).
func byteSize(key, raw string) (int64, error) {
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a byte quantity: %v", key, err)
	}
	n := q.Value()
	if n < 0 {
		return 0, fmt.Errorf("%s must not be negative, got %q", key, raw)
	}
	return n, nil
}
