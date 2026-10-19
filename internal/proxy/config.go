package proxy

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Environment variable names read by the proxy module.
const (
	EnvServerURL             = "MULTICA_SERVER_URL"
	EnvWorkspaceID           = "MULTICA_WORKSPACE_ID"
	EnvDaemonID              = "FOREMAN_DAEMON_ID"
	EnvClaimInterval         = "FOREMAN_CLAIM_INTERVAL"
	EnvClaimIntervalIdle     = "FOREMAN_CLAIM_INTERVAL_IDLE"
	EnvLeaseRefreshInterval  = "FOREMAN_LEASE_REFRESH_INTERVAL"
	EnvWSEnabled             = "FOREMAN_WS_ENABLED"
	EnvModelCatalogEnabled   = "FOREMAN_MODEL_CATALOG_ENABLED"
	EnvModelCatalogTimeout   = "FOREMAN_MODEL_CATALOG_TIMEOUT"
	EnvModelCatalogCacheTTL  = "FOREMAN_MODEL_CATALOG_CACHE_TTL"
	EnvWorkspaceStateTimeout = "FOREMAN_WORKSPACE_STATE_TIMEOUT"

	EnvSkillCacheFreshTTL        = "FOREMAN_SKILL_CACHE_FRESH_TTL"
	EnvSkillCacheDowngradeWindow = "FOREMAN_SKILL_CACHE_DOWNGRADE_WINDOW"
	EnvSkillCacheRetention       = "FOREMAN_SKILL_CACHE_RETENTION"
	EnvSkillCacheMaxEntries      = "FOREMAN_SKILL_CACHE_MAX_ENTRIES"
	EnvSkillCacheMaxBytes        = "FOREMAN_SKILL_CACHE_MAX_BYTES"
)

// Default values applied when the environment variable is unset.
const (
	// DefaultDaemonID is the contract's foreman-<cluster> pattern collapsed
	// to the single-cluster deployment v1 supports.
	DefaultDaemonID             = "foreman"
	DefaultClaimInterval        = 5 * time.Second
	DefaultClaimIntervalIdle    = 30 * time.Second
	DefaultLeaseRefreshInterval = 15 * time.Second
	DefaultWSEnabled            = true
	DefaultModelCatalogEnabled  = true
	DefaultModelCatalogTimeout  = 45 * time.Second
	DefaultModelCatalogCacheTTL = 6 * time.Hour
	// DefaultWorkspaceStateTimeout bounds one C-22 refresh (§1.7 有界超时):
	// the S-2 answer may wait this long and then replays the last snapshot.
	DefaultWorkspaceStateTimeout = 5 * time.Second

	// Skill-bundle resolve cache (§1.8). The fresh window is the period a
	// resolved payload answers without touching upstream; the downgrade window
	// is the outer age at which an entry may still answer an upstream failure
	// (0 = off, the ADR-045 option B form); retention reclaims an entry left
	// unused for that long, and the two capacity bounds are reclaimed
	// least-recently-used first.
	DefaultSkillCacheFreshTTL        = 30 * time.Minute
	DefaultSkillCacheDowngradeWindow = 6 * time.Hour
	DefaultSkillCacheRetention       = 6 * time.Hour
	DefaultSkillCacheMaxEntries      = 1024
	DefaultSkillCacheMaxBytes        = 256 << 20 // 256Mi

	// MaxLeaseRefreshInterval is the upper bound on the renewal cadence: §10
	// requires a claimed, not-yet-started task to be renewed every ≤20s.
	MaxLeaseRefreshInterval = 20 * time.Second

	// ProbeHeartbeatInterval is the fixed heartbeat interval written into the
	// catalog probe env; the startup self-check ties ModelCatalogTimeout to it.
	ProbeHeartbeatInterval = 15 * time.Second

	// ClientCapabilities is the verbatim X-Client-Capabilities header value;
	// adding or dropping an entry changes server response shapes (§1.1).
	ClientCapabilities = "skill-bundles-v1,coalesced-comments-v1,rpc-v1"

	// controlTimeout is the control-plane HTTP timeout (§1.5).
	controlTimeout = 30 * time.Second
	// leaseTimeout is the C4 prepare-lease timeout (§1.5).
	leaseTimeout = 10 * time.Second
)

// Config is the proxy module's runtime configuration.
type Config struct {
	ServerURL   string // MULTICA_SERVER_URL, no trailing slash
	WorkspaceID string // MULTICA_WORKSPACE_ID
	DaemonID    string // FOREMAN_DAEMON_ID
	// Version is sent as X-Client-Version before the startup probe registers,
	// and for the process identity (§1.1). After C1 it is superseded by the
	// startup discovery's cli_version (the template CLI's real version).
	Version string

	ClaimInterval        time.Duration
	ClaimIntervalIdle    time.Duration
	LeaseRefreshInterval time.Duration
	WSEnabled            bool

	// ModelCatalogEnabled is the path-level switch of the model catalog route
	// (ack → probe → C21); false disables consumption but not the startup probe.
	ModelCatalogEnabled bool
	// ModelCatalogTimeout is one catalog probe's own budget, anchored at the
	// moment the ack is consumed (§1.3).
	ModelCatalogTimeout time.Duration
	// ModelCatalogCacheTTL is the catalog cache staleness threshold: crossing
	// it only logs a warning, it never stops answering (§10).
	ModelCatalogCacheTTL time.Duration

	// WorkspaceStateTimeout bounds one C-22 refresh triggered by an S-2 call
	// (§1.7 有界超时). On expiry the answer replays the last successful
	// snapshot; the checkout is never blocked.
	WorkspaceStateTimeout time.Duration

	// SkillCacheFreshTTL is the resolve cache's fresh window: an entry younger
	// than it answers locally without touching upstream (§1.8).
	SkillCacheFreshTTL time.Duration
	// SkillCacheDowngradeWindow is the outer age bound at which an entry may
	// still answer an upstream failure; 0 turns the downgrade off (§1.8).
	SkillCacheDowngradeWindow time.Duration
	// SkillCacheRetention reclaims an entry left unused for that long (§1.8).
	SkillCacheRetention time.Duration
	// SkillCacheMaxEntries and SkillCacheMaxBytes bound the cache; the excess
	// is reclaimed least-recently-used first (§1.8).
	SkillCacheMaxEntries int
	SkillCacheMaxBytes   int64
}

// LoadConfig reads the proxy configuration from env (os.Getenv when getenv
// is nil) and applies the contract defaults. The server token is loaded
// separately via auth.LoadServerToken.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{
		DaemonID:              DefaultDaemonID,
		Version:               "dev",
		ClaimInterval:         DefaultClaimInterval,
		ClaimIntervalIdle:     DefaultClaimIntervalIdle,
		LeaseRefreshInterval:  DefaultLeaseRefreshInterval,
		WSEnabled:             DefaultWSEnabled,
		ModelCatalogEnabled:   DefaultModelCatalogEnabled,
		ModelCatalogTimeout:   DefaultModelCatalogTimeout,
		ModelCatalogCacheTTL:  DefaultModelCatalogCacheTTL,
		WorkspaceStateTimeout: DefaultWorkspaceStateTimeout,

		SkillCacheFreshTTL:        DefaultSkillCacheFreshTTL,
		SkillCacheDowngradeWindow: DefaultSkillCacheDowngradeWindow,
		SkillCacheRetention:       DefaultSkillCacheRetention,
		SkillCacheMaxEntries:      DefaultSkillCacheMaxEntries,
		SkillCacheMaxBytes:        DefaultSkillCacheMaxBytes,
	}

	cfg.ServerURL = strings.TrimRight(strings.TrimSpace(getenv(EnvServerURL)), "/")
	if cfg.ServerURL == "" {
		return Config{}, fmt.Errorf("%s is required", EnvServerURL)
	}
	parsed, err := url.Parse(cfg.ServerURL)
	if err != nil {
		return Config{}, fmt.Errorf("%s is not a valid URL: %v", EnvServerURL, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return Config{}, fmt.Errorf("%s must use http or https, got %q", EnvServerURL, parsed.Scheme)
	}
	if parsed.Host == "" {
		return Config{}, fmt.Errorf("%s must name a host", EnvServerURL)
	}
	cfg.WorkspaceID = strings.TrimSpace(getenv(EnvWorkspaceID))
	if cfg.WorkspaceID == "" {
		return Config{}, fmt.Errorf("%s is required", EnvWorkspaceID)
	}
	if v := strings.TrimSpace(getenv(EnvDaemonID)); v != "" {
		cfg.DaemonID = v
	}

	if cfg.ClaimInterval, err = envDuration(getenv, EnvClaimInterval, cfg.ClaimInterval); err != nil {
		return Config{}, err
	}
	if cfg.ClaimIntervalIdle, err = envDuration(getenv, EnvClaimIntervalIdle, cfg.ClaimIntervalIdle); err != nil {
		return Config{}, err
	}
	if cfg.LeaseRefreshInterval, err = envDuration(getenv, EnvLeaseRefreshInterval, cfg.LeaseRefreshInterval); err != nil {
		return Config{}, err
	}
	// The interval is the renewal cadence itself, so a value above §10's 20s
	// bound leaves a claimed task unrenewed for longer than the contract
	// allows — one delayed tick plus the C4 request timeout then approaches
	// the 45s server lease and the task can be redispatched before it starts.
	if cfg.LeaseRefreshInterval > MaxLeaseRefreshInterval {
		return Config{}, fmt.Errorf("%s (%s) must be <= %s (§10 lease renewal bound)",
			EnvLeaseRefreshInterval, cfg.LeaseRefreshInterval, MaxLeaseRefreshInterval)
	}
	if raw := strings.TrimSpace(getenv(EnvWSEnabled)); raw != "" {
		b, perr := strconv.ParseBool(raw)
		if perr != nil {
			return Config{}, fmt.Errorf("%s must be a boolean: %v", EnvWSEnabled, perr)
		}
		cfg.WSEnabled = b
	}
	if raw := strings.TrimSpace(getenv(EnvModelCatalogEnabled)); raw != "" {
		b, perr := strconv.ParseBool(raw)
		if perr != nil {
			return Config{}, fmt.Errorf("%s must be a boolean: %v", EnvModelCatalogEnabled, perr)
		}
		cfg.ModelCatalogEnabled = b
	}
	if cfg.ModelCatalogTimeout, err = envDuration(getenv, EnvModelCatalogTimeout, cfg.ModelCatalogTimeout); err != nil {
		return Config{}, err
	}
	if cfg.ModelCatalogCacheTTL, err = envDuration(getenv, EnvModelCatalogCacheTTL, cfg.ModelCatalogCacheTTL); err != nil {
		return Config{}, err
	}
	if cfg.WorkspaceStateTimeout, err = envDuration(getenv, EnvWorkspaceStateTimeout, cfg.WorkspaceStateTimeout); err != nil {
		return Config{}, err
	}
	if cfg.WorkspaceStateTimeout <= 0 {
		return Config{}, fmt.Errorf("%s must be positive", EnvWorkspaceStateTimeout)
	}
	// §1.3 startup self-check: probeMinBudget (= timeout/3) must cover one
	// heartbeat period, otherwise a probe started on the minimum budget can
	// never receive its command before the budget runs out.
	if cfg.ModelCatalogTimeout < 3*ProbeHeartbeatInterval {
		return Config{}, fmt.Errorf("%s must be >= 3x the probe heartbeat interval (%s), got %s",
			EnvModelCatalogTimeout, ProbeHeartbeatInterval, cfg.ModelCatalogTimeout)
	}
	if cfg.ModelCatalogCacheTTL <= 0 {
		return Config{}, fmt.Errorf("%s must be positive", EnvModelCatalogCacheTTL)
	}
	if cfg.SkillCacheFreshTTL, err = envDuration(getenv, EnvSkillCacheFreshTTL, cfg.SkillCacheFreshTTL); err != nil {
		return Config{}, err
	}
	// Zero is a value here, not a missing one: it turns the downgrade off
	// (§1.8「降级窗口可配置，取零即关闭」), so it must survive a round trip.
	if raw := strings.TrimSpace(getenv(EnvSkillCacheDowngradeWindow)); raw != "" {
		d, perr := time.ParseDuration(raw)
		if perr != nil || d < 0 {
			return Config{}, fmt.Errorf("%s must be a non-negative duration: %q", EnvSkillCacheDowngradeWindow, raw)
		}
		cfg.SkillCacheDowngradeWindow = d
	}
	if cfg.SkillCacheRetention, err = envDuration(getenv, EnvSkillCacheRetention, cfg.SkillCacheRetention); err != nil {
		return Config{}, err
	}
	if raw := strings.TrimSpace(getenv(EnvSkillCacheMaxEntries)); raw != "" {
		n, perr := strconv.Atoi(raw)
		if perr != nil || n < 1 {
			return Config{}, fmt.Errorf("%s must be a positive integer: %q", EnvSkillCacheMaxEntries, raw)
		}
		cfg.SkillCacheMaxEntries = n
	}
	if raw := strings.TrimSpace(getenv(EnvSkillCacheMaxBytes)); raw != "" {
		n, perr := byteSize(EnvSkillCacheMaxBytes, raw)
		if perr != nil {
			return Config{}, perr
		}
		if n < 1 {
			return Config{}, fmt.Errorf("%s must be positive", EnvSkillCacheMaxBytes)
		}
		cfg.SkillCacheMaxBytes = n
	}
	// A retention shorter than the fresh window would reclaim an entry before
	// its fresh window could ever be used: G10 requires the cache to be bounded
	// and usable, so the pair is rejected instead of silently weakened.
	if cfg.SkillCacheRetention < cfg.SkillCacheFreshTTL {
		return Config{}, fmt.Errorf("%s (%s) must be >= %s (%s)",
			EnvSkillCacheRetention, cfg.SkillCacheRetention, EnvSkillCacheFreshTTL, cfg.SkillCacheFreshTTL)
	}
	// A downgrade window shorter than the fresh window can never apply: the
	// downgrade is only reached once an entry is past fresh (a fresher entry
	// answers locally without consulting upstream), so such a pair would turn
	// the knob off silently. Zero stays the documented off value (§1.8).
	if cfg.SkillCacheDowngradeWindow != 0 && cfg.SkillCacheDowngradeWindow < cfg.SkillCacheFreshTTL {
		return Config{}, fmt.Errorf("%s (%s) must be 0 or >= %s (%s): a shorter window could never apply",
			EnvSkillCacheDowngradeWindow, cfg.SkillCacheDowngradeWindow, EnvSkillCacheFreshTTL, cfg.SkillCacheFreshTTL)
	}
	return cfg, nil
}

func envDuration(getenv func(string) string, key string, def time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(getenv(key))
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration: %q", key, raw)
	}
	return d, nil
}

// byteSize parses the contract's quantity syntax (256Mi, 1Gi, plain bytes),
// the same form the node-cache bound uses.
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
