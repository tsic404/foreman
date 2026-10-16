package proxy

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/scheduler"
)

// Environment variable names from the contract config table (§5.1).
const (
	EnvServerURL            = "MULTICA_SERVER_URL"
	EnvWorkspaceID          = "MULTICA_WORKSPACE_ID"
	EnvDaemonID             = "FOREMAN_DAEMON_ID"
	EnvClaimInterval        = "FOREMAN_CLAIM_INTERVAL"
	EnvClaimIntervalIdle    = "FOREMAN_CLAIM_INTERVAL_IDLE"
	EnvLeaseRefreshInterval = "FOREMAN_LEASE_REFRESH_INTERVAL"
	EnvWSEnabled            = "FOREMAN_WS_ENABLED"
	EnvModelCatalogEnabled  = "FOREMAN_MODEL_CATALOG_ENABLED"
	EnvModelCatalogTimeout  = "FOREMAN_MODEL_CATALOG_TIMEOUT"
	EnvModelCatalogCacheTTL = "FOREMAN_MODEL_CATALOG_CACHE_TTL"
)

// Contract defaults (§5.1).
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

	// ProbeHeartbeatInterval is the fixed heartbeat interval written into the
	// catalog probe env; the startup self-check ties ModelCatalogTimeout to it.
	ProbeHeartbeatInterval = 15 * time.Second

	// ClientCapabilities is the verbatim X-Client-Capabilities header value;
	// adding or dropping an entry changes server response shapes (§1.1).
	ClientCapabilities = "skill-bundles-v1,coalesced-comments-v1,rpc-v1"

	// controlTimeout is the control-plane HTTP timeout (§4).
	controlTimeout = 30 * time.Second
	// leaseTimeout is the C4 prepare-lease timeout (§4).
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
	// moment the ack is consumed (§1.1 C21 第 6 条).
	ModelCatalogTimeout time.Duration
	// ModelCatalogCacheTTL is the catalog cache staleness threshold: crossing
	// it only logs a warning, it never stops answering (§5.1).
	ModelCatalogCacheTTL time.Duration
}

// LoadConfig reads the proxy configuration from env (os.Getenv when getenv
// is nil) and applies the contract defaults. The server token is loaded
// separately via auth.LoadServerToken.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{
		DaemonID:             DefaultDaemonID,
		Version:              "dev",
		ClaimInterval:        DefaultClaimInterval,
		ClaimIntervalIdle:    DefaultClaimIntervalIdle,
		LeaseRefreshInterval: DefaultLeaseRefreshInterval,
		WSEnabled:            DefaultWSEnabled,
		ModelCatalogEnabled:  DefaultModelCatalogEnabled,
		ModelCatalogTimeout:  DefaultModelCatalogTimeout,
		ModelCatalogCacheTTL: DefaultModelCatalogCacheTTL,
	}

	cfg.ServerURL = strings.TrimRight(strings.TrimSpace(getenv(EnvServerURL)), "/")
	if cfg.ServerURL == "" {
		return Config{}, fmt.Errorf("%s is required", EnvServerURL)
	}
	if _, err := url.Parse(cfg.ServerURL); err != nil {
		return Config{}, fmt.Errorf("%s is not a valid URL: %v", EnvServerURL, err)
	}
	cfg.WorkspaceID = strings.TrimSpace(getenv(EnvWorkspaceID))
	if cfg.WorkspaceID == "" {
		return Config{}, fmt.Errorf("%s is required", EnvWorkspaceID)
	}
	if v := strings.TrimSpace(getenv(EnvDaemonID)); v != "" {
		cfg.DaemonID = v
	}

	var err error
	if cfg.ClaimInterval, err = envDuration(getenv, EnvClaimInterval, cfg.ClaimInterval); err != nil {
		return Config{}, err
	}
	if cfg.ClaimIntervalIdle, err = envDuration(getenv, EnvClaimIntervalIdle, cfg.ClaimIntervalIdle); err != nil {
		return Config{}, err
	}
	if cfg.LeaseRefreshInterval, err = envDuration(getenv, EnvLeaseRefreshInterval, cfg.LeaseRefreshInterval); err != nil {
		return Config{}, err
	}
	// The server-side prepare lease is 45s (§1.1 C3); refreshing slower
	// guarantees duplicate dispatch.
	if cfg.LeaseRefreshInterval >= scheduler.PrepareLeaseTTL {
		return Config{}, fmt.Errorf("%s must be < %s (server prepare lease), got %s",
			EnvLeaseRefreshInterval, scheduler.PrepareLeaseTTL, cfg.LeaseRefreshInterval)
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
	// §5.1 startup self-check: probeMinBudget (= timeout/3) must cover one
	// heartbeat period, otherwise a probe started on the minimum budget can
	// never receive its command before the budget runs out.
	if cfg.ModelCatalogTimeout < 3*ProbeHeartbeatInterval {
		return Config{}, fmt.Errorf("%s must be >= 3x the probe heartbeat interval (%s), got %s",
			EnvModelCatalogTimeout, ProbeHeartbeatInterval, cfg.ModelCatalogTimeout)
	}
	if cfg.ModelCatalogCacheTTL <= 0 {
		return Config{}, fmt.Errorf("%s must be positive", EnvModelCatalogCacheTTL)
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
