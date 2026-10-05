package proxy

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
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
	// Version is sent as X-Client-Version / cli_version; build-time stamped.
	Version string
	// RuntimeName/RuntimeType/RuntimeVersion describe the single runtime
	// Foreman registers (C1); the type field pins the omp provider contract.
	RuntimeName    string
	RuntimeType    string
	RuntimeVersion string

	ClaimInterval        time.Duration
	ClaimIntervalIdle    time.Duration
	LeaseRefreshInterval time.Duration
	WSEnabled            bool
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
		RuntimeName:          "foreman-omp",
		RuntimeType:          "omp",
		RuntimeVersion:       "unknown",
		ClaimInterval:        DefaultClaimInterval,
		ClaimIntervalIdle:    DefaultClaimIntervalIdle,
		LeaseRefreshInterval: DefaultLeaseRefreshInterval,
		WSEnabled:            DefaultWSEnabled,
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
	if cfg.LeaseRefreshInterval >= 45*time.Second {
		return Config{}, fmt.Errorf("%s must be < 45s (server prepare lease), got %s", EnvLeaseRefreshInterval, cfg.LeaseRefreshInterval)
	}
	if raw := strings.TrimSpace(getenv(EnvWSEnabled)); raw != "" {
		b, perr := strconv.ParseBool(raw)
		if perr != nil {
			return Config{}, fmt.Errorf("%s must be a boolean: %v", EnvWSEnabled, perr)
		}
		cfg.WSEnabled = b
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
