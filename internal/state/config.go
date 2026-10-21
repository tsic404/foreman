// Package state implements Foreman's shared state face (阶段一 of
// ADR-046): the Job Token entries, the revocation set and the task mapping
// records live in a shared, persistent store instead of process memory
// alone. 02-contracts.md §6.1/§7, §4.3; AC-73/TC-tech-multi-08.
//
// Stage one keeps the single-instance form: the shared store is the
// persistence face the instance restores from on restart, and the K8s Job
// objects remain the durable truth (ADR-002) when the store is lost.
package state

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Environment variable names of the shared state module.
const (
	// EnvURL names the shared state connection. For the Redis backend it is
	// a redis:// URL (rediss:// for TLS). A URL carrying master_name names a
	// sentinel topology — the host is the first sentinel, each addr value
	// adds another — and is served by a failover client, so the deployer
	// upgrades availability without changing the contract (§7); any other
	// URL is the single-endpoint form. It is the connection information the
	// deployment interface list must carry (02-contracts.md §7 部署面).
	EnvURL = "FOREMAN_STATE_URL"
	// EnvPassword carries the shared state credential separately from the
	// URL, so the URL can stay free of secrets and the value can come from a
	// Secret keyRef. The contract requires the credential carrier to be part
	// of the deployment interface list (§7).
	EnvPassword = "FOREMAN_STATE_PASSWORD"
	// EnvDialTimeout bounds one connection attempt to the shared store.
	EnvDialTimeout = "FOREMAN_STATE_DIAL_TIMEOUT"
	// EnvTTL is the retention of a live entry snapshot in the shared store.
	// It must comfortably cover the longest credential lifetime the process
	// can restart across: the Job Token TTL (claim → boot → task → drain,
	// §6.1). The default covers the default token TTL with margin.
	EnvTTL = "FOREMAN_STATE_ENTRY_TTL"
)

const (
	// DefaultTTL is the shared-store retention of one entry snapshot: the
	// default Job Token TTL (24h7m) plus the 24h settled-record retention,
	// so a restart up to that age restores both the live row and its
	// terminal record.
	DefaultTTL = 48 * time.Hour
	// DefaultDialTimeout is the default bound of one shared-store
	// connection attempt.
	DefaultDialTimeout = 3 * time.Second
)

// Config is the shared state module's runtime configuration.
type Config struct {
	// URL is the shared store connection string (FOREMAN_STATE_URL).
	URL string
	// Password is the shared store credential (FOREMAN_STATE_PASSWORD).
	Password string
	// DialTimeout bounds one connection attempt.
	DialTimeout time.Duration
	// EntryTTL is the retention of one entry snapshot.
	EntryTTL time.Duration
}

// LoadConfig reads the shared state configuration from env (os.Getenv when
// getenv is nil). An unset FOREMAN_STATE_URL is the legal default: the
// shared store is optional at stage one, and its absence degrades to the
// existing K8s-rebuild path (§4.3) — never a startup error.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{
		DialTimeout: DefaultDialTimeout,
		EntryTTL:    DefaultTTL,
	}
	cfg.URL = strings.TrimSpace(getenv(EnvURL))
	cfg.Password = getenv(EnvPassword)
	if raw := strings.TrimSpace(getenv(EnvDialTimeout)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvDialTimeout, err)
		}
		if d <= 0 {
			return Config{}, fmt.Errorf("%s must be positive, got %s", EnvDialTimeout, d)
		}
		cfg.DialTimeout = d
	}
	if raw := strings.TrimSpace(getenv(EnvTTL)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a duration: %v", EnvTTL, err)
		}
		if d <= 0 {
			return Config{}, fmt.Errorf("%s must be positive, got %s", EnvTTL, d)
		}
		cfg.EntryTTL = d
	}
	return cfg, nil
}
