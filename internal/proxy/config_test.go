package proxy

import (
	"strings"
	"testing"
	"time"
)

// leaseEnv supplies the two required keys; lease is empty unless the case
// overrides FOREMAN_LEASE_REFRESH_INTERVAL.
func leaseEnv(lease string) func(string) string {
	env := map[string]string{
		EnvServerURL:   "http://multica.example",
		EnvWorkspaceID: "ws-test",
	}
	if lease != "" {
		env[EnvLeaseRefreshInterval] = lease
	}
	return func(key string) string { return env[key] }
}

// TestLoadConfigLeaseRefreshBound pins §10: the accepted renewal cadence is at
// most 20s, and a refused value names the env, the value and the bound so the
// resulting CrashLoop is attributable.
func TestLoadConfigLeaseRefreshBound(t *testing.T) {
	cases := map[string]struct {
		lease   string
		want    time.Duration
		wantErr bool
	}{
		"default":              {"", DefaultLeaseRefreshInterval, false},
		"contract cadence":     {"15s", 15 * time.Second, false},
		"upper bound accepted": {"20s", MaxLeaseRefreshInterval, false},
		"just above bound":     {"21s", 0, true},
		"issue regression":     {"30s", 0, true},
		"within old ceiling":   {"44s", 0, true},
	}
	for name, tc := range cases {
		cfg, err := LoadConfig(leaseEnv(tc.lease))
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s: %s %q was accepted, want a refusal above %s",
					name, EnvLeaseRefreshInterval, tc.lease, MaxLeaseRefreshInterval)
				continue
			}
			for _, want := range []string{EnvLeaseRefreshInterval, tc.lease, MaxLeaseRefreshInterval.String()} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s: error %q does not name %q", name, err, want)
				}
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: LoadConfig: %v", name, err)
			continue
		}
		if cfg.LeaseRefreshInterval != tc.want {
			t.Errorf("%s: LeaseRefreshInterval = %s, want %s", name, cfg.LeaseRefreshInterval, tc.want)
		}
	}
}

// TestLoadConfigSkillCache pins the resolve cache's configuration surface
// (§1.8): the downgrade window's zero is a value (its off switch), a retention
// shorter than the fresh window is refused instead of silently weakening the
// cache the design requires, and so is a downgrade window below the fresh
// window, which could never apply. The equality boundary stays legal.
func TestLoadConfigSkillCache(t *testing.T) {
	env := func(overrides map[string]string) func(string) string {
		base := map[string]string{
			EnvServerURL:   "http://multica.example",
			EnvWorkspaceID: "ws-test",
		}
		for k, v := range overrides {
			base[k] = v
		}
		return func(key string) string { return base[key] }
	}

	cfg, err := LoadConfig(env(nil))
	if err != nil {
		t.Fatalf("LoadConfig defaults: %v", err)
	}
	if cfg.SkillCacheFreshTTL != DefaultSkillCacheFreshTTL ||
		cfg.SkillCacheDowngradeWindow != DefaultSkillCacheDowngradeWindow ||
		cfg.SkillCacheRetention != DefaultSkillCacheRetention ||
		cfg.SkillCacheMaxEntries != DefaultSkillCacheMaxEntries ||
		cfg.SkillCacheMaxBytes != DefaultSkillCacheMaxBytes {
		t.Errorf("skill cache defaults = %+v, want the contract defaults", cfg)
	}

	// Zero is the documented off switch, so it must survive parsing.
	cfg, err = LoadConfig(env(map[string]string{EnvSkillCacheDowngradeWindow: "0"}))
	if err != nil {
		t.Fatalf("LoadConfig(zero downgrade): %v", err)
	}
	if cfg.SkillCacheDowngradeWindow != 0 {
		t.Errorf("SkillCacheDowngradeWindow = %s, want 0", cfg.SkillCacheDowngradeWindow)
	}

	_, err = LoadConfig(env(map[string]string{
		EnvSkillCacheFreshTTL:   "2h",
		EnvSkillCacheRetention:  "1h",
		EnvSkillCacheMaxEntries: "8",
		EnvSkillCacheMaxBytes:   "512Mi",
	}))
	if err == nil {
		t.Fatal("a retention below the fresh window was accepted")
	}
	for _, want := range []string{EnvSkillCacheRetention, EnvSkillCacheFreshTTL} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}

	// The downgrade is only reachable past the fresh window, so a shorter one
	// would be a knob that never fires: it is refused too.
	_, err = LoadConfig(env(map[string]string{
		EnvSkillCacheFreshTTL:        "1h",
		EnvSkillCacheDowngradeWindow: "5m",
	}))
	if err == nil {
		t.Fatal("a downgrade window below the fresh window was accepted")
	}
	for _, want := range []string{EnvSkillCacheDowngradeWindow, EnvSkillCacheFreshTTL} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
	// Equal windows are the boundary the design allows: at that age the entry
	// is stale, which is exactly where the downgrade may answer.
	if _, err := LoadConfig(env(map[string]string{
		EnvSkillCacheFreshTTL:        "1h",
		EnvSkillCacheDowngradeWindow: "1h",
		EnvSkillCacheRetention:       "1h",
	})); err != nil {
		t.Errorf("LoadConfig(window == fresh): %v", err)
	}

	for _, bad := range []map[string]string{
		{EnvSkillCacheMaxEntries: "0"},
		{EnvSkillCacheMaxBytes: "not-a-size"},
		{EnvSkillCacheDowngradeWindow: "-1s"},
		{EnvSkillCacheFreshTTL: "0s"},
	} {
		if _, err := LoadConfig(env(bad)); err == nil {
			t.Errorf("LoadConfig(%v) was accepted, want a refusal", bad)
		}
	}

	cfg, err = LoadConfig(env(map[string]string{
		EnvSkillCacheMaxBytes:   "512Mi",
		EnvSkillCacheMaxEntries: "8",
	}))
	if err != nil {
		t.Fatalf("LoadConfig(valid overrides): %v", err)
	}
	if cfg.SkillCacheMaxBytes != 512<<20 || cfg.SkillCacheMaxEntries != 8 {
		t.Errorf("overrides = %d entries / %d bytes, want 8 / 512Mi", cfg.SkillCacheMaxEntries, cfg.SkillCacheMaxBytes)
	}
}

// TestLoadConfigModelCatalogBudget pins §1.3's startup self-check: the catalog
// budget must cover three probe heartbeat intervals, because a probe starts on
// a third of the budget as its floor and that floor has to last one heartbeat
// delivery. The refused value names the env, the value and the bound, so the
// resulting CrashLoop is attributable.
func TestLoadConfigModelCatalogBudget(t *testing.T) {
	env := func(timeout string) func(string) string {
		base := map[string]string{
			EnvServerURL:   "http://multica.example",
			EnvWorkspaceID: "ws-test",
		}
		if timeout != "" {
			base[EnvModelCatalogTimeout] = timeout
		}
		return func(key string) string { return base[key] }
	}

	// The default equals the bound (3 x 15s): the boundary is accepted, and a
	// legal override must reach the returned value — a loader that dropped or
	// clamped it to the default would otherwise pass.
	accepted := []struct {
		timeout string
		want    time.Duration
	}{
		{"", 45 * time.Second},
		{"45s", 45 * time.Second},
		{"1h", time.Hour},
	}
	for _, tc := range accepted {
		cfg, err := LoadConfig(env(tc.timeout))
		if err != nil {
			t.Errorf("LoadConfig(%q): %v", tc.timeout, err)
			continue
		}
		if cfg.ModelCatalogTimeout != tc.want {
			t.Errorf("LoadConfig(%q): ModelCatalogTimeout = %s, want %s", tc.timeout, cfg.ModelCatalogTimeout, tc.want)
		}
	}

	for _, timeout := range []string{"30s", "44.999s"} {
		_, err := LoadConfig(env(timeout))
		if err == nil {
			t.Errorf("LoadConfig(%q) was accepted, want a refusal", timeout)
			continue
		}
		// The refusal must state the inequality and the actual value: the
		// operator reads it out of a CrashLoop line.
		for _, want := range []string{EnvModelCatalogTimeout, ">= 3x", timeout, ProbeHeartbeatInterval.String()} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("LoadConfig(%q): error %q does not name %q", timeout, err, want)
			}
		}
	}
}
