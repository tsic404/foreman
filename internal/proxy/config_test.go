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

// TestLoadConfigServerURLScheme pins §1.5's configuration-error face for the
// upstream address: the WS derivation only understands http(s), so any other
// scheme must be refused at load time rather than silently producing a broken
// ws:// URL.
func TestLoadConfigServerURLScheme(t *testing.T) {
	cases := map[string]struct {
		url     string
		wantErr bool
	}{
		"http":      {"http://multica.example", false},
		"https":     {"https://multica.example", false},
		"ftp":       {"ftp://multica.example", true},
		"no scheme": {"multica.example:8080", true},
		"no host":   {"http://", true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env := map[string]string{EnvServerURL: tc.url, EnvWorkspaceID: "ws-test"}
			_, err := LoadConfig(func(key string) string { return env[key] })
			if tc.wantErr != (err != nil) {
				t.Fatalf("LoadConfig(%q) err = %v, wantErr %v", tc.url, err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), EnvServerURL) {
				t.Errorf("error %q must name %s", err, EnvServerURL)
			}
		})
	}
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
