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
