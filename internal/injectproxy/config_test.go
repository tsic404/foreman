package injectproxy

import (
	"log/slog"
	"testing"
)

func envFunc(env map[string]string) func(string) string {
	return func(k string) string { return env[k] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(envFunc(map[string]string{EnvUpstreamURL: "http://test-multica.test.svc:8080"}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.DataAddr != DefaultDataAddr || cfg.ControlAddr != DefaultControlAddr {
		t.Fatalf("addresses = %q/%q, want %q/%q", cfg.DataAddr, cfg.ControlAddr, DefaultDataAddr, DefaultControlAddr)
	}
	if cfg.RecordDir != DefaultRecordDir {
		t.Fatalf("record dir = %q, want %q", cfg.RecordDir, DefaultRecordDir)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Fatalf("log level = %v, want info", cfg.LogLevel)
	}
	if got := cfg.RecordResponseBodySources(); len(got) != 1 || got[0] != DefaultRecordResponseBodies {
		t.Fatalf("response body face = %v, want [%s]", got, DefaultRecordResponseBodies)
	}
	if !cfg.RecordsResponseBody("/api/daemon/tasks/claim") {
		t.Fatal("default face does not cover the C3 claim path")
	}
	if cfg.RecordsResponseBody("/api/daemon/tasks/claim/extra") {
		t.Fatal("default face is not anchored to the claim path")
	}
}

func TestLoadConfigDefaultPorts(t *testing.T) {
	for _, tc := range []struct{ upstream, want string }{
		{"http://host.test", "host.test:80"},
		{"https://host.test", "host.test:443"},
		{"http://host.test:9000", "host.test:9000"},
	} {
		cfg, err := LoadConfig(envFunc(map[string]string{EnvUpstreamURL: tc.upstream}))
		if err != nil {
			t.Fatalf("LoadConfig(%q): %v", tc.upstream, err)
		}
		if cfg.Upstream.Host != tc.want {
			t.Fatalf("upstream host for %q = %q, want %q", tc.upstream, cfg.Upstream.Host, tc.want)
		}
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := LoadConfig(envFunc(map[string]string{
		EnvUpstreamURL:          "https://test-multica.test.svc",
		EnvDataAddr:             ":9443",
		EnvControlAddr:          "127.0.0.1:9444",
		EnvRecordDir:            "/records",
		EnvRecordResponseBodies: `^/api/daemon/tasks/[^/]+/complete$`,
		EnvLogLevel:             "debug",
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.DataAddr != ":9443" || cfg.ControlAddr != "127.0.0.1:9444" || cfg.RecordDir != "/records" {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Fatalf("log level = %v, want debug", cfg.LogLevel)
	}
	if !cfg.RecordsResponseBody("/api/daemon/tasks/t-1/complete") {
		t.Fatal("configured face does not cover the terminal path")
	}
	if cfg.RecordsResponseBody("/api/daemon/tasks/claim") {
		t.Fatal("configured face still covers the claim path after the default was replaced")
	}
}

func TestLoadConfigNeverMatchingFace(t *testing.T) {
	cfg, err := LoadConfig(envFunc(map[string]string{
		EnvUpstreamURL:          "http://test-multica.test.svc",
		EnvRecordResponseBodies: `^$`,
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.RecordsResponseBody("/api/daemon/tasks/claim") {
		t.Fatal("^$ must record status codes only")
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"missing upstream": {},
		"bad scheme":       {EnvUpstreamURL: "ftp://host.test"},
		"no host":          {EnvUpstreamURL: "http://"},
		"same addresses":   {EnvUpstreamURL: "http://host.test", EnvDataAddr: ":9000", EnvControlAddr: ":9000"},
		"bad regex":        {EnvUpstreamURL: "http://host.test", EnvRecordResponseBodies: `^/api/($`},
		"bad log level":    {EnvUpstreamURL: "http://host.test", EnvLogLevel: "trace"},
	} {
		if _, err := LoadConfig(envFunc(env)); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}
