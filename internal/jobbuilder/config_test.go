package jobbuilder

import (
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/auth"
)

func envFrom(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.JobNamespace != DefaultJobNamespace {
		t.Errorf("JobNamespace = %q, want %q", cfg.JobNamespace, DefaultJobNamespace)
	}
	if cfg.JobImage != DefaultJobImage {
		t.Errorf("JobImage = %q, want %q", cfg.JobImage, DefaultJobImage)
	}
	if cfg.JobTokenTTL != DefaultJobTokenTTL {
		t.Errorf("JobTokenTTL = %s, want %s", cfg.JobTokenTTL, DefaultJobTokenTTL)
	}
	// The loaded default must be the very value the auth module's startup
	// budget check validates: jobbuilder signs the token, so a second literal
	// here could pass the self-check and still expire inside the drain
	// window (ADR-017). Pin both against the single source.
	if cfg.JobTokenTTL != auth.DefaultJobTokenTTL {
		t.Errorf("JobTokenTTL = %s, want auth's %s", cfg.JobTokenTTL, auth.DefaultJobTokenTTL)
	}
	if auth.DefaultJobTokenTTL <= auth.DefaultTaskMaxDuration {
		t.Errorf("default TTL %s does not cover the task duration %s",
			auth.DefaultJobTokenTTL, auth.DefaultTaskMaxDuration)
	}
	if !cfg.PreferNodeReuse {
		t.Error("PreferNodeReuse must default to true")
	}
	if cfg.TemplatePath != TemplatePath {
		t.Errorf("TemplatePath = %q, want the fixed %q", cfg.TemplatePath, TemplatePath)
	}
	if cfg.JobDrainTimeout != DefaultJobDrainTimeout || cfg.ProbeHeartbeatInterval != DefaultProbeHeartbeatInterval {
		t.Errorf("drain/heartbeat = %s/%s, want %s/%s",
			cfg.JobDrainTimeout, cfg.ProbeHeartbeatInterval, DefaultJobDrainTimeout, DefaultProbeHeartbeatInterval)
	}
}

// TestLoadConfigIgnoresTemplateShapeKeys: the template is the only Job shape
// source (ADR-030), so the keys that used to tune the deleted built-in
// template must not be read at all. An arbitrary value — an illegal one
// included — is ignored and never refuses startup; the five resource/placement
// keys are the idle entries whose deletion follows ADR-028 / ADR-035.
func TestLoadConfigIgnoresTemplateShapeKeys(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{
		"FOREMAN_JOB_IMAGE_PULL_SECRETS": `["a"]`,
		"FOREMAN_JOB_TOLERATIONS":        `[`,
		"FOREMAN_JOB_TTL_SECONDS":        "x",
		"FOREMAN_REPO_CACHE_MODE":        "isolated",
		"FOREMAN_JOB_CPU_REQUEST":        "lots",
		"FOREMAN_JOB_MEMORY_REQUEST":     "lots",
		"FOREMAN_JOB_CPU_LIMIT":          "lots",
		"FOREMAN_JOB_MEMORY_LIMIT":       "lots",
		"FOREMAN_JOB_NODE_SELECTOR":      `{`,
		"FOREMAN_TASK_MAX_DURATION":      "-1",
	}))
	if err != nil {
		t.Fatalf("LoadConfig with template-shape keys set: %v", err)
	}
	if cfg.JobNamespace != DefaultJobNamespace || cfg.JobImage != DefaultJobImage {
		t.Errorf("the ignored keys changed namespace/image to %q/%q", cfg.JobNamespace, cfg.JobImage)
	}
}

// TestLoadConfigJobImageVerbatim: FOREMAN_JOB_IMAGE is a full image reference
// taken verbatim — tag, digest and tag@digest forms are all legal, and the
// shape is not a predicate (ADR-012).
func TestLoadConfigJobImageVerbatim(t *testing.T) {
	refs := []string{
		"ghcr.io/tsic404/foreman-job:latest",
		"ghcr.io/tsic404/foreman-job:v0.1.0",
		"ghcr.io/tsic404/foreman-job@" + testDigest,
		"ghcr.io/tsic404/foreman-job:v0.1.0@" + testDigest,
		"registry.example.com:5000/team/job",
	}
	for _, ref := range refs {
		cfg, err := LoadConfig(envFrom(map[string]string{EnvJobImage: ref}))
		if err != nil {
			t.Fatalf("LoadConfig(%q): %v", ref, err)
		}
		if cfg.JobImage != ref {
			t.Errorf("JobImage = %q, want the verbatim %q", cfg.JobImage, ref)
		}
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{
		EnvJobNamespace:    "agents",
		EnvJobImage:        "registry.example.com/foreman-job:v2",
		EnvJobTokenTTL:     "12h",
		EnvPreferNodeReuse: "false",
		EnvJobDrainTimeout: "90s",
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.JobNamespace != "agents" || cfg.JobImage != "registry.example.com/foreman-job:v2" {
		t.Errorf("namespace/image = %q/%q", cfg.JobNamespace, cfg.JobImage)
	}
	if cfg.JobTokenTTL != 12*time.Hour || cfg.JobDrainTimeout != 90*time.Second || cfg.PreferNodeReuse {
		t.Errorf("ttl/drain/reuse = %s/%s/%v", cfg.JobTokenTTL, cfg.JobDrainTimeout, cfg.PreferNodeReuse)
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	cases := []struct {
		name string
		env  string
		bad  string
	}{
		{"bad token ttl", EnvJobTokenTTL, "abc"},
		{"negative token ttl", EnvJobTokenTTL, "-1h"},
		{"bad reuse flag", EnvPreferNodeReuse, "maybe"},
		{"bad drain bound", EnvJobDrainTimeout, "soon"},
		{"negative drain bound", EnvJobDrainTimeout, "-1s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{tc.env: tc.bad}
			if _, err := LoadConfig(envFrom(env)); err == nil || !strings.Contains(err.Error(), tc.env) {
				t.Errorf("%s=%s: err = %v, want a %s error", tc.env, tc.bad, err, tc.env)
			}
		})
	}
}
