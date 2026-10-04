package jobbuilder

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

func envFrom(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{EnvJobImageDigest: testDigest}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.JobNamespace != DefaultJobNamespace {
		t.Errorf("JobNamespace = %q, want %q", cfg.JobNamespace, DefaultJobNamespace)
	}
	if cfg.JobImage != DefaultJobImage {
		t.Errorf("JobImage = %q, want %q", cfg.JobImage, DefaultJobImage)
	}
	if cfg.JobImageDigest != testDigest {
		t.Errorf("JobImageDigest = %q", cfg.JobImageDigest)
	}
	if len(cfg.ImagePullSecrets) != 1 || cfg.ImagePullSecrets[0] != "registry-tsic" {
		t.Errorf("ImagePullSecrets = %v", cfg.ImagePullSecrets)
	}
	if cfg.JobTokenTTL != DefaultJobTokenTTL {
		t.Errorf("JobTokenTTL = %s, want %s", cfg.JobTokenTTL, DefaultJobTokenTTL)
	}
	if cfg.TaskMaxDuration != DefaultTaskMaxDuration {
		t.Errorf("TaskMaxDuration = %s, want %s", cfg.TaskMaxDuration, DefaultTaskMaxDuration)
	}
	if cfg.JobTTLSeconds != DefaultJobTTLSeconds {
		t.Errorf("JobTTLSeconds = %d, want %d", cfg.JobTTLSeconds, DefaultJobTTLSeconds)
	}
	if cfg.StateRoot != DefaultStateRoot {
		t.Errorf("StateRoot = %q, want %q", cfg.StateRoot, DefaultStateRoot)
	}
	if cfg.RepoCacheMode != CacheModeShared {
		t.Errorf("RepoCacheMode = %q, want %q", cfg.RepoCacheMode, CacheModeShared)
	}
	if !cfg.PreferNodeReuse {
		t.Error("PreferNodeReuse must default to true")
	}
	if cfg.CPURequest.String() != "1" || cfg.MemoryRequest.String() != "2Gi" ||
		cfg.CPULimit.String() != "4" || cfg.MemoryLimit.String() != "8Gi" {
		t.Errorf("resources = %s/%s %s/%s", cfg.CPURequest.String(), cfg.MemoryRequest.String(), cfg.CPULimit.String(), cfg.MemoryLimit.String())
	}
	if len(cfg.NodeSelector) != 1 || cfg.NodeSelector["kubernetes.io/os"] != "linux" {
		t.Errorf("NodeSelector = %v", cfg.NodeSelector)
	}
	if len(cfg.Tolerations) != 2 || cfg.Tolerations[0].Key != "unreachable" || cfg.Tolerations[1].Key != "maybe_unreachable" {
		t.Errorf("Tolerations = %v", cfg.Tolerations)
	}
}

func TestLoadConfigRequiresDigest(t *testing.T) {
	if _, err := LoadConfig(envFrom(map[string]string{})); err == nil || !strings.Contains(err.Error(), EnvJobImageDigest) {
		t.Fatalf("err = %v, want a %s error", err, EnvJobImageDigest)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{
		EnvJobImageDigest:      testDigest,
		EnvJobNamespace:        "agents",
		EnvJobImage:            "registry.example.com/foreman-job:v2",
		EnvJobImagePullSecrets: `["a","b"]`,
		EnvJobTokenTTL:         "12h",
		EnvTaskMaxDuration:     "3600",
		EnvJobTTLSeconds:       "300",
		EnvStateRoot:           "/data/foreman",
		EnvRepoCacheMode:       CacheModeIsolated,
		EnvPreferNodeReuse:     "false",
		EnvJobCPURequest:       "500m",
		EnvJobMemoryRequest:    "1Gi",
		EnvJobCPULimit:         "2",
		EnvJobMemoryLimit:      "4Gi",
		EnvJobNodeSelector:     `{"kubernetes.io/os":"linux","pool":"agents"}`,
		EnvJobTolerations:      `[{"key":"dedicated","operator":"Equal","value":"agents","effect":"NoSchedule"}]`,
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.JobNamespace != "agents" || cfg.JobImage != "registry.example.com/foreman-job:v2" {
		t.Errorf("namespace/image = %q/%q", cfg.JobNamespace, cfg.JobImage)
	}
	if len(cfg.ImagePullSecrets) != 2 || cfg.ImagePullSecrets[1] != "b" {
		t.Errorf("ImagePullSecrets = %v", cfg.ImagePullSecrets)
	}
	if cfg.JobTokenTTL != 12*time.Hour || cfg.TaskMaxDuration != time.Hour || cfg.JobTTLSeconds != 300 {
		t.Errorf("durations = %s/%s ttl %d", cfg.JobTokenTTL, cfg.TaskMaxDuration, cfg.JobTTLSeconds)
	}
	if cfg.StateRoot != "/data/foreman" || cfg.RepoCacheMode != CacheModeIsolated || cfg.PreferNodeReuse {
		t.Errorf("state/mode/reuse = %q/%q/%v", cfg.StateRoot, cfg.RepoCacheMode, cfg.PreferNodeReuse)
	}
	if cfg.CPURequest.String() != "500m" || cfg.MemoryRequest.String() != "1Gi" ||
		cfg.CPULimit.String() != "2" || cfg.MemoryLimit.String() != "4Gi" {
		t.Errorf("resources = %s/%s %s/%s", cfg.CPURequest.String(), cfg.MemoryRequest.String(), cfg.CPULimit.String(), cfg.MemoryLimit.String())
	}
	if len(cfg.NodeSelector) != 2 || cfg.NodeSelector["pool"] != "agents" {
		t.Errorf("NodeSelector = %v", cfg.NodeSelector)
	}
	if len(cfg.Tolerations) != 1 || cfg.Tolerations[0].Key != "dedicated" || cfg.Tolerations[0].Operator != corev1.TolerationOpEqual {
		t.Errorf("Tolerations = %v", cfg.Tolerations)
	}
}

func TestLoadConfigRejectsOverflow(t *testing.T) {
	// Boundary values: one past the limit must fail, the limit itself must pass.
	cases := []struct {
		env  string
		pass string
		fail string
	}{
		{EnvTaskMaxDuration, "9223372036", "9223372037"}, // math.MaxInt64 / int64(time.Second)
		{EnvJobTTLSeconds, "2147483647", "2147483648"},   // math.MaxInt32
	}
	for _, tc := range cases {
		env := map[string]string{EnvJobImageDigest: testDigest, tc.env: tc.fail}
		if _, err := LoadConfig(envFrom(env)); err == nil || !strings.Contains(err.Error(), tc.env) {
			t.Errorf("%s=%s: err = %v, want a %s error", tc.env, tc.fail, err, tc.env)
		}
		env[tc.env] = tc.pass
		if _, err := LoadConfig(envFrom(env)); err != nil {
			t.Errorf("%s=%s: unexpected error %v", tc.env, tc.pass, err)
		}
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want string
	}{
		{"bad digest", EnvJobImageDigest, EnvJobImageDigest},
		{"bad token ttl", EnvJobTokenTTL, EnvJobTokenTTL},
		{"bad max duration", EnvTaskMaxDuration, EnvTaskMaxDuration},
		{"bad job ttl", EnvJobTTLSeconds, EnvJobTTLSeconds},
		{"bad cache mode", EnvRepoCacheMode, EnvRepoCacheMode},
		{"bad reuse flag", EnvPreferNodeReuse, EnvPreferNodeReuse},
		{"bad cpu request", EnvJobCPURequest, EnvJobCPURequest},
		{"bad node selector", EnvJobNodeSelector, EnvJobNodeSelector},
		{"bad tolerations", EnvJobTolerations, EnvJobTolerations},
	}
	bad := map[string]string{
		EnvJobImageDigest:  "sha256:xyz",
		EnvJobTokenTTL:     "abc",
		EnvTaskMaxDuration: "-1",
		EnvJobTTLSeconds:   "x",
		EnvRepoCacheMode:   "weird",
		EnvPreferNodeReuse: "maybe",
		EnvJobCPURequest:   "lots",
		EnvJobNodeSelector: `{`,
		EnvJobTolerations:  `[`,
	}
	for _, tc := range cases {
		env := map[string]string{EnvJobImageDigest: testDigest, tc.env: bad[tc.env]}
		if _, err := LoadConfig(envFrom(env)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want a %s error", tc.name, err, tc.want)
		}
	}
}
