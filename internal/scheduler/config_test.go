package scheduler

import (
	"testing"
	"time"
)

func envFrom(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(envFrom(nil))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.JobNamespace != "multica-agents" {
		t.Fatalf("JobNamespace = %q", cfg.JobNamespace)
	}
	if cfg.MaxInflightJobs != 100 {
		t.Fatalf("MaxInflightJobs = %d", cfg.MaxInflightJobs)
	}
	if cfg.MaxJobsPerNode != 4 {
		t.Fatalf("MaxJobsPerNode = %d, want contract default 4", cfg.MaxJobsPerNode)
	}
	if cfg.ClaimBatchMax != 32 {
		t.Fatalf("ClaimBatchMax = %d", cfg.ClaimBatchMax)
	}
	if cfg.JobBootTimeout != 300*time.Second {
		t.Fatalf("JobBootTimeout = %v", cfg.JobBootTimeout)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{
		EnvMaxInflightJobs:      "7",
		EnvMaxJobsPerNode:       "2",
		EnvClaimBatchMax:        "5",
		EnvJobBootTimeout:       "90s",
		"FOREMAN_JOB_NAMESPACE": "agents-x",
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.MaxInflightJobs != 7 || cfg.ClaimBatchMax != 5 || cfg.MaxJobsPerNode != 2 ||
		cfg.JobBootTimeout != 90*time.Second || cfg.JobNamespace != "agents-x" {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestLoadConfigClampsClaimBatchToServerCeiling(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{EnvClaimBatchMax: "100"}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.ClaimBatchMax != 32 {
		t.Fatalf("ClaimBatchMax = %d, want clamped 32", cfg.ClaimBatchMax)
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	for _, env := range []map[string]string{
		{EnvMaxInflightJobs: "0"},
		{EnvMaxInflightJobs: "abc"},
		{EnvMaxJobsPerNode: "0"},
		{EnvMaxJobsPerNode: "-1"},
		{EnvMaxJobsPerNode: "4.5"},
		{EnvClaimBatchMax: "-1"},
		{EnvJobBootTimeout: "10"},
		{EnvJobBootTimeout: "-5s"},
	} {
		if _, err := LoadConfig(envFrom(env)); err == nil {
			t.Fatalf("env %v accepted", env)
		}
	}
}
