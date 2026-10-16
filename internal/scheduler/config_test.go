package scheduler

import (
	"maps"
	"testing"
	"time"
)

func envFrom(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}

// testWorkspaceID is the workspace every scheduler test binds its probe Job
// Token to; LoadConfig refuses to build without one.
const testWorkspaceID = "ws-test"

// testEnv is envFrom plus the env keys LoadConfig requires: a test that is not
// about a missing key supplies this one instead.
func testEnv(env map[string]string) func(string) string {
	if _, ok := env[EnvWorkspaceID]; ok {
		return envFrom(env)
	}
	merged := make(map[string]string, len(env)+1)
	maps.Copy(merged, env)
	merged[EnvWorkspaceID] = testWorkspaceID
	return envFrom(merged)
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(testEnv(nil))
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
	if cfg.JobDrainTimeout != 120*time.Second {
		t.Fatalf("JobDrainTimeout = %v, want contract default 120s", cfg.JobDrainTimeout)
	}
	if cfg.WorkspaceID != testWorkspaceID {
		t.Fatalf("WorkspaceID = %q, want %q", cfg.WorkspaceID, testWorkspaceID)
	}
	// The startup window defaults to the task Job's boot bound: one
	// "schedule + pull + register" magnitude, no second magic number.
	if cfg.StartupProbeTimeout != cfg.JobBootTimeout {
		t.Fatalf("StartupProbeTimeout = %v, want the boot bound %v", cfg.StartupProbeTimeout, cfg.JobBootTimeout)
	}
	if !cfg.ModelCatalogEnabled {
		t.Fatal("ModelCatalogEnabled = false, want the catalog route on by default")
	}
	if cfg.ModelCatalogTimeout != 45*time.Second {
		t.Fatalf("ModelCatalogTimeout = %v, want 45s", cfg.ModelCatalogTimeout)
	}
	if cfg.ReconcileInterval != 30*time.Second {
		t.Fatalf("ReconcileInterval = %v, want 30s", cfg.ReconcileInterval)
	}
}

// MULTICA_WORKSPACE_ID binds the probe Job Token (the catalog probe carries no
// task), so a configuration without one is refused at startup.
func TestLoadConfigRequiresWorkspaceID(t *testing.T) {
	if _, err := LoadConfig(envFrom(map[string]string{})); err == nil {
		t.Fatal("LoadConfig without a workspace id: want an error")
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := LoadConfig(testEnv(map[string]string{
		EnvMaxInflightJobs:      "7",
		EnvMaxJobsPerNode:       "2",
		EnvClaimBatchMax:        "5",
		EnvJobBootTimeout:       "90s",
		EnvJobDrainTimeout:      "0",
		"FOREMAN_JOB_NAMESPACE": "agents-x",
		EnvWorkspaceID:          "ws-x",
		EnvStartupProbeTimeout:  "2m",
		EnvModelCatalogEnabled:  "false",
		EnvModelCatalogTimeout:  "10s",
		EnvReconcileInterval:    "5s",
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.MaxInflightJobs != 7 || cfg.ClaimBatchMax != 5 || cfg.MaxJobsPerNode != 2 ||
		cfg.JobBootTimeout != 90*time.Second || cfg.JobDrainTimeout != 0 || cfg.JobNamespace != "agents-x" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.WorkspaceID != "ws-x" || cfg.StartupProbeTimeout != 2*time.Minute ||
		cfg.ModelCatalogEnabled || cfg.ModelCatalogTimeout != 10*time.Second ||
		cfg.ReconcileInterval != 5*time.Second {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestLoadConfigClampsClaimBatchToServerCeiling(t *testing.T) {
	cfg, err := LoadConfig(testEnv(map[string]string{EnvClaimBatchMax: "100"}))
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
		// Zero is legal here (reclaim on the next observation point); a
		// negative bound is not.
		{EnvJobDrainTimeout: "-1s"},
		{EnvStartupProbeTimeout: "10"},
		{EnvStartupProbeTimeout: "0s"},
		{EnvModelCatalogEnabled: "yes"},
		{EnvModelCatalogTimeout: "0s"},
		{EnvReconcileInterval: "-1s"},
	} {
		if _, err := LoadConfig(testEnv(env)); err == nil {
			t.Fatalf("env %v accepted", env)
		}
	}
}
