package scheduler

import (
	"maps"
	"strings"
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
// The workspace identity is not a scheduler-config requirement: the
// user-scoped credential form discovers its set at startup and fills it in
// (ADR-049). workspace.LoadDeployment is the deployment-shape authority.
func TestLoadConfigAcceptsDiscoveryForm(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{}))
	if err != nil {
		t.Fatalf("LoadConfig without a workspace id: %v", err)
	}
	if cfg.WorkspaceID != "" || len(cfg.Workspaces) != 0 {
		t.Errorf("config = %+v, want the discovery form to stay unset until startup", cfg)
	}
}

// FOREMAN_JOB_BOOT_TIMEOUT=0 switches the upper bound off: the config
// accepts it, and the startup probe window — which otherwise mirrors the boot
// bound — falls back to the default magnitude rather than a zero window that
// would fail the first probe before it could register.
func TestLoadConfigAcceptsZeroBootTimeout(t *testing.T) {
	cfg, err := LoadConfig(testEnv(map[string]string{EnvJobBootTimeout: "0"}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.JobBootTimeout != 0 {
		t.Fatalf("JobBootTimeout = %v, want the bound switched off", cfg.JobBootTimeout)
	}
	if cfg.StartupProbeTimeout != DefaultJobBootTimeout {
		t.Fatalf("StartupProbeTimeout = %v, want the default %v", cfg.StartupProbeTimeout, DefaultJobBootTimeout)
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

// TestLoadConfigPerWorkspaceCapDomain pins the per-workspace cap's value
// domain (AC-101): absent leaves the level off, a positive value at or below
// the global cap arms it, and one above the global cap is refused with both
// figures named.
func TestLoadConfigPerWorkspaceCapDomain(t *testing.T) {
	cfg, err := LoadConfig(testEnv(nil))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.MaxInflightJobsPerWorkspace != 0 {
		t.Fatalf("MaxInflightJobsPerWorkspace = %d with the key unset, want 0 (level off)",
			cfg.MaxInflightJobsPerWorkspace)
	}

	// The boundary is legal: equal to the global cap stays on.
	cfg, err = LoadConfig(testEnv(map[string]string{
		EnvMaxInflightJobs:         "5",
		EnvMaxInflightPerWorkspace: "5",
	}))
	if err != nil {
		t.Fatalf("a per-workspace cap equal to the global cap was refused: %v", err)
	}
	if cfg.MaxInflightJobs != 5 || cfg.MaxInflightJobsPerWorkspace != 5 {
		t.Fatalf("caps = %d/%d, want 5/5", cfg.MaxInflightJobs, cfg.MaxInflightJobsPerWorkspace)
	}

	_, err = LoadConfig(testEnv(map[string]string{
		EnvMaxInflightJobs:         "5",
		EnvMaxInflightPerWorkspace: "6",
	}))
	if err == nil {
		t.Fatal("a per-workspace cap above the global cap was accepted")
	}
	for _, want := range []string{EnvMaxInflightPerWorkspace, "(6)", EnvMaxInflightJobs, "(5)", "must not exceed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
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

// TestLoadConfigDeliveryConfirmWindow guards §5's window semantics
// (ADR-032): 30s by default, an explicit lower value accepted, and 0 legal —
// it disarms the convergence exit.
func TestLoadConfigDeliveryConfirmWindow(t *testing.T) {
	cfg, err := LoadConfig(testEnv(nil))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.DeliveryConfirmTimeout != DefaultDeliveryConfirmTimeout {
		t.Fatalf("DeliveryConfirmTimeout = %v, want %v", cfg.DeliveryConfirmTimeout, DefaultDeliveryConfirmTimeout)
	}

	cfg, err = LoadConfig(testEnv(map[string]string{EnvDeliveryConfirmTimeout: "5s"}))
	if err != nil || cfg.DeliveryConfirmTimeout != 5*time.Second {
		t.Fatalf("explicit window = %v (%v), want 5s", cfg.DeliveryConfirmTimeout, err)
	}

	cfg, err = LoadConfig(testEnv(map[string]string{EnvDeliveryConfirmTimeout: "0"}))
	if err != nil || cfg.DeliveryConfirmTimeout != 0 {
		t.Fatalf("window 0 = %v (%v), want the disabled form", cfg.DeliveryConfirmTimeout, err)
	}

	// The ordering premise is checked against the configured boot bound, not
	// the default one.
	if _, err := LoadConfig(testEnv(map[string]string{
		EnvJobBootTimeout: "10s", EnvDeliveryConfirmTimeout: "20s",
	})); err == nil {
		t.Fatal("a window above a shortened boot bound must be refused")
	}
	if _, err := LoadConfig(testEnv(map[string]string{
		EnvJobBootTimeout: "10s", EnvDeliveryConfirmTimeout: "5s",
	})); err != nil {
		t.Fatalf("a window below the configured boot bound must be accepted: %v", err)
	}

	// A boot bound switched off (0 = no upper bound) has nothing for the
	// window to stay under: any positive value is accepted, including the
	// 30s the unset key already defaults to, and the bound stays off.
	for _, raw := range []string{"30s", "10m"} {
		cfg, err := LoadConfig(testEnv(map[string]string{
			EnvJobBootTimeout: "0", EnvDeliveryConfirmTimeout: raw,
		}))
		if err != nil {
			t.Fatalf("window %s under a disabled boot bound: %v", raw, err)
		}
		want, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("parse %q: %v", raw, err)
		}
		if cfg.JobBootTimeout != 0 || cfg.DeliveryConfirmTimeout != want {
			t.Fatalf("window %s under a disabled boot bound = %v, boot = %v; want %v and 0",
				raw, cfg.DeliveryConfirmTimeout, cfg.JobBootTimeout, want)
		}
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	for _, env := range []map[string]string{
		{EnvMaxInflightJobs: "0"},
		{EnvMaxInflightJobs: "abc"},
		// The per-workspace cap is a positive integer or absent; zero means
		// "level off" only as the absent form, never as a written value.
		{EnvMaxInflightPerWorkspace: "0"},
		{EnvMaxInflightPerWorkspace: "-1"},
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
		// The delivery confirmation window has to fire before the boot bound
		// (ADR-032): a negative one is meaningless, one at the bound is the
		// form the window exists to replace.
		{EnvDeliveryConfirmTimeout: "-1s"},
		{EnvDeliveryConfirmTimeout: "300s"},
		{EnvDeliveryConfirmTimeout: "10m"},
	} {
		if _, err := LoadConfig(testEnv(env)); err == nil {
			t.Fatalf("env %v accepted", env)
		}
	}
}
