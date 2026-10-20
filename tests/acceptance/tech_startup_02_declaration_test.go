// TC-tech-startup-02 / TC-tech-startup-05 exercise the declaration chain with
// the production builder: C1 declares exactly what the startup probe reported
// (AC-37), and the register-before window answers empty repos without blocking
// the registration (AC-40). The whole path runs here — real jobbuilder (CM
// absent), real probe Job and credential, real fake server, real scheduler —
// which is also the smoke run for this change (the cluster-level parts of
// AC-37's read-back, e.g. the server-side runtime rows, are registered as
// QA_BLOCKED in the issue).
package acceptance

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestStartupDeclarationComesFromProbeRegistration drives one startup probe
// through the production render path and asserts the three faces of the
// declaration:
//
//	pre-C1 window   → S2 answers empty repos and does not block the registration
//	probe register  → S4 returns the probe's own job runtime
//	C1              → declares the probe's runtimes[] / cli_version verbatim
func TestStartupDeclarationComesFromProbeRegistration(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	h.rebuildScheduler(t, func(c *schedulerConfig) { c.StartupProbeTimeout = 5 * time.Second })

	// The template environment reports two providers (one with a resolvable
	// custom runtime profile) with its own CLI version and one failed profile:
	// all of it travels into C1 verbatim.
	const cliVersion = "0.6.1"
	const failedProfiles = `[{"profile_id":"pf-1","command_name":"foreman-zz","reason":"no such binary"}]`
	emptyReposSeen := false
	h.jobs.onCreateJob = func(job *batchv1Job) {
		token := h.builder.probeToken(job.Name)
		if token == "" {
			t.Errorf("the probe Secret carries no Job Token")
			return
		}
		if !strings.HasPrefix(token, "fmj_") {
			t.Errorf("probe token = %q, want the fmj_ prefix", token)
		}
		// The register-before window: repos are not known yet, and that must
		// not stop the daemon from registering (S2 / AC-40).
		code, body := h.call("GET", "/api/daemon/workspaces/ws-1/repos", token, nil)
		if code != 200 {
			t.Errorf("S2 in the pre-C1 window = %d: %s", code, body)
		}
		var repos struct {
			Repos        []any           `json:"repos"`
			ReposVersion string          `json:"repos_version"`
			Settings     json.RawMessage `json:"settings"`
		}
		if err := json.Unmarshal(body, &repos); err != nil {
			t.Errorf("decode S2 body: %v (%s)", err, body)
		}
		if len(repos.Repos) != 0 || repos.ReposVersion != "" {
			t.Errorf("pre-C1 repos = %s, want an empty list and version", body)
		}
		if repos.Settings != nil {
			t.Errorf("pre-C1 S2 carries settings: %s", body)
		}
		emptyReposSeen = true

		code, body = h.call("POST", "/api/daemon/register", token, []byte(`{
			"daemon_id": "`+job.Name+`",
			"cli_version": "`+cliVersion+`",
			"runtimes": [
				{"name": "foreman-omp", "type": "omp", "version": "`+cliVersion+`"},
				{"name": "foreman-prof", "type": "omp", "version": "0.6.2", "profile_id": "prof-omp"},
				{"name": "foreman-zz", "type": "zz", "version": "3.1.4"}
			],
			"failed_profiles": `+failedProfiles+`
		}`))
		if code != 200 {
			t.Errorf("probe S4 = %d: %s", code, body)
		}
	}

	discoveries, err := h.sched.RunStartupProbe(t.Context())
	if err != nil {
		t.Fatalf("RunStartupProbe: %v", err)
	}
	discovery, ok := discoveries[h.schedCfg.WorkspaceID]
	if !ok {
		t.Fatalf("discoveries = %+v, want an entry for workspace %q", discoveries, h.schedCfg.WorkspaceID)
	}
	if !emptyReposSeen {
		t.Fatal("the pre-C1 S2 window was never exercised")
	}
	if discovery.CLIVersion != cliVersion || len(discovery.Runtimes) != 3 {
		t.Fatalf("discovery = %+v, want the probe's registration body (N = 3)", discovery)
	}
	if len(discovery.FailedProfiles) != 1 {
		t.Fatalf("discovery failed_profiles = %v, want the probe's entry", discovery.FailedProfiles)
	}

	// C1 declares the probe's discovery verbatim, and its declaration is what
	// the runtime set (and every later C2/C3/C20) reads.
	h.upstream.setRuntimeID("omp", "", "rid-omp")
	h.upstream.setRuntimeID("omp", "prof-omp", "rid-omp-prof")
	h.upstream.setRuntimeID("zz", "", "rid-zz")
	// C-1 seeds the workspace state carrier; S-2 replays it verbatim (§1.7).
	seedRepos := `[{"url":"https://example.invalid/r","x_repo_key":{"n":1}}]`
	seedSettings := `{"github_enabled":false,"co_authored_by_enabled":true}`
	h.upstream.setRegisterWorkspace(seedRepos, "seed-v", seedSettings)
	h.declare(discovery)
	register := h.upstream.calls("POST", "/api/daemon/register")
	if len(register) != 1 {
		t.Fatalf("C1 calls = %d, want 1", len(register))
	}
	var body struct {
		CLIVersion     string `json:"cli_version"`
		FailedProfiles []struct {
			ProfileID   string `json:"profile_id"`
			CommandName string `json:"command_name"`
			Reason      string `json:"reason"`
		} `json:"failed_profiles"`
		Runtimes []struct {
			Name      string `json:"name"`
			Type      string `json:"type"`
			Version   string `json:"version"`
			Status    string `json:"status"`
			ProfileID string `json:"profile_id"`
		} `json:"runtimes"`
	}
	if err := json.Unmarshal(register[0].Body, &body); err != nil {
		t.Fatalf("decode C1 body: %v", err)
	}
	if body.CLIVersion != cliVersion {
		t.Errorf("C1 cli_version = %q, want the probe's %q (the template CLI's truth)", body.CLIVersion, cliVersion)
	}
	// Declared count = discovered count, entry by entry (C-1).
	if len(body.Runtimes) != len(discovery.Runtimes) {
		t.Fatalf("C1 runtimes = %d, want the discovery's %d", len(body.Runtimes), len(discovery.Runtimes))
	}
	for i, rt := range body.Runtimes {
		want := discovery.Runtimes[i]
		if rt.Type != want.Type || rt.Version != want.Version || rt.ProfileID != want.ProfileID || rt.Status != "online" {
			t.Errorf("C1 runtime[%d] = %+v, want %+v carried verbatim (with profile_id) and status online", i, rt, want)
		}
	}
	if len(body.FailedProfiles) != 1 || body.FailedProfiles[0].ProfileID != "pf-1" ||
		body.FailedProfiles[0].CommandName != "foreman-zz" || body.FailedProfiles[0].Reason != "no such binary" {
		t.Errorf("C1 failed_profiles = %+v, want the probe's entry carried verbatim", body.FailedProfiles)
	}
	if got := h.client.RuntimeIDs(); len(got) != 3 || got[0] != "rid-omp" || got[1] != "rid-omp-prof" || got[2] != "rid-zz" {
		t.Fatalf("real runtime ids = %v, want all declared runtimes", got)
	}
	if got := h.client.CLIVersion(); got != cliVersion {
		t.Errorf("X-Client-Version after C1 = %q, want the declaration's cli_version", got)
	}
	// AC-37's request-header face: the C1 request itself is already past the
	// probe's registration, so it carries the template CLI's version, not
	// Foreman's own.
	if got := register[0].Header.Get("X-Client-Version"); got != cliVersion {
		t.Errorf("C1 X-Client-Version = %q, want the template CLI's %q", got, cliVersion)
	}

	// AC-37's workspace-state leg: S-2 replays the C-1 seed (settings and
	// repos verbatim) once the declaration is in.
	h.jobs.onCreateJob = nil
	payload := json.RawMessage(`{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1"
	}`)
	if err := h.sched.OnClaim(t.Context(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := taskToken(t, h, "task-1")
	code, s2 := h.call("GET", "/api/daemon/workspaces/ws-1/repos", token, nil)
	if code != 200 {
		t.Fatalf("S-2 after C-1 = %d: %s", code, s2)
	}
	var wsState struct {
		Repos    json.RawMessage `json:"repos"`
		Settings json.RawMessage `json:"settings"`
	}
	if err := json.Unmarshal(s2, &wsState); err != nil {
		t.Fatalf("decode S-2: %v (%s)", err, s2)
	}
	if string(wsState.Settings) != seedSettings {
		t.Errorf("S-2 settings = %s, want the C-1 seed %s", wsState.Settings, seedSettings)
	}
	if string(wsState.Repos) != seedRepos {
		t.Errorf("S-2 repos = %s, want the C-1 seed %s", wsState.Repos, seedRepos)
	}

	// A heartbeat now runs against the declared runtime (per-runtime stream).
	ack, err := h.client.Heartbeat(t.Context(), "rid-omp")
	if err != nil || ack.Status != "ok" {
		t.Fatalf("heartbeat after C1 = %+v, %v", ack, err)
	}
}

// TestStartupProbeRenderIsProbeShaped guards the probe Job the production
// builder hands to Kubernetes: probe identity, no task face, and the two
// literal substitutions that give the probe its own credential and profile.
func TestStartupProbeRenderIsProbeShaped(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	h.jobs.onCreateJob = func(job *batchv1Job) {
		token := h.builder.probeToken(job.Name)
		h.call("POST", "/api/daemon/register", token, []byte(`{
			"daemon_id": "`+job.Name+`",
			"cli_version": "0.6.1",
			"runtimes": [{"name": "foreman-omp", "type": "omp", "version": "0.6.1"}]
		}`))
	}
	if _, err := h.sched.RunStartupProbe(t.Context()); err != nil {
		t.Fatalf("RunStartupProbe: %v", err)
	}

	probes := h.jobs.probeJobsCreated()
	if len(probes) != 1 {
		t.Fatalf("probe Jobs = %v, want exactly one", probes)
	}
	job := h.createdJob(t, probes[0])
	if job.Labels["foreman.tsic.top/role"] != "catalog-probe" {
		t.Errorf("probe labels = %v", job.Labels)
	}
	if _, hasTask := job.Labels["foreman.tsic.top/task-id"]; hasTask {
		t.Error("a probe Job must never carry a task-id label")
	}
	if len(job.Annotations) != 0 {
		t.Errorf("probe annotations = %v, want none", job.Annotations)
	}
	if len(job.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("probe Job has no containers")
	}
	container := job.Spec.Template.Spec.Containers[0]
	if got := strings.Join(container.Args, " "); !strings.Contains(got, "--profile foreman-catalog") {
		t.Errorf("probe args = %q, want the probe profile", got)
	}
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds <= 0 {
		t.Error("probe Job must carry its budget in activeDeadlineSeconds")
	}
	foundHeartbeat, foundServerURL := false, false
	for _, env := range container.Env {
		switch env.Name {
		case "MULTICA_DAEMON_HEARTBEAT_INTERVAL":
			foundHeartbeat = env.Value == "15s"
		case "MULTICA_SERVER_URL":
			foundServerURL = env.Value != ""
		}
	}
	if !foundHeartbeat {
		t.Error("probe env must pin the heartbeat interval to 15s")
	}
	if !foundServerURL {
		t.Error("probe env must point the daemon at Foreman")
	}
}
