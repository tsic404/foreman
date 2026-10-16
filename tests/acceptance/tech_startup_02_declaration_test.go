// TC-tech-startup-02 / TC-tech-startup-05 exercise the declaration chain with
// the production builder: C1 declares what the startup probe reported entry by
// entry — its runtime type/version, cli_version and failed profiles — while the
// runtime name is Foreman's fixed display name (AC-37), and the
// register-before window answers empty repos without blocking the
// registration (AC-40). The whole path runs here — real jobbuilder (CM
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

// declaredRuntimeName is §5.2's runtime display name: every C1 declaration
// carries it, never the name a probe pod reported for itself (ADR-031).
const declaredRuntimeName = "foreman-job"

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

	// The template environment reports two providers with its own CLI version
	// and one failed profile: all three travel into C1 verbatim.
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
			Repos        []any  `json:"repos"`
			ReposVersion string `json:"repos_version"`
		}
		if err := json.Unmarshal(body, &repos); err != nil {
			t.Errorf("decode S2 body: %v (%s)", err, body)
		}
		if len(repos.Repos) != 0 || repos.ReposVersion != "" {
			t.Errorf("pre-C1 repos = %s, want an empty list and version", body)
		}
		emptyReposSeen = true

		code, body = h.call("POST", "/api/daemon/register", token, []byte(`{
			"daemon_id": "`+job.Name+`",
			"cli_version": "`+cliVersion+`",
			"runtimes": [
				{"name": "foreman-omp", "type": "omp", "version": "`+cliVersion+`"},
				{"name": "foreman-zz", "type": "zz", "version": "3.1.4"}
			],
			"failed_profiles": `+failedProfiles+`
		}`))
		if code != 200 {
			t.Errorf("probe S4 = %d: %s", code, body)
		}
	}

	discovery, err := h.sched.RunStartupProbe(t.Context())
	if err != nil {
		t.Fatalf("RunStartupProbe: %v", err)
	}
	if !emptyReposSeen {
		t.Fatal("the pre-C1 S2 window was never exercised")
	}
	if discovery.CLIVersion != cliVersion || len(discovery.Runtimes) != 2 {
		t.Fatalf("discovery = %+v, want the probe's registration body (N = 2)", discovery)
	}
	if len(discovery.FailedProfiles) != 1 {
		t.Fatalf("discovery failed_profiles = %v, want the probe's entry", discovery.FailedProfiles)
	}

	// C1 declares the probe's discovery verbatim, and its declaration is what
	// the runtime set (and every later C2/C3/C20) reads.
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
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
			Name    string `json:"name"`
			Type    string `json:"type"`
			Version string `json:"version"`
			Status  string `json:"status"`
		} `json:"runtimes"`
	}
	if err := json.Unmarshal(register[0].Body, &body); err != nil {
		t.Fatalf("decode C1 body: %v", err)
	}
	if body.CLIVersion != cliVersion {
		t.Errorf("C1 cli_version = %q, want the probe's %q (the template CLI's truth)", body.CLIVersion, cliVersion)
	}
	// Declared count = discovered count, entry by entry (§1.1 C1).
	if len(body.Runtimes) != len(discovery.Runtimes) {
		t.Fatalf("C1 runtimes = %d, want the discovery's %d", len(body.Runtimes), len(discovery.Runtimes))
	}
	for i, rt := range body.Runtimes {
		want := discovery.Runtimes[i]
		if rt.Type != want.Type || rt.Version != want.Version || rt.Status != "online" {
			t.Errorf("C1 runtime[%d] = %+v, want %+v carried verbatim with status online", i, rt, want)
		}
		// The probe reported its own pod's runtime name ("foreman-omp"); the
		// declaration carries Foreman's fixed display name instead (§1.1 C1
		// 的取值来源与声明域, AC-37, ADR-031).
		if rt.Name != declaredRuntimeName {
			t.Errorf("C1 runtime[%d] name = %q, want the fixed runtime display name %q", i, rt.Name, declaredRuntimeName)
		}
	}
	if len(body.FailedProfiles) != 1 || body.FailedProfiles[0].ProfileID != "pf-1" ||
		body.FailedProfiles[0].CommandName != "foreman-zz" || body.FailedProfiles[0].Reason != "no such binary" {
		t.Errorf("C1 failed_profiles = %+v, want the probe's entry carried verbatim", body.FailedProfiles)
	}
	if got := h.client.RuntimeIDs(); len(got) != 2 || got[0] != "rid-omp" || got[1] != "rid-zz" {
		t.Fatalf("real runtime ids = %v, want both declared runtimes", got)
	}
	// S31's take-off face (§1.2 S31, TC-tech-startup-02): the declaration side
	// carries the fixed name while the startup discovery keeps what the probe
	// reported for its own pod — the two sides differ on `name` and that is the
	// assertion.
	refs := h.client.RuntimeRefs()
	if len(refs) != len(discovery.Runtimes) {
		t.Fatalf("declaration = %+v, want one entry per discovered runtime", refs)
	}
	for i, ref := range refs {
		if ref.Name != declaredRuntimeName {
			t.Errorf("declaration[%d] name = %q, want the fixed runtime display name", i, ref.Name)
		}
		if got := h.client.Declaration().Runtimes[i].Name; got != discovery.Runtimes[i].Name || got == declaredRuntimeName {
			t.Errorf("startup discovery[%d] name = %q, want the probe's own reported name %q", i, got, discovery.Runtimes[i].Name)
		}
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
	job := h.probeJob(t, probes[0])
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

// probeJob returns the created probe Job by name.
func (h *harness) probeJob(t *testing.T, name string) *batchv1Job {
	t.Helper()
	h.jobs.mu.Lock()
	defer h.jobs.mu.Unlock()
	for _, job := range h.jobs.createdJobs {
		if job.Name == name {
			return job
		}
	}
	t.Fatalf("probe Job %s not recorded", name)
	return nil
}
