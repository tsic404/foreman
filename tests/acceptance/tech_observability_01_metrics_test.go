// TC-tech-observability-01 implements AC-10's metric face: the §9.2 families
// exist before any recording, and the policy / refresh families carry the
// designed labels. The semantic half drives production paths — a completed and
// a failed task through the real builder, the startup policy gate, and both
// refresh sequences — so the counters are read from the seams those paths use.
package acceptance

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
)

// TestTechObservability01DesignedFamiliesAtBoot is the presence half: every
// designed series of the four families is exposed with its zero value before
// the first task or refresh, so a scrape at process start already shows the
// vocabulary.
func TestTechObservability01DesignedFamiliesAtBoot(t *testing.T) {
	exposition := scrapeMetrics(t, observability.NewMetrics())
	for _, want := range []string{
		`foreman_policy_hits_total{axis="resources",source="direct"} 0`,
		`foreman_policy_hits_total{axis="resources",source="no_definition"} 0`,
		`foreman_policy_hits_total{axis="resources",source="no_identity"} 0`,
		`foreman_policy_hits_total{axis="capabilities_add",source="direct"} 0`,
		`foreman_policy_hits_total{axis="capabilities_add",source="no_definition"} 0`,
		`foreman_policy_hits_total{axis="capabilities_add",source="no_identity"} 0`,
		`foreman_policy_hits_total{axis="node_selector",source="direct"} 0`,
		`foreman_policy_hits_total{axis="node_selector",source="no_definition"} 0`,
		`foreman_policy_hits_total{axis="node_selector",source="no_identity"} 0`,
		`foreman_policy_name_resolutions_total{conclusion="resolved"} 0`,
		`foreman_policy_name_resolutions_total{conclusion="absent"} 0`,
		`foreman_policy_name_resolutions_total{conclusion="ambiguous"} 0`,
		`foreman_policy_name_resolutions_total{conclusion="unavailable"} 0`,
		`foreman_runtime_profile_refreshes_total{result="success"} 0`,
		`foreman_runtime_profile_refreshes_total{result="unchanged"} 0`,
		`foreman_runtime_profile_refreshes_total{result="failed"} 0`,
		`foreman_workspace_state_refreshes_total{result="success"} 0`,
		`foreman_workspace_state_refreshes_total{result="stale_replay"} 0`,
		`foreman_workspace_state_refreshes_total{result="failed"} 0`,
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("missing designed series %q in the boot exposition", want)
		}
	}
	// Cardinality discipline: the new families must not carry task or Job
	// identifiers (ADR-009's reverse check).
	for _, bad := range []string{"task_id=", "issue_id=", "job_name=", "node_name="} {
		if strings.Contains(exposition, bad) {
			t.Errorf("high-cardinality label %q in the exposition", bad)
		}
	}
}

// observability01Resolver is the platform-side name authority: one conclusion
// per key kind, unavailable for anything unlisted.
type observability01Resolver map[string]jobbuilder.NameConclusion

func (r observability01Resolver) ResolveName(_ context.Context, kind, _ string) (jobbuilder.NameConclusion, error) {
	if conclusion, ok := r[kind]; ok {
		return conclusion, nil
	}
	return jobbuilder.NameUnavailable, nil
}

// TestTechObservability01PolicyFaces drives one completed and one failed task
// through the real builder: the hit counter must follow the policy.applied
// source of each axis, and the startup gate must count one conclusion per key.
func TestTechObservability01PolicyFaces(t *testing.T) {
	policyPath := writePolicyFile(t, `
agents:
  omp:
    resourcePolicy: agent
    resources:
      requests: { cpu: "250m" }
projects:
  proj-x:
    resources:
      requests: { cpu: "500m" }
`)
	h := newHarness(t, false)
	h.useRealBuilderWithPolicy(t, deployedTemplatePath(t), policyPath, func(c *jobbuilder.Config) {
		c.NameResolver = observability01Resolver{
			"agent":   jobbuilder.NameResolved,
			"project": jobbuilder.NameUnavailable,
		}
	})

	// task-1 hits the agent's resource definition; task-2 has no agent
	// identity, so every axis reads no_identity.
	observability01RunTask(t, h, "task-1", "omp", true)
	observability01RunTask(t, h, "task-2", "", false)

	for _, want := range []struct {
		axis   string
		source string
		count  int
	}{
		{"resources", "direct", 1},
		{"resources", "no_identity", 1},
		{"capabilities_add", "no_definition", 1},
		{"capabilities_add", "no_identity", 1},
		{"node_selector", "no_definition", 1},
		{"node_selector", "no_identity", 1},
	} {
		if got := h.metrics.policyHitCount(want.axis, want.source); got != want.count {
			t.Errorf("policy hits axis=%s source=%s = %d, want %d", want.axis, want.source, got, want.count)
		}
	}
	if got := h.metrics.nameResolutionCount(string(jobbuilder.NameResolved)); got != 1 {
		t.Errorf("resolved conclusions = %d, want 1", got)
	}
	if got := h.metrics.nameResolutionCount(string(jobbuilder.NameUnavailable)); got != 1 {
		t.Errorf("unavailable conclusions = %d, want 1", got)
	}
	if got := h.metrics.taskResultCount(string(registry.ResultCompleted)); got != 1 {
		t.Errorf("completed tasks = %d, want 1", got)
	}
	if got := h.metrics.taskResultCount(string(registry.ResultFailed)); got != 1 {
		t.Errorf("failed tasks = %d, want 1", got)
	}

	// The counter and the event must agree on every axis source.
	hit := tcPolicyAxisEvent(t, h, "task-1")
	for field, want := range map[string]string{
		"resource_source":      "direct",
		"cap_source":           "no_definition",
		"node_selector_source": "no_definition",
	} {
		if got := hit[field]; got != want {
			t.Errorf("task-1 policy.applied %s = %v, want %q", field, got, want)
		}
	}
	miss := tcPolicyAxisEvent(t, h, "task-2")
	for _, field := range []string{"resource_source", "cap_source", "node_selector_source"} {
		if got := miss[field]; got != "no_identity" {
			t.Errorf("task-2 policy.applied %s = %v, want no_identity", field, got)
		}
	}
}

// TestTechObservability01NameResolutionRefusals covers the two conclusions
// that refuse startup: the gate records the conclusion before returning, so a
// refused boot is still visible at the metric face.
func TestTechObservability01NameResolutionRefusals(t *testing.T) {
	for _, tc := range []struct {
		name       string
		conclusion jobbuilder.NameConclusion
	}{
		{"absent", jobbuilder.NameAbsent},
		{"ambiguous", jobbuilder.NameAmbiguous},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics := &recordingMetrics{}
			cfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
			if err != nil {
				t.Fatalf("jobbuilder.LoadConfig: %v", err)
			}
			cfg.TemplatePath = deployedTemplatePath(t)
			cfg.PolicyPath = writePolicyFile(t, `
agents:
  omp:
    resourcePolicy: agent
`)
			cfg.NameResolver = observability01Resolver{"agent": tc.conclusion}
			cfg.Metrics = metrics
			cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))

			if _, err := jobbuilder.NewBuilder(cfg); !errors.Is(err, jobbuilder.ErrInvalidPolicy) {
				t.Fatalf("NewBuilder error = %v, want ErrInvalidPolicy", err)
			}
			if got := metrics.nameResolutionCount(string(tc.conclusion)); got != 1 {
				t.Errorf("%s conclusions = %d, want 1", tc.conclusion, got)
			}
		})
	}
}

// TestTechObservability01RefreshFamilies drives both refresh sequences: C-22
// records failed (register-before window, no snapshot), success and
// stale_replay, and §1.6 records unchanged, failed and success.
func TestTechObservability01RefreshFamilies(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.upstream.setRuntimeID("omp", "", "rid-omp")
	h.upstream.setRuntimeID("omp", "prof-a", "rid-prof-a")
	h.upstream.setRegisterWorkspace(`[]`, "v1", `{}`)

	// §1.7's register-before window: the refresh has no snapshot to answer
	// from, which is the designed `failed` outcome of §9.2.
	if err := h.client.RefreshWorkspaceState(t.Context()); err != nil {
		t.Fatalf("pre-register refresh: %v", err)
	}
	h.declare(declaredRuntimes())

	// C-22: a failed refresh replays the last snapshot, a successful one
	// replaces it.
	h.upstream.setRepos(http.StatusInternalServerError, `{"error":"boom"}`, nil)
	if err := h.client.RefreshWorkspaceState(t.Context()); err == nil {
		t.Fatal("a failed C-22 must surface")
	}
	h.upstream.setRepos(http.StatusOK, workspaceBody(`[]`, "v2", json.RawMessage(`{}`)), nil)
	if err := h.client.RefreshWorkspaceState(t.Context()); err != nil {
		t.Fatalf("RefreshWorkspaceState: %v", err)
	}
	if got := workspaceRefreshCount(h, "stale_replay"); got != 1 {
		t.Errorf("stale_replay refreshes = %d, want 1", got)
	}
	if got := workspaceRefreshCount(h, "success"); got != 1 {
		t.Errorf("successful refreshes = %d, want 1", got)
	}
	if got := workspaceRefreshCount(h, "failed"); got != 1 {
		t.Errorf("failed refreshes = %d, want 1", got)
	}

	// §1.6: unchanged projection, failed C-18, then a committed change.
	h.upstream.setProfiles(http.StatusOK, profileListBody())
	if err := h.client.RuntimeProfiles(t.Context()); err != nil {
		t.Fatalf("C18 startup snapshot: %v", err)
	}
	if err := h.client.RefreshRuntimeProfiles(t.Context()); err != nil {
		t.Fatalf("RefreshRuntimeProfiles(unchanged): %v", err)
	}

	h.upstream.setProfiles(http.StatusServiceUnavailable, `{"error":"boom"}`)
	if err := h.client.RefreshRuntimeProfiles(t.Context()); err == nil {
		t.Fatal("a failed C-18 must surface")
	}

	h.upstream.setProfiles(http.StatusOK, profileListBody("prof-a"))
	h.jobs.onCreateJob = func(job *batchv1Job) {
		if job.Labels[registry.LabelRole] != string(registry.RoleCatalogProbe) {
			return
		}
		token := h.builder.probeToken(job.Name)
		body := []byte(`{"daemon_id":"` + job.Name + `","runtimes":[` +
			`{"name":"foreman-omp","type":"omp"},` +
			`{"name":"foreman-prof-a","type":"omp","profile_id":"prof-a"}]}`)
		if code, answer := h.call("POST", "/api/daemon/register", token, body); code != http.StatusOK {
			t.Errorf("probe S-4 = %d: %s", code, answer)
		}
	}
	if err := h.client.RefreshRuntimeProfiles(t.Context()); err != nil {
		t.Fatalf("RefreshRuntimeProfiles(success): %v", err)
	}

	for _, want := range []struct {
		result string
		count  int
	}{
		{"unchanged", 1},
		{"failed", 1},
		{"success", 1},
	} {
		if got := profileRefreshCount(h, want.result); got != want.count {
			t.Errorf("profile refreshes result=%s = %d, want %d", want.result, got, want.count)
		}
	}
}

// observability01RunTask drives one claimed task to a terminal result through
// the daemon face. The claim carries the agent identity the policy axes read.
func observability01RunTask(t *testing.T, h *harness, taskID, agentName string, isSuccess bool) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"id": taskID, "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-zz", "issue_id": "issue-1", "issue_identifier": "TSI-1",
		"agent": map[string]any{"name": agentName},
	})
	if err != nil {
		t.Fatalf("marshal claim: %v", err)
	}
	tcPolicyAxisClaimRaw(t, h, payload)

	token := issueJobToken(t, h, taskID)
	registerTaskDaemon(t, h, taskID, token)
	claimTask(t, h, taskID, token)
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 %s = %d: %s", taskID, code, body)
	}
	if isSuccess {
		if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/complete", token, []byte(`{"exit_code":0}`)); code != http.StatusOK {
			t.Fatalf("S13 %s = %d: %s", taskID, code, body)
		}
	} else {
		if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/fail", token,
			[]byte(`{"error":"boom","failure_reason":"agent_error"}`)); code != http.StatusOK {
			t.Fatalf("S14 %s = %d: %s", taskID, code, body)
		}
	}
	waitTerminal(t, h, taskID)
}

// profileRefreshCount counts the §1.6 outcomes recorded under one result.
func profileRefreshCount(h *harness, result string) int {
	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()
	n := 0
	for _, got := range h.metrics.profileResults {
		if got == result {
			n++
		}
	}
	return n
}
