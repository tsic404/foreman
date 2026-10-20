// TC-tech-observability-01 implements AC-10's metric face: every §9.2 family
// is exposed before any recording, the task-driven counters and labels are
// read after one completed and one failed task, and the policy / refresh
// families carry the designed labels. The task half drives the production
// paths — the real builder, the daemon face, the scheduler's terminal
// settlement and the production forward client — so the counters are read
// from the seams those paths use.
package acceptance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// observability01Families is the §9.2 metric surface. Every family must be
// visible from process start (ADR-009's confirmation: a scrape greps the
// whole designed vocabulary), including the families whose series are only
// materialised by a recorder.
var observability01Families = []string{
	"foreman_tasks_total",
	"foreman_task_claims_total",
	"foreman_claim_errors_total",
	"foreman_job_create_seconds",
	"foreman_job_boot_seconds",
	"foreman_inflight_jobs",
	"foreman_forward_total",
	"foreman_forward_latency_seconds",
	"foreman_auth_failures_total",
	"foreman_token_usage_total",
	"foreman_duplicate_dispatch_total",
	"foreman_pending_reports",
	"foreman_ws_connected",
	"foreman_ws_reconnects_total",
	"foreman_heartbeat_suspect",
	"foreman_delivery_claims_total",
	"foreman_delivery_latency_seconds",
	"foreman_delivery_unconfirmed_total",
	"foreman_cred_reclaim_total",
	"foreman_job_drain_seconds",
	"foreman_job_delete_failed_total",
	"foreman_model_catalog_relays_total",
	"foreman_model_catalog_seconds",
	"foreman_skill_cache_resolves_total",
	"foreman_skill_cache_entries",
	"foreman_skill_cache_bytes",
	"foreman_skill_cache_evictions_total",
	"foreman_workspace_state_refreshes_total",
	"foreman_runtime_profile_refreshes_total",
	"foreman_policy_hits_total",
	"foreman_policy_name_resolutions_total",
}

// TestTechObservability01DesignedFamiliesAtBoot is the presence half: every
// designed §9.2 family is exposed before the first task or refresh, so a
// scrape at process start already shows the vocabulary.
func TestTechObservability01DesignedFamiliesAtBoot(t *testing.T) {
	exposition := scrapeMetrics(t, observability.NewMetrics())
	for _, family := range observability01Families {
		if !strings.Contains(exposition, family) {
			t.Errorf("missing designed family %q in the boot exposition", family)
		}
	}
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

func (r observability01Resolver) ResolveName(_ context.Context, _, kind, name string) (jobbuilder.NameResolution, error) {
	conclusion, ok := r[kind]
	if !ok {
		conclusion = jobbuilder.NameUnavailable
	}
	resolution := jobbuilder.NameResolution{Conclusion: conclusion}
	if conclusion == jobbuilder.NameResolved && kind == jobbuilder.PolicyKeyProject {
		resolution.Identifier = projectIDFor(name)
	}
	return resolution, nil
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

// TestTechObservability01TaskFaces is the semantic half of AC-10: one
// completed and one failed task run through the daemon face, then the
// task-driven §9.2 families are read. The scheduler forwards through the
// production proxy client, so the forward counter is the one the deployment
// records, not a test stand-in.
func TestTechObservability01TaskFaces(t *testing.T) {
	h := newHarnessWithRealServer(t, false)
	observability01RunTask(t, h, "task-1", "", true)
	observability01RunTask(t, h, "task-2", "", false)

	// The Job-termination face: both verdicts delete their Job, and the
	// reclaim that follows is the job_terminated branch (ADR-017).
	for _, taskID := range []string{"task-1", "task-2"} {
		if err := h.sched.OnJobTerminated(t.Context(), "fm-"+taskID,
			scheduler.JobTerminationReasonJobTerminated, time.Now(), 0); err != nil {
			t.Fatalf("OnJobTerminated(%s): %v", taskID, err)
		}
	}

	for _, want := range []struct {
		name string
		got  int
		want int
	}{
		{"foreman_task_claims_total", h.metrics.taskClaimCount(), 2},
		{"foreman_job_create_seconds samples", h.metrics.jobCreateSampleCount(), 2},
		{"foreman_job_boot_seconds samples", h.metrics.jobBootSampleCount(), 2},
		{"foreman_job_drain_seconds samples", h.metrics.drainSampleCount(), 2},
		{"foreman_claim_errors_total", h.metrics.claimErrorCount(), 0},
		{"foreman_duplicate_dispatch_total", h.metrics.duplicateDispatchCount(), 0},
		{"foreman_pending_reports", h.metrics.pendingReportsDepth(), 0},
		{"foreman_auth_failures_total", h.metrics.authFailureTotal(), 0},
		{"foreman_job_delete_failed_total", h.metrics.jobDeleteFailureCount(), 0},
	} {
		if want.got != want.want {
			t.Errorf("%s = %d, want %d", want.name, want.got, want.want)
		}
	}

	if got := h.metrics.taskResultCount(string(registry.ResultCompleted)); got != 1 {
		t.Errorf("foreman_tasks_total{result=completed} = %d, want 1", got)
	}
	if got := h.metrics.taskResultCount(string(registry.ResultFailed)); got != 1 {
		t.Errorf("foreman_tasks_total{result=failed} = %d, want 1", got)
	}
	if got := h.metrics.inflightLast(); got != 0 {
		t.Errorf("foreman_inflight_jobs = %d after both terminals, want 0", got)
	}
	if !h.metrics.inflightSaw(1) {
		t.Error("foreman_inflight_jobs never read 1 while a task was in flight")
	}
	if got := h.metrics.credReclaimCount(scheduler.JobTerminationReasonJobTerminated); got != 2 {
		t.Errorf("foreman_cred_reclaim_total{reason=job_terminated} = %d, want 2", got)
	}
	if got := h.metrics.credReclaimCount(scheduler.JobTerminationReasonDrainTimeout); got != 0 {
		t.Errorf("foreman_cred_reclaim_total{reason=drain_timeout} = %d, want 0", got)
	}
	if got := h.metrics.deliveryClaimCount(proxy.DeliveryClaimDelivered); got != 2 {
		t.Errorf("foreman_delivery_claims_total{result=delivered} = %d, want 2", got)
	}
	for _, want := range []struct {
		endpoint string
		count    int
	}{
		{"start", 2},
		{"complete", 1},
		{"fail", 1},
	} {
		if got := h.metrics.forwardCount(want.endpoint, http.StatusOK); got != want.count {
			t.Errorf("foreman_forward_total{endpoint=%s,code=200} = %d, want %d",
				want.endpoint, got, want.count)
		}
	}
	for _, want := range []struct {
		provider   string
		model      string
		input      int64
		output     int64
		cacheRead  int64
		cacheWrite int64
	}{
		{"omp", "kimi", 100, 50, 30, 20},
		{"zz", "glm", 200, 25, 40, 10},
	} {
		input, output, cacheRead, cacheWrite := h.metrics.tokenUsageFor(want.provider, want.model)
		for _, direction := range []struct {
			name string
			got  int64
			want int64
		}{
			{"input", input, want.input},
			{"output", output, want.output},
			{"cache_read", cacheRead, want.cacheRead},
			{"cache_write", cacheWrite, want.cacheWrite},
		} {
			if direction.got != direction.want {
				t.Errorf("foreman_token_usage_total{provider=%q,model=%q,direction=%s} = %d, want %d",
					want.provider, want.model, direction.name, direction.got, direction.want)
			}
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

// TestTechObservability01UsageLabelSpaceBounded: the S-12 body is the Job
// daemon's own report, so its provider and model strings are task-controlled.
// The two bounds are judged separately, and the counts stay complete under both.
func TestTechObservability01UsageLabelSpaceBounded(t *testing.T) {
	// The ceilings are the acceptance face's bounds, not the implementation's
	// limits: the metric may admit fewer, never more.
	const (
		usageLabelCeiling = 2048
		usageLabelBytes   = 128
		seriesEntries     = 4096
	)

	// postUsage drives one claimed task to S-12 with body and returns the
	// harness whose seam recorded the outcome.
	postUsage := func(t *testing.T, body []byte) *harness {
		t.Helper()
		h := newHarnessWithRealServer(t, false)
		const taskID = "task-usage-labels"
		payload, err := json.Marshal(map[string]any{
			"id": taskID, "agent_id": "agent-1", "workspace_id": "ws-1",
			"runtime_id": "rid-zz", "issue_id": "issue-1", "issue_identifier": "TSI-1",
			"agent": map[string]any{"name": "agent-a"},
		})
		if err != nil {
			t.Fatalf("marshal claim: %v", err)
		}
		tcPolicyAxisClaimRaw(t, h, payload)
		token := issueJobToken(t, h, taskID)
		registerTaskDaemon(t, h, taskID, token)
		claimTask(t, h, taskID, token)
		if code, answer := h.call("POST", "/api/daemon/tasks/"+taskID+"/start", token, []byte(`{}`)); code != http.StatusOK {
			t.Fatalf("S8 = %d: %s", code, answer)
		}
		if code, answer := h.call("POST", "/api/daemon/tasks/"+taskID+"/usage", token, body); code != http.StatusOK {
			t.Fatalf("S12 = %d: %s", code, answer)
		}
		return h
	}

	t.Run("an overlong value is cut to the bound", func(t *testing.T) {
		// Three bytes per rune: the cut must not leave half a rune behind.
		long := strings.Repeat("中", 200)
		h := postUsage(t, []byte(fmt.Sprintf(
			`{"usage":[{"provider":%q,"model":%q,"input_tokens":1}]}`, long, long)))

		got := h.metrics.tokenUsageSnapshot()
		if len(got) != 1 {
			t.Fatalf("usage entries = %d, want 1", len(got))
		}
		for _, label := range []struct {
			name  string
			value string
		}{
			{"provider", got[0].provider},
			{"model", got[0].model},
		} {
			if label.value == "" || len(label.value) > usageLabelBytes || !strings.HasPrefix(long, label.value) {
				t.Errorf("%s label = %d bytes, want a non-empty prefix of the reported value within %d bytes",
					label.name, len(label.value), usageLabelBytes)
			}
			if !utf8.ValidString(label.value) {
				t.Errorf("%s label is not valid UTF-8", label.name)
			}
		}
		if got[0].input != 1 {
			t.Errorf("input tokens = %d, want 1", got[0].input)
		}
	})

	t.Run("pairs past the budget fold onto one label", func(t *testing.T) {
		reports := make([]string, 0, seriesEntries)
		for i := range seriesEntries {
			reports = append(reports, fmt.Sprintf(`{"provider":"provider-%d","model":"model-%d","input_tokens":1}`, i, i))
		}
		h := postUsage(t, []byte(`{"usage":[`+strings.Join(reports, ",")+`]}`))

		pairs := make(map[string]struct{})
		var total int64
		for _, u := range h.metrics.tokenUsageSnapshot() {
			pairs[u.provider+"\x00"+u.model] = struct{}{}
			total += u.input
		}
		if total != seriesEntries {
			t.Errorf("usage entries counted = %d, want %d: folding must not drop counts", total, seriesEntries)
		}
		if len(pairs) > usageLabelCeiling {
			t.Errorf("distinct (provider, model) pairs = %d, want at most %d", len(pairs), usageLabelCeiling)
		}
	})
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
	// S12: the usage report is accumulated on the forward path (§9.2 /
	// ADR-009 decision 4). The success and failure tasks carry distinct
	// provider/model labels so both label values are judged.
	provider, model := "omp", "kimi"
	input, output := int64(100), int64(50)
	cacheRead, cacheWrite := int64(30), int64(20)
	if !isSuccess {
		provider, model = "zz", "glm"
		input, output = 200, 25
		cacheRead, cacheWrite = 40, 10
	}
	usage := fmt.Sprintf(`{"usage":[{"provider":%q,"model":%q,"input_tokens":%d,"output_tokens":%d,`+
		`"cache_read_tokens":%d,"cache_write_tokens":%d}]}`,
		provider, model, input, output, cacheRead, cacheWrite)
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/usage", token, []byte(usage)); code != http.StatusOK {
		t.Fatalf("S12 %s = %d: %s", taskID, code, body)
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
