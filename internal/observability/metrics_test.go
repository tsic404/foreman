package observability

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", rec.Code)
	}
	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("read /metrics body: %v", err)
	}
	return string(body)
}

// TestMetricsExposeAllDesignedNames is TC-tech-observability-01 step 3 at
// process start: every metric name from observability.md §指标 must appear
// even before any recording.
func TestMetricsExposeAllDesignedNames(t *testing.T) {
	out := scrape(t, NewMetrics())
	for _, name := range []string{
		metricTasksTotal, metricTaskClaimsTotal, metricClaimErrorsTotal,
		metricJobBootSeconds, metricJobCreateSeconds, metricInflightJobs,
		metricForwardTotal, metricForwardLatency, metricAuthFailuresTotal,
		metricTokenUsageTotal, metricDuplicateDispatch, metricPendingReports,
		metricWSConnected, metricWSReconnectsTotal, metricHeartbeatSuspect,
	} {
		if !strings.Contains(out, name) {
			t.Fatalf("metric %s missing from exposition:\n%s", name, out)
		}
	}
}

// TestMetricsCardinalityDiscipline is TC-tech-observability-01 step 5.
func TestMetricsCardinalityDiscipline(t *testing.T) {
	m := NewMetrics()
	m.TaskClaimed()
	m.TaskTerminal(ResultCompleted)
	m.JobBootSeconds(12.5)
	m.JobCreateSeconds(1.5)
	m.InflightJobs(3)
	m.TokenUsage("omp", "kimi", 100, 50)
	m.Forward("complete", 200, 250*time.Millisecond)
	m.Forward(ForwardAgentAPI, 502, time.Second)
	m.ClaimError("503")
	m.AuthFailure("expired")
	m.DuplicateDispatch()
	m.PendingReports(2)
	m.WSConnect("server")
	m.WSConnect("job")
	m.WSReconnect("job")
	m.HeartbeatSuspect(1)

	out := scrape(t, m)
	for _, bad := range []string{"task_id=", "issue_id=", "job_name=", "node_name="} {
		if strings.Contains(out, bad) {
			t.Fatalf("high-cardinality label %s in exposition", bad)
		}
	}
	for _, want := range []string{
		`foreman_tasks_total{result="completed"} 1`,
		`foreman_token_usage_total{direction="input",model="kimi",provider="omp"} 100`,
		`foreman_token_usage_total{direction="output",model="kimi",provider="omp"} 50`,
		`foreman_forward_total{code="200",endpoint="complete"} 1`,
		`foreman_claim_errors_total{code="503"} 1`,
		`foreman_auth_failures_total{reason="expired"} 1`,
		`foreman_duplicate_dispatch_total 1`,
		`foreman_pending_reports 2`,
		`foreman_ws_connected{side="server"} 1`,
		`foreman_ws_connected{side="job"} 1`,
		`foreman_ws_reconnects_total{side="job"} 1`,
		`foreman_heartbeat_suspect 1`,
		`foreman_inflight_jobs 3`,
		`foreman_task_claims_total 1`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("want series %q in exposition:\n%s", want, out)
		}
	}
}

// TestWSJobGaugeNeverNegative covers the degradation/restart edge: a
// disconnect without a matching connect must not drive the gauge below 0.
func TestWSJobGaugeNeverNegative(t *testing.T) {
	m := NewMetrics()
	m.WSDisconnect("job")
	m.WSConnect("job")
	m.WSConnect("job")
	m.WSDisconnect("job")
	m.WSDisconnect("job")
	m.WSDisconnect("job")
	out := scrape(t, m)
	if !strings.Contains(out, `foreman_ws_connected{side="job"} 0`) {
		t.Fatalf("job ws gauge went negative:\n%s", out)
	}
}

// TestWSDegradationContract: with WS disabled no recorder is ever called,
// yet the WS series stay registered at 0 (observability.md §降级契约).
func TestWSDegradationContract(t *testing.T) {
	out := scrape(t, NewMetrics())
	for _, want := range []string{
		`foreman_ws_connected{side="server"} 0`,
		`foreman_ws_connected{side="job"} 0`,
		`foreman_ws_reconnects_total{side="server"} 0`,
		`foreman_ws_reconnects_total{side="job"} 0`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("want %q in fresh exposition", want)
		}
	}
}

func TestMetricsIsolatedRegistries(t *testing.T) {
	a, b := NewMetrics(), NewMetrics()
	a.TaskClaimed()
	if strings.Contains(scrape(t, b), `foreman_task_claims_total 1`) {
		t.Fatal("metrics instances share registry state")
	}
}
