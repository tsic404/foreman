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
// process start: every metric name from §9.2 that v2 actually records must
// appear even before any recording. A name whose family has no v2 recorder is
// deliberately absent — it would expose a series that can never move.
func TestMetricsExposeAllDesignedNames(t *testing.T) {
	out := scrape(t, NewMetrics())
	for _, name := range []string{
		metricTasksTotal, metricTaskClaimsTotal, metricClaimErrorsTotal,
		metricInflightJobs,
		metricForwardTotal, metricForwardLatency, metricAuthFailuresTotal,
		metricTokenUsageTotal, metricDuplicateDispatch,
		metricDeliveryClaimsTotal, metricDeliveryLatency,
		metricCredReclaimTotal, metricJobDrainSeconds,
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
	m.InflightJobs(3)
	m.TokenUsage("omp", "kimi", 100, 50, 30, 20)
	m.Forward("complete", 200, 250*time.Millisecond)
	m.Forward(ForwardAgentAPI, 502, time.Second)
	m.ClaimError("503")
	m.AuthFailure("missing")
	m.DuplicateDispatch()
	m.AuthFailure("invalid")
	m.CredReclaimed("task_terminated", 12.5)

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
		`foreman_token_usage_total{direction="cache_read",model="kimi",provider="omp"} 30`,
		`foreman_token_usage_total{direction="cache_write",model="kimi",provider="omp"} 20`,
		`foreman_forward_total{code="200",endpoint="complete"} 1`,
		`foreman_claim_errors_total{code="503"} 1`,
		`foreman_auth_failures_total{reason="missing"} 1`,
		`foreman_duplicate_dispatch_total 1`,
		`foreman_auth_failures_total{reason="invalid"} 1`,
		`foreman_cred_reclaim_total{reason="task_terminated"} 1`,
		`foreman_job_drain_seconds_count 1`,
		`foreman_inflight_jobs 3`,
		`foreman_task_claims_total 1`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("want series %q in exposition:\n%s", want, out)
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
