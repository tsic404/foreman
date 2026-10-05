package observability

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metric names and label vocabularies (observability.md §指标). Labels are
// limited to the listed dimensions — task_id/issue_id/job_name/node_name
// are forbidden as labels (cardinality discipline); use logs to locate a
// task.
const (
	metricTasksTotal        = "foreman_tasks_total"
	metricTaskClaimsTotal   = "foreman_task_claims_total"
	metricClaimErrorsTotal  = "foreman_claim_errors_total"
	metricJobBootSeconds    = "foreman_job_boot_seconds"
	metricJobCreateSeconds  = "foreman_job_create_seconds"
	metricInflightJobs      = "foreman_inflight_jobs"
	metricForwardTotal      = "foreman_forward_total"
	metricForwardLatency    = "foreman_forward_latency_seconds"
	metricAuthFailuresTotal = "foreman_auth_failures_total"
	metricTokenUsageTotal   = "foreman_token_usage_total"
	metricDuplicateDispatch = "foreman_duplicate_dispatch_total"
	metricPendingReports    = "foreman_pending_reports"
	metricWSConnected       = "foreman_ws_connected"
	metricWSReconnectsTotal = "foreman_ws_reconnects_total"
	metricHeartbeatSuspect  = "foreman_heartbeat_suspect"
)

// Terminal result label values (contract §3.1).
const (
	ResultCompleted = "completed"
	ResultFailed    = "failed"
	ResultCancelled = "cancelled"
)

// Forward endpoint label values (observability.md §指标); the proxy's
// Endpoint enum maps onto these, "agent-api" is the S29 passthrough.
const (
	ForwardAgentAPI = "agent-api"
)

// forwardEndpoints pre-initializes the forward series so the metric family
// is visible from process start (TC-tech-observability-01 greps names).
var forwardEndpoints = []string{
	"start", "progress", "messages", "usage", "complete", "fail",
	"cancel-ack", "session", "wait-local-directory", ForwardAgentAPI,
}

// authFailureReasons is the foreman_auth_failures_total label vocabulary.
var authFailureReasons = []string{"malformed", "signature", "expired", "scope", "unknown_job"}

// wsSides is the side label vocabulary (ADR-010).
var wsSides = []string{"server", "job"}

// Metrics is the Prometheus implementation of the contract §2 Metrics
// interface plus the recorders for the remaining designed metrics. It owns
// a private registry — no global state — so a collector panic is contained
// by promhttp's recover and never corrupts other modules.
type Metrics struct {
	reg *prometheus.Registry

	tasksTotal        *prometheus.CounterVec
	taskClaims        prometheus.Counter
	claimErrors       *prometheus.CounterVec
	jobBootSeconds    prometheus.Histogram
	jobCreateSeconds  prometheus.Histogram
	inflightJobs      prometheus.Gauge
	forwardTotal      *prometheus.CounterVec
	forwardLatency    *prometheus.HistogramVec
	authFailures      *prometheus.CounterVec
	tokenUsage        *prometheus.CounterVec
	duplicateDispatch prometheus.Counter
	pendingReports    prometheus.Gauge
	wsConnected       *prometheus.GaugeVec
	wsReconnects      *prometheus.CounterVec
	heartbeatSuspect  prometheus.Gauge

	// wsMu guards wsJobs, the authoritative side=job connection count; the
	// gauge is Set from it so a disconnect without a matching connect (e.g.
	// a connection made before a restart) can never drive it negative.
	wsMu   sync.Mutex
	wsJobs int
}

// NewMetrics builds the metric set on a fresh private registry. All series
// with enumerable label sets are pre-initialized to 0 so /metrics exposes
// every designed name from boot; the WS series stay 0 when
// FOREMAN_WS_ENABLED=false (degradation contract: metrics present, value 0).
func NewMetrics() *Metrics {
	m := &Metrics{reg: prometheus.NewRegistry()}

	m.tasksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricTasksTotal,
		Help: "Terminal tasks by result (success rate source).",
	}, []string{"result"})
	m.taskClaims = prometheus.NewCounter(prometheus.CounterOpts{
		Name: metricTaskClaimsTotal,
		Help: "Tasks claimed from the server.",
	})
	m.claimErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricClaimErrorsTotal,
		Help: "Claim failures by HTTP status code or transport.",
	}, []string{"code"})
	m.jobBootSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    metricJobBootSeconds,
		Help:    "claim -> daemon start latency.",
		Buckets: []float64{1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600},
	})
	m.jobCreateSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    metricJobCreateSeconds,
		Help:    "claim -> Job created latency.",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	})
	m.inflightJobs = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: metricInflightJobs,
		Help: "Non-terminal registry entries.",
	})
	m.forwardTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricForwardTotal,
		Help: "Lifecycle forwards to the server by endpoint and HTTP code.",
	}, []string{"endpoint", "code"})
	m.forwardLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    metricForwardLatency,
		Help:    "Forward call latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"endpoint"})
	m.authFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricAuthFailuresTotal,
		Help: "Job Token verification failures by reason.",
	}, []string{"reason"})
	m.tokenUsage = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricTokenUsageTotal,
		Help: "Token usage from forwarded usage reports.",
	}, []string{"provider", "model", "direction"})
	m.duplicateDispatch = prometheus.NewCounter(prometheus.CounterOpts{
		Name: metricDuplicateDispatch,
		Help: "Server re-dispatches of an already-known task (must stay 0).",
	})
	m.pendingReports = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: metricPendingReports,
		Help: "Terminal reports waiting in the local retry queue.",
	})
	m.wsConnected = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricWSConnected,
		Help: "WS state: server is 0/1; job counts established Job connections.",
	}, []string{"side"})
	m.wsReconnects = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricWSReconnectsTotal,
		Help: "WS reconnects after a drop, by side.",
	}, []string{"side"})
	m.heartbeatSuspect = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: metricHeartbeatSuspect,
		Help: "Tasks with both heartbeat channels silent for over 60s.",
	})

	collectors := []prometheus.Collector{
		m.tasksTotal, m.taskClaims, m.claimErrors, m.jobBootSeconds,
		m.jobCreateSeconds, m.inflightJobs, m.forwardTotal, m.forwardLatency,
		m.authFailures, m.tokenUsage, m.duplicateDispatch, m.pendingReports,
		m.wsConnected, m.wsReconnects, m.heartbeatSuspect,
	}
	for _, c := range collectors {
		m.reg.MustRegister(c)
	}

	// Pre-initialize enumerable series; token_usage keeps empty provider/
	// model so the family is visible before the first usage report lands.
	for _, result := range []string{ResultCompleted, ResultFailed, ResultCancelled} {
		m.tasksTotal.WithLabelValues(result)
	}
	m.claimErrors.WithLabelValues("transport")
	for _, ep := range forwardEndpoints {
		m.forwardTotal.WithLabelValues(ep, "200")
		m.forwardLatency.WithLabelValues(ep)
	}
	for _, reason := range authFailureReasons {
		m.authFailures.WithLabelValues(reason)
	}
	for _, direction := range []string{"input", "output", "cache_read", "cache_write"} {
		m.tokenUsage.WithLabelValues("", "", direction)
	}
	for _, side := range wsSides {
		m.wsConnected.WithLabelValues(side)
		m.wsReconnects.WithLabelValues(side)
	}
	return m
}

// Handler serves the Prometheus text exposition. HTTPErrorOnError maps a
// collector failure to 500 (observability.md §错误处理).
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{ErrorHandling: promhttp.HTTPErrorOnError})
}

// TaskClaimed counts a successfully claimed task (contract §2).
func (m *Metrics) TaskClaimed() { m.taskClaims.Inc() }

// TaskTerminal counts a settled task by result (contract §2).
func (m *Metrics) TaskTerminal(result string) { m.tasksTotal.WithLabelValues(result).Inc() }

// JobBootSeconds observes claim -> daemon start latency (contract §2).
func (m *Metrics) JobBootSeconds(seconds float64) { m.jobBootSeconds.Observe(seconds) }

// InflightJobs sets the current non-terminal entry count (contract §2).
func (m *Metrics) InflightJobs(n int) { m.inflightJobs.Set(float64(n)) }

// TokenUsage accumulates a forwarded usage report (contract §2). The
// contract signature carries input/output only; cache directions exist in
// the label vocabulary for a future report shape.
func (m *Metrics) TokenUsage(provider, model string, input, output int64) {
	m.tokenUsage.WithLabelValues(provider, model, "input").Add(float64(input))
	m.tokenUsage.WithLabelValues(provider, model, "output").Add(float64(output))
}

// Forward records one forwarded call's outcome and latency (contract §2).
// endpoint takes the proxy's Endpoint string; "agent-api" for S29.
func (m *Metrics) Forward(endpoint string, code int, d time.Duration) {
	m.forwardTotal.WithLabelValues(endpoint, strconv.Itoa(code)).Inc()
	m.forwardLatency.WithLabelValues(endpoint).Observe(d.Seconds())
}

// JobCreateSeconds observes claim -> Job created latency.
func (m *Metrics) JobCreateSeconds(seconds float64) { m.jobCreateSeconds.Observe(seconds) }

// ClaimError counts a failed claim cycle; code is an HTTP status or
// "transport".
func (m *Metrics) ClaimError(code string) { m.claimErrors.WithLabelValues(code).Inc() }

// DuplicateDispatch counts a server re-dispatch of a known task.
func (m *Metrics) DuplicateDispatch() { m.duplicateDispatch.Inc() }

// AuthFailure counts a Job Token verification failure by reason.
func (m *Metrics) AuthFailure(reason string) { m.authFailures.WithLabelValues(reason).Inc() }

// PendingReports sets the terminal-report retry queue depth.
func (m *Metrics) PendingReports(n int) { m.pendingReports.Set(float64(n)) }

// WSConnect marks a WS side up: side=server pins the gauge to 1, side=job
// adds one established Job connection.
func (m *Metrics) WSConnect(side string) {
	if side == "server" {
		m.wsConnected.WithLabelValues(side).Set(1)
		return
	}
	m.wsMu.Lock()
	m.wsJobs++
	m.wsConnected.WithLabelValues(side).Set(float64(m.wsJobs))
	m.wsMu.Unlock()
}

// WSDisconnect is WSConnect's counterpart.
func (m *Metrics) WSDisconnect(side string) {
	if side == "server" {
		m.wsConnected.WithLabelValues(side).Set(0)
		return
	}
	m.wsMu.Lock()
	if m.wsJobs > 0 {
		m.wsJobs--
	}
	m.wsConnected.WithLabelValues(side).Set(float64(m.wsJobs))
	m.wsMu.Unlock()
}

// WSReconnect counts one WS reconnect after a drop.
func (m *Metrics) WSReconnect(side string) { m.wsReconnects.WithLabelValues(side).Inc() }

// HeartbeatSuspect sets how many tasks currently fail the 60s heartbeat
// freshness check.
func (m *Metrics) HeartbeatSuspect(n int) { m.heartbeatSuspect.Set(float64(n)) }
