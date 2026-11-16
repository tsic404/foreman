package observability

import (
	"net/http"
	"strconv"

	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	metricTasksTotal       = "foreman_tasks_total"
	metricTaskClaimsTotal  = "foreman_task_claims_total"
	metricClaimErrorsTotal = "foreman_claim_errors_total"
	// S6 delivery face (ADR-032): the claim requests by result and the
	// claim->commit latency.
	metricDeliveryClaimsTotal = "foreman_delivery_claims_total"
	metricDeliveryLatency     = "foreman_delivery_latency_seconds"

	metricInflightJobs = "foreman_inflight_jobs"

	metricForwardTotal      = "foreman_forward_total"
	metricForwardLatency    = "foreman_forward_latency_seconds"
	metricAuthFailuresTotal = "foreman_auth_failures_total"
	metricTokenUsageTotal   = "foreman_token_usage_total"
	metricDuplicateDispatch = "foreman_duplicate_dispatch_total"

	metricCredReclaimTotal = "foreman_cred_reclaim_total"
	metricJobDrainSeconds  = "foreman_job_drain_seconds"
)

const (
	ResultCompleted = "completed"
	ResultFailed    = "failed"
	ResultCancelled = "cancelled"
)

const (
	ForwardAgentAPI = "agent-api"
)

var forwardEndpoints = []string{
	"start", "progress", "messages", "usage", "complete", "fail",
	"cancel-ack", "session", "wait-local-directory", ForwardAgentAPI,
}

const (
	DeliveryClaimDelivered        = "delivered"
	DeliveryClaimRedelivered      = "redelivered"
	DeliveryClaimEmpty            = "empty"
	DeliveryClaimEmptyUnconfirmed = "empty_unconfirmed"
)

var deliveryClaimResults = []string{
	DeliveryClaimDelivered, DeliveryClaimRedelivered,
	DeliveryClaimEmpty, DeliveryClaimEmptyUnconfirmed,
}

// authFailureReasons are the values the daemon credential check actually
// reports: a missing header, an unknown token, and a token that does not
// verify. Declaring any other value here would pre-initialize series that can
// never move.
var authFailureReasons = []string{"missing", "unknown", "invalid"}

// credReclaimReasons is what reclaim actually sends. Only a terminal task is
// reclaimed in v2, so that is the only reason.
var credReclaimReasons = []string{"task_terminated"}

type Metrics struct {
	reg *prometheus.Registry

	tasksTotal      *prometheus.CounterVec
	taskClaims      prometheus.Counter
	claimErrors     *prometheus.CounterVec
	deliveryClaims  *prometheus.CounterVec
	deliveryLatency prometheus.Histogram

	inflightJobs prometheus.Gauge

	forwardTotal   *prometheus.CounterVec
	forwardLatency *prometheus.HistogramVec
	authFailures   *prometheus.CounterVec

	tokenUsage        *prometheus.CounterVec
	duplicateDispatch prometheus.Counter

	credReclaims    *prometheus.CounterVec
	jobDrainSeconds prometheus.Histogram
}

// NewMetrics builds the metric set on a fresh private registry. All series
// with enumerable label sets are pre-initialized to 0 so /metrics exposes
// every name this build actually records from boot.
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
	m.deliveryClaims = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricDeliveryClaimsTotal,
		Help: "S6 task claims by result (delivered|redelivered|empty|empty_unconfirmed).",
	}, []string{"result"})
	m.deliveryLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    metricDeliveryLatency,
		Help:    "claim -> committed S6 delivery latency.",
		Buckets: []float64{0.05, 0.25, 1, 2.5, 5, 10, 30, 60, 120},
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

	m.credReclaims = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricCredReclaimTotal,
		Help: "Job Token credential reclaims by reason.",
	}, []string{"reason"})
	m.jobDrainSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    metricJobDrainSeconds,
		Help:    "Task verdict -> credential reclaim duration.",
		Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600},
	})

	collectors := []prometheus.Collector{
		m.tasksTotal, m.taskClaims, m.claimErrors,
		m.deliveryClaims, m.deliveryLatency,
		m.inflightJobs, m.forwardTotal, m.forwardLatency,
		m.authFailures, m.tokenUsage, m.duplicateDispatch,

		m.credReclaims, m.jobDrainSeconds,
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

	for _, reason := range credReclaimReasons {
		m.credReclaims.WithLabelValues(reason)
	}
	for _, result := range deliveryClaimResults {
		m.deliveryClaims.WithLabelValues(result)
	}

	return m
}

// Handler serves the Prometheus text exposition. HTTPErrorOnError maps a
// collector failure to 500.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{ErrorHandling: promhttp.HTTPErrorOnError})
}

// TaskClaimed counts a successfully claimed task (§9.2).
func (m *Metrics) TaskClaimed() { m.taskClaims.Inc() }

// TaskTerminal counts a settled task by result (§9.2).
func (m *Metrics) TaskTerminal(result string) { m.tasksTotal.WithLabelValues(result).Inc() }

// InflightJobs sets the current non-terminal entry count (§9.2).
func (m *Metrics) InflightJobs(n int) { m.inflightJobs.Set(float64(n)) }

// TokenUsage accumulates a forwarded usage report (§9.2). The upstream S-12
// entry carries input/output and the two prompt-cache directions; the
// optional cost figure stays with the upstream (ADR-009 decision 4).
func (m *Metrics) TokenUsage(provider, model string, input, output, cacheRead, cacheWrite int64) {
	m.tokenUsage.WithLabelValues(provider, model, "input").Add(float64(input))
	m.tokenUsage.WithLabelValues(provider, model, "output").Add(float64(output))
	m.tokenUsage.WithLabelValues(provider, model, "cache_read").Add(float64(cacheRead))
	m.tokenUsage.WithLabelValues(provider, model, "cache_write").Add(float64(cacheWrite))
}

// Forward records one forwarded call's outcome and latency (§9.2).
// endpoint takes the proxy's Endpoint string; "agent-api" for S29.
func (m *Metrics) Forward(endpoint string, code int, d time.Duration) {
	m.forwardTotal.WithLabelValues(endpoint, strconv.Itoa(code)).Inc()
	m.forwardLatency.WithLabelValues(endpoint).Observe(d.Seconds())
}

// ClaimError counts a failed claim cycle; code is an HTTP status or
// "transport".
func (m *Metrics) ClaimError(code string) { m.claimErrors.WithLabelValues(code).Inc() }

// DeliveryClaim counts one task Job's S6 claim by result (ADR-032). result is
// one of the deliveryClaimResults values, passed through the proxy's seam.
func (m *Metrics) DeliveryClaim(result string) {
	m.deliveryClaims.WithLabelValues(result).Inc()
}

// DeliveryLatency observes one committed S6 delivery's claim -> commit span
// (ADR-032).
func (m *Metrics) DeliveryLatency(seconds float64) { m.deliveryLatency.Observe(seconds) }

// DuplicateDispatch counts a server re-dispatch of a known task.
func (m *Metrics) DuplicateDispatch() { m.duplicateDispatch.Inc() }

// AuthFailure counts a Job Token verification failure by reason.
func (m *Metrics) AuthFailure(reason string) { m.authFailures.WithLabelValues(reason).Inc() }

// CredReclaimed counts one credential reclaim by reason and observes how long
// the task sat terminal before the slot was released (ADR-017).

func (m *Metrics) CredReclaimed(reason string, drainSeconds float64) {
	m.credReclaims.WithLabelValues(reason).Inc()
	m.jobDrainSeconds.Observe(drainSeconds)
}
