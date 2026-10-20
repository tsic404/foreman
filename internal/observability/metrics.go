package observability

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metric names and label vocabularies (§9.2). Labels are
// limited to the listed dimensions — task_id/issue_id/job_name/node_name
// are forbidden as labels (cardinality discipline); use logs to locate a
// task.
const (
	metricTasksTotal       = "foreman_tasks_total"
	metricTaskClaimsTotal  = "foreman_task_claims_total"
	metricClaimErrorsTotal = "foreman_claim_errors_total"
	// S6 delivery face (ADR-032): the claim requests by result, the delivery
	// latency and the deliveries the confirmation window judged undelivered.
	metricDeliveryClaimsTotal      = "foreman_delivery_claims_total"
	metricDeliveryLatency          = "foreman_delivery_latency_seconds"
	metricDeliveryUnconfirmedTotal = "foreman_delivery_unconfirmed_total"
	metricJobBootSeconds           = "foreman_job_boot_seconds"
	metricJobCreateSeconds         = "foreman_job_create_seconds"
	metricInflightJobs             = "foreman_inflight_jobs"
	metricInflightJobsWorkspace    = "foreman_inflight_jobs_workspace"
	metricForwardTotal             = "foreman_forward_total"
	metricForwardLatency           = "foreman_forward_latency_seconds"
	metricAuthFailuresTotal        = "foreman_auth_failures_total"
	metricTokenUsageTotal          = "foreman_token_usage_total"
	metricDuplicateDispatch        = "foreman_duplicate_dispatch_total"
	metricPendingReports           = "foreman_pending_reports"
	metricWSConnected              = "foreman_ws_connected"
	metricWSReconnectsTotal        = "foreman_ws_reconnects_total"
	metricHeartbeatSuspect         = "foreman_heartbeat_suspect"
	metricCredReclaimTotal         = "foreman_cred_reclaim_total"
	metricJobDrainSeconds          = "foreman_job_drain_seconds"
	metricJobDeleteFailed          = "foreman_job_delete_failed_total"
	// Catalog relay metrics (ADR-019 / ADR-025): the relay outcome by C21
	// status code ("none" = abandoned) and the consumption-to-answer span.
	metricModelCatalogRelays = "foreman_model_catalog_relays_total"
	metricModelCatalogSecond = "foreman_model_catalog_seconds"
	// Skill-bundle resolve cache (§9.2): the resolve outcome by result, the
	// store's two size gauges, and the reclaim counter by reason.
	metricSkillCacheResolves  = "foreman_skill_cache_resolves_total"
	metricSkillCacheEntries   = "foreman_skill_cache_entries"
	metricSkillCacheBytes     = "foreman_skill_cache_bytes"
	metricSkillCacheEvictions = "foreman_skill_cache_evictions_total"
	// Workspace state refresh (§9.2): C-22 outcomes and the §1.6 runtime
	// profile refresh outcomes, both by result.
	metricWorkspaceStateRefreshes = "foreman_workspace_state_refreshes_total"
	metricRuntimeProfileRefreshes = "foreman_runtime_profile_refreshes_total"
	// Policy faces (§9.2): the three axes' hit counts by source, and the
	// startup name-resolution conclusions.
	metricPolicyHits            = "foreman_policy_hits_total"
	metricPolicyNameResolutions = "foreman_policy_name_resolutions_total"
)

// Terminal result label values (§4.1).
const (
	ResultCompleted = "completed"
	ResultFailed    = "failed"
	ResultCancelled = "cancelled"
)

// Forward endpoint label values (§9.2); the proxy's
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

// Delivery claim result label values (ADR-032; the proxy passes these
// through its own Metrics seam): a committed delivery, a delivered payload
// handed over again once the confirmation window released it — the
// lost-response signature that recovered — a plain empty batch, and an empty
// batch answering an entry whose delivery was committed but still unconfirmed
// inside its window.
const (
	DeliveryClaimDelivered        = "delivered"
	DeliveryClaimRedelivered      = "redelivered"
	DeliveryClaimEmpty            = "empty"
	DeliveryClaimEmptyUnconfirmed = "empty_unconfirmed"
)

// deliveryClaimResults pre-initializes the foreman_delivery_claims_total
// series so every designed label value is visible from boot.
var deliveryClaimResults = []string{
	DeliveryClaimDelivered, DeliveryClaimRedelivered,
	DeliveryClaimEmpty, DeliveryClaimEmptyUnconfirmed,
}

// authFailureReasons is the foreman_auth_failures_total label vocabulary.
// "revoked" is the same-name reuse face: the entry reappeared with a
// predecessor's token (ADR-017).
var authFailureReasons = []string{"malformed", "signature", "expired", "scope", "unknown_job", "revoked"}

// credReclaimReasons is the foreman_cred_reclaim_total label vocabulary
// (ADR-017): the drain window closed on the Job's termination, or on the
// FOREMAN_JOB_DRAIN_TIMEOUT upper bound.
var credReclaimReasons = []string{"job_terminated", "drain_timeout"}

// wsSides is the side label vocabulary (ADR-010).
var wsSides = []string{"server", "job"}

// modelCatalogCodes is the foreman_model_catalog_relays_total label
// vocabulary: the C21 status returned to the probe daemon, or "none" when the
// relay was abandoned (§9.2).
var modelCatalogCodes = []string{"none", "200", "400", "401", "403", "404", "409", "500", "502", "503"}

// skillCacheResults is the foreman_skill_cache_resolves_total label vocabulary
// (§9.2): a fresh hit, a miss, a refresh of an expired entry, a downgraded
// answer, and a failure that had nothing to answer with. The proxy passes these
// through its own Metrics seam.
var skillCacheResults = []string{"hit", "miss", "stale_hit", "degraded", "upstream_failure"}

// skillCacheEvictReasons is the foreman_skill_cache_evictions_total label
// vocabulary: the entry-count bound, the byte bound, the retention, and the
// integrity check's own verdict.
var skillCacheEvictReasons = []string{"capacity", "retention", "damaged"}

// workspaceStateResults is the foreman_workspace_state_refreshes_total label
// vocabulary (§9.2): a successful C-22, a failed refresh answered from the last
// snapshot, and a refresh request with no seeded snapshot to replay (§1.7's
// register-before window).
var workspaceStateResults = []string{"success", "stale_replay", "failed"}

// runtimeProfileResults is the foreman_runtime_profile_refreshes_total label
// vocabulary (§9.2): a committed refresh, an unchanged projection, and a
// failed sequence.
var runtimeProfileResults = []string{"success", "unchanged", "failed"}

// policyAxes is the foreman_policy_hits_total axis label vocabulary: the three
// faces the policy renderer writes (§8.2).
var policyAxes = []string{"resources", "capabilities_add", "node_selector"}

// policySources is the foreman_policy_hits_total source label vocabulary: the
// policy.applied *_source values (§9.1), so a hit stays distinguishable from
// the two documented miss shapes.
var policySources = []string{"direct", "no_definition", "no_identity"}

// policyNameConclusions is the foreman_policy_name_resolutions_total label
// vocabulary (ADR-035 decision 4).
var policyNameConclusions = []string{"resolved", "absent", "ambiguous", "unavailable"}

// Metrics is the Prometheus implementation of the designed metric surface
// (§9.2) plus the recorders for the remaining designed metrics. It owns
// a private registry — no global state — so a collector panic is contained
// by promhttp's recover and never corrupts other modules.
type Metrics struct {
	reg *prometheus.Registry

	tasksTotal         *prometheus.CounterVec
	taskClaims         prometheus.Counter
	claimErrors        *prometheus.CounterVec
	deliveryClaims     *prometheus.CounterVec
	deliveryLatency    prometheus.Histogram
	deliveryUnconfirm  prometheus.Counter
	jobBootSeconds     prometheus.Histogram
	jobCreateSeconds   prometheus.Histogram
	inflightJobs       prometheus.Gauge
	inflightWorkspace  *prometheus.GaugeVec
	forwardTotal       *prometheus.CounterVec
	forwardLatency     *prometheus.HistogramVec
	authFailures       *prometheus.CounterVec
	tokenUsage         *prometheus.CounterVec
	duplicateDispatch  prometheus.Counter
	pendingReports     prometheus.Gauge
	wsConnected        *prometheus.GaugeVec
	wsReconnects       *prometheus.CounterVec
	heartbeatSuspect   prometheus.Gauge
	credReclaims       *prometheus.CounterVec
	jobDrainSeconds    prometheus.Histogram
	jobDeleteFailures  *prometheus.CounterVec
	catalogRelays      *prometheus.CounterVec
	catalogSeconds     prometheus.Histogram
	skillResolves      *prometheus.CounterVec
	skillEntries       prometheus.Gauge
	skillBytes         prometheus.Gauge
	skillEvictions     *prometheus.CounterVec
	workspaceRefreshes *prometheus.CounterVec
	profileRefreshes   *prometheus.CounterVec
	policyHits         *prometheus.CounterVec
	policyResolutions  *prometheus.CounterVec

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
	m.deliveryClaims = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricDeliveryClaimsTotal,
		Help: "S6 task claims by result (delivered|redelivered|empty|empty_unconfirmed).",
	}, []string{"result"})
	m.deliveryLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    metricDeliveryLatency,
		Help:    "claim -> committed S6 delivery latency.",
		Buckets: []float64{0.05, 0.25, 1, 2.5, 5, 10, 30, 60, 120},
	})
	m.deliveryUnconfirm = prometheus.NewCounter(prometheus.CounterOpts{
		Name: metricDeliveryUnconfirmedTotal,
		Help: "Deliveries that reached the boot bound never confirmed by the daemon.",
	})
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
	m.inflightWorkspace = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricInflightJobsWorkspace,
		Help: "Current non-terminal Job count per workspace (§10 两级预算).",
	}, []string{"workspace"})
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
	m.credReclaims = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricCredReclaimTotal,
		Help: "Job Token credential reclaims by reason.",
	}, []string{"reason"})
	m.jobDrainSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    metricJobDrainSeconds,
		Help:    "Task verdict -> credential reclaim duration.",
		Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600},
	})
	m.jobDeleteFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricJobDeleteFailed,
		Help: "Job deletions that failed on a path with no retry handle, by stage.",
	}, []string{"stage"})

	m.catalogRelays = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricModelCatalogRelays,
		Help: "Model catalog relays by C21 status code; none = abandoned.",
	}, []string{"code"})
	m.catalogSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    metricModelCatalogSecond,
		Help:    "Consume pending_model_list -> relay finished (or abandoned).",
		Buckets: []float64{0.05, 0.5, 1, 5, 15, 30, 45, 60, 90},
	})

	m.skillResolves = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricSkillCacheResolves,
		Help: "Skill-bundle resolves by result (hit|miss|stale_hit|degraded|upstream_failure).",
	}, []string{"result"})
	m.skillEntries = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: metricSkillCacheEntries,
		Help: "Skill-bundle cache entries currently held.",
	})
	m.skillBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: metricSkillCacheBytes,
		Help: "Skill-bundle cache response bytes currently held.",
	})
	m.skillEvictions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricSkillCacheEvictions,
		Help: "Skill-bundle cache entries reclaimed by reason.",
	}, []string{"reason"})
	m.workspaceRefreshes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricWorkspaceStateRefreshes,
		Help: "Workspace state (settings/repos) refreshes by result.",
	}, []string{"result"})
	m.profileRefreshes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricRuntimeProfileRefreshes,
		Help: "Runtime profile refresh sequences by result.",
	}, []string{"result"})
	m.policyHits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricPolicyHits,
		Help: "Policy axis evaluations by axis (resources|capabilities_add|node_selector) and source (direct|no_definition|no_identity).",
	}, []string{"axis", "source"})
	m.policyResolutions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricPolicyNameResolutions,
		Help: "Startup policy name resolutions by conclusion.",
	}, []string{"conclusion"})

	collectors := []prometheus.Collector{
		m.tasksTotal, m.taskClaims, m.claimErrors, m.jobBootSeconds,
		m.deliveryClaims, m.deliveryLatency, m.deliveryUnconfirm,
		m.jobCreateSeconds, m.inflightJobs, m.inflightWorkspace, m.forwardTotal, m.forwardLatency,
		m.authFailures, m.tokenUsage, m.duplicateDispatch, m.pendingReports,
		m.wsConnected, m.wsReconnects, m.heartbeatSuspect,
		m.credReclaims, m.jobDrainSeconds, m.jobDeleteFailures, m.catalogRelays, m.catalogSeconds,
		m.skillResolves, m.skillEntries, m.skillBytes, m.skillEvictions,
		m.workspaceRefreshes, m.profileRefreshes, m.policyHits,
		m.policyResolutions,
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
	m.jobDeleteFailures.WithLabelValues("catch_up")
	for _, ep := range forwardEndpoints {
		m.forwardTotal.WithLabelValues(ep, "200")
		m.forwardLatency.WithLabelValues(ep)
	}
	for _, reason := range authFailureReasons {
		m.authFailures.WithLabelValues(reason)
	}
	for _, code := range modelCatalogCodes {
		m.catalogRelays.WithLabelValues(code)
	}
	for _, direction := range []string{"input", "output", "cache_read", "cache_write"} {
		m.tokenUsage.WithLabelValues("", "", direction)
	}
	for _, side := range wsSides {
		m.wsConnected.WithLabelValues(side)
		m.wsReconnects.WithLabelValues(side)
	}
	for _, reason := range credReclaimReasons {
		m.credReclaims.WithLabelValues(reason)
	}
	for _, result := range deliveryClaimResults {
		m.deliveryClaims.WithLabelValues(result)
	}
	for _, result := range skillCacheResults {
		m.skillResolves.WithLabelValues(result)
	}
	for _, reason := range skillCacheEvictReasons {
		m.skillEvictions.WithLabelValues(reason)
	}
	for _, result := range workspaceStateResults {
		m.workspaceRefreshes.WithLabelValues(result)
	}
	for _, result := range runtimeProfileResults {
		m.profileRefreshes.WithLabelValues(result)
	}
	for _, axis := range policyAxes {
		for _, source := range policySources {
			m.policyHits.WithLabelValues(axis, source)
		}
	}
	for _, conclusion := range policyNameConclusions {
		m.policyResolutions.WithLabelValues(conclusion)
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

// JobBootSeconds observes claim -> daemon start latency (§9.2).
func (m *Metrics) JobBootSeconds(seconds float64) { m.jobBootSeconds.Observe(seconds) }

// InflightJobs sets the current non-terminal entry count (§9.2).
func (m *Metrics) InflightJobs(n int) { m.inflightJobs.Set(float64(n)) }

// InflightJobsWorkspace sets one workspace's non-terminal entry count: the
// two-level budget's observable face (§10).
func (m *Metrics) InflightJobsWorkspace(workspaceID string, n int) {
	m.inflightWorkspace.WithLabelValues(workspaceID).Set(float64(n))
}

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

// JobCreateSeconds observes claim -> Job created latency.
func (m *Metrics) JobCreateSeconds(seconds float64) { m.jobCreateSeconds.Observe(seconds) }

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

// DeliveryUnconfirmed counts one entry that reached the boot bound on a
// committed delivery the daemon never confirmed (ADR-032): the window
// released the payload for another attempt, and no attempt was started.
func (m *Metrics) DeliveryUnconfirmed() { m.deliveryUnconfirm.Inc() }

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

// CredReclaimed counts one credential reclaim by reason (job_terminated or
// drain_timeout) and observes how long the drain window lasted (ADR-017).
// drain_timeout > 0 means the upper bound closed the window — a review input,
// not a failure.
// ModelCatalogRelay records one catalog relay outcome (ADR-019 / ADR-025):
// code is the C21 status returned to the probe daemon, or "none" when no relay
// happened; seconds spans consumption to answer (near zero for a cache replay).
func (m *Metrics) ModelCatalogRelay(code string, seconds float64) {
	if code == "" {
		code = "none"
	}
	m.catalogRelays.WithLabelValues(code).Inc()
	m.catalogSeconds.Observe(seconds)
}

func (m *Metrics) CredReclaimed(reason string, drainSeconds float64) {
	m.credReclaims.WithLabelValues(reason).Inc()
	m.jobDrainSeconds.Observe(drainSeconds)
}

// JobDeleteFailed counts a Job deletion that failed where the caller has no
// retry handle left (the post-reclaim catch-up: the entry is already gone, so
// only the template's TTL remains).
func (m *Metrics) JobDeleteFailed(stage string) {
	m.jobDeleteFailures.WithLabelValues(stage).Inc()
}

// SkillCache records one skill-bundle resolve by result (§9.2): the proxy's
// SkillResult label value passes through unchanged.
func (m *Metrics) SkillCache(result string) { m.skillResolves.WithLabelValues(result).Inc() }

// SkillCacheSize sets the store's entry and byte gauges (§9.2 条目数与字节量).
func (m *Metrics) SkillCacheSize(entries int, bytes int64) {
	m.skillEntries.Set(float64(entries))
	m.skillBytes.Set(float64(bytes))
}

// SkillCacheEviction counts one reclaimed entry by reason (§9.2 回收计数).
func (m *Metrics) SkillCacheEviction(reason string) {
	m.skillEvictions.WithLabelValues(reason).Inc()
}

// WorkspaceStateRefresh records one C-22 outcome (§9.2).
func (m *Metrics) WorkspaceStateRefresh(result string) {
	m.workspaceRefreshes.WithLabelValues(result).Inc()
}

// RuntimeProfileRefresh records one §1.6 refresh outcome (§9.2).
func (m *Metrics) RuntimeProfileRefresh(result string) {
	m.profileRefreshes.WithLabelValues(result).Inc()
}

// PolicyHit records one policy axis evaluation (§9.2 策略命中): axis is one of
// resources / capabilities_add / node_selector, source is the policy.applied
// *_source value (direct / no_definition / no_identity), so a hit and the two
// miss shapes stay distinguishable.
func (m *Metrics) PolicyHit(axis, source string) {
	m.policyHits.WithLabelValues(axis, source).Inc()
}

// PolicyNameResolution records one startup policy key's conclusion (§9.2
// 名字解析结论, ADR-035 decision 4).
func (m *Metrics) PolicyNameResolution(conclusion string) {
	m.policyResolutions.WithLabelValues(conclusion).Inc()
}
