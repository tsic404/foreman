package proxy

import "time"

// Metrics is the proxy module's observability seam (§9.2). The
// observability module provides the Prometheus implementation; nil degrades
// to no-ops. Endpoint label values are the scheduler.Endpoint strings plus
// EndpointAgentAPI for S29 passthrough.
type Metrics interface {
	// Forward records one forwarded call and its outcome (§9.2
	// foreman_forward_total / foreman_forward_latency_seconds).
	Forward(endpoint string, code int, d time.Duration)
	// ClaimError records a failed C3 claim by HTTP status or "transport".
	ClaimError(code string)
	// AuthFailure records a Job Token verification failure by reason label.
	AuthFailure(reason string)
	// WSConnect/WSDisconnect track foreman_ws_connected by side
	// ("server"=C20, "job"=S30); WSReconnect counts reconnects.
	WSConnect(side string)
	WSDisconnect(side string)
	WSReconnect(side string)
	// ModelCatalogRelay records one catalog relay outcome: code is the C21
	// status returned to the probe daemon, or "none" when no relay happened
	// (§9.2 foreman_model_catalog_relays_total).
	ModelCatalogRelay(code string, seconds float64)
	// DeliveryClaim records one task Job's S6 claim by result
	// (§9.2 foreman_delivery_claims_total, ADR-032). A catalog
	// probe's claim carries no task face and is not counted.
	DeliveryClaim(result string)
	// DeliveryLatency observes one committed S6 delivery's latency, from the
	// task's claim to the delivery commit (§9.2
	// foreman_delivery_latency_seconds).
	DeliveryLatency(seconds float64)
	// SkillCache records one skill-bundle resolve by result (§9.2
	// foreman_skill_cache_resolves_total): one of the SkillResult values.
	SkillCache(result string)
	// SkillCacheSize sets the store's entry and byte gauges (§9.2 条目数与
	// 字节量).
	SkillCacheSize(entries int, bytes int64)
	// SkillCacheEviction counts one reclaimed entry by reason: one of the
	// SkillEvict values (§9.2 回收计数).
	SkillCacheEviction(reason string)
	// RuntimeProfileRefresh records one §1.6 refresh outcome
	// (§9.2 runtime profile 刷新，按结果): one of the ProfileRefresh values.
	RuntimeProfileRefresh(result string)
	// WorkspaceStateRefresh records one C-22 refresh outcome
	// (§9.2 workspace 状态刷新，按结果): one of the WorkspaceStateRefresh values.
	WorkspaceStateRefresh(result string)
}

// Runtime profile refresh result label values (§9.2). One outcome per refresh
// attempt: success = a changed list was committed; unchanged = the sequence
// completed without moving the declaration, which covers both an unchanged
// projection and a C-18 404 (the face is gone, so there is no list to compare —
// the event's c18 field separates the two); failed = a step of the sequence
// failed (C-18 error/5xx, probe failure, C-1 failure).
const (
	ProfileRefreshSuccess   = "success"
	ProfileRefreshUnchanged = "unchanged"
	ProfileRefreshFailed    = "failed"
)

// Workspace state refresh result label values (§9.2). One outcome per refresh
// attempt; the register-before window records one per request: success = the
// carrier was replaced; stale_replay = the attempt failed and the answer came
// from the last successful snapshot; failed = the request found no seeded
// snapshot to answer from (§1.7's register-before window, where C-22 is never
// attempted).
const (
	WorkspaceStateSuccess     = "success"
	WorkspaceStateStaleReplay = "stale_replay"
	WorkspaceStateFailure     = "failed"
)

// EndpointAgentAPI is the foreman_forward_total endpoint label for S29
// agent-API passthrough (§9.2).
const EndpointAgentAPI = "agent-api"

// S6 claim result label values (§9.2 foreman_delivery_claims_total,
// ADR-032): delivered = the payload was handed over; redelivered = the payload
// was handed over again after the confirmation window released a delivery that
// went unconfirmed; empty = an empty batch; empty_unconfirmed = an empty batch
// answering an entry whose delivery was committed and is still inside its
// confirmation window — the lost-response signature.
const (
	DeliveryClaimDelivered        = "delivered"
	DeliveryClaimRedelivered      = "redelivered"
	DeliveryClaimEmpty            = "empty"
	DeliveryClaimEmptyUnconfirmed = "empty_unconfirmed"
)

// WS side label values (§9.2 foreman_ws_connected).
const (
	WSSideServer = "server"
	WSSideJob    = "job"
)

type noopMetrics struct{}

func (noopMetrics) Forward(string, int, time.Duration) {}
func (noopMetrics) ClaimError(string)                  {}
func (noopMetrics) AuthFailure(string)                 {}
func (noopMetrics) WSConnect(string)                   {}
func (noopMetrics) WSDisconnect(string)                {}
func (noopMetrics) WSReconnect(string)                 {}
func (noopMetrics) ModelCatalogRelay(string, float64)  {}
func (noopMetrics) DeliveryClaim(string)               {}
func (noopMetrics) DeliveryLatency(float64)            {}
func (noopMetrics) SkillCache(string)                  {}
func (noopMetrics) SkillCacheSize(int, int64)          {}
func (noopMetrics) SkillCacheEviction(string)          {}
func (noopMetrics) RuntimeProfileRefresh(string)       {}
func (noopMetrics) WorkspaceStateRefresh(string)       {}

func orNoopMetrics(m Metrics) Metrics {
	if m == nil {
		return noopMetrics{}
	}
	return m
}
