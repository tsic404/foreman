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
}

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

func orNoopMetrics(m Metrics) Metrics {
	if m == nil {
		return noopMetrics{}
	}
	return m
}
