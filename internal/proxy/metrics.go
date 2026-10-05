package proxy

import "time"

// Metrics is the proxy module's observability seam (observability.md). The
// observability module provides the Prometheus implementation; nil degrades
// to no-ops. Endpoint label values are the scheduler.Endpoint strings plus
// EndpointAgentAPI for S29 passthrough.
type Metrics interface {
	// Forward records one forwarded call and its outcome (§metrics
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
}

// EndpointAgentAPI is the foreman_forward_total endpoint label for S29
// agent-API passthrough (observability.md).
const EndpointAgentAPI = "agent-api"

// WS side label values (observability.md foreman_ws_connected).
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

func orNoopMetrics(m Metrics) Metrics {
	if m == nil {
		return noopMetrics{}
	}
	return m
}
