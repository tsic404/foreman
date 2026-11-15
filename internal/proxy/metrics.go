package proxy

import "time"

// Metrics is the proxy module's observability seam. The observability module
// provides the Prometheus implementation; nil degrades to no-ops.
type Metrics interface {
	Forward(endpoint string, code int, d time.Duration)
	ClaimError(code string)
	AuthFailure(reason string)
	DeliveryClaim(result string)
	DeliveryLatency(seconds float64)
}

type noopMetrics struct{}

func (noopMetrics) Forward(string, int, time.Duration) {}
func (noopMetrics) ClaimError(string)                  {}
func (noopMetrics) AuthFailure(string)                 {}
func (noopMetrics) DeliveryClaim(string)               {}
func (noopMetrics) DeliveryLatency(float64)            {}

func orNoopMetrics(m Metrics) Metrics {
	if m == nil {
		return noopMetrics{}
	}
	return m
}