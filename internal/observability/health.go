package observability

import (
	"sort"
	"sync"
)

// Health is the liveness view the health endpoint serves (§9.3: the health
// endpoint is the input of the liveness probe). A component registers while it
// runs; a registered component that stopped means the process can no longer do
// its job, so the probe must restart it instead of reading a healthy process.
type Health struct {
	mu      sync.Mutex
	running map[string]bool
}

// NewHealth builds an empty liveness view.
func NewHealth() *Health {
	return &Health{running: map[string]bool{}}
}

// Running marks component as running and returns the function its owner calls
// when the component stops. Registering is the criticality statement: only
// loops the process cannot serve without are mounted here.
func (h *Health) Running(component string) (stopped func()) {
	if h == nil {
		return func() {}
	}
	h.mu.Lock()
	h.running[component] = true
	h.mu.Unlock()
	return func() {
		h.mu.Lock()
		h.running[component] = false
		h.mu.Unlock()
	}
}

// Stopped lists the registered components that are no longer running, sorted
// for a stable response.
func (h *Health) Stopped() []string {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var stopped []string
	for component, running := range h.running {
		if !running {
			stopped = append(stopped, component)
		}
	}
	sort.Strings(stopped)
	return stopped
}
