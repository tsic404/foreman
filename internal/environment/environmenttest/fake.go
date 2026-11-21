// Package environmenttest provides the scriptable probe and rule-reference
// fixtures the environment tests drive. It is test support, not production
// code: the scenarios inject the failures a real rsbxd cannot be asked to
// produce on demand.
package environmenttest

import (
	"context"
	"sync"

	"github.com/tsic404/foreman/internal/environment"
)

// Probe is a scriptable environment.Probe. Inspect, when set, decides every
// answer; otherwise Result and Err are returned. Remote models the target's
// files so a test can prove retirement does not touch them.
type Probe struct {
	mu sync.Mutex

	InspectFunc func(target environment.Target) (environment.ProbeResult, error)
	Result      environment.ProbeResult
	Err         error

	Calls      int
	LastTarget environment.Target
	Remote     map[string]string
}

// NewProbe returns a probe with an empty remote tree.
func NewProbe() *Probe {
	return &Probe{Remote: map[string]string{}}
}

// Inspect answers one verification attempt.
func (p *Probe) Inspect(ctx context.Context, target environment.Target) (environment.ProbeResult, error) {
	p.mu.Lock()
	p.Calls++
	p.LastTarget = target
	inspect := p.InspectFunc
	result := p.Result
	err := p.Err
	p.mu.Unlock()
	if inspect != nil {
		return inspect(target)
	}
	return result, err
}

// CallCount answers how many verification attempts were made.
func (p *Probe) CallCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Calls
}

// RemoteFile reads one modelled remote file.
func (p *Probe) RemoteFile(path string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, ok := p.Remote[path]
	return value, ok
}
