package injectproxy

import (
	"fmt"
	"regexp"
	"sync"
	"time"
)

// Mode is one of the injection modes of the access-path contract (§4.2).
type Mode string

const (
	ModeNone        Mode = "none"
	ModeTerminal5xx Mode = "terminal-5xx"
	ModeUpstream5xx Mode = "upstream-5xx"
	ModeWSDrop      Mode = "ws-drop"
)

// Statuses POST /inject falls back to when the body leaves status out: the
// terminal-5xx case is the terminal endpoint's retry scenario (503), the
// upstream-5xx case the S29 passthrough one (500).
const (
	DefaultTerminalStatus = 503
	DefaultUpstreamStatus = 500
)

// maxDurationS bounds duration_s. It is not a test window anyone needs to
// exceed, and it keeps the window far inside int64 nanoseconds: beyond that
// the conversion is implementation-defined, and on amd64 the negative result
// would expire the injection before the first read.
const maxDurationS float64 = 1_000_000

// State is the injection state as the control plane reports it; GET
// /inject/state and both POST replies share this shape.
type State struct {
	Mode      string  `json:"mode"`
	PathRegex string  `json:"path_regex"`
	Status    int     `json:"status"`
	Until     *string `json:"until"`
}

// Request is POST /inject's body.
type Request struct {
	Mode      string  `json:"mode"`
	PathRegex string  `json:"path_regex"`
	Status    int     `json:"status"`
	DurationS float64 `json:"duration_s"`
}

// tunnelHandle is one established WS connection under the injector's watch: a
// ws-drop has to close every open one and refuse new upgrades for its window.
type tunnelHandle struct {
	path  string
	close func()
}

// Injector holds the single active injection (§4.2: at most one mode at a
// time) plus the WS tunnels a ws-drop closes.
//
// Expiry is lazy — every state read and every data-plane decision applies it —
// so an expired injection cannot outlive its window even though nothing runs
// on a timer.
type Injector struct {
	now func() time.Time

	mu      sync.Mutex
	mode    Mode
	pattern *regexp.Regexp
	source  string
	status  int
	until   time.Time
	tunnels map[*tunnelHandle]struct{}
}

// NewInjector builds an injector in the default zero-injection state.
func NewInjector() *Injector {
	return &Injector{now: time.Now, mode: ModeNone, tunnels: map[*tunnelHandle]struct{}{}}
}

// Set applies one injection. It takes effect immediately and replaces
// whatever was active: the contract keeps at most one mode at a time, and a
// leftover injection must not block the next scenario's setup.
func (in *Injector) Set(req Request) (State, error) {
	mode := Mode(req.Mode)
	status := req.Status
	switch mode {
	case ModeNone:
		return in.Reset(), nil
	case ModeTerminal5xx:
		if status == 0 {
			status = DefaultTerminalStatus
		}
	case ModeUpstream5xx:
		if status == 0 {
			status = DefaultUpstreamStatus
		}
	case ModeWSDrop:
		// status is meaningless for a connection-level drop.
		status = 0
	default:
		return State{}, fmt.Errorf("unknown mode %q (want none|terminal-5xx|upstream-5xx|ws-drop)", req.Mode)
	}
	if mode != ModeWSDrop && (status < 100 || status > 599) {
		return State{}, fmt.Errorf("status %d is outside 100..599", status)
	}
	// One range check rejects a negative window, a value whose nanoseconds
	// would overflow int64 (an implementation-defined duration that silently
	// expires the injection before it is read), and NaN.
	if !(req.DurationS >= 0 && req.DurationS <= maxDurationS) {
		return State{}, fmt.Errorf("duration_s %v is outside 0..%.0f", req.DurationS, maxDurationS)
	}

	var pattern *regexp.Regexp
	if req.PathRegex != "" {
		compiled, err := regexp.Compile(req.PathRegex)
		if err != nil {
			return State{}, fmt.Errorf("path_regex %q is not a valid regex: %v", req.PathRegex, err)
		}
		pattern = compiled
	}

	in.mu.Lock()
	in.mode = mode
	in.pattern = pattern
	in.source = req.PathRegex
	in.status = status
	in.until = time.Time{}
	if req.DurationS > 0 {
		in.until = in.now().Add(time.Duration(req.DurationS * float64(time.Second)))
	}
	var drop []func()
	if mode == ModeWSDrop {
		drop = in.collectDropsLocked()
	}
	state := in.stateLocked()
	in.mu.Unlock()

	// Tunnels are closed outside the lock: a close is a syscall on someone
	// else's connection, not state of ours.
	for _, closeFn := range drop {
		closeFn()
	}
	return state, nil
}

// Reset clears the injection; it is equivalent to mode=none (§4.2).
func (in *Injector) Reset() State {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.clearLocked()
	return in.stateLocked()
}

// State reports the active injection.
func (in *Injector) State() State {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.expireLocked()
	return in.stateLocked()
}

// HTTP5xxStatus reports the injected status for a request path, if a 5xx mode
// is active and matches it. The path is the request path without query.
func (in *Injector) HTTP5xxStatus(path string) (int, bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.expireLocked()
	if in.mode != ModeTerminal5xx && in.mode != ModeUpstream5xx {
		return 0, false
	}
	if !in.matchesLocked(path) {
		return 0, false
	}
	return in.status, true
}

// WSDropActive reports whether a ws-drop window is open for a WS upgrade on
// path: established tunnels are already closed, new upgrades have to be
// refused. HTTP requests are never affected (§4.2).
func (in *Injector) WSDropActive(path string) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.expireLocked()
	return in.mode == ModeWSDrop && in.matchesLocked(path)
}

// RegisterTunnel puts an established WS connection under the injector's watch
// and returns the deregistration for its teardown. A tunnel that is already
// covered by an open ws-drop window is closed immediately — registering after
// the drop must not let it slide through.
func (in *Injector) RegisterTunnel(path string, closeFn func()) func() {
	handle := &tunnelHandle{path: path, close: closeFn}

	in.mu.Lock()
	in.expireLocked()
	drop := in.mode == ModeWSDrop && in.matchesLocked(path)
	if !drop {
		in.tunnels[handle] = struct{}{}
	}
	in.mu.Unlock()

	if drop {
		closeFn()
		return func() {}
	}
	return func() {
		in.mu.Lock()
		delete(in.tunnels, handle)
		in.mu.Unlock()
	}
}

// collectDropsLocked collects the close functions of the matching tunnels and
// forgets them. Called with the lock held.
func (in *Injector) collectDropsLocked() []func() {
	var out []func()
	for handle := range in.tunnels {
		if in.matchesLocked(handle.path) {
			out = append(out, handle.close)
			delete(in.tunnels, handle)
		}
	}
	return out
}

func (in *Injector) matchesLocked(path string) bool {
	if in.pattern == nil {
		return true
	}
	return in.pattern.MatchString(path)
}

// expireLocked applies the duration window: an injection whose deadline has
// passed is gone, whether or not anyone reset it.
func (in *Injector) expireLocked() {
	if in.mode != ModeNone && !in.until.IsZero() && !in.now().Before(in.until) {
		in.clearLocked()
	}
}

func (in *Injector) clearLocked() {
	in.mode = ModeNone
	in.pattern = nil
	in.source = ""
	in.status = 0
	in.until = time.Time{}
}

func (in *Injector) stateLocked() State {
	state := State{Mode: string(in.mode), PathRegex: in.source, Status: in.status}
	if !in.until.IsZero() {
		until := in.until.UTC().Format(time.RFC3339Nano)
		state.Until = &until
	}
	return state
}
