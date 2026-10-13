package injectproxy

import (
	"math"
	"testing"
	"time"
)

func TestInjectorStartsInNone(t *testing.T) {
	state := NewInjector().State()
	if state.Mode != string(ModeNone) || state.PathRegex != "" || state.Status != 0 || state.Until != nil {
		t.Fatalf("initial state = %+v, want none/empty/0/null", state)
	}
}

func TestInjectorTerminal5xx(t *testing.T) {
	inj := NewInjector()
	state, err := inj.Set(Request{
		Mode:      string(ModeTerminal5xx),
		PathRegex: `^/api/daemon/tasks/[^/]+/(complete|fail|cancel-ack)$`,
		Status:    503,
		DurationS: 300,
	})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if state.Mode != string(ModeTerminal5xx) || state.Status != 503 {
		t.Fatalf("state = %+v", state)
	}
	if state.Until == nil {
		t.Fatal("until is null with a duration window")
	}
	until, err := time.Parse(time.RFC3339Nano, *state.Until)
	if err != nil {
		t.Fatalf("until %q is not RFC3339: %v", *state.Until, err)
	}
	if d := time.Until(until); d <= 0 || d > 301*time.Second {
		t.Fatalf("until is %v away, want ~300s", d)
	}

	if status, ok := inj.HTTP5xxStatus("/api/daemon/tasks/t-1/complete"); !ok || status != 503 {
		t.Fatalf("terminal path = %d/%v, want 503/true", status, ok)
	}
	if _, ok := inj.HTTP5xxStatus("/api/daemon/tasks/claim"); ok {
		t.Fatal("claim path matched a terminal-only injection")
	}
	if inj.WSDropActive("/api/daemon/ws") {
		t.Fatal("ws-drop reported active under terminal-5xx")
	}
}

func TestInjectorDefaultsStatusPerMode(t *testing.T) {
	inj := NewInjector()
	if _, err := inj.Set(Request{Mode: string(ModeTerminal5xx)}); err != nil {
		t.Fatalf("Set terminal: %v", err)
	}
	if status, ok := inj.HTTP5xxStatus("/anything"); !ok || status != DefaultTerminalStatus {
		t.Fatalf("terminal default = %d/%v, want %d", status, ok, DefaultTerminalStatus)
	}
	if _, err := inj.Set(Request{Mode: string(ModeUpstream5xx)}); err != nil {
		t.Fatalf("Set upstream: %v", err)
	}
	if status, ok := inj.HTTP5xxStatus("/api/daemon/tasks/whatever/status"); !ok || status != DefaultUpstreamStatus {
		t.Fatalf("upstream default = %d/%v, want %d", status, ok, DefaultUpstreamStatus)
	}
	if until := inj.State().Until; until != nil {
		t.Fatalf("until = %v without a duration window", *until)
	}
}

func TestInjectorReplacesTheActiveMode(t *testing.T) {
	inj := NewInjector()
	if _, err := inj.Set(Request{Mode: string(ModeTerminal5xx), Status: 503}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := inj.Set(Request{Mode: string(ModeWSDrop)}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	state := inj.State()
	if state.Mode != string(ModeWSDrop) || state.Status != 0 {
		t.Fatalf("state = %+v, want ws-drop with no status", state)
	}
	if _, ok := inj.HTTP5xxStatus("/api/daemon/tasks/claim"); ok {
		t.Fatal("the replaced 5xx mode is still active")
	}
	if !inj.WSDropActive("/api/daemon/ws") {
		t.Fatal("ws-drop does not apply to all paths when path_regex is empty")
	}
}

func TestInjectorExpiresOnItsWindow(t *testing.T) {
	inj := NewInjector()
	now := time.Now()
	inj.now = func() time.Time { return now }

	if _, err := inj.Set(Request{Mode: string(ModeTerminal5xx), Status: 503, DurationS: 120}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	now = now.Add(119 * time.Second)
	if _, ok := inj.HTTP5xxStatus("/p"); !ok {
		t.Fatal("injection expired before its window")
	}
	now = now.Add(2 * time.Second)
	if _, ok := inj.HTTP5xxStatus("/p"); ok {
		t.Fatal("injection survived its window")
	}
	state := inj.State()
	if state.Mode != string(ModeNone) || state.PathRegex != "" || state.Until != nil {
		t.Fatalf("expired state = %+v, want none", state)
	}
}

func TestInjectorReset(t *testing.T) {
	inj := NewInjector()
	if _, err := inj.Set(Request{Mode: string(ModeWSDrop), DurationS: 600}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	state := inj.Reset()
	if state.Mode != string(ModeNone) || state.Until != nil {
		t.Fatalf("reset state = %+v, want none", state)
	}
	if inj.WSDropActive("/api/daemon/ws") {
		t.Fatal("ws-drop still active after reset")
	}
}

func TestInjectorRejectsBadRequests(t *testing.T) {
	for name, req := range map[string]Request{
		"unknown mode":   {Mode: "explode"},
		"bad regex":      {Mode: string(ModeTerminal5xx), PathRegex: `^/api/($`},
		"status 99":      {Mode: string(ModeTerminal5xx), Status: 99},
		"status 600":     {Mode: string(ModeUpstream5xx), Status: 600},
		"duration -1":    {Mode: string(ModeWSDrop), DurationS: -1},
		"duration 1e300": {Mode: string(ModeTerminal5xx), Status: 503, DurationS: 1e300},
		"duration NaN":   {Mode: string(ModeTerminal5xx), Status: 503, DurationS: math.NaN()},
		"empty mode":     {},
		"mode mismatch":  {Mode: "none ", Status: 503},
	} {
		if _, err := NewInjector().Set(req); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}

// TestInjectorAcceptsTheLongestWindow pins the accepted end of the bound: a
// duration at the cap must still expire by itself, not overflow into the past.
func TestInjectorAcceptsTheLongestWindow(t *testing.T) {
	inj := NewInjector()
	now := time.Now()
	inj.now = func() time.Time { return now }

	state, err := inj.Set(Request{Mode: string(ModeTerminal5xx), Status: 503, DurationS: maxDurationS})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	until, err := time.Parse(time.RFC3339Nano, *state.Until)
	if err != nil {
		t.Fatalf("until %q: %v", *state.Until, err)
	}
	if !until.After(now) {
		t.Fatalf("until = %v, want a future deadline", until)
	}
	if _, ok := inj.HTTP5xxStatus("/p"); !ok {
		t.Fatal("injection expired immediately")
	}
	now = now.Add(time.Duration(maxDurationS)*time.Second + time.Second)
	if _, ok := inj.HTTP5xxStatus("/p"); ok {
		t.Fatal("injection survived its window")
	}
}

func TestInjectorDropsMatchingTunnels(t *testing.T) {
	inj := NewInjector()
	var droppedA, droppedB, droppedC int
	deregA := inj.RegisterTunnel("/api/daemon/ws", func() { droppedA++ })
	inj.RegisterTunnel("/api/daemon/ws", func() { droppedB++ })
	inj.RegisterTunnel("/other/ws", func() { droppedC++ })

	if _, err := inj.Set(Request{Mode: string(ModeWSDrop), PathRegex: `^/api/daemon/ws$`}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if droppedA != 1 || droppedB != 1 {
		t.Fatalf("matching tunnels dropped %d/%d times, want 1/1", droppedA, droppedB)
	}
	if droppedC != 0 {
		t.Fatalf("non-matching tunnel dropped %d times", droppedC)
	}

	// A connection that registers while the window is open is dropped too.
	inj.RegisterTunnel("/api/daemon/ws", func() { droppedA++ })
	if droppedA != 2 {
		t.Fatalf("late-registered tunnel dropped %d times, want 2", droppedA)
	}

	// Starting a second window must not re-close tunnels that are gone, and a
	// deregistered tunnel must be forgotten.
	inj.Reset()
	var droppedD int
	deregD := inj.RegisterTunnel("/api/daemon/ws", func() { droppedD++ })
	deregD()
	if _, err := inj.Set(Request{Mode: string(ModeWSDrop)}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if droppedD != 0 {
		t.Fatalf("deregistered tunnel dropped %d times", droppedD)
	}
	deregA()
}
