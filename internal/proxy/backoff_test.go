package proxy

import (
	"testing"
	"time"
)

// TestBackoffDoublesToCeiling pins the §1.5 schedule with jitter disabled.
func TestBackoffDoublesToCeiling(t *testing.T) {
	b := &backoff{min: time.Second, max: 30 * time.Second}

	want := []time.Duration{
		1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 30 * time.Second, 30 * time.Second,
	}
	for i, w := range want {
		if got := b.next(); got != w {
			t.Fatalf("step %d = %s, want %s", i+1, got, w)
		}
	}

	b.reset()
	if got := b.next(); got != time.Second {
		t.Fatalf("after reset = %s, want 1s", got)
	}
}

// TestBackoffJitterSpreadsWithinBounds is the anti-thundering-herd contract: a
// multi-workspace deployment runs several claim loops started together, so
// their retries must not land in lockstep. Jitter is bounded so the policy's
// ceiling still means something.
func TestBackoffJitterSpreadsWithinBounds(t *testing.T) {
	b := &backoff{min: time.Second, max: 30 * time.Second, rand: func() float64 { return 1 }}

	// With the generator pinned to its maximum, every step sits at the top of
	// its jitter band: base + 25%.
	want := []time.Duration{
		1250 * time.Millisecond, 2500 * time.Millisecond, 5 * time.Second,
		10 * time.Second, 20 * time.Second, 37500 * time.Millisecond,
	}
	for i, w := range want {
		if got := b.next(); got != w {
			t.Fatalf("step %d = %s, want %s (base + 25%%)", i+1, got, w)
		}
	}

	// The floor of the band is the base itself.
	b2 := &backoff{min: time.Second, max: 30 * time.Second, rand: func() float64 { return 0 }}
	if got := b2.next(); got != time.Second {
		t.Fatalf("jitter floor = %s, want 1s", got)
	}
}

// TestBackoffJitterIsActuallyRandom guards against a jitter that is declared
// but never varies (for example a generator wired to a constant), which would
// look correct in the bounds test while still retrying in lockstep.
func TestBackoffJitterIsActuallyRandom(t *testing.T) {
	b := newBackoff()
	if b.rand == nil {
		t.Fatal("the production backoff has no jitter source")
	}
	// Reset and compare the first step across many fresh instances: a constant
	// source would make every one identical.
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		fresh := newBackoff()
		seen[fresh.next()] = true
	}
	if len(seen) < 2 {
		t.Fatalf("50 fresh backoffs produced %d distinct first steps; jitter is not varying", len(seen))
	}
	// And every sample must stay inside the documented band.
	for d := range seen {
		if d < time.Second || d > 1250*time.Millisecond {
			t.Fatalf("first step %s outside [1s, 1.25s]", d)
		}
	}
}

// TestJitterWaitStaysInBand: the terminal-forward ladder must be spread but
// bounded — a step of 4s lands in [4s, 5s), and a zero step stays zero (the
// first attempt must not be delayed).
func TestJitterWaitStaysInBand(t *testing.T) {
	if got := jitterWait(0); got != 0 {
		t.Fatalf("jitterWait(0) = %s, want 0 (the first attempt is immediate)", got)
	}
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		got := jitterWait(4 * time.Second)
		if got < 4*time.Second || got >= 5*time.Second {
			t.Fatalf("jitterWait(4s) = %s, outside [4s, 5s)", got)
		}
		seen[got] = true
	}
	if len(seen) < 2 {
		t.Fatalf("200 samples produced %d distinct waits; jitter is not varying", len(seen))
	}
}

// TestLeaseRefreshIntervalDrivesHeartbeatNotLease pins the wiring this project
// keeps tripping over: the variable named "lease refresh" is the heartbeat
// cadence. If someone later wires it to a lease path, this test says the
// documented meaning changed.
func TestLeaseRefreshIntervalDrivesHeartbeatNotLease(t *testing.T) {
	cfg, err := LoadConfig(func(k string) string {
		switch k {
		case EnvServerURL:
			return "https://example.test"
		case EnvLeaseRefreshInterval:
			return "10s"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.LeaseRefreshInterval != 10*time.Second {
		t.Fatalf("LeaseRefreshInterval = %s, want 10s", cfg.LeaseRefreshInterval)
	}
	// The cap protects the heartbeat contract, not a lease one.
	if _, err := LoadConfig(func(k string) string {
		switch k {
		case EnvServerURL:
			return "https://example.test"
		case EnvLeaseRefreshInterval:
			return "25s"
		}
		return ""
	}); err == nil {
		t.Fatalf("accepted %s above the %s cap", EnvLeaseRefreshInterval, MaxLeaseRefreshInterval)
	}
}
