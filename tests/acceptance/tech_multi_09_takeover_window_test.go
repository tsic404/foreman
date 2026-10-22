// TC-tech-multi-09 / AC-99: the takeover window's startup self-check. A quiet
// period shorter than one lease renewal tick ends before the old holder's
// next renewal clears its hold, so the loader refuses that combination and
// names the inequality with both actual values; the boundary, the shipped
// defaults and any longer window start.
package acceptance

import (
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/coordination"
)

func TestTechMulti09TakeoverWindowSelfCheck(t *testing.T) {
	env := func(kv map[string]string) func(string) string {
		return func(key string) string { return kv[key] }
	}

	// The shipped defaults are a valid pair.
	cfg, err := coordination.LoadConfig(nil)
	if err != nil {
		t.Fatalf("the default configuration was refused: %v", err)
	}
	if tick := cfg.LeaseTTL / 3; cfg.TakeoverWindow < tick {
		t.Fatalf("default window %s is shorter than the renewal tick %s of the default lease %s",
			cfg.TakeoverWindow, tick, cfg.LeaseTTL)
	}

	// Below one renewal tick: refused, with the inequality and both values.
	_, err = coordination.LoadConfig(env(map[string]string{
		coordination.EnvLeaseTTL:       "9s",
		coordination.EnvTakeoverWindow: "2.999s",
	}))
	if err == nil {
		t.Fatal("a takeover window below one renewal tick was accepted")
	}
	for _, want := range []string{coordination.EnvTakeoverWindow, coordination.EnvLeaseTTL, ">=", "2.999s", "9s", "3s"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
	}

	// A longer lease with the default window is the same short window.
	if _, err := coordination.LoadConfig(env(map[string]string{coordination.EnvLeaseTTL: "60s"})); err == nil {
		t.Fatal("a 60s lease with the default window was accepted")
	}

	// The boundary and any longer window start.
	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{"boundary", map[string]string{coordination.EnvLeaseTTL: "9s", coordination.EnvTakeoverWindow: "3s"}},
		{"above", map[string]string{coordination.EnvLeaseTTL: "9s", coordination.EnvTakeoverWindow: "5s"}},
		{"longer lease, window scaled", map[string]string{coordination.EnvLeaseTTL: "60s", coordination.EnvTakeoverWindow: "20s"}},
	} {
		if _, err := coordination.LoadConfig(env(tc.env)); err != nil {
			t.Errorf("%s: a compliant window was refused: %v", tc.name, err)
		}
	}
}
