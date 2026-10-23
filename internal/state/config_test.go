package state

import (
	"testing"
	"time"
)

// TestLoadConfigCallTimeout pins the store-side answer bound's deployment
// entry: unset means the default, a value overrides it, and a non-positive
// value is refused like the other duration knobs.
func TestLoadConfigCallTimeout(t *testing.T) {
	cfg, err := LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.CallTimeout != DefaultCallTimeout {
		t.Fatalf("default call timeout = %s, want %s", cfg.CallTimeout, DefaultCallTimeout)
	}

	cfg, err = LoadConfig(func(key string) string {
		if key == EnvCallTimeout {
			return "750ms"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.CallTimeout != 750*time.Millisecond {
		t.Fatalf("call timeout = %s, want 750ms", cfg.CallTimeout)
	}

	if _, err := LoadConfig(func(key string) string {
		if key == EnvCallTimeout {
			return "0s"
		}
		return ""
	}); err == nil {
		t.Fatal("a non-positive call timeout was accepted")
	}
}
