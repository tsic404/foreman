package auth

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const testKeyHex = "4242424242424242424242424242424242424242424242424242424242424242"

func envFrom(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{EnvJobTokenKey: testKeyHex}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.Key) != MinKeyBytes {
		t.Fatalf("key length = %d, want %d", len(cfg.Key), MinKeyBytes)
	}
	if cfg.TokenTTL != DefaultJobTokenTTL {
		t.Fatalf("TokenTTL = %s, want %s", cfg.TokenTTL, DefaultJobTokenTTL)
	}
	if cfg.TaskMaxDuration != DefaultTaskMaxDuration {
		t.Fatalf("TaskMaxDuration = %s, want %s", cfg.TaskMaxDuration, DefaultTaskMaxDuration)
	}
}

func TestLoadConfigRequiresKey(t *testing.T) {
	if _, err := LoadConfig(envFrom(map[string]string{})); err == nil || !strings.Contains(err.Error(), EnvJobTokenKey) {
		t.Fatalf("err = %v, want a %s error", err, EnvJobTokenKey)
	}
}

func TestLoadConfigRejectsBadKeyHex(t *testing.T) {
	if _, err := LoadConfig(envFrom(map[string]string{EnvJobTokenKey: "not-hex"})); err == nil {
		t.Fatal("expected a hex decode error")
	}
}

func TestLoadConfigRejectsShortKey(t *testing.T) {
	_, err := LoadConfig(envFrom(map[string]string{EnvJobTokenKey: strings.Repeat("42", 16)}))
	if !errors.Is(err, ErrKeyTooShort) {
		t.Fatalf("err = %v, want ErrKeyTooShort", err)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{
		EnvJobTokenKey:     testKeyHex,
		EnvJobTokenTTL:     "48h",
		EnvTaskMaxDuration: "3600",
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.TokenTTL != 48*time.Hour || cfg.TaskMaxDuration != time.Hour {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestLoadConfigStartupSelfCheck(t *testing.T) {
	cases := map[string]struct {
		ttl     string
		max     string
		wantErr bool
	}{
		"ttl below default max":          {"1h", "", true},
		"ttl equal to max":               {"1h", "3600", false},
		"ttl above max":                  {"24h", "3600", false},
		"max disabled":                   {"1s", "0", false},
		"zero ttl with max disabled":     {"0s", "0", true},
		"negative ttl with max disabled": {"-1h", "0", true},
	}
	for name, tc := range cases {
		env := map[string]string{EnvJobTokenKey: testKeyHex, EnvJobTokenTTL: tc.ttl}
		if tc.max != "" {
			env[EnvTaskMaxDuration] = tc.max
		}
		_, err := LoadConfig(envFrom(env))
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr = %v", name, err, tc.wantErr)
		}
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	cases := map[string]map[string]string{
		"bad ttl":      {EnvJobTokenKey: testKeyHex, EnvJobTokenTTL: "abc"},
		"bad max":      {EnvJobTokenKey: testKeyHex, EnvTaskMaxDuration: "abc"},
		"negative max": {EnvJobTokenKey: testKeyHex, EnvTaskMaxDuration: "-5"},
	}
	for name, env := range cases {
		if _, err := LoadConfig(envFrom(env)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
