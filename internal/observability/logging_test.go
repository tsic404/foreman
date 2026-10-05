package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Fatalf("default log level = %v, want info", cfg.LogLevel)
	}
	if !cfg.MetricsEnabled {
		t.Fatal("default metrics enabled = false, want true")
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	env := map[string]string{
		EnvLogLevel:       "debug",
		EnvMetricsEnabled: "false",
	}
	cfg, err := LoadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Fatalf("log level = %v, want debug", cfg.LogLevel)
	}
	if cfg.MetricsEnabled {
		t.Fatal("metrics enabled = true, want false")
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	for _, env := range []map[string]string{
		{EnvLogLevel: "trace"},
		{EnvMetricsEnabled: "maybe"},
	} {
		if _, err := LoadConfig(func(k string) string { return env[k] }); err == nil {
			t.Fatalf("env %v: expected error", env)
		}
	}
}

// logLine decodes one JSON log line into a map for field assertions.
func logLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &m); err != nil {
		t.Fatalf("log line is not JSON: %v (%q)", err, buf.String())
	}
	return m
}

func TestLoggerFixedFields(t *testing.T) {
	var buf bytes.Buffer
	NewLogger(&buf, slog.LevelInfo, "scheduler").Info("task.claimed", "task_id", "t-1")
	m := logLine(t, &buf)
	for _, key := range []string{"ts", "level", "msg", "component"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("missing fixed field %q in %v", key, m)
		}
	}
	if m["msg"] != "task.claimed" || m["component"] != "scheduler" {
		t.Fatalf("unexpected fields: %v", m)
	}
	if _, ok := m["time"]; ok {
		t.Fatalf("slog default time key not renamed: %v", m)
	}
}

func TestLoggerLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(&buf, slog.LevelInfo, "proxy")
	l.Debug("daemon.heartbeat")
	if buf.Len() != 0 {
		t.Fatalf("debug line emitted at info level: %q", buf.String())
	}
	l.Info("task.started")
	if buf.Len() == 0 {
		t.Fatal("info line suppressed at info level")
	}
}

func TestLoggerRedactsTokens(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(&buf, slog.LevelInfo, "auth")
	l.Info("token is fmj_abcdef123456 and mdt_xyz987654321",
		"detail", "mul_AAAAAAAA-bbbb mat_cccccccc1234",
		"auth_token", "mat_supersecretvalue",
		"nested", slog.Group("inner", "tok", "fmj_deadbeef99"))
	out := buf.String()
	for _, tok := range []string{"fmj_abcdef123456", "mdt_xyz987654321", "mul_AAAAAAAA", "mat_cccccccc1234", "mat_supersecretvalue", "fmj_deadbeef99"} {
		if strings.Contains(out, tok) {
			t.Fatalf("token %q leaked in %q", tok, out)
		}
	}
	m := logLine(t, &buf)
	if m["auth_token"] != redactedValue {
		t.Fatalf("auth_token attr = %v, want redacted", m["auth_token"])
	}
}

func TestLoggerKeepsNonTokenStrings(t *testing.T) {
	var buf bytes.Buffer
	NewLogger(&buf, slog.LevelInfo, "scheduler").Info("job.created", "job_name", "fm-12345")
	m := logLine(t, &buf)
	if m["job_name"] != "fm-12345" {
		t.Fatalf("non-token attr mangled: %v", m)
	}
}

// TestLoggerRedactsPreboundAttrs covers the With path: attrs bound at
// logger construction bypass Handle, so WithAttrs must scrub them itself.
func TestLoggerRedactsPreboundAttrs(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(&buf, slog.LevelInfo, "proxy").
		With("auth_token", "mat_preboundtoken99", "job_tok", "fmj_boundtoken123")
	l.Info("daemon.registered", "task_id", "t-1")
	out := buf.String()
	if strings.Contains(out, "mat_preboundtoken99") || strings.Contains(out, "fmj_boundtoken123") {
		t.Fatalf("pre-bound token leaked: %q", out)
	}
	m := logLine(t, &buf)
	if m["auth_token"] != redactedValue {
		t.Fatalf("pre-bound auth_token = %v, want redacted", m["auth_token"])
	}
}
