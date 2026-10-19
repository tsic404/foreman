package observability

import (
	"bytes"
	"encoding/json"
	"errors"
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
}

func TestLoadConfigFromEnv(t *testing.T) {
	env := map[string]string{
		EnvLogLevel: "debug",
	}
	cfg, err := LoadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Fatalf("log level = %v, want debug", cfg.LogLevel)
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	for _, env := range []map[string]string{
		{EnvLogLevel: "trace"},
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

// TestLoggerRedactsErrorAttrs covers the dominant "err", err call shape:
// the value is an error, not a string, so it lands in KindAny. Before the
// KindAny fix the token rode through verbatim; the rendered text must now be
// scrubbed like any other string.
func TestLoggerRedactsErrorAttrs(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(&buf, slog.LevelInfo, "proxy")
	l.Error("upstream.failed",
		"err", errors.New("upstream rejected Bearer fmj_errorleak12345"),
		slog.Group("nested", "err", errors.New("peer echoed mat_groupleak12345")))
	out := buf.String()
	for _, tok := range []string{"fmj_errorleak12345", "mat_groupleak12345"} {
		if strings.Contains(out, tok) {
			t.Fatalf("token %q leaked in %q", tok, out)
		}
	}
	m := logLine(t, &buf)
	if got, want := m["err"], "upstream rejected Bearer "+redactedValue; got != want {
		t.Fatalf("err = %v, want %q", got, want)
	}
}

// namedInt is a non-native KindAny value: a named type whose underlying type
// is numeric does not become KindInt64.
type namedInt int

// unmarshalableStringer forces the fmt.Sprint fallback: json.Marshal rejects
// it, and its String method carries a token.
type unmarshalableStringer struct{ token string }

func (u unmarshalableStringer) String() string { return "auth " + u.token }

func (unmarshalableStringer) MarshalJSON() ([]byte, error) {
	return nil, errors.New("unmarshalable")
}

// TestLoggerRedactsNonStringAnyAttrs covers KindAny values that are neither
// strings nor errors (structs, named numerics): the value keeps its JSON shape
// and the encoded text is scrubbed, so a token inside a struct cannot leak.
func TestLoggerRedactsNonStringAnyAttrs(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(&buf, slog.LevelInfo, "scheduler")
	l.Info("relay.failed",
		"cause", struct{ Detail string }{Detail: "peer echoed mat_structleak1234"},
		"attempt", namedInt(7),
		"fallback", unmarshalableStringer{token: "mul_fallbackleak1234"})
	out := buf.String()
	for _, tok := range []string{"mat_structleak1234", "mul_fallbackleak1234"} {
		if strings.Contains(out, tok) {
			t.Fatalf("token %q leaked in %q", tok, out)
		}
	}
	m := logLine(t, &buf)
	cause, ok := m["cause"].(map[string]any)
	if !ok || cause["Detail"] != "peer echoed "+redactedValue {
		t.Fatalf("cause = %v, want the JSON object with the token replaced", m["cause"])
	}
	if got := m["attempt"]; got != float64(7) {
		t.Fatalf("attempt = %v, want the numeric 7", got)
	}
	if got := m["fallback"]; got != "auth "+redactedValue {
		t.Fatalf("fallback = %v, want the fmt.Sprint text with the token replaced", got)
	}
}

// marshalerError is both an error and a json.Marshaler. slog.JSONHandler
// renders it through MarshalJSON, not Error(), so the redactor must keep that
// JSON shape while scrubbing the encoded text.
type marshalerError struct{ token string }

func (e marshalerError) Error() string { return "raw " + e.token }

func (e marshalerError) MarshalJSON() ([]byte, error) {
	return []byte(`{"detail":"echoed ` + e.token + `"}`), nil
}

// TestLoggerRedactsMarshalerErrorAttrs pins the dispatch exception: a
// Marshaler error keeps its JSON shape (not Error() text) and is still
// scrubbed, so removing the exception cannot silently change the rendering.
func TestLoggerRedactsMarshalerErrorAttrs(t *testing.T) {
	var buf bytes.Buffer
	NewLogger(&buf, slog.LevelInfo, "proxy").
		Error("upstream.failed", "err", marshalerError{token: "fmj_marshalerleak123"})
	out := buf.String()
	if strings.Contains(out, "fmj_marshalerleak123") {
		t.Fatalf("token leaked in %q", out)
	}
	m := logLine(t, &buf)
	errObj, ok := m["err"].(map[string]any)
	if !ok || errObj["detail"] != "echoed "+redactedValue {
		t.Fatalf("err = %v, want the JSON object with the token replaced", m["err"])
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
		With("auth_token", "mat_preboundtoken99", "job_tok", "fmj_boundtoken123",
			"err", errors.New("renewal failed for mdt_prebounderr1234"))
	l.Info("daemon.registered", "task_id", "t-1")
	out := buf.String()
	if strings.Contains(out, "mat_preboundtoken99") || strings.Contains(out, "fmj_boundtoken123") ||
		strings.Contains(out, "mdt_prebounderr1234") {
		t.Fatalf("pre-bound token leaked: %q", out)
	}
	m := logLine(t, &buf)
	if m["auth_token"] != redactedValue {
		t.Fatalf("pre-bound auth_token = %v, want redacted", m["auth_token"])
	}
}
