package injectproxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// logLine decodes one JSON log line for field assertions.
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
	NewLogger(&buf, slog.LevelInfo, "injectproxy").Info("injectproxy started", "data_addr", "127.0.0.1:8080")
	line := logLine(t, &buf)
	for _, key := range []string{"ts", "level", "msg", "component"} {
		if _, ok := line[key]; !ok {
			t.Fatalf("missing fixed field %q in %v", key, line)
		}
	}
	if _, ok := line["time"]; ok {
		t.Fatalf("slog's default time key was not renamed: %v", line)
	}
}

func TestLoggerFiltersBelowLevel(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, slog.LevelInfo, "injectproxy")
	log.Debug("request head read ended")
	if buf.Len() != 0 {
		t.Fatalf("debug line emitted at info level: %q", buf.String())
	}
}

func TestLoggerRedactsCredentials(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, slog.LevelInfo, "injectproxy")
	log.Info("relay failed for fmj_abcdef123456 on mdt_xyz987654321",
		"detail", "mul_AAAAAAAA-bbbb mat_cccccccc1234",
		"auth_token", "mat_supersecretvalue",
		"nested", slog.Group("inner", "tok", "fmj_deadbeef99"))
	out := buf.String()
	for _, token := range []string{"fmj_abcdef123456", "mdt_xyz987654321", "mul_AAAAAAAA", "mat_cccccccc1234", "mat_supersecretvalue", "fmj_deadbeef99"} {
		if strings.Contains(out, token) {
			t.Fatalf("token %q leaked in %q", token, out)
		}
	}
}

func TestLoggerRedactsErrorAttrs(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, slog.LevelInfo, "injectproxy")
	// The errors on the data path are built from wire bytes; slog's JSON
	// handler writes err.Error() verbatim unless the handler rewrites it.
	log.Warn("malformed header line", "err", errors.New(`malformed header line "Authorization Bearer fmj_leakme12345678"`))
	log.Warn("wrapped", "err", fmt.Errorf("read head: %w", errors.New("token mdt_leakme9876543210 rejected")))
	out := buf.String()
	for _, token := range []string{"fmj_leakme12345678", "mdt_leakme9876543210"} {
		if strings.Contains(out, token) {
			t.Fatalf("token %q leaked in %q", token, out)
		}
	}
	if !strings.Contains(out, "[redacted]") {
		t.Fatalf("nothing was redacted: %q", out)
	}
}

func TestLoggerRedactsPreboundAttrs(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, slog.LevelInfo, "injectproxy").
		With("auth_header", "Bearer mdt_preboundvalue99")
	log.Info("request injected")
	if strings.Contains(buf.String(), "mdt_preboundvalue99") {
		t.Fatalf("pre-bound credential leaked: %q", buf.String())
	}
}

func TestLoggerKeepsNonCredentialValues(t *testing.T) {
	var buf bytes.Buffer
	NewLogger(&buf, slog.LevelInfo, "injectproxy").Info("request injected", "path", "/api/daemon/tasks/t-1/complete", "status", 503)
	line := logLine(t, &buf)
	if line["path"] != "/api/daemon/tasks/t-1/complete" || line["status"] != float64(503) {
		t.Fatalf("log line mangled: %v", line)
	}
}
