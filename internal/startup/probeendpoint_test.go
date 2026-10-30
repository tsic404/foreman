package startup

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// probeEndpointLog decodes the JSON log lines the waiter emits.
func probeEndpointLog(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			t.Fatalf("log line %q is not JSON: %v", line, err)
		}
		out = append(out, fields)
	}
	return out
}

func probeEndpointEvent(t *testing.T, buf *bytes.Buffer, msg string) map[string]any {
	t.Helper()
	for _, fields := range probeEndpointLog(t, buf) {
		if fields["msg"] == msg {
			return fields
		}
	}
	return nil
}

// TestWaitForProbeEndpointWaitsForAReadyAnswer pins the cold-start gate: the
// wait only returns once the Job-side address answers 2xx, and it polls the
// health path the ops surface already serves (§9.3).
func TestWaitForProbeEndpointWaitsForAReadyAnswer(t *testing.T) {
	var calls atomic.Int64
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	logs := &bytes.Buffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))
	if err := waitForProbeEndpoint(t.Context(), srv.URL, log, 5*time.Second, 10*time.Millisecond, time.Second); err != nil {
		t.Fatalf("waitForProbeEndpoint: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3 (two not-ready answers, then ready)", got)
	}
	for _, path := range paths {
		if path != probeEndpointHealthPath {
			t.Fatalf("polled %q, want the health endpoint %q", path, probeEndpointHealthPath)
		}
	}
	event := probeEndpointEvent(t, logs, "probe.endpoint_ready")
	if event == nil {
		t.Fatalf("the ready transition must be visible: %s", logs.String())
	}
}

// TestWaitForProbeEndpointLeavesTheVerdictToTheProbe pins the timeout face: a
// Service that never answers does not invent a new failure — the probe still
// runs and its window reports the fixed reason classes (§10).
func TestWaitForProbeEndpointLeavesTheVerdictToTheProbe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	logs := &bytes.Buffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))
	start := time.Now()
	if err := waitForProbeEndpoint(t.Context(), srv.URL, log, 150*time.Millisecond, 10*time.Millisecond, time.Second); err != nil {
		t.Fatalf("an unreachable endpoint must not fail the gate itself: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("returned after %s, want the wait window to run out", elapsed)
	}
	event := probeEndpointEvent(t, logs, "probe.endpoint_wait_timeout")
	if event == nil {
		t.Fatalf("the timeout must be visible: %s", logs.String())
	}
	if event["err"] == nil {
		t.Errorf("the timeout event must carry the last attempt's error: %+v", event)
	}
}

// TestWaitForProbeEndpointStopsOnContextEnd pins the process-context face: a
// canceled startup must not leave the wait polling.
func TestWaitForProbeEndpointStopsOnContextEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := waitForProbeEndpoint(ctx, srv.URL, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)), 5*time.Second, 10*time.Millisecond, time.Second)
	if err == nil {
		t.Fatal("a canceled context must surface as an error")
	}
}

// TestWaitForProbeEndpointSkipsAnUnparseableAddress pins the configuration
// face: the gate does not pre-empt the probe's own failure classification.
func TestWaitForProbeEndpointSkipsAnUnparseableAddress(t *testing.T) {
	logs := &bytes.Buffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))
	if err := waitForProbeEndpoint(t.Context(), "foreman.foreman.svc:8080", log, time.Second, 10*time.Millisecond, time.Second); err != nil {
		t.Fatalf("unparseable address must be skipped, not fatal: %v", err)
	}
	if event := probeEndpointEvent(t, logs, "probe.endpoint_wait_skipped"); event == nil {
		t.Fatalf("the skip must be visible: %s", logs.String())
	}
}
