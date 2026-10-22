// TC-tech-ops-01 (AC-65, AC-66): the config-error face fails the process
// explicitly and the health endpoint's liveness face follows the critical
// loops. A controlled upstream answers the workspace credential check (C-17)
// with 403 — the credential/identity mismatch §1.5 places at the startup
// self-check, never a per-response-code verdict at runtime (ADR-049: runtime
// 401/403 backs off and retries, AC-86). The check is not retried: one request,
// the loop stops, the component.failed event names it with the error summary,
// and the supervisor hands main the error it exits non-zero on. In the
// serviceable window before the other loops drain, /foreman/healthz answers
// 503 and lists the stopped component; while every loop runs it answers 200.
//
// The health/supervisor composition is the production one: the supervisor,
// the liveness view and the ops handler are the packages cmd/foreman wires.
package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/lifecycle"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// ops01Healthz reads the liveness endpoint and decodes its body.
func ops01Healthz(t *testing.T, base string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(base + "/foreman/healthz")
	if err != nil {
		t.Fatalf("GET /foreman/healthz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode /foreman/healthz: %v", err)
	}
	return resp.StatusCode, body
}

func TestTechOps01ConfigErrorFailsTheProcessAndHealthReportsIt(t *testing.T) {
	var calls atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/daemon/workspaces", func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"token does not match daemon_id"}`))
	})
	upstream := httptest.NewServer(mux)
	t.Cleanup(upstream.Close)

	client := proxy.NewClient(proxy.Config{
		ServerURL:   upstream.URL,
		WorkspaceID: "ws-1",
		DaemonID:    "foreman",
		Version:     "0.1.0",
	}, "mdt_ws-1")

	health := observability.NewHealth()
	ops := observability.NewOpsHandler(observability.NewMetrics(), registry.New(time.Now), nil,
		observability.OpsConfig{Health: health})
	opsSrv := httptest.NewServer(ops)
	t.Cleanup(opsSrv.Close)

	logs := &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup := lifecycle.New(slog.New(slog.NewJSONHandler(logs, nil)), health, cancel)

	trigger := make(chan struct{})
	release := make(chan struct{})
	// The critical loops stay running across the failure: they hold the
	// serviceable window the TC reads the endpoint in.
	for _, name := range []string{"claim/ws-1", "heartbeat/ws-1", "lease/ws-1"} {
		sup.Run(ctx, name, func(context.Context) error {
			<-release
			return nil
		})
	}
	// The credential check is the failing critical component. Its C-17 request
	// is the controlled 403.
	sup.Run(ctx, "workspace-credential/ws-1", func(ctx context.Context) error {
		<-trigger
		return client.ValidateWorkspaceScope(ctx)
	})

	// Normal start: every loop runs, the endpoint answers available (200).
	if status, body := ops01Healthz(t, opsSrv.URL); status != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("healthz while every loop runs = %d %+v, want 200 ok", status, body)
	}

	close(trigger)
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the 403 config error must stop the process instead of retrying")
	}

	// AC-65: the 403 is not retried, the failure names the component and
	// carries the error summary, and it is the value main exits non-zero on.
	if got := calls.Load(); got != 1 {
		t.Fatalf("C-17 calls = %d, want 1 (a configuration error is not retried)", got)
	}
	err := sup.Failure()
	if err == nil {
		t.Fatal("the component failure produced no process-level error")
	}
	if !strings.Contains(err.Error(), "workspace-credential/ws-1") || !strings.Contains(err.Error(), "403") {
		t.Errorf("process-level error = %v, want the component name and the 403 summary", err)
	}
	if !proxy.IsCredentialConfigError(err) {
		t.Errorf("error %v is not classified as the credential configuration error", err)
	}
	var failedEvent bool
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if json.Unmarshal([]byte(line), &event) != nil {
			continue
		}
		if event["msg"] != "component.failed" {
			continue
		}
		if event["component"] != "workspace-credential/ws-1" {
			t.Errorf("component.failed names %v, want workspace-credential/ws-1", event["component"])
			continue
		}
		if summary := fmt.Sprint(event["err"]); !strings.Contains(summary, "403") {
			t.Errorf("component.failed err = %q, want the 403 summary", summary)
		}
		failedEvent = true
	}
	if !failedEvent {
		t.Fatalf("no component.failed event naming the component:\n%s", logs.String())
	}

	// The stopped marker is written by the component goroutine's defer after
	// the supervisor cancels the root context: poll for it before reading the
	// endpoint, so the 503 assertion does not depend on scheduling.
	stoppedDeadline := time.Now().Add(2 * time.Second)
	for len(health.Stopped()) == 0 && time.Now().Before(stoppedDeadline) {
		time.Sleep(time.Millisecond)
	}
	if got := health.Stopped(); len(got) != 1 || got[0] != "workspace-credential/ws-1" {
		t.Fatalf("health stopped = %v, want [workspace-credential/ws-1]", got)
	}

	// AC-66: before the process exits, the endpoint answers unavailable and
	// lists the stopped loop while the other loops still run.
	status, body := ops01Healthz(t, opsSrv.URL)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("healthz after the loop stopped = %d, want 503", status)
	}
	if body["status"] != "failed" {
		t.Errorf("healthz status = %v, want failed", body["status"])
	}
	stopped, _ := body["stopped_components"].([]any)
	if len(stopped) != 1 || stopped[0] != "workspace-credential/ws-1" {
		t.Errorf("stopped_components = %v, want [workspace-credential/ws-1]", body["stopped_components"])
	}

	close(release)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer drainCancel()
	if !sup.Wait(drainCtx) {
		t.Fatal("the loops did not drain")
	}
}
