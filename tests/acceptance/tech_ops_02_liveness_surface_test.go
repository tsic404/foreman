// TC-tech-ops-02 (AC-66): the liveness face of the health endpoint. A legal
// configuration starts serving: the production loop set runs under the
// supervisor, the liveness view feeds the ops handler, and the controlled
// upstream observes this instance's registration, heartbeat, claim and
// subscription requests. While those requests keep arriving the endpoint
// answers 200. Requests still visible with an unavailable endpoint is the
// verdict this case rejects — the probe would restart a process that still
// serves; the stopped-loop answer is §9.3's face, not this one.
package acceptance

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/lifecycle"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// ops02SustainWindow covers one heartbeat cadence (15s, §1.2) on top of the
// claim rounds in between: the endpoint is judged for as long as every family
// of serving traffic is observed to keep arriving.
const ops02SustainWindow = 20 * time.Second

// ops02SampleInterval bounds the gap between two liveness reads, so a
// transient unavailable answer cannot hide between samples.
const ops02SampleInterval = 250 * time.Millisecond

// ops02Healthz reads the liveness endpoint and decodes its body.
func ops02Healthz(t *testing.T, base string) (int, map[string]any) {
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

func TestTechOps02HealthEndpointStaysAvailableWhileServing(t *testing.T) {
	h := newHarnessWithWS(t, false)
	h.upstream.setRuntimeID("omp", "", "rid-omp")

	// The declaration production installs after the startup probe: the claim
	// loop's preflight is what sends C-1, so the registration the upstream
	// observes belongs to the running composition, not to the fixture.
	h.client.Declare(registry.Discovery{
		CLIVersion: "0.6.1",
		Runtimes: []registry.DiscoveredRuntime{
			{Name: "foreman-omp", Type: "omp", Version: "0.6.1"},
		},
	})

	// Production composition: the supervisor owns the loop set, the liveness
	// view is what the ops handler reads, and the loops come from the
	// control-plane builder cmd/foreman also uses.
	health := observability.NewHealth()
	logs := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup := lifecycle.New(logger, health, cancel)
	reporter := proxy.NewModelCatalogReporter(h.cfg, h.client, h.sched, h.client.CatalogSlots(), h.metrics)
	plane := proxy.NewControlPlane(proxy.ControlPlaneOptions{
		ProxyConfig: h.cfg,
		Client:      h.client,
		Scheduler:   h.sched,
		Registry:    h.reg,
		LeaseHooks:  h.sched,
		Reporter:    reporter,
		Metrics:     h.metrics,
		Logger:      logger,
		WorkspaceID: "ws-1",
		WSEnabled:   h.cfg.WSEnabled,
	})
	for _, component := range plane.Components() {
		sup.Run(ctx, component.Name+"/ws-1", component.Run)
	}
	t.Cleanup(func() {
		cancel()
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer drainCancel()
		if !sup.Wait(drainCtx) {
			t.Error("the control loops did not drain")
		}
	})

	opsSrv := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{
			Health:       health,
			RuntimeIDs:   h.client.RuntimeIDs,
			WorkspaceIDs: []string{"ws-1"},
			StartedAt:    time.Now(),
		}))
	t.Cleanup(opsSrv.Close)

	// Precondition: the controlled upstream observes the instance's four
	// serving families. Without this the 200 below would be a claim about a
	// process that may not be serving at all.
	waitFor(t, "registration, heartbeat, claim and subscription requests", 10*time.Second, func() bool {
		return len(h.upstream.calls(http.MethodPost, "/api/daemon/register")) >= 1 &&
			len(h.upstream.heartbeatRuntimeIDs()) >= 1 &&
			len(h.upstream.calls(http.MethodPost, "/api/daemon/tasks/claim")) >= 1 &&
			h.upstream.wsDialCount() >= 1
	})
	if got := len(h.upstream.calls(http.MethodPost, "/api/daemon/register")); got != 1 {
		t.Fatalf("C-1 calls = %d, want the declaration's single registration (no rebuild)", got)
	}
	if got := h.client.RuntimeIDs(); len(got) != 1 || got[0] != "rid-omp" {
		t.Fatalf("declared runtime ids = %v, want [rid-omp]", got)
	}

	// The endpoint answers available while every loop serves.
	status, body := ops02Healthz(t, opsSrv.URL)
	if status != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("healthz with every loop running = %d %+v, want 200 ok", status, body)
	}
	if ids, _ := body["runtime_ids"].([]any); len(ids) != 1 || ids[0] != "rid-omp" {
		t.Errorf("healthz runtime_ids = %v, want the declared [rid-omp]", body["runtime_ids"])
	}

	// The judgment window: the same four families must keep arriving, and
	// every sample the endpoint answers in that time must be 200. A request
	// family going silent fails here too — an endpoint judged healthy while
	// the process serves nothing is the other half of the criterion.
	claimsBefore := len(h.upstream.calls(http.MethodPost, "/api/daemon/tasks/claim"))
	heartbeatsBefore := len(h.upstream.calls(http.MethodPost, "/api/daemon/heartbeat"))
	dialsBefore := h.upstream.wsDialCount()
	deadline := time.Now().Add(ops02SustainWindow)
	for {
		status, body := ops02Healthz(t, opsSrv.URL)
		if status != http.StatusOK || body["status"] != "ok" {
			t.Fatalf("healthz = %d %+v while the upstream still observes the instance: requests visible with an unavailable endpoint is the failure this criterion rejects", status, body)
		}
		claims := len(h.upstream.calls(http.MethodPost, "/api/daemon/tasks/claim"))
		heartbeats := len(h.upstream.calls(http.MethodPost, "/api/daemon/heartbeat"))
		if claims > claimsBefore && heartbeats > heartbeatsBefore {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the upstream stopped observing serving traffic: claims %d -> %d, heartbeats %d -> %d",
				claimsBefore, claims, heartbeatsBefore, heartbeats)
		}
		time.Sleep(ops02SampleInterval)
	}

	// The registration identity and the subscription did not move while the
	// endpoint was answering: a rebuild or reconnect is not this case's
	// scenario, and seeing one would mean the observed state was not steady.
	if got := len(h.upstream.calls(http.MethodPost, "/api/daemon/register")); got != 1 {
		t.Errorf("C-1 calls after the window = %d, want 1", got)
	}
	if got := h.upstream.wsDialCount(); got != dialsBefore {
		t.Errorf("WS dials across the window = %d, want %d (the subscription stayed up)", got, dialsBefore)
	}
	if ids := h.upstream.heartbeatRuntimeIDs(); len(ids) != 1 || ids["rid-omp"] == 0 {
		t.Errorf("heartbeat runtime ids = %v, want the declared rid-omp", ids)
	}
	if err := sup.Failure(); err != nil {
		t.Fatalf("a control loop stopped during the window: %v", err)
	}
}
