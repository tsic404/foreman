// TC-tech-startup-01-catalog-disabled implements the catalog-route-off branch
// of AC-36 — registered as an implementation-side supplement to
// TC-tech-startup-01: with FOREMAN_MODEL_CATALOG_ENABLED=false the startup
// orchestration completes as soon as S4 registered — it neither waits out the
// window for a discovery nor emits a failure event, and a heartbeat ack's
// pending_model_list is not consumed (no probe Job, no C21, a visible warn).
package acceptance

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
)

// TestStartupCatalogDisabledCompletesAtRegistration runs the startup
// orchestration with the catalog route off, driving the probe's S4 through
// the real fake server: the orchestration returns the registration's
// declaration instead of waiting for a discovery the disabled route never
// produces.
func TestStartupCatalogDisabledCompletesAtRegistration(t *testing.T) {
	h := newHarness(t, false)
	h.rebuildScheduler(t, func(c *schedulerConfig) { c.StartupProbeTimeout = 10 * time.Second })
	// The probe daemon registers as soon as its Job exists, exactly like the
	// pod that lands in a real cluster — and never reports a discovery.
	h.jobs.onCreateJob = func(job *batchv1Job) {
		token := h.builder.probeToken(job.Name)
		if token == "" {
			t.Errorf("no probe Job Token issued for %s", job.Name)
			return
		}
		code, body := h.call("POST", "/api/daemon/register", token, []byte(`{
			"daemon_id": "`+job.Name+`",
			"cli_version": "0.6.1",
			"runtimes": [{"name": "foreman-omp", "type": "omp", "version": "0.6.1"}]
		}`))
		if code != 200 {
			t.Errorf("probe register = %d: %s", code, body)
		}
	}

	begin := time.Now()
	discovery, err := h.sched.RunStartupProbe(t.Context())
	elapsed := time.Since(begin)
	if err != nil {
		t.Fatalf("RunStartupProbe: %v", err)
	}
	if len(discovery.Runtimes) != 1 || discovery.Runtimes[0].Type != "omp" || discovery.CLIVersion != "0.6.1" {
		t.Fatalf("discovery = %+v, want the probe's registration body", discovery)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("orchestration waited %s after registration with the catalog route off", elapsed)
	}
	if events := h.events("startup.probe_failed"); len(events) != 0 {
		t.Fatalf("a disabled catalog route is a supported degradation, not a failure: %+v", events)
	}
	if probes := h.probes.ListProbes(); len(probes) != 0 {
		t.Errorf("the probe must be torn down before C1: %+v", probes)
	}
	if got := h.jobs.probeJobsCreated(); len(got) != 1 {
		t.Errorf("probe Jobs created = %v, want exactly the startup probe", got)
	}
	// The registration still drives C1: one declared runtime, the probe's.
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp"}
	h.declare(discovery)
	if got := h.client.RuntimeIDs(); len(got) != 1 || got[0] != "rid-omp" {
		t.Fatalf("declared runtime ids = %v, want the single omp runtime", got)
	}
}

// TestStartupCatalogDisabledDoesNotConsumeAck covers the ack face of the same
// switch: a pending_model_list is not consumed — no probe Job is started, no
// C21 is sent, and the drop is visible as model.catalog_relay_failed with the
// documented reason.
func TestStartupCatalogDisabledDoesNotConsumeAck(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	logs := &bytes.Buffer{}
	reporter := proxy.NewModelCatalogReporter(h.cfg, h.client, h.sched, h.slots, h.metrics,
		proxy.WithReporterLogger(slog.New(slog.NewJSONHandler(logs, nil))),
	)
	reporter.OnAck(t.Context(), "rid-omp", "req-disabled")

	if got := h.upstream.pathCalls("/api/daemon/runtimes/rid-omp/models/"); len(got) != 0 {
		t.Errorf("a disabled route must not produce a C21: %v", got)
	}
	if got := h.jobs.probeJobsCreated(); len(got) != 0 {
		t.Errorf("a disabled route must not start a probe Job: %v", got)
	}
	if codes := h.metrics.codes(); len(codes) != 1 || codes[0] != "none" {
		t.Errorf("relay metric codes = %v, want one abandoned relay", codes)
	}
	event := findJSONEvent(t, logs.Bytes(), "model.catalog_relay_failed")
	if event == nil {
		t.Fatalf("the disabled path must be visible: %s", logs.String())
	}
	if event["reason"] != "catalog relay disabled" {
		t.Errorf("reason = %v, want the documented literal", event["reason"])
	}
	if event["request_id"] != "req-disabled" {
		t.Errorf("request_id = %v, want it verbatim (the server's id locates the drop)", event["request_id"])
	}
}

// TestStartupCatalogEnabledStillWaitsForDiscovery is the contrast case: with
// the route on, an unlanded discovery is reported (fourth class) instead of
// silently completing.
func TestStartupCatalogEnabledStillWaitsForDiscovery(t *testing.T) {
	h := newHarness(t, true)
	h.rebuildScheduler(t, func(c *schedulerConfig) { c.StartupProbeTimeout = 600 * time.Millisecond })
	h.jobs.onCreateJob = func(job *batchv1Job) {
		token := h.builder.probeToken(job.Name)
		code, body := h.call("POST", "/api/daemon/register", token, []byte(`{
			"daemon_id": "`+job.Name+`",
			"cli_version": "0.6.1",
			"runtimes": [{"name": "foreman-omp", "type": "omp", "version": "0.6.1"}]
		}`))
		if code != 200 {
			t.Errorf("probe register = %d: %s", code, body)
		}
	}

	discovery, err := h.sched.RunStartupProbe(t.Context())
	if err != nil {
		t.Fatalf("the fourth class must not stop startup: %v", err)
	}
	if len(discovery.Runtimes) != 1 {
		t.Fatalf("discovery = %+v", discovery)
	}
	events := h.events("startup.probe_failed")
	if len(events) != 1 || events[0]["reason"] != "discovery_not_landed" {
		t.Fatalf("events = %+v, want the discovery-not-landed class", events)
	}
	if got := len(h.jobs.deletedJobs); got != 1 || !strings.HasPrefix(h.jobs.deletedJobs[0], "fm-catalog-startup-") {
		t.Errorf("probe Jobs deleted = %v, want the startup probe", h.jobs.deletedJobs)
	}
}

// findJSONEvent returns the first logged line carrying msg.
func findJSONEvent(t *testing.T, raw []byte, msg string) map[string]any {
	t.Helper()
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			continue
		}
		if fields["msg"] == msg {
			return fields
		}
	}
	return nil
}
