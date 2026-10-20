// TC-tech-protocol-01 (AC-01): the startup declaration equals the discovery
// entry by entry — count, name, type, version and profile_id — and each real
// runtime gets exactly one heartbeat stream. A declaration that dropped or
// merged an entry would fail the count/entry comparison; a second stream for
// one runtime would show up as two heartbeats for that id.
package acceptance

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// tcProtocol01Heartbeats returns the runtime ids observed on the heartbeat
// route, with their call counts.
func tcProtocol01Heartbeats(t *testing.T, h *harness) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, call := range h.upstream.calls(http.MethodPost, "/api/daemon/heartbeat") {
		var body struct {
			RuntimeID string `json:"runtime_id"`
		}
		if err := json.Unmarshal(call.Body, &body); err != nil {
			t.Fatalf("decode heartbeat body: %v", err)
		}
		counts[body.RuntimeID]++
	}
	return counts
}

func TestProtocol01RegistrationAndHeartbeatStreams(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.upstream.setRuntimeID("omp", "prof-1", "rid-omp-prof")

	discovery := registry.Discovery{
		CLIVersion: "0.6.1",
		Runtimes: []registry.DiscoveredRuntime{
			{Name: "foreman-omp", Type: "omp", Version: "0.6.1"},
			{Name: "foreman-zz", Type: "zz", Version: "3.1.4"},
			{Name: "foreman-omp-prof", Type: "omp", Version: "0.6.1", ProfileID: "prof-1"},
		},
		FailedProfiles: []json.RawMessage{json.RawMessage(`{"profile_id":"pf","command_name":"zz","reason":"x"}`)},
	}
	h.declare(discovery)

	// The declaration: one entry per discovered runtime, field for field. The
	// negative form is the comparison itself — a body declaring one entry for
	// this two-identity (three-entry) discovery fails here.
	registers := h.upstream.calls(http.MethodPost, "/api/daemon/register")
	if len(registers) != 1 {
		t.Fatalf("C-1 calls = %d, want 1", len(registers))
	}
	var body struct {
		CLIVersion string `json:"cli_version"`
		Runtimes   []struct {
			Name      string `json:"name"`
			Type      string `json:"type"`
			Version   string `json:"version"`
			Status    string `json:"status"`
			ProfileID string `json:"profile_id"`
		} `json:"runtimes"`
		FailedProfiles []json.RawMessage `json:"failed_profiles"`
	}
	if err := json.Unmarshal(registers[0].Body, &body); err != nil {
		t.Fatalf("decode C-1 body: %v", err)
	}
	if body.CLIVersion != discovery.CLIVersion {
		t.Errorf("cli_version = %q, want the discovery's %q", body.CLIVersion, discovery.CLIVersion)
	}
	if len(body.Runtimes) != len(discovery.Runtimes) {
		t.Fatalf("declared runtimes = %d, want the discovery's %d (one per discovered entry)",
			len(body.Runtimes), len(discovery.Runtimes))
	}
	byIdentity := map[string]struct {
		name, version, status string
	}{}
	for _, runtime := range body.Runtimes {
		byIdentity[registry.RuntimeIdentityKey(runtime.Type, runtime.ProfileID)] = struct {
			name, version, status string
		}{runtime.Name, runtime.Version, runtime.Status}
	}
	for _, want := range discovery.Runtimes {
		got, ok := byIdentity[registry.RuntimeIdentityKey(want.Type, want.ProfileID)]
		if !ok {
			t.Errorf("discovery entry (%s, %q) is absent from the declaration", want.Type, want.ProfileID)
			continue
		}
		if got.name != want.Name || got.version != want.Version || got.status != "online" {
			t.Errorf("declared (%s, %q) = %+v, want name %q version %q online",
				want.Type, want.ProfileID, got, want.Name, want.Version)
		}
	}
	if len(body.FailedProfiles) != len(discovery.FailedProfiles) {
		t.Errorf("failed_profiles = %d, want %d", len(body.FailedProfiles), len(discovery.FailedProfiles))
	}

	// One heartbeat stream per declared real runtime: the loop starts one
	// goroutine per runtime and each sends immediately.
	ctx, cancel := context.WithCancel(t.Context())
	loop := proxy.NewHeartbeatLoop(h.client, nil)
	go func() { _ = loop.Run(ctx) }()
	t.Cleanup(cancel)

	wantIDs := h.client.RuntimeIDs()
	if len(wantIDs) != len(discovery.Runtimes) {
		t.Fatalf("declared real runtime ids = %d, want %d", len(wantIDs), len(discovery.Runtimes))
	}
	waitFor(t, "one heartbeat per real runtime", 5*time.Second, func() bool {
		counts := tcProtocol01Heartbeats(t, h)
		for _, id := range wantIDs {
			if counts[id] < 1 {
				return false
			}
		}
		return true
	})
	time.Sleep(100 * time.Millisecond)
	counts := tcProtocol01Heartbeats(t, h)
	for _, id := range wantIDs {
		if counts[id] != 1 {
			t.Errorf("heartbeats for %s = %d, want exactly one stream", id, counts[id])
		}
	}
	if len(counts) != len(wantIDs) {
		t.Errorf("heartbeat streams = %d, want %d", len(counts), len(wantIDs))
	}
	// No 4xx/5xx: a rejected heartbeat would have triggered the rebuild path,
	// which sends a second C-1.
	if got := len(h.upstream.calls(http.MethodPost, "/api/daemon/register")); got != 1 {
		t.Errorf("C-1 calls after the heartbeat round = %d, want 1 (no rebuild was triggered)", got)
	}
}
