// TC-tech-startup-03 (N ≥ 2 directory attribution) / AC-38 + AC-39: with two
// real runtimes of one provider — a builtin and a profile entry, declared in an
// order that is not the name order — the startup discovery lands in the slot of
// the runtime the probe actually served, and a command for the other runtime
// falls back to a probe that runs *that* runtime's startup command. The
// cluster-level faces of TC-tech-startup-03 (claim attribution, one WS
// connection per runtime) stay QA_BLOCKED; the attribution face is here.
package acceptance

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// startupAttributionDiscovery declares one provider twice: the builtin entry
// first, the profile entry second. A name sort ranks the profile entry first,
// which is the entry the probe's S4 answer names (§2.3).
func startupAttributionDiscovery() registry.Discovery {
	return registry.Discovery{
		CLIVersion: "0.6.1",
		Runtimes: []registry.DiscoveredRuntime{
			{Name: "zz-omp", Type: "omp", Version: "0.6.1"},
			{Name: "aa-omp-prof", Type: "omp", Version: "0.6.2", ProfileID: "pf-1"},
		},
	}
}

// serveAttributionDaemon plays the probe pod's daemon for both probe modes: it
// registers the whole declared set, then discovers the catalog of the runtime
// the S4 answer names — the same input the official daemon's own startup
// command selection reads.
func serveAttributionDaemon(t *testing.T, h *harness, profileCatalog, builtinCatalog string) {
	t.Helper()
	h.jobs.onCreateJob = func(job *batchv1Job) {
		go func(name string) {
			token := h.builder.probeToken(name)
			if token == "" {
				t.Errorf("probe Job %s has no Job Token", name)
				return
			}
			code, body, err := h.probeCall("POST", "/api/daemon/register", token, []byte(`{
				"daemon_id": "`+name+`",
				"cli_version": "0.6.1",
				"runtimes": [
					{"name": "zz-omp", "type": "omp", "version": "0.6.1"},
					{"name": "aa-omp-prof", "type": "omp", "version": "0.6.2", "profile_id": "pf-1"}
				]
			}`))
			if err != nil || code != 200 {
				t.Errorf("probe S4 = %d: %s (%v)", code, body, err)
				return
			}
			var answer struct {
				Runtimes []struct {
					Name      string  `json:"name"`
					ProfileID *string `json:"profile_id"`
				} `json:"runtimes"`
			}
			if err := json.Unmarshal(body, &answer); err != nil || len(answer.Runtimes) != 1 {
				t.Errorf("probe S4 answer = %s (%v), want exactly one runtime", body, err)
				return
			}
			session, ok := h.probes.ProbeByJob(name)
			if !ok {
				t.Errorf("probe session %s missing at S4", name)
				return
			}
			code, body, err = h.probeCall("POST", "/api/daemon/heartbeat", token,
				[]byte(`{"runtime_id": "`+session.JobRuntime+`"}`))
			if err != nil || code != 200 {
				t.Errorf("probe S5 = %d: %s (%v)", code, body, err)
				return
			}
			var ack struct {
				PendingModelList *struct {
					ID string `json:"id"`
				} `json:"pending_model_list"`
			}
			if err := json.Unmarshal(body, &ack); err != nil || ack.PendingModelList == nil {
				t.Errorf("probe S5 ack carries no command: %s (%v)", body, err)
				return
			}
			catalog := builtinCatalog
			if answer.Runtimes[0].ProfileID != nil {
				catalog = profileCatalog
			}
			code, body, err = h.probeCall("POST",
				"/api/daemon/runtimes/"+session.JobRuntime+"/models/"+ack.PendingModelList.ID+"/result",
				token, []byte(catalog))
			if err != nil || code != 200 {
				t.Errorf("probe S27 = %d: %s (%v)", code, body, err)
			}
		}(job.Name)
	}
}

func TestStartup03DirectoryAttributionFollowsTheServedRuntime(t *testing.T) {
	h := newHarness(t, true)
	h.useRealBuilder(t)
	h.rebuildScheduler(t, func(c *schedulerConfig) { c.StartupProbeTimeout = 5 * time.Second })

	const profileCatalog = `{"status":"completed","models":[{"id":"prof-model"}]}`
	const builtinCatalog = `{"status":"completed","models":[{"id":"builtin-model"}]}`
	serveAttributionDaemon(t, h, profileCatalog, builtinCatalog)

	discovery := startupAttributionDiscovery()
	discoveries, err := h.sched.RunStartupProbe(t.Context())
	if err != nil {
		t.Fatalf("RunStartupProbe: %v", err)
	}
	if got, ok := discoveries[h.schedCfg.WorkspaceID]; !ok || len(got.Runtimes) != 2 {
		t.Fatalf("discoveries = %+v, want the probe's two declared runtimes", discoveries)
	}

	// C-1 declares the same two identities; the ids are the slots' keys.
	h.upstream.setRuntimeID("omp", "", "rid-omp")
	h.upstream.setRuntimeID("omp", "pf-1", "rid-omp-prof")
	h.declare(discovery)

	slot, ok := h.slots.Get("rid-omp-prof")
	if !ok || string(slot.Body) != profileCatalog {
		t.Fatalf("profile runtime slot = %q (cached=%v), want the discovery of the runtime the probe served", slot.Body, ok)
	}
	if other, cached := h.slots.Get("rid-omp"); cached {
		t.Fatalf("builtin runtime slot = %q, want no slot: the probe discovered the profile entry", other.Body)
	}
}

func TestStartup03FallbackProbeRunsTheRequestedRuntime(t *testing.T) {
	h := newHarness(t, true)
	h.useRealBuilder(t)
	h.rebuildScheduler(t, func(c *schedulerConfig) { c.StartupProbeTimeout = 5 * time.Second })

	const profileCatalog = `{"status":"completed","models":[{"id":"prof-model"}]}`
	const builtinCatalog = `{"status":"completed","models":[{"id":"builtin-model"}]}`
	h.upstream.setRuntimeID("omp", "", "rid-omp")
	h.upstream.setRuntimeID("omp", "pf-1", "rid-omp-prof")
	h.declare(startupAttributionDiscovery())
	serveAttributionDaemon(t, h, profileCatalog, builtinCatalog)

	// The builtin runtime has no slot (§1.3: startup fills one runtime only),
	// so its command falls back to a probe.
	if _, cached := h.slots.Get("rid-omp"); cached {
		t.Fatal("the builtin runtime's slot must start empty")
	}
	reporter := h.newReporter(t)
	reporter.OnAck(t.Context(), "rid-omp", "req-builtin")

	const resultPath = "/api/daemon/runtimes/rid-omp/models/req-builtin/result"
	waitFor(t, "the fallback probe's C21", 5*time.Second, func() bool {
		return len(h.upstream.pathCalls(resultPath)) > 0
	})
	relayed := h.upstream.calls("POST", resultPath)
	if len(relayed) != 1 {
		t.Fatalf("C21 calls = %d, want 1", len(relayed))
	}
	if string(relayed[0].Body) != builtinCatalog {
		t.Errorf("C21 body = %s, want the requested runtime's own catalog", relayed[0].Body)
	}
	if got := h.upstream.pathCalls("/api/daemon/runtimes/rid-omp-prof/models/"); len(got) != 0 {
		t.Errorf("the profile runtime received %d catalog results, want none", len(got))
	}
	if slot, ok := h.slots.Get("rid-omp-prof"); ok && string(slot.Body) != profileCatalog {
		t.Errorf("profile runtime slot = %q, want it untouched", slot.Body)
	}
}
