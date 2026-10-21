// TC-tech-multiws-07 implements AC-87: the startup probe's registration scope
// is S-1's answer (the discovery set) — one probe Job, no template-side
// narrowing, no filtering or padding by Foreman — and the effective service
// scope is exactly what the probe actually registered.
package acceptance

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/workspace"
)

// probeDaemon plays the startup probe pod: it reads S-1, asserts it equals
// want, and registers to every workspace in it — the official daemon's
// behaviour (it has no workspace whitelist). skip names workspaces the
// simulated daemon fails to register (the unregistered-member face).
func probeDaemon(t *testing.T, m *multiWS, want []string, skip string, versions map[string]string) func(job *batchv1Job) {
	t.Helper()
	return func(job *batchv1Job) {
		if job.Labels[registry.LabelRole] != registry.RoleCatalogProbe {
			return
		}
		token := m.builder.tokenFor(job.Name)
		if token == "" {
			t.Errorf("probe Job %s carries no token", job.Name)
			return
		}
		code, body := m.call("GET", "/api/daemon/workspaces", token, nil)
		if code != 200 {
			t.Errorf("probe S-1 = %d: %s", code, body)
			return
		}
		var answer []struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(body, &answer); err != nil {
			t.Errorf("decode S-1: %v (%s)", err, body)
			return
		}
		ids := make([]string, 0, len(answer))
		for _, row := range answer {
			ids = append(ids, row.ID)
		}
		if len(ids) != len(want) {
			t.Errorf("probe S-1 = %v, want the discovery set %v", ids, want)
			return
		}
		for i := range want {
			if ids[i] != want[i] {
				t.Errorf("probe S-1 = %v, want the discovery set %v", ids, want)
				return
			}
		}
		for _, workspaceID := range ids {
			if workspaceID == skip {
				continue
			}
			version := versions[workspaceID]
			if version == "" {
				version = "0.6.1"
			}
			register := map[string]any{
				"daemon_id":    job.Name,
				"workspace_id": workspaceID,
				"cli_version":  version,
				"runtimes": []map[string]string{
					{"name": "foreman-" + workspaceID, "type": "omp", "version": version},
				},
			}
			raw, _ := json.Marshal(register)
			if code, body := m.call("POST", "/api/daemon/register", token, raw); code != 200 {
				t.Errorf("probe S-4 for %s = %d: %s", workspaceID, code, body)
			}
		}
	}
}

func TestTechMultiWS07ProbeRegistrationScope(t *testing.T) {
	t.Run("the discovery set is the whole registration scope", func(t *testing.T) {
		stub := newDiscoveryStub(t, []workspace.Ref{
			{ID: "ws-a", Name: "A"},
			{ID: "ws-b", Name: "B"},
		})
		m, _ := newUserScopedWS(t, stub, nil)
		m.rebuildScheduler(func(cfg *schedulerConfig) { cfg.StartupProbeTimeout = 5 * time.Second })
		m.jobs.onCreateJob = probeDaemon(t, m, []string{"ws-a", "ws-b"}, "", map[string]string{"ws-a": "0.6.1", "ws-b": "0.6.2"})

		discoveries, err := m.sched.RunStartupProbe(t.Context())
		if err != nil {
			t.Fatalf("RunStartupProbe: %v", err)
		}
		if len(discoveries) != 2 {
			t.Fatalf("discoveries = %v, want both discovered workspaces", discoveries)
		}
		if got := discoveries["ws-a"].CLIVersion; got != "0.6.1" {
			t.Errorf("A cli_version = %q, want 0.6.1", got)
		}
		if got := discoveries["ws-b"].CLIVersion; got != "0.6.2" {
			t.Errorf("B cli_version = %q, want 0.6.2 (per-workspace registration body)", got)
		}
		// One probe Job only, never one per workspace.
		if got := countProbeJobs(m); got != 1 {
			t.Errorf("probe Jobs created = %d, want 1", got)
		}
		// Foreman's own write face keeps the template's scope config: the
		// reference env key survives verbatim and no narrowing key is added.
		assertProbeScopeConfigUntouched(t, m)
	})

	t.Run("the manifest narrows the discovery set", func(t *testing.T) {
		stub := newDiscoveryStub(t, []workspace.Ref{
			{ID: "ws-a", Name: "A"},
			{ID: "ws-b", Name: "B"},
		})
		m, discovered := newUserScopedWS(t, stub, []string{"ws-a"})
		if len(m.clients) != 1 || m.clients["ws-a"] == nil {
			t.Fatalf("clients = %v, want only the manifested workspace", m.set.IDs())
		}
		if len(discovered.Excluded) != 1 || discovered.Excluded[0].ID != "ws-b" {
			t.Fatalf("excluded = %v, want the visible ws-b", discovered.Excluded)
		}
		m.rebuildScheduler(func(cfg *schedulerConfig) { cfg.StartupProbeTimeout = 5 * time.Second })
		m.jobs.onCreateJob = probeDaemon(t, m, []string{"ws-a"}, "", nil)

		discoveries, err := m.sched.RunStartupProbe(t.Context())
		if err != nil {
			t.Fatalf("RunStartupProbe: %v", err)
		}
		if len(discoveries) != 1 {
			t.Fatalf("discoveries = %v, want only ws-a", discoveries)
		}
		if _, ok := discoveries["ws-a"]; !ok {
			t.Fatalf("discoveries = %v, want ws-a", discoveries)
		}
		if _, ok := discoveries["ws-b"]; ok {
			t.Fatalf("ws-b was not in the discovery set but has a declaration: %v", discoveries)
		}
		if got := countProbeJobs(m); got != 1 {
			t.Errorf("probe Jobs created = %d, want 1", got)
		}
	})

	t.Run("a discovered workspace the probe never registers is not served", func(t *testing.T) {
		stub := newDiscoveryStub(t, []workspace.Ref{
			{ID: "ws-a", Name: "A"},
			{ID: "ws-b", Name: "B"},
		})
		m, _ := newUserScopedWS(t, stub, nil)
		m.rebuildScheduler(func(cfg *schedulerConfig) { cfg.StartupProbeTimeout = 5 * time.Second })
		m.jobs.onCreateJob = probeDaemon(t, m, []string{"ws-a", "ws-b"}, "ws-b", nil)

		discoveries, err := m.sched.RunStartupProbe(t.Context())
		if err != nil {
			t.Fatalf("RunStartupProbe: %v", err)
		}
		if _, ok := discoveries["ws-b"]; ok {
			t.Fatalf("B was not registered by the probe but has a declaration: %v", discoveries)
		}
		if !strings.Contains(m.logs.String(), "probe.workspace_not_registered") {
			t.Errorf("the unregistered workspace is silent; want the probe.workspace_not_registered event")
		}
	})
}

func countProbeJobs(m *multiWS) int {
	m.jobs.mu.Lock()
	defer m.jobs.mu.Unlock()
	n := 0
	for _, job := range m.jobs.createdJobs {
		if job.Labels[registry.LabelRole] == registry.RoleCatalogProbe {
			n++
		}
	}
	return n
}

// assertProbeScopeConfigUntouched renders a probe with the production builder
// and asserts Foreman neither writes nor deletes the template's own
// workspace-scope configuration (§1.2, AC-87 negative).
func assertProbeScopeConfigUntouched(t *testing.T, m *multiWS) {
	t.Helper()
	builder := newRealMultiBuilder(t, m, []string{"ws-a", "ws-b"}, "")
	job, _, _, err := builder.BuildCatalogProbe("scope-check", "runtime-x", "ws-a", time.Minute)
	if err != nil {
		t.Fatalf("BuildCatalogProbe: %v", err)
	}
	var root string
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "MULTICA_WORKSPACES_ROOT" {
			root = env.Value
		}
		if env.Name == "MULTICA_WORKSPACE_ID" || env.Name == "MULTICA_WORKSPACES" {
			t.Errorf("Foreman injected a scope-narrowing env %q", env.Name)
		}
	}
	if root != "/state/workspaces" {
		t.Errorf("MULTICA_WORKSPACES_ROOT = %q, want the template's value untouched", root)
	}
	if got := job.Labels[registry.LabelWorkspaceID]; got != "ws-a" {
		t.Errorf("probe workspace label = %q, want the probe credential's ws-a", got)
	}
}
