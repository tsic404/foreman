// TC-tech-multiws-07 implements AC-87: one startup probe serves the whole
// deployment; the template's daemon decides which workspaces it registers to,
// and Foreman serves exactly the observed set — never injecting, overriding or
// stripping that configuration.
package acceptance

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// probeDaemon plays the startup probe pod: it reads S-1 and registers to the
// workspaces the "template" chose (all when limitedTo is empty).
func probeDaemon(t *testing.T, m *multiWS, limitedTo string, versions map[string]string) func(job *batchv1Job) {
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
		if len(ids) != 2 || ids[0] != "ws-a" || ids[1] != "ws-b" {
			t.Errorf("probe S-1 = %v, want the configured set [ws-a ws-b]", ids)
		}
		for _, workspaceID := range ids {
			if limitedTo != "" && workspaceID != limitedTo {
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
	t.Run("template does not limit the scope", func(t *testing.T) {
		m := newMultiWS(t, "ws-a", "ws-b")
		m.rebuildScheduler(func(cfg *schedulerConfig) { cfg.StartupProbeTimeout = 5 * time.Second })
		m.jobs.onCreateJob = probeDaemon(t, m, "", map[string]string{"ws-a": "0.6.1", "ws-b": "0.6.2"})

		discoveries, err := m.sched.RunStartupProbe(t.Context())
		if err != nil {
			t.Fatalf("RunStartupProbe: %v", err)
		}
		if len(discoveries) != 2 {
			t.Fatalf("discoveries = %v, want both configured workspaces", discoveries)
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

	t.Run("template limits the scope to A", func(t *testing.T) {
		m := newMultiWS(t, "ws-a", "ws-b")
		m.rebuildScheduler(func(cfg *schedulerConfig) { cfg.StartupProbeTimeout = 5 * time.Second })
		m.jobs.onCreateJob = probeDaemon(t, m, "ws-a", nil)

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
			t.Fatalf("B was not registered by the probe but has a declaration: %v", discoveries)
		}
		if !strings.Contains(m.logs.String(), "probe.workspace_not_registered") {
			t.Errorf("the unserved workspace is silent; want the probe.workspace_not_registered event")
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
