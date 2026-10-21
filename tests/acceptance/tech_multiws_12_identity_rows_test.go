// TC-tech-multiws-12 implements AC-97: server-side real runtime rows come only
// from Foreman's own per-workspace registration — the identity is the
// workspace's deployment identity, distinct across workspaces and stable across
// restarts — while the probe's own daemon identity stays in its probe session
// and never produces a server-side row. The workspace-scoped form's C-17 set
// mismatch refuses startup.
package acceptance

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/workspace"
)

func TestTechMultiWS12IdentityAndRowSource(t *testing.T) {
	visible := []workspace.Ref{{ID: "ws-a", Name: "A"}, {ID: "ws-b", Name: "B"}}
	deployment := workspace.Deployment{Scope: workspace.ScopeUser, UserToken: "mul_user", BaseDaemonID: "foreman"}

	t.Run("identities are distinct and stable across restarts", func(t *testing.T) {
		first := deployment.Identities(visible)
		second := deployment.Identities(visible)
		if first[0].DaemonID == first[1].DaemonID {
			t.Fatalf("two workspaces share the identity %q (F2)", first[0].DaemonID)
		}
		for i := range first {
			if first[i].DaemonID != second[i].DaemonID {
				t.Fatalf("identity of %s changed across restarts: %q then %q",
					first[i].WorkspaceID, first[i].DaemonID, second[i].DaemonID)
			}
		}
		// The probe's identity shape (fm-catalog-<request>) is never a
		// deployment identity.
		for _, identity := range first {
			if strings.HasPrefix(identity.DaemonID, "fm-catalog-") {
				t.Errorf("deployment identity %q has the probe identity shape", identity.DaemonID)
			}
		}
	})

	t.Run("probe registration produces no server-side row", func(t *testing.T) {
		stub := newDiscoveryStub(t, visible)
		m, discovered := newUserScopedWS(t, stub, nil)
		m.rebuildScheduler(func(cfg *schedulerConfig) { cfg.StartupProbeTimeout = 5 * time.Second })
		m.jobs.onCreateJob = probeDaemon(t, m, []string{"ws-a", "ws-b"}, "", nil)

		discoveries, err := m.sched.RunStartupProbe(t.Context())
		if err != nil {
			t.Fatalf("RunStartupProbe: %v", err)
		}
		if len(discoveries) != 2 {
			t.Fatalf("probe registrations = %v, want both workspaces", discoveries)
		}
		// The probe's identity is its Job name (fm-catalog-<request>); it
		// registered only against the local S-4 face and its session is gone
		// after teardown.
		probeNames := make(map[string]bool)
		m.jobs.mu.Lock()
		for _, job := range m.jobs.createdJobs {
			if job.Labels[registry.LabelRole] == registry.RoleCatalogProbe {
				probeNames[job.Name] = true
			}
		}
		m.jobs.mu.Unlock()
		if len(probeNames) == 0 {
			t.Fatal("no probe Job was created")
		}

		for _, workspaceID := range []string{"ws-a", "ws-b"} {
			m.upstream[workspaceID].setRuntimeID("omp", "", "rid-"+workspaceID)
			m.clients[workspaceID].Declare(discoveries[workspaceID])
			if err := m.clients[workspaceID].Register(t.Context()); err != nil {
				t.Fatalf("C-1 for %s: %v", workspaceID, err)
			}
		}

		// Every server-side register call carries a deployment identity — one
		// per workspace, never a probe Job name — and no upstream request
		// carries the probe's credential.
		for _, workspaceID := range []string{"ws-a", "ws-b"} {
			for _, req := range m.upstream[workspaceID].all() {
				for _, value := range req.Header.Values("Authorization") {
					if strings.Contains(value, "fmj_") {
						t.Errorf("%s request %s carried a probe Job Token", workspaceID, req.Path)
					}
				}
				if req.Path != "/api/daemon/register" {
					continue
				}
				var body struct {
					WorkspaceID string `json:"workspace_id"`
					DaemonID    string `json:"daemon_id"`
				}
				if err := json.Unmarshal(req.Body, &body); err != nil {
					t.Fatalf("decode register body: %v", err)
				}
				if probeNames[body.DaemonID] {
					t.Errorf("server-side register row uses the probe identity %q", body.DaemonID)
				}
				if body.DaemonID != "foreman-"+workspaceID {
					t.Errorf("register daemon_id = %q, want the workspace's deployment identity", body.DaemonID)
				}
				if body.WorkspaceID != workspaceID {
					t.Errorf("register workspace_id = %q, want %q", body.WorkspaceID, workspaceID)
				}
			}
		}

		// Restarting the process re-derives the same identities, so the same
		// workspace reuses its row instead of creating a second one.
		restarted := deployment.Identities(discovered.Set)
		for i, identity := range deployment.Identities(discovered.Set) {
			if restarted[i].DaemonID != identity.DaemonID {
				t.Fatalf("restart changed %s's identity", identity.WorkspaceID)
			}
		}
	})

	t.Run("workspace-scoped C-17 set mismatch refuses startup", func(t *testing.T) {
		// A workspace-scoped credential whose C-17 answer is not exactly the
		// configured workspace is a configuration error (AC-97).
		stub := newDiscoveryStub(t, []workspace.Ref{{ID: "ws-a", Name: "A"}, {ID: "ws-b", Name: "B"}})
		m := newMultiWSWithURLs(t, []string{"ws-a"}, map[string]string{"ws-a": stub.server.URL})
		err := m.clients["ws-a"].ValidateWorkspaceScope(t.Context())
		if err == nil {
			t.Fatal("a C-17 answer that is not exactly the configured workspace must refuse startup")
		}
		if !strings.Contains(err.Error(), "ws-a") {
			t.Errorf("error = %v, want it to name the configured workspace", err)
		}
	})
}
