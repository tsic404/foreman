// TC-tech-multiws-10 implements AC-95: the discovery set's narrowing face.
// A manifest narrows the visible set (visible-but-unlisted workspaces are not
// registered, not served and visible in the discovery event), a manifest entry
// outside the visible set refuses startup naming that workspace, an empty
// manifest refuses startup, and the workspace-scoped credential form accepts
// no manifest at all.
package acceptance

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/workspace"
)

func TestTechMultiWS10NarrowingFace(t *testing.T) {
	t.Run("manifest narrows to the listed workspace", func(t *testing.T) {
		stub := newDiscoveryStub(t, []workspace.Ref{
			{ID: "ws-a", Name: "A"},
			{ID: "ws-b", Name: "B"},
		})
		m, discovered := newUserScopedWS(t, stub, []string{"ws-a"})
		if discovered.Source != "user_scope_manifest" {
			t.Errorf("discovery source = %q, want user_scope_manifest", discovered.Source)
		}
		if len(discovered.Set) != 1 || discovered.Set[0].ID != "ws-a" {
			t.Fatalf("discovery set = %v, want only ws-a", discovered.Set)
		}
		if len(discovered.Excluded) != 1 || discovered.Excluded[0].ID != "ws-b" {
			t.Fatalf("excluded = %v, want the visible ws-b", discovered.Excluded)
		}
		// The unlisted workspace has no client, hence no control plane, no
		// registration and no heartbeat.
		if got := m.set.IDs(); len(got) != 1 || got[0] != "ws-a" {
			t.Fatalf("client set = %v, want only ws-a", got)
		}
		if _, ok := m.upstream["ws-b"]; ok {
			t.Fatal("the excluded workspace got an upstream client")
		}
		// §9.1 发现事件 names the excluded workspace (never silent).
		var logs bytes.Buffer
		proxy.LogDiscoveryEvent(slog.New(slog.NewJSONHandler(&logs, nil)), discovered)
		var event map[string]any
		if err := json.Unmarshal(logs.Bytes(), &event); err != nil {
			t.Fatalf("decode discovery event: %v", err)
		}
		excluded, _ := event["excluded"].([]any)
		if len(excluded) != 1 {
			t.Fatalf("discovery event excluded = %v, want ws-b", event["excluded"])
		}
		entry, _ := excluded[0].(map[string]any)
		if entry["id"] != "ws-b" || entry["name"] != "B" {
			t.Errorf("excluded entry = %v, want id+name of ws-b", entry)
		}

		// The probe sees the narrowed set and registers only to it.
		m.rebuildScheduler(func(cfg *schedulerConfig) { cfg.StartupProbeTimeout = 5 * time.Second })
		m.jobs.onCreateJob = probeDaemon(t, m, []string{"ws-a"}, "", nil)
		discoveries, err := m.sched.RunStartupProbe(t.Context())
		if err != nil {
			t.Fatalf("RunStartupProbe: %v", err)
		}
		if len(discoveries) != 1 {
			t.Fatalf("discoveries = %v, want only ws-a", discoveries)
		}
	})

	t.Run("manifest entry outside the visible set refuses startup", func(t *testing.T) {
		stub := newDiscoveryStub(t, []workspace.Ref{{ID: "ws-a", Name: "A"}})
		discoveryClient := proxy.NewClient(proxy.Config{ServerURL: stub.server.URL, Version: "0.1.0"}, "mul_user")
		_, err := discoveryClient.DiscoverWorkspaces(t.Context(), []string{"ws-a", "ws-ghost"})
		if err == nil {
			t.Fatal("a manifest naming an invisible workspace must refuse startup")
		}
		if !bytes.Contains([]byte(err.Error()), []byte("ws-ghost")) {
			t.Errorf("error = %v, want it to name ws-ghost", err)
		}
	})

	t.Run("empty manifest refuses startup", func(t *testing.T) {
		if _, err := workspace.LoadDeployment(func(key string) string {
			switch key {
			case "MULTICA_TOKEN":
				return "mul_user"
			case "FOREMAN_WORKSPACES_MANIFEST":
				return `[]`
			}
			return ""
		}); err == nil {
			t.Fatal("an empty manifest must refuse startup")
		}
	})

	t.Run("workspace-scoped form rejects a manifest", func(t *testing.T) {
		if _, err := workspace.LoadDeployment(func(key string) string {
			switch key {
			case "FOREMAN_WORKSPACES":
				return `[{"workspace_id":"ws-a","daemon_id":"foreman-a","token":"mdt_a"}]`
			case "FOREMAN_WORKSPACES_MANIFEST":
				return `["ws-a"]`
			}
			return ""
		}); err == nil {
			t.Fatal("the workspace-scoped credential form must refuse a manifest")
		}
	})
}
