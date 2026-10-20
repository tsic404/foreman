// TC-tech-multiws-03 implements AC-83: the truth faces — workspace state,
// runtime profile replay source, model catalog slots and the skill-bundle
// cache — are read and written per workspace and never replayed across.
package acceptance

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

func TestTechMultiWS03TruthFacesPartitioned(t *testing.T) {
	m := newMultiWS(t, "ws-a", "ws-b")

	// C-1 seeds each workspace's own carrier. The stubs are separate servers,
	// so a cross-workspace read would have to come from the wrong client.
	m.upstream["ws-a"].setRegisterWorkspace(
		`[{"url":"https://example.invalid/a"}]`, "va",
		`{"co_authored_by_enabled":true,"owner":"a"}`)
	m.upstream["ws-b"].setRegisterWorkspace(
		`[{"url":"https://example.invalid/b"}]`, "vb",
		`{"co_authored_by_enabled":false,"owner":"b"}`)
	m.upstream["ws-a"].setRuntimeID("omp", "", "rid-a")
	m.upstream["ws-b"].setRuntimeID("omp", "", "rid-b")

	discovery := func(name string) registry.Discovery {
		return registry.Discovery{CLIVersion: "0.6.1", Runtimes: []registry.DiscoveredRuntime{{Name: name, Type: "omp", Version: "0.6.1"}}}
	}
	if err := m.clients["ws-a"].RegisterWithDiscovery(context.Background(), discovery("foreman-a")); err != nil {
		t.Fatalf("C1(A): %v", err)
	}
	if err := m.clients["ws-b"].RegisterWithDiscovery(context.Background(), discovery("foreman-b")); err != nil {
		t.Fatalf("C1(B): %v", err)
	}
	stateA := m.clients["ws-a"].WorkspaceState()
	stateB := m.clients["ws-b"].WorkspaceState()
	if string(stateA.Settings) == string(stateB.Settings) {
		t.Fatalf("two workspaces seeded the same settings: %s", stateA.Settings)
	}
	assertRawEqual(t, "A settings", stateA.Settings, `{"co_authored_by_enabled":true,"owner":"a"}`)
	assertRawEqual(t, "B settings", stateB.Settings, `{"co_authored_by_enabled":false,"owner":"b"}`)
	assertRawEqual(t, "A repos", stateA.Repos, `[{"url":"https://example.invalid/a"}]`)
	assertRawEqual(t, "B repos", stateB.Repos, `[{"url":"https://example.invalid/b"}]`)

	// C-22 refresh on A replaces only A's snapshot; B's last success stands.
	m.upstream["ws-a"].setRepos(200, `{"workspace_id":"ws-a","repos":[{"url":"https://example.invalid/a2"}],"repos_version":"va2","settings":{"owner":"a2"}}`, nil)
	if err := m.clients["ws-a"].RefreshWorkspaceState(context.Background()); err != nil {
		t.Fatalf("C-22(A): %v", err)
	}
	assertRawEqual(t, "A refreshed settings", m.clients["ws-a"].WorkspaceState().Settings, `{"owner":"a2"}`)
	assertRawEqual(t, "B settings after A refresh", m.clients["ws-b"].WorkspaceState().Settings, `{"co_authored_by_enabled":false,"owner":"b"}`)

	// S-3 replay source per workspace: each client replays its own C-18 body.
	m.upstream["ws-a"].setProfiles(200, `{"workspace_id":"ws-a","runtime_profiles":[{"id":"pa","display_name":"A"}]}`)
	m.upstream["ws-b"].setProfiles(200, `{"workspace_id":"ws-b","runtime_profiles":[{"id":"pb","display_name":"B"}]}`)
	if err := m.clients["ws-a"].RuntimeProfiles(context.Background()); err != nil {
		t.Fatalf("C-18(A): %v", err)
	}
	if err := m.clients["ws-b"].RuntimeProfiles(context.Background()); err != nil {
		t.Fatalf("C-18(B): %v", err)
	}
	codeA, bodyA := m.clients["ws-a"].S3Response()
	codeB, bodyB := m.clients["ws-b"].S3Response()
	if codeA != 200 || codeB != 200 {
		t.Fatalf("S-3 codes = %d/%d, want 200/200", codeA, codeB)
	}
	if string(bodyA) == string(bodyB) {
		t.Fatalf("S-3 replay sources are identical across workspaces: %s", bodyA)
	}
	var profileA, profileB struct {
		WorkspaceID string `json:"workspace_id"`
	}
	_ = json.Unmarshal(bodyA, &profileA)
	_ = json.Unmarshal(bodyB, &profileB)
	if profileA.WorkspaceID != "ws-a" || profileB.WorkspaceID != "ws-b" {
		t.Errorf("S-3 workspace ids = %q/%q, want ws-a/ws-b", profileA.WorkspaceID, profileB.WorkspaceID)
	}

	// The skill-bundle cache is process-local but partitioned: the same
	// payload in two workspaces is a miss on both sides, so each upstream is
	// touched exactly once.
	m.upstream["ws-a"].setSkillAnswer(200, `{"skills":[]}`)
	m.upstream["ws-b"].setSkillAnswer(200, `{"skills":[]}`)
	payload := []byte(`{"skills":[{"id":"s1","source":"workspace","hash":"h1"}]}`)
	cacheA := m.set.SkillBundles("ws-a", m.metrics)
	cacheB := m.set.SkillBundles("ws-b", m.metrics)
	if cacheA == nil || cacheB == nil {
		t.Fatal("skill caches missing")
	}
	if _, _, err := cacheA.Resolve(context.Background(), "task-a", "rid-a", payload); err != nil {
		t.Fatalf("resolve A: %v", err)
	}
	if _, _, err := cacheB.Resolve(context.Background(), "task-b", "rid-b", payload); err != nil {
		t.Fatalf("resolve B: %v", err)
	}
	if got := m.upstream["ws-a"].skillCalls(); got != 1 {
		t.Errorf("upstream A resolve calls = %d, want 1", got)
	}
	if got := m.upstream["ws-b"].skillCalls(); got != 1 {
		t.Errorf("upstream B resolve calls = %d, want 1 (a cross-workspace cache hit leaked)", got)
	}
}
