package workspace

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func envMap(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestLoadDeploymentUserScope(t *testing.T) {
	dep, err := LoadDeployment(envMap(map[string]string{
		"MULTICA_TOKEN":     "mul_user",
		"FOREMAN_DAEMON_ID": "foreman",
	}))
	if err != nil {
		t.Fatalf("LoadDeployment: %v", err)
	}
	if dep.Scope != ScopeUser {
		t.Fatalf("scope = %v, want user", dep.Scope)
	}
	if dep.UserToken != "mul_user" || dep.BaseDaemonID != "foreman" {
		t.Errorf("deployment = %+v, want the token and base identity", dep)
	}
	if dep.Manifest != nil {
		t.Errorf("manifest = %v, want none", dep.Manifest)
	}

	// The base identity defaults when unset.
	dep, err = LoadDeployment(envMap(map[string]string{"MULTICA_TOKEN": "mdt_user"}))
	if err != nil {
		t.Fatalf("LoadDeployment without a base identity: %v", err)
	}
	if dep.BaseDaemonID != DefaultDaemonID {
		t.Errorf("base identity = %q, want %q", dep.BaseDaemonID, DefaultDaemonID)
	}
}

func TestLoadDeploymentManifest(t *testing.T) {
	dep, err := LoadDeployment(envMap(map[string]string{
		"MULTICA_TOKEN":               "mul_user",
		"FOREMAN_WORKSPACES_MANIFEST": `["ws-a","ws-b","ws-a"]`,
	}))
	if err != nil {
		t.Fatalf("LoadDeployment: %v", err)
	}
	if len(dep.Manifest) != 2 || dep.Manifest[0] != "ws-a" || dep.Manifest[1] != "ws-b" {
		t.Fatalf("manifest = %v, want the de-duplicated list", dep.Manifest)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(path, []byte(`["ws-a"]`), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	dep, err = LoadDeployment(envMap(map[string]string{
		"MULTICA_TOKEN":                    "mul_user",
		"FOREMAN_WORKSPACES_MANIFEST_FILE": path,
	}))
	if err != nil {
		t.Fatalf("LoadDeployment (manifest file): %v", err)
	}
	if len(dep.Manifest) != 1 || dep.Manifest[0] != "ws-a" {
		t.Fatalf("manifest = %v, want [ws-a]", dep.Manifest)
	}
}

func TestLoadDeploymentRefusesBadShapes(t *testing.T) {
	cases := map[string]map[string]string{
		"empty manifest": {
			"MULTICA_TOKEN":               "mul_user",
			"FOREMAN_WORKSPACES_MANIFEST": `[]`,
		},
		"manifest with the workspace-scoped list": {
			"FOREMAN_WORKSPACES":          `[{"workspace_id":"ws-a","daemon_id":"a","token":"mdt_a"}]`,
			"FOREMAN_WORKSPACES_MANIFEST": `["ws-a"]`,
		},
		"manifest with the legacy single-workspace form": {
			"MULTICA_WORKSPACE_ID":        "ws-a",
			"MULTICA_TOKEN":               "mdt_a",
			"FOREMAN_WORKSPACES_MANIFEST": `["ws-a"]`,
		},
		"manifest and manifest file together": {
			"MULTICA_TOKEN":                    "mul_user",
			"FOREMAN_WORKSPACES_MANIFEST":      `["ws-a"]`,
			"FOREMAN_WORKSPACES_MANIFEST_FILE": "/tmp/whatever.json",
		},
		"no credential at all": {},
		"bad credential form": {
			"MULTICA_TOKEN": "nope",
		},
	}
	for name, values := range cases {
		if _, err := LoadDeployment(envMap(values)); err == nil {
			t.Errorf("%s: LoadDeployment accepted the shape", name)
		}
	}
}

func TestLoadDeploymentWorkspaceScope(t *testing.T) {
	dep, err := LoadDeployment(envMap(map[string]string{
		"FOREMAN_WORKSPACES": `[
			{"workspace_id":"ws-a","daemon_id":"foreman-a","token":"mdt_a"},
			{"workspace_id":"ws-b","daemon_id":"foreman-b","token":"mdt_b"}
		]`,
	}))
	if err != nil {
		t.Fatalf("LoadDeployment: %v", err)
	}
	if dep.Scope != ScopeWorkspace {
		t.Fatalf("scope = %v, want workspace", dep.Scope)
	}
	if got := dep.Set.IDs(); len(got) != 2 || got[0] != "ws-a" || got[1] != "ws-b" {
		t.Fatalf("set = %v, want [ws-a ws-b]", got)
	}

	// The legacy triple is the workspace-scoped form with one pair.
	dep, err = LoadDeployment(envMap(map[string]string{
		"MULTICA_WORKSPACE_ID": "ws-a",
		"MULTICA_TOKEN":        "mdt_a",
	}))
	if err != nil {
		t.Fatalf("LoadDeployment (legacy): %v", err)
	}
	if dep.Scope != ScopeWorkspace || dep.Set.Len() != 1 {
		t.Fatalf("legacy deployment = %+v, want a workspace-scoped set of one", dep)
	}
}

func TestResolveDiscoverySet(t *testing.T) {
	visible := []Ref{{ID: "ws-a", Name: "A"}, {ID: "ws-b", Name: "B"}, {ID: "ws-c", Name: "C"}}

	set, excluded, err := ResolveDiscoverySet(visible, nil)
	if err != nil {
		t.Fatalf("ResolveDiscoverySet: %v", err)
	}
	if len(set) != 3 || len(excluded) != 0 {
		t.Fatalf("set/excluded = %v/%v, want the whole visible set", set, excluded)
	}

	set, excluded, err = ResolveDiscoverySet(visible, []string{"ws-a", "ws-c"})
	if err != nil {
		t.Fatalf("ResolveDiscoverySet: %v", err)
	}
	if len(set) != 2 || set[0].ID != "ws-a" || set[1].ID != "ws-c" {
		t.Fatalf("set = %v, want the manifested workspaces in visible order", set)
	}
	if len(excluded) != 1 || excluded[0].ID != "ws-b" {
		t.Fatalf("excluded = %v, want the visible-but-unlisted ws-b", excluded)
	}

	if _, _, err := ResolveDiscoverySet(visible, []string{"ws-a", "ws-ghost"}); err == nil {
		t.Fatal("a manifest entry outside the visible set must be an error")
	} else if !strings.Contains(err.Error(), "ws-ghost") {
		t.Errorf("error = %v, want it to name ws-ghost", err)
	}
}

func TestDerivedDaemonID(t *testing.T) {
	a := DerivedDaemonID("foreman", "ws-a")
	if a != "foreman-ws-a" {
		t.Fatalf("derived id = %q", a)
	}
	if a == DerivedDaemonID("foreman", "ws-b") {
		t.Fatal("two workspaces share a derived identity")
	}
	if a != DerivedDaemonID("foreman", "ws-a") {
		t.Fatal("derivation is not stable")
	}
}

func TestDeploymentIdentities(t *testing.T) {
	dep := Deployment{Scope: ScopeUser, UserToken: "mul_user", BaseDaemonID: "foreman"}
	identities := dep.Identities([]Ref{{ID: "ws-a"}, {ID: "ws-b"}})
	if len(identities) != 2 {
		t.Fatalf("identities = %d, want 2", len(identities))
	}
	for i, identity := range identities {
		if identity.Token != "mul_user" {
			t.Errorf("identity %d token = %q, want the single user-scoped credential", i, identity.Token)
		}
	}
	if identities[0].DaemonID == identities[1].DaemonID {
		t.Fatal("identities collide across workspaces")
	}
	if _, err := NewSet(identities); err != nil {
		t.Fatalf("NewSet on derived identities: %v", err)
	}
}
