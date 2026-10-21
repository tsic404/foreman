package main

import (
	"bytes"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/workspace"
)

// workspaceSetFor builds the configured workspace set the startup holds.
func workspaceSetFor(t *testing.T, ids ...string) workspace.Set {
	t.Helper()
	identities := make([]workspace.Identity, 0, len(ids))
	for _, id := range ids {
		identities = append(identities, workspace.Identity{WorkspaceID: id, DaemonID: "foreman-" + id, Token: "mdt_" + id})
	}
	set, err := workspace.NewSet(identities)
	if err != nil {
		t.Fatalf("workspace.NewSet: %v", err)
	}
	return set
}

// clientSetFor groups the test clients the way startup does.
func clientSetFor(t *testing.T, clients ...*proxy.Client) *proxy.ClientSet {
	t.Helper()
	set, err := proxy.NewClientSet(clients...)
	if err != nil {
		t.Fatalf("proxy.NewClientSet: %v", err)
	}
	return set
}

// policyConfigForRefs points the policy gate at a display-name partition key
// and the given resolution refs.
func policyConfigForRefs(t *testing.T, key string, refs []jobbuilder.WorkspaceRef) (jobbuilder.Config, *strings.Builder) {
	t.Helper()
	manifest := `
workspaces:
  ` + key + `:
    agents:
      omp:
        resourcePolicy: agent
`
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write policy: %v", err)
	}
	cfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("jobbuilder.LoadConfig: %v", err)
	}
	logs := &strings.Builder{}
	cfg.PolicyPath = path
	cfg.Workspaces = refs
	cfg.Logger = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return cfg, logs
}

// TestPolicyWorkspaceRefsSeedsOnlySeededNames pins the §1.1/AC-86 boundary of
// the partition-key resolution set: a user-scoped discovery failure already
// refused startup (AC-96), and a workspace-scoped C-17 transient failure only
// warns (AC-86) — so its unseeded name must never be offered to the policy
// gate as a display name.
func TestPolicyWorkspaceRefsSeedsOnlySeededNames(t *testing.T) {
	t.Run("unseeded C-17 contributes id only", func(t *testing.T) {
		stub := newStartupStub(t)
		stub.c17Status = http.StatusInternalServerError
		client := startupClient(stub.server.URL, "ws-a")

		refs := policyWorkspaceRefs(clientSetFor(t, client), workspaceSetFor(t, "ws-a"))

		if len(refs) != 1 {
			t.Fatalf("refs = %v, want one", refs)
		}
		if refs[0].ID != "ws-a" || refs[0].Name != "" {
			t.Errorf("ref = %+v, want id ws-a with no name (unseeded)", refs[0])
		}
	})

	t.Run("seeded C-17 contributes the name", func(t *testing.T) {
		stub := newStartupStub(t)
		client := startupClient(stub.server.URL, "ws-a")
		if err := client.ValidateWorkspaceScope(t.Context()); err != nil {
			t.Fatalf("ValidateWorkspaceScope: %v", err)
		}

		refs := policyWorkspaceRefs(clientSetFor(t, client), workspaceSetFor(t, "ws-a"))

		if len(refs) != 1 || refs[0].Name != "Workspace A" {
			t.Errorf("refs = %v, want the seeded display name", refs)
		}
	})
}

// TestWorkspaceScopeC17TransientFailureDoesNotRefuseNameKeys drives the full
// startup seam: the self-check's transient C-17 failure (warn + continue) and
// the policy gate's name key over the same workspace. The boot must go through
// with the key unseeded — never absent.
func TestWorkspaceScopeC17TransientFailureDoesNotRefuseNameKeys(t *testing.T) {
	stub := newStartupStub(t)
	stub.c17Status = http.StatusInternalServerError
	client := startupClient(stub.server.URL, "ws-a")
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))

	if err := runStartupSelfCheck(t.Context(), workspace.ScopeWorkspace,
		[]*proxy.Client{client}, log); err != nil {
		t.Fatalf("a transient C-17 failure must not refuse startup: %v", err)
	}

	refs := policyWorkspaceRefs(clientSetFor(t, client), workspaceSetFor(t, "ws-a"))
	if len(refs) != 1 || refs[0].Name != "" {
		t.Fatalf("refs = %v, want the unseeded id-only ref", refs)
	}

	cfg, policyLogs := policyConfigForRefs(t, "Workspace A", refs)
	if _, err := jobbuilder.LoadPolicy(cfg); err != nil {
		t.Fatalf("a name-key policy over an unseeded authority refused startup: %v", err)
	}
	if !strings.Contains(logs.String(), "workspace.credential_check_failed") {
		t.Errorf("missing the self-check warn:\n%s", logs.String())
	}
	if !strings.Contains(policyLogs.String(), `"key":"Workspace A","conclusion":"unseeded"`) {
		t.Errorf("the unseeded conclusion is not recorded:\n%s", policyLogs.String())
	}
}

// TestC17RecoversThenNameKeyResolves drives the recovery face: the same
// workspace-scoped client whose C-17 failed transiently picks up the name on a
// later attempt, and the name key then resolves.
func TestC17RecoversThenNameKeyResolves(t *testing.T) {
	stub := newStartupStub(t)
	stub.c17Status = http.StatusInternalServerError
	client := startupClient(stub.server.URL, "ws-a")
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	if err := runStartupSelfCheck(t.Context(), workspace.ScopeWorkspace,
		[]*proxy.Client{client}, log); err != nil {
		t.Fatalf("a transient C-17 failure must not refuse startup: %v", err)
	}

	stub.setC17(http.StatusOK, `[{"id":"ws-a","name":"Workspace A"}]`)
	if err := client.ValidateWorkspaceScope(t.Context()); err != nil {
		t.Fatalf("recovered C-17 attempt: %v", err)
	}

	refs := policyWorkspaceRefs(clientSetFor(t, client), workspaceSetFor(t, "ws-a"))
	if len(refs) != 1 || refs[0].Name != "Workspace A" {
		t.Fatalf("refs = %v, want the recovered display name", refs)
	}

	cfg, policyLogs := policyConfigForRefs(t, "Workspace A", refs)
	if _, err := jobbuilder.LoadPolicy(cfg); err != nil {
		t.Fatalf("a name-key policy over a recovered authority refused startup: %v", err)
	}
	if !strings.Contains(policyLogs.String(), `"key":"Workspace A","conclusion":"resolved"`) {
		t.Errorf("the recovered key did not resolve:\n%s", policyLogs.String())
	}
}
