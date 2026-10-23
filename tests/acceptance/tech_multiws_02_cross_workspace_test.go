// TC-tech-multiws-02 implements AC-82: the negative cross-workspace faces —
// Job Tokens, credential revocation, catalog-probe sessions and policy
// definitions never cross the workspace partition.
package acceptance

import (
	"errors"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

func TestTechMultiWS02CrossWorkspaceRejected(t *testing.T) {
	m := newMultiWS(t, "ws-a", "ws-b")
	entryA := m.claim("ws-a", "task-a1", "rid-a")
	entryB := m.claim("ws-b", "task-b1", "rid-b")
	tokenA := m.builder.tokenFor(entryA.JobName)
	tokenB := m.builder.tokenFor(entryB.JobName)
	if tokenA == "" || tokenB == "" {
		t.Fatal("missing Job Tokens")
	}

	// A's token addresses B's task: the workspace is part of the resource
	// adjudication, so this is a 403 (not a 404).
	if code, body := m.call("GET", "/api/daemon/tasks/"+entryB.TaskID+"/status", tokenA, nil); code != 403 {
		t.Errorf("A token on B task = %d, want 403: %s", code, body)
	}
	// A's token addresses B's workspace face: 403, and B's works untouched.
	if code, body := m.call("GET", "/api/daemon/workspaces/ws-b/repos", tokenA, nil); code != 403 {
		t.Errorf("A token on B repos = %d, want 403: %s", code, body)
	}
	if code, body := m.call("GET", "/api/daemon/workspaces/ws-a/repos", tokenA, nil); code != 200 {
		t.Errorf("A token on A repos = %d, want 200: %s", code, body)
	}

	// Revoking A does not touch B's in-flight credential.
	m.issuer.Revoke(m.issuer.Hash(tokenA), "ws-a")
	if code, _ := m.call("GET", "/api/daemon/workspaces/ws-a/repos", tokenA, nil); code != 401 {
		t.Errorf("revoked A token = %d, want 401", code)
	}
	if code, body := m.call("GET", "/api/daemon/workspaces/ws-b/repos", tokenB, nil); code != 200 {
		t.Errorf("B token after A revocation = %d, want 200: %s", code, body)
	}

	// Catalog-probe sessions are workspace-pinned: A's probe token cannot
	// address B's workspace, and its own answer still works.
	probeA, err := m.sched.StartProbe(t.Context(), "req-a", "rid-a", "ws-a", registry.ProbeCommandDriven, 10*time.Second)
	if err != nil {
		t.Fatalf("StartProbe(A): %v", err)
	}
	probeTokenA := m.builder.tokenFor(probeA.JobName)
	if probeTokenA == "" {
		t.Fatal("A probe token missing")
	}
	if code, body := m.call("GET", "/api/daemon/workspaces/ws-b/repos", probeTokenA, nil); code != 403 {
		t.Errorf("A probe token on B repos = %d, want 403: %s", code, body)
	}
	if code, body := m.call("GET", "/api/daemon/workspaces/ws-a/repos", probeTokenA, nil); code != 200 {
		t.Errorf("A probe token on A repos = %d, want 200: %s", code, body)
	}

	// Policy definitions are selected from the Job's own workspace partition:
	// the same agent name exists on both sides, but only A's table defines it.
	const policy = `workspaces:
  ws-a:
    agents:
      shared:
        resourcePolicy: agent
        capPolicy: agent
        nodeSelectorPolicy: agent
        resources:
          requests: {cpu: 500m, memory: 128Mi}
        capabilities:
          add: ["NET_ADMIN"]
        nodeSelector: {pool: a}
  ws-b:
    agents: {}
`
	builder := newRealMultiBuilder(t, m, []string{"ws-a", "ws-b"}, policy)
	jobA, _, _, err := builder.Build(taskEntryFor("11111111-1111-4111-8111-111111111111", "ws-a", "shared"))
	if err != nil {
		t.Fatalf("Build(A): %v", err)
	}
	jobB, _, _, err := builder.Build(taskEntryFor("22222222-2222-4222-8222-222222222222", "ws-b", "shared"))
	if err != nil {
		t.Fatalf("Build(B): %v", err)
	}

	containerA := jobA.Spec.Template.Spec.Containers[0]
	if got := containerA.Resources.Requests.Cpu().String(); got != "500m" {
		t.Errorf("A cpu request = %q, want 500m", got)
	}
	if containerA.SecurityContext == nil || containerA.SecurityContext.Capabilities == nil ||
		len(containerA.SecurityContext.Capabilities.Add) != 1 || containerA.SecurityContext.Capabilities.Add[0] != "NET_ADMIN" {
		t.Errorf("A capabilities = %+v, want the NET_ADMIN addition", containerA.SecurityContext)
	}
	if got := jobA.Spec.Template.Spec.NodeSelector["pool"]; got != "a" {
		t.Errorf("A nodeSelector pool = %q, want a", got)
	}

	containerB := jobB.Spec.Template.Spec.Containers[0]
	if !containerB.Resources.Requests.Cpu().IsZero() {
		t.Errorf("B cpu request = %q, want none (A's definition leaked)", containerB.Resources.Requests.Cpu())
	}
	if _, has := jobB.Spec.Template.Spec.NodeSelector["pool"]; has {
		t.Errorf("B nodeSelector carries A's pool definition: %+v", jobB.Spec.Template.Spec.NodeSelector)
	}
}

// The revocation set stays partitioned after a restart seeds the in-process
// cache from the shared store (AC-74 / AC-82): the digest under test is B's
// own token digest recorded under A, the shape a cross-workspace digest
// collision would produce and the only way one workspace's revocation could
// ever reach another's adjudication.
func TestTechMultiWS02RevocationRestoreStaysPartitioned(t *testing.T) {
	m := newMultiWS(t, "ws-a", "ws-b")
	m.claim("ws-a", "task-a1", "rid-a")
	entryB := m.claim("ws-b", "task-b1", "rid-b")
	tokenB := m.builder.tokenFor(entryB.JobName)
	if tokenB == "" {
		t.Fatal("missing B Job Token")
	}
	digestB := m.issuer.Hash(tokenB)
	ctx := t.Context()

	store, _ := multi08Store(t)
	if err := store.Revoke(ctx, "ws-a", digestB, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Revoke under ws-a: %v", err)
	}
	restarted, err := proxy.JobTokenIssuer(testKey, m.reg, m.probes, auth.DefaultJobTokenTTL, store)
	if err != nil {
		t.Fatalf("JobTokenIssuer: %v", err)
	}
	if err := restarted.RestoreRevocations(ctx); err != nil {
		t.Fatalf("RestoreRevocations: %v", err)
	}
	if _, err := restarted.Verify(tokenB, auth.RequestScope{}); err != nil {
		t.Fatalf("B token adjudicated by A's revocation: %v", err)
	}

	// Control: the same digest recorded under B is B's own revocation, so the
	// restored set must reject it — proving the seed really ran.
	if err := store.Revoke(ctx, "ws-b", digestB, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Revoke under ws-b: %v", err)
	}
	controlled, err := proxy.JobTokenIssuer(testKey, m.reg, m.probes, auth.DefaultJobTokenTTL, store)
	if err != nil {
		t.Fatalf("JobTokenIssuer: %v", err)
	}
	if err := controlled.RestoreRevocations(ctx); err != nil {
		t.Fatalf("RestoreRevocations: %v", err)
	}
	if _, err := controlled.Verify(tokenB, auth.RequestScope{}); !errors.Is(err, auth.ErrRevoked) {
		t.Fatalf("restored ws-b revocation: Verify err = %v, want ErrRevoked", err)
	}
}
