package jobbuilder

import (
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

// TestSameTaskIDOnTwoSourcesGetsDistinctObjects pins the attempt identity the
// Job face derives: one upstream task id on two registered servers must not
// collide on the daemon id, the Job name, the credential Secret or the token
// subject. The source-less form keeps its historical name.
func TestSameTaskIDOnTwoSourcesGetsDistinctObjects(t *testing.T) {
	cfg := testConfig()
	entry := testEntry()
	entry.WorkspaceID = "ws-shared"

	entry.SourceID = "src-a"
	jobA, secretA := mustBuild(t, cfg, entry)
	entry.SourceID = "src-b"
	jobB, secretB := mustBuild(t, cfg, entry)

	if jobA.Name == jobB.Name {
		t.Fatalf("both sources rendered Job name %q", jobA.Name)
	}
	if secretA.Name == secretB.Name {
		t.Fatalf("both sources rendered credential Secret %q", secretA.Name)
	}
	if got := jobA.Labels[registry.LabelSourceID]; got != "src-a" {
		t.Fatalf("Job source label = %q, want src-a", got)
	}
	if got := jobB.Labels[registry.LabelSourceID]; got != "src-b" {
		t.Fatalf("Job source label = %q, want src-b", got)
	}
	if got := jobA.Annotations[registry.AnnoDaemonID]; got == jobB.Annotations[registry.AnnoDaemonID] {
		t.Fatalf("both sources share daemon id %q", got)
	}
	if got := secretA.Labels[registry.LabelSourceID]; got != "src-a" {
		t.Fatalf("Secret source label = %q, want src-a", got)
	}

	daemonA := DaemonIDForInScope(testTaskID, "src-a", "ws-shared")
	daemonB := DaemonIDForInScope(testTaskID, "src-b", "ws-shared")
	if daemonA == daemonB {
		t.Fatalf("both sources derived daemon id %q", daemonA)
	}
	if legacy := DaemonIDForInScope(testTaskID, "", "ws-shared"); legacy != DaemonIDFor(testTaskID) {
		t.Fatalf("source-less daemon id = %q, want the historical %q", legacy, DaemonIDFor(testTaskID))
	}
}
