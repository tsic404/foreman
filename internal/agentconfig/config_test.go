package agentconfig

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testNow = time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)

func testService(t *testing.T) *Service {
	t.Helper()
	service, err := New(Config{
		Store: NewMemoryStore(),
		Clock: func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return service
}

func testScope() Scope {
	return Scope{ServerID: "atlas", WorkspaceID: "core", Backend: "omp", Profile: "primary"}
}

// TestWriteValidatesAndNeverStoresContent is CFG-07's storage boundary: the
// revision is recorded, the content is validated and discarded, and neither
// the response nor the persisted registry carries it.
func TestWriteValidatesAndNeverStoresContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent-configs.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	service, err := New(Config{Store: store, Clock: func() time.Time { return testNow }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const marker = "fictional-provider-credential"
	view, err := service.Write(context.Background(), testScope(), WriteRequest{
		SecretRef: "omp-primary-config",
		FileName:  "config.json",
		Config:    json.RawMessage(`{"provider":"omp","token":"` + marker + `"}`),
	}, "ops")
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if view.Revision != 1 || view.SecretRef != "omp-primary-config" || view.Size == 0 {
		t.Fatalf("view = %+v, want the first revision", view)
	}
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(encoded), marker) {
		t.Fatalf("the view echoes the content: %s", encoded)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the registry: %v", err)
	}
	if strings.Contains(string(raw), marker) {
		t.Fatalf("the registry stores the content: %s", raw)
	}

	second, err := service.Write(context.Background(), testScope(), WriteRequest{
		SecretRef: "omp-primary-config",
		FileName:  "config.json",
		Config:    json.RawMessage(`{"provider":"omp","revision":2}`),
	}, "ops")
	if err != nil {
		t.Fatalf("second Write: %v", err)
	}
	if second.Revision != 2 {
		t.Fatalf("revision = %d, want 2", second.Revision)
	}
	if second.Digest == view.Digest {
		t.Fatal("a changed document kept the old digest")
	}
}

func TestWriteRejectsNonObjectAndUnsafeNames(t *testing.T) {
	service := testService(t)
	for _, request := range []WriteRequest{
		{SecretRef: "ref", FileName: "config.json", Config: json.RawMessage(`[]`)},
		{SecretRef: "ref", FileName: "config.json", Config: json.RawMessage(`null`)},
		{SecretRef: "ref", FileName: "../config.json", Config: json.RawMessage(`{}`)},
		{SecretRef: "ref", FileName: "config.yaml", Config: json.RawMessage(`{}`)},
		{SecretRef: "", FileName: "config.json", Config: json.RawMessage(`{}`)},
		{SecretRef: "ref", FileName: "config.json", Config: json.RawMessage(`{`)},
	} {
		if _, err := service.Write(context.Background(), testScope(), request, "ops"); err == nil {
			t.Fatalf("Write(%+v) succeeded, want a validation error", request)
		}
	}
}

// TestProjectionIsPerAttemptAndReadOnly is the runtime projection clause: each
// attempt gets its own Secret name and the file is mounted read-only under the
// fixed path.
func TestProjectionIsPerAttemptAndReadOnly(t *testing.T) {
	service := testService(t)
	if _, err := service.Write(context.Background(), testScope(), WriteRequest{
		SecretRef: "omp-primary-config", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp"}`),
	}, "ops"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	first, err := service.Project(context.Background(), testScope(), "attempt-1")
	if err != nil {
		t.Fatalf("Project attempt-1: %v", err)
	}
	second, err := service.Project(context.Background(), testScope(), "attempt-2")
	if err != nil {
		t.Fatalf("Project attempt-2: %v", err)
	}
	if !first.ReadOnly {
		t.Fatal("the projection is not read-only")
	}
	if first.MountPath != MountDir+"/config.json" {
		t.Fatalf("mount path = %q", first.MountPath)
	}
	if first.SecretName == second.SecretName {
		t.Fatalf("both attempts share Secret %q", first.SecretName)
	}
	if first.Revision != 1 || first.SecretRef != "omp-primary-config" {
		t.Fatalf("projection = %+v", first)
	}
}

func TestProjectionRequiresAWrittenRevision(t *testing.T) {
	service := testService(t)
	if _, err := service.Project(context.Background(), testScope(), "attempt-1"); err == nil {
		t.Fatal("Project succeeded without a revision")
	}
}

func TestAuditCarriesNoContent(t *testing.T) {
	service := testService(t)
	const marker = "fictional-audit-secret"
	if _, err := service.Write(context.Background(), testScope(), WriteRequest{
		SecretRef: "ref", FileName: "config.json",
		Config: json.RawMessage(`{"token":"` + marker + `"}`),
	}, "ops"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	entries, err := service.Audit(context.Background())
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	raw, err := json.Marshal(entries)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(raw), marker) {
		t.Fatalf("audit carries the content: %s", raw)
	}
}

// TestLongAttemptIDsStillProduceDNSLabelSizedNames keeps the derived Secret
// and volume names usable when an upstream attempt id is at the accepted
// maximum.
func TestLongAttemptIDsStillProduceDNSLabelSizedNames(t *testing.T) {
	service := testService(t)
	if _, err := service.Write(context.Background(), testScope(), WriteRequest{
		SecretRef: "ref", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp"}`),
	}, "ops"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	attemptA := strings.Repeat("a", 64)
	attemptB := strings.Repeat("b", 64)
	first, err := service.Project(context.Background(), testScope(), attemptA)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	second, err := service.Project(context.Background(), testScope(), attemptB)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	for _, name := range []string{first.SecretName, first.VolumeName, second.SecretName, second.VolumeName} {
		if len(name) > 63 {
			t.Errorf("derived name %q is %d characters, want at most 63", name, len(name))
		}
	}
	if first.SecretName == second.SecretName || first.VolumeName == second.VolumeName {
		t.Fatal("two attempts share a derived name")
	}
}

// TestProjectionFreezesRevisionAcrossRotation is CFG-07's frozen-binding
// clause: an attempt keeps the exact revision it first projected, while a new
// attempt sees the rotated one. A restart reads the same binding back.
func TestProjectionFreezesRevisionAcrossRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent-configs.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	service, err := New(Config{Store: store, Clock: func() time.Time { return testNow }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	first, err := service.Write(context.Background(), testScope(), WriteRequest{
		SecretRef: "omp-primary-config", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp","revision":1}`),
	}, "ops")
	if err != nil {
		t.Fatalf("first Write: %v", err)
	}
	projected, err := service.Project(context.Background(), testScope(), "attempt-1")
	if err != nil {
		t.Fatalf("Project attempt-1: %v", err)
	}
	if projected.Revision != first.Revision || projected.Digest != first.Digest {
		t.Fatalf("projection = %+v, want revision 1", projected)
	}
	second, err := service.Write(context.Background(), testScope(), WriteRequest{
		SecretRef: "omp-primary-config", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp","revision":2}`),
	}, "ops")
	if err != nil {
		t.Fatalf("second Write: %v", err)
	}
	again, err := service.Project(context.Background(), testScope(), "attempt-1")
	if err != nil {
		t.Fatalf("Project attempt-1 after rotation: %v", err)
	}
	if again.Revision != first.Revision || again.Digest != first.Digest {
		t.Fatalf("attempt-1 moved to %+v after rotation, want revision 1", again)
	}
	fresh, err := service.Project(context.Background(), testScope(), "attempt-2")
	if err != nil {
		t.Fatalf("Project attempt-2: %v", err)
	}
	if fresh.Revision != second.Revision || fresh.Digest != second.Digest {
		t.Fatalf("attempt-2 = %+v, want the new revision 2", fresh)
	}

	reloaded, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reload NewFileStore: %v", err)
	}
	restarted, err := New(Config{Store: reloaded, Clock: func() time.Time { return testNow }})
	if err != nil {
		t.Fatalf("reload New: %v", err)
	}
	afterRestart, err := restarted.Project(context.Background(), testScope(), "attempt-1")
	if err != nil {
		t.Fatalf("Project after restart: %v", err)
	}
	if afterRestart.Revision != first.Revision || afterRestart.Digest != first.Digest {
		t.Fatalf("attempt-1 after restart = %+v, want the frozen revision 1", afterRestart)
	}
}

// TestAttemptCannotSwitchScope keeps one attempt from silently reading another
// configuration scope: a mismatch is a refusal, not a new projection.
func TestAttemptCannotSwitchScope(t *testing.T) {
	service := testService(t)
	if _, err := service.Write(context.Background(), testScope(), WriteRequest{
		SecretRef: "ref", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp"}`),
	}, "ops"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := service.Project(context.Background(), testScope(), "attempt-1"); err != nil {
		t.Fatalf("Project: %v", err)
	}
	other := testScope()
	other.Profile = "secondary"
	if _, err := service.Write(context.Background(), other, WriteRequest{
		SecretRef: "ref-secondary", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp"}`),
	}, "ops"); err != nil {
		t.Fatalf("second scope Write: %v", err)
	}
	if _, err := service.Project(context.Background(), other, "attempt-1"); err == nil {
		t.Fatal("an attempt moved to another configuration scope")
	}
}

// TestFileStoreWriteFailureKeepsRevision: a failed rename leaves both the
// visible revision and the persisted document on the old value.
func TestFileStoreWriteFailureKeepsRevision(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent-configs.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	service, err := New(Config{Store: store, Clock: func() time.Time { return testNow }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	first, err := service.Write(context.Background(), testScope(), WriteRequest{
		SecretRef: "ref", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp"}`),
	}, "ops")
	if err != nil {
		t.Fatalf("first Write: %v", err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read registry: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove registry: %v", err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("mkdir registry path: %v", err)
	}
	if _, err := service.Write(context.Background(), testScope(), WriteRequest{
		SecretRef: "ref", FileName: "config.json",
		Config: json.RawMessage(`{"provider":"omp","revision":2}`),
	}, "ops"); err == nil {
		t.Fatal("Write succeeded although the registry rename could not land")
	}
	current, ok, err := service.Get(context.Background(), testScope())
	if err != nil || !ok {
		t.Fatalf("Get: %+v, %v", current, err)
	}
	if current.Revision != first.Revision || current.Digest != first.Digest {
		t.Fatalf("visible revision = %+v, want revision 1", current)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove blocking directory: %v", err)
	}
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("restore registry: %v", err)
	}
	reloaded, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	persisted, ok, err := reloaded.GetRevision(context.Background(), testScope())
	if err != nil || !ok {
		t.Fatalf("persisted revision: %+v, %v", persisted, err)
	}
	if persisted.Revision != first.Revision || persisted.Digest != first.Digest {
		t.Fatalf("persisted revision = %+v, want revision 1", persisted)
	}
}
