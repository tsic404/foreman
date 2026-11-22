package environment_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/environment/environmenttest"
)

// A document written by the previous release has no Registered field and may
// carry a tree id from a failed first attempt. The migration must not turn
// that observation into ownership, and a never-verified target that was
// drained must stay out of the registered count.
func TestFileStoreMigratesUntrustedTreeIDsAndRegistration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "environments.json")
	raw := `{
  "environments": [
    {
      "ID": "drained",
      "Target": {"Endpoint":"drained.example.test:7443","TLSRef":"ref/drained","StateRoot":"/srv/rsbx/drained","TrustDomain":"build","AllowedSources":["atlas"],"Capabilities":["nix"],"Concurrency":2},
      "Phase": "draining",
      "Verification": {"State":"failed","TreeID":"tree-observed","LastAttemptAt":"2026-11-21T09:00:00Z"},
      "Revision": 2
    },
    {
      "ID": "stale",
      "Target": {"Endpoint":"stale.example.test:7443","TLSRef":"ref/stale","StateRoot":"/srv/rsbx/stale","TrustDomain":"build","AllowedSources":["atlas"],"Capabilities":["nix"],"Concurrency":2},
      "Phase": "active",
      "Verification": {"State":"stale","TreeID":"tree-owned","LastAttemptAt":"2026-11-21T09:00:00Z"},
      "Revision": 3
    }
  ],
  "tombstones": []
}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("write the old document: %v", err)
	}
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newFileService(t, path, []string{"build"}, probe)
	ctx := t.Context()

	drained, err := service.Get(ctx, "drained", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get drained: %v", err)
	}
	if drained.Registered {
		t.Fatalf("a drained, never-verified target was migrated as registered: %+v", drained)
	}
	if drained.Verification.TreeID != "" || drained.Verification.ObservedTreeID != "tree-observed" {
		t.Fatalf("migrated verification = %+v, want the tree only as an observation", drained.Verification)
	}
	stale, err := service.Get(ctx, "stale", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get stale: %v", err)
	}
	if !stale.Registered || stale.Verification.TreeID != "tree-owned" {
		t.Fatalf("a stale, previously verified target lost its registration: %+v", stale)
	}
	if got := service.CountRegistered(ctx); got != 1 {
		t.Fatalf("CountRegistered = %d, want only the previously verified target", got)
	}
}

// A legacy first attempt that failed, followed by an edit, is a pending row
// with a stale verification and a tree id. That combination is not evidence of
// ownership: the tree id must become an observation, the row must not count as
// an environment, and the original id must verify normally after the repair.
func TestFileStoreKeepsAFailedFirstAttemptRepairable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "environments.json")
	raw := `{
  "environments": [
    {
      "ID": "build-a",
      "Target": {"Endpoint":"build-a.example.test:7443","TLSRef":"ref/build-a","StateRoot":"/srv/rsbx/build-a","TrustDomain":"build","AllowedSources":["atlas"],"Capabilities":["nix"],"Concurrency":2},
      "Phase": "pending",
      "Verification": {"State":"stale","TreeID":"tree-wrong","LastAttemptAt":"2026-11-21T09:00:00Z"},
      "Revision": 2
    }
  ],
  "tombstones": []
}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("write the old document: %v", err)
	}
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-real")
	service := newFileService(t, path, []string{"build"}, probe)
	ctx := t.Context()

	before, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get before the repair: %v", err)
	}
	if before.Registered {
		t.Fatalf("a failed first attempt was migrated as registered: %+v", before)
	}
	if before.Verification.TreeID != "" || before.Verification.ObservedTreeID != "tree-wrong" {
		t.Fatalf("migrated verification = %+v, want the failed tree only as an observation", before.Verification)
	}
	if got := service.CountRegistered(ctx); got != 0 {
		t.Fatalf("CountRegistered before the repair = %d, want 0", got)
	}

	verified, err := service.Verify(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Verify the repaired target under its original id: %v", err)
	}
	if !verified.Registered || !verified.Schedulable || verified.Verification.TreeID != "tree-real" {
		t.Fatalf("repaired view = %+v, want the real tree under the original id", verified)
	}
	if got := service.CountRegistered(ctx); got != 1 {
		t.Fatalf("CountRegistered after the repair = %d, want 1", got)
	}
}

func TestFileStoreWriteFailureDoesNotPublishARegistration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "environments.json")
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newFileService(t, path, []string{"build"}, probe)
	ctx := t.Context()

	if err := os.Mkdir(path+".tmp", 0o700); err != nil {
		t.Fatalf("make the temp path unwritable: %v", err)
	}
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err == nil {
		t.Fatal("Register succeeded although the document write failed")
	}
	if _, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("Get after the failed write = %v, want ErrNotFound", err)
	}
	if candidates := service.Candidates(ctx, environment.CandidateFilter{}); len(candidates) != 0 {
		t.Fatalf("Candidates after the failed write = %+v, want none", candidates)
	}
	if got := service.CountRegistered(ctx); got != 0 {
		t.Fatalf("CountRegistered = %d, want 0", got)
	}

	if err := os.Remove(path + ".tmp"); err != nil {
		t.Fatalf("restore the temp path: %v", err)
	}
	reopened := newFileService(t, path, []string{"build"}, probe)
	if got := reopened.CountRegistered(ctx); got != 0 {
		t.Fatalf("CountRegistered after restart = %d, want 0", got)
	}
}

func TestFileStoreSlotWriteFailureLeavesTheLedgerUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "environments.json")
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newFileService(t, path, []string{"build"}, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := os.Mkdir(path+".tmp", 0o700); err != nil {
		t.Fatalf("make the temp path unwritable: %v", err)
	}
	if _, err := service.AcquireSlot(ctx, "build-a", "slot-1"); err == nil {
		t.Fatal("AcquireSlot succeeded although the document write failed")
	}
	view, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if view.ActiveSlots != 0 || view.UsedSlots != 0 || !view.Schedulable {
		t.Fatalf("the failed slot write changed the live record: %+v", view)
	}

	if err := os.Remove(path + ".tmp"); err != nil {
		t.Fatalf("restore the temp path: %v", err)
	}
	reopened := newFileService(t, path, []string{"build"}, probe)
	restored, err := reopened.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	if restored.ActiveSlots != 0 || restored.UsedSlots != 0 {
		t.Fatalf("the failed slot write reached the document: %+v", restored)
	}
}

func TestFileStoreRetireWriteFailureKeepsTheRegistration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "environments.json")
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newFileService(t, path, []string{"build"}, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := os.Mkdir(path+".tmp", 0o700); err != nil {
		t.Fatalf("make the temp path unwritable: %v", err)
	}
	if _, err := service.Retire(ctx, "build-a", "ops"); err == nil {
		t.Fatal("Retire succeeded although the document write failed")
	}
	if _, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); err != nil {
		t.Fatalf("the failed retirement removed the live registration: %v", err)
	}
	retired, err := service.Retired(ctx, environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Retired: %v", err)
	}
	if len(retired) != 0 {
		t.Fatalf("the failed retirement published a tombstone: %+v", retired)
	}

	if err := os.Remove(path + ".tmp"); err != nil {
		t.Fatalf("restore the temp path: %v", err)
	}
	reopened := newFileService(t, path, []string{"build"}, probe)
	if _, err := reopened.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	retired, err = reopened.Retired(ctx, environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Retired after restart: %v", err)
	}
	if len(retired) != 0 {
		t.Fatalf("the failed retirement reached the document: %+v", retired)
	}
}

func TestFileStoreRenameFailureKeepsThePreviousDocument(t *testing.T) {
	path := filepath.Join(t.TempDir(), "environments.json")
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newFileService(t, path, []string{"build"}, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the registry: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove the registry: %v", err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("replace the registry with a directory: %v", err)
	}

	if _, err := service.AcquireSlot(ctx, "build-a", "slot-1"); err == nil {
		t.Fatal("AcquireSlot succeeded although the document replace failed")
	}
	view, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if view.ActiveSlots != 0 || view.UsedSlots != 0 {
		t.Fatalf("the failed replace changed the live record: %+v", view)
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove the blocking directory: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("restore the registry: %v", err)
	}
	reopened := newFileService(t, path, []string{"build"}, probe)
	restored, err := reopened.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	if restored.ActiveSlots != 0 || restored.UsedSlots != 0 {
		t.Fatalf("the failed replace reached the document: %+v", restored)
	}
}

func TestFileStoreSupersedeWriteFailurePublishesNeitherRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "environments.json")
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-" + target.StateRoot), nil
	}
	service := newFileService(t, path, []string{"build"}, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-a: %v", err)
	}
	if err := os.Mkdir(path+".tmp", 0o700); err != nil {
		t.Fatalf("make the temp path unwritable: %v", err)
	}

	if _, err := service.Register(ctx, registration("build-b", "build-a.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err == nil {
		t.Fatal("Register succeeded although the document write failed")
	}
	old, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-a: %v", err)
	}
	if !old.Schedulable {
		t.Fatalf("the failed replacement superseded build-a in memory: %+v", old)
	}
	if _, err := service.Get(ctx, "build-b", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("the failed replacement published build-b: %v", err)
	}
}
