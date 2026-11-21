package catalog_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/catalog/catalogtest"
)

// TestFileStoreKeepsRegistrationsAcrossARestart is the durability half of the
// registration: a restart re-reads the same sources, while a snapshot is not
// persisted (one sync rebuilds it).
func TestFileStoreKeepsRegistrationsAcrossARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog-sources.json")
	store, err := catalog.NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	service, err := catalog.New(catalog.Config{
		Store:       store,
		Upstream:    seededUpstream(),
		Credentials: catalogtest.Credentials{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	register(t, service, "atlas", true)

	// A second store over the same file is what a restarted process sees.
	reopened, err := catalog.NewFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	sources, err := reopened.ListSources(context.Background())
	if err != nil {
		t.Fatalf("ListSources: %v", err)
	}
	if len(sources) != 1 || sources[0].ID != "atlas" {
		t.Fatalf("reopened sources = %+v, want the atlas registration", sources)
	}
	if sources[0].URL != "https://atlas.example.test" || sources[0].CredentialRef != "atlas-credential" {
		t.Fatalf("reopened registration lost its fields: %+v", sources[0])
	}
	snapshot, ok, err := reopened.GetSnapshot(context.Background(), "atlas")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if ok {
		t.Fatalf("a snapshot was persisted: %+v", snapshot)
	}
}

// TestFileStoreRefusesAMalformedRegistry keeps a corrupt document a startup
// error instead of an empty registry.
func TestFileStoreRefusesAMalformedRegistry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog-sources.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := catalog.NewFileStore(path); err == nil {
		t.Fatal("NewFileStore accepted a malformed registry")
	}
}
