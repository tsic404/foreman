package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// FileStore keeps the source registry in one JSON document and everything else
// in memory. Registrations are operator input: losing them on a restart would
// silently empty the management surface. Snapshots and audit rows are a read
// model one sync rebuilds, so they stay process-local until a shared store is
// wired (04-architecture.md §3.1: a source's directory is upstream truth).
type FileStore struct {
	*MemoryStore
	path string
	// writeMu serializes the read-modify-write of the document: the memory
	// store is already consistent under the service's lock, but two writers
	// must not interleave their temp-file renames.
	writeMu sync.Mutex
}

// NewFileStore loads the registry at path, or starts empty when the file does
// not exist yet. A malformed document is an error: silently starting empty
// would discard every registration the operator made.
func NewFileStore(path string) (*FileStore, error) {
	store := &FileStore{MemoryStore: NewMemoryStore(), path: path}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the catalog source registry: %w", err)
	}
	var sources []Source
	if err := json.Unmarshal(raw, &sources); err != nil {
		return nil, fmt.Errorf("parse the catalog source registry %s: %w", path, err)
	}
	for _, src := range sources {
		if err := store.MemoryStore.PutSource(context.Background(), src); err != nil {
			return nil, err
		}
	}
	return store, nil
}

// PutSource stores the record and rewrites the registry document atomically, so
// a crash leaves either the previous document or the new one.
func (f *FileStore) PutSource(ctx context.Context, src Source) error {
	if err := f.MemoryStore.PutSource(ctx, src); err != nil {
		return err
	}
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	sources, err := f.MemoryStore.ListSources(ctx)
	if err != nil {
		return err
	}
	slices.SortFunc(sources, func(a, b Source) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		}
		return 0
	})
	raw, err := json.MarshalIndent(sources, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if dir := filepath.Dir(f.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create the catalog registry directory: %w", err)
		}
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write the catalog source registry: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("replace the catalog source registry: %w", err)
	}
	return nil
}

var _ Store = (*FileStore)(nil)
