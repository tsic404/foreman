package environment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileStore keeps registrations and tombstones in one JSON document. It is
// the development and test carrier; a production Deployment uses SecretStore
// so the registry survives a Pod rebuild.
type FileStore struct {
	*MemoryStore
	path string
	// writeMu serializes the read-modify-write of the document: the memory
	// store is consistent under the service's lock, but two writers must not
	// interleave their temp-file renames.
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
		return nil, fmt.Errorf("read the environment registry: %w", err)
	}
	document, err := decodeEnvironmentDocument(raw, path)
	if err != nil {
		return nil, err
	}
	store.restoreDocument(document, document.LedgerID)
	return store, nil
}

// PutEnvironment stores one record after the document write succeeds.
func (f *FileStore) PutEnvironment(ctx context.Context, env Environment) error {
	return f.Transact(ctx, func(ctx context.Context, tx Store) error {
		return tx.PutEnvironment(ctx, env)
	})
}

// PutEnvironments stores several records in one atomic document replace.
func (f *FileStore) PutEnvironments(ctx context.Context, envs []Environment) error {
	return f.Transact(ctx, func(ctx context.Context, tx Store) error {
		return tx.PutEnvironments(ctx, envs)
	})
}

// RetireEnvironment removes the registration and records the tombstone in one
// atomic document write.
func (f *FileStore) RetireEnvironment(ctx context.Context, id ID, tomb Tombstone) error {
	return f.Transact(ctx, func(ctx context.Context, tx Store) error {
		return tx.RetireEnvironment(ctx, id, tomb)
	})
}

// Transact runs one read-decide-write sequence and replaces the document once,
// only after every decision in the callback succeeded.
func (f *FileStore) Transact(ctx context.Context, fn func(context.Context, Store) error) error {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	environments, tombstones, ledgerID, generation := f.MemoryStore.snapshotLedger()
	tx := newLedgerTx(f, environments, tombstones, memoryCarrierVersion(ledgerID, generation), ledgerID, generation)
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if tx.dirty {
		if err := f.persist(tx.environments, tx.tombstones, generation+1, ledgerID); err != nil {
			return err
		}
		f.MemoryStore.replaceMaps(tx.environments, tx.tombstones, ledgerID, generation+1)
	}
	tx.commitAudits(ctx)
	return nil
}

func (f *FileStore) persist(environments map[ID]Environment, tombstones map[ID]Tombstone, generation uint64, ledgerID string) error {
	raw, err := encodeEnvironmentDocument(environments, tombstones, generation, ledgerID, "")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(f.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create the environment registry directory: %w", err)
		}
	}
	tmp := f.path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("write the environment registry: %w", err)
	}
	_, writeErr := file.Write(raw)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("write the environment registry: %w", writeErr)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace the environment registry: %w", err)
	}
	return nil
}

var _ Store = (*FileStore)(nil)
