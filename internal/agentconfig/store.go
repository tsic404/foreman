package agentconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

// Audit actions.
const (
	AuditWrite = "write_agent_config"
	AuditOK    = "ok"
)

// AuditRetention bounds the in-memory audit ring.
const AuditRetention = 1024

// Store keeps configuration revisions and audit rows. The content never
// reaches either: the revision carries only metadata.
type Store interface {
	ListRevisions(ctx context.Context) ([]Revision, error)
	GetRevision(ctx context.Context, scope Scope) (Revision, bool, error)
	PutRevision(ctx context.Context, revision Revision) error
	GetAttempt(ctx context.Context, attemptID string) (AttemptBinding, bool, error)
	PutAttempt(ctx context.Context, binding AttemptBinding) error

	AppendAudit(ctx context.Context, entry AuditEntry) error
	ListAudit(ctx context.Context) ([]AuditEntry, error)
}

// MemoryStore is the in-process store.
type MemoryStore struct {
	mu        sync.RWMutex
	revisions map[string]Revision
	attempts  map[string]AttemptBinding
	audit     []AuditEntry
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{revisions: map[string]Revision{}, attempts: map[string]AttemptBinding{}}
}

// ListRevisions returns every revision in server/workspace/backend order.
func (m *MemoryStore) ListRevisions(_ context.Context) ([]Revision, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Revision, 0, len(m.revisions))
	for _, revision := range m.revisions {
		out = append(out, revision)
	}
	slices.SortFunc(out, func(a, b Revision) int {
		return strings.Compare(a.Scope.Key(), b.Scope.Key())
	})
	return out, nil
}

// GetRevision reads one scope.
func (m *MemoryStore) GetRevision(_ context.Context, scope Scope) (Revision, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	revision, ok := m.revisions[scope.Key()]
	return revision, ok, nil
}

// PutRevision upserts one scope.
func (m *MemoryStore) PutRevision(_ context.Context, revision Revision) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revisions[revision.Scope.Key()] = revision
	return nil
}

// GetAttempt reads one frozen attempt binding.
func (m *MemoryStore) GetAttempt(_ context.Context, attemptID string) (AttemptBinding, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	binding, ok := m.attempts[attemptID]
	return binding, ok, nil
}

// PutAttempt freezes one attempt binding.
func (m *MemoryStore) PutAttempt(_ context.Context, binding AttemptBinding) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempts[binding.AttemptID] = binding
	return nil
}

// AppendAudit appends one row, dropping the oldest beyond the bound.
func (m *MemoryStore) AppendAudit(_ context.Context, entry AuditEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.audit = append(m.audit, entry)
	if len(m.audit) > AuditRetention {
		m.audit = append([]AuditEntry(nil), m.audit[len(m.audit)-AuditRetention:]...)
	}
	return nil
}

// ListAudit returns the rows newest last.
func (m *MemoryStore) ListAudit(_ context.Context) ([]AuditEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]AuditEntry(nil), m.audit...), nil
}

// FileStore persists revisions in one JSON document and keeps audit rows in
// memory.
type FileStore struct {
	*MemoryStore
	path    string
	writeMu sync.Mutex
}

// NewFileStore loads the revisions at path, or starts empty when the file does
// not exist yet.
func NewFileStore(path string) (*FileStore, error) {
	store := &FileStore{MemoryStore: NewMemoryStore(), path: path}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the Agent configuration registry: %w", err)
	}
	var document struct {
		Revisions []Revision       `json:"revisions"`
		Attempts  []AttemptBinding `json:"attempts,omitempty"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("parse the Agent configuration registry %s: %w", path, err)
	}
	for _, revision := range document.Revisions {
		if err := store.MemoryStore.PutRevision(context.Background(), revision); err != nil {
			return nil, err
		}
	}
	for _, binding := range document.Attempts {
		if err := store.MemoryStore.PutAttempt(context.Background(), binding); err != nil {
			return nil, err
		}
	}
	return store, nil
}

// PutRevision stores the revision and rewrites the document atomically.
func (f *FileStore) PutRevision(ctx context.Context, revision Revision) error {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	revisions, err := f.MemoryStore.ListRevisions(ctx)
	if err != nil {
		return err
	}
	replaced := false
	for i := range revisions {
		if revisions[i].Scope.Key() == revision.Scope.Key() {
			revisions[i] = revision
			replaced = true
			break
		}
	}
	if !replaced {
		revisions = append(revisions, revision)
	}
	slices.SortFunc(revisions, func(a, b Revision) int {
		return strings.Compare(a.Scope.Key(), b.Scope.Key())
	})
	attempts, err := f.MemoryStore.listAttempts(ctx)
	if err != nil {
		return err
	}
	raw, err := json.MarshalIndent(struct {
		Revisions []Revision       `json:"revisions"`
		Attempts  []AttemptBinding `json:"attempts,omitempty"`
	}{Revisions: revisions, Attempts: attempts}, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if dir := filepath.Dir(f.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create the Agent configuration registry directory: %w", err)
		}
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write the Agent configuration registry: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("replace the Agent configuration registry: %w", err)
	}
	return f.MemoryStore.PutRevision(ctx, revision)
}

// PutAttempt freezes one attempt binding and rewrites the document atomically.
func (f *FileStore) PutAttempt(ctx context.Context, binding AttemptBinding) error {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	revisions, err := f.MemoryStore.ListRevisions(ctx)
	if err != nil {
		return err
	}
	attempts, err := f.MemoryStore.listAttempts(ctx)
	if err != nil {
		return err
	}
	replaced := false
	for i := range attempts {
		if attempts[i].AttemptID == binding.AttemptID {
			attempts[i] = binding
			replaced = true
			break
		}
	}
	if !replaced {
		attempts = append(attempts, binding)
	}
	raw, err := json.MarshalIndent(struct {
		Revisions []Revision       `json:"revisions"`
		Attempts  []AttemptBinding `json:"attempts,omitempty"`
	}{Revisions: revisions, Attempts: attempts}, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if dir := filepath.Dir(f.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create the Agent configuration registry directory: %w", err)
		}
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write the Agent configuration registry: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("replace the Agent configuration registry: %w", err)
	}
	return f.MemoryStore.PutAttempt(ctx, binding)
}

func (m *MemoryStore) listAttempts(_ context.Context) ([]AttemptBinding, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]AttemptBinding, 0, len(m.attempts))
	for _, binding := range m.attempts {
		out = append(out, binding)
	}
	slices.SortFunc(out, func(a, b AttemptBinding) int {
		return strings.Compare(a.AttemptID, b.AttemptID)
	})
	return out, nil
}

var _ Store = (*FileStore)(nil)
