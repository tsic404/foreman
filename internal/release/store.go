package release

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

// AuditEntry is one administrative action on the release face. It carries
// versions, platforms and impact counts only: never an artifact URL with
// userinfo, a credential or an Agent configuration value.
type AuditEntry struct {
	At         time.Time `json:"at"`
	Actor      string    `json:"actor"`
	Action     string    `json:"action"`
	CLIID      string    `json:"cli_id"`
	ServerID   string    `json:"server_id,omitempty"`
	Version    string    `json:"version,omitempty"`
	OldVersion string    `json:"old_version,omitempty"`
	Result     string    `json:"result"`
	Detail     string    `json:"detail,omitempty"`
	Known      bool      `json:"impact_known"`
	Agents     *int      `json:"impact_agents,omitempty"`
	Workspaces int       `json:"impact_workspaces"`
	InFlight   int       `json:"impact_in_flight"`
}

// Audit actions and results.
const (
	AuditRegisterDefinition = "register_cli"
	AuditUpdateSettings     = "update_cli_settings"
	AuditRegisterVersion    = "register_cli_version"
	AuditCheck              = "check_cli"
	AuditPublish            = "publish_cli"

	AuditOK       = "ok"
	AuditFailed   = "failed"
	AuditRejected = "rejected"
)

// AuditRetention bounds the in-memory audit ring.
const AuditRetention = 1024

// Store keeps definitions and audit rows. Definitions are operator input and
// must survive a restart; audit rows are bounded and process-local until a
// shared store is wired.
type Store interface {
	ListDefinitions(ctx context.Context) ([]Definition, error)
	GetDefinition(ctx context.Context, id string) (Definition, bool, error)
	PutDefinition(ctx context.Context, definition Definition) error

	AppendAudit(ctx context.Context, entry AuditEntry) error
	ListAudit(ctx context.Context) ([]AuditEntry, error)
}

// MemoryStore is the in-process store used by tests and by a deployment that
// has no writable state root.
type MemoryStore struct {
	mu          sync.RWMutex
	definitions map[string]Definition
	audit       []AuditEntry
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{definitions: map[string]Definition{}}
}

// ListDefinitions returns every definition in id order.
func (m *MemoryStore) ListDefinitions(_ context.Context) ([]Definition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Definition, 0, len(m.definitions))
	for _, definition := range m.definitions {
		out = append(out, cloneDefinition(definition))
	}
	slices.SortFunc(out, func(a, b Definition) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		}
		return 0
	})
	return out, nil
}

// GetDefinition reads one definition.
func (m *MemoryStore) GetDefinition(_ context.Context, id string) (Definition, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	definition, ok := m.definitions[id]
	if !ok {
		return Definition{}, false, nil
	}
	return cloneDefinition(definition), true, nil
}

// PutDefinition upserts one definition.
func (m *MemoryStore) PutDefinition(_ context.Context, definition Definition) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.definitions[definition.ID] = cloneDefinition(definition)
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

// FileStore persists definitions in one JSON document and keeps audit rows in
// memory. A malformed document refuses startup instead of silently losing
// every release pointer.
type FileStore struct {
	*MemoryStore
	path    string
	writeMu sync.Mutex
}

// NewFileStore loads the definitions at path, or starts empty when the file
// does not exist yet.
func NewFileStore(path string) (*FileStore, error) {
	store := &FileStore{MemoryStore: NewMemoryStore(), path: path}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the CLI release registry: %w", err)
	}
	var document struct {
		Definitions []Definition `json:"definitions"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("parse the CLI release registry %s: %w", path, err)
	}
	for _, definition := range document.Definitions {
		if err := store.MemoryStore.PutDefinition(context.Background(), definition); err != nil {
			return nil, err
		}
	}
	return store, nil
}

// PutDefinition stores the definition and rewrites the document atomically.
func (f *FileStore) PutDefinition(ctx context.Context, definition Definition) error {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	definitions, err := f.MemoryStore.ListDefinitions(ctx)
	if err != nil {
		return err
	}
	replaced := false
	for i := range definitions {
		if definitions[i].ID == definition.ID {
			definitions[i] = cloneDefinition(definition)
			replaced = true
			break
		}
	}
	if !replaced {
		definitions = append(definitions, cloneDefinition(definition))
	}
	slices.SortFunc(definitions, func(a, b Definition) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		}
		return 0
	})
	raw, err := json.MarshalIndent(struct {
		Definitions []Definition `json:"definitions"`
	}{Definitions: definitions}, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if dir := filepath.Dir(f.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create the CLI release registry directory: %w", err)
		}
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write the CLI release registry: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("replace the CLI release registry: %w", err)
	}
	return f.MemoryStore.PutDefinition(ctx, definition)
}

var _ Store = (*FileStore)(nil)

// cloneDefinition deep-copies every slice and the verified map, so a reader or
// a caller's later edit cannot mutate stored state.
func cloneDefinition(in Definition) Definition {
	out := in
	out.RequiredPlatforms = append([]Platform(nil), in.RequiredPlatforms...)
	out.Candidates = make([]Candidate, 0, len(in.Candidates))
	for _, candidate := range in.Candidates {
		copied := candidate
		copied.Artifacts = append([]Artifact(nil), candidate.Artifacts...)
		out.Candidates = append(out.Candidates, copied)
	}
	out.Releases = make([]Release, 0, len(in.Releases))
	for _, release := range in.Releases {
		copied := release
		copied.Artifacts = append([]Artifact(nil), release.Artifacts...)
		out.Releases = append(out.Releases, copied)
	}
	if in.Current != nil {
		current := *in.Current
		current.Artifacts = append([]Artifact(nil), in.Current.Artifacts...)
		out.Current = &current
	}
	if in.Verified != nil {
		out.Verified = make(map[string]VerifiedVersion, len(in.Verified))
		for version, verified := range in.Verified {
			copied := verified
			copied.Artifacts = append([]Artifact(nil), verified.Artifacts...)
			copied.Checks = append([]Verification(nil), verified.Checks...)
			out.Verified[version] = copied
		}
	}
	return out
}
