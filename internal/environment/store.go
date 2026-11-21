package environment

import (
	"context"
	"strconv"
	"sync"
)

// Store is the environment registry's persistence face. The service serializes
// read-modify-write sequences, so a store implementation never has to merge
// concurrent writers.
type Store interface {
	PutEnvironment(ctx context.Context, env Environment) error
	// PutEnvironments stores several registrations in one durable step. The
	// service uses it when a registration also supersedes the old tree at the
	// same endpoint, so a persistence failure cannot publish half the change.
	PutEnvironments(ctx context.Context, envs []Environment) error
	GetEnvironment(ctx context.Context, id ID) (Environment, bool, error)
	ListEnvironments(ctx context.Context) ([]Environment, error)
	// RetireEnvironment removes one registration and records its tombstone in
	// one step, so a crash cannot leave an untombstoned ID reusable.
	RetireEnvironment(ctx context.Context, id ID, tomb Tombstone) error
	GetTombstone(ctx context.Context, id ID) (Tombstone, bool, error)
	ListTombstones(ctx context.Context) ([]Tombstone, error)
	AppendAudit(ctx context.Context, entry AuditEntry) error
	ListAudit(ctx context.Context) ([]AuditEntry, error)
	// CarrierVersion answers the opaque version of the carrier snapshot this
	// view was read from. It advances on every committed write and cannot be
	// rewound by restoring a document inside the same object, so probe evidence
	// recorded against one version is stale as soon as anything commits.
	CarrierVersion(ctx context.Context) (string, error)
	// LedgerID answers the instance identity of the ledger this view was read
	// from. A carrier recreated from an empty template is a new instance even
	// when its generation starts over, so evidence cannot cross instances.
	LedgerID(ctx context.Context) (string, error)
	// Generation answers the generation of the ledger this view was read from.
	// Every committed write advances it, so evidence recorded against one
	// generation is stale as soon as any other change commits.
	Generation(ctx context.Context) (uint64, error)
}

// MemoryStore is the in-process store.
type MemoryStore struct {
	txMu         sync.Mutex
	mu           sync.RWMutex
	ledgerID     string
	generation   uint64
	environments map[ID]Environment
	tombstones   map[ID]Tombstone
	audit        []AuditEntry
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		ledgerID:     newLedgerID(),
		environments: map[ID]Environment{},
		tombstones:   map[ID]Tombstone{},
	}
}

// Transact runs one read-decide-write sequence under the in-process
// transaction lock and publishes the buffered writes and audits together.
func (m *MemoryStore) Transact(ctx context.Context, fn func(context.Context, Store) error) error {
	m.txMu.Lock()
	defer m.txMu.Unlock()
	environments, tombstones, ledgerID, generation := m.snapshotLedger()
	tx := newLedgerTx(m, environments, tombstones, memoryCarrierVersion(ledgerID, generation), ledgerID, generation)
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if tx.dirty {
		m.replaceMaps(tx.environments, tx.tombstones, ledgerID, generation+1)
	}
	tx.commitAudits(ctx)
	return nil
}

// memoryCarrierVersion renders the version of an in-process ledger snapshot.
// The generation advances on every committed write, so it is the fence the
// in-process carriers use in place of an object resource version.
func memoryCarrierVersion(ledgerID string, generation uint64) string {
	return ledgerID + "/" + strconv.FormatUint(generation, 10)
}

// CarrierVersion answers the version of the committed in-process ledger.
func (m *MemoryStore) CarrierVersion(_ context.Context) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return memoryCarrierVersion(m.ledgerID, m.generation), nil
}

// LedgerID answers the instance identity of this store's ledger.
func (m *MemoryStore) LedgerID(_ context.Context) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ledgerID, nil
}

// Generation answers the committed ledger generation.
func (m *MemoryStore) Generation(_ context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.generation, nil
}

// PutEnvironment upserts one registration.
func (m *MemoryStore) PutEnvironment(ctx context.Context, env Environment) error {
	return m.Transact(ctx, func(ctx context.Context, tx Store) error {
		return tx.PutEnvironment(ctx, env)
	})
}

// PutEnvironments upserts several registrations under one lock.
func (m *MemoryStore) PutEnvironments(ctx context.Context, envs []Environment) error {
	return m.Transact(ctx, func(ctx context.Context, tx Store) error {
		return tx.PutEnvironments(ctx, envs)
	})
}

// GetEnvironment reads one registration.
func (m *MemoryStore) GetEnvironment(_ context.Context, id ID) (Environment, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	env, ok := m.environments[id]
	if !ok {
		return Environment{}, false, nil
	}
	return env.clone(), true, nil
}

// ListEnvironments lists every registration.
func (m *MemoryStore) ListEnvironments(_ context.Context) ([]Environment, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Environment, 0, len(m.environments))
	for _, env := range m.environments {
		out = append(out, env.clone())
	}
	return out, nil
}

// RetireEnvironment removes one registration and records its tombstone.
func (m *MemoryStore) RetireEnvironment(ctx context.Context, id ID, tomb Tombstone) error {
	return m.Transact(ctx, func(ctx context.Context, tx Store) error {
		return tx.RetireEnvironment(ctx, id, tomb)
	})
}

// GetTombstone reads one retirement record.
func (m *MemoryStore) GetTombstone(_ context.Context, id ID) (Tombstone, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tomb, ok := m.tombstones[id]
	if !ok {
		return Tombstone{}, false, nil
	}
	return tomb.clone(), true, nil
}

// ListTombstones lists every retirement record.
func (m *MemoryStore) ListTombstones(_ context.Context) ([]Tombstone, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Tombstone, 0, len(m.tombstones))
	for _, tomb := range m.tombstones {
		out = append(out, tomb.clone())
	}
	return out, nil
}

// AppendAudit appends one audit row, dropping the oldest beyond the retention
// bound.
func (m *MemoryStore) AppendAudit(_ context.Context, entry AuditEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.audit = append(m.audit, entry)
	if len(m.audit) > AuditRetention {
		m.audit = append([]AuditEntry(nil), m.audit[len(m.audit)-AuditRetention:]...)
	}
	return nil
}

// ListAudit returns the audit rows, newest last.
func (m *MemoryStore) ListAudit(_ context.Context) ([]AuditEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]AuditEntry(nil), m.audit...), nil
}

// snapshotLedger returns deep copies of both maps and the committed
// generation. A durable store builds the next document from this snapshot and
// only publishes it after the write succeeded, so a failed write leaves the
// live registry untouched.
func (m *MemoryStore) snapshotLedger() (map[ID]Environment, map[ID]Tombstone, string, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	environments := make(map[ID]Environment, len(m.environments))
	for id, env := range m.environments {
		environments[id] = env.clone()
	}
	tombstones := make(map[ID]Tombstone, len(m.tombstones))
	for id, tomb := range m.tombstones {
		tombstones[id] = tomb.clone()
	}
	return environments, tombstones, m.ledgerID, m.generation
}

// replaceMaps publishes a document that was already persisted.
func (m *MemoryStore) replaceMaps(environments map[ID]Environment, tombstones map[ID]Tombstone, ledgerID string, generation uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.environments = environments
	m.tombstones = tombstones
	m.ledgerID = ledgerID
	m.generation = generation
}

// restoreDocument seeds a store from a decoded document. The caller passes the
// identity it resolved for the carrier; an empty value keeps the store's own,
// so the first write materializes it.
func (m *MemoryStore) restoreDocument(document environmentDocument, ledgerID string) {
	environments, tombstones := environmentDocumentMaps(document)
	if ledgerID == "" {
		ledgerID, _ = m.LedgerID(context.Background())
	}
	m.replaceMaps(environments, tombstones, ledgerID, document.Generation)
}
