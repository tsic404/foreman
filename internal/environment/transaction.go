package environment

import (
	"context"
	"sync"
)

// TransactionalStore runs one read-decide-write sequence against a single
// committed snapshot. The Store passed to fn is bound to that snapshot: reads
// see it, writes are buffered, and the batch commits only if the carrier has
// not moved. A conflict re-runs fn against the new snapshot, so a decision is
// never applied to a ledger other than the one it was made on.
type TransactionalStore interface {
	Store
	Transact(ctx context.Context, fn func(context.Context, Store) error) error
}

// ledgerTx is the buffered view one transaction decides on. Audit rows are
// held here until the caller's commit succeeds, so a conflicting or failed
// attempt leaves no success record behind.
type ledgerTx struct {
	base         Store
	carrier      string
	ledgerID     string
	generation   uint64
	environments map[ID]Environment
	tombstones   map[ID]Tombstone
	audits       []AuditEntry
	dirty        bool
}

func newLedgerTx(base Store, environments map[ID]Environment, tombstones map[ID]Tombstone, carrier, ledgerID string, generation uint64) *ledgerTx {
	return &ledgerTx{
		base:         base,
		carrier:      carrier,
		ledgerID:     ledgerID,
		generation:   generation,
		environments: environments,
		tombstones:   tombstones,
	}
}

// CarrierVersion answers the carrier version this transaction decides on.
func (t *ledgerTx) CarrierVersion(context.Context) (string, error) {
	return t.carrier, nil
}

// LedgerID answers the ledger instance this transaction decides on.
func (t *ledgerTx) LedgerID(context.Context) (string, error) {
	return t.ledgerID, nil
}

// Generation answers the committed generation this transaction decides on.
func (t *ledgerTx) Generation(context.Context) (uint64, error) {
	return t.generation, nil
}

// environmentDocumentMaps turns one decoded document into the mutable maps a
// transaction and the in-memory cache both use.
func environmentDocumentMaps(document environmentDocument) (map[ID]Environment, map[ID]Tombstone) {
	environments := make(map[ID]Environment, len(document.Environments))
	for _, env := range document.Environments {
		environments[env.ID] = env.clone()
	}
	tombstones := make(map[ID]Tombstone, len(document.Tombstones))
	for _, tomb := range document.Tombstones {
		tombstones[tomb.ID] = tomb.clone()
	}
	return environments, tombstones
}

func (t *ledgerTx) GetEnvironment(_ context.Context, id ID) (Environment, bool, error) {
	env, ok := t.environments[id]
	if !ok {
		return Environment{}, false, nil
	}
	return env.clone(), true, nil
}

func (t *ledgerTx) ListEnvironments(_ context.Context) ([]Environment, error) {
	out := make([]Environment, 0, len(t.environments))
	for _, env := range t.environments {
		out = append(out, env.clone())
	}
	return out, nil
}

func (t *ledgerTx) PutEnvironment(_ context.Context, env Environment) error {
	t.environments[env.ID] = env.clone()
	t.dirty = true
	return nil
}

func (t *ledgerTx) PutEnvironments(_ context.Context, envs []Environment) error {
	for _, env := range envs {
		t.environments[env.ID] = env.clone()
	}
	t.dirty = true
	return nil
}

func (t *ledgerTx) GetTombstone(_ context.Context, id ID) (Tombstone, bool, error) {
	tomb, ok := t.tombstones[id]
	if !ok {
		return Tombstone{}, false, nil
	}
	return tomb.clone(), true, nil
}

func (t *ledgerTx) ListTombstones(_ context.Context) ([]Tombstone, error) {
	out := make([]Tombstone, 0, len(t.tombstones))
	for _, tomb := range t.tombstones {
		out = append(out, tomb.clone())
	}
	return out, nil
}

func (t *ledgerTx) RetireEnvironment(_ context.Context, id ID, tomb Tombstone) error {
	delete(t.environments, id)
	t.tombstones[id] = tomb.clone()
	t.dirty = true
	return nil
}

func (t *ledgerTx) AppendAudit(_ context.Context, entry AuditEntry) error {
	t.audits = append(t.audits, entry)
	return nil
}

func (t *ledgerTx) ListAudit(ctx context.Context) ([]AuditEntry, error) {
	return t.base.ListAudit(ctx)
}

// commitAudits publishes the buffered rows only after the caller committed the
// ledger change.
func (t *ledgerTx) commitAudits(ctx context.Context) {
	for _, entry := range t.audits {
		_ = t.base.AppendAudit(ctx, entry)
	}
}

// serialStore gives a custom Store the single-process transaction boundary the
// service relies on when the implementation has no native one.
type serialStore struct {
	mu sync.Mutex
	Store
}

func (s *serialStore) Transact(ctx context.Context, fn func(context.Context, Store) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(ctx, s.Store)
}
