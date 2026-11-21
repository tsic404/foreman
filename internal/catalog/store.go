package catalog

import (
	"context"
	"sync"
)

// Store is the catalog's persistence face. One deployment has one store; every
// source's record, snapshot and audit rows are partitioned by the stable server
// id. It is a dumb key/value seam: the service serializes read-modify-write
// sequences, so a store implementation never has to merge concurrent writers.
type Store interface {
	PutSource(ctx context.Context, src Source) error
	GetSource(ctx context.Context, id ServerID) (Source, bool, error)
	ListSources(ctx context.Context) ([]Source, error)

	// PutSnapshot stores one complete generation and makes it current. The
	// previous generation stays readable until this call succeeds.
	PutSnapshot(ctx context.Context, snap Snapshot) error
	GetSnapshot(ctx context.Context, id ServerID) (Snapshot, bool, error)

	// PutAgentBinding projects a read-back-confirmed binding onto the current
	// snapshot. It never creates a generation: a read-back is not a sync.
	PutAgentBinding(ctx context.Context, id ServerID, workspaceID, agentID, runtimeID string, bound bool, management Management) (Snapshot, bool, error)

	AppendAudit(ctx context.Context, entry AuditEntry) error
	ListAudit(ctx context.Context) ([]AuditEntry, error)
}

// MemoryStore is the in-process store. The catalog is a read model over the
// upstream directory, so losing it on restart costs one sync, not an
// authoritative fact.
type MemoryStore struct {
	mu        sync.RWMutex
	sources   map[ServerID]Source
	snapshots map[ServerID]Snapshot
	audit     []AuditEntry
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		sources:   map[ServerID]Source{},
		snapshots: map[ServerID]Snapshot{},
	}
}

// PutSource upserts one source record.
func (m *MemoryStore) PutSource(_ context.Context, src Source) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sources[src.ID] = src
	return nil
}

// GetSource reads one source record.
func (m *MemoryStore) GetSource(_ context.Context, id ServerID) (Source, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	src, ok := m.sources[id]
	return src, ok, nil
}

// ListSources lists every source record.
func (m *MemoryStore) ListSources(_ context.Context) ([]Source, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Source, 0, len(m.sources))
	for _, src := range m.sources {
		out = append(out, src)
	}
	return out, nil
}

// PutSnapshot publishes one complete generation.
func (m *MemoryStore) PutSnapshot(_ context.Context, snap Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshots[snap.ServerID] = snap.clone()
	return nil
}

// GetSnapshot reads the current generation of one source.
func (m *MemoryStore) GetSnapshot(_ context.Context, id ServerID) (Snapshot, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap, ok := m.snapshots[id]
	if !ok {
		return Snapshot{}, false, nil
	}
	return snap.clone(), true, nil
}

// PutAgentBinding projects one confirmed binding onto the current snapshot and
// returns the updated row. The snapshot's generation does not move: this is a
// read-back projection, not a directory sync.
func (m *MemoryStore) PutAgentBinding(_ context.Context, id ServerID, workspaceID, agentID, runtimeID string, bound bool, management Management) (Snapshot, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap, ok := m.snapshots[id]
	if !ok {
		return Snapshot{}, false, nil
	}
	found := false
	for i := range snap.Agents {
		agent := &snap.Agents[i]
		if agent.WorkspaceID != workspaceID || agent.ID != agentID {
			continue
		}
		agent.RuntimeID = runtimeID
		agent.RuntimeBound = bound
		agent.Management = management
		found = true
		break
	}
	if !found {
		return Snapshot{}, false, nil
	}
	m.snapshots[id] = snap
	return snap.clone(), true, nil
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

// ListAudit returns the audit rows, newest last (the ring's append order).
func (m *MemoryStore) ListAudit(_ context.Context) ([]AuditEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]AuditEntry(nil), m.audit...), nil
}
