package proxy

import (
	"sync"
	"time"
)

// CatalogSlot is one real runtime's catalog cache entry: the probe daemon's
// reported bytes verbatim plus the moment it landed. The landing time is local
// only — it drives the staleness warning and never enters the replayed body,
// which stays byte-identical (§1.3).
type CatalogSlot struct {
	Body     []byte
	LandedAt time.Time
}

// SlotState is the read-only projection /foreman/register-source exposes.
type SlotState struct {
	RuntimeID  string    `json:"runtime_id"`
	Provider   string    `json:"provider"`
	ProfileID  string    `json:"profile_id"`
	Cached     bool      `json:"cached"`
	LandedAt   time.Time `json:"landed_at"`
	AgeSeconds float64   `json:"age_seconds"`
}

// CatalogSlots holds one catalog slot per real runtime. It is written by the
// fake server (probe results) and read by the reporter (cache replay); the
// key is always the real runtime id (§1.3).
type CatalogSlots struct {
	mu        sync.RWMutex
	byRuntime map[string]CatalogSlot
}

// NewCatalogSlots builds an empty slot store.
func NewCatalogSlots() *CatalogSlots {
	return &CatalogSlots{byRuntime: map[string]CatalogSlot{}}
}

// Get returns the slot bound to runtimeID.
func (s *CatalogSlots) Get(runtimeID string) (CatalogSlot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	slot, ok := s.byRuntime[runtimeID]
	return slot, ok
}

// Put stores body as the runtime's catalog cache.
func (s *CatalogSlots) Put(runtimeID string, body []byte, at time.Time) {
	if runtimeID == "" {
		return
	}
	stored := make([]byte, len(body))
	copy(stored, body)
	s.mu.Lock()
	s.byRuntime[runtimeID] = CatalogSlot{Body: stored, LandedAt: at}
	s.mu.Unlock()
}

// Delete drops the slot bound to runtimeID: an entry that left the declaration
// no longer owns a catalog slot (§1.6 退出条目的目录槽随条目失效).
func (s *CatalogSlots) Delete(runtimeID string) {
	s.mu.Lock()
	delete(s.byRuntime, runtimeID)
	s.mu.Unlock()
}

// Snapshot projects the slots for the operator endpoint, in the refs order.
func (s *CatalogSlots) Snapshot(refs []RuntimeRef, now time.Time) []SlotState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	states := make([]SlotState, 0, len(refs))
	for _, ref := range refs {
		state := SlotState{RuntimeID: ref.ID, Provider: ref.Provider, ProfileID: ref.ProfileID}
		if slot, ok := s.byRuntime[ref.ID]; ok {
			state.Cached = true
			state.LandedAt = slot.LandedAt
			state.AgeSeconds = now.Sub(slot.LandedAt).Seconds()
		}
		states = append(states, state)
	}
	return states
}
