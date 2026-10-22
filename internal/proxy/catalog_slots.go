package proxy

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// catalogSlotTTL is the shared slot row's retention: the model-catalog cache
// window (the slot is a cache, so expiry only degrades to a miss, §1.3).
const catalogSlotTTL = 6 * time.Hour

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

// SlotStore is the shared catalog-slot face (§1.3 跨实例口径: 槽的读与写可由
// 不同实例完成). Satisfied by internal/state's CoordStore.
type SlotStore interface {
	PutCatalogSlot(ctx context.Context, runtimeID string, raw []byte, ttl time.Duration) error
	GetCatalogSlot(ctx context.Context, runtimeID string) ([]byte, bool, error)
	DeleteCatalogSlot(ctx context.Context, runtimeID string) error
}

// slotRow is the shared row's body: the slot bytes and the landing instant,
// base64-encoded by encoding/json.
type slotRow struct {
	Body     []byte    `json:"body"`
	LandedAt time.Time `json:"landed_at"`
}

// CatalogSlots holds one catalog slot per real runtime. It is written by the
// fake server (probe results) and read by the reporter (cache replay); the key
// is always the real runtime id (§1.3). With a shared store attached the slot
// is cross-instance: a result landed on one replica replays on any other.
type CatalogSlots struct {
	mu        sync.RWMutex
	byRuntime map[string]CatalogSlot
	shared    SlotStore
}

// NewCatalogSlots builds an empty slot store.
func NewCatalogSlots() *CatalogSlots {
	return &CatalogSlots{byRuntime: map[string]CatalogSlot{}}
}

// AttachShared wires the cross-instance slot face.
func (s *CatalogSlots) AttachShared(store SlotStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shared = store
}

func slotCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

// Get returns the slot bound to runtimeID, reading the shared row on a local
// miss (and caching it locally).
func (s *CatalogSlots) Get(runtimeID string) (CatalogSlot, bool) {
	s.mu.RLock()
	slot, ok := s.byRuntime[runtimeID]
	shared := s.shared
	s.mu.RUnlock()
	if ok || shared == nil || runtimeID == "" {
		return slot, ok
	}
	ctx, cancel := slotCtx()
	raw, found, err := shared.GetCatalogSlot(ctx, runtimeID)
	cancel()
	if err != nil || !found {
		return CatalogSlot{}, false
	}
	var row slotRow
	if json.Unmarshal(raw, &row) != nil {
		return CatalogSlot{}, false
	}
	slot = CatalogSlot{Body: row.Body, LandedAt: row.LandedAt}
	s.mu.Lock()
	s.byRuntime[runtimeID] = slot
	s.mu.Unlock()
	return slot, true
}

// Put stores body as the runtime's catalog cache (locally and, when attached,
// in the shared face; a shared write failure only costs a cross-instance cache
// miss, never a correctness violation).
func (s *CatalogSlots) Put(runtimeID string, body []byte, at time.Time) {
	if runtimeID == "" {
		return
	}
	stored := append([]byte(nil), body...)
	s.mu.Lock()
	s.byRuntime[runtimeID] = CatalogSlot{Body: stored, LandedAt: at}
	shared := s.shared
	s.mu.Unlock()
	if shared == nil {
		return
	}
	raw, err := json.Marshal(slotRow{Body: stored, LandedAt: at})
	if err != nil {
		return
	}
	ctx, cancel := slotCtx()
	defer cancel()
	_ = shared.PutCatalogSlot(ctx, runtimeID, raw, catalogSlotTTL)
}

// Delete drops the slot bound to runtimeID: an entry that left the declaration
// no longer owns a catalog slot (§1.6 退出条目的目录槽随条目失效).
func (s *CatalogSlots) Delete(runtimeID string) {
	s.mu.Lock()
	delete(s.byRuntime, runtimeID)
	shared := s.shared
	s.mu.Unlock()
	if shared == nil || runtimeID == "" {
		return
	}
	ctx, cancel := slotCtx()
	defer cancel()
	_ = shared.DeleteCatalogSlot(ctx, runtimeID)
}

// Snapshot projects the slots for the operator endpoint, in the refs order.
func (s *CatalogSlots) Snapshot(refs []RuntimeRef, now time.Time) []SlotState {
	states := make([]SlotState, 0, len(refs))
	for _, ref := range refs {
		state := SlotState{RuntimeID: ref.ID, Provider: ref.Provider, ProfileID: ref.ProfileID}
		if slot, ok := s.Get(ref.ID); ok {
			state.Cached = true
			state.LandedAt = slot.LandedAt
			state.AgeSeconds = now.Sub(slot.LandedAt).Seconds()
		}
		states = append(states, state)
	}
	return states
}
