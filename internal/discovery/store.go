package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// MaxRequests bounds the persisted request history.
const MaxRequests = 512

// Store keeps requests and each environment's latest result.
type Store interface {
	ListRequests(ctx context.Context) ([]Request, error)
	GetRequest(ctx context.Context, id string) (Request, bool, error)
	PutRequest(ctx context.Context, request Request) error

	GetLatest(ctx context.Context, envKey string) (Latest, bool, error)
	PutLatest(ctx context.Context, envKey string, latest Latest) error
}

// MemoryStore is the in-process store.
type MemoryStore struct {
	mu       sync.RWMutex
	requests map[string]Request
	order    []string
	latest   map[string]Latest
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{requests: map[string]Request{}, latest: map[string]Latest{}}
}

// ListRequests returns the requests newest last.
func (m *MemoryStore) ListRequests(_ context.Context) ([]Request, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Request, 0, len(m.order))
	for _, id := range m.order {
		if request, ok := m.requests[id]; ok {
			out = append(out, cloneRequest(request))
		}
	}
	return out, nil
}

// GetRequest reads one request.
func (m *MemoryStore) GetRequest(_ context.Context, id string) (Request, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	request, ok := m.requests[id]
	return cloneRequest(request), ok, nil
}

// PutRequest upserts one request and keeps the history bounded.
func (m *MemoryStore) PutRequest(_ context.Context, request Request) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.requests[request.ID]; !exists {
		m.order = append(m.order, request.ID)
	}
	m.requests[request.ID] = cloneRequest(request)
	if len(m.order) > MaxRequests {
		for _, id := range m.order[:len(m.order)-MaxRequests] {
			delete(m.requests, id)
		}
		m.order = append([]string(nil), m.order[len(m.order)-MaxRequests:]...)
	}
	return nil
}

// GetLatest reads one environment's current result.
func (m *MemoryStore) GetLatest(_ context.Context, envKey string) (Latest, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	latest, ok := m.latest[envKey]
	return cloneLatest(latest), ok, nil
}

// PutLatest stores one environment's current result.
func (m *MemoryStore) PutLatest(_ context.Context, envKey string, latest Latest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latest[envKey] = cloneLatest(latest)
	return nil
}

// FileStore persists requests and latest results in one JSON document.
type FileStore struct {
	*MemoryStore
	path    string
	writeMu sync.Mutex
}

// NewFileStore loads the document at path, or starts empty when it does not
// exist.
func NewFileStore(path string) (*FileStore, error) {
	store := &FileStore{MemoryStore: NewMemoryStore(), path: path}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the discovery registry: %w", err)
	}
	var document struct {
		Requests []Request         `json:"requests"`
		Latest   map[string]Latest `json:"latest"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("parse the discovery registry %s: %w", path, err)
	}
	for _, request := range document.Requests {
		if err := store.MemoryStore.PutRequest(context.Background(), request); err != nil {
			return nil, err
		}
	}
	for key, latest := range document.Latest {
		if err := store.MemoryStore.PutLatest(context.Background(), key, latest); err != nil {
			return nil, err
		}
	}
	return store, nil
}

// PutRequest stores the request and rewrites the document atomically.
func (f *FileStore) PutRequest(ctx context.Context, request Request) error {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	requests, latest, err := f.snapshot(ctx)
	if err != nil {
		return err
	}
	replaced := false
	for i := range requests {
		if requests[i].ID == request.ID {
			requests[i] = cloneRequest(request)
			replaced = true
			break
		}
	}
	if !replaced {
		requests = append(requests, cloneRequest(request))
	}
	if len(requests) > MaxRequests {
		requests = append([]Request(nil), requests[len(requests)-MaxRequests:]...)
	}
	if err := f.persist(ctx, requests, latest); err != nil {
		return err
	}
	return f.MemoryStore.PutRequest(ctx, request)
}

// PutLatest stores the latest result and rewrites the document atomically.
func (f *FileStore) PutLatest(ctx context.Context, envKey string, latest Latest) error {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	requests, values, err := f.snapshot(ctx)
	if err != nil {
		return err
	}
	values[envKey] = cloneLatest(latest)
	if err := f.persist(ctx, requests, values); err != nil {
		return err
	}
	return f.MemoryStore.PutLatest(ctx, envKey, latest)
}

func (f *FileStore) snapshot(ctx context.Context) ([]Request, map[string]Latest, error) {
	requests, err := f.MemoryStore.ListRequests(ctx)
	if err != nil {
		return nil, nil, err
	}
	var latest map[string]Latest
	f.MemoryStore.mu.RLock()
	latest = make(map[string]Latest, len(f.MemoryStore.latest))
	for key, value := range f.MemoryStore.latest {
		latest[key] = cloneLatest(value)
	}
	f.MemoryStore.mu.RUnlock()
	return requests, latest, nil
}

func (f *FileStore) persist(_ context.Context, requests []Request, latest map[string]Latest) error {
	raw, err := json.MarshalIndent(struct {
		Requests []Request         `json:"requests"`
		Latest   map[string]Latest `json:"latest"`
	}{Requests: requests, Latest: latest}, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if dir := filepath.Dir(f.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create the discovery registry directory: %w", err)
		}
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write the discovery registry: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("replace the discovery registry: %w", err)
	}
	return nil
}

var _ Store = (*FileStore)(nil)

func cloneRequest(request Request) Request {
	out := request
	if request.Result != nil {
		result := *request.Result
		result.Models = append([]Model(nil), request.Result.Models...)
		result.Capabilities = append([]Capability(nil), request.Result.Capabilities...)
		out.Result = &result
	}
	if request.CompletedAt != nil {
		completed := *request.CompletedAt
		out.CompletedAt = &completed
	}
	if request.Deadline != nil {
		deadline := *request.Deadline
		out.Deadline = &deadline
	}
	return out
}

func cloneLatest(latest Latest) Latest {
	out := latest
	out.Result.Models = append([]Model(nil), latest.Result.Models...)
	out.Result.Capabilities = append([]Capability(nil), latest.Result.Capabilities...)
	return out
}
