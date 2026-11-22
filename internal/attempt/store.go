package attempt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Record is the attempt's durable session fact. It is written before the
// first dispatch, so a rebuilt Pod continues the original environment instead
// of selecting another endpoint (02-contracts.md V2-C03).
type Record struct {
	AttemptID     string `json:"attempt_id"`
	EnvironmentID string `json:"environment_id"`
	Generation    string `json:"generation,omitempty"`
	Endpoint      string `json:"endpoint"`
	SessionID     string `json:"session_id,omitempty"`
	Started       bool   `json:"started"`
	Delivered     bool   `json:"delivered"`
	Closed        bool   `json:"closed"`
	Isolated      bool   `json:"isolated"`
}

// Store keeps one record per attempt. Load's bool is false when the attempt
// has never started.
type Store interface {
	Load(ctx context.Context, attemptID string) (Record, bool, error)
	Save(ctx context.Context, record Record) error
}

// MemoryStore is the in-process store used by the tests and a single Job.
type MemoryStore struct {
	mu      sync.Mutex
	records map[string]Record
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: map[string]Record{}}
}

// Load returns one attempt's record.
func (s *MemoryStore) Load(_ context.Context, attemptID string) (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[attemptID]
	return record, ok, nil
}

// Save writes one attempt's record.
func (s *MemoryStore) Save(_ context.Context, record Record) error {
	if record.AttemptID == "" {
		return errors.New("attempt record needs an attempt id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = map[string]Record{}
	}
	s.records[record.AttemptID] = record
	return nil
}

// FileStore keeps records in one JSON document so a restarted Job-side
// process reads the session it already started.
type FileStore struct {
	mu   sync.Mutex
	path string
}

// NewFileStore returns a store backed by path.
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, errors.New("attempt store path is required")
	}
	return &FileStore{path: path}, nil
}

// Load reads one attempt's record.
func (s *FileStore) Load(_ context.Context, attemptID string) (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := s.read()
	if err != nil {
		return Record{}, false, err
	}
	record, ok := records[attemptID]
	return record, ok, nil
}

// Save updates one attempt's record.
func (s *FileStore) Save(_ context.Context, record Record) error {
	if record.AttemptID == "" {
		return errors.New("attempt record needs an attempt id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := s.read()
	if err != nil {
		return err
	}
	records[record.AttemptID] = record
	body, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("encode attempt store: %w", err)
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create attempt store directory: %w", err)
	}
	temp, err := os.CreateTemp(dir, ".attempt-*.tmp")
	if err != nil {
		return fmt.Errorf("create attempt store temporary file: %w", err)
	}
	name := temp.Name()
	defer func() { _ = os.Remove(name) }()
	if _, err := temp.Write(body); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write attempt store: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close attempt store: %w", err)
	}
	if err := os.Rename(name, s.path); err != nil {
		return fmt.Errorf("replace attempt store: %w", err)
	}
	return nil
}

func (s *FileStore) read() (map[string]Record, error) {
	body, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]Record{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read attempt store: %w", err)
	}
	records := map[string]Record{}
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, fmt.Errorf("decode attempt store: %w", err)
	}
	return records, nil
}
