package release

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ArtifactCache keeps verified bytes addressable by release domain and digest.
// Only a complete verification writes here, so a half-downloaded or refused
// payload is never visible to a consumer.
type ArtifactCache interface {
	Put(ctx context.Context, domain, digest string, body []byte) error
	Get(ctx context.Context, domain, digest string) ([]byte, bool, error)
}

// MemoryArtifactCache is the in-process cache used by tests and by a
// deployment without a writable state root.
type MemoryArtifactCache struct {
	mu     sync.RWMutex
	bodies map[string][]byte
}

// NewMemoryArtifactCache returns an empty cache.
func NewMemoryArtifactCache() *MemoryArtifactCache {
	return &MemoryArtifactCache{bodies: map[string][]byte{}}
}

// Put stores one verified body.
func (m *MemoryArtifactCache) Put(_ context.Context, domain, digest string, body []byte) error {
	if err := validateCacheKey(domain, digest, body); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bodies[cacheKey(domain, digest)] = append([]byte(nil), body...)
	return nil
}

// Get returns one verified body.
func (m *MemoryArtifactCache) Get(_ context.Context, domain, digest string) ([]byte, bool, error) {
	if err := validateCacheKey(domain, digest, nil); err != nil {
		return nil, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	body, ok := m.bodies[cacheKey(domain, digest)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), body...), true, nil
}

// FileArtifactCache stores verified bodies under a private root. A body is
// written to a temporary file and renamed into place only after it is whole.
type FileArtifactCache struct {
	root string
	mu   sync.Mutex
}

// NewFileArtifactCache records the cache root. The directory is created on the
// first write: a read-only or absent default root must not fail startup, and a
// write that cannot create it leaves the release pointer unchanged.
func NewFileArtifactCache(root string) (*FileArtifactCache, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, errors.New("artifact cache root is required")
	}
	return &FileArtifactCache{root: root}, nil
}

// Put stores one verified body atomically.
func (f *FileArtifactCache) Put(_ context.Context, domain, digest string, body []byte) error {
	if err := validateCacheKey(domain, digest, body); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := os.MkdirAll(f.root, 0o700); err != nil {
		return fmt.Errorf("create artifact cache root: %w", err)
	}
	dir := filepath.Join(f.root, domainKey(domain))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create artifact cache domain: %w", err)
	}
	path := filepath.Join(dir, strings.ToLower(digest))
	if existing, err := os.ReadFile(path); err == nil {
		if digestOfBytes(existing) == strings.ToLower(digest) {
			return nil
		}
	}
	tmp, err := os.CreateTemp(dir, ".artifact-*")
	if err != nil {
		return fmt.Errorf("create artifact cache entry: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("protect artifact cache entry: %w", err)
	}
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write artifact cache entry: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync artifact cache entry: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close artifact cache entry: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("publish artifact cache entry: %w", err)
	}
	return nil
}

// Get returns one verified body. A damaged file is removed and reported as a
// miss so the caller can refetch the same digest from the registered source.
func (f *FileArtifactCache) Get(_ context.Context, domain, digest string) ([]byte, bool, error) {
	if err := validateCacheKey(domain, digest, nil); err != nil {
		return nil, false, err
	}
	path := filepath.Join(f.root, domainKey(domain), strings.ToLower(digest))
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read artifact cache entry: %w", err)
	}
	if digestOfBytes(body) != strings.ToLower(digest) {
		_ = os.Remove(path)
		return nil, false, nil
	}
	return body, true, nil
}

func validateCacheKey(domain, digest string, body []byte) error {
	if strings.TrimSpace(domain) == "" {
		return errors.New("artifact cache domain is required")
	}
	if !digestPattern.MatchString(strings.ToLower(digest)) {
		return errors.New("artifact cache digest must be 64 lowercase hex characters")
	}
	if body != nil && digestOfBytes(body) != strings.ToLower(digest) {
		return errors.New("artifact cache body does not match its digest")
	}
	return nil
}

func cacheKey(domain, digest string) string {
	return domain + "\x00" + strings.ToLower(digest)
}

func domainKey(domain string) string {
	sum := sha256.Sum256([]byte(domain))
	return hex.EncodeToString(sum[:])
}

func digestOfBytes(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
