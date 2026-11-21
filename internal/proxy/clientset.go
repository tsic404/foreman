package proxy

import (
	"fmt"
	"sync"

	"github.com/tsic404/foreman/internal/scope"
)

// ClientSet is the deployment's fake-client set: one Client per workspace
// identity, because every F→S call carries that workspace's credential and
// daemon identity (§1.1). A single-workspace deployment is a set of one.
type ClientSet struct {
	clients []*Client
	byScope map[string]*Client
	byID    map[string][]*Client

	bundlesMu sync.Mutex
	bundles   map[string]*SkillCache
}

// NewClientSet groups clients by the workspace each one serves.
func NewClientSet(clients ...*Client) (*ClientSet, error) {
	set := &ClientSet{
		clients: make([]*Client, 0, len(clients)),
		byScope: make(map[string]*Client, len(clients)),
		byID:    make(map[string][]*Client, len(clients)),
		bundles: make(map[string]*SkillCache),
	}
	for _, client := range clients {
		if client == nil {
			continue
		}
		workspaceID := client.WorkspaceID()
		if workspaceID == "" {
			return nil, fmt.Errorf("proxy client has no workspace id")
		}
		key := client.ScopeKey()
		if _, dup := set.byScope[key]; dup {
			return nil, fmt.Errorf("workspace %s has two proxy clients for source %q", workspaceID, client.SourceID())
		}
		set.clients = append(set.clients, client)
		set.byScope[key] = client
		set.byID[workspaceID] = append(set.byID[workspaceID], client)
	}
	if len(set.clients) == 0 {
		return nil, fmt.Errorf("proxy client set is empty")
	}
	return set, nil
}

// For returns the client bound to workspaceID when exactly one source serves
// it. The legacy single-source deployment always has one.
func (s *ClientSet) For(workspaceID string) (*Client, bool) {
	if s == nil {
		return nil, false
	}
	clients := s.byID[workspaceID]
	if len(clients) != 1 {
		return nil, false
	}
	return clients[0], true
}

// ForSource returns the client bound to one source and workspace pair.
func (s *ClientSet) ForSource(sourceID, workspaceID string) (*Client, bool) {
	if s == nil {
		return nil, false
	}
	client, ok := s.byScope[scope.Key(sourceID, workspaceID)]
	return client, ok
}

// ForScope returns the client bound to a partition key.
func (s *ClientSet) ForScope(key string) (*Client, bool) {
	if s == nil {
		return nil, false
	}
	client, ok := s.byScope[key]
	return client, ok
}

// All returns the clients in configuration order.
func (s *ClientSet) All() []*Client {
	if s == nil {
		return nil
	}
	return append([]*Client(nil), s.clients...)
}

// IDs returns the served workspace ids in configuration order.
func (s *ClientSet) IDs() []string {
	if s == nil {
		return nil
	}
	ids := make([]string, 0, len(s.clients))
	for _, client := range s.clients {
		ids = append(ids, client.WorkspaceID())
	}
	return ids
}

// Has reports whether workspaceID is served.
func (s *ClientSet) Has(workspaceID string) bool {
	if s == nil {
		return false
	}
	return len(s.byID[workspaceID]) > 0
}

// HasScope reports whether the source and workspace pair is served.
func (s *ClientSet) HasScope(sourceID, workspaceID string) bool {
	_, ok := s.ForSource(sourceID, workspaceID)
	return ok
}

// RuntimeIDs returns every workspace's declared real runtime ids.
func (s *ClientSet) RuntimeIDs() []string {
	if s == nil {
		return nil
	}
	var ids []string
	for _, client := range s.clients {
		ids = append(ids, client.RuntimeIDs()...)
	}
	return ids
}

// RuntimeIdentity resolves one real runtime id to its declared identity across
// the workspace clients. Each client owns the ids the server gave its own
// registration, so the lookup is unambiguous.
func (s *ClientSet) RuntimeIdentity(runtimeID string) (string, bool) {
	if s == nil || runtimeID == "" {
		return "", false
	}
	for _, client := range s.clients {
		if identity, ok := client.RuntimeIdentity(runtimeID); ok {
			return identity, true
		}
	}
	return "", false
}

// SkillBundles returns the resolve cache of one workspace when exactly one
// source serves it. The cache is
// process-local and partitioned by workspace (NG11, §1.8): the same payload
// in two workspaces never shares an entry.
func (s *ClientSet) SkillBundles(workspaceID string, metrics Metrics) *SkillCache {
	client, ok := s.For(workspaceID)
	if !ok {
		return nil
	}
	return s.bundlesFor(client, metrics)
}

// SkillBundlesFor returns the resolve cache of one source and workspace pair.
// The cache is partitioned by that pair: the same payload in two sources never
// shares an entry, even when both expose the same workspace id (02-contracts
// V2-C03 技能缓存按原授权域).
func (s *ClientSet) SkillBundlesFor(sourceID, workspaceID string, metrics Metrics) *SkillCache {
	client, ok := s.ForSource(sourceID, workspaceID)
	if !ok {
		return nil
	}
	return s.bundlesFor(client, metrics)
}

func (s *ClientSet) bundlesFor(client *Client, metrics Metrics) *SkillCache {
	s.bundlesMu.Lock()
	defer s.bundlesMu.Unlock()
	key := client.ScopeKey()
	if cache, ok := s.bundles[key]; ok {
		return cache
	}
	cache := NewSkillCache(client.cfg, client, metrics)
	s.bundles[key] = cache
	return cache
}

// Scopes returns the served partition keys in configuration order.
func (s *ClientSet) Scopes() []string {
	if s == nil {
		return nil
	}
	keys := make([]string, 0, len(s.clients))
	for _, client := range s.clients {
		keys = append(keys, client.ScopeKey())
	}
	return keys
}
