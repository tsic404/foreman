package proxy

import (
	"fmt"
	"sync"
)

// ClientSet is the deployment's fake-client set: one Client per workspace
// identity, because every F→S call carries that workspace's credential and
// daemon identity (§1.1). A single-workspace deployment is a set of one.
type ClientSet struct {
	clients []*Client
	byID    map[string]*Client

	bundlesMu sync.Mutex
	bundles   map[string]*SkillCache
}

// NewClientSet groups clients by their configured workspace.
func NewClientSet(clients ...*Client) (*ClientSet, error) {
	set := &ClientSet{
		clients: make([]*Client, 0, len(clients)),
		byID:    make(map[string]*Client, len(clients)),
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
		if _, dup := set.byID[workspaceID]; dup {
			return nil, fmt.Errorf("workspace %s has two proxy clients", workspaceID)
		}
		set.clients = append(set.clients, client)
		set.byID[workspaceID] = client
	}
	if len(set.clients) == 0 {
		return nil, fmt.Errorf("proxy client set is empty")
	}
	return set, nil
}

// For returns the client bound to workspaceID.
func (s *ClientSet) For(workspaceID string) (*Client, bool) {
	if s == nil {
		return nil, false
	}
	client, ok := s.byID[workspaceID]
	return client, ok
}

// All returns the clients in configuration order.
func (s *ClientSet) All() []*Client {
	if s == nil {
		return nil
	}
	return append([]*Client(nil), s.clients...)
}

// IDs returns the configured workspace ids in configuration order.
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

// Has reports whether workspaceID is configured.
func (s *ClientSet) Has(workspaceID string) bool {
	_, ok := s.For(workspaceID)
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

// SkillBundles returns the resolve cache of one workspace. The cache is
// process-local and partitioned by workspace (NG11, §1.8): the same payload
// in two workspaces never shares an entry.
func (s *ClientSet) SkillBundles(workspaceID string, metrics Metrics) *SkillCache {
	s.bundlesMu.Lock()
	defer s.bundlesMu.Unlock()
	if cache, ok := s.bundles[workspaceID]; ok {
		return cache
	}
	client, ok := s.For(workspaceID)
	if !ok {
		return nil
	}
	cache := NewSkillCache(client.cfg, client, metrics)
	s.bundles[workspaceID] = cache
	return cache
}
