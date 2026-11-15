package proxy

import "fmt"

// ClientSet is the deployment's fake-client set: one Client per workspace,
// because every F→S call carries that workspace's credential and daemon
// identity. A single-workspace deployment is a set of one.
type ClientSet struct {
	clients []*Client
	byID    map[string]*Client
}

// NewClientSet groups clients by the workspace each one serves.
func NewClientSet(clients ...*Client) (*ClientSet, error) {
	set := &ClientSet{
		clients: make([]*Client, 0, len(clients)),
		byID:    make(map[string]*Client, len(clients)),
	}
	for _, client := range clients {
		if client == nil {
			continue
		}
		ws := client.WorkspaceID()
		if ws == "" {
			return nil, fmt.Errorf("proxy client has no workspace id")
		}
		if _, dup := set.byID[ws]; dup {
			return nil, fmt.Errorf("workspace %s has two proxy clients", ws)
		}
		set.clients = append(set.clients, client)
		set.byID[ws] = client
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
	c, ok := s.byID[workspaceID]
	return c, ok
}

// All returns the clients in configuration order.
func (s *ClientSet) All() []*Client {
	return append([]*Client(nil), s.clients...)
}

// IDs returns the served workspace ids in configuration order.
func (s *ClientSet) IDs() []string {
	ids := make([]string, 0, len(s.clients))
	for _, c := range s.clients {
		ids = append(ids, c.WorkspaceID())
	}
	return ids
}

// Has reports whether workspaceID is served.
func (s *ClientSet) Has(workspaceID string) bool {
	_, ok := s.For(workspaceID)
	return ok
}