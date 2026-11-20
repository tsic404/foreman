// Package catalogtest provides the stub upstream and credential resolver the
// catalog's own tests drive. It is a test-support package, not production
// code: the scenarios inject the failures the contract names (revocation,
// truncation, refusal, timeout, a competing edit) that a live server cannot be
// asked to produce on demand.
package catalogtest

import (
	"context"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/catalog"
)

// Agent is one stub agent row.
type Agent struct {
	WorkspaceID string
	ID          string
	Name        string
	RuntimeID   string
	Bound       bool
}

// Catalog is one workspace's stub answer.
type Catalog struct {
	Projects []catalog.Project
	Agents   []Agent
	Runtimes []catalog.Runtime
}

// Upstream is a scriptable catalog.Upstream. Every seam has a failure hook, so
// a test states the injected fault instead of racing a real server.
type Upstream struct {
	mu sync.Mutex

	Workspaces []catalog.Workspace
	Catalogs   map[string]Catalog
	Agents     map[string]Agent
	ListErr    error
	CatalogErr map[string]error
	// CatalogGate, when non-nil and CatalogGateAfter workspaces were already
	// answered, holds the next WorkspaceCatalog until the gate closes. It is
	// how a scenario parks a sync between its read and its publish.
	CatalogGate      <-chan struct{}
	CatalogGateAfter int
	// CatalogGateReached, when non-nil, receives a value just before the gate
	// blocks, so a scenario can act while the sync is parked between its read
	// and its publish.
	CatalogGateReached chan<- struct{}
	CatalogCalls       int
	ReadErr            error
	// ReadErrAfter fails the reads beyond the first N successful ones, so a
	// test can let the pre-write comparison read land and fail the read-back.
	ReadErrAfter int
	WriteErr     error
	// WriteLandsThenFails models a write the upstream applied whose answer was
	// lost in transit: the binding moves, the caller sees a transport failure.
	WriteLandsThenFails bool
	Reads               int
	Writes              int
	WriteHappened       bool
	// WriteDelay holds one write until the delay elapses or the caller's
	// context ends. A context that ends after the write landed models the
	// lost-answer timeout the reconcile budget must survive.
	WriteDelay time.Duration
	// WriteGuard runs inside the conditional write after the expected value
	// was checked: a test uses it to land a competing edit between the
	// pre-read and this write, or to refuse the write. It must not call back
	// into the stub.
	WriteGuard func(agentID, expectedRuntimeID, runtimeID string) error
	// CompetingEditBeforeWrite lands another administrator's change inside
	// the conditional write's window, right before the condition is checked.
	CompetingEditBeforeWrite string
	// ConditionalWrite declares whether this adapter can submit a write that
	// is conditional on the value it read. The production HTTP adapter cannot;
	// a scenario that exercises the full protocol opts in.
	ConditionalWrite bool
	// RequireWorkspace makes the agent calls fail unless the request names the
	// agent's workspace, modelling the anchored upstream's scoped routes.
	RequireWorkspace bool
	// WriteApplies, when non-nil, decides whether one write lands. It answers
	// false to model a write the upstream accepted but did not apply.
	WriteApplies func(agentID, runtimeID string) bool
}

// NewUpstream builds an empty stub.
func NewUpstream() *Upstream {
	return &Upstream{
		Catalogs:   map[string]Catalog{},
		Agents:     map[string]Agent{},
		CatalogErr: map[string]error{},
	}
}

// ListWorkspaces answers the source's workspace set.
func (u *Upstream) ListWorkspaces(_ context.Context, _ catalog.Source) ([]catalog.Workspace, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.ListErr != nil {
		return nil, u.ListErr
	}
	return append([]catalog.Workspace(nil), u.Workspaces...), nil
}

// WorkspaceCatalog answers one workspace's directory.
func (u *Upstream) WorkspaceCatalog(_ context.Context, _ catalog.Source, workspaceID string) (catalog.WorkspaceCatalog, error) {
	u.mu.Lock()
	u.CatalogCalls++
	call := u.CatalogCalls
	gate := u.CatalogGate
	gateAfter := u.CatalogGateAfter
	u.mu.Unlock()
	if gate != nil && call > gateAfter {
		if u.CatalogGateReached != nil {
			select {
			case u.CatalogGateReached <- struct{}{}:
			default:
			}
		}
		<-gate
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if err, ok := u.CatalogErr[workspaceID]; ok && err != nil {
		return catalog.WorkspaceCatalog{}, err
	}
	entry := u.Catalogs[workspaceID]
	out := catalog.WorkspaceCatalog{
		Projects: append([]catalog.Project(nil), entry.Projects...),
		Runtimes: append([]catalog.Runtime(nil), entry.Runtimes...),
	}
	for _, agent := range entry.Agents {
		out.Agents = append(out.Agents, catalog.Agent{
			WorkspaceID: agent.WorkspaceID, ID: agent.ID, Name: agent.Name,
			RuntimeID: agent.RuntimeID, RuntimeBound: agent.Bound,
		})
	}
	return out, nil
}

// ReadAgent answers one agent's current binding.
func (u *Upstream) ReadAgent(ctx context.Context, _ catalog.Source, workspaceID, agentID string) (catalog.Agent, bool, error) {
	if err := ctx.Err(); err != nil {
		return catalog.Agent{}, false, &catalog.UpstreamError{Op: "get agent", Kind: catalog.UpstreamTransient, Message: "request cancelled"}
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.Reads++
	if u.ReadErr != nil && (u.ReadErrAfter == 0 || u.Reads > u.ReadErrAfter) {
		return catalog.Agent{}, false, u.ReadErr
	}
	agent, ok := u.Agents[agentID]
	if !ok {
		return catalog.Agent{}, false, nil
	}
	if u.RequireWorkspace && workspaceID != agent.WorkspaceID {
		return catalog.Agent{}, false, &catalog.UpstreamError{Op: "get agent", Status: 400, Kind: catalog.UpstreamRejected, Message: "workspace_id is required and must match the agent's workspace"}
	}
	return catalog.Agent{
		WorkspaceID: agent.WorkspaceID, ID: agent.ID, Name: agent.Name,
		RuntimeID: agent.RuntimeID, RuntimeBound: agent.Bound,
	}, true, nil
}

// SetAgentRuntime records one conditional binding write.
func (u *Upstream) SetAgentRuntime(ctx context.Context, _ catalog.Source, workspaceID, agentID, expectedRuntimeID, runtimeID string) error {
	if u.WriteDelay > 0 {
		select {
		case <-ctx.Done():
			return u.finishTimedOutWrite(agentID, runtimeID)
		case <-time.After(u.WriteDelay):
		}
	}
	if err := ctx.Err(); err != nil {
		return u.finishTimedOutWrite(agentID, runtimeID)
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.Writes++
	u.WriteHappened = true
	agent, ok := u.Agents[agentID]
	if !ok {
		return nil
	}
	if u.RequireWorkspace && workspaceID != agent.WorkspaceID {
		return &catalog.UpstreamError{Op: "put agent", Status: 400, Kind: catalog.UpstreamRejected, Message: "workspace_id is required and must match the agent's workspace"}
	}
	if u.WriteGuard != nil {
		if err := u.WriteGuard(agentID, expectedRuntimeID, runtimeID); err != nil {
			return err
		}
	}
	if u.CompetingEditBeforeWrite != "" {
		agent.RuntimeID = u.CompetingEditBeforeWrite
		agent.Bound = true
		u.Agents[agentID] = agent
	}
	if u.ConditionalWrite && agent.RuntimeID != expectedRuntimeID {
		return &catalog.UpstreamError{Op: "put agent", Status: 409, Kind: catalog.UpstreamConflict, Message: "the binding changed since it was read"}
	}
	if u.WriteErr != nil {
		if u.WriteLandsThenFails {
			if agent, ok := u.Agents[agentID]; ok {
				agent.RuntimeID = runtimeID
				agent.Bound = runtimeID != ""
				u.Agents[agentID] = agent
			}
		}
		return u.WriteErr
	}
	if u.WriteApplies != nil && !u.WriteApplies(agentID, runtimeID) {
		return nil
	}
	agent.RuntimeID = runtimeID
	agent.Bound = runtimeID != ""
	u.Agents[agentID] = agent
	return nil
}

// finishTimedOutWrite models a write whose answer was lost after the upstream
// applied it: the caller sees a transient failure, the binding moved.
func (u *Upstream) finishTimedOutWrite(agentID, runtimeID string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.Writes++
	u.WriteHappened = true
	if u.WriteLandsThenFails {
		if agent, ok := u.Agents[agentID]; ok {
			agent.RuntimeID = runtimeID
			agent.Bound = runtimeID != ""
			u.Agents[agentID] = agent
		}
	}
	return &catalog.UpstreamError{Op: "put agent", Kind: catalog.UpstreamTransient, Message: "request failed"}
}

// BindingWriteSupported declares the adapter capability.
func (u *Upstream) BindingWriteSupported(catalog.Source) (bool, string) {
	if u.ConditionalWrite {
		return true, ""
	}
	return false, "the stub declares no conditional agent update"
}

// SetBinding records an upstream binding directly, modelling another
// administrator's change.
func (u *Upstream) SetBinding(agentID, runtimeID string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	agent := u.Agents[agentID]
	agent.RuntimeID = runtimeID
	agent.Bound = runtimeID != ""
	u.Agents[agentID] = agent
}

// Credentials resolves every reference to a fixed token.
type Credentials struct {
	// Tokens overrides the token per reference; the default is RefToken.
	Tokens map[string]string
	Err    error
}

// RefToken is the token every reference resolves to unless overridden. It is a
// nonce the leak assertions search for, not a credential.
const RefToken = "tok-catalog-test-9f31"

// Token resolves one reference.
func (c Credentials) Token(ref string) (string, error) {
	if c.Err != nil {
		return "", c.Err
	}
	if token, ok := c.Tokens[ref]; ok {
		return token, nil
	}
	return RefToken, nil
}
