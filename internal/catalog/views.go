package catalog

import (
	"context"
	"slices"
	"time"
)

// Scope is one (server, workspace) pair a grant covers. A workspace id is only
// unique inside one upstream, so a scope that names only a workspace is
// resolved to a source before it is used; the pair is the unit every read and
// write checks (§8 跨域检查在服务端).
type Scope struct {
	ServerID    ServerID
	WorkspaceID string
}

// Grant is the caller's read scope, resolved server-side from the credential,
// never from a query parameter (§8 跨域检查在服务端). A nil scope set is the
// deployment-wide grant.
type Grant struct {
	Scopes []Scope
}

// IsDeploymentWide reports whether the grant covers every source and workspace.
func (g Grant) IsDeploymentWide() bool { return g.Scopes == nil }

// Allows reports whether the grant covers one (server, workspace) pair.
func (g Grant) Allows(serverID ServerID, workspaceID string) bool {
	if g.Scopes == nil {
		return true
	}
	return slices.ContainsFunc(g.Scopes, func(scope Scope) bool {
		return scope.ServerID == serverID && scope.WorkspaceID == workspaceID
	})
}

// AllowsServer reports whether the grant covers any workspace of one server.
func (g Grant) AllowsServer(serverID ServerID) bool {
	if g.Scopes == nil {
		return true
	}
	return slices.ContainsFunc(g.Scopes, func(scope Scope) bool {
		return scope.ServerID == serverID
	})
}

// AllowsWorkspace reports whether the grant covers one workspace on any
// server. It is only a pre-filter for a query parameter; the per-row checks
// still use Allows with the row's server id.
func (g Grant) AllowsWorkspace(workspaceID string) bool {
	if g.Scopes == nil {
		return true
	}
	return slices.ContainsFunc(g.Scopes, func(scope Scope) bool {
		return scope.WorkspaceID == workspaceID
	})
}

// SourceView is one source's management-face projection.
type SourceView struct {
	ServerID                    string    `json:"server_id"`
	URL                         string    `json:"url"`
	CredentialRef               string    `json:"credential_ref"`
	AllowedWorkspaces           []string  `json:"allowed_workspaces"`
	AutoSync                    bool      `json:"auto_sync"`
	SyncPeriodSeconds           int       `json:"sync_period_s"`
	RetryBudget                 int       `json:"retry_budget"`
	Connection                  string    `json:"connection"`
	Sync                        string    `json:"sync"`
	Stale                       bool      `json:"stale"`
	Generation                  uint64    `json:"generation"`
	Workspaces                  int       `json:"workspaces"`
	WorkspacesWithForemanAgents int       `json:"workspaces_with_foreman_agents"`
	Agents                      int       `json:"agents"`
	ManagedAgents               int       `json:"managed_agents"`
	LastAttemptAt               string    `json:"last_attempt_at,omitempty"`
	LastSuccessAt               string    `json:"last_success_at,omitempty"`
	NextSyncAt                  string    `json:"next_sync_at,omitempty"`
	LastError                   string    `json:"last_error,omitempty"`
	FailureCount                int       `json:"failure_count"`
	WriteBack                   WriteBack `json:"write_back"`
}

// sourceView projects a source record without its snapshot-derived counts.
// hasSnapshot states whether a directory exists; without one the binding
// editor is closed with a reason instead of enabled.
func sourceView(src Source, hasSnapshot bool, writeBack WriteBack) SourceView {
	return SourceView{
		ServerID:          string(src.ID),
		URL:               src.URL,
		CredentialRef:     src.CredentialRef,
		AllowedWorkspaces: append([]string{}, src.AllowedWorkspaces...),
		AutoSync:          src.AutoSync,
		SyncPeriodSeconds: int(src.SyncPeriod / time.Second),
		RetryBudget:       src.RetryBudget,
		Connection:        string(src.Connection),
		Sync:              string(src.Sync),
		Stale:             true,
		Generation:        src.Generation,
		LastAttemptAt:     stamp(src.LastAttemptAt),
		LastSuccessAt:     stamp(src.LastSuccessAt),
		NextSyncAt:        stamp(src.NextSyncAt),
		LastError:         src.LastError,
		FailureCount:      src.FailureCount,
		WriteBack:         writeBack,
	}
}

// WorkspaceView is one workspace row with its per-workspace counts.
type WorkspaceView struct {
	ServerID          string `json:"server_id"`
	WorkspaceID       string `json:"workspace_id"`
	Name              string `json:"name"`
	AgentCount        int    `json:"agent_count"`
	ForemanAgentCount int    `json:"foreman_agent_count"`
}

// ProjectView is one project row.
type ProjectView struct {
	ServerID    string `json:"server_id"`
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	Title       string `json:"title"`
}

// RuntimeView is one runtime row. Managed marks the runtimes Foreman
// registered in this source and workspace.
type RuntimeView struct {
	ServerID    string `json:"server_id"`
	WorkspaceID string `json:"workspace_id"`
	RuntimeID   string `json:"runtime_id"`
	Name        string `json:"name"`
	Provider    string `json:"provider"`
	Status      string `json:"status"`
	IsManaged   bool   `json:"is_managed"`
}

// RuntimeOptionView is one runtime candidate in the binding editor.
type RuntimeOptionView struct {
	RuntimeID string `json:"runtime_id"`
	Name      string `json:"name"`
	Provider  string `json:"provider"`
	Status    string `json:"status"`
}

// AgentView is one agent row: its actual binding, its management class and the
// two candidate groups the editor offers.
type AgentView struct {
	ServerID          string              `json:"server_id"`
	WorkspaceID       string              `json:"workspace_id"`
	AgentID           string              `json:"agent_id"`
	Name              string              `json:"name"`
	RuntimeID         string              `json:"runtime_id"`
	RuntimeBound      bool                `json:"runtime_bound"`
	Management        string              `json:"management"`
	SyncedAt          string              `json:"synced_at,omitempty"`
	Tombstoned        bool                `json:"tombstoned"`
	ForemanRuntimes   []RuntimeOptionView `json:"foreman_runtimes"`
	UnmanagedRuntimes []RuntimeOptionView `json:"unmanaged_runtimes"`
	WriteBack         WriteBack           `json:"write_back"`
}

// TombstoneView is one confirmed deletion.
type TombstoneView struct {
	Kind        string `json:"kind"`
	WorkspaceID string `json:"workspace_id"`
	ID          string `json:"id"`
	RemovedAt   string `json:"removed_at"`
	Generation  uint64 `json:"generation"`
}

// DirectoryView is one source's current catalog generation, cropped to the
// caller's grant.
type DirectoryView struct {
	ServerID   string          `json:"server_id"`
	Generation uint64          `json:"generation"`
	SyncedAt   string          `json:"synced_at,omitempty"`
	Sync       string          `json:"sync"`
	Connection string          `json:"connection"`
	Stale      bool            `json:"stale"`
	LastError  string          `json:"last_error,omitempty"`
	Workspaces []WorkspaceView `json:"workspaces"`
	Projects   []ProjectView   `json:"projects"`
	Agents     []AgentView     `json:"agents"`
	Runtimes   []RuntimeView   `json:"runtimes"`
	Tombstones []TombstoneView `json:"tombstones"`
}

// CountsView is the overview's four numbers (04-architecture.md §2).
type CountsView struct {
	Servers                     int `json:"servers"`
	Workspaces                  int `json:"workspaces"`
	WorkspacesWithForemanAgents int `json:"workspaces_with_foreman_agents"`
	Agents                      int `json:"agents"`
	Environments                int `json:"environments"`
	// EnvironmentsAvailable is false when the environment registry is not
	// wired: the count is then unknown, not zero (§2 生产对失联显示陈旧/未知,
	// 不用零代替).
	EnvironmentsAvailable bool `json:"environments_available"`
}

// AttemptView is one in-flight attempt row.
type AttemptView struct {
	ServerID        string `json:"server_id"`
	WorkspaceID     string `json:"workspace_id"`
	AgentID         string `json:"agent_id"`
	AgentName       string `json:"agent_name"`
	IssueID         string `json:"issue_id"`
	IssueIdentifier string `json:"issue_identifier"`
	Environment     string `json:"environment,omitempty"`
	Status          string `json:"status"`
	StartedAt       string `json:"started_at,omitempty"`
	// SandboxPosture is the run detail's posture fact: privilege and the user
	// the sandbox client container runs as. Known=false renders as "not
	// recorded" rather than as a non-privileged default (ADR-002).
	SandboxPosture PostureView `json:"sandbox_posture"`
}

// PostureView is one attempt's sandbox posture as the run detail presents it.
type PostureView struct {
	Known      bool `json:"known"`
	Privileged bool `json:"privileged"`
	// RunAsUser is the execution user, and RunAsUserSet says whether the Job
	// declared one. Only the UID decides root: RunAsNonRoot is an admission
	// rule, not an identity, so its false value never implies root.
	RunAsUser    int64 `json:"run_as_user"`
	RunAsUserSet bool  `json:"run_as_user_set"`
	RunAsNonRoot bool  `json:"run_as_non_root"`
}

// InProgressView is the overview's in-flight section. Available is false when
// the run facts are not wired: an empty list then must not be read as "nothing
// is running".
type InProgressView struct {
	Available bool          `json:"available"`
	Agents    int           `json:"agents"`
	Attempts  int           `json:"attempts"`
	Rows      []AttemptView `json:"rows"`
}

// OverviewView is the home page's read model.
type OverviewView struct {
	Counts     CountsView     `json:"counts"`
	InProgress InProgressView `json:"in_progress"`
}

// AuditView is one audit row projection.
type AuditView struct {
	Entries []AuditEntry `json:"entries"`
}

// Directory builds one source's catalog view. A workspace filter narrows the
// answer; a workspace outside the grant is answered as absent by the caller.
func (s *Service) Directory(ctx context.Context, id ServerID, workspaceID string, grant Grant) (DirectoryView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok, err := s.store.GetSource(ctx, id)
	if err != nil {
		return DirectoryView{}, err
	}
	if !ok {
		return DirectoryView{}, ErrSourceNotFound
	}
	snap, hasSnapshot, err := s.store.GetSnapshot(ctx, src.ID)
	if err != nil {
		return DirectoryView{}, err
	}
	if !sourceVisible(src, snap, hasSnapshot, grant) {
		// A source the grant cannot reach is indistinguishable from one that
		// does not exist (§8 跨域检查在服务端).
		return DirectoryView{}, ErrSourceNotFound
	}
	view := DirectoryView{
		ServerID:   string(src.ID),
		Generation: src.Generation,
		Sync:       string(src.Sync),
		Connection: string(src.Connection),
		Stale:      src.Connection != ConnectionOK || src.Sync != SyncOK,
		LastError:  src.LastError,
		Workspaces: []WorkspaceView{},
		Projects:   []ProjectView{},
		Agents:     []AgentView{},
		Runtimes:   []RuntimeView{},
		Tombstones: []TombstoneView{},
	}
	if !hasSnapshot {
		return view, nil
	}
	view.SyncedAt = stamp(snap.SyncedAt)
	managed := s.managedByWorkspace(src, snap)
	agentCounts := map[string][2]int{}
	for _, agent := range snap.Agents {
		counts := agentCounts[agent.WorkspaceID]
		counts[0]++
		if agent.Management == ManagedForeman {
			counts[1]++
		}
		agentCounts[agent.WorkspaceID] = counts
	}
	for _, workspace := range snap.Workspaces {
		if workspaceID != "" && workspace.ID != workspaceID {
			continue
		}
		if !grant.Allows(src.ID, workspace.ID) {
			continue
		}
		counts := agentCounts[workspace.ID]
		view.Workspaces = append(view.Workspaces, WorkspaceView{
			ServerID: string(src.ID), WorkspaceID: workspace.ID, Name: workspace.Name,
			AgentCount: counts[0], ForemanAgentCount: counts[1],
		})
	}
	for _, project := range snap.Projects {
		if workspaceID != "" && project.WorkspaceID != workspaceID {
			continue
		}
		if !grant.Allows(src.ID, project.WorkspaceID) {
			continue
		}
		view.Projects = append(view.Projects, ProjectView{
			ServerID: string(src.ID), WorkspaceID: project.WorkspaceID,
			ProjectID: project.ID, Title: project.Title,
		})
	}
	for _, runtime := range snap.Runtimes {
		if workspaceID != "" && runtime.WorkspaceID != workspaceID {
			continue
		}
		if !grant.Allows(src.ID, runtime.WorkspaceID) {
			continue
		}
		view.Runtimes = append(view.Runtimes, RuntimeView{
			ServerID: string(src.ID), WorkspaceID: runtime.WorkspaceID,
			RuntimeID: runtime.ID, Name: runtime.Name, Provider: runtime.Provider,
			Status: runtime.Status, IsManaged: managed[runtime.WorkspaceID][runtime.ID],
		})
	}
	tombstoned := map[string]bool{}
	for _, tombstone := range snap.Tombstones {
		if tombstone.Kind != TombstoneAgent {
			continue
		}
		tombstoned[tombstone.WorkspaceID+"/"+tombstone.ID] = true
	}
	for _, agent := range snap.Agents {
		if workspaceID != "" && agent.WorkspaceID != workspaceID {
			continue
		}
		if !grant.Allows(src.ID, agent.WorkspaceID) {
			continue
		}
		writeBack := s.writeBackCapability(src, true)
		view.Agents = append(view.Agents, buildAgentView(src, snap, agent, managed, tombstoned, writeBack))
	}
	for _, tombstone := range snap.Tombstones {
		if !grant.Allows(src.ID, tombstone.WorkspaceID) {
			continue
		}
		view.Tombstones = append(view.Tombstones, TombstoneView{
			Kind: string(tombstone.Kind), WorkspaceID: tombstone.WorkspaceID,
			ID: tombstone.ID, RemovedAt: stamp(tombstone.RemovedAt), Generation: tombstone.Generation,
		})
	}
	return view, nil
}

// Agents lists agent rows across the sources the grant covers, optionally
// narrowed to one source and one workspace.
func (s *Service) Agents(ctx context.Context, serverID ServerID, workspaceID string, grant Grant) ([]AgentView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sources, err := s.store.ListSources(ctx)
	if err != nil {
		return nil, err
	}
	var out []AgentView
	for _, src := range sources {
		if serverID != "" && src.ID != serverID {
			continue
		}
		snap, ok, err := s.store.GetSnapshot(ctx, src.ID)
		if err != nil {
			return nil, err
		}
		if !ok || !sourceVisible(src, snap, true, grant) {
			continue
		}
		managed := s.managedByWorkspace(src, snap)
		tombstoned := map[string]bool{}
		for _, tombstone := range snap.Tombstones {
			if tombstone.Kind == TombstoneAgent {
				tombstoned[tombstone.WorkspaceID+"/"+tombstone.ID] = true
			}
		}
		writeBack := s.writeBackCapability(src, true)
		for _, agent := range snap.Agents {
			if workspaceID != "" && agent.WorkspaceID != workspaceID {
				continue
			}
			if !grant.Allows(src.ID, agent.WorkspaceID) {
				continue
			}
			out = append(out, buildAgentView(src, snap, agent, managed, tombstoned, writeBack))
		}
	}
	return out, nil
}

// Audit returns the audit rows the grant covers, newest first.
func (s *Service) Audit(ctx context.Context, serverID ServerID, grant Grant) ([]AuditEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.store.ListAudit(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]AuditEntry, 0, len(entries))
	for _, entry := range entries {
		if serverID != "" && entry.ServerID != string(serverID) {
			continue
		}
		if entry.WorkspaceID != "" && !grant.Allows(ServerID(entry.ServerID), entry.WorkspaceID) {
			continue
		}
		if entry.ServerID != "" && !grant.IsDeploymentWide() && entry.WorkspaceID == "" {
			// A source-level action carries no workspace; only the
			// deployment-wide credential reads it.
			continue
		}
		out = append(out, entry)
	}
	slices.Reverse(out)
	return out, nil
}

// managedByWorkspace answers the Foreman runtime ids per workspace.
func (s *Service) managedByWorkspace(src Source, snap Snapshot) map[string]map[string]bool {
	managed := map[string]map[string]bool{}
	for _, workspace := range snap.Workspaces {
		managed[workspace.ID] = s.managedSet(src, workspace.ID)
	}
	return managed
}

// managedSet is the Foreman runtime id set of one source and workspace.
func (s *Service) managedSet(src Source, workspaceID string) map[string]bool {
	if s.cfg.ManagedRuntimeIDs == nil {
		return nil
	}
	set := map[string]bool{}
	for _, runtimeID := range s.cfg.ManagedRuntimeIDs(src, workspaceID) {
		if runtimeID != "" {
			set[runtimeID] = true
		}
	}
	return set
}

// sourceVisible reports whether the grant reaches a source, either through its
// configured scope or through a workspace its directory holds. Without a
// directory only the configured scope can prove it.
func sourceVisible(src Source, snap Snapshot, hasSnapshot bool, grant Grant) bool {
	if grant.IsDeploymentWide() || grantTouches(grant, src) {
		return true
	}
	if !hasSnapshot {
		return false
	}
	for _, workspace := range snap.Workspaces {
		if grant.Allows(src.ID, workspace.ID) {
			return true
		}
	}
	return false
}

// buildAgentView assembles one agent row.
func buildAgentView(src Source, snap Snapshot, agent Agent, managed map[string]map[string]bool, tombstoned map[string]bool, writeBack WriteBack) AgentView {
	set := managed[agent.WorkspaceID]
	view := AgentView{
		ServerID: string(src.ID), WorkspaceID: agent.WorkspaceID, AgentID: agent.ID,
		Name: agent.Name, RuntimeID: agent.RuntimeID, RuntimeBound: agent.RuntimeBound,
		// The class is derived from the binding the directory holds against
		// the runtime set this deployment knows now, not from a stored copy a
		// later registration would make stale.
		Management: string(managementFor(agent, set)), SyncedAt: stamp(snap.SyncedAt),
		Tombstoned:        tombstoned[agent.WorkspaceID+"/"+agent.ID],
		ForemanRuntimes:   []RuntimeOptionView{},
		UnmanagedRuntimes: []RuntimeOptionView{},
		WriteBack:         writeBack,
	}
	if view.Tombstoned {
		view.WriteBack = WriteBack{Reason: "the agent was removed upstream; binding is closed"}
	}
	for _, runtime := range snap.Runtimes {
		if runtime.WorkspaceID != agent.WorkspaceID {
			continue
		}
		option := RuntimeOptionView{
			RuntimeID: runtime.ID, Name: runtime.Name,
			Provider: runtime.Provider, Status: runtime.Status,
		}
		if set[runtime.ID] {
			view.ForemanRuntimes = append(view.ForemanRuntimes, option)
		} else {
			view.UnmanagedRuntimes = append(view.UnmanagedRuntimes, option)
		}
	}
	return view
}

// writeBackCapability answers whether this source may be written. A source
// whose credential was revoked, one with no catalog yet, or an upstream
// adapter without a conditional write shows the reason instead of an enabled
// editor.
func (s *Service) writeBackCapability(src Source, hasSnapshot bool) WriteBack {
	capability := writeBackCapability(src, hasSnapshot)
	if !capability.Enabled {
		return capability
	}
	if supported, reason := s.upstream.BindingWriteSupported(src); !supported {
		return WriteBack{Reason: reason}
	}
	return capability
}

// writeBackCapability is the source-state half of the capability answer.
func writeBackCapability(src Source, hasSnapshot bool) WriteBack {
	switch {
	case src.Connection == ConnectionUnauthorized:
		return WriteBack{Reason: "the source credential is revoked; binding is read-only"}
	case !hasSnapshot:
		return WriteBack{Reason: "the source has no synchronized directory yet"}
	case src.Connection == ConnectionFailed:
		return WriteBack{Reason: "the source is unreachable; binding is read-only"}
	}
	return WriteBack{Enabled: true}
}

// stamp formats a time for the wire; the zero time becomes an empty string so a
// missing fact is not shown as a 0001 date.
func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
