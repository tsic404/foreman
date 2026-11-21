package catalog

import (
	"context"
	"slices"
)

// Overview builds the home page's read model (04-architecture.md §2). Every
// number is a set cardinality over grant-visible sources, never a sum of
// per-source totals: servers are connected stable ids, workspaces are
// (serverId, workspaceId), agents are (serverId, workspaceId, agentId) bound
// to a Foreman runtime, environments are stable registered ids. Two agents
// sharing a name are two (UI-01). A fact the deployment cannot see is
// unavailable, never zero.
//
// A run row carries the identities frozen at dispatch; this function never
// re-derives them from the current directory, so adding, syncing or removing
// a source cannot move an existing attempt to another server or blank it.
func (s *Service) Overview(ctx context.Context, grant Grant) (OverviewView, error) {
	// The run facts are read before the service lock: the coordinator seam may
	// itself resolve identities through this catalog, and holding the lock
	// across it would deadlock that callback.
	inProgress := InProgressView{Available: s.cfg.RunIndex != nil, Rows: []AttemptView{}}
	var attempts []RunAttempt
	if s.cfg.RunIndex != nil {
		attempts = s.cfg.RunIndex.RunningAttempts()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	sources, err := s.store.ListSources(ctx)
	if err != nil {
		return OverviewView{}, err
	}
	counts := CountsView{}
	agents := map[string]bool{}
	for _, src := range sources {
		snap, ok, err := s.store.GetSnapshot(ctx, src.ID)
		if err != nil {
			return OverviewView{}, err
		}
		visible := make([]Workspace, 0, len(snap.Workspaces))
		seenWorkspace := map[string]bool{}
		for _, workspace := range snap.Workspaces {
			if !grant.Allows(src.ID, workspace.ID) || seenWorkspace[workspace.ID] {
				continue
			}
			seenWorkspace[workspace.ID] = true
			visible = append(visible, workspace)
		}
		// The numbers are computed over the grant-visible sources, never
		// globally and then cropped (§2).
		if !sourceVisible(src, snap, ok, grant) {
			continue
		}
		if src.Connection == ConnectionOK {
			counts.Servers++
		}
		if !ok {
			continue
		}
		counts.Workspaces += len(visible)
		withForeman := map[string]bool{}
		for _, agent := range snap.Agents {
			if !grant.Allows(src.ID, agent.WorkspaceID) {
				continue
			}
			if managementFor(agent, s.managedSet(src, agent.WorkspaceID)) != ManagedForeman {
				continue
			}
			key := string(src.ID) + "\x00" + agent.WorkspaceID + "\x00" + agent.ID
			if agents[key] {
				continue
			}
			if !seenWorkspace[agent.WorkspaceID] {
				continue
			}
			agents[key] = true
			withForeman[agent.WorkspaceID] = true
		}
		counts.WorkspacesWithForemanAgents += len(withForeman)
	}
	counts.Agents = len(agents)
	counts.EnvironmentsAvailable = s.cfg.EnvironmentCount != nil
	if counts.EnvironmentsAvailable {
		counts.Environments = s.cfg.EnvironmentCount(ctx, grant)
	}

	if s.cfg.RunIndex != nil {
		runningAgents := map[string]bool{}
		for _, attempt := range attempts {
			if !grant.Allows(ServerID(attempt.ServerID), attempt.WorkspaceID) {
				continue
			}
			runningAgents[attempt.ServerID+"\x00"+attempt.WorkspaceID+"\x00"+attempt.AgentID] = true
			inProgress.Rows = append(inProgress.Rows, attemptView(attempt))
		}
		slices.SortFunc(inProgress.Rows, func(a, b AttemptView) int {
			switch {
			case a.StartedAt < b.StartedAt:
				return -1
			case a.StartedAt > b.StartedAt:
				return 1
			}
			return 0
		})
		inProgress.Agents = len(runningAgents)
		inProgress.Attempts = len(inProgress.Rows)
	}
	return OverviewView{Counts: counts, InProgress: inProgress}, nil
}

// attemptView projects one coordinator attempt row. The row carries the
// identities of the moment it was dispatched, never a later directory read
// (§2 运行绑定使用当时快照).
func attemptView(attempt RunAttempt) AttemptView {
	return AttemptView{
		ServerID:        attempt.ServerID,
		WorkspaceID:     attempt.WorkspaceID,
		AgentID:         attempt.AgentID,
		AgentName:       attempt.AgentName,
		IssueID:         attempt.IssueID,
		IssueIdentifier: attempt.IssueIdentifier,
		Environment:     attempt.Environment,
		Status:          attempt.Status,
		StartedAt:       stamp(attempt.StartedAt),
	}
}
