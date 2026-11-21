package catalog

import (
	"context"
	"errors"
	"strings"
)

// SetBinding writes one agent's runtime association and reports success only
// when the upstream read-back shows it (02-contracts V2-C02, 04-architecture
// §3.2). Three gates protect the write:
//
//   - The adapter must support a conditional write (the pinned Multica API
//     does not, so its capability answer keeps the surface read-only with the
//     reason instead of sending an unconditional body).
//   - The pre-read and the write both name the agent's workspace, and the
//     answer must name that agent and workspace — a cross-grant id cannot be
//     smuggled through an authorized workspace path.
//   - The write carries the pre-read value, so a competing edit between the
//     read and the write is a conflict upstream, not a silent overwrite.
//
// A refusal is rejected; a timeout is reconciled by exactly one read-back under
// its own budget, and an unsettled one is unknown, never applied. The local
// catalog moves only after the read-back confirms the change.
func (s *Service) SetBinding(ctx context.Context, req BindingRequest, actor string) (BindingOutcome, error) {
	if strings.TrimSpace(req.WorkspaceID) == "" || strings.TrimSpace(req.AgentID) == "" {
		return BindingOutcome{}, errors.New("workspace_id and agent_id are required")
	}
	if strings.TrimSpace(req.RuntimeID) == "" {
		return BindingOutcome{}, errors.New("runtime_id is required")
	}

	s.mu.Lock()
	src, ok, err := s.store.GetSource(ctx, req.ServerID)
	if err != nil {
		s.mu.Unlock()
		return BindingOutcome{}, err
	}
	if !ok {
		s.mu.Unlock()
		return BindingOutcome{}, ErrSourceNotFound
	}
	snap, hasSnapshot, err := s.store.GetSnapshot(ctx, req.ServerID)
	s.mu.Unlock()
	if err != nil {
		return BindingOutcome{}, err
	}
	outcome := BindingOutcome{
		ServerID: string(src.ID), WorkspaceID: req.WorkspaceID, AgentID: req.AgentID,
		RequestedRuntimeID: req.RuntimeID, ObservedAt: s.now(),
	}
	capability := s.writeBackCapability(src, hasSnapshot)
	if !capability.Enabled {
		outcome.Result = BindingUnsupported
		outcome.Detail = capability.Reason
		s.recordBinding(ctx, actor, outcome, capability.Reason)
		return outcome, nil
	}
	if hasSnapshot && tombstoned(snap, req.WorkspaceID, req.AgentID) {
		outcome.Result = BindingUnsupported
		outcome.Detail = "the agent was removed upstream; binding is closed"
		s.recordBinding(ctx, actor, outcome, outcome.Detail)
		return outcome, nil
	}
	if hasSnapshot {
		for _, agent := range snap.Agents {
			if agent.ID != req.AgentID {
				continue
			}
			if agent.WorkspaceID != req.WorkspaceID {
				// The agent exists, but not in the workspace the grant names:
				// the request must not borrow the authorized path's scope.
				outcome.Result = BindingRejected
				outcome.Detail = "the agent does not belong to the requested workspace"
				s.recordBinding(ctx, actor, outcome, outcome.Detail)
				return outcome, nil
			}
			break
		}
	}

	readCtx, cancelRead := context.WithTimeout(ctx, s.cfg.SyncTimeout)
	current, found, err := s.upstream.ReadAgent(readCtx, src, req.WorkspaceID, req.AgentID)
	cancelRead()
	if err != nil {
		outcome.ObservedAt = s.now()
		switch {
		case IsUnauthorized(err):
			outcome.Result = BindingUnsupported
			outcome.Detail = "the source credential is revoked"
			s.markUnauthorized(ctx, src.ID, err)
		default:
			// The previous value could not be established, so the comparison
			// the contract requires cannot pass.
			outcome.Result = BindingUnknown
			outcome.Detail = "the current binding could not be read; no write was sent"
		}
		s.recordBinding(ctx, actor, outcome, outcome.Detail)
		return outcome, nil
	}
	if !found {
		outcome.Result = BindingUnsupported
		outcome.Detail = "the agent no longer exists upstream"
		s.recordBinding(ctx, actor, outcome, outcome.Detail)
		return outcome, nil
	}
	if current.WorkspaceID != "" && current.WorkspaceID != req.WorkspaceID {
		outcome.Result = BindingRejected
		outcome.Detail = "the upstream agent belongs to another workspace"
		s.recordBinding(ctx, actor, outcome, outcome.Detail)
		return outcome, nil
	}
	outcome.ObservedRuntimeID = current.RuntimeID
	outcome.ObservedBound = current.RuntimeBound
	outcome.PreviousRuntimeID = current.RuntimeID
	if current.RuntimeID != req.ExpectedRuntimeID {
		// A competing edit happened between the caller's read and this write.
		// Overwriting it would be the false success the contract forbids.
		outcome.Result = BindingConflict
		outcome.Detail = "the upstream binding changed since it was read; nothing was written"
		outcome.Management = s.classify(src, current.WorkspaceID, current.RuntimeID, current.RuntimeBound)
		s.projectBinding(ctx, src.ID, current)
		s.recordBinding(ctx, actor, outcome, outcome.Detail)
		return outcome, nil
	}

	writeCtx, cancelWrite := context.WithTimeout(ctx, s.cfg.SyncTimeout)
	writeErr := s.upstream.SetAgentRuntime(writeCtx, src, req.WorkspaceID, req.AgentID, req.ExpectedRuntimeID, req.RuntimeID)
	cancelWrite()
	if writeErr != nil {
		if IsConflict(writeErr) {
			outcome.ObservedAt = s.now()
			outcome.UpstreamStatus = upstreamStatus(writeErr)
			outcome.Result = BindingConflict
			outcome.Detail = "the upstream refused the conditional write: another edit landed after the read; nothing was overwritten"
			s.recordBinding(ctx, actor, outcome, outcome.Detail)
			return outcome, nil
		}
		if !IsTransient(writeErr) {
			outcome.ObservedAt = s.now()
			outcome.UpstreamStatus = upstreamStatus(writeErr)
			switch {
			case IsUnauthorized(writeErr):
				outcome.Result = BindingUnsupported
				outcome.Detail = "the source credential cannot write bindings"
				s.markUnauthorized(ctx, src.ID, writeErr)
			default:
				outcome.Result = BindingRejected
				outcome.Detail = boundedError(writeErr)
			}
			s.recordBinding(ctx, actor, outcome, outcome.Detail)
			return outcome, nil
		}
		// A timeout or transport failure leaves the write's outcome unknown.
		// One read-back under its own budget decides; the write is never
		// repeated blindly, and the cancelled write context is never reused.
		if ctx.Err() != nil {
			outcome.ObservedAt = s.now()
			outcome.Result = BindingUnknown
			outcome.Detail = "the request ended before the write outcome could be reconciled"
			s.recordBinding(ctx, actor, outcome, outcome.Detail)
			return outcome, nil
		}
	}

	// The reconcile budget is derived from the still-valid outer request, not
	// from the write context the deadline just cancelled.
	reconcileCtx, cancelReconcile := context.WithTimeout(ctx, s.cfg.SyncTimeout)
	observed, observedFound, readErr := s.upstream.ReadAgent(reconcileCtx, src, req.WorkspaceID, req.AgentID)
	cancelReconcile()
	outcome.ObservedAt = s.now()
	if readErr != nil || !observedFound {
		switch {
		case writeErr == nil:
			outcome.Result = BindingNotApplied
			outcome.Detail = "the write was accepted but the read-back could not confirm it"
			if readErr != nil {
				outcome.Detail = boundedError(readErr)
			}
		default:
			outcome.Result = BindingUnknown
			outcome.Detail = "the write outcome could not be confirmed by read-back"
			if readErr != nil {
				outcome.Detail = boundedError(readErr)
			}
		}
		s.recordBinding(ctx, actor, outcome, outcome.Detail)
		return outcome, nil
	}
	outcome.ObservedRuntimeID = observed.RuntimeID
	outcome.ObservedBound = observed.RuntimeBound
	if observed.ID == "" {
		observed.ID = req.AgentID
	}
	if observed.WorkspaceID == "" {
		observed.WorkspaceID = req.WorkspaceID
	}
	outcome.Management = s.classify(src, observed.WorkspaceID, observed.RuntimeID, observed.RuntimeBound)

	switch {
	case observed.RuntimeID == req.RuntimeID:
		outcome.Result = BindingApplied
		outcome.Detail = ""
		s.projectBinding(ctx, src.ID, observed)
		s.recordBinding(ctx, actor, outcome, "read-back confirmed the requested runtime")
	case observed.RuntimeID == req.ExpectedRuntimeID:
		outcome.Result = BindingNotApplied
		outcome.Detail = "the upstream answer did not take effect; the previous runtime is still in force"
		s.projectBinding(ctx, src.ID, observed)
		s.recordBinding(ctx, actor, outcome, outcome.Detail)
	default:
		outcome.Result = BindingUnknown
		outcome.Detail = "read-back returned a runtime that is neither the requested nor the previous value"
		s.projectBinding(ctx, src.ID, observed)
		s.recordBinding(ctx, actor, outcome, outcome.Detail)
	}
	return outcome, nil
}

// projectBinding moves the local projection to a read-back-confirmed value. It
// never invents a value: the caller passes what the upstream answered.
func (s *Service) projectBinding(ctx context.Context, id ServerID, observed Agent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok, err := s.store.GetSource(ctx, id)
	if err != nil || !ok {
		return
	}
	management := s.classify(src, observed.WorkspaceID, observed.RuntimeID, observed.RuntimeBound)
	if _, ok, err := s.store.PutAgentBinding(ctx, id, observed.WorkspaceID, observed.ID, observed.RuntimeID, observed.RuntimeBound, management); err != nil || !ok {
		if err != nil {
			s.log.Warn("catalog.binding_projection_failed", "server_id", string(id), "err", err.Error())
		}
	}
	s.bindingEpoch[id]++
	if s.confirmed[id] == nil {
		s.confirmed[id] = map[string]confirmedBinding{}
	}
	s.confirmed[id][observed.WorkspaceID+"\x00"+observed.ID] = confirmedBinding{
		workspaceID:  observed.WorkspaceID,
		agentID:      observed.ID,
		runtimeID:    observed.RuntimeID,
		runtimeBound: observed.RuntimeBound,
		management:   management,
		epoch:        s.bindingEpoch[id],
	}
}

// classify answers one binding's management class against this deployment's
// runtime set for the source and workspace.
func (s *Service) classify(src Source, workspaceID, runtimeID string, bound bool) Management {
	agent := Agent{WorkspaceID: workspaceID, RuntimeID: runtimeID, RuntimeBound: bound}
	return managementFor(agent, s.managedSet(src, workspaceID))
}

// markUnauthorized records an immediate revocation on the source.
func (s *Service) markUnauthorized(ctx context.Context, id ServerID, cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok, err := s.store.GetSource(ctx, id)
	if err != nil || !ok {
		return
	}
	src.Connection = ConnectionUnauthorized
	src.LastError = boundedError(cause)
	_ = s.store.PutSource(ctx, src)
	s.log.Warn("catalog.source_unauthorized", "server_id", string(id), "err", src.LastError)
}

// recordBinding writes the audit row. The actor is the credential class, never
// a token.
func (s *Service) recordBinding(ctx context.Context, actor string, outcome BindingOutcome, detail string) {
	entry := AuditEntry{
		At: s.now(), Actor: actor, Action: AuditBindingWrite,
		ServerID: outcome.ServerID, WorkspaceID: outcome.WorkspaceID, AgentID: outcome.AgentID,
		OldRuntimeID: outcome.PreviousRuntimeID, NewRuntimeID: outcome.ObservedRuntimeID,
		Result: string(outcome.Result), Detail: detail,
	}
	if entry.Result == string(BindingApplied) {
		entry.Result = AuditOK
	}
	if entry.Detail == "" {
		entry.Detail = outcome.Detail
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendAuditLocked(ctx, entry)
}

// tombstoned reports whether the agent's row is a confirmed deletion.
func tombstoned(snap Snapshot, workspaceID, agentID string) bool {
	for _, tombstone := range snap.Tombstones {
		if tombstone.Kind == TombstoneAgent && tombstone.WorkspaceID == workspaceID && tombstone.ID == agentID {
			return true
		}
	}
	return false
}

// upstreamStatus reads the status of an upstream refusal.
func upstreamStatus(err error) int {
	var upstream *UpstreamError
	if errors.As(err, &upstream) {
		return upstream.Status
	}
	return 0
}

// BindingStatusCode maps one result onto the management surface's HTTP status.
// Only applied is 200; every other result is an explicit non-success answer.
func BindingStatusCode(result BindingResult) int {
	switch result {
	case BindingApplied:
		return 200
	case BindingConflict, BindingNotApplied:
		return 409
	case BindingRejected:
		return 422
	case BindingUnknown:
		return 504
	case BindingUnsupported:
		return 501
	default:
		return 500
	}
}
