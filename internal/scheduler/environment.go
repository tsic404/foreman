package scheduler

import (
	"context"
	"errors"

	"github.com/tsic404/foreman/internal/registry"
)

// EnvironmentRequest is the claim facts the rule → environment resolver reads.
// It carries no credential and no Job template: a rule only selects where the
// attempt runs (04-architecture.md §3.4).
type EnvironmentRequest struct {
	// SourceID is the claim's stable server identity. A workspace id is only
	// unique inside one source, so the resolver must not infer it from the
	// workspace alone.
	SourceID       string
	WorkspaceID    string
	AgentID        string
	IssueID        string
	ProjectID      string
	PriorSessionID string
	AttemptID      string
}

// EnvironmentBinding is the reservation a claim freezes.
type EnvironmentBinding struct {
	EnvironmentID string
	SlotID        string
	RuleID        string
}

// EnvironmentBinder is the one resolver preview and real dispatch share
// (02-contracts V2-C01: 与真实派发共用已发布规则、环境解析器及准入判据).
// Preview calls it read-only; the claim path reserves a slot through Bind.
// A nil seam keeps the pre-environment behaviour for callers that never
// configured a registry.
type EnvironmentBinder interface {
	Bind(ctx context.Context, request EnvironmentRequest) (EnvironmentBinding, error)
	Release(ctx context.Context, binding EnvironmentBinding) error
}

// Errors the binder reports. Saturation defers the claim so the server
// re-dispatches after the prepare lease; a blocked or unknown placement
// refuses the claim without creating a Job.
var (
	// ErrEnvironmentSaturated means the task is eligible but no slot is free.
	ErrEnvironmentSaturated = errors.New("no environment slot is available")
	// ErrEnvironmentBlocked means no configuration change-free placement
	// exists; the claim is refused rather than moved to another runtime.
	ErrEnvironmentBlocked = errors.New("the task is not eligible for any registered environment")
)

// releaseEnvironment returns one entry's environment slot. It is best-effort:
// a release the registry cannot record leaves the slot visible there instead of
// pretending the remote session closed.
func (s *Scheduler) releaseEnvironment(ctx context.Context, e registry.TaskEntry) {
	if s.cfg.EnvironmentBinder == nil || e.EnvironmentID == "" || e.EnvironmentSlotID == "" {
		return
	}
	binding := EnvironmentBinding{EnvironmentID: e.EnvironmentID, SlotID: e.EnvironmentSlotID}
	if err := s.cfg.EnvironmentBinder.Release(ctx, binding); err != nil {
		s.log.WarnContext(ctx, "task.environment_release_failed",
			"task_id", e.TaskID, "environment_id", e.EnvironmentID,
			"slot_id", e.EnvironmentSlotID, "err", err.Error())
	}
}
