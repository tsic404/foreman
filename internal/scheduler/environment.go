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
	// The fixed foreman-job needs the frozen placement facts, not only the
	// ids: the endpoint, the persistent tree generation and the two verified
	// CLI artifacts (02-contracts.md V2-C03/C04). They are empty on a partial
	// legacy binder, which keeps the template path.
	Endpoint            string
	TLSRef              string
	StateRoot           string
	EnvironmentGen      string
	EnvironmentIdentity string
	Platform            string
	AgentBackend        string
	MulticaVersion      string
	MulticaDigest       string
	AgentVersion        string
	AgentDigest         string
	ConfigRevision      uint64
	ConfigSecretRef     string
	ConfigFileName      string
	ConfigDigest        string
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

// EnvironmentPodReleaser is the optional second half of the release gate: the
// binder can complete a reservation once the bound Job's pods are provably
// gone. A binder that does not implement it leaves the slot for the registry's
// own reconciliation.
type EnvironmentPodReleaser interface {
	ReleaseAfterPodClosed(ctx context.Context, binding EnvironmentBinding) error
}

// EnvironmentIsolator is the release for an attempt that created a Job. It
// records neither close fact, so the reservation keeps its capacity until the
// trusted Pod-close observer and the Job-side remote close each report their
// own evidence.
type EnvironmentIsolator interface {
	Isolate(ctx context.Context, binding EnvironmentBinding) error
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

// isolateEnvironment keeps an attempt's reservation without claiming that its
// Pod or remote session closed. It is the release for every path where a Job
// may still exist; freeing capacity from a drain bound would return it while
// the Pod could still be running.
func (s *Scheduler) isolateEnvironment(ctx context.Context, e registry.TaskEntry) {
	if s.cfg.EnvironmentBinder == nil || e.EnvironmentID == "" || e.EnvironmentSlotID == "" {
		return
	}
	isolator, ok := s.cfg.EnvironmentBinder.(EnvironmentIsolator)
	if !ok {
		// A binder without the seam cannot isolate; freeing the slot instead
		// would be the very bug this path exists to prevent.
		s.log.WarnContext(ctx, "task.environment_isolate_unavailable",
			"task_id", e.TaskID, "environment_id", e.EnvironmentID, "slot_id", e.EnvironmentSlotID)
		return
	}
	binding := EnvironmentBinding{EnvironmentID: e.EnvironmentID, SlotID: e.EnvironmentSlotID}
	if err := isolator.Isolate(ctx, binding); err != nil {
		s.log.WarnContext(ctx, "task.environment_isolate_failed",
			"task_id", e.TaskID, "environment_id", e.EnvironmentID,
			"slot_id", e.EnvironmentSlotID, "err", err.Error())
	}
}

// releaseEnvironmentAfterPodClose tells the environment ledger that the
// attempt's Pods are gone, so a reservation whose remote close was already
// recorded can return its capacity. It is only called on a Job-terminated
// fact: a drain bound that expires with pods still alive proves nothing. A
// failure is returned so a caller that keeps the entry as the Pod-close debt
// handle can retry it instead of dropping the only submission path.
func (s *Scheduler) releaseEnvironmentAfterPodClose(ctx context.Context, e registry.TaskEntry) error {
	releaser, ok := s.cfg.EnvironmentBinder.(EnvironmentPodReleaser)
	if !ok || e.EnvironmentID == "" || e.EnvironmentSlotID == "" {
		return nil
	}
	binding := EnvironmentBinding{EnvironmentID: e.EnvironmentID, SlotID: e.EnvironmentSlotID}
	if err := releaser.ReleaseAfterPodClosed(ctx, binding); err != nil {
		s.log.WarnContext(ctx, "task.environment_pod_close_release_failed",
			"task_id", e.TaskID, "environment_id", e.EnvironmentID,
			"slot_id", e.EnvironmentSlotID, "err", err.Error())
		return err
	}
	return nil
}

// slotCloseDebtApplies reports whether this attempt has a reservation the
// scheduler can still complete with the trusted Pod-close half. A binder
// without that seam (the legacy template path) has no way to submit the fact,
// so keeping the entry would only leak the row.
func (s *Scheduler) slotCloseDebtApplies(e registry.TaskEntry) bool {
	if s.cfg.EnvironmentBinder == nil || e.EnvironmentID == "" || e.EnvironmentSlotID == "" {
		return false
	}
	_, ok := s.cfg.EnvironmentBinder.(EnvironmentPodReleaser)
	return ok
}
