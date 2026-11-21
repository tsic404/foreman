package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

// TestDispatchGateKeepsAClaimConfirmedBeforeTheEntryExists is the confirmation
// boundary review finding 7 names: the upstream had already confirmed the task
// to R0 when the response arrived, and an administrator rebinds the agent
// before the caller reaches this point. Nothing about that later edit revokes
// the confirmation, so the claim must still become a Job — the gate may not
// treat "no local entry yet" as "not the owner".
func TestDispatchGateKeepsAClaimConfirmedBeforeTheEntryExists(t *testing.T) {
	f := newFixture(t)
	// The rebound agent's *current* class is non-managed, but the runtime the
	// task was confirmed to is one this deployment registered.
	f.sched.cfg.DispatchGate = func(_ context.Context, _ string, _ string, realRuntimeID string, claimed []string) (bool, bool) {
		return realRuntimeID == "real-runtime" && slices.Contains(claimed, realRuntimeID), true
	}

	e := f.claimConfirmed(t, "task-1", ClaimScope{RuntimeIDs: []string{"real-runtime"}})
	if e.State != registry.StateJobCreated {
		t.Fatalf("state = %v, want job_created for the confirmed runtime", e.State)
	}
	if e.RealRuntimeID != "real-runtime" {
		t.Fatalf("real_runtime_id = %q, want the confirmed runtime", e.RealRuntimeID)
	}
	if len(f.jobs.createdJobs) != 1 || f.jobs.createdJobs[0].Name != "fm-task-1" {
		t.Fatalf("jobs created: %+v", f.jobs.createdJobs)
	}
}

// TestDispatchGateRefusesAnUnownedRuntimeVisibly: a runtime this deployment
// never registered yields no Job, and the refusal is an error the loop reports
// — the claim is not silently dropped.
func TestDispatchGateRefusesAnUnownedRuntimeVisibly(t *testing.T) {
	f := newFixture(t)
	f.sched.cfg.DispatchGate = func(context.Context, string, string, string, []string) (bool, bool) {
		return false, true
	}

	err := f.sched.OnClaim(context.Background(), claimPayload("task-1"))
	if !errors.Is(err, ErrUnownedClaim) {
		t.Fatalf("err = %v, want ErrUnownedClaim", err)
	}
	if len(f.jobs.createdJobs) != 0 || len(f.jobs.createdSecrets) != 0 {
		t.Fatalf("jobs/secrets created = %d/%d, want none for an unowned runtime",
			len(f.jobs.createdJobs), len(f.jobs.createdSecrets))
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("a refused claim left a registry entry")
	}
}

// TestDispatchGateLeavesAnUnknownRuntimeOnTheLegacyPath: no evidence about
// ownership keeps the pre-catalog behaviour instead of refusing every claim.
func TestDispatchGateLeavesAnUnknownRuntimeOnTheLegacyPath(t *testing.T) {
	f := newFixture(t)
	f.sched.cfg.DispatchGate = func(context.Context, string, string, string, []string) (bool, bool) {
		return false, false
	}

	e := f.claim(t, "task-1")
	if e.State != registry.StateJobCreated {
		t.Fatalf("state = %v, want the legacy job_created path", e.State)
	}
	if len(f.jobs.createdJobs) != 1 {
		t.Fatalf("jobs created = %d, want 1", len(f.jobs.createdJobs))
	}
}

// TestDispatchGateDoesNotRevokeAnAttemptAlreadyRunning: an existing entry keeps
// converging through the duplicate path; the gate never reaches it, so a later
// rebind cannot abandon a Job that is already running.
func TestDispatchGateDoesNotRevokeAnAttemptAlreadyRunning(t *testing.T) {
	f := newFixture(t)
	f.sched.cfg.DispatchGate = func(context.Context, string, string, string, []string) (bool, bool) {
		return true, true
	}
	f.claimConfirmed(t, "task-1", ClaimScope{RuntimeIDs: []string{"real-runtime"}})

	f.sched.cfg.DispatchGate = func(context.Context, string, string, string, []string) (bool, bool) {
		return false, true
	}
	f.server.status = "running"
	if err := f.sched.OnClaimReservedFor(context.Background(), "ws-1", claimPayload("task-1"), SlotReservation{},
		ClaimScope{RuntimeIDs: []string{"real-runtime"}}); err != nil {
		t.Fatalf("duplicate OnClaim: %v", err)
	}
	if _, ok := f.reg.Get("task-1"); !ok {
		t.Fatal("the running attempt was abandoned instead of kept")
	}
	if len(f.jobs.createdJobs) != 1 {
		t.Fatalf("jobs created = %d, want the single existing Job", len(f.jobs.createdJobs))
	}
}

// TestDispatchGateOwnsTheClaimRequestAfterARegistrationGeneration is the
// registration-generation window: the C3 request carried R0 and upstream
// confirmed the task to it, then a successful refresh replaced the live
// declaration with R1 before the first entry landed. Admission and the entry's
// identity are read from the request's frozen scope, not the new generation.
func TestDispatchGateOwnsTheClaimRequestAfterARegistrationGeneration(t *testing.T) {
	f := newFixture(t)
	f.sched.cfg.DispatchGate = func(_ context.Context, _ string, _ string, realRuntimeID string, claimed []string) (bool, bool) {
		return slices.Contains(claimed, realRuntimeID), true
	}
	payload := json.RawMessage(fmt.Sprintf(`{
		"id": "task-1", "agent_id": "agent-1", "issue_id": "issue-1",
		"workspace_id": "ws-1", "runtime_id": "rid-old", "auth_token": "mat_secret",
		"agent": {"model": "x"}
	}`))

	err := f.sched.OnClaimReservedFor(context.Background(), "ws-1", payload, SlotReservation{},
		ClaimScope{RuntimeIDs: []string{"rid-old"}, Identities: map[string]string{"rid-old": "omp\x00builtin"}})
	if err != nil {
		t.Fatalf("OnClaimReservedFor: %v", err)
	}
	e, ok := f.reg.Get("task-1")
	if !ok {
		t.Fatal("the request-confirmed claim left no entry")
	}
	if e.RealRuntimeID != "rid-old" || e.RuntimeIdentity != "omp\x00builtin" {
		t.Fatalf("identity = %q/%q, want the claim request's frozen identity", e.RealRuntimeID, e.RuntimeIdentity)
	}
	if len(f.jobs.createdJobs) != 1 || f.jobs.createdJobs[0].Name != "fm-task-1" {
		t.Fatalf("jobs created: %+v", f.jobs.createdJobs)
	}

	// The same request cannot make the refresh's runtime a replacement owner:
	// it was not carried by the C3 request that returned the task.
	second := json.RawMessage(fmt.Sprintf(`{
		"id": "task-2", "agent_id": "agent-1", "issue_id": "issue-2",
		"workspace_id": "ws-1", "runtime_id": "rid-new", "auth_token": "mat_secret",
		"agent": {"model": "x"}
	}`))
	err = f.sched.OnClaimReservedFor(context.Background(), "ws-1", second, SlotReservation{},
		ClaimScope{RuntimeIDs: []string{"rid-old"}})
	if !errors.Is(err, ErrUnownedClaim) {
		t.Fatalf("err = %v, want ErrUnownedClaim for a runtime outside the request", err)
	}
	if len(f.jobs.createdJobs) != 1 {
		t.Fatalf("jobs created = %d, want no replacement Job", len(f.jobs.createdJobs))
	}
}
