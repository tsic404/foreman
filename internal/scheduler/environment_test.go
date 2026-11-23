package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

type fakeEnvironmentBinder struct {
	requests  []EnvironmentRequest
	binding   EnvironmentBinding
	err       error
	released  []EnvironmentBinding
	isolated  []EnvironmentBinding
	podClosed []EnvironmentBinding
}

func (f *fakeEnvironmentBinder) Bind(_ context.Context, request EnvironmentRequest) (EnvironmentBinding, error) {
	f.requests = append(f.requests, request)
	return f.binding, f.err
}

func (f *fakeEnvironmentBinder) Release(_ context.Context, binding EnvironmentBinding) error {
	f.released = append(f.released, binding)
	return nil
}

func (f *fakeEnvironmentBinder) Isolate(_ context.Context, binding EnvironmentBinding) error {
	f.isolated = append(f.isolated, binding)
	return nil
}

func (f *fakeEnvironmentBinder) ReleaseAfterPodClosed(_ context.Context, binding EnvironmentBinding) error {
	f.podClosed = append(f.podClosed, binding)
	return nil
}

// sourcePlacementBinder answers from the source identity carried by the
// request, the way the published-rule resolver does. An empty source is the
// ambiguous workspace-only lookup and is refused.
type sourcePlacementBinder struct {
	bySource map[string]EnvironmentBinding
	requests []EnvironmentRequest
}

func (b *sourcePlacementBinder) Bind(_ context.Context, request EnvironmentRequest) (EnvironmentBinding, error) {
	b.requests = append(b.requests, request)
	binding, ok := b.bySource[request.SourceID]
	if !ok {
		return EnvironmentBinding{}, ErrEnvironmentBlocked
	}
	return binding, nil
}

func (b *sourcePlacementBinder) Release(context.Context, EnvironmentBinding) error { return nil }

// TestClaimFreezesTheEnvironmentReservation: a real dispatch calls the same
// resolver preview uses and carries the reserved environment and slot on the
// mapping row.
func TestClaimFreezesTheEnvironmentReservation(t *testing.T) {
	f := newFixture(t)
	binder := &fakeEnvironmentBinder{binding: EnvironmentBinding{
		EnvironmentID: "build-a", SlotID: "slot-1", RuleID: "a5",
	}}
	f.sched.cfg.EnvironmentBinder = binder
	e := f.claim(t, "task-1")
	if e.EnvironmentID != "build-a" || e.EnvironmentSlotID != "slot-1" {
		t.Fatalf("entry environment = %q/%q, want build-a/slot-1", e.EnvironmentID, e.EnvironmentSlotID)
	}
	if len(binder.requests) != 1 {
		t.Fatalf("bind calls = %d, want 1", len(binder.requests))
	}
	request := binder.requests[0]
	if request.AttemptID != registry.AttemptID("task-1", 1) || request.WorkspaceID != "ws-1" || request.AgentID != "agent-1" {
		t.Fatalf("bind request = %+v, want the claim's identity", request)
	}
	if len(f.jobs.createdJobs) != 1 {
		t.Fatalf("jobs created = %d, want 1", len(f.jobs.createdJobs))
	}
}

// TestTwoSourcesSharingAWorkspaceKeepTheirOwnEnvironment: a workspace id is
// only unique inside one server. Two claims naming the same workspace must
// each carry their own source into the resolver, select that source's
// environment, and never be refused as an environment block.
func TestTwoSourcesSharingAWorkspaceKeepTheirOwnEnvironment(t *testing.T) {
	f := newFixture(t)
	binder := &sourcePlacementBinder{bySource: map[string]EnvironmentBinding{
		"src-a": {EnvironmentID: "build-a", SlotID: "slot-a", RuleID: "rule-a"},
		"src-b": {EnvironmentID: "build-b", SlotID: "slot-b", RuleID: "rule-b"},
	}}
	f.sched.cfg.EnvironmentBinder = binder

	for i, source := range []string{"src-a", "src-b"} {
		taskID := fmt.Sprintf("task-%d", i+1)
		if err := f.sched.OnClaimForInSource(context.Background(), source, "ws-1", claimPayload(taskID)); err != nil {
			t.Fatalf("OnClaimForInSource(%s, ws-1): %v; a shared workspace id must not be refused as %v",
				source, err, ErrEnvironmentBlocked)
		}
		entry, ok := f.reg.Get(taskID, partitionKey(source, "ws-1"))
		if !ok {
			t.Fatalf("%s's claim is not registered under its own scope", source)
		}
		want := binder.bySource[source]
		if entry.EnvironmentID != want.EnvironmentID || entry.EnvironmentSlotID != want.SlotID {
			t.Fatalf("%s's entry = %q/%q, want %q/%q",
				source, entry.EnvironmentID, entry.EnvironmentSlotID, want.EnvironmentID, want.SlotID)
		}
	}
	if len(binder.requests) != 2 {
		t.Fatalf("bind calls = %d, want one per source", len(binder.requests))
	}
	if binder.requests[0].SourceID != "src-a" || binder.requests[1].SourceID != "src-b" {
		t.Fatalf("bind sources = %q/%q, want src-a/src-b",
			binder.requests[0].SourceID, binder.requests[1].SourceID)
	}
	if len(f.jobs.createdJobs) != 2 {
		t.Fatalf("jobs created = %d, want one per source", len(f.jobs.createdJobs))
	}
}

// TestSaturatedEnvironmentDefersTheClaim: a saturated placement creates no
// Job and no registry entry, so the server re-dispatches after the prepare
// lease lapses instead of the task failing on a transient capacity fact.
func TestSaturatedEnvironmentDefersTheClaim(t *testing.T) {
	f := newFixture(t)
	f.sched.cfg.EnvironmentBinder = &fakeEnvironmentBinder{err: ErrEnvironmentSaturated}
	if err := f.sched.OnClaim(context.Background(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if len(f.jobs.createdJobs) != 0 || len(f.jobs.createdSecrets) != 0 {
		t.Fatalf("jobs/secrets created = %d/%d, want none", len(f.jobs.createdJobs), len(f.jobs.createdSecrets))
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("a deferred claim left a registry entry")
	}
}

// TestBlockedEnvironmentRefusesWithoutAJob: a configuration block is visible
// and never falls back to another runtime.
func TestBlockedEnvironmentRefusesWithoutAJob(t *testing.T) {
	f := newFixture(t)
	f.sched.cfg.EnvironmentBinder = &fakeEnvironmentBinder{err: errors.New("no rule matches")}
	err := f.sched.OnClaim(context.Background(), claimPayload("task-1"))
	if !errors.Is(err, ErrEnvironmentBlocked) {
		t.Fatalf("OnClaim err = %v, want ErrEnvironmentBlocked", err)
	}
	if len(f.jobs.createdJobs) != 0 {
		t.Fatalf("jobs created = %d, want none", len(f.jobs.createdJobs))
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("a refused claim left a registry entry")
	}
}

// TestReleaseEnvironmentOnlyTouchesReservedEntries keeps the teardown paths
// honest: an entry without a reservation never calls the binder.
func TestReleaseEnvironmentOnlyTouchesReservedEntries(t *testing.T) {
	f := newFixture(t)
	binder := &fakeEnvironmentBinder{}
	f.sched.cfg.EnvironmentBinder = binder
	f.sched.releaseEnvironment(context.Background(), registry.TaskEntry{TaskID: "task-1"})
	if len(binder.released) != 0 {
		t.Fatalf("releases = %d, want none for an unreserved entry", len(binder.released))
	}
	f.sched.releaseEnvironment(context.Background(), registry.TaskEntry{
		TaskID: "task-1", EnvironmentID: "build-a", EnvironmentSlotID: "slot-1",
	})
	if len(binder.released) != 1 || binder.released[0].SlotID != "slot-1" {
		t.Fatalf("releases = %+v, want the entry's slot", binder.released)
	}
}

// TestFailedClaimWriteReleasesTheEnvironmentReservation: the slot is reserved
// before the mapping row exists. When the claim cannot proceed to that row,
// the reservation must go back instead of leaking until the slot TTL.
func TestFailedClaimWriteReleasesTheEnvironmentReservation(t *testing.T) {
	f := newFixture(t)
	binder := &fakeEnvironmentBinder{binding: EnvironmentBinding{
		EnvironmentID: "build-a", SlotID: "slot-1", RuleID: "a5",
	}}
	f.sched.cfg.EnvironmentBinder = binder
	f.sched.shared = &fakeCoordStore{counts: map[string]int64{"": int64(f.sched.cfg.MaxInflightJobs)}}
	err := f.sched.OnClaim(context.Background(), claimPayload("task-1"))
	if !errors.Is(err, ErrNoSlot) {
		t.Fatalf("OnClaim err = %v, want ErrNoSlot", err)
	}
	if len(binder.released) != 1 || binder.released[0].SlotID != "slot-1" {
		t.Fatalf("releases = %+v, want the reserved slot", binder.released)
	}
	if _, ok := f.reg.Get("task-1"); ok {
		t.Fatal("a failed claim write left a registry entry")
	}
}

// TestDrainTimeoutIsolatesTheEnvironmentInsteadOfReleasing: a drain bound that
// expires proves nothing about the Pod. OnJobTerminated must isolate the
// reservation (capacity stays held) instead of calling the no-Pod release, and
// only a real job_terminated fact may submit the Pod-close half.
func TestDrainTimeoutIsolatesTheEnvironmentInsteadOfReleasing(t *testing.T) {
	f := newFixture(t)
	binder := &fakeEnvironmentBinder{binding: EnvironmentBinding{
		EnvironmentID: "build-a", SlotID: "slot-1", RuleID: "a5",
	}}
	f.sched.cfg.EnvironmentBinder = binder
	f.claim(t, "task-1")
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultFailed, "", f.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}

	if err := f.sched.OnJobTerminated(context.Background(), "fm-task-1",
		JobTerminationReasonDrainTimeout, time.Time{}, 1); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if len(binder.released) != 0 {
		t.Fatalf("a drain timeout released the slot: %+v", binder.released)
	}
	if len(binder.podClosed) != 0 {
		t.Fatalf("a drain timeout claimed the Pod was closed: %+v", binder.podClosed)
	}
	if len(binder.isolated) != 1 || binder.isolated[0].SlotID != "slot-1" {
		t.Fatalf("isolates = %+v, want the entry's slot isolated", binder.isolated)
	}

	// A real Job-terminated fact carries the Pod-close half and no isolate.
	g := newFixture(t)
	terminated := &fakeEnvironmentBinder{binding: EnvironmentBinding{
		EnvironmentID: "build-b", SlotID: "slot-2", RuleID: "a6",
	}}
	g.sched.cfg.EnvironmentBinder = terminated
	g.claim(t, "task-2")
	if _, _, err := g.reg.MarkTerminal("task-2", registry.ResultCompleted, "", g.now); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if err := g.sched.OnJobTerminated(context.Background(), "fm-task-2",
		JobTerminationReasonJobTerminated, g.now, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if len(terminated.podClosed) != 1 || terminated.podClosed[0].SlotID != "slot-2" {
		t.Fatalf("pod-closed = %+v, want the Job-terminated fact to submit the Pod close", terminated.podClosed)
	}
}
