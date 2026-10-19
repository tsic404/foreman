package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// newDrainRegistry builds a registry whose drain upper bound is `timeout`
// (FOREMAN_JOB_DRAIN_TIMEOUT); zero makes the bound fall due at once.
func newDrainRegistry(t *testing.T, timeout time.Duration, entries ...registry.TaskEntry) *registry.Registry {
	t.Helper()
	reg := registry.New(time.Now, registry.WithDrainTimeout(timeout))
	for _, e := range entries {
		if err := reg.Put(e); err != nil {
			t.Fatalf("registry.Put(%s): %v", e.TaskID, err)
		}
	}
	return reg
}

// drainingEntry is a settled entry whose credential is still valid: the
// verdict is written (done index + drain bound) by the registry itself, so
// the test exercises the same transition production uses.
func drainingEntry(t *testing.T, reg *registry.Registry, taskID string) registry.TaskEntry {
	t.Helper()
	e, _, err := reg.MarkTerminal(taskID, registry.ResultFailed, "", time.Now())
	if err != nil {
		t.Fatalf("MarkTerminal(%s): %v", taskID, err)
	}
	if !e.IsDraining() {
		t.Fatalf("entry %s is not draining after its verdict: %+v", taskID, e)
	}
	return e
}

// The Job's termination is the primary fact: when every pod of the instance
// has terminated, the reclaim is driven by that fact and carries the kubelet
// termination moment (ADR-017).
func TestReclaimIfDueUsesTheJobTerminationFact(t *testing.T) {
	e := testEntry("t1")
	reg := newDrainRegistry(t, time.Hour, e)
	drainingEntry(t, reg, "t1")
	finished := time.Now().Add(-2 * time.Minute)
	set := &fakeSettler{terminated: true, lastTerminatedAt: finished, alivePods: 0}

	rec := newTestReconciler(t, reg, &fakeObjects{}, &fakeStatus{status: "running"}, set)
	if err := rec.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	reclaims, _ := set.drains()
	if reclaims != 1 {
		t.Fatalf("reclaims = %d, want the drain closed on the Job's termination", reclaims)
	}
	got := set.reclaims[0]
	if got.reason != scheduler.JobTerminationReasonJobTerminated {
		t.Fatalf("reason = %q, want %q", got.reason, scheduler.JobTerminationReasonJobTerminated)
	}
	if !got.at.Equal(finished) {
		t.Fatalf("pod terminated at = %s, want the kubelet fact %s", got.at, finished)
	}
	if got.orphans != 0 {
		t.Fatalf("orphan pods = %d, want 0", got.orphans)
	}
}

// The upper bound closes the window unconditionally when the Job has not
// terminated: the reclaim claims no termination fact (zero value) and reports
// the pods still alive as orphans (ADR-017 兜底上界).
func TestReclaimIfDueFallsBackToTheUpperBound(t *testing.T) {
	e := testEntry("t1")
	reg := newDrainRegistry(t, 0, e) // bound already due
	drainingEntry(t, reg, "t1")
	set := &fakeSettler{terminated: false, alivePods: 2}

	rec := newTestReconciler(t, reg, &fakeObjects{}, &fakeStatus{status: "running"}, set)
	if err := rec.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	reclaims, _ := set.drains()
	if reclaims != 1 {
		t.Fatalf("reclaims = %d, want the bound to close the window", reclaims)
	}
	got := set.reclaims[0]
	if got.reason != scheduler.JobTerminationReasonDrainTimeout {
		t.Fatalf("reason = %q, want %q", got.reason, scheduler.JobTerminationReasonDrainTimeout)
	}
	if !got.at.IsZero() {
		t.Fatalf("pod terminated at = %s, want the zero value on the bound path", got.at)
	}
	if got.orphans != 2 {
		t.Fatalf("orphan pods = %d, want the live pods (2)", got.orphans)
	}
}

// Inside the window with a live pod there is nothing to close: the credential
// stays valid (ADR-017 — the drain window is exactly this state).
func TestReclaimIfDueWaitsInsideTheWindow(t *testing.T) {
	e := testEntry("t1")
	reg := newDrainRegistry(t, time.Hour, e)
	drainingEntry(t, reg, "t1")
	set := &fakeSettler{terminated: false, alivePods: 1}

	rec := newTestReconciler(t, reg, &fakeObjects{}, &fakeStatus{status: "running"}, set)
	if err := rec.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if reclaims, _ := set.drains(); reclaims != 0 {
		t.Fatalf("reclaims = %d, want none while the Job has a live pod", reclaims)
	}
}

// A failed pod read is not a verdict: the reclaim must not read the missing
// evidence as "the Job terminated" (that would reclaim before the pod is
// gone, the exact regression ADR-017 exists to remove).
func TestReclaimIfDueDoesNotTreatEvidenceErrorsAsFacts(t *testing.T) {
	e := testEntry("t1")
	reg := newDrainRegistry(t, time.Hour, e)
	drainingEntry(t, reg, "t1")
	set := &fakeSettler{terminatedErr: errors.New("apiserver unavailable")}

	rec := newTestReconciler(t, reg, &fakeObjects{}, &fakeStatus{status: "running"}, set)
	if err := rec.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if reclaims, _ := set.drains(); reclaims != 0 {
		t.Fatalf("reclaims = %d, want none without a termination fact", reclaims)
	}
	if _, ok := reg.Get("t1"); !ok {
		t.Fatal("the credential must survive an unreadable evidence surface")
	}
}

// The upper bound still closes the window when the evidence surface is down
// (an unreachable node, a lost informer event): the reclaim falls to
// drain_timeout instead of waiting forever.
func TestReclaimIfDueBoundClosesWhenEvidenceIsDown(t *testing.T) {
	e := testEntry("t1")
	reg := newDrainRegistry(t, 0, e)
	drainingEntry(t, reg, "t1")
	set := &fakeSettler{terminatedErr: errors.New("apiserver unavailable")}

	rec := newTestReconciler(t, reg, &fakeObjects{}, &fakeStatus{status: "running"}, set)
	if err := rec.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	reclaims, _ := set.drains()
	if reclaims != 1 || set.reclaims[0].reason != scheduler.JobTerminationReasonDrainTimeout {
		t.Fatalf("reclaims = %+v, want one drain_timeout", set.reclaims)
	}
}

// A drain bound that has already passed is not a wait source: arming the loop
// with it would make every round a zero-delay retry against a failing K8s API
// (rule 7 retries a reclaim without bound). A bound still ahead is.
func TestNextDeadlineIgnoresPassedDrainBounds(t *testing.T) {
	e := testEntry("t1")
	e.JobPhase = registry.JobPhaseDraining
	e.DrainDeadline = time.Now().Add(-time.Minute)
	reg := newDrainRegistry(t, time.Hour, e)

	rec := newTestReconciler(t, reg, &fakeObjects{}, &fakeStatus{status: "running"}, &fakeSettler{})
	if next, ok := rec.nextDeadline(); ok {
		t.Fatalf("nextDeadline = %v, %v; want no wait source for a passed bound", next, ok)
	}

	ahead := time.Now().Add(time.Minute)
	e.DrainDeadline = ahead
	if err := reg.Put(e); err != nil {
		t.Fatalf("Put: %v", err)
	}
	next, ok := rec.nextDeadline()
	if !ok || !next.Equal(ahead) {
		t.Fatalf("nextDeadline = %v, %v; want the pending bound %v", next, ok, ahead)
	}
}
