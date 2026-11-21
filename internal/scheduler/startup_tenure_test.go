package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	batchv1 "k8s.io/api/batch/v1"

	"github.com/tsic404/foreman/internal/coordination"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/state"
	"github.com/tsic404/foreman/internal/tenure"
)

// startupTenureFace is the shared store the startup takeover cases run
// against: the registry's state face and the coordination face are the same
// store, exactly as the deployment wires them.
type startupTenureFace struct {
	store state.Store
	coord state.CoordStore
	mr    *miniredis.Miniredis
}

func startupTenureStore(t *testing.T) startupTenureFace {
	t.Helper()
	mr := miniredis.RunT(t)
	st, err := state.New(state.Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    time.Hour,
	})
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	coord, ok := st.(state.CoordStore)
	if !ok {
		t.Fatal("the store does not expose the coordination face")
	}
	return startupTenureFace{store: st, coord: coord, mr: mr}
}

func startupLeaderConfig() coordination.Config {
	return coordination.Config{
		LeaseTTL:       300 * time.Millisecond,
		TakeoverWindow: 100 * time.Millisecond,
		InstanceTTL:    time.Second,
		TaskLockTTL:    time.Second,
	}
}

// newStartupTenureFixture builds a fixture whose scheduler carries the
// stage-two coordination face and a real leader gate, so Start's tenure
// binding is exercised against the store's lease, not a stubbed boolean.
func newStartupTenureFixture(t *testing.T, face startupTenureFace, instanceID string, gate func() bool) *fixture {
	t.Helper()
	f := newFixtureUnder(t,
		[]registry.Option{registry.WithStateStore(face.store)},
		nil,
		WithSharedStore(face.coord, instanceID, time.Second),
		WithLeaderGate(gate),
	)
	t.Cleanup(func() { _ = f.reg.Close(context.Background()) })
	return f
}

func runStartupLeader(t *testing.T, face startupTenureFace, instanceID string) (*coordination.Leader, func()) {
	t.Helper()
	leader := coordination.New(face.coord, instanceID, startupLeaderConfig())
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = leader.Run(ctx)
	}()
	if !leader.WaitHeld(ctx, 5*time.Second) {
		stop()
		<-done
		t.Fatalf("%s never acquired the control-plane lease", instanceID)
	}
	return leader, func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not stop its lease loop", instanceID)
		}
	}
}

func waitForStartupLoss(t *testing.T, leader *coordination.Leader) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for leader.Held() {
		if time.Now().After(deadline) {
			t.Fatal("the holder never lost the control-plane lease")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// clearStartupLease hands the control-plane lease to the next instance. A
// renewal or release already in flight lands after the first delete and can
// recreate the key for the old owner, so keep deleting until the holder reads
// empty before the successor starts.
func clearStartupLease(t *testing.T, face startupTenureFace) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	for {
		face.mr.Del("foreman:leader")
		holder, err := face.coord.LeadershipHolder(ctx)
		if err != nil {
			t.Fatalf("LeadershipHolder: %v", err)
		}
		if holder == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the control-plane lease stayed with %q", holder)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestStartupRebuildStopsWhenTheTenureEnds pins R9's rebuild half: the
// startup rebuild writes registry rows and shared rows, so a holder that loses
// the lease while its Job list is in flight must stop there — no rebuilt row,
// no slot reset, no converge — and the next holder must be able to finish the
// same work.
func TestStartupRebuildStopsWhenTheTenureEnds(t *testing.T) {
	ctx := context.Background()
	face := startupTenureStore(t)
	coord, mr := face.coord, face.mr

	oldLeader, stopOld := runStartupLeader(t, face, "instance-old")

	// A pending reservation of the previous holder: the startup slot reset
	// clears it, so its survival is the observable "the reset never ran".
	if _, ok, err := coord.ReserveSlot(ctx, "", 0); err != nil || !ok {
		t.Fatalf("ReserveSlot: ok=%v err=%v", ok, err)
	}

	old := newStartupTenureFixture(t, face, "instance-old", oldLeader.Held)
	old.jobs.jobs = []batchv1.Job{jobObject("task-1")}
	old.server.status = "running"
	var statusCalls atomic.Int64
	old.server.onStatus = func() { statusCalls.Add(1) }

	entered := make(chan struct{})
	var enteredOnce sync.Once
	cancelled := make(chan struct{})
	var cancelledOnce sync.Once
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	old.jobs.onListJobsCtx = func(ctx context.Context) {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-ctx.Done():
			cancelledOnce.Do(func() { close(cancelled) })
		case <-release:
		}
	}

	done := make(chan error, 1)
	go func() { done <- old.sched.Start(ctx) }()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the startup rebuild never reached the Job list")
	}

	// Break the store the way a failed renewal does: the holder drops its
	// hold, and this instance must stop writing for the next holder.
	mr.SetError("shared state outage")
	waitForStartupLoss(t, oldLeader)
	// A start bound to the tenure cancels the in-flight phase here; an unbound
	// start never does, and the release below drives it into the writes the
	// assertions forbid. Bounded so the reverted path fails on the effect.
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
	}
	stopOld()
	mr.SetError("")
	unblock()

	var startErr error
	select {
	case startErr = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after the rebuild was released")
	}

	if e, ok := old.reg.Get("task-1"); ok {
		t.Fatalf("the old holder rebuilt the index after losing the lease: %+v", e)
	}
	if rows, err := coord.ListEntries(ctx); err != nil {
		t.Fatalf("ListEntries: %v", err)
	} else if len(rows) != 0 {
		t.Fatalf("the old holder wrote %d shared row(s) after losing the lease, want none", len(rows))
	}
	if _, reservations, err := coord.SlotSnapshot(ctx, ""); err != nil {
		t.Fatalf("SlotSnapshot: %v", err)
	} else if reservations != 1 {
		t.Fatalf("the startup slot reset cleared the previous holder's reservation after the tenure ended")
	}
	if got := len(old.jobs.deletedJobs); got != 0 {
		t.Fatalf("the old holder deleted %d Job(s) after losing the lease, want none", got)
	}
	if got := statusCalls.Load(); got != 0 {
		t.Fatalf("the old holder converged %d task(s) after losing the lease, want none", got)
	}
	if !errors.Is(startErr, tenure.ErrLost) {
		t.Fatalf("Start = %v, want tenure.ErrLost", startErr)
	}

	// New holder: the same store, a fresh instance and registry. Its start
	// must finish the rebuild, the slot reset and the converge the old holder
	// was stopped from doing.
	clearStartupLease(t, face)
	newLeader, stopNew := runStartupLeader(t, face, "instance-new")
	defer stopNew()

	next := newStartupTenureFixture(t, face, "instance-new", newLeader.Held)
	next.jobs.jobs = []batchv1.Job{jobObject("task-1")}
	next.server.status = "running"
	if err := next.sched.Start(ctx); err != nil {
		t.Fatalf("the new holder's Start: %v", err)
	}
	e, ok := next.reg.Get("task-1")
	if !ok || e.State != registry.StateJobCreated || e.StartedAt.IsZero() {
		t.Fatalf("the new holder did not rebuild and converge the source: entry=%+v ok=%v", e, ok)
	}
	if rows, err := coord.ListEntries(ctx); err != nil {
		t.Fatalf("ListEntries: %v", err)
	} else if len(rows) != 1 || rows[0].TaskID != "task-1" {
		t.Fatalf("shared rows = %+v, want the rebuilt task", rows)
	}
	if _, reservations, err := coord.SlotSnapshot(ctx, ""); err != nil {
		t.Fatalf("SlotSnapshot: %v", err)
	} else if reservations != 0 {
		t.Fatalf("the new holder did not clear the dead holder's reservation: %d live", reservations)
	}
}

// TestStartupConvergeStopsWhenTheTenureEnds pins R9's converge half: the
// initial reconcile settles tasks and deletes Jobs, so a holder that loses the
// lease while a C13 read is in flight must not finish that entry — and the
// next holder must settle it instead.
func TestStartupConvergeStopsWhenTheTenureEnds(t *testing.T) {
	ctx := context.Background()
	face := startupTenureStore(t)
	mr := face.mr

	oldLeader, stopOld := runStartupLeader(t, face, "instance-old")

	old := newStartupTenureFixture(t, face, "instance-old", oldLeader.Held)
	old.jobs.jobs = []batchv1.Job{jobObject("task-1")}
	old.server.status = "failed"
	entered := make(chan struct{})
	var enteredOnce sync.Once
	old.server.onStatus = func() { enteredOnce.Do(func() { close(entered) }) }
	cancelled := make(chan struct{})
	var cancelledOnce sync.Once
	old.server.onStatusCancelled = func() { cancelledOnce.Do(func() { close(cancelled) }) }
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	old.server.statusWait = release

	done := make(chan error, 1)
	go func() { done <- old.sched.Start(ctx) }()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the startup converge never reached the C13 read")
	}

	mr.SetError("shared state outage")
	waitForStartupLoss(t, oldLeader)
	// Bound to the tenure, the C13 read is cancelled here; unbound, it only
	// ends at the release below, which is what drives the reverted path into
	// the settle these assertions forbid.
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
	}
	stopOld()
	mr.SetError("")
	unblock()

	var startErr error
	select {
	case startErr = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after the converge read was released")
	}

	if e, ok := old.reg.Get("task-1"); !ok || e.IsTerminal() {
		t.Fatalf("the old holder settled the task after losing the lease: %+v ok=%v", e, ok)
	}
	if got := len(old.jobs.deletedJobs); got != 0 {
		t.Fatalf("the old holder deleted %d Job(s) after losing the lease, want none", got)
	}
	if !errors.Is(startErr, tenure.ErrLost) {
		t.Fatalf("Start = %v, want tenure.ErrLost", startErr)
	}

	// The new holder finishes the convergence: the terminal task is settled
	// and its Job is removed.
	clearStartupLease(t, face)
	newLeader, stopNew := runStartupLeader(t, face, "instance-new")
	defer stopNew()

	next := newStartupTenureFixture(t, face, "instance-new", newLeader.Held)
	next.jobs.jobs = []batchv1.Job{jobObject("task-1")}
	next.server.status = "failed"
	if err := next.sched.Start(ctx); err != nil {
		t.Fatalf("the new holder's Start: %v", err)
	}
	e, ok := next.reg.Get("task-1")
	if !ok || !e.IsTerminal() {
		t.Fatalf("the new holder did not settle the task: %+v ok=%v", e, ok)
	}
	if !next.jobs.jobDeleted("fm-task-1") {
		t.Fatalf("the new holder did not delete the settled Job: %v", next.jobs.deletedJobs)
	}
}
