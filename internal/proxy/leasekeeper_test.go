package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// fakeLeaseClient programs PrepareLease answers per task.
type fakeLeaseClient struct {
	calls []string
	ready map[string]bool
	err   map[string]error
}

func (f *fakeLeaseClient) PrepareLease(_ context.Context, taskID string) (bool, error) {
	f.calls = append(f.calls, taskID)
	if err := f.err[taskID]; err != nil {
		return false, err
	}
	return f.ready[taskID], nil
}

// fakeLeaseHooks records the settlements the keeper drives.
type fakeLeaseHooks struct {
	bootTimeout []string
	converge    []string
	vanished    []string
}

func (f *fakeLeaseHooks) OnBootTimeout(_ context.Context, taskID string) error {
	f.bootTimeout = append(f.bootTimeout, taskID)
	return nil
}

func (f *fakeLeaseHooks) ConvergeLeaseRefused(_ context.Context, taskID string) error {
	f.converge = append(f.converge, taskID)
	return nil
}

func (f *fakeLeaseHooks) OnTaskVanished(_ context.Context, taskID string) error {
	f.vanished = append(f.vanished, taskID)
	return nil
}

func leaseEntry(taskID string, state registry.State, started bool, deadline time.Time) registry.TaskEntry {
	e := registry.TaskEntry{
		TaskID:       taskID,
		WorkspaceID:  "ws-1",
		JobName:      "fm-" + taskID,
		DaemonID:     "fm-" + taskID,
		State:        state,
		ClaimedAt:    deadline.Add(-time.Minute),
		BootDeadline: deadline,
		Payload:      json.RawMessage(`{"id":"` + taskID + `"}`),
	}
	if started {
		e.StartedAt = deadline.Add(-time.Minute)
	}
	return e
}

func TestLeaseKeeperRound(t *testing.T) {
	now := time.Now()
	reg := registry.New(time.Now)
	client := &fakeLeaseClient{
		ready: map[string]bool{"t-pre": true, "t-late": true, "t-refused": false, "t-err": true},
		err:   map[string]error{"t-gone": scheduler.ErrTaskNotFound, "t-err": errors.New("5xx")},
	}
	hooks := &fakeLeaseHooks{}

	entries := []registry.TaskEntry{
		leaseEntry("t-pre", registry.StatePending, false, now.Add(time.Hour)),
		leaseEntry("t-late", registry.StateJobCreated, false, now.Add(-time.Second)),
		leaseEntry("t-refused", registry.StateDelivering, false, now.Add(time.Hour)),
		leaseEntry("t-gone", registry.StateDaemonRegistered, false, now.Add(time.Hour)),
		leaseEntry("t-err", registry.StatePending, false, now.Add(time.Hour)),
		leaseEntry("t-running", registry.StateRunning, true, now.Add(time.Hour)),
		leaseEntry("t-term", registry.StateTerminal, false, now.Add(-time.Hour)),
	}
	for _, e := range entries {
		if err := reg.Put(e); err != nil {
			t.Fatalf("Put %s: %v", e.TaskID, err)
		}
	}

	k := NewLeaseKeeper(client, reg, hooks, time.Minute)
	k.round(t.Context())

	// The keeper leases every pre-start entry and skips running/terminal.
	wantCalls := map[string]bool{"t-pre": true, "t-late": true, "t-refused": true, "t-gone": true, "t-err": true}
	for _, got := range client.calls {
		if !wantCalls[got] {
			t.Errorf("unexpected lease call for %s (running/terminal must be skipped)", got)
		}
		delete(wantCalls, got)
	}
	if len(wantCalls) != 0 {
		t.Errorf("missing lease calls: %v", wantCalls)
	}

	// Boot deadline past with the lease still ready → fail the task.
	if len(hooks.bootTimeout) != 1 || hooks.bootTimeout[0] != "t-late" {
		t.Errorf("bootTimeout = %v", hooks.bootTimeout)
	}
	// ready=false (C4 400) → C13 convergence.
	if len(hooks.converge) != 1 || hooks.converge[0] != "t-refused" {
		t.Errorf("converge = %v", hooks.converge)
	}
	// Task deleted server-side → vanish settlement.
	if len(hooks.vanished) != 1 || hooks.vanished[0] != "t-gone" {
		t.Errorf("vanished = %v", hooks.vanished)
	}
}

func TestLeaseKeeperSkipsTerminalAndStarted(t *testing.T) {
	reg := registry.New(time.Now)
	client := &fakeLeaseClient{ready: map[string]bool{}}
	now := time.Now()
	for _, e := range []registry.TaskEntry{
		leaseEntry("t-running", registry.StateRunning, true, now.Add(time.Hour)),
		leaseEntry("t-term", registry.StateTerminal, false, now.Add(-time.Hour)),
	} {
		if err := reg.Put(e); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	k := NewLeaseKeeper(client, reg, &fakeLeaseHooks{}, time.Minute)
	k.round(t.Context())
	if len(client.calls) != 0 {
		t.Errorf("no lease calls expected for running/terminal entries, got %v", client.calls)
	}
}

// TestLeaseKeeperSkipsRebuiltEntries guards failure-handling scenario #2:
// an entry rebuilt from a K8s Job after a restart has no payload and a zero
// boot deadline — leasing it would block server redispatch, and the zero
// deadline would fake a boot timeout on a healthy pre-start task.
func TestLeaseKeeperSkipsRebuiltEntries(t *testing.T) {
	reg := registry.New(time.Now)
	client := &fakeLeaseClient{ready: map[string]bool{"t-rebuilt": true}}
	hooks := &fakeLeaseHooks{}
	rebuilt := registry.TaskEntry{
		TaskID:      "t-rebuilt",
		WorkspaceID: "ws-1",
		JobName:     "fm-t-rebuilt",
		DaemonID:    "fm-t-rebuilt",
		State:       registry.StateJobCreated,
		ClaimedAt:   time.Now().Add(-time.Hour),
		// BootDeadline zero, Payload empty — as TaskEntryFromJob rebuilds it.
	}
	if err := reg.Put(rebuilt); err != nil {
		t.Fatalf("Put: %v", err)
	}

	k := NewLeaseKeeper(client, reg, hooks, time.Minute)
	k.round(t.Context())
	if len(client.calls) != 0 {
		t.Errorf("rebuilt entries must not be leased, got %v", client.calls)
	}
	if len(hooks.bootTimeout) != 0 {
		t.Errorf("a zero boot deadline must not trigger OnBootTimeout: %v", hooks.bootTimeout)
	}
}
