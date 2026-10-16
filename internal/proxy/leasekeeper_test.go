package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// fakeLeaseClient programs PrepareLease answers per task and records the
// runtime each renewal was addressed to.
type fakeLeaseClient struct {
	calls    []string
	runtimes []string
	ready    map[string]bool
	err      map[string]error
}

func (f *fakeLeaseClient) PrepareLease(_ context.Context, runtimeID, taskID string) (bool, error) {
	f.calls = append(f.calls, taskID)
	f.runtimes = append(f.runtimes, runtimeID)
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
		TaskID:        taskID,
		WorkspaceID:   "ws-1",
		JobName:       "fm-" + taskID,
		DaemonID:      "fm-" + taskID,
		RealRuntimeID: "rt-real",
		State:         state,
		ClaimedAt:     deadline.Add(-time.Minute),
		BootDeadline:  deadline,
		Payload:       json.RawMessage(`{"id":"` + taskID + `"}`),
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

// TestLeaseKeeperProjectsLeaseKeepUntil guards contract §3.1: every
// successful C4 renewal advances the entry's lease_keep_until by the server's
// 45s lease TTL — the field surface protocol-lease.md 场景 A reads as
// "lease_keep_until 持续推进". A refused or failed renewal moves nothing.
func TestLeaseKeeperProjectsLeaseKeepUntil(t *testing.T) {
	reg := registry.New(time.Now)
	client := &fakeLeaseClient{
		ready: map[string]bool{"t-renew": true, "t-refused": false},
		err:   map[string]error{"t-err": errors.New("5xx")},
	}
	for _, e := range []registry.TaskEntry{
		leaseEntry("t-renew", registry.StatePending, false, time.Now().Add(time.Hour)),
		leaseEntry("t-refused", registry.StatePending, false, time.Now().Add(time.Hour)),
		leaseEntry("t-err", registry.StatePending, false, time.Now().Add(time.Hour)),
	} {
		if err := reg.Put(e); err != nil {
			t.Fatalf("Put %s: %v", e.TaskID, err)
		}
	}

	k := NewLeaseKeeper(client, reg, &fakeLeaseHooks{}, 15*time.Second)
	var buf bytes.Buffer
	k.log = slog.New(slog.NewJSONHandler(&buf, nil)).With("component", "lease-keeper")
	first := time.Now().Add(time.Minute)
	k.now = func() time.Time { return first }
	k.round(t.Context())

	renewed, ok := reg.Get("t-renew")
	if !ok {
		t.Fatal("t-renew vanished")
	}
	if want := first.Add(scheduler.PrepareLeaseTTL); !renewed.LeaseKeepUntil.Equal(want) {
		t.Fatalf("lease_keep_until = %v, want %v", renewed.LeaseKeepUntil, want)
	}
	// The renewal leaves the Info line protocol-lease.md 场景 A reads: the
	// handler's default level (info) admits it, a debug record would not.
	records := logRecords(t, buf.Bytes(), "lease.renewed")
	if len(records) != 1 {
		t.Fatalf("lease.renewed records = %d, want 1 (one per renewal)", len(records))
	}
	if got := records[0]["level"]; got != "INFO" {
		t.Errorf("lease.renewed level = %v, want INFO (default-level observation surface)", got)
	}
	// observability.md §结构化日志: the field carries the same value
	// /foreman/tasks projects, so it is compared against the projection, not
	// against a re-derived format (a truncated/TZ-forced rendering would pass
	// such a self-consistent check but fail the design's wording).
	if got, want := records[0]["lease_keep_until"], projectedLeaseKeepUntil(t, renewed); got != want {
		t.Errorf("lease.renewed lease_keep_until = %v, projection = %v", got, want)
	}
	if got := records[0]["task_id"]; got != "t-renew" {
		t.Errorf("lease.renewed task_id = %v", got)
	}

	// The next tick renews again: the deadline advances by the interval.
	second := first.Add(15 * time.Second)
	k.now = func() time.Time { return second }
	k.round(t.Context())
	renewed, _ = reg.Get("t-renew")
	if want := second.Add(scheduler.PrepareLeaseTTL); !renewed.LeaseKeepUntil.Equal(want) {
		t.Fatalf("second lease_keep_until = %v, want %v", renewed.LeaseKeepUntil, want)
	}

	for _, id := range []string{"t-refused", "t-err"} {
		e, _ := reg.Get(id)
		if !e.LeaseKeepUntil.IsZero() {
			t.Errorf("%s lease_keep_until = %v, want untouched", id, e.LeaseKeepUntil)
		}
	}
	// A refused or failed renewal is not an observation of a renewing lease.
	for _, record := range logRecords(t, buf.Bytes(), "lease.renewed") {
		if got := record["task_id"]; got == "t-refused" || got == "t-err" {
			t.Errorf("lease.renewed recorded for non-renewed task %v", got)
		}
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

// projectedLeaseKeepUntil returns the entry's lease_keep_until exactly as
// /foreman/tasks renders it: TaskEntry's own JSON encoding, the same value
// the lease.renewed line must carry (observability.md §结构化日志).
func projectedLeaseKeepUntil(t *testing.T, e registry.TaskEntry) string {
	t.Helper()
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal entry: %v", err)
	}
	var projected struct {
		LeaseKeepUntil string `json:"lease_keep_until"`
	}
	if err := json.Unmarshal(raw, &projected); err != nil {
		t.Fatalf("unmarshal entry: %v", err)
	}
	return projected.LeaseKeepUntil
}

// logRecords returns every JSON log record whose msg equals event — the
// proxy counterpart of the scheduler tests' findLogRecord. Only records the
// handler's level admits appear, so it pins the event's level too.
func logRecords(t *testing.T, raw []byte, event string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record["msg"] == event {
			out = append(out, record)
		}
	}
	return out
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
