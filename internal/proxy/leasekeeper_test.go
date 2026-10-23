package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
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
	unconfirmed []string
	converge    []string
	vanished    []string
}

func (f *fakeLeaseHooks) OnDeliveryUnconfirmed(_ context.Context, taskID string) error {
	f.unconfirmed = append(f.unconfirmed, taskID)
	return nil
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

// TestLeaseKeeperProjectsLeaseKeepUntil guards §4.1: every
// successful C4 renewal advances the entry's lease_keep_until by the server's
// 45s lease TTL — the field surface the acceptance case reads as
// "lease_keep_until 持续推进". A refused or failed renewal moves nothing.
func TestLeaseKeeperProjectsLeaseKeepUntil(t *testing.T) {
	// A fixed clock makes the assertion discriminating: the keeper's default
	// clock is the registry's own, so the stamp is exactly this clock + TTL.
	// A second time source (time.Now) cannot satisfy the equality.
	clock := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	reg := registry.New(func() time.Time { return clock })
	client := &fakeLeaseClient{
		ready: map[string]bool{"t-renew": true, "t-refused": false},
		err:   map[string]error{"t-err": errors.New("5xx")},
	}
	for _, e := range []registry.TaskEntry{
		leaseEntry("t-renew", registry.StatePending, false, clock.Add(time.Hour)),
		leaseEntry("t-refused", registry.StatePending, false, clock.Add(time.Hour)),
		leaseEntry("t-err", registry.StatePending, false, clock.Add(time.Hour)),
	} {
		if err := reg.Put(e); err != nil {
			t.Fatalf("Put %s: %v", e.TaskID, err)
		}
	}

	// No WithLeaseClock: the default must be the registry's clock (§4.1), so
	// a keeper assembled without an explicit clock still writes one timeline.
	k := NewLeaseKeeper(client, reg, &fakeLeaseHooks{}, 15*time.Second)
	var buf bytes.Buffer
	k.log = slog.New(slog.NewJSONHandler(&buf, nil)).With("component", "lease-keeper")
	first := clock
	k.round(t.Context())

	renewed, ok := reg.Get("t-renew")
	if !ok {
		t.Fatal("t-renew vanished")
	}
	if want := first.Add(scheduler.PrepareLeaseTTL); !renewed.LeaseKeepUntil.Equal(want) {
		t.Fatalf("lease_keep_until = %v, want %v", renewed.LeaseKeepUntil, want)
	}
	// The renewal leaves the Info line the acceptance case reads: the
	// handler's default level (info) admits it, a debug record would not.
	records := logRecords(t, buf.Bytes(), "lease.renewed")
	if len(records) != 1 {
		t.Fatalf("lease.renewed records = %d, want 1 (one per renewal)", len(records))
	}
	if got := records[0]["level"]; got != "INFO" {
		t.Errorf("lease.renewed level = %v, want INFO (default-level observation surface)", got)
	}
	// §9.1: the field carries the same value
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
	clock = second
	k.round(t.Context())
	renewed, _ = reg.Get("t-renew")
	if want := second.Add(scheduler.PrepareLeaseTTL); !renewed.LeaseKeepUntil.Equal(want) {
		t.Fatalf("second lease_keep_until = %v, want %v", renewed.LeaseKeepUntil, want)
	}

	// WithLeaseClock remains an explicit override of the registry default.
	override := second.Add(time.Minute)
	k = NewLeaseKeeper(client, reg, &fakeLeaseHooks{}, 15*time.Second,
		WithLeaseClock(func() time.Time { return override }))
	k.round(t.Context())
	renewed, _ = reg.Get("t-renew")
	if want := override.Add(scheduler.PrepareLeaseTTL); !renewed.LeaseKeepUntil.Equal(want) {
		t.Fatalf("override lease_keep_until = %v, want %v", renewed.LeaseKeepUntil, want)
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
// the lease.renewed line must carry (§9.1).
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

// TestLeaseKeeperSkipsRebuiltEntries guards §4.3:
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

// TestLeaseKeeperBootBoundSplitsTheDeliveryAttribution guards ADR-032's bound
// model: the boot deadline is the entry's only terminal bound, and the
// confirmation window is not one — an expired window releases the delivery on
// the claim path, it never settles an entry here. The attribution splits on
// whether a delivery was ever committed: a delivered entry keeps the delivery
// face, a never-delivered one the boot face, whether or not a window is armed.
func TestLeaseKeeperBootBoundSplitsTheDeliveryAttribution(t *testing.T) {
	now := time.Now()
	reg := registry.New(time.Now)
	client := &fakeLeaseClient{ready: map[string]bool{
		"t-delivered": true, "t-delivered-early": true, "t-undelivered": true, "t-in-window": true,
	}}
	hooks := &fakeLeaseHooks{}

	// Delivered, window expired, boot bound passed: the delivery attribution.
	delivered := leaseEntry("t-delivered", registry.StateDelivering, false, now.Add(-time.Second))
	delivered.DeliveredAt = now.Add(-time.Minute)
	delivered.DeliveryDeadline = now.Add(-30 * time.Second)
	// Delivered with the window still open but the boot bound passed: the
	// window cannot shield the entry from the boot bound any more.
	early := leaseEntry("t-delivered-early", registry.StateDelivering, false, now.Add(-time.Second))
	early.DeliveredAt = now.Add(-time.Second)
	early.DeliveryDeadline = now.Add(time.Minute)
	// Never delivered: the boot face keeps its own attribution.
	undelivered := leaseEntry("t-undelivered", registry.StateDelivering, false, now.Add(-time.Second))
	// Delivered, window expired, boot bound still ahead: the window is not a
	// bound — nothing settles here, the claim path re-delivers.
	inWindow := leaseEntry("t-in-window", registry.StateDelivering, false, now.Add(time.Minute))
	inWindow.DeliveredAt = now.Add(-time.Minute)
	inWindow.DeliveryDeadline = now.Add(-30 * time.Second)
	for _, e := range []registry.TaskEntry{delivered, early, undelivered, inWindow} {
		if err := reg.Put(e); err != nil {
			t.Fatalf("Put %s: %v", e.TaskID, err)
		}
	}

	k := NewLeaseKeeper(client, reg, hooks, time.Minute)
	k.round(t.Context())

	wantUnconfirmed := []string{"t-delivered", "t-delivered-early"}
	slices.Sort(hooks.unconfirmed)
	if !slices.Equal(hooks.unconfirmed, wantUnconfirmed) {
		t.Errorf("unconfirmed = %v, want %v", hooks.unconfirmed, wantUnconfirmed)
	}
	if !slices.Equal(hooks.bootTimeout, []string{"t-undelivered"}) {
		t.Errorf("bootTimeout = %v, want only the never-delivered entry", hooks.bootTimeout)
	}
}

// A switched-off boot bound (FOREMAN_JOB_BOOT_TIMEOUT=0) leaves the
// entry's deadline unset while the entry is still this process's claim: the
// renewals must continue — otherwise the server's prepare lease lapses and it
// redispatches the task (§4.1) — and no settlement may fire on the
// renewal path: `now.After(zero)` is true for every clock, so an unguarded
// check would fail a healthy pre-start task, and the attribution split needs a
// bound to hold against (ADR-026: the bound's presence and the payload's
// availability are separate criteria).
func TestLeaseKeeperRenewsWithoutBootBound(t *testing.T) {
	reg := registry.New(time.Now)
	client := &fakeLeaseClient{ready: map[string]bool{
		"t-unbounded": true, "t-unbounded-delivered": true,
	}}
	hooks := &fakeLeaseHooks{}
	e := leaseEntry("t-unbounded", registry.StatePending, false, time.Time{})
	// A delivered entry with no bound either: the renewal path has nothing to
	// attribute against, the claim path re-delivers.
	delivered := leaseEntry("t-unbounded-delivered", registry.StateDelivering, false, time.Time{})
	delivered.DeliveredAt = time.Now().Add(-time.Minute)
	for _, entry := range []registry.TaskEntry{e, delivered} {
		if err := reg.Put(entry); err != nil {
			t.Fatalf("Put %s: %v", entry.TaskID, err)
		}
	}

	k := NewLeaseKeeper(client, reg, hooks, time.Minute)
	k.round(t.Context())

	if len(client.calls) != 2 {
		t.Fatalf("unbounded pre-start claims must still be renewed, calls = %v", client.calls)
	}
	if len(hooks.bootTimeout) != 0 {
		t.Errorf("no upper bound means no boot timeout: %v", hooks.bootTimeout)
	}
	if len(hooks.unconfirmed) != 0 {
		t.Errorf("no upper bound means no delivery attribution: %v", hooks.unconfirmed)
	}
}
