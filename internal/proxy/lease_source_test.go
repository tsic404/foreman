package proxy

import (
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// TestLeaseKeeperRenewsOnlyItsOwnSource pins R3b: a workspace id alone is not
// a partition when two servers expose it. A keeper bound to one source must
// not send C4 for another source's task — the request would carry that
// source's task/runtime identity, and a successful answer would advance a
// lease fact this domain does not own (02-contracts V2-C03/C06).
func TestLeaseKeeperRenewsOnlyItsOwnSource(t *testing.T) {
	clock := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	reg := registry.New(func() time.Time { return clock })
	client := &fakeLeaseClient{ready: map[string]bool{"task-a": true}}
	unconfirmed := func(taskID, sourceID, runtimeID string) registry.TaskEntry {
		e := leaseEntry(taskID, registry.StatePending, false, clock.Add(time.Hour))
		e.SourceID = sourceID
		e.RealRuntimeID = runtimeID
		if err := reg.Put(e); err != nil {
			t.Fatalf("Put %s: %v", taskID, err)
		}
		return e
	}
	own := unconfirmed("task-a", "src-a", "rt-a")
	other := unconfirmed("task-b", "src-b", "rt-b")

	keeper := NewLeaseKeeper(client, reg, &fakeLeaseHooks{}, 15*time.Second,
		WithLeaseWorkspace("ws-1"), WithLeaseSource("src-a"))
	keeper.round(t.Context())

	if len(client.calls) != 1 || client.calls[0] != "task-a" {
		t.Fatalf("C4 renewals = %v, want only this source's task-a", client.calls)
	}
	if len(client.runtimes) != 1 || client.runtimes[0] != "rt-a" {
		t.Fatalf("C4 runtimes = %v, want this source's own rt-a", client.runtimes)
	}
	ownAfter, ok := reg.Get("task-a", own.ScopeKey())
	if !ok {
		t.Fatal("task-a vanished")
	}
	if want := clock.Add(scheduler.PrepareLeaseTTL); !ownAfter.LeaseKeepUntil.Equal(want) {
		t.Fatalf("task-a lease_keep_until = %v, want %v", ownAfter.LeaseKeepUntil, want)
	}
	otherAfter, ok := reg.Get("task-b", other.ScopeKey())
	if !ok {
		t.Fatal("task-b vanished")
	}
	if !otherAfter.LeaseKeepUntil.IsZero() {
		t.Fatalf("task-b lease_keep_until = %v; another source's keeper must not extend it",
			otherAfter.LeaseKeepUntil)
	}
}
