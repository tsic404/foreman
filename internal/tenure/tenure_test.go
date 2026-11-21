package tenure

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestBoundCancelsWhenTheTenureEnds pins the shared fence: a bound effect is
// cancelled the moment the gate closes and reports the loss.
func TestBoundCancelsWhenTheTenureEnds(t *testing.T) {
	var held atomic.Bool
	held.Store(true)
	gate := held.Load
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bound, stop, lost := Bound(ctx, gate)
	defer stop()
	if !Holds(gate) {
		t.Fatal("the gate started closed")
	}
	if err := Check(gate); err != nil {
		t.Fatalf("Check while held = %v, want nil", err)
	}

	held.Store(false)
	select {
	case <-bound.Done():
	case <-time.After(3 * PollInterval):
		t.Fatal("the bound context was not cancelled after the tenure ended")
	}
	if !lost() {
		t.Fatal("Bound did not report the tenure loss")
	}
	if !errors.Is(Check(gate), ErrLost) {
		t.Fatalf("Check after loss = %v, want ErrLost", Check(gate))
	}
}

// TestNilGateIsTheSingleInstanceForm: no coordination face means no tenure to
// wait on, and every check is open.
func TestNilGateIsTheSingleInstanceForm(t *testing.T) {
	bound, stop, lost := Bound(context.Background(), nil)
	defer stop()
	if bound.Err() != nil || lost() {
		t.Fatal("a nil gate produced a lost tenure")
	}
	if err := Check(nil); err != nil {
		t.Fatalf("Check(nil) = %v, want nil", err)
	}
	if !Await(context.Background(), nil) {
		t.Fatal("Await(nil) parked a single-instance process")
	}
}

// TestAwaitParksUntilTheGateOpens: a follower waits instead of exiting, then
// proceeds when it becomes the holder.
func TestAwaitParksUntilTheGateOpens(t *testing.T) {
	var held atomic.Bool
	gate := held.Load
	done := make(chan bool, 1)
	go func() { done <- Await(context.Background(), gate) }()
	select {
	case <-done:
		t.Fatal("Await returned while the gate was closed")
	case <-time.After(2 * PollInterval):
	}
	held.Store(true)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("Await reported ctx cancellation")
		}
	case <-time.After(3 * PollInterval):
		t.Fatal("Await did not proceed after the gate opened")
	}
}

// TestWatchReleasesOpenResourcesWhenTheTenureEnds pins the long-lived form: a
// holder's open resources are torn down by the watcher, not by its next loop
// iteration, and a stopped watcher never releases a successor's resources.
func TestWatchReleasesOpenResourcesWhenTheTenureEnds(t *testing.T) {
	var held atomic.Bool
	held.Store(true)
	var released atomic.Int32
	stop := Watch(context.Background(), held.Load, func() { released.Add(1) })
	if released.Load() != 0 {
		t.Fatal("the watcher released a live tenure")
	}
	held.Store(false)
	deadline := time.Now().Add(3 * PollInterval)
	for released.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the watcher did not release the open resources after the tenure ended")
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	held.Store(true)
	held.Store(false)
	time.Sleep(2 * PollInterval)
	if got := released.Load(); got != 1 {
		t.Fatalf("release calls = %d, want exactly 1 (a stopped watcher released again)", got)
	}
}
