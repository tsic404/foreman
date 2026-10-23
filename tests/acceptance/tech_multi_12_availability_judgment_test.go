// TC-tech-multi-12 / AC-106: only a storage-side failure is evidence that the
// shared state is unavailable. A caller's cancellation (an abandoned request,
// the shutdown window) neither opens nor closes an outage, emits no
// availability transition, and leaves both readers — the issuance gate and the
// last-instance deregistration gate — on the same verdict (§10 共享状态不可用
// 的判定).
package acceptance

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestTechMulti12CancellationKeepsTheAvailabilityVerdict covers the three
// event/interval criteria: a cancellation on a healthy store opens nothing, and
// a cancellation during a real outage closes nothing.
func TestTechMulti12CancellationKeepsTheAvailabilityVerdict(t *testing.T) {
	grace := 500 * time.Millisecond
	tw := newTwoInstances(t, twoInstanceOptions{
		grace:     grace,
		leaderCfg: smallLeaderCfg(2 * time.Second),
	})
	tw.declareAll(t)
	tw.a.startLeader(t)
	stopClaim := startConcurrency01ClaimLoop(t, tw.a)
	defer stopClaim()

	// One delivered task gives the issuance gate a token to adjudicate.
	tw.a.upstream.enqueueTask("task-cancel")
	tw.waitForTaskJob(t, "task-cancel", 10*time.Second)
	token := tw.token(t, tw.a, "task-cancel")
	tw.registerVia(t, tw.b, "task-cancel", token)
	if got := tw.claimVia(t, tw.b, "task-cancel", token); len(got) != 1 {
		t.Fatalf("live delivery = %d tasks, want 1", len(got))
	}
	tw.startVia(t, tw.b, "task-cancel", token)
	tw.a.upstream.setTaskStatus("task-cancel", "running")
	if code := tw.statusVia(t, tw.b, "task-cancel", token); code != http.StatusOK {
		t.Fatalf("healthy status = %d, want 200", code)
	}

	// The fixture's own startup may already have recorded a transient script
	// error; the cancellation is judged by the transitions it adds, not by the
	// absolute history.
	baseline := tw.stateEventLines()

	// A caller that has already cancelled: the store reports the cancellation,
	// which says nothing about the store itself.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tw.coord.DropQueue(cancelled, "ws-1", "task-cancel"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled operation = %v, want context.Canceled", err)
	}
	if !tw.coord.Available() || tw.coord.DownFor() != 0 {
		t.Fatalf("a cancellation moved the outage face: available=%v downFor=%s",
			tw.coord.Available(), tw.coord.DownFor())
	}
	if got := tw.stateEventLines(); len(got) != len(baseline) {
		t.Fatalf("a cancellation added availability events: %v -> %v", baseline, got)
	}
	// Had the cancellation opened an interval, this request would now be past
	// the grace window and fail closed.
	time.Sleep(grace + 150*time.Millisecond)
	if code := tw.statusVia(t, tw.b, "task-cancel", token); code != http.StatusOK {
		t.Fatalf("status after a cancellation = %d, want 200 (no outage was opened)", code)
	}

	// A real storage failure opens the interval.
	tw.mr.SetError("shared state outage")
	if err := tw.coord.DropQueue(context.Background(), "ws-1", "task-cancel"); err == nil {
		t.Fatal("the storage failure was not reported")
	}
	if tw.coord.Available() {
		t.Fatal("a storage failure did not open the outage")
	}
	if lines := tw.stateEventLines(); len(lines) != len(baseline)+1 || lines[len(lines)-1] != "unavailable" {
		t.Fatalf("events after the failure = %v, want the baseline plus one unavailable", lines)
	}

	// The cancellation that follows must not close the open interval.
	if err := tw.coord.DropQueue(cancelled, "ws-1", "task-cancel"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled operation during the outage = %v, want context.Canceled", err)
	}
	if tw.coord.Available() {
		t.Fatal("a cancellation closed the open outage")
	}
	if lines := tw.stateEventLines(); len(lines) != len(baseline)+1 {
		t.Fatalf("a cancellation during the outage moved the events: %v -> %v", baseline, lines)
	}
	// Past the grace window the open interval still decides: fail closed.
	time.Sleep(grace + 200*time.Millisecond)
	if code := tw.statusVia(t, tw.b, "task-cancel", token); code != http.StatusUnauthorized && code != http.StatusServiceUnavailable {
		t.Fatalf("status past the grace window during the outage = %d, want 401/503", code)
	}

	// A real success closes the interval exactly once.
	tw.mr.SetError("")
	if err := tw.coord.DropQueue(context.Background(), "ws-1", "task-cancel"); err != nil {
		t.Fatalf("store recovery: %v", err)
	}
	if !tw.coord.Available() {
		t.Fatal("a store success did not close the outage")
	}
	lines := tw.stateEventLines()
	if len(lines) != len(baseline)+2 || lines[len(lines)-2] != "unavailable" || !strings.HasPrefix(lines[len(lines)-1], "recovered") {
		t.Fatalf("events after recovery = %v, want the baseline plus unavailable then one recovered", lines)
	}
}

// TestTechMulti12ShutdownCancellationKeepsTheLastInstanceVerdict covers the
// fourth criterion at this layer: the shutdown path cancels its loops, one
// in-flight store call ends in context.Canceled, and the last-instance verdict
// — the gate that decides whether C14 is sent — must still hold (§4.6 优雅关闭).
// The C14 emission belongs to the shutdown path in cmd/foreman, which the
// acceptance harness does not run.
func TestTechMulti12ShutdownCancellationKeepsTheLastInstanceVerdict(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{grace: time.Second})
	tw.declareAll(t)
	tw.a.startLeader(t)

	// instance-a is the only live instance; its liveness row is the evidence
	// the gate reads.
	if err := tw.coord.HeartbeatInstance(context.Background(), "instance-a", time.Minute); err != nil {
		t.Fatalf("HeartbeatInstance: %v", err)
	}
	shutdown, cancel := context.WithCancel(context.Background())
	cancel()
	baseline := tw.stateEventLines()
	if err := tw.coord.HeartbeatInstance(shutdown, "instance-a", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("heartbeat on the cancelled shutdown context = %v, want context.Canceled", err)
	}
	if !tw.coord.Available() {
		t.Fatal("a shutdown cancellation was read as a store outage")
	}
	if lines := tw.stateEventLines(); len(lines) != len(baseline) {
		t.Fatalf("a shutdown cancellation emitted availability events: %v -> %v", baseline, lines)
	}

	last, err := tw.a.currentLeader().LastInstance(context.Background())
	if err != nil || !last {
		t.Fatalf("last-instance verdict after a shutdown cancellation = %v, %v; want true, nil", last, err)
	}
}
