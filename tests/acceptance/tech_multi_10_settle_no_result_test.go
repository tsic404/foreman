// TC-tech-multi-10 (AC-73): the retention face of a task the server no longer
// holds. A terminal report whose upstream answer is 404 settles with no
// result (§4.4 任务已不存在); the done key that settlement writes must hold
// the terminal projection only (§4.3) — no delivery body, no decided terminal
// body, no token digest. The live row that the settlement started from still
// carries all three faces, so the fixture cannot pass because the report
// never had a body to strip.
package acceptance

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// multi10Forwards counts the terminal forwards the upstream stub observed for
// taskID: the in-flight marker the positive-control read waits on.
func multi10Forwards(h *harness, taskID string) int {
	path := "/api/daemon/tasks/" + taskID + "/complete"
	n := 0
	for _, req := range h.upstream.all() {
		if req.Path == path {
			n++
		}
	}
	return n
}

// TestMulti10SettleNoResultRetainsProjectionOnly drives a terminal report into
// the 404 settlement and judges the done key it writes by reading the store
// directly (GetSettled does no stripping) and by restarting over the same
// store. The verdict is written before the forward, so the row read while the
// report is parked upstream is the pre-settlement row: it must still hold the
// delivery body, the decided body and the token digest.
func TestMulti10SettleNoResultRetainsProjectionOnly(t *testing.T) {
	const taskID = "task-1"
	store, _ := multi08Store(t)
	// The production forward client is the seam that surfaces the upstream's
	// 404: the recording stub cannot answer per endpoint without also failing
	// the start forward.
	h := buildHarnessWith(t, false, deliveryConfirmWindow, nil, true, false,
		harnessOptions{store: store})
	token := multi08DriveClaim(t, h, taskID)

	before, ok := h.reg.Get(taskID)
	if !ok {
		t.Fatalf("%s missing after start", taskID)
	}
	if len(before.Payload) == 0 || before.TokenHash == "" {
		t.Fatalf("fixture: the pre-verdict row carries no delivery body / token digest "+
			"(payload %d bytes, digest %q)", len(before.Payload), before.TokenHash)
	}

	// Park the terminal forward: the verdict and the decided body are written
	// before the report leaves (§4.4), so a read while the request sits at the
	// upstream is the pre-settlement row, and the done key at that instant is
	// the terminal-mark write the settlement is about to replace.
	h.upstream.setLifecycleStatus(string(scheduler.EPComplete), http.StatusNotFound)
	gate := h.upstream.holdLifecycle(string(scheduler.EPComplete))
	var release sync.Once
	unblock := func() { release.Do(func() { close(gate) }) }
	defer unblock()

	type answer struct {
		code int
		body []byte
		err  error
	}
	done := make(chan answer, 1)
	go func() {
		code, body, err := forward06Post(h.jobsHTTP.URL+"/api/daemon/tasks/"+taskID+"/complete",
			token, []byte(`{"exit_code":0}`))
		done <- answer{code: code, body: body, err: err}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for multi10Forwards(h, taskID) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the terminal report never reached the upstream")
		}
		time.Sleep(2 * time.Millisecond)
	}

	inFlight, ok := h.reg.Get(taskID)
	if !ok {
		t.Fatalf("%s missing while the terminal report is in flight", taskID)
	}
	if !inFlight.IsTerminal() {
		t.Errorf("positive control: the verdict is not written before the forward (state %s)", inFlight.State)
	}
	if len(inFlight.Payload) == 0 {
		t.Errorf("positive control: the live row lost its delivery body before settlement")
	}
	if inFlight.TokenHash == "" {
		t.Errorf("positive control: the live row lost its token digest before settlement")
	}
	if len(inFlight.DecidedReport) == 0 {
		t.Errorf("positive control: the live row never registered the decided terminal body")
	}
	preSettle, ok, err := store.GetSettled(t.Context(), inFlight.WorkspaceID, taskID)
	if err != nil {
		t.Fatalf("GetSettled before settlement: %v", err)
	}
	if !ok {
		t.Fatal("the terminal-mark write left no done key to settle")
	}
	if preSettle.Result != registry.ResultCompleted {
		t.Errorf("done key before settlement has result %q, want %q (the 404 settlement clears it)",
			preSettle.Result, registry.ResultCompleted)
	}

	unblock()
	got := <-done
	if got.err != nil {
		t.Fatalf("terminal report: %v", got.err)
	}
	if got.code != http.StatusOK {
		t.Fatalf("terminal report against a 404 upstream = %d (%s), want 200", got.code, got.body)
	}

	multi08Flush(t, h)

	// The done key body: GetSettled unmarshals the snapshot and strips
	// nothing, so this is the row the retention window actually holds. The
	// cleared result is the settlement's own signature — the terminal-mark
	// write above carried completed — so a pass here cannot be the earlier
	// write.
	row, ok, err := store.GetSettled(t.Context(), before.WorkspaceID, taskID)
	if err != nil {
		t.Fatalf("GetSettled after settlement: %v", err)
	}
	if !ok {
		t.Fatal("the 404 settlement wrote no done key")
	}
	if row.Result != "" || row.FailureReason != "" {
		t.Errorf("done key was not rewritten by the 404 settlement: result %q, reason %q",
			row.Result, row.FailureReason)
	}
	multi08AssertProjectionOnly(t, "settled done key", row)

	// The restart face: the recovered terminal record answers the retention
	// window too, with the same projection-only shape.
	h2 := buildHarnessWith(t, false, deliveryConfirmWindow, nil, false, false,
		harnessOptions{store: store})
	if err := h2.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild after restart: %v", err)
	}
	restored, ok := h2.reg.Settled(taskID)
	if !ok {
		t.Fatal("the settled record was lost across restart")
	}
	multi08AssertProjectionOnly(t, "after restart", restored)
	if restored.Result != "" {
		t.Errorf("restored settled record has result %q, want none", restored.Result)
	}
}
