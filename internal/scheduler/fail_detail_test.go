package scheduler

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

// TestFailJobErrorCarriesTheTerminationFacts: the synthesized C10 names the
// pod-level end facts, and the key set stays the contract's — the facts are
// free text, never new keys.
func TestFailJobErrorCarriesTheTerminationFacts(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "running"
	detail := "container=agent exit=1 reason=Error node=company-01 pod=fm-task-1-x finished=2026-11-05T13:14:04Z"

	if err := f.sched.FailJob(context.Background(), "fm-task-1", FailureReasonJobFailed, detail); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if len(f.server.forwards) != 1 || f.server.forwards[0].ep != EPFail {
		t.Fatalf("forwards = %+v, want exactly one fail report", f.server.forwards)
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(f.server.forwards[0].body), &body); err != nil {
		t.Fatalf("decode body %q: %v", f.server.forwards[0].body, err)
	}
	if want := "container ended without a terminal report: " + detail; body["error"] != want {
		t.Fatalf("error = %q, want %q", body["error"], want)
	}
	if body["failure_reason"] != FailureReasonJobFailed {
		t.Fatalf("failure_reason = %q, want %q", body["failure_reason"], FailureReasonJobFailed)
	}
	if len(body) != 2 {
		t.Fatalf("body = %v, want exactly error and failure_reason", body)
	}
}

// TestFailJobWithoutDetailKeepsTheFixedSentence guards the paths with no pod
// facts (a missing Job, the refused-registration compensation): the C10 text
// must stay the one the platform's records already carry.
func TestFailJobWithoutDetailKeepsTheFixedSentence(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = "running"

	if err := f.sched.FailJob(context.Background(), "fm-task-1", FailureReasonJobMissing, ""); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if len(f.server.forwards) != 1 {
		t.Fatalf("forwards = %+v, want exactly one fail report", f.server.forwards)
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(f.server.forwards[0].body), &body); err != nil {
		t.Fatalf("decode body %q: %v", f.server.forwards[0].body, err)
	}
	if body["error"] != "container ended without a terminal report" {
		t.Fatalf("error = %q, want the fixed sentence", body["error"])
	}
}

// TestFailJobDetailReplaysByteForByte: the detail reaches the durable queue
// before encode, so a report a restarted process replays carries the same
// facts — the queue hands over the stored bytes, it does not re-synthesize.
func TestFailJobDetailReplaysByteForByte(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	p, err := NewPendingReports(dir)
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	WithPendingReports(p)(f.sched)
	e := f.claim(t, "task-1")
	e.State = registry.StateRunning
	f.reg.Put(e)
	f.server.status = "running"
	f.server.forwardCode = 503
	detail := "container=agent exit=137 reason=OOMKilled node=company-01 pod=fm-task-1-x"

	if err := f.sched.FailJob(context.Background(), "fm-task-1", FailureReasonJobFailed, detail); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if !p.Queued("task-1") {
		t.Fatal("the undelivered fail report must stay in the durable queue")
	}
	if len(f.server.forwards) != 1 {
		t.Fatalf("forwards = %+v, want the attempted fail report", f.server.forwards)
	}
	forwarded := f.server.forwards[0].body

	// The process is gone: a fresh queue over the same directory is all a
	// restart has, and the replay must send the stored bytes unchanged.
	restarted, err := NewPendingReports(dir)
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	sender := &fakeSender{code: 200}
	if remaining, err := restarted.Drain(context.Background(), sender); err != nil || remaining != 0 {
		t.Fatalf("Drain after restart = %d, %v", remaining, err)
	}
	if len(sender.sent) != 1 || sender.sent[0] != "task-1:fail:"+forwarded {
		t.Fatalf("replayed = %v, want the forwarded bytes %q", sender.sent, forwarded)
	}
}
