// The `first` gates only matter when a losing writer's MarkTerminal returns
// first=false: OnReport's terminal branch loses to a settlement landed at the
// queue landing, settleFailure to a verdict already written before failEntry
// (it does not pre-check IsTerminal), settleReplayed to a verdict written
// between its fresh read and its write, OnTaskVanished to a verdict written
// after its pre-check, and ConvergeLeaseRefused to a verdict written while its
// C13 answer was in flight. Each case must leave the single observation to
// the path that actually transitioned the entry.
package scheduler

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// enqueueHookQueue runs onEnqueue at the queue landing, the last step before
// OnReport's MarkTerminal.
type enqueueHookQueue struct {
	*fakePendingReports
	onEnqueue func()
}

func (q *enqueueHookQueue) Enqueue(ctx context.Context, taskID, jobName, daemonID, workspaceID string, ep Endpoint, body []byte, failureReason string) error {
	if q.onEnqueue != nil {
		q.onEnqueue()
	}
	return q.fakePendingReports.Enqueue(ctx, taskID, jobName, daemonID, workspaceID, ep, body, failureReason)
}

// TestOnReportTerminalLosingReportDoesNotCountTheVerdict guards the `first`
// check: a report whose MarkTerminal did not transition the entry must not
// emit a terminal metric or task.terminal event of its own. The competing
// path's single observation is the only one.
func TestOnReportTerminalLosingReportDoesNotCountTheVerdict(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")

	buf := &strings.Builder{}
	f.sched.log = slog.New(slog.NewJSONHandler(buf, nil)).With("component", "scheduler")

	winner := &enqueueHookQueue{fakePendingReports: f.pending}
	winner.onEnqueue = func() {
		// The attribution gate has already judged this report undecided. A
		// settlement path now wins the entry while the report holds the task
		// lock: SettleTerminal would block on that lock, so the hook lands the
		// verdict and its observation directly, like setStatusHook does for
		// the C13 round trip.
		if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, "", f.now); err != nil {
			t.Errorf("winning MarkTerminal: %v", err)
		}
		f.metrics.TaskTerminal(string(registry.ResultCompleted))
		f.sched.log.InfoContext(context.Background(), "task.terminal",
			"task_id", "task-1", "result", string(registry.ResultCompleted))
	}
	f.sched.pendingReports = winner

	code, _, err := f.sched.OnReport(context.Background(), EPFail, e,
		[]byte(`{"error":"agent crashed"}`), "")
	if err != nil || code != 200 {
		t.Fatalf("OnReport = %d, %v", code, err)
	}

	entry, ok := f.reg.Get("task-1")
	if !ok || entry.Result != registry.ResultCompleted {
		t.Fatalf("entry = %+v, %v; want the competing verdict", entry, ok)
	}
	if got := f.metrics.terminal; len(got) != 1 || got[0] != string(registry.ResultCompleted) {
		t.Errorf("terminal metric = %v, want exactly the competing verdict once", got)
	}
	records := collectLogRecords(t, []byte(buf.String()), "task.terminal")
	if len(records) != 1 {
		t.Fatalf("task.terminal events = %d, want exactly the competing verdict once", len(records))
	}
	if records[0]["result"] != string(registry.ResultCompleted) {
		t.Fatalf("task.terminal result = %v, want completed", records[0]["result"])
	}
}

// TestFailEntryOnADecidedTaskDoesNotCountTheVerdict guards settleFailure's
// `first` check: failEntry does not pre-check IsTerminal, so a task another
// path already decided still reaches settleFailure and its MarkTerminal
// returns first=false. That report must not count a terminal transition nor
// log a compensation for a verdict it did not write.
func TestFailEntryOnADecidedTaskDoesNotCountTheVerdict(t *testing.T) {
	f := newFixture(t)
	e := f.claim(t, "task-1")
	if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, "", f.now); err != nil {
		t.Fatalf("winning MarkTerminal: %v", err)
	}

	buf := &strings.Builder{}
	f.sched.log = slog.New(slog.NewJSONHandler(buf, nil)).With("component", "scheduler")

	if err := f.sched.failEntry(context.Background(), e, failReport{
		reason:  FailureReasonJobFailed,
		message: "container ended without a terminal report",
	}); err != nil {
		t.Fatalf("failEntry: %v", err)
	}

	entry, ok := f.reg.Get("task-1")
	if !ok || entry.Result != registry.ResultCompleted {
		t.Fatalf("entry = %+v, %v; want the first verdict completed", entry, ok)
	}
	if got := f.metrics.terminal; len(got) != 0 {
		t.Errorf("terminal metric = %v, want no count for the losing report", got)
	}
	if records := collectLogRecords(t, []byte(buf.String()), "task.failed_compensated"); len(records) != 0 {
		t.Errorf("task.failed_compensated events = %d, want 0 for the losing report", len(records))
	}
}

// TestSettleReplayedLosingTheVanishedRaceDoesNotCountTheVerdict guards
// settleReplayed's `first` check. The verdict is written between the fresh
// read and the write — the window a vanished verdict can land in — so the
// replay's MarkTerminal returns first=false and it must leave the count to
// the path that actually transitioned the entry.
func TestSettleReplayedLosingTheVanishedRaceDoesNotCountTheVerdict(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")

	fired := false
	f.sched.now = func() time.Time {
		// MarkTerminal's timestamp argument is evaluated after the fresh read
		// and before the write. The guard keeps the nested clock read inside
		// MarkTerminal from re-entering this hook.
		if !fired {
			fired = true
			if _, _, err := f.reg.MarkTerminal("task-1", "", "", f.now); err != nil {
				t.Errorf("winning MarkTerminal: %v", err)
			}
		}
		return f.now
	}

	f.sched.settleReplayed(context.Background(), "task-1", EPComplete, "")

	entry, ok := f.reg.Get("task-1")
	if !ok || !entry.IsTerminal() || entry.Result != "" {
		t.Fatalf("entry = %+v, %v; want the vanished verdict to have won", entry, ok)
	}
	if got := f.metrics.terminal; len(got) != 0 {
		t.Errorf("terminal metric = %v, want no count for the losing replay", got)
	}
}

// TestConvergeLeaseRefusedLosingTheRaceDoesNotCountTheVerdict guards
// ConvergeLeaseRefused's `first` check: a settlement landing while the C13
// answer is in flight leaves the entry terminal at the write (first=false), so
// that round must not count or log a verdict it did not write.
func TestConvergeLeaseRefusedLosingTheRaceDoesNotCountTheVerdict(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	f.server.status = string(registry.ResultFailed)

	buf := &strings.Builder{}
	f.sched.log = slog.New(slog.NewJSONHandler(buf, nil)).With("component", "scheduler")

	f.server.onStatus = func() {
		f.server.onStatus = nil
		if _, _, err := f.reg.MarkTerminal("task-1", registry.ResultCompleted, "", f.now); err != nil {
			t.Errorf("winning MarkTerminal: %v", err)
		}
		f.metrics.TaskTerminal(string(registry.ResultCompleted))
	}

	if err := f.sched.ConvergeLeaseRefused(context.Background(), "task-1"); err != nil {
		t.Fatalf("ConvergeLeaseRefused: %v", err)
	}

	entry, ok := f.reg.Get("task-1")
	if !ok || entry.Result != registry.ResultCompleted {
		t.Fatalf("entry = %+v, %v; want the competing verdict", entry, ok)
	}
	if got := f.metrics.terminal; len(got) != 1 || got[0] != string(registry.ResultCompleted) {
		t.Errorf("terminal metric = %v, want exactly the competing verdict once", got)
	}
	if records := collectLogRecords(t, []byte(buf.String()), "task.terminal"); len(records) != 0 {
		t.Errorf("task.terminal events = %d, want 0 for the losing convergence", len(records))
	}
}

// TestOnTaskVanishedOnADecidedTaskDoesNotLogTheVanishedEvent guards the
// decided-entry branch: a vanished callback that arrives after another path
// decided the task only retries the Job deletion, and must not emit a second
// task.vanished event nor rewrite the verdict.
func TestOnTaskVanishedOnADecidedTaskDoesNotLogTheVanishedEvent(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "task-1")
	if err := f.sched.SettleTerminal(context.Background(), "task-1", string(registry.ResultCompleted)); err != nil {
		t.Fatalf("SettleTerminal: %v", err)
	}

	buf := &strings.Builder{}
	f.sched.log = slog.New(slog.NewJSONHandler(buf, nil)).With("component", "scheduler")

	if err := f.sched.OnTaskVanished(context.Background(), "task-1"); err != nil {
		t.Fatalf("OnTaskVanished: %v", err)
	}

	entry, ok := f.reg.Get("task-1")
	if !ok || entry.Result != registry.ResultCompleted {
		t.Fatalf("entry = %+v, %v; want the first verdict completed", entry, ok)
	}
	if records := collectLogRecords(t, []byte(buf.String()), "task.vanished"); len(records) != 0 {
		t.Errorf("task.vanished events = %d, want 0 for the already-decided entry", len(records))
	}
}
