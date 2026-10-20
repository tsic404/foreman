// TC-tech-recovery-03 (AC-08): a network partition raises the suspect signal
// without acting. A live task whose daemon is silent on both heartbeat
// channels sets the suspect gauge and logs the event, but the Job is kept and
// the mapping stays live — the daemon may still be working. Once the heartbeat
// returns and the server reports the task completed, the next round converges
// it normally.
package acceptance

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// tcRecovery03Metrics records the suspect gauge the reconciler publishes.
type tcRecovery03Metrics struct {
	mu       sync.Mutex
	suspects int
	seen     bool
}

func (m *tcRecovery03Metrics) HeartbeatSuspect(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.suspects = n
	m.seen = true
}

func (m *tcRecovery03Metrics) value() (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.suspects, m.seen
}

// tcRecovery03Runtime rewrites the task's runtime row with the given heartbeat
// age.
func tcRecovery03Runtime(t *testing.T, h *harness, taskID string, age time.Duration) {
	t.Helper()
	entry, ok := h.reg.Get(taskID)
	if !ok {
		t.Fatalf("entry %s missing", taskID)
	}
	rt, ok := h.reg.RuntimeByID(entry.JobRuntimeID)
	if !ok {
		t.Fatalf("runtime %s missing", entry.JobRuntimeID)
	}
	rt.LastHeartbeatAt = time.Now().Add(-age)
	h.reg.PutRuntime(rt)
}

func TestRecovery03PartitionMarksSuspectWithoutActing(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	claimTask(t, h, "task-1", token)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != 200 {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	// The partition: the daemon has been silent for an hour, and the server
	// still holds the task as running.
	tcRecovery03Runtime(t, h, "task-1", time.Hour)
	h.server.setStatus("task-1", "running")

	metrics := &tcRecovery03Metrics{}
	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		h.reg, queueObjects{jobs: h.jobs}, h.server, h.sched,
		recovery.WithMetrics(metrics),
		recovery.WithLogger(slog.New(slog.NewJSONHandler(h.logs, nil))),
	)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// The suspect signal is observable; nothing is deleted or settled.
	if got, seen := metrics.value(); !seen || got != 1 {
		t.Errorf("heartbeat suspect gauge = %d (seen %v), want 1", got, seen)
	}
	events := h.events("task.heartbeat_suspect")
	if len(events) != 1 || events[0]["task_id"] != "task-1" {
		t.Errorf("task.heartbeat_suspect = %+v, want one event for task-1", events)
	}
	if entry, ok := h.reg.Get("task-1"); !ok || entry.IsTerminal() {
		t.Errorf("entry = %+v (%v), want the live entry kept during the partition", entry, ok)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job was deleted during the partition; the daemon may still be working")
	}
	if forwards := h.server.forwarded(scheduler.EPFail); len(forwards) != 0 {
		t.Errorf("compensation reports during the partition = %+v, want none", forwards)
	}

	// Recovery: the heartbeat returns and the server reports the task
	// completed. The next round converges it normally and the gauge drops.
	tcRecovery03Runtime(t, h, "task-1", 0)
	h.server.setStatus("task-1", "completed")
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile after recovery: %v", err)
	}
	entry := waitTerminal(t, h, "task-1")
	if entry.Result != registry.ResultCompleted {
		t.Errorf("result = %s, want completed after recovery", entry.Result)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job was not deleted after the server-reported completion")
	}
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile after settlement: %v", err)
	}
	if got, _ := metrics.value(); got != 0 {
		t.Errorf("heartbeat suspect gauge after recovery = %d, want 0", got)
	}
}
