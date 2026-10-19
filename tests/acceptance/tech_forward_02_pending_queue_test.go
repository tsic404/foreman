// TC-tech-forward-02 (AC-16) drives the terminal-report durability face end to
// end with the production components: the daemon's terminal report is written
// ahead of its forward, the upstream stays unavailable through the retry face,
// and the report survives in the durable queue. The pending gauge shows the
// depth (0 → 1), the forward-failure event is logged, and the Job is kept —
// a deleted Job must never stand in for a delivered report. Once the upstream
// recovers, the recovery loop's drain re-delivers the report within two
// rounds, the queue depth falls back to zero, the Job is cleaned up, and the
// server takes exactly one terminal write with the daemon's own body.
package acceptance

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/scheduler"
)

// queueObjects is the recovery module's ObjectClient over the harness's
// cluster fake: a Job is visible until it is deleted, exactly like the
// apiserver's list face.
type queueObjects struct{ jobs *jobClient }

func (o queueObjects) GetJob(_ context.Context, name string) (*batchv1.Job, error) {
	o.jobs.mu.Lock()
	defer o.jobs.mu.Unlock()
	for i := range o.jobs.jobs {
		if o.jobs.jobs[i].Name == name {
			return o.jobs.jobs[i].DeepCopy(), nil
		}
	}
	return nil, nil
}

func (o queueObjects) ListPods(ctx context.Context) ([]corev1.Pod, error) {
	return o.jobs.ListPods(ctx, "")
}

func TestForward02TerminalReportSurvivesTheUpstreamOutage(t *testing.T) {
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

	// The upstream is down for the whole retry face: the daemon's terminal
	// report exhausts it and the daemon is told to retry (502).
	report := []byte(`{"result":"ok"}`)
	h.server.setForwardCode(503)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, report); code != 502 {
		t.Fatalf("S13 with a down upstream = %d (%s), want 502", code, body)
	}

	// AC-16's outage observables: the report is queued (depth 0 → 1), the
	// failure is visible, and the objects are kept for the replay.
	if got := h.pending.Len(); got != 1 {
		t.Fatalf("pending reports = %d, want 1", got)
	}
	if got := h.metrics.pendingReportsDepth(); got != 1 {
		t.Fatalf("foreman_pending_reports = %d, want 1", got)
	}
	if !strings.Contains(h.logs.String(), `"task.forward_failed"`) {
		t.Fatalf("no forward-failure event in the scheduler log:\n%s", h.logs.String())
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job was deleted before its terminal report landed")
	}
	entry, ok := h.reg.Get("task-1")
	if !ok || !entry.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want a retained terminal entry", entry, ok)
	}

	// The upstream recovers: the recovery loop's rounds drain the queue. The
	// contract bounds the catch-up to two rounds.
	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		h.reg, queueObjects{jobs: h.jobs}, h.server, h.sched)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	h.server.setForwardCode(0)
	for round := 1; round <= 2 && h.pending.Len() > 0; round++ {
		if err := rec.Reconcile(t.Context()); err != nil {
			t.Fatalf("Reconcile round %d: %v", round, err)
		}
	}
	if got := h.pending.Len(); got != 0 {
		t.Fatalf("pending reports after two convergence rounds = %d, want 0", got)
	}
	if got := h.metrics.pendingReportsDepth(); got != 0 {
		t.Fatalf("foreman_pending_reports after the drain = %d, want 0", got)
	}

	// The server took exactly one terminal write, with the daemon's body: a
	// replay never rewrites a settled task.
	if got := h.server.acceptedFor(scheduler.EPComplete); got != 1 {
		t.Fatalf("accepted complete writes = %d, want exactly 1", got)
	}
	forwards := h.server.forwarded(scheduler.EPComplete)
	last := forwards[len(forwards)-1]
	if !bytes.Equal(last.Body, report) {
		t.Fatalf("delivered body = %s, want the daemon's %s", last.Body, report)
	}

	// The report landed, so the verdict-time Job deletion runs.
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job must be deleted once the terminal report landed")
	}
}
