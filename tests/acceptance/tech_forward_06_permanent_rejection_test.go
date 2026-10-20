// TC-tech-forward-06 (AC-16) exercises the permanent-rejection exit of the
// terminal-report face. §1.5 makes every non-408/429 4xx a permanent failure;
// the 404/409 answers are the two settlement classes, so the third exit — a
// 403 (or any other permanent 4xx) — is a discard face. The report is written
// ahead of the forward, so the entry and its carrier exist while the forward
// is in flight; the rejection drops both. The status-coded forward counter
// records the refused attempt, neither a same-process round nor a rebuilt
// process retries or replays it, and the task does not come back from the
// carrier face after the restart.
package acceptance

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// forward06Harness is the delivery fixture over the production forward client:
// foreman_forward_total is recorded by that client, so the scenario that
// judges the status-coded counter must drive it rather than the recording
// stub.
func forward06Harness(t *testing.T) *harness {
	t.Helper()
	h := newHarnessWithRealServer(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	// The rebuild reads the Job back, so the restart must see the production
	// Job (labels + annotations), not the canned one.
	h.useRealBuilder(t)
	return h
}

// forward06Post is a Job-side request that returns its error instead of
// calling t.Fatalf: it runs on the scenario goroutine that observes the
// in-flight write-ahead window.
func forward06Post(url, token string, body []byte) (int, []byte, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, raw, nil
}

// forward06Completes counts the terminal forwards the stub upstream observed
// for taskID: the wire face of "no retry and no replay".
func forward06Completes(h *harness, taskID string) int {
	path := "/api/daemon/tasks/" + taskID + "/complete"
	n := 0
	for _, req := range h.upstream.all() {
		if req.Path == path {
			n++
		}
	}
	return n
}

func TestForward06PermanentRejectionExitsTheQueue(t *testing.T) {
	h := forward06Harness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	claimTask(t, h, "task-1", token)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != 200 {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}

	report := []byte(`{"result":"ok"}`)
	h.upstream.setLifecycleStatus(string(scheduler.EPComplete), http.StatusForbidden)
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
		code, body, err := forward06Post(h.jobsHTTP.URL+"/api/daemon/tasks/task-1/complete", token, report)
		done <- answer{code, body, err}
	}()

	// Wait until the forward is in flight: the write-ahead enqueue has landed
	// on the queue and its carrier before the request is sent (§10), so both
	// faces read one report here.
	deadline := time.Now().Add(10 * time.Second)
	for forward06Completes(h, "task-1") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the terminal report never reached the upstream")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := h.pending.Len(); got != 1 {
		t.Fatalf("pending reports while the report is in flight = %d, want 1", got)
	}
	if got := h.metrics.pendingReportsDepth(); got != 1 {
		t.Fatalf("foreman_pending_reports while in flight = %d, want 1", got)
	}
	carriers := h.jobs.carrierSecrets()
	if len(carriers) != 1 {
		t.Fatalf("carriers while in flight = %d, want 1", len(carriers))
	}
	if got := carriers[0].Data["body"]; !bytes.Equal(got, report) {
		t.Fatalf("carrier body = %s, want the forwarded %s", got, report)
	}

	unblock()
	res := <-done
	if res.err != nil {
		t.Fatalf("terminal report: %v", res.err)
	}
	// The permanent 4xx is not the transient-outage answer: the daemon sees
	// the rejection itself, not the 502 the retry-exhausted path returns.
	if res.code != http.StatusForbidden {
		t.Fatalf("S13 = %d (%s), want 403", res.code, res.body)
	}

	// The AC-16 exit face: the pending count falls back to zero, the task's
	// carrier entry is gone, and the status-coded forward counter records the
	// refused attempt (and no accepted one).
	if got := h.pending.Len(); got != 0 {
		t.Fatalf("pending reports after the rejection = %d, want 0", got)
	}
	if got := h.metrics.pendingReportsDepth(); got != 0 {
		t.Fatalf("foreman_pending_reports after the rejection = %d, want 0", got)
	}
	if got := len(h.jobs.carrierSecrets()); got != 0 {
		t.Fatalf("carriers after the rejection = %d, want 0", got)
	}
	if got := h.metrics.forwardCount(string(scheduler.EPComplete), http.StatusForbidden); got != 1 {
		t.Fatalf("foreman_forward_total{endpoint=complete,code=403} = %d, want 1", got)
	}
	if got := h.metrics.forwardCount(string(scheduler.EPComplete), http.StatusOK); got != 0 {
		t.Fatalf("accepted complete forwards = %d, want 0", got)
	}
	// The verdict written before the forward stands, and the terminal path
	// tears the Job down.
	entry, ok := h.reg.Get("task-1")
	if !ok || !entry.IsTerminal() || entry.Result != registry.ResultCompleted {
		t.Fatalf("entry = %+v, %v; want the retained completed verdict", entry, ok)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the Job must be torn down on a permanent rejection")
	}

	// Same-process convergence: two rounds over the retained terminal entry
	// and the empty queue forward nothing. A retry or a replay would show as
	// a second complete request.
	rec, err := recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		h.reg, queueObjects{jobs: h.jobs}, h.serverClient, h.sched)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}
	for round := 1; round <= 2; round++ {
		if err := rec.Reconcile(t.Context()); err != nil {
			t.Fatalf("Reconcile round %d: %v", round, err)
		}
	}
	if got := forward06Completes(h, "task-1"); got != 1 {
		t.Fatalf("complete forwards after two rounds = %d, want the single refused attempt", got)
	}

	// Cross-process face: a fresh process rebuilds from the cluster objects
	// and the carrier list. The permanently refused task has neither, so the
	// rebuilt queue stays empty, the entry does not reappear, and the rebuilt
	// convergence rounds replay nothing. restartOverCluster wires the rebuilt
	// scheduler onto h.serverClient — the same production forward seam the
	// harness itself runs on — so a replay from the rebuilt process reaches
	// both the wire and the status-coded counter; the snapshots below are
	// taken on those two faces, not on the recorder stub.
	reg, pending, sched := restartOverCluster(t, h)
	if err := sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("rebuilt queue depth = %d, want 0", got)
	}
	if _, ok := reg.Get("task-1"); ok {
		t.Fatal("the permanently refused task reappeared after the rebuild")
	}
	wireBefore := forward06Completes(h, "task-1")
	forbiddenBefore := h.metrics.forwardCount(string(scheduler.EPComplete), http.StatusForbidden)
	if wireBefore != 1 || forbiddenBefore != 1 {
		t.Fatalf("complete forwards before the rebuilt rounds = %d/%d, want 1/1 (the single refused attempt)",
			wireBefore, forbiddenBefore)
	}
	rec, err = recovery.New(
		recovery.Config{ReconcileInterval: time.Second},
		reg, queueObjects{jobs: h.jobs}, h.serverClient, sched)
	if err != nil {
		t.Fatalf("recovery.New (rebuilt process): %v", err)
	}
	for round := 1; round <= 2; round++ {
		if err := rec.Reconcile(t.Context()); err != nil {
			t.Fatalf("rebuilt Reconcile round %d: %v", round, err)
		}
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("rebuilt queue depth after two rounds = %d, want 0", got)
	}
	if got := forward06Completes(h, "task-1"); got != wireBefore {
		t.Fatalf("complete requests on the wire after the rebuilt rounds = %d, want the %d before them (no replay)",
			got, wireBefore)
	}
	if got := h.metrics.forwardCount(string(scheduler.EPComplete), http.StatusForbidden); got != forbiddenBefore {
		t.Fatalf("foreman_forward_total{endpoint=complete,code=403} after the rebuilt rounds = %d, want the %d before them (no replay)",
			got, forbiddenBefore)
	}
}
