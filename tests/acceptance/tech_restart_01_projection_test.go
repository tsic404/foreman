// TC-tech-restart-01 (AC-09) covers the projection face of the restart
// judgement: after the index is rebuilt, /foreman/tasks must show "no start
// fact yet" as an empty string with the key present, so the step-3 poll keeps
// polling — the RFC3339 zero satisfied `!= ""` at once and made the adoption
// face of step 4 undecidable — and the adopted entry of step 4 must show
// started_at non-empty while its Job survives.
package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestRestart01AbsentStartFactsProjectEmptyThenAdopt(t *testing.T) {
	h := newHarness(t, false)
	// The rebuild reads the Job objects back, so the scenario needs the
	// production Job (labels + annotations), not the canned one.
	h.useRealBuilder(t)
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	// 步骤 1: one claimed task, its Job placed in the cluster.
	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	// This process claimed the task, so the boot upper bound exists and
	// projects RFC3339; no start fact exists yet (02-contracts.md §4.1).
	assertProjectedTime(t, ops.URL, "task-1", "boot_deadline", true)
	assertProjectedTime(t, ops.URL, "task-1", "started_at", false)

	// 步骤 2: restart — the in-memory index is gone, the Jobs are not.
	if err := h.reg.Delete("task-1"); err != nil {
		t.Fatalf("drop entry: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	// 步骤 3: the rebuilt entry has neither a start fact nor a boot upper
	// bound (this process never claimed it) — both project empty with their
	// key present, which is what the poll samples.
	assertProjectedTime(t, ops.URL, "task-1", "started_at", false)
	assertProjectedTime(t, ops.URL, "task-1", "boot_deadline", false)

	// 步骤 4: C13 reports the task running → adopt: started_at carries the
	// observation instant, the Job is kept, the entry stays non-terminal.
	h.server.setStatus("task-1", "running")
	if err := h.sched.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertProjectedTime(t, ops.URL, "task-1", "started_at", true)
	projected := projectedEntry(t, ops.URL, "task-1")
	// Adoption does not advance the lifecycle state: the projection keeps the
	// rebuild's initial phase (ADR-026 后果) — the running face is started_at,
	// never state.
	if state := projected["state"]; state != "job_created" {
		t.Errorf("state = %v, want the rebuild phase job_created after adoption", state)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Error("the adopted task's Job was deleted")
	}
}

// TestRestart01AdoptedEntryIgnoresStatusFallback guards ADR-026 决策 5 on the
// production convergence driver: once C13 adopted a rebuilt entry
// (started_at non-empty), a later queued/dispatched answer — a server
// re-queue or status jitter — must not send it back to the discard row. The
// Job survives and the entry stays in the projection with its start fact.
func TestRestart01AdoptedEntryIgnoresStatusFallback(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	// Restart: the index is gone, the Job is not — the rebuilt entry has no
	// payload and no start fact.
	if err := h.reg.Delete("task-1"); err != nil {
		t.Fatalf("drop entry: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	rec, err := recovery.New(recovery.Config{ReconcileInterval: time.Minute},
		h.reg, recoveryObjects{jobs: h.jobs}, h.server, h.sched)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}

	// C13 = running → adopt (start fact set), Job kept.
	h.server.setStatus("task-1", "running")
	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile adopt: %v", err)
	}
	assertProjectedTime(t, ops.URL, "task-1", "started_at", true)

	for _, status := range []string{"queued", "dispatched"} {
		h.server.setStatus("task-1", status)
		if err := rec.Reconcile(t.Context()); err != nil {
			t.Fatalf("Reconcile on C13=%s: %v", status, err)
		}
		if h.jobs.jobDeleted("fm-task-1") {
			t.Fatalf("C13=%s deleted the Job of an adopted entry", status)
		}
		projected := projectedEntry(t, ops.URL, "task-1")
		if state := projected["state"]; state == "terminal" {
			t.Errorf("C13=%s settled the adopted entry terminal", status)
		}
		assertProjectedTime(t, ops.URL, "task-1", "started_at", true)
	}
}

// TestRestart01StartFactWinsOverRoundSnapshot guards ADR-026 决策 5 at the
// deletion surface: the round snapshot is taken before C13 is queried, so an
// S8 start that lands inside the C13 round trip leaves started_at zero in
// that snapshot. The release must re-read the live entry under the task lock
// and keep the Job — a snapshot-based reading deletes an adopted task.
func TestRestart01StartFactWinsOverRoundSnapshot(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	// Restart: the rebuilt entry has neither a payload nor a start fact.
	if err := h.reg.Delete("task-1"); err != nil {
		t.Fatalf("drop entry: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	rec, err := recovery.New(recovery.Config{ReconcileInterval: time.Minute},
		h.reg, recoveryObjects{jobs: h.jobs}, h.server, h.sched)
	if err != nil {
		t.Fatalf("recovery.New: %v", err)
	}

	// C13 answers dispatched (the discard row), and S8's start lands before
	// the release runs: MarkStarted is ordered behind the same task lock the
	// deletion surface re-reads under.
	h.server.setStatus("task-1", "dispatched")
	var startErr error
	h.server.setStatusHook(func() {
		h.server.setStatusHook(nil)
		e, ok := h.reg.Get("task-1")
		if !ok {
			startErr = fmt.Errorf("entry gone before the S8 round")
			return
		}
		if _, _, startErr = h.sched.OnReport(t.Context(), scheduler.EPStart, e,
			[]byte(`{"status":"running"}`), ""); startErr != nil {
			startErr = fmt.Errorf("S8 start: %w", startErr)
		}
	})

	if err := rec.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if startErr != nil {
		t.Fatalf("interleaved start: %v", startErr)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("the release deleted the Job after S8 established the start fact")
	}
	if _, ok := h.reg.Get("task-1"); !ok {
		t.Fatal("the adopted entry disappeared from the index")
	}
	assertProjectedTime(t, ops.URL, "task-1", "started_at", true)
}

// recoveryObjects adapts the scenario's K8s surface to the recovery
// reconciler's object read face: GetJob answers from the Jobs the scheduler
// created, ListPods from the programmed pod list.
type recoveryObjects struct{ jobs *jobClient }

func (o recoveryObjects) GetJob(ctx context.Context, name string) (*batchv1.Job, error) {
	jobs, err := o.jobs.ListJobs(ctx)
	if err != nil {
		return nil, err
	}
	for i := range jobs {
		if jobs[i].Name == name {
			return &jobs[i], nil
		}
	}
	return nil, nil
}

func (o recoveryObjects) ListPods(ctx context.Context) ([]corev1.Pod, error) {
	return o.jobs.ListPods(ctx, "")
}

// TestRestart01UnboundedBootProjectsEmpty (02-contracts.md §4.1 / §4.2): with
// FOREMAN_JOB_BOOT_TIMEOUT=0 the claim writes no upper bound at all, so the
// projection keeps the key and carries the empty string — the same form a
// rebuilt entry has, and never a degenerate claimed_at that a consumer's
// `!= ""` test would read as a real instant.
func TestRestart01UnboundedBootProjectsEmpty(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	h.rebuildScheduler(t, func(c *scheduler.Config) { c.JobBootTimeout = 0 })
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	assertProjectedTime(t, ops.URL, "task-1", "boot_deadline", false)
	assertProjectedTime(t, ops.URL, "task-1", "started_at", false)
	if state := projectedEntry(t, ops.URL, "task-1")["state"]; state == "terminal" {
		t.Errorf("state = %v, want the unbounded claim still live", state)
	}
}

// TestRestart01DeletedJobLeavesNoGhostEntry guards the discard-then-restart
// combination of TC-tech-restart-01 步骤 2/5: a Job removed from the cluster
// must not reappear as an entry when the index is rebuilt from the list face —
// the fake cluster has to drop it from ListJobs the way the apiserver does.
func TestRestart01DeletedJobLeavesNoGhostEntry(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	// The process restart that precedes the discard: the index is gone, so the
	// entry is rebuilt from the cluster without a payload — the shape the
	// discard row is for (§4.3: 判别式 = 载荷可得性).
	if err := h.reg.Delete("task-1"); err != nil {
		t.Fatalf("registry delete: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	// The pre-start discard (failure-handling 场景 #2) removes the objects and
	// the entry; the restart that follows must find nothing to rebuild.
	if err := h.sched.DiscardTask(t.Context(), "task-1"); err != nil {
		t.Fatalf("DiscardTask: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if entries := projectedList(t, ops.URL); len(entries) != 0 {
		t.Fatalf("a discarded Job came back as a ghost entry: %v", entries)
	}
}

// restart01JobLive reports whether the Job object still exists on the list
// face (the deletion log is historical, so it cannot answer this).
func restart01JobLive(t *testing.T, h *harness, name string) bool {
	t.Helper()
	jobs, err := h.jobs.ListJobs(t.Context())
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	for i := range jobs {
		if jobs[i].Name == name {
			return true
		}
	}
	return false
}

// TestRestart01RedispatchSurvivesAStaleRelease covers TC-tech-restart-01
// 步骤 2/5 from the other side: the rebuilt entry is re-dispatched (the claim
// replaces it under the same display name), and the release round that
// decided "discard and redispatch" from its pre-claim snapshot must lose to
// the live entry — its Job, credential Secret and mapping stay (§4.3: the
// discard row is the payload-less rebuilt entry, never a claim's).
func TestRestart01RedispatchSurvivesAStaleRelease(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)

	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	// Restart: the index is rebuilt from the cluster, so the entry loses its
	// payload — the discard row's shape.
	if err := h.reg.Delete("task-1"); err != nil {
		t.Fatalf("drop entry: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	// The server re-dispatches the task: the claim replaces the rebuilt entry.
	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	if e, ok := h.reg.Get("task-1"); !ok || len(e.Payload) == 0 || e.Attempt != 2 {
		t.Fatalf("entry = %+v, %v; want the replacement claim", e, ok)
	}

	jobsBefore := len(h.jobs.deletedJobNames())
	secretsBefore := len(h.jobs.deletedSecretNames())
	if err := h.sched.DiscardTask(t.Context(), "task-1"); err != nil {
		t.Fatalf("DiscardTask: %v", err)
	}
	if e, ok := h.reg.Get("task-1"); !ok || len(e.Payload) == 0 {
		t.Fatalf("entry = %+v, %v; want the successor entry kept", e, ok)
	}
	if got := len(h.jobs.deletedJobNames()); got != jobsBefore {
		t.Fatalf("job deletions = %v, want none for the live successor", h.jobs.deletedJobNames()[jobsBefore:])
	}
	if got := len(h.jobs.deletedSecretNames()); got != secretsBefore {
		t.Fatalf("secret deletions = %v, want none for the live successor", h.jobs.deletedSecretNames()[secretsBefore:])
	}
	if !restart01JobLive(t, h, "fm-task-1") {
		t.Fatal("the successor's Job was deleted")
	}
	if !h.jobs.hasSecret("fm-task-1-cred") {
		t.Fatal("the successor's credential Secret was deleted")
	}
}

// TestRestart01OverlappingRebuildKeepsLiveClaim covers the startup race of
// AC-09: a rebuild that overlaps the claim loop lists the Job the claim just
// created, and its projection — no payload, no token digest, no boot/lease
// deadline — must not replace the live entry. The claim's next S6 must still
// deliver the task instead of answering an empty batch (task.payload_missing).
func TestRestart01OverlappingRebuildKeepsLiveClaim(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	live, ok := h.reg.Get("task-1")
	if !ok || len(live.Payload) == 0 {
		t.Fatalf("live entry = %+v, want the claimed payload", live)
	}

	// The overlapping rebuild: ListJobs returns the Job created above, and
	// its projection would be written over the live entry.
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	kept, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("the live entry disappeared")
	}
	if len(kept.Payload) == 0 {
		t.Error("the rebuild dropped the live payload")
	}
	if kept.TokenHash == "" {
		t.Error("the rebuild dropped the token digest")
	}
	if kept.BootDeadline.IsZero() || kept.LeaseKeepUntil.IsZero() {
		t.Errorf("the rebuild dropped a deadline: boot=%v lease=%v", kept.BootDeadline, kept.LeaseKeepUntil)
	}
	assertProjectedTime(t, ops.URL, "task-1", "boot_deadline", true)
	assertProjectedTime(t, ops.URL, "task-1", "lease_keep_until", true)

	// The delivery face: the daemon's next claim still gets the task body.
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	if raw := claimTask(t, h, "task-1", token); !bytes.Contains(raw, []byte(`"id":"task-1"`)) {
		t.Fatalf("S6 after the rebuild = %s, want the task payload", raw)
	}
	if events := h.events("task.payload_missing"); len(events) != 0 {
		t.Fatalf("task.payload_missing events = %d, want 0", len(events))
	}
}

// TestRestart01RebuiltEntryLeaseSkipsC4 covers the S7 half of AC-09: a
// rebuilt entry carries no dispatch record, so a forward would address
// /api/daemon/runtimes//tasks/{tid}/prepare-lease, whose bare 404 reads as
// "the task was deleted" and settles a live task as vanished. The renewal is
// answered locally and the entry stays for the startup convergence (ADR-026).
func TestRestart01RebuiltEntryLeaseSkipsC4(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)

	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	claimed, ok := h.reg.Get("task-1")
	if !ok || claimed.RealRuntimeID != "rt-1" {
		t.Fatalf("claimed entry = %+v, want the dispatch record rt-1", claimed)
	}

	// Restart: the in-memory index is gone, the Job is not. The rebuilt entry
	// has no payload and no dispatch record.
	if err := h.reg.Delete("task-1"); err != nil {
		t.Fatalf("drop entry: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	rebuilt, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("the Job was not rebuilt into an entry")
	}
	if rebuilt.RealRuntimeID != "" {
		t.Fatalf("rebuilt RealRuntimeID = %q, want empty", rebuilt.RealRuntimeID)
	}

	token := issueJobToken(t, h, "task-1")
	code, body := h.call("POST",
		"/api/daemon/runtimes/"+rebuilt.JobRuntimeID+"/tasks/task-1/prepare-lease",
		token, []byte(`{}`))
	if code != http.StatusOK || string(body) != `{"status":"ok"}` {
		t.Fatalf("S7 = %d %s, want 200 {\"status\":\"ok\"}", code, body)
	}
	for _, req := range h.upstream.pathCalls("/api/daemon/runtimes/") {
		if strings.HasSuffix(req.Path, "/prepare-lease") {
			t.Errorf("C4 was forwarded for a rebuilt entry: %s", req.Path)
		}
	}

	// Not settled as vanished: the entry survives for the convergence round,
	// which adopts the running task and keeps its Job.
	after, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("S7 removed the rebuilt entry")
	}
	if after.IsTerminal() {
		t.Fatalf("S7 settled the rebuilt entry as %s/%s", after.State, after.Result)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Fatal("S7 deleted the rebuilt entry's Job")
	}
	if !after.LeaseKeepUntil.Equal(rebuilt.LeaseKeepUntil) {
		t.Errorf("S7 moved lease_keep_until to %v without a server renewal", after.LeaseKeepUntil)
	}

	h.server.setStatus("task-1", "running")
	if err := h.sched.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	adopted, ok := h.reg.Get("task-1")
	if !ok || adopted.StartedAt.IsZero() {
		t.Fatalf("the rebuilt entry was not adopted by convergence: %+v", adopted)
	}
	if h.jobs.jobDeleted("fm-task-1") {
		t.Error("the adopted task's Job was deleted")
	}
}

// claimPayload is a claim as the server hands it over (S-6).
func claimPayload(taskID string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"id":%q,"agent_id":"agent-1","workspace_id":"ws-1","runtime_id":"rt-1"}`, taskID))
}

// projectedList reads the whole task list back through the ops endpoint.
func projectedList(t *testing.T, base string) []map[string]any {
	t.Helper()
	resp, err := http.Get(base + "/foreman/tasks")
	if err != nil {
		t.Fatalf("GET /foreman/tasks: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /foreman/tasks: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /foreman/tasks = %d: %s", resp.StatusCode, raw)
	}
	var entries []map[string]any
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("decode /foreman/tasks: %v", err)
	}
	return entries
}

// projectedEntry reads one task back through the ops endpoint.
func projectedEntry(t *testing.T, base, taskID string) map[string]any {
	t.Helper()
	resp, err := http.Get(base + "/foreman/tasks/" + taskID)
	if err != nil {
		t.Fatalf("GET /foreman/tasks/%s: %v", taskID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /foreman/tasks/%s: %v", taskID, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /foreman/tasks/%s = %d: %s", taskID, resp.StatusCode, raw)
	}
	var projected map[string]any
	if err := json.Unmarshal(raw, &projected); err != nil {
		t.Fatalf("decode /foreman/tasks/%s: %v", taskID, err)
	}
	return projected
}

// assertProjectedTime asserts the field's presence and its two legal forms:
// RFC3339 when the fact exists, the empty string while it does not.
func assertProjectedTime(t *testing.T, base, taskID, field string, wantValue bool) {
	t.Helper()
	got, ok := projectedEntry(t, base, taskID)[field]
	if !ok {
		t.Fatalf("%s is missing from the projection of %s", field, taskID)
	}
	if !wantValue {
		if got != "" {
			t.Errorf("%s = %v, want the empty string while the fact is absent", field, got)
		}
		return
	}
	value, ok := got.(string)
	if !ok || value == "" {
		t.Fatalf("%s = %v, want an RFC3339 instant", field, got)
	}
	if _, err := time.Parse(time.RFC3339, value); err != nil {
		t.Errorf("%s = %q is not RFC3339: %v", field, value, err)
	}
}
