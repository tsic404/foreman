// TC-tech-auth-06 (AC-30, ADR-017): the Job Token's lifetime follows the Job,
// not the task verdict: after the terminal report the entry stays in drain and
// its closing claim, deregister and status poll are accepted (delivery empty).
// The reclaim (revoke → delete Secret → drop the entry) runs only when the pods
// terminated (or the drain bound expires), and it makes the token unusable:
// non-terminal endpoints answer 401 for own and cross-task requests alike; a
// terminal replay is the idempotent 200 only for the token's own task (§2.1
// rule 1 403 otherwise) and never writes twice; retry-handle and sweep follow.
package acceptance

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// tcAuth06Settle drives one task through claim → register → claim → start →
// complete and returns the daemon token and the settled entry.
func tcAuth06Settle(t *testing.T, h *harness, taskID string) (string, registry.TaskEntry) {
	t.Helper()
	if err := h.sched.OnClaim(t.Context(), deliveryPayload(taskID)); err != nil {
		t.Fatalf("OnClaim(%s): %v", taskID, err)
	}
	// The Job's own token (the one the builder issued and OnClaim recorded the
	// digest of): the reclaim's revoke step names exactly this digest.
	token := h.builder.probeToken("fm-" + taskID)
	if token == "" {
		t.Fatalf("%s: the builder issued no Job Token", taskID)
	}
	registerTaskDaemon(t, h, taskID, token)
	claimTask(t, h, taskID, token)
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8(%s) = %d (%s), want 200", taskID, code, body)
	}
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/complete", token,
		[]byte(`{"exit_code":0}`)); code != http.StatusOK {
		t.Fatalf("S13(%s) = %d (%s), want 200", taskID, code, body)
	}
	entry := waitTerminal(t, h, taskID)
	if !entry.IsDraining() {
		t.Fatalf("%s: entry is not draining after its verdict: %+v", taskID, entry)
	}
	return token, entry
}

// tcAuth06ClaimCount returns the number of tasks one claim answered.
func tcAuth06ClaimCount(t *testing.T, h *harness, taskID, token string) int {
	t.Helper()
	raw := claimTask(t, h, taskID, token)
	var answer struct {
		Tasks []json.RawMessage `json:"tasks"`
	}
	if err := json.Unmarshal(raw, &answer); err != nil {
		t.Fatalf("decode claim answer: %v", err)
	}
	return len(answer.Tasks)
}

func TestAuth06TokenLifetimeFollowsJobTermination(t *testing.T) {
	h := newDeliveryHarness(t)
	h.revoker = h.issuer
	h.rebuildScheduler(t, func(*scheduler.Config) {})

	token, entry := tcAuth06Settle(t, h, "task-1")
	if !h.jobs.jobDeleted(entry.JobName) {
		t.Fatal("the verdict-time Job deletion did not run")
	}
	// The claim's delete-then-create cleanup may already have recorded one
	// deletion; the verdict must not add another (the drain window owns the
	// Secret).
	secretDeletesAtVerdict := len(h.jobs.deletedSecretNames())

	// Drain window: the daemon's closing calls are normal paths, not 401s.
	h.upstream.setTaskStatus("task-1", "completed")
	for _, call := range []struct {
		method, path string
		body         []byte
	}{
		{http.MethodGet, "/api/daemon/tasks/task-1/status", nil},
		{http.MethodPost, "/api/daemon/deregister", []byte(`{}`)},
	} {
		if code, body := h.call(call.method, call.path, token, call.body); code != http.StatusOK {
			t.Errorf("%s %s in drain = %d (%s), want 200", call.method, call.path, code, body)
		}
	}
	if got := tcAuth06ClaimCount(t, h, "task-1", token); got != 0 {
		t.Errorf("drain claim delivered %d tasks, want an empty batch", got)
	}

	// Reclaim driven by the pod termination fact: the token was usable up to
	// this point (the drain window), and the reclaim closes it.
	podTerminatedAt := time.Now().Add(-2 * time.Second)
	reclaimAt := time.Now()
	if err := h.sched.OnJobTerminated(t.Context(), entry.JobName,
		scheduler.JobTerminationReasonJobTerminated, podTerminatedAt, 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if podTerminatedAt.After(reclaimAt) {
		t.Fatal("fixture error: the termination fact must precede the reclaim")
	}
	if _, ok := h.reg.Get("task-1"); ok {
		t.Error("the entry survived the reclaim")
	}
	if !h.reg.IsDone("ws-1", "task-1") {
		t.Error("the done index lost the terminal record")
	}
	if got := len(h.jobs.deletedSecretNames()); got != secretDeletesAtVerdict+1 {
		t.Errorf("Secret deletions after the reclaim = %d, want one more than the verdict's %d",
			got, secretDeletesAtVerdict)
	}

	// After the reclaim: non-terminal endpoints 401, the terminal replay is
	// the idempotent 200 answered by the done index — and it re-forwards
	// nothing, so the server sees no second terminal write.
	if code, body := h.call("POST", "/api/daemon/tasks/claim", token,
		[]byte(`{"daemon_id":"fm-task-1","runtime_ids":["`+entry.JobRuntimeID+`"],"max_tasks":1}`)); code != http.StatusUnauthorized {
		t.Errorf("claim after reclaim = %d (%s), want 401", code, body)
	}
	if code, body := h.call("POST", "/api/daemon/deregister", token, []byte(`{}`)); code != http.StatusUnauthorized {
		t.Errorf("deregister after reclaim = %d (%s), want 401", code, body)
	}
	forwardsBefore := len(h.server.forwarded(scheduler.EPComplete))
	acceptedBefore := h.server.acceptedFor(scheduler.EPComplete)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token,
		[]byte(`{"exit_code":0}`)); code != http.StatusOK {
		t.Errorf("terminal replay after reclaim = %d (%s), want the idempotent 200", code, body)
	}
	if got := len(h.server.forwarded(scheduler.EPComplete)); got != forwardsBefore {
		t.Errorf("terminal replay forwarded C9 again: %d → %d forwards, want no second write",
			forwardsBefore, got)
	}
	if got := h.server.acceptedFor(scheduler.EPComplete); got != acceptedBefore {
		t.Errorf("terminal replay applied a second terminal write: %d → %d accepted, want no second write",
			acceptedBefore, got)
	}
	if _, ok := h.reg.Get("task-1"); ok {
		t.Error("the replay resurrected the reclaimed entry")
	}

	// Evidence: the reclaim names the Job-termination reason, the kubelet
	// fact and zero orphan pods; the upper-bound reason stays at zero.
	events := h.events("cred.reclaimed")
	if len(events) != 1 {
		t.Fatalf("cred.reclaimed events = %d, want 1", len(events))
	}
	event := events[0]
	if event["reason"] != scheduler.JobTerminationReasonJobTerminated {
		t.Errorf("reclaim reason = %v, want %q", event["reason"], scheduler.JobTerminationReasonJobTerminated)
	}
	if event["pod_terminated_at_source"] != scheduler.PodTerminatedAtSourceK8s {
		t.Errorf("pod_terminated_at_source = %v, want %q", event["pod_terminated_at_source"], scheduler.PodTerminatedAtSourceK8s)
	}
	if event["pod_terminated_at"] != podTerminatedAt.UTC().Format(time.RFC3339) {
		t.Errorf("pod_terminated_at = %v, want the kubelet fact %s",
			event["pod_terminated_at"], podTerminatedAt.UTC().Format(time.RFC3339))
	}
	if event["orphan_pods"] != float64(0) {
		t.Errorf("orphan_pods = %v, want 0", event["orphan_pods"])
	}
	if got := h.metrics.credReclaimCount(scheduler.JobTerminationReasonJobTerminated); got != 1 {
		t.Errorf("reclaim metric{job_terminated} = %d, want 1", got)
	}
	if got := h.metrics.credReclaimCount(scheduler.JobTerminationReasonDrainTimeout); got != 0 {
		t.Errorf("reclaim metric{drain_timeout} = %d, want 0", got)
	}
}

// TestAuth06ReclaimedTokenAdjudicatesCrossTaskRequests pins §6.4's two
// post-reclaim verdicts. Non-terminal endpoints answer 401 for own and
// cross-task requests alike (the entry's absence, not the resource, is the
// verdict), while a terminal replay is the idempotent 200 only for the token's
// own task: every other task's terminal path is §2.1 rule 1's 403 — never a
// replay, and never a forward or a settlement of the other task.
func TestAuth06ReclaimedTokenAdjudicatesCrossTaskRequests(t *testing.T) {
	h := newDeliveryHarness(t)
	h.revoker = h.issuer
	h.rebuildScheduler(t, func(*scheduler.Config) {})

	tokenA, entryA := tcAuth06Settle(t, h, "task-a")
	// A second live task is the cross-task target: it must stay non-terminal
	// through every refused request.
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-b")); err != nil {
		t.Fatalf("OnClaim(task-b): %v", err)
	}
	entryB, ok := h.reg.Get("task-b")
	if !ok || entryB.IsTerminal() {
		t.Fatalf("task-b entry = %+v/%v, want a live non-terminal entry", entryB, ok)
	}

	if err := h.sched.OnJobTerminated(t.Context(), entryA.JobName,
		scheduler.JobTerminationReasonJobTerminated, time.Now(), 0); err != nil {
		t.Fatalf("OnJobTerminated(%s): %v", entryA.JobName, err)
	}
	if _, ok := h.reg.Get("task-a"); ok {
		t.Fatal("the entry survived the reclaim")
	}

	// Non-terminal endpoints: the reclaimed token is an unknown Job whether
	// the request names its own task, another task, or another Job's runtime.
	for _, call := range []struct {
		name, method, path string
		body               []byte
	}{
		{"own status", http.MethodGet, "/api/daemon/tasks/task-a/status", nil},
		{"cross-task status", http.MethodGet, "/api/daemon/tasks/task-b/status", nil},
		{"own prepare-lease", http.MethodPost,
			"/api/daemon/runtimes/" + entryA.JobRuntimeID + "/tasks/task-a/prepare-lease", []byte(`{}`)},
		{"cross-runtime prepare-lease", http.MethodPost,
			"/api/daemon/runtimes/" + entryB.JobRuntimeID + "/tasks/task-a/prepare-lease", []byte(`{}`)},
		{"own deregister", http.MethodPost, "/api/daemon/deregister", []byte(`{}`)},
		{"cross-runtime claim", http.MethodPost, "/api/daemon/tasks/claim",
			[]byte(`{"daemon_id":"fm-task-a","runtime_ids":["` + entryB.JobRuntimeID + `"],"max_tasks":1}`)},
	} {
		if code, body := h.call(call.method, call.path, tokenA, call.body); code != http.StatusUnauthorized {
			t.Errorf("%s after reclaim = %d (%s), want 401", call.name, code, body)
		}
	}

	// Terminal replay: only the token's own task path reaches the done index;
	// each cross-task terminal endpoint is the scope mismatch, counted once.
	scopeBefore := h.metrics.authFailureCount("scope")
	for _, ep := range []scheduler.Endpoint{scheduler.EPComplete, scheduler.EPFail, scheduler.EPCancelAck} {
		forwardsBefore := len(h.server.forwarded(ep))
		code, body := h.call(http.MethodPost, "/api/daemon/tasks/task-b/"+string(ep), tokenA,
			[]byte(`{"exit_code":0}`))
		if code != http.StatusForbidden {
			t.Errorf("cross-task %s replay = %d (%s), want 403", ep, code, body)
		}
		if got := len(h.server.forwarded(ep)); got != forwardsBefore {
			t.Errorf("cross-task %s was forwarded (%d → %d), want the refused request never to reach the server",
				ep, forwardsBefore, got)
		}
	}
	if got := h.metrics.authFailureCount("scope") - scopeBefore; got != 3 {
		t.Errorf("scope auth failures for the cross-task replays = %d, want 3", got)
	}
	if entry, ok := h.reg.Get("task-b"); !ok || entry.IsTerminal() {
		t.Errorf("task-b entry = %+v/%v, want it untouched by the refused replays", entry, ok)
	}
}

func TestAuth06SecretDeletionFailureKeepsTheRetryHandle(t *testing.T) {
	h := newDeliveryHarness(t)
	h.revoker = h.issuer
	h.rebuildScheduler(t, func(*scheduler.Config) {})

	token, entry := tcAuth06Settle(t, h, "task-2")

	deletesBefore := len(h.jobs.deletedSecretNames())
	h.jobs.setDeleteSecretError(errors.New("apiserver unavailable"))
	if err := h.sched.OnJobTerminated(t.Context(), entry.JobName,
		scheduler.JobTerminationReasonJobTerminated, time.Now(), 0); err == nil {
		t.Fatal("OnJobTerminated: want the Secret deletion error surfaced")
	}
	if _, ok := h.reg.Get("task-2"); !ok {
		t.Fatal("the entry was dropped before its Secret: the retry handle is gone")
	}
	if got := len(h.jobs.deletedSecretNames()); got != deletesBefore {
		t.Fatalf("Secret deletions = %d, want the failed deletion not recorded (%d)", got, deletesBefore)
	}
	// The revoke step precedes the Secret deletion: the token is already
	// unusable even though the entry survives.
	if code, body := h.call("POST", "/api/daemon/deregister", token, []byte(`{}`)); code != http.StatusUnauthorized {
		t.Errorf("deregister after the revoke = %d (%s), want 401", code, body)
	}

	h.jobs.setDeleteSecretError(nil)
	if err := h.sched.OnJobTerminated(t.Context(), entry.JobName,
		scheduler.JobTerminationReasonJobTerminated, time.Now(), 0); err != nil {
		t.Fatalf("OnJobTerminated retry: %v", err)
	}
	if _, ok := h.reg.Get("task-2"); ok {
		t.Error("the entry survived the successful retry")
	}
	if got := len(h.jobs.deletedSecretNames()); got != deletesBefore+1 {
		t.Errorf("Secret deletions after the retry = %d, want %d", got, deletesBefore+1)
	}
}

func TestAuth06DrainUpperBoundReclaimsWithoutATerminationFact(t *testing.T) {
	h := newDeliveryHarness(t)
	h.revoker = h.issuer
	h.rebuildScheduler(t, func(*scheduler.Config) {})

	_, entry := tcAuth06Settle(t, h, "task-3")
	if err := h.sched.OnJobTerminated(t.Context(), entry.JobName,
		scheduler.JobTerminationReasonDrainTimeout, time.Time{}, 1); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	events := h.events("cred.reclaimed")
	if len(events) != 1 {
		t.Fatalf("cred.reclaimed events = %d, want 1", len(events))
	}
	event := events[0]
	if event["reason"] != scheduler.JobTerminationReasonDrainTimeout {
		t.Errorf("reclaim reason = %v, want %q", event["reason"], scheduler.JobTerminationReasonDrainTimeout)
	}
	if event["pod_terminated_at"] != "" {
		t.Errorf("pod_terminated_at = %v, want empty on the upper-bound path", event["pod_terminated_at"])
	}
	if event["orphan_pods"] != float64(1) {
		t.Errorf("orphan_pods = %v, want 1", event["orphan_pods"])
	}
	if got := h.metrics.credReclaimCount(scheduler.JobTerminationReasonDrainTimeout); got != 1 {
		t.Errorf("reclaim metric{drain_timeout} = %d, want 1", got)
	}
}

func TestAuth06OrphanSecretSweepLeavesTheNegativeShapes(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.reg.Put(registry.TaskEntry{
		TaskID: "task-live", JobName: "fm-task-live", State: registry.StatePending,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	secret := func(name string, labels map[string]string) corev1.Secret {
		return corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
	}
	managed := map[string]string{registry.LabelManagedBy: registry.ManagedByForeman}
	// One helper per label shape: the workspace partition must come from the
	// object itself, so each shape is a separate negative.
	withTaskWorkspace := func(taskID, workspaceID string) map[string]string {
		return map[string]string{
			registry.LabelManagedBy:   registry.ManagedByForeman,
			registry.LabelTaskID:      taskID,
			registry.LabelWorkspaceID: workspaceID,
		}
	}
	withTask := func(taskID string) map[string]string {
		return withTaskWorkspace(taskID, "ws-1")
	}
	withTaskNoWorkspace := func(taskID string) map[string]string {
		return map[string]string{
			registry.LabelManagedBy: registry.ManagedByForeman,
			registry.LabelTaskID:    taskID,
		}
	}
	h.jobs.programListed(nil, []corev1.Secret{
		// Positive: name and task-id agree, no entry, no Job.
		secret("fm-task-orphan-cred", withTask("task-orphan")),
		// Negative: no task-id label at all.
		secret("fm-task-nolabel-cred", managed),
		// Negative: empty task-id value.
		secret("fm-task-empty-cred", withTask("")),
		// Negative: the label disagrees with the name.
		secret("fm-task-other-cred", withTask("task-orphan")),
		// Negative: a live entry owns it.
		secret("fm-task-live-cred", withTask("task-live")),
		// Negative: no workspace identity label — there is no partition, and
		// the deployment's own workspace must not stand in for it.
		secret("fm-task-nows-cred", withTaskNoWorkspace("task-nows")),
		// Negative: the workspace identity label is present but empty.
		secret("fm-task-emptyws-cred", withTaskWorkspace("task-emptyws", "")),
		// Negative: another workspace's orphan — this deployment does not
		// serve ws-2, so the object is outside its sweep domain.
		secret("fm-task-foreignws-cred", withTaskWorkspace("task-foreignws", "ws-2")),
	})

	// Two convergence rounds: the sweep is a round action, and the positive
	// orphan must be gone within the bound while the negatives survive both.
	for round := 1; round <= 2; round++ {
		if err := h.sched.SweepOrphanSecrets(t.Context()); err != nil {
			t.Fatalf("SweepOrphanSecrets round %d: %v", round, err)
		}
	}
	deleted := h.jobs.deletedSecretNames()
	if len(deleted) != 1 || deleted[0] != "fm-task-orphan-cred" {
		t.Fatalf("deleted Secrets = %v, want only fm-task-orphan-cred", deleted)
	}
	events := h.events("cred.orphan_secret_removed")
	if len(events) != 1 || events[0]["task_id"] != "task-orphan" {
		t.Fatalf("cred.orphan_secret_removed = %+v, want one event for task-orphan", events)
	}
	// The two unpartitioned shapes (missing and empty workspace label) are
	// skipped fail-closed and reported once per round with the round's count.
	// The foreign workspace is partitioned, so it is neither swept nor counted.
	skipped := h.events("cred.orphan_secret_workspace_unlabelled")
	if len(skipped) != 2 {
		t.Fatalf("cred.orphan_secret_workspace_unlabelled = %+v, want one event per round", skipped)
	}
	for round, event := range skipped {
		if event["count"] != float64(2) {
			t.Errorf("round %d skipped count = %v, want 2", round+1, event["count"])
		}
	}
}
