// TC-tech-multi-08 (AC-73, ADR-046 决策 0): stage-one restart persistence.
// A single instance issues Job Tokens, builds the mapping and terminal
// records through the shared state face, then the process restarts — the new
// process (fresh registry, fresh issuer, same shared store, empty K8s view)
// restores the entries from the store instead of rebuilding them from K8s:
// the pre-restart tokens still verify, the mapping projection and the
// terminal records match field by field, no entry is rebuilt from the K8s
// list, and new claims and deliveries work as before.
//
// The negative face — a restart that throws the entries back to the
// in-process initial state fails — is the K8s-rebuild fallback the store
// miss exercises: with the store dropped, the restore answers nothing and
// the K8s rebuild owns recovery exactly as before stage one.
package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
	"github.com/tsic404/foreman/internal/state"
)

// multi08Store builds the stage-one shared state face over miniredis.
func multi08Store(t *testing.T) (state.Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	cfg := state.Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    48 * time.Hour,
	}
	s, err := state.New(cfg)
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	return s, mr
}

// multi08Flush drains the registry's ordered writer: the harness writes the
// shared state asynchronously (the writer is outside the registry lock), so a
// restart in the test must wait for the queue to land, exactly as a real
// process would have landed it long before the restart.
func multi08Flush(t *testing.T, h *harness) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.reg.FlushState(ctx); err != nil {
		t.Fatalf("FlushState: %v", err)
	}
}

// multi08DriveClaim runs one full claim → register → delivery → start round
// against the harness, so the shared store holds a delivered, started entry
// and its credential.
func multi08DriveClaim(t *testing.T, h *harness, taskID string) string {
	t.Helper()
	// The claim payload dispatches under rid-zz; the harness must declare the
	// runtime set the way a deployment's startup probe does (the S-4 select
	// then matches the daemon's declared provider).
	if len(h.upstream.registerIDs) == 0 {
		h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
		h.declare(declaredRuntimes())
	}
	if err := h.sched.OnClaim(t.Context(), deliveryPayload(taskID)); err != nil {
		t.Fatalf("OnClaim(%s): %v", taskID, err)
	}
	token := h.builder.probeToken("fm-" + taskID)
	if token == "" {
		t.Fatalf("%s: the builder issued no Job Token", taskID)
	}
	registerTaskDaemon(t, h, taskID, token)
	claimTask(t, h, taskID, token)
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskID+"/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8(%s) = %d (%s), want 200", taskID, code, body)
	}
	return token
}

// TestMulti08RestartPersistsTokensAndMapping drives the full AC-73 criterion:
// the restart restores the shared-store rows (tokens verify, projection and
// terminal records match, no K8s rebuild) and new claims deliver as before.
func TestMulti08RestartPersistsTokensAndMapping(t *testing.T) {
	store, _ := multi08Store(t)
	h := newHarnessWithSharedState(t, false, store)
	// The upstream stub is per-harness; the restarted harness gets its own
	// stub, and the shared store is what must carry the state across.
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	// One full lifecycle on the first process: the entry is delivered and
	// running, its token and its boot/lease/delivery facts live in the
	// shared store.
	token1 := multi08DriveClaim(t, h, "task-1")
	multi08Flush(t, h)
	before, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("task-1 missing after claim")
	}

	// Restart: a fresh process with an empty in-memory index, seeing the SAME
	// cluster the first process did. The entries must come from the shared
	// store, not from the K8s list (AC-73: 已存在的条目不再从 K8s 全量重建) —
	// the K8s rebuild cannot restore the payload or the claim-time deadlines,
	// so their survival proves the shared row won.
	h2 := newHarnessWithSharedState(t, false, store)
	h2.jobs = h.jobs
	h2.rebuildScheduler(t, func(*scheduler.Config) {})
	if err := h2.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild after restart: %v", err)
	}
	restored, ok := h2.reg.Get("task-1")
	if !ok {
		t.Fatal("restart lost the mapping entry (the shared store did not restore it)")
	}
	// AC-73's "映射投影逐字段一致": the whole projection — every key, not a
	// hand-picked subset — must match the pre-restart entry.
	beforeJSON := multi08ProjectionJSON(t, before)
	restoredJSON := multi08ProjectionJSON(t, restored)
	for key, want := range beforeJSON {
		if got, ok := restoredJSON[key]; !ok || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("projection field %s = %v after restart, want %v", key, got, want)
		}
	}
	for key := range restoredJSON {
		if _, ok := beforeJSON[key]; !ok {
			t.Errorf("projection grew field %s after restart", key)
		}
	}

	// The fresh process's stub plays the server the same way the first
	// process's did: the task is still running server-side.
	h2.upstream.enqueueTask("task-1")
	h2.upstream.setTaskStatus("task-1", "running")

	// The pre-restart token still verifies on the fresh process: a status
	// query (a non-terminal daemon face) answers 200, not 401 (AC-73:
	// 重启前签发的 Job Token 重启后照常通过校验).
	if code, body := h2.call("GET", "/api/daemon/tasks/task-1/status", token1, nil); code != http.StatusOK {
		t.Errorf("pre-restart token on the fresh process = %d (%s), want 200", code, body)
	}
	// The restored row still carries what the K8s projection cannot: the
	// delivery body and the claim-time deadlines. Their presence is the
	// "not rebuilt from K8s" discriminator.
	if len(restored.Payload) == 0 {
		t.Error("the restored row lost its delivery body (the K8s rebuild would have none)")
	}
	if !restored.BootDeadline.Equal(before.BootDeadline) || restored.BootDeadline.IsZero() {
		t.Errorf("boot deadline = %v, want the claim-time value %v", restored.BootDeadline, before.BootDeadline)
	}

	// A new claim on the fresh process delivers as before.
	token2 := multi08DriveClaim(t, h2, "task-2")
	if code, _ := h2.call("POST", "/api/daemon/tasks/task-2/start", token2, []byte(`{}`)); code != http.StatusOK {
		t.Errorf("new claim S8 on the fresh process = %d, want 200", code)
	}
}

// TestMulti08SettledRecordSurvivesRestart covers the terminal-record half of
// AC-73: the verdict and the drain state are restored, and the post-reclaim
// single-task endpoint keeps answering with the retained record.
func TestMulti08SettledRecordSurvivesRestart(t *testing.T) {
	store, _ := multi08Store(t)
	h := newHarnessWithSharedState(t, false, store)
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	// Full lifecycle to a terminal verdict: the settled record is written to
	// the shared store at the verdict instant (registry.MarkTerminal).
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.revoker = h.issuer
	h.rebuildScheduler(t, func(*scheduler.Config) {})
	token, entry := tcAuth06Settle(t, h, "task-1")
	if !entry.IsDraining() {
		t.Fatalf("entry not draining after verdict: %+v", entry)
	}
	beforeSettled, ok := h.reg.Settled("task-1")
	if !ok {
		t.Fatal("the first process writes no settled record")
	}
	// §4.3: the retention window holds the terminal projection only. The
	// projection comparison below cannot see these faces (json:"-"), so
	// assert them on the record itself. The stripped faces are the negative
	// half: the projection that remains must still carry the verdict.
	multi08AssertProjectionOnly(t, "before restart", beforeSettled)
	multi08AssertProjectionContent(t, "before restart", beforeSettled,
		registry.StateTerminal, registry.ResultCompleted)

	// Reclaim the credential on the first process (revoke → delete Secret →
	// drop the entry): the live row is gone, the settled record stays in the
	// shared store.
	if err := h.sched.OnJobTerminated(t.Context(), entry.JobName,
		scheduler.JobTerminationReasonJobTerminated, time.Now().Add(-time.Second), 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if _, ok := h.reg.Get("task-1"); ok {
		t.Fatal("live entry survived the reclaim")
	}
	multi08Flush(t, h)

	// The retention window's own row, read straight from the shared store
	// (GetSettled strips nothing): the restart path strips on read, so only
	// this read judges what the done key actually holds (§4.3).
	row, ok, err := store.GetSettled(t.Context(), beforeSettled.WorkspaceID, "task-1")
	if err != nil {
		t.Fatalf("GetSettled: %v", err)
	}
	if !ok {
		t.Fatal("the retention window holds no settled row after the reclaim")
	}
	multi08AssertProjectionOnly(t, "shared settled row", row)
	multi08AssertProjectionContent(t, "shared settled row", row,
		registry.StateTerminal, registry.ResultCompleted)

	// The post-reclaim answer face is that record (the live row is gone, so
	// the endpoint falls through to the retention window): it must serve the
	// verdict, not an empty row.
	code, raw := httpGet(t, ops.URL+"/foreman/tasks/task-1")
	if code != http.StatusOK {
		t.Errorf("post-reclaim GET /foreman/tasks/task-1 = %d (%s), want 200", code, raw)
	} else {
		var projected map[string]any
		if err := json.Unmarshal(raw, &projected); err != nil {
			t.Fatalf("decode retained projection: %v (%s)", err, raw)
		}
		if projected["state"] != string(registry.StateTerminal) || projected["result"] != string(registry.ResultCompleted) {
			t.Errorf("retained projection state/result = %v/%v, want terminal/completed",
				projected["state"], projected["result"])
		}
	}

	// Restart: the settled record must restore the post-reclaim answer, and
	// its projection must match field by field (AC-73's 终态记录逐字段一致).
	h2 := newHarnessWithSharedState(t, false, store)
	if err := h2.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild after restart: %v", err)
	}
	settled, ok := h2.reg.Settled("task-1")
	if !ok {
		t.Fatal("settled record lost across restart")
	}
	multi08AssertProjectionOnly(t, "after restart", settled)
	multi08AssertProjectionContent(t, "after restart", settled,
		registry.StateTerminal, registry.ResultCompleted)
	wantJSON := multi08ProjectionJSON(t, beforeSettled)
	gotJSON := multi08ProjectionJSON(t, settled)
	for key, want := range wantJSON {
		if got, ok := gotJSON[key]; !ok || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("settled field %s = %v after restart, want %v", key, got, want)
		}
	}
	for key := range gotJSON {
		if _, ok := wantJSON[key]; !ok {
			t.Errorf("settled record grew field %s after restart", key)
		}
	}
	// The reclaimed token's terminal replay is the idempotent 200 on the
	// fresh process too (the done index owns it, and the revocation the
	// first process wrote survives in the shared store).
	if code, body := h2.call("POST", "/api/daemon/tasks/task-1/complete", token,
		[]byte(`{"exit_code":0}`)); code != http.StatusOK {
		t.Errorf("post-reclaim terminal replay on the fresh process = %d (%s), want 200", code, body)
	}
}

// TestMulti08SettledRecordStripsDecidedBody makes §4.3's 定论体 face of the
// projection discriminating. The live mapping record is given the instance's
// decided body (§4.4) before the verdict write, so the settled projection is
// derived from a row that really holds one — the shape the other scenarios
// cannot produce, since a boundary report registers its body after the
// verdict. All three settled faces (local index, shared done key, post-restart
// restore) must still carry no part of it, while the live row keeps the body
// for the drain's terminal replay.
func TestMulti08SettledRecordStripsDecidedBody(t *testing.T) {
	store, _ := multi08Store(t)
	h := newHarnessWithSharedState(t, false, store)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.revoker = h.issuer
	h.rebuildScheduler(t, func(*scheduler.Config) {})

	multi08DriveClaim(t, h, "task-1")
	decided := []byte(`{"exit_code":0,"result":"completed"}`)
	if _, ok := h.reg.RegisterDecidedReport("task-1", decided); !ok {
		t.Fatal("RegisterDecidedReport: task-1 has no live entry")
	}
	// The convergence settlement is the verdict write for an entry whose
	// decided body is already on the mapping record (C13 already-terminal).
	if err := h.sched.SettleTerminal(t.Context(), "task-1", string(registry.ResultCompleted)); err != nil {
		t.Fatalf("SettleTerminal: %v", err)
	}
	live, ok := h.reg.Get("task-1")
	if !ok || !live.IsDraining() {
		t.Fatalf("task-1 not draining after the verdict: %+v", live)
	}
	// The fixture's positive control: the row the projection was taken from
	// holds the body, and the live row keeps it.
	if !bytes.Equal(live.DecidedReport, decided) {
		t.Fatalf("live row decided body = %q, want %q", live.DecidedReport, decided)
	}
	multi08Flush(t, h)

	beforeSettled, ok := h.reg.Settled("task-1")
	if !ok {
		t.Fatal("the first process writes no settled record")
	}
	multi08AssertProjectionOnly(t, "before restart", beforeSettled)
	multi08AssertProjectionContent(t, "before restart", beforeSettled,
		registry.StateTerminal, registry.ResultCompleted)

	// Reclaim the credential: the live row goes, the settled record stays.
	if err := h.sched.OnJobTerminated(t.Context(), live.JobName,
		scheduler.JobTerminationReasonJobTerminated, time.Now().Add(-time.Second), 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if _, ok := h.reg.Get("task-1"); ok {
		t.Fatal("live entry survived the reclaim")
	}
	multi08Flush(t, h)
	row, ok, err := store.GetSettled(t.Context(), live.WorkspaceID, "task-1")
	if err != nil {
		t.Fatalf("GetSettled: %v", err)
	}
	if !ok {
		t.Fatal("the retention window holds no settled row after the reclaim")
	}
	multi08AssertProjectionOnly(t, "shared settled row", row)
	multi08AssertProjectionContent(t, "shared settled row", row,
		registry.StateTerminal, registry.ResultCompleted)

	h2 := newHarnessWithSharedState(t, false, store)
	if err := h2.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild after restart: %v", err)
	}
	settled, ok := h2.reg.Settled("task-1")
	if !ok {
		t.Fatal("settled record lost across restart")
	}
	multi08AssertProjectionOnly(t, "after restart", settled)
	multi08AssertProjectionContent(t, "after restart", settled,
		registry.StateTerminal, registry.ResultCompleted)
}

// TestMulti08RevocationSurvivesRestart covers the revocation half of AC-73:
// a digest revoked before the restart stays revoked after it, because the
// revocation set lives in the shared store (§6.1 跨实例一致, stage-one
// restart persistence).
func TestMulti08RevocationSurvivesRestart(t *testing.T) {
	store, _ := multi08Store(t)
	h := newHarnessWithSharedState(t, false, store)
	token := multi08DriveClaim(t, h, "task-1")

	// Revoke through the shared issuer: the digest lands in the store.
	h.revoker = h.issuer
	h.rebuildScheduler(t, func(*scheduler.Config) {})
	h.issuer.Revoke(h.issuer.Hash(token), "ws-1")
	if code, _ := h.call("GET", "/api/daemon/tasks/task-1/status", token, nil); code != http.StatusUnauthorized {
		t.Fatalf("revoked token on the first process = %d, want 401", code)
	}
	multi08Flush(t, h)

	// Restart: the fresh process restores both faces the adjudication needs —
	// the entry index (Rebuild, as every other scenario does) and the
	// revocation set seeded from the shared store (RestoreRevocations). The
	// entry has to exist: without it Verify answers ErrUnknownJob before it
	// ever consults the revocation set, so a 401 would prove nothing about the
	// write-through.
	h2 := newHarnessWithSharedState(t, false, store)
	h2.jobs = h.jobs
	h2.rebuildScheduler(t, func(*scheduler.Config) {})
	if err := h2.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild after restart: %v", err)
	}
	if _, ok := h2.reg.Get("task-1"); !ok {
		t.Fatal("restart lost the mapping entry: ErrUnknownJob would mask the revocation check")
	}
	if err := h2.issuer.RestoreRevocations(t.Context()); err != nil {
		t.Fatalf("RestoreRevocations: %v", err)
	}
	// The discriminator is the adjudication verdict, not the status code:
	// only a restored entry reaches the revocation set, so ErrRevoked proves
	// the digest came back from the shared store. With the Revoke
	// write-through removed the fresh issuer knows no revocation and the
	// token verifies.
	if _, err := h2.issuer.Verify(token, auth.RequestScope{}); !errors.Is(err, auth.ErrRevoked) {
		t.Fatalf("revoked token after restart: Verify err = %v, want ErrRevoked", err)
	}
	if code, body := h2.call("GET", "/api/daemon/tasks/task-1/status", token, nil); code != http.StatusUnauthorized {
		t.Errorf("revoked token after restart = %d (%s), want 401", code, body)
	}
}

// TestMulti08RevocationSeedAdjudicatesWithoutSharedRead closes the gap the
// restart scenario above leaves open: there the shared read is still up, so a
// seed that flattens ListRevoked's workspace grouping is masked — the
// per-request lookup answers the correct verdict and the test stays green.
// Here the shared face fails its reads after the seed (the §10 outage shape),
// so the restored in-process set is the only adjudication source and the
// grouping is what decides.
func TestMulti08RevocationSeedAdjudicatesWithoutSharedRead(t *testing.T) {
	store, mr := multi08Store(t)
	h := newHarnessWithSharedState(t, false, store)
	token := multi08DriveClaim(t, h, "task-1")

	h.revoker = h.issuer
	h.rebuildScheduler(t, func(*scheduler.Config) {})
	h.issuer.Revoke(h.issuer.Hash(token), "ws-1")
	multi08Flush(t, h)

	// Restart: the fresh process restores the entry index and seeds the
	// revocation set, exactly as cmd/foreman's startup does.
	h2 := newHarnessWithSharedState(t, false, store)
	h2.jobs = h.jobs
	h2.rebuildScheduler(t, func(*scheduler.Config) {})
	if err := h2.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild after restart: %v", err)
	}
	if _, ok := h2.reg.Get("task-1"); !ok {
		t.Fatal("restart lost the mapping entry: ErrUnknownJob would mask the revocation check")
	}
	if err := h2.issuer.RestoreRevocations(t.Context()); err != nil {
		t.Fatalf("RestoreRevocations: %v", err)
	}

	// The shared read goes down after the seed: every per-request store read
	// fails, so only the seeded set can reject the token.
	mr.SetError("LOADING foreman shared state is loading")
	// Self-proof of the premise the verdict rests on: with the injection gone
	// the store fallback would answer and the assertion below would hold for
	// a seed that never ran.
	if _, err := store.IsRevoked(t.Context(), "ws-1", h2.issuer.Hash(token)); err == nil {
		t.Fatal("shared read still answers after SetError: the seed is not the only adjudication source")
	}
	if _, err := h2.issuer.Verify(token, auth.RequestScope{}); !errors.Is(err, auth.ErrRevoked) {
		t.Fatalf("seeded revocation with the shared read down: Verify err = %v, want ErrRevoked", err)
	}
}

// TestMulti08UndeliveredTaskDeliversAfterRestart covers the delivery half of
// AC-73: a claimed-but-not-yet-delivered task's payload is part of the mapping
// record in the shared store (§4.3: 任务载荷存在共享状态的条目里), so the
// restarted process delivers it instead of logging task.payload_missing.
func TestMulti08UndeliveredTaskDeliversAfterRestart(t *testing.T) {
	store, _ := multi08Store(t)
	h := newHarnessWithSharedState(t, false, store)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	// Claim + register only: the payload is still in the entry, undelivered.
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := h.builder.probeToken("fm-task-1")
	registerTaskDaemon(t, h, "task-1", token)
	before, _ := h.reg.Get("task-1")
	if before.DeliveredAt.IsZero() != true {
		t.Fatalf("fixture error: task-1 was already delivered")
	}
	multi08Flush(t, h)

	// Restart: the fresh process restores the entry, including its payload.
	h2 := newHarnessWithSharedState(t, false, store)
	if err := h2.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	restored, ok := h2.reg.Get("task-1")
	if !ok {
		t.Fatal("entry lost across restart")
	}
	if len(restored.Payload) == 0 {
		t.Fatal("the undelivered payload was not restored")
	}
	// The daemon's claim delivers the payload on the fresh process.
	raw := claimTask(t, h2, "task-1", token)
	var answer struct {
		Tasks []json.RawMessage `json:"tasks"`
	}
	if err := json.Unmarshal(raw, &answer); err != nil {
		t.Fatalf("decode claim answer: %v", err)
	}
	if len(answer.Tasks) != 1 {
		t.Fatalf("post-restart claim delivered %d tasks, want 1", len(answer.Tasks))
	}
	if !bytesContains(answer.Tasks[0], `"id":"task-1"`) && !bytesContains(answer.Tasks[0], `"id": "task-1"`) {
		t.Errorf("delivered payload lost its task id: %s", answer.Tasks[0])
	}
}

// bytesContains is a local substring helper for the raw payload check.
func bytesContains(b []byte, sub string) bool {
	return strings.Contains(string(b), sub)
}

// TestMulti08StoreLossFallsBackToK8sRebuild covers the negative face of
// AC-73: when the shared store is gone, the restore answers nothing and the
// K8s rebuild owns recovery (the disposal table, §4.3) exactly as before
// stage one.
func TestMulti08StoreLossFallsBackToK8sRebuild(t *testing.T) {
	store, mr := multi08Store(t)
	h := newHarnessWithSharedState(t, false, store)
	h.useRealBuilder(t)
	if err := h.sched.OnClaim(t.Context(), claimPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}

	// Drain the write queue first (the writer runs outside the registry
	// lock), then lose the store: its rows are gone, the K8s Job is not. The
	// fresh harness sees the same cluster view the first process had (the K8s
	// rebuild reads the Jobs back, §4.3).
	multi08Flush(t, h)
	mr.FlushAll()
	h2 := newHarnessWithSharedState(t, false, store)
	// The cluster is not lost — the fresh process sees the same Jobs. The
	// scheduler is rebuilt over the shared job client so its K8s rebuild
	// reads them.
	h2.jobs = h.jobs
	h2.rebuildScheduler(t, func(*scheduler.Config) {})
	if err := h2.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild after store loss: %v", err)
	}
	// The rebuilt entry comes from the K8s Job (labels + annotations), the
	// same path stage one replaced for the store-backed rows.
	rebuilt, ok := h2.reg.Get("task-1")
	if !ok {
		t.Fatal("store loss left the task unrecovered")
	}
	if rebuilt.JobName != "fm-task-1" || rebuilt.State != registry.StateJobCreated {
		t.Errorf("rebuilt entry = %s/%s, want the K8s-rebuilt form (fm-task-1, job_created)",
			rebuilt.JobName, rebuilt.State)
	}
	if len(rebuilt.Payload) != 0 {
		t.Fatal("a K8s-rebuilt entry must not carry a payload")
	}

	// The disposal table (§4.3) then runs: the server still holds the task
	// pre-delivery (queued), so the rebuilt row is the "投递前" class — the
	// Job and the credential go, the entry is dropped, and the server
	// re-dispatches. No second Job may be created by the rebuild itself.
	jobsBefore := len(h.jobs.createdJobs)
	h2.upstream.enqueueTask("task-1")
	h2.upstream.setTaskStatus("task-1", "queued")
	if err := h2.sched.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile after store loss: %v", err)
	}
	if _, ok := h2.reg.Get("task-1"); ok {
		t.Error("the pre-delivery rebuilt entry was not discarded")
	}
	if !h2.jobs.jobDeleted("fm-task-1") {
		t.Error("the pre-delivery rebuilt Job was not deleted")
	}
	if got := len(h.jobs.createdJobs); got != jobsBefore {
		t.Errorf("the rebuild created %d extra Jobs, want none", got-jobsBefore)
	}
}

// multi08AssertProjectionOnly asserts a settled record retains none of the
// non-projected faces: the retention window must never hold a delivery body,
// a token digest or a decided terminal body (§4.3). The projection (json:"-"
// on these fields) cannot express the boundary, so it is asserted directly.
func multi08AssertProjectionOnly(t *testing.T, label string, e registry.TaskEntry) {
	t.Helper()
	if e.Payload != nil {
		t.Errorf("%s: settled record retains a delivery body (%d bytes)", label, len(e.Payload))
	}
	if e.TokenHash != "" {
		t.Errorf("%s: settled record retains a token digest %q", label, e.TokenHash)
	}
	if e.DecidedReport != nil {
		t.Errorf("%s: settled record retains a decided terminal body (%d bytes)", label, len(e.DecidedReport))
	}
}

// multi08AssertProjectionContent asserts the retained record still carries the
// projection the strip leaves in place: the terminal state and the verdict's
// result. Without it a record stripped of its own content passes every absence
// check, and the retention window answers a blank row.
func multi08AssertProjectionContent(t *testing.T, label string, e registry.TaskEntry,
	wantState registry.State, wantResult registry.Result) {
	t.Helper()
	if e.State != wantState {
		t.Errorf("%s: retained state = %q, want %q", label, e.State, wantState)
	}
	if e.Result != wantResult {
		t.Errorf("%s: retained result = %q, want %q", label, e.Result, wantResult)
	}
}

// multi08ProjectionJSON renders the entry through the same projection the ops
// endpoint serves (the projection is TaskEntry's own JSON).
func multi08ProjectionJSON(t *testing.T, e registry.TaskEntry) map[string]any {
	t.Helper()
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal projection: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal projection: %v", err)
	}
	return out
}
