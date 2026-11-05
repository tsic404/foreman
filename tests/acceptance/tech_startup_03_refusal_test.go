// S-4 refusal faces (AC-38, AC-09): a zero-hit declaration never settles the
// entry on the spot. A Job that never registered is refused round after round
// and converges only when its boot bound expires; a Job that already
// registered keeps its registration and task; a K8s-rebuilt entry answers its
// daemon's registration because the Job annotations carry the claim's
// dispatch record. TC1–TC4 are the incident's scenarios; TC2 is the negative
// face that binds the verdict to the boot bound rather than to any one round.
package acceptance

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// s4RefusalMsg is the refusal event's msg field (§9.1).
const s4RefusalMsg = "job declared no runtime for its identity; refusing registration"

// degradedRegisterBody declares only the omp provider: the shape a Job's
// daemon produces when one CLI's version probe is killed upstream.
const degradedRegisterBody = `{
	"daemon_id": "fm-%s",
	"runtimes": [{"name": "foreman-omp", "type": "omp", "version": "0.6.1"}]
}`

// registerDeclaring performs S4 with a raw declaration body for taskID.
func registerDeclaring(h *harness, taskID, token string) (int, []byte) {
	return h.call("POST", "/api/daemon/register", token, []byte(strings.ReplaceAll(
		degradedRegisterBody, "%s", taskID)))
}

// assertNoSettlement polls for the window a detached settlement would need and
// fails as soon as the entry turns terminal or a fail report is forwarded.
func assertNoSettlement(t *testing.T, h *harness, taskID string) {
	t.Helper()
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e, ok := h.reg.Get(taskID); !ok || e.IsTerminal() {
			t.Fatalf("entry %s settled (%+v) on a refusal that must keep it", taskID, e)
		}
		if n := len(h.server.forwarded(scheduler.EPFail)); n != 0 {
			t.Fatalf("fail reports forwarded on a re-registration refusal = %d, want 0", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestStartup03RefusalKeepsRegisteredTask is TC1: an entry that already
// registered keeps its task, its registration and its Job when the next
// registration declares no runtime for its identity. Its bound half — the
// entry still converges at the boot bound after the refusal — runs in
// TestStartup03RefusedRegistrationStillConvergesAtTheBootBound.
func TestStartup03RefusalKeepsRegisteredTask(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)

	before, ok := h.reg.Get("task-1")
	if !ok || before.DaemonRegisteredAt.IsZero() {
		t.Fatalf("entry = %+v, %v; want the recorded registration", before, ok)
	}

	code, body := registerDeclaring(h, "task-1", token)
	if code != 409 {
		t.Fatalf("degraded re-registration = %d (%s), want the 409 refusal", code, body)
	}
	assertNoSettlement(t, h, "task-1")

	after, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("entry disappeared on a refusal that must keep it")
	}
	if after.State != before.State {
		t.Errorf("state = %s, want %s (a refusal is not a transition)", after.State, before.State)
	}
	if !after.DaemonRegisteredAt.Equal(before.DaemonRegisteredAt) {
		t.Errorf("daemon_registered_at = %s, want the standing %s",
			after.DaemonRegisteredAt, before.DaemonRegisteredAt)
	}
	rt, ok := h.reg.RuntimeByID(after.JobRuntimeID)
	if !ok || !rt.Online {
		t.Errorf("job runtime %s = %+v, %v; want the standing registration kept online",
			after.JobRuntimeID, rt, ok)
	}
	if h.jobs.jobDeleted(after.JobName) {
		t.Errorf("Job %s deleted by a re-registration refusal", after.JobName)
	}
}

// TestStartup03RefusedRegistrationStillConvergesAtTheBootBound is TC1's bound
// half: a refusal settles nothing, so the boot bound still owns a registered
// entry that never started. The attribution splits on whether a delivery was
// ever committed (§4.4, AC-46): never delivered ⇒ boot timeout, delivered ⇒
// delivery unconfirmed. The refusal must not short-circuit either verdict.
func TestStartup03RefusedRegistrationStillConvergesAtTheBootBound(t *testing.T) {
	const boot = 1200 * time.Millisecond

	t.Run("never delivered", func(t *testing.T) {
		h := newDeliveryHarnessWithBootBound(t, boot)
		if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
			t.Fatalf("OnClaim: %v", err)
		}
		token := issueJobToken(t, h, "task-1")
		registerTaskDaemon(t, h, "task-1", token)
		if code, body := registerDeclaring(h, "task-1", token); code != 409 {
			t.Fatalf("degraded re-registration = %d (%s), want the 409 refusal", code, body)
		}
		h.startLeaseKeeper(t)

		settled := waitTerminal(t, h, "task-1")
		if settled.Result != registry.ResultFailed || settled.FailureReason != scheduler.FailureReasonJobBootTimeout {
			t.Fatalf("verdict = result %s reason %q, want failed/%s (a refusal never delivers one)",
				settled.Result, settled.FailureReason, scheduler.FailureReasonJobBootTimeout)
		}
	})

	t.Run("delivered", func(t *testing.T) {
		h := newDeliveryHarnessWithBootBound(t, boot)
		if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
			t.Fatalf("OnClaim: %v", err)
		}
		token := issueJobToken(t, h, "task-1")
		registerTaskDaemon(t, h, "task-1", token)
		if got := claimTask(t, h, "task-1", token); !bytes.Contains(got, []byte(`"id":"task-1"`)) {
			t.Fatalf("first S6 claim = %s, want the task payload", got)
		}
		if code, body := registerDeclaring(h, "task-1", token); code != 409 {
			t.Fatalf("degraded re-registration = %d (%s), want the 409 refusal", code, body)
		}
		h.startLeaseKeeper(t)

		settled := waitTerminal(t, h, "task-1")
		if settled.Result != registry.ResultFailed || settled.FailureReason != scheduler.FailureReasonDeliveryUnconfirmed {
			t.Fatalf("verdict = result %s reason %q, want failed/%s (never %s)",
				settled.Result, settled.FailureReason,
				scheduler.FailureReasonDeliveryUnconfirmed, scheduler.FailureReasonJobBootTimeout)
		}
	})
}

// TestStartup03RefusalPersistsUntilTheBootBound is TC2's negative face: a Job
// whose daemon never declares its identity is refused round after round and is
// settled only when its boot bound expires. The verdict is the boot timeout —
// the refusal itself never decides anything, because a single round cannot
// tell a degraded declaration from an incompatible environment.
func TestStartup03RefusalPersistsUntilTheBootBound(t *testing.T) {
	const boot = 300 * time.Millisecond
	h := newDeliveryHarnessWithBootBound(t, boot)
	h.startLeaseKeeper(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")

	// Three rounds, each declaring a set without the Job's identity: every
	// one is refused and none settles the entry.
	for round := 1; round <= 3; round++ {
		if code, body := registerDeclaring(h, "task-1", token); code != 409 {
			t.Fatalf("round %d refusal = %d (%s), want 409", round, code, body)
		}
		if e, ok := h.reg.Get("task-1"); !ok || e.IsTerminal() {
			t.Fatalf("round %d settled the entry: %+v, %v", round, e, ok)
		}
	}
	if got := h.metrics.s4RefusalCount(proxy.S4RefusalFirst); got != 3 {
		t.Errorf("refusals stage=first = %d, want 3 (one per round)", got)
	}
	if got := h.metrics.s4RefusalCount(proxy.S4RefusalReRegister); got != 0 {
		t.Errorf("refusals stage=re_register = %d, want 0", got)
	}
	if got := len(h.server.forwarded(scheduler.EPFail)); got != 0 {
		t.Errorf("fail reports before the boot bound = %d, want 0", got)
	}

	// The bound expires with no successful registration: the entry converges
	// on the boot-timeout attribution, not on the refusal.
	waitFor(t, "the boot bound to settle the entry", 5*time.Second, func() bool {
		e, ok := h.reg.Get("task-1")
		return ok && e.IsTerminal()
	})
	settled, _ := h.reg.Get("task-1")
	if settled.Result != registry.ResultFailed {
		t.Fatalf("result = %s, want failed", settled.Result)
	}
	if settled.FailureReason != scheduler.FailureReasonJobBootTimeout {
		t.Errorf("failure_reason = %q, want %q", settled.FailureReason, scheduler.FailureReasonJobBootTimeout)
	}
}

// TestStartup03RebuildKeepsDispatchRecord is TC3: the Job's annotations carry
// the claim's dispatch record, so a rebuilt entry answers the registration its
// Job's daemon sends — the path that used to kill every adopted task.
func TestStartup03RebuildKeepsDispatchRecord(t *testing.T) {
	h := newDeliveryHarness(t)
	h.useRealBuilder(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	job := h.createdJob(t, "fm-task-1")
	if got := job.Annotations[registry.AnnoRealRuntimeID]; got != "rid-zz" {
		t.Errorf("%s = %q, want rid-zz", registry.AnnoRealRuntimeID, got)
	}
	if got := job.Annotations[registry.AnnoRuntimeProvider]; got != "zz" {
		t.Errorf("%s = %q, want zz", registry.AnnoRuntimeProvider, got)
	}
	if _, present := job.Annotations[registry.AnnoRuntimeProfileID]; present {
		t.Errorf("%s is written for a builtin runtime; want absent", registry.AnnoRuntimeProfileID)
	}

	// The restart that loses the shared-state row: the Job object is the only
	// carrier left, so the dispatch record must come back from it.
	if err := h.reg.Delete("task-1"); err != nil {
		t.Fatalf("drop entry: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	rebuilt, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("the Job did not rebuild an entry")
	}
	want := registry.RuntimeIdentityKey("zz", "")
	if rebuilt.RealRuntimeID != "rid-zz" || rebuilt.RuntimeIdentity != want {
		t.Errorf("rebuilt dispatch record = (%q, %q), want (rid-zz, %q)",
			rebuilt.RealRuntimeID, rebuilt.RuntimeIdentity, want)
	}

	token := issueJobToken(t, h, "task-1")
	code, body := h.call("POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-task-1",
		"runtimes": [{"name": "foreman-zz", "type": "zz", "version": "3.1.4"}]
	}`))
	if code != 200 {
		t.Fatalf("rebuilt entry's registration = %d (%s), want 200", code, body)
	}
	registered, ok := h.reg.Get("task-1")
	if !ok || registered.IsTerminal() || registered.State != registry.StateDaemonRegistered {
		t.Fatalf("entry = %+v, %v; want the registered live entry", registered, ok)
	}
	if got := len(h.server.forwarded(scheduler.EPFail)); got != 0 {
		t.Errorf("fail reports forwarded for a matching registration = %d, want 0", got)
	}
}

// TestStartup03RefusalStagesDistinguishable is TC4: the two refusal paths are
// separable by the metric's stage label and by the event's declaration fields.
func TestStartup03RefusalStagesDistinguishable(t *testing.T) {
	h := newDeliveryHarness(t)
	for _, taskID := range []string{"task-1", "task-2"} {
		if err := h.sched.OnClaim(t.Context(), deliveryPayload(taskID)); err != nil {
			t.Fatalf("OnClaim(%s): %v", taskID, err)
		}
	}
	// task-1: never registered — refused, kept until its boot bound.
	firstToken := issueJobToken(t, h, "task-1")
	if code, body := registerDeclaring(h, "task-1", firstToken); code != 409 {
		t.Fatalf("first refusal = %d (%s), want 409", code, body)
	}
	// task-2: registered, then degraded — refused, kept.
	secondToken := issueJobToken(t, h, "task-2")
	registerTaskDaemon(t, h, "task-2", secondToken)
	if code, body := registerDeclaring(h, "task-2", secondToken); code != 409 {
		t.Fatalf("re-registration refusal = %d (%s), want 409", code, body)
	}

	if got := h.metrics.s4RefusalCount(proxy.S4RefusalFirst); got != 1 {
		t.Errorf("refusals stage=first = %d, want 1", got)
	}
	if got := h.metrics.s4RefusalCount(proxy.S4RefusalReRegister); got != 1 {
		t.Errorf("refusals stage=re_register = %d, want 1", got)
	}

	events := h.handlerEvents(s4RefusalMsg)
	if len(events) != 2 {
		t.Fatalf("refusal events = %d, want 2", len(events))
	}
	byStage := map[string]map[string]any{}
	for _, event := range events {
		stage, _ := event["stage"].(string)
		byStage[stage] = event
	}
	first, ok := byStage[proxy.S4RefusalFirst]
	if !ok {
		t.Fatalf("no stage=first event among %v", events)
	}
	reRegister, ok := byStage[proxy.S4RefusalReRegister]
	if !ok {
		t.Fatalf("no stage=re_register event among %v", events)
	}
	// Both events name the declaration and the entry's own state; only the
	// already-registered one carries the standing registration instant.
	for stage, event := range byStage {
		declared, _ := event["declared_runtimes"].([]any)
		if len(declared) != 1 || declared[0] != "omp/" {
			t.Errorf("stage=%s declared_runtimes = %v, want [omp/]", stage, event["declared_runtimes"])
		}
		if event["entry_state"] == "" {
			t.Errorf("stage=%s event carries no entry_state", stage)
		}
	}
	if got, _ := first["entry_state"].(string); got != string(registry.StateJobCreated) {
		t.Errorf("stage=first entry_state = %q, want %s", got, registry.StateJobCreated)
	}
	if got, _ := reRegister["entry_state"].(string); got != string(registry.StateDaemonRegistered) {
		t.Errorf("stage=re_register entry_state = %q, want %s", got, registry.StateDaemonRegistered)
	}
	if got, _ := first["registered_at"].(string); got != "" {
		t.Errorf("stage=first registered_at = %q, want empty", got)
	}
	if got, _ := reRegister["registered_at"].(string); got == "" {
		t.Error("stage=re_register registered_at is empty, want the standing registration instant")
	}
	// The dispatch identity is the third discrimination field: both entries
	// were dispatched under the zz identity, so a non-empty value separates a
	// degraded declaration from a lost dispatch record.
	wantIdentity := registry.RuntimeIdentityKey("zz", "")
	for stage, event := range byStage {
		if got, _ := event["identity"].(string); got != wantIdentity {
			t.Errorf("stage=%s identity = %q, want %q", stage, got, wantIdentity)
		}
	}
	// §9.1's task-face line carries the daemon identity as its own field: the
	// runtime identity cannot stand in for it, and the Job name is the
	// template's display name, not the daemon's.
	if got, _ := first["daemon_id"].(string); got != "fm-task-1" {
		t.Errorf("stage=first daemon_id = %q, want fm-task-1", got)
	}
	if got, _ := reRegister["daemon_id"].(string); got != "fm-task-2" {
		t.Errorf("stage=re_register daemon_id = %q, want fm-task-2", got)
	}
	// The task-face line's own identity fields are asserted by value too: a
	// field that is only "present and non-empty" would survive its removal,
	// so each of the three is pinned to the entry it belongs to.
	for stage, want := range map[string]struct{ task, job, workspace string }{
		proxy.S4RefusalFirst:      {"task-1", "fm-task-1", "ws-1"},
		proxy.S4RefusalReRegister: {"task-2", "fm-task-2", "ws-1"},
	} {
		event := byStage[stage]
		if got, _ := event["task_id"].(string); got != want.task {
			t.Errorf("stage=%s task_id = %q, want %q", stage, got, want.task)
		}
		if got, _ := event["job_name"].(string); got != want.job {
			t.Errorf("stage=%s job_name = %q, want %q", stage, got, want.job)
		}
		if got, _ := event["workspace_id"].(string); got != want.workspace {
			t.Errorf("stage=%s workspace_id = %q, want %q", stage, got, want.workspace)
		}
	}
}

// TestStartup03RebuildWithoutRecordReportsUnknownIdentity is TC4's other
// branch: a Job rendered before the dispatch record existed rebuilds with no
// identity, and the refusal event must say so — the empty field is what
// distinguishes a lost record from a degraded declaration.
func TestStartup03RebuildWithoutRecordReportsUnknownIdentity(t *testing.T) {
	// The canned builder writes no dispatch record, which is exactly the Job
	// shape rendered before this face existed.
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if err := h.reg.Delete("task-1"); err != nil {
		t.Fatalf("drop entry: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	rebuilt, ok := h.reg.Get("task-1")
	if !ok || rebuilt.RuntimeIdentity != "" {
		t.Fatalf("rebuilt entry = %+v, %v; want the unknown identity", rebuilt, ok)
	}

	token := issueJobToken(t, h, "task-1")
	code, body := h.call("POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-task-1",
		"runtimes": [{"name": "foreman-zz", "type": "zz", "version": "3.1.4"}]
	}`))
	if code != 409 {
		t.Fatalf("unknown-identity registration = %d (%s), want the 409 refusal", code, body)
	}
	events := h.handlerEvents(s4RefusalMsg)
	if len(events) != 1 {
		t.Fatalf("refusal events = %d, want 1", len(events))
	}
	if got, _ := events[0]["identity"].(string); got != "" {
		t.Errorf("identity = %q, want empty for a rebuilt Job without a record", got)
	}
	if got, _ := events[0]["stage"].(string); got != proxy.S4RefusalFirst {
		t.Errorf("stage = %q, want %s (no registration evidence)", got, proxy.S4RefusalFirst)
	}
	// An unknown identity compares against nothing, so nothing here is a
	// verdict: the entry survives the refusal, and only its Job ending or a
	// later registration can move it (§2.3).
	assertNoSettlement(t, h, "task-1")
	live, ok := h.reg.Get("task-1")
	if !ok || live.IsTerminal() {
		t.Fatalf("entry = %+v, %v; want the live entry after the refusal", live, ok)
	}
}

// TestStartup03AdoptedRebuildRefusalIsReRegister covers the stage rule's third
// input: a rebuilt entry carries no registration instant, but the disposal
// table adopts a post-delivery Job by recording its startup fact (§4.3), and a
// startup fact alone is enough for the re_register stage (§9.1).
func TestStartup03AdoptedRebuildRefusalIsReRegister(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if err := h.reg.Delete("task-1"); err != nil {
		t.Fatalf("drop entry: %v", err)
	}
	if err := h.sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	// The disposal table's adopt row, on the rebuilt entry: the server reports
	// the task post-delivery, so the start fact is recorded while the
	// registration instant stays absent (it lives only in the shared state).
	if err := h.sched.AdoptRunning(t.Context(), "task-1", "running"); err != nil {
		t.Fatalf("AdoptRunning: %v", err)
	}
	adopted, ok := h.reg.Get("task-1")
	if !ok || adopted.StartedAt.IsZero() || !adopted.DaemonRegisteredAt.IsZero() {
		t.Fatalf("adopted entry = %+v, %v; want a startup fact and no registration instant", adopted, ok)
	}

	token := issueJobToken(t, h, "task-1")
	if code, body := registerDeclaring(h, "task-1", token); code != 409 {
		t.Fatalf("adopted entry's registration = %d (%s), want the 409 refusal", code, body)
	}
	events := h.handlerEvents(s4RefusalMsg)
	if len(events) != 1 {
		t.Fatalf("refusal events = %d, want 1", len(events))
	}
	if got, _ := events[0]["stage"].(string); got != proxy.S4RefusalReRegister {
		t.Errorf("stage = %q, want %s (a startup fact without a registration instant is still re_register)",
			got, proxy.S4RefusalReRegister)
	}
	if got, _ := events[0]["registered_at"].(string); got != "" {
		t.Errorf("registered_at = %q, want empty for a rebuilt entry", got)
	}
}

// TestStartup03RefusalRetriesDegradedFirstRegistration is the incident's first
// shape: the daemon's version probes were killed, so its first registration
// declares nothing. The refusal must not settle the task — the next round
// declares the runtime and the Job registers.
func TestStartup03RefusalRetriesDegradedFirstRegistration(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")

	code, body := h.call("POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-task-1",
		"runtimes": []
	}`))
	if code != 409 {
		t.Fatalf("empty declaration = %d (%s), want the 409 refusal", code, body)
	}
	assertNoSettlement(t, h, "task-1")
	if got := h.metrics.s4RefusalCount(proxy.S4RefusalFirst); got != 1 {
		t.Errorf("refusals stage=first = %d, want 1", got)
	}

	// The daemon's next round declares the runtime: the same entry registers.
	registerTaskDaemon(t, h, "task-1", token)
	registered, ok := h.reg.Get("task-1")
	if !ok || registered.State != registry.StateDaemonRegistered {
		t.Fatalf("entry = %+v, %v; want the registered entry", registered, ok)
	}
	if got := len(h.server.forwarded(scheduler.EPFail)); got != 0 {
		t.Errorf("fail reports after the recovery registration = %d, want 0", got)
	}
}

// TestStartup03RefusalRetriesProviderPresentDeclaration is the retryable shape
// one step further: the declaration names the Job's provider but not its
// profile identity. A killed probe for that one profile is indistinguishable
// from a missing one, so the refusal stays retryable rather than settling.
func TestStartup03RefusalRetriesProviderPresentDeclaration(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")

	code, body := h.call("POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-task-1",
		"runtimes": [{"name": "foreman-zz-pf", "type": "zz", "version": "", "profile_id": "pf"}]
	}`))
	if code != 409 {
		t.Fatalf("provider-present declaration = %d (%s), want the 409 refusal", code, body)
	}
	assertNoSettlement(t, h, "task-1")
	if e, ok := h.reg.Get("task-1"); !ok || e.State != registry.StateJobCreated {
		t.Fatalf("entry = %+v, %v; want the untouched job_created entry", e, ok)
	}
}
