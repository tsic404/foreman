// TC-tech-lease-01 / AC-05 (03-acceptance.md:91): prepare-lease keep-alive and
// the convergence of an interrupted renewal, judged on the wire against the
// production claim loop, lease keeper and daemon face: renewal every ≤20s (§10),
// one active Job and no redispatch while renewals run, the redispatch after they
// lapse recognized (count +1, pre-start entry dropped), the task run once.
// The server's 45s lease runs on its own clock, so the fixture plays the
// redispatch instead of waiting the lease out — what the design judges is the
// recognition of it, not the server's timer.
package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

const (
	// lease01Task is the scenario's task: one claimed, not-yet-started task is
	// the whole renewal window.
	lease01Task = "task-1"
	// lease01JobName is the Job the task's daemon runs under. The daemon id is
	// derived from the task id, never from the display name (ADR-034), so the
	// redispatch's replacement Job reuses this name.
	lease01JobName = "fm-" + lease01Task
	// lease01RenewBound is §10's renewal bound: claim to start, a task is
	// renewed every ≤20s. The lease scenarios judge it on the wire timestamps,
	// never on the configured value — that face is the config guard's own test.
	lease01RenewBound = 20 * time.Second
	// lease01Cadence is the keeper interval the scenarios run with: far below
	// the bound, so the judged renewal run fits a bounded window — and an
	// implementation renewing slower than the bound cannot produce it.
	lease01Cadence = 20 * time.Millisecond
	// lease01RenewalsJudged is the renewal run the cadence is read off: two
	// intervals need three renewals.
	lease01RenewalsJudged = 3
	// lease01ClaimCadence shortens the production claim loop's polling for a
	// bounded wait; the polling cadence is no clause of this TC.
	lease01ClaimCadence = 10 * time.Millisecond
	// lease01Wait bounds the waits on the observable a scenario is about to
	// assert: the fixture's cadences put every one of them well inside it.
	lease01Wait = 5 * time.Second
)

// newLease01Harness is the scenario fixture: the production components, two
// declared runtimes, the catalog route off, and the confirmation window at zero
// — the single-delivery form (ADR-032), so "the task runs once" is judged
// without another TC's re-delivery exit in the way. The lease keeper is NOT
// running yet: each scenario starts it where its own ordering requires.
func newLease01Harness(t *testing.T) *harness {
	t.Helper()
	h := newHarnessWithWindow(t, false, 0)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	return h
}

// lease01AwaitClaimed waits until the claim chain has placed the task's Job and
// returns the job_created entry. OnClaim records the attempt before it builds
// the Job (reg.Put → Build → CreateSecret → CreateJob → MarkJobCreated), so a
// scenario that waited on the entry alone would act inside the claim.
func lease01AwaitClaimed(t *testing.T, h *harness) registry.TaskEntry {
	t.Helper()
	waitFor(t, "the claim to place the task Job", lease01Wait, func() bool {
		entry, ok := h.reg.Get(lease01Task)
		return ok && entry.State == registry.StateJobCreated && h.jobs.jobCreated(lease01JobName)
	})
	entry, _ := h.reg.Get(lease01Task)
	return entry
}

// lease01ClaimLoop runs the production claim loop (C3) at the shortened cadence
// until the test ends: both the claim that opens the renewal window and the
// server's redispatch arrive through it.
func lease01ClaimLoop(t *testing.T, h *harness) {
	t.Helper()
	loopCfg := h.cfg
	loopCfg.ClaimInterval = lease01ClaimCadence
	loopCfg.ClaimIntervalIdle = lease01ClaimCadence
	loop := proxy.NewClaimLoop(loopCfg, h.client, h.sched)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = loop.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// lease01Keeper runs the production lease keeper at the scenario's cadence until
// the returned stop function runs (or the test ends). Stopping it is how a
// scenario models the renewal interruption: the lease the server holds on the
// task is then left to lapse.
func lease01Keeper(t *testing.T, h *harness, interval time.Duration) func() {
	t.Helper()
	keeper := proxy.NewLeaseKeeper(h.client, h.reg, h.sched, interval)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = keeper.Run(ctx)
	}()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		<-done
	}
	t.Cleanup(stop)
	return stop
}

// lease01RenewalPath is C-4's path for the task: the renewal is addressed to the
// runtime the claim was dispatched under (the entry's dispatch record).
func lease01RenewalPath(t *testing.T, h *harness) string {
	t.Helper()
	entry, ok := h.reg.Get(lease01Task)
	if !ok {
		t.Fatalf("entry %s missing", lease01Task)
	}
	return fmt.Sprintf("/api/daemon/runtimes/%s/tasks/%s/prepare-lease", entry.RealRuntimeID, lease01Task)
}

// lease01Renewals returns the C-4 renewals the stub observed for the task, in
// arrival order.
func lease01Renewals(t *testing.T, h *harness) []upstreamRequest {
	t.Helper()
	return h.upstream.calls("POST", lease01RenewalPath(t, h))
}

// lease01ForeignRenewals lists the run's renewals that were NOT addressed to the
// task's own runtime: such a renewal keeps no lease on the task while reading as
// one — the runtime in C-4's path is the claim's dispatch record (ADR-025), not
// a free choice.
func lease01ForeignRenewals(t *testing.T, h *harness) []upstreamRequest {
	t.Helper()
	want := lease01RenewalPath(t, h)
	var foreign []upstreamRequest
	for _, req := range h.upstream.pathCalls("/api/daemon/runtimes/") {
		if req.Method != "POST" || !strings.HasSuffix(req.Path, "/prepare-lease") || req.Path == want {
			continue
		}
		foreign = append(foreign, req)
	}
	return foreign
}

// lease01LiveJobs samples the live task-Job count for the whole scenario: a
// redispatch that accumulated a second Job instead of replacing the first shows
// up between the samples, not only at the checkpoints.
func lease01LiveJobs(t *testing.T, h *harness) *atomic.Int64 {
	t.Helper()
	maxJobs := &atomic.Int64{}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if n := int64(h.liveTaskJobs()); n > maxJobs.Load() {
				maxJobs.Store(n)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})
	return maxJobs
}

// lease01ClaimBody is one S6 claim body for the task's Job daemon: the body a
// credential the daemon face refuses never reaches.
func lease01ClaimBody(t *testing.T, entry registry.TaskEntry) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"daemon_id":   entry.DaemonID,
		"runtime_ids": []string{entry.JobRuntimeID},
		"max_tasks":   4,
	})
	if err != nil {
		t.Fatalf("marshal claim body: %v", err)
	}
	return raw
}

// TestTechLease01RenewalCadenceStaysWithinTheBound judges the row's first clause
// where it is observable: the C-4 renewals the stub upstream saw for a claimed,
// not-yet-started task. Three renewals in a window far shorter than the bound
// are the executable form of "renewed every ≤20s" — an implementation renewing
// slower than the bound cannot produce them, and every interval read off the
// wire must stay inside it.
func TestTechLease01RenewalCadenceStaysWithinTheBound(t *testing.T) {
	h := newLease01Harness(t)
	h.upstream.enqueueTask(lease01Task)
	lease01ClaimLoop(t, h)
	claimed := lease01AwaitClaimed(t, h)

	lease01Keeper(t, h, lease01Cadence)
	waitFor(t, "the renewal run the cadence is judged on", lease01Wait, func() bool {
		return len(lease01Renewals(t, h)) >= lease01RenewalsJudged
	})
	renewals := lease01Renewals(t, h)

	// The first renewal belongs to the claim's own window: "claim 之后 … 每不超
	// 过 20s 续租" starts at the claim, not at the first tick.
	if first := renewals[0].At.Sub(claimed.ClaimedAt); first > lease01RenewBound {
		t.Fatalf("the first renewal came %s after the claim, want within %s (§10)", first, lease01RenewBound)
	}
	for i := 1; i < len(renewals); i++ {
		if gap := renewals[i].At.Sub(renewals[i-1].At); gap > lease01RenewBound {
			t.Fatalf("renewal %d came %s after the previous one, want within %s (§10)",
				i+1, gap, lease01RenewBound)
		}
	}
	if foreign := lease01ForeignRenewals(t, h); len(foreign) != 0 {
		t.Fatalf("renewals addressed to another runtime: %d (first %s)", len(foreign), foreign[0].Path)
	}
	// The entry is still the pre-start one the renewals are for: a task that had
	// started owes no renewal, so its window is what this run measured.
	if entry, _ := h.reg.Get(lease01Task); !entry.StartedAt.IsZero() {
		t.Fatalf("entry started at %v — the measured window is not the pre-start one", entry.StartedAt)
	}
}

// TestTechLease01UnbrokenRenewalKeepsOneJobWithoutRedispatch judges the row's
// second clause: with the renewals running, the task holds one active Job for
// the whole window and no redispatch is counted — the server's lease never
// lapses, so nothing re-dispatches it and nothing may build a second Job.
func TestTechLease01UnbrokenRenewalKeepsOneJobWithoutRedispatch(t *testing.T) {
	h := newLease01Harness(t)
	h.upstream.enqueueTask(lease01Task)
	lease01ClaimLoop(t, h)
	lease01AwaitClaimed(t, h)

	live := lease01LiveJobs(t, h)
	lease01Keeper(t, h, lease01Cadence)
	waitFor(t, "a renewal run spanning several rounds", lease01Wait, func() bool {
		return len(lease01Renewals(t, h)) >= 2*lease01RenewalsJudged
	})

	if n := h.metrics.duplicateDispatchCount(); n != 0 {
		t.Errorf("duplicate dispatches = %d, want 0 while the renewals are unbroken", n)
	}
	if events := h.events("duplicate_dispatch"); len(events) != 0 {
		t.Errorf("duplicate_dispatch events = %d, want 0 while the renewals are unbroken", len(events))
	}
	if n := h.createdTaskJobNames()[lease01JobName]; n != 1 {
		t.Errorf("Job %s created %d times, want exactly 1", lease01JobName, n)
	}
	if n := live.Load(); n != 1 {
		t.Errorf("live task Jobs peaked at %d, want exactly 1 through the renewal window", n)
	}
	entry, ok := h.reg.Get(lease01Task)
	if !ok || entry.IsTerminal() {
		t.Fatalf("entry %s = %v, want the live pre-start entry", lease01Task, entry.State)
	}
	if entry.Attempt != 1 {
		t.Errorf("attempt = %d, want 1 (no redispatch replaced the entry)", entry.Attempt)
	}
}

// TestTechLease01InterruptedRenewalIsRecognizedAndRunsOnce judges the row's last
// two clauses. The renewals stop, the server's lease on the pre-start task
// lapses and it re-dispatches the task; the redispatch must be recognized — the
// duplicate-dispatch count +1, the pre-start entry dropped without residue, its
// credential revoked (ADR-017) — and the task must still run exactly once.
func TestTechLease01InterruptedRenewalIsRecognizedAndRunsOnce(t *testing.T) {
	h := newLease01Harness(t)
	// The revocation set is what keeps the discarded attempt's credential out of
	// the re-run task (ADR-017): a deployment wires the Job Token authority as
	// the scheduler's revoker, and so does the fixture.
	h.revoker = h.issuer
	h.rebuildScheduler(t, func(*scheduler.Config) {})
	h.upstream.enqueueTask(lease01Task)
	lease01ClaimLoop(t, h)
	lease01AwaitClaimed(t, h)

	live := lease01LiveJobs(t, h)
	stopKeeper := lease01Keeper(t, h, lease01Cadence)
	waitFor(t, "the renewals that keep the lease alive", lease01Wait, func() bool {
		return len(lease01Renewals(t, h)) >= 2
	})
	oldToken := h.builder.probeToken(lease01JobName)
	if oldToken == "" {
		t.Fatal("the first attempt's Job Token was not recorded: the revoked face cannot be judged")
	}

	// The renewals stop, the lease lapses, and the server re-dispatches the task:
	// C13 answers the pre-start form the redispatch rule partitions on, and the
	// queued task comes back through C3.
	stopKeeper()
	h.server.setStatus(lease01Task, taskStatusDispatched)
	if !h.upstream.requeueTask(lease01Task) {
		t.Fatalf("requeueTask(%s): the task is not the stub's dispatched one", lease01Task)
	}
	waitFor(t, "the redispatch to be recognized and its replacement Job placed", lease01Wait, func() bool {
		entry, ok := h.reg.Get(lease01Task)
		return ok && entry.Attempt == 2 && entry.State == registry.StateJobCreated &&
			h.metrics.duplicateDispatchCount() >= 1 &&
			h.createdTaskJobNames()[lease01JobName] == 2
	})

	if n := h.metrics.duplicateDispatchCount(); n != 1 {
		t.Errorf("duplicate dispatches = %d, want exactly 1 for the one redispatch", n)
	}
	if events := h.events("duplicate_dispatch"); len(events) != 1 {
		t.Errorf("duplicate_dispatch events = %d, want 1", len(events))
	}
	entry, ok := h.reg.Get(lease01Task)
	if !ok {
		t.Fatal("the redispatch must leave one live entry for the task")
	}
	if entry.Attempt != 2 {
		t.Errorf("attempt = %d, want 2: the redispatch replaces the discarded entry", entry.Attempt)
	}
	if entry.IsTerminal() || entry.IsDraining() {
		t.Errorf("entry = %s (draining %v), want a live pre-start entry with no drain residue",
			entry.State, entry.IsDraining())
	}
	if n := len(h.reg.List()); n != 1 {
		t.Errorf("registry entries = %d, want 1 (the discarded attempt left nothing behind)", n)
	}
	if h.reg.IsDone("ws-1", lease01Task) {
		t.Error("the discarded attempt must not settle the task in the done index")
	}
	// Discarded, not accumulated: the old attempt's Job went (the claim path
	// never deletes a Job) and the replacement is the second create under the
	// same name.
	if !h.jobs.jobDeleted(lease01JobName) {
		t.Errorf("Job %s was not deleted: the discarded attempt was not torn down", lease01JobName)
	}
	if n := h.createdTaskJobNames()[lease01JobName]; n != 2 {
		t.Errorf("Job %s created %d times, want 2 (discard then replacement)", lease01JobName, n)
	}

	// Only once: the discarded attempt's credential is refused on the re-run
	// task's faces, so the old pod can never take it (ADR-017).
	if code, body := h.call("POST", "/api/daemon/tasks/claim", oldToken, lease01ClaimBody(t, entry)); code != 401 {
		t.Errorf("S6 with the discarded attempt's token = %d (%s), want 401", code, body)
	}
	deregisterBody := []byte(`{"daemon_id":"` + lease01JobName + `"}`)
	if code, body := h.call("POST", "/api/daemon/deregister", oldToken, deregisterBody); code != 401 {
		t.Errorf("S19 with the discarded attempt's token = %d (%s), want 401", code, body)
	}
	if n := h.metrics.authFailureCount("revoked"); n < 1 {
		t.Errorf("auth failures recorded as revoked = %d, want at least 1", n)
	}

	// And the task executes once: the replacement Job's daemon claims it, starts
	// it and completes it, with one delivery and one start fact.
	token := issueJobToken(t, h, lease01Task)
	registerTaskDaemon(t, h, lease01Task, token)
	if got := claimTask(t, h, lease01Task, token); !bytes.Contains(got, []byte(`"id":"`+lease01Task+`"`)) {
		t.Fatalf("S6 after the redispatch = %s, want the task payload", got)
	}
	if code, body := h.call("POST", "/api/daemon/tasks/"+lease01Task+"/start", token, []byte(`{}`)); code != 200 {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	if code, body := h.call("POST", "/api/daemon/tasks/"+lease01Task+"/complete", token, []byte(`{"exit_code":0}`)); code != 200 {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}
	settled := waitTerminal(t, h, lease01Task)
	if settled.Result != registry.ResultCompleted {
		t.Errorf("verdict = %s/%s, want completed", settled.State, settled.Result)
	}
	if n := h.metrics.deliveryClaimCount(proxy.DeliveryClaimDelivered); n != 1 {
		t.Errorf("deliveries = %d, want 1 (one hand-over to one daemon)", n)
	}
	if n := len(h.server.forwarded(scheduler.EPStart)); n != 1 {
		t.Errorf("start facts forwarded = %d, want 1 (the task ran once)", n)
	}
	if n := live.Load(); n != 1 {
		t.Errorf("live task Jobs peaked at %d, want exactly 1: the redispatch must never double-run the task", n)
	}
}
