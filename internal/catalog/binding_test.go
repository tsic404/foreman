package catalog_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/catalog/catalogtest"
)

// bindingService is a synced source whose Foreman runtime set is declared, so
// the management class of each binding is decidable.
func bindingService(t *testing.T, upstream *catalogtest.Upstream) *catalog.Service {
	t.Helper()
	clock := &fixedClock{now: timeAt()}
	service, err := catalog.New(catalog.Config{
		Upstream:          upstream,
		Credentials:       catalogtest.Credentials{},
		Clock:             clock.Now,
		ManagedRuntimeIDs: managedRuntimes,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	register(t, service, "atlas", false)
	if _, err := service.Sync(context.Background(), "atlas"); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	return service
}

// managedCount is the overview's "adopted Foreman agents" number.
func managedCount(t *testing.T, service *catalog.Service) int {
	t.Helper()
	view, err := service.Overview(context.Background(), catalog.Grant{})
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}
	return view.Counts.Agents
}

// TestBindingWriteBackIsReadBackBeforeItCounts is BIND-03's core: the local
// answer follows the upstream read-back, not the write.
func TestBindingWriteBackIsReadBackBeforeItCounts(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	if got := managedCount(t, service); got != 2 {
		t.Fatalf("managed agents before any write = %d, want 2", got)
	}

	// The upstream accepts the write and applies it: read-back confirms.
	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingApplied {
		t.Fatalf("result = %q (%s), want applied", outcome.Result, outcome.Detail)
	}
	if outcome.ObservedRuntimeID != "rt-foreman" || outcome.Management != catalog.ManagedForeman {
		t.Fatalf("outcome = %+v, want the read-back value and the managed class", outcome)
	}
	if got := managedCount(t, service); got != 3 {
		t.Fatalf("managed agents after the confirmed write = %d, want 3", got)
	}
	if upstream.Writes != 1 {
		t.Fatalf("writes = %d, want exactly one", upstream.Writes)
	}
}

// TestBindingAcceptedButNotAppliedIsNotSuccess covers a write the upstream
// answered 2xx for while the read-back still shows the old value.
func TestBindingAcceptedButNotAppliedIsNotSuccess(t *testing.T) {
	upstream := seededUpstream()
	upstream.WriteApplies = func(_, _ string) bool { return false }
	service := bindingService(t, upstream)

	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingNotApplied {
		t.Fatalf("result = %q (%s), want not_applied", outcome.Result, outcome.Detail)
	}
	if got := managedCount(t, service); got != 2 {
		t.Fatalf("managed agents = %d, want the count unchanged", got)
	}
	if code := catalog.BindingStatusCode(outcome.Result); code == 200 {
		t.Fatal("a write that did not take effect answered 200")
	}
}

// TestBindingRefusalIsRejected covers the upstream's refusal class: nothing is
// written locally and the answer is an explicit rejection.
func TestBindingRefusalIsRejected(t *testing.T) {
	upstream := seededUpstream()
	upstream.WriteErr = &catalog.UpstreamError{Op: "put agent", Status: 422, Kind: catalog.UpstreamRejected, Message: "runtime not in workspace"}
	service := bindingService(t, upstream)

	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingRejected {
		t.Fatalf("result = %q (%s), want rejected", outcome.Result, outcome.Detail)
	}
	if outcome.UpstreamStatus != 422 {
		t.Fatalf("upstream status = %d, want 422", outcome.UpstreamStatus)
	}
	if got := managedCount(t, service); got != 2 {
		t.Fatalf("managed agents = %d, want the count unchanged after a refusal", got)
	}
	if upstream.Writes != 1 {
		t.Fatalf("writes = %d, want one attempt and no blind retry", upstream.Writes)
	}
}

// TestBindingTimeoutIsReconciledOnceAndNeverFaked covers the unknown class: a
// transport failure is settled by one read-back, and a write that cannot be
// confirmed stays unknown rather than applied.
func TestBindingTimeoutIsReconciledOnceAndNeverFaked(t *testing.T) {
	upstream := seededUpstream()
	upstream.WriteErr = &catalog.UpstreamError{Op: "put agent", Kind: catalog.UpstreamTransient, Message: "request failed"}
	// The comparison read lands; the read-back is the one that fails.
	upstream.ReadErr = &catalog.UpstreamError{Op: "get agent", Kind: catalog.UpstreamTransient, Message: "request failed"}
	upstream.ReadErrAfter = 1
	service := bindingService(t, upstream)

	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingUnknown {
		t.Fatalf("result = %q (%s), want unknown", outcome.Result, outcome.Detail)
	}
	if code := catalog.BindingStatusCode(outcome.Result); code == 200 {
		t.Fatal("an unconfirmed write answered 200")
	}
	if upstream.Writes != 1 {
		t.Fatalf("writes = %d, want exactly one: a timeout is never repeated blindly", upstream.Writes)
	}
	if got := managedCount(t, service); got != 2 {
		t.Fatalf("managed agents = %d, want the count unchanged", got)
	}
}

// TestBindingTimeoutThenReadBackConfirms covers the recovery side of the same
// class: the read-back that does settle it reports the applied value.
func TestBindingTimeoutThenReadBackConfirms(t *testing.T) {
	upstream := seededUpstream()
	upstream.WriteErr = &catalog.UpstreamError{Op: "put agent", Kind: catalog.UpstreamTransient, Message: "request failed"}
	service := bindingService(t, upstream)

	// The write reached the upstream even though its answer was lost.
	upstream.WriteLandsThenFails = true
	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingApplied {
		t.Fatalf("result = %q (%s), want applied from the read-back", outcome.Result, outcome.Detail)
	}
	if got := managedCount(t, service); got != 3 {
		t.Fatalf("managed agents = %d, want the read-back value counted", got)
	}
}

// TestCompetingEditIsConflictAndSendsNoWrite covers the third failure class:
// the upstream moved on between the caller's read and the write.
func TestCompetingEditIsConflictAndSendsNoWrite(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	upstream.SetBinding("agent-outside", "rt-somewhere-else")

	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingConflict {
		t.Fatalf("result = %q (%s), want conflict", outcome.Result, outcome.Detail)
	}
	if upstream.WriteHappened {
		t.Fatal("a competing edit was overwritten: the write was sent")
	}
	if outcome.ObservedRuntimeID != "rt-somewhere-else" {
		t.Fatalf("observed runtime = %q, want the competing value", outcome.ObservedRuntimeID)
	}
	if got := managedCount(t, service); got != 2 {
		t.Fatalf("managed agents = %d, want the count unchanged", got)
	}
}

// TestSwitchToUnmanagedStopsForemanDispatch covers the switch's observable
// half: moving an agent to an outside runtime drops it from the managed count
// and leaves an already-running attempt's snapshot alone.
func TestSwitchToUnmanagedStopsForemanDispatch(t *testing.T) {
	upstream := seededUpstream()
	clock := &fixedClock{now: timeAt()}
	attempts := &staticRuns{rows: []catalog.RunAttempt{{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-managed",
		AgentName: "dev", IssueID: "issue-1", IssueIdentifier: "TSI-1",
		Environment: "env-1", Status: "running", StartedAt: timeAt(),
	}}}
	service, err := catalog.New(catalog.Config{
		Upstream:          upstream,
		Credentials:       catalogtest.Credentials{},
		Clock:             clock.Now,
		ManagedRuntimeIDs: managedRuntimes,
		RunIndex:          attempts,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	register(t, service, "atlas", false)
	if _, err := service.Sync(context.Background(), "atlas"); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	before, err := service.Overview(context.Background(), catalog.Grant{})
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}

	// Managed -> unmanaged -> managed, each step confirmed by read-back.
	if outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-managed",
		ExpectedRuntimeID: "rt-foreman", RuntimeID: "rt-outside",
	}, "test"); err != nil || outcome.Result != catalog.BindingApplied {
		t.Fatalf("switch to unmanaged: %+v %v", outcome, err)
	}
	during, err := service.Overview(context.Background(), catalog.Grant{})
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}
	if during.Counts.Agents != before.Counts.Agents-1 {
		t.Fatalf("managed agents = %d, want %d after the switch", during.Counts.Agents, before.Counts.Agents-1)
	}
	if during.InProgress.Attempts != 1 || during.InProgress.Rows[0].Environment != "env-1" {
		t.Fatalf("the running attempt's snapshot changed with the binding: %+v", during.InProgress)
	}

	if outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-managed",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test"); err != nil || outcome.Result != catalog.BindingApplied {
		t.Fatalf("switch back to managed: %+v %v", outcome, err)
	}
	after, err := service.Overview(context.Background(), catalog.Grant{})
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}
	if after.Counts.Agents != before.Counts.Agents {
		t.Fatalf("managed agents = %d, want the original %d", after.Counts.Agents, before.Counts.Agents)
	}
}

// TestBindingOnATombstonedAgentIsClosed covers the deletion rule: a removed
// agent stops accepting new bindings but keeps its audit.
func TestBindingOnATombstonedAgentIsClosed(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	if _, err := service.Sync(context.Background(), "atlas"); err != nil {
		t.Fatalf("second Sync: %v", err)
	}

	// Remove one agent upstream, then sync: the row becomes a tombstone.
	entry := upstream.Catalogs["ws-core"]
	kept := entry.Agents[:0]
	for _, agent := range entry.Agents {
		if agent.ID != "agent-outside" {
			kept = append(kept, agent)
		}
	}
	entry.Agents = kept
	upstream.Catalogs["ws-core"] = entry
	if _, err := service.Sync(context.Background(), "atlas"); err != nil {
		t.Fatalf("Sync after removal: %v", err)
	}

	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingUnsupported {
		t.Fatalf("result = %q (%s), want unsupported for a tombstoned agent", outcome.Result, outcome.Detail)
	}
	if upstream.WriteHappened {
		t.Fatal("a tombstoned agent received a binding write")
	}
	audit, err := service.Audit(context.Background(), "atlas", catalog.Grant{})
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if len(audit) == 0 {
		t.Fatal("the closed binding left no audit row")
	}
}

// TestBindingRequiresTheIdentityTriple keeps an incomplete request from
// reaching the upstream.
func TestBindingRequiresTheIdentityTriple(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	cases := []catalog.BindingRequest{
		{WorkspaceID: "ws-core", AgentID: "agent-outside", RuntimeID: "rt-foreman"},
		{ServerID: "atlas", AgentID: "agent-outside", RuntimeID: "rt-foreman"},
		{ServerID: "atlas", WorkspaceID: "ws-core", RuntimeID: "rt-foreman"},
		{ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside"},
	}
	for _, req := range cases {
		if _, err := service.SetBinding(context.Background(), req, "test"); err == nil {
			t.Fatalf("SetBinding accepted an incomplete request: %+v", req)
		}
	}
	if upstream.WriteHappened {
		t.Fatal("an incomplete request reached the upstream")
	}
}

// TestBindingOnAnUnknownSourceIsNotFound keeps a missing source distinct from a
// failed write.
func TestBindingOnAnUnknownSourceIsNotFound(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	_, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "nowhere", WorkspaceID: "ws-core", AgentID: "agent-outside", RuntimeID: "rt-foreman",
	}, "test")
	if !errors.Is(err, catalog.ErrSourceNotFound) {
		t.Fatalf("err = %v, want ErrSourceNotFound", err)
	}
}

// TestBindingIsClosedBeforeTheFirstSync keeps the editor closed until the
// directory proves which runtimes this source has.
func TestBindingIsClosedBeforeTheFirstSync(t *testing.T) {
	upstream := seededUpstream()
	service, _ := newService(t, upstream, false)
	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingUnsupported {
		t.Fatalf("result = %q (%s), want unsupported before the first sync", outcome.Result, outcome.Detail)
	}
	if upstream.WriteHappened {
		t.Fatal("a binding write was sent before any directory existed")
	}
}

// TestBindingAuditNamesTheOldAndNewRuntime keeps the audit row reconstructable:
// the old value is what was read, the new one what the read-back showed.
func TestBindingAuditNamesTheOldAndNewRuntime(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	if _, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "operator"); err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	audit, err := service.Audit(context.Background(), "atlas", catalog.Grant{})
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	var found bool
	for _, entry := range audit {
		if entry.Action != catalog.AuditBindingWrite || entry.AgentID != "agent-outside" {
			continue
		}
		found = true
		if entry.OldRuntimeID != "rt-outside" || entry.NewRuntimeID != "rt-foreman" {
			t.Fatalf("audit runtime pair = %q -> %q, want rt-outside -> rt-foreman", entry.OldRuntimeID, entry.NewRuntimeID)
		}
		if entry.Result != catalog.AuditOK || entry.Actor != "operator" {
			t.Fatalf("audit row = %+v, want an ok row attributed to the caller", entry)
		}
	}
	if !found {
		t.Fatal("the applied binding left no audit row")
	}
}

// TestCompetingEditInsideTheWriteWindowIsAConflict covers the window a plain
// read-back cannot close: another administrator changes the binding after our
// comparison read. The conditional write must fail, the competing value must
// survive, and the outcome must not be applied.
func TestCompetingEditInsideTheWriteWindowIsAConflict(t *testing.T) {
	upstream := seededUpstream()
	upstream.CompetingEditBeforeWrite = "rt-racer"
	service := bindingService(t, upstream)

	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingConflict {
		t.Fatalf("result = %q (%s), want conflict", outcome.Result, outcome.Detail)
	}
	if upstream.Writes != 1 {
		t.Fatalf("writes = %d, want the one conditional attempt", upstream.Writes)
	}
	if got := upstream.Agents["agent-outside"].RuntimeID; got != "rt-racer" {
		t.Fatalf("upstream runtime = %q, want the competing value untouched", got)
	}
	if got := managedCount(t, service); got != 2 {
		t.Fatalf("managed agents = %d, want the competing value's count", got)
	}
	if code := catalog.BindingStatusCode(outcome.Result); code == 200 {
		t.Fatal("a conflicting write answered 200")
	}
}

// TestBindingIsClosedWithoutAConditionalWrite is the contract's fallback: an
// adapter that cannot submit a conditional update must not send the
// unconditional one. Zero reads, zero writes, and an explicit reason.
func TestBindingIsClosedWithoutAConditionalWrite(t *testing.T) {
	upstream := seededUpstream()
	upstream.ConditionalWrite = false
	service := bindingService(t, upstream)

	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingUnsupported || outcome.Detail == "" {
		t.Fatalf("outcome = %+v, want unsupported with the capability reason", outcome)
	}
	if upstream.Writes != 0 || upstream.Reads != 0 {
		t.Fatalf("reads/writes = %d/%d, want 0/0", upstream.Reads, upstream.Writes)
	}
	if got := managedCount(t, service); got != 2 {
		t.Fatalf("managed agents = %d, want the count unchanged", got)
	}
}

// TestBindingTimeoutReconcilesUnderItsOwnBudget covers the lost-answer path: the
// write context expires after the upstream applied the change, and the
// read-back runs under a fresh bounded budget derived from the still-valid
// request. Reusing the cancelled write context would never send the GET.
func TestBindingTimeoutReconcilesUnderItsOwnBudget(t *testing.T) {
	upstream := seededUpstream()
	upstream.WriteDelay = 30 * time.Millisecond
	upstream.WriteLandsThenFails = true
	service, err := catalog.New(catalog.Config{
		Upstream:          upstream,
		Credentials:       catalogtest.Credentials{},
		Clock:             timeAt,
		ManagedRuntimeIDs: managedRuntimes,
		SyncTimeout:       5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	register(t, service, "atlas", false)
	if _, err := service.Sync(context.Background(), "atlas"); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	outcome, err := service.SetBinding(context.Background(), catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingApplied {
		t.Fatalf("result = %q (%s), want applied by the fresh reconcile read", outcome.Result, outcome.Detail)
	}
	if upstream.Writes != 1 {
		t.Fatalf("writes = %d, want exactly one", upstream.Writes)
	}
	if upstream.Reads != 2 {
		t.Fatalf("reads = %d, want the comparison read plus the reconcile read", upstream.Reads)
	}
	if got := managedCount(t, service); got != 3 {
		t.Fatalf("managed agents = %d, want the read-back value counted", got)
	}
}

// TestBindingTimeoutWithADeadRequestStaysUnknown checks the other side of the
// same budget: when the outer request has ended, no reconcile call is sent and
// the outcome stays unknown instead of fabricating one.
func TestBindingTimeoutWithADeadRequestStaysUnknown(t *testing.T) {
	upstream := seededUpstream()
	upstream.WriteDelay = 30 * time.Millisecond
	upstream.WriteLandsThenFails = true
	service, err := catalog.New(catalog.Config{
		Upstream:          upstream,
		Credentials:       catalogtest.Credentials{},
		Clock:             timeAt,
		ManagedRuntimeIDs: managedRuntimes,
		// A generous budget keeps the outer cancellation the only deadline
		// the write can observe, so the case does not race two timers.
		SyncTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	register(t, service, "atlas", false)
	if _, err := service.Sync(context.Background(), "atlas"); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(2 * time.Millisecond)
		cancel()
	}()
	outcome, err := service.SetBinding(ctx, catalog.BindingRequest{
		ServerID: "atlas", WorkspaceID: "ws-core", AgentID: "agent-outside",
		ExpectedRuntimeID: "rt-outside", RuntimeID: "rt-foreman",
	}, "test")
	if err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	if outcome.Result != catalog.BindingUnknown {
		t.Fatalf("result = %q (%s), want unknown", outcome.Result, outcome.Detail)
	}
	if upstream.Writes != 1 {
		t.Fatalf("writes = %d, want exactly one", upstream.Writes)
	}
	if upstream.Reads != 1 {
		t.Fatalf("reads = %d, want only the comparison read (no reconcile after the request ended)", upstream.Reads)
	}
}

// TestClaimOwnedAnswersOnTheConfirmedRuntimeAlone pins the confirmation
// boundary: ownership follows the runtime the C3 request that returned the task
// carried, never the agent's current binding. An agent the directory now
// reports as outside still owns a claim that arrived under this deployment's
// runtime — that is exactly the in-flight rebind review finding 7 describes —
// while a runtime the request did not carry is a refusing answer.
func TestClaimOwnedAnswersOnTheConfirmedRuntimeAlone(t *testing.T) {
	upstream := seededUpstream()
	service := bindingService(t, upstream)
	ctx := context.Background()

	// agent-outside's binding points at rt-outside, yet its claim under
	// rt-foreman is still this deployment's: the confirmation predates the
	// edit the snapshot reflects.
	if owned, known := service.ClaimOwned(ctx, "ws-core", "agent-outside", "rt-foreman", []string{"rt-foreman", "rt-foreman-dev"}); !owned || !known {
		t.Fatalf("ClaimOwned(confirmed runtime) = %v, %v, want true, true", owned, known)
	}
	if owned, known := service.ClaimOwned(ctx, "ws-dev", "agent-managed", "rt-foreman-dev", []string{"rt-foreman"}); owned || !known {
		t.Fatalf("ClaimOwned(another workspace's runtime) = %v, %v, want false, true", owned, known)
	}
	if owned, known := service.ClaimOwned(ctx, "ws-core", "agent-managed", "rt-someone-else", []string{"rt-foreman", "rt-foreman-dev"}); owned || !known {
		t.Fatalf("ClaimOwned(unregistered runtime) = %v, %v, want false, true", owned, known)
	}
}

// TestClaimOwnedLeavesAClaimWithoutRequestEvidenceUnknown: a caller that
// carries no frozen C3 request has no evidence about ownership. The answer
// stays unknown instead of being inferred from the live declaration, which a
// refresh may already have replaced.
func TestClaimOwnedLeavesAClaimWithoutRequestEvidenceUnknown(t *testing.T) {
	service, err := catalog.New(catalog.Config{
		Upstream:          catalogtest.NewUpstream(),
		Credentials:       catalogtest.Credentials{},
		Clock:             timeAt,
		ManagedRuntimeIDs: managedRuntimes,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if owned, known := service.ClaimOwned(context.Background(), "ws-core", "agent-managed", "rt-foreman", nil); owned || known {
		t.Fatalf("ClaimOwned(no request evidence) = %v, %v, want false, false", owned, known)
	}
}

// TestClaimOwnedKeepsAConfirmationAcrossARegistrationGeneration is the
// registration-generation window at the gate: the C3 request carried
// rt-foreman, a successful refresh then replaced the live declaration with
// rt-foreman-2, and the claim still answers on the request's own set. The
// replacement runtime was not part of that request, so it creates no Job.
func TestClaimOwnedKeepsAConfirmationAcrossARegistrationGeneration(t *testing.T) {
	live := []string{"rt-foreman"}
	service, err := catalog.New(catalog.Config{
		Upstream:    seededUpstream(),
		Credentials: catalogtest.Credentials{},
		Clock:       timeAt,
		ManagedRuntimeIDs: func(catalog.Source, string) []string {
			return live
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	register(t, service, "atlas", false)
	ctx := context.Background()

	// The generation moved after the claim request was answered.
	live = []string{"rt-foreman-2"}
	if owned, known := service.ClaimOwned(ctx, "ws-core", "agent-outside", "rt-foreman", []string{"rt-foreman"}); !owned || !known {
		t.Fatalf("ClaimOwned(request's runtime after the refresh) = %v, %v, want true, true", owned, known)
	}
	if owned, known := service.ClaimOwned(ctx, "ws-core", "agent-outside", "rt-foreman-2", []string{"rt-foreman"}); owned || !known {
		t.Fatalf("ClaimOwned(refreshed runtime outside the request) = %v, %v, want false, true", owned, known)
	}
}
