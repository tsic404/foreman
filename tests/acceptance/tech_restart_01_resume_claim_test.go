// TC-tech-restart-01: a Multica resume path never becomes an rsbx session, and
// two executions of one attempt contend for one lease; the normal first Run
// executes the full three-stage path and only the winner may dispatch.
package acceptance

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/environment/environmenttest"
	"github.com/tsic404/foreman/internal/release"
	"github.com/tsic404/foreman/internal/rules"
)

// v2LedgerSlots is the Job-side Slots adapter backed by the real environment
// service. It is deliberately thin: every claim decision is made by the
// ledger, never by the test.
type v2LedgerSlots struct {
	service *environment.Service
}

func (l v2LedgerSlots) Status(ctx context.Context, binding attempt.Binding) (attempt.SlotStatus, error) {
	slot, found, err := l.service.SlotState(ctx, environment.ID(binding.EnvironmentID), binding.SlotID)
	if err != nil {
		return attempt.SlotStatus{}, err
	}
	if !found {
		return attempt.SlotStatus{Exists: false}, nil
	}
	return attempt.SlotStatus{
		Exists: true, State: string(slot.State), Phase: string(slot.Phase),
		ClaimID: slot.ClaimID, SessionID: slot.SessionID, Started: slot.Started,
	}, nil
}

func (l v2LedgerSlots) Begin(ctx context.Context, binding attempt.Binding, claimID string) error {
	_, err := l.service.BeginSlot(ctx, environment.ID(binding.EnvironmentID), binding.SlotID, claimID)
	return err
}

func (l v2LedgerSlots) TakeOver(ctx context.Context, binding attempt.Binding, claimID, expectedClaimID string) error {
	_, err := l.service.TakeOverSlot(ctx, environment.ID(binding.EnvironmentID), binding.SlotID, claimID, expectedClaimID)
	return err
}

func (l v2LedgerSlots) Bind(ctx context.Context, binding attempt.Binding, claimID string, session attempt.Session) error {
	_, err := l.service.BindSlot(ctx, environment.ID(binding.EnvironmentID), binding.SlotID, claimID, session.ID)
	return err
}

func (l v2LedgerSlots) MarkStarted(ctx context.Context, binding attempt.Binding, claimID string) error {
	_, err := l.service.MarkSlotStarted(ctx, environment.ID(binding.EnvironmentID), binding.SlotID, claimID)
	return err
}

func (l v2LedgerSlots) Release(ctx context.Context, binding attempt.Binding, claimID string, confirmed bool) error {
	// A Job-side adapter can prove the remote close but never the Pod close;
	// the trusted platform observer supplies the second half.
	_, err := l.service.ReleaseSlotByClaim(ctx, environment.ID(binding.EnvironmentID), binding.SlotID, claimID, confirmed, false)
	return err
}

func v2LedgerEnvironment(t *testing.T) *environment.Service {
	t.Helper()
	probe := environmenttest.NewProbe()
	probe.Result = environment.ProbeResult{
		TreeID: "tree-1", Identity: "sha256:tree-1", Capabilities: []string{"nix"},
		Capacity: 4, Version: "rsbxd-test",
	}
	service, err := environment.New(environment.Config{
		Probe:               probe,
		AllowedTrustDomains: []string{"build"},
	})
	if err != nil {
		t.Fatalf("environment.New: %v", err)
	}
	if _, err := service.Register(t.Context(), environment.Registration{
		ID: "build-a", Endpoint: "build-a.example.test:7443", TLSRef: "ref/build-a",
		StateRoot: "/srv/rsbx/build-a", TrustDomain: "build",
		AllowedSources: []string{"atlas"}, Capabilities: []string{"nix"}, Concurrency: 2,
	}, "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	return service
}

type v2LedgerCatalog struct{}

func (v2LedgerCatalog) Directory(context.Context, catalog.ServerID, string, catalog.Grant) (catalog.DirectoryView, error) {
	return catalog.DirectoryView{
		ServerID: "atlas", SyncedAt: "2026-11-22T00:00:00Z",
		Sync: string(catalog.SyncOK), Connection: string(catalog.ConnectionOK),
		Projects: []catalog.ProjectView{{
			ServerID: "atlas", WorkspaceID: "core", ProjectID: "mobile", Title: "mobile",
		}},
		Agents: []catalog.AgentView{{
			ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", Name: "dev",
			RuntimeID: "rt-1", RuntimeBound: true, Management: string(catalog.ManagedForeman),
		}},
		Runtimes: []catalog.RuntimeView{{
			ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-1", Provider: "codex",
		}},
	}, nil
}

type v2LedgerIssues struct{}

func (v2LedgerIssues) ByIssue(context.Context, string, string) (string, error) {
	return "mobile", nil
}

type v2LedgerReleases struct{}

func (v2LedgerReleases) CurrentRelease(context.Context, string, release.Kind, string) (string, map[string]string, bool) {
	return "1.0.0", map[string]string{"linux/amd64": "sha256:" + strings.Repeat("4", 64)}, true
}

func v2LedgerRules(t *testing.T, environments *environment.Service) *rules.Service {
	t.Helper()
	service, err := rules.New(rules.Config{
		Environments: environments,
		Catalog:      v2LedgerCatalog{},
		Issues:       v2LedgerIssues{},
		Releases:     v2LedgerReleases{},
		Sessions:     rules.NewMemorySessionIndex(8),
		Platform:     "linux/amd64",
		JobNamespace: "foreman",
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("rules.New: %v", err)
	}
	ctx := t.Context()
	if _, err := service.Draft(ctx, 0, []rules.Rule{{
		ID: "resume-rule", ServerID: "atlas", WorkspaceID: "core",
		AgentID: "dev", EnvironmentID: "build-a", Enabled: true,
	}}, catalog.Grant{}, "test"); err != nil {
		t.Fatalf("Draft: %v", err)
	}
	if _, err := service.Publish(ctx, 0, catalog.Grant{}, "test"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return service
}

// TestV2ResumeBindingKeepsTheRemoteSessionEmpty drives the real resolver and
// reservation ledger with a non-empty Multica resume path. The first Run must
// execute all three stages; the prior path never becomes an rsbx session.
func TestV2ResumeBindingKeepsTheRemoteSessionEmpty(t *testing.T) {
	ctx := t.Context()
	environments := v2LedgerEnvironment(t)
	rulesService := v2LedgerRules(t, environments)
	const attemptID = "task-v2-resume"
	const priorSession = "/state/home/.multica/pi-sessions/sess-1.jsonl"

	frozen, err := rulesService.Bind(ctx, rules.Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
		PriorSessionID: priorSession, AttemptID: attemptID,
	})
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if frozen.SlotID != attemptID || frozen.Resolution.Environment == nil {
		t.Fatalf("binding = %+v, want a reserved environment", frozen)
	}
	slot, found, err := environments.SlotState(ctx, environment.ID(frozen.EnvironmentID), frozen.SlotID)
	if err != nil || !found {
		t.Fatalf("SlotState after Bind = (%+v, %v, %v)", slot, found, err)
	}
	if slot.SessionID != "" || slot.ClaimID != "" || slot.Phase != environment.SlotPhaseIdle {
		t.Fatalf("new reservation = %+v, want an idle slot with no remote session", slot)
	}

	choice := frozen.Resolution.Environment
	binding := attempt.Binding{
		AttemptID: attemptID, TaskID: attemptID, SourceID: "atlas",
		WorkspaceID: "core", AgentID: "dev", DaemonID: "fm-" + attemptID,
		JobRuntimeID: "runtime-job-1", EnvironmentID: frozen.EnvironmentID,
		Endpoint: choice.Endpoint, TLSRef: choice.TLSRef, StateRoot: choice.StateRoot,
		EnvironmentGen: choice.Generation, ResumeSessionID: priorSession,
		SlotID: frozen.SlotID,
		Image:  "ghcr.io/tsic404/rsbx", ImageDigest: "sha256:" + strings.Repeat("1", 64),
		Platform: frozen.Resolution.CLIs.Platform, ProxyURL: "http://foreman.foreman.svc.cluster.local:8080",
		CLIs: []attempt.CLI{
			{Kind: attempt.KindMultica, Version: frozen.Resolution.CLIs.MulticaVersion,
				Platform: frozen.Resolution.CLIs.Platform, Digest: frozen.Resolution.CLIs.MulticaDigest,
				DownloadURL: "http://foreman.foreman.svc.cluster.local:8080/cli/multica/1.0.0/linux/amd64"},
			{Kind: attempt.KindAgent, Backend: "codex", Version: frozen.Resolution.CLIs.AgentVersion,
				Platform: frozen.Resolution.CLIs.Platform, Digest: frozen.Resolution.CLIs.AgentDigest,
				DownloadURL: "http://foreman.foreman.svc.cluster.local:8080/cli/agent/1.0.0/linux/amd64?backend=codex"},
		},
	}
	if binding.SessionID != "" {
		t.Fatalf("the frozen binding invented remote session %q from the resume path", binding.SessionID)
	}

	cache := &v2Cache{artifacts: v2Artifacts(binding)}
	remote := &v2Remote{session: v2ReadySession(binding), close: attempt.CloseEvidence{Confirmed: true}}
	registrar := &v2Registrar{}
	runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	supervisor := v2FixedSupervisor(t, v2StorePath(t), cache, remote, registrar, runner, &v2Slots{})
	supervisor.Slots = v2LedgerSlots{service: environments}
	supervisor.NewClaim = func() string { return "claim-resume" }

	outcome, err := supervisor.Run(ctx, binding)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if outcome.Stage != attempt.StageClosed || !outcome.Delivered || !outcome.SlotReleased {
		t.Fatalf("outcome = %+v, want a delivered, released attempt", outcome)
	}
	if got := kinds(cache.calls()); got != "multica,agent" {
		t.Fatalf("prepared CLIs = %s, want the full three-stage path", got)
	}
	if remote.readyCalls != 1 || registrar.calls != 1 || runner.calls != 1 {
		t.Fatalf("ready=%d register=%d run=%d, want one each", remote.readyCalls, registrar.calls, runner.calls)
	}
	if len(remote.closed) != 1 || remote.closed[0] == priorSession || remote.closed[0] != outcome.Session.ID {
		t.Fatalf("closed sessions = %v, want only the opened rsbx session", remote.closed)
	}
	slot, found, err = environments.SlotState(ctx, environment.ID(frozen.EnvironmentID), frozen.SlotID)
	if err != nil || !found {
		t.Fatalf("slot after the confirmed close = (%+v, %v, %v), want the reservation kept until the Pod close", slot, found, err)
	}
	if !slot.RemoteClosed {
		t.Fatalf("slot = %+v, want the remote close recorded", slot)
	}
	if _, err := environments.ReleaseSlotOnPodClosed(ctx, environment.ID(frozen.EnvironmentID), frozen.SlotID); err != nil {
		t.Fatalf("ReleaseSlotOnPodClosed: %v", err)
	}
	if _, found, err := environments.SlotState(ctx, environment.ID(frozen.EnvironmentID), frozen.SlotID); err != nil || found {
		t.Fatalf("slot after both close facts = (found=%v, err=%v), want it released", found, err)
	}
}

type v2BeginBarrier struct {
	mu      sync.Mutex
	arrived int
	want    int
	release chan struct{}
}

func newV2BeginBarrier(want int) *v2BeginBarrier {
	return &v2BeginBarrier{want: want, release: make(chan struct{})}
}

func (b *v2BeginBarrier) wait() {
	b.mu.Lock()
	b.arrived++
	if b.arrived == b.want {
		close(b.release)
	}
	b.mu.Unlock()
	<-b.release
}

type v2RacingSlots struct {
	attempt.Slots
	barrier *v2BeginBarrier
	mu      sync.Mutex
	claim   string
}

func (s *v2RacingSlots) Begin(ctx context.Context, binding attempt.Binding, claimID string) error {
	s.mu.Lock()
	s.claim = claimID
	s.mu.Unlock()
	s.barrier.wait()
	return s.Slots.Begin(ctx, binding, claimID)
}

func (s *v2RacingSlots) claimID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claim
}

type v2GatedRunner struct {
	inner   *v2Runner
	started chan struct{}
	gate    <-chan struct{}
}

func (r *v2GatedRunner) Run(ctx context.Context, binding attempt.Binding, session attempt.Session, agent attempt.Artifact) (attempt.RunResult, error) {
	close(r.started)
	<-r.gate
	return r.inner.Run(ctx, binding, session, agent)
}

type v2LedgerRun struct {
	index   int
	outcome attempt.Outcome
	err     error
}

type v2LedgerInstance struct {
	cache     *v2Cache
	remote    *v2Remote
	registrar *v2Registrar
	runner    *v2Runner
	racing    *v2RacingSlots
	sup       *attempt.Supervisor
}

func newV2LedgerInstance(t *testing.T, binding attempt.Binding, ledger attempt.Slots, claim string, gate <-chan struct{}) *v2LedgerInstance {
	t.Helper()
	cache := &v2Cache{artifacts: v2Artifacts(binding)}
	remote := &v2Remote{session: v2ReadySession(binding), close: attempt.CloseEvidence{Confirmed: true}}
	registrar := &v2Registrar{}
	runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	racing := &v2RacingSlots{Slots: ledger}
	sup := v2FixedSupervisor(t, v2StorePath(t), cache, remote, registrar, runner, &v2Slots{})
	sup.Slots = racing
	sup.Runner = &v2GatedRunner{inner: runner, started: make(chan struct{}), gate: gate}
	sup.NewClaim = func() string { return claim }
	return &v2LedgerInstance{cache: cache, remote: remote, registrar: registrar, runner: runner, racing: racing, sup: sup}
}

// TestV2ConcurrentExecutionsRaceForOneClaim starts two Supervisors of the same
// attempt against one real ledger. Both read idle before either Begin; exactly
// one wins, prepares, binds, starts and releases. The loser never dispatches
// and cannot release the winner's started slot.
func TestV2ConcurrentExecutionsRaceForOneClaim(t *testing.T) {
	ctx := t.Context()
	environments := v2LedgerEnvironment(t)
	binding := v2FixedBinding()
	if _, err := environments.AcquireSlot(ctx, environment.ID(binding.EnvironmentID), binding.SlotID); err != nil {
		t.Fatalf("AcquireSlot: %v", err)
	}

	barrier := newV2BeginBarrier(2)
	gate := make(chan struct{})
	first := newV2LedgerInstance(t, binding, v2LedgerSlots{service: environments}, "claim-a", gate)
	second := newV2LedgerInstance(t, binding, v2LedgerSlots{service: environments}, "claim-b", gate)
	first.racing.barrier = barrier
	second.racing.barrier = barrier

	results := make(chan v2LedgerRun, 2)
	run := func(index int, instance *v2LedgerInstance) {
		outcome, err := instance.sup.Run(ctx, binding)
		results <- v2LedgerRun{index: index, outcome: outcome, err: err}
	}
	go run(0, first)
	go run(1, second)

	winner := -1
	select {
	case <-first.sup.Runner.(*v2GatedRunner).started:
		winner = 0
	case <-second.sup.Runner.(*v2GatedRunner).started:
		winner = 1
	case <-time.After(5 * time.Second):
		t.Fatal("neither execution reached the dispatch stage")
	}
	loser := 1 - winner
	instances := []*v2LedgerInstance{first, second}

	var loserRun v2LedgerRun
	select {
	case loserRun = <-results:
		if loserRun.index != loser {
			t.Fatalf("both executions reached dispatch; result %+v", loserRun)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the losing execution did not refuse")
	}
	if !errors.Is(loserRun.err, attempt.ErrRefused) || loserRun.outcome.Delivered || loserRun.outcome.SlotReleased {
		t.Fatalf("loser = (%+v, %v), want a refused zero-execution result", loserRun.outcome, loserRun.err)
	}
	losing, winning := instances[loser], instances[winner]
	if len(losing.cache.calls()) != 0 || losing.remote.readyCalls != 0 || losing.registrar.calls != 0 || losing.runner.calls != 0 {
		t.Fatalf("loser prepared/opened/dispatched: cache=%v ready=%d register=%d run=%d",
			losing.cache.calls(), losing.remote.readyCalls, losing.registrar.calls, losing.runner.calls)
	}
	loserClaim, winnerClaim := losing.racing.claimID(), winning.racing.claimID()
	if loserClaim == "" || winnerClaim == "" || loserClaim == winnerClaim {
		t.Fatalf("claims = %q/%q, want two distinct execution leases", loserClaim, winnerClaim)
	}

	slot, found, err := environments.SlotState(ctx, environment.ID(binding.EnvironmentID), binding.SlotID)
	if err != nil || !found {
		t.Fatalf("SlotState while the winner is dispatching = (%+v, %v, %v)", slot, found, err)
	}
	if slot.ClaimID != winnerClaim || !slot.Started || slot.SessionID == "" {
		t.Fatalf("slot = %+v, want the winner's started session", slot)
	}
	if _, err := environments.ReleaseSlotByClaim(ctx, environment.ID(binding.EnvironmentID), binding.SlotID, loserClaim, true, false); !errors.Is(err, environment.ErrSlotClaimed) {
		t.Fatalf("loser ReleaseSlotByClaim = %v, want ErrSlotClaimed", err)
	}
	if slot, found, err := environments.SlotState(ctx, environment.ID(binding.EnvironmentID), binding.SlotID); err != nil || !found || slot.ClaimID != winnerClaim {
		t.Fatalf("slot after the losing release = (%+v, %v, %v), want the winner intact", slot, found, err)
	}

	close(gate)
	var winnerRun v2LedgerRun
	select {
	case winnerRun = <-results:
		if winnerRun.index != winner {
			t.Fatalf("winner result = %+v", winnerRun)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the winning execution did not finish")
	}
	if winnerRun.err != nil || winnerRun.outcome.Stage != attempt.StageClosed || !winnerRun.outcome.Delivered {
		t.Fatalf("winner = (%+v, %v), want a delivered closed attempt", winnerRun.outcome, winnerRun.err)
	}
	if len(winning.cache.calls()) != 2 {
		t.Fatalf("winner prepared %v, want exactly two CLIs", winning.cache.calls())
	}
	if winning.remote.readyCalls != 1 || winning.registrar.calls != 1 || winning.runner.calls != 1 {
		t.Fatalf("winner ready=%d register=%d run=%d, want one each",
			winning.remote.readyCalls, winning.registrar.calls, winning.runner.calls)
	}
	slot, found, err = environments.SlotState(ctx, environment.ID(binding.EnvironmentID), binding.SlotID)
	if err != nil || !found {
		t.Fatalf("slot after the winner released = (%+v, %v, %v), want the reservation kept until the Pod close", slot, found, err)
	}
	if !slot.RemoteClosed {
		t.Fatalf("slot = %+v, want the winner's remote close recorded", slot)
	}
	if _, err := environments.ReleaseSlotOnPodClosed(ctx, environment.ID(binding.EnvironmentID), binding.SlotID); err != nil {
		t.Fatalf("ReleaseSlotOnPodClosed: %v", err)
	}
	if _, found, err := environments.SlotState(ctx, environment.ID(binding.EnvironmentID), binding.SlotID); err != nil || found {
		t.Fatalf("slot after both close facts = (found=%v, err=%v), want it gone", found, err)
	}
}
