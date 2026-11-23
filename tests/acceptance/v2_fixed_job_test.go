// The fixed foreman-job's cold-start contract: prepare only the attempt's two
// CLIs, wait for the frozen rsbx session before registering, dispatch once,
// never fall back locally, and keep an unconfirmed slot isolated across a
// restart. 01-scope V2-G3, 02-contracts V2-C03/C04/C07,
// 04-architecture §6/§8, V2-AC03/AC09/AC11.
package acceptance

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/release"
)

// TestV2FixedJobColdStartPreparesExactlyTwoCLIs covers V2-AC03/TC03 and
// JOB-08: the cache already has the Multica CLI, so only the selected Codex
// CLI is fetched; no Claude or omp artifact is requested.
func TestV2FixedJobColdStartPreparesExactlyTwoCLIs(t *testing.T) {
	binding := v2FixedBinding()
	cache := &v2Cache{
		present:   map[string]bool{attempt.KindMultica: true},
		artifacts: v2Artifacts(binding),
	}
	remote := &v2Remote{session: v2ReadySession(binding), close: attempt.CloseEvidence{Confirmed: true}}
	registrar := &v2Registrar{}
	runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	slots := newV2Slots()
	supervisor := v2FixedSupervisor(t, v2StorePath(t), cache, remote, registrar, runner, slots)

	outcome, err := supervisor.Run(context.Background(), binding)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if outcome.Stage != attempt.StageClosed || !outcome.Delivered {
		t.Fatalf("outcome = %+v, want a delivered closed attempt", outcome)
	}
	if got := kinds(cache.calls()); got != "multica,agent" {
		t.Fatalf("requested CLIs = %s, want exactly multica,agent", got)
	}
	if got := kinds(cache.fetches()); got != "agent" {
		t.Fatalf("fetched CLIs = %s, want only the missing agent CLI", got)
	}
	for _, cli := range append(cache.calls(), cache.fetches()...) {
		if cli.Backend == "claude" || cli.Backend == "omp" {
			t.Fatalf("the Job requested an unselected backend %q", cli.Backend)
		}
	}
	if remote.readyCalls != 1 || registrar.calls != 1 || runner.calls != 1 {
		t.Fatalf("ready=%d register=%d run=%d, want one each", remote.readyCalls, registrar.calls, runner.calls)
	}
	if got := slots.released(); len(got) != 1 || !got[0] {
		t.Fatalf("slot releases = %v, want one confirmed release", got)
	}
}

// TestV2FixedJobPrepareFailureIsZeroExecution covers V2-AC03/TC03: a cache
// that cannot supply the selected Agent CLI refuses before rsbx, registration
// or any business command, and releases the reservation with no remote side.
func TestV2FixedJobPrepareFailureIsZeroExecution(t *testing.T) {
	binding := v2FixedBinding()
	cache := &v2Cache{artifacts: v2Artifacts(binding)}
	delete(cache.artifacts, attempt.KindAgent)
	remote := &v2Remote{session: v2ReadySession(binding), close: attempt.CloseEvidence{Confirmed: true}}
	registrar := &v2Registrar{}
	runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	slots := newV2Slots()
	supervisor := v2FixedSupervisor(t, v2StorePath(t), cache, remote, registrar, runner, slots)

	outcome, err := supervisor.Run(context.Background(), binding)
	if !errors.Is(err, attempt.ErrRefused) {
		t.Fatalf("Run error = %v, want a refusal", err)
	}
	if outcome.Stage != attempt.StageRefused || outcome.Delivered {
		t.Fatalf("outcome = %+v, want a refused, undelivered attempt", outcome)
	}
	assertZeroBusinessExecution(t, remote, registrar, runner)
	if got := slots.released(); len(got) != 1 || !got[0] {
		t.Fatalf("slot releases = %v, want the reservation returned", got)
	}
}

// TestV2FixedJobStartedCommitFailureIsZeroExecution pins the dispatch fence:
// the shared "started" commit is what lets the server hand the payload over.
// A commit that fails refuses the attempt before the runner, so no business
// command runs even though registration succeeded.
func TestV2FixedJobStartedCommitFailureIsZeroExecution(t *testing.T) {
	binding := v2FixedBinding()
	cache := &v2Cache{artifacts: v2Artifacts(binding)}
	// The reference adapter cannot confirm the remote close through the
	// carrier; the reservation therefore stays isolated.
	remote := &v2Remote{session: v2ReadySession(binding), close: attempt.CloseEvidence{Confirmed: false}}
	registrar := &v2Registrar{}
	runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	slots := newV2Slots()
	slots.startErr = errors.New("the shared ledger refused the start commit")
	supervisor := v2FixedSupervisor(t, v2StorePath(t), cache, remote, registrar, runner, slots)

	outcome, err := supervisor.Run(context.Background(), binding)
	if !errors.Is(err, attempt.ErrRefused) {
		t.Fatalf("Run error = %v, want a refusal", err)
	}
	if outcome.Stage != attempt.StageRefused || outcome.Delivered {
		t.Fatalf("outcome = %+v, want a refused, undelivered attempt", outcome)
	}
	if runner.calls != 0 {
		t.Fatalf("runner calls = %d, want zero business execution", runner.calls)
	}
	if got := slots.released(); len(got) != 1 || got[0] {
		t.Fatalf("slot releases = %v, want one unconfirmed release (isolated)", got)
	}
}

// TestV2FixedJobReadyUnknownIsolatesWithoutFallback covers V2-AC03/TC03 and
// 04-architecture §6 ②: a lost ready answer may still have created a session,
// so the slot is isolated, never released, and no Pod-local execution starts.
func TestV2FixedJobReadyUnknownIsolatesWithoutFallback(t *testing.T) {
	binding := v2FixedBinding()
	cache := &v2Cache{artifacts: v2Artifacts(binding)}
	remote := &v2Remote{readyErr: errors.New("ready response lost after the session was created")}
	registrar := &v2Registrar{}
	runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	slots := newV2Slots()
	storePath := v2StorePath(t)
	supervisor := v2FixedSupervisor(t, storePath, cache, remote, registrar, runner, slots)

	outcome, err := supervisor.Run(context.Background(), binding)
	if !errors.Is(err, attempt.ErrRefused) {
		t.Fatalf("Run error = %v, want a refusal with zero business execution", err)
	}
	if outcome.Stage != attempt.StageIsolated || !outcome.Isolated || outcome.Delivered {
		t.Fatalf("outcome = %+v, want an isolated, undelivered attempt", outcome)
	}
	if got := slots.released(); len(got) != 1 || got[0] {
		t.Fatalf("slot releases = %v, want exactly one unconfirmed release", got)
	}
	if remote.closeCalls != 0 {
		t.Fatalf("close calls = %d: there is no known session to close", remote.closeCalls)
	}
	assertZeroBusinessExecution(t, remote, registrar, runner)
	recordStore, loadErr := attempt.NewFileStore(storePath)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	got, ok, err := recordStore.Load(context.Background(), binding.AttemptID)
	if err != nil || !ok {
		t.Fatalf("isolated record = (%+v, %v, %v), want it persisted", got, ok, err)
	}
	if !got.Isolated || got.Closed || got.Started {
		t.Fatalf("record = %+v, want a persisted isolated, unstarted attempt", got)
	}
}

// TestV2FixedJobReadyNotCreatedReleasesReservation covers the one ready
// failure that proves no remote session exists: capacity returns immediately.
func TestV2FixedJobReadyNotCreatedReleasesReservation(t *testing.T) {
	binding := v2FixedBinding()
	cache := &v2Cache{artifacts: v2Artifacts(binding)}
	remote := &v2Remote{readyErr: fmt.Errorf("%w: the helper refused before creating", attempt.ErrSessionNotCreated)}
	registrar := &v2Registrar{}
	runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	slots := newV2Slots()
	supervisor := v2FixedSupervisor(t, v2StorePath(t), cache, remote, registrar, runner, slots)

	outcome, err := supervisor.Run(context.Background(), binding)
	if !errors.Is(err, attempt.ErrRefused) {
		t.Fatalf("Run error = %v, want a refusal", err)
	}
	if outcome.Stage != attempt.StageRefused || outcome.Isolated {
		t.Fatalf("outcome = %+v, want a refused attempt with no remote session", outcome)
	}
	if got := slots.released(); len(got) != 1 || !got[0] {
		t.Fatalf("slot releases = %v, want the reservation returned", got)
	}
	assertZeroBusinessExecution(t, remote, registrar, runner)
}

// TestV2FixedJobRegistrationMismatchIsZeroExecution covers 04-architecture
// §6 ②: a daemon that registers another identity/runtime never receives the
// task; the opened session is closed instead.
func TestV2FixedJobRegistrationMismatchIsZeroExecution(t *testing.T) {
	binding := v2FixedBinding()
	cache := &v2Cache{artifacts: v2Artifacts(binding)}
	remote := &v2Remote{session: v2ReadySession(binding), close: attempt.CloseEvidence{Confirmed: true}}
	registrar := &v2Registrar{registration: attempt.Registration{DaemonID: "another-daemon"}}
	runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	slots := newV2Slots()
	supervisor := v2FixedSupervisor(t, v2StorePath(t), cache, remote, registrar, runner, slots)

	outcome, err := supervisor.Run(context.Background(), binding)
	if !errors.Is(err, attempt.ErrRefused) {
		t.Fatalf("Run error = %v, want a refusal", err)
	}
	if outcome.Stage != attempt.StageRefused || outcome.Session.ID == "" {
		t.Fatalf("outcome = %+v, want a refused attempt that closed its session", outcome)
	}
	if runner.calls != 0 {
		t.Fatalf("business runs = %d, want zero after a registration mismatch", runner.calls)
	}
	if registrar.calls != 1 || remote.closeCalls != 1 {
		t.Fatalf("register=%d close=%d, want one registration check and one close", registrar.calls, remote.closeCalls)
	}
}

// TestV2FixedJobInterruptionNeverReplaysOrSwitchesEndpoint covers V2-AC09/TC09:
// an interrupted dispatch is recorded as started; a restarted Job returns
// interrupted without a second run, and a binding with another endpoint is
// refused instead of moving the session.
func TestV2FixedJobInterruptionNeverReplaysOrSwitchesEndpoint(t *testing.T) {
	storePath := v2StorePath(t)
	binding := v2FixedBinding()
	cache := &v2Cache{artifacts: v2Artifacts(binding)}
	remote := &v2Remote{session: v2ReadySession(binding), close: attempt.CloseEvidence{Confirmed: true}}
	registrar := &v2Registrar{}
	runner := &v2Runner{result: attempt.RunResult{Delivered: true}, err: errors.New("agent stream reset")}
	slots := newV2Slots()

	first := v2FixedSupervisor(t, storePath, cache, remote, registrar, runner, slots)
	outcome, err := first.Run(context.Background(), binding)
	if !errors.Is(err, attempt.ErrInterrupted) {
		t.Fatalf("first Run error = %v, want interrupted", err)
	}
	if outcome.Stage != attempt.StageInterrupted || !outcome.Delivered {
		t.Fatalf("first outcome = %+v, want interrupted with possible side effects", outcome)
	}
	if runner.calls != 1 {
		t.Fatalf("first run calls = %d, want exactly one dispatch", runner.calls)
	}

	// A fresh supervisor reads the same durable record: the process restarted.
	restartRunner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	restart := v2FixedSupervisor(t, storePath, cache, remote, registrar, restartRunner, slots)
	outcome, err = restart.Run(context.Background(), binding)
	if !errors.Is(err, attempt.ErrInterrupted) {
		t.Fatalf("restart Run error = %v, want interrupted", err)
	}
	if restartRunner.calls != 0 {
		t.Fatalf("restart dispatched %d times, want zero replays", restartRunner.calls)
	}
	if outcome.Stage != attempt.StageInterrupted {
		t.Fatalf("restart stage = %s, want interrupted", outcome.Stage)
	}
	if outcome.Isolated {
		t.Fatalf("restart outcome = %+v, want the proven close to stay closed", outcome)
	}

	relocated := binding
	relocated.Endpoint = "build-b.example.test:7443"
	movedRunner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	movedRemote := &v2Remote{close: attempt.CloseEvidence{Confirmed: true}}
	moved := v2FixedSupervisor(t, storePath, cache, movedRemote, registrar, movedRunner, slots)
	outcome, err = moved.Run(context.Background(), relocated)
	if !errors.Is(err, attempt.ErrSessionRelocated) {
		t.Fatalf("moved Run error = %v, want session_relocated", err)
	}
	if movedRunner.calls != 0 || movedRemote.readyCalls != 0 {
		t.Fatalf("endpoint switch reached remote/business: run=%d ready=%d", movedRunner.calls, movedRemote.readyCalls)
	}
	if outcome.Stage != attempt.StageRefused {
		t.Fatalf("moved stage = %s, want refused", outcome.Stage)
	}
}

// TestV2FixedJobUnconfirmedCloseStaysIsolatedAcrossRestart covers V2-AC09/TC09
// and V2-C03: only a proven remote close releases the slot; otherwise the
// durable attempt and the ledger stay isolated, and a restart does not rerun.
func TestV2FixedJobUnconfirmedCloseStaysIsolatedAcrossRestart(t *testing.T) {
	storePath := v2StorePath(t)
	binding := v2FixedBinding()
	cache := &v2Cache{artifacts: v2Artifacts(binding)}
	remote := &v2Remote{
		session:  v2ReadySession(binding),
		close:    attempt.CloseEvidence{Confirmed: false, Detail: "the listener accepted the close but the session is still listed"},
		closeErr: nil,
	}
	registrar := &v2Registrar{}
	runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	slots := newV2Slots()

	first := v2FixedSupervisor(t, storePath, cache, remote, registrar, runner, slots)
	outcome, err := first.Run(context.Background(), binding)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if outcome.Stage != attempt.StageIsolated || !outcome.Isolated || outcome.Close.Confirmed {
		t.Fatalf("outcome = %+v, want an isolated, unconfirmed close", outcome)
	}
	if got := slots.released(); len(got) != 1 || got[0] {
		t.Fatalf("slot releases = %v, want one unconfirmed release", got)
	}

	restartRunner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	restart := v2FixedSupervisor(t, storePath, cache, remote, registrar, restartRunner, slots)
	outcome, err = restart.Run(context.Background(), binding)
	if !errors.Is(err, attempt.ErrInterrupted) {
		t.Fatalf("restart Run error = %v, want interrupted (not a rerun)", err)
	}
	if !outcome.Isolated {
		t.Fatalf("restart outcome = %+v, want the isolated slot still visible", outcome)
	}
	if restartRunner.calls != 0 {
		t.Fatalf("restart business runs = %d, want zero", restartRunner.calls)
	}
}

// TestV2FixedJobIsolatedRegistrationFailureRecoversBeforePrepare covers
// V2-C03: a registration failure whose close was not confirmed leaves an
// isolated record even before dispatch. A restart must process that session
// first, not prepare again, open another session, or release the slot.
func TestV2FixedJobIsolatedRegistrationFailureRecoversBeforePrepare(t *testing.T) {
	storePath := v2StorePath(t)
	binding := v2FixedBinding()
	cache := &v2Cache{artifacts: v2Artifacts(binding)}
	remote := &v2Remote{
		session: v2ReadySession(binding),
		close:   attempt.CloseEvidence{Confirmed: false, Detail: "close not acknowledged"},
	}
	registrar := &v2Registrar{err: errors.New("registration rejected")}
	runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	slots := newV2Slots()

	first := v2FixedSupervisor(t, storePath, cache, remote, registrar, runner, slots)
	outcome, err := first.Run(context.Background(), binding)
	if !errors.Is(err, attempt.ErrRefused) {
		t.Fatalf("first Run error = %v, want a refusal", err)
	}
	if !outcome.Isolated || outcome.Session.ID == "" || outcome.Delivered {
		t.Fatalf("first outcome = %+v, want an isolated session left behind", outcome)
	}
	if got := slots.released(); len(got) != 1 || got[0] {
		t.Fatalf("slot releases = %v, want one unconfirmed release", got)
	}

	restartCache := &v2Cache{err: errors.New("preparation must not run")}
	restartRemote := &v2Remote{session: v2ReadySession(binding), close: attempt.CloseEvidence{Confirmed: false}}
	restartRegistrar := &v2Registrar{}
	restartRunner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	restart := v2FixedSupervisor(t, storePath, restartCache, restartRemote, restartRegistrar, restartRunner, slots)
	outcome, err = restart.Run(context.Background(), binding)
	if !errors.Is(err, attempt.ErrRefused) {
		t.Fatalf("restart Run error = %v, want a refusal", err)
	}
	if !outcome.Isolated || outcome.Stage != attempt.StageIsolated {
		t.Fatalf("restart outcome = %+v, want the original session still isolated", outcome)
	}
	if calls := restartCache.calls(); len(calls) != 0 {
		t.Fatalf("restart prepared %v, want the isolated session handled first", calls)
	}
	if restartRemote.readyCalls != 0 || restartRegistrar.calls != 0 || restartRunner.calls != 0 {
		t.Fatalf("restart opened/used a new session: ready=%d register=%d run=%d",
			restartRemote.readyCalls, restartRegistrar.calls, restartRunner.calls)
	}
	if got := slots.released(); len(got) != 2 || got[1] {
		t.Fatalf("slot releases = %v, want no true release on restart", got)
	}
}

// TestV2FixedJobSharedLedgerGatesRebuiltPods covers the cross-Pod boundary:
// the local record may disappear with the Pod, but the shared reservation still
// carries Started/isolated and the original session, so a rebuilt Job never
// re-dispatches or opens a second session.
func TestV2FixedJobSharedLedgerGatesRebuiltPods(t *testing.T) {
	for _, tc := range []struct {
		name         string
		delivered    bool
		registrarErr error
		wantStarted  bool
	}{
		{name: "started attempt", delivered: true, wantStarted: true},
		{name: "undispatched registration failure", registrarErr: errors.New("registration rejected")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storePath := v2StorePath(t)
			binding := v2FixedBinding()
			firstCache := &v2Cache{artifacts: v2Artifacts(binding)}
			firstRemote := &v2Remote{
				session: v2ReadySession(binding),
				close:   attempt.CloseEvidence{Confirmed: false, Detail: "close not acknowledged"},
			}
			firstRegistrar := &v2Registrar{err: tc.registrarErr}
			firstRunner := &v2Runner{result: attempt.RunResult{Delivered: tc.delivered}}
			slots := newV2Slots()
			first := v2FixedSupervisor(t, storePath, firstCache, firstRemote, firstRegistrar, firstRunner, slots)
			outcome, err := first.Run(context.Background(), binding)
			if tc.registrarErr == nil {
				if err != nil || outcome.Stage != attempt.StageIsolated {
					t.Fatalf("first outcome = (%+v, %v), want an isolated completed run", outcome, err)
				}
			} else if !errors.Is(err, attempt.ErrRefused) || !outcome.Isolated {
				t.Fatalf("first outcome = (%+v, %v), want an isolated refusal", outcome, err)
			}
			if _, err := os.Stat(storePath); err != nil {
				t.Fatalf("the first attempt did not write the configured store: %v", err)
			}
			if err := os.Remove(storePath); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("remove the local record: %v", err)
			}

			restartCache := &v2Cache{err: errors.New("preparation must not run")}
			restartRemote := &v2Remote{session: v2ReadySession(binding), close: attempt.CloseEvidence{Confirmed: false}}
			restartRegistrar := &v2Registrar{}
			restartRunner := &v2Runner{result: attempt.RunResult{Delivered: true}}
			restart := v2FixedSupervisor(t, storePath, restartCache, restartRemote, restartRegistrar, restartRunner, slots)
			outcome, err = restart.Run(context.Background(), binding)
			if err == nil {
				t.Fatalf("restart Run succeeded, want the unresolved session to stay isolated")
			}
			if !outcome.Isolated || outcome.Delivered {
				t.Fatalf("restart outcome = %+v, want isolated and no new dispatch", outcome)
			}
			if len(restartCache.calls()) != 0 {
				t.Fatalf("restart prepared CLIs without the local record")
			}
			if restartRemote.readyCalls != 0 || restartRegistrar.calls != 0 || restartRunner.calls != 0 {
				t.Fatalf("restart opened/used another session: ready=%d register=%d run=%d",
					restartRemote.readyCalls, restartRegistrar.calls, restartRunner.calls)
			}
			for _, confirmed := range slots.released() {
				if confirmed {
					t.Fatalf("a true release escaped while the remote session was unconfirmed: %v", slots.released())
				}
			}
			status, err := slots.Status(context.Background(), binding)
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if !status.Exists || status.State != attempt.SlotIsolated || status.SessionID == "" {
				t.Fatalf("ledger = %+v, want the original isolated session", status)
			}
			if status.Started != tc.wantStarted {
				t.Fatalf("ledger started = %v, want %v", status.Started, tc.wantStarted)
			}
		})
	}
}

// TestV2FixedJobReadyIdentityMustMatchFrozenFacts covers V2-C03: a ready
// response missing the generation or endpoint, naming another tree/endpoint,
// or naming a different session than the frozen one never reaches business
// execution.
func TestV2FixedJobReadyIdentityMustMatchFrozenFacts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*attempt.Session)
		binding func(*attempt.Binding)
	}{
		{name: "missing session id", mutate: func(s *attempt.Session) { s.ID = "" }},
		{name: "missing environment", mutate: func(s *attempt.Session) { s.EnvironmentID = "" }},
		{name: "wrong environment", mutate: func(s *attempt.Session) { s.EnvironmentID = "build-b" }},
		{name: "missing generation", mutate: func(s *attempt.Session) { s.Generation = "" }},
		{name: "wrong generation", mutate: func(s *attempt.Session) { s.Generation = "tree-2" }},
		{name: "missing endpoint", mutate: func(s *attempt.Session) { s.Endpoint = "" }},
		{name: "wrong endpoint", mutate: func(s *attempt.Session) { s.Endpoint = "build-b.example.test:7443" }},
		{name: "wrong frozen session", binding: func(b *attempt.Binding) { b.SessionID = "session-expected" }, mutate: func(s *attempt.Session) { s.ID = "session-other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := v2FixedBinding()
			if tc.binding != nil {
				tc.binding(&binding)
			}
			session := v2ReadySession(binding)
			if tc.mutate != nil {
				tc.mutate(&session)
			}
			cache := &v2Cache{artifacts: v2Artifacts(binding)}
			remote := &v2Remote{session: session, close: attempt.CloseEvidence{Confirmed: true}}
			registrar := &v2Registrar{}
			runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
			slots := newV2Slots()
			supervisor := v2FixedSupervisor(t, v2StorePath(t), cache, remote, registrar, runner, slots)

			outcome, err := supervisor.Run(context.Background(), binding)
			if !errors.Is(err, attempt.ErrRefused) {
				t.Fatalf("Run error = %v, want a refusal", err)
			}
			if outcome.Delivered {
				t.Fatalf("outcome = %+v, want zero business execution", outcome)
			}
			if registrar.calls != 0 || runner.calls != 0 {
				t.Fatalf("register=%d run=%d, want zero business execution", registrar.calls, runner.calls)
			}
			if remote.readyCalls != 1 || len(remote.readyEnvs) != 1 || remote.readyEnvs[0] != binding.EnvironmentID {
				t.Fatalf("ready calls = %d envs=%v, want exactly the frozen endpoint", remote.readyCalls, remote.readyEnvs)
			}
			if remote.closeCalls != 1 {
				t.Fatalf("close calls = %d, want the opened session closed", remote.closeCalls)
			}
			if got := slots.released(); len(got) != 1 || !got[0] {
				t.Fatalf("slot releases = %v, want a confirmed release after close", got)
			}
		})
	}
}

// TestV2FixedJobBindingRefusesUnfrozenCarrierAndProxyInputs covers V2-AC19:
// a binding without the digest-pinned rsbx carrier, a matching client
// platform, or a proxy-only download address never reaches preparation,
// rsbx, registration or a business command.
func TestV2FixedJobBindingRefusesUnfrozenCarrierAndProxyInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*attempt.Binding)
	}{
		{name: "missing image digest", mutate: func(b *attempt.Binding) { b.ImageDigest = "" }},
		{name: "tagged carrier", mutate: func(b *attempt.Binding) { b.Image = "ghcr.io/tsic404/rsbx:latest" }},
		{name: "platform mismatch", mutate: func(b *attempt.Binding) { b.CLIs[0].Platform = "linux/arm64" }},
		{name: "direct release source", mutate: func(b *attempt.Binding) {
			b.CLIs[1].DownloadURL = "https://github.com/multica-ai/multica/releases/download/v0.6.1/agent"
		}},
		{name: "movable CLI version", mutate: func(b *attempt.Binding) { b.CLIs[0].Version = "latest" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := v2FixedBinding()
			tc.mutate(&binding)
			cache := &v2Cache{artifacts: v2Artifacts(binding)}
			remote := &v2Remote{session: v2ReadySession(binding), close: attempt.CloseEvidence{Confirmed: true}}
			registrar := &v2Registrar{}
			runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
			supervisor := v2FixedSupervisor(t, v2StorePath(t), cache, remote, registrar, runner, newV2Slots())

			outcome, err := supervisor.Run(context.Background(), binding)
			if !errors.Is(err, attempt.ErrRefused) || outcome.Stage != attempt.StageRefused {
				t.Fatalf("Run = (%+v, %v), want a refusal", outcome, err)
			}
			if len(cache.calls()) != 0 {
				t.Fatalf("preparation ran for an unfrozen binding: %v", cache.calls())
			}
			assertZeroBusinessExecution(t, remote, registrar, runner)
		})
	}
}

// TestV2FixedJobDownloadFailureNamesTheArtifactAndNeverFallsBack covers
// V2-AC19 and V2-C11: a proxy refusal or budget exhaustion stops the attempt
// before rsbx, registration and business execution; the cache is asked for
// exactly the frozen two artifacts.
func TestV2FixedJobDownloadFailureNamesTheArtifactAndNeverFallsBack(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stage string
	}{
		{name: "proxy refused", stage: "download"},
		{name: "budget exhausted", stage: "budget"},
		{name: "digest mismatch", stage: "verify"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := v2FixedBinding()
			agent := binding.CLIs[1]
			cache := &v2Cache{
				artifacts: v2Artifacts(binding), failKind: agent.Kind,
				err: &attempt.DownloadError{
					Kind: agent.Kind, Backend: agent.Backend, Version: agent.Version,
					Platform: agent.Platform, Digest: agent.Digest, Stage: tc.stage,
					Reason: "refused by the frozen proxy",
				},
			}
			remote := &v2Remote{session: v2ReadySession(binding), close: attempt.CloseEvidence{Confirmed: true}}
			registrar := &v2Registrar{}
			runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
			slots := newV2Slots()
			supervisor := v2FixedSupervisor(t, v2StorePath(t), cache, remote, registrar, runner, slots)

			outcome, err := supervisor.Run(context.Background(), binding)
			if !errors.Is(err, attempt.ErrRefused) || outcome.Stage != attempt.StageRefused || outcome.Delivered {
				t.Fatalf("Run = (%+v, %v), want a refused, undelivered attempt", outcome, err)
			}
			if !strings.Contains(err.Error(), agent.Kind) || !strings.Contains(err.Error(), agent.Digest) {
				t.Fatalf("error = %v, want the failing CLI kind and frozen digest", err)
			}
			assertZeroBusinessExecution(t, remote, registrar, runner)
			calls := cache.calls()
			if len(calls) != 2 || calls[1].Kind != agent.Kind {
				t.Fatalf("cache calls = %v, want the Multica CLI then the failing frozen Agent artifact", calls)
			}
			if got := slots.released(); len(got) != 1 || !got[0] {
				t.Fatalf("slot releases = %v, want the idle reservation returned", got)
			}
		})
	}
}

// TestV2FixedJobUnauthorizedCLIRequestIsRefusedBeforeReady covers V2-AC19 and
// V2-C09/C11: the download face authorizes the attempt's exact frozen tuple,
// so a published neighbour version is refused before any bytes are read and
// the Job never opens a session or registers.
func TestV2FixedJobUnauthorizedCLIRequestIsRefusedBeforeReady(t *testing.T) {
	frozenMultica := "sha256:" + strings.Repeat("2", 64)
	frozenAgent := "sha256:" + strings.Repeat("3", 64)
	newerMultica := "sha256:" + strings.Repeat("4", 64)
	releases := &v2DownloadReleases{
		server: "src-a",
		releases: []v2PublishedCLI{
			{kind: release.KindMultica, version: "0.6.1", platform: "linux/amd64",
				digest: frozenMultica, body: []byte("#!/bin/sh\necho multica\n")},
			{kind: release.KindMultica, version: "0.7.0", platform: "linux/amd64",
				digest: newerMultica, body: []byte("#!/bin/sh\necho newer\n")},
			{kind: release.KindAgent, adapter: "codex", version: "0.167.1", platform: "linux/amd64",
				digest: frozenAgent, body: []byte("#!/bin/sh\necho codex\n")},
		},
	}
	frozen := v2FixedBinding()
	handler := &proxy.CLIDistribution{
		Verifier: v2DownloadVerifier{claims: auth.Claims{
			TaskID: frozen.TaskID, SourceID: frozen.SourceID, WorkspaceID: frozen.WorkspaceID,
		}},
		Releases: releases,
		Attempts: v2DownloadBindings{binding: frozen},
	}
	mux := http.NewServeMux()
	mux.Handle("GET /cli/{kind}/{version}/{os}/{arch}", handler)
	server := httptest.NewServer(mux)
	defer server.Close()

	// The Job-side binding asks for a published version this attempt did not
	// freeze; the authoritative row still names 0.6.1.
	binding := frozen
	binding.ProxyURL = server.URL
	binding.CLIs = append([]attempt.CLI(nil), frozen.CLIs...)
	binding.CLIs[0].Version = "0.7.0"
	binding.CLIs[0].Digest = newerMultica
	binding.CLIs[0].DownloadURL = server.URL + "/cli/multica/0.7.0/linux/amd64?digest=" + newerMultica
	binding.CLIs[1].DownloadURL = server.URL + "/cli/agent/0.167.1/linux/amd64?digest=" + frozenAgent + "&backend=codex"

	store, err := attempt.NewFileStore(v2StorePath(t))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	remote := &v2Remote{session: v2ReadySession(binding), close: attempt.CloseEvidence{Confirmed: true}}
	registrar := &v2Registrar{}
	runner := &v2Runner{result: attempt.RunResult{Delivered: true}}
	slots := newV2Slots()
	supervisor := &attempt.Supervisor{
		Cache:  &attempt.ProxyCache{BaseURL: server.URL, Token: "fmj_attempt", Dir: t.TempDir()},
		Remote: remote, Registrar: registrar, Runner: runner, Slots: slots, Store: store,
	}

	outcome, err := supervisor.Run(context.Background(), binding)
	if !errors.Is(err, attempt.ErrRefused) {
		t.Fatalf("Run error = %v, want a refusal", err)
	}
	if outcome.Stage != attempt.StageRefused || outcome.Delivered {
		t.Fatalf("outcome = %+v, want a refused, undelivered attempt", outcome)
	}
	var download *attempt.DownloadError
	if !errors.As(err, &download) || download.Stage != "download" {
		t.Fatalf("Run error = %v, want the proxy's download refusal", err)
	}
	if !strings.Contains(download.Reason, "403") {
		t.Fatalf("download reason = %q, want the proxy's 403", download.Reason)
	}
	if releases.reads.Load() != 0 {
		t.Fatalf("the published artifact was read %d times", releases.reads.Load())
	}
	if remote.readyCalls != 0 || registrar.calls != 0 || runner.calls != 0 {
		t.Fatalf("ready=%d register=%d run=%d, want zero after the unauthorized tuple",
			remote.readyCalls, registrar.calls, runner.calls)
	}
	if got := slots.released(); len(got) != 1 || !got[0] {
		t.Fatalf("slot releases = %v, want the idle reservation returned", got)
	}
}

func assertZeroBusinessExecution(t *testing.T, remote *v2Remote, registrar *v2Registrar, runner *v2Runner) {
	t.Helper()
	if registrar.calls != 0 {
		t.Errorf("registration calls = %d, want zero business execution", registrar.calls)
	}
	if runner.calls != 0 {
		t.Errorf("business runs = %d, want zero", runner.calls)
	}
	if remote.readyCalls > 1 {
		t.Errorf("ready calls = %d, want at most the one refused attempt", remote.readyCalls)
	}
}

func kinds(clis []attempt.CLI) string {
	names := make([]string, 0, len(clis))
	for _, cli := range clis {
		names = append(names, cli.Kind)
	}
	return strings.Join(names, ",")
}
