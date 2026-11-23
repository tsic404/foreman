package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/environment/environmenttest"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
	"github.com/tsic404/foreman/internal/state"
)

// ledgerBinder adapts a real environment.Service to the scheduler's
// environment seam, the way the composition root's rulesBinder does: isolate,
// remote close and the trusted Pod-close half are the ledger's own transitions.
type ledgerBinder struct{ svc *environment.Service }

func (b ledgerBinder) Bind(context.Context, scheduler.EnvironmentRequest) (scheduler.EnvironmentBinding, error) {
	return scheduler.EnvironmentBinding{}, scheduler.ErrEnvironmentBlocked
}

func (b ledgerBinder) Release(ctx context.Context, binding scheduler.EnvironmentBinding) error {
	_, err := b.svc.ReleaseSlot(ctx, environment.ID(binding.EnvironmentID), binding.SlotID, true)
	return slotErr(err)
}

func (b ledgerBinder) Isolate(ctx context.Context, binding scheduler.EnvironmentBinding) error {
	_, err := b.svc.IsolateSlot(ctx, environment.ID(binding.EnvironmentID), binding.SlotID)
	return slotErr(err)
}

func (b ledgerBinder) ReleaseAfterPodClosed(ctx context.Context, binding scheduler.EnvironmentBinding) error {
	_, err := b.svc.ReleaseSlotOnPodClosed(ctx, environment.ID(binding.EnvironmentID), binding.SlotID)
	return slotErr(err)
}

// slotErr mirrors the rules adapter: a slot the ledger no longer holds is an
// already-settled release, not a failure.
func slotErr(err error) error {
	if errors.Is(err, environment.ErrNotFound) || errors.Is(err, environment.ErrSlotNotFound) {
		return nil
	}
	return err
}

func newLedgerService(t *testing.T, store environment.Store) *environment.Service {
	t.Helper()
	probe := environmenttest.NewProbe()
	probe.Result = environment.ProbeResult{
		TreeID: "tree-1", Identity: "identity-1",
		Capabilities: []string{"nix"}, Capacity: 1, Version: "rsbxd-test",
	}
	svc, err := environment.New(environment.Config{
		Store:               store,
		Probe:               probe,
		AllowedTrustDomains: []string{"build"},
		Clock:               time.Now,
	})
	if err != nil {
		t.Fatalf("environment.New: %v", err)
	}
	return svc
}

func registerSingleSlotLedger(t *testing.T, svc *environment.Service) {
	t.Helper()
	if _, err := svc.Register(context.Background(), environment.Registration{
		ID: "build-a", Endpoint: "build-a.example.test:7443", TLSRef: "ref/build-a",
		StateRoot: "/srv/rsbx/build-a", TrustDomain: "build",
		AllowedSources: []string{"atlas"}, Capabilities: []string{"nix"}, Concurrency: 1,
	}, "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}
}

func newLedgerScheduler(t *testing.T, reg *registry.Registry, jobs *fakeJobClient, svc *environment.Service) *scheduler.Scheduler {
	t.Helper()
	pending, err := scheduler.NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	cfg := schedConfig()
	cfg.EnvironmentBinder = ledgerBinder{svc: svc}
	sched, err := scheduler.New(cfg, reg, jobs, stubBuilder{}, newFakeServerClient(map[string]string{"t1": "running"}), nil,
		scheduler.WithPendingReports(pending),
		scheduler.WithRevoker(&fakeRevoker{}),
		scheduler.WithProbeRegistry(registry.NewProbeRegistry()))
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	return sched
}

// reserveAndSettle puts one reserved, already-remote-closed attempt into
// draining with the drain bound due immediately.
func reserveAndSettle(t *testing.T, svc *environment.Service, reg *registry.Registry) {
	t.Helper()
	ctx := context.Background()
	if _, err := svc.AcquireSlot(ctx, "build-a", "slot-1"); err != nil {
		t.Fatalf("AcquireSlot: %v", err)
	}
	// The remote close is already a fact (a prepare failure proves no session
	// was created); only the Pod is still alive, so capacity stays held.
	if _, err := svc.ConfirmRemoteClose(ctx, "build-a", "slot-1"); err != nil {
		t.Fatalf("ConfirmRemoteClose: %v", err)
	}
	entry := registry.TaskEntry{
		TaskID: "t1", JobName: "fm-t1", DaemonID: "fm-t1", JobRuntimeID: "rt-t1",
		State: registry.StateRunning, ClaimedAt: time.Now().Add(-time.Hour),
		JobCreatedAt:      time.Now().Add(-time.Minute),
		EnvironmentID:     "build-a",
		EnvironmentSlotID: "slot-1",
	}
	if err := reg.Put(entry); err != nil {
		t.Fatalf("registry.Put: %v", err)
	}
	if _, _, err := reg.MarkTerminal("t1", registry.ResultFailed, "", time.Now()); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
}

func assertCapacityHeld(t *testing.T, svc *environment.Service, why string) {
	t.Helper()
	if _, err := svc.AcquireSlot(context.Background(), "build-a", "probe"); !errors.Is(err, environment.ErrFull) {
		t.Fatalf("%s: AcquireSlot err = %v, want ErrFull", why, err)
	}
}

func newReconcilerOver(t *testing.T, reg *registry.Registry, sched *scheduler.Scheduler) *Reconciler {
	t.Helper()
	// The reconciler's object face is cluster-wide; the scheduler's own job
	// client (fakeJobClient) carries the termination evidence it reads.
	return newTestReconciler(t, reg, &fakeObjects{}, newFakeServerClient(map[string]string{"t1": "running"}), sched)
}

// TestSlotCloseDebtIsKeptUntilThePodIsReallyGone pins the same-attempt
// lifecycle the drain bound used to break: the credential is reclaimed at the
// bound and the entry is kept as the Pod-close debt handle, so the later,
// real Pod disappearance still submits the trusted half and returns capacity.
func TestSlotCloseDebtIsKeptUntilThePodIsReallyGone(t *testing.T) {
	ctx := context.Background()
	svc := newLedgerService(t, environment.NewMemoryStore())
	registerSingleSlotLedger(t, svc)
	reg := registry.New(time.Now) // zero drain bound: due on the next round
	jobs := newFakeJobClient()
	sched := newLedgerScheduler(t, reg, jobs, svc)
	reserveAndSettle(t, svc, reg)
	assertCapacityHeld(t, svc, "the remote close alone must keep the slot held")

	jobs.setPods("fm-t1", drainPod("fm-t1", corev1.PodRunning))
	rec := newReconcilerOver(t, reg, sched)
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile at the drain bound: %v", err)
	}
	kept, ok := reg.Get("t1")
	if !ok || !kept.SlotCloseOwed || !kept.IsDraining() {
		t.Fatalf("the drain bound dropped the Pod-close handle: %+v ok=%v", kept, ok)
	}
	assertCapacityHeld(t, svc, "a drain bound must never release the slot")

	// The Pod really disappears: the same entry's debt resolves and the
	// capacity comes back without a second timed release.
	jobs.setPods("fm-t1")
	if err := rec.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile after the Pod is gone: %v", err)
	}
	if _, ok := reg.Get("t1"); ok {
		t.Fatal("the resolved Pod-close debt left a live entry behind")
	}
	if _, err := svc.AcquireSlot(ctx, "build-a", "slot-2"); err != nil {
		t.Fatalf("the trusted Pod-close fact did not return the capacity: %v", err)
	}
}

// TestSlotCloseDebtSurvivesARestart pins the durable half: the kept entry is
// the debt's only handle, so a process restart must restore both the marker
// and the draining Job phase and still resolve the reservation when the Pod
// is gone.
func TestSlotCloseDebtSurvivesARestart(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	store, err := state.New(state.Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    48 * time.Hour,
	})
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	svc := newLedgerService(t, environment.NewMemoryStore())
	registerSingleSlotLedger(t, svc)
	jobs := newFakeJobClient()

	regA := registry.New(time.Now, registry.WithStateStore(store))
	t.Cleanup(func() { regA.Close(ctx) })
	schedA := newLedgerScheduler(t, regA, jobs, svc)
	reserveAndSettle(t, svc, regA)
	jobs.setPods("fm-t1", drainPod("fm-t1", corev1.PodRunning))
	if err := newReconcilerOver(t, regA, schedA).Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile at the drain bound: %v", err)
	}
	if kept, ok := regA.Get("t1"); !ok || !kept.SlotCloseOwed {
		t.Fatalf("the drain bound did not keep the debt: %+v ok=%v", kept, ok)
	}
	regA.Close(ctx)

	regB := registry.New(time.Now, registry.WithStateStore(store))
	t.Cleanup(func() { regB.Close(ctx) })
	if _, restored, err := regB.Restore(ctx); err != nil || !restored {
		t.Fatalf("Restore: restored=%v err=%v", restored, err)
	}
	kept, ok := regB.Get("t1")
	if !ok || !kept.SlotCloseOwed || !kept.IsDraining() {
		t.Fatalf("the restart dropped the Pod-close debt: %+v ok=%v", kept, ok)
	}

	jobs.setPods("fm-t1")
	if err := newReconcilerOver(t, regB, newLedgerScheduler(t, regB, jobs, svc)).Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile after the restart: %v", err)
	}
	if _, ok := regB.Get("t1"); ok {
		t.Fatal("the restored debt was not resolved by the trusted Pod-close fact")
	}
	if _, err := svc.AcquireSlot(ctx, "build-a", "slot-2"); err != nil {
		t.Fatalf("the restored debt did not return the capacity: %v", err)
	}
}
