package environment_test

import (
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/environment/environmenttest"
)

var testNow = time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)

func testClock() time.Time { return testNow }

func probeFacts(tree string) environment.ProbeResult {
	return environment.ProbeResult{
		TreeID:       tree,
		Identity:     "sha256:" + tree,
		Capabilities: []string{"nix"},
		Capacity:     8,
		Version:      "rsbxd-test",
	}
}

func registration(id, endpoint, root string, capabilities []string, concurrency int) environment.Registration {
	return environment.Registration{
		ID:             id,
		Endpoint:       endpoint,
		TLSRef:         "ref/" + id,
		StateRoot:      root,
		TrustDomain:    "build",
		AllowedSources: []string{"atlas"},
		Capabilities:   capabilities,
		Concurrency:    concurrency,
	}
}

func newService(t *testing.T, probe environment.Probe) *environment.Service {
	t.Helper()
	service, err := environment.New(environment.Config{
		Probe:               probe,
		AllowedTrustDomains: []string{"build", "lab", "delivery"},
		Clock:               testClock,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return service
}

// ENV-04 / V2-C07: a target that cannot be verified is stored with its failure
// on its face, but it is not an environment and cannot take a slot.
func TestRegisterLeavesAFailedTargetOutOfThePool(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.Err = errors.New("connection refused")
	service := newService(t, probe)
	ctx := t.Context()

	view, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if view.Verified || view.Schedulable {
		t.Fatalf("a failed target is verified=%v schedulable=%v, want both false", view.Verified, view.Schedulable)
	}
	if view.Phase != string(environment.PhasePending) {
		t.Fatalf("phase = %q, want pending", view.Phase)
	}
	if view.Verification.State != string(environment.VerificationFailed) {
		t.Fatalf("verification state = %q, want failed", view.Verification.State)
	}
	if service.CountRegistered(ctx) != 0 {
		t.Fatalf("CountRegistered = %d, want 0: an unverified target is not an environment", service.CountRegistered(ctx))
	}
	if candidates := service.Candidates(ctx, environment.CandidateFilter{}); len(candidates) != 0 {
		t.Fatalf("Candidates = %+v, want none for an unverified target", candidates)
	}
	if _, err := service.AcquireSlot(ctx, "build-a", "slot-1", "session-1"); !errors.Is(err, environment.ErrNotVerified) {
		t.Fatalf("AcquireSlot on an unverified target = %v, want ErrNotVerified", err)
	}
}

// V2-C07 / 04-architecture §3.3: identity, declared capabilities and the
// configured limit are checked before the target enters the pool.
func TestRegisterChecksIdentityCapabilitiesAndCapacity(t *testing.T) {
	cases := []struct {
		name     string
		result   environment.ProbeResult
		expected string
		want     string
	}{
		{
			name:     "missing capability",
			result:   environment.ProbeResult{TreeID: "tree-1", Identity: "sha256:one", Capabilities: []string{"docker"}, Capacity: 8},
			expected: "",
			want:     "nix",
		},
		{
			name:     "capacity too small",
			result:   environment.ProbeResult{TreeID: "tree-1", Identity: "sha256:one", Capabilities: []string{"nix"}, Capacity: 2},
			expected: "",
			want:     "capacity",
		},
		{
			name:     "identity mismatch",
			result:   environment.ProbeResult{TreeID: "tree-1", Identity: "sha256:one", Capabilities: []string{"nix"}, Capacity: 8},
			expected: "sha256:two",
			want:     "identity",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probe := environmenttest.NewProbe()
			probe.Result = tc.result
			service := newService(t, probe)
			reg := registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 4)
			reg.ExpectedIdentity = tc.expected
			view, err := service.Register(t.Context(), reg, "ops")
			if err != nil {
				t.Fatalf("Register: %v", err)
			}
			if view.Verified || view.Schedulable {
				t.Fatalf("verification passed, want the %s check to refuse it", tc.name)
			}
			if !strings.Contains(view.Verification.Error, tc.want) {
				t.Fatalf("verification error = %q, want it to mention %q", view.Verification.Error, tc.want)
			}
			if got := service.CountRegistered(t.Context()); got != 0 {
				t.Fatalf("CountRegistered = %d, want 0", got)
			}
		})
	}

	t.Run("verified target enters the pool", func(t *testing.T) {
		probe := environmenttest.NewProbe()
		probe.Result = probeFacts("tree-1")
		service := newService(t, probe)
		view, err := service.Register(t.Context(), registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 4), "ops")
		if err != nil {
			t.Fatalf("Register: %v", err)
		}
		if !view.Verified || !view.Schedulable || view.Phase != string(environment.PhaseActive) {
			t.Fatalf("view = %+v, want an active, verified, schedulable environment", view)
		}
		if view.Verification.TreeID != "tree-1" {
			t.Fatalf("tree id = %q, want tree-1", view.Verification.TreeID)
		}
		if service.CountRegistered(t.Context()) != 1 {
			t.Fatalf("CountRegistered = %d, want 1", service.CountRegistered(t.Context()))
		}
		candidates := service.Candidates(t.Context(), environment.CandidateFilter{TrustDomain: "build", Source: "atlas", Capabilities: []string{"nix"}})
		if len(candidates) != 1 || candidates[0].ID != "build-a" || candidates[0].FreeSlots != 4 {
			t.Fatalf("Candidates = %+v, want build-a with 4 free slots", candidates)
		}
	})
}

// 04-architecture §3.3: replacing the physical tree behind the same address
// requires a new environment ID; the old registration stops matching instead
// of adopting the new tree.
func TestSameEndpointWithANewTreeGetsANewIDAndSupersedesTheOld(t *testing.T) {
	tree := "tree-1"
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		return probeFacts(tree), nil
	}
	service := newService(t, probe)
	ctx := t.Context()

	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-a: %v", err)
	}

	tree = "tree-2"
	viewB, err := service.Register(ctx, registration("build-b", "build-a.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2), "ops")
	if err != nil {
		t.Fatalf("Register build-b: %v", err)
	}
	if viewB.Verification.TreeID != "tree-2" || !viewB.Schedulable {
		t.Fatalf("build-b = %+v, want the new tree verified under a new id", viewB)
	}
	viewA, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-a: %v", err)
	}
	if viewA.Schedulable {
		t.Fatalf("build-a is still schedulable after the tree changed: %+v", viewA)
	}
	if viewA.Verification.State != string(environment.VerificationFailed) {
		t.Fatalf("build-a verification state = %q, want failed", viewA.Verification.State)
	}
	if !strings.Contains(viewA.Verification.Error, "different persistent tree") &&
		!strings.Contains(viewA.Verification.Error, "no longer matches") {
		t.Fatalf("build-a error = %q, want the tree change named", viewA.Verification.Error)
	}

	verified, err := service.Verify(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Verify build-a: %v", err)
	}
	if verified.Verified {
		t.Fatalf("build-a re-verified against tree-2 under the old id: %+v", verified)
	}
	if !strings.Contains(verified.Verification.Error, "different persistent tree") {
		t.Fatalf("Verify error = %q, want the tree change", verified.Verification.Error)
	}
}

// V2-C07: the persistent tree is the ownership fact. A second ID for the same
// tree is a duplicate target, not a second environment.
func TestDuplicateTreeIsRefused(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newService(t, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-a: %v", err)
	}
	_, err := service.Register(ctx, registration("build-b", "build-b.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2), "ops")
	if !errors.Is(err, environment.ErrTreeExists) {
		t.Fatalf("duplicate tree registration = %v, want ErrTreeExists", err)
	}
	if _, err := service.Get(ctx, "build-b", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("a refused registration left build-b behind: %v", err)
	}
}

// V2-C07: drain refuses new slots atomically and leaves existing sessions
// untouched. Resume puts the verified environment back in the pool.
func TestDrainRefusesNewSlotsAndKeepsExistingSessions(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newService(t, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := service.AcquireSlot(ctx, "build-a", "slot-1", "session-1"); err != nil {
		t.Fatalf("AcquireSlot slot-1: %v", err)
	}
	drained, err := service.Drain(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if drained.Phase != string(environment.PhaseDraining) || drained.ActiveSlots != 1 {
		t.Fatalf("drained view = %+v, want draining with the existing slot", drained)
	}
	if _, err := service.AcquireSlot(ctx, "build-a", "slot-2", "session-2"); !errors.Is(err, environment.ErrDraining) {
		t.Fatalf("AcquireSlot while draining = %v, want ErrDraining", err)
	}
	slots, err := service.Slots(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Slots: %v", err)
	}
	if len(slots) != 1 || slots[0].ID != "slot-1" || slots[0].State != string(environment.SlotActive) {
		t.Fatalf("slots = %+v, want the existing active slot", slots)
	}
	if candidates := service.Candidates(ctx, environment.CandidateFilter{}); len(candidates) != 0 {
		t.Fatalf("Candidates while draining = %+v, want none", candidates)
	}
	if _, err := service.ReleaseSlot(ctx, "build-a", "slot-1", true); err != nil {
		t.Fatalf("ReleaseSlot: %v", err)
	}
	resumed, err := service.Resume(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.Phase != string(environment.PhaseActive) || !resumed.Schedulable {
		t.Fatalf("resumed view = %+v, want active and schedulable", resumed)
	}
	if candidates := service.Candidates(ctx, environment.CandidateFilter{}); len(candidates) != 1 {
		t.Fatalf("Candidates after resume = %+v, want build-a", candidates)
	}
}

// 04-architecture §3.3: lowering the limit cannot force an existing session to
// rerun or free its slot.
func TestLoweringTheLimitDoesNotEvictExistingSessions(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newService(t, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for _, slot := range []string{"slot-1", "slot-2"} {
		if _, err := service.AcquireSlot(ctx, "build-a", slot, "session-"+slot); err != nil {
			t.Fatalf("AcquireSlot %s: %v", slot, err)
		}
	}
	limit := 1
	updated, err := service.Update(ctx, "build-a", environment.Patch{Concurrency: &limit}, "ops")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Concurrency != 1 || updated.ActiveSlots != 2 {
		t.Fatalf("updated view = %+v, want limit 1 with two live slots", updated)
	}
	if _, err := service.AcquireSlot(ctx, "build-a", "slot-3", "session-3"); !errors.Is(err, environment.ErrFull) {
		t.Fatalf("AcquireSlot over the lowered limit = %v, want ErrFull", err)
	}
	for _, slot := range []string{"slot-1", "slot-2"} {
		if _, err := service.ReleaseSlot(ctx, "build-a", slot, true); err != nil {
			t.Fatalf("ReleaseSlot %s: %v", slot, err)
		}
	}
	if _, err := service.AcquireSlot(ctx, "build-a", "slot-3", "session-3"); err != nil {
		t.Fatalf("AcquireSlot after both releases: %v", err)
	}
}

// V2-C07 / ENV-04: active slots, isolated slots, unconfirmed cleanups and rule
// references each block retirement; the environment remains registered.
func TestRetirementBlockers(t *testing.T) {
	t.Run("active slot", func(t *testing.T) {
		probe := environmenttest.NewProbe()
		probe.Result = probeFacts("tree-1")
		service := newService(t, probe)
		ctx := t.Context()
		if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
			t.Fatalf("Register: %v", err)
		}
		if _, err := service.AcquireSlot(ctx, "build-a", "slot-1", "session-1"); err != nil {
			t.Fatalf("AcquireSlot: %v", err)
		}
		_, err := service.Retire(ctx, "build-a", "ops")
		var blocked *environment.BlockedError
		if !errors.As(err, &blocked) {
			t.Fatalf("Retire with an active slot = %v, want BlockedError", err)
		}
		if !strings.Contains(err.Error(), "active slot") {
			t.Fatalf("Retire error = %q, want an active-slot blocker", err)
		}
		if _, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); err != nil {
			t.Fatalf("the blocked environment disappeared: %v", err)
		}
	})

	t.Run("isolated slot", func(t *testing.T) {
		probe := environmenttest.NewProbe()
		probe.Result = probeFacts("tree-1")
		service := newService(t, probe)
		ctx := t.Context()
		if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
			t.Fatalf("Register: %v", err)
		}
		if _, err := service.AcquireSlot(ctx, "build-a", "slot-1", "session-1"); err != nil {
			t.Fatalf("AcquireSlot: %v", err)
		}
		if _, err := service.ReleaseSlot(ctx, "build-a", "slot-1", false); err != nil {
			t.Fatalf("ReleaseSlot unconfirmed: %v", err)
		}
		_, err := service.Retire(ctx, "build-a", "ops")
		if !strings.Contains(err.Error(), "isolated slot") {
			t.Fatalf("Retire with an isolated slot = %v, want an isolated-slot blocker", err)
		}
		if _, err := service.ConfirmCleanup(ctx, "build-a", "slot-1", "ops"); err != nil {
			t.Fatalf("ConfirmCleanup: %v", err)
		}
		if _, err := service.Retire(ctx, "build-a", "ops"); err != nil {
			t.Fatalf("Retire after cleanup confirmation: %v", err)
		}
	})

	t.Run("unconfirmed non-slot cleanup", func(t *testing.T) {
		probe := environmenttest.NewProbe()
		probe.Result = probeFacts("tree-1")
		service := newService(t, probe)
		ctx := t.Context()
		if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
			t.Fatalf("Register: %v", err)
		}
		if err := service.AddPendingCleanup(ctx, "build-a", "cleanup-1", "worktree:attempt-1"); err != nil {
			t.Fatalf("AddPendingCleanup: %v", err)
		}
		_, err := service.Retire(ctx, "build-a", "ops")
		if !strings.Contains(err.Error(), "unconfirmed cleanup") {
			t.Fatalf("Retire with an unconfirmed cleanup = %v, want an unconfirmed-cleanup blocker", err)
		}
		if err := service.ConfirmPendingCleanup(ctx, "build-a", "cleanup-1", "ops"); err != nil {
			t.Fatalf("ConfirmPendingCleanup: %v", err)
		}
		if _, err := service.Retire(ctx, "build-a", "ops"); err != nil {
			t.Fatalf("Retire after cleanup confirmation: %v", err)
		}
	})

	t.Run("rule reference", func(t *testing.T) {
		probe := environmenttest.NewProbe()
		probe.Result = probeFacts("tree-1")
		rules := environment.NewMemoryRuleReferences()
		rules.Add("build-a", "rule-core-dev")
		service, err := environment.New(environment.Config{
			Probe:               probe,
			AllowedTrustDomains: []string{"build"},
			RuleReferences:      rules,
			Clock:               testClock,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		ctx := t.Context()
		if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
			t.Fatalf("Register: %v", err)
		}
		_, err = service.Retire(ctx, "build-a", "ops")
		if !strings.Contains(err.Error(), "rule") {
			t.Fatalf("Retire with a rule reference = %v, want a rule blocker", err)
		}
		rules.Remove("build-a", "rule-core-dev")
		if _, err := service.Retire(ctx, "build-a", "ops"); err != nil {
			t.Fatalf("Retire after dropping the rule reference: %v", err)
		}
	})
}

// ENV-04: retirement removes the registration and leaves a tombstone; it does
// not call the target or touch its files.
func TestRetireRemovesOnlyTheRegistration(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	probe.Remote["/srv/rsbx/build-a/tree.marker"] = "keep-me"
	service := newService(t, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	callsBeforeRetire := probe.CallCount()
	retirement, err := service.Retire(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Retire: %v", err)
	}
	if retirement.Tombstone.ID != "build-a" || retirement.Tombstone.TreeID != "tree-1" {
		t.Fatalf("tombstone = %+v, want the stable id and tree", retirement.Tombstone)
	}
	if got := probe.CallCount(); got != callsBeforeRetire {
		t.Fatalf("retirement made %d probe call(s), want none", got-callsBeforeRetire)
	}
	if value, ok := probe.RemoteFile("/srv/rsbx/build-a/tree.marker"); !ok || value != "keep-me" {
		t.Fatalf("remote file = %q, %v, want it untouched", value, ok)
	}
	if _, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("Get after retirement = %v, want ErrNotFound", err)
	}
	retired, err := service.Retired(ctx, environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Retired: %v", err)
	}
	if len(retired) != 1 || retired[0].ID != "build-a" {
		t.Fatalf("Retired = %+v, want build-a's tombstone", retired)
	}
}

// 04-architecture §3.3: a retired stable id stays retired; a replacement tree
// at the same address registers under a new id.
func TestRetiredIDCannotBeReusedButANewIDCanUseTheSameAddress(t *testing.T) {
	tree := "tree-1"
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		return probeFacts(tree), nil
	}
	service := newService(t, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-a: %v", err)
	}
	if _, err := service.Retire(ctx, "build-a", "ops"); err != nil {
		t.Fatalf("Retire build-a: %v", err)
	}
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); !errors.Is(err, environment.ErrRetired) {
		t.Fatalf("re-registering a retired id = %v, want ErrRetired", err)
	}
	tree = "tree-2"
	if _, err := service.Register(ctx, registration("build-c", "build-a.example.test:7443", "/srv/rsbx/build-c", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register the replacement tree under a new id: %v", err)
	}
}

// 04-architecture §8: isolated slots and tombstones survive a restart.
func TestFileStoreKeepsIsolatedSlotsAndTombstonesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "environments.json")
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-" + target.StateRoot), nil
	}

	store, err := environment.NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	service, err := environment.New(environment.Config{
		Store:               store,
		Probe:               probe,
		AllowedTrustDomains: []string{"build"},
		Clock:               testClock,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-a: %v", err)
	}
	if _, err := service.AcquireSlot(ctx, "build-a", "slot-1", "session-1"); err != nil {
		t.Fatalf("AcquireSlot: %v", err)
	}
	if _, err := service.ReleaseSlot(ctx, "build-a", "slot-1", false); err != nil {
		t.Fatalf("ReleaseSlot unconfirmed: %v", err)
	}
	if _, err := service.Register(ctx, registration("build-b", "build-b.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-b: %v", err)
	}
	if _, err := service.Retire(ctx, "build-b", "ops"); err != nil {
		t.Fatalf("Retire build-b: %v", err)
	}

	reopened, err := environment.NewFileStore(path)
	if err != nil {
		t.Fatalf("reopen NewFileStore: %v", err)
	}
	restarted, err := environment.New(environment.Config{
		Store:               reopened,
		Probe:               probe,
		AllowedTrustDomains: []string{"build"},
		Clock:               testClock,
	})
	if err != nil {
		t.Fatalf("New after restart: %v", err)
	}
	view, err := restarted.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-a after restart: %v", err)
	}
	if view.IsolatedSlots != 1 || view.ActiveSlots != 0 {
		t.Fatalf("build-a after restart = %+v, want one isolated slot", view)
	}
	if _, err := restarted.Retire(ctx, "build-a", "ops"); err == nil || !strings.Contains(err.Error(), "isolated slot") {
		t.Fatalf("Retire after restart = %v, want the isolated slot to block", err)
	}
	if _, err := restarted.Get(ctx, "build-b", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("build-b after restart = %v, want its retirement to persist", err)
	}
	if _, err := restarted.Register(ctx, registration("build-b", "build-b.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); !errors.Is(err, environment.ErrRetired) {
		t.Fatalf("re-registering build-b after restart = %v, want ErrRetired", err)
	}
}

// V2-G3 / 04-architecture §3.3: selection uses the registered environment
// facts only. No node label or Pod affinity takes part.
func TestCandidatesUseOnlyEnvironmentFacts(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return environment.ProbeResult{
			TreeID:       "tree-" + target.Endpoint,
			Identity:     "sha256:" + target.Endpoint,
			Capabilities: []string{"nix", "docker"},
			Capacity:     8,
		}, nil
	}
	service := newService(t, probe)
	ctx := t.Context()
	regA := registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 1)
	regB := registration("build-b", "build-b.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 4)
	regC := registration("lab-gpu", "lab-gpu.example.test:7443", "/srv/rsbx/lab-gpu", []string{"nix", "gpu"}, 4)
	regC.TrustDomain = "lab"
	for _, reg := range []environment.Registration{regA, regB, regC} {
		if _, err := service.Register(ctx, reg, "ops"); err != nil {
			t.Fatalf("Register %s: %v", reg.ID, err)
		}
	}
	candidates := service.Candidates(ctx, environment.CandidateFilter{TrustDomain: "build", Source: "atlas", Capabilities: []string{"nix"}})
	if len(candidates) != 2 || candidates[0].ID != "build-b" || candidates[1].ID != "build-a" {
		t.Fatalf("Candidates = %+v, want build-b then build-a by free capacity", candidates)
	}
	if got := service.Candidates(ctx, environment.CandidateFilter{TrustDomain: "build", Capabilities: []string{"gpu"}}); len(got) != 0 {
		t.Fatalf("Candidates with gpu = %+v, want none in the build domain", got)
	}
	if got := service.Candidates(ctx, environment.CandidateFilter{TrustDomain: "build", Source: "harbor"}); len(got) != 0 {
		t.Fatalf("Candidates for a source outside the allowed set = %+v, want none", got)
	}

	// The selection result carries no Kubernetes placement field. The scheduler
	// cannot choose a node label or a Pod affinity because this package never
	// exposes one.
	typ := reflect.TypeOf(environment.Candidate{})
	for _, field := range []string{"NodeSelector", "Affinity", "NodeName", "Labels", "Taints"} {
		if _, ok := typ.FieldByName(field); ok {
			t.Errorf("Candidate exposes the Kubernetes placement field %q", field)
		}
	}
}

// V2-C07: an environment registered without an allowed-source restriction is
// open to every source, and a source-scoped grant can read it.
func TestAnEnvironmentWithoutAllowedSourcesIsOpenToEverySource(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newService(t, probe)
	ctx := t.Context()
	reg := registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2)
	reg.AllowedSources = nil
	if _, err := service.Register(ctx, reg, "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	grant := environment.Grant{Sources: []string{"atlas"}}
	if _, err := service.Get(ctx, "build-a", grant); err != nil {
		t.Fatalf("Get with a source grant = %v, want the unrestricted environment", err)
	}
	if _, err := service.Get(ctx, "build-a", environment.Grant{Sources: []string{}}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("Get with a grant that resolved to no source = %v, want ErrNotFound", err)
	}
	candidates := service.Candidates(ctx, environment.CandidateFilter{Source: "harbor"})
	if len(candidates) != 1 || candidates[0].ID != "build-a" {
		t.Fatalf("Candidates = %+v, want build-a for an unrestricted source", candidates)
	}
}

// V2-C07: a configuration change invalidates the verification; a live slot
// refuses relocation, while a policy-only edit does not require a re-verify.
func TestUpdateMarksTheTargetStaleAndRefusesRelocationWithSlots(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newService(t, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := service.AcquireSlot(ctx, "build-a", "slot-1", "session-1"); err != nil {
		t.Fatalf("AcquireSlot: %v", err)
	}
	endpoint := "build-b.example.test:7443"
	if _, err := service.Update(ctx, "build-a", environment.Patch{Endpoint: &endpoint}, "ops"); !errors.Is(err, environment.ErrSlotsPresent) {
		t.Fatalf("Update endpoint with a live slot = %v, want ErrSlotsPresent", err)
	}
	if _, err := service.ReleaseSlot(ctx, "build-a", "slot-1", true); err != nil {
		t.Fatalf("ReleaseSlot: %v", err)
	}
	domain := "delivery"
	updated, err := service.Update(ctx, "build-a", environment.Patch{TrustDomain: &domain}, "ops")
	if err != nil {
		t.Fatalf("Update trust domain: %v", err)
	}
	if !updated.Verified || !updated.Schedulable {
		t.Fatalf("a policy-only edit invalidated the verification: %+v", updated)
	}
	capabilities := []string{"nix", "gpu"}
	updated, err = service.Update(ctx, "build-a", environment.Patch{Capabilities: &capabilities}, "ops")
	if err != nil {
		t.Fatalf("Update capabilities: %v", err)
	}
	if updated.Verified || updated.Schedulable {
		t.Fatalf("a capability edit left the environment schedulable: %+v", updated)
	}
	if updated.Verification.State != string(environment.VerificationStale) {
		t.Fatalf("verification state = %q, want stale", updated.Verification.State)
	}
	if candidates := service.Candidates(ctx, environment.CandidateFilter{}); len(candidates) != 0 {
		t.Fatalf("Candidates with a stale verification = %+v, want none", candidates)
	}
	probe.Result = environment.ProbeResult{
		TreeID:       "tree-1",
		Identity:     "sha256:tree-1",
		Capabilities: []string{"nix", "gpu"},
		Capacity:     8,
	}
	verified, err := service.Verify(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !verified.Verified || !verified.Schedulable {
		t.Fatalf("verification after the capability edit = %+v, want active", verified)
	}
}

// 04-architecture §3.3: the state root identifies the physical tree. Moving
// the registration to another root would reuse the stable id for a different
// tree, so the operator has to register a new environment id.
func TestUpdateRefusesAStateRootChange(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newService(t, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	root := "/srv/rsbx/build-b"
	if _, err := service.Update(ctx, "build-a", environment.Patch{StateRoot: &root}, "ops"); !errors.Is(err, environment.ErrTreeChanged) {
		t.Fatalf("Update state root = %v, want ErrTreeChanged", err)
	}
	view, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if view.StateRoot != "/srv/rsbx/build-a" || !view.Schedulable {
		t.Fatalf("the refused update changed the registration: %+v", view)
	}
}

// 04-architecture §3.3: a target whose declared data-residency domain is not
// on the deployment's allowlist never enters the pool.
func TestTrustDomainAllowlistKeepsUnauthorizedDomainsOutOfThePool(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service, err := environment.New(environment.Config{
		Probe:               probe,
		AllowedTrustDomains: []string{"build"},
		Clock:               testClock,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := t.Context()
	reg := registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2)
	reg.TrustDomain = "lab"
	if _, err := service.Register(ctx, reg, "ops"); err == nil {
		t.Fatal("a target outside the allowed trust domains was registered")
	}
	if service.CountRegistered(ctx) != 0 {
		t.Fatalf("CountRegistered = %d, want 0", service.CountRegistered(ctx))
	}
	reg.TrustDomain = "build"
	if _, err := service.Register(ctx, reg, "ops"); err != nil {
		t.Fatalf("Register an allowed domain: %v", err)
	}
	domain := "lab"
	if _, err := service.Update(ctx, "build-a", environment.Patch{TrustDomain: &domain}, "ops"); err == nil {
		t.Fatal("Update moved a registration to a domain outside the allowlist")
	}
}
