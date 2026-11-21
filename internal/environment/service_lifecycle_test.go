package environment_test

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/environment/environmenttest"
)

func newFileService(t *testing.T, path string, domains []string, probe environment.Probe) *environment.Service {
	t.Helper()
	store, err := environment.NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	service, err := environment.New(environment.Config{
		Store:               store,
		Probe:               probe,
		AllowedTrustDomains: domains,
		Clock:               testClock,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return service
}

// 04-architecture §3.3: a state root is a path inside one host, not a global
// ownership key. Two hosts commonly use the same path and must remain two
// selectable trees; a real replacement at one endpoint still supersedes only
// that endpoint's old registration.
func TestDifferentEndpointsWithTheSameStateRootKeepBothTrees(t *testing.T) {
	trees := map[string]string{
		"build-a.example.test:7443": "tree-a",
		"build-b.example.test:7443": "tree-b",
	}
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return probeFacts(trees[target.Endpoint]), nil
	}
	service := newService(t, probe)
	ctx := t.Context()

	viewA, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx", []string{"nix"}, 2), "ops")
	if err != nil {
		t.Fatalf("Register build-a: %v", err)
	}
	viewB, err := service.Register(ctx, registration("build-b", "build-b.example.test:7443", "/srv/rsbx", []string{"nix"}, 2), "ops")
	if err != nil {
		t.Fatalf("Register build-b: %v", err)
	}
	if !viewA.Schedulable || !viewB.Schedulable {
		t.Fatalf("same-path hosts are not both selectable: build-a=%+v build-b=%+v", viewA, viewB)
	}
	if candidates := service.Candidates(ctx, environment.CandidateFilter{TrustDomain: "build"}); len(candidates) != 2 {
		t.Fatalf("Candidates = %+v, want both hosts with the same state root", candidates)
	}

	trees["build-a.example.test:7443"] = "tree-c"
	if _, err := service.Register(ctx, registration("build-c", "build-a.example.test:7443", "/srv/rsbx/build-c", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register the replacement tree: %v", err)
	}
	oldA, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-a: %v", err)
	}
	if oldA.Schedulable {
		t.Fatalf("build-a stayed selectable after its endpoint changed tree: %+v", oldA)
	}
	stillB, err := service.Get(ctx, "build-b", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-b: %v", err)
	}
	if !stillB.Schedulable {
		t.Fatalf("the replacement at another endpoint superseded build-b: %+v", stillB)
	}
}

// The absence of a residency allowlist is not a deployment-wide approval.
func TestEmptyTrustDomainAllowlistRefusesEveryRegistration(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service, err := environment.New(environment.Config{Probe: probe, Clock: testClock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = service.Register(t.Context(), registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops")
	if err == nil || !strings.Contains(err.Error(), "not an allowed data-residency domain") {
		t.Fatalf("Register without an allowlist = %v, want a residency refusal", err)
	}
	if got := service.CountRegistered(t.Context()); got != 0 {
		t.Fatalf("CountRegistered = %d, want 0", got)
	}
	if candidates := service.Candidates(t.Context(), environment.CandidateFilter{}); len(candidates) != 0 {
		t.Fatalf("Candidates = %+v, want none", candidates)
	}
}

// Removing a domain from the allowlist takes effect on the restored record:
// the registration remains visible for repair or retirement, but it is not a
// candidate and cannot take a new slot.
func TestRemovingTheTrustDomainGrantAfterRestartDropsThePool(t *testing.T) {
	path := filepath.Join(t.TempDir(), "environments.json")
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")

	first := newFileService(t, path, []string{"build"}, probe)
	if _, err := first.Register(t.Context(), registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	revoked := newFileService(t, path, []string{"lab"}, probe)
	view, err := revoked.Get(t.Context(), "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	if view.PolicyAllowed || view.Schedulable {
		t.Fatalf("a revoked domain is still selectable: %+v", view)
	}
	if !view.Registered || !view.Verified {
		t.Fatalf("the restored record lost its registration facts: %+v", view)
	}
	if got := revoked.CountRegistered(t.Context()); got != 0 {
		t.Fatalf("CountRegistered = %d, want 0 after the grant was removed", got)
	}
	if candidates := revoked.Candidates(t.Context(), environment.CandidateFilter{}); len(candidates) != 0 {
		t.Fatalf("Candidates = %+v, want none after the grant was removed", candidates)
	}
	if _, err := revoked.AcquireSlot(t.Context(), "build-a", "slot-1", "session-1"); !errors.Is(err, environment.ErrTrustDomainDenied) {
		t.Fatalf("AcquireSlot = %v, want ErrTrustDomainDenied", err)
	}
	if _, err := revoked.Verify(t.Context(), "build-a", "ops"); !errors.Is(err, environment.ErrTrustDomainDenied) {
		t.Fatalf("Verify = %v, want ErrTrustDomainDenied", err)
	}

	restored := newFileService(t, path, []string{"build"}, probe)
	if candidates := restored.Candidates(t.Context(), environment.CandidateFilter{}); len(candidates) != 1 {
		t.Fatalf("Candidates after restoring the grant = %+v, want build-a", candidates)
	}
	if got := restored.CountRegistered(t.Context()); got != 1 {
		t.Fatalf("CountRegistered after restoring the grant = %d, want 1", got)
	}
}

// A target that never completed a trusted verification is not an environment,
// even if an operator drains it. A verified environment keeps counting while
// draining.
func TestDrainingNeverTurnsAnUnverifiedTargetIntoARegisteredEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "environments.json")
	probe := environmenttest.NewProbe()
	probe.Err = errors.New("connection refused")
	service := newFileService(t, path, []string{"build"}, probe)
	ctx := t.Context()

	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := service.Drain(ctx, "build-a", "ops"); !errors.Is(err, environment.ErrNotVerified) {
		t.Fatalf("Drain on a never-verified target = %v, want ErrNotVerified", err)
	}
	if got := service.CountRegistered(ctx); got != 0 {
		t.Fatalf("CountRegistered = %d, want 0", got)
	}
	view, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if view.Phase != string(environment.PhasePending) || view.Registered {
		t.Fatalf("the refused drain changed the record: %+v", view)
	}

	reopened := newFileService(t, path, []string{"build"}, probe)
	if got := reopened.CountRegistered(ctx); got != 0 {
		t.Fatalf("CountRegistered after restart = %d, want 0", got)
	}

	probe.Err = nil
	probe.Result = probeFacts("tree-1")
	verified, err := reopened.Verify(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !verified.Registered || !verified.Schedulable {
		t.Fatalf("verified view = %+v, want a registered environment", verified)
	}
	drained, err := reopened.Drain(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Drain after verification: %v", err)
	}
	if drained.Phase != string(environment.PhaseDraining) || !drained.Registered {
		t.Fatalf("drained view = %+v, want draining and still registered", drained)
	}
	if got := reopened.CountRegistered(ctx); got != 1 {
		t.Fatalf("CountRegistered while draining = %d, want 1", got)
	}

	afterRestart := newFileService(t, path, []string{"build"}, probe)
	if got := afterRestart.CountRegistered(ctx); got != 1 {
		t.Fatalf("CountRegistered after restart while draining = %d, want 1", got)
	}
}

// A tree id seen by a failed first verification is an observation, not
// ownership. Fixing the identity and verifying again must work under the same
// environment id; the observation also must not reserve the tree.
func TestFirstIdentityFailureDoesNotFreezeTheObservedTree(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.Result = environment.ProbeResult{
		TreeID:       "tree-a",
		Identity:     "sha256:wrong",
		Capabilities: []string{"nix"},
		Capacity:     8,
	}
	service := newService(t, probe)
	ctx := t.Context()
	reg := registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2)
	reg.ExpectedIdentity = "sha256:right"

	failed, err := service.Register(ctx, reg, "ops")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if failed.Verification.TreeID != "" || failed.Verification.ObservedTreeID != "tree-a" {
		t.Fatalf("failed first verification = %+v, want an observation without ownership", failed.Verification)
	}

	probe.Result = environment.ProbeResult{
		TreeID:       "tree-a",
		Identity:     "sha256:right",
		Capabilities: []string{"nix"},
		Capacity:     8,
	}
	if _, err := service.Register(ctx, registration("build-b", "build-b.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("the failed observation reserved tree-a: %v", err)
	}

	probe.Result = environment.ProbeResult{
		TreeID:       "tree-b",
		Identity:     "sha256:right",
		Capabilities: []string{"nix"},
		Capacity:     8,
	}
	verified, err := service.Verify(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Verify after fixing the target: %v", err)
	}
	if !verified.Verified || !verified.Schedulable || verified.Verification.TreeID != "tree-b" {
		t.Fatalf("verified view = %+v, want tree-b under the original id", verified)
	}
}

// A failed registration owns no tree, so it must not supersede the verified
// environment that still serves the endpoint.
func TestFailedFirstVerificationDoesNotSupersedeTheEndpoint(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.Result = environment.ProbeResult{
		TreeID:       "tree-a",
		Identity:     "sha256:one",
		Capabilities: []string{"nix"},
		Capacity:     8,
	}
	service := newService(t, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-a: %v", err)
	}

	probe.Result = environment.ProbeResult{
		TreeID:       "tree-b",
		Identity:     "sha256:wrong",
		Capabilities: []string{"nix"},
		Capacity:     8,
	}
	regB := registration("build-b", "build-a.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2)
	regB.ExpectedIdentity = "sha256:right"
	failed, err := service.Register(ctx, regB, "ops")
	if err != nil {
		t.Fatalf("Register build-b: %v", err)
	}
	if failed.Verified || failed.Schedulable {
		t.Fatalf("the failed registration entered the pool: %+v", failed)
	}
	existing, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-a: %v", err)
	}
	if !existing.Schedulable {
		t.Fatalf("a failed registration superseded the verified endpoint: %+v", existing)
	}
}

// A rejected re-verification of an environment that another tree already
// replaced must not revoke the replacement. Only this attempt's failure is
// recorded; the valid tree at the same endpoint stays selectable.
func TestFailedReVerificationDoesNotSupersedeTheReplacementTree(t *testing.T) {
	tree := "tree-a"
	capabilities := []string{"nix"}
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		return environment.ProbeResult{
			TreeID:       tree,
			Identity:     "sha256:" + tree,
			Capabilities: capabilities,
			Capacity:     8,
			Version:      "rsbxd-test",
		}, nil
	}
	service := newService(t, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-a: %v", err)
	}
	tree = "tree-b"
	if _, err := service.Register(ctx, registration("build-b", "build-a.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-b: %v", err)
	}

	// build-a still reports its old tree, but the target no longer provides a
	// capability the registration requires.
	tree = "tree-a"
	capabilities = []string{"docker"}
	failed, err := service.Verify(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Verify build-a: %v", err)
	}
	if failed.Verified || failed.Schedulable {
		t.Fatalf("the rejected re-verification entered the pool: %+v", failed)
	}
	replacement, err := service.Get(ctx, "build-b", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-b: %v", err)
	}
	if !replacement.Schedulable || replacement.Verification.State != string(environment.VerificationOK) {
		t.Fatalf("a rejected re-verification revoked the valid replacement: %+v", replacement)
	}
	candidates := service.Candidates(ctx, environment.CandidateFilter{TrustDomain: "build"})
	if len(candidates) != 1 || candidates[0].ID != "build-b" {
		t.Fatalf("Candidates = %+v, want only build-b", candidates)
	}
}

// The overview count follows the caller's source grant and counts stable ids,
// not rows produced by overlapping scopes.
func TestCountRegisteredForGrantUsesStableIDsAndTheGrant(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-" + target.Endpoint), nil
	}
	service := newService(t, probe)
	ctx := t.Context()
	regA := registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2)
	regA.AllowedSources = []string{"atlas"}
	regB := registration("build-b", "build-b.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2)
	regB.AllowedSources = []string{"harbor"}
	regC := registration("build-c", "build-c.example.test:7443", "/srv/rsbx/build-c", []string{"nix"}, 2)
	regC.AllowedSources = nil
	for _, reg := range []environment.Registration{regA, regB, regC} {
		if _, err := service.Register(ctx, reg, "ops"); err != nil {
			t.Fatalf("Register %s: %v", reg.ID, err)
		}
	}
	cases := []struct {
		name  string
		grant environment.Grant
		want  int
	}{
		{name: "deployment wide", grant: environment.Grant{DeploymentWide: true}, want: 3},
		{name: "atlas", grant: environment.Grant{Sources: []string{"atlas"}}, want: 2},
		{name: "harbor", grant: environment.Grant{Sources: []string{"harbor"}}, want: 2},
		{name: "no resolved source", grant: environment.Grant{Sources: []string{}}, want: 0},
		{name: "duplicate source", grant: environment.Grant{Sources: []string{"atlas", "atlas"}}, want: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := service.CountRegisteredForGrant(ctx, tc.grant); got != tc.want {
				t.Fatalf("CountRegisteredForGrant = %d, want %d", got, tc.want)
			}
		})
	}
}
