package environment_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/environment/environmenttest"
)

const testRegistrySecret = "foreman-environment-registry"

func registrySecretClient() *fake.Clientset {
	return fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testRegistrySecret,
			Namespace:       "foreman",
			ResourceVersion: "1",
			UID:             types.UID("carrier-1"),
		},
		Type: corev1.SecretTypeOpaque,
	})
}

func newSecretService(t *testing.T, client *fake.Clientset, domains []string, probe environment.Probe) *environment.Service {
	t.Helper()
	store, err := environment.NewSecretStore(t.Context(), client.CoreV1().Secrets("foreman"), testRegistrySecret)
	if err != nil {
		t.Fatalf("NewSecretStore: %v", err)
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

// The Secret is the cross-Pod carrier: a fresh process must still see the
// isolated slot, the tombstone and the retired id.
func TestSecretStoreKeepsSlotsAndTombstonesAcrossPodRebuild(t *testing.T) {
	client := registrySecretClient()
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-" + target.StateRoot), nil
	}
	first := newSecretService(t, client, []string{"build"}, probe)
	ctx := t.Context()
	if _, err := first.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-a: %v", err)
	}
	if _, err := first.AcquireSlot(ctx, "build-a", "slot-1"); err != nil {
		t.Fatalf("AcquireSlot: %v", err)
	}
	if _, err := first.ReleaseSlot(ctx, "build-a", "slot-1", false); err != nil {
		t.Fatalf("ReleaseSlot unconfirmed: %v", err)
	}
	if _, err := first.Register(ctx, registration("build-b", "build-b.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-b: %v", err)
	}
	if _, err := first.Retire(ctx, "build-b", "ops"); err != nil {
		t.Fatalf("Retire build-b: %v", err)
	}

	secret, err := client.CoreV1().Secrets("foreman").Get(ctx, testRegistrySecret, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the carrier: %v", err)
	}
	rebuilt := fake.NewSimpleClientset(secret.DeepCopy())
	second := newSecretService(t, rebuilt, []string{"build"}, probe)

	view, err := second.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-a after the rebuild: %v", err)
	}
	if view.IsolatedSlots != 1 || view.ActiveSlots != 0 {
		t.Fatalf("build-a after the rebuild = %+v, want one isolated slot", view)
	}
	if _, err := second.Retire(ctx, "build-a", "ops"); err == nil || !strings.Contains(err.Error(), "isolated slot") {
		t.Fatalf("Retire after the rebuild = %v, want the isolated slot to block", err)
	}
	if _, err := second.Get(ctx, "build-b", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("build-b after the rebuild = %v, want its retirement to persist", err)
	}
	if _, err := second.Register(ctx, registration("build-b", "build-b.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); !errors.Is(err, environment.ErrRetired) {
		t.Fatalf("re-registering build-b after the rebuild = %v, want ErrRetired", err)
	}
}

func TestSecretStoreUpdateFailureLeavesNoRegistration(t *testing.T) {
	client := registrySecretClient()
	client.PrependReactor("update", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("write denied")
	})
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newSecretService(t, client, []string{"build"}, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err == nil {
		t.Fatal("Register succeeded although the Secret update failed")
	}
	if _, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("Get after the failed update = %v, want ErrNotFound", err)
	}
	if got := service.CountRegistered(ctx); got != 0 {
		t.Fatalf("CountRegistered = %d, want 0", got)
	}
}

func TestSecretStoreConflictLeavesNoRegistration(t *testing.T) {
	client := registrySecretClient()
	client.PrependReactor("update", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, testRegistrySecret, errors.New("conflict"))
	})
	probe := environmenttest.NewProbe()
	probe.Result = probeFacts("tree-1")
	service := newSecretService(t, client, []string{"build"}, probe)
	ctx := t.Context()
	_, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops")
	if !errors.Is(err, environment.ErrConflict) {
		t.Fatalf("Register on a Secret conflict = %v, want ErrConflict", err)
	}
	if _, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("Get after the conflict = %v, want ErrNotFound", err)
	}
}

// casRegistry models the API server's resourceVersion contract: an update
// carrying an older version is refused, and an accepted update advances it.
type casRegistry struct {
	mu           sync.Mutex
	version      int
	secret       *corev1.Secret
	beforeUpdate func(*casRegistry)
}

func newCASRegistry(t *testing.T) (*fake.Clientset, *casRegistry) {
	t.Helper()
	return newCASRegistryWithSecret(t, nil)
}

func newCASRegistryWithSecret(t *testing.T, initial *corev1.Secret) (*fake.Clientset, *casRegistry) {
	t.Helper()
	carrier := &casRegistry{
		version: 1,
		secret: &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:            testRegistrySecret,
				Namespace:       "foreman",
				ResourceVersion: "1",
				UID:             types.UID("carrier-1"),
			},
			Type: corev1.SecretTypeOpaque,
		},
	}
	if initial != nil {
		carrier.secret = initial.DeepCopy()
		if version, err := strconv.Atoi(initial.ResourceVersion); err == nil && version > 0 {
			carrier.version = version
		}
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		carrier.mu.Lock()
		defer carrier.mu.Unlock()
		return true, carrier.secret.DeepCopy(), nil
	})
	client.PrependReactor("update", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		update := action.(k8stesting.UpdateAction)
		incoming := update.GetObject().(*corev1.Secret)
		carrier.mu.Lock()
		defer carrier.mu.Unlock()
		if carrier.beforeUpdate != nil {
			hook := carrier.beforeUpdate
			carrier.beforeUpdate = nil
			hook(carrier)
		}
		if incoming.ResourceVersion != carrier.secret.ResourceVersion {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Resource: "secrets"},
				testRegistrySecret,
				errors.New("resource version mismatch"),
			)
		}
		next := carrier.secret.DeepCopy()
		next.Data = incoming.Data
		next.Labels = incoming.Labels
		carrier.version++
		next.ResourceVersion = strconv.Itoa(carrier.version)
		carrier.secret = next
		return true, next.DeepCopy(), nil
	})
	return client, carrier
}

// document copies the current carrier document.
func (c *casRegistry) document() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.secret.Data["environments.json"]...)
}

// setDocument replaces the carrier document and advances its version.
func (c *casRegistry) setDocument(raw []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	c.secret.Data = map[string][]byte{"environments.json": append([]byte(nil), raw...)}
	c.secret.ResourceVersion = strconv.Itoa(c.version)
}

// injectNext makes the next update land after a competing commit that replaces
// the document, so the incoming update carries an older version.
func (c *casRegistry) injectNext(raw []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.beforeUpdate = func(carrier *casRegistry) {
		carrier.version++
		carrier.secret.Data = map[string][]byte{"environments.json": append([]byte(nil), raw...)}
		carrier.secret.ResourceVersion = strconv.Itoa(carrier.version)
	}
}

// recreate replaces the carrier with a fresh empty Secret of the same name, as
// deleting and reapplying the manifest would. The object identity changes.
func (c *casRegistry) recreate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	c.secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testRegistrySecret,
			Namespace:       "foreman",
			ResourceVersion: strconv.Itoa(c.version),
			UID:             types.UID("carrier-" + strconv.Itoa(c.version)),
		},
		Type: corev1.SecretTypeOpaque,
	}
}

// restore replaces the carrier with a new object of the same name (new UID)
// whose document is the given bytes, as recreating the Secret from a backup
// would.
func (c *casRegistry) restore(raw []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	c.secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testRegistrySecret,
			Namespace:       "foreman",
			ResourceVersion: strconv.Itoa(c.version),
			UID:             types.UID("carrier-" + strconv.Itoa(c.version)),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"environments.json": append([]byte(nil), raw...)},
	}
}

// clear empties the document of the current carrier object without changing
// its UID, as wiping the data key by hand would.
func (c *casRegistry) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	c.secret.Data = nil
	c.secret.ResourceVersion = strconv.Itoa(c.version)
}

// Two Pods overlap during a rolling update: both load the same snapshot, then
// write. The loser must reload the committed ledger and re-run the whole
// operation against it, so it sees the winner's registration, slot and
// tombstone instead of retrying a stale document forever.
func TestSecretStoreConflictingWriterReloadsTheCommittedLedger(t *testing.T) {
	client, carrier := newCASRegistry(t)
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-" + target.StateRoot), nil
	}
	first := newSecretService(t, client, []string{"build"}, probe)
	second := newSecretService(t, client, []string{"build"}, probe)
	ctx := t.Context()

	if _, err := first.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register on the first writer: %v", err)
	}
	if _, err := second.Register(ctx, registration("build-b", "build-b.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register on the second writer after the conflict: %v", err)
	}
	if _, err := second.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); err != nil {
		t.Fatalf("the second writer cannot see the first registration: %v", err)
	}
	if _, err := second.AcquireSlot(ctx, "build-b", "slot-1"); err != nil {
		t.Fatalf("AcquireSlot on the second writer: %v", err)
	}

	// The first writer still holds the pre-registration revision. Its
	// verification conflicts, reloads and runs again against the committed
	// record rather than resending the old document; the reload also shows the
	// slot the second writer committed.
	verified, err := first.Verify(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Verify on the first writer after the conflict: %v", err)
	}
	if !verified.Verified || verified.Verification.TreeID != "tree-/srv/rsbx/build-a" {
		t.Fatalf("verified view = %+v, want build-a's tree", verified)
	}
	observed, err := first.Get(ctx, "build-b", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("the first writer cannot see the second registration: %v", err)
	}
	if observed.ActiveSlots != 1 {
		t.Fatalf("the first writer cannot see the committed slot: %+v", observed)
	}

	if _, err := second.Retire(ctx, "build-a", "ops"); err != nil {
		t.Fatalf("Retire on the second writer: %v", err)
	}
	if _, err := first.Register(ctx, registration("build-c", "build-c.example.test:7443", "/srv/rsbx/build-c", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register on the first writer after the retirement: %v", err)
	}
	retired, err := first.Retired(ctx, environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Retired on the first writer: %v", err)
	}
	if len(retired) != 1 || retired[0].ID != "build-a" {
		t.Fatalf("Retired = %+v, want build-a's tombstone", retired)
	}
	if _, err := first.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); !errors.Is(err, environment.ErrRetired) {
		t.Fatalf("re-registering the retired id = %v, want ErrRetired", err)
	}
	if carrier.version < 7 {
		t.Fatalf("resourceVersion advanced to %d, want every retry to commit", carrier.version)
	}
}

func readRegistrySecret(t *testing.T, client *fake.Clientset) *corev1.Secret {
	t.Helper()
	secret, err := client.CoreV1().Secrets("foreman").Get(t.Context(), testRegistrySecret, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the carrier: %v", err)
	}
	return secret
}

func countAudit(t *testing.T, service *environment.Service, action string) int {
	t.Helper()
	entries, err := service.Audit(t.Context())
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	count := 0
	for _, entry := range entries {
		if entry.Action == action {
			count++
		}
	}
	return count
}

// A competing commit that lands between the decision's read and its write must
// make the whole decision run again on the committed snapshot. The other Pod's
// drain and this writer's policy edit both survive; the first attempt's audit
// row is discarded with its batch.
func TestOverlappingWritersKeepBothCommits(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-" + target.StateRoot), nil
	}
	ctx := t.Context()

	seedClient := registrySecretClient()
	seed := newSecretService(t, seedClient, []string{"build"}, probe)
	if _, err := seed.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register on the seed writer: %v", err)
	}
	before := readRegistrySecret(t, seedClient)
	if _, err := seed.Drain(ctx, "build-a", "ops"); err != nil {
		t.Fatalf("Drain on the competing writer: %v", err)
	}
	after := readRegistrySecret(t, seedClient)

	client, carrier := newCASRegistryWithSecret(t, before)
	writer := newSecretService(t, client, []string{"build"}, probe)
	carrier.injectNext(after.Data["environments.json"])

	sources := []string{"harbor"}
	view, err := writer.Update(ctx, "build-a", environment.Patch{AllowedSources: &sources}, "ops")
	if err != nil {
		t.Fatalf("Update after the competing commit: %v", err)
	}
	if view.Phase != string(environment.PhaseDraining) {
		t.Fatalf("phase = %q, want the competing drain preserved", view.Phase)
	}
	if len(view.AllowedSources) != 1 || view.AllowedSources[0] != "harbor" {
		t.Fatalf("allowed sources = %v, want this writer's edit preserved", view.AllowedSources)
	}
	if got := countAudit(t, writer, environment.AuditUpdate); got != 1 {
		t.Fatalf("update audit rows = %d, want exactly the committed attempt", got)
	}
}

// A no-write success must be based on the committed ledger. A stale instance
// that believes E is draining must not answer a later drain without committing
// it, and reads must converge without an unrelated write to trigger a refresh.
func TestStaleInstanceDrainUsesTheCommittedState(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-" + target.StateRoot), nil
	}
	client, _ := newCASRegistry(t)
	first := newSecretService(t, client, []string{"build"}, probe)
	second := newSecretService(t, client, []string{"build"}, probe)
	ctx := t.Context()

	if _, err := first.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-a: %v", err)
	}
	if _, err := second.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); err != nil {
		t.Fatalf("the second instance cannot read the registration: %v", err)
	}
	if _, err := first.Drain(ctx, "build-a", "ops"); err != nil {
		t.Fatalf("Drain build-a: %v", err)
	}
	draining, err := second.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get while draining: %v", err)
	}
	if draining.Phase != string(environment.PhaseDraining) {
		t.Fatalf("the second instance did not converge on the drain: %+v", draining)
	}
	if _, err := first.Resume(ctx, "build-a", "ops"); err != nil {
		t.Fatalf("Resume build-a: %v", err)
	}

	drained, err := second.Drain(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Drain on the stale instance: %v", err)
	}
	if drained.Phase != string(environment.PhaseDraining) {
		t.Fatalf("drain view = %+v, want draining", drained)
	}
	if _, err := first.AcquireSlot(ctx, "build-a", "slot-1"); !errors.Is(err, environment.ErrDraining) {
		t.Fatalf("AcquireSlot after the drain = %v, want ErrDraining", err)
	}

	if _, err := first.Register(ctx, registration("build-b", "build-b.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-b: %v", err)
	}
	if _, err := second.Get(ctx, "build-b", environment.Grant{DeploymentWide: true}); err != nil {
		t.Fatalf("the second instance cannot read the new registration: %v", err)
	}
	again, err := second.Drain(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("idempotent Drain: %v", err)
	}
	if again.Phase != string(environment.PhaseDraining) {
		t.Fatalf("idempotent drain view = %+v, want draining", again)
	}
}

// A carrier document that cannot be decoded must stop every operation at the
// previous version: no write may use a newer resourceVersion to overwrite it,
// and reads must not answer from the old cache.
func TestUnreadableCarrierIsNeverOverwritten(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-" + target.StateRoot), nil
	}
	client, carrier := newCASRegistry(t)
	service := newSecretService(t, client, []string{"build"}, probe)
	ctx := t.Context()
	if _, err := service.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-a: %v", err)
	}
	valid := carrier.document()
	corrupt := []byte("{not a registry document")
	carrier.setDocument(corrupt)

	concurrency := 3
	if _, err := service.Update(ctx, "build-a", environment.Patch{Concurrency: &concurrency}, "ops"); err == nil {
		t.Fatal("Update succeeded on an unreadable carrier")
	}
	if got := carrier.document(); !bytes.Equal(got, corrupt) {
		t.Fatalf("the failed update rewrote the unreadable carrier: %s", got)
	}
	if _, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); err == nil {
		t.Fatal("Get answered from the cache while the carrier is unreadable")
	}
	if got := countAudit(t, service, environment.AuditUpdate); got != 0 {
		t.Fatalf("update audit rows = %d, want none for the failed attempt", got)
	}

	carrier.setDocument(valid)
	if _, err := service.Register(ctx, registration("build-b", "build-b.example.test:7443", "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register after the carrier was repaired: %v", err)
	}
	if _, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); err != nil {
		t.Fatalf("Get build-a after the repair: %v", err)
	}
	if _, err := service.Get(ctx, "build-b", environment.Grant{DeploymentWide: true}); err != nil {
		t.Fatalf("Get build-b after the repair: %v", err)
	}
}

// Slot results are audited only after the commit that carries them. A
// conflicting attempt that ultimately fails leaves no success row, and a
// retried success records exactly once.
func TestSlotAuditFollowsTheCommit(t *testing.T) {
	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-" + target.StateRoot), nil
	}
	ctx := t.Context()

	seedClient := registrySecretClient()
	seed := newSecretService(t, seedClient, []string{"build"}, probe)
	if _, err := seed.Register(ctx, registration("build-a", "build-a.example.test:7443", "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register on the seed writer: %v", err)
	}
	if _, err := seed.AcquireSlot(ctx, "build-a", "slot-1"); err != nil {
		t.Fatalf("AcquireSlot on the seed writer: %v", err)
	}
	withSlot := readRegistrySecret(t, seedClient)
	if _, err := seed.ReleaseSlot(ctx, "build-a", "slot-1", true); err != nil {
		t.Fatalf("ReleaseSlot on the seed writer: %v", err)
	}
	withoutSlot := readRegistrySecret(t, seedClient)

	t.Run("failed retry leaves no success row", func(t *testing.T) {
		client, carrier := newCASRegistryWithSecret(t, withSlot)
		service := newSecretService(t, client, []string{"build"}, probe)
		carrier.injectNext(withoutSlot.Data["environments.json"])
		if _, err := service.ReleaseSlot(ctx, "build-a", "slot-1", true); !errors.Is(err, environment.ErrSlotNotFound) {
			t.Fatalf("ReleaseSlot after the competing release = %v, want ErrSlotNotFound", err)
		}
		if got := countAudit(t, service, environment.AuditSlotRelease); got != 0 {
			t.Fatalf("release audit rows = %d, want none for the failed attempt", got)
		}
		if got := countAudit(t, service, environment.AuditSlotIsolate); got != 0 {
			t.Fatalf("isolate audit rows = %d, want none for the failed attempt", got)
		}
		if got := carrier.document(); !bytes.Equal(got, withoutSlot.Data["environments.json"]) {
			t.Fatalf("the failed release rewrote the carrier: %s", got)
		}
	})

	t.Run("retried success records once", func(t *testing.T) {
		client, carrier := newCASRegistryWithSecret(t, withSlot)
		service := newSecretService(t, client, []string{"build"}, probe)
		carrier.injectNext(withSlot.Data["environments.json"])
		if _, err := service.ReleaseSlot(ctx, "build-a", "slot-1", true); err != nil {
			t.Fatalf("ReleaseSlot after an unrelated competing commit: %v", err)
		}
		if got := countAudit(t, service, environment.AuditSlotRelease); got != 1 {
			t.Fatalf("release audit rows = %d, want exactly one", got)
		}
		view, err := service.Get(ctx, "build-a", environment.Grant{DeploymentWide: true})
		if err != nil {
			t.Fatalf("Get after the release: %v", err)
		}
		if view.UsedSlots != 0 {
			t.Fatalf("used slots = %d, want 0", view.UsedSlots)
		}
	})
}

// Probe evidence belongs to the endpoint generation it was observed against.
// A writer whose CAS conflicts because a competing registration committed must
// not apply its old answer to the reloaded ledger: it has to probe again, and
// the tree that just registered at the same endpoint must stay authoritative.
func TestRegisterRetryReprobesAfterAnotherTreeRegistersAtTheEndpoint(t *testing.T) {
	ctx := t.Context()
	const endpoint = "shared.example.test:7443"

	probeB := environmenttest.NewProbe()
	probeB.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-b"), nil
	}
	seedClient := registrySecretClient()
	seed := newSecretService(t, seedClient, []string{"build"}, probeB)
	if _, err := seed.Register(ctx, registration("build-b", endpoint, "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-b on the competing writer: %v", err)
	}
	competing := readRegistrySecret(t, seedClient)

	client, carrier := newCASRegistry(t)
	probeA := environmenttest.NewProbe()
	probeA.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		if probeA.CallCount() > 1 {
			return probeFacts("tree-b"), nil
		}
		return probeFacts("tree-a"), nil
	}
	writer := newSecretService(t, client, []string{"build"}, probeA)
	// The competing registration commits between this writer's decision and its
	// update, so the update conflicts and the decision re-runs.
	carrier.injectNext(competing.Data["environments.json"])

	_, err := writer.Register(ctx, registration("build-e", endpoint, "/srv/rsbx/build-e", []string{"nix"}, 2), "ops")
	if !errors.Is(err, environment.ErrTreeExists) || !strings.Contains(err.Error(), "build-b") {
		t.Fatalf("Register with a stale probe answer = %v, want ErrTreeExists naming build-b", err)
	}
	if got := probeA.CallCount(); got != 2 {
		t.Fatalf("probe calls = %d, want the conflicting answer probed again exactly once", got)
	}
	if _, err := writer.Get(ctx, "build-e", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("the stale registration was stored: %v", err)
	}
	if _, err := writer.AcquireSlot(ctx, "build-e", "slot-1"); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("AcquireSlot for the stale registration = %v, want ErrNotFound", err)
	}
	winner, err := writer.Get(ctx, "build-b", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-b: %v", err)
	}
	if !winner.Verified || !winner.Schedulable || winner.Verification.TreeID != "tree-b" {
		t.Fatalf("build-b = %+v, want the competing registration still verified and schedulable", winner)
	}
	if _, err := writer.AcquireSlot(ctx, "build-b", "slot-b"); err != nil {
		t.Fatalf("AcquireSlot on build-b: %v", err)
	}
}

// A pending registration re-verifies against the current endpoint generation.
// The stale observation of the tree that B replaced must not be used as-is: it
// would mark B's new tree failed and admit the pending writer.
func TestPendingVerifyReprobesAfterAnotherTreeRegistersAtTheEndpoint(t *testing.T) {
	client, _ := newCASRegistry(t)
	ctx := t.Context()
	const endpoint = "shared.example.test:7443"

	probeB := environmenttest.NewProbe()
	probeB.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-b"), nil
	}
	second := newSecretService(t, client, []string{"build"}, probeB)

	var registerErr error
	probeA := environmenttest.NewProbe()
	probeA.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		switch calls := probeA.CallCount(); {
		case calls == 1:
			return environment.ProbeResult{}, errors.New("connection refused")
		case calls == 2:
			_, registerErr = second.Register(ctx, registration("build-b", endpoint, "/srv/rsbx/build-b", []string{"nix"}, 2), "ops")
			return probeFacts("tree-a"), nil
		default:
			return probeFacts("tree-b"), nil
		}
	}
	first := newSecretService(t, client, []string{"build"}, probeA)

	pending, err := first.Register(ctx, registration("build-e", endpoint, "/srv/rsbx/build-e", []string{"nix"}, 2), "ops")
	if err != nil {
		t.Fatalf("Register build-e: %v", err)
	}
	if pending.Schedulable {
		t.Fatalf("the pending registration is schedulable: %+v", pending)
	}

	view, err := first.Verify(ctx, "build-e", "ops")
	if err != nil {
		t.Fatalf("Verify build-e: %v", err)
	}
	if registerErr != nil {
		t.Fatalf("the competing registration failed: %v", registerErr)
	}
	if view.Verified || view.Schedulable {
		t.Fatalf("Verify admitted the pending registration with a stale answer: %+v", view)
	}
	if !strings.Contains(view.Verification.Error, "build-b") {
		t.Fatalf("verification error = %q, want the tree held by build-b", view.Verification.Error)
	}
	if got := probeA.CallCount(); got < 3 {
		t.Fatalf("probe calls = %d, want the stale answer probed again", got)
	}
	if _, err := first.AcquireSlot(ctx, "build-e", "slot-1"); !errors.Is(err, environment.ErrNotVerified) {
		t.Fatalf("AcquireSlot for the pending registration = %v, want ErrNotVerified", err)
	}
	winner, err := first.Get(ctx, "build-b", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-b: %v", err)
	}
	if !winner.Verified || !winner.Schedulable || winner.Verification.TreeID != "tree-b" {
		t.Fatalf("build-b = %+v, want the competing registration still verified and schedulable", winner)
	}
	if _, err := first.AcquireSlot(ctx, "build-b", "slot-b"); err != nil {
		t.Fatalf("AcquireSlot on build-b: %v", err)
	}
}

// A re-verification that stores the same tree, state and phase still commits
// a new generation. Probe evidence taken before it must be rejected: a content
// digest of the visible rows cannot tell the two generations apart.
func TestRegisterReprobesAfterASameValueReverification(t *testing.T) {
	client, _ := newCASRegistry(t)
	ctx := t.Context()
	const endpoint = "shared.example.test:7443"

	probeB := environmenttest.NewProbe()
	probeB.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-b"), nil
	}
	second := newSecretService(t, client, []string{"build"}, probeB)
	if _, err := second.Register(ctx, registration("build-b", endpoint, "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-b: %v", err)
	}
	before, err := second.Get(ctx, "build-b", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-b: %v", err)
	}

	var reverifyErr error
	probeA := environmenttest.NewProbe()
	probeA.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		if probeA.CallCount() > 1 {
			return probeFacts("tree-b"), nil
		}
		_, reverifyErr = second.Verify(ctx, "build-b", "ops")
		return probeFacts("tree-a"), nil
	}
	writer := newSecretService(t, client, []string{"build"}, probeA)

	_, err = writer.Register(ctx, registration("build-a", endpoint, "/srv/rsbx/build-a", []string{"nix"}, 2), "ops")
	if reverifyErr != nil {
		t.Fatalf("the same-value re-verification failed: %v", reverifyErr)
	}
	if !errors.Is(err, environment.ErrTreeExists) || !strings.Contains(err.Error(), "build-b") {
		t.Fatalf("Register with a stale probe answer = %v, want ErrTreeExists naming build-b", err)
	}
	if got := probeA.CallCount(); got != 2 {
		t.Fatalf("probe calls = %d, want the answer probed again after the re-verification", got)
	}
	if _, err := writer.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("the stale registration was stored: %v", err)
	}
	after, err := writer.Get(ctx, "build-b", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-b after the rejected registration: %v", err)
	}
	if !after.Verified || !after.Schedulable || after.Verification.TreeID != "tree-b" || after.Phase != before.Phase {
		t.Fatalf("build-b = %+v, want it unchanged and schedulable", after)
	}
	if _, err := writer.AcquireSlot(ctx, "build-b", "slot-b"); err != nil {
		t.Fatalf("AcquireSlot on build-b: %v", err)
	}
}

// Registering C supersedes B, re-verifying B supersedes C, retiring C removes
// its row: every visible registration is back to the earlier value. The
// generation must not be reusable by that round trip, so A's old answer is
// still stale.
func TestRegisterReprobesAfterTheEndpointStateRoundTrips(t *testing.T) {
	client, _ := newCASRegistry(t)
	ctx := t.Context()
	const endpoint = "shared.example.test:7443"

	tree := "tree-b"
	probeBC := environmenttest.NewProbe()
	probeBC.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		return probeFacts(tree), nil
	}
	second := newSecretService(t, client, []string{"build"}, probeBC)
	if _, err := second.Register(ctx, registration("build-b", endpoint, "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-b: %v", err)
	}
	before, err := second.Get(ctx, "build-b", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-b: %v", err)
	}

	var roundTripErr error
	probeA := environmenttest.NewProbe()
	probeA.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		if probeA.CallCount() > 1 {
			return probeFacts("tree-b"), nil
		}
		roundTripErr = func() error {
			tree = "tree-c"
			if _, err := second.Register(ctx, registration("build-c", endpoint, "/srv/rsbx/build-c", []string{"nix"}, 2), "ops"); err != nil {
				return err
			}
			tree = "tree-b"
			if _, err := second.Verify(ctx, "build-b", "ops"); err != nil {
				return err
			}
			if _, err := second.Retire(ctx, "build-c", "ops"); err != nil {
				return err
			}
			return nil
		}()
		return probeFacts("tree-a"), nil
	}
	writer := newSecretService(t, client, []string{"build"}, probeA)

	_, err = writer.Register(ctx, registration("build-a", endpoint, "/srv/rsbx/build-a", []string{"nix"}, 2), "ops")
	if roundTripErr != nil {
		t.Fatalf("the endpoint state round trip failed: %v", roundTripErr)
	}
	if !errors.Is(err, environment.ErrTreeExists) || !strings.Contains(err.Error(), "build-b") {
		t.Fatalf("Register with a stale probe answer = %v, want ErrTreeExists naming build-b", err)
	}
	if got := probeA.CallCount(); got != 2 {
		t.Fatalf("probe calls = %d, want the answer probed again after the round trip", got)
	}
	if _, err := writer.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("the stale registration was stored: %v", err)
	}
	after, err := writer.Get(ctx, "build-b", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-b after the rejected registration: %v", err)
	}
	if !after.Verified || !after.Schedulable || after.Verification.TreeID != before.Verification.TreeID || after.Phase != before.Phase {
		t.Fatalf("build-b = %+v, want the round trip to leave its visible state unchanged", after)
	}
	if _, err := writer.AcquireSlot(ctx, "build-b", "slot-b"); err != nil {
		t.Fatalf("AcquireSlot on build-b: %v", err)
	}
	if retired, err := writer.Retired(ctx, environment.Grant{DeploymentWide: true}); err != nil {
		t.Fatalf("Retired: %v", err)
	} else if len(retired) != 1 || retired[0].ID != "build-c" {
		t.Fatalf("tombstones = %+v, want only build-c", retired)
	}
}

// A carrier recreated from the empty template is a new ledger instance whose
// counter can equal the old value. Probe evidence from the old instance must
// not be applied to the new one.
func TestRegisterReprobesAfterTheCarrierIsRecreated(t *testing.T) {
	client, carrier := newCASRegistry(t)
	ctx := t.Context()
	const endpoint = "shared.example.test:7443"

	probeB := environmenttest.NewProbe()
	probeB.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-b"), nil
	}
	old := newSecretService(t, client, []string{"build"}, probeB)
	if _, err := old.Register(ctx, registration("build-b", endpoint, "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-b on the old carrier: %v", err)
	}

	var recreateErr error
	probeA := environmenttest.NewProbe()
	probeA.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		if probeA.CallCount() > 1 {
			return probeFacts("tree-b"), nil
		}
		// The carrier is deleted and recreated empty, then a new registration
		// lands on it: the counter restarts at the value the old one had.
		carrier.recreate()
		fresh := newSecretService(t, client, []string{"build"}, probeB)
		_, recreateErr = fresh.Register(ctx, registration("build-b-new", endpoint, "/srv/rsbx/build-b-new", []string{"nix"}, 2), "ops")
		return probeFacts("tree-a"), nil
	}
	writer := newSecretService(t, client, []string{"build"}, probeA)

	_, err := writer.Register(ctx, registration("build-a", endpoint, "/srv/rsbx/build-a", []string{"nix"}, 2), "ops")
	if recreateErr != nil {
		t.Fatalf("the registration on the recreated carrier failed: %v", recreateErr)
	}
	if !errors.Is(err, environment.ErrTreeExists) || !strings.Contains(err.Error(), "build-b-new") {
		t.Fatalf("Register with a probe from the old carrier = %v, want ErrTreeExists naming build-b-new", err)
	}
	if got := probeA.CallCount(); got != 2 {
		t.Fatalf("probe calls = %d, want the answer probed again on the new instance", got)
	}
	if _, err := writer.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("the stale registration was stored: %v", err)
	}
	if _, err := writer.Get(ctx, "build-b", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("the old registration survived the carrier recreation: %v", err)
	}
	winner, err := writer.Get(ctx, "build-b-new", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-b-new: %v", err)
	}
	if !winner.Verified || !winner.Schedulable || winner.Verification.TreeID != "tree-b" {
		t.Fatalf("build-b-new = %+v, want the new registration verified and schedulable", winner)
	}
	if _, err := writer.AcquireSlot(ctx, "build-b-new", "slot-b"); err != nil {
		t.Fatalf("AcquireSlot on build-b-new: %v", err)
	}
}

// Restoring a complete old document into a new carrier object must not inherit
// the old ledger identity: the new object starts a new instance, and a probe
// captured against the old one cannot be applied at the same counter value.
func TestRegisterReprobesAfterTheOldDocumentIsRestoredIntoANewCarrier(t *testing.T) {
	client, carrier := newCASRegistry(t)
	ctx := t.Context()
	const endpoint = "shared.example.test:7443"

	probeB := environmenttest.NewProbe()
	probeB.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-b"), nil
	}
	first := newSecretService(t, client, []string{"build"}, probeB)
	if _, err := first.Register(ctx, registration("build-b", endpoint, "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-b: %v", err)
	}
	saved := carrier.document()
	if _, err := first.Verify(ctx, "build-b", "ops"); err != nil {
		t.Fatalf("Verify build-b: %v", err)
	}

	var restoreErr error
	probeA := environmenttest.NewProbe()
	probeA.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		if probeA.CallCount() > 1 {
			return probeFacts("tree-b"), nil
		}
		// Recreate the carrier as a new object from the saved complete document,
		// then re-verify B through a fresh store: the counter returns to the
		// value A captured.
		carrier.restore(saved)
		fresh := newSecretService(t, client, []string{"build"}, probeB)
		_, restoreErr = fresh.Verify(ctx, "build-b", "ops")
		return probeFacts("tree-a"), nil
	}
	writer := newSecretService(t, client, []string{"build"}, probeA)

	_, err := writer.Register(ctx, registration("build-a", endpoint, "/srv/rsbx/build-a", []string{"nix"}, 2), "ops")
	if restoreErr != nil {
		t.Fatalf("Verify on the restored carrier failed: %v", restoreErr)
	}
	if !errors.Is(err, environment.ErrTreeExists) || !strings.Contains(err.Error(), "build-b") {
		t.Fatalf("Register with a probe from the old ledger = %v, want ErrTreeExists naming build-b", err)
	}
	if got := probeA.CallCount(); got != 2 {
		t.Fatalf("probe calls = %d, want the answer probed again on the new instance", got)
	}
	if _, err := writer.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("the stale registration was stored: %v", err)
	}
	winner, err := writer.Get(ctx, "build-b", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-b after the restore: %v", err)
	}
	if !winner.Verified || !winner.Schedulable || winner.Verification.TreeID != "tree-b" {
		t.Fatalf("build-b = %+v, want the re-verified registration still schedulable", winner)
	}
	if _, err := writer.AcquireSlot(ctx, "build-b", "slot-b"); err != nil {
		t.Fatalf("AcquireSlot on build-b: %v", err)
	}
}

// Clearing the document of the same carrier object loses the initialized
// identity. A registration that follows must mint a new instance rather than
// reuse the cached one, so an old probe cannot match the fresh ledger.
func TestRegisterReprobesAfterTheDocumentIsClearedOnTheSameCarrier(t *testing.T) {
	client, carrier := newCASRegistry(t)
	ctx := t.Context()
	const endpoint = "shared.example.test:7443"

	probeB := environmenttest.NewProbe()
	probeB.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-b"), nil
	}
	first := newSecretService(t, client, []string{"build"}, probeB)
	if _, err := first.Register(ctx, registration("build-b", endpoint, "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-b: %v", err)
	}
	second := newSecretService(t, client, []string{"build"}, probeB)

	var clearErr error
	probeA := environmenttest.NewProbe()
	probeA.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		if probeA.CallCount() > 1 {
			return probeFacts("tree-b"), nil
		}
		// Wipe the document without recreating the object, then register a new
		// tree through the store that had seen the initialized ledger.
		carrier.clear()
		_, clearErr = second.Register(ctx, registration("build-b-new", endpoint, "/srv/rsbx/build-b-new", []string{"nix"}, 2), "ops")
		return probeFacts("tree-a"), nil
	}
	writer := newSecretService(t, client, []string{"build"}, probeA)

	_, err := writer.Register(ctx, registration("build-a", endpoint, "/srv/rsbx/build-a", []string{"nix"}, 2), "ops")
	if clearErr != nil {
		t.Fatalf("Register on the cleared carrier failed: %v", clearErr)
	}
	if !errors.Is(err, environment.ErrTreeExists) || !strings.Contains(err.Error(), "build-b-new") {
		t.Fatalf("Register with a probe from the cleared ledger = %v, want ErrTreeExists naming build-b-new", err)
	}
	if got := probeA.CallCount(); got != 2 {
		t.Fatalf("probe calls = %d, want the answer probed again on the new instance", got)
	}
	if _, err := writer.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("the stale registration was stored: %v", err)
	}
	winner, err := writer.Get(ctx, "build-b-new", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-b-new: %v", err)
	}
	if !winner.Verified || !winner.Schedulable || winner.Verification.TreeID != "tree-b" {
		t.Fatalf("build-b-new = %+v, want the new registration verified and schedulable", winner)
	}
	if _, err := writer.AcquireSlot(ctx, "build-b-new", "slot-b"); err != nil {
		t.Fatalf("AcquireSlot on build-b-new: %v", err)
	}
}

// hookStore runs a one-shot action inside a chosen transaction of the service
// under test, so a competing commit can be placed between reads that a method
// would otherwise perform separately.
type hookStore struct {
	*environment.SecretStore
	mu    sync.Mutex
	calls int
	at    int
	hook  func()
}

func (h *hookStore) Transact(ctx context.Context, fn func(context.Context, environment.Store) error) error {
	h.mu.Lock()
	h.calls++
	hook := h.hook
	if h.calls == h.at {
		h.hook = nil
	} else {
		hook = nil
	}
	h.mu.Unlock()
	if hook != nil {
		hook()
	}
	return h.SecretStore.Transact(ctx, fn)
}

// newSecretServiceOnStore builds a service on an existing store.
func newSecretServiceOnStore(t *testing.T, store environment.Store, domains []string, probe environment.Probe) *environment.Service {
	t.Helper()
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

// Restoring the same complete document into the same carrier object moves the
// object's resource version without changing anything the document records.
// The carrier fence must reject a probe captured before that restore.
func TestRegisterReprobesAfterTheSameCarrierIsRestoredInPlace(t *testing.T) {
	client, carrier := newCASRegistry(t)
	ctx := t.Context()
	const endpoint = "shared.example.test:7443"

	probeB := environmenttest.NewProbe()
	probeB.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		return probeFacts("tree-b"), nil
	}
	first := newSecretService(t, client, []string{"build"}, probeB)
	if _, err := first.Register(ctx, registration("build-b", endpoint, "/srv/rsbx/build-b", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-b: %v", err)
	}
	saved := carrier.document()
	if _, err := first.Verify(ctx, "build-b", "ops"); err != nil {
		t.Fatalf("Verify build-b: %v", err)
	}
	captured := carrier.document()

	var (
		restoreErr        error
		restoredIdentical bool
	)
	probeA := environmenttest.NewProbe()
	probeA.InspectFunc = func(environment.Target) (environment.ProbeResult, error) {
		if probeA.CallCount() > 1 {
			return probeFacts("tree-b"), nil
		}
		// Restore the saved complete document into the same object and re-verify
		// B: the document bytes return to the state A captured.
		carrier.setDocument(saved)
		_, restoreErr = first.Verify(ctx, "build-b", "ops")
		restoredIdentical = bytes.Equal(carrier.document(), captured)
		return probeFacts("tree-a"), nil
	}
	writer := newSecretService(t, client, []string{"build"}, probeA)

	_, err := writer.Register(ctx, registration("build-a", endpoint, "/srv/rsbx/build-a", []string{"nix"}, 2), "ops")
	if restoreErr != nil {
		t.Fatalf("Verify after the in-place restore failed: %v", restoreErr)
	}
	if !restoredIdentical {
		t.Fatalf("the restored ledger is not byte-identical, so the carrier fence is not what rejects the answer")
	}
	if !errors.Is(err, environment.ErrTreeExists) || !strings.Contains(err.Error(), "build-b") {
		t.Fatalf("Register with a probe from before the restore = %v, want ErrTreeExists naming build-b", err)
	}
	if got := probeA.CallCount(); got != 2 {
		t.Fatalf("probe calls = %d, want the answer probed again after the restore", got)
	}
	if _, err := writer.Get(ctx, "build-a", environment.Grant{DeploymentWide: true}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("the stale registration was stored: %v", err)
	}
	winner, err := writer.Get(ctx, "build-b", environment.Grant{DeploymentWide: true})
	if err != nil {
		t.Fatalf("Get build-b after the restore: %v", err)
	}
	if !winner.Verified || !winner.Schedulable || winner.Verification.TreeID != "tree-b" {
		t.Fatalf("build-b = %+v, want it verified and schedulable", winner)
	}
	if _, err := writer.AcquireSlot(ctx, "build-b", "slot-b"); err != nil {
		t.Fatalf("AcquireSlot on build-b: %v", err)
	}
}

// Verify must read the target, its revision and the ledger epoch from one
// snapshot. A restore that brings back the same revision with another target
// must not let a probe of the old target verify the new one.
func TestVerifyReadsTargetAndEpochFromOneSnapshot(t *testing.T) {
	client, carrier := newCASRegistry(t)
	ctx := t.Context()
	const endpoint = "shared.example.test:7443"
	const otherEndpoint = "other.example.test:7443"

	probe := environmenttest.NewProbe()
	probe.InspectFunc = func(target environment.Target) (environment.ProbeResult, error) {
		if target.Endpoint == otherEndpoint {
			return environment.ProbeResult{}, errors.New("connection refused")
		}
		return probeFacts("tree-a"), nil
	}
	setup := newSecretService(t, client, []string{"build"}, probe)
	if _, err := setup.Register(ctx, registration("build-a", endpoint, "/srv/rsbx/build-a", []string{"nix"}, 2), "ops"); err != nil {
		t.Fatalf("Register build-a: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(carrier.document(), &raw); err != nil {
		t.Fatalf("decode the carrier document: %v", err)
	}
	environments, ok := raw["environments"].([]any)
	if !ok || len(environments) != 1 {
		t.Fatalf("unexpected document shape: %s", carrier.document())
	}
	target, ok := environments[0].(map[string]any)["Target"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected document shape: %s", carrier.document())
	}
	target["Endpoint"] = otherEndpoint
	restored, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("encode the restored document: %v", err)
	}

	store, err := environment.NewSecretStore(ctx, client.CoreV1().Secrets("foreman"), testRegistrySecret)
	if err != nil {
		t.Fatalf("NewSecretStore: %v", err)
	}
	hooked := &hookStore{SecretStore: store, at: 2, hook: func() { carrier.setDocument(restored) }}
	writer := newSecretServiceOnStore(t, hooked, []string{"build"}, probe)

	view, err := writer.Verify(ctx, "build-a", "ops")
	if err != nil {
		t.Fatalf("Verify build-a: %v", err)
	}
	if view.Verified || view.Schedulable {
		t.Fatalf("Verify applied a probe of the old target to the restored record: %+v", view)
	}
	if !strings.Contains(view.Verification.Error, "connection refused") {
		t.Fatalf("verification error = %q, want the probe of the restored target", view.Verification.Error)
	}
	if _, err := writer.AcquireSlot(ctx, "build-a", "slot-1"); !errors.Is(err, environment.ErrNotVerified) {
		t.Fatalf("AcquireSlot = %v, want ErrNotVerified", err)
	}
}

func TestSecretStoreRequiresTheCarrier(t *testing.T) {
	client := fake.NewSimpleClientset()
	_, err := environment.NewSecretStore(t.Context(), client.CoreV1().Secrets("foreman"), testRegistrySecret)
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("NewSecretStore without the carrier = %v, want a missing-carrier error", err)
	}
}
