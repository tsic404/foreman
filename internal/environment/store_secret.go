package environment

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

// SecretStore keeps the registry in one Kubernetes Secret so it survives a
// Pod rebuild. The Secret must exist before startup: a missing carrier is a
// deployment error, not a reason to silently start with an empty pool.
type SecretStore struct {
	*MemoryStore
	secrets corev1client.SecretInterface
	name    string

	writeMu     sync.Mutex
	instanceID  string
	carrierUID  string
	initialized bool
}

// NewSecretStore loads the registry Secret. The Secret is created by the
// deployment manifest; an absent or malformed one refuses startup.
func NewSecretStore(ctx context.Context, secrets corev1client.SecretInterface, name string) (*SecretStore, error) {
	if secrets == nil {
		return nil, errors.New("environment: a secret carrier needs the Kubernetes client")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errors.New("environment: a secret carrier needs a name")
	}
	store := &SecretStore{MemoryStore: NewMemoryStore(), secrets: secrets, name: name}
	secret, err := secrets.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("environment registry secret %s is missing; apply the deployment manifest", name)
	}
	if err != nil {
		return nil, fmt.Errorf("read the environment registry secret %s: %w", name, err)
	}
	document := environmentDocument{}
	if raw := secret.Data[environmentDocumentKey]; len(raw) > 0 {
		document, err = decodeEnvironmentDocument(raw, "secret "+name)
		if err != nil {
			return nil, err
		}
	}
	store.instanceID = store.resolveLedgerIdentity(document, string(secret.UID))
	store.restoreDocument(document, store.instanceID)
	return store, nil
}

// resolveLedgerIdentity answers the instance identity for a document read
// through the carrier with the given object uid. Construction calls it before
// the store is shared; every later call holds writeMu.
//
// The document's identity is adopted only when it names this exact carrier
// object. A document written through another object (a recreated Secret or a
// restored copy) is never inherited: it starts a new instance. An object whose
// initialized document no longer carries its identity (cleared data, replaced
// document) also starts a new instance, so evidence from the old one cannot
// match. Only a genuinely uninitialized object keeps the pending identity its
// first write will materialize.
func (s *SecretStore) resolveLedgerIdentity(document environmentDocument, uid string) string {
	if document.LedgerID != "" && document.CarrierUID == uid {
		s.instanceID = document.LedgerID
		s.carrierUID = uid
		s.initialized = true
		return s.instanceID
	}
	if s.carrierUID == uid && s.instanceID != "" && !s.initialized {
		return s.instanceID
	}
	s.carrierUID = uid
	s.instanceID = newLedgerID()
	s.initialized = false
	return s.instanceID
}

// PutEnvironment stores one record through the transaction path.
func (s *SecretStore) PutEnvironment(ctx context.Context, env Environment) error {
	return s.Transact(ctx, func(ctx context.Context, tx Store) error {
		return tx.PutEnvironment(ctx, env)
	})
}

// PutEnvironments stores several records in one Secret update.
func (s *SecretStore) PutEnvironments(ctx context.Context, envs []Environment) error {
	return s.Transact(ctx, func(ctx context.Context, tx Store) error {
		return tx.PutEnvironments(ctx, envs)
	})
}

// RetireEnvironment removes the registration and records the tombstone in one
// Secret update.
func (s *SecretStore) RetireEnvironment(ctx context.Context, id ID, tomb Tombstone) error {
	return s.Transact(ctx, func(ctx context.Context, tx Store) error {
		return tx.RetireEnvironment(ctx, id, tomb)
	})
}

// Transact reads the committed Secret, runs the whole decision against that
// snapshot, and commits the buffered batch only while the carrier still has
// the version the decision was based on. A conflict discards the batch,
// including its audit rows, and re-runs the decision on the new snapshot.
func (s *SecretStore) Transact(ctx context.Context, fn func(context.Context, Store) error) error {
	for attempt := 0; ; attempt++ {
		s.writeMu.Lock()
		secret, err := s.secrets.Get(ctx, s.name, metav1.GetOptions{})
		if err != nil {
			s.writeMu.Unlock()
			return fmt.Errorf("read the environment registry secret %s: %w", s.name, err)
		}
		document := environmentDocument{}
		if raw := secret.Data[environmentDocumentKey]; len(raw) > 0 {
			document, err = decodeEnvironmentDocument(raw, "secret "+s.name)
			if err != nil {
				// A version whose document cannot be read must never become the
				// base of a write: leave the previous cache untouched and fail
				// the operation until the carrier is readable again.
				s.writeMu.Unlock()
				return err
			}
		}
		ledgerID := s.resolveLedgerIdentity(document, string(secret.UID))
		environments, tombstones := environmentDocumentMaps(document)
		carrier := string(secret.UID) + "/" + secret.ResourceVersion
		tx := newLedgerTx(s, environments, tombstones, carrier, ledgerID, document.Generation)
		if err := fn(ctx, tx); err != nil {
			s.writeMu.Unlock()
			return err
		}
		if !tx.dirty {
			s.MemoryStore.replaceMaps(tx.environments, tx.tombstones, ledgerID, document.Generation)
			s.writeMu.Unlock()
			tx.commitAudits(ctx)
			return nil
		}
		raw, err := encodeEnvironmentDocument(tx.environments, tx.tombstones, document.Generation+1, ledgerID, string(secret.UID))
		if err != nil {
			s.writeMu.Unlock()
			return err
		}
		_, err = s.secrets.Update(ctx, s.carrier(secret, raw), metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			s.writeMu.Unlock()
			if attempt >= storeConflictRetries {
				return fmt.Errorf("%w: the environment registry secret changed; retry", ErrConflict)
			}
			continue
		}
		if err != nil {
			s.writeMu.Unlock()
			return fmt.Errorf("update the environment registry secret %s: %w", s.name, err)
		}
		s.MemoryStore.replaceMaps(tx.environments, tx.tombstones, ledgerID, document.Generation+1)
		// The pending identity is now materialized: losing the document from
		// here on must start a new instance, not revive this one.
		s.initialized = true
		s.writeMu.Unlock()
		tx.commitAudits(ctx)
		return nil
	}
}

func (s *SecretStore) carrier(secret *corev1.Secret, raw []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            s.name,
			Namespace:       secret.Namespace,
			ResourceVersion: secret.ResourceVersion,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "foreman",
				"app.kubernetes.io/component": "environment-registry",
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{environmentDocumentKey: raw},
	}
}

var _ TransactionalStore = (*SecretStore)(nil)
