package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// The pending-report carrier is the queue's cross-process persistence face
// (ADR-046, 02-contracts.md §3, §4.6): one Foreman-created Secret per queued
// terminal report, carrying the report body verbatim plus the identity the
// replay needs. Its lifecycle is the queue's: written on enqueue, deleted on
// settlement (补报成功 or the two settlement classes).
const (
	// carrierSecretPrefix names the object. The name is not a locator — the
	// task-id label is — so the hash only keeps names unique and DNS-safe
	// for task ids the label grammar would not accept.
	carrierSecretPrefix = "foreman-pending-"

	// MaxCarrierBytes bounds one carrier's data, matching the apiserver's
	// Secret size limit. A report beyond it cannot be persisted and is kept
	// in process memory only (ADR-046 decision 6).
	MaxCarrierBytes = 1 << 20

	// carrierEnvelopeBytes reserves room for the object's labels and keys in
	// the bound computation; the exact apiserver accounting is not the
	// contract, the order of magnitude is.
	carrierEnvelopeBytes = 512
)

// Carrier data keys. The body is stored as the exact bytes that were
// forwarded, so a replay is byte-identical to the enqueue (§4.4 定论体).
const (
	carrierKeyTaskID     = "task_id"
	carrierKeyJobName    = "job_name"
	carrierKeyDaemonID   = "daemon_id"
	carrierKeyEndpoint   = "endpoint"
	carrierKeyBody       = "body"
	carrierKeyEnqueuedAt = "enqueued_at"
	carrierKeyAttempts   = "attempts"
	// carrierKeyFailureReason carries the attribution a synthesized failure
	// verdict was decided with (§4.1). It is queue identity, not body data:
	// an inbound failure body also carries the key, and reading it from there
	// would mislabel a server-side verdict.
	carrierKeyFailureReason = "failure_reason"
)

// ErrCarrierPayloadTooLarge reports a report the carrier object cannot hold.
// Such a report stays in process memory only; the error is the 告警面, never
// a reason to drop it (ADR-046 decision 6).
var ErrCarrierPayloadTooLarge = errors.New("pending report exceeds the carrier object bound")

// Carrier is one pending report's durable form (ADR-046 decision 1).
type Carrier struct {
	Name       string
	TaskID     string
	JobName    string
	DaemonID   string
	Endpoint   string
	Body       []byte
	EnqueuedAt time.Time
	Attempts   int
	// FailureReason is the verdict's attribution when the queued report was
	// a failure Foreman judged itself (§4.1). Empty for a report the server
	// decided.
	FailureReason string
}

// CarrierStore is the K8s Secret face the pending-report queue persists
// through. PutCarrier is create-only for a given task: one queued report per
// task, so a re-enqueue replaces the object. DeleteCarrier is idempotent.
type CarrierStore interface {
	PutCarrier(ctx context.Context, c Carrier) error
	DeleteCarrier(ctx context.Context, taskID string) error
	ListCarriers(ctx context.Context) ([]Carrier, error)
}

// NewSecretCarrierStore returns the production carrier store over the
// scheduler's Secret access (RBAC: §3 — create/delete/get/list).
func NewSecretCarrierStore(jobs JobClient) CarrierStore {
	return &secretCarrierStore{jobs: jobs}
}

type secretCarrierStore struct {
	jobs JobClient
}

// carrierSecretName derives the object name from the task id. Hashing keeps
// it DNS-1123 safe and unique; the task-id label, not this name, is the
// locator (§6.5).
func carrierSecretName(taskID string) string {
	sum := sha256.Sum256([]byte(taskID))
	return carrierSecretPrefix + hex.EncodeToString(sum[:20])
}

func (c Carrier) data() map[string][]byte {
	return map[string][]byte{
		carrierKeyTaskID:        []byte(c.TaskID),
		carrierKeyJobName:       []byte(c.JobName),
		carrierKeyDaemonID:      []byte(c.DaemonID),
		carrierKeyEndpoint:      []byte(c.Endpoint),
		carrierKeyBody:          c.Body,
		carrierKeyEnqueuedAt:    []byte(c.EnqueuedAt.UTC().Format(time.RFC3339Nano)),
		carrierKeyAttempts:      []byte(strconv.Itoa(c.Attempts)),
		carrierKeyFailureReason: []byte(c.FailureReason),
	}
}

func (c Carrier) sizeBytes() int {
	n := carrierEnvelopeBytes
	for k, v := range c.data() {
		n += len(k) + len(v)
	}
	return n
}

func (s *secretCarrierStore) PutCarrier(ctx context.Context, c Carrier) error {
	if c.TaskID == "" {
		return errors.New("carrier task id is required")
	}
	if c.sizeBytes() > MaxCarrierBytes {
		return fmt.Errorf("%w: %d bytes > %d", ErrCarrierPayloadTooLarge, c.sizeBytes(), MaxCarrierBytes)
	}
	name := carrierSecretName(c.TaskID)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				registry.LabelManagedBy: registry.ManagedByForeman,
				registry.LabelRole:      registry.RolePendingReport,
				registry.LabelTaskID:    c.TaskID,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: c.data(),
	}
	if err := s.jobs.CreateSecret(ctx, secret); err != nil {
		// One carrier per task: a retry after a lost create response finds
		// its own object. The body is the decided one either way (a
		// re-enqueue keeps the original enqueue clock and the first verdict
		// owns the body), so the existing object is the landed write.
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("create carrier %s: %w", name, err)
	}
	return nil
}

func (s *secretCarrierStore) DeleteCarrier(ctx context.Context, taskID string) error {
	if err := s.jobs.DeleteSecret(ctx, carrierSecretName(taskID)); err != nil {
		return fmt.Errorf("delete carrier for task %s: %w", taskID, err)
	}
	return nil
}

func (s *secretCarrierStore) ListCarriers(ctx context.Context) ([]Carrier, error) {
	secrets, err := s.jobs.ListSecrets(ctx)
	if err != nil {
		return nil, fmt.Errorf("list carrier secrets: %w", err)
	}
	out := make([]Carrier, 0)
	for i := range secrets {
		labels := secrets[i].GetLabels()
		if labels[registry.LabelRole] != registry.RolePendingReport {
			continue
		}
		if labels[registry.LabelManagedBy] != registry.ManagedByForeman {
			continue
		}
		taskID := labels[registry.LabelTaskID]
		if taskID == "" {
			continue
		}
		data := secrets[i].Data
		c := Carrier{
			Name:          secrets[i].Name,
			TaskID:        taskID,
			JobName:       string(data[carrierKeyJobName]),
			DaemonID:      string(data[carrierKeyDaemonID]),
			Endpoint:      string(data[carrierKeyEndpoint]),
			Body:          data[carrierKeyBody],
			FailureReason: string(data[carrierKeyFailureReason]),
		}
		if raw := string(data[carrierKeyEnqueuedAt]); raw != "" {
			if at, perr := time.Parse(time.RFC3339Nano, raw); perr == nil {
				c.EnqueuedAt = at
			}
		}
		if raw := string(data[carrierKeyAttempts]); raw != "" {
			if n, perr := strconv.Atoi(raw); perr == nil {
				c.Attempts = n
			}
		}
		out = append(out, c)
	}
	return out, nil
}
