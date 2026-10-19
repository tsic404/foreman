package scheduler

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// memCarrierStore is the cluster carrier face in memory: one carrier per
// task, with programmable failures.
type memCarrierStore struct {
	mu       sync.Mutex
	carriers map[string]Carrier
	putErr   error
	listErr  error
	delErr   error
	puts     int
}

func newMemCarrierStore() *memCarrierStore {
	return &memCarrierStore{carriers: map[string]Carrier{}}
}

func (s *memCarrierStore) PutCarrier(_ context.Context, c Carrier) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	if s.putErr != nil {
		return s.putErr
	}
	s.carriers[c.TaskID] = c
	return nil
}

func (s *memCarrierStore) DeleteCarrier(_ context.Context, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.delErr != nil {
		return s.delErr
	}
	delete(s.carriers, taskID)
	return nil
}

func (s *memCarrierStore) ListCarriers(context.Context) ([]Carrier, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]Carrier, 0, len(s.carriers))
	for _, c := range s.carriers {
		out = append(out, c)
	}
	return out, nil
}

func (s *memCarrierStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.carriers)
}

func TestPendingCarrierHoldsTheForwardedBodyVerbatim(t *testing.T) {
	store := newMemCarrierStore()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	body := []byte(`{"result":"ok","summary":"done"}`)
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPComplete, body, ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if store.count() != 1 {
		t.Fatalf("carriers = %d, want 1", store.count())
	}
	got := store.carriers["task-1"]
	if !bytes.Equal(got.Body, body) {
		t.Errorf("carrier body = %s, want the forwarded %s", got.Body, body)
	}
	if got.Endpoint != string(EPComplete) || got.JobName != "fm-task-1" || got.DaemonID != "fm-task-1" {
		t.Errorf("carrier identity = %+v, want the enqueue's replay identity", got)
	}

	p.Drop(context.Background(), "task-1")
	if store.count() != 0 {
		t.Errorf("carriers after settlement = %d, want 0", store.count())
	}
}

func TestPendingRestoreRebuildsTheQueueFromCarriers(t *testing.T) {
	store := newMemCarrierStore()
	dir := t.TempDir()
	p, err := NewPendingReports(dir, WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	body := []byte(`{"result":"ok"}`)
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPComplete, body, ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// A process exit discards the node-local directory (emptyDir); the
	// carrier is all that is left.
	var gauges []int
	restarted, err := NewPendingReports(t.TempDir(),
		WithPendingReportsCarrier(store),
		WithPendingReportsGauge(func(n int) { gauges = append(gauges, n) }))
	if err != nil {
		t.Fatalf("NewPendingReports(restarted): %v", err)
	}
	if err := restarted.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := restarted.Len(); got != 1 {
		t.Fatalf("rebuilt depth = %d, want 1 (the carrier count)", got)
	}
	if !restarted.Queued("task-1") {
		t.Fatal("the restored report is not queued")
	}
	if len(gauges) == 0 || gauges[len(gauges)-1] != 1 {
		t.Errorf("gauge = %v, want the rebuilt depth 1", gauges)
	}

	sender := &fakeSender{code: 200}
	remaining, err := restarted.Drain(context.Background(), sender)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if remaining != 0 {
		t.Errorf("remaining = %d, want 0", remaining)
	}
	if len(sender.sent) != 1 || sender.sent[0] != "task-1:complete:"+string(body) {
		t.Errorf("sent = %v, want the carrier's body replayed", sender.sent)
	}
	if store.count() != 0 {
		t.Errorf("carriers after the replay = %d, want 0", store.count())
	}
}

// TestPendingCarrierCarriesTheFailureAttribution: the attribution a
// synthesized failure verdict was decided with rides the queue entry and its
// carrier, so a replay after a process exit restores it instead of sending an
// empty one (§4.1). The body is never the source.
func TestPendingCarrierCarriesTheFailureAttribution(t *testing.T) {
	store := newMemCarrierStore()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	body := []byte(`{"error":"job vanished"}`)
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPFail, body, FailureReasonJobMissing); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if got := store.carriers["task-1"].FailureReason; got != FailureReasonJobMissing {
		t.Fatalf("carrier attribution = %q, want %q", got, FailureReasonJobMissing)
	}

	restarted, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports(restarted): %v", err)
	}
	if restarted.Rebuilt() {
		t.Fatal("a fresh carrier-backed queue must not read as rebuilt")
	}
	if !restarted.Queued("task-1") {
		t.Fatal("an uncompleted rebuild must not read as an empty queue")
	}
	if err := restarted.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !restarted.HasQueuedEntry("task-1") {
		t.Fatal("the restored report is not queued")
	}

	sender := &fakeSender{code: 200}
	if _, err := restarted.Drain(context.Background(), sender); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(sender.reasons) != 1 || sender.reasons[0] != FailureReasonJobMissing {
		t.Fatalf("replayed attribution = %v, want %q", sender.reasons, FailureReasonJobMissing)
	}
	if len(sender.sent) != 1 || sender.sent[0] != "task-1:fail:"+string(body) {
		t.Fatalf("replayed = %v, want the carrier's body", sender.sent)
	}
}

func TestPendingRestoreFailureIsNotAnEmptyQueue(t *testing.T) {
	store := newMemCarrierStore()
	store.carriers["task-1"] = Carrier{
		TaskID: "task-1", JobName: "fm-task-1", DaemonID: "fm-task-1",
		Endpoint: string(EPComplete), Body: []byte(`{"result":"ok"}`), EnqueuedAt: time.Now(),
	}
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	store.listErr = errors.New("apiserver unavailable")
	if err := p.Restore(context.Background()); err == nil {
		t.Fatal("Restore: want the list error surfaced")
	}
	// An uncompleted list is unknown, not empty (§4.3): the settlement gate
	// reads every task as queued and the rebuild predicate stays false, while
	// the depth and the known-membership face report what the process holds.
	if p.Rebuilt() {
		t.Fatal("Rebuilt = true after a failed list")
	}
	if !p.Queued("task-1") {
		t.Fatal("a failed list read as an empty queue")
	}
	if p.HasQueuedEntry("task-1") || p.Len() != 0 {
		t.Fatalf("known state after the failed list: HasQueuedEntry=%v Len=%d, want none",
			p.HasQueuedEntry("task-1"), p.Len())
	}

	store.listErr = nil
	if err := p.Restore(context.Background()); err != nil {
		t.Fatalf("Restore retry: %v", err)
	}
	if !p.Rebuilt() || p.Len() != 1 || !p.Queued("task-1") || !p.HasQueuedEntry("task-1") {
		t.Fatalf("after the retry: Rebuilt=%v Len=%d Queued=%v HasQueuedEntry=%v, want the carrier restored",
			p.Rebuilt(), p.Len(), p.Queued("task-1"), p.HasQueuedEntry("task-1"))
	}
}

func TestPendingCarrierWriteFailureIsRetriedByDrain(t *testing.T) {
	store := newMemCarrierStore()
	store.putErr = errors.New("apiserver unavailable")
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPComplete, []byte(`{"result":"ok"}`), ""); err == nil {
		t.Fatal("Enqueue: want the carrier write error surfaced")
	}
	if !p.Queued("task-1") {
		t.Fatal("the report must stay queued in process memory")
	}

	store.putErr = nil
	sender := &fakeSender{code: 503}
	if _, err := p.Drain(context.Background(), sender); err == nil {
		t.Fatal("Drain: want the undelivered report surfaced")
	}
	if store.count() != 1 {
		t.Fatalf("carriers after the retry = %d, want 1", store.count())
	}
	if !p.Queued("task-1") {
		t.Fatal("the report left the queue before it was delivered")
	}
}

func TestPendingCarrierBoundKeepsOversizeReportsInProcess(t *testing.T) {
	store := newMemCarrierStore()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	body := []byte(`{"v":"` + strings.Repeat("x", MaxCarrierBytes) + `"}`)
	err = p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", EPComplete, body, "")
	if !errors.Is(err, ErrCarrierPayloadTooLarge) {
		t.Fatalf("Enqueue = %v, want ErrCarrierPayloadTooLarge", err)
	}
	if store.count() != 0 {
		t.Fatalf("carriers = %d, want none past the bound", store.count())
	}
	if !p.Queued("task-1") {
		t.Fatal("an oversize report must stay queued in process memory")
	}
}

func TestSecretCarrierStoreShapeAndDomains(t *testing.T) {
	jobs := &fakeJobs{existingJobs: map[string]*batchv1.Job{}}
	store := NewSecretCarrierStore(jobs)
	body := []byte(`{"result":"ok"}`)
	if err := store.PutCarrier(context.Background(), Carrier{
		TaskID: "task-1", JobName: "fm-task-1", DaemonID: "fm-task-1",
		Endpoint: string(EPComplete), Body: body, EnqueuedAt: time.Now(), Attempts: 2,
	}); err != nil {
		t.Fatalf("PutCarrier: %v", err)
	}
	if len(jobs.createdSecrets) != 1 {
		t.Fatalf("created secrets = %d, want 1", len(jobs.createdSecrets))
	}
	secret := jobs.createdSecrets[0]
	if secret.Labels[registry.LabelRole] != registry.RolePendingReport {
		t.Errorf("role label = %q, want %q", secret.Labels[registry.LabelRole], registry.RolePendingReport)
	}
	if secret.Labels[registry.LabelManagedBy] != registry.ManagedByForeman {
		t.Errorf("managed-by label = %q", secret.Labels[registry.LabelManagedBy])
	}
	if secret.Labels[registry.LabelTaskID] != "task-1" {
		t.Errorf("task-id label = %q", secret.Labels[registry.LabelTaskID])
	}
	if !bytes.Equal(secret.Data[carrierKeyBody], body) {
		t.Errorf("carrier body = %s, want the enqueued bytes", secret.Data[carrierKeyBody])
	}

	// The list face returns the carrier with its replay identity intact.
	jobs.secrets = []corev1.Secret{*secret.DeepCopy()}
	carriers, err := store.ListCarriers(context.Background())
	if err != nil {
		t.Fatalf("ListCarriers: %v", err)
	}
	if len(carriers) != 1 {
		t.Fatalf("carriers = %d, want 1", len(carriers))
	}
	got := carriers[0]
	if got.TaskID != "task-1" || got.Endpoint != string(EPComplete) ||
		got.JobName != "fm-task-1" || got.DaemonID != "fm-task-1" || !bytes.Equal(got.Body, body) {
		t.Errorf("listed carrier = %+v, want the stored identity and body", got)
	}
}
