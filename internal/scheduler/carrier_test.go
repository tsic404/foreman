package scheduler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

	// onPut and onDelete run outside s.mu before the operation takes effect:
	// a test holds one open to interleave a carrier call with the other face.
	onPut    func(c Carrier)
	onDelete func(name string)
}

func newMemCarrierStore() *memCarrierStore {
	return &memCarrierStore{carriers: map[string]Carrier{}}
}

func (s *memCarrierStore) PutCarrier(_ context.Context, c Carrier) error {
	if s.onPut != nil {
		s.onPut(c)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	if s.putErr != nil {
		return s.putErr
	}
	c.Name = carrierSecretName(c.TaskID)
	s.carriers[c.TaskID] = c
	return nil
}

func (s *memCarrierStore) DeleteCarrier(_ context.Context, name string) error {
	if s.onDelete != nil {
		s.onDelete(name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.delErr != nil {
		return s.delErr
	}
	for taskID, c := range s.carriers {
		if c.Name == name {
			delete(s.carriers, taskID)
			return nil
		}
	}
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

// restoreForServe lands the startup rebuild production performs before any
// report arrives (scheduler.Rebuild → RestorePendingReports). Without it the
// carrier backlog is unknown, and the retention bound refuses new entries
// (§4.6): unknown capacity is not room.
func restoreForServe(t *testing.T, p *PendingReports) {
	t.Helper()
	if err := p.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
}

func TestPendingCarrierHoldsTheForwardedBodyVerbatim(t *testing.T) {
	store := newMemCarrierStore()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	restoreForServe(t, p)
	body := []byte(`{"result":"ok","summary":"done"}`)
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", "", EPComplete, body, ""); err != nil {
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
	restoreForServe(t, p)
	body := []byte(`{"result":"ok"}`)
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", "ws-a", EPComplete, body, ""); err != nil {
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
	if len(sender.workspaces) != 1 || sender.workspaces[0] != "ws-a" {
		t.Errorf("replayed workspace = %v, want the carrier's ws-a", sender.workspaces)
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
	restoreForServe(t, p)
	body := []byte(`{"error":"job vanished"}`)
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", "", EPFail, body, FailureReasonJobMissing); err != nil {
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

// TestPendingCarrierHoldsTaskIDsTheFileGrammarRejects: the node-local queue
// names its files after the task id, the carrier names its object after a hash
// and rides the id on a label. A "." or "_" id is a legal label value, so its
// report must keep the cross-process face instead of losing it silently
// (§3, §6.5, §10).
func TestPendingCarrierHoldsTaskIDsTheFileGrammarRejects(t *testing.T) {
	store := newMemCarrierStore()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	restoreForServe(t, p)
	body := []byte(`{"failure_reason":"invalid_task_id"}`)
	ids := []string{"task.a", "task_a"}
	for _, taskID := range ids {
		if err := p.Enqueue(context.Background(), taskID, "", "", "", EPFail, body, FailureReasonInvalidTaskID); err != nil {
			t.Fatalf("Enqueue(%q): %v", taskID, err)
		}
		if got := store.carriers[taskID].TaskID; got != taskID {
			t.Fatalf("carrier(%q) task id = %q, want the raw id on the label face", taskID, got)
		}
	}
	if p.Len() != len(ids) {
		t.Fatalf("Len = %d, want one carrier-backed entry per report", p.Len())
	}

	// A restart keeps only the cluster: the rebuilt queue holds both reports
	// and replays the bodies they were enqueued with.
	restarted, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports(restarted): %v", err)
	}
	restoreForServe(t, restarted)
	if got := restarted.Len(); got != len(ids) {
		t.Fatalf("rebuilt depth = %d, want %d (the carrier entries)", got, len(ids))
	}
	sender := &fakeSender{code: 200}
	if remaining, err := restarted.Drain(context.Background(), sender); err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v, want the replayed reports settled", remaining, err)
	}
	if len(sender.sent) != len(ids) {
		t.Fatalf("replayed = %v, want one replay per report", sender.sent)
	}
	for _, taskID := range ids {
		want := taskID + ":fail:" + string(body)
		found := false
		for _, sent := range sender.sent {
			if sent == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("replayed = %v, want %q", sender.sent, want)
		}
	}
	if store.count() != 0 {
		t.Fatalf("carriers after the replays = %d, want 0", store.count())
	}
}

// TestPendingCarrierRetriesTaskIDsTheFileGrammarRejects: such a report has no
// node-local file, so its carrier write is the only durability face it can get
// — a failed write must be retried by the drain, not abandoned.
func TestPendingCarrierRetriesTaskIDsTheFileGrammarRejects(t *testing.T) {
	store := newMemCarrierStore()
	store.putErr = errors.New("apiserver unavailable")
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	restoreForServe(t, p)
	if err := p.Enqueue(context.Background(), "task.a", "", "", "", EPFail, []byte(`{}`), ""); err == nil {
		t.Fatal("Enqueue: want the carrier write error surfaced")
	}

	store.putErr = nil
	sender := &fakeSender{code: 503}
	if _, err := p.Drain(context.Background(), sender); err == nil {
		t.Fatal("Drain: want the undelivered report surfaced")
	}
	if store.count() != 1 {
		t.Fatalf("carriers after the retry = %d, want 1", store.count())
	}
	if !p.Queued("task.a") {
		t.Fatal("the retried report must read as queued")
	}
}

// TestPendingCarrierUnbackedFileGrammarRejectingTaskIDStaysCounted: an id the
// file grammar rejects has no disk face to wait for, so a carrier write that
// fails leaves the in-memory copy as the queue itself. The depth and the
// membership face must both see it — the Job gate and the gauge read those
// answers — and the report leaves both once a drain delivers it from staging.
func TestPendingCarrierUnbackedFileGrammarRejectingTaskIDStaysCounted(t *testing.T) {
	const taskID = "task.a"
	store := newMemCarrierStore()
	store.putErr = errors.New("apiserver unavailable")
	var gauge int
	p, err := NewPendingReports(t.TempDir(),
		WithPendingReportsCarrier(store),
		WithPendingReportsGauge(func(n int) { gauge = n }))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	restoreForServe(t, p)
	err = p.Enqueue(context.Background(), taskID, "fm-task.a", "fm-task.a", "ws-1", EPFail, []byte(`{"result":"ok"}`), "")
	if !errors.Is(err, store.putErr) {
		t.Fatalf("Enqueue = %v, want the refused carrier write", err)
	}
	if store.count() != 0 {
		t.Fatalf("carriers = %d, want 0: the write was refused", store.count())
	}
	if got := p.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1: the in-memory copy is the whole queue", got)
	}
	if gauge != 1 {
		t.Fatalf("foreman_pending_reports = %d, want 1", gauge)
	}
	if !p.Queued(taskID) || !p.HasQueuedEntry(taskID) {
		t.Fatalf("Queued = %v, HasQueuedEntry = %v; want true: the Job must wait for this report",
			p.Queued(taskID), p.HasQueuedEntry(taskID))
	}

	// The drain delivers straight from staging: no durability face ever held
	// the report, and nothing keeps it queued afterwards.
	sender := &fakeSender{code: 200}
	remaining, err := p.Drain(context.Background(), sender)
	if err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v; want the staged report delivered", remaining, err)
	}
	if len(sender.sent) != 1 || sender.sent[0] != taskID+":fail:"+`{"result":"ok"}` {
		t.Fatalf("sent = %v, want the one staged report", sender.sent)
	}
	if p.Queued(taskID) || p.Len() != 0 || gauge != 0 {
		t.Fatalf("after delivery: Queued=%v Len=%d gauge=%d, want false/0/0",
			p.Queued(taskID), p.Len(), gauge)
	}
}

// TestPendingCarrierTaskIDOutsideLabelGrammarIsVisible: an id the carrier's
// task-id label cannot hold has no cross-process face at all. That is an error
// event and a returned error — the alarm face (§10, ADR-046 decision 2) — and
// the report stays in process memory for its delivery; it is never dropped
// silently.
func TestPendingCarrierTaskIDOutsideLabelGrammarIsVisible(t *testing.T) {
	cases := []struct{ name, taskID string }{
		{name: "path separator", taskID: "../escape"},
		{name: "beyond the label length", taskID: strings.Repeat("a", 64)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemCarrierStore()
			var logs bytes.Buffer
			p, err := NewPendingReports(t.TempDir(),
				WithPendingReportsCarrier(store),
				WithPendingReportsLogger(slog.New(slog.NewJSONHandler(&logs, nil))))
			if err != nil {
				t.Fatalf("NewPendingReports: %v", err)
			}
			restoreForServe(t, p)
			err = p.Enqueue(context.Background(), tc.taskID, "", "", "ws-1", EPFail, []byte(`{}`), "")
			if !errors.Is(err, ErrCarrierTaskIDInvalid) {
				t.Fatalf("Enqueue = %v, want ErrCarrierTaskIDInvalid", err)
			}
			if store.count() != 0 {
				t.Fatalf("carriers = %d, want none: the label cannot hold the id", store.count())
			}
			if !bytes.Contains(logs.Bytes(), []byte(`"reason":"pending-report carrier cannot carry the task id"`)) {
				t.Fatalf("no undelivered event for the unpersistable task id:\n%s", logs.String())
			}
			if !bytes.Contains(logs.Bytes(), []byte(`"workspace_id":"ws-1"`)) {
				t.Fatalf("the undelivered event misses the workspace (contract §9.1):\n%s", logs.String())
			}

			// The report is still delivered from process memory.
			sender := &fakeSender{code: 200}
			if remaining, err := p.Drain(context.Background(), sender); err != nil || remaining != 0 {
				t.Fatalf("Drain = %d, %v, want the staged report delivered", remaining, err)
			}
			if len(sender.sent) != 1 || sender.sent[0] != tc.taskID+":fail:{}" {
				t.Fatalf("sent = %v, want the report delivered from memory", sender.sent)
			}
		})
	}
}

// TestSecretCarrierStoreCarriesTaskIDsTheFileGrammarRejects pins the label
// face the rebuild and the sweep read: the raw task id rides the task-id label,
// never the hashed object name.
func TestSecretCarrierStoreCarriesTaskIDsTheFileGrammarRejects(t *testing.T) {
	jobs := &fakeJobs{existingJobs: map[string]*batchv1.Job{}}
	store := NewSecretCarrierStore(jobs)
	if err := store.PutCarrier(context.Background(), Carrier{
		TaskID: "task.1_a", Endpoint: string(EPFail), Body: []byte(`{}`),
	}); err != nil {
		t.Fatalf("PutCarrier: %v", err)
	}
	if len(jobs.createdSecrets) != 1 {
		t.Fatalf("created secrets = %d, want 1", len(jobs.createdSecrets))
	}
	if got := jobs.createdSecrets[0].Labels[registry.LabelTaskID]; got != "task.1_a" {
		t.Fatalf("task-id label = %q, want the raw id", got)
	}

	err := store.PutCarrier(context.Background(), Carrier{TaskID: "../escape", Endpoint: string(EPFail)})
	if !errors.Is(err, ErrCarrierTaskIDInvalid) {
		t.Fatalf("PutCarrier = %v, want ErrCarrierTaskIDInvalid", err)
	}
	if len(jobs.createdSecrets) != 1 {
		t.Fatalf("created secrets = %d, want the invalid id refused before the write", len(jobs.createdSecrets))
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
	restoreForServe(t, p)
	if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", "", EPComplete, []byte(`{"result":"ok"}`), ""); err == nil {
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
	restoreForServe(t, p)
	body := []byte(`{"v":"` + strings.Repeat("x", MaxCarrierBytes) + `"}`)
	err = p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", "", EPComplete, body, "")
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
		TaskID: "task-1", JobName: "fm-task-1", DaemonID: "fm-task-1", WorkspaceID: "ws-a",
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
	if secret.Labels[registry.LabelWorkspaceID] != "ws-a" {
		t.Errorf("workspace label = %q, want ws-a", secret.Labels[registry.LabelWorkspaceID])
	}
	if got := string(secret.Data[carrierKeyWorkspaceID]); got != "ws-a" {
		t.Errorf("workspace data = %q, want ws-a", got)
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
		got.JobName != "fm-task-1" || got.DaemonID != "fm-task-1" ||
		got.WorkspaceID != "ws-a" || !bytes.Equal(got.Body, body) {
		t.Errorf("listed carrier = %+v, want the stored identity, workspace and body", got)
	}
}

// createOnlySecretJobs is the apiserver's Secret create face: the first
// create lands, a second one under the same name gets the AlreadyExists the
// real apiserver answers.
type createOnlySecretJobs struct {
	*fakeJobs
	landed map[string]*corev1.Secret
}

func (f *createOnlySecretJobs) CreateSecret(_ context.Context, secret *corev1.Secret) error {
	if _, ok := f.landed[secret.Name]; ok {
		return apierrors.NewAlreadyExists(corev1.Resource("secrets"), secret.Name)
	}
	f.landed[secret.Name] = secret.DeepCopy()
	return nil
}

// TestSecretCarrierKeepsTheFirstBodyOnReEnqueue pins what the queue's replay
// depends on: the carrier is create-only, so a re-enqueue adopts the object
// already there and the body a restart replays stays the first enqueue's.
// The disk face replaces the entry's body; that the two agree rests on the
// decided body being the same at both writes, which the same-body and
// different-body cases below hold to one outcome at this layer.
func TestSecretCarrierKeepsTheFirstBodyOnReEnqueue(t *testing.T) {
	first := []byte(`{"result":"ok","summary":"done"}`)
	for _, tc := range []struct {
		name      string
		reEnqueue []byte
	}{
		{name: "same body", reEnqueue: first},
		{name: "different body", reEnqueue: []byte(`{"result":"ok","summary":"done","late":true}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jobs := &createOnlySecretJobs{fakeJobs: &fakeJobs{}, landed: map[string]*corev1.Secret{}}
			p, err := NewPendingReports(t.TempDir(),
				WithPendingReportsCarrier(NewSecretCarrierStore(jobs)))
			if err != nil {
				t.Fatalf("NewPendingReports: %v", err)
			}
			restoreForServe(t, p)
			for _, body := range [][]byte{first, tc.reEnqueue} {
				if err := p.Enqueue(context.Background(), "task-1", "fm-task-1", "fm-task-1", "ws-a", EPComplete, body, ""); err != nil {
					t.Fatalf("Enqueue(%s): %v", body, err)
				}
			}
			landed, ok := jobs.landed[carrierSecretName("task-1")]
			if !ok {
				t.Fatal("no carrier landed")
			}
			if got := landed.Data[carrierKeyBody]; !bytes.Equal(got, first) {
				t.Fatalf("carrier body = %s, want the first enqueue's %s", got, first)
			}
		})
	}
}

// TestCarrierDeleteKeepsAnEntryEnqueuedInTheWindow pins the settle/delete
// race of §6.5 and ADR-046 decision 3: the carrier delete is a network call
// issued after the entry left the queue, and a duplicate terminal report can
// enqueue in that window. Its carrier write answers AlreadyExists for the
// settled entry's object — a landed write — so a delete that removes the
// object strips a live entry of its only cross-process face while
// carrierBacked stays set; a process exit then loses the report AC-16 says
// is kept.
func TestCarrierDeleteKeepsAnEntryEnqueuedInTheWindow(t *testing.T) {
	store := newMemCarrierStore()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	restoreForServe(t, p)
	ctx := context.Background()
	body := []byte(`{"result":"ok"}`)
	if err := p.Enqueue(ctx, "task-1", "fm-task-1", "fm-task-1", "", EPComplete, body, ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The settlement removed the entry; its carrier delete is still pending.
	p.mu.Lock()
	p.dropLocked("task-1")
	p.mu.Unlock()

	// A duplicate report lands in that window and adopts the old object.
	if err := p.Enqueue(ctx, "task-1", "fm-task-1", "fm-task-1", "", EPComplete, body, ""); err != nil {
		t.Fatalf("re-Enqueue: %v", err)
	}
	if !p.Queued("task-1") {
		t.Fatal("the duplicate report must read as queued")
	}

	// The delayed delete must leave the object the live entry relies on.
	p.deleteCarrier(ctx, "task-1")
	if got := store.count(); got != 1 {
		t.Fatalf("carriers after the delayed delete = %d, want 1", got)
	}

	// The carrier is the cross-process face: a restart rebuilds from it.
	restarted, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports(restarted): %v", err)
	}
	restoreForServe(t, restarted)
	if !restarted.HasQueuedEntry("task-1") {
		t.Fatal("the rebuilt queue lost the report its carrier holds")
	}
}

// TestSweepCarrierDeletesOnlySettledEntries: the sweep's criterion is the
// settlement alone — a live entry keeps its object, a settled one lets it go,
// and an uncompleted rebuild deletes nothing (unknown is not empty).
func TestSweepCarrierDeletesOnlySettledEntries(t *testing.T) {
	store := newMemCarrierStore()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	restoreForServe(t, p)
	ctx := context.Background()
	if err := p.Enqueue(ctx, "task-1", "fm-task-1", "fm-task-1", "", EPComplete, []byte(`{"result":"ok"}`), ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	name := carrierSecretName("task-1")
	if deleted, err := p.SweepCarrier(ctx, "task-1", name); err != nil || deleted {
		t.Fatalf("SweepCarrier on a live entry = %v, %v; want kept", deleted, err)
	}
	if got := store.count(); got != 1 {
		t.Fatalf("carriers = %d, want the live entry's object", got)
	}

	p.mu.Lock()
	p.dropLocked("task-1")
	p.mu.Unlock()
	if deleted, err := p.SweepCarrier(ctx, "task-1", name); err != nil || !deleted {
		t.Fatalf("SweepCarrier on a settled entry = %v, %v; want deleted", deleted, err)
	}
	if got := store.count(); got != 0 {
		t.Fatalf("carriers after the sweep = %d, want none", got)
	}

	// A queue whose carrier list did not complete reads every task as queued:
	// the sweep must not touch anything before the rebuild.
	if err := store.PutCarrier(ctx, Carrier{
		TaskID: "task-2", Endpoint: string(EPFail), Body: []byte(`{}`), EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("PutCarrier: %v", err)
	}
	fresh, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if deleted, err := fresh.SweepCarrier(ctx, "task-2", carrierSecretName("task-2")); err != nil || deleted {
		t.Fatalf("SweepCarrier before the rebuild = %v, %v; want kept", deleted, err)
	}
}

// TestCarrierWriteDoesNotSerializeAcrossTasks pins the carrier lock's scope:
// the invariant is per task, so one task's slow cluster write must not hold
// up another task's enqueue.
func TestCarrierWriteDoesNotSerializeAcrossTasks(t *testing.T) {
	store := newMemCarrierStore()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	store.onPut = func(c Carrier) {
		if c.TaskID == "task-slow" {
			entered <- struct{}{}
			<-release
		}
	}
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	restoreForServe(t, p)
	ctx := context.Background()

	slowDone := make(chan error, 1)
	go func() {
		slowDone <- p.Enqueue(ctx, "task-slow", "fm-task-slow", "fm-task-slow", "", EPComplete, []byte(`{"result":"ok"}`), "")
	}()
	<-entered

	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	fastDone := make(chan error, 1)
	go func() {
		fastDone <- p.Enqueue(ctx, "task-fast", "fm-task-fast", "fm-task-fast", "", EPComplete, []byte(`{"result":"ok"}`), "")
	}()
	select {
	case err := <-fastDone:
		if err != nil {
			t.Fatalf("Enqueue(task-fast): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Enqueue(task-fast) waited on another task's carrier write")
	}

	close(release)
	released = true
	if err := <-slowDone; err != nil {
		t.Fatalf("Enqueue(task-slow): %v", err)
	}
	if got := store.count(); got != 2 {
		t.Fatalf("carriers = %d, want both tasks' objects", got)
	}
}

// TestCarrierDeleteSerializesWithAnEnqueueForTheSameTask pins the other side
// of the per-task lock: while the settled entry's carrier delete is in
// flight, a duplicate report for the same task must wait, then write its own
// object. An enqueue released into that window adopts the object the delete
// then removes — a live entry with no carrier.
func TestCarrierDeleteSerializesWithAnEnqueueForTheSameTask(t *testing.T) {
	store := newMemCarrierStore()
	ctx := context.Background()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	restoreForServe(t, p)
	// An orphan object the queue no longer holds: the sweep's delete target.
	if err := store.PutCarrier(ctx, Carrier{
		TaskID: "task-1", Endpoint: string(EPComplete),
		Body: []byte(`{"result":"ok"}`), EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("PutCarrier: %v", err)
	}

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	store.onDelete = func(string) {
		entered <- struct{}{}
		<-release
	}
	deleteDone := make(chan struct{})
	go func() {
		p.deleteCarrier(ctx, "task-1")
		close(deleteDone)
	}()
	<-entered

	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	enqueued := make(chan error, 1)
	go func() {
		enqueued <- p.Enqueue(ctx, "task-1", "fm-task-1", "fm-task-1", "", EPComplete, []byte(`{"result":"ok"}`), "")
	}()
	select {
	case err := <-enqueued:
		t.Fatalf("Enqueue completed while the same task's carrier delete was in flight: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	released = true
	<-deleteDone
	if err := <-enqueued; err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !p.Queued("task-1") {
		t.Fatal("the enqueued report must read as queued")
	}
	if got := store.count(); got != 1 {
		t.Fatalf("carriers = %d, want the enqueued report's object", got)
	}
}

// TestSweepCarrierSerializesWithAnEnqueueForTheSameTask pins the sweep's lock
// key: the delete runs under the task id the listed object carries, the key
// the carrier write takes too — never the object name. A sweep keyed by name
// (or one that took no lock) lets a duplicate report enqueue in the window
// and adopt an object the sweep then removes: a live entry with no carrier
// (§6.5, ADR-046 decision 1).
func TestSweepCarrierSerializesWithAnEnqueueForTheSameTask(t *testing.T) {
	store := newMemCarrierStore()
	ctx := context.Background()
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	restoreForServe(t, p)
	// An orphan object the queue no longer holds: the sweep's delete target.
	if err := store.PutCarrier(ctx, Carrier{
		TaskID: "task-1", Endpoint: string(EPComplete),
		Body: []byte(`{"result":"ok"}`), EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("PutCarrier: %v", err)
	}

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	store.onDelete = func(string) {
		entered <- struct{}{}
		<-release
	}
	swept := make(chan error, 1)
	go func() {
		deleted, err := p.SweepCarrier(ctx, "task-1", carrierSecretName("task-1"))
		if err == nil && !deleted {
			err = errors.New("SweepCarrier kept a settled entry's object")
		}
		swept <- err
	}()
	<-entered

	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	enqueued := make(chan error, 1)
	go func() {
		enqueued <- p.Enqueue(ctx, "task-1", "fm-task-1", "fm-task-1", "", EPComplete, []byte(`{"result":"ok"}`), "")
	}()
	select {
	case err := <-enqueued:
		t.Fatalf("Enqueue completed while the same task's carrier sweep was in flight: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	released = true
	if err := <-swept; err != nil {
		t.Fatalf("SweepCarrier: %v", err)
	}
	if err := <-enqueued; err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !p.Queued("task-1") {
		t.Fatal("the enqueued report must read as queued")
	}
	// The sweep removed the orphan, then the enqueue wrote its own object:
	// the live entry still has its cross-process face.
	if got := store.count(); got != 1 {
		t.Fatalf("carriers = %d, want the enqueued report's object", got)
	}
}

// TestSweepCarrierDeletesTheListedName pins the sweep's delete target: the
// name comes from the listing, never re-derived from the task id. A carrier
// written under a non-derived name still carries the task-id label, so a
// sweep that derived the name would leave the listed object behind.
func TestSweepCarrierDeletesTheListedName(t *testing.T) {
	jobs := &fakeJobs{existingJobs: map[string]*batchv1.Job{}}
	store := NewSecretCarrierStore(jobs)
	p, err := NewPendingReports(t.TempDir(), WithPendingReportsCarrier(store))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	restoreForServe(t, p)
	ctx := context.Background()

	legacy := "pending-report-legacy-task-1"
	if legacy == carrierSecretName("task-1") {
		t.Fatalf("%q is the derived name; the test needs a different one", legacy)
	}
	jobs.secrets = []corev1.Secret{{
		ObjectMeta: metav1.ObjectMeta{
			Name: legacy,
			Labels: map[string]string{
				registry.LabelRole:      registry.RolePendingReport,
				registry.LabelManagedBy: registry.ManagedByForeman,
				registry.LabelTaskID:    "task-1",
			},
		},
		Type: corev1.SecretTypeOpaque,
	}}
	listed, err := store.ListCarriers(ctx)
	if err != nil || len(listed) != 1 || listed[0].Name != legacy {
		t.Fatalf("listed carriers = %+v, %v; want the legacy object", listed, err)
	}

	deleted, err := p.SweepCarrier(ctx, listed[0].TaskID, listed[0].Name)
	if err != nil || !deleted {
		t.Fatalf("SweepCarrier = %v, %v; want the listed object deleted", deleted, err)
	}
	if !jobs.secretDeleted(legacy) {
		t.Fatalf("DeleteSecret did not get the listed name %q (deleted: %v)", legacy, jobs.deletedSecrets)
	}
}
