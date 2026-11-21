package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
)

// sourceSender records the source each replay is routed through.
type sourceSender struct {
	seen []string
}

func (s *sourceSender) DeliverQueued(_ context.Context, _ Endpoint, taskID, _ string, _ []byte, _ string) (int, []byte, error) {
	s.seen = append(s.seen, "legacy/"+taskID)
	return 200, nil, nil
}

func (s *sourceSender) DeliverQueuedInSource(_ context.Context, _ Endpoint, taskID, sourceID, _ string, _ []byte, _ string) (int, []byte, error) {
	s.seen = append(s.seen, sourceID+"/"+taskID)
	return 200, nil, nil
}

// keyedCarrierStore keys one object per queue slot, which is what the real
// Secret face does after the key gained the source partition.
type keyedCarrierStore struct {
	mu       sync.Mutex
	carriers map[string]Carrier
}

func newKeyedCarrierStore() *keyedCarrierStore {
	return &keyedCarrierStore{carriers: map[string]Carrier{}}
}

func (s *keyedCarrierStore) PutCarrier(_ context.Context, c Carrier) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c.Name = carrierSecretName(c.Key)
	s.carriers[c.Key] = c
	return nil
}

func (s *keyedCarrierStore) DeleteCarrier(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, c := range s.carriers {
		if c.Name == name {
			delete(s.carriers, key)
			return nil
		}
	}
	return nil
}

func (s *keyedCarrierStore) ListCarriers(context.Context) ([]Carrier, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Carrier, 0, len(s.carriers))
	for _, c := range s.carriers {
		out = append(out, c)
	}
	return out, nil
}

// TestPendingReportsKeepTwoSourcesApart pins the queue's attempt identity:
// the upstream task id alone is not unique across registered servers, so two
// queued reports for the same id on two sources must occupy two slots, have
// two carriers, replay to their own source, and survive a restart as two.
func TestPendingReportsKeepTwoSourcesApart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	carriers := newKeyedCarrierStore()
	p, err := NewPendingReports(dir, WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := p.Restore(ctx); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	keyA := pendingKeyFor("src-a", "ws-shared", "task-1")
	keyB := pendingKeyFor("src-b", "ws-shared", "task-1")
	if keyA == keyB {
		t.Fatalf("both attempts share the queue slot %q", keyA)
	}
	if carrierSecretName(keyA) == carrierSecretName(keyB) {
		t.Fatal("both attempts share one carrier object")
	}
	for _, attempt := range []struct {
		key, source, daemon, body string
	}{
		{keyA, "src-a", "fm-task-1-a", `{"source":"src-a"}`},
		{keyB, "src-b", "fm-task-1-b", `{"source":"src-b"}`},
	} {
		if err := p.EnqueueInSource(ctx, "task-1", attempt.daemon, attempt.daemon, attempt.source, "ws-shared",
			EPFail, []byte(attempt.body), "", attempt.key); err != nil {
			t.Fatalf("EnqueueInSource(%s): %v", attempt.source, err)
		}
	}
	if p.Len() != 2 {
		t.Fatalf("queue depth = %d, want the two attempts", p.Len())
	}
	if files := p.listLocked(); len(files) != 2 {
		t.Fatalf("disk entries = %d, want two distinct queue files", len(files))
	}

	// A restart rebuilds both from their carriers.
	restarted, err := NewPendingReports(dir, WithPendingReportsCarrier(carriers))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := restarted.Restore(ctx); err != nil {
		t.Fatalf("Restore after restart: %v", err)
	}
	if restarted.Len() != 2 || !restarted.Queued("task-1", keyA) || !restarted.Queued("task-1", keyB) {
		t.Fatalf("restored queue = depth %d, queued(%q)=%v, queued(%q)=%v; want both attempts",
			restarted.Len(), keyA, restarted.Queued("task-1", keyA), keyB, restarted.Queued("task-1", keyB))
	}

	// Draining one attempt must not take the other with it, and each replay
	// must stay on its own source.
	sender := &sourceSender{}
	if _, err := restarted.Drain(ctx, sender); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(sender.seen) != 2 {
		t.Fatalf("replays = %v, want one per source", sender.seen)
	}
	seen := map[string]bool{sender.seen[0]: true, sender.seen[1]: true}
	if !seen["src-a/task-1"] || !seen["src-b/task-1"] {
		t.Fatalf("replays = %v, want src-a/task-1 and src-b/task-1", sender.seen)
	}

	// Dropping one slot leaves the other queued.
	if err := restarted.EnqueueInSource(ctx, "task-1", "fm-task-1-a", "fm-task-1-a", "src-a", "ws-shared",
		EPFail, []byte(`{}`), "", keyA); err != nil {
		t.Fatalf("re-enqueue: %v", err)
	}
	if err := restarted.EnqueueInSource(ctx, "task-1", "fm-task-1-b", "fm-task-1-b", "src-b", "ws-shared",
		EPFail, []byte(`{}`), "", keyB); err != nil {
		t.Fatalf("re-enqueue: %v", err)
	}
	restarted.Drop(ctx, "task-1", keyA)
	if restarted.Queued("task-1", keyA) {
		t.Fatal("the dropped attempt is still queued")
	}
	if !restarted.Queued("task-1", keyB) {
		t.Fatal("dropping one source's report took the other source's report with it")
	}
}

// TestSameTaskIDOnTwoSourcesClaimsTwoAttempts pins the claim path's attempt
// identity. The upstream task id is only unique inside one source: the second
// server's claim of the same id must become its own attempt with its own Job,
// credential and daemon identity, never a duplicate of the first (which would
// drop it as already delivered). Before the registry and name face carried the
// source, the second claim resolved to the first entry and was dropped.
func TestSameTaskIDOnTwoSourcesClaimsTwoAttempts(t *testing.T) {
	f := newFixture(t)
	for _, source := range []string{"src-a", "src-b"} {
		if err := f.sched.OnClaimForInSource(context.Background(), source, "ws-1", claimPayload("task-1")); err != nil {
			t.Fatalf("OnClaimForInSource(%s): %v", source, err)
		}
	}

	first, firstOK := f.reg.Get("task-1", partitionKey("src-a", "ws-1"))
	second, secondOK := f.reg.Get("task-1", partitionKey("src-b", "ws-1"))
	// The duplicate counter is read first: it names the branch the defect
	// drives (a bare task-id lookup treats the second source's claim as a
	// redelivery of the first), instead of only its downstream missing row.
	if f.metrics.duplicates != 0 {
		t.Fatalf("duplicate dispatches = %d, want 0: the second source's claim entered the duplicate branch (created jobs = %d, src-a entry = %v, src-b entry = %v)",
			f.metrics.duplicates, len(f.jobs.createdJobs), firstOK, secondOK)
	}
	if !firstOK {
		t.Fatal("src-a's attempt is not registered")
	}
	if !secondOK {
		t.Fatal("src-b's attempt is not registered")
	}
	if first.DaemonID == second.DaemonID {
		t.Fatalf("both attempts share daemon identity %q", first.DaemonID)
	}
	if first.JobName == second.JobName {
		t.Fatalf("both attempts share Job name %q", first.JobName)
	}
	if len(f.jobs.createdJobs) != 2 {
		t.Fatalf("jobs created = %d, want one per source attempt", len(f.jobs.createdJobs))
	}
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatalf("deletedJobs = %v, want no attempt treated as a duplicate's corpse", f.jobs.deletedJobs)
	}
	if len(f.server.forwards) != 0 {
		t.Fatalf("claims reported failures: %+v", f.server.forwards)
	}
}

// TestClaimDefersOnNameHeldByAnotherSourcesJob pins the stale-corpse rule to
// the attempt identity: a Job under this attempt's name that names another
// source is that attempt's object, not this one's corpse, and is never
// deleted. The claim defers so the outer lease retries later.
func TestClaimDefersOnNameHeldByAnotherSourcesJob(t *testing.T) {
	f := newFixture(t)
	// The colliding object sits exactly where this claim renders its Job, but
	// its labels name the other source's attempt of the same task id.
	jobName := jobbuilder.DaemonIDForInScope("task-1", "src-a", "ws-1")
	f.jobs.existingJobs = map[string]*batchv1.Job{jobName: {
		ObjectMeta: metav1.ObjectMeta{
			Name: jobName,
			Labels: map[string]string{
				registry.LabelManagedBy: registry.ManagedByForeman,
				registry.LabelTaskID:    "task-1",
				registry.LabelSourceID:  "src-b",
			},
		},
	}}

	err := f.sched.OnClaimForInSource(context.Background(), "src-a", "ws-1", claimPayload("task-1"))
	if !errors.Is(err, errJobNameHeld) {
		t.Fatalf("OnClaimForInSource err = %v, want errJobNameHeld", err)
	}
	if len(f.jobs.deletedJobs) != 0 {
		t.Fatalf("deletedJobs = %v, want another source's Job left alone", f.jobs.deletedJobs)
	}
	if _, ok := f.reg.Get("task-1", partitionKey("src-a", "ws-1")); ok {
		t.Fatal("deferred claim kept its entry: the lease would be renewed, never re-dispatched")
	}
}
