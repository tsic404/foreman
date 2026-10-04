package scheduler

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/tsic404/foreman/internal/registry"
)

func managedJob(name string) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: "multica-agents",
		Labels:    map[string]string{registry.LabelManagedBy: registry.ManagedByForeman},
	}}
}

func TestJobClientCreateListDelete(t *testing.T) {
	c := NewJobClient(fake.NewSimpleClientset(), "multica-agents")
	ctx := context.Background()

	if err := c.CreateJob(ctx, managedJob("fm-a")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	// An unmanaged Job must not enter the rebuild list.
	foreign := managedJob("foreign")
	delete(foreign.Labels, registry.LabelManagedBy)
	if err := c.CreateJob(ctx, foreign); err != nil {
		t.Fatalf("CreateJob foreign: %v", err)
	}

	jobs, err := c.ListJobs(ctx)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Name != "fm-a" {
		t.Fatalf("ListJobs = %+v", jobs)
	}

	if err := c.DeleteJob(ctx, "fm-a"); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	if err := c.DeleteJob(ctx, "fm-a"); err != nil {
		t.Fatalf("DeleteJob not idempotent: %v", err)
	}

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "fm-a-cred", Namespace: "multica-agents"}}
	if err := c.CreateSecret(ctx, secret); err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}
	if err := c.DeleteSecret(ctx, "fm-a-cred"); err != nil {
		t.Fatalf("DeleteSecret: %v", err)
	}
	if err := c.DeleteSecret(ctx, "fm-a-cred"); err != nil {
		t.Fatalf("DeleteSecret not idempotent: %v", err)
	}
}

func TestWatchJobsDeliversInformerEvents(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	c := NewJobClient(clientset, "multica-agents")

	// The fake clientset gives no sync marker (the reflector's watch-list
	// bookmark never arrives), so gate on the watch actually being
	// established before mutating objects.
	watchStarted := make(chan struct{})
	clientset.PrependWatchReactor("jobs", func(k8stesting.Action) (bool, watch.Interface, error) {
		select {
		case <-watchStarted:
		default:
			close(watchStarted)
		}
		return false, nil, nil
	})

	events := make(chan JobEvent, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan error, 1)
	go func() { watchDone <- c.WatchJobs(ctx, func(e JobEvent) { events <- e }) }()

	select {
	case <-watchStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("informer never started watching")
	}

	if err := c.CreateJob(ctx, managedJob("fm-watch")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	select {
	case e := <-events:
		if e.Kind != JobAdded || e.Job.Name != "fm-watch" {
			t.Fatalf("event = %+v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no add event")
	}

	if err := c.DeleteJob(ctx, "fm-watch"); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	select {
	case e := <-events:
		if e.Kind != JobDeleted || e.Job.Name != "fm-watch" {
			t.Fatalf("event = %+v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no delete event")
	}

	cancel()
	if err := <-watchDone; err != nil {
		t.Fatalf("WatchJobs = %v", err)
	}
}
