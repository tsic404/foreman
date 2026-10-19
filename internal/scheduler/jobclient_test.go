package scheduler

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

	if _, err := c.CreateJob(ctx, managedJob("fm-a")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	// An unmanaged Job must not enter the rebuild list.
	foreign := managedJob("foreign")
	delete(foreign.Labels, registry.LabelManagedBy)
	if _, err := c.CreateJob(ctx, foreign); err != nil {
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

// A Job delete must cascade to its pods: the Job strategy's default
// propagation is Orphan, which drops the Job object and strips the pods'
// ownerReferences, so they outlive ttlSecondsAfterFinished (measured on k3s
// v1.32.5: the pod still Running, owners=0, with no Job left). The cascade
// is Background (ADR-017): the Job object goes at once and the GC deletes
// the pods asynchronously (SIGTERM → terminationGracePeriodSeconds), which
// is why "Job object gone" is not "Job terminated". The fake clientset runs
// no garbage collector, so the delete request itself is what gets pinned
// here.
func TestDeleteJobRequestsBackgroundCascade(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	c := NewJobClient(clientset, "multica-agents")
	ctx := context.Background()

	if _, err := c.CreateJob(ctx, managedJob("fm-a")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := c.DeleteJob(ctx, "fm-a"); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}

	var policies []string
	for _, action := range clientset.Actions() {
		del, ok := action.(k8stesting.DeleteAction)
		if !ok || action.GetResource().Resource != "jobs" {
			continue
		}
		if p := del.GetDeleteOptions().PropagationPolicy; p != nil {
			policies = append(policies, string(*p))
		} else {
			policies = append(policies, "<nil>")
		}
	}
	if len(policies) != 1 || policies[0] != string(metav1.DeletePropagationBackground) {
		t.Fatalf("Job delete propagation policies = %v, want [Background]", policies)
	}
}

// CreateJob returns the object the apiserver holds: its creationTimestamp is
// the instance basis the pod evidence filters on (§4.1), so it must
// come back from the API and not from the caller's local copy.
func TestCreateJobReturnsTheAcceptedObject(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	// The fake tracker stamps no creationTimestamp, so the reactor supplies
	// the apiserver-side value the caller must read back from the response.
	stamp := time.Now().UTC().Truncate(time.Second)
	clientset.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job)
		job.CreationTimestamp = metav1.NewTime(stamp)
		return false, nil, nil
	})
	c := NewJobClient(clientset, "multica-agents")

	created, err := c.CreateJob(context.Background(), managedJob("fm-created"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if created == nil || created.Name != "fm-created" {
		t.Fatalf("CreateJob = %+v, want the accepted Job", created)
	}
	if !created.CreationTimestamp.Time.Equal(stamp) {
		t.Errorf("creationTimestamp = %s, want the apiserver's %s", created.CreationTimestamp.Time, stamp)
	}
}

// ListSecrets and ListPods are the sweep's read faces: both are scoped to the
// foreman-managed objects of the configured namespace.
func TestJobClientListSecretsAndPods(t *testing.T) {
	managed := map[string]string{registry.LabelManagedBy: registry.ManagedByForeman}
	clientset := fake.NewSimpleClientset(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "fm-a-cred", Namespace: "multica-agents", Labels: managed}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "foreign-cred", Namespace: "multica-agents"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "fm-a-pod", Namespace: "multica-agents", Labels: map[string]string{"job-name": "fm-a"}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "fm-b-pod", Namespace: "multica-agents", Labels: map[string]string{"job-name": "fm-b"}}},
	)
	c := NewJobClient(clientset, "multica-agents")
	ctx := context.Background()

	secrets, err := c.ListSecrets(ctx)
	if err != nil {
		t.Fatalf("ListSecrets: %v", err)
	}
	if len(secrets) != 1 || secrets[0].Name != "fm-a-cred" {
		t.Fatalf("ListSecrets = %+v, want only the foreman-managed Secret", secrets)
	}

	pods, err := c.ListPods(ctx, jobNameSelector("fm-a"))
	if err != nil {
		t.Fatalf("ListPods: %v", err)
	}
	if len(pods) != 1 || pods[0].Name != "fm-a-pod" {
		t.Fatalf("ListPods = %+v, want only fm-a's pod", pods)
	}
}
