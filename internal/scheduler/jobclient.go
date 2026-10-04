package scheduler

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/tsic404/foreman/internal/registry"
)

// JobEventKind classifies a watched Job change.
type JobEventKind string

// Watch event kinds delivered to a JobEventHandler.
const (
	JobAdded   JobEventKind = "added"
	JobUpdated JobEventKind = "updated"
	JobDeleted JobEventKind = "deleted"
)

// JobEvent is one observed change of a foreman Job.
type JobEvent struct {
	Kind JobEventKind
	Job  *batchv1.Job
}

// JobEventHandler consumes Job watch events; recovery wires OnJobGone to the
// deleted kind.
type JobEventHandler func(JobEvent)

// JobClient is the scheduler's K8s object access (task-mapping 内部结构).
// Deletes are idempotent: a missing object counts as success.
type JobClient interface {
	CreateJob(ctx context.Context, job *batchv1.Job) error
	DeleteJob(ctx context.Context, name string) error
	CreateSecret(ctx context.Context, secret *corev1.Secret) error
	DeleteSecret(ctx context.Context, name string) error
	// ListJobs returns the foreman-managed Jobs in the configured namespace.
	ListJobs(ctx context.Context) ([]batchv1.Job, error)
	// WatchJobs streams Job changes via a client-go informer until ctx ends.
	WatchJobs(ctx context.Context, handler JobEventHandler) error
}

// k8sJobClient implements JobClient against the batch/v1 API.
type k8sJobClient struct {
	client    kubernetes.Interface
	namespace string
}

// NewJobClient wraps a clientset for the Job namespace (RBAC: contract §1.3).
func NewJobClient(client kubernetes.Interface, namespace string) JobClient {
	return &k8sJobClient{client: client, namespace: namespace}
}

func (c *k8sJobClient) CreateJob(ctx context.Context, job *batchv1.Job) error {
	_, err := c.client.BatchV1().Jobs(c.namespace).Create(ctx, job, metav1.CreateOptions{})
	return err
}

func (c *k8sJobClient) DeleteJob(ctx context.Context, name string) error {
	err := c.client.BatchV1().Jobs(c.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (c *k8sJobClient) CreateSecret(ctx context.Context, secret *corev1.Secret) error {
	_, err := c.client.CoreV1().Secrets(c.namespace).Create(ctx, secret, metav1.CreateOptions{})
	return err
}

func (c *k8sJobClient) DeleteSecret(ctx context.Context, name string) error {
	err := c.client.CoreV1().Secrets(c.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (c *k8sJobClient) ListJobs(ctx context.Context) ([]batchv1.Job, error) {
	list, err := c.client.BatchV1().Jobs(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: managedJobSelector(),
	})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// WatchJobs runs a shared informer scoped to the foreman Jobs and forwards
// every add/update/delete to handler until ctx is cancelled. The initial
// list arrives as JobAdded events; there is no separate synced gate (the
// reflector's watch-list sync marker never arrives from some API clients,
// and consumers reconcile periodically anyway).
func (c *k8sJobClient) WatchJobs(ctx context.Context, handler JobEventHandler) error {
	factory := informers.NewSharedInformerFactoryWithOptions(
		c.client,
		0, // no periodic resync: the stream plus the initial list suffices
		informers.WithNamespace(c.namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = managedJobSelector()
		}),
	)
	informer := factory.Batch().V1().Jobs().Informer()
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if job, ok := obj.(*batchv1.Job); ok {
				handler(JobEvent{Kind: JobAdded, Job: job})
			}
		},
		UpdateFunc: func(_, newObj any) {
			if job, ok := newObj.(*batchv1.Job); ok {
				handler(JobEvent{Kind: JobUpdated, Job: job})
			}
		},
		DeleteFunc: func(obj any) {
			if job, ok := obj.(*batchv1.Job); ok {
				handler(JobEvent{Kind: JobDeleted, Job: job})
				return
			}
			// A missed delete arrives as the tombstone wrapper.
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				if job, ok := tombstone.Obj.(*batchv1.Job); ok {
					handler(JobEvent{Kind: JobDeleted, Job: job})
				}
			}
		},
	}); err != nil {
		return fmt.Errorf("register job informer handler: %w", err)
	}
	factory.Start(ctx.Done())
	<-ctx.Done()
	return nil
}

// managedJobSelector selects exactly the Jobs Foreman owns (contract §3.2).
func managedJobSelector() string {
	return labels.Set{registry.LabelManagedBy: registry.ManagedByForeman}.String()
}
