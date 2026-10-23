package scheduler

import (
	"context"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/tsic404/foreman/internal/registry"
)

// JobClient is the scheduler's K8s object access.
// Deletes are idempotent: a missing object counts as success.
type JobClient interface {
	// CreateJob creates the Job and returns the object the apiserver
	// accepted. Its metadata.creationTimestamp is the instance-filter basis
	// of the pod evidence (§4.1) — never substitute a local clock
	// reading, or an apiserver clock that lags would filter out this Job's
	// own pod and read the empty set as "terminated".
	CreateJob(ctx context.Context, job *batchv1.Job) (*batchv1.Job, error)
	DeleteJob(ctx context.Context, name string) error
	// GetJob returns the Job, or nil when it is gone (the recovery
	// module's ObjectClient uses the same convention).
	GetJob(ctx context.Context, name string) (*batchv1.Job, error)
	CreateSecret(ctx context.Context, secret *corev1.Secret) error
	DeleteSecret(ctx context.Context, name string) error
	// ListJobs returns the foreman-managed Jobs in the configured namespace.
	ListJobs(ctx context.Context) ([]batchv1.Job, error)
	// ListJobsByTask returns the foreman-managed Jobs carrying one task's
	// task-id label in one workspace: the per-task presence face the orphan
	// sweep re-evaluates under the task's write lock, where the round's own
	// list would already be stale (§6.5).
	ListJobsByTask(ctx context.Context, workspaceID, taskID string) ([]batchv1.Job, error)
	// ListSecrets returns the foreman-managed Secrets in the configured
	// namespace — both the credential domain and the pending-report carrier
	// domain. It is the list face of the orphan/carrier sweeps and of the
	// queue rebuild (§3, §4.3, §6.5).
	ListSecrets(ctx context.Context) ([]corev1.Secret, error)
	// ListPods returns the pods matching labelSelector in the configured
	// namespace — the one value surface behind the termination evidence.
	ListPods(ctx context.Context, labelSelector string) ([]corev1.Pod, error)
}

// k8sJobClient implements JobClient against the batch/v1 API.
type k8sJobClient struct {
	client    kubernetes.Interface
	namespace string
}

// NewJobClient wraps a clientset for the Job namespace (RBAC: §3).
func NewJobClient(client kubernetes.Interface, namespace string) JobClient {
	return &k8sJobClient{client: client, namespace: namespace}
}

func (c *k8sJobClient) CreateJob(ctx context.Context, job *batchv1.Job) (*batchv1.Job, error) {
	return c.client.BatchV1().Jobs(c.namespace).Create(ctx, job, metav1.CreateOptions{})
}

// jobDeleteOptions deletes the Job with the Background propagation policy.
// The policy is explicit, never left to the Job strategy: batch/v1 defaults
// to OrphanDependents (pkg/registry/batch/job/strategy.go ·
// DefaultGarbageCollectionPolicy, kept for compatibility), so a bare Delete
// would keep the pods alive without a SIGTERM — the termination evidence
// would stay false forever, every reclaim would fall to the drain upper
// bound, and the pods would be left with no reclaim path (ADR-017, §3).
// Background removes the Job object at once and lets the GC
// delete the pods asynchronously (SIGTERM → terminationGracePeriodSeconds),
// which is why "Job object gone" is not "Job terminated".
func jobDeleteOptions() metav1.DeleteOptions {
	return metav1.DeleteOptions{PropagationPolicy: new(metav1.DeletePropagationBackground)}
}

func (c *k8sJobClient) DeleteJob(ctx context.Context, name string) error {
	err := c.client.BatchV1().Jobs(c.namespace).Delete(ctx, name, jobDeleteOptions())
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// GetJob returns the Job, or nil when it is gone.
func (c *k8sJobClient) GetJob(ctx context.Context, name string) (*batchv1.Job, error) {
	job, err := c.client.BatchV1().Jobs(c.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return job, nil
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

// ListJobsByTask selects by the label pair that identifies a task's Jobs
// (§4.3), never by a name composed from the task id. The workspace term is
// filtered here rather than in the selector: an empty value is a partition of
// its own (objects without the label), not a wildcard (§6.5 按 workspace
// 分区).
func (c *k8sJobClient) ListJobsByTask(ctx context.Context, workspaceID, taskID string) ([]batchv1.Job, error) {
	list, err := c.client.BatchV1().Jobs(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{
			registry.LabelManagedBy: registry.ManagedByForeman,
			registry.LabelTaskID:    taskID,
		}.String(),
	})
	if err != nil {
		return nil, err
	}
	out := make([]batchv1.Job, 0, len(list.Items))
	for i := range list.Items {
		if list.Items[i].Labels[registry.LabelWorkspaceID] == workspaceID {
			out = append(out, list.Items[i])
		}
	}
	return out, nil
}

func (c *k8sJobClient) ListSecrets(ctx context.Context) ([]corev1.Secret, error) {
	list, err := c.client.CoreV1().Secrets(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: managedJobSelector(),
	})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (c *k8sJobClient) ListPods(ctx context.Context, labelSelector string) ([]corev1.Pod, error) {
	list, err := c.client.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// managedJobSelector selects exactly the Jobs Foreman owns (§4.3).
func managedJobSelector() string {
	return labels.Set{registry.LabelManagedBy: registry.ManagedByForeman}.String()
}
