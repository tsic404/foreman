package recovery

import (
	"context"
	"log/slog"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// podJobNameLabel is the Job controller's back-reference carried by every
// Job pod; the owning foreman Job is derived from it.
const podJobNameLabel = "job-name"

// ObjectClient is the recovery module's K8s read surface for the object
// facts of the 判据模型 (failure-handling.md): whether the container ended
// and why (依赖: client-go Job/Pod 读).
type ObjectClient interface {
	// GetJob returns the foreman Job, or nil when it is gone.
	GetJob(ctx context.Context, name string) (*batchv1.Job, error)
	// ListPods returns the pods of every foreman Job in the Job namespace.
	ListPods(ctx context.Context) ([]corev1.Pod, error)
}

// indexPods groups pods by their owning Job name so a round reads the pod
// facts of each entry without an API call per entry.
func indexPods(pods []corev1.Pod) map[string][]corev1.Pod {
	byJob := make(map[string][]corev1.Pod, len(pods))
	for _, pod := range pods {
		jobName := pod.Labels[podJobNameLabel]
		if jobName == "" {
			continue
		}
		byJob[jobName] = append(byJob[jobName], pod)
	}
	return byJob
}

// jobEndReason applies the Job/Pod facts of 判据模型: the Job object says
// whether the container ended, the pod refines why. A failed or succeeded
// Job both mean the container is gone — the daemon (its main process) cannot
// report anything after that, so the task must converge. Deadline kills
// (#5) and node losses (#11) are named from the pod; anything else is
// job_failed (#3).
//
// The pod evidence is read first: a lost node shows up there before the Job
// controller marks the Job failed (场景 #11).
func jobEndReason(job *batchv1.Job, pods []corev1.Pod) (string, bool) {
	if job == nil {
		return "", false
	}
	for i := range pods {
		if podTerminatedByDeadline(&pods[i]) {
			return scheduler.FailureReasonJobDeadline, true
		}
		if podLost(&pods[i]) {
			return scheduler.FailureReasonJobEvicted, true
		}
	}
	return endReasonFromJob(job)
}

// endReasonFromJob reads the Job's own end evidence: the Failed condition
// (the k8s Job controller sets reason DeadlineExceeded for
// activeDeadlineSeconds kills) or a completed/failed pod count.
func endReasonFromJob(job *batchv1.Job) (string, bool) {
	for _, cond := range job.Status.Conditions {
		if cond.Type != batchv1.JobFailed || cond.Status != corev1.ConditionTrue {
			continue
		}
		if cond.Reason == "DeadlineExceeded" {
			return scheduler.FailureReasonJobDeadline, true
		}
		return scheduler.FailureReasonJobFailed, true
	}
	if job.Status.Failed > 0 || job.Status.Succeeded > 0 {
		return scheduler.FailureReasonJobFailed, true
	}
	return "", false
}

// podTerminatedByDeadline reports a pod killed by activeDeadlineSeconds.
func podTerminatedByDeadline(pod *corev1.Pod) bool {
	if pod.Status.Reason == "DeadlineExceeded" {
		return true
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.Reason == "DeadlineExceeded" {
			return true
		}
	}
	return false
}

// podLost reports a pod whose node went away — evicted, or its status no
// longer observable (failure-handling 场景 #11).
func podLost(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodUnknown {
		return true
	}
	switch pod.Status.Reason {
	case "Evicted", "NodeLost", "Shutdown":
		return true
	}
	return false
}

// K8sObjects is the client-go implementation of ObjectClient and Watcher in
// the Job namespace (RBAC: contract §1.3 — Job/Pod reads only).
type K8sObjects struct {
	client    kubernetes.Interface
	namespace string
	log       *slog.Logger
}

// NewK8sObjects wraps a clientset for the Job namespace.
func NewK8sObjects(client kubernetes.Interface, namespace string) *K8sObjects {
	return &K8sObjects{
		client:    client,
		namespace: namespace,
		log:       slog.Default().With("component", "job-watcher"),
	}
}

// GetJob returns the foreman Job; a deleted Job is (nil, nil) — the
// reconcile step that owns the missing-Job case.
func (o *K8sObjects) GetJob(ctx context.Context, name string) (*batchv1.Job, error) {
	job, err := o.client.BatchV1().Jobs(o.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return job, nil
}

// ListPods returns the pods of the foreman Jobs (label
// foreman.tsic.top/task-id, contract §3.2).
func (o *K8sObjects) ListPods(ctx context.Context) ([]corev1.Pod, error) {
	list, err := o.client.CoreV1().Pods(o.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: registry.LabelTaskID,
	})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// Watch streams Job/Pod changes until ctx ends and calls notify for every
// change that can move a task forward: any Job change, and the pod
// transitions from running to ended or gone. It is an accelerator only —
// the reconciler reads the object state itself, so a missed event costs
// latency, never correctness.
func (o *K8sObjects) Watch(ctx context.Context, notify func()) error {
	factory := informers.NewSharedInformerFactoryWithOptions(
		o.client,
		0, // no periodic resync: the stream plus the initial list suffices
		informers.WithNamespace(o.namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			// Both foreman Jobs and their pods carry the task-id label.
			opts.LabelSelector = registry.LabelTaskID
		}),
	)
	jobs := factory.Batch().V1().Jobs().Informer()
	if _, err := jobs.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { notify() },
		UpdateFunc: func(_, _ any) { notify() },
		DeleteFunc: func(any) { notify() },
	}); err != nil {
		return err
	}
	pods := factory.Core().V1().Pods().Informer()
	if _, err := pods.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { notifyPod(obj, notify) },
		UpdateFunc: func(_, newObj any) { notifyPod(newObj, notify) },
		DeleteFunc: func(obj any) { notifyPod(obj, notify) },
	}); err != nil {
		return err
	}
	factory.Start(ctx.Done())
	<-ctx.Done()
	return nil
}

// notifyPod filters the chatty pod stream down to the transitions the
// reconciler cares about.
func notifyPod(obj any, notify func()) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			pod, ok = tombstone.Obj.(*corev1.Pod)
		}
		if !ok {
			return
		}
	}
	if pod.DeletionTimestamp != nil || podTerminatedByDeadline(pod) || podLost(pod) {
		notify()
		return
	}
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		notify()
	}
}
