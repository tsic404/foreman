package recovery

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestIndexPodsGroupsByOwningJob(t *testing.T) {
	pods := []corev1.Pod{
		podFor("fm-a", nil),
		podFor("fm-b", nil),
		podFor("fm-a", nil),
		{ObjectMeta: metav1.ObjectMeta{Name: "stray"}}, // no job-name label
	}
	byJob := indexPods(pods)
	if len(byJob["fm-a"]) != 2 || len(byJob["fm-b"]) != 1 {
		t.Fatalf("indexPods = %d/%d pods, want 2/1", len(byJob["fm-a"]), len(byJob["fm-b"]))
	}
	if _, ok := byJob[""]; ok {
		t.Fatal("a pod without the job-name label must not be indexed")
	}
}

func TestJobEndReason(t *testing.T) {
	tests := []struct {
		name   string
		job    *batchv1.Job
		pods   []corev1.Pod
		reason string
		ended  bool
	}{
		{name: "no job", job: nil, ended: false},
		{name: "still running", job: runningJob("fm-a"), ended: false},
		{
			name:   "failed container",
			job:    failedJob("fm-a", "BackoffLimitExceeded"),
			reason: scheduler.FailureReasonJobFailed,
			ended:  true,
		},
		{
			name:   "deadline kill",
			job:    failedJob("fm-a", "DeadlineExceeded"),
			reason: scheduler.FailureReasonJobDeadline,
			ended:  true,
		},
		{
			name: "succeeded container",
			job: func() *batchv1.Job {
				job := runningJob("fm-a")
				job.Status.Succeeded = 1
				return job
			}(),
			reason: scheduler.FailureReasonJobFailed,
			ended:  true,
		},
		{
			name:   "evicted pod",
			job:    failedJob("fm-a", "BackoffLimitExceeded"),
			pods:   []corev1.Pod{podFor("fm-a", func(p *corev1.Pod) { p.Status.Reason = "Evicted" })},
			reason: scheduler.FailureReasonJobEvicted,
			ended:  true,
		},
		{
			name:   "lost node",
			job:    failedJob("fm-a", "BackoffLimitExceeded"),
			pods:   []corev1.Pod{podFor("fm-a", func(p *corev1.Pod) { p.Status.Phase = corev1.PodUnknown })},
			reason: scheduler.FailureReasonJobEvicted,
			ended:  true,
		},
		{
			name: "deadline at the container",
			job:  failedJob("fm-a", "BackoffLimitExceeded"),
			pods: []corev1.Pod{podFor("fm-a", func(p *corev1.Pod) {
				p.Status.ContainerStatuses = []corev1.ContainerStatus{{
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "DeadlineExceeded"}},
				}}
			})},
			reason: scheduler.FailureReasonJobDeadline,
			ended:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, ended := jobEndReason(tt.job, tt.pods)
			if ended != tt.ended || reason != tt.reason {
				t.Fatalf("jobEndReason = (%q, %v), want (%q, %v)", reason, ended, tt.reason, tt.ended)
			}
		})
	}
}

func TestK8sObjectsReadsJobsAndForemanPods(t *testing.T) {
	client := fake.NewSimpleClientset()
	objs := NewK8sObjects(client, "multica-agents")
	ctx := context.Background()

	missing, err := objs.GetJob(ctx, "fm-gone")
	if err != nil || missing != nil {
		t.Fatalf("GetJob(missing) = (%v, %v), want (nil, nil)", missing, err)
	}

	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      "fm-a",
		Namespace: "multica-agents",
		Labels:    map[string]string{registry.LabelTaskID: "a"},
	}}
	if _, err := client.BatchV1().Jobs("multica-agents").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	got, err := objs.GetJob(ctx, "fm-a")
	if err != nil || got == nil || got.Name != "fm-a" {
		t.Fatalf("GetJob(fm-a) = (%v, %v)", got, err)
	}

	foremanPod := podFor("fm-a", nil)
	foremanPod.Namespace = "multica-agents"
	foremanPod.Labels[registry.LabelTaskID] = "a"
	foreign := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "multica-agents"}}
	for _, pod := range []*corev1.Pod{&foremanPod, foreign} {
		if _, err := client.CoreV1().Pods("multica-agents").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod: %v", err)
		}
	}
	pods, err := objs.ListPods(ctx)
	if err != nil {
		t.Fatalf("ListPods: %v", err)
	}
	if len(pods) != 1 || pods[0].Name != foremanPod.Name {
		t.Fatalf("ListPods = %+v, want only the foreman job pod", pods)
	}
}

func TestK8sObjectsWatchNotifiesOnJobAndPodChanges(t *testing.T) {
	client := fake.NewSimpleClientset()
	objs := NewK8sObjects(client, "multica-agents")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	notified := make(chan struct{}, 8)
	go func() {
		_ = objs.Watch(ctx, func() {
			select {
			case notified <- struct{}{}:
			default:
			}
		})
	}()

	managed := func(name string) *batchv1.Job {
		return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "multica-agents",
			Labels:    map[string]string{registry.LabelTaskID: name},
		}}
	}
	if _, err := client.BatchV1().Jobs("multica-agents").Create(ctx, managed("fm-first"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	waitNotify(t, notified, "the initial Job list")

	if _, err := client.BatchV1().Jobs("multica-agents").Create(ctx, managed("fm-second"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	waitNotify(t, notified, "a Job change")

	pod := podFor("fm-second", func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed })
	pod.Namespace = "multica-agents"
	pod.Labels[registry.LabelTaskID] = "second"
	if _, err := client.CoreV1().Pods("multica-agents").Create(ctx, &pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	waitNotify(t, notified, "an ended Pod")
}

func waitNotify(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("no notification for %s", what)
	}
}
