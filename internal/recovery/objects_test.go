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

func TestInstancePodsDropsPredecessorPods(t *testing.T) {
	since := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	pods := []corev1.Pod{
		podFor("fm-a", func(p *corev1.Pod) {
			p.CreationTimestamp = metav1.NewTime(since.Add(-time.Second))
		}),
		podFor("fm-a", func(p *corev1.Pod) { p.CreationTimestamp = metav1.NewTime(since) }),
		podFor("fm-a", func(p *corev1.Pod) {
			p.CreationTimestamp = metav1.NewTime(since.Add(time.Second))
		}),
	}
	got := instancePods(pods, since)
	if len(got) != 2 {
		t.Fatalf("instancePods = %d pods, want the 2 created at or after the Job", len(got))
	}
	if got[0].CreationTimestamp.Time.Before(since) {
		t.Fatalf("instancePods kept a pod created before the Job: %v", got[0].CreationTimestamp)
	}
}

func TestPodNodeNamePicksTheNewestScheduledPod(t *testing.T) {
	base := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	pods := []corev1.Pod{
		podFor("fm-a", func(p *corev1.Pod) {
			p.CreationTimestamp = metav1.NewTime(base)
			p.Spec.NodeName = "company-01"
		}),
		podFor("fm-a", func(p *corev1.Pod) {
			p.CreationTimestamp = metav1.NewTime(base.Add(time.Minute))
			p.Spec.NodeName = "company-02"
		}),
		// The newest pod of the rebuild has no placement yet: it must not
		// blank the node the predecessor still holds.
		podFor("fm-a", func(p *corev1.Pod) {
			p.CreationTimestamp = metav1.NewTime(base.Add(2 * time.Minute))
		}),
	}
	if got := podNodeName(pods); got != "company-02" {
		t.Fatalf("podNodeName = %q, want company-02 from the newest scheduled pod", got)
	}
	if got := podNodeName([]corev1.Pod{podFor("fm-a", nil)}); got != "" {
		t.Fatalf("podNodeName = %q, want empty while the pod is pending", got)
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
		{
			// The kill's condition surface stops at FailureTarget: the
			// controller publishes it before the pods terminate, and this
			// instance's pod is already gone, so neither the Failed condition
			// nor the pod fallback carries the fact.
			name: "failure target only, pod gone",
			job: &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "fm-a"},
				Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
					Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded",
				}}},
			},
			reason: scheduler.FailureReasonJobDeadline,
			ended:  true,
		},
		{
			// The fixed order (§4.4, ADR-031): the Job's deadline condition is
			// read first, so a deadline-killed Job whose pod also lost its
			// node is still the platform's deadline, never an eviction.
			name:   "job condition wins over a lost pod",
			job:    failedJob("fm-a", "DeadlineExceeded"),
			pods:   []corev1.Pod{podFor("fm-a", func(p *corev1.Pod) { p.Status.Phase = corev1.PodUnknown })},
			reason: scheduler.FailureReasonJobDeadline,
			ended:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, _, ended := jobEndReason(tt.job, tt.pods, time.Time{})
			if ended != tt.ended || reason != tt.reason {
				t.Fatalf("jobEndReason = (%q, %v), want (%q, %v)", reason, ended, tt.reason, tt.ended)
			}
		})
	}
}

// TestJobEndDetailNamesTheTerminatingContainer pins what the synthesized fail
// report carries: the four start-up shapes the platform could not tell apart
// before (init, prepare, agent exit, OOM kill), the eviction fallback that has
// no container status, the Job controller's own condition, and the empty
// detail that must leave the fixed sentence alone.
func TestJobEndDetailNamesTheTerminatingContainer(t *testing.T) {
	finished := time.Date(2026, 11, 5, 13, 14, 4, 0, time.UTC)
	placed := func(p *corev1.Pod) {
		p.Spec.NodeName = "company-01"
		p.CreationTimestamp = metav1.NewTime(finished.Add(-time.Minute))
	}
	endedAt := func(name string, exit int32, reason string) corev1.ContainerStatus {
		return corev1.ContainerStatus{
			Name: name,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: exit, Reason: reason, FinishedAt: metav1.NewTime(finished),
			}},
		}
	}

	tests := []struct {
		name   string
		job    *batchv1.Job
		pods   []corev1.Pod
		reason string
		detail string
	}{
		{
			name: "init container fails",
			job:  failedJob("fm-a", "BackoffLimitExceeded"),
			pods: []corev1.Pod{podFor("fm-a", func(p *corev1.Pod) {
				placed(p)
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{endedAt("init", 2, "Error")}
			})},
			reason: scheduler.FailureReasonJobFailed,
			detail: "container=init exit=2 reason=Error node=company-01 pod=fm-a-x finished=2026-11-05T13:14:04Z job_reason=BackoffLimitExceeded",
		},
		{
			name: "prepare fails after the init step succeeded",
			job:  failedJob("fm-a", "BackoffLimitExceeded"),
			pods: []corev1.Pod{podFor("fm-a", func(p *corev1.Pod) {
				placed(p)
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{
					endedAt("init", 0, "Completed"),
					endedAt("prepare", 3, "Error"),
				}
			})},
			reason: scheduler.FailureReasonJobFailed,
			detail: "container=prepare exit=3 reason=Error others=1 node=company-01 pod=fm-a-x finished=2026-11-05T13:14:04Z job_reason=BackoffLimitExceeded",
		},
		{
			name: "agent container exits non-zero",
			job:  failedJob("fm-a", "BackoffLimitExceeded"),
			pods: []corev1.Pod{podFor("fm-a", func(p *corev1.Pod) {
				placed(p)
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{endedAt("prepare", 0, "Completed")}
				p.Status.ContainerStatuses = []corev1.ContainerStatus{endedAt("agent", 1, "Error")}
			})},
			reason: scheduler.FailureReasonJobFailed,
			detail: "container=agent exit=1 reason=Error others=1 node=company-01 pod=fm-a-x finished=2026-11-05T13:14:04Z job_reason=BackoffLimitExceeded",
		},
		{
			name: "agent container is OOM-killed",
			job:  failedJob("fm-a", "BackoffLimitExceeded"),
			pods: []corev1.Pod{podFor("fm-a", func(p *corev1.Pod) {
				placed(p)
				p.Status.ContainerStatuses = []corev1.ContainerStatus{endedAt("agent", 137, "OOMKilled")}
			})},
			reason: scheduler.FailureReasonJobFailed,
			detail: "container=agent exit=137 reason=OOMKilled node=company-01 pod=fm-a-x finished=2026-11-05T13:14:04Z job_reason=BackoffLimitExceeded",
		},
		{
			name: "evicted pod has no container status",
			job:  failedJob("fm-a", "BackoffLimitExceeded"),
			pods: []corev1.Pod{podFor("fm-a", func(p *corev1.Pod) {
				placed(p)
				p.Status.Phase = corev1.PodFailed
				p.Status.Reason = "Evicted"
			})},
			reason: scheduler.FailureReasonJobEvicted,
			detail: "node=company-01 pod=fm-a-x job_reason=BackoffLimitExceeded",
		},
		{
			name: "platform deadline kill",
			job:  failedJob("fm-a", "DeadlineExceeded"),
			pods: []corev1.Pod{podFor("fm-a", func(p *corev1.Pod) {
				placed(p)
				p.Status.ContainerStatuses = []corev1.ContainerStatus{endedAt("agent", 143, "DeadlineExceeded")}
			})},
			reason: scheduler.FailureReasonJobDeadline,
			detail: `container=agent exit=143 reason=DeadlineExceeded node=company-01 pod=fm-a-x finished=2026-11-05T13:14:04Z job_reason=DeadlineExceeded`,
		},
		{
			name: "job condition is carried beside the pod facts",
			job: func() *batchv1.Job {
				job := failedJob("fm-a", "BackoffLimitExceeded")
				job.Status.Conditions[0].Message = "Job has reached the specified backoff limit"
				return job
			}(),
			pods: []corev1.Pod{podFor("fm-a", func(p *corev1.Pod) {
				placed(p)
				p.Status.ContainerStatuses = []corev1.ContainerStatus{endedAt("agent", 1, "Error")}
			})},
			reason: scheduler.FailureReasonJobFailed,
			detail: `container=agent exit=1 reason=Error node=company-01 pod=fm-a-x finished=2026-11-05T13:14:04Z ` +
				`job_reason=BackoffLimitExceeded job_message="Job has reached the specified backoff limit"`,
		},
		{
			// The Job object alone carries the end (no condition, no pod):
			// the detail is empty and the report keeps its fixed sentence.
			name:   "no pod facts",
			job:    &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "fm-a"}, Status: batchv1.JobStatus{Failed: 1}},
			reason: scheduler.FailureReasonJobFailed,
			detail: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, detail, ended := jobEndReason(tt.job, tt.pods, time.Time{})
			if !ended || reason != tt.reason {
				t.Fatalf("jobEndReason = (%q, %v), want (%q, true)", reason, ended, tt.reason)
			}
			if detail != tt.detail {
				t.Fatalf("detail = %q, want %q", detail, tt.detail)
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
