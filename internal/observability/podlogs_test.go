package observability

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	cgtesting "k8s.io/client-go/testing"
)

func podWithJobLabel(name, jobName string, created time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "multica-agents",
			Labels:            map[string]string{"batch.kubernetes.io/job-name": jobName},
			CreationTimestamp: metav1.NewTime(created),
		},
	}
}

func TestK8sPodLogReaderReadsLogs(t *testing.T) {
	pod := podWithJobLabel("fm-t-1-a", "fm-t-1", time.Now())
	client := fake.NewSimpleClientset(pod)

	var gotTail int64
	client.Fake.PrependReactor("get", "pods/log",
		func(action cgtesting.Action) (bool, runtime.Object, error) {
			ga, ok := action.(cgtesting.GenericAction)
			if !ok {
				t.Fatalf("action = %T, want GenericAction", action)
			}
			opts, ok := ga.GetValue().(*corev1.PodLogOptions)
			if !ok {
				t.Fatalf("value = %T, want *PodLogOptions", ga.GetValue())
			}
			if opts.TailLines == nil {
				t.Fatal("TailLines not propagated")
			}
			gotTail = *opts.TailLines
			return true, &runtime.Unknown{Raw: []byte("job logs here")}, nil
		})

	r := NewK8sPodLogReader(client)
	data, err := r.ReadLogs(context.Background(), "multica-agents", "fm-t-1", 50)
	if err != nil {
		t.Fatalf("ReadLogs: %v", err)
	}
	if string(data) != "job logs here" {
		t.Fatalf("logs = %q", data)
	}
	if gotTail != 50 {
		t.Fatalf("tail = %d, want 50", gotTail)
	}
}

// TestNewestPod covers the retried-Job case: several pods share the Job
// label; the live attempt is the newest one.
func TestNewestPod(t *testing.T) {
	now := time.Now()
	pods := []corev1.Pod{
		*podWithJobLabel("fm-t-1-older", "fm-t-1", now.Add(-time.Hour)),
		*podWithJobLabel("fm-t-1-retry", "fm-t-1", now),
		*podWithJobLabel("fm-t-1-oldest", "fm-t-1", now.Add(-2*time.Hour)),
	}
	if got := newestPod(pods); got.Name != "fm-t-1-retry" {
		t.Fatalf("newestPod = %s, want fm-t-1-retry", got.Name)
	}
}

func TestK8sPodLogReaderNoPods(t *testing.T) {
	r := NewK8sPodLogReader(fake.NewSimpleClientset())
	_, err := r.ReadLogs(context.Background(), "multica-agents", "fm-gone", 200)
	if !errors.Is(err, ErrJobLogsUnavailable) {
		t.Fatalf("err = %v, want ErrJobLogsUnavailable", err)
	}
}

func TestK8sPodLogReaderStreamError(t *testing.T) {
	client := fake.NewSimpleClientset(podWithJobLabel("fm-t-1-a", "fm-t-1", time.Now()))
	client.Fake.PrependReactor("get", "pods/log",
		func(cgtesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("forbidden")
		})
	r := NewK8sPodLogReader(client)
	_, err := r.ReadLogs(context.Background(), "multica-agents", "fm-t-1", 200)
	if err == nil || errors.Is(err, ErrJobLogsUnavailable) {
		t.Fatalf("err = %v, want wrapped stream error, not unavailable", err)
	}
}
