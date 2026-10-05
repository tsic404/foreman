package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ErrJobLogsUnavailable means the Job's pod is gone (finished Jobs are
// deleted) or never scheduled; the endpoint maps it to 404
// (observability.md §错误处理).
var ErrJobLogsUnavailable = errors.New("job logs unavailable")

// PodLogReader is the Job log proxy's read seam (ADR-009 point 3).
type PodLogReader interface {
	ReadLogs(ctx context.Context, namespace, jobName string, tailLines int64) ([]byte, error)
}

// K8sPodLogReader reads Job logs through the pods/log subresource; it needs
// pods: list and pods/log: get (contract §1.3 RBAC).
type K8sPodLogReader struct {
	client kubernetes.Interface
}

// NewK8sPodLogReader wires the client-go backed reader.
func NewK8sPodLogReader(client kubernetes.Interface) *K8sPodLogReader {
	return &K8sPodLogReader{client: client}
}

// ReadLogs returns the tail of the newest pod of jobName. The Job's pods
// carry the batch.kubernetes.io/job-name label (K8s >= 1.27).
func (r *K8sPodLogReader) ReadLogs(ctx context.Context, namespace, jobName string, tailLines int64) ([]byte, error) {
	pods, err := r.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "batch.kubernetes.io/job-name=" + jobName,
	})
	if err != nil {
		return nil, fmt.Errorf("list pods for job %s: %w", jobName, err)
	}
	if len(pods.Items) == 0 {
		return nil, ErrJobLogsUnavailable
	}
	pod := newestPod(pods.Items)
	req := r.client.CoreV1().Pods(namespace).GetLogs(pod.Name,
		&corev1.PodLogOptions{TailLines: &tailLines})
	stream, err := req.Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("stream pod %s logs: %w", pod.Name, err)
	}
	defer stream.Close()
	data, err := io.ReadAll(stream)
	if err != nil {
		return nil, fmt.Errorf("read pod %s logs: %w", pod.Name, err)
	}
	return data, nil
}

// newestPod picks the pod still writing logs: a retried Job has several,
// the latest creation timestamp is the live attempt.
func newestPod(pods []corev1.Pod) corev1.Pod {
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].CreationTimestamp.After(pods[j].CreationTimestamp.Time)
	})
	return pods[0]
}
