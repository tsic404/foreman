package registry

import (
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func jobWithMeta(labels, annos map[string]string) batchv1.Job {
	return batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "fm-task-1",
			Namespace:   "multica-agents",
			Labels:      labels,
			Annotations: annos,
		},
	}
}

func fullJobMeta() (map[string]string, map[string]string) {
	return map[string]string{
		LabelManagedBy:   ManagedByForeman,
		LabelTaskID:      "task-1",
		LabelAgentID:     "agent-1",
		LabelWorkspaceID: "ws-1",
	}, map[string]string{
		AnnoIssueID:         "issue-1",
		AnnoIssueIdentifier: "MULTI-1",
		AnnoDaemonID:        "fm-task-1",
		AnnoJobRuntimeID:    "rt-1",
		AnnoClaimedAt:       "2026-10-04T08:30:00Z",
		AnnoAttempt:         "3",
	}
}

func TestTaskEntryFromJobRestoresIdentity(t *testing.T) {
	labels, annos := fullJobMeta()
	e, err := TaskEntryFromJob(jobWithMeta(labels, annos))
	if err != nil {
		t.Fatalf("TaskEntryFromJob: %v", err)
	}
	if e.TaskID != "task-1" || e.AgentID != "agent-1" || e.WorkspaceID != "ws-1" {
		t.Fatalf("labels not restored: %+v", e)
	}
	if e.IssueID != "issue-1" || e.IssueIdentifier != "MULTI-1" {
		t.Fatalf("issue annotations not restored: %+v", e)
	}
	if e.DaemonID != "fm-task-1" || e.JobRuntimeID != "rt-1" {
		t.Fatalf("identity annotations not restored: %+v", e)
	}
	if e.JobName != "fm-task-1" || e.JobNamespace != "multica-agents" {
		t.Fatalf("object identity not restored: %+v", e)
	}
	if e.Attempt != 3 {
		t.Fatalf("attempt = %d", e.Attempt)
	}
	if e.ClaimedAt.IsZero() {
		t.Fatal("claimed_at not parsed")
	}
	// Conservative rebuild state; the C13 convergence advances it.
	if e.State != StateJobCreated {
		t.Fatalf("state = %v, want job_created", e.State)
	}
	if e.Payload != nil {
		t.Fatal("payload restored from annotations (F3 forbids it)")
	}
}

func TestTaskEntryFromJobRequiresMappingKeys(t *testing.T) {
	labels, annos := fullJobMeta()

	delete(labels, LabelTaskID)
	if _, err := TaskEntryFromJob(jobWithMeta(labels, annos)); err == nil {
		t.Fatal("missing task-id label accepted")
	}
	labels[LabelTaskID] = "task-1"

	for _, key := range []string{AnnoDaemonID, AnnoJobRuntimeID} {
		v := annos[key]
		delete(annos, key)
		if _, err := TaskEntryFromJob(jobWithMeta(labels, annos)); err == nil {
			t.Fatalf("missing %s accepted", key)
		}
		annos[key] = v
	}
}

func TestTaskEntryFromJobRejectsBadAnnotations(t *testing.T) {
	labels, annos := fullJobMeta()

	annos[AnnoClaimedAt] = "not-a-time"
	if _, err := TaskEntryFromJob(jobWithMeta(labels, annos)); err == nil ||
		!strings.Contains(err.Error(), AnnoClaimedAt) {
		t.Fatalf("bad claimed-at accepted: %v", err)
	}

	labels, annos = fullJobMeta()
	annos[AnnoAttempt] = "x"
	if _, err := TaskEntryFromJob(jobWithMeta(labels, annos)); err == nil ||
		!strings.Contains(err.Error(), AnnoAttempt) {
		t.Fatalf("bad attempt accepted: %v", err)
	}
}

func TestTaskEntryFromJobToleratesOptionalAnnotationsMissing(t *testing.T) {
	labels, _ := fullJobMeta()
	// claimed-at/attempt are optional: a Job labelled by an older build must
	// still rebuild.
	e, err := TaskEntryFromJob(jobWithMeta(labels, map[string]string{
		AnnoDaemonID:     "fm-task-1",
		AnnoJobRuntimeID: "rt-1",
	}))
	if err != nil {
		t.Fatalf("TaskEntryFromJob: %v", err)
	}
	if e.Attempt != 0 || !e.ClaimedAt.IsZero() {
		t.Fatalf("unexpected defaults: %+v", e)
	}
}
