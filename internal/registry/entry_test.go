package registry

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestProjectedTimeMatchesTaskEntryProjection pins the log-side renderer to
// the projection encoding: §9.1 / ADR-041 registers the lease.renewed field
// as carrying the same value /foreman/tasks projects, so
// ProjectedTime must go through the encoder the projection uses (not a copy
// of its layout: RFC3339 truncates the nanoseconds, .UTC() would drop the
// value's own zone).
func TestProjectedTimeMatchesTaskEntryProjection(t *testing.T) {
	for _, tt := range []time.Time{
		time.Date(2026, 10, 14, 23, 3, 57, 266810347, time.UTC),
		time.Date(2026, 10, 14, 23, 3, 57, 0, time.FixedZone("UTC+8", 8*3600)),
		{},
	} {
		raw, err := json.Marshal(TaskEntry{LeaseKeepUntil: tt})
		if err != nil {
			t.Fatalf("marshal entry: %v", err)
		}
		var projected struct {
			LeaseKeepUntil string `json:"lease_keep_until"`
		}
		if err := json.Unmarshal(raw, &projected); err != nil {
			t.Fatalf("unmarshal entry: %v", err)
		}
		if got := ProjectedTime(tt); got != projected.LeaseKeepUntil {
			t.Errorf("ProjectedTime(%v) = %q, projection = %q", tt, got, projected.LeaseKeepUntil)
		}
	}
}

// TestTaskEntryProjectionRendersAbsentTimesAsEmpty pins the two fields whose
// fact may not exist yet (§4.1, ADR-026): started_at and boot_deadline
// keep their key and carry an empty string while absent — an RFC3339 zero would
// satisfy AC-09's `!= ""` adoption poll before the fact exists — and render
// RFC3339 once set. The remaining time fields keep the encoder's form, which
// the embedded projection must not disturb.
func TestTaskEntryProjectionRendersAbsentTimesAsEmpty(t *testing.T) {
	observed := time.Date(2026, 10, 16, 14, 39, 9, 839000000, time.UTC)
	deadline := observed.Add(5 * time.Minute)
	for _, tc := range []struct {
		name         string
		startedAt    time.Time
		bootDeadline time.Time
	}{
		{"neither fact", time.Time{}, time.Time{}},
		{"both facts", observed, deadline},
		{"start fact only", observed, time.Time{}},
		{"boot bound only", time.Time{}, deadline},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(TaskEntry{
				TaskID:       "t-1",
				ClaimedAt:    observed,
				StartedAt:    tc.startedAt,
				BootDeadline: tc.bootDeadline,
			})
			if err != nil {
				t.Fatalf("marshal entry: %v", err)
			}
			var projected map[string]any
			if err := json.Unmarshal(raw, &projected); err != nil {
				t.Fatalf("unmarshal entry: %v", err)
			}
			if got, want := projected["claimed_at"], ProjectedTime(observed); got != want {
				t.Errorf("claimed_at = %v, want %q", got, want)
			}
			for _, field := range []struct {
				key  string
				fact time.Time
			}{
				{"started_at", tc.startedAt},
				{"boot_deadline", tc.bootDeadline},
			} {
				got, ok := projected[field.key]
				if !ok {
					t.Fatalf("projection dropped %q: %v", field.key, projected)
				}
				want := ""
				if !field.fact.IsZero() {
					want = ProjectedTime(field.fact)
				}
				if got != want {
					t.Errorf("%s = %v, want %q", field.key, got, want)
				}
				if want != "" {
					if _, err := time.Parse(time.RFC3339, want); err != nil {
						t.Errorf("%s = %q is not RFC3339: %v", field.key, want, err)
					}
				}
			}
		})
	}
}

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

func TestTaskEntryFromJobRestoresTriggerContext(t *testing.T) {
	labels, annos := fullJobMeta()
	annos[AnnoAutopilotRunID] = "run-1"
	e, err := TaskEntryFromJob(jobWithMeta(labels, annos))
	if err != nil {
		t.Fatalf("TaskEntryFromJob: %v", err)
	}
	if e.AutopilotRunID != "run-1" || e.ChatSessionID != "" {
		t.Fatalf("trigger context not restored: %+v", e)
	}
}

// TestTaskEntryFromJobWithoutIssueContext (ADR-014): the two issue keys are
// simply absent on an autopilot-triggered Job — a missing conditional-block key
// means that context does not exist, never a rebuild error.
func TestTaskEntryFromJobWithoutIssueContext(t *testing.T) {
	labels, annos := fullJobMeta()
	delete(annos, AnnoIssueID)
	delete(annos, AnnoIssueIdentifier)
	annos[AnnoAutopilotRunID] = "run-1"

	e, err := TaskEntryFromJob(jobWithMeta(labels, annos))
	if err != nil {
		t.Fatalf("TaskEntryFromJob: %v", err)
	}
	if e.IssueID != "" || e.IssueIdentifier != "" || e.ChatSessionID != "" {
		t.Fatalf("absent contexts must rebuild empty: %+v", e)
	}
	if e.AutopilotRunID != "run-1" {
		t.Fatalf("autopilot anchor = %q, want run-1", e.AutopilotRunID)
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
