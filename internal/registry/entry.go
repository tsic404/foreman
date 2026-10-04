// Package registry keeps the in-memory task mapping index
// (taskID ↔ jobName ↔ nodeName ↔ daemonID ↔ jobRuntimeID) and its state
// machine. The K8s Job object is the only durable truth (ADR-002); this
// package rebuilds the index from Job labels/annotations after a restart.
// Design: docs/05-modules/task-mapping.md, contracts §3.1/§3.3/§3.4.
package registry

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
)

// State is the task lifecycle stage (contract §3.4). It has exactly six
// values; "failed" is not a state but a Result of StateTerminal.
type State string

// Lifecycle states (contract §3.4, exhaustive).
const (
	StatePending          State = "pending"
	StateJobCreated       State = "job_created"
	StateDaemonRegistered State = "daemon_registered"
	StateDelivering       State = "delivering"
	StateRunning          State = "running"
	StateTerminal         State = "terminal"
)

// Result classifies a terminal entry; set only when State == StateTerminal.
type Result string

// Terminal results (contract §3.1).
const (
	ResultCompleted Result = "completed"
	ResultFailed    Result = "failed"
	ResultCancelled Result = "cancelled"
)

// IsTerminal reports whether s is a server-side terminal status (C13).
func IsTerminalResult(s string) bool {
	return s == string(ResultCompleted) || s == string(ResultFailed) || s == string(ResultCancelled)
}

// TaskEntry is the mapping table row (contract §3.1). Payload is the full
// claim payload kept in process memory only: it carries auth_token and must
// never be serialized into annotations, Secrets, or logs (F3).
type TaskEntry struct {
	TaskID             string    `json:"task_id"`
	AgentID            string    `json:"agent_id"`
	IssueID            string    `json:"issue_id"`
	IssueIdentifier    string    `json:"issue_identifier"`
	WorkspaceID        string    `json:"workspace_id"`
	JobName            string    `json:"job_name"`
	JobNamespace       string    `json:"job_namespace"`
	DaemonID           string    `json:"daemon_id"`
	JobRuntimeID       string    `json:"job_runtime_id"`
	NodeName           string    `json:"node_name"`
	State              State     `json:"state"`
	Result             Result    `json:"result"`
	Attempt            int       `json:"attempt"`
	ClaimedAt          time.Time `json:"claimed_at"`
	JobCreatedAt       time.Time `json:"job_created_at"`
	DaemonRegisteredAt time.Time `json:"daemon_registered_at"`
	DeliveredAt        time.Time `json:"delivered_at"`
	StartedAt          time.Time `json:"started_at"`
	BootDeadline       time.Time `json:"boot_deadline"`
	LeaseKeepUntil     time.Time `json:"lease_keep_until"`
	LastStatusSeen     string    `json:"last_status_seen"`
	PayloadVersion     int       `json:"payload_version"`

	Payload json.RawMessage `json:"-"`
}

// IsTerminal reports whether the entry has reached its terminal state.
func (e TaskEntry) IsTerminal() bool { return e.State == StateTerminal }

// JobRuntime is the fake runtime registered to a Job's daemon (contract §3.3).
type JobRuntime struct {
	JobRuntimeID    string    `json:"job_runtime_id"`
	DaemonID        string    `json:"daemon_id"`
	TaskID          string    `json:"task_id"`
	Provider        string    `json:"provider"`
	Name            string    `json:"name"`
	Online          bool      `json:"online"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	DeliveredTaskID string    `json:"delivered_task_id"`
}

// Label and annotation keys carried by every foreman Job (contract §3.2).
// jobbuilder owns the write side; these copies are the read side for rebuild.
const (
	LabelManagedBy   = "app.kubernetes.io/managed-by"
	ManagedByForeman = "foreman"

	LabelTaskID      = "foreman.tsic.top/task-id"
	LabelAgentID     = "foreman.tsic.top/agent-id"
	LabelWorkspaceID = "foreman.tsic.top/workspace-id"

	AnnoIssueID         = "foreman.tsic.top/issue-id"
	AnnoIssueIdentifier = "foreman.tsic.top/issue-identifier"
	AnnoDaemonID        = "foreman.tsic.top/daemon-id"
	AnnoJobRuntimeID    = "foreman.tsic.top/job-runtime-id"
	AnnoClaimedAt       = "foreman.tsic.top/claimed-at"
	AnnoAttempt         = "foreman.tsic.top/attempt"
)

// TaskEntryFromJob rebuilds an entry from a Job's labels and annotations
// (contract §3.2). The state starts at the conservative StateJobCreated and
// is advanced by the startup C13 convergence; the payload is not restored
// (it never leaves process memory, so a restarted Foreman cannot re-deliver).
func TaskEntryFromJob(job batchv1.Job) (TaskEntry, error) {
	labels := job.GetLabels()
	annos := job.GetAnnotations()

	e := TaskEntry{
		TaskID:       labels[LabelTaskID],
		AgentID:      labels[LabelAgentID],
		WorkspaceID:  labels[LabelWorkspaceID],
		JobName:      job.GetName(),
		JobNamespace: job.GetNamespace(),
		State:        StateJobCreated,

		IssueID:         annos[AnnoIssueID],
		IssueIdentifier: annos[AnnoIssueIdentifier],
		DaemonID:        annos[AnnoDaemonID],
		JobRuntimeID:    annos[AnnoJobRuntimeID],
	}
	if e.TaskID == "" {
		return TaskEntry{}, fmt.Errorf("job %s: missing label %s", job.GetName(), LabelTaskID)
	}
	if e.DaemonID == "" {
		return TaskEntry{}, fmt.Errorf("job %s: missing annotation %s", job.GetName(), AnnoDaemonID)
	}
	if e.JobRuntimeID == "" {
		return TaskEntry{}, fmt.Errorf("job %s: missing annotation %s", job.GetName(), AnnoJobRuntimeID)
	}
	if raw := annos[AnnoClaimedAt]; raw != "" {
		claimedAt, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return TaskEntry{}, fmt.Errorf("job %s: annotation %s %q is not RFC3339: %v", job.GetName(), AnnoClaimedAt, raw, err)
		}
		e.ClaimedAt = claimedAt
	}
	if raw := annos[AnnoAttempt]; raw != "" {
		attempt, err := strconv.Atoi(raw)
		if err != nil {
			return TaskEntry{}, fmt.Errorf("job %s: annotation %s %q is not an integer: %v", job.GetName(), AnnoAttempt, raw, err)
		}
		e.Attempt = attempt
	}
	return e, nil
}
