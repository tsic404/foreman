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

// JobPhase is the Job lifecycle phase, orthogonal to the task state
// (contract §3.4). "gone" is expressed by the entry's absence, so only the
// two live phases are named; the zero value means alive, which is what a
// rebuilt entry starts from.
type JobPhase string

// Job lifecycle phases (contract §3.4, exhaustive).
const (
	JobPhaseAlive    JobPhase = "alive"
	JobPhaseDraining JobPhase = "draining"
)

// TaskEntry is the mapping table row (contract §3.1). The four trigger-context
// fields (issue/autopilot/chat) are optional: an empty value means that context
// does not exist and is never an error (ADR-014). The JSON projection keeps all
// four keys with an empty string instead of omitting them, so consumers can
// test `== ""` (§3.1). Payload is the task body kept in process memory only —
// the raw claim payload until delivery, the S6 delivery body (runtime_id
// rewritten to job_runtime_id) afterwards: it carries auth_token and must never
// be serialized into annotations, Secrets, or logs (F3).
type TaskEntry struct {
	TaskID             string    `json:"task_id"`
	AgentID            string    `json:"agent_id"`
	IssueID            string    `json:"issue_id"`
	IssueIdentifier    string    `json:"issue_identifier"`
	AutopilotRunID     string    `json:"autopilot_run_id"`
	ChatSessionID      string    `json:"chat_session_id"`
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
	// LeaseKeepUntil is the local upper bound of the server's prepare lease
	// (contract §3.1): the last successful renewal plus PrepareLeaseTTL,
	// stamped after the response returned (Registry.SetLeaseKeepUntil).
	LeaseKeepUntil time.Time `json:"lease_keep_until"`
	LastStatusSeen string    `json:"last_status_seen"`
	PayloadVersion int       `json:"payload_version"`

	// Job lifecycle fields (contract §3.1): memory-only — never projected
	// into /foreman/tasks, never written to annotations, never persisted.
	// The credential stays valid while the entry exists, so an entry in
	// draining is still the authorization row for its Job (ADR-017).
	JobPhase      JobPhase  `json:"-"`
	DrainDeadline time.Time `json:"-"`
	TokenHash     string    `json:"-"`

	// PriorSessionID is the claim payload's prior_session_id: where the
	// resuming session's file lives, hence the soft-affinity key source
	// (ADR-015). Payload-derived and memory-only like the body itself — not
	// annotated, not projected, and not rebuilt after a restart, because a
	// rebuilt entry's Job already exists and never goes through Build.
	PriorSessionID string `json:"-"`

	// Payload: raw claim payload until MarkDelivering replaces it with the
	// delivered body — the form the ops preview must serve (S6 / AC-07).
	Payload json.RawMessage `json:"-"`
}

// ProjectedTime renders t exactly as the /foreman/tasks projection renders
// TaskEntry's time fields: through the time.Time JSON encoder that
// encoding/json uses for them (RFC3339Nano, the value's own location), not
// through a copy of its layout string. A log field that mirrors a projected
// field must go through here — observability.md §结构化日志 requires the
// lease.renewed lease_keep_until to carry the same value the projection does,
// and a duplicated layout would drift from it silently. An unencodable value
// (year outside [0,9999]) yields "" — the projection itself cannot encode it
// either.
func ProjectedTime(t time.Time) string {
	b, err := t.MarshalJSON()
	if err != nil {
		return ""
	}
	// MarshalJSON answers a JSON string: drop the quoting.
	return string(b[1 : len(b)-1])
}

// IsTerminal reports whether the entry has reached its terminal state.
func (e TaskEntry) IsTerminal() bool { return e.State == StateTerminal }

// IsDraining reports whether the entry's Job is past its task's verdict: the
// task is terminal, the Job is being torn down, and the credential stays
// valid until the Job's pods are gone or the drain upper bound expires
// (ADR-017).
func (e TaskEntry) IsDraining() bool { return e.JobPhase == JobPhaseDraining }

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
	AnnoAutopilotRunID  = "foreman.tsic.top/autopilot-run-id"
	AnnoChatSessionID   = "foreman.tsic.top/chat-session-id"
	AnnoDaemonID        = "foreman.tsic.top/daemon-id"
	AnnoJobRuntimeID    = "foreman.tsic.top/job-runtime-id"
	AnnoClaimedAt       = "foreman.tsic.top/claimed-at"
	AnnoAttempt         = "foreman.tsic.top/attempt"
)

// TaskEntryFromJob rebuilds an entry from a Job's labels and annotations
// (contract §3.2). The state starts at the conservative StateJobCreated and
// is advanced by the startup C13 convergence; the payload is not restored
// (it never leaves process memory, so a restarted Foreman cannot re-deliver).
// The four trigger-context annotations are conditional blocks: a missing key
// means that context does not exist, never a rebuild error (ADR-014).
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
		// The apiserver timestamp is the instance-filter basis of the pod
		// evidence (contract §3.1): a rebuilt entry must carry the object's
		// value, never a local clock reading.
		JobCreatedAt: job.CreationTimestamp.Time,

		IssueID:         annos[AnnoIssueID],
		IssueIdentifier: annos[AnnoIssueIdentifier],
		AutopilotRunID:  annos[AnnoAutopilotRunID],
		ChatSessionID:   annos[AnnoChatSessionID],
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
