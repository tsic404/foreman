// Package registry keeps Foreman's in-memory view of the tasks it has
// claimed and the long-lived multica daemons that run them. In v2 there is no
// K8s Job object behind a task: a claimed task is handed directly to a
// registered daemon, so the entry's durable truth is the server's own
// dispatch (a restart rebuilds in-flight tasks from the server), and this
// package is the process-local index plus its state machine.
//
// Design (v2): 02-contracts.md §4.1/§4.2 mapping and state machine, minus the
// Job-created phase and the node/session affinity that belonged to pod
// placement.
package registry

import (
	"encoding/json"
	"time"
)

// State is the task lifecycle stage. v2 drops StateJobCreated (a task is
// deliverable as soon as a daemon is chosen, not when a Job object exists);
// "failed" stays a Result of StateTerminal, not a state.
type State string

// Lifecycle states (§4.2, exhaustive).
const (
	StatePending          State = "pending"
	StateDaemonRegistered State = "daemon_registered"
	StateDelivering       State = "delivering"
	StateRunning          State = "running"
	StateTerminal         State = "terminal"
)

// Result classifies a terminal entry; set only when State == StateTerminal.
type Result string

// Terminal results (§4.1).
const (
	ResultCompleted Result = "completed"
	ResultFailed    Result = "failed"
	ResultCancelled Result = "cancelled"
)

// IsTerminalResult reports whether s is a server-side terminal status (C13).
func IsTerminalResult(s string) bool {
	return s == string(ResultCompleted) || s == string(ResultFailed) || s == string(ResultCancelled)
}

// IsTerminal reports whether e has reached a terminal state.
func (e TaskEntry) IsTerminal() bool { return e.State == StateTerminal }

// TaskEntry is one claimed task's mapping row. It carries the claim identity,
// the daemon it was dispatched to, and the lifecycle facts the proxy and the
// dispatcher forward. Payload is the claim body (never projected, never
// persisted beyond the process); it is the daemon-side delivery face.
type TaskEntry struct {
	TaskID          string    `json:"task_id"`
	AgentID         string    `json:"agent_id"`
	IssueID         string    `json:"issue_id"`
	IssueIdentifier string    `json:"issue_identifier"`
	AutopilotRunID  string    `json:"autopilot_run_id"`
	ChatSessionID   string    `json:"chat_session_id"`
	WorkspaceID     string    `json:"workspace_id"`
	DaemonID        string    `json:"daemon_id"`        // the registered daemon the task was handed to
	RuntimeID       string    `json:"runtime_id"`       // the daemon's declared runtime id
	State           State     `json:"state"`
	Result          Result    `json:"result"`
	FailureReason   string    `json:"failure_reason"`
	Attempt         int       `json:"attempt"`
	ClaimedAt       time.Time `json:"claimed_at"`
	AssignedAt      time.Time `json:"assigned_at"`      // dispatch commit instant (was job_created_at / delivered_at)
	StartedAt       time.Time `json:"started_at"`
	CompletedAt     time.Time `json:"completed_at"`     // verdict instant
	LeaseKeepUntil  time.Time `json:"lease_keep_until"`
	TokenHash       string    `json:"-"`                // the dispatched daemon credential's digest (ADR-017)
	Payload         json.RawMessage `json:"-"`          // the claim body, handed to the daemon at delivery

	// PriorSessionID is the claim payload's prior_session_id; a long-lived
	// daemon uses it to resume the session it already holds, so Foreman
	// prefers a daemon that carries it (session affinity).
	PriorSessionID string `json:"-"`
}

// Assigned reports whether the task has been handed to a daemon.
func (e TaskEntry) Assigned() bool { return e.State == StateDelivering || e.State == StateRunning }