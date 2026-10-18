// Package registry keeps the in-memory task mapping index
// (taskID ↔ jobName ↔ nodeName ↔ daemonID ↔ jobRuntimeID) and its state
// machine. The K8s Job object is the only durable truth (ADR-002); this
// package rebuilds the index from Job labels/annotations after a restart.
// Design: 02-contracts.md §4.1/§4.2, §2.3 (mapping index and state machine).
package registry

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
)

// State is the task lifecycle stage (§4.2). It has exactly six
// values; "failed" is not a state but a Result of StateTerminal.
type State string

// Lifecycle states (§4.2, exhaustive).
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

// Terminal results (§4.1).
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
// (§4.2). "gone" is expressed by the entry's absence, so only the
// two live phases are named; the zero value means alive, which is what a
// rebuilt entry starts from.
type JobPhase string

// Job lifecycle phases (§4.2, exhaustive).
const (
	JobPhaseAlive    JobPhase = "alive"
	JobPhaseDraining JobPhase = "draining"
)

// TaskEntry is the mapping table row (§4.1). The four trigger-context
// fields (issue/autopilot/chat) are optional: an empty value means that context
// does not exist and is never an error (ADR-014). The JSON projection keeps all
// four keys with an empty string instead of omitting them, so consumers can
// test `== ""` (§4.1). Payload is the task body kept in process memory only —
// the raw claim payload until delivery, the S6 delivery body (runtime_id
// rewritten to job_runtime_id) afterwards: it carries auth_token and must never
// be serialized into annotations, Secrets, or logs (F3).
type TaskEntry struct {
	TaskID          string `json:"task_id"`
	AgentID         string `json:"agent_id"`
	IssueID         string `json:"issue_id"`
	IssueIdentifier string `json:"issue_identifier"`
	AutopilotRunID  string `json:"autopilot_run_id"`
	ChatSessionID   string `json:"chat_session_id"`
	WorkspaceID     string `json:"workspace_id"`
	JobName         string `json:"job_name"`
	JobNamespace    string `json:"job_namespace"`
	DaemonID        string `json:"daemon_id"`
	JobRuntimeID    string `json:"job_runtime_id"`
	NodeName        string `json:"node_name"`
	State           State  `json:"state"`
	Result          Result `json:"result"`
	// FailureReason is the externally observable attribution of a terminal
	// failure Foreman judged itself (§4.1): empty for a verdict the
	// server made (S13/S14/S15 or C13) and for non-failed results.
	FailureReason      string    `json:"failure_reason"`
	Attempt            int       `json:"attempt"`
	ClaimedAt          time.Time `json:"claimed_at"`
	JobCreatedAt       time.Time `json:"job_created_at"`
	DaemonRegisteredAt time.Time `json:"daemon_registered_at"`
	// DeliveredAt is the commit instant of the delivery that stands on the
	// entry: the first one, or the latest re-delivery once an expired
	// confirmation window released the payload again. Once non-zero it never
	// goes back to zero, so it also answers "was this entry ever delivered"
	// (§4.1).
	DeliveredAt time.Time `json:"delivered_at"`
	// Deliveries counts the delivery commits this entry accepted: 1 for the
	// single-delivery form, more when expired confirmation windows released
	// the payload for another attempt (§4.1, ADR-032).
	Deliveries int `json:"deliveries"`
	// ClaimsAfterDelivery counts the S6 claim calls answered with an empty
	// batch after this entry's delivery was committed (§4.1): > 0
	// means the daemon kept asking inside the confirmation window, i.e. the
	// request arrived and the response was lost, rather than the request
	// never arriving.
	ClaimsAfterDelivery int `json:"claims_after_delivery"`
	// StartedAt and BootDeadline may have no fact yet (§4.1,
	// ADR-026): the projection then carries an empty string, never the
	// RFC3339 zero — see MarshalJSON.
	StartedAt    time.Time `json:"started_at"`
	BootDeadline time.Time `json:"boot_deadline"`
	// LeaseKeepUntil is the local upper bound of the server's prepare lease
	// (§10): the last successful renewal plus PrepareLeaseTTL,
	// stamped after the response returned (Registry.SetLeaseKeepUntil).
	LeaseKeepUntil time.Time `json:"lease_keep_until"`
	LastStatusSeen string    `json:"last_status_seen"`
	PayloadVersion int       `json:"payload_version"`

	// Job lifecycle fields (§4.3): memory-only — never projected
	// into /foreman/tasks, never written to annotations, never persisted.
	// The credential stays valid while the entry exists, so an entry in
	// draining is still the authorization row for its Job (ADR-017).
	JobPhase      JobPhase  `json:"-"`
	DrainDeadline time.Time `json:"-"`
	TokenHash     string    `json:"-"`

	// DeliveryDeadline is the S6 delivery confirmation window's expiry
	// (§5, ADR-032): stamped at each accepted delivery commit from
	// FOREMAN_DELIVERY_CONFIRM_TIMEOUT. Passing it with no start fact
	// releases the delivery for another attempt (the re-arm), so the window
	// bounds the re-delivery rather than the entry. Zero means no window is
	// armed: either nothing was delivered yet, or the re-delivery exit is
	// configured off and the boot bound alone owns the entry.
	DeliveryDeadline time.Time `json:"-"`

	// CompletedAt is the verdict instant, the stamp the settled index keeps
	// (registry.DoneAt). Memory-only, like the rest of the drain bookkeeping:
	// the projection's timestamp keys keep their existing set.
	CompletedAt time.Time `json:"-"`

	// PriorSessionID is the claim payload's prior_session_id: where the
	// resuming session's file lives, hence the soft-affinity key source
	// (ADR-015). Payload-derived and memory-only like the body itself — not
	// annotated, not projected, and not rebuilt after a restart, because a
	// rebuilt entry's Job already exists and never goes through Build.
	PriorSessionID string `json:"-"`

	// AgentName is the claim payload's agent.name — the policy keyspace's
	// agent side (ADR-035) and the <agent-name> / <agent-segment> source.
	// Memory-only like PriorSessionID; empty means the identity is
	// unavailable, which degrades the axes rather than failing the task.
	AgentName string `json:"-"`

	// ProjectName is the task's project identity (§4.1 项目身份):
	// read at claim time from the payload's structured project key, or from
	// the read-only reverse lookup by issue id. Memory-only: never annotated,
	// never projected, never rebuilt.
	ProjectName string `json:"-"`
	// ProjectSource records where ProjectName came from (payload / api /
	// none); policy.applied projects it, nothing else reads it.
	ProjectSource string `json:"-"`

	// RealRuntimeID is the real runtime the claim was dispatched under: the
	// C4 lease, the C19 resolve and the S8 body translation are all addressed
	// by it when Foreman declares N runtimes. Like the payload it is
	// memory-only and absent on rebuilt entries, whose Job is already past
	// its start.
	RealRuntimeID string `json:"-"`

	// Payload: raw claim payload until MarkDelivering replaces it with the
	// delivered body — the form the ops preview must serve (S6 / AC-07).
	Payload json.RawMessage `json:"-"`
}

// MarshalJSON renders the entry as /foreman/tasks projects it (§4.1,
// §9.3): the projection is TaskEntry's own JSON — no second
// view type to keep in step — with started_at and boot_deadline rendered as an
// empty string while their fact does not exist yet. Both keys stay present, and
// the RFC3339 zero is banned: it would satisfy a consumer's `!= ""` test (the
// adoption poll of AC-09) before the fact exists. A set value goes through the
// same time.Time encoder every other projected time field uses.
func (e TaskEntry) MarshalJSON() ([]byte, error) {
	type entry TaskEntry
	return json.Marshal(struct {
		entry
		StartedAt    string `json:"started_at"`
		BootDeadline string `json:"boot_deadline"`
	}{entry(e), projectedOptionalTime(e.StartedAt), projectedOptionalTime(e.BootDeadline)})
}

// projectedOptionalTime renders a time field whose fact may not exist yet: an
// empty string while it does not, the encoder's RFC3339 form once it does.
func projectedOptionalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return ProjectedTime(t)
}

// ProjectedTime renders t exactly as the /foreman/tasks projection renders
// TaskEntry's time fields: through the time.Time JSON encoder that
// encoding/json uses for them (RFC3339Nano, the value's own location), not
// through a copy of its layout string. A log field that mirrors a projected
// field must go through here — §9.1 / ADR-041 requires the lease.renewed
// lease_keep_until to carry the same value the projection does,
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

// DeliveryRearmable reports whether an expired confirmation window released
// this entry's delivery for another attempt (ADR-032): the entry still stands
// on a committed delivery, no start fact arrived, and the armed window has
// passed. It is the S6 gate's re-delivery term — and the one place that says
// the commit the daemon never answered may be handed over again.
func (e TaskEntry) DeliveryRearmable(at time.Time) bool {
	return e.State == StateDelivering && e.StartedAt.IsZero() &&
		!e.DeliveredAt.IsZero() && !e.DeliveryDeadline.IsZero() &&
		at.After(e.DeliveryDeadline)
}

// DeliveryUnconfirmed reports a delivered entry still without a start fact
// that this claim may not re-deliver: the window is open (the response may be
// in flight and the daemon may still be preparing the payload it received) or
// no window is configured at all. An empty batch answering it is the
// lost-response signature (§9.2 result=empty_unconfirmed).
func (e TaskEntry) DeliveryUnconfirmed(at time.Time) bool {
	return e.State == StateDelivering && e.StartedAt.IsZero() &&
		!e.DeliveredAt.IsZero() && !e.DeliveryRearmable(at)
}

// JobRuntime is the fake runtime registered to a Job's daemon (§2.3).
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

// Label and annotation keys carried by every foreman Job.
// jobbuilder owns the write side; these copies are the read side for rebuild.
const (
	LabelManagedBy   = "app.kubernetes.io/managed-by"
	ManagedByForeman = "foreman"

	// LabelRole and RoleCatalogProbe mark a catalog-probe Job/Secret; they are
	// the probe domain's discriminator, mutually exclusive with LabelTaskID
	// (§6.5).
	LabelRole        = "foreman.tsic.top/role"
	RoleCatalogProbe = "catalog-probe"
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
// (§4.3). The state starts at the conservative StateJobCreated and
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
		// evidence (§4.1): a rebuilt entry must carry the object's
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
