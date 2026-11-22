// Package attempt owns the fixed foreman-job's three-stage contract:
// prepare only this attempt's two CLI artifacts, establish and verify the
// frozen rsbx session before registering, then dispatch the selected Agent
// once and settle the remote-side close. It is the Job-side half of
// 02-contracts.md V2-C03/C04 and 04-architecture.md §6.
package attempt

import (
	"errors"
	"fmt"
	"strings"
)

// CLI kinds. The fixed Job prepares exactly one of each; any other backend is
// neither downloaded nor mounted (04-architecture.md §6 ①).
const (
	KindMultica = "multica"
	KindAgent   = "agent"
)

// Platform is the client container's platform in os/arch form. The two
// artifacts are chosen for this platform, never for the remote machine's.
const platformSeparator = "/"

// CLI is one frozen artifact reference. Version and Digest come from the
// claim-time release snapshot; the Job may only use those exact bytes.
type CLI struct {
	Kind     string `json:"kind"`
	Backend  string `json:"backend,omitempty"`
	Version  string `json:"version"`
	Platform string `json:"platform"`
	Digest   string `json:"sha256"`
}

// Config is the selected Agent configuration projection. The Job mounts only
// this reference; other backends' Secrets are not part of the attempt.
type Config struct {
	SecretRef  string `json:"secret_ref,omitempty"`
	SecretName string `json:"secret_name,omitempty"`
	FileName   string `json:"file_name,omitempty"`
	MountPath  string `json:"mount_path,omitempty"`
	Revision   uint64 `json:"revision,omitempty"`
	Digest     string `json:"sha256,omitempty"`
}

// Binding is the immutable input snapshot one fixed Job starts from. It
// carries references, never Secret values.
type Binding struct {
	AttemptID      string `json:"attempt_id"`
	TaskID         string `json:"task_id"`
	SourceID       string `json:"source_id,omitempty"`
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	DaemonID       string `json:"daemon_id"`
	JobRuntimeID   string `json:"job_runtime_id"`
	EnvironmentID  string `json:"environment_id"`
	Endpoint       string `json:"endpoint"`
	TLSRef         string `json:"tls_ref,omitempty"`
	StateRoot      string `json:"state_root"`
	EnvironmentGen string `json:"environment_generation,omitempty"`
	// SessionID is the frozen remote rsbx session this attempt must resume.
	// Empty means the claim has no prior remote session; a non-empty value is
	// an exact identity requirement, never an affinity hint.
	SessionID string `json:"session_id,omitempty"`
	// ResumeSessionID is the Multica session path the claim resumes. It is an
	// affinity key on the client side, not an rsbx session identity.
	ResumeSessionID string `json:"resume_session_id,omitempty"`
	SlotID          string `json:"slot_id"`
	CLIs            []CLI  `json:"clis"`
	Config          Config `json:"config"`
}

// Validate checks the closure the fixed Job relies on. The two CLIs are the
// whole preparation set: one Multica CLI and the selected Agent's CLI.
func (b Binding) Validate() error {
	for name, value := range map[string]string{
		"attempt id": b.AttemptID, "task id": b.TaskID, "workspace id": b.WorkspaceID,
		"agent id": b.AgentID, "daemon id": b.DaemonID, "job runtime id": b.JobRuntimeID,
		"environment id": b.EnvironmentID, "endpoint": b.Endpoint, "TLS reference": b.TLSRef,
		"state root": b.StateRoot, "environment generation": b.EnvironmentGen,
		"slot id": b.SlotID,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if len(b.CLIs) != 2 {
		return fmt.Errorf("exactly two CLI artifacts are required, got %d", len(b.CLIs))
	}
	seen := map[string]bool{}
	for _, cli := range b.CLIs {
		if cli.Kind != KindMultica && cli.Kind != KindAgent {
			return fmt.Errorf("unsupported CLI kind %q", cli.Kind)
		}
		if seen[cli.Kind] {
			return fmt.Errorf("CLI kind %q is declared twice", cli.Kind)
		}
		seen[cli.Kind] = true
		if strings.TrimSpace(cli.Version) == "" || strings.TrimSpace(cli.Platform) == "" || strings.TrimSpace(cli.Digest) == "" {
			return fmt.Errorf("%s CLI needs version, platform and digest", cli.Kind)
		}
		if strings.Count(cli.Platform, platformSeparator) != 1 {
			return fmt.Errorf("%s CLI platform %q is not os/arch", cli.Kind, cli.Platform)
		}
	}
	if !seen[KindMultica] || !seen[KindAgent] {
		return errors.New("both the Multica CLI and the selected Agent CLI are required")
	}
	for _, cli := range b.CLIs {
		if cli.Kind == KindAgent && strings.TrimSpace(cli.Backend) == "" {
			return errors.New("the Agent CLI needs its backend identity")
		}
	}
	return nil
}

// Platform returns the client platform of the attempt.
func (b Binding) Platform() string {
	for _, cli := range b.CLIs {
		if cli.Kind == KindMultica {
			return cli.Platform
		}
	}
	return ""
}

// Multica returns the frozen Multica CLI reference.
func (b Binding) Multica() (CLI, bool) {
	return b.cli(KindMultica)
}

// Agent returns the frozen selected Agent CLI reference.
func (b Binding) Agent() (CLI, bool) {
	return b.cli(KindAgent)
}

func (b Binding) cli(kind string) (CLI, bool) {
	for _, cli := range b.CLIs {
		if cli.Kind == kind {
			return cli, true
		}
	}
	return CLI{}, false
}

// Stage is one observable step of the fixed Job.
type Stage string

const (
	StagePreparing   Stage = "preparing"
	StageConnecting  Stage = "connecting"
	StageRegistering Stage = "registering"
	StageDispatching Stage = "dispatching"
	StageClosing     Stage = "closing"
	StageClosed      Stage = "closed"
	StageIsolated    Stage = "isolated"
	StageRefused     Stage = "refused"
	StageInterrupted Stage = "interrupted"
)

// Artifact is one verified local CLI file.
type Artifact struct {
	Kind     string `json:"kind"`
	Backend  string `json:"backend,omitempty"`
	Version  string `json:"version"`
	Platform string `json:"platform"`
	Digest   string `json:"sha256"`
	Path     string `json:"path"`
}

// Session is the remote rsbx session. Its identity is the stable environment
// and tree generation plus the session id, never the Pod's hostname.
type Session struct {
	ID            string `json:"id"`
	EnvironmentID string `json:"environment_id"`
	Generation    string `json:"generation,omitempty"`
	Endpoint      string `json:"endpoint,omitempty"`
}

// Registration is the daemon's registration fact.
type Registration struct {
	DaemonID  string `json:"daemon_id"`
	RuntimeID string `json:"runtime_id"`
	Version   string `json:"version"`
}

// CloseEvidence is the remote-side close observation. Confirmed is the only
// value that lets the slot return to the pool.
type CloseEvidence struct {
	Confirmed bool   `json:"confirmed"`
	Detail    string `json:"detail,omitempty"`
}

// SlotStatus is the authoritative shared ledger's view of one attempt's
// reservation. A rebuilt Job reads it before preparing anything: a started or
// isolated record means a remote session may already exist, and the original
// session id must be closed before any new session is opened.
type SlotStatus struct {
	Exists bool   `json:"exists"`
	State  string `json:"state,omitempty"`
	Phase  string `json:"phase,omitempty"`
	// ClaimID is the execution lease the ledger currently grants. Recovery
	// names it as the compare-and-swap token for TakeOver.
	ClaimID   string `json:"claim_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	Started   bool   `json:"started,omitempty"`
}

// SlotActive and SlotIsolated are the ledger states the fixed Job reads. An
// isolated slot keeps its session id and stays out of the free pool.
const (
	SlotActive   = "active"
	SlotIsolated = "isolated"
)

// Slot phases record how far the attempt got before an interruption. Opening
// without a session id is an unknown remote result and is never retried.
const (
	SlotPhaseIdle      = ""
	SlotPhaseOpening   = "opening"
	SlotPhaseConnected = "connected"
	SlotPhaseStarted   = "started"
)

// RunResult is one dispatch's result. Delivered becomes true only after the
// payload reached the remote Agent; an interrupted call is never retried.
type RunResult struct {
	Delivered bool `json:"delivered"`
	ExitCode  int  `json:"exit_code,omitempty"`
}

// Outcome is the fixed Job's evidence. It is what the acceptance cases read
// to prove zero business execution, an unchanged endpoint and the close state.
type Outcome struct {
	Stage        Stage         `json:"stage"`
	Prepared     []Artifact    `json:"prepared"`
	Session      Session       `json:"session"`
	Registration Registration  `json:"registration"`
	Delivered    bool          `json:"delivered"`
	Close        CloseEvidence `json:"close"`
	Isolated     bool          `json:"isolated"`
	SlotReleased bool          `json:"slot_released"`
	Detail       string        `json:"detail,omitempty"`
}

// ErrRefused marks a refusal before delivery. A refused attempt must have
// executed no business command and must not have fallen back to another
// endpoint.
var ErrRefused = errors.New("attempt refused")

// ErrInterrupted marks an attempt whose dispatch started but did not finish.
// It carries possible business side effects and must not be replayed.
var ErrInterrupted = errors.New("attempt interrupted")

// ErrSessionRelocated marks a restart whose frozen binding names a different
// environment than the session already started on.
var ErrSessionRelocated = errors.New("the attempt session belongs to another environment")

// ErrNotDelivered marks a dispatch that failed before the payload reached the
// remote Agent.
var ErrNotDelivered = errors.New("the task was not delivered")

// ErrSessionNotCreated is the one ready failure that proves no remote session
// exists, so the reservation may return to the pool. Every other ready error
// leaves the creation result unknown and must keep the slot isolated.
var ErrSessionNotCreated = errors.New("the remote session was not created")

// Refusal is one stage-scoped rejection. Stage names the observable step, so
// an operator can tell preparation, readiness and registration failures apart.
type Refusal struct {
	Stage  Stage
	Reason string
	Err    error
}

func (r *Refusal) Error() string {
	if r == nil {
		return ""
	}
	if r.Err != nil {
		return fmt.Sprintf("%v at %s: %s: %v", ErrRefused, r.Stage, r.Reason, r.Err)
	}
	return fmt.Sprintf("%v at %s: %s", ErrRefused, r.Stage, r.Reason)
}

func (r *Refusal) Unwrap() error { return r.Err }

func (r *Refusal) Is(target error) bool { return target == ErrRefused || errors.Is(r.Err, target) }

func refuse(stage Stage, reason string, err error) error {
	return &Refusal{Stage: stage, Reason: reason, Err: err}
}

// InterruptedError carries the stage at which an in-flight attempt stopped.
type InterruptedError struct {
	Stage  Stage
	Reason string
	Err    error
}

func (e *InterruptedError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return fmt.Sprintf("%v at %s: %s: %v", ErrInterrupted, e.Stage, e.Reason, e.Err)
	}
	return fmt.Sprintf("%v at %s: %s", ErrInterrupted, e.Stage, e.Reason)
}

func (e *InterruptedError) Unwrap() error { return e.Err }

func (e *InterruptedError) Is(target error) bool {
	return target == ErrInterrupted || errors.Is(e.Err, target)
}

func interrupted(stage Stage, reason string, err error) error {
	return &InterruptedError{Stage: stage, Reason: reason, Err: err}
}
