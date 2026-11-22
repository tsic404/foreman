package attempt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// The fixed Job's deployment adapters. Each is an executable path supplied by
// the Job environment; the phase is passed as argv[1] and the request as one
// JSON environment variable, so operator input never reaches a shell.
const (
	EnvCLIHelper      = "FOREMAN_JOB_CLI_HELPER"
	EnvRSBXHelper     = "FOREMAN_JOB_RSBX_HELPER"
	EnvRegisterHelper = "FOREMAN_JOB_REGISTER_HELPER"
	EnvRunnerHelper   = "FOREMAN_JOB_RUNNER_HELPER"
	EnvSlotHelper     = "FOREMAN_JOB_SLOT_HELPER"

	requestEnv = "FOREMAN_JOB_REQUEST"
	maxHelper  = 64 << 10
)

// ExecCache calls the deployment's CLI fetch helper with one frozen artifact
// reference. The helper receives no other backend and must answer one
// verified local path. A cache hit is the helper's business; the digest check
// below is the Job's.
type ExecCache struct {
	Path string
	Env  []string
}

// Ensure requests exactly one artifact.
func (c ExecCache) Ensure(ctx context.Context, cli CLI) (Artifact, error) {
	var artifact Artifact
	if err := runHelper(ctx, c.Path, c.Env, "prepare", cli, &artifact); err != nil {
		return Artifact{}, err
	}
	return artifact, nil
}

// ExecRemote calls the deployment's rsbx adapter for readiness and close.
type ExecRemote struct {
	Path string
	Env  []string
}

type readyRequest struct {
	Binding Binding `json:"binding"`
}

type closeRequest struct {
	Session Session `json:"session"`
}

type registerRequest struct {
	Binding Binding `json:"binding"`
	Session Session `json:"session"`
}

// Ready opens the frozen session and answers its identity.
func (r ExecRemote) Ready(ctx context.Context, binding Binding) (Session, error) {
	var session Session
	if err := runHelper(ctx, r.Path, r.Env, "ready", readyRequest{Binding: binding}, &session); err != nil {
		var exit *helperExitError
		if errors.As(err, &exit) && exit.Code == exitSessionNotCreated {
			return Session{}, fmt.Errorf("%w: %w", ErrSessionNotCreated, err)
		}
		return Session{}, err
	}
	return session, nil
}

// Close asks the adapter whether the remote session is provably closed.
func (r ExecRemote) Close(ctx context.Context, session Session) (CloseEvidence, error) {
	var evidence CloseEvidence
	if err := runHelper(ctx, r.Path, r.Env, "close", closeRequest{Session: session}, &evidence); err != nil {
		return CloseEvidence{}, err
	}
	return evidence, nil
}

// ExecRegistrar calls the adapter that waits for the daemon registration and
// reports the identity/runtime/version the upstream acknowledged.
type ExecRegistrar struct {
	Path string
	Env  []string
}

// Register observes one registration.
func (r ExecRegistrar) Register(ctx context.Context, binding Binding, session Session) (Registration, error) {
	var registration Registration
	request := registerRequest{Binding: binding, Session: session}
	if err := runHelper(ctx, r.Path, r.Env, "register", request, &registration); err != nil {
		return Registration{}, err
	}
	return registration, nil
}

// ExecRunner calls the adapter that starts the selected Agent in the session.
type ExecRunner struct {
	Path string
	Env  []string
}

type runRequest struct {
	Binding  Binding  `json:"binding"`
	Session  Session  `json:"session"`
	Artifact Artifact `json:"artifact"`
}

type runResponse struct {
	Delivered bool   `json:"delivered"`
	ExitCode  int    `json:"exit_code,omitempty"`
	Error     string `json:"error,omitempty"`
}

// ExecSlots reports the attempt's slot settlement to the environment ledger.
// confirmed is true only when the adapter proved the remote close.
type ExecSlots struct {
	Path string
	Env  []string
}

type releaseRequest struct {
	Binding   Binding `json:"binding"`
	ClaimID   string  `json:"claim_id"`
	Confirmed bool    `json:"confirmed"`
}

type slotRequest struct {
	Binding         Binding  `json:"binding"`
	Session         *Session `json:"session,omitempty"`
	ClaimID         string   `json:"claim_id,omitempty"`
	ExpectedClaimID string   `json:"expected_claim_id,omitempty"`
}

// Status reads the shared ledger's reservation. A missing reservation is an
// answer, not an error: Exists is false.
func (s ExecSlots) Status(ctx context.Context, binding Binding) (SlotStatus, error) {
	var status SlotStatus
	if err := runHelper(ctx, s.Path, s.Env, "status", slotRequest{Binding: binding}, &status); err != nil {
		return SlotStatus{}, err
	}
	return status, nil
}

// Begin claims the idle reservation and records the unknown ready window.
func (s ExecSlots) Begin(ctx context.Context, binding Binding, claimID string) error {
	return runHelper(ctx, s.Path, s.Env, "begin", slotRequest{Binding: binding, ClaimID: claimID}, &struct{}{})
}

// TakeOver fences a reservation that may already own a session.
func (s ExecSlots) TakeOver(ctx context.Context, binding Binding, claimID, expectedClaimID string) error {
	return runHelper(ctx, s.Path, s.Env, "takeover", slotRequest{
		Binding: binding, ClaimID: claimID, ExpectedClaimID: expectedClaimID,
	}, &struct{}{})
}

// Bind pins the exact remote session the attempt opened.
func (s ExecSlots) Bind(ctx context.Context, binding Binding, claimID string, session Session) error {
	request := slotRequest{Binding: binding, ClaimID: claimID, Session: &session}
	return runHelper(ctx, s.Path, s.Env, "bind", request, &struct{}{})
}

// MarkStarted commits dispatch before any business command runs.
func (s ExecSlots) MarkStarted(ctx context.Context, binding Binding, claimID string) error {
	return runHelper(ctx, s.Path, s.Env, "started", slotRequest{Binding: binding, ClaimID: claimID}, &struct{}{})
}

// Release settles one attempt's reservation.
func (s ExecSlots) Release(ctx context.Context, binding Binding, claimID string, confirmed bool) error {
	return runHelper(ctx, s.Path, s.Env, "release", releaseRequest{Binding: binding, ClaimID: claimID, Confirmed: confirmed}, &struct{}{})
}

// Run starts one execution and reports whether the payload was delivered.
func (r ExecRunner) Run(ctx context.Context, binding Binding, session Session, agent Artifact) (RunResult, error) {
	var response runResponse
	request := runRequest{Binding: binding, Session: session, Artifact: agent}
	if err := runHelper(ctx, r.Path, r.Env, "run", request, &response); err != nil {
		return RunResult{}, err
	}
	if response.Error == "not_delivered" {
		return RunResult{Delivered: false, ExitCode: response.ExitCode}, ErrNotDelivered
	}
	if response.Error != "" {
		return RunResult{Delivered: response.Delivered, ExitCode: response.ExitCode}, errors.New(response.Error)
	}
	return RunResult{Delivered: response.Delivered, ExitCode: response.ExitCode}, nil
}

func runHelper(ctx context.Context, path string, env []string, phase string, request, answer any) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("the attempt helper path is empty")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode the attempt helper request: %w", err)
	}
	stdout := &boundedBuffer{limit: maxHelper}
	stderr := &boundedBuffer{limit: maxHelper}
	cmd := exec.CommandContext(ctx, path, phase)
	cmd.Env = append(append([]string{}, os.Environ()...), env...)
	cmd.Env = append(cmd.Env, requestEnv+"="+string(body))
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("the attempt helper did not finish: %w", ctx.Err())
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return &helperExitError{Code: exit.ExitCode(), Stderr: strings.TrimSpace(stderr.String()), err: err}
		}
		return fmt.Errorf("run the attempt helper: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(answer); err != nil {
		return errors.New("the attempt helper answered unexpected JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("the attempt helper answered trailing JSON")
	}
	return nil
}

// exitSessionNotCreated is the helper's one proven "no remote session"
// answer. Every other nonzero exit leaves the creation result unknown.
const exitSessionNotCreated = 4

type helperExitError struct {
	Code   int
	Stderr string
	err    error
}

func (e *helperExitError) Error() string {
	return fmt.Sprintf("the attempt helper exited with status %d: %s", e.Code, e.Stderr)
}

func (e *helperExitError) Unwrap() error { return e.err }

type boundedBuffer struct {
	buf   bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	remaining := b.limit - b.buf.Len()
	if remaining > 0 {
		if len(p) < remaining {
			remaining = len(p)
		}
		_, _ = b.buf.Write(p[:remaining])
	}
	return len(p), nil
}

func (b *boundedBuffer) Bytes() []byte  { return b.buf.Bytes() }
func (b *boundedBuffer) String() string { return b.buf.String() }
