package attempt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// Cache makes one frozen CLI artifact available locally. It must never fetch
// or install another backend: preparation is the exact two-entry set.
type Cache interface {
	Ensure(ctx context.Context, cli CLI) (Artifact, error)
}

// Remote is the frozen rsbx session: ready, close, and no endpoint discovery.
type Remote interface {
	Ready(ctx context.Context, binding Binding) (Session, error)
	Close(ctx context.Context, session Session) (CloseEvidence, error)
}

// Registrar is the daemon registration fact the Job observes before dispatch.
type Registrar interface {
	Register(ctx context.Context, binding Binding, session Session) (Registration, error)
}

// Runner executes the selected Agent inside the remote session.
type Runner interface {
	Run(ctx context.Context, binding Binding, session Session, agent Artifact) (RunResult, error)
}

// Slots is the attempt's shared reservation ledger. Status is the authority a
// rebuilt Job reads before doing anything: it carries the original session,
// the current execution claim and whether dispatch was committed. Begin is an
// atomic winner for the idle reservation; TakeOver is a compare-and-swap fence
// for recovery. Bind pins the exact session; MarkStarted commits dispatch.
// Release with confirmed=false keeps the reservation isolated.
type Slots interface {
	Status(ctx context.Context, binding Binding) (SlotStatus, error)
	Begin(ctx context.Context, binding Binding, claimID string) error
	TakeOver(ctx context.Context, binding Binding, claimID, expectedClaimID string) error
	Bind(ctx context.Context, binding Binding, claimID string, session Session) error
	MarkStarted(ctx context.Context, binding Binding, claimID string) error
	Release(ctx context.Context, binding Binding, claimID string, confirmed bool) error
}

// Supervisor runs the fixed foreman-job's three stages. Every seam is
// injected: the Job-side binary wires exec adapters, the acceptance fixtures
// wire scripted fakes.
type Supervisor struct {
	Cache     Cache
	Remote    Remote
	Registrar Registrar
	Runner    Runner
	Slots     Slots
	Store     Store
	Logger    *slog.Logger
	// NewClaim mints one execution lease. Nil uses NewClaimID.
	NewClaim func() string
}

// Run performs one attempt. A returned *Refusal means no business command
// ran; a returned *InterruptedError means dispatch began and must not be
// replayed on another endpoint.
func (s *Supervisor) Run(ctx context.Context, binding Binding) (Outcome, error) {
	out := Outcome{Stage: StagePreparing}
	if err := binding.Validate(); err != nil {
		out.Stage = StageRefused
		return out, refuse(StagePreparing, "binding_invalid", err)
	}
	if s.Cache == nil || s.Remote == nil || s.Registrar == nil || s.Runner == nil || s.Slots == nil {
		out.Stage = StageRefused
		return out, refuse(StagePreparing, "wiring_incomplete", errors.New("a required attempt seam is not wired"))
	}
	store := s.Store
	if store == nil {
		store = NewMemoryStore()
	}

	record, found, err := store.Load(ctx, binding.AttemptID)
	if err != nil {
		out.Stage = StageRefused
		return out, refuse(StagePreparing, "store_unreadable", err)
	}
	if found {
		if record.EnvironmentID != "" && record.EnvironmentID != binding.EnvironmentID {
			out.Stage = StageRefused
			out.Session = Session{ID: record.SessionID, EnvironmentID: record.EnvironmentID, Endpoint: record.Endpoint}
			return out, refuse(StageConnecting, "session_relocated", ErrSessionRelocated)
		}
		if record.Endpoint != "" && record.Endpoint != binding.Endpoint {
			out.Stage = StageRefused
			out.Session = Session{ID: record.SessionID, EnvironmentID: record.EnvironmentID, Endpoint: record.Endpoint}
			return out, refuse(StageConnecting, "session_relocated", ErrSessionRelocated)
		}
	}

	status, err := s.Slots.Status(ctx, binding)
	if err != nil {
		out.Stage = StageRefused
		return out, refuse(StagePreparing, "slot_unreadable", err)
	}
	if status.SessionID != "" && record.SessionID != "" && status.SessionID != record.SessionID {
		out.Stage = StageIsolated
		out.Isolated = true
		out.Session = Session{ID: status.SessionID, EnvironmentID: binding.EnvironmentID, Generation: binding.EnvironmentGen, Endpoint: binding.Endpoint}
		claim, claimErr := s.takeOver(ctx, binding, status)
		if claimErr != nil {
			out.Stage = StageRefused
			out.Isolated = false
			return out, refuse(StageConnecting, "slot_not_claimed", claimErr)
		}
		s.release(ctx, binding, claim, false, &out, "session_identity_conflict")
		return out, refuse(StageConnecting, "session_identity_conflict", errors.New("the reservation and the local record name different sessions"))
	}
	if binding.SessionID != "" && status.SessionID != "" && status.SessionID != binding.SessionID {
		out.Stage = StageIsolated
		out.Isolated = true
		out.Session = Session{ID: status.SessionID, EnvironmentID: binding.EnvironmentID, Generation: binding.EnvironmentGen, Endpoint: binding.Endpoint}
		claim, claimErr := s.takeOver(ctx, binding, status)
		if claimErr != nil {
			out.Stage = StageRefused
			out.Isolated = false
			return out, refuse(StageConnecting, "slot_not_claimed", claimErr)
		}
		s.release(ctx, binding, claim, false, &out, "session_identity_conflict")
		return out, refuse(StageConnecting, "session_identity_conflict", errors.New("the reservation session is not the frozen session"))
	}
	if found && record.Closed && !record.Isolated && status.State != SlotIsolated {
		// The remote close was already proven. A rebuilt Job only reports the
		// terminal state; it never opens a second session or re-dispatches.
		out.Session = Session{ID: record.SessionID, EnvironmentID: record.EnvironmentID, Generation: record.Generation, Endpoint: record.Endpoint}
		out.Delivered = record.Delivered
		if status.Exists {
			claim, claimErr := s.takeOver(ctx, binding, status)
			if claimErr != nil {
				out.Stage = StageRefused
				return out, refuse(StagePreparing, "slot_not_claimed", claimErr)
			}
			s.release(ctx, binding, claim, true, &out, "already_closed")
		}
		if record.Started {
			out.Stage = StageInterrupted
			return out, interrupted(StageDispatching, "already_started", errors.New("the attempt already dispatched business work"))
		}
		out.Stage = StageClosed
		return out, nil
	}

	started := status.Started || status.Phase == SlotPhaseStarted || (found && record.Started)
	isolated := status.State == SlotIsolated || (found && record.Isolated)
	if !started && status.Exists && status.Phase == SlotPhaseOpening && status.SessionID == "" {
		// A previous process began opening a session and stopped before the
		// session id could be recorded. The remote result is unknown.
		isolated = true
	}
	prior := priorSession(binding, record, found, status)

	if !status.Exists {
		if found && (record.Started || record.Isolated || record.SessionID != "") {
			return s.recoverPrior(ctx, store, binding, record, prior, started, false, "", &out)
		}
		if found && record.Closed {
			out.Stage = StageClosed
			out.Delivered = record.Delivered
			return out, nil
		}
		out.Stage = StageRefused
		return out, refuse(StagePreparing, "slot_missing", errors.New("the attempt has no shared reservation"))
	}
	if started || isolated || prior.ID != "" {
		claim, claimErr := s.takeOver(ctx, binding, status)
		if claimErr != nil {
			out.Stage = StageRefused
			return out, refuse(StagePreparing, "slot_not_claimed", claimErr)
		}
		return s.recoverPrior(ctx, store, binding, record, prior, started, true, claim, &out)
	}

	// The reservation is idle. Mark the unknown window before the remote call,
	// so a rebuilt Job can isolate any interrupted ready attempt.
	claim := s.mintClaim()
	if err := s.Slots.Begin(ctx, binding, claim); err != nil {
		out.Stage = StageRefused
		return out, refuse(StagePreparing, "slot_not_claimed", err)
	}

	// ① 只准备所需 CLI.
	prepared, err := s.prepare(ctx, binding)
	out.Prepared = prepared
	if err != nil {
		out.Stage = StageRefused
		s.release(ctx, binding, claim, true, &out, "prepare_failed")
		return out, refuse(StagePreparing, "cli_prepare_failed", err)
	}

	// ② 启动 rsbx 并等待 ready，再向 Foreman 注册并核对身份.
	out.Stage = StageConnecting
	session, err := s.Remote.Ready(ctx, binding)
	if err != nil {
		if errors.Is(err, ErrSessionNotCreated) {
			out.Stage = StageRefused
			s.release(ctx, binding, claim, true, &out, "remote_not_created")
			return out, refuse(StageConnecting, "remote_not_ready", err)
		}
		record = Record{
			AttemptID: binding.AttemptID, EnvironmentID: binding.EnvironmentID,
			Generation: binding.EnvironmentGen, Endpoint: binding.Endpoint, Isolated: true,
		}
		if saveErr := store.Save(ctx, record); saveErr != nil {
			s.log().Warn("attempt.isolated_record_failed", "attempt_id", binding.AttemptID, "err", saveErr.Error())
		}
		out.Stage = StageIsolated
		out.Isolated = true
		s.release(ctx, binding, claim, false, &out, "remote_ready_unknown")
		return out, refuse(StageConnecting, "remote_ready_unknown", err)
	}
	if err := verifySession(binding, session); err != nil {
		out.Stage = StageRefused
		out.Session = session
		s.bindBestEffort(ctx, binding, claim, session)
		s.close(ctx, store, binding, claim, baseRecord(binding, session), session, &out)
		return out, refuse(StageConnecting, "remote_identity_mismatch", err)
	}
	out.Session = session

	if err := s.Slots.Bind(ctx, binding, claim, session); err != nil {
		out.Stage = StageRefused
		s.close(ctx, store, binding, claim, baseRecord(binding, session), session, &out)
		return out, refuse(StageConnecting, "session_not_bound", err)
	}
	record = baseRecord(binding, session)
	if err := store.Save(ctx, record); err != nil {
		out.Stage = StageRefused
		s.close(ctx, store, binding, claim, record, session, &out)
		return out, refuse(StageConnecting, "session_not_persisted", err)
	}

	out.Stage = StageRegistering
	registration, err := s.Registrar.Register(ctx, binding, session)
	if err != nil {
		out.Stage = StageRefused
		s.close(ctx, store, binding, claim, record, session, &out)
		return out, refuse(StageRegistering, "registration_failed", err)
	}
	out.Registration = registration
	if err := verifyRegistration(binding, prepared, registration); err != nil {
		out.Stage = StageRefused
		s.close(ctx, store, binding, claim, record, session, &out)
		return out, refuse(StageRegistering, "registration_mismatch", err)
	}

	// ③ 派发并执行. The shared ledger commits the dispatch before the local
	// record, so a rebuilt Pod with no local file still refuses to replay.
	agent, ok := preparedArtifact(prepared, KindAgent)
	if !ok {
		out.Stage = StageRefused
		s.close(ctx, store, binding, claim, record, session, &out)
		return out, refuse(StagePreparing, "agent_artifact_missing", errors.New("the selected Agent CLI was not prepared"))
	}
	if err := s.Slots.MarkStarted(ctx, binding, claim); err != nil {
		out.Stage = StageRefused
		s.close(ctx, store, binding, claim, record, session, &out)
		return out, refuse(StageDispatching, "dispatch_not_marked", err)
	}
	record.Started = true
	if err := store.Save(ctx, record); err != nil {
		out.Stage = StageRefused
		s.close(ctx, store, binding, claim, record, session, &out)
		return out, refuse(StageDispatching, "dispatch_not_persisted", err)
	}
	out.Stage = StageDispatching
	result, runErr := s.Runner.Run(ctx, binding, session, agent)
	switch {
	case errors.Is(runErr, ErrNotDelivered):
		record.Delivered = false
		_ = store.Save(ctx, record)
		out.Stage = StageRefused
		s.close(ctx, store, binding, claim, record, session, &out)
		if out.Isolated {
			out.Stage = StageIsolated
		}
		return out, refuse(StageDispatching, "not_delivered", runErr)
	case runErr != nil:
		out.Stage = StageInterrupted
		out.Delivered = result.Delivered
		record.Delivered = result.Delivered
		_ = store.Save(ctx, record)
		s.close(ctx, store, binding, claim, record, session, &out)
		return out, interrupted(StageDispatching, "dispatch_interrupted", runErr)
	case !result.Delivered:
		record.Delivered = false
		_ = store.Save(ctx, record)
		out.Stage = StageRefused
		s.close(ctx, store, binding, claim, record, session, &out)
		if out.Isolated {
			out.Stage = StageIsolated
		}
		return out, refuse(StageDispatching, "not_delivered", ErrNotDelivered)
	}
	out.Delivered = true
	record.Delivered = true
	_ = store.Save(ctx, record)

	// Settle both sides. An unconfirmed remote close keeps the slot isolated.
	out.Stage = StageClosing
	s.close(ctx, store, binding, claim, record, session, &out)
	if out.Isolated {
		out.Stage = StageIsolated
		return out, nil
	}
	out.Stage = StageClosed
	return out, nil
}

// recoverPrior handles any reservation that may already own a remote session.
// It never prepares a CLI or opens a new session: the original session is
// closed first, and an unproven close leaves the slot isolated.
func (s *Supervisor) recoverPrior(ctx context.Context, store Store, binding Binding, record Record, prior Session, started, canRelease bool, claim string, out *Outcome) (Outcome, error) {
	if record.AttemptID == "" {
		record.AttemptID = binding.AttemptID
	}
	if record.EnvironmentID == "" {
		record.EnvironmentID = binding.EnvironmentID
	}
	if record.Endpoint == "" {
		record.Endpoint = binding.Endpoint
	}
	if record.Generation == "" {
		record.Generation = binding.EnvironmentGen
	}
	if prior.ID == "" {
		record.Isolated = true
		if err := store.Save(ctx, record); err != nil {
			s.log().Warn("attempt.isolated_record_failed", "attempt_id", binding.AttemptID, "err", err.Error())
		}
		out.Stage = StageIsolated
		out.Isolated = true
		if canRelease {
			s.release(ctx, binding, claim, false, out, "unknown_session")
		}
		if started {
			return *out, interrupted(StageDispatching, "already_started", errors.New("the attempt already dispatched business work"))
		}
		return *out, refuse(StageConnecting, "previous_session_unknown", errors.New("the previous remote session is not provably closed"))
	}

	out.Session = prior
	if canRelease {
		s.close(ctx, store, binding, claim, record, prior, out)
	} else {
		s.closeOnly(ctx, store, binding, record, prior, out)
	}
	if out.Close.Confirmed {
		out.Stage = StageClosed
	} else {
		out.Stage = StageIsolated
	}
	if started {
		if out.Close.Confirmed && record.Delivered {
			return *out, nil
		}
		return *out, interrupted(StageDispatching, "already_started", errors.New("the attempt already dispatched business work"))
	}
	if out.Close.Confirmed {
		return *out, refuse(StageConnecting, "previous_session_closed", errors.New("the previous session was closed without dispatch"))
	}
	return *out, refuse(StageConnecting, "previous_session_isolated", errors.New("the previous session is not provably closed"))
}

func (s *Supervisor) prepare(ctx context.Context, binding Binding) ([]Artifact, error) {
	order := []string{KindMultica, KindAgent}
	prepared := make([]Artifact, 0, len(order))
	for _, kind := range order {
		want, ok := binding.cli(kind)
		if !ok {
			return prepared, fmt.Errorf("%s CLI is missing from the attempt", kind)
		}
		artifact, err := s.Cache.Ensure(ctx, want)
		if err != nil {
			return prepared, fmt.Errorf("prepare %s CLI: %w", kind, err)
		}
		if err := verifyArtifact(want, artifact); err != nil {
			return prepared, err
		}
		prepared = append(prepared, artifact)
	}
	return prepared, nil
}

func verifyArtifact(want CLI, got Artifact) error {
	switch {
	case got.Kind != want.Kind:
		return fmt.Errorf("%s CLI resolved as %q", want.Kind, got.Kind)
	case got.Version != want.Version:
		return fmt.Errorf("%s CLI version %q does not match the frozen %q", want.Kind, got.Version, want.Version)
	case got.Platform != want.Platform:
		return fmt.Errorf("%s CLI platform %q does not match the frozen %q", want.Kind, got.Platform, want.Platform)
	case got.Digest != want.Digest:
		return fmt.Errorf("%s CLI digest %q does not match the frozen %q", want.Kind, got.Digest, want.Digest)
	case got.Path == "":
		return fmt.Errorf("%s CLI has no local path", want.Kind)
	}
	return nil
}

// verifySession requires the complete frozen identity. A missing generation or
// endpoint is not a wildcard: an incomplete ready answer is rejected.
func verifySession(binding Binding, session Session) error {
	switch {
	case session.ID == "":
		return errors.New("rsbx reported no session id")
	case session.EnvironmentID == "":
		return errors.New("rsbx reported no environment id")
	case session.EnvironmentID != binding.EnvironmentID:
		return fmt.Errorf("rsbx session environment %q is not the frozen %q", session.EnvironmentID, binding.EnvironmentID)
	case session.Generation == "":
		return errors.New("rsbx reported no tree generation")
	case session.Generation != binding.EnvironmentGen:
		return fmt.Errorf("rsbx session generation %q is not the frozen %q", session.Generation, binding.EnvironmentGen)
	case session.Endpoint == "":
		return errors.New("rsbx reported no endpoint")
	case session.Endpoint != binding.Endpoint:
		return fmt.Errorf("rsbx session endpoint %q is not the frozen %q", session.Endpoint, binding.Endpoint)
	case binding.SessionID != "" && session.ID != binding.SessionID:
		return fmt.Errorf("rsbx session id %q is not the frozen %q", session.ID, binding.SessionID)
	}
	return nil
}

func verifyRegistration(binding Binding, prepared []Artifact, registration Registration) error {
	switch {
	case registration.DaemonID != binding.DaemonID:
		return fmt.Errorf("daemon registered as %q, want %q", registration.DaemonID, binding.DaemonID)
	case registration.RuntimeID != binding.JobRuntimeID:
		return fmt.Errorf("runtime registered as %q, want %q", registration.RuntimeID, binding.JobRuntimeID)
	}
	multica, ok := preparedArtifact(prepared, KindMultica)
	if !ok {
		return errors.New("the Multica CLI artifact is missing")
	}
	if registration.Version != multica.Version {
		return fmt.Errorf("daemon registered version %q, want %q", registration.Version, multica.Version)
	}
	return nil
}

func preparedArtifact(prepared []Artifact, kind string) (Artifact, bool) {
	for _, artifact := range prepared {
		if artifact.Kind == kind {
			return artifact, true
		}
	}
	return Artifact{}, false
}

func priorSession(binding Binding, record Record, found bool, status SlotStatus) Session {
	id := status.SessionID
	if id == "" && found && !record.Closed {
		id = record.SessionID
	}
	if id == "" {
		return Session{}
	}
	return Session{
		ID:            id,
		EnvironmentID: firstNonEmpty(record.EnvironmentID, binding.EnvironmentID),
		Generation:    firstNonEmpty(record.Generation, binding.EnvironmentGen),
		Endpoint:      firstNonEmpty(record.Endpoint, binding.Endpoint),
	}
}

func baseRecord(binding Binding, session Session) Record {
	return Record{
		AttemptID: binding.AttemptID, EnvironmentID: binding.EnvironmentID,
		Generation: binding.EnvironmentGen, Endpoint: binding.Endpoint,
		SessionID: session.ID,
	}
}

func (s *Supervisor) bindBestEffort(ctx context.Context, binding Binding, claim string, session Session) {
	if err := s.Slots.Bind(ctx, binding, claim, session); err != nil {
		s.log().Warn("attempt.session_not_bound", "attempt_id", binding.AttemptID, "err", err.Error())
	}
}

func (s *Supervisor) mintClaim() string {
	if s.NewClaim != nil {
		return s.NewClaim()
	}
	return NewClaimID()
}

// takeOver fences the execution that owned the slot the rebuilt Job read.
func (s *Supervisor) takeOver(ctx context.Context, binding Binding, status SlotStatus) (string, error) {
	claim := s.mintClaim()
	if err := s.Slots.TakeOver(ctx, binding, claim, status.ClaimID); err != nil {
		return "", err
	}
	return claim, nil
}

func (s *Supervisor) release(ctx context.Context, binding Binding, claim string, confirmed bool, out *Outcome, reason string) {
	if s.Slots == nil {
		return
	}
	if err := s.Slots.Release(ctx, binding, claim, confirmed); err != nil {
		out.Detail = reason + ": release failed: " + err.Error()
		s.log().Warn("attempt.slot_release_failed",
			"attempt_id", binding.AttemptID, "environment_id", binding.EnvironmentID,
			"confirmed", confirmed, "err", err.Error())
		return
	}
	out.SlotReleased = true
}

func (s *Supervisor) close(ctx context.Context, store Store, binding Binding, claim string, record Record, session Session, out *Outcome) {
	s.closeOnly(ctx, store, binding, record, session, out)
	s.release(ctx, binding, claim, out.Close.Confirmed, out, "remote_close")
}

func (s *Supervisor) closeOnly(ctx context.Context, store Store, binding Binding, record Record, session Session, out *Outcome) {
	evidence, err := s.Remote.Close(ctx, session)
	if err != nil {
		out.Close = CloseEvidence{Confirmed: false, Detail: err.Error()}
		out.Detail = "remote close failed: " + err.Error()
	} else {
		out.Close = evidence
	}
	out.Isolated = !out.Close.Confirmed
	record.Closed = out.Close.Confirmed
	record.Isolated = out.Isolated
	if record.AttemptID != "" {
		if err := store.Save(ctx, record); err != nil {
			s.log().Warn("attempt.record_close_failed", "attempt_id", record.AttemptID, "err", err.Error())
		}
	}
}

func (s *Supervisor) log() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
