package jobexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/attempt"
)

// The reference adapters. Each phase answers one JSON document and never
// reports a fact it did not observe: an observation that is missing is an
// error, and only "no session was started" is the dedicated exit status.

// Prepare fetches exactly one frozen CLI artifact from the Foreman proxy.
func Prepare(ctx context.Context, raw json.RawMessage) (any, error) {
	var cli attempt.CLI
	if err := decodeRequest(raw, &cli); err != nil {
		return nil, err
	}
	settings, err := LoadSettings()
	if err != nil {
		return nil, err
	}
	if settings.ProxyURL == "" {
		return nil, fmt.Errorf("%s (or %s) is required: the CLI comes only from the frozen proxy", EnvProxyURL, EnvControlURL)
	}
	// Every prepare phase is its own helper process, so the attempt's spent
	// bytes live on disk under a lock. The cache object below only sees the
	// remainder, which is what makes the two artifacts share one budget.
	remaining, commit, release, err := reserveAttemptBudget(settings.SessionDir, settings.CLIBudget)
	if err != nil {
		return nil, err
	}
	defer release()
	cache := &attempt.ProxyCache{
		BaseURL:  settings.ProxyURL,
		Token:    settings.Token,
		Dir:      settings.CLIDir,
		MaxBytes: remaining,
		Attempts: 2,
	}
	artifact, ensureErr := cache.Ensure(ctx, cli)
	// Bytes fetched before a failed verification still count against the
	// attempt: the budget caps downloads, not successful downloads.
	if err := errors.Join(ensureErr, commit(cache.Consumed())); err != nil {
		return nil, err
	}
	return artifact, nil
}

type readyRequest struct {
	Binding attempt.Binding `json:"binding"`
}

// Ready proves the frozen session is usable before it answers its identity.
// The proof is one bounded command through the carrier: the marker shows the
// remote view executed it. The tree generation and target identity come from
// a fresh control-plane observation compared with the frozen binding, so the
// probe never asserts a host path the daemon may not expose. A hanging
// connection, a missing marker or an unparsable TLS reference is "no session
// was created", never a ready answer echoed from the input.
func Ready(ctx context.Context, raw json.RawMessage) (any, error) {
	var request readyRequest
	if err := decodeRequest(raw, &request); err != nil {
		return nil, err
	}
	binding := request.Binding
	for name, value := range map[string]string{
		"attempt id": binding.AttemptID, "environment id": binding.EnvironmentID,
		"environment generation": binding.EnvironmentGen, "endpoint": binding.Endpoint,
	} {
		if value == "" {
			return nil, Exit(ExitSessionNotCreated, fmt.Errorf("the frozen binding carries no %s", name))
		}
	}
	settings, err := LoadSettings()
	if err != nil {
		return nil, err
	}
	dir := settings.attemptDir(binding.AttemptID)
	// Ready answers an environment identity, so it needs an actual observation
	// of the environment's verified tree and identity, read afresh from the
	// control plane. The frozen binding alone is an input snapshot: comparing it
	// with itself (or with a directory that still exists) proves nothing.
	control, err := settings.Control()
	if err != nil {
		return nil, Exit(ExitSessionNotCreated, err)
	}
	observedGeneration, observeErr := observeEnvironment(ctx, control, binding)
	if observeErr != nil {
		return nil, Exit(ExitSessionNotCreated, observeErr)
	}
	if record, ok, err := loadRecord(dir); err == nil && ok && record.ID == SessionID(binding) && processAlive(record.PID) {
		return record.Session(), nil
	}
	target, err := resolveSessionTarget(binding, settings)
	if err != nil {
		return nil, Exit(ExitSessionNotCreated, err)
	}
	entry := settings.Entry()
	if err := ensureExecutable(entry[0]); err != nil {
		return nil, Exit(ExitSessionNotCreated, err)
	}
	if err := ensureExecutable(settings.RsbxBinary); err != nil {
		return nil, Exit(ExitSessionNotCreated, err)
	}
	if err := probeSession(ctx, binding, settings, target); err != nil {
		// The probe runs before any session starts, so every failure — a
		// hang, a missing marker, an unreachable target — is "no session was
		// created".
		return nil, Exit(ExitSessionNotCreated, err)
	}
	record, started, err := startSession(binding, settings, target, observedGeneration)
	if err != nil {
		if !started {
			return nil, Exit(ExitSessionNotCreated, err)
		}
		return nil, err
	}
	if err := waitSession(ctx, record, settings.SessionSettle); err != nil {
		_ = stopProcess(record.PID, settings.SessionStopGrace)
		removeRecord(dir)
		if errors.Is(err, ErrDaemonUnreachable) {
			return nil, Exit(ExitSessionNotCreated, err)
		}
		return nil, err
	}
	return record.Session(), nil
}

// observeEnvironment reads the verified environment identity and compares it
// with the frozen binding. Both the tree generation and the target identity
// must be observed and equal; a tree replaced under the same path or a target
// whose identity no longer matches the frozen claim refuses the session.
func observeEnvironment(ctx context.Context, control *Control, binding attempt.Binding) (string, error) {
	fact, err := control.ObserveEnvironment(ctx)
	if err != nil {
		return "", fmt.Errorf("the environment's verified identity is not observable: %w", err)
	}
	if strings.TrimSpace(fact.TreeID) == "" || strings.TrimSpace(fact.Identity) == "" {
		return "", errors.New("the environment's verified tree identity and target identity are not both observable")
	}
	if fact.TreeID != binding.EnvironmentGen {
		return "", fmt.Errorf("the observed tree generation %q is not the frozen %q", fact.TreeID, binding.EnvironmentGen)
	}
	if binding.EnvironmentIdentity == "" || fact.Identity != binding.EnvironmentIdentity {
		return "", fmt.Errorf("the observed target identity %q is not the frozen %q", fact.Identity, binding.EnvironmentIdentity)
	}
	return fact.TreeID, nil
}

type closeRequest struct {
	Session attempt.Session `json:"session"`
}

// Close stops the local carrier and answers only what it could observe. The
// carrier has no close protocol: a local process exit proves the connection
// ended, not that the remote session did. The answer is therefore always
// "not confirmed", and the record stays behind as the reconciliation hint.
func Close(_ context.Context, raw json.RawMessage) (any, error) {
	var request closeRequest
	if err := decodeRequest(raw, &request); err != nil {
		return nil, err
	}
	settings, err := LoadSettings()
	if err != nil {
		return attempt.CloseEvidence{Confirmed: false, Detail: err.Error()}, nil
	}
	record, err := resolveSession(settings, request.Session)
	if err != nil {
		return attempt.CloseEvidence{Confirmed: false, Detail: err.Error()}, nil
	}
	if err := stopProcess(record.PID, settings.SessionStopGrace); err != nil {
		return attempt.CloseEvidence{Confirmed: false, Detail: err.Error()}, nil
	}
	return attempt.CloseEvidence{
		Confirmed: false,
		Detail: fmt.Sprintf("session process %d exited locally; the carrier offers no remote close confirmation, so the session stays unknown",
			record.PID),
	}, nil
}

type registerRequest struct {
	Binding attempt.Binding `json:"binding"`
	Session attempt.Session `json:"session"`
}

// Register waits for the registration Foreman actually recorded and answers
// exactly that. It never answers a success it did not observe.
func Register(ctx context.Context, raw json.RawMessage) (any, error) {
	var request registerRequest
	if err := decodeRequest(raw, &request); err != nil {
		return nil, err
	}
	settings, err := LoadSettings()
	if err != nil {
		return nil, err
	}
	control, err := settings.Control()
	if err != nil {
		return nil, err
	}
	record, err := resolveSession(settings, request.Session)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(settings.RegistrationGrace)
	var lastErr error
	for {
		snapshot, snapshotErr := control.Snapshot(ctx)
		if snapshotErr != nil {
			lastErr = snapshotErr
		} else if registration := snapshot.Registration; registration != nil && registration.DaemonID != "" && registration.RuntimeID != "" {
			return attempt.Registration{
				DaemonID:     registration.DaemonID,
				RuntimeID:    registration.RuntimeID,
				Version:      registration.Version,
				AgentVersion: registration.AgentVersion,
			}, nil
		}
		if !processAlive(record.PID) {
			return nil, fmt.Errorf("the session process exited before the daemon registered: %s", logTail(record.LogPath))
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return nil, fmt.Errorf("no daemon registration was observed within %s: %w", settings.RegistrationGrace, lastErr)
			}
			return nil, fmt.Errorf("no daemon registration was observed within %s", settings.RegistrationGrace)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

type runRequest struct {
	Binding  attempt.Binding  `json:"binding"`
	Session  attempt.Session  `json:"session"`
	Artifact attempt.Artifact `json:"artifact"`
}

type runAnswer struct {
	Delivered bool   `json:"delivered"`
	ExitCode  int    `json:"exit_code,omitempty"`
	Error     string `json:"error,omitempty"`
}

// Run observes the one dispatch this attempt may make. The payload is
// delivered by the daemon the session started; this adapter separates the
// server's send commit from actual arrival. Arrival is proven only by a start
// fact or the daemon's own terminal report. An observation gap after a
// not-arrived read is an error, never an upgrade to delivered: the attempt is
// isolated and never replayed.
func Run(ctx context.Context, raw json.RawMessage) (any, error) {
	var request runRequest
	if err := decodeRequest(raw, &request); err != nil {
		return nil, err
	}
	settings, err := LoadSettings()
	if err != nil {
		return nil, err
	}
	control, err := settings.Control()
	if err != nil {
		return nil, err
	}
	record, err := resolveSession(settings, request.Session)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(settings.DeliveryTimeout)
	// One deadline owns every path of this dispatch: each projection read, the
	// final read after the carrier exits, and the wait between reads. The
	// helper's own context carries only signal cancellation, so a request that
	// hangs during the final observation must still end with this window
	// instead of stretching it to the client's separate HTTP timeout.
	dispatchCtx, cancelDispatch := context.WithDeadline(ctx, deadline)
	defer cancelDispatch()
	var lastErr error
	var last TaskFact
	for {
		// The dispatch deadline bounds every path, including a projection that
		// keeps failing after one successful read. The dispatch context ends
		// with the deadline, so a hanging request cannot outlive it either.
		snapshot, snapshotErr := control.Snapshot(dispatchCtx)
		if snapshotErr != nil {
			lastErr = snapshotErr
		} else {
			lastErr = nil
			last = snapshot.Task
			if snapshot.Task.State == TaskStateTerminal {
				return runAnswer{Delivered: snapshot.Task.Delivered, ExitCode: exitCodeFor(snapshot.Task.Result)}, nil
			}
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				// The projection stopped answering: the attempt is unknown, so
				// the supervisor isolates it instead of replaying or guessing.
				return nil, fmt.Errorf("the dispatch could not be observed within %s: %w", settings.DeliveryTimeout, lastErr)
			}
			if last.Delivered {
				// Arrived and still running: the attempt is never replayed on
				// another endpoint, but it did not finish here either.
				return runAnswer{Delivered: true, Error: "the payload arrived but the attempt did not finish within the dispatch window"}, nil
			}
			return runAnswer{Delivered: false, Error: "not_delivered"}, nil
		}
		if !processAlive(record.PID) {
			final, finalErr := control.Snapshot(dispatchCtx)
			if finalErr != nil {
				// A missing final observation cannot prove arrival. Returning
				// the error makes the supervisor isolate the attempt without
				// replaying it, which is the only safe answer for "unknown".
				return nil, fmt.Errorf("the session exited before the dispatch could be confirmed: %w", finalErr)
			}
			switch {
			case final.Task.State == TaskStateTerminal:
				return runAnswer{Delivered: final.Task.Delivered, ExitCode: exitCodeFor(final.Task.Result)}, nil
			case final.Task.Delivered:
				return runAnswer{Delivered: true, Error: "the session exited after delivery; the attempt is not replayed"}, nil
			default:
				return runAnswer{Delivered: false, Error: "not_delivered"}, nil
			}
		}
		// The wait is bounded by the remaining window too, so the loop cannot
		// overshoot the deadline by a full poll interval.
		wait := time.Until(deadline)
		if wait > time.Second {
			wait = time.Second
		}
		if wait < 0 {
			wait = 0
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

// exitCodeFor maps the server's verdict onto an exit status. The verdict
// itself is the server's; this adapter does not re-judge the task.
func exitCodeFor(result string) int {
	if result == "completed" {
		return 0
	}
	return 1
}

type slotRequest struct {
	Binding         attempt.Binding  `json:"binding"`
	Session         *attempt.Session `json:"session,omitempty"`
	ClaimID         string           `json:"claim_id,omitempty"`
	ExpectedClaimID string           `json:"expected_claim_id,omitempty"`
}

type releaseRequest struct {
	Binding   attempt.Binding `json:"binding"`
	ClaimID   string          `json:"claim_id,omitempty"`
	Confirmed bool            `json:"confirmed,omitempty"`
}

// Status reads the shared ledger before any session opens.
func Status(ctx context.Context, raw json.RawMessage) (any, error) {
	var request slotRequest
	if err := decodeRequest(raw, &request); err != nil {
		return nil, err
	}
	control, err := controlFromSettings()
	if err != nil {
		return nil, err
	}
	snapshot, err := control.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return attempt.SlotStatus{
		Exists:    snapshot.Slot.Exists,
		State:     snapshot.Slot.State,
		Phase:     snapshot.Slot.Phase,
		ClaimID:   snapshot.Slot.ClaimID,
		SessionID: snapshot.Slot.SessionID,
		Started:   snapshot.Slot.Started,
	}, nil
}

// Begin claims the idle reservation.
func Begin(ctx context.Context, raw json.RawMessage) (any, error) {
	var request slotRequest
	if err := decodeRequest(raw, &request); err != nil {
		return nil, err
	}
	return commitSlot(ctx, "begin", SlotRequest{ClaimID: request.ClaimID})
}

// TakeOver fences a reservation that may already own a session.
func TakeOver(ctx context.Context, raw json.RawMessage) (any, error) {
	var request slotRequest
	if err := decodeRequest(raw, &request); err != nil {
		return nil, err
	}
	return commitSlot(ctx, "takeover", SlotRequest{ClaimID: request.ClaimID, ExpectedClaimID: request.ExpectedClaimID})
}

// Bind pins the exact session this attempt opened.
func Bind(ctx context.Context, raw json.RawMessage) (any, error) {
	var request slotRequest
	if err := decodeRequest(raw, &request); err != nil {
		return nil, err
	}
	if request.Session == nil || request.Session.ID == "" {
		return nil, errors.New("bind needs the session this attempt opened")
	}
	return commitSlot(ctx, "bind", SlotRequest{ClaimID: request.ClaimID, SessionID: request.Session.ID})
}

// MarkStarted commits the dispatch.
func MarkStarted(ctx context.Context, raw json.RawMessage) (any, error) {
	var request slotRequest
	if err := decodeRequest(raw, &request); err != nil {
		return nil, err
	}
	return commitSlot(ctx, "started", SlotRequest{ClaimID: request.ClaimID})
}

// Release settles the reservation. Only a proven remote close frees capacity.
func Release(ctx context.Context, raw json.RawMessage) (any, error) {
	var request releaseRequest
	if err := decodeRequest(raw, &request); err != nil {
		return nil, err
	}
	return commitSlot(ctx, "release", SlotRequest{ClaimID: request.ClaimID, Confirmed: request.Confirmed})
}

func commitSlot(ctx context.Context, phase string, request SlotRequest) (any, error) {
	control, err := controlFromSettings()
	if err != nil {
		return nil, err
	}
	if _, err := control.Slot(ctx, phase, request); err != nil {
		return nil, err
	}
	return struct{}{}, nil
}

func controlFromSettings() (*Control, error) {
	settings, err := LoadSettings()
	if err != nil {
		return nil, err
	}
	return settings.Control()
}
