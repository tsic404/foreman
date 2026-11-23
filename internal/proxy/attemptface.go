package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/registry"
)

// AttemptFacePath is the attempt projection the reference Job adapters read.
// It is Foreman's own face, outside the upstream daemon API, so it never joins
// the daemon-face census; every answer is scoped to the Job token's own
// attempt.
const AttemptFacePath = "/foreman/attempt"

// SlotLedger is the environment service's Job-side reservation seam.
type SlotLedger interface {
	SlotState(ctx context.Context, id environment.ID, slotID string) (environment.Slot, bool, error)
	BeginSlot(ctx context.Context, id environment.ID, slotID, claimID string) (environment.Slot, error)
	TakeOverSlot(ctx context.Context, id environment.ID, slotID, claimID, expectedClaimID string) (environment.Slot, error)
	BindSlot(ctx context.Context, id environment.ID, slotID, claimID, sessionID string) (environment.Slot, error)
	MarkSlotStarted(ctx context.Context, id environment.ID, slotID, claimID string) (environment.Slot, error)
	ReleaseSlotByClaim(ctx context.Context, id environment.ID, slotID, claimID string, remoteConfirmed, podClosed bool) (environment.Slot, error)
}

// AttemptFace answers the facts a Job-side adapter may rely on: what the
// attempt is, what the upstream confirmed when the daemon registered, what the
// task's delivery looks like, and what the shared ledger holds. It never
// trusts an identifier from the request: the binding comes from the token's
// own frozen attempt row.
type AttemptFace struct {
	Verifier TokenVerifier
	Registry *registry.Registry
	Slots    SlotLedger
	// Environment answers the registered environment's verified identity facts.
	// The Job-side ready check compares them with the frozen binding; nil is
	// "not observable", which the adapter must treat as no ready rather than
	// echoing the input.
	Environment func(ctx context.Context, id string) (EnvironmentFact, error)
	// PodsClosed is the trusted, same-binding platform observation the release
	// gate needs: true only when the attempt's Job has no running Pod left.
	// Nil is "unknown", never "closed": capacity is not returned on a fact the
	// face cannot observe.
	PodsClosed func(ctx context.Context, entry registry.TaskEntry, binding attempt.Binding) (bool, error)
}

// EnvironmentFact is the environment ledger's last verified identity. TreeID is
// the tree's ownership identity and Identity the target identity the
// deployment's own probe observed; both are empty until a verification succeeded.
type EnvironmentFact struct {
	TreeID   string `json:"tree_id"`
	Identity string `json:"identity"`
	State    string `json:"state"`
}

// ServeHTTP routes the projection and the ledger transitions.
func (f *AttemptFace) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	entry, binding, ok := f.resolve(w, r)
	if !ok {
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == AttemptFacePath:
		f.snapshot(w, r, entry, binding)
	case r.Method == http.MethodGet && r.URL.Path == AttemptFacePath+"/environment":
		f.environment(w, r, binding)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, AttemptFacePath+"/slots/"):
		f.transition(w, r, entry, binding)
	default:
		writeError(w, http.StatusNotFound, "no such attempt route")
	}
}

func (f *AttemptFace) resolve(w http.ResponseWriter, r *http.Request) (registry.TaskEntry, attempt.Binding, bool) {
	token, ok := bearerToken(r)
	if !ok || f.Verifier == nil {
		writeError(w, http.StatusUnauthorized, "authorization required")
		return registry.TaskEntry{}, attempt.Binding{}, false
	}
	claims, err := f.Verifier.Verify(token, auth.RequestScope{})
	if err != nil {
		writeError(w, auth.HTTPStatus(err), http.StatusText(auth.HTTPStatus(err)))
		return registry.TaskEntry{}, attempt.Binding{}, false
	}
	if claims.Role == auth.RoleCatalogProbe || claims.TaskID == "" {
		writeError(w, http.StatusForbidden, "only a task attempt may read the attempt projection")
		return registry.TaskEntry{}, attempt.Binding{}, false
	}
	if f.Registry == nil {
		writeError(w, http.StatusServiceUnavailable, "the attempt projection is not wired")
		return registry.TaskEntry{}, attempt.Binding{}, false
	}
	entry, ok := f.Registry.TaskInScope(claims.SourceID, claims.WorkspaceID, claims.TaskID)
	if !ok {
		writeError(w, http.StatusNotFound, "no such attempt")
		return registry.TaskEntry{}, attempt.Binding{}, false
	}
	binding, ok := frozenBinding(entry)
	if !ok || binding.TaskID != claims.TaskID {
		writeError(w, http.StatusNotFound, "the attempt carries no frozen binding")
		return registry.TaskEntry{}, attempt.Binding{}, false
	}
	return entry, binding, true
}

// registrationFactView answers what the upstream confirmed. It is built from
// the persisted registration fact alone: a synthesized runtime row (a shared
// read or a restart restores one for every live Job) is not a registration
// observation and never fills this view.
type registrationFactView struct {
	DaemonID  string `json:"daemon_id"`
	RuntimeID string `json:"runtime_id"`
	// Version is the Multica CLI the daemon actually runs. AgentVersion is the
	// selected Agent runtime's own version; the two are distinct facts.
	Version      string `json:"version"`
	AgentVersion string `json:"agent_version,omitempty"`
	ObservedAt   string `json:"observed_at,omitempty"`
}

type taskFactView struct {
	State  string `json:"state"`
	Result string `json:"result,omitempty"`
	// Sent is the server-side delivery commit: the payload was written to the
	// S-6 response. Delivered is arrival, proven only by a start fact or a
	// daemon-reported terminal verdict; a lost response leaves Sent true and
	// Delivered false.
	Sent                bool   `json:"sent"`
	SentAt              string `json:"sent_at,omitempty"`
	Delivered           bool   `json:"delivered"`
	Started             bool   `json:"started"`
	StartedAt           string `json:"started_at,omitempty"`
	ClaimsAfterDelivery int    `json:"claims_after_delivery,omitempty"`
}

type slotFactView struct {
	Exists    bool   `json:"exists"`
	State     string `json:"state,omitempty"`
	Phase     string `json:"phase,omitempty"`
	ClaimID   string `json:"claim_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	Started   bool   `json:"started,omitempty"`
}

type attemptSnapshotView struct {
	Attempt struct {
		TaskID        string `json:"task_id"`
		EnvironmentID string `json:"environment_id"`
		SlotID        string `json:"slot_id"`
	} `json:"attempt"`
	Registration *registrationFactView `json:"registration"`
	Task         taskFactView          `json:"task"`
	Slot         slotFactView          `json:"slot"`
}

func (f *AttemptFace) snapshot(w http.ResponseWriter, r *http.Request, entry registry.TaskEntry, binding attempt.Binding) {
	var view attemptSnapshotView
	view.Attempt.TaskID = binding.TaskID
	view.Attempt.EnvironmentID = binding.EnvironmentID
	view.Attempt.SlotID = binding.SlotID
	view.Task = taskFactView{
		State:               string(entry.State),
		Result:              string(entry.Result),
		Sent:                !entry.DeliveredAt.IsZero(),
		SentAt:              formatFactTime(entry.DeliveredAt),
		Delivered:           arrivalProven(entry),
		Started:             !entry.StartedAt.IsZero(),
		StartedAt:           formatFactTime(entry.StartedAt),
		ClaimsAfterDelivery: entry.ClaimsAfterDelivery,
	}
	if !entry.DaemonRegisteredAt.IsZero() && entry.DaemonID == binding.DaemonID && entry.JobRuntimeID == binding.JobRuntimeID {
		view.Registration = &registrationFactView{
			DaemonID:     entry.DaemonID,
			RuntimeID:    entry.JobRuntimeID,
			Version:      entry.DaemonCLIVersion,
			AgentVersion: entry.AgentRuntimeVersion,
			ObservedAt:   formatFactTime(entry.DaemonRegisteredAt),
		}
	}
	slot, err := f.slot(r.Context(), binding)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "the shared slot ledger is unreadable")
		return
	}
	view.Slot = slot
	writeJSON(w, http.StatusOK, view)
}

// environment runs the deployment's read-only probe for the attempt's
// environment and answers a fresh observation. It never reads the frozen
// binding: the adapter compares this answer with the binding itself.
func (f *AttemptFace) environment(w http.ResponseWriter, r *http.Request, binding attempt.Binding) {
	if f.Environment == nil {
		writeError(w, http.StatusServiceUnavailable, "the environment observation is not wired")
		return
	}
	fact, err := f.Environment(r.Context(), binding.EnvironmentID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "the environment observation failed")
		return
	}
	writeJSON(w, http.StatusOK, fact)
}

func (f *AttemptFace) slot(ctx context.Context, binding attempt.Binding) (slotFactView, error) {
	if f.Slots == nil {
		return slotFactView{}, errors.New("no ledger")
	}
	slot, found, err := f.Slots.SlotState(ctx, environment.ID(binding.EnvironmentID), binding.SlotID)
	if err != nil {
		return slotFactView{}, err
	}
	if !found {
		return slotFactView{Exists: false}, nil
	}
	return slotFactView{
		Exists:    true,
		State:     string(slot.State),
		Phase:     string(slot.Phase),
		ClaimID:   slot.ClaimID,
		SessionID: slot.SessionID,
		Started:   slot.Started,
	}, nil
}

type slotTransitionRequest struct {
	ClaimID         string `json:"claim_id,omitempty"`
	ExpectedClaimID string `json:"expected_claim_id,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
	Confirmed       bool   `json:"confirmed,omitempty"`
}

func (f *AttemptFace) transition(w http.ResponseWriter, r *http.Request, entry registry.TaskEntry, binding attempt.Binding) {
	if f.Slots == nil {
		writeError(w, http.StatusServiceUnavailable, "the shared slot ledger is not wired")
		return
	}
	phase := strings.TrimPrefix(r.URL.Path, AttemptFacePath+"/slots/")
	var request slotTransitionRequest
	body, ok := readLimitedBody(w, r)
	if !ok {
		return
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &request); err != nil {
			writeError(w, http.StatusBadRequest, "invalid ledger request")
			return
		}
	}
	environmentID := environment.ID(binding.EnvironmentID)
	var (
		slot environment.Slot
		err  error
	)
	switch phase {
	case "begin":
		slot, err = f.Slots.BeginSlot(r.Context(), environmentID, binding.SlotID, request.ClaimID)
	case "takeover":
		slot, err = f.Slots.TakeOverSlot(r.Context(), environmentID, binding.SlotID, request.ClaimID, request.ExpectedClaimID)
	case "bind":
		slot, err = f.Slots.BindSlot(r.Context(), environmentID, binding.SlotID, request.ClaimID, request.SessionID)
	case "started":
		slot, err = f.Slots.MarkSlotStarted(r.Context(), environmentID, binding.SlotID, request.ClaimID)
	case "release":
		podClosed := false
		if f.PodsClosed != nil {
			closed, observeErr := f.PodsClosed(r.Context(), entry, binding)
			if observeErr != nil {
				writeError(w, http.StatusServiceUnavailable, "the pod-close fact is unreadable")
				return
			}
			podClosed = closed
		}
		slot, err = f.Slots.ReleaseSlotByClaim(r.Context(), environmentID, binding.SlotID, request.ClaimID, request.Confirmed, podClosed)
	default:
		writeError(w, http.StatusNotFound, "no such ledger phase")
		return
	}
	if err != nil {
		writeError(w, ledgerStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]slotFactView{"slot": {
		Exists:    true,
		State:     string(slot.State),
		Phase:     string(slot.Phase),
		ClaimID:   slot.ClaimID,
		SessionID: slot.SessionID,
		Started:   slot.Started,
	}})
}

func ledgerStatus(err error) int {
	switch {
	case errors.Is(err, environment.ErrNotFound), errors.Is(err, environment.ErrSlotNotFound):
		return http.StatusNotFound
	case errors.Is(err, environment.ErrSlotClaimed), errors.Is(err, environment.ErrSlotIsolated),
		errors.Is(err, environment.ErrSlotPhase), errors.Is(err, environment.ErrSlotSessionBound):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func readLimitedBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "the ledger request is unreadable")
		return nil, false
	}
	if len(body) > 64<<10 {
		writeError(w, http.StatusRequestEntityTooLarge, "the ledger request is too large")
		return nil, false
	}
	return body, true
}

func formatFactTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

// arrivalProven reports whether the payload is known to have reached the
// daemon. The delivery commit alone is not arrival: the S-6 response may have
// been lost. Only a start fact proves the daemon consumed the delivery. A
// terminal state proves nothing by itself: Foreman writes the same empty
// failure reason whether the daemon reported the verdict or an upstream status
// read (C13) settled the attempt, so an empty reason cannot stand in for a
// report from this attempt's daemon. An attempt that reaches a terminal without
// a start fact stays "not observed" and the supervisor isolates it.
func arrivalProven(entry registry.TaskEntry) bool {
	return !entry.StartedAt.IsZero()
}

// frozenBinding reads one entry's frozen attempt row. Both the CLI face and
// the attempt projection answer only an attempt that actually froze its
// binding.
func frozenBinding(entry registry.TaskEntry) (attempt.Binding, bool) {
	if len(entry.FixedAttempt) == 0 {
		return attempt.Binding{}, false
	}
	var binding attempt.Binding
	if err := json.Unmarshal(entry.FixedAttempt, &binding); err != nil {
		return attempt.Binding{}, false
	}
	return binding, true
}
