package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/registry"
)

type fakeAttemptVerifier struct {
	claims auth.Claims
	err    error
}

func (f fakeAttemptVerifier) Verify(string, auth.RequestScope) (auth.Claims, error) {
	return f.claims, f.err
}

type fakeSlotLedger struct {
	slot      environment.Slot
	exists    bool
	err       error
	releases  []bool
	podClosed []bool
}

func (f *fakeSlotLedger) SlotState(context.Context, environment.ID, string) (environment.Slot, bool, error) {
	return f.slot, f.exists, f.err
}

func (f *fakeSlotLedger) BeginSlot(context.Context, environment.ID, string, string) (environment.Slot, error) {
	return f.slot, f.err
}

func (f *fakeSlotLedger) TakeOverSlot(context.Context, environment.ID, string, string, string) (environment.Slot, error) {
	return f.slot, f.err
}

func (f *fakeSlotLedger) BindSlot(context.Context, environment.ID, string, string, string) (environment.Slot, error) {
	return f.slot, f.err
}

func (f *fakeSlotLedger) MarkSlotStarted(context.Context, environment.ID, string, string) (environment.Slot, error) {
	return f.slot, f.err
}

func (f *fakeSlotLedger) ReleaseSlotByClaim(_ context.Context, _ environment.ID, _, _ string, remoteConfirmed, podClosed bool) (environment.Slot, error) {
	f.releases = append(f.releases, remoteConfirmed)
	f.podClosed = append(f.podClosed, podClosed)
	return f.slot, f.err
}

func attemptEntry(t *testing.T) registry.TaskEntry {
	t.Helper()
	binding, err := json.Marshal(attempt.Binding{
		AttemptID: "attempt-1", TaskID: "task-1", SourceID: "src-1", WorkspaceID: "ws-1",
		DaemonID: "fm-task-1", JobRuntimeID: "runtime-1", EnvironmentID: "env-1", SlotID: "slot-1",
	})
	if err != nil {
		t.Fatalf("encode the frozen binding: %v", err)
	}
	return registry.TaskEntry{
		TaskID: "task-1", SourceID: "src-1", WorkspaceID: "ws-1", DaemonID: "fm-task-1",
		JobRuntimeID: "runtime-1", FixedAttempt: binding, State: registry.StateRunning,
	}
}

func attemptFaceRig(t *testing.T, ledger *fakeSlotLedger) (*AttemptFace, *registry.Registry) {
	t.Helper()
	reg := registry.New(time.Now)
	if err := reg.Put(attemptEntry(t)); err != nil {
		t.Fatalf("seed the attempt row: %v", err)
	}
	return &AttemptFace{
		Verifier: fakeAttemptVerifier{claims: auth.Claims{
			JobName: "fm-task-1", TaskID: "task-1", SourceID: "src-1", WorkspaceID: "ws-1",
		}},
		Registry: reg,
		Slots:    ledger,
	}, reg
}

// attemptRequest is one projection request carrying the Job token the adapter
// already holds. The face resolves the attempt from the token alone.
func attemptRequest(method, path, body string) *http.Request {
	var request *http.Request
	if body == "" {
		request = httptest.NewRequest(method, path, nil)
	} else {
		request = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	request.Header.Set("Authorization", "Bearer fmj_attempt")
	return request
}

// TestAttemptFaceAnswersOnlyTheTokensOwnAttempt pins the projection's scoping:
// the binding comes from the token's frozen row, and a probe token — which has
// no attempt — is refused rather than answered with an empty projection.
func TestAttemptFaceAnswersOnlyTheTokensOwnAttempt(t *testing.T) {
	face, _ := attemptFaceRig(t, &fakeSlotLedger{exists: true, slot: environment.Slot{ID: "slot-1", State: environment.SlotActive, Phase: environment.SlotPhaseOpening}})

	rec := httptest.NewRecorder()
	face.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, AttemptFacePath, nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("without a token = %d, want 401", rec.Code)
	}

	face.Verifier = fakeAttemptVerifier{claims: auth.Claims{Role: auth.RoleCatalogProbe, JobName: "fm-probe"}}
	rec = httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodGet, AttemptFacePath, ""))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("with a probe token = %d, want 403", rec.Code)
	}

	face.Verifier = fakeAttemptVerifier{claims: auth.Claims{TaskID: "task-2", SourceID: "src-1", WorkspaceID: "ws-1"}}
	rec = httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodGet, AttemptFacePath, ""))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("for another task = %d, want 404", rec.Code)
	}
}

// TestAttemptFaceReportsNoRegistrationUntilOneIsObserved pins the fact shape
// the adapters read: a runtime row is not a registration observation. The
// view stays null until the S-4 fact is persisted on the entry, even when a
// live-looking runtime row (the shape a shared read or a restart synthesizes)
// exists.
func TestAttemptFaceReportsNoRegistrationUntilOneIsObserved(t *testing.T) {
	ledger := &fakeSlotLedger{exists: true, slot: environment.Slot{ID: "slot-1", State: environment.SlotActive}}
	face, reg := attemptFaceRig(t, ledger)

	rec := httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodGet, AttemptFacePath, ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("snapshot = %d, want 200: %s", rec.Code, rec.Body)
	}
	var view attemptSnapshotView
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("decode the snapshot: %v", err)
	}
	if view.Registration != nil {
		t.Fatalf("registration = %+v, want null until the upstream confirmed one", view.Registration)
	}
	if view.Attempt.TaskID != "task-1" || view.Attempt.EnvironmentID != "env-1" || view.Attempt.SlotID != "slot-1" {
		t.Errorf("attempt = %+v, want the frozen binding", view.Attempt)
	}

	reg.PutRuntime(registry.JobRuntime{
		JobRuntimeID: "runtime-1", DaemonID: "fm-task-1", TaskID: "task-1",
		Provider: "omp", Name: "foreman-job", Version: "0.11.1", Online: true,
		LastHeartbeatAt: time.Now(),
	})
	rec = httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodGet, AttemptFacePath, ""))
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("decode the snapshot: %v", err)
	}
	if view.Registration != nil {
		t.Fatalf("registration = %+v, want null: a synthesized runtime row is not an observed S-4", view.Registration)
	}

	if _, ok := reg.MarkDaemonRegistered("fm-task-1", time.Now(), "0.11.1", "19.0.1"); !ok {
		t.Fatal("MarkDaemonRegistered refused the entry")
	}
	rec = httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodGet, AttemptFacePath, ""))
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("decode the snapshot: %v", err)
	}
	if view.Registration == nil {
		t.Fatal("registration = null, want the row Foreman recorded")
	}
	if view.Registration.DaemonID != "fm-task-1" || view.Registration.RuntimeID != "runtime-1" || view.Registration.Version != "0.11.1" {
		t.Errorf("registration = %+v, want the recorded daemon/runtime/version", view.Registration)
	}
	if view.Registration.AgentVersion != "19.0.1" {
		t.Errorf("agent version = %q, want the separately recorded 19.0.1", view.Registration.AgentVersion)
	}
	if view.Registration.ObservedAt == "" {
		t.Error("observed_at is empty: the view lost the registration instant")
	}
}

// TestAttemptFaceRegistrationComesFromThePersistedEntryAcrossInstances pins
// the restart/shared-read path: a fresh registry that only caches the entry
// (and synthesizes its runtime row) must still answer the registration the
// shared row recorded, with its own versions.
func TestAttemptFaceRegistrationComesFromThePersistedEntryAcrossInstances(t *testing.T) {
	entry := attemptEntry(t)
	entry.State = registry.StateRunning
	entry.DaemonRegisteredAt = time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)
	entry.DaemonCLIVersion = "0.11.1"
	entry.AgentRuntimeVersion = "19.0.1"

	reg := registry.New(time.Now)
	reg.Cache(entry)
	face := &AttemptFace{
		Verifier: fakeAttemptVerifier{claims: auth.Claims{
			JobName: "fm-task-1", TaskID: "task-1", SourceID: "src-1", WorkspaceID: "ws-1",
		}},
		Registry: reg,
		Slots:    &fakeSlotLedger{exists: true, slot: environment.Slot{ID: "slot-1", State: environment.SlotActive}},
	}
	rec := httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodGet, AttemptFacePath, ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("snapshot = %d, want 200: %s", rec.Code, rec.Body)
	}
	var view attemptSnapshotView
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("decode the snapshot: %v", err)
	}
	if view.Registration == nil {
		t.Fatal("registration = null after a restart, want the persisted S-4 fact")
	}
	if view.Registration.Version != "0.11.1" || view.Registration.AgentVersion != "19.0.1" {
		t.Errorf("registration = %+v, want the persisted daemon and agent versions", view.Registration)
	}
	if view.Registration.ObservedAt != "2026-11-24T09:00:00Z" {
		t.Errorf("observed_at = %q, want the persisted registration instant", view.Registration.ObservedAt)
	}
}

// TestAttemptFaceReportsAnOfflineRuntimeAsNoRegistration pins the liveness
// half: a runtime Foreman recorded but no longer sees online is not a current
// registration answer.
func TestAttemptFaceReportsAnOfflineRuntimeAsNoRegistration(t *testing.T) {
	face, reg := attemptFaceRig(t, &fakeSlotLedger{exists: true})
	reg.PutRuntime(registry.JobRuntime{JobRuntimeID: "runtime-1", DaemonID: "fm-task-1", TaskID: "task-1", Online: false})

	rec := httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodGet, AttemptFacePath, ""))
	var view attemptSnapshotView
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("decode the snapshot: %v", err)
	}
	if view.Registration != nil {
		t.Fatalf("registration = %+v, want null for an offline runtime", view.Registration)
	}
}

// TestAttemptFaceLedgerTransitionsCarryTheAttemptsOwnIds pins the ledger seam:
// the environment and slot always come from the frozen binding, so a request
// body cannot move the transition onto another reservation.
func TestAttemptFaceLedgerTransitionsCarryTheAttemptsOwnIds(t *testing.T) {
	ledger := &fakeSlotLedger{exists: true}
	face, _ := attemptFaceRig(t, ledger)

	body := `{"claim_id":"c-1","session_id":"sbx-1","confirmed":true}`
	rec := httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodPost, AttemptFacePath+"/slots/release", body))
	if rec.Code != http.StatusOK {
		t.Fatalf("release = %d, want 200: %s", rec.Code, rec.Body)
	}
	if len(ledger.releases) != 1 || !ledger.releases[0] {
		t.Fatalf("releases = %v, want the confirmed close committed once", ledger.releases)
	}
	if len(ledger.podClosed) != 1 || ledger.podClosed[0] {
		t.Fatalf("podClosed = %v, want false: no observer may not free capacity", ledger.podClosed)
	}

	rec = httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodPost, AttemptFacePath+"/slots/nope", `{}`))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown phase = %d, want 404", rec.Code)
	}

	ledger.err = environment.ErrSlotClaimed
	rec = httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodPost, AttemptFacePath+"/slots/begin", `{"claim_id":"c-2"}`))
	if rec.Code != http.StatusConflict {
		t.Fatalf("claimed slot = %d, want 409", rec.Code)
	}
}

// TestAttemptFaceReleaseNeedsBothCloseFacts pins the two-sided gate at the
// face: the remote close comes from the adapter, the Pod close from the
// trusted observer, and both reach the ledger as separate values.
func TestAttemptFaceReleaseNeedsBothCloseFacts(t *testing.T) {
	ledger := &fakeSlotLedger{exists: true}
	face, _ := attemptFaceRig(t, ledger)
	face.PodsClosed = func(context.Context, registry.TaskEntry, attempt.Binding) (bool, error) {
		return true, nil
	}
	rec := httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodPost, AttemptFacePath+"/slots/release", `{"claim_id":"c-1","confirmed":true}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("release = %d, want 200: %s", rec.Code, rec.Body)
	}
	if len(ledger.releases) != 1 || !ledger.releases[0] {
		t.Fatalf("remoteConfirmed = %v, want true", ledger.releases)
	}
	if len(ledger.podClosed) != 1 || !ledger.podClosed[0] {
		t.Fatalf("podClosed = %v, want true from the observer", ledger.podClosed)
	}

	face.PodsClosed = func(context.Context, registry.TaskEntry, attempt.Binding) (bool, error) {
		return false, errors.New("pods are unreadable")
	}
	rec = httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodPost, AttemptFacePath+"/slots/release", `{"claim_id":"c-1","confirmed":true}`))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unreadable pod fact = %d, want 503", rec.Code)
	}
}

// TestAttemptFaceSeparatesSendFromArrival pins the projection's delivery
// vocabulary: the server commit is "sent", arrival needs the start fact, and a
// lost response stays sent-but-not-delivered.
func TestAttemptFaceSeparatesSendFromArrival(t *testing.T) {
	entry := attemptEntry(t)
	entry.State = registry.StateDelivering
	entry.DeliveredAt = time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)
	entry.ClaimsAfterDelivery = 2
	reg := registry.New(time.Now)
	reg.Cache(entry)
	face := &AttemptFace{
		Verifier: fakeAttemptVerifier{claims: auth.Claims{
			JobName: "fm-task-1", TaskID: "task-1", SourceID: "src-1", WorkspaceID: "ws-1",
		}},
		Registry: reg,
		Slots:    &fakeSlotLedger{exists: true, slot: environment.Slot{ID: "slot-1", State: environment.SlotActive}},
	}
	read := func() attemptSnapshotView {
		t.Helper()
		rec := httptest.NewRecorder()
		face.ServeHTTP(rec, attemptRequest(http.MethodGet, AttemptFacePath, ""))
		var view attemptSnapshotView
		if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
			t.Fatalf("decode the snapshot: %v", err)
		}
		return view
	}
	view := read()
	if !view.Task.Sent || view.Task.SentAt == "" {
		t.Fatalf("task = %+v, want the send commit exposed", view.Task)
	}
	if view.Task.Delivered {
		t.Fatal("a send commit without a start fact was projected as arrival")
	}
	if view.Task.ClaimsAfterDelivery != 2 {
		t.Errorf("claims_after_delivery = %d, want the lost-response count", view.Task.ClaimsAfterDelivery)
	}

	entry.StartedAt = time.Date(2026, 11, 24, 9, 1, 0, 0, time.UTC)
	reg.Cache(entry)
	view = read()
	if !view.Task.Delivered {
		t.Fatal("a start fact was not projected as arrival")
	}
}

// TestAttemptFaceNeverInfersArrivalFromATerminalState pins the fabrication
// path: Foreman writes a terminal with an empty failure reason whether the
// daemon reported the verdict or an upstream status read (C13) settled the
// attempt. A terminal without this attempt's start fact is not arrival.
func TestAttemptFaceNeverInfersArrivalFromATerminalState(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry func(registry.TaskEntry) registry.TaskEntry
	}{
		{
			name: "never sent, upstream cancelled",
			entry: func(e registry.TaskEntry) registry.TaskEntry {
				e.State = registry.StateTerminal
				e.Result = "cancelled"
				return e
			},
		},
		{
			name: "never sent, task vanished",
			entry: func(e registry.TaskEntry) registry.TaskEntry {
				e.State = registry.StateTerminal
				e.Result = ""
				return e
			},
		},
		{
			name: "sent, never arrived, external terminal",
			entry: func(e registry.TaskEntry) registry.TaskEntry {
				e.State = registry.StateTerminal
				e.Result = "completed"
				e.DeliveredAt = time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)
				return e
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := tc.entry(attemptEntry(t))
			reg := registry.New(time.Now)
			reg.Cache(entry)
			face := &AttemptFace{
				Verifier: fakeAttemptVerifier{claims: auth.Claims{
					JobName: "fm-task-1", TaskID: "task-1", SourceID: "src-1", WorkspaceID: "ws-1",
				}},
				Registry: reg,
				Slots:    &fakeSlotLedger{exists: true, slot: environment.Slot{ID: "slot-1", State: environment.SlotActive}},
			}
			rec := httptest.NewRecorder()
			face.ServeHTTP(rec, attemptRequest(http.MethodGet, AttemptFacePath, ""))
			var view attemptSnapshotView
			if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
				t.Fatalf("decode the snapshot: %v", err)
			}
			if view.Task.Delivered {
				t.Fatalf("a terminal without a start fact was projected as arrival: %+v", view.Task)
			}
		})
	}
}

// TestAttemptFaceRefusesAnUnreadableLedger pins the failure answer: a ledger
// the face cannot read is not an empty slot, so the adapter stops instead of
// opening a session over an unknown reservation.
func TestAttemptFaceRefusesAnUnreadableLedger(t *testing.T) {
	face, _ := attemptFaceRig(t, &fakeSlotLedger{err: errors.New("store is down")})
	rec := httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodGet, AttemptFacePath, ""))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unreadable ledger = %d, want 503: %s", rec.Code, rec.Body)
	}
}

// TestAttemptFaceObservesTheEnvironmentThroughAReadOnlyProbe pins the ready
// identity source: the route runs the deployment's probe and answers its fresh
// facts, never the frozen binding. A face without the seam is unavailable, so
// the adapter refuses ready instead of echoing the input.
func TestAttemptFaceObservesTheEnvironmentThroughAReadOnlyProbe(t *testing.T) {
	face, _ := attemptFaceRig(t, &fakeSlotLedger{exists: true, slot: environment.Slot{ID: "slot-1", State: environment.SlotActive}})
	var observedID string
	face.Environment = func(_ context.Context, id string) (EnvironmentFact, error) {
		observedID = id
		return EnvironmentFact{TreeID: "tree-observed", Identity: "sha256:identity", State: "observed"}, nil
	}
	rec := httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodGet, AttemptFacePath+"/environment", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("environment observation = %d, want 200: %s", rec.Code, rec.Body)
	}
	var fact EnvironmentFact
	if err := json.Unmarshal(rec.Body.Bytes(), &fact); err != nil {
		t.Fatalf("decode the observation: %v", err)
	}
	if fact.TreeID != "tree-observed" || fact.Identity != "sha256:identity" {
		t.Errorf("observation = %+v, want the probe's own facts", fact)
	}
	if observedID != "env-1" {
		t.Errorf("probe ran for %q, want the token's own environment", observedID)
	}

	face.Environment = nil
	rec = httptest.NewRecorder()
	face.ServeHTTP(rec, attemptRequest(http.MethodGet, AttemptFacePath+"/environment", ""))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unwired observation = %d, want 503", rec.Code)
	}
}
