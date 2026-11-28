package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/environment"
)

// TestS4KeepsDaemonAndAgentVersionsApart walks the real registration path: the
// S-4 body reports the Multica CLI version (cli_version) and the selected
// Agent runtime's own version. The entry and the projection must keep them
// apart, and the runtime row the daemon face reads carries the CLI version.
func TestS4KeepsDaemonAndAgentVersionsApart(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-versions")
	entry, ok := rig.reg.Get("t-versions")
	if !ok {
		t.Fatal("the claimed entry is missing")
	}
	binding, err := json.Marshal(attempt.Binding{
		AttemptID: "t-versions-1", TaskID: entry.TaskID, SourceID: entry.SourceID,
		WorkspaceID: entry.WorkspaceID, DaemonID: entry.DaemonID, JobRuntimeID: entry.JobRuntimeID,
		EnvironmentID: "env-1", SlotID: "slot-1",
	})
	if err != nil {
		t.Fatalf("encode the frozen binding: %v", err)
	}
	entry.FixedAttempt = binding
	rig.reg.Cache(entry)

	token := rig.token(t, "t-versions")
	code, body := rig.call(t, "POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-t-versions",
		"cli_version": "0.11.1",
		"runtimes": [{"name": "foreman-omp", "type": "omp", "version": "19.0.1"}]
	}`))
	if code != http.StatusOK {
		t.Fatalf("S4 = %d: %s", code, body)
	}

	stored, ok := rig.reg.Get("t-versions")
	if !ok {
		t.Fatal("the entry disappeared after S4")
	}
	if stored.DaemonCLIVersion != "0.11.1" {
		t.Errorf("daemon CLI version = %q, want 0.11.1", stored.DaemonCLIVersion)
	}
	if stored.AgentRuntimeVersion != "19.0.1" {
		t.Errorf("agent runtime version = %q, want 19.0.1", stored.AgentRuntimeVersion)
	}
	runtime, ok := rig.reg.RuntimeByID(stored.JobRuntimeID)
	if !ok {
		t.Fatal("the runtime row is missing")
	}
	if runtime.Version != "0.11.1" {
		t.Errorf("runtime version = %q, want the daemon CLI version 0.11.1", runtime.Version)
	}

	face := &AttemptFace{
		Verifier: rig.issuer,
		Registry: rig.reg,
		Slots:    &fakeSlotLedger{exists: true, slot: environment.Slot{ID: "slot-1", State: environment.SlotActive}},
	}
	server := httptest.NewServer(face)
	t.Cleanup(server.Close)
	request, err := http.NewRequest(http.MethodGet, server.URL+AttemptFacePath, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("read the projection: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("projection = %d", response.StatusCode)
	}
	var view attemptSnapshotView
	if err := json.NewDecoder(response.Body).Decode(&view); err != nil {
		t.Fatalf("decode the projection: %v", err)
	}
	if view.Registration == nil {
		t.Fatal("the projection lost the observed registration")
	}
	if view.Registration.Version != "0.11.1" {
		t.Errorf("projected version = %q, want the daemon CLI version", view.Registration.Version)
	}
	if view.Registration.AgentVersion != "19.0.1" {
		t.Errorf("projected agent_version = %q, want the Agent runtime version", view.Registration.AgentVersion)
	}
}
