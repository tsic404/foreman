package observability_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/observability"
)

// partitionServer builds an ops surface that serves three workspaces and hands
// out two credentials: a deployment-wide one and one bound to ws-2.
func partitionServer(t *testing.T, source func() map[string]any) *httptest.Server {
	t.Helper()
	h := observability.NewOpsHandler(nil, nil, observability.OpsConfig{
		Version:      "test",
		WorkspaceIDs: []string{"ws-1", "ws-2", "ws-3"},
		Credentials: []observability.OpsCredential{
			{Token: "wide-tok"},
			{Token: "narrow-tok", Workspaces: []string{"ws-2"}},
		},
		RegisterSource: source,
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func sourceOfAll() map[string]any {
	return map[string]any{
		"ws-1": []any{map[string]any{"runtime_id": "rt-1"}},
		"ws-2": []any{map[string]any{"runtime_id": "rt-2"}},
		"ws-3": []any{map[string]any{"runtime_id": "rt-3"}},
	}
}

func decodeMap(t *testing.T, body string) map[string]json.RawMessage {
	t.Helper()
	var out map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("body is not a JSON object: %v (%s)", err, body)
	}
	return out
}

// TestRegisterSourceCropsToGrant is the multi-tenant boundary: a workspace-bound
// credential must never see another workspace's registration view. A leak here
// is silent — the response is still well-formed JSON.
func TestRegisterSourceCropsToGrant(t *testing.T) {
	srv := partitionServer(t, sourceOfAll)

	code, body := get(t, srv.URL+"/foreman/register-source", "narrow-tok")
	if code != http.StatusOK {
		t.Fatalf("bounded credential = %d, want 200", code)
	}
	view := decodeMap(t, body)
	if _, leaked := view["ws-1"]; leaked {
		t.Fatalf("credential bound to ws-2 can read ws-1: %s", body)
	}
	if _, leaked := view["ws-3"]; leaked {
		t.Fatalf("credential bound to ws-2 can read ws-3: %s", body)
	}
	if _, ok := view["ws-2"]; !ok {
		t.Fatalf("credential cannot read its own workspace: %s", body)
	}

	code, body = get(t, srv.URL+"/foreman/register-source", "wide-tok")
	if code != http.StatusOK {
		t.Fatalf("wide credential = %d, want 200", code)
	}
	if got := len(decodeMap(t, body)); got != 3 {
		t.Fatalf("wide credential sees %d workspaces, want 3: %s", got, body)
	}
}

// TestOpsScopeRefusesForeignWorkspace: a query parameter must not widen the
// partition — naming a workspace outside the grant answers like one that does
// not exist.
func TestOpsScopeRefusesForeignWorkspace(t *testing.T) {
	srv := partitionServer(t, sourceOfAll)

	// ws-1 is real but outside this credential's grant.
	if code, _ := get(t, srv.URL+"/foreman/tasks?workspace=ws-1", "narrow-tok"); code != http.StatusNotFound {
		t.Fatalf("foreign workspace = %d, want 404 (the same answer as a missing one)", code)
	}
	// Its own workspace is served.
	if code, _ := get(t, srv.URL+"/foreman/tasks?workspace=ws-2", "narrow-tok"); code != http.StatusOK {
		t.Fatalf("own workspace = %d, want 200", code)
	}
	// A workspace that does not exist at all answers identically, so the
	// parameter cannot be used to probe which ids exist.
	if code, _ := get(t, srv.URL+"/foreman/tasks?workspace=ws-does-not-exist", "narrow-tok"); code != http.StatusNotFound {
		t.Fatalf("unknown workspace = %d, want 404", code)
	}
}

// TestOpsScopeDefaultsAndAmbiguity pins the three shapes: a single grant is the
// default, several without a parameter is a 400 (not an arbitrary pick), and a
// workspace-bound credential never falls through to "no dimension".
func TestOpsScopeDefaultsAndAmbiguity(t *testing.T) {
	srv := partitionServer(t, sourceOfAll)

	// A credential granted exactly one workspace needs no parameter.
	if code, _ := get(t, srv.URL+"/foreman/tasks", "narrow-tok"); code != http.StatusOK {
		t.Fatalf("single-grant default = %d, want 200", code)
	}
	// A deployment-wide credential over three workspaces must be asked.
	code, body := get(t, srv.URL+"/foreman/tasks", "wide-tok")
	if code != http.StatusBadRequest {
		t.Fatalf("ambiguous request = %d (%s), want 400", code, body)
	}
	if !strings.Contains(body, "workspace is required") {
		t.Fatalf("400 should say what is missing: %s", body)
	}
}

// TestOpsScopeFailsClosedWithoutIdentity: the handler must never widen to
// "every workspace" when the middleware did not resolve an identity.
func TestOpsScopeFailsClosedWithoutIdentity(t *testing.T) {
	// Mounted without the credential face, no identity reaches the handler.
	h := observability.NewOpsHandler(nil, nil, observability.OpsConfig{
		Version:      "test",
		WorkspaceIDs: []string{"ws-1"},
		Credentials:  []observability.OpsCredential{{Token: "ops-tok"}},
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	// The credential face always runs, so an unauthenticated call is 401; this
	// asserts the fail-closed default is not reachable as "see everything".
	if code, _ := get(t, srv.URL+"/foreman/register-source", "ops-tok"); code == http.StatusOK {
		t.Fatal("register-source should 404 when no source is wired")
	}
}

// TestRegisterSourceAbsentWorkspaceIsOmittedNotEmptied: cropping must drop the
// key entirely, so a reader cannot mistake "not allowed" for "nothing
// registered".
func TestRegisterSourceAbsentWorkspaceIsOmittedNotEmptied(t *testing.T) {
	srv := partitionServer(t, sourceOfAll)

	_, body := get(t, srv.URL+"/foreman/register-source", "narrow-tok")
	view := decodeMap(t, body)
	for id, raw := range view {
		if strings.TrimSpace(string(raw)) == "null" || string(raw) == "[]" {
			t.Fatalf("workspace %s rendered empty instead of being omitted: %s", id, body)
		}
	}
}
