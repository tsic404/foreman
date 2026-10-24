// TC-tech-auth-07 (AC-110): a catalog probe whose session and token point at a
// workspace outside the deployment's discovery set. S-1 and every endpoint that
// resolves a workspace for the request answer 403 — never a list-shaped
// success, 401 or 404 — while a cross-workspace probe request keeps the same
// code and is told apart by the error body. A removed session is still 401.
package acceptance

import (
	"encoding/json"
	"net/http"
	"testing"
)

// requireForbidden asserts the 403 a workspace-resolving face owes a request
// outside the discovery set, and returns the reason its body states.
func requireForbidden(t *testing.T, face string, code int, body []byte) string {
	t.Helper()
	if code != http.StatusForbidden {
		t.Fatalf("%s = %d: %s, want 403", face, code, body)
	}
	var answer map[string]string
	if err := json.Unmarshal(body, &answer); err != nil {
		t.Fatalf("%s: 403 body is not the unified error object: %v (%s)", face, err, body)
	}
	if answer["error"] == "" {
		t.Fatalf("%s: 403 body states no reason: %s", face, body)
	}
	return answer["error"]
}

func TestAuth07ProbeOutsideTheDiscoverySetIsForbidden(t *testing.T) {
	// Two served workspaces: ws-2 is in the discovery set, so the
	// cross-workspace request below is not confounded with an unserved id.
	m := newMultiWS(t, "ws-1", "ws-2")

	outside := probeSeed{
		jobName:     "fm-catalog-req-outside",
		requestID:   "req-outside",
		jobRuntime:  "rt-req-outside",
		realRuntime: "rid-omp",
		workspaceID: "ws-outside",
	}
	outsideToken, _ := seedProbeSession(t, m.issuer, m.probes, outside)

	// S-1 is the probe's bootstrap answer: a list-shaped success — even an
	// empty one — would read as "this workspace is served".
	code, body := m.call(http.MethodGet, "/api/daemon/workspaces", outsideToken, nil)
	requireForbidden(t, "S-1", code, body)

	for _, face := range []struct {
		name   string
		method string
		path   string
		body   []byte
	}{
		{
			name:   "S-2 repos",
			method: http.MethodGet,
			path:   "/api/daemon/workspaces/" + outside.workspaceID + "/repos",
		},
		{
			name:   "S-3 runtime profiles",
			method: http.MethodGet,
			path:   "/api/daemon/workspaces/" + outside.workspaceID + "/runtime-profiles",
		},
		{
			name:   "S-4 register",
			method: http.MethodPost,
			path:   "/api/daemon/register",
			body: []byte(`{"daemon_id":"` + outside.jobName + `","workspace_id":"` + outside.workspaceID +
				`","cli_version":"0.6.1","runtimes":[{"name":"foreman-job","type":"omp","version":"0.6.1"}]}`),
		},
	} {
		faceCode, faceBody := m.call(face.method, face.path, outsideToken, face.body)
		requireForbidden(t, face.name, faceCode, faceBody)
	}

	// Same status, different cause: a probe pinned to a served workspace
	// asking for another one is also 403. On that endpoint the two reasons are
	// distinguished by the body, not by the code.
	inside := probeSeed{
		jobName:     "fm-catalog-req-inside",
		requestID:   "req-inside",
		jobRuntime:  "rt-req-inside",
		realRuntime: "rid-omp",
		workspaceID: "ws-1",
	}
	insideToken, _ := seedProbeSession(t, m.issuer, m.probes, inside)

	code, body = m.call(http.MethodGet, "/api/daemon/workspaces/"+outside.workspaceID+"/repos", outsideToken, nil)
	outsideReason := requireForbidden(t, "S-2 from the session outside the discovery set", code, body)
	code, body = m.call(http.MethodGet, "/api/daemon/workspaces/ws-2/repos", insideToken, nil)
	crossReason := requireForbidden(t, "S-2 cross-workspace", code, body)
	if outsideReason == crossReason {
		t.Fatalf("both 403s state %q; the causes are not distinguishable by body", outsideReason)
	}

	// The session is the token's sole adjudication face (§6.3): once it is
	// gone the same token is 401 on S-1, as the probe lifecycle case judges.
	if err := m.probes.DeleteProbe(outside.jobName); err != nil {
		t.Fatalf("DeleteProbe: %v", err)
	}
	code, body = m.call(http.MethodGet, "/api/daemon/workspaces", outsideToken, nil)
	if code != http.StatusUnauthorized {
		t.Fatalf("S-1 after the session was removed = %d: %s, want 401", code, body)
	}
}
