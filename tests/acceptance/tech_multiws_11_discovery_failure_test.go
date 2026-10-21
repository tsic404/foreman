// TC-tech-multiws-11 implements AC-96: discovery unavailable at startup —
// upstream unreachable, authentication rejected, or an empty workspace set —
// refuses startup, with an event naming the reason and the credential scope.
// No half-configured service (no workspace registered, no claim) can come out
// of it: the failure happens before any identity or client exists.
package acceptance

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/workspace"
)

func TestTechMultiWS11DiscoveryFailure(t *testing.T) {
	// Each failure class returns an error from the startup discovery step and
	// logs the §9.1 failure event with the credential scope.
	cases := []struct {
		name      string
		prepare   func(t *testing.T) *discoveryStub
		statusErr int
	}{
		{
			name: "upstream unreachable",
			prepare: func(t *testing.T) *discoveryStub {
				stub := newDiscoveryStub(t, []workspace.Ref{{ID: "ws-a", Name: "A"}})
				stub.server.Close()
				return stub
			},
		},
		{
			name: "authentication rejected",
			prepare: func(t *testing.T) *discoveryStub {
				stub := newDiscoveryStub(t, []workspace.Ref{{ID: "ws-a", Name: "A"}})
				stub.setStatus(http.StatusForbidden)
				return stub
			},
			statusErr: http.StatusForbidden,
		},
		{
			name: "empty workspace set",
			prepare: func(t *testing.T) *discoveryStub {
				return newDiscoveryStub(t, nil)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := tc.prepare(t)
			discoveryClient := proxy.NewClient(proxy.Config{ServerURL: stub.server.URL, Version: "0.1.0"}, "mul_user")

			var logs bytes.Buffer
			log := slog.New(slog.NewJSONHandler(&logs, nil))
			_, err := discoveryClient.DiscoverWorkspaces(t.Context(), nil)
			if err == nil {
				t.Fatal("discovery failure must refuse startup")
			}
			if tc.statusErr != 0 {
				if !proxy.IsCredentialConfigError(err) {
					t.Errorf("error = %v, want the credential-rejection classification", err)
				}
			}
			proxy.LogDiscoveryFailure(log, workspace.ScopeUser.String(), err)

			var event map[string]any
			if decodeErr := json.Unmarshal(logs.Bytes(), &event); decodeErr != nil {
				t.Fatalf("decode discovery failure event: %v (%s)", decodeErr, logs.String())
			}
			if event["msg"] != "workspace.discovery_failed" {
				t.Errorf("event = %v, want workspace.discovery_failed", event["msg"])
			}
			if event["credential_scope"] != "user" {
				t.Errorf("event credential_scope = %v, want user", event["credential_scope"])
			}
			if reason, _ := event["err"].(string); reason == "" {
				t.Errorf("event carries no reason: %v", event)
			}

			// No partial service: the failure precedes identity derivation, so
			// there is no workspace to register or claim for.
			deployment := workspace.Deployment{Scope: workspace.ScopeUser, UserToken: "mul_user", BaseDaemonID: "foreman"}
			if identities := deployment.Identities(nil); len(identities) != 0 {
				t.Fatalf("identities = %v after a failed discovery, want none", identities)
			}
		})
	}
}
