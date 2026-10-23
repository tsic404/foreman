// TC-tech-multiws-15 implements AC-104: a C-17 answer entry carries a
// display name in both credential forms. An empty (or whitespace-only) name
// is a malformed answer, so startup refuses and names the workspace — the
// entry is never skipped silently, and "" never reaches the name cache where
// it would read as "authority not answered" and degrade a genuinely absent
// partition key to a warning.
package acceptance

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/workspace"
)

func TestTechMultiWS15C17AnswerCompleteness(t *testing.T) {
	t.Run("user-scoped discovery refuses and names the workspace", func(t *testing.T) {
		stub := newDiscoveryStub(t, []workspace.Ref{{ID: "ws-a", Name: "A"}, {ID: "ws-b"}})
		discoveryClient := proxy.NewClient(proxy.Config{ServerURL: stub.server.URL, Version: "0.1.0"}, "mul_user")

		var logs bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&logs, nil))
		_, err := discoveryClient.DiscoverWorkspaces(t.Context(), nil)
		if err == nil {
			t.Fatal("an answer entry without a display name must refuse startup")
		}
		var answer *proxy.DiscoveryAnswerError
		if !errors.As(err, &answer) || answer.WorkspaceID != "ws-b" {
			t.Fatalf("error = %v, want a DiscoveryAnswerError naming ws-b", err)
		}
		if !proxy.IsStartupFatal(err) {
			t.Errorf("error = %v, want the startup-fatal classification", err)
		}
		proxy.LogDiscoveryFailure(log, workspace.ScopeUser.String(), err)
		event := logs.String()
		for _, want := range []string{`"msg":"workspace.discovery_failed"`, `"credential_scope":"user"`, "ws-b"} {
			if !strings.Contains(event, want) {
				t.Errorf("discovery failure event = %s, want it to carry %s", event, want)
			}
		}
	})

	t.Run("workspace-scoped self-check refuses for empty and whitespace names", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			visible []workspace.Ref
		}{
			{"empty name", []workspace.Ref{{ID: "ws-a"}}},
			{"whitespace name", []workspace.Ref{{ID: "ws-a", Name: "  "}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				stub := newDiscoveryStub(t, tc.visible)
				client := proxy.NewClient(proxy.Config{
					ServerURL:   stub.server.URL,
					WorkspaceID: "ws-a",
					Version:     "0.1.0",
				}, "mdt_a")

				err := client.ValidateWorkspaceScope(t.Context())
				if err == nil {
					t.Fatal("a C-17 entry without a display name must refuse startup")
				}
				var answer *proxy.DiscoveryAnswerError
				if !errors.As(err, &answer) || answer.WorkspaceID != "ws-a" {
					t.Fatalf("error = %v, want a DiscoveryAnswerError naming ws-a", err)
				}
				if !proxy.IsStartupFatal(err) {
					t.Errorf("error = %v, want the startup-fatal classification", err)
				}
				if name, seeded := client.WorkspaceName(); seeded {
					t.Errorf("WorkspaceName = %q, want unseeded after a malformed answer", name)
				}
			})
		}
	})
}
