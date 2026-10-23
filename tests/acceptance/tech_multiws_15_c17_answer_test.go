// TC-tech-multiws-15 implements AC-104: a C-17 answer entry carries a
// display name in both credential forms. An empty (or whitespace-only) name
// is a malformed answer, so startup refuses and names the workspace — the
// entry is never skipped silently, and "" never reaches the name cache where
// it would read as "authority not answered" and degrade a genuinely absent
// partition key to a warning.
//
// The seam test holds the classification; the process test below holds the
// exit: AC-104 judges "非零退出" on the process, and a main that drops run()'s
// error would leave the seam assertions green.
package acceptance

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

// writeStubKubeconfig writes the smallest kubeconfig the binary loads: the
// refusal under test lands before any apiserver call, so the client is built
// and never used and the endpoint may be unreachable.
func writeStubKubeconfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubeconfig")
	const body = `apiVersion: v1
kind: Config
clusters:
- name: stub
  cluster:
    server: http://127.0.0.1:1
contexts:
- name: stub
  context:
    cluster: stub
    user: stub
current-context: stub
users:
- name: stub
  user: {}
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return path
}

// TestTechMultiWS15C17AnswerRefusesProcess drives the refusal through the real
// binary: both credential forms read a C-17 answer whose only display-name
// value is empty or whitespace-only, and the process must exit non-zero with
// the failure event naming the offending workspace. The service face never
// opens — no registration, no heartbeat, no claim — and the startup never
// continues to the "authority not answered" degradation.
func TestTechMultiWS15C17AnswerRefusesProcess(t *testing.T) {
	bin := buildForemanBinary(t)
	kubeconfig := writeStubKubeconfig(t)

	for _, form := range []struct {
		name      string
		overrides map[string]string
		answer    string
		offender  string
		wantEvent []string
	}{
		{
			name: "user-scoped discovery",
			overrides: map[string]string{
				// No workspace id selects the user-scoped discovery form.
				"MULTICA_WORKSPACE_ID": "",
				"MULTICA_TOKEN":        "mul_user",
				"KUBECONFIG":           kubeconfig,
			},
			// A valid entry precedes the malformed one: the event must name
			// the offending answer entry (ws-b), not the first workspace.
			answer:   `[{"id":"ws-a","name":"A"},{"id":"ws-b","name":%s}]`,
			offender: "ws-b",
			wantEvent: []string{
				`"msg":"workspace.discovery_failed"`,
				`"credential_scope":"user"`,
			},
		},
		{
			name: "workspace-scoped self-check",
			overrides: map[string]string{
				"MULTICA_WORKSPACE_ID": "ws-a",
				"MULTICA_TOKEN":        "mdt_ws-a",
				"KUBECONFIG":           kubeconfig,
			},
			answer:   `[{"id":"ws-a","name":%s}]`,
			offender: "ws-a",
			wantEvent: []string{
				`"msg":"foreman.startup_aborted"`,
				`"stage":"workspace_discovery"`,
				`"workspace_id":"ws-a"`,
			},
		},
	} {
		for _, displayName := range []struct{ name, value string }{
			{"empty name", `""`},
			{"whitespace-only name", `"   "`},
		} {
			t.Run(form.name+" refuses on "+displayName.name, func(t *testing.T) {
				upstream := newUpstreamStub(t)
				upstream.setWorkspacesAnswer(fmt.Sprintf(form.answer, displayName.value))

				run := runForeman(t, bin, upstream.server.URL, form.overrides)
				if run.code <= 0 {
					t.Fatalf("exit code = %d, want non-zero (AC-104 refuses startup); stdout:\n%s\nstderr:\n%s",
						run.code, run.stdout, run.stderr)
				}
				// The operator-facing error line names the workspace...
				if !strings.Contains(run.stderr, form.offender) {
					t.Errorf("stderr does not name workspace %s:\n%s", form.offender, run.stderr)
				}
				// ...and so does the structured startup event, with the reason
				// shape of its credential form.
				for _, want := range form.wantEvent {
					if !strings.Contains(run.stdout, want) {
						t.Errorf("startup event does not carry %s:\n%s", want, run.stdout)
					}
				}
				if !strings.Contains(run.stdout, `"err":"list workspaces: C-17 answer has no display name for workspace `+form.offender+`"`) {
					t.Errorf("startup event does not name the malformed entry %s:\n%s", form.offender, run.stdout)
				}

				// Never the degraded boot: the success discovery event is the
				// one a started deployment emits.
				if strings.Contains(run.stdout, `"msg":"workspace.discovery"`) {
					t.Errorf("startup continued past the malformed answer:\n%s", run.stdout)
				}
				// The service face never opened.
				for _, path := range []string{
					"/api/daemon/register",
					"/api/daemon/heartbeat",
					"/api/daemon/tasks/claim",
				} {
					if calls := upstream.pathCalls(path); len(calls) != 0 {
						t.Errorf("%s reached the server %d times during a refused startup, want 0", path, len(calls))
					}
				}
				if calls := upstream.pathCalls("/api/daemon/workspaces"); len(calls) == 0 {
					t.Error("C-17 was never called: the refusal did not come from the malformed answer")
				}
			})
		}
	}
}
