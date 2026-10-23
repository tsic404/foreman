// TC-tech-startup-06 (AC-109): the startup gate's fatal/transient classification
// (§1.1 启动自检与失败面).
//
// The other acceptance cases stop at the self-check's inputs (deployment
// loading, C-17 listing, workspace-scope validation), so a regression in the
// gate that decides whether main may serve was only caught by the cmd/foreman
// unit tests. This case drives the production gate (startup.RunSelfCheck —
// the function whose returned error main exits non-zero on) against a
// controlled upstream and asserts both classes at that seam:
//
//   - configuration defects (credential rejected, C-17 set mismatch, answer
//     entry without a display name) return the process error and log
//     foreman.startup_aborted with the workspace and stage;
//   - transient failures (5xx, unreachable upstream) return nil, log only
//     workspace.credential_check_failed, and the check completes.
//
// The classification is the criterion: a credential rejection that stops
// being fatal, or a malformed answer that stops being fatal, fails this case.
package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/startup"
	"github.com/tsic404/foreman/internal/workspace"
)

const selfCheckWorkspace = "ws-a"

// selfCheckUpstream answers C-17 and C-18 with programmable answers and counts
// the calls each face saw, so a self-check that retries a configuration error
// stays visible.
type selfCheckUpstream struct {
	mu        sync.Mutex
	c17Status int
	c17Body   string
	c18Status int
	c18Body   string
	c17Calls  int
	c18Calls  int
	server    *httptest.Server
}

func newSelfCheckUpstream(t *testing.T) *selfCheckUpstream {
	t.Helper()
	up := &selfCheckUpstream{
		c17Status: http.StatusOK,
		c17Body:   `[{"id":"ws-a","name":"Workspace A"}]`,
		c18Status: http.StatusNotFound,
		c18Body:   `{"error":"missing"}`,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/daemon/workspaces", func(w http.ResponseWriter, _ *http.Request) {
		up.mu.Lock()
		status, body := up.c17Status, up.c17Body
		up.c17Calls++
		up.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	})
	mux.HandleFunc("GET /api/daemon/workspaces/{ws}/runtime-profiles", func(w http.ResponseWriter, _ *http.Request) {
		up.mu.Lock()
		status, body := up.c18Status, up.c18Body
		up.c18Calls++
		up.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	})
	up.server = httptest.NewServer(mux)
	t.Cleanup(up.server.Close)
	return up
}

func (u *selfCheckUpstream) answerC17(status int, body string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.c17Status, u.c17Body = status, body
}

func (u *selfCheckUpstream) answerC18(status int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.c18Status = status
}

func (u *selfCheckUpstream) calls() (c17, c18 int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.c17Calls, u.c18Calls
}

// selfCheckRun is one gate invocation and everything it logged.
type selfCheckRun struct {
	err    error
	events []map[string]any
	c17    int
	c18    int
}

// runStartupGate drives the production gate over a real proxy client against
// the controlled upstream: one client for the configured workspace.
func runStartupGate(t *testing.T, up *selfCheckUpstream) selfCheckRun {
	t.Helper()
	client := proxy.NewClient(proxy.Config{
		ServerURL:   up.server.URL,
		WorkspaceID: selfCheckWorkspace,
		DaemonID:    "foreman-" + selfCheckWorkspace,
		Version:     "0.1.0",
	}, "mdt_"+selfCheckWorkspace)

	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	err := startup.RunSelfCheck(context.Background(), workspace.ScopeWorkspace, []*proxy.Client{client}, log)

	run := selfCheckRun{err: err}
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if decodeErr := json.Unmarshal([]byte(line), &event); decodeErr != nil {
			t.Fatalf("decode startup log line: %v (%s)", decodeErr, line)
		}
		run.events = append(run.events, event)
	}
	run.c17, run.c18 = up.calls()
	return run
}

// selfCheckEvents returns the decoded log lines carrying msg.
func selfCheckEvents(events []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, event := range events {
		if event["msg"] == msg {
			out = append(out, event)
		}
	}
	return out
}

// assertRefusesStartup pins the fatal class at the process seam: the gate
// returns an error main exits non-zero on, the abort event names the workspace
// and the failed stage, the transient warn never fires, and the configuration
// defect is not retried.
func assertRefusesStartup(t *testing.T, run selfCheckRun, stage string) map[string]any {
	t.Helper()
	if run.err == nil {
		t.Fatal("the startup gate returned nil, so the process would serve a configuration defect")
	}
	if !strings.Contains(run.err.Error(), selfCheckWorkspace) {
		t.Errorf("gate error = %v, want it to name the workspace", run.err)
	}
	aborted := selfCheckEvents(run.events, "foreman.startup_aborted")
	if len(aborted) != 1 {
		t.Fatalf("foreman.startup_aborted lines = %d, want exactly one", len(aborted))
	}
	if got := aborted[0]["stage"]; got != stage {
		t.Errorf("startup_aborted stage = %v, want %s", got, stage)
	}
	if got := aborted[0]["workspace_id"]; got != selfCheckWorkspace {
		t.Errorf("startup_aborted workspace_id = %v, want %s", got, selfCheckWorkspace)
	}
	if reason, _ := aborted[0]["err"].(string); reason == "" {
		t.Error("startup_aborted carries no reason")
	}
	if got := len(selfCheckEvents(run.events, "workspace.credential_check_failed")); got != 0 {
		t.Errorf("workspace.credential_check_failed lines = %d, want none (not transient)", got)
	}
	if got := len(selfCheckEvents(run.events, "workspace.discovery")); got != 0 {
		t.Errorf("workspace.discovery lines = %d, want none (startup was refused)", got)
	}
	if run.c17 != 1 {
		t.Errorf("C-17 calls = %d, want 1 (a configuration error is not retried)", run.c17)
	}
	return aborted[0]
}

// assertTransientContinues pins the non-fatal class: no process error, the
// transient warn fires once, the check runs to its discovery event, and no
// abort is recorded — a server blip must not turn into CrashLoopBackOff.
func assertTransientContinues(t *testing.T, run selfCheckRun, wantC17 int) {
	t.Helper()
	if run.err != nil {
		t.Fatalf("the startup gate refused a transient failure: %v", run.err)
	}
	if got := len(selfCheckEvents(run.events, "foreman.startup_aborted")); got != 0 {
		t.Fatalf("foreman.startup_aborted lines = %d, want none", got)
	}
	warns := selfCheckEvents(run.events, "workspace.credential_check_failed")
	if len(warns) != 1 {
		t.Fatalf("workspace.credential_check_failed lines = %d, want 1", len(warns))
	}
	if got := warns[0]["workspace_id"]; got != selfCheckWorkspace {
		t.Errorf("credential_check_failed workspace_id = %v, want %s", got, selfCheckWorkspace)
	}
	discovered := selfCheckEvents(run.events, "workspace.discovery")
	if len(discovered) != 1 {
		t.Fatalf("workspace.discovery lines = %d, want 1 (the gate must complete)", len(discovered))
	}
	if got := discovered[0]["source"]; got != proxy.DiscoverySourceWorkspaceScope {
		t.Errorf("discovery source = %v, want %s", got, proxy.DiscoverySourceWorkspaceScope)
	}
	refs, _ := discovered[0]["workspaces"].([]any)
	if len(refs) != 1 {
		t.Fatalf("discovery workspaces = %v, want one", discovered[0]["workspaces"])
	}
	ref, _ := refs[0].(map[string]any)
	if ref["id"] != selfCheckWorkspace {
		t.Errorf("discovery ref = %v, want id %s", ref, selfCheckWorkspace)
	}
	if run.c17 != wantC17 {
		t.Errorf("C-17 calls = %d, want %d", run.c17, wantC17)
	}
}

func TestStartupSelfCheckClassification(t *testing.T) {
	t.Run("rejected credential aborts startup", func(t *testing.T) {
		up := newSelfCheckUpstream(t)
		up.answerC17(http.StatusForbidden, `{"error":"forbidden"}`)

		aborted := assertRefusesStartup(t, runStartupGate(t, up), "workspace_credential")
		if reason, _ := aborted["err"].(string); !strings.Contains(reason, "403") {
			t.Errorf("startup_aborted err = %q, want the rejection status", reason)
		}
	})

	t.Run("workspace set mismatch aborts startup", func(t *testing.T) {
		up := newSelfCheckUpstream(t)
		up.answerC17(http.StatusOK, `[{"id":"ws-a","name":"A"},{"id":"ws-b","name":"B"}]`)

		aborted := assertRefusesStartup(t, runStartupGate(t, up), "workspace_credential")
		visible, _ := aborted["visible_workspaces"].([]any)
		if len(visible) != 2 {
			t.Errorf("visible_workspaces = %v, want the two answered ids", aborted["visible_workspaces"])
		}
	})

	t.Run("answer without a display name aborts startup", func(t *testing.T) {
		up := newSelfCheckUpstream(t)
		up.answerC17(http.StatusOK, `[{"id":"ws-a","name":"  "}]`)

		aborted := assertRefusesStartup(t, runStartupGate(t, up), "workspace_discovery")
		if reason, _ := aborted["err"].(string); !strings.Contains(reason, selfCheckWorkspace) {
			t.Errorf("startup_aborted err = %q, want it to name %s", reason, selfCheckWorkspace)
		}
	})

	t.Run("5xx only warns and completes", func(t *testing.T) {
		up := newSelfCheckUpstream(t)
		up.answerC17(http.StatusInternalServerError, `{"error":"boom"}`)

		assertTransientContinues(t, runStartupGate(t, up), 1)
	})

	t.Run("unreachable upstream only warns and completes", func(t *testing.T) {
		up := newSelfCheckUpstream(t)
		up.server.Close()

		assertTransientContinues(t, runStartupGate(t, up), 0)
	})

	t.Run("C-18 failure never refuses startup", func(t *testing.T) {
		up := newSelfCheckUpstream(t)
		up.answerC18(http.StatusInternalServerError)

		run := runStartupGate(t, up)
		if run.err != nil {
			t.Fatalf("a C-18 snapshot failure refused startup: %v", run.err)
		}
		if got := len(selfCheckEvents(run.events, "foreman.startup_aborted")); got != 0 {
			t.Fatalf("foreman.startup_aborted lines = %d, want none", got)
		}
		failed := selfCheckEvents(run.events, "runtime.profiles_snapshot_failed")
		if len(failed) != 1 {
			t.Fatalf("runtime.profiles_snapshot_failed lines = %d, want 1", len(failed))
		}
		if got := failed[0]["workspace_id"]; got != selfCheckWorkspace {
			t.Errorf("profiles_snapshot_failed workspace_id = %v, want %s", got, selfCheckWorkspace)
		}
		discovered := selfCheckEvents(run.events, "workspace.discovery")
		if len(discovered) != 1 || discovered[0]["source"] != proxy.DiscoverySourceWorkspaceScope {
			t.Fatalf("discovery event = %v, want the workspace-scoped source", discovered)
		}
		if run.c17 != 1 || run.c18 != 1 {
			t.Errorf("calls c17=%d c18=%d, want one each", run.c17, run.c18)
		}
	})
}
