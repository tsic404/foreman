package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/startup"
	"github.com/tsic404/foreman/internal/workspace"
)

// startupStub answers C-17 and C-18 with programmable answers and records the
// calls each face saw.
type startupStub struct {
	mu        sync.Mutex
	c17Status int
	c17Body   string
	c18Status int
	c17Calls  int
	c18Calls  int
	server    *httptest.Server
}

func newStartupStub(t *testing.T) *startupStub {
	t.Helper()
	stub := &startupStub{c17Status: http.StatusOK, c17Body: `[{"id":"ws-a","name":"Workspace A"}]`, c18Status: http.StatusNotFound}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/daemon/workspaces", func(w http.ResponseWriter, _ *http.Request) {
		stub.mu.Lock()
		status, body := stub.c17Status, stub.c17Body
		stub.c17Calls++
		stub.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
	mux.HandleFunc("GET /api/daemon/workspaces/{ws}/runtime-profiles", func(w http.ResponseWriter, _ *http.Request) {
		stub.mu.Lock()
		status := stub.c18Status
		stub.c18Calls++
		stub.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	})
	stub.server = httptest.NewServer(mux)
	t.Cleanup(stub.server.Close)
	return stub
}

func (s *startupStub) setC17(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.c17Status, s.c17Body = status, body
}

func (s *startupStub) calls() (c17, c18 int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.c17Calls, s.c18Calls
}

func startupClient(url, workspaceID string) *proxy.Client {
	return proxy.NewClient(proxy.Config{
		ServerURL:   url,
		WorkspaceID: workspaceID,
		DaemonID:    "foreman-" + workspaceID,
		Version:     "0.1.0",
	}, "mdt_"+workspaceID)
}

// events decodes every JSON log line carrying msg.
func events(t *testing.T, logs *bytes.Buffer, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("decode log line: %v (%s)", err, line)
		}
		if event["msg"] == msg {
			out = append(out, event)
		}
	}
	return out
}

// TestRunStartupSelfCheckWorkspaceScope pins the fail-fast boundary of §1.1:
// only a configuration error (C-17 set mismatch, 401/403) refuses startup. A
// transient upstream failure (5xx, unreachable) warns and leaves the loops to
// converge — otherwise a server blip CrashLoopBackOffs every existing
// single-workspace deployment (AC-86).
func TestRunStartupSelfCheckWorkspaceScope(t *testing.T) {
	t.Run("rejected credential aborts startup", func(t *testing.T) {
		stub := newStartupStub(t)
		stub.setC17(http.StatusForbidden, `{"error":"forbidden"}`)
		var logs bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&logs, nil))
		err := startup.RunSelfCheck(t.Context(), workspace.ScopeWorkspace,
			[]*proxy.Client{startupClient(stub.server.URL, "ws-a")}, log)
		if err == nil {
			t.Fatal("a 403 C-17 answer must refuse startup")
		}
		if !proxy.IsCredentialConfigError(err) {
			t.Errorf("error = %v, want the configuration-error classification", err)
		}
		if got := events(t, &logs, "foreman.startup_aborted"); len(got) != 1 {
			t.Fatalf("startup_aborted lines = %d, want 1", len(got))
		}
	})

	t.Run("set mismatch aborts startup", func(t *testing.T) {
		stub := newStartupStub(t)
		stub.setC17(http.StatusOK, `[{"id":"ws-a","name":"A"},{"id":"ws-b","name":"B"}]`)
		var logs bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&logs, nil))
		err := startup.RunSelfCheck(t.Context(), workspace.ScopeWorkspace,
			[]*proxy.Client{startupClient(stub.server.URL, "ws-a")}, log)
		if err == nil {
			t.Fatal("a C-17 superset must refuse startup")
		}
		var mismatch *proxy.WorkspaceMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("error = %v, want a WorkspaceMismatchError", err)
		}
		aborted := events(t, &logs, "foreman.startup_aborted")
		if len(aborted) != 1 {
			t.Fatalf("startup_aborted lines = %d, want 1", len(aborted))
		}
		if visible, _ := aborted[0]["visible_workspaces"].([]any); len(visible) != 2 {
			t.Errorf("visible_workspaces = %v, want both ids", aborted[0]["visible_workspaces"])
		}
	})

	t.Run("5xx only warns and continues", func(t *testing.T) {
		stub := newStartupStub(t)
		stub.setC17(http.StatusInternalServerError, `{"error":"boom"}`)
		var logs bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&logs, nil))
		if err := startup.RunSelfCheck(t.Context(), workspace.ScopeWorkspace,
			[]*proxy.Client{startupClient(stub.server.URL, "ws-a")}, log); err != nil {
			t.Fatalf("a 5xx C-17 answer must not refuse startup: %v", err)
		}
		if got := events(t, &logs, "foreman.startup_aborted"); len(got) != 0 {
			t.Fatalf("startup_aborted lines = %d, want none", len(got))
		}
		if got := events(t, &logs, "workspace.credential_check_failed"); len(got) != 1 {
			t.Fatalf("credential_check_failed lines = %d, want 1", len(got))
		}
	})

	t.Run("unreachable upstream only warns and continues", func(t *testing.T) {
		stub := newStartupStub(t)
		stub.server.Close()
		var logs bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&logs, nil))
		if err := startup.RunSelfCheck(t.Context(), workspace.ScopeWorkspace,
			[]*proxy.Client{startupClient(stub.server.URL, "ws-a")}, log); err != nil {
			t.Fatalf("an unreachable C-17 must not refuse startup: %v", err)
		}
		if got := events(t, &logs, "workspace.credential_check_failed"); len(got) != 1 {
			t.Fatalf("credential_check_failed lines = %d, want 1", len(got))
		}
	})

	t.Run("exact set passes and emits the discovery event", func(t *testing.T) {
		stub := newStartupStub(t)
		var logs bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&logs, nil))
		if err := startup.RunSelfCheck(t.Context(), workspace.ScopeWorkspace,
			[]*proxy.Client{startupClient(stub.server.URL, "ws-a")}, log); err != nil {
			t.Fatalf("RunSelfCheck: %v", err)
		}
		discovered := events(t, &logs, "workspace.discovery")
		if len(discovered) != 1 || discovered[0]["source"] != proxy.DiscoverySourceWorkspaceScope {
			t.Fatalf("discovery event = %v, want the workspace-scoped source", discovered)
		}
	})

	t.Run("C-18 failure is never fatal", func(t *testing.T) {
		stub := newStartupStub(t)
		stub.mu.Lock()
		stub.c18Status = http.StatusInternalServerError
		stub.mu.Unlock()
		var logs bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&logs, nil))
		if err := startup.RunSelfCheck(t.Context(), workspace.ScopeUser,
			[]*proxy.Client{startupClient(stub.server.URL, "ws-a")}, log); err != nil {
			t.Fatalf("a C-18 failure must not refuse startup: %v", err)
		}
		c17, c18 := stub.calls()
		if c17 != 0 {
			t.Errorf("C-17 calls in the user-scoped form = %d, want 0 (discovery already ran)", c17)
		}
		if c18 != 1 {
			t.Errorf("C-18 calls = %d, want 1", c18)
		}
	})
}

// TestDiscoverUserScope pins AC-96's process-level face: a failed discovery
// returns the error the caller propagates (main exits non-zero), emits the
// failure event with the credential scope, and produces no workspace set —
// never a half-configured service.
func TestDiscoverUserScope(t *testing.T) {
	deployment := workspace.Deployment{Scope: workspace.ScopeUser, UserToken: "mul_user", BaseDaemonID: "foreman"}

	t.Run("success derives one identity per visible workspace", func(t *testing.T) {
		stub := newStartupStub(t)
		stub.setC17(http.StatusOK, `[{"id":"ws-a","name":"A"},{"id":"ws-b","name":"B"}]`)
		var logs bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&logs, nil))
		set, discovered, err := discoverUserScope(t.Context(), deployment,
			proxy.Config{ServerURL: stub.server.URL, Version: "0.1.0"}, nil, log)
		if err != nil {
			t.Fatalf("discoverUserScope: %v", err)
		}
		if set.Len() != 2 {
			t.Fatalf("set = %v, want both workspaces", set.IDs())
		}
		a, _ := set.Get("ws-a")
		b, _ := set.Get("ws-b")
		if a.DaemonID == b.DaemonID {
			t.Fatalf("derived identities collide: %q", a.DaemonID)
		}
		if a.Token != "mul_user" || b.Token != "mul_user" {
			t.Error("discovered identities must share the single user-scoped credential")
		}
		if discovered == nil || discovered.Source != proxy.DiscoverySourceUserVisible {
			t.Fatalf("discovery = %+v, want the visible-set source", discovered)
		}
		if got := events(t, &logs, "workspace.discovery"); len(got) != 1 {
			t.Fatalf("discovery event lines = %d, want 1", len(got))
		}
	})

	t.Run("manifest narrows the set", func(t *testing.T) {
		stub := newStartupStub(t)
		stub.setC17(http.StatusOK, `[{"id":"ws-a","name":"A"},{"id":"ws-b","name":"B"}]`)
		withManifest := deployment
		withManifest.Manifest = []string{"ws-a"}
		var logs bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&logs, nil))
		set, discovered, err := discoverUserScope(t.Context(), withManifest,
			proxy.Config{ServerURL: stub.server.URL, Version: "0.1.0"}, nil, log)
		if err != nil {
			t.Fatalf("discoverUserScope: %v", err)
		}
		if set.Len() != 1 || set.IDs()[0] != "ws-a" {
			t.Fatalf("set = %v, want only ws-a", set.IDs())
		}
		if discovered.Source != proxy.DiscoverySourceUserManifest || len(discovered.Excluded) != 1 {
			t.Fatalf("discovery = %+v, want the manifest source and the excluded ws-b", discovered)
		}
	})

	for name, program := range map[string]func(*startupStub){
		"upstream unreachable": func(stub *startupStub) { stub.server.Close() },
		"auth rejected":        func(stub *startupStub) { stub.setC17(http.StatusForbidden, `{"error":"forbidden"}`) },
		"empty set":            func(stub *startupStub) { stub.setC17(http.StatusOK, `[]`) },
	} {
		t.Run(name+" refuses startup", func(t *testing.T) {
			stub := newStartupStub(t)
			program(stub)
			var logs bytes.Buffer
			log := slog.New(slog.NewJSONHandler(&logs, nil))
			set, discovered, err := discoverUserScope(t.Context(), deployment,
				proxy.Config{ServerURL: stub.server.URL, Version: "0.1.0"}, nil, log)
			if err == nil {
				t.Fatal("a failed discovery must return the error main turns into a non-zero exit")
			}
			if discovered != nil || set.Len() != 0 {
				t.Fatalf("failed discovery produced set=%v discovery=%+v, want no service", set.IDs(), discovered)
			}
			failures := events(t, &logs, "workspace.discovery_failed")
			if len(failures) != 1 {
				t.Fatalf("discovery_failed lines = %d, want 1", len(failures))
			}
			if failures[0]["credential_scope"] != "user" {
				t.Errorf("credential_scope = %v, want user", failures[0]["credential_scope"])
			}
			if reason, _ := failures[0]["err"].(string); reason == "" {
				t.Errorf("failure event carries no reason: %v", failures[0])
			}
		})
	}
}

// TestIsolateWorkspaceLoopSharedCredential pins the user-scoped deployment's
// failure face through the supervisor path: one rejected credential shared by
// every workspace's control loop makes all of them back off and retry — none
// exits, and no request can continue past the credential.
func TestIsolateWorkspaceLoopSharedCredential(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	credentialErr := &proxy.StatusError{Code: http.StatusUnauthorized}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var mu sync.Mutex
	retries := map[string]int{}
	done := make(chan string, 2)
	for _, workspaceID := range []string{"ws-a", "ws-b"} {
		go func(workspaceID string) {
			err := isolateWorkspaceLoop(ctx, "claim/"+workspaceID, log, time.Millisecond, func(context.Context) error {
				mu.Lock()
				retries[workspaceID]++
				n := retries[workspaceID]
				mu.Unlock()
				if n < 3 {
					return credentialErr
				}
				return nil
			})
			if err != nil {
				t.Errorf("%s loop returned %v, want nil after the retries", workspaceID, err)
			}
			done <- workspaceID
		}(workspaceID)
	}
	deadline := time.After(2 * time.Second)
	for range 2 {
		select {
		case <-done:
		case <-deadline:
			t.Fatal("a workspace loop did not survive the shared credential failure")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, workspaceID := range []string{"ws-a", "ws-b"} {
		if retries[workspaceID] < 3 {
			t.Errorf("%s retries = %d, want >= 3 (every workspace backs off on the shared credential)", workspaceID, retries[workspaceID])
		}
	}
}

// TestC17AnswerWithoutDisplayNameAbortsStartup pins the C-17 completeness rule
// at the startup seam: a 200 answer carrying a workspace without a display
// name is a malformed answer, not a transient failure — the process refuses to
// start (both credential forms) instead of booting with the name authority
// unseeded and every absent partition key degraded to a warning.
func TestC17AnswerWithoutDisplayNameAbortsStartup(t *testing.T) {
	t.Run("workspace-scoped self-check aborts and names the workspace", func(t *testing.T) {
		stub := newStartupStub(t)
		stub.setC17(http.StatusOK, `[{"id":"ws-a","name":""}]`)
		var logs bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&logs, nil))

		err := startup.RunSelfCheck(t.Context(), workspace.ScopeWorkspace,
			[]*proxy.Client{startupClient(stub.server.URL, "ws-a")}, log)
		if err == nil {
			t.Fatal("a C-17 answer without a display name must refuse startup")
		}
		var answer *proxy.DiscoveryAnswerError
		if !errors.As(err, &answer) || answer.WorkspaceID != "ws-a" {
			t.Fatalf("error = %v, want a DiscoveryAnswerError naming ws-a", err)
		}
		if !proxy.IsStartupFatal(err) {
			t.Error("the malformed answer must classify as startup-fatal")
		}
		aborted := events(t, &logs, "foreman.startup_aborted")
		if len(aborted) != 1 {
			t.Fatalf("startup_aborted lines = %d, want 1", len(aborted))
		}
		if aborted[0]["stage"] != "workspace_discovery" {
			t.Errorf("startup_aborted stage = %v, want workspace_discovery", aborted[0]["stage"])
		}
		if summary, _ := aborted[0]["err"].(string); !strings.Contains(summary, "ws-a") {
			t.Errorf("startup_aborted err = %q, want it to name ws-a", summary)
		}
		if got := events(t, &logs, "workspace.credential_check_failed"); len(got) != 0 {
			t.Errorf("credential_check_failed lines = %d, want none (not transient)", len(got))
		}
	})

	t.Run("user-scoped discovery aborts", func(t *testing.T) {
		stub := newStartupStub(t)
		stub.setC17(http.StatusOK, `[{"id":"ws-a","name":"A"},{"id":"ws-b","name":""}]`)
		var logs bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&logs, nil))
		deployment := workspace.Deployment{Scope: workspace.ScopeUser, UserToken: "mul_user", BaseDaemonID: "foreman"}

		_, _, err := discoverUserScope(t.Context(), deployment,
			proxy.Config{ServerURL: stub.server.URL, Version: "0.1.0"}, nil, log)
		if err == nil {
			t.Fatal("a user-scoped discovery answer without a display name must refuse startup")
		}
		var answer *proxy.DiscoveryAnswerError
		if !errors.As(err, &answer) || answer.WorkspaceID != "ws-b" {
			t.Fatalf("error = %v, want a DiscoveryAnswerError naming ws-b", err)
		}
		failed := events(t, &logs, "workspace.discovery_failed")
		if len(failed) != 1 {
			t.Fatalf("workspace.discovery_failed lines = %d, want 1", len(failed))
		}
		if failed[0]["credential_scope"] != "user" {
			t.Errorf("credential_scope = %v, want user", failed[0]["credential_scope"])
		}
	})
}
