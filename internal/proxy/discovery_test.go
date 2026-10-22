package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/workspace"
)

// newC17Stub answers C-17 with a programmable status/body.
func newC17Stub(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/daemon/workspaces" {
			http.NotFound(w, r)
			return
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// An entry without an id is skipped and the drop is logged: silently
// shrinking the discovery set would hide an upstream defect.
func TestListWorkspacesLogsSkippedEntries(t *testing.T) {
	srv := newC17Stub(t, http.StatusOK, `[{"id":"","name":"broken"},{"id":"ws-a","name":"A"}]`)
	var logs bytes.Buffer
	c := NewClient(Config{ServerURL: srv.URL, Version: "0.1.0"}, "mul_user",
		WithLogger(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))

	list, err := c.ListWorkspaces(t.Context())
	if err != nil {
		t.Fatalf("ListWorkspaces: %v", err)
	}
	if len(list) != 1 || list[0].ID != "ws-a" {
		t.Fatalf("list = %v, want only the well-formed entry", list)
	}
	if !strings.Contains(logs.String(), "workspace.discovery_entry_skipped") {
		t.Fatalf("logs = %s, want the skipped entry recorded", logs.String())
	}
}

func TestListWorkspaces(t *testing.T) {
	srv := newC17Stub(t, http.StatusOK, `[{"id":"ws-a","name":"A"},{"id":"ws-b","name":"B"}]`)
	c := NewClient(Config{ServerURL: srv.URL, Version: "0.1.0"}, "mul_user")

	list, err := c.ListWorkspaces(t.Context())
	if err != nil {
		t.Fatalf("ListWorkspaces: %v", err)
	}
	if len(list) != 2 || list[0].ID != "ws-a" || list[0].Name != "A" || list[1].ID != "ws-b" {
		t.Fatalf("list = %v, want the C-17 answer verbatim", list)
	}
}

func TestDiscoverWorkspaces(t *testing.T) {
	srv := newC17Stub(t, http.StatusOK, `[{"id":"ws-a","name":"A"},{"id":"ws-b","name":"B"}]`)
	c := NewClient(Config{ServerURL: srv.URL, Version: "0.1.0"}, "mul_user")

	disc, err := c.DiscoverWorkspaces(t.Context(), nil)
	if err != nil {
		t.Fatalf("DiscoverWorkspaces: %v", err)
	}
	if disc.Source != DiscoverySourceUserVisible || disc.Scope != "user" {
		t.Errorf("discovery = %+v, want the visible-set source", disc)
	}
	if len(disc.Set) != 2 || len(disc.Excluded) != 0 {
		t.Errorf("set/excluded = %v/%v, want the whole visible set", disc.Set, disc.Excluded)
	}

	disc, err = c.DiscoverWorkspaces(t.Context(), []string{"ws-a"})
	if err != nil {
		t.Fatalf("DiscoverWorkspaces (manifest): %v", err)
	}
	if disc.Source != DiscoverySourceUserManifest {
		t.Errorf("source = %q, want the manifest source", disc.Source)
	}
	if len(disc.Set) != 1 || disc.Set[0].ID != "ws-a" {
		t.Errorf("set = %v, want [ws-a]", disc.Set)
	}
	if len(disc.Excluded) != 1 || disc.Excluded[0].ID != "ws-b" {
		t.Errorf("excluded = %v, want [ws-b]", disc.Excluded)
	}
}

func TestDiscoverWorkspacesFailures(t *testing.T) {
	t.Run("empty set", func(t *testing.T) {
		srv := newC17Stub(t, http.StatusOK, `[]`)
		c := NewClient(Config{ServerURL: srv.URL, Version: "0.1.0"}, "mul_user")
		if _, err := c.DiscoverWorkspaces(t.Context(), nil); err == nil {
			t.Fatal("an empty discovery set must be an error")
		}
	})
	t.Run("auth rejected", func(t *testing.T) {
		srv := newC17Stub(t, http.StatusForbidden, `{"error":"forbidden"}`)
		c := NewClient(Config{ServerURL: srv.URL, Version: "0.1.0"}, "mul_user")
		_, err := c.DiscoverWorkspaces(t.Context(), nil)
		if err == nil {
			t.Fatal("a rejected discovery must be an error")
		}
		var status *StatusError
		if !errors.As(err, &status) || status.Code != http.StatusForbidden {
			t.Fatalf("error = %v, want a 403 StatusError", err)
		}
		if !IsCredentialConfigError(err) {
			t.Error("the rejection must classify as a startup configuration error")
		}
	})
	t.Run("invisible manifest entry", func(t *testing.T) {
		srv := newC17Stub(t, http.StatusOK, `[{"id":"ws-a","name":"A"}]`)
		c := NewClient(Config{ServerURL: srv.URL, Version: "0.1.0"}, "mul_user")
		_, err := c.DiscoverWorkspaces(t.Context(), []string{"ws-a", "ws-ghost"})
		if err == nil || !strings.Contains(err.Error(), "ws-ghost") {
			t.Fatalf("error = %v, want it to name ws-ghost", err)
		}
	})
}

func TestValidateWorkspaceScope(t *testing.T) {
	t.Run("exact answer passes and seeds the name", func(t *testing.T) {
		srv := newC17Stub(t, http.StatusOK, `[{"id":"ws-a","name":"Workspace A"}]`)
		c := NewClient(Config{ServerURL: srv.URL, WorkspaceID: "ws-a", Version: "0.1.0"}, "mdt_a")
		if err := c.ValidateWorkspaceScope(t.Context()); err != nil {
			t.Fatalf("ValidateWorkspaceScope: %v", err)
		}
		if _, name := c.Workspace(); name != "Workspace A" {
			t.Errorf("cached name = %q, want the C-17 name", name)
		}
	})
	t.Run("superset is a mismatch", func(t *testing.T) {
		srv := newC17Stub(t, http.StatusOK, `[{"id":"ws-a","name":"A"},{"id":"ws-b","name":"B"}]`)
		c := NewClient(Config{ServerURL: srv.URL, WorkspaceID: "ws-a", Version: "0.1.0"}, "mdt_a")
		err := c.ValidateWorkspaceScope(t.Context())
		var mismatch *WorkspaceMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("error = %v, want a WorkspaceMismatchError", err)
		}
		if len(mismatch.Visible) != 2 {
			t.Errorf("visible = %v, want both ids", mismatch.Visible)
		}
		if !IsCredentialConfigError(err) {
			t.Error("the mismatch must classify as a startup configuration error")
		}
	})
	t.Run("empty answer is a mismatch", func(t *testing.T) {
		srv := newC17Stub(t, http.StatusOK, `[]`)
		c := NewClient(Config{ServerURL: srv.URL, WorkspaceID: "ws-a", Version: "0.1.0"}, "mdt_a")
		var mismatch *WorkspaceMismatchError
		if err := c.ValidateWorkspaceScope(t.Context()); !errors.As(err, &mismatch) {
			t.Fatalf("error = %v, want a WorkspaceMismatchError", err)
		}
	})
}

func TestDiscoveryEventRendersSetAndExclusions(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	LogDiscoveryEvent(log, Discovery{
		Source: DiscoverySourceUserManifest,
		Scope:  "user",
		Set:    []workspace.Ref{{ID: "ws-a", Name: "A"}},
		Excluded: []workspace.Ref{
			{ID: "ws-b", Name: "B"},
		},
	})
	var event map[string]any
	if err := json.Unmarshal([]byte(buf.String()), &event); err != nil {
		t.Fatalf("decode event: %v (%s)", err, buf.String())
	}
	if event["msg"] != "workspace.discovery" || event["source"] != DiscoverySourceUserManifest {
		t.Errorf("event = %v", event)
	}
	if workspaces, _ := event["workspaces"].([]any); len(workspaces) != 1 {
		t.Errorf("event workspaces = %v", event["workspaces"])
	}
	if excluded, _ := event["excluded"].([]any); len(excluded) != 1 {
		t.Errorf("event excluded = %v", event["excluded"])
	}

	buf.Reset()
	LogServingEvent(log, []workspace.Ref{{ID: "ws-a", Name: "A"}}, []workspace.Ref{{ID: "ws-b", Name: "B"}})
	if err := json.Unmarshal([]byte(buf.String()), &event); err != nil {
		t.Fatalf("decode serving event: %v", err)
	}
	if event["msg"] != "workspace.serving" {
		t.Errorf("event = %v", event)
	}
	if unregistered, _ := event["unregistered"].([]any); len(unregistered) != 1 {
		t.Errorf("event unregistered = %v", event["unregistered"])
	}
}
