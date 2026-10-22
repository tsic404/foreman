// TC-tech-multiws-09 implements AC-94: with only a user-scoped credential and
// no manifest, the discovery set is every visible workspace; the startup probe
// registers to all of them, Foreman declares and serves each, and both sides'
// tasks run. The startup events name the discovery source and set.
package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/scheduler"
	"github.com/tsic404/foreman/internal/workspace"
)

func TestTechMultiWS09NoManifestDiscovery(t *testing.T) {
	stub := newDiscoveryStub(t, []workspace.Ref{
		{ID: "ws-a", Name: "Workspace A"},
		{ID: "ws-b", Name: "Workspace B"},
	})
	m, discovered := newUserScopedWS(t, stub, nil)

	// Discovery: source = the user-scoped credential's visible set; the set
	// carries every visible workspace with its display name.
	if discovered.Source != "user_scope_visible" {
		t.Errorf("discovery source = %q, want user_scope_visible", discovered.Source)
	}
	if len(discovered.Set) != 2 || discovered.Set[0].ID != "ws-a" || discovered.Set[1].ID != "ws-b" {
		t.Fatalf("discovery set = %v, want [ws-a ws-b]", discovered.Set)
	}
	if discovered.Set[0].Name != "Workspace A" || discovered.Set[1].Name != "Workspace B" {
		t.Errorf("discovery names = %q/%q, want the C-17 names", discovered.Set[0].Name, discovered.Set[1].Name)
	}
	if len(discovered.Excluded) != 0 {
		t.Errorf("excluded = %v, want none without a manifest", discovered.Excluded)
	}

	// §9.1 发现事件: one line with source and set (id + display name).
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	proxy.LogDiscoveryEvent(log, discovered)
	var event map[string]any
	if err := json.Unmarshal(logs.Bytes(), &event); err != nil {
		t.Fatalf("decode discovery event: %v (%s)", err, logs.String())
	}
	if event["msg"] != "workspace.discovery" || event["source"] != "user_scope_visible" {
		t.Errorf("discovery event = %v, want workspace.discovery with the source", event)
	}
	workspaces, _ := event["workspaces"].([]any)
	if len(workspaces) != 2 {
		t.Fatalf("discovery event workspaces = %v, want two entries", event["workspaces"])
	}
	first, _ := workspaces[0].(map[string]any)
	if first["id"] != "ws-a" || first["name"] != "Workspace A" {
		t.Errorf("discovery event entry = %v, want id+name of ws-a", first)
	}

	// The probe registers to the whole discovery set (S-1's answer).
	m.rebuildScheduler(func(cfg *schedulerConfig) { cfg.StartupProbeTimeout = 5 * time.Second })
	m.jobs.onCreateJob = probeDaemon(t, m, []string{"ws-a", "ws-b"}, "", nil)
	discoveries, err := m.sched.RunStartupProbe(t.Context())
	if err != nil {
		t.Fatalf("RunStartupProbe: %v", err)
	}
	if len(discoveries) != 2 {
		t.Fatalf("probe registrations = %v, want both discovered workspaces", discoveries)
	}

	// Foreman's own C-1 per workspace: one deployment identity per workspace,
	// distinct across them, each carrying its own workspace id.
	for _, workspaceID := range []string{"ws-a", "ws-b"} {
		m.upstream[workspaceID].setRuntimeID("omp", "", "rid-"+workspaceID)
		m.clients[workspaceID].Declare(discoveries[workspaceID])
		if err := m.clients[workspaceID].Register(t.Context()); err != nil {
			t.Fatalf("C-1 for %s: %v", workspaceID, err)
		}
	}
	daemonIDs := map[string]string{}
	for _, workspaceID := range []string{"ws-a", "ws-b"} {
		registers := m.upstream[workspaceID].calls("POST", "/api/daemon/register")
		if len(registers) != 1 {
			t.Fatalf("%s register calls = %d, want 1", workspaceID, len(registers))
		}
		var body struct {
			WorkspaceID string `json:"workspace_id"`
			DaemonID    string `json:"daemon_id"`
		}
		if err := json.Unmarshal(registers[0].Body, &body); err != nil {
			t.Fatalf("decode register body: %v", err)
		}
		if body.WorkspaceID != workspaceID {
			t.Errorf("register workspace_id = %q, want %q", body.WorkspaceID, workspaceID)
		}
		if body.DaemonID != "foreman-"+workspaceID {
			t.Errorf("register daemon_id = %q, want the derived identity foreman-%s", body.DaemonID, workspaceID)
		}
		daemonIDs[workspaceID] = body.DaemonID
	}
	if daemonIDs["ws-a"] == daemonIDs["ws-b"] {
		t.Fatalf("both workspaces registered under %q (F2)", daemonIDs["ws-a"])
	}

	// §9.1 服务事件: the effective service scope names both workspaces.
	logs.Reset()
	proxy.LogServingEvent(log, discovered.Set, nil)
	var serving map[string]any
	if err := json.Unmarshal(logs.Bytes(), &serving); err != nil {
		t.Fatalf("decode serving event: %v (%s)", err, logs.String())
	}
	served, _ := serving["workspaces"].([]any)
	if serving["msg"] != "workspace.serving" || len(served) != 2 {
		t.Fatalf("serving event = %v, want both workspaces", serving)
	}
	if unregistered, _ := serving["unregistered"].([]any); len(unregistered) != 0 {
		t.Errorf("serving event unregistered = %v, want none", unregistered)
	}

	// Both sides' tasks run: claim → Job → delivery → terminal.
	for _, workspaceID := range []string{"ws-a", "ws-b"} {
		taskID := "task-" + workspaceID
		entry := m.claim(workspaceID, taskID, "rid-"+workspaceID)
		if entry.WorkspaceID != workspaceID {
			t.Fatalf("entry workspace = %q, want %q", entry.WorkspaceID, workspaceID)
		}
		if _, ok := m.reg.MarkDaemonRegistered(entry.DaemonID, time.Now()); !ok {
			t.Fatalf("MarkDaemonRegistered(%s) refused", entry.DaemonID)
		}
		delivery, err := m.sched.Deliver(context.Background(), entry.WorkspaceID, entry.DaemonID, entry.JobRuntimeID, 1)
		tasks := delivery.Tasks
		if err != nil || len(tasks) != 1 {
			t.Fatalf("Deliver(%s) = %d tasks, %v", workspaceID, len(tasks), err)
		}
		entry, _ = m.reg.Get(taskID)
		code, body, err := m.sched.OnReport(t.Context(), scheduler.EPComplete, entry,
			[]byte(`{"status":"completed"}`), "")
		if err != nil || code/100 != 2 {
			t.Fatalf("complete %s = %d/%s err=%v", taskID, code, body, err)
		}
	}
	for _, workspaceID := range []string{"ws-a", "ws-b"} {
		if got := len(m.tasksByWorkspace(workspaceID)); got != 1 {
			t.Errorf("workspace %s entries = %d, want 1", workspaceID, got)
		}
	}
}
