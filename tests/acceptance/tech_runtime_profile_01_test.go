// TC-tech-runtime-profile-01 implements AC-57 in the form this repo can
// execute: the declaration identity (provider, profile_id), the S-3 three-state
// replay, S-4 selection and profile_id echo, the builtin + multi-profile
// coexistence, the zero-hit refusal, and the runtime refresh sequence
// (replay-source-first, C-1 commit, per-entry deregistration, rollback).
// Cluster-level read-back of the server-side runtime rows stays QA-side.
package acceptance

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// profileListBody is one C-18 answer with the given profile ids.
func profileListBody(profiles ...string) string {
	entries := make([]map[string]any, 0, len(profiles))
	for _, id := range profiles {
		entries = append(entries, map[string]any{
			"id": id, "display_name": "display-" + id, "runtime_type": "omp",
			"protocol_family": "omp", "command_name": id + "-cli",
			"fixed_args": []string{"--" + id}, "visibility": "workspace", "enabled": true,
		})
	}
	raw, _ := json.Marshal(map[string]any{
		"workspace_id": "ws-1", "runtime_profiles": entries,
	})
	return string(raw)
}

// taskToken issues the Job Token of a claimed task.
func taskToken(t *testing.T, h *harness, taskID string) string {
	t.Helper()
	token, err := h.issuer.Issue("fm-"+taskID, taskID, "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue(fm-%s): %v", taskID, err)
	}
	return token
}

// registerTaskDaemon drives one S-4 for the task's daemon.
func registerJobDaemon(t *testing.T, h *harness, taskID, token, runtimes string) (int, []byte) {
	t.Helper()
	return h.call("POST", "/api/daemon/register", token,
		[]byte(`{"daemon_id":"fm-`+taskID+`","runtimes":`+runtimes+`}`))
}

func TestTechRuntimeProfile01DeclarationAndThreeStateReplay(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.setRuntimeID("omp", "", "rid-omp")
	h.upstream.setRuntimeID("omp", "prof-a", "rid-prof-a")

	claim := func(taskID, runtimeID string) string {
		payload := json.RawMessage(`{
			"id": "` + taskID + `", "agent_id": "agent-1", "workspace_id": "ws-1",
			"runtime_id": "` + runtimeID + `", "issue_id": "issue-1", "issue_identifier": "TSI-1"
		}`)
		if err := h.sched.OnClaim(t.Context(), payload); err != nil {
			t.Fatalf("OnClaim(%s): %v", taskID, err)
		}
		return taskToken(t, h, taskID)
	}
	tok := claim("task-1", "rid-prof-a")

	// C-18 lists one profile the template cannot resolve; the probe never
	// declared it, so it must not produce a runtime (no synthesis, ADR-043 D).
	h.upstream.setProfiles(200, profileListBody("prof-a", "prof-missing"))
	if err := h.client.RuntimeProfiles(t.Context()); err != nil {
		t.Fatalf("C18: %v", err)
	}
	// S-3 replays the C-18 body byte for byte.
	code, s3 := h.call("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", tok, nil)
	if code != 200 {
		t.Fatalf("S-3 = %d: %s", code, s3)
	}
	if want := profileListBody("prof-a", "prof-missing"); string(s3) != want {
		t.Errorf("S-3 body = %s, want the C-18 body verbatim (%s)", s3, want)
	}

	// The probe registration declares builtin + prof-a; a repeated identity
	// merges into one entry (same (type, profile_id) unique per declaration).
	discovery := registry.Discovery{
		CLIVersion: "0.6.1",
		Runtimes: []registry.DiscoveredRuntime{
			{Name: "foreman-omp", Type: "omp", Version: "0.6.1"},
			{Name: "foreman-prof-a", Type: "omp", Version: "0.6.2", ProfileID: "prof-a"},
			{Name: "foreman-prof-a-dup", Type: "omp", Version: "0.6.2", ProfileID: "prof-a"},
		},
	}
	h.declare(discovery)
	register := h.upstream.calls("POST", "/api/daemon/register")
	if len(register) != 1 {
		t.Fatalf("C1 calls = %d, want 1", len(register))
	}
	var c1 struct {
		Runtimes []struct {
			Name      string `json:"name"`
			Type      string `json:"type"`
			ProfileID string `json:"profile_id"`
		} `json:"runtimes"`
	}
	if err := json.Unmarshal(register[0].Body, &c1); err != nil {
		t.Fatalf("decode C1: %v (%s)", err, register[0].Body)
	}
	if len(c1.Runtimes) != 2 {
		t.Fatalf("C1 runtimes = %+v, want builtin + one merged profile entry", c1.Runtimes)
	}
	if c1.Runtimes[0].ProfileID != "" || c1.Runtimes[0].Name != "foreman-omp" {
		t.Errorf("builtin entry = %+v", c1.Runtimes[0])
	}
	if c1.Runtimes[1].ProfileID != "prof-a" || c1.Runtimes[1].Name != "foreman-prof-a" {
		t.Errorf("profile entry = %+v", c1.Runtimes[1])
	}
	if ids := h.client.RuntimeIDs(); len(ids) != 2 || ids[0] != "rid-omp" || ids[1] != "rid-prof-a" {
		t.Fatalf("runtime ids = %v", ids)
	}
	// The unresolvable profile produced no runtime line.
	for _, ref := range h.client.RuntimeRefs() {
		if ref.ProfileID == "prof-missing" {
			t.Error("an unresolvable profile must not be declared")
		}
	}

	// S-4 selects by identity and echoes profile_id (builtin → null).
	code, body := registerJobDaemon(t, h, "task-1", tok,
		`[{"name":"foreman-omp","type":"omp"},{"name":"foreman-prof-a","type":"omp","profile_id":"prof-a"}]`)
	if code != 200 {
		t.Fatalf("S-4 profile = %d: %s", code, body)
	}
	var reg struct {
		Runtimes []map[string]json.RawMessage `json:"runtimes"`
	}
	if err := json.Unmarshal(body, &reg); err != nil || len(reg.Runtimes) != 1 {
		t.Fatalf("S-4 body = %s (%v)", body, err)
	}
	if got := string(reg.Runtimes[0]["profile_id"]); got != `"prof-a"` {
		t.Errorf("profile task S-4 profile_id = %s, want prof-a", got)
	}

	// The builtin task of the same provider selects the builtin entry.
	builtinTok := claim("task-2", "rid-omp")
	code, body = registerJobDaemon(t, h, "task-2", builtinTok,
		`[{"name":"foreman-prof-a","type":"omp","profile_id":"prof-a"},{"name":"foreman-omp","type":"omp"}]`)
	if code != 200 {
		t.Fatalf("S-4 builtin = %d: %s", code, body)
	}
	if err := json.Unmarshal(body, &reg); err != nil || len(reg.Runtimes) != 1 {
		t.Fatalf("S-4 builtin body = %s (%v)", body, err)
	}
	if got := string(reg.Runtimes[0]["profile_id"]); got != "null" {
		t.Errorf("builtin S-4 profile_id = %s, want null", got)
	}

	// Declared but absent in the Job: zero hit refuses registration (409) and
	// settles the entry; it must never fall back to the same type's builtin.
	refusedTok := claim("task-3", "rid-prof-a")
	code, body = registerJobDaemon(t, h, "task-3", refusedTok,
		`[{"name":"foreman-omp","type":"omp"}]`)
	if code != 409 {
		t.Fatalf("zero-hit S-4 = %d: %s, want 409", code, body)
	}
	if entry, ok := h.reg.Get("task-3"); !ok || !entry.IsTerminal() {
		t.Errorf("the refused task did not settle: %+v (ok=%v)", entry, ok)
	}
}

func TestTechRuntimeProfile01RefreshSequence(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.setRuntimeID("omp", "", "rid-omp")
	h.upstream.setRuntimeID("omp", "prof-a", "rid-prof-a")
	h.upstream.setRuntimeID("omp", "prof-b", "rid-prof-b")
	h.upstream.setProfiles(200, profileListBody("prof-a"))
	if err := h.client.RuntimeProfiles(t.Context()); err != nil {
		t.Fatalf("C18: %v", err)
	}
	discovery := registry.Discovery{
		CLIVersion: "0.6.1",
		Runtimes: []registry.DiscoveredRuntime{
			{Name: "foreman-omp", Type: "omp", Version: "0.6.1"},
			{Name: "foreman-prof-a", Type: "omp", Version: "0.6.2", ProfileID: "prof-a"},
		},
	}
	h.declare(discovery)
	payload := json.RawMessage(`{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1"
	}`)
	if err := h.sched.OnClaim(t.Context(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	tok := taskToken(t, h, "task-1")

	// The profile is enabled/disabled at the server: prof-a leaves, prof-b
	// enters. The refresh must make the candidate S-3 source before the
	// discovery probe registers, commit only after C-1, and deregister the
	// dropped row on its own.
	candidate := profileListBody("prof-b")
	h.upstream.setProfiles(200, candidate)
	var probeSawCandidate bool
	// The callback plays the probe daemon: it reads S-3 and registers exactly
	// the runtimes the current candidate allows.
	wantCandidate := candidate
	probeRuntimes := `[{"name":"foreman-omp","type":"omp"},{"name":"foreman-prof-b","type":"omp","profile_id":"prof-b"}]`
	h.jobs.onCreateJob = func(job *batchv1Job) {
		if job.Labels[registry.LabelRole] != string(registry.RoleCatalogProbe) {
			return
		}
		probeTok := h.builder.probeToken(job.Name)
		code, body := h.call("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", probeTok, nil)
		if code != 200 || string(body) != wantCandidate {
			t.Errorf("the discovery probe read S-3 = %d %s, want the candidate list %s", code, body, wantCandidate)
		} else if wantCandidate == candidate {
			probeSawCandidate = true
		}
		code, body = h.call("POST", "/api/daemon/register", probeTok,
			[]byte(`{"daemon_id":"`+job.Name+`","runtimes":`+probeRuntimes+`}`))
		if code != 200 {
			t.Errorf("probe S-4 = %d: %s", code, body)
		}
	}

	refresher := proxy.NewProfileRefresher(h.client)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = refresher.Run(ctx)
	}()
	defer func() { cancel(); <-done }()
	refresher.Notify()
	waitFor(t, "the refresh to commit the new declaration", 10*time.Second, func() bool {
		refs := h.client.RuntimeRefs()
		return len(refs) == 2 && refs[1].ProfileID == "prof-b"
	})
	if !probeSawCandidate {
		t.Error("the probe never observed the candidate S-3 source")
	}

	// S-3 now replays the committed candidate.
	code, body := h.call("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", tok, nil)
	if code != 200 || string(body) != candidate {
		t.Errorf("S-3 after commit = %d %s, want the new list", code, body)
	}
	if got := h.client.DeclarationSource(); got != "discovery-probe" {
		t.Errorf("declaration source = %q, want discovery-probe", got)
	}
	// C-1 committed the probe's body entry by entry.
	registers := h.upstream.calls("POST", "/api/daemon/register")
	if len(registers) != 2 {
		t.Fatalf("C1 calls = %d, want the startup one plus the refresh commit", len(registers))
	}
	var last struct {
		Runtimes []struct {
			ProfileID string `json:"profile_id"`
		} `json:"runtimes"`
	}
	if err := json.Unmarshal(registers[1].Body, &last); err != nil {
		t.Fatalf("decode refresh C1: %v", err)
	}
	if len(last.Runtimes) != 2 || last.Runtimes[0].ProfileID != "" || last.Runtimes[1].ProfileID != "prof-b" {
		t.Errorf("refresh C1 runtimes = %+v, want builtin + prof-b", last.Runtimes)
	}
	// The dropped row is deregistered alone (C-14 逐条注销).
	deregisters := h.upstream.calls("POST", "/api/daemon/deregister")
	if len(deregisters) != 1 {
		t.Fatalf("C-14 calls = %d, want one per dropped entry", len(deregisters))
	}
	var dereg struct {
		RuntimeIDs []string `json:"runtime_ids"`
	}
	if err := json.Unmarshal(deregisters[0].Body, &dereg); err != nil {
		t.Fatalf("decode deregister: %v", err)
	}
	if len(dereg.RuntimeIDs) != 1 || dereg.RuntimeIDs[0] != "rid-prof-a" {
		t.Errorf("deregister body = %v, want exactly rid-prof-a", dereg.RuntimeIDs)
	}

	// --- failure rollback: probe failure keeps the committed source ---
	committed := profileListBody("prof-b")
	h.upstream.setProfiles(200, profileListBody("prof-c"))
	probeErr := h.builder.(*builder).probeErr
	h.builder.(*builder).probeErr = errors.New("probe create boom")
	if err := h.client.RefreshRuntimeProfiles(t.Context()); err == nil {
		t.Fatal("a failed discovery probe must surface")
	}
	h.builder.(*builder).probeErr = probeErr
	code, body = h.call("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", tok, nil)
	if code != 200 || string(body) != committed {
		t.Errorf("S-3 after probe failure = %d %s, want the last committed body", code, body)
	}
	if refs := h.client.RuntimeRefs(); len(refs) != 2 || refs[1].ProfileID != "prof-b" {
		t.Errorf("refs after probe failure = %+v, want the committed set", refs)
	}

	// --- failure rollback: C-1 failure keeps the committed source ---
	wantCandidate = profileListBody("prof-d")
	probeRuntimes = `[{"name":"foreman-omp","type":"omp"},{"name":"foreman-prof-d","type":"omp","profile_id":"prof-d"}]`
	h.upstream.setProfiles(200, wantCandidate)
	h.upstream.setRegisterStatus(503, `{"error":"boom"}`)
	if err := h.client.RefreshRuntimeProfiles(t.Context()); err == nil {
		t.Fatal("a failed C-1 commit must surface")
	}
	h.upstream.setRegisterStatus(200, "")
	code, body = h.call("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", tok, nil)
	if code != 200 || string(body) != committed {
		t.Errorf("S-3 after C-1 failure = %d %s, want the last committed body", code, body)
	}
	if refs := h.client.RuntimeRefs(); len(refs) != 2 || refs[1].ProfileID != "prof-b" {
		t.Errorf("refs after C-1 failure = %+v, want the committed set", refs)
	}

	// --- no self-driven polling: without the event nothing changes ---
	h.upstream.setProfiles(200, profileListBody("prof-e"))
	time.Sleep(100 * time.Millisecond)
	code, body = h.call("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", tok, nil)
	if code != 200 || string(body) != committed {
		t.Errorf("S-3 without an event = %d %s, want the committed body (WS-closed degradation)", code, body)
	}
}

// TestTechRuntimeProfile01ProfileWireShapeKeepsTheIdentityKey pins the pairing
// rule itself: the C-1 answer's null profile_id and the declared empty string
// are the same identity, while two profiles of one provider stay distinct.
func TestTechRuntimeProfile01ProfileWireShapeKeepsTheIdentityKey(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.setRuntimeID("omp", "", "rid-builtin")
	h.upstream.setRuntimeID("omp", "prof-a", "rid-a")
	h.upstream.setRuntimeID("omp", "prof-b", "rid-b")
	h.declare(registry.Discovery{Runtimes: []registry.DiscoveredRuntime{
		{Name: "builtin", Type: "omp", Version: "1"},
		{Name: "a", Type: "omp", Version: "1", ProfileID: "prof-a"},
		{Name: "b", Type: "omp", Version: "1", ProfileID: "prof-b"},
	}})

	refs := h.client.RuntimeRefs()
	if len(refs) != 3 {
		t.Fatalf("refs = %+v, want three distinct identities", refs)
	}
	seen := map[string]string{}
	for _, ref := range refs {
		seen[ref.Provider+"\x00"+ref.ProfileID] = ref.ID
	}
	for identity, want := range map[string]string{
		"omp\x00": "rid-builtin", "omp\x00prof-a": "rid-a", "omp\x00prof-b": "rid-b",
	} {
		if seen[identity] != want {
			t.Errorf("identity %q = %q, want %q", identity, seen[identity], want)
		}
	}
	if got := strings.Join(h.client.RuntimeIDs(), ","); got != "rid-builtin,rid-a,rid-b" {
		t.Errorf("runtime ids = %s", got)
	}
}

// TestTechRuntimeProfile01ThreeStateUnavailable covers the third S-3 state: a
// C-18 that never succeeded (5xx, or a transport failure) answers 503, and a
// later successful fetch replaces that state with the body it fetched.
func TestTechRuntimeProfile01ThreeStateUnavailable(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.setRuntimeID("omp", "", "rid-omp")
	h.declare(registry.Discovery{Runtimes: []registry.DiscoveredRuntime{{Name: "foreman-omp", Type: "omp", Version: "1"}}})
	// The Job daemon's token is only valid while its entry exists, so the
	// scenario claims the task (as the server would dispatch it).
	if err := h.sched.OnClaim(t.Context(), json.RawMessage(`{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1"
	}`)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	tok := taskToken(t, h, "task-1")

	// 5xx: the snapshot never succeeded, so S-3 is 503 — not 200 with an empty
	// list and not 404.
	h.upstream.setProfiles(http.StatusBadGateway, `{"error":"boom"}`)
	if err := h.client.RuntimeProfiles(t.Context()); err == nil {
		t.Fatal("a 5xx C-18 must surface as a failed snapshot")
	}
	if code, _ := h.call("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", tok, nil); code != 503 {
		t.Errorf("S-3 after a 5xx = %d, want 503", code)
	}

	// 404 of the face itself is a different state and stays 404.
	h.upstream.setProfiles(http.StatusNotFound, `{"error":"not found"}`)
	if err := h.client.RuntimeProfiles(t.Context()); err != nil {
		t.Fatalf("a 404 C-18 is not an error: %v", err)
	}
	if code, _ := h.call("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", tok, nil); code != 404 {
		t.Errorf("S-3 after a 404 = %d, want 404", code)
	}

	// A successful fetch clears both: S-3 replays the body byte for byte.
	body := profileListBody("prof-a")
	h.upstream.setProfiles(200, body)
	if err := h.client.RuntimeProfiles(t.Context()); err != nil {
		t.Fatalf("C18: %v", err)
	}
	code, got := h.call("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", tok, nil)
	if code != 200 || string(got) != body {
		t.Errorf("S-3 after a success = %d %s, want 200 %s", code, got, body)
	}
}

// TestTechRuntimeProfile01RefreshDeregistersEveryDroppedEntry pins the §1.6
// 增删语义: each entry that leaves the declaration is deregistered on its own
// request, so a batch carrying every dropped id at once is not this contract —
// with two entries leaving, one request per entry is required.
func TestTechRuntimeProfile01RefreshDeregistersEveryDroppedEntry(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.setRuntimeID("omp", "", "rid-omp")
	h.upstream.setRuntimeID("omp", "prof-a", "rid-prof-a")
	h.upstream.setRuntimeID("omp", "prof-b", "rid-prof-b")
	h.upstream.setRuntimeID("omp", "prof-c", "rid-prof-c")
	h.upstream.setProfiles(200, profileListBody("prof-a", "prof-b"))
	if err := h.client.RuntimeProfiles(t.Context()); err != nil {
		t.Fatalf("C18: %v", err)
	}
	h.declare(registry.Discovery{Runtimes: []registry.DiscoveredRuntime{
		{Name: "foreman-omp", Type: "omp", Version: "1"},
		{Name: "foreman-prof-a", Type: "omp", Version: "1", ProfileID: "prof-a"},
		{Name: "foreman-prof-b", Type: "omp", Version: "1", ProfileID: "prof-b"},
	}})

	// prof-a and prof-b are disabled, prof-c enabled: two entries leave.
	h.upstream.setProfiles(200, profileListBody("prof-c"))
	h.jobs.onCreateJob = func(job *batchv1Job) {
		if job.Labels[registry.LabelRole] != string(registry.RoleCatalogProbe) {
			return
		}
		probeTok := h.builder.probeToken(job.Name)
		// The discovery probe is registration-only: its heartbeat ack must
		// carry no pending_model_list (§1.6 只等注册，不做模型目录发现).
		session, ok := h.probes.ProbeByJob(job.Name)
		if !ok {
			t.Errorf("probe session %s missing at Job creation", job.Name)
			return
		}
		code, ack := h.call("POST", "/api/daemon/heartbeat", probeTok,
			[]byte(`{"runtime_id":"`+session.JobRuntime+`"}`))
		if code != 200 {
			t.Errorf("probe heartbeat = %d: %s", code, ack)
		} else if strings.Contains(string(ack), "pending_model_list") {
			t.Errorf("the discovery probe was handed a model catalog command: %s", ack)
		}
		code, body := h.call("POST", "/api/daemon/register", probeTok, []byte(`{
			"daemon_id": "`+job.Name+`",
			"runtimes": [{"name":"foreman-omp","type":"omp"},{"name":"foreman-prof-c","type":"omp","profile_id":"prof-c"}]
		}`))
		if code != 200 {
			t.Errorf("probe S-4 = %d: %s", code, body)
		}
	}
	if err := h.client.RefreshRuntimeProfiles(t.Context()); err != nil {
		t.Fatalf("RefreshRuntimeProfiles: %v", err)
	}
	deregisters := h.upstream.calls("POST", "/api/daemon/deregister")
	if len(deregisters) != 2 {
		t.Fatalf("C-14 calls = %d, want one per dropped entry (2)", len(deregisters))
	}
	dropped := map[string]int{}
	for _, call := range deregisters {
		var body struct {
			RuntimeIDs []string `json:"runtime_ids"`
		}
		if err := json.Unmarshal(call.Body, &body); err != nil {
			t.Fatalf("decode deregister: %v (%s)", err, call.Body)
		}
		if len(body.RuntimeIDs) != 1 {
			t.Fatalf("deregister body = %v, want exactly one id per request", body.RuntimeIDs)
		}
		dropped[body.RuntimeIDs[0]]++
	}
	if dropped["rid-prof-a"] != 1 || dropped["rid-prof-b"] != 1 || len(dropped) != 2 {
		t.Errorf("dropped rows = %v, want rid-prof-a and rid-prof-b once each", dropped)
	}
	if refs := h.client.RuntimeRefs(); len(refs) != 2 || refs[1].ProfileID != "prof-c" {
		t.Errorf("declaration after the refresh = %+v, want builtin + prof-c", refs)
	}
}

// TestTechRuntimeProfile01RefreshKeepsStateWhenC18Fails pins the first step of
// the §1.6 sequence: a C-18 that fails on the event path ends the sequence
// before the candidate exists, so neither the S-3 replay source nor the
// declaration moves.
func TestTechRuntimeProfile01RefreshKeepsStateWhenC18Fails(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.setRuntimeID("omp", "", "rid-omp")
	h.upstream.setRuntimeID("omp", "prof-a", "rid-prof-a")
	committed := profileListBody("prof-a")
	h.upstream.setProfiles(200, committed)
	if err := h.client.RuntimeProfiles(t.Context()); err != nil {
		t.Fatalf("C18: %v", err)
	}
	h.declare(registry.Discovery{Runtimes: []registry.DiscoveredRuntime{
		{Name: "foreman-omp", Type: "omp", Version: "1"},
		{Name: "foreman-prof-a", Type: "omp", Version: "1", ProfileID: "prof-a"},
	}})
	if err := h.sched.OnClaim(t.Context(), json.RawMessage(`{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1"
	}`)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	tok := taskToken(t, h, "task-1")

	h.upstream.setProfiles(http.StatusServiceUnavailable, `{"error":"boom"}`)
	if err := h.client.RefreshRuntimeProfiles(t.Context()); err == nil {
		t.Fatal("a failed C-18 re-fetch must surface")
	}
	code, body := h.call("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", tok, nil)
	if code != 200 || string(body) != committed {
		t.Errorf("S-3 after a failed re-fetch = %d %s, want the committed body", code, body)
	}
	if refs := h.client.RuntimeRefs(); len(refs) != 2 || refs[1].ID != "rid-prof-a" {
		t.Errorf("declaration after a failed re-fetch = %+v, want it untouched", refs)
	}
}

// TestTechRuntimeProfile01RefreshCoalescesEventsWhileInFlight pins the §1.6
// concurrency rule: events arriving while a refresh is in flight collapse into
// exactly one more run ("结束后再跑一次"), never into a queue of runs.
func TestTechRuntimeProfile01RefreshCoalescesEventsWhileInFlight(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.setRuntimeID("omp", "", "rid-omp")
	h.upstream.setRuntimeID("omp", "prof-a", "rid-prof-a")
	// The projection never changes, so each run ends at C-18 without probing.
	h.upstream.setProfiles(200, profileListBody("prof-a"))
	if err := h.client.RuntimeProfiles(t.Context()); err != nil {
		t.Fatalf("C18: %v", err)
	}
	h.declare(registry.Discovery{Runtimes: []registry.DiscoveredRuntime{
		{Name: "foreman-omp", Type: "omp", Version: "1"},
		{Name: "foreman-prof-a", Type: "omp", Version: "1", ProfileID: "prof-a"},
	}})

	release := h.upstream.gateProfiles()
	defer release()
	refresher := proxy.NewProfileRefresher(h.client)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); _ = refresher.Run(ctx) }()
	defer func() { cancel(); <-done }()

	refresher.Notify()
	// The first run is now blocked inside C-18 (call 2: the startup snapshot
	// was call 1).
	waitFor(t, "the first refresh to reach C-18", 10*time.Second, func() bool { return h.upstream.profileCalls() == 2 })
	refresher.Notify()
	refresher.Notify()
	refresher.Notify()
	release()
	// One more run: the three events merged into a single re-run.
	waitFor(t, "the merged re-run", 10*time.Second, func() bool { return h.upstream.profileCalls() == 3 })
	time.Sleep(200 * time.Millisecond)
	if got := h.upstream.profileCalls(); got != 3 {
		t.Errorf("C-18 calls = %d, want 3 (startup + in-flight + one merged re-run)", got)
	}
}
