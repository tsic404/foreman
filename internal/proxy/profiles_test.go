package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

// fakeDiscoveryRunner records the registration-discovery probes a refresh
// sequence asks for.
type fakeDiscoveryRunner struct {
	calls int
	disc  registry.Discovery
	err   error
}

func (f *fakeDiscoveryRunner) RunDiscoveryProbe(context.Context) (registry.Discovery, error) {
	f.calls++
	return f.disc, f.err
}

func TestSelectRuntimesPairsByIdentity(t *testing.T) {
	c := NewClient(testConfig("http://127.0.0.1:1"), "mdt_test")
	profile := func(id string) *string { return &id }
	declared := []runtimePayload{
		{Name: "builtin", Type: "omp", ProfileID: ""},
		{Name: "a", Type: "omp", ProfileID: "prof-a"},
		{Name: "b", Type: "omp", ProfileID: "prof-b"},
	}
	answers := []runtimeAnswer{
		{ID: "rid-builtin", Name: "builtin", Provider: "omp", ProfileID: nil},
		{ID: "rid-a", Name: "a", Provider: "omp", ProfileID: profile("prof-a")},
		{ID: "rid-b", Name: "b", Provider: "omp", ProfileID: profile("prof-b")},
	}
	refs, err := c.selectRuntimes(declared, answers)
	if err != nil {
		t.Fatalf("selectRuntimes: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("refs = %+v, want three identities", refs)
	}
	for i, want := range []struct{ provider, profile, id string }{
		{"omp", "", "rid-builtin"}, {"omp", "prof-a", "rid-a"}, {"omp", "prof-b", "rid-b"},
	} {
		got := refs[i]
		if got.Provider != want.provider || got.ProfileID != want.profile || got.ID != want.id {
			t.Errorf("refs[%d] = %+v, want (%s,%s)=%s", i, got, want.provider, want.profile, want.id)
		}
	}

	// A declared identity the answer does not carry fails the declaration (a
	// missing profile row must never silently disappear).
	if _, err := c.selectRuntimes(declared, answers[:2]); err == nil {
		t.Fatal("a missing answered identity must fail C-1 pairing")
	}
	// An answer for another identity does not satisfy it.
	foreign := []runtimeAnswer{{ID: "rid-x", Provider: "omp", ProfileID: profile("prof-x")}}
	if _, err := c.selectRuntimes(declared[:1], foreign); err == nil {
		t.Fatal("a foreign identity must not pair with the builtin entry")
	}
}

func TestProfileProjectionIgnoresOrderAndUnlistedFields(t *testing.T) {
	base := `{"workspace_id":"ws-1","runtime_profiles":[
		{"id":"a","display_name":"A","runtime_type":"omp","command_name":"a","enabled":true},
		{"id":"b","display_name":"B","runtime_type":"omp","command_name":"b","enabled":false}
	],"x_unlisted":1}`
	reordered := `{"runtime_profiles":[
		{"enabled":false,"command_name":"b","runtime_type":"omp","display_name":"B","id":"b","x_profile_key":"ignored"},
		{"id":"a","command_name":"a","display_name":"A","runtime_type":"omp","enabled":true}
	]}`
	if profileProjectionChanged([]byte(base), []byte(reordered)) {
		t.Error("key order, profile order and unlisted fields are not part of the projection")
	}
	changed := `{"runtime_profiles":[{"id":"a","display_name":"A2","runtime_type":"omp","command_name":"a","enabled":true}]}`
	if !profileProjectionChanged([]byte(base), []byte(changed)) {
		t.Error("a display_name change must count as changed")
	}
	if !profileProjectionChanged([]byte(base), nil) {
		t.Error("an empty candidate differs from a non-empty committed list")
	}
	if profileProjectionChanged(nil, []byte(`{"runtime_profiles":[]}`)) {
		t.Error("a nil committed body reads as the empty list")
	}
	// An unreadable candidate is "changed": the declaration must come from a
	// probe, not from a body Foreman could not read.
	if !profileProjectionChanged(nil, []byte(`[]`)) {
		t.Error("an unreadable candidate must count as changed")
	}
}

func TestWorkspaceStateResponsePresence(t *testing.T) {
	// Pre-C1 window: empty repos, no settings key.
	pre := workspaceStateResponse(WorkspaceState{})
	if string(pre["repos"].(json.RawMessage)) != "[]" {
		t.Errorf("pre-C1 repos = %v", pre["repos"])
	}
	if _, has := pre["settings"]; has {
		t.Error("pre-C1 must not carry settings")
	}

	// Seeded: a nil raw field is absent, `{}` and `null` are values.
	seeded := workspaceStateResponse(WorkspaceState{
		Repos: json.RawMessage(`[{"url":"u"}]`), ReposVersion: json.RawMessage(`"v"`), Seeded: true,
	})
	if _, has := seeded["settings"]; has {
		t.Error("an upstream omission must leave the settings key absent")
	}
	empty := workspaceStateResponse(WorkspaceState{Seeded: true, Settings: json.RawMessage(`{}`)})
	if string(empty["settings"].(json.RawMessage)) != "{}" {
		t.Errorf("settings {} = %v", empty["settings"])
	}
	null := workspaceStateResponse(WorkspaceState{Seeded: true, Settings: json.RawMessage(`null`)})
	if string(null["settings"].(json.RawMessage)) != "null" {
		t.Errorf("settings null = %v", null["settings"])
	}
}

func TestRefreshUnchangedKeepsTheReplaySourceWithoutAProbe(t *testing.T) {
	srv, stub := newStubUpstream(t)
	candidate := `{"workspace_id":"ws-1","runtime_profiles":[
		{"id":"a","display_name":"A","runtime_type":"omp","command_name":"a"}
	]}`
	stub.on("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, json.RawMessage(candidate))
	})
	runner := &fakeDiscoveryRunner{err: errors.New("a probe must not start for an unchanged projection")}
	metrics := &fakeMetrics{}
	c := NewClient(testConfig(srv.URL), "mdt_test", WithDiscoveryProbe(runner), WithMetrics(metrics))
	c.acceptProfiles([]byte(candidate))

	// A reordered/equivalent list with an unlisted field: unchanged.
	stub.on("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{
			"workspace_id": "ws-1",
			"runtime_profiles": []map[string]any{{
				"command_name": "a", "runtime_type": "omp", "display_name": "A", "id": "a",
				"x_extra": true,
			}},
		})
	})
	if err := c.RefreshRuntimeProfiles(t.Context()); err != nil {
		t.Fatalf("RefreshRuntimeProfiles: %v", err)
	}
	if runner.calls != 0 {
		t.Fatalf("discovery probes = %d, want 0 for an unchanged projection", runner.calls)
	}
	code, body := c.S3Response()
	if code != 200 {
		t.Fatalf("S-3 = %d", code)
	}
	var got struct {
		RuntimeProfiles []map[string]any `json:"runtime_profiles"`
	}
	if err := json.Unmarshal(body, &got); err != nil || len(got.RuntimeProfiles) != 1 {
		t.Errorf("S-3 body = %s (%v)", body, err)
	}
	metrics.mu.Lock()
	results := append([]string(nil), metrics.profileRefreshes...)
	metrics.mu.Unlock()
	found := false
	for _, result := range results {
		if result == ProfileRefreshUnchanged {
			found = true
		}
	}
	if !found {
		t.Errorf("profile refresh metrics = %v, want an unchanged outcome", results)
	}
}

func TestRefreshRollsBackWhenTheProbeFails(t *testing.T) {
	srv, stub := newStubUpstream(t)
	committed := `{"workspace_id":"ws-1","runtime_profiles":[{"id":"a","display_name":"A"}]}`
	candidate := `{"workspace_id":"ws-1","runtime_profiles":[{"id":"b","display_name":"B"}]}`
	stub.on("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, json.RawMessage(candidate))
	})
	runner := &fakeDiscoveryRunner{err: errors.New("probe boom")}
	metrics := &fakeMetrics{}
	c := NewClient(testConfig(srv.URL), "mdt_test", WithDiscoveryProbe(runner), WithMetrics(metrics))
	c.acceptProfiles([]byte(committed))

	if err := c.RefreshRuntimeProfiles(t.Context()); err == nil {
		t.Fatal("a failed probe must surface")
	}
	code, body := c.S3Response()
	if code != 200 || string(body) != committed {
		t.Fatalf("S-3 = %d %s, want the last committed body", code, body)
	}
	metrics.mu.Lock()
	results := append([]string(nil), metrics.profileRefreshes...)
	metrics.mu.Unlock()
	if len(results) != 1 || results[0] != ProfileRefreshFailed {
		t.Errorf("profile refresh metrics = %v, want one failed outcome", results)
	}
}

// TestRefreshRollsBackTheDeclarationOnCommitFailure pins the commit point:
// a refresh that registered nothing keeps the old runtime set (ids included),
// not the empty-id declaration the probe produced.
func TestRefreshRollsBackTheDeclarationOnCommitFailure(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{
			"runtimes": []map[string]any{{"id": "rid-old", "name": "builtin", "provider": "omp", "status": "online"}},
			"repos":    []any{}, "repos_version": "v1", "settings": nil,
		})
	})
	committed := `{"workspace_id":"ws-1","runtime_profiles":[{"id":"a","display_name":"A"}]}`
	candidate := `{"workspace_id":"ws-1","runtime_profiles":[{"id":"b","display_name":"B"}]}`
	stub.on("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, json.RawMessage(candidate))
	})
	runner := &fakeDiscoveryRunner{disc: registry.Discovery{
		Runtimes: []registry.DiscoveredRuntime{{Name: "builtin", Type: "omp", Version: "2"}},
	}}
	c := NewClient(testConfig(srv.URL), "mdt_test", WithDiscoveryProbe(runner))
	if err := c.RegisterWithDiscovery(t.Context(), registry.Discovery{
		Runtimes: []registry.DiscoveredRuntime{{Name: "builtin", Type: "omp", Version: "1"}},
	}); err != nil {
		t.Fatalf("startup register: %v", err)
	}
	c.acceptProfiles([]byte(committed))
	// The commit fails: the stub answers 503 from now on.
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 503, map[string]string{"error": "boom"})
	})
	if err := c.RefreshRuntimeProfiles(t.Context()); err == nil {
		t.Fatal("a failed C-1 must surface")
	}
	refs := c.RuntimeRefs()
	if len(refs) != 1 || refs[0].ID != "rid-old" || refs[0].Name != "builtin" {
		t.Errorf("refs after a failed commit = %+v, want the committed set", refs)
	}
	if got := c.Declaration().Runtimes[0].Version; got != "1" {
		t.Errorf("declaration version = %q, want the committed one", got)
	}
	if code, body := c.S3Response(); code != 200 || string(body) != committed {
		t.Errorf("S-3 after a failed commit = %d %s", code, body)
	}
}

// TestSelectRuntimesTieBreakPrefersOnlineNamedCandidate pins the §2.3 order for
// one identity answered by more than one row (the restart leaves old rows
// behind): online rows whose name matches this declaration come first, then the
// lexicographically smallest id — never the response order.
func TestSelectRuntimesTieBreakPrefersOnlineNamedCandidate(t *testing.T) {
	c := NewClient(testConfig("http://127.0.0.1:1"), "mdt_test")
	profile := func(id string) *string { return &id }
	declared := []runtimePayload{{Name: "X", Type: "omp", ProfileID: "prof-a"}}

	answers := []runtimeAnswer{
		{ID: "c", Name: "X", Provider: "omp", Status: "online", ProfileID: profile("prof-a")},
		{ID: "b", Name: "X", Provider: "omp", Status: "online", ProfileID: profile("prof-a")},
		{ID: "a", Name: "X", Provider: "omp", Status: "offline", ProfileID: profile("prof-a")},
	}
	refs, err := c.selectRuntimes(declared, answers)
	if err != nil {
		t.Fatalf("selectRuntimes: %v", err)
	}
	if len(refs) != 1 || refs[0].ID != "b" {
		t.Errorf("refs = %+v, want the smallest id among the online name matches (b)", refs)
	}

	// No online name match decides among the whole candidate set by id.
	answers = []runtimeAnswer{
		{ID: "z", Name: "other", Provider: "omp", Status: "online", ProfileID: profile("prof-a")},
		{ID: "m", Name: "other", Provider: "omp", Status: "offline", ProfileID: profile("prof-a")},
	}
	refs, err = c.selectRuntimes(declared, answers)
	if err != nil {
		t.Fatalf("selectRuntimes: %v", err)
	}
	if len(refs) != 1 || refs[0].ID != "m" {
		t.Errorf("refs = %+v, want the smallest id of the whole candidate set (m)", refs)
	}
}

// TestRefreshKeepsTheDeclarationUntilC1Succeeds pins the §1.6 commit point: the
// candidate becomes the S-3 replay source before the probe, but the declaration
// itself moves only once the server accepted the new C-1 — while that request is
// in flight the previous set is still the live one (heartbeats, subscriptions
// and the claim list read it).
func TestRefreshKeepsTheDeclarationUntilC1Succeeds(t *testing.T) {
	srv, stub := newStubUpstream(t)
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{
			"runtimes": []map[string]any{{"id": "rid-old", "name": "builtin", "provider": "omp", "status": "online"}},
			"repos":    []any{}, "repos_version": "v1", "settings": nil,
		})
	})
	committed := `{"workspace_id":"ws-1","runtime_profiles":[{"id":"a","display_name":"A"}]}`
	candidate := `{"workspace_id":"ws-1","runtime_profiles":[{"id":"b","display_name":"B"}]}`
	runner := &fakeDiscoveryRunner{disc: registry.Discovery{
		Runtimes: []registry.DiscoveredRuntime{{Name: "builtin", Type: "omp", Version: "2"}},
	}}
	c := NewClient(testConfig(srv.URL), "mdt_test", WithDiscoveryProbe(runner))
	if err := c.RegisterWithDiscovery(t.Context(), registry.Discovery{
		Runtimes: []registry.DiscoveredRuntime{{Name: "builtin", Type: "omp", Version: "1"}},
	}); err != nil {
		t.Fatalf("startup register: %v", err)
	}
	c.acceptProfiles([]byte(committed))

	entered, release := make(chan struct{}), make(chan struct{})
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		close(entered)
		<-release
		writeJSONStub(w, 200, map[string]any{
			"runtimes": []map[string]any{{"id": "rid-new", "name": "builtin", "provider": "omp", "status": "online"}},
			"repos":    []any{}, "repos_version": "v2", "settings": nil,
		})
	})
	stub.on("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, json.RawMessage(candidate))
	})

	done := make(chan error, 1)
	go func() { done <- c.RefreshRuntimeProfiles(context.Background()) }()
	<-entered
	if code, body := c.S3Response(); code != 200 || string(body) != candidate {
		t.Errorf("S-3 during the refresh = %d %s, want the candidate (replay source first)", code, body)
	}
	refs := c.RuntimeRefs()
	if len(refs) != 1 || refs[0].ID != "rid-old" {
		t.Errorf("declaration during C-1 = %+v, want the previous set (commit point = C-1 success)", refs)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("RefreshRuntimeProfiles: %v", err)
	}
	refs = c.RuntimeRefs()
	if len(refs) != 1 || refs[0].ID != "rid-new" {
		t.Errorf("declaration after the commit = %+v, want the registered set", refs)
	}
}

// TestRebuildAfterRuntimeLossRecordsEveryBranch drives the §1.6/"runtime not
// found" sequence down each C-18 outcome and pins the §9.2 outcome of every one:
// an unchanged list, a 404 face and an unavailable face must all be observable,
// not only the branch that goes through the refresh sequence.
func TestRebuildAfterRuntimeLossRecordsEveryBranch(t *testing.T) {
	newClient := func(t *testing.T, profiles func(w http.ResponseWriter)) (*Client, *fakeMetrics) {
		t.Helper()
		srv, stub := newStubUpstream(t)
		stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
			writeJSONStub(w, 200, map[string]any{
				"runtimes": []map[string]any{{"id": "rid-new", "name": "builtin", "provider": "omp", "status": "online"}},
				"repos":    []any{}, "repos_version": "v1", "settings": nil,
			})
		})
		if profiles != nil {
			stub.on("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
				profiles(w)
			})
		}
		metrics := &fakeMetrics{}
		c := NewClient(testConfig(srv.URL), "mdt_test", WithMetrics(metrics))
		disc := registry.Discovery{Runtimes: []registry.DiscoveredRuntime{{Name: "builtin", Type: "omp", Version: "1"}}}
		if err := c.RegisterWithDiscovery(t.Context(), disc); err != nil {
			t.Fatalf("startup register: %v", err)
		}
		c.mu.Lock()
		c.refs = []RuntimeRef{{ID: "rid-old", Provider: "omp", Name: "builtin"}}
		c.mu.Unlock()
		return c, metrics
	}
	results := func(m *fakeMetrics) []string {
		m.mu.Lock()
		defer m.mu.Unlock()
		return append([]string(nil), m.profileRefreshes...)
	}
	profiles := `{"workspace_id":"ws-1","runtime_profiles":[{"id":"a","display_name":"A"}]}`

	t.Run("unchanged list", func(t *testing.T) {
		c, metrics := newClient(t, func(w http.ResponseWriter) { writeJSONStub(w, 200, json.RawMessage(profiles)) })
		c.acceptProfiles([]byte(profiles))
		fresh, err := c.rebuildRuntime(t.Context(), "rid-old")
		if err != nil {
			t.Fatalf("rebuildRuntime: %v", err)
		}
		if fresh != "rid-new" {
			t.Errorf("fresh id = %q, want the re-registered row", fresh)
		}
		if got := results(metrics); len(got) != 1 || got[0] != ProfileRefreshUnchanged {
			t.Errorf("metrics = %v, want one unchanged outcome", got)
		}
	})

	t.Run("profile face gone", func(t *testing.T) {
		c, metrics := newClient(t, func(w http.ResponseWriter) { writeJSONStub(w, 404, map[string]string{"error": "not found"}) })
		if _, err := c.rebuildRuntime(t.Context(), "rid-old"); err != nil {
			t.Fatalf("rebuildRuntime: %v", err)
		}
		if code, _ := c.S3Response(); code != 404 {
			t.Errorf("S-3 = %d, want 404 from the gone face", code)
		}
		if got := results(metrics); len(got) != 1 || got[0] != ProfileRefreshUnchanged {
			t.Errorf("metrics = %v, want one unchanged outcome", got)
		}
	})

	t.Run("profile list unavailable", func(t *testing.T) {
		c, metrics := newClient(t, func(w http.ResponseWriter) { writeJSONStub(w, 503, map[string]string{"error": "boom"}) })
		c.acceptProfiles([]byte(profiles))
		if _, err := c.rebuildRuntime(t.Context(), "rid-old"); err != nil {
			t.Fatalf("rebuildRuntime: %v", err)
		}
		if code, body := c.S3Response(); code != 200 || string(body) != profiles {
			t.Errorf("S-3 = %d %s, want the committed replay source (availability first)", code, body)
		}
		if got := results(metrics); len(got) != 1 || got[0] != ProfileRefreshFailed {
			t.Errorf("metrics = %v, want one failed outcome", got)
		}
	})
}

// TestClaimRuntimeLossRefetchesProfiles: C-3 carries the whole declared set, so
// its 404 names no row — the re-register still has to re-fetch C-18 first
// (ADR-043 §9) and move every stale id in one C-1.
func TestClaimRuntimeLossRefetchesProfiles(t *testing.T) {
	srv, stub := newStubUpstream(t)
	var registers atomic.Int32
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		id := "rid-old"
		if registers.Add(1) > 1 {
			id = "rid-new"
		}
		writeJSONStub(w, 200, map[string]any{
			"runtimes": []map[string]any{{"id": id, "name": "builtin", "provider": "omp", "status": "online"}},
			"repos":    []any{}, "repos_version": "v1", "settings": nil,
		})
	})
	stub.on("POST", "/api/daemon/tasks/claim", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 404, map[string]string{"error": "runtime not found"})
	})
	stub.on("GET", "/api/daemon/workspaces/ws-1/runtime-profiles", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{"workspace_id": "ws-1", "runtime_profiles": []any{}})
	})
	c := NewClient(testConfig(srv.URL), "mdt_test")
	if err := c.RegisterWithDiscovery(t.Context(), testDiscovery()); err != nil {
		t.Fatalf("startup register: %v", err)
	}

	_, _ = c.ClaimTasks(t.Context(), 1)

	profilesAt, registerAt := -1, -1
	for i, req := range stub.all() {
		switch {
		case req.Method == "GET" && req.Path == "/api/daemon/workspaces/ws-1/runtime-profiles" && profilesAt < 0:
			profilesAt = i
		case req.Method == "POST" && req.Path == "/api/daemon/register" && i > 0 && registerAt < 0 && registers.Load() > 1:
			registerAt = i
		}
	}
	if profilesAt < 0 || registerAt < 0 || profilesAt > registerAt {
		t.Fatalf("request order = profiles at %d, re-register at %d; want C-18 before the C-1", profilesAt, registerAt)
	}
	if got := firstRuntimeID(c); got != "rid-new" {
		t.Errorf("runtime id = %q, want the re-registered row", got)
	}
}

// TestWorkspaceStateKeepsReposVersionPresence pins §1.7's presence rule for the
// third carrier field: an explicit `null` is a value and replays as `null`, an
// absent key stays absent, and a shape this build does not interpret replays
// verbatim instead of failing the whole refresh.
func TestWorkspaceStateKeepsReposVersionPresence(t *testing.T) {
	newClient := func(t *testing.T, answer string) *Client {
		t.Helper()
		srv, stub := newStubUpstream(t)
		stub.on("GET", "/api/daemon/workspaces/ws-1/repos", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
			w.WriteHeader(200)
			_, _ = w.Write([]byte(answer))
		})
		c := NewClient(testConfig(srv.URL), "mdt_test")
		c.mu.Lock()
		c.state = WorkspaceState{Seeded: true, Repos: json.RawMessage(`[]`)}
		c.mu.Unlock()
		return c
	}
	answerOf := func(state WorkspaceState) string {
		raw, err := json.Marshal(workspaceStateResponse(state))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var got map[string]json.RawMessage
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return string(got["repos_version"])
	}

	c := newClient(t, `{"repos":[],"repos_version":null,"settings":{}}`)
	if err := c.RefreshWorkspaceState(t.Context()); err != nil {
		t.Fatalf("C-22 with an explicit null version: %v", err)
	}
	if got := answerOf(c.WorkspaceState()); got != "null" {
		t.Errorf("explicit null repos_version = %q, want the key present as null", got)
	}

	c = newClient(t, `{"repos":[],"settings":{}}`)
	if err := c.RefreshWorkspaceState(t.Context()); err != nil {
		t.Fatalf("C-22 without a version: %v", err)
	}
	if got := answerOf(c.WorkspaceState()); got != "" {
		t.Errorf("absent repos_version = %q, want the key absent", got)
	}

	c = newClient(t, `{"repos":[],"repos_version":{"generation":7},"settings":{}}`)
	if err := c.RefreshWorkspaceState(t.Context()); err != nil {
		t.Fatalf("C-22 with a non-string version must not fail the refresh: %v", err)
	}
	if got := answerOf(c.WorkspaceState()); got != `{"generation":7}` {
		t.Errorf("non-string repos_version = %q, want it replayed verbatim", got)
	}
}

// TestWorkspaceStateRefreshBeforeSeedRecordsFailed pins §9.2's third outcome:
// in §1.7's register-before window there is no snapshot to answer from, so the
// refresh records `failed` and no C-22 leaves the process.
func TestWorkspaceStateRefreshBeforeSeedRecordsFailed(t *testing.T) {
	srv, stub := newStubUpstream(t)
	var metrics fakeMetrics
	c := NewClient(testConfig(srv.URL), "mdt_test", WithMetrics(&metrics))
	if err := c.RefreshWorkspaceState(t.Context()); err != nil {
		t.Fatalf("register-before refresh = %v, want nil", err)
	}
	metrics.mu.Lock()
	got := append([]string(nil), metrics.workspaceRefreshes...)
	metrics.mu.Unlock()
	if len(got) != 1 || got[0] != WorkspaceStateFailure {
		t.Fatalf("workspace refreshes = %v, want [%s]", got, WorkspaceStateFailure)
	}
	if calls := stub.calls(http.MethodGet, "/api/daemon/workspaces/ws-1/repos"); len(calls) != 0 {
		t.Errorf("register-before refresh made %d C-22 calls, want 0", len(calls))
	}
}
