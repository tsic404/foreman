package proxy_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/daemonauth"
	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// claimDispatcher answers Deliver with whatever the test sets, and records the
// arguments the daemon face passed.
type claimDispatcher struct {
	runtimes  []string
	maxTasks  int
	workspace string
	daemonID  string

	delivery dispatch.Delivery
	err      error
	calls    int
}

func (c *claimDispatcher) OnClaim(context.Context, string, json.RawMessage) error { return nil }
func (c *claimDispatcher) Deliver(_ context.Context, workspaceID, daemonID string, runtimeIDs []string, maxTasks int) (dispatch.Delivery, error) {
	c.calls++
	c.workspace, c.daemonID, c.runtimes, c.maxTasks = workspaceID, daemonID, runtimeIDs, maxTasks
	return c.delivery, c.err
}
func (c *claimDispatcher) OnReport(context.Context, dispatch.Endpoint, registry.TaskEntry, []byte, string) (int, []byte, error) {
	return http.StatusOK, nil, nil
}
func (c *claimDispatcher) OnTaskVanished(context.Context, string) error { return nil }
func (c *claimDispatcher) OnBootTimeout(context.Context, string) error  { return nil }

func claimRig(t *testing.T, disp *claimDispatcher, opts ...proxy.ServerOption) (*registry.Registry, string) {
	t.Helper()
	store, err := daemonauth.Load(func(k string) string {
		switch k {
		case daemonauth.EnvDaemonTokens:
			return "worker-1=" + testToken
		case daemonauth.EnvDaemonTokenWorkspaces:
			return "worker-1=ws-1"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("creds: %v", err)
	}
	reg := registry.New()
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "worker-1", DaemonID: "worker-1", WorkspaceID: "ws-1",
		Provider: "omp", Name: "worker-1", Online: true, RegisteredAt: reg.Now(),
	})
	httpSrv := httptest.NewServer(proxy.NewFakeServer(store, reg, disp, opts...))
	t.Cleanup(httpSrv.Close)
	return reg, httpSrv.URL
}

// TestClaimPassesDaemonAndCapacityThrough: the daemon's own identity and its
// requested batch size must reach the dispatcher unchanged — a claim that
// silently capped the batch would starve a multi-runtime daemon.
func TestClaimPassesDaemonAndCapacityThrough(t *testing.T) {
	disp := &claimDispatcher{delivery: dispatch.Delivery{Delivered: true,
		Tasks: []json.RawMessage{json.RawMessage(`{"id":"t1"}`)}}}
	_, url := claimRig(t, disp)

	body := `{"daemon_id":"worker-1","runtime_ids":["worker-1"],"max_tasks":3}`
	if code := postWithToken(t, url, "/api/daemon/tasks/claim", testToken, body); code != http.StatusOK {
		t.Fatalf("claim = %d, want 200", code)
	}
	if disp.calls != 1 {
		t.Fatalf("Deliver called %d times, want 1", disp.calls)
	}
	if disp.workspace != "ws-1" || disp.daemonID != "worker-1" {
		t.Fatalf("workspace=%q daemon=%q", disp.workspace, disp.daemonID)
	}
	if disp.maxTasks != 3 {
		t.Fatalf("maxTasks = %d, want 3", disp.maxTasks)
	}
	if len(disp.runtimes) != 1 || disp.runtimes[0] != "worker-1" {
		t.Fatalf("runtime ids = %v", disp.runtimes)
	}
}

// TestClaimReturnsDeliveredTasks: the payload must be handed back verbatim,
// because the daemon parses it without Foreman's help.
func TestClaimReturnsDeliveredTasks(t *testing.T) {
	disp := &claimDispatcher{delivery: dispatch.Delivery{Delivered: true,
		Tasks: []json.RawMessage{json.RawMessage(`{"id":"t1","x":2}`)}}}
	_, url := claimRig(t, disp)

	body := `{"daemon_id":"worker-1","runtime_ids":["worker-1"],"max_tasks":1}`
	req, _ := http.NewRequest(http.MethodPost, url+"/api/daemon/tasks/claim", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Tasks []json.RawMessage `json:"tasks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Tasks) != 1 || !strings.Contains(string(out.Tasks[0]), `"t1"`) {
		t.Fatalf("tasks = %v", out.Tasks)
	}
}

// TestClaimEmptyIsAlwaysAnArray: the daemon decodes tasks as a list. A null
// body would make it treat the claim as malformed instead of "nothing ready".
func TestClaimEmptyIsAlwaysAnArray(t *testing.T) {
	// The ordinary empty case, and the "no bound task" refusal.
	for name, disp := range map[string]*claimDispatcher{
		"empty delivery":   {delivery: dispatch.Delivery{}},
		"runtime mismatch": {err: dispatch.ErrRuntimeMismatch},
		"unknown daemon":   {err: dispatch.ErrUnknownDaemon},
	} {
		t.Run(name, func(t *testing.T) {
			_, url := claimRig(t, disp)
			body := `{"daemon_id":"worker-1","runtime_ids":["worker-1"],"max_tasks":1}`
			req, _ := http.NewRequest(http.MethodPost, url+"/api/daemon/tasks/claim", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+testToken)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200 (an idle daemon is not an error)", resp.StatusCode)
			}
			var raw map[string]json.RawMessage
			if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if string(raw["tasks"]) != "[]" {
				t.Fatalf("tasks = %s, want []", raw["tasks"])
			}
		})
	}
}

// TestClaimGenuineFailureIs500: a real dispatch failure must be an error the
// daemon retries, not a silent empty batch that looks like "no work".
func TestClaimGenuineFailureIs500(t *testing.T) {
	disp := &claimDispatcher{err: errors.New("registry exploded")}
	_, url := claimRig(t, disp)

	body := `{"daemon_id":"worker-1","runtime_ids":["worker-1"],"max_tasks":1}`
	if code := postWithToken(t, url, "/api/daemon/tasks/claim", testToken, body); code != http.StatusInternalServerError {
		t.Fatalf("claim = %d, want 500", code)
	}
}

// TestClaimRejectsForeignDaemonID: a credential may only ask for its own
// identity's work.
func TestClaimRejectsForeignDaemonID(t *testing.T) {
	disp := &claimDispatcher{}
	_, url := claimRig(t, disp)

	body := `{"daemon_id":"someone-else","runtime_ids":["someone-else"],"max_tasks":1}`
	if code := postWithToken(t, url, "/api/daemon/tasks/claim", testToken, body); code != http.StatusForbidden {
		t.Fatalf("foreign claim = %d, want 403", code)
	}
	if disp.calls != 0 {
		t.Fatal("a foreign claim reached the dispatcher")
	}
}

// TestPrepareLeaseRequiresOwnedRuntime: S-7 is runtime-scoped, so a daemon may
// only renew a lease on a runtime it registered.
func TestPrepareLeaseRequiresOwnedRuntime(t *testing.T) {
	disp := &claimDispatcher{}
	reg, url := claimRig(t, disp)
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "other-rt", DaemonID: "worker-2", WorkspaceID: "ws-1",
		Provider: "omp", Name: "worker-2", Online: true,
	})

	if code := postWithToken(t, url, "/api/daemon/runtimes/other-rt/tasks/t-1/prepare-lease", testToken, `{}`); code != http.StatusNotFound {
		t.Fatalf("foreign runtime = %d, want 404", code)
	}
	if code := postWithToken(t, url, "/api/daemon/runtimes/nope/tasks/t-1/prepare-lease", testToken, `{}`); code != http.StatusNotFound {
		t.Fatalf("unknown runtime = %d, want 404", code)
	}
}

// TestPrepareLeaseAnswersLocallyWithoutForwarder: with no C-4 seam wired the
// route still answers ok, which is the documented test fallback.
func TestPrepareLeaseAnswersLocallyWithoutForwarder(t *testing.T) {
	disp := &claimDispatcher{}
	_, url := claimRig(t, disp)

	if code := postWithToken(t, url, "/api/daemon/runtimes/worker-1/tasks/t-1/prepare-lease", testToken, `{}`); code != http.StatusOK {
		t.Fatalf("lease = %d, want 200", code)
	}
}

// TestPrepareLeaseForwardOutcomes: the forwarder's three answers map to three
// distinct HTTP results the daemon acts on differently.
func TestPrepareLeaseForwardOutcomes(t *testing.T) {
	cases := []struct {
		name  string
		ready bool
		err   error
		want  int
	}{
		{"renewable", true, nil, http.StatusOK},
		{"not renewable", false, nil, http.StatusBadRequest},
		{"forward failed", false, errors.New("boom"), http.StatusBadGateway},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			disp := &claimDispatcher{}
			_, url := claimRig(t, disp, proxy.WithLeaseForwarder(
				func(context.Context, string, string) (bool, error) { return tc.ready, tc.err }))
			if code := postWithToken(t, url, "/api/daemon/runtimes/worker-1/tasks/t-1/prepare-lease", testToken, `{}`); code != tc.want {
				t.Fatalf("lease = %d, want %d", code, tc.want)
			}
		})
	}
}
