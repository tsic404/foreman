// Daemon-face protocol integrity: TC-tech-protocol-02 (AC-59).
//
// Foreman is a middle layer, so the Job daemon's daemon-API traffic never
// leaves the Job (§1.2 rule 1). Every route the real server serves on that
// face is therefore either answered locally through a declared rewrite or
// refusal, or forwarded verbatim. This file pins that surface, so the class of
// gap a manual audit finds ("the upstream sends X, Foreman drops X") fails a
// test instead of shipping:
//
//  1. upstreamDaemonFace is a snapshot of the real server's daemon-facing API.
//     Every entry declares Foreman's handling; the registered pattern set must
//     equal the declared one both ways, so a route cannot be added to or
//     dropped from fakeserver.go's table without the census moving.
//  2. Routes Foreman refuses or never modelled are probed: the daemon must see
//     the declared answer and the upstream must not be called. A probe lives on
//     the entry it drives, and TestDaemonFaceProbeCoverage fails when an entry
//     that owes one loses it.
//  3. Routes Foreman forwards are probed: the upstream must see the declared
//     path, and its status, header set and body must reach the daemon verbatim.
//  4. The carriers that reproduce upstream fields are field-checked, and every
//     upstream-sourced field the daemon reads but Foreman does not carry is
//     registered with the ADR that rules on it (settings / profile_id /
//     runtime-profiles; ADR-043 / ADR-044 proposed).
//
// Snapshot provenance — multica server 18fa9f7: cmd/server/router.go:1559-1613
// (the /api/daemon group), :1919 (/api/tokens/current/renew), :1694
// (/api/workspaces), :2341 (/api/runtimes); daemon call sites in
// server/internal/daemon/{client,daemon,wakeup,gc}.go. When the upstream adds,
// moves or removes a route, this table is the diff surface.
//
// Out of scope here: the ops endpoints (S25 /metrics, S31 /foreman/*) live on
// a separate mux mounted by cmd/foreman, and S29 itself is dispatched in
// ServeHTTP rather than registered — its fidelity is TC-tech-forward-03/04
// (fakeserver_test.go's TestPassthrough* family).
package proxy

import (
	"bytes"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

// daemonFaceHandling is how the local daemon face treats an upstream route.
type daemonFaceHandling string

const (
	// faceServed: a local handler owns the route (§1.2 S-table). The upstream
	// is still involved for some of them, through a declared rewrite: S2/S4
	// replay the C1 cache and S7/S26 forward with the real runtime id.
	faceServed daemonFaceHandling = "served"
	// faceForwarded: the caller's request is forwarded verbatim, credential
	// included (§1.2 S23 rides the S29 passthrough).
	faceForwarded daemonFaceHandling = "forwarded"
	// faceRefused: the route is registered and answers the declared 404 without
	// touching the upstream, which is what switches the daemon off the path
	// (§1.2 S3/S22/S27/S32). The 404 is what a task Job gets; S27's models path
	// is the one member with a probe-session branch behind it (see its entry).
	faceRefused daemonFaceHandling = "refused"
	// faceUnmodelled: the upstream serves the route, Foreman has no local
	// pattern for it, and an fmj_ call lands on the catch-all 404.
	faceUnmodelled daemonFaceHandling = "unmodelled"
)

// daemonFaceEntry is one route of the real server's daemon-facing API plus the
// local face's declared handling of it.
type daemonFaceEntry struct {
	// name keys the probes and names the failure.
	name string
	// upstream is the route as the real server serves it ("METHOD /path");
	// empty when Foreman registers a pattern the upstream has no counterpart
	// for (defensive registration).
	upstream string
	// caller is the Job daemon call site that uses the route (server repo
	// relative), empty when no daemon code calls it.
	caller string
	// local is Foreman's registered pattern; empty for faceUnmodelled.
	local string
	// design is the §1.2 endpoint id.
	design string
	// handling is the declared treatment.
	handling daemonFaceHandling
	// probe drives the entry through the real FakeServer. Every entry whose
	// daemon-visible answer is not the local S-table handler's own work must
	// carry one, and a served entry must not; TestDaemonFaceProbeCoverage is
	// what keeps that true. The probe carries no name of its own — the entry it
	// sits on is the single source of both the route and its subtest name.
	probe *daemonFaceProbe
	// note records the consequence or deviation a reader of the census needs.
	note string
}

// upstreamDaemonFace is the census. Keep it ordered like the upstream router
// (registration order is irrelevant to the assertions, the reader is not).
var upstreamDaemonFace = []daemonFaceEntry{
	// ---- served (the §1.2 S-table owns the route) ----
	{
		name: "S-4 register", upstream: "POST /api/daemon/register",
		caller: "internal/daemon/client.go:1083",
		local:  "POST /api/daemon/register", design: "S-4", handling: faceServed,
		note: "answers exactly one job runtime (provider selection); repos/repos_version replay the C1 cache",
	},
	{
		name: "S-19 deregister", upstream: "POST /api/daemon/deregister",
		caller: "internal/daemon/client.go:1067",
		local:  "POST /api/daemon/deregister", design: "S-19", handling: faceServed,
	},
	{
		name: "S-5 heartbeat", upstream: "POST /api/daemon/heartbeat",
		caller: "internal/daemon/client.go:737",
		local:  "POST /api/daemon/heartbeat", design: "S-5", handling: faceServed,
		note: "任务 Job 恒无待办字段 (F9); an unknown runtime gets 404 runtime not found so the daemon re-registers",
	},
	{
		name: "S-30 ws", upstream: "GET /api/daemon/ws",
		caller: "internal/daemon/wakeup.go:521",
		local:  "GET /api/daemon/ws", design: "S-30", handling: faceServed,
		note: "registered only when the WS handler is wired; otherwise the dial gets the catch-all 404 (FOREMAN_WS_ENABLED=false, §5.1)",
	},
	{
		name: "S-1 workspaces", upstream: "GET /api/daemon/workspaces",
		caller: "internal/daemon/client.go:799",
		local:  "GET /api/daemon/workspaces", design: "S-1", handling: faceServed,
		note: "synthesized from the C17 cache; the daemon's ETag/304 and legacy-404 paths never apply here",
	},
	{
		name: "S-2 repos", upstream: "GET /api/daemon/workspaces/{workspaceId}/repos",
		caller: "internal/daemon/client.go:1098",
		local:  "GET /api/daemon/workspaces/{ws}/repos", design: "S-2", handling: faceServed,
		note: "repos/repos_version replay the C1 cache verbatim; settings is a declared deviation (see TestDaemonFaceDeclaredDeviations)",
	},
	{
		name: "S-6 claim", upstream: "POST /api/daemon/tasks/claim",
		caller: "internal/daemon/client.go:306",
		local:  "POST /api/daemon/tasks/claim", design: "S-6", handling: faceServed,
		note: "delivers the stored claim payload with only runtime_id rewritten (§2.5)",
	},
	{
		name: "S-7 prepare-lease", upstream: "POST /api/daemon/runtimes/{runtimeId}/tasks/{taskId}/prepare-lease",
		caller: "internal/daemon/client.go:436",
		local:  "POST /api/daemon/runtimes/{rid}/tasks/{tid}/prepare-lease", design: "S-7", handling: faceServed,
		note: "C4 is forwarded with the real runtime id; the daemon's answer is the fixed status body",
	},
	{
		name: "S-26 skill-bundles resolve", upstream: "POST /api/daemon/runtimes/{runtimeId}/tasks/{taskId}/skill-bundles/resolve",
		caller: "internal/daemon/client.go:419",
		local:  "POST /api/daemon/runtimes/{rid}/tasks/{tid}/skill-bundles/resolve", design: "S-26", handling: faceServed,
		note: "forwarded with the real runtime id and Foreman's credential; upstream status/bytes replayed",
	},
	{
		name: "S-9 task status", upstream: "GET /api/daemon/tasks/{taskId}/status",
		caller: "internal/daemon/client.go:716",
		local:  "GET /api/daemon/tasks/{tid}/status", design: "S-9", handling: faceServed,
		note: "the daemon substring-matches \"task not found\" (client.go:206-215), so the local 404 must keep that text",
	},
	{
		name: "S-8 start", upstream: "POST /api/daemon/tasks/{taskId}/start",
		caller: "internal/daemon/client.go:480",
		local:  "POST /api/daemon/tasks/{tid}/start", design: "S-8", handling: faceServed,
	},
	{
		name: "S-10 progress", upstream: "POST /api/daemon/tasks/{taskId}/progress",
		caller: "internal/daemon/client.go:587",
		local:  "POST /api/daemon/tasks/{tid}/progress", design: "S-10", handling: faceServed,
	},
	{
		name: "S-11 messages", upstream: "POST /api/daemon/tasks/{taskId}/messages",
		caller: "internal/daemon/client.go:615",
		local:  "POST /api/daemon/tasks/{tid}/messages", design: "S-11", handling: faceServed,
	},
	{
		name: "S-12 usage", upstream: "POST /api/daemon/tasks/{taskId}/usage",
		caller: "internal/daemon/client.go:648",
		local:  "POST /api/daemon/tasks/{tid}/usage", design: "S-12", handling: faceServed,
	},
	{
		name: "S-13 complete", upstream: "POST /api/daemon/tasks/{taskId}/complete",
		caller: "internal/daemon/client.go:621",
		local:  "POST /api/daemon/tasks/{tid}/complete", design: "S-13", handling: faceServed,
	},
	{
		name: "S-14 fail", upstream: "POST /api/daemon/tasks/{taskId}/fail",
		caller: "internal/daemon/client.go:657",
		local:  "POST /api/daemon/tasks/{tid}/fail", design: "S-14", handling: faceServed,
		note: "the attribution gate may replace the daemon body (§4.4)",
	},
	{
		name: "S-15 cancel-ack", upstream: "POST /api/daemon/tasks/{taskId}/cancel-ack",
		caller: "internal/daemon/client.go:570",
		local:  "POST /api/daemon/tasks/{tid}/cancel-ack", design: "S-15", handling: faceServed,
	},
	{
		name: "S-16 session", upstream: "POST /api/daemon/tasks/{taskId}/session",
		caller: "internal/daemon/client.go:692",
		local:  "POST /api/daemon/tasks/{tid}/session", design: "S-16", handling: faceServed,
	},
	{
		name: "S-17 wait-local-directory", upstream: "POST /api/daemon/tasks/{taskId}/wait-local-directory",
		caller: "internal/daemon/client.go:535",
		local:  "POST /api/daemon/tasks/{tid}/wait-local-directory", design: "S-17", handling: faceServed,
	},
	{
		name: "S-18 recover-orphans", upstream: "POST /api/daemon/runtimes/{runtimeId}/recover-orphans",
		caller: "internal/daemon/client.go:709",
		local:  "POST /api/daemon/runtimes/{rid}/recover-orphans", design: "S-18", handling: faceServed,
		note: "answers zeros: orphans inside Jobs are the recovery module's business",
	},
	{
		name: "S-21 issue gc-check (batch)", upstream: "POST /api/daemon/workspaces/{workspaceId}/issues/gc-check",
		caller: "internal/daemon/client.go:915",
		local:  "POST /api/daemon/workspaces/{ws}/issues/gc-check", design: "S-21", handling: faceServed,
		note: "answers an empty batch; the daemon's legacy fallback keys on the upstream's plain-text 404 body (client.go:902), which never appears here",
	},
	{
		name: "S-20 task gc-check", upstream: "GET /api/daemon/tasks/{taskId}/gc-check",
		caller: "internal/daemon/client.go:1020",
		local:  "GET /api/daemon/tasks/{tid}/gc-check", design: "S-20", handling: faceServed,
		note: "forwards the query and replays upstream status/bytes verbatim",
	},
	{
		name: "S-24 token renew", upstream: "POST /api/tokens/current/renew",
		caller: "internal/daemon/client.go:787",
		local:  "POST /api/tokens/current/renew", design: "S-24", handling: faceServed,
		note: "synthesized now+24h: the Job Token is not renewed through this endpoint",
	},
	{
		name: "S-28 workspaces (legacy)", upstream: "GET /api/workspaces",
		caller: "internal/daemon/client.go:855 (only after /workspaces/{id} 404s)",
		local:  "GET /api/workspaces", design: "S-28", handling: faceServed,
	},

	// ---- refused (registered, always 404) ----
	{
		name: "S-3 runtime profiles", upstream: "GET /api/daemon/workspaces/{workspaceId}/runtime-profiles",
		caller: "internal/daemon/client.go:1136",
		local:  "GET /api/daemon/workspaces/{ws}/runtime-profiles", design: "S-3", handling: faceRefused,
		note:  "the daemon tolerates any 404 here (client.go:1133-1135); carrying the profiles is ADR-043 (proposed)",
		probe: &daemonFaceProbe{method: "GET", path: "/api/daemon/workspaces/ws-1/runtime-profiles", want: 404},
	},
	{
		name: "S-32 legacy per-runtime claim", upstream: "POST /api/daemon/runtimes/{runtimeId}/tasks/claim",
		caller: "internal/daemon/client.go:335 (batch-claim 404 fallback only)",
		local:  "POST /api/daemon/runtimes/{rid}/tasks/claim", design: "S-32", handling: faceRefused,
		note:  "no delivery face; the first request per entry is recorded as the signal that the daemon left the batch path (ADR-042)",
		probe: &daemonFaceProbe{method: "POST", path: "/api/daemon/runtimes/{rid}/tasks/claim", body: "{}", want: 404},
	},
	{
		name: "S-27 update result", upstream: "POST /api/daemon/runtimes/{runtimeId}/update/{updateId}/result",
		caller: "internal/daemon/client.go:749",
		local:  "POST /api/daemon/runtimes/{rid}/update/{req}/result", design: "S-27", handling: faceRefused,
		note:  "任务 Job 的待办恒空 (F9), so no self-update result is ever accepted",
		probe: &daemonFaceProbe{method: "POST", path: "/api/daemon/runtimes/{rid}/update/req-1/result", body: "{}", want: 404},
	},
	{
		name: "S-27 local skills result", upstream: "POST /api/daemon/runtimes/{runtimeId}/local-skills/{requestId}/result",
		caller: "internal/daemon/client.go:759",
		local:  "POST /api/daemon/runtimes/{rid}/local-skills/{req}/result", design: "S-27", handling: faceRefused,
		probe: &daemonFaceProbe{method: "POST", path: "/api/daemon/runtimes/{rid}/local-skills/req-1/result", body: "{}", want: 404},
	},
	{
		name: "S-27 local skills import result", upstream: "POST /api/daemon/runtimes/{runtimeId}/local-skills/import/{requestId}/result",
		caller: "internal/daemon/client.go:764",
		local:  "POST /api/daemon/runtimes/{rid}/local-skills/import/{req}/result", design: "S-27", handling: faceRefused,
		probe: &daemonFaceProbe{method: "POST", path: "/api/daemon/runtimes/{rid}/local-skills/import/req-1/result", body: "{}", want: 404},
	},
	{
		name: "S-27 model result", upstream: "POST /api/daemon/runtimes/{runtimeId}/models/{requestId}/result",
		caller: "internal/daemon/client.go:754",
		local:  "POST /api/daemon/runtimes/{rid}/models/{req}/result", design: "S-27", handling: faceRefused,
		note:  "a task Job gets the 404 (F9); behind it the probe sessions split by source — a command id is relayed verbatim, a startup-discovery id is absorbed (probe_handlers_test.go)",
		probe: &daemonFaceProbe{method: "POST", path: "/api/daemon/runtimes/{rid}/models/req-1/result", body: "{}", want: 404},
	},
	{
		name: "S-22 issue gc-check", upstream: "GET /api/daemon/issues/{issueId}/gc-check",
		caller: "internal/daemon/client.go:941 (batch-404 fallback only)",
		local:  "GET /api/daemon/issues/{id}/gc-check", design: "S-22", handling: faceRefused,
		note:  "GC is off Job-side; 404 switches the daemon off the path",
		probe: &daemonFaceProbe{method: "GET", path: "/api/daemon/issues/issue-1/gc-check", want: 404},
	},
	{
		name: "S-22 chat-session gc-check", upstream: "GET /api/daemon/chat-sessions/{sessionId}/gc-check",
		caller: "internal/daemon/client.go:984",
		local:  "GET /api/daemon/chat-sessions/{id}/gc-check", design: "S-22", handling: faceRefused,
		probe: &daemonFaceProbe{method: "GET", path: "/api/daemon/chat-sessions/session-1/gc-check", want: 404},
	},
	{
		name: "S-22 autopilot-run gc-check", upstream: "GET /api/daemon/autopilot-runs/{runId}/gc-check",
		caller: "internal/daemon/client.go:1003",
		local:  "GET /api/daemon/autopilot-runs/{id}/gc-check", design: "S-22", handling: faceRefused,
		probe: &daemonFaceProbe{method: "GET", path: "/api/daemon/autopilot-runs/run-1/gc-check", want: 404},
	},

	// ---- forwarded (verbatim, caller's credential) ----
	{
		name: "S-23 plugin-hooks (POST)", upstream: "POST /api/daemon/tasks/{id}/plugin-hooks",
		caller: "internal/daemon/client.go:1393",
		local:  "POST /api/daemon/tasks/{tid}/plugin-hooks", design: "S-23", handling: faceForwarded,
		note: "the Job Token is verified locally first, then the caller's own header set goes upstream",
		probe: &daemonFaceProbe{method: "POST", path: "/api/daemon/tasks/{tid}/plugin-hooks", body: `{"hook":"a"}`, want: http.StatusTeapot,
			upstream: "/api/daemon/tasks/{tid}/plugin-hooks"},
	},
	{
		name: "S-23 plugin-hooks (GET)", local: "GET /api/daemon/tasks/{tid}/plugin-hooks",
		design: "S-23", handling: faceForwarded,
		note: "defensive: no upstream counterpart and no daemon caller; the forward lands on the upstream's own 404",
		probe: &daemonFaceProbe{method: "GET", path: "/api/daemon/tasks/{tid}/plugin-hooks", want: http.StatusTeapot,
			upstream: "/api/daemon/tasks/{tid}/plugin-hooks"},
	},
	{
		name: "S-23 plugin-mcp credential", upstream: "GET /api/daemon/tasks/{id}/plugin-mcp/{contributionId}/credential",
		caller: "internal/daemon/client.go:250",
		local:  "GET /api/daemon/tasks/{tid}/plugin-mcp/{cid}/credential", design: "S-23", handling: faceForwarded,
		probe: &daemonFaceProbe{method: "GET", path: "/api/daemon/tasks/{tid}/plugin-mcp/cid-1/credential", want: http.StatusTeapot,
			upstream: "/api/daemon/tasks/{tid}/plugin-mcp/cid-1/credential"},
	},
	{
		name: "S-23 remote-mcp credential", upstream: "GET /api/daemon/tasks/{id}/remote-mcp/{contributionId}/credential",
		caller: "internal/daemon/client.go:248 (default route)",
		local:  "GET /api/daemon/tasks/{tid}/remote-mcp/{cid}/credential", design: "S-23", handling: faceForwarded,
		note: "the OSS server does not register remote-mcp (cloud-only): the upstream's own 404 is replayed, which is why the local route must exist",
		probe: &daemonFaceProbe{method: "GET", path: "/api/daemon/tasks/{tid}/remote-mcp/cid-1/credential", want: http.StatusTeapot,
			upstream: "/api/daemon/tasks/{tid}/remote-mcp/cid-1/credential"},
	},

	// ---- unmodelled (an fmj_ call lands on the catch-all 404) ----
	{
		name: "unmodelled: batch claim alias", upstream: "POST /api/daemon/claim",
		handling: faceUnmodelled,
		note:     "transitional alias; no daemon code calls it (client.go:306 uses /tasks/claim)",
		probe:    &daemonFaceProbe{method: "POST", path: "/api/daemon/claim", body: "{}", want: 404},
	},
	{
		name: "unmodelled: pending tasks", upstream: "GET /api/daemon/runtimes/{runtimeId}/tasks/pending",
		handling: faceUnmodelled,
		note:     "no daemon caller",
		probe:    &daemonFaceProbe{method: "GET", path: "/api/daemon/runtimes/{rid}/tasks/pending", want: 404},
	},
	{
		name: "unmodelled: supplement claim", upstream: "POST /api/daemon/tasks/{taskId}/supplements/claim",
		caller:   "internal/daemon/client.go:446",
		handling: faceUnmodelled,
		note:     "the daemon reaches it only when the start ack advertises supplement_capability; 404 or 412 switches the loop off (task_supplement.go:110-113). Registered as an open gap: ADR-044 item 12",
		probe:    &daemonFaceProbe{method: "POST", path: "/api/daemon/tasks/{tid}/supplements/claim", body: "{}", want: 404},
	},
	{
		name: "unmodelled: supplement ack", upstream: "POST /api/daemon/tasks/{taskId}/supplements/{commentId}/ack",
		caller:   "internal/daemon/client.go:457",
		handling: faceUnmodelled,
		note:     "same face as the claim half",
		probe:    &daemonFaceProbe{method: "POST", path: "/api/daemon/tasks/{tid}/supplements/comment-1/ack", body: "{}", want: 404},
	},
	{
		name: "unmodelled: task messages (GET)", upstream: "GET /api/daemon/tasks/{taskId}/messages",
		handling: faceUnmodelled,
		note:     "server-side catch-up read; no daemon caller (the daemon only POSTs to this path)",
		probe:    &daemonFaceProbe{method: "GET", path: "/api/daemon/tasks/{tid}/messages", want: 404},
	},
	{
		name: "unmodelled: user runtime list", upstream: "GET /api/runtimes",
		handling: faceUnmodelled,
		note:     "user/CLI face, never called with a daemon credential; non-fmj_ callers reach the upstream through the S29 passthrough",
		probe:    &daemonFaceProbe{method: "GET", path: "/api/runtimes", want: 404},
	},
}

// TestDaemonFaceCensus keeps the local face and the census in step: every
// upstream route is declared, every declared local pattern is registered, and
// no pattern is registered without being declared.
func TestDaemonFaceCensus(t *testing.T) {
	// The WS endpoint is conditional (§1.2 S30); the WS rig is the setup where
	// the full table is registered.
	rig, _ := newTestRigWS(t)
	fs, ok := rig.handler.(*FakeServer)
	if !ok {
		t.Fatalf("rig handler is %T, want *FakeServer", rig.handler)
	}

	registered := map[string]bool{}
	for _, pattern := range fs.registeredPatterns() {
		if registered[pattern] {
			t.Errorf("pattern %q is registered twice", pattern)
		}
		registered[pattern] = true
	}
	// The catch-all is the face's own last resort, not a route of the face.
	if !registered["/api/"] {
		t.Error("the catch-all pattern /api/ is not registered")
	}
	delete(registered, "/api/")

	declared := map[string]string{} // pattern → census name
	upstreamSeen := map[string]string{}
	for _, e := range upstreamDaemonFace {
		if e.upstream != "" {
			if prev, dup := upstreamSeen[e.upstream]; dup {
				t.Errorf("upstream route %s declared twice (%s, %s)", e.upstream, prev, e.name)
			}
			upstreamSeen[e.upstream] = e.name
		}
		switch e.handling {
		case faceUnmodelled:
			if e.local != "" {
				t.Errorf("%s: unmodelled entry must not declare a local pattern", e.name)
			}
			continue
		case faceServed, faceForwarded, faceRefused:
			if e.local == "" {
				t.Errorf("%s: %s entry must declare its local pattern", e.name, e.handling)
				continue
			}
		default:
			t.Errorf("%s: unknown handling %q", e.name, e.handling)
			continue
		}
		if prev, dup := declared[e.local]; dup {
			t.Errorf("pattern %s declared twice (%s, %s)", e.local, prev, e.name)
		}
		declared[e.local] = e.name
	}

	for pattern, name := range declared {
		if !registered[pattern] {
			t.Errorf("%s: declared pattern %q is not registered", name, pattern)
		}
	}
	for pattern := range registered {
		if _, ok := declared[pattern]; !ok {
			t.Errorf("pattern %q is registered but not declared in upstreamDaemonFace", pattern)
		}
	}
}

// TestDaemonFaceProbeCoverage keeps the census and its probes one artifact: the
// handling class decides whether an entry is driven through the real FakeServer
// and how, so a route that loses its probe fails here instead of thinning the
// AC-59 coverage silently. The entry is the only place the probe exists, and a
// class whose probe rule is not written down here fails too.
func TestDaemonFaceProbeCoverage(t *testing.T) {
	for _, e := range upstreamDaemonFace {
		p := e.probe
		switch e.handling {
		case faceRefused, faceUnmodelled:
			// The declared answer is local: the 404, upstream untouched.
			if p == nil {
				t.Errorf("%s: %s entry carries no probe", e.name, e.handling)
				continue
			}
			if p.upstream != "" {
				t.Errorf("%s: %s probe reaches the upstream at %s; the answer is local", e.name, e.handling, p.upstream)
			}
			if p.want != http.StatusNotFound {
				t.Errorf("%s: %s probe wants status %d, want the declared 404", e.name, e.handling, p.want)
			}
		case faceForwarded:
			if p == nil {
				t.Errorf("%s: forwarded entry carries no probe", e.name)
				continue
			}
			if p.upstream == "" {
				t.Errorf("%s: forwarded probe declares no upstream path", e.name)
			}
		case faceServed:
			// The §1.2 S-table tests assert what the local handler answers; a
			// probe here would route the entry through the refusal/forward
			// sweeps, which is not how the census declares it.
			if p != nil {
				t.Errorf("%s: served entry carries a probe; declare the refusal or the forward it observes", e.name)
			}
		default:
			t.Errorf("%s: unknown handling %q — its probe rule is undeclared", e.name, e.handling)
		}
	}
}

// daemonFaceProbe drives one census entry through the real FakeServer. It is a
// field of the entry it drives, so a probe cannot outlive its declaration or
// drift from the entry's name.
type daemonFaceProbe struct {
	method string
	// path is the concrete request path; {tid}, {rid} and {ws} are substituted
	// with the rig's task, job runtime and workspace.
	path string
	body string
	// want is the status the daemon must see.
	want int
	// upstream is the concrete path the call must reach the upstream on; empty
	// when the call must not reach the upstream at all. The two probe tests
	// select on it, so the shape decides which of them drives the entry.
	upstream string
}

// TestDaemonFaceUnservedRoutesRefuseLocally drives every census entry whose
// answer must stay local — the refused and the unmodelled: the declared status
// reaches the daemon and the upstream is never called. A route that silently
// starts forwarding (or starts answering) breaks the daemon's capability
// detection, which reads these 404s. Body shape is checked too: the daemon
// matches the local 404 text ("task not found", "runtime not found") through
// the same requestError path it uses upstream (server/internal/daemon/client.go:206-215).
func TestDaemonFaceUnservedRoutesRefuseLocally(t *testing.T) {
	for _, e := range upstreamDaemonFace {
		p := e.probe
		if p == nil || p.upstream != "" {
			continue
		}
		t.Run(e.name, func(t *testing.T) {
			rig := newTestRig(t)
			taskID := "t-face"
			rig.claimTask(t, taskID)
			tok := rig.token(t, taskID)
			jobRID := rig.registerDaemon(t, taskID, tok)
			path := faceProbePath(p.path, taskID, jobRID)

			before := len(rig.stub.all())
			code, body := rig.call(t, p.method, path, tok, []byte(p.body))
			if code != p.want {
				t.Fatalf("%s %s = %d (%s), want %d", p.method, path, code, body, p.want)
			}
			if after := len(rig.stub.all()); after != before {
				t.Errorf("%s %s reached the upstream (%d new calls); a refused route must be answered locally",
					p.method, path, after-before)
			}
			var errBody struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(body, &errBody); err != nil || errBody.Error == "" {
				t.Errorf("%s %s body = %s, want the {\"error\":…} shape", p.method, path, body)
			}
		})
	}
}

// TestDaemonFaceForwardedRoutesReachUpstream drives every census entry declared
// as a verbatim forward: the declared path reaches the upstream, the caller's
// own credential goes with it, and the upstream's status, header set and body
// reach the caller as given (§2.4 — a forward moves bytes, it does not rewrite
// them).
func TestDaemonFaceForwardedRoutesReachUpstream(t *testing.T) {
	const headerMarker = "X-Face-Upstream"
	for _, e := range upstreamDaemonFace {
		p := e.probe
		if p == nil || p.upstream == "" {
			continue
		}
		t.Run(e.name, func(t *testing.T) {
			rig := newTestRig(t)
			taskID := "t-face"
			rig.claimTask(t, taskID)
			tok := rig.token(t, taskID)
			jobRID := rig.registerDaemon(t, taskID, tok)
			path := faceProbePath(p.path, taskID, jobRID)
			upstream := faceProbePath(p.upstream, taskID, jobRID)

			wantBody := []byte(`{"credential_header":"x","credential":"v","x_future_field":true}`)
			rig.stub.on(p.method, upstream, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
				w.Header().Set(headerMarker, "kept")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(p.want)
				_, _ = w.Write(wantBody)
			})

			before := len(rig.stub.all())
			// callWithHeaders rather than call: the header set the caller sees
			// back is half of what §2.4 says a forward replays, and it is the
			// half a rewritten answer would silently drop.
			code, respHeader, body := rig.callWithHeaders(t, p.method, path,
				http.Header{"Authorization": {"Bearer " + tok}}, []byte(p.body))
			calls := rig.stub.all()[before:]

			if len(calls) != 1 {
				t.Fatalf("%s %s: upstream calls = %d, want 1", p.method, path, len(calls))
			}
			if calls[0].Method != p.method || calls[0].Path != upstream {
				t.Errorf("upstream saw %s %s, want %s %s", calls[0].Method, calls[0].Path, p.method, upstream)
			}
			if got := calls[0].Header.Get("Authorization"); got != "Bearer "+tok {
				t.Errorf("Authorization = %q, want the caller's own token forwarded verbatim", got)
			}
			if code != p.want {
				t.Errorf("%s %s = %d, want the upstream status %d replayed", p.method, path, code, p.want)
			}
			if !bytes.Equal(body, wantBody) {
				t.Errorf("body = %s, want the upstream bytes verbatim", body)
			}
			if got := respHeader.Get(headerMarker); got != "kept" {
				t.Errorf("%s %s: response %s = %q, want the upstream's own header replayed",
					p.method, path, headerMarker, got)
			}
			if got := respHeader.Get("Content-Type"); got != "application/json" {
				t.Errorf("%s %s: response Content-Type = %q, want the upstream's own value",
					p.method, path, got)
			}
		})
	}
}

// faceProbePath substitutes the probe placeholders.
func faceProbePath(path, taskID, jobRuntimeID string) string {
	r := strings.NewReplacer("{tid}", taskID, "{rid}", jobRuntimeID, "{ws}", "ws-1")
	return r.Replace(path)
}

// TestDaemonFaceCarrierFidelity field-checks the carriers that reproduce
// upstream payloads: the claim payload (§2.5: only runtime_id moves, every
// other field — including keys this build does not model — survives) and the
// S20 gc-check answer (status and bytes verbatim).
func TestDaemonFaceCarrierFidelity(t *testing.T) {
	t.Run("claim payload keeps every field but runtime_id", func(t *testing.T) {
		rig := newTestRig(t)
		taskID := "t-carry"
		payload := json.RawMessage(`{
			"id": "t-carry", "agent_id": "agent-1", "issue_id": "issue-1",
			"issue_identifier": "TSI-1", "workspace_id": "ws-1",
			"runtime_id": "rid-real", "dispatched_at": "2026-10-18T00:00:00Z",
			"auth_token": "mat_tasktoken", "start_claim_supported": true,
			"agent": {"model": "k3", "thinking": {"level": 2}},
			"repos": [{"url": "https://example.invalid/r", "ref": "main"}],
			"remote_mcp_connections": [{"id": "mcp-1", "daemon_token": "tok"}],
			"plugin_hook_tools": ["a", "b"],
			"coalesced_comments": [{"id": "c-1", "body": "hi"}],
			"prior_session_id": "sess-1", "trigger_kind": "assign",
			"x_future_field": {"nested": [1, 2, {"deep": true}], "null": null}
		}`)
		if err := rig.sched.OnClaim(t.Context(), payload); err != nil {
			t.Fatalf("OnClaim: %v", err)
		}
		tok := rig.token(t, taskID)
		jobRID := rig.registerDaemon(t, taskID, tok)

		code, body := rig.call(t, "POST", "/api/daemon/tasks/claim", tok,
			[]byte(`{"daemon_id":"fm-`+taskID+`","runtime_ids":["`+jobRID+`"],"max_tasks":1}`))
		if code != 200 {
			t.Fatalf("S6 = %d: %s", code, body)
		}
		var resp struct {
			Tasks []json.RawMessage `json:"tasks"`
		}
		if err := json.Unmarshal(body, &resp); err != nil || len(resp.Tasks) != 1 {
			t.Fatalf("S6 body = %s (%v)", body, err)
		}

		var want map[string]json.RawMessage
		if err := json.Unmarshal(payload, &want); err != nil {
			t.Fatalf("payload: %v", err)
		}
		runtimeRaw, err := json.Marshal(jobRID)
		if err != nil {
			t.Fatalf("marshal runtime id: %v", err)
		}
		want["runtime_id"] = runtimeRaw
		wantRaw, err := json.Marshal(want)
		if err != nil {
			t.Fatalf("marshal want: %v", err)
		}
		assertSameJSON(t, resp.Tasks[0], wantRaw)
	})

	t.Run("task gc-check replays upstream status and bytes", func(t *testing.T) {
		rig := newTestRig(t)
		taskID := "t-gc"
		rig.claimTask(t, taskID)
		tok := rig.token(t, taskID)
		// The upstream's payload may carry fields this build never modelled;
		// the daemon must see the same bytes either way.
		upstreamBody := []byte(`{"status":"completed","completed_at":"2026-10-05T00:00:00Z","x_future_field":[1,2]}`)
		rig.stub.on("GET", "/api/daemon/tasks/"+taskID+"/gc-check",
			func(w http.ResponseWriter, _ *http.Request, _ []byte) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(upstreamBody)
			})

		code, body := rig.call(t, "GET", "/api/daemon/tasks/"+taskID+"/gc-check", tok, nil)
		if code != http.StatusOK || !bytes.Equal(body, upstreamBody) {
			t.Errorf("S20 = %d %s, want %d %s verbatim", code, body, http.StatusOK, upstreamBody)
		}
	})
}

// assertSameJSON compares two JSON documents semantically (key order and
// whitespace are not part of the comparison; numbers and unknown keys are).
func assertSameJSON(t *testing.T, got, want []byte) {
	t.Helper()
	var g, w any
	if err := decodeJSON(got, &g); err != nil {
		t.Fatalf("decode got: %v (%s)", err, got)
	}
	if err := decodeJSON(want, &w); err != nil {
		t.Fatalf("decode want: %v (%s)", err, want)
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("JSON differs\n got: %s\nwant: %s", got, want)
	}
}

func decodeJSON(raw []byte, out *any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return dec.Decode(out)
}

// stubFaceRegister registers the C1 answer the deviation register below reads:
// the workspace settings block, a profile_id on the answered runtime, and a key
// this build does not model — everything the real server can put on the
// register response.
func stubFaceRegister(answer map[string]any) func(*stubUpstream) {
	return func(stub *stubUpstream) {
		stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
			writeJSONStub(w, 200, answer)
		})
	}
}

// faceRegisterAnswer is the C1 answer above. The field set is the point: the
// deviations in TestDaemonFaceDeclaredDeviations are only observable because
// these upstream fields are present.
func faceRegisterAnswer() map[string]any {
	return map[string]any{
		"runtimes": []map[string]any{{
			"id": "rid-real", "name": "foreman-omp", "provider": "omp", "status": "online",
			"profile_id": "prof-custom",
		}},
		"repos":         json.RawMessage(faceReposCarrier),
		"repos_version": "abc123",
		"settings": map[string]any{
			"github_enabled": true, "co_authored_by_enabled": false,
		},
		"x_future_field": map[string]any{"keep": true},
	}
}

// faceReposCarrier is the C1 repos array the deviation tests use: it carries a
// per-repo key this build does not model, so "replayed verbatim" is observable
// rather than assumed.
const faceReposCarrier = `[{"url":"https://example.invalid/r","description":"d","ref":"main","x_repo_key":{"nested":1}}]`

// TestDaemonFaceDeclaredDeviations is the executable register of the
// upstream-sourced fields the Job daemon reads but Foreman does not carry.
//
// The declared shape is asserted on purpose: the deviation is a decision, not
// an accident, so it must not drift in either direction unnoticed. The moment
// the passthrough lands (ADR-043 for profile_id and the runtime-profiles route,
// ADR-044 for settings; design PR #79 — proposed, not merged), its assertion
// moves with it. A half-carried field — settings on S-4 but not on S-2 — fails
// here, which is exactly the trap the manual audit flagged.
func TestDaemonFaceDeclaredDeviations(t *testing.T) {
	answer := faceRegisterAnswer()
	raw, err := json.Marshal(answer)
	if err != nil {
		t.Fatalf("register answer: %v", err)
	}
	// Non-vacuity: the upstream answer really presents the fields under test,
	// so "the daemon does not see them" is a statement about Foreman, not about
	// the fixture.
	for _, want := range []string{`"co_authored_by_enabled":false`, `"profile_id":"prof-custom"`} {
		if !bytes.Contains(raw, []byte(want)) {
			t.Fatalf("C1 fixture does not present %s: %s", want, raw)
		}
	}

	rig := newTestRigWithRegister(t, stubFaceRegister(answer))
	taskID := "t-devia"
	rig.claimTask(t, taskID)
	tok := rig.token(t, taskID)

	_, s2 := rig.call(t, "GET", "/api/daemon/workspaces/ws-1/repos", tok, nil)
	_, s4 := rig.call(t, "POST", "/api/daemon/register", tok,
		[]byte(`{"daemon_id":"fm-`+taskID+`","runtimes":[{"name":"foreman-job","type":"omp"}]}`))

	// Deviation 1 (ADR-044, proposed): settings. The upstream answer carries
	// it, the daemon writes it into its workspace state and gates the GitHub
	// hook and the Co-authored-by trailer on it; the local face answers null,
	// so the daemon falls back to "enabled" (server/internal/daemon/daemon.go
	// :3261-3281).
	for name, body := range map[string][]byte{"S-2 repos": s2, "S-4 register": s4} {
		var resp map[string]json.RawMessage
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("%s: %v (%s)", name, err, body)
		}
		settings, ok := resp["settings"]
		if !ok {
			t.Errorf("%s: settings key absent — the deviation is \"answered null\", not \"omitted\"", name)
			continue
		}
		if string(settings) != "null" {
			t.Errorf("%s: settings = %s, want the declared deviation (null) — if the ADR-044 passthrough landed, flip this register", name, settings)
		}
		// The carrier half that does work: repos round-trips as upstream gave
		// it, unknown per-repo keys included.
		var got, want any
		if err := decodeJSON(resp["repos"], &got); err != nil {
			t.Fatalf("%s: repos: %v", name, err)
		}
		if err := decodeJSON([]byte(faceReposCarrier), &want); err != nil {
			t.Fatalf("carrier: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: repos = %s, want the upstream array verbatim (%s)", name, resp["repos"], faceReposCarrier)
		}
	}

	// Deviation 2 (ADR-043, proposed): profile_id. The daemon selects a custom
	// profile's start command from it; the local face omits it, so a profile
	// task starts as builtin.
	var reg struct {
		Runtimes []map[string]json.RawMessage `json:"runtimes"`
	}
	if err := json.Unmarshal(s4, &reg); err != nil || len(reg.Runtimes) != 1 {
		t.Fatalf("S-4 body = %s (%v)", s4, err)
	}
	if _, has := reg.Runtimes[0]["profile_id"]; has {
		t.Errorf("S-4 runtime carries profile_id — if the ADR-043 passthrough landed, flip this register: %s", s4)
	}
}
