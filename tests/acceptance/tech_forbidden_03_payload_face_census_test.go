// TC-tech-forbidden-03 (AC-06, AC-14, F3 negative) — the payload write-face
// census. The claim payload carries the task-level credential and the prompt
// context; the design confines it to the shared state face and process memory,
// plus the two faces that hand it over (S6 delivery and the ops preview,
// §2.5/§9.3). The verdict body has its own two registered faces: the mapping
// record's decided_report (§4.3) and the pending-report carrier Secret while a
// report is queued (§3, §4.6). Every other face — the ops routes, the Job and
// pod annotations/labels, both Secrets and the shared-state snapshot — is
// judged by a declared key set, so a face that grows a key or carries a body
// byte fails mechanically instead of by review. The log vocabulary's source
// scan is one-sided: an out-of-vocabulary key fails, but a clean scan is not
// evidence that the face emits nothing else (ADR-055 §6).
//
// Scope and known blind spots, so a green run is not read as more than it is:
//   - The probe Job's render (probe.go's label block and
//     probeCredentialSecret) is outside this census: it receives no claim
//     payload and is covered by AC-32 / TC-tech-catalog-04/05.
//   - The Job and pod annotation/label key sets are closed by driving the
//     three trigger shapes and by the render's single write point
//     (jobbuilder/contract.go), not by a source-level AST scan.
//   - The log vocabulary's type index reads only types declared in the same
//     file (tcPayloadAttrClassType); a receiver whose type comes back from a
//     function call is unclassifiable and fails closed.
//
// The ops route set is closed by convention, not by construction: register is
// the one mount point, and the pattern is recorded by the same call that
// mounts it. TestPayloadCensusOpsMountsGoThroughRegister reads this package's
// source and binds values by declaration: the mount receiver must resolve to
// the route table's construction, the mounted pattern must resolve to the
// recorded parameter or an alias of it, and a second name bound to the table,
// a shadowing parameter or range variable, a table name used inside the
// closure for anything but a mount's receiver, a reassigned name, an escaped
// mount method, a foreign receiver or a second route table fails the census.
// The scan is syntactic: it does not follow a pattern mutated through a
// pointer, so a green run is not a proof about runtime values.
package acceptance

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/state"
)

const (
	// tcPayloadMarker is a nonce planted in every payload value no face may
	// echo: a leak carries the marker with it.
	tcPayloadMarker = "plcensus7f3a"
	// tcPayloadVerdictMarker is the nonce in the terminal report body. It is
	// separate from tcPayloadMarker so a face carrying the verdict body is
	// distinguishable from one carrying the delivery body.
	tcPayloadVerdictMarker = "vrdcensus9c1"
	tcPayloadTaskID        = "task-1"
	tcPayloadWSID          = "ws-1"
)

// tcPayloadCensusPayload is the census claim payload. The identity keys carry
// their normal values (the design writes issue-id, attempt and the trigger
// context into annotations, §4.3); the marker rides only the fields that have
// no write face outside the delivery body and the shared state snapshot.
// trigger is the trigger-context JSON fragment the shape carries (§2.5: the
// three contexts are judged independently and their absence is legal).
func tcPayloadCensusPayload(taskID, trigger string) json.RawMessage {
	return json.RawMessage(`{
		"id": "` + taskID + `",
		"agent_id": "agent-1",
		"workspace_id": "` + tcPayloadWSID + `",
		"runtime_id": "rid-zz",
		"auth_token": "mat_` + tcPayloadMarker + `",
		"prompt": "` + tcPayloadMarker + `-prompt",
		"coalesced_comments": [{"id": "c-1", "body": "` + tcPayloadMarker + `-comment"}],
		"x_future_field": {"marker": "` + tcPayloadMarker + `"},
		"agent": {"model": "k3", "name": "zz"},
		` + trigger + `
	}`)
}

// tcPayloadVerdictBody is the S13 body the census sends. Its summary carries
// the verdict marker: the body is the 定论体 the mapping record and the carrier
// keep (§4.3, §4.6), so a copy of it on any other face is visible to the scan.
func tcPayloadVerdictBody() []byte {
	return []byte(`{"status":"completed","result":"ok","summary":"` + tcPayloadVerdictMarker + `"}`)
}

// tcPayloadCensusTrigger is one trigger-context shape: the fragment the payload
// carries and the conditional annotation keys that fragment must produce
// (§4.3: 字段非空 ⇔ 键存在).
type tcPayloadCensusTrigger struct {
	name  string
	extra string
	keys  []string
}

// tcPayloadCensusTriggers covers all three contexts: the issue shape, the
// autopilot shape (no issue context — the legal no-issue payload) and the chat
// shape.
var tcPayloadCensusTriggers = []tcPayloadCensusTrigger{
	{
		name:  "issue",
		extra: `"issue_id": "issue-1", "issue_identifier": "TSI-1"`,
		keys:  []string{"foreman.tsic.top/issue-id", "foreman.tsic.top/issue-identifier"},
	},
	{
		name:  "autopilot",
		extra: `"autopilot_run_id": "run-1"`,
		keys:  []string{"foreman.tsic.top/autopilot-run-id"},
	},
	{
		name:  "chat",
		extra: `"chat_session_id": "session-1"`,
		keys:  []string{"foreman.tsic.top/chat-session-id"},
	},
}

// tcPayloadCensusIssueTrigger is the issue shape the non-annotation scenarios
// drive.
const tcPayloadCensusIssueTrigger = `"issue_id": "issue-1", "issue_identifier": "TSI-1"`

// tcPayloadOpsRoutes is the ops surface's declared route set (§9.3). The
// preview is the one route whose answer is the body itself; every other route
// may not carry it.
var tcPayloadOpsRoutes = []string{
	"GET /metrics",
	"GET /foreman/healthz",
	"GET /foreman/tasks",
	"GET /foreman/tasks/{taskID}",
	"GET /foreman/tasks/{taskID}/logs",
	"GET /foreman/tasks/{taskID}/payload-preview",
	"GET /foreman/register-source",
	"GET /foreman/version",
}

// tcPayloadPreviewRoute is the declared preview pattern (§9.3: 载荷预览是条目态
// 端点 — the delivery body is answerable only while the entry lives).
const tcPayloadPreviewRoute = "GET /foreman/tasks/{taskID}/payload-preview"

// tcPayloadOpsPaths maps the declared route patterns to concrete probe paths;
// it is the one place the {taskID} placeholder is filled, so a route's probe
// path cannot drift from its declaration.
func tcPayloadOpsPaths(t *testing.T, routes []string) []string {
	t.Helper()
	paths := make([]string, 0, len(routes))
	for _, route := range routes {
		pattern, ok := strings.CutPrefix(route, http.MethodGet+" ")
		if !ok {
			t.Fatalf("declared ops route %q is not a GET pattern", route)
		}
		paths = append(paths, strings.ReplaceAll(pattern, "{taskID}", tcPayloadTaskID))
	}
	return paths
}

// tcPayloadOpsProbePaths derives the response-body scan paths from the
// declared route set: registering a route is the one act that also puts it
// under the scan. The preview is the declared body face and is dropped.
func tcPayloadOpsProbePaths(t *testing.T, routes []string) []string {
	t.Helper()
	all := tcPayloadOpsPaths(t, routes)
	probe := make([]string, 0, len(all))
	for i, route := range routes {
		if route == tcPayloadPreviewRoute {
			continue
		}
		probe = append(probe, all[i])
	}
	return probe
}

// tcPayloadOpsMarkerHits asks every given path and returns the ones whose
// answer carries needle. The census scan and its self-proof share this
// function, so the growth test exercises the scan the census runs.
func tcPayloadOpsMarkerHits(t *testing.T, baseURL string, paths []string, needle string) []string {
	t.Helper()
	var hits []string
	for _, path := range paths {
		if _, answer := httpGet(t, baseURL+path); bytes.Contains(answer, []byte(needle)) {
			hits = append(hits, path)
		}
	}
	return hits
}

// tcPayloadProjectionKeys is the task projection's key set (§4.1). The
// payload, the verdict body and the token digest are absent from it.
var tcPayloadProjectionKeys = []string{
	"agent_id", "attempt", "autopilot_run_id", "boot_deadline",
	"chat_session_id", "claims_after_delivery", "claimed_at",
	"daemon_id", "daemon_registered_at", "delivered_at", "deliveries",
	"failure_reason", "issue_id", "issue_identifier", "job_created_at",
	"job_name", "job_namespace", "job_runtime_id", "last_status_seen",
	"lease_keep_until", "node_name", "result", "started_at", "state",
	"task_id", "workspace_id",
}

// tcPayloadSnapshotKeys is the shared-state snapshot's key set (§4.3,
// ADR-046 决策 0). It is the persisted face that carries the delivery body and
// the verdict body — the design puts them there on purpose, so a restart can
// keep delivering. The verdict body also lands on the pending-report carrier
// Secret while a report is queued (§3, §4.6); token_hash is pinned as the
// snapshot's current shape, not as a design requirement (§6.1 puts the digest
// in the revocation set; ADR-017 makes OnClaim its only writer).
var tcPayloadSnapshotKeys = []string{
	"agent_id", "attempt", "autopilot_run_id", "boot_deadline",
	"chat_session_id", "claims_after_delivery", "claimed_at",
	"completed_at", "daemon_id", "daemon_registered_at", "decided_report",
	"delivered_at", "deliveries", "delivery_deadline", "drain_deadline",
	"failure_reason", "issue_id", "issue_identifier", "job_created_at",
	"job_name", "job_namespace", "job_phase", "job_runtime_id",
	"last_status_seen", "lease_keep_until", "node_name", "payload",
	"real_runtime_id", "result", "runtime_identity", "started_at", "state",
	"task_id", "token_hash", "workspace_id",
}

// tcPayloadCarrierKeys is the pending-report carrier Secret's data key set
// (§3: 内容 = 终态上报体逐字字节与重放所需身份; §4.6). It is the second
// design-registered face that carries the verdict body.
var tcPayloadCarrierKeys = []string{
	"attempts", "body", "daemon_id", "endpoint", "enqueued_at",
	"failure_reason", "job_name", "task_id", "workspace_id",
}

// tcPayloadCarrierLabelKeys is the carrier's label set (§6.5: 角色标签（载体）
// ∧ managed-by；任务标识取标签面；workspace 维度随条目).
var tcPayloadCarrierLabelKeys = []string{
	registry.LabelManagedBy,
	registry.LabelRole,
	registry.LabelTaskID,
	registry.LabelWorkspaceID,
}

// tcPayloadRequiredAnnotationKeys is the annotation set every task Job carries
// (§4.3). The four trigger-context keys are conditional blocks, added by
// tcPayloadCensusTriggers.
var tcPayloadRequiredAnnotationKeys = []string{
	"foreman.tsic.top/attempt",
	"foreman.tsic.top/claimed-at",
	"foreman.tsic.top/daemon-id",
	"foreman.tsic.top/job-runtime-id",
}

// tcPayloadJobAnnotationKeys is the full annotation key set a reference render
// may write: the required block plus every conditional trigger key. The census
// judges the rendered key set as a subset of it — a key outside it is a new
// write face, whichever prefix it carries.
var tcPayloadJobAnnotationKeys = append(append([]string(nil),
	tcPayloadRequiredAnnotationKeys...),
	"foreman.tsic.top/issue-id",
	"foreman.tsic.top/issue-identifier",
	"foreman.tsic.top/autopilot-run-id",
	"foreman.tsic.top/chat-session-id",
)

// tcPayloadPodAnnotationKeys is the pod template's annotation set: §4.3 lists
// Jobs, never pods, so the reference render writes none.
var tcPayloadPodAnnotationKeys []string

// tcPayloadJobLabelKeys is the Job label set Foreman writes (§4.3).
var tcPayloadJobLabelKeys = []string{
	"app.kubernetes.io/managed-by",
	"app.kubernetes.io/name",
	"foreman.tsic.top/agent-id",
	"foreman.tsic.top/task-id",
	"foreman.tsic.top/workspace-id",
}

// tcPayloadPodLabelKeys is the pod-template label set (§4.3): no annotations,
// no managed-by — the app name plus the three identity labels.
var tcPayloadPodLabelKeys = []string{
	"app.kubernetes.io/name",
	"foreman.tsic.top/daemon-id",
	"foreman.tsic.top/task-id",
	"foreman.tsic.top/workspace-id",
}

// tcPayloadSecretKeys is the credential Secret's data key set: the Job Token
// and nothing else (§6.2, §8.2 A3).
var tcPayloadSecretKeys = []string{"config.json"}

// tcPayloadLogKeys is the vocabulary the source scan checks against, plus the
// JSON envelope. The scan fails on any key it reads outside the list; a clean
// scan is not qualifying evidence and does not claim the vocabulary covers
// every emit path (§9.1: 日志不得记录载荷字段名; ADR-055 §6). "time" is a plain
// slog.JSONHandler's time key; the production logger renames it to "ts" (§9.1).
var tcPayloadLogKeys = []string{
	"ack_runtime_id", "action", "active_jobs", "adopted",
	"after", "age_seconds", "agent_id", "agent_memory",
	"agent_memory_ttl", "agent_state", "agent_state_ttl", "attempt",
	"attempts", "authenticated",
	"autopilot_run_id", "backoff", "bare_repos", "before",
	"body_bytes", "boot_seconds", "bound", "budget",
	"bytes", "c18", "cache_bytes", "cache_max_bytes",
	"cache_measured", "cache_ttl", "cancel_ack_timeout_seconds", "candidates",
	"cap_policy", "cap_source", "carriers", "changed",
	"chat_session_id", "claim_workspace", "claims_after_delivery", "cli_version",
	"code", "component", "conclusion", "count",
	"credential_scope", "daemon_id", "dead_definitions", "decision",
	"declaration", "deliver_latency_ms", "delivered_at", "deliveries",
	"detail", "drain_seconds", "duration_ms", "duration_seconds",
	"effect", "endpoint", "entries", "err",
	"error_source", "excluded", "fact_source", "failed_profiles",
	"failure_reason", "fallback", "file", "freed_bytes",
	"identity", "image", "instance_id", "interval",
	"issue_id", "issue_identifier", "job_event", "job_image",
	"job_name", "job_namespace", "job_runtime_id", "jobs",
	"key", "kind", "last_heartbeat_age_seconds", "lease_keep_until",
	"level", "limit_bytes", "listen", "max_bytes",
	"max_jobs_per_node", "missing", "msg", "name",
	"name_source", "node", "node_name", "node_selector_policy",
	"node_selector_source", "note", "old_job_name", "op",
	"orphan_pods", "orphaned", "outage_seconds", "overridden_keys",
	"path", "payload_workspace", "phase", "picked",
	"pod_status", "pod_terminated_at", "pod_terminated_at_source", "policy",
	"previous", "probe", "probed", "profile_id",
	"project_source", "provider", "queued", "quiesce",
	"read_bytes", "reason", "register", "remaining_ms",
	"remote_addr", "removed_count", "replay", "request_id",
	"reservations_cleared", "resolved_workspace_id", "resource_policy",
	"resource_source", "result",
	"retried", "retried_write", "retry_in", "runtime_id",
	"runtimes", "saturated", "secret", "server_url",
	"session_exists", "side", "skill_cache", "skill_cache_max_bytes",
	"skill_cache_ttl", "skipped", "soft_affinity_suppressed",
	"source", "stage", "state_root", "status",
	"step", "task_dir_ttl", "task_dirs", "task_id",
	"template", "template_written_faces", "time", "trigger",
	"ts", "ttl", "ttl_seconds", "type",
	"unregistered", "version", "visible_workspaces", "waited_s",
	"window_seconds",
	"workspace_id", "workspace_name", "workspaces", "ws_enabled",
}

// tcPayloadSameSet fails when got and want differ as sets, naming both
// directions: a declared key that vanished and an observed key nobody
// declared.
func tcPayloadSameSet(t *testing.T, label string, got, want []string) {
	t.Helper()
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	if slices.Equal(g, w) {
		return
	}
	t.Errorf("%s key set drifted\n  declared, not observed: %v\n  observed, not declared: %v",
		label, tcPayloadDiff(w, g), tcPayloadDiff(g, w))
}

// tcPayloadDiff returns the entries of a that b lacks.
func tcPayloadDiff(a, b []string) []string {
	var out []string
	for _, entry := range a {
		if !slices.Contains(b, entry) {
			out = append(out, entry)
		}
	}
	return out
}

// tcPayloadJSONKeys reads a struct type's JSON key set from its tags: an
// exported field with an explicit name projects under it, json:"-" keeps it
// out, and an exported field with no tag is itself a census failure (it would
// project under its Go name).
func tcPayloadJSONKeys(t *testing.T, typ reflect.Type) (keys, hidden []string) {
	t.Helper()
	for i := range typ.NumField() {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		switch {
		case tag == "":
			t.Errorf("%s.%s carries no json tag: it would project under its Go name", typ.Name(), field.Name)
		case name == "-":
			hidden = append(hidden, field.Name)
		default:
			keys = append(keys, name)
		}
	}
	return keys, hidden
}

// tcPayloadObjectKeys returns a decoded JSON object's key set.
func tcPayloadObjectKeys(t *testing.T, raw []byte) []string {
	t.Helper()
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("decode JSON object: %v (%s)", err, raw)
	}
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	return keys
}

// tcPayloadMountOps mounts the ops surface over the harness registry with the
// census workspace configured.
func tcPayloadMountOps(t *testing.T, h *harness) *httptest.Server {
	t.Helper()
	ops := httptest.NewServer(observability.NewOpsHandler(
		observability.NewMetrics(), h.reg, nil,
		observability.OpsConfig{WorkspaceIDs: []string{tcPayloadWSID}}))
	t.Cleanup(ops.Close)
	return ops
}

// TestPayloadCensusOpsRouteSetIsDeclared pins the ops surface's route set both
// ways: a route added without a census entry fails, and so does a declared
// route the surface no longer mounts.
func TestPayloadCensusOpsRouteSetIsDeclared(t *testing.T) {
	h := newHarness(t, false)
	ops := observability.NewOpsHandler(observability.NewMetrics(), h.reg, nil,
		observability.OpsConfig{WorkspaceIDs: []string{tcPayloadWSID}})
	tcPayloadSameSet(t, "the ops surface", ops.RegisteredPatterns(), tcPayloadOpsRoutes)

	// A nil metrics registry swaps /metrics for the 503 readiness answer, but
	// the route still exists: a branch mounted outside the shared registration
	// entry point would drop it from the set and hide a real route.
	unwired := observability.NewOpsHandler(nil, h.reg, nil,
		observability.OpsConfig{WorkspaceIDs: []string{tcPayloadWSID}})
	tcPayloadSameSet(t, "the ops surface without a metrics registry", unwired.RegisteredPatterns(), tcPayloadOpsRoutes)

	// Every declared route answers on its own path. A mounted route's 404 is
	// the surface's {"error":…} shape; the mux's own miss is plain text, so
	// the two are distinguishable. The paths are derived from the declaration,
	// so a new route is probed without a second list to edit.
	server := httptest.NewServer(ops)
	t.Cleanup(server.Close)
	paths := tcPayloadOpsPaths(t, tcPayloadOpsRoutes)
	for i, route := range tcPayloadOpsRoutes {
		code, body := httpGet(t, server.URL+paths[i])
		if code == http.StatusOK {
			continue
		}
		var errBody struct {
			Error string `json:"error"`
		}
		if code != http.StatusNotFound || json.Unmarshal(body, &errBody) != nil || errBody.Error == "" {
			t.Errorf("%s %s = %d (%s), want 200 or the surface's 404 error shape",
				route, paths[i], code, body)
		}
	}
}

// TestPayloadCensusOpsBodiesCarryNoPayload drives a real claim and delivery
// and scans every ops answer for the payload marker. The preview is the one
// sanctioned body face, and only in its declared window.
func TestPayloadCensusOpsBodiesCarryNoPayload(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	if err := h.sched.OnClaim(t.Context(), tcPayloadCensusPayload(tcPayloadTaskID, tcPayloadCensusIssueTrigger)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, tcPayloadTaskID)
	registerTaskDaemon(t, h, tcPayloadTaskID, token)
	ops := tcPayloadMountOps(t, h)

	// Before delivery the preview answers 404: the claim-time payload is not
	// the delivery body (§9.3).
	if code, body := httpGet(t, ops.URL+"/foreman/tasks/"+tcPayloadTaskID+"/payload-preview"); code != http.StatusNotFound {
		t.Errorf("preview before delivery = %d (%s), want 404", code, body)
	}

	// The projection is the declared key set, and the list endpoint agrees.
	code, body := httpGet(t, ops.URL+"/foreman/tasks/"+tcPayloadTaskID)
	if code != http.StatusOK {
		t.Fatalf("GET /foreman/tasks/%s = %d (%s), want 200", tcPayloadTaskID, code, body)
	}
	tcPayloadSameSet(t, "the task projection", tcPayloadObjectKeys(t, body), tcPayloadProjectionKeys)

	claimTask(t, h, tcPayloadTaskID, token)
	entry, ok := h.reg.Get(tcPayloadTaskID)
	if !ok || len(entry.Payload) == 0 {
		t.Fatalf("entry %s carries no delivery body after S6: %+v", tcPayloadTaskID, entry)
	}

	// The delivered body is served by the preview alone, byte for byte.
	code, preview := httpGet(t, ops.URL+"/foreman/tasks/"+tcPayloadTaskID+"/payload-preview")
	if code != http.StatusOK || !bytes.Equal(preview, entry.Payload) {
		t.Errorf("preview = %d %s, want the delivery body %s", code, preview, entry.Payload)
	}
	if !strings.Contains(string(preview), tcPayloadMarker) {
		t.Error("the preview does not serve the payload: the census's marker never appears")
	}

	// No other route carries a payload byte, after delivery included. The scan
	// set is derived from tcPayloadOpsRoutes (minus the preview), so a route
	// appended to the declaration is scanned without a second list to edit.
	probePaths := tcPayloadOpsProbePaths(t, tcPayloadOpsRoutes)
	if hits := tcPayloadOpsMarkerHits(t, ops.URL, probePaths, tcPayloadMarker); len(hits) != 0 {
		t.Errorf("ops routes carry the payload marker: %v", hits)
	}

	// A terminal report puts the verdict body in the entry; no ops route is
	// its face either (§9.3), so the same derived set is rescanned for it.
	if code, body := h.call("POST", "/api/daemon/tasks/"+tcPayloadTaskID+"/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	if code, body := h.call("POST", "/api/daemon/tasks/"+tcPayloadTaskID+"/complete", token, tcPayloadVerdictBody()); code != http.StatusOK {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}
	if hits := tcPayloadOpsMarkerHits(t, ops.URL, probePaths, tcPayloadVerdictMarker); len(hits) != 0 {
		t.Errorf("ops routes carry the verdict body: %v", hits)
	}
}

// TestPayloadCensusOpsProbePathsFollowTheDeclaration proves the body scan is
// derived from the declaration rather than transcribed: appending a route to
// tcPayloadOpsRoutes puts its probe path under the scan with no second edit,
// and a handler that answers the delivery body at that path is reported by the
// same scan the census runs.
func TestPayloadCensusOpsProbePathsFollowTheDeclaration(t *testing.T) {
	all := tcPayloadOpsPaths(t, tcPayloadOpsRoutes)
	paths := tcPayloadOpsProbePaths(t, tcPayloadOpsRoutes)
	for i, route := range tcPayloadOpsRoutes {
		if route == tcPayloadPreviewRoute {
			if slices.Contains(paths, all[i]) {
				t.Errorf("the derived scan includes the declared body face %s", all[i])
			}
			continue
		}
		if !slices.Contains(paths, all[i]) {
			t.Errorf("declared route %s has no scan path", route)
		}
	}

	// The probe shape: a route that returns the entry payload, appended to the
	// declared set like any other route.
	const leakRoute = "GET /foreman/tasks/{taskID}/leak"
	leakPath := "/foreman/tasks/" + tcPayloadTaskID + "/leak"
	extended := tcPayloadOpsProbePaths(t, append(slices.Clone(tcPayloadOpsRoutes), leakRoute))
	if !slices.Contains(extended, leakPath) {
		t.Fatalf("a route appended to the declaration is not scanned: %v", extended)
	}
	if len(extended) != len(paths)+1 {
		t.Errorf("scan paths = %d, want one more after appending a route: %v", len(extended), extended)
	}

	// The scan has the discriminant power: an answer carrying the marker on
	// the appended path is reported, and no other path matches.
	mux := http.NewServeMux()
	mux.HandleFunc("/foreman/tasks/{taskID}/leak", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(tcPayloadMarker))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	hits := tcPayloadOpsMarkerHits(t, server.URL, extended, tcPayloadMarker)
	distinct := map[string]bool{}
	for _, hit := range hits {
		distinct[hit] = true
	}
	if !distinct[leakPath] {
		t.Errorf("the scan missed a leaking answer on %s: hits %v", leakPath, hits)
	}
	if len(distinct) != 1 {
		t.Errorf("the scan reported %v, want only %s", hits, leakPath)
	}
}

// TestPayloadCensusOpsMountsGoThroughRegister enforces the convention the ops
// route set relies on: register is the one mount point, every mount in it
// passes the pattern it records, and the package keeps one route table. The
// census reads the observability package back, so a route mounted beside the
// recording call — served on the wire, absent from RegisteredPatterns — fails
// here instead of passing unobserved.
func TestPayloadCensusOpsMountsGoThroughRegister(t *testing.T) {
	fset := token.NewFileSet()
	files := tcPayloadObservabilitySource(t, fset)
	if findings := tcPayloadOpsMountAudit(fset, files); len(findings) != 0 {
		t.Fatalf("the ops mount audit failed:\n  %s", strings.Join(findings, "\n  "))
	}
}

// TestPayloadCensusOpsMountAuditHasTeeth feeds the audit each bypass shape and
// requires it reported: a mount inside register whose pattern is a literal, a
// mount through a receiver alias, an escaped mount method, a reassigned
// recorded pattern, a shadowing parameter or range variable, the table used
// inside the closure for anything but a mount's receiver, a mount on a foreign
// receiver, and a second route table — assigned to a field, built with new, or
// declared with the ServeMux type. Every source is the real endpoints.go with
// one line injected, so the proof cannot drift from the audit the census runs.
func TestPayloadCensusOpsMountAuditHasTeeth(t *testing.T) {
	const path = "../../internal/observability/endpoints.go"
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	inject := func(t *testing.T, anchor, addition string) []string {
		t.Helper()
		if !bytes.Contains(source, []byte(anchor)) {
			t.Fatalf("%s has no %q to inject beside", path, anchor)
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, bytes.Replace(source, []byte(anchor), []byte(anchor+addition), 1), 0)
		if err != nil {
			t.Fatalf("parse injected %s: %v", path, err)
		}
		return tcPayloadOpsMountAudit(fset, []*ast.File{file})
	}

	t.Run("mount inside register with a literal pattern", func(t *testing.T) {
		findings := inject(t, "\t\tmux.Handle(pattern, h)\n",
			"\t\tmux.HandleFunc(\"GET /foreman/tasks/{taskID}/leak\", func(w http.ResponseWriter, r *http.Request) {})\n")
		if !strings.Contains(strings.Join(findings, "\n"), "/foreman/tasks/{taskID}/leak") {
			t.Errorf("the audit missed the unrecorded mount: %v", findings)
		}
	})

	t.Run("second route table", func(t *testing.T) {
		findings := inject(t, "\tmux := http.NewServeMux()\n", "\taux := new(http.ServeMux)\n")
		if !strings.Contains(strings.Join(findings, "\n"), "2 route tables") {
			t.Errorf("the audit missed the second route table: %v", findings)
		}
	})

	t.Run("second route table declared with the ServeMux type", func(t *testing.T) {
		findings := inject(t, "\tmux := http.NewServeMux()\n", "\tvar aux *http.ServeMux\n")
		if !strings.Contains(strings.Join(findings, "\n"), "2 route tables") {
			t.Errorf("the audit missed the declared second route table: %v", findings)
		}
	})

	t.Run("mount through a receiver alias", func(t *testing.T) {
		findings := inject(t, "\t\tmux.Handle(pattern, h)\n",
			"\t\tm := mux\n\t\tm.HandleFunc(\"GET /foreman/tasks/{taskID}/leak\", func(w http.ResponseWriter, r *http.Request) {})\n")
		if !strings.Contains(strings.Join(findings, "\n"), "/foreman/tasks/{taskID}/leak") {
			t.Errorf("the audit missed the mount through an alias: %v", findings)
		}
	})

	t.Run("mount method escaped as a value", func(t *testing.T) {
		findings := inject(t, "\t\tmux.Handle(pattern, h)\n", "\t\tlate := mux.Handle\n\t\t_ = late\n")
		if !strings.Contains(strings.Join(findings, "\n"), "used as a value") {
			t.Errorf("the audit missed the escaped mount method: %v", findings)
		}
	})

	t.Run("recorded pattern reassigned", func(t *testing.T) {
		findings := inject(t, "\t\tmux.Handle(pattern, h)\n",
			"\t\tpattern = \"GET /foreman/tasks/{taskID}/leak\"\n")
		if !strings.Contains(strings.Join(findings, "\n"), "/foreman/tasks/{taskID}/leak") {
			t.Errorf("the audit missed the reassigned pattern: %v", findings)
		}
	})

	t.Run("mount on a non-identifier receiver", func(t *testing.T) {
		findings := inject(t, "\t\tmux.Handle(pattern, h)\n",
			"\t\thandler.mux.HandleFunc(\"GET /foreman/tasks/{taskID}/leak\", func(w http.ResponseWriter, r *http.Request) {})\n")
		if !strings.Contains(strings.Join(findings, "\n"), "/foreman/tasks/{taskID}/leak") {
			t.Errorf("the audit missed the field-receiver mount: %v", findings)
		}
	})

	t.Run("second route table in a field", func(t *testing.T) {
		findings := inject(t, "\tmux := http.NewServeMux()\n", "\th.mux = http.NewServeMux()\n")
		if !strings.Contains(strings.Join(findings, "\n"), "2 route tables") {
			t.Errorf("the audit missed the field-assigned second table: %v", findings)
		}
	})

	t.Run("table passed out of the register closure", func(t *testing.T) {
		findings := inject(t, "\t\tmux.Handle(pattern, h)\n", "\t\tmountExtra(mux)\n")
		if !strings.Contains(strings.Join(findings, "\n"), "inside the register closure") {
			t.Errorf("the audit missed the table leaving the mount point: %v", findings)
		}
	})

	t.Run("recorded pattern shadowed by a function literal parameter", func(t *testing.T) {
		findings := inject(t, "\t\tmux.Handle(pattern, h)\n",
			"\t\tadd := func(pattern string, h http.Handler) {\n\t\t\tmux.Handle(pattern, h)\n\t\t}\n\t\t_ = add\n")
		if !strings.Contains(strings.Join(findings, "\n"), "shadowed") {
			t.Errorf("the audit missed the shadowed recorded pattern: %v", findings)
		}
	})

	t.Run("route table shadowed by a range variable", func(t *testing.T) {
		findings := inject(t, "\t\tmux.Handle(pattern, h)\n",
			"\t\tfor _, mux := range []*http.ServeMux{nil} {\n\t\t\tmux.Handle(pattern, h)\n\t\t}\n")
		if !strings.Contains(strings.Join(findings, "\n"), "shadowed") {
			t.Errorf("the audit missed the shadowed route table: %v", findings)
		}
	})
}

// tcPayloadObservabilitySource parses every non-test Go file of the
// observability package: the route table set is a package property, so a
// second ServeMux beside endpoints.go must be visible to the audit.
func tcPayloadObservabilitySource(t *testing.T, fset *token.FileSet) []*ast.File {
	t.Helper()
	dir := filepath.Join("..", "..", "internal", "observability")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	files := make([]*ast.File, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		files = append(files, file)
	}
	return files
}

// tcPayloadOpsMountAudit reports every route table and mount the census cannot
// tie to the recorded binding. It resolves names lexically, not by spelling: a
// receiver or pattern name is followed to its declaration, a name introduced
// by a parameter, range variable or short declaration shadows the binding it
// reuses, and anything it cannot prove — a table name used for anything but a
// mount's receiver inside the closure, a non-identifier receiver, an escaped
// mount method, a second table construction — fails closed. It returns
// findings (empty = clean) so the self-proof runs the same audit over an
// injected source.
func tcPayloadOpsMountAudit(fset *token.FileSet, files []*ast.File) []string {
	var findings []string
	at := func(pos token.Pos) string { return fset.Position(pos).String() }
	add := func(format string, args ...any) {
		findings = append(findings, fmt.Sprintf(format, args...))
	}

	// One route table in the package. Every construction counts wherever it
	// appears — NewServeMux, new(http.ServeMux), a ServeMux composite literal,
	// or a variable declared with the ServeMux type — so a second table cannot
	// hide in a field, a struct literal, a return or a slice element.
	var tables []token.Pos
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				if tcPayloadIsServeMuxCall(node) {
					tables = append(tables, node.Pos())
				}
			case *ast.CompositeLit:
				if tcPayloadServeMuxType(node.Type) {
					tables = append(tables, node.Pos())
				}
			case *ast.ValueSpec:
				if len(node.Values) == 0 && node.Type != nil && tcPayloadServeMuxType(node.Type) {
					for _, name := range node.Names {
						tables = append(tables, name.Pos())
					}
				}
			}
			return true
		})
	}
	if len(tables) != 1 {
		positions := make([]string, 0, len(tables))
		for _, pos := range tables {
			positions = append(positions, at(pos))
		}
		sort.Strings(positions)
		add("the observability package declares %d route tables (%s), want one: a second ServeMux is a route set register cannot record",
			len(tables), strings.Join(positions, ", "))
	}
	// The table's names are function-local: only the identifiers inside the
	// function that builds it can be that value. Tracking by name package-wide
	// would read an unrelated `m` or `mux` in another function as the table.
	var owner *ast.FuncDecl
	if len(tables) == 1 {
		for _, file := range files {
			ast.Inspect(file, func(n ast.Node) bool {
				fn, ok := n.(*ast.FuncDecl)
				if ok && fn.Body != nil && tables[0] >= fn.Body.Pos() && tables[0] < fn.Body.End() {
					owner = fn
				}
				return true
			})
		}
		if owner == nil {
			add("the route table at %s is not built inside a function the audit can trace", at(tables[0]))
		}
	}

	// The table's names are the binding of the construction and the local
	// aliases of those names; a write anywhere else breaks that proof.
	tableNames := map[string]bool{}
	bound := map[token.Pos]bool{}
	if len(tables) == 1 && owner != nil {
		target := tables[0]
		ast.Inspect(owner.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.AssignStmt:
				if len(node.Lhs) == 1 && len(node.Rhs) == 1 && tcPayloadValueAt(node.Rhs[0], target) {
					if ident, ok := node.Lhs[0].(*ast.Ident); ok && ident.Name != "_" {
						tableNames[ident.Name] = true
						bound[ident.Pos()] = true
					}
				}
			case *ast.ValueSpec:
				for i, value := range node.Values {
					if i < len(node.Names) && node.Names[i].Name != "_" && tcPayloadValueAt(value, target) {
						tableNames[node.Names[i].Name] = true
						bound[node.Names[i].Pos()] = true
					}
				}
				if len(node.Values) == 0 && node.Type != nil && tcPayloadServeMuxType(node.Type) {
					for _, name := range node.Names {
						if name.Name == "_" {
							continue
						}
						tableNames[name.Name] = true
						bound[name.Pos()] = true
					}
				}
			}
			return true
		})
		for changed := true; changed; {
			changed = false
			ast.Inspect(owner.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.AssignStmt:
					if len(node.Lhs) == 1 && len(node.Rhs) == 1 {
						if rhs, ok := node.Rhs[0].(*ast.Ident); ok && tableNames[rhs.Name] {
							if lhs, ok := node.Lhs[0].(*ast.Ident); ok && lhs.Name != "_" {
								bound[lhs.Pos()] = true
								if !tableNames[lhs.Name] {
									tableNames[lhs.Name] = true
									changed = true
								}
							}
						}
					}
				case *ast.ValueSpec:
					for i, name := range node.Names {
						if name.Name == "_" {
							continue
						}
						if i < len(node.Values) {
							if rhs, ok := node.Values[i].(*ast.Ident); ok && tableNames[rhs.Name] {
								bound[name.Pos()] = true
								tableNames[name.Name] = true
							}
						}
					}
				}
				return true
			})
		}
		ast.Inspect(owner.Body, func(n ast.Node) bool {
			assign, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, lhs := range assign.Lhs {
				ident, ok := lhs.(*ast.Ident)
				if ok && ident.Name != "_" && tableNames[ident.Name] && !bound[ident.Pos()] {
					add("the route table %s is reassigned at %s: the name no longer holds the table the audit proved", ident.Name, at(ident.Pos()))
				}
			}
			return true
		})
	}

	// register is the one closure that records a mount, and its first
	// parameter is the pattern it records.
	var register *ast.FuncLit
	var registerFile *ast.File
	registers := 0
	if owner != nil {
		for _, file := range files {
			ast.Inspect(file, func(n ast.Node) bool {
				assign, ok := n.(*ast.AssignStmt)
				if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
					return true
				}
				ident, ok := assign.Lhs[0].(*ast.Ident)
				if !ok || ident.Name != "register" {
					return true
				}
				fun, ok := assign.Rhs[0].(*ast.FuncLit)
				if !ok {
					return true
				}
				if assign.Pos() < owner.Body.Pos() || assign.Pos() >= owner.Body.End() {
					return true
				}
				registers++
				register = fun
				registerFile = file
				return true
			})
		}
	}
	if registers != 1 {
		add("the observability package declares %d register closures, want one: the single mount point is the invariant", registers)
		return findings
	}
	recorded := ""
	if params := register.Type.Params.List; len(params) > 0 && len(params[0].Names) == 1 {
		recorded = params[0].Names[0].Name
	}
	if recorded == "" {
		add("the register closure at %s has no single named first parameter to bind the mounted pattern to", at(register.Pos()))
	}

	// Bindings are resolved lexically from their declaration sites: the
	// mounted pattern must be the recorded parameter or an alias of it, and
	// the mount receiver must be the route table or an alias of it. A
	// parameter, range variable or short declaration that reuses either name
	// shadows the binding, and a mount that names the shadow passes a value
	// the record never saw.
	recordedPos := token.NoPos
	if recorded != "" {
		recordedPos = register.Type.Params.List[0].Names[0].Pos()
	}
	patternScopes := tcPayloadNewScoped[token.Pos]()
	tableScopes := tcPayloadNewScoped[token.Pos]()
	patternAt := map[token.Pos]token.Pos{}
	tableAt := map[token.Pos]token.Pos{}
	declaredAt := map[token.Pos]bool{}
	if recordedPos.IsValid() {
		patternScopes.declare(recorded, recordedPos)
	}
	if len(tables) == 1 {
		for name := range tableNames {
			tableScopes.declare(name, tables[0])
		}
	}
	declare := func(name string, pos token.Pos, patternBinding token.Pos, what string) {
		declaredAt[pos] = true
		if value, ok := patternScopes.lookup(name); ok && value.IsValid() {
			add("the recorded pattern %s is shadowed at %s by a %s: a mount that names it passes a value the record never saw",
				name, at(pos), what)
			patternBinding = token.NoPos
		}
		patternScopes.declare(name, patternBinding)
		if value, ok := tableScopes.lookup(name); ok && value.IsValid() {
			add("the route table %s is shadowed at %s by a %s: a mount on it would not be the single route table",
				name, at(pos), what)
		}
		tableScopes.declare(name, token.NoPos)
	}
	write := func(name string, pos token.Pos, rhs ast.Expr) {
		if value, ok := patternScopes.lookup(name); ok && value.IsValid() {
			add("the recorded pattern %s is reassigned at %s to %s: the mount would pass a name, not the recorded value",
				name, at(pos), tcPayloadExprString(fset, rhs))
			patternScopes.drop(name)
		}
		if value, ok := tableScopes.lookup(name); ok && value.IsValid() {
			add("the route table %s is reassigned at %s: the name no longer holds the table the audit proved",
				name, at(pos))
			tableScopes.drop(name)
		}
	}

	// Frames open on function literals and on the block-like statements that
	// declare their own names, so a shadow is discarded when its scope ends
	// and a captured binding still resolves to the frame that owns it.
	var openFrames []bool
	ast.Inspect(register.Body, func(n ast.Node) bool {
		if n == nil {
			if openFrames[len(openFrames)-1] {
				patternScopes.pop()
				tableScopes.pop()
			}
			openFrames = openFrames[:len(openFrames)-1]
			return true
		}
		opened := false
		openFrame := func() {
			patternScopes.push()
			tableScopes.push()
			opened = true
		}
		switch node := n.(type) {
		case *ast.FuncLit:
			openFrame()
			fields := node.Type.Params.List
			if node.Type.Results != nil {
				fields = append(fields, node.Type.Results.List...)
			}
			for _, field := range fields {
				for _, name := range field.Names {
					declare(name.Name, name.Pos(), token.NoPos, "function parameter")
				}
			}
		case *ast.RangeStmt:
			openFrame()
			for _, expr := range []ast.Expr{node.Key, node.Value} {
				ident, ok := expr.(*ast.Ident)
				if !ok {
					continue
				}
				if node.Tok == token.DEFINE {
					declare(ident.Name, ident.Pos(), token.NoPos, "range variable")
					continue
				}
				write(ident.Name, ident.Pos(), ident)
			}
		case *ast.BlockStmt, *ast.ForStmt, *ast.IfStmt, *ast.SwitchStmt,
			*ast.TypeSwitchStmt, *ast.SelectStmt, *ast.CaseClause, *ast.CommClause:
			openFrame()
		case *ast.AssignStmt:
			for i, lhs := range node.Lhs {
				ident, ok := lhs.(*ast.Ident)
				if !ok {
					continue
				}
				var rhs ast.Expr
				switch {
				case len(node.Rhs) == len(node.Lhs):
					rhs = node.Rhs[i]
				case len(node.Rhs) == 1:
					rhs = node.Rhs[0]
				}
				if node.Tok != token.DEFINE {
					write(ident.Name, ident.Pos(), rhs)
					continue
				}
				binding := token.NoPos
				if source, ok := rhs.(*ast.Ident); ok {
					if value, ok := patternScopes.lookup(source.Name); ok && value.IsValid() {
						binding = value
					}
				}
				declare(ident.Name, ident.Pos(), binding, "short declaration")
			}
		case *ast.ValueSpec:
			for i, name := range node.Names {
				binding := token.NoPos
				if i < len(node.Values) {
					if source, ok := node.Values[i].(*ast.Ident); ok {
						if value, ok := patternScopes.lookup(source.Name); ok && value.IsValid() {
							binding = value
						}
					}
				}
				declare(name.Name, name.Pos(), binding, "variable declaration")
			}
		}
		if ident, ok := n.(*ast.Ident); ok {
			if value, ok := patternScopes.lookup(ident.Name); ok {
				patternAt[ident.Pos()] = value
			}
			if value, ok := tableScopes.lookup(ident.Name); ok {
				tableAt[ident.Pos()] = value
			}
		}
		openFrames = append(openFrames, opened)
		return true
	})

	// A mount method that is not the function of a call escapes as a value:
	// the audit cannot follow the name it is stored under.
	mountFuns := map[token.Pos]bool{}
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				mountFuns[sel.Pos()] = true
			}
			return true
		})
	}
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || (sel.Sel.Name != "Handle" && sel.Sel.Name != "HandleFunc") {
				return true
			}
			// Handle is also slog.Handler's method; outside the register
			// file only HandleFunc is unambiguously a route mount.
			if file != registerFile && sel.Sel.Name == "Handle" {
				return true
			}
			if !mountFuns[sel.Pos()] {
				add("the mount method %s at %s is used as a value: a route mounted through it would be served but never recorded",
					tcPayloadExprString(fset, sel), at(sel.Pos()))
			}
			return true
		})
	}

	// Inside the register closure the table names may appear only as the
	// receiver of a mount: passing the table to a function, taking a method
	// value or assigning it hands the mux to code the audit cannot follow.
	mountReceiver := map[token.Pos]bool{}
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (sel.Sel.Name != "Handle" && sel.Sel.Name != "HandleFunc") {
				return true
			}
			if ident, ok := sel.X.(*ast.Ident); ok {
				mountReceiver[ident.Pos()] = true
			}
			return true
		})
	}

	// The declaration and the dispatch read are the only table uses allowed
	// outside the register closure.
	allowedUse := map[token.Pos]bool{}
	for pos := range bound {
		allowedUse[pos] = true
	}
	if owner != nil {
		ast.Inspect(owner.Body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if ok && sel.Sel.Name == "ServeHTTP" {
				if ident, ok := sel.X.(*ast.Ident); ok && tableNames[ident.Name] {
					allowedUse[ident.Pos()] = true
				}
			}
			return true
		})
		ast.Inspect(owner.Body, func(n ast.Node) bool {
			ident, ok := n.(*ast.Ident)
			if !ok || !tableNames[ident.Name] {
				return true
			}
			if ident.Pos() >= register.Body.Pos() && ident.Pos() < register.Body.End() {
				if declaredAt[ident.Pos()] {
					return true
				}
				if !mountReceiver[ident.Pos()] {
					add("the route table is used inside the register closure at %s for something other than a mount's receiver: the mux would leave the one mount point", at(ident.Pos()))
				}
				return true
			}
			if !allowedUse[ident.Pos()] {
				add("the route table is used outside the register closure at %s: a route mounted there would be served but never recorded", at(ident.Pos()))
			}
			return true
		})
	}

	mounted := 0
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (sel.Sel.Name != "Handle" && sel.Sel.Name != "HandleFunc") {
				return true
			}
			// Handle is also slog.Handler's method; outside the register
			// file only HandleFunc is unambiguously a route mount.
			if file != registerFile && sel.Sel.Name == "Handle" {
				return true
			}
			pattern := "<no pattern>"
			if len(call.Args) > 0 {
				pattern = tcPayloadExprString(fset, call.Args[0])
			}
			receiver, ok := sel.X.(*ast.Ident)
			inOwner := owner != nil && call.Pos() >= owner.Body.Pos() && call.Pos() < owner.Body.End()
			if !ok || !inOwner || !tableNames[receiver.Name] {
				add("the mount of %s at %s is on %s, which the audit cannot prove is the single route table: the route would be served but never recorded",
					pattern, at(call.Pos()), tcPayloadExprString(fset, sel.X))
				return true
			}
			if call.Pos() < register.Body.Pos() || call.Pos() >= register.Body.End() {
				add("the mount of %s at %s is outside the register closure: the route would be served but never recorded", pattern, at(call.Pos()))
				return true
			}
			mounted++
			if value, ok := tableAt[receiver.Pos()]; !ok || !value.IsValid() {
				add("the mount of %s at %s is on %s, which a declaration inside the register closure shadows: the route would be served by a value the audit never bound to the table",
					pattern, at(call.Pos()), tcPayloadExprString(fset, sel.X))
				return true
			}
			if len(call.Args) == 0 {
				add("the mount at %s passes no pattern: the route would be served but never listed", at(call.Pos()))
				return true
			}
			arg, ok := call.Args[0].(*ast.Ident)
			if !ok {
				add("the mount of %s at %s does not pass the pattern register records: the route would be served but never listed",
					pattern, at(call.Pos()))
				return true
			}
			if value, ok := patternAt[arg.Pos()]; !ok || !value.IsValid() {
				add("the mount of %s at %s does not pass the pattern register records: the route would be served but never listed",
					pattern, at(call.Pos()))
			}
			return true
		})
	}
	if mounted == 0 {
		add("the register closure mounts nothing: the declared single mount point is not the mount point")
	}
	return findings
}

// tcPayloadValueAt reports whether expr is the construction at pos, directly
// or through a unary or parenthesised wrapper.
func tcPayloadValueAt(expr ast.Expr, pos token.Pos) bool {
	if expr == nil {
		return false
	}
	if expr.Pos() == pos {
		return true
	}
	switch value := expr.(type) {
	case *ast.UnaryExpr:
		return tcPayloadValueAt(value.X, pos)
	case *ast.ParenExpr:
		return tcPayloadValueAt(value.X, pos)
	}
	return false
}

// tcPayloadIsServeMuxCall reports whether call builds a ServeMux.
func tcPayloadIsServeMuxCall(call *ast.CallExpr) bool {
	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		return fun.Sel.Name == "NewServeMux"
	case *ast.Ident:
		return fun.Name == "new" && len(call.Args) == 1 && tcPayloadServeMuxType(call.Args[0])
	}
	return false
}

// tcPayloadExprString renders an expression for a failure message.
func tcPayloadExprString(fset *token.FileSet, expr ast.Expr) string {
	var buf bytes.Buffer
	if err := printer.Fprint(&buf, fset, expr); err != nil {
		return "<unprintable>"
	}
	return buf.String()
}

// tcPayloadServeMuxType reports whether expr names the ServeMux type.
func tcPayloadServeMuxType(expr ast.Expr) bool {
	switch value := expr.(type) {
	case *ast.SelectorExpr:
		return value.Sel.Name == "ServeMux"
	case *ast.StarExpr:
		return tcPayloadServeMuxType(value.X)
	}
	return false
}

// TestPayloadCensusStructKeySets pins the two JSON faces by type: the
// projection may not grow a key, and the snapshot is the one struct that
// declares the body — the census says so rather than leaving it implicit.
func TestPayloadCensusStructKeySets(t *testing.T) {
	projection, hidden := tcPayloadJSONKeys(t, reflect.TypeFor[registry.TaskEntry]())
	tcPayloadSameSet(t, "the projection type", projection, tcPayloadProjectionKeys)
	for _, field := range []string{
		"Payload", "DecidedReport", "TokenHash", "PriorSessionID", "AgentName",
		"ProjectID", "ProjectSource", "RealRuntimeID", "RuntimeIdentity",
		"JobPhase", "DrainDeadline", "DeliveryDeadline", "CompletedAt",
	} {
		if !slices.Contains(hidden, field) {
			t.Errorf("registry.TaskEntry.%s is not json:\"-\": a payload-adjacent field would reach the projection", field)
		}
	}

	snapshot, snapshotHidden := tcPayloadJSONKeys(t, reflect.TypeFor[state.Snapshot]())
	tcPayloadSameSet(t, "the shared-state snapshot type", snapshot, tcPayloadSnapshotKeys)
	if len(snapshotHidden) != 0 {
		t.Errorf("the snapshot hides %v: the shared state face is where the design keeps the body, not a place to drop it", snapshotHidden)
	}
	// The two bodies are the design's (§2.5/§4.3/§10, ADR-046 决策 0). The
	// digest is not: §6.1 keeps the token hash in the revocation set and
	// ADR-017 makes OnClaim its only writer, so token_hash is pinned by the
	// key set above as the snapshot's shape, never required here.
	for _, key := range []string{"payload", "decided_report"} {
		if !slices.Contains(snapshot, key) {
			t.Errorf("the snapshot no longer carries %q (§2.5/§4.3/§10: the body lives with its entry)", key)
		}
	}
}

// TestPayloadCensusSharedSnapshotIsTheOnlyPersistedBody drives a real claim
// against a Redis-backed shared state face and scans the raw keys: inside the
// shared state the entry snapshot is the one face that carries the delivery
// and verdict bodies, and it must.
func TestPayloadCensusSharedSnapshotIsTheOnlyPersistedBody(t *testing.T) {
	store, mr := multi08Store(t)
	h := newHarnessWithSharedState(t, false, store)
	h.useRealBuilder(t)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	if err := h.sched.OnClaim(t.Context(), tcPayloadCensusPayload(tcPayloadTaskID, tcPayloadCensusIssueTrigger)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, tcPayloadTaskID)
	registerTaskDaemon(t, h, tcPayloadTaskID, token)
	claimTask(t, h, tcPayloadTaskID, token)
	// The verdict registers the decided body, so the snapshot's whole key set
	// (decided_report included) is observable in one entry.
	if code, body := h.call("POST", "/api/daemon/tasks/"+tcPayloadTaskID+"/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	report := tcPayloadVerdictBody()
	if code, body := h.call("POST", "/api/daemon/tasks/"+tcPayloadTaskID+"/complete", token, report); code != http.StatusOK {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}
	waitTerminal(t, h, tcPayloadTaskID)
	multi08Flush(t, h)

	entry, ok := h.reg.Get(tcPayloadTaskID)
	if !ok || len(entry.Payload) == 0 {
		t.Fatalf("entry %s carries no delivery body: %+v", tcPayloadTaskID, entry)
	}
	if !bytes.Equal(entry.DecidedReport, report) {
		t.Fatalf("entry decided report = %s, want the forwarded S13 body %s", entry.DecidedReport, report)
	}

	// The delivery body is raw JSON in the snapshot; the verdict body is a
	// []byte field, which json encodes as base64. Both forms are scanned: a
	// plaintext marker outside the entry key, or the encoded verdict body
	// outside it, is a body on an unregistered face.
	encodedVerdict := base64.StdEncoding.EncodeToString(report)
	entryKey := ""
	carriedDelivery, carriedVerdict := false, false
	for _, key := range mr.Keys() {
		value, err := mr.Get(key)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		isEntry := strings.HasPrefix(key, "foreman:entry:")
		for _, needle := range []string{tcPayloadMarker, tcPayloadVerdictMarker} {
			if strings.Contains(value, needle) && !isEntry {
				t.Errorf("%s carries %q: the shared state's entry snapshot is the only persisted body face", key, needle)
			}
		}
		if strings.Contains(value, tcPayloadMarker) {
			carriedDelivery = true
			entryKey = key
		}
		if strings.Contains(value, encodedVerdict) {
			if !isEntry {
				t.Errorf("%s carries the verdict body: the entry snapshot is its only persisted face", key)
				continue
			}
			carriedVerdict = true
		}
	}
	if !carriedDelivery {
		t.Fatal("no persisted key carries the delivery body: a restart could no longer deliver (§2.5)")
	}
	if !carriedVerdict {
		t.Fatal("no persisted key carries the verdict body: a restart could no longer replay it (§4.3)")
	}
	if entryKey == "" {
		t.Fatal("the entry snapshot did not carry the delivery body")
	}

	raw, err := mr.Get(entryKey)
	if err != nil {
		t.Fatalf("read %s: %v", entryKey, err)
	}
	tcPayloadSameSet(t, "the entry snapshot", tcPayloadObjectKeys(t, []byte(raw)), tcPayloadSnapshotKeys)
	var snapshot struct {
		Payload       json.RawMessage `json:"payload"`
		DecidedReport []byte          `json:"decided_report"`
	}
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		t.Fatalf("decode snapshot: %v (%s)", err, raw)
	}
	if !bytes.Equal(snapshot.Payload, entry.Payload) {
		t.Errorf("snapshot payload = %s, want the delivery body %s", snapshot.Payload, entry.Payload)
	}
	if !bytes.Equal(snapshot.DecidedReport, report) {
		t.Errorf("snapshot decided_report = %s, want the verdict body %s", snapshot.DecidedReport, report)
	}
}

// TestPayloadCensusCarrierIsTheDeclaredVerdictFace drives a terminal report
// the upstream refuses, so the report stays queued and its carrier Secret is
// observable: the carrier is the second design-registered face that carries
// the verdict body (§3, §4.6), and its key set, label set and content are
// judged like every other face.
func TestPayloadCensusCarrierIsTheDeclaredVerdictFace(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	if err := h.sched.OnClaim(t.Context(), tcPayloadCensusPayload(tcPayloadTaskID, tcPayloadCensusIssueTrigger)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, tcPayloadTaskID)
	registerTaskDaemon(t, h, tcPayloadTaskID, token)
	claimTask(t, h, tcPayloadTaskID, token)
	if code, body := h.call("POST", "/api/daemon/tasks/"+tcPayloadTaskID+"/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	// The queue is write-ahead (§10): a report the upstream refuses stays on
	// its carrier instead of being lost.
	h.server.setForwardCode(http.StatusServiceUnavailable)
	report := tcPayloadVerdictBody()
	if code, body := h.call("POST", "/api/daemon/tasks/"+tcPayloadTaskID+"/complete", token, report); code != http.StatusBadGateway {
		t.Fatalf("S13 with a down upstream = %d (%s), want 502", code, body)
	}

	carriers := h.jobs.carrierSecrets()
	if len(carriers) != 1 {
		t.Fatalf("carriers = %d, want the one queued report", len(carriers))
	}
	carrier := carriers[0]
	keys := make([]string, 0, len(carrier.Data))
	for key := range carrier.Data {
		keys = append(keys, key)
	}
	tcPayloadSameSet(t, "the pending-report carrier", keys, tcPayloadCarrierKeys)
	// §6.5: the carrier label face is the locator (role + managed-by + task id
	// + workspace), so it is judged both ways: a missing required label is as
	// much a failure as an undeclared one.
	tcPayloadSameSet(t, "the pending-report carrier labels", tcPayloadMapKeys(carrier.Labels), tcPayloadCarrierLabelKeys)
	if !bytes.Equal(carrier.Data["body"], report) {
		t.Errorf("carrier body = %s, want the enqueued verdict body %s", carrier.Data["body"], report)
	}

	// The carrier is a verdict face, never a delivery face.
	raw := tcPayloadSecretScan(t, carrier)
	if strings.Contains(raw, tcPayloadMarker) {
		t.Errorf("the carrier carries the delivery body: %s", raw)
	}
	if !strings.Contains(raw, tcPayloadVerdictMarker) {
		t.Errorf("the carrier does not carry the verdict body: %s", raw)
	}

	// The mapping record keeps the same verdict body (§4.3); the carrier adds
	// no second decision.
	entry, ok := h.reg.Get(tcPayloadTaskID)
	if !ok || !bytes.Equal(entry.DecidedReport, report) {
		t.Fatalf("entry decided report = %s, want %s", entry.DecidedReport, report)
	}
}

// TestPayloadCensusJobWriteFaceIsTheDeclaredKeySet renders the production
// template once per trigger shape, requires the rendered Job, pod and Secret
// key sets to be declared, and scans the whole objects for a payload byte or a
// task credential.
func TestPayloadCensusJobWriteFaceIsTheDeclaredKeySet(t *testing.T) {
	for i, trigger := range tcPayloadCensusTriggers {
		t.Run(trigger.name, func(t *testing.T) {
			tcPayloadCensusJobFace(t, trigger, fmt.Sprintf("task-%d", i+1))
		})
	}
}

func tcPayloadCensusJobFace(t *testing.T, trigger tcPayloadCensusTrigger, taskID string) {
	t.Helper()
	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	if err := h.sched.OnClaim(t.Context(), tcPayloadCensusPayload(taskID, trigger.extra)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	job := h.createdJob(t, "fm-"+taskID)
	secret := h.createdSecret(t, "fm-"+taskID+"-cred")

	// The rendered key set is judged as a whole: every key this render writes
	// must be declared, whatever prefix it carries. Template-owned keys are
	// part of the reference render's shape, not a hole in the census.
	tcPayloadSubset(t, "the Job annotations", tcPayloadMapKeys(job.Annotations), tcPayloadJobAnnotationKeys)
	tcPayloadSubset(t, "the Job labels", tcPayloadMapKeys(job.Labels), tcPayloadJobLabelKeys)
	tcPayloadSubset(t, "the pod-template labels", tcPayloadMapKeys(job.Spec.Template.Labels), tcPayloadPodLabelKeys)
	tcPayloadSubset(t, "the pod-template annotations", tcPayloadMapKeys(job.Spec.Template.Annotations), tcPayloadPodAnnotationKeys)
	// The declared keys the design requires are actually written (§4.3): the
	// trigger block is conditional, the identity block is not.
	tcPayloadSubset(t, "the required Job annotations", tcPayloadRequiredAnnotationKeys, tcPayloadMapKeys(job.Annotations))
	tcPayloadSubset(t, "the trigger Job annotations", trigger.keys, tcPayloadMapKeys(job.Annotations))

	// The credential Secret is the Job Token and nothing else.
	keys := make([]string, 0, len(secret.StringData)+len(secret.Data))
	for key := range secret.StringData {
		keys = append(keys, key)
	}
	for key := range secret.Data {
		keys = append(keys, key)
	}
	tcPayloadSameSet(t, "the credential Secret", keys, tcPayloadSecretKeys)
	config := secret.StringData["config.json"]
	if config == "" {
		config = string(secret.Data["config.json"])
	}
	var credential struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(config), &credential); err != nil || credential.Token == "" {
		t.Fatalf("credential Secret = %q, want {token:…}", config)
	}

	// The whole rendered object is scanned, not a hand-picked field list: a
	// payload byte in args, command, volumes, resources or anything else the
	// template grows is caught by the same marshal. The Secret is scanned by
	// value (json.Marshal base64-encodes Data) plus its metadata.
	for label, raw := range map[string]string{
		"the Job":               tcPayloadMarshal(t, job),
		"the credential Secret": tcPayloadSecretScan(t, *secret),
	} {
		for _, needle := range []string{tcPayloadMarker, "mat_"} {
			if strings.Contains(raw, needle) {
				t.Errorf("%s carries %q: %s", label, needle, raw)
			}
		}
	}
}

// tcPayloadSubset fails when got holds an entry want does not declare, naming
// it: an undeclared write is the failure the census exists for.
func tcPayloadSubset(t *testing.T, label string, got, want []string) {
	t.Helper()
	if extra := tcPayloadDiff(got, want); len(extra) != 0 {
		t.Errorf("%s carries undeclared keys: %v", label, extra)
	}
}

// tcPayloadMarshal serializes one object for the byte scan.
func tcPayloadMarshal(t *testing.T, object any) string {
	t.Helper()
	raw, err := json.Marshal(object)
	if err != nil {
		t.Fatalf("marshal %T: %v", object, err)
	}
	return string(raw)
}

// tcPayloadSecretScan returns every byte a Secret carries: the data values
// verbatim (json.Marshal would base64 them), the string data and the metadata.
func tcPayloadSecretScan(t *testing.T, secret corev1.Secret) string {
	t.Helper()
	var buf bytes.Buffer
	for key, value := range secret.Data {
		buf.WriteString(key)
		buf.Write(value)
	}
	for key, value := range secret.StringData {
		buf.WriteString(key)
		buf.WriteString(value)
	}
	meta, err := json.Marshal(secret.ObjectMeta)
	if err != nil {
		t.Fatalf("marshal Secret metadata: %v", err)
	}
	buf.Write(meta)
	return buf.String()
}

// tcPayloadMapKeys returns a map's key set.
func tcPayloadMapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// TestPayloadCensusLogVocabulary checks the log face on two sides. The source
// scan is a one-sided defect finder: a key it reads outside the vocabulary
// fails, while its silence is not qualifying evidence (ADR-055 §6). The driven
// lifecycle requires every key this scenario observes to be declared and
// neither marker to appear; a miss does not prove absence, so the log/metric
// face's qualifying evidence stays with TC-tech-forbidden-09.
func TestPayloadCensusLogVocabulary(t *testing.T) {
	tcPayloadSameSet(t, "the log vocabulary", tcPayloadLogSourceKeys(t), tcPayloadLogKeys)

	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	if err := h.sched.OnClaim(t.Context(), tcPayloadCensusPayload(tcPayloadTaskID, tcPayloadCensusIssueTrigger)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, tcPayloadTaskID)
	registerTaskDaemon(t, h, tcPayloadTaskID, token)
	claimTask(t, h, tcPayloadTaskID, token)
	if code, body := h.call("POST", "/api/daemon/tasks/"+tcPayloadTaskID+"/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	if code, body := h.call("POST", "/api/daemon/tasks/"+tcPayloadTaskID+"/complete", token, tcPayloadVerdictBody()); code != http.StatusOK {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}
	waitTerminal(t, h, tcPayloadTaskID)

	logs := h.logs.String() + h.handlerLogs.String()
	for _, marker := range []string{tcPayloadMarker, tcPayloadVerdictMarker} {
		if strings.Contains(logs, marker) {
			t.Errorf("a log line carries %q: %s", marker, tcPayloadLogLines(logs, marker))
		}
	}
	if strings.Contains(logs, "auth_token") {
		t.Errorf("a log line carries the payload field name auth_token: %s", tcPayloadLogLines(logs, "auth_token"))
	}

	observed := map[string]bool{}
	for _, line := range strings.Split(logs, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			t.Fatalf("log line is not JSON: %s", line)
		}
		for key := range fields {
			observed[key] = true
		}
	}
	if len(observed) == 0 {
		t.Fatal("the lifecycle emitted no log line")
	}
	keys := make([]string, 0, len(observed))
	for key := range observed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	undeclared := tcPayloadDiff(keys, tcPayloadLogKeys)
	if len(undeclared) != 0 {
		t.Errorf("log attributes outside the declared vocabulary: %v", undeclared)
	}
}

// tcPayloadLogEnvelope is what the JSON handler writes around every record:
// the fixed fields of §9.1 plus the plain handler's time key.
var tcPayloadLogEnvelope = []string{"component", "level", "msg", "time", "ts"}

// tcPayloadLogCalls maps a log method to the argument index its key/value
// pairs start at. LogAttrs carries slog.Attr values, whose keys the attribute
// constructors below already cover.
var tcPayloadLogCalls = map[string]int{
	"Info": 1, "Warn": 1, "Error": 1, "Debug": 1,
	"InfoContext": 2, "WarnContext": 2, "ErrorContext": 2, "DebugContext": 2,
	"Log": 3, "LogContext": 3,
}

// tcPayloadAttrCtors are the slog attribute constructors whose first argument
// is the key.
var tcPayloadAttrCtors = map[string]bool{
	"String": true, "Int": true, "Int64": true, "Uint64": true,
	"Float64": true, "Bool": true, "Duration": true, "Time": true,
	"Any": true, "Group": true,
}

// tcPayloadLogSourceKeys reads the production tree back and returns the
// attribute keys a log call reads. The scan is a defect finder: an undeclared
// literal key on a path this suite never drives (the node collector, the skill
// cache, the client's retry face) still fails, but a clean scan proves nothing
// about the paths it cannot read (ADR-055 §6).
func tcPayloadLogSourceKeys(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	seen := map[string]bool{}
	used := map[string]bool{}
	for _, root := range []string{
		filepath.Join("..", "..", "internal"),
		filepath.Join("..", "..", "cmd"),
	} {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			tcPayloadScanLogKeys(t, fset, file, seen, used)
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", root, err)
		}
	}
	for symbol := range tcPayloadForwardedParams {
		if !used[symbol] {
			t.Fatalf("tcPayloadForwardedParams declares %s, which the scan never met: a stale exemption would let a new key through", symbol)
		}
	}
	for symbol := range tcPayloadAttrKeySlots {
		if !used[symbol] {
			t.Fatalf("tcPayloadAttrKeySlots declares %s, which the scan never met: a stale exemption would let a dynamic key through", symbol)
		}
	}
	for _, key := range tcPayloadLogEnvelope {
		seen[key] = true
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// tcPayloadForwardedParams declares the parameters that legitimately re-emit a
// key this scan cannot judge at a call site. slog.Handler's WithGroup is called
// by the logging framework, not by this tree, so the redaction handler passes
// the framework's group name through without owning a key. Every other
// parameter reaching a key position introduces a key behind the caller's back:
// the scan fails instead of trusting a call site it never reads.
var tcPayloadForwardedParams = map[string]bool{
	"observability.redactHandler.WithGroup.name": true,
}

// tcPayloadScoped is a stack of lexical frames for names the scan tracks. A
// name resolves to the innermost frame that declares it, so a closure's own
// variable shadows the captured variable of the same spelling instead of
// overwriting its state, while a write through a captured name updates the
// frame that owns it. A frame pops when its block or function ends, so a name
// introduced inside a closure cannot outlive it.
type tcPayloadScoped[T any] struct {
	frames []map[string]T
}

// tcPayloadNewScoped starts a tracker with the file's package-level frame.
func tcPayloadNewScoped[T any]() *tcPayloadScoped[T] {
	return &tcPayloadScoped[T]{frames: []map[string]T{{}}}
}

// push opens one lexical frame: a block, a function, or a function literal.
func (s *tcPayloadScoped[T]) push() {
	s.frames = append(s.frames, map[string]T{})
}

// pop closes the innermost frame, discarding every declaration it holds.
func (s *tcPayloadScoped[T]) pop() {
	s.frames = s.frames[:len(s.frames)-1]
}

// lookup resolves a name from the innermost frame outwards.
func (s *tcPayloadScoped[T]) lookup(name string) (T, bool) {
	for i := len(s.frames) - 1; i >= 0; i-- {
		if value, ok := s.frames[i][name]; ok {
			return value, true
		}
	}
	var zero T
	return zero, false
}

// declare introduces or refreshes a name in the innermost frame (:= and var).
func (s *tcPayloadScoped[T]) declare(name string, value T) {
	s.frames[len(s.frames)-1][name] = value
}

// assign updates the frame that owns the name, because a plain = writes the
// variable declared there — an outer or captured one included. A name no frame
// owns can only be package-level, so it lands in the root frame.
func (s *tcPayloadScoped[T]) assign(name string, value T) {
	for i := len(s.frames) - 1; i >= 0; i-- {
		if _, ok := s.frames[i][name]; ok {
			s.frames[i][name] = value
			return
		}
	}
	s.frames[0][name] = value
}

// drop stops tracking the frame that owns the name; a shadowing inner binding
// leaves the outer one alone.
func (s *tcPayloadScoped[T]) drop(name string) {
	for i := len(s.frames) - 1; i >= 0; i-- {
		if _, ok := s.frames[i][name]; ok {
			var zero T
			s.frames[i][name] = zero
			return
		}
	}
}

// dropAll stops tracking every visible name: a write the scan cannot attribute
// might land in any of their backing arrays.
func (s *tcPayloadScoped[T]) dropAll() {
	var zero T
	for _, frame := range s.frames {
		for name := range frame {
			frame[name] = zero
		}
	}
}

// tcPayloadScanLogKeys collects one file's log attribute keys. Every key
// position must hold a string literal: an unreadable key fails the scan
// instead of being skipped, because a skipped key is exactly how an
// unregistered write face would stay invisible. The only non-literal keys
// accepted re-emit a key this scan judges elsewhere: a parameter declared in
// tcPayloadForwardedParams, and the .Key of a declaration registered in
// tcPayloadAttrKeySlots.
//
// The scan keeps one invariant over the key/value slices it tracks: a name is
// tracked only while every write that can reach its backing array is either
// judged (a constant even index, an append of pairs, the spread itself) or
// ends the tracking. A value escapes at two points — a write and a call — and
// both apply the same rule: a form the scan cannot attribute drops the names it
// mentions instead of assuming they are unrelated. A closure that captures a
// slice, a slice expression naming the same array, and an alias are all
// instances of that rule, not cases of their own: there is no branch that
// leaves a name tracked over a write this scan never read.
func tcPayloadScanLogKeys(t *testing.T, fset *token.FileSet, file *ast.File, seen, used map[string]bool) {
	t.Helper()
	slogName := tcPayloadSlogImport(file)
	types := tcPayloadFileTypes(file)
	scopes := tcPayloadFuncScopes(file, slogName, types)
	// states tracks identifiers carrying a []any key/value slice, one lexical
	// frame per block and function. It is rebuilt in source order, so a
	// reassignment drops the name again: a name that once held judged fields
	// must not keep a later, unjudged value inside the log face.
	states := tcPayloadNewScoped[bool]()
	// attrStates tracks identifiers whose static type is slog.Attr, in the same
	// lexical frames. Writing .Key on one renames an emitted field; writing
	// .Key on anything else (a config struct, a chat record) is not a log key,
	// and the type — not the file's imports — is what tells them apart.
	attrStates := tcPayloadNewScoped[tcPayloadAttrClass]()
	var walked []ast.Node
	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			node := walked[len(walked)-1]
			walked = walked[:len(walked)-1]
			switch node := node.(type) {
			case *ast.FuncDecl, *ast.FuncLit, *ast.BlockStmt:
				// A block's names end with it. A closure therefore cannot
				// leave a name it declared visible outside, and a local that
				// shadows an outer name cannot overwrite the outer state: the
				// write resolves to the frame that owns it.
				states.pop()
				attrStates.pop()
			case *ast.AssignStmt:
				tcPayloadTrackAssign(t, fset, node, slogName, scopes, seen, used, states, attrStates, types)
			case *ast.ValueSpec:
				tcPayloadTrackDecl(states, node)
				tcPayloadTrackAttrDecl(scopes, attrStates, node, slogName, types)
			}
			return true
		}
		walked = append(walked, n)
		switch node := n.(type) {
		case *ast.FuncDecl, *ast.FuncLit, *ast.BlockStmt:
			// A closure shares the enclosing function's variables, so a write
			// through a captured name resolves to the outer frame and is
			// judged there. Its own declarations live in its frames and end
			// with them; nothing is inherited by reference.
			states.push()
			attrStates.push()
		case *ast.CallExpr:
			tcPayloadJudgeCall(t, fset, node, scopes, slogName, seen, used, states)
		case *ast.CompositeLit:
			switch {
			case tcPayloadFieldsLiteral(node):
				// A key/value slice spread into a log call as fields...
				// (fields := []any{"key", v, slog.String("nested", w)};
				// log.Warn(msg, fields...)). slog reads it exactly like a
				// call's argument list, so it goes through the same judge.
				tcPayloadJudgeSlots(t, fset, node.Elts, scopes, slogName, seen, used)
			case tcPayloadAttrLiteral(node, slogName):
				tcPayloadJudgeAttrLiteral(t, fset, node, scopes, seen, used)
			}
		}
		return true
	})
}

// tcPayloadJudgeCall judges one call's key positions: the log methods, the
// logger's With and WithGroup, the slog attribute constructors, a slog.Attr
// literal, and an append of key/value pairs. Every other call is opaque: a
// tracked slice handed to it can be written by the callee, so that slice
// stops being tracked rather than keeping a value this scan never read.
func tcPayloadJudgeCall(t *testing.T, fset *token.FileSet, call *ast.CallExpr, scopes []tcPayloadScope, slogName string, seen, used map[string]bool, fields *tcPayloadScoped[bool]) {
	t.Helper()
	if id, ok := call.Fun.(*ast.Ident); ok {
		if id.Name == "append" {
			tcPayloadJudgeAppend(t, fset, call, scopes, seen, used, fields)
			return
		}
		tcPayloadDropTrackedArgs(call, fields)
		return
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		tcPayloadDropTrackedArgs(call, fields)
		return
	}
	name := sel.Sel.Name
	if start, isLog := tcPayloadLogCalls[name]; isLog {
		// A zero-argument method of the same name is not a log call
		// (error.Error is the common one); it cannot carry a key either.
		if len(call.Args) <= start {
			return
		}
		tcPayloadJudgeArgs(t, fset, call, call.Args[start:], scopes, slogName, seen, used, fields)
		return
	}
	switch {
	case name == "With":
		tcPayloadJudgeArgs(t, fset, call, call.Args, scopes, slogName, seen, used, fields)
	case name == "WithGroup":
		// WithGroup takes one group name. Any other arity cannot be placed,
		// so it fails rather than dropping the argument from the census.
		if len(call.Args) != 1 || call.Ellipsis.IsValid() {
			t.Fatalf("%s: WithGroup takes one group name, got %d arguments",
				fset.Position(call.Pos()), len(call.Args))
		}
		tcPayloadRecordKey(t, fset, call.Pos(), scopes, seen, used, call.Args[0])
	case tcPayloadAttrCtors[name] && tcPayloadSlogPkg(sel, slogName):
		if len(call.Args) == 0 {
			return
		}
		tcPayloadRecordKey(t, fset, call.Pos(), scopes, seen, used, call.Args[0])
		if name == "Group" {
			// Group's key/value pairs sit in Args[1:], at the same positions
			// a log call takes them; leaving them unread would drop every
			// key nested under a group.
			tcPayloadJudgeArgs(t, fset, call, call.Args[1:], scopes, slogName, seen, used, fields)
		}
	default:
		// A method this scan does not read is as opaque as a plain call: the
		// callee can write into a tracked slice it is handed.
		tcPayloadDropTrackedArgs(call, fields)
	}
}

// tcPayloadJudgeArgs judges one variadic key/value argument list the way slog
// reads it: a slog.Attr element takes one slot and carries its own key, every
// other element pairs a key with a value. A trailing key and an unjudgeable
// spread both fail.
func tcPayloadJudgeArgs(t *testing.T, fset *token.FileSet, call *ast.CallExpr, args []ast.Expr, scopes []tcPayloadScope, slogName string, seen, used map[string]bool, fields *tcPayloadScoped[bool]) {
	t.Helper()
	var spread ast.Expr
	if call.Ellipsis.IsValid() && len(args) > 0 {
		spread, args = args[len(args)-1], args[:len(args)-1]
	}
	tcPayloadJudgeSlots(t, fset, args, scopes, slogName, seen, used)
	if spread != nil {
		// A spread slice is judged from the slice itself: its keys were
		// collected where it was built, and an untracked slice would carry
		// keys this scan cannot see.
		tcPayloadTrackedFields(t, fset, spread, fields)
	}
}

// tcPayloadJudgeSlots judges one slog element list — a call's arguments or a
// []any literal read as fields — the way slog reads it: a slog.Attr element
// takes one slot and carries its own key, every other element pairs a key
// with a value. A trailing key fails. Only the slot rule is implemented
// here; the []any branch must not fork a second reading of the same list.
func tcPayloadJudgeSlots(t *testing.T, fset *token.FileSet, args []ast.Expr, scopes []tcPayloadScope, slogName string, seen, used map[string]bool) {
	t.Helper()
	for i := 0; i < len(args); {
		if tcPayloadAttrExpr(args[i], slogName) {
			i++
			continue
		}
		if i+1 >= len(args) {
			t.Fatalf("%s: key/value elements carry a dangling key with no value",
				fset.Position(args[i].Pos()))
		}
		tcPayloadRecordKey(t, fset, args[i].Pos(), scopes, seen, used, args[i])
		i += 2
	}
}

// tcPayloadJudgeAttrLiteral judges a slog.Attr literal: the Attr carries its
// own key wherever it is passed, so the Key field is a key position in its own
// right and a placeholder key next to it is dropped by slog.
func tcPayloadJudgeAttrLiteral(t *testing.T, fset *token.FileSet, lit *ast.CompositeLit, scopes []tcPayloadScope, seen, used map[string]bool) {
	t.Helper()
	for i, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			// A positional literal puts the key first.
			if i == 0 {
				tcPayloadRecordKey(t, fset, lit.Pos(), scopes, seen, used, elt)
			}
			continue
		}
		if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Key" {
			tcPayloadRecordKey(t, fset, lit.Pos(), scopes, seen, used, kv.Value)
		}
	}
}

// tcPayloadJudgeAppend judges an append carrying key/value pairs. A tracked
// target is read pair by pair: the scan keeps the invariant that such a slice
// holds complete pairs, so a single appended element — whose slot depends on a
// length the scan does not track — drops the name instead, and a dangling
// multi-element append fails. A target this scan does not track fails as soon
// as an appended element takes a key position, because those pairs would enter
// a slice the vocabulary never sees. Appends of plain values to other slices
// carry no key and are left alone.
func tcPayloadJudgeAppend(t *testing.T, fset *token.FileSet, call *ast.CallExpr, scopes []tcPayloadScope, seen, used map[string]bool, fields *tcPayloadScoped[bool]) {
	t.Helper()
	if len(call.Args) == 0 {
		return
	}
	target, ok := call.Args[0].(*ast.Ident)
	isTracked := false
	if ok {
		tracked, found := fields.lookup(target.Name)
		isTracked = found && tracked
	}
	if call.Ellipsis.IsValid() {
		if isTracked {
			if len(call.Args) != 2 {
				t.Fatalf("%s: append of %d arguments with a spread cannot be judged",
					fset.Position(call.Pos()), len(call.Args))
			}
			tcPayloadTrackedFields(t, fset, call.Args[1], fields)
		}
		return
	}
	if isTracked {
		// A tracked slice holds complete pairs, so its length is even and a
		// multi-element append lands on a key slot first. One element is
		// different: it lands on a key slot only while the current length is
		// even, and this scan does not track that length. The name therefore
		// stops being tracked rather than letting an unjudged key reach a
		// spread; it does not return silently.
		if len(call.Args) == 2 {
			fields.drop(target.Name)
			return
		}
		if (len(call.Args)-1)%2 != 0 {
			t.Fatalf("%s: append to %s carries a dangling key with no value",
				fset.Position(call.Pos()), target.Name)
		}
		for i := 1; i < len(call.Args); i += 2 {
			tcPayloadRecordKey(t, fset, call.Pos(), scopes, seen, used, call.Args[i])
		}
		return
	}
	if len(call.Args) < 3 {
		// append(s) and append(s, v) add no key/value pair to a slice the
		// scan does not track.
		return
	}
	for i := 1; i < len(call.Args); i += 2 {
		if _, literal := tcPayloadLiteral(call.Args[i]); literal {
			t.Fatalf("%s: append of a key/value pair to %s, a slice this scan does not track",
				fset.Position(call.Pos()), tcPayloadExprName(call.Args[0]))
		}
	}
}

// tcPayloadTrackAssign rebuilds the fields tracking of one assignment after
// its right-hand side has been judged: a name keeps tracking only while it
// holds a key/value slice this scan reads, and a write into such a slice has
// exactly two outcomes — a key position is judged, anything else drops the
// slice from tracking. There is no silent third exit: an element this scan
// never read must not reach a spread argument. An assignment to an attribute's
// Key renames the emitted field, so its right-hand side is a key position. The
// receiver's static class decides: a slog.Attr is judged, a value provably not
// one (a plain struct's Key field) is left alone, and a receiver the scan
// cannot classify fails rather than passing unjudged.
func tcPayloadTrackAssign(t *testing.T, fset *token.FileSet, node *ast.AssignStmt, slogName string, scopes []tcPayloadScope, seen, used map[string]bool, fields *tcPayloadScoped[bool], attrs *tcPayloadScoped[tcPayloadAttrClass], types tcPayloadTypeIndex) {
	t.Helper()
	for i, lhs := range node.Lhs {
		switch target := lhs.(type) {
		case *ast.Ident:
			if target.Name == "_" {
				continue
			}
			rhs := tcPayloadAssignRHS(node, i)
			tracked := tcPayloadFieldsSource(rhs, fields)
			if !tracked && rhs != nil {
				// A value bound to a name this scan does not track can still
				// alias a tracked slice (fields[:], []any(fields), a conversion
				// or call result). The alias escapes the census, so every name
				// it mentions stops being tracked rather than keeping a value
				// any later write can change unjudged.
				tcPayloadDropSpanning(fields, rhs)
			}
			if node.Tok == token.DEFINE {
				fields.declare(target.Name, tracked)
				tcPayloadTrackAttrAssignment(scopes, attrs, target.Name, rhs, node.Pos(), slogName, types)
			} else {
				fields.assign(target.Name, tracked)
			}
		case *ast.IndexExpr:
			tcPayloadTrackElementWrite(t, fset, node, i, target.X, target.Index, scopes, seen, used, fields)
		case *ast.SelectorExpr:
			if target.Sel.Name == "Key" {
				// Renaming an attribute's Key renames the emitted field, so
				// the right-hand side is a key position. The receiver's static
				// type decides: a slog.Attr renames a log field, a value that
				// is provably not one (a config struct's own Key field) is not
				// a log key. A receiver this scan cannot classify is a failure,
				// not a skip: the receiver's spelling must not decide whether
				// the write face is judged.
				class := tcPayloadAttrClassOf(target.X, node.Pos(), scopes, attrs, slogName, types)
				if class == tcPayloadAttrNotAttr {
					continue
				}
				rhs := tcPayloadAssignRHS(node, i)
				if rhs == nil {
					t.Fatalf("%s: .Key assignment carries no readable right-hand side", fset.Position(node.Pos()))
				}
				if class == tcPayloadAttrUnknown {
					t.Fatalf("%s: cannot prove the receiver of .Key is not a slog.Attr; the write is unjudged", fset.Position(node.Pos()))
				}
				tcPayloadRecordKey(t, fset, rhs.Pos(), scopes, seen, used, rhs)
				continue
			}
			tcPayloadTrackElementWrite(t, fset, node, i, target.X, nil, scopes, seen, used, fields)
		case *ast.SliceExpr:
			tcPayloadTrackElementWrite(t, fset, node, i, target.X, nil, scopes, seen, used, fields)
		}
	}
}

// tcPayloadAssignRHS returns the right-hand side an assignment gives one
// left-hand side, or nil when the assignment's shape cannot place it.
func tcPayloadAssignRHS(node *ast.AssignStmt, i int) ast.Expr {
	if len(node.Rhs) == 1 {
		return node.Rhs[0]
	}
	if i < len(node.Rhs) {
		return node.Rhs[i]
	}
	return nil
}

// tcPayloadTrackElementWrite handles a write into a tracked slice. A constant
// even index lands on a key position — key/value slices alternate key, value —
// so the right-hand side is judged like every other key. Every other index, a
// field write, and a multi-value right-hand side cannot be placed, and the
// slice stops being tracked instead of keeping a value this scan never read.
// A target this scan cannot attribute (an untracked name, an alias it did not
// follow, a field) may still share a tracked slice's backing array, so that
// write drops every tracked name in scope rather than treating "not tracked"
// as "unrelated".
func tcPayloadTrackElementWrite(t *testing.T, fset *token.FileSet, node *ast.AssignStmt, i int, base, index ast.Expr, scopes []tcPayloadScope, seen, used map[string]bool, fields *tcPayloadScoped[bool]) {
	t.Helper()
	id, ok := base.(*ast.Ident)
	if !ok {
		tcPayloadDropFields(fields)
		return
	}
	if tracked, found := fields.lookup(id.Name); !found || !tracked {
		tcPayloadDropFields(fields)
		return
	}
	rhs := tcPayloadAssignRHS(node, i)
	if index != nil && tcPayloadEvenIndex(index) && rhs != nil {
		tcPayloadRecordKey(t, fset, rhs.Pos(), scopes, seen, used, rhs)
		return
	}
	fields.drop(id.Name)
}

// tcPayloadDropFields stops tracking every slice in scope: a write the scan
// cannot attribute might land in any of them, and a name kept tracked would
// let that key reach a spread unjudged.
func tcPayloadDropFields(fields *tcPayloadScoped[bool]) {
	fields.dropAll()
}

// tcPayloadDropTrackedArgs stops tracking every slice reachable from an
// opaque call's arguments: the callee can write through any expression that
// still names the backing array — a bare name, fields[:], fields[0:n], a
// conversion wrapping one — and the write would be invisible to this scan.
// Every tracked name appearing anywhere inside an argument therefore stops
// being tracked; an argument the scan cannot attribute is never assumed
// harmless.
func tcPayloadDropTrackedArgs(call *ast.CallExpr, fields *tcPayloadScoped[bool]) {
	tcPayloadDropSpanning(fields, call.Args...)
}

// tcPayloadDropSpanning stops tracking every name inside exprs that still
// aliases a tracked slice. It is the same rule at the write point and the call
// point: a narrow form (a constant even index, an append of pairs) is judged,
// every other form drops the names it mentions. A name left tracked over a
// write this scan never read is the one exit the invariant forbids.
func tcPayloadDropSpanning(fields *tcPayloadScoped[bool], exprs ...ast.Expr) {
	for _, expr := range exprs {
		if expr == nil {
			continue
		}
		ast.Inspect(expr, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				fields.drop(id.Name)
			}
			return true
		})
	}
}

// tcPayloadEvenIndex reports a constant index on a key position: key/value
// slices alternate key, value, so the even indexes hold keys.
func tcPayloadEvenIndex(expr ast.Expr) bool {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.INT {
		return false
	}
	value, err := strconv.Atoi(lit.Value)
	return err == nil && value >= 0 && value%2 == 0
}

// tcPayloadTrackDecl rebuilds the fields tracking of one var declaration.
func tcPayloadTrackDecl(fields *tcPayloadScoped[bool], spec *ast.ValueSpec) {
	for i, name := range spec.Names {
		if name.Name == "_" {
			continue
		}
		var value ast.Expr
		if len(spec.Values) == 1 {
			value = spec.Values[0]
		} else if i < len(spec.Values) {
			value = spec.Values[i]
		}
		fields.declare(name.Name, tcPayloadFieldsSource(value, fields))
	}
}

// tcPayloadFieldsSource reports whether expr produces a key/value slice: a
// []any literal, an append into one this scan already reads, or an identifier
// naming one — an alias shares the backing array, so a write through it is a
// write into the original.
func tcPayloadFieldsSource(expr ast.Expr, fields *tcPayloadScoped[bool]) bool {
	switch node := expr.(type) {
	case *ast.CompositeLit:
		return tcPayloadFieldsLiteral(node)
	case *ast.Ident:
		tracked, _ := fields.lookup(node.Name)
		return tracked
	case *ast.CallExpr:
		id, ok := node.Fun.(*ast.Ident)
		if !ok || id.Name != "append" || len(node.Args) == 0 {
			return false
		}
		target, ok := node.Args[0].(*ast.Ident)
		if !ok {
			return false
		}
		tracked, found := fields.lookup(target.Name)
		return found && tracked
	}
	return false
}

// tcPayloadExprName names an expression for a failure message.
func tcPayloadExprName(expr ast.Expr) string {
	if id, ok := expr.(*ast.Ident); ok {
		return id.Name
	}
	return "the target expression"
}

// tcPayloadScope is one function's parameter set, keyed by its position range,
// plus the symbol that names it in tcPayloadForwardedParams and the parameters
// whose declared type is slog.Attr (the only receivers whose .Key write renames
// an emitted field).
type tcPayloadScope struct {
	start, end token.Pos
	symbol     string
	params     map[string]bool
	class      map[string]tcPayloadAttrClass
}

// tcPayloadFuncScopes records every function's symbol and parameter names, so
// a forwarded key can be tied to the exact declaration that owns it.
func tcPayloadFuncScopes(file *ast.File, slogName string, types tcPayloadTypeIndex) []tcPayloadScope {
	var scopes []tcPayloadScope
	ast.Inspect(file, func(n ast.Node) bool {
		var start, end token.Pos
		var params *ast.FieldList
		symbol := ""
		switch fn := n.(type) {
		case *ast.FuncDecl:
			start, end, params = fn.Pos(), fn.End(), fn.Type.Params
			symbol = file.Name.Name + "." + fn.Name.Name
			if recv := tcPayloadReceiverName(fn); recv != "" {
				symbol = file.Name.Name + "." + recv + "." + fn.Name.Name
			}
		case *ast.FuncLit:
			start, end, params = fn.Pos(), fn.End(), fn.Type.Params
		default:
			return true
		}
		names := map[string]bool{}
		classes := map[string]tcPayloadAttrClass{}
		if params != nil {
			for _, field := range params.List {
				for _, name := range field.Names {
					names[name.Name] = true
					classes[name.Name] = tcPayloadAttrClassType(field.Type, slogName, types)
				}
			}
		}
		scopes = append(scopes, tcPayloadScope{start: start, end: end, symbol: symbol, params: names, class: classes})
		return true
	})
	return scopes
}

// tcPayloadReceiverName returns a method's receiver type name, unwrapping the
// pointer a method on *T declares.
func tcPayloadReceiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}
	typ := fn.Recv.List[0].Type
	for {
		switch node := typ.(type) {
		case *ast.StarExpr:
			typ = node.X
		case *ast.IndexExpr:
			typ = node.X
		case *ast.Ident:
			return node.Name
		default:
			return ""
		}
	}
}

// tcPayloadForwardedParam reports a bare identifier naming a parameter of the
// innermost enclosing function, and returns its declaration symbol.
func tcPayloadForwardedParam(expr ast.Expr, pos token.Pos, scopes []tcPayloadScope) (string, bool) {
	id, ok := expr.(*ast.Ident)
	if !ok {
		return "", false
	}
	innermost := tcPayloadInnermostScope(pos, scopes)
	if innermost < 0 || !scopes[innermost].params[id.Name] {
		return "", false
	}
	return scopes[innermost].symbol + "." + id.Name, true
}

// tcPayloadInnermostScope returns the index of the smallest function scope
// containing pos, or -1 outside every function.
func tcPayloadInnermostScope(pos token.Pos, scopes []tcPayloadScope) int {
	innermost := -1
	for i, scope := range scopes {
		if pos < scope.start || pos >= scope.end {
			continue
		}
		if innermost < 0 || scope.end-scope.start < scopes[innermost].end-scopes[innermost].start {
			innermost = i
		}
	}
	return innermost
}

// tcPayloadAttrClass is what a declaration's static type says about .Key
// writes on it. The zero value is "not known": a receiver this scan cannot
// classify fails, because a silent skip is exactly the write face the census
// exists to close.
type tcPayloadAttrClass int

const (
	tcPayloadAttrUnknown   tcPayloadAttrClass = iota
	tcPayloadAttrIsAttr                       // slog.Attr (or *slog.Attr): .Key renames a log field
	tcPayloadAttrIsAttrSeq                    // []slog.Attr: an element is a slog.Attr
	tcPayloadAttrNotAttr                      // provably not a slog.Attr: .Key is an ordinary field
)

// tcPayloadTypeIndex maps a file's named types to their declared type
// expressions, so a local's static type can be classified without a type
// checker. A name this file does not declare stays unknown and fails closed.
type tcPayloadTypeIndex map[string]ast.Expr

// tcPayloadFileTypes indexes the named types declared in one file.
func tcPayloadFileTypes(file *ast.File) tcPayloadTypeIndex {
	types := tcPayloadTypeIndex{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			types[ts.Name.Name] = ts.Type
		}
	}
	return types
}

// tcPayloadAttrClassType classifies a type expression: slog.Attr and pointers
// to it are attribute values, a slice or array of them yields an attribute per
// element, and a type that cannot carry an attribute's Key (a scalar, a map,
// an interface, an ordinary struct) is provably not one.
func tcPayloadAttrClassType(expr ast.Expr, slogName string, types tcPayloadTypeIndex) tcPayloadAttrClass {
	switch node := expr.(type) {
	case nil:
		return tcPayloadAttrUnknown
	case *ast.ParenExpr:
		return tcPayloadAttrClassType(node.X, slogName, types)
	case *ast.StarExpr:
		return tcPayloadAttrClassType(node.X, slogName, types)
	case *ast.SelectorExpr:
		if tcPayloadSlogPkg(node, slogName) && node.Sel.Name == "Attr" {
			return tcPayloadAttrIsAttr
		}
		return tcPayloadAttrNotAttr
	case *ast.Ident:
		if underlying, ok := types[node.Name]; ok {
			return tcPayloadAttrClassType(underlying, slogName, types)
		}
		switch node.Name {
		case "any", "error", "string", "bool", "byte", "rune",
			"int", "int8", "int16", "int32", "int64",
			"uint", "uint8", "uint16", "uint32", "uint64", "uintptr",
			"float32", "float64", "complex64", "complex128":
			return tcPayloadAttrNotAttr
		}
		return tcPayloadAttrUnknown
	case *ast.ArrayType:
		switch tcPayloadAttrClassType(node.Elt, slogName, types) {
		case tcPayloadAttrIsAttr:
			return tcPayloadAttrIsAttrSeq
		case tcPayloadAttrNotAttr:
			return tcPayloadAttrNotAttr
		}
		return tcPayloadAttrUnknown
	case *ast.StructType:
		// An embedded slog.Attr promotes its Key field; a field literally
		// named Key is the attribute only when its type is one. Any other
		// struct's Key field is not a log key.
		for _, field := range node.Fields.List {
			if len(field.Names) == 0 {
				if tcPayloadAttrClassType(field.Type, slogName, types) == tcPayloadAttrIsAttr {
					return tcPayloadAttrIsAttr
				}
				continue
			}
			for _, name := range field.Names {
				if name.Name == "Key" && tcPayloadAttrClassType(field.Type, slogName, types) == tcPayloadAttrIsAttr {
					return tcPayloadAttrIsAttr
				}
			}
		}
		return tcPayloadAttrNotAttr
	case *ast.MapType, *ast.InterfaceType, *ast.FuncType, *ast.ChanType:
		return tcPayloadAttrNotAttr
	}
	return tcPayloadAttrUnknown
}

// tcPayloadAttrClassOf classifies an expression that may denote a slog.Attr —
// a .Key receiver or a declaration's right-hand side. Every shape either
// resolves through a declaration in reach or stays unknown; none is skipped
// because of the way it is spelled.
func tcPayloadAttrClassOf(expr ast.Expr, pos token.Pos, scopes []tcPayloadScope, attrs *tcPayloadScoped[tcPayloadAttrClass], slogName string, types tcPayloadTypeIndex) tcPayloadAttrClass {
	switch node := expr.(type) {
	case *ast.ParenExpr:
		return tcPayloadAttrClassOf(node.X, pos, scopes, attrs, slogName, types)
	case *ast.StarExpr:
		return tcPayloadAttrClassOf(node.X, pos, scopes, attrs, slogName, types)
	case *ast.Ident:
		if class, ok := attrs.lookup(node.Name); ok {
			return class
		}
		if class, ok := tcPayloadParamClass(node.Name, pos, scopes); ok {
			return class
		}
		return tcPayloadAttrUnknown
	case *ast.IndexExpr:
		// An element of a slice or array of attributes is an attribute; an
		// element of a slice of anything else is not one.
		switch tcPayloadAttrClassOf(node.X, pos, scopes, attrs, slogName, types) {
		case tcPayloadAttrIsAttrSeq:
			return tcPayloadAttrIsAttr
		case tcPayloadAttrNotAttr:
			return tcPayloadAttrNotAttr
		}
		return tcPayloadAttrUnknown
	case *ast.CompositeLit:
		return tcPayloadAttrClassType(node.Type, slogName, types)
	case *ast.UnaryExpr:
		if node.Op == token.AND {
			return tcPayloadAttrClassOf(node.X, pos, scopes, attrs, slogName, types)
		}
		return tcPayloadAttrUnknown
	case *ast.TypeAssertExpr:
		if node.Type == nil {
			return tcPayloadAttrUnknown
		}
		return tcPayloadAttrClassType(node.Type, slogName, types)
	case *ast.BasicLit:
		return tcPayloadAttrNotAttr
	}
	return tcPayloadAttrUnknown
}

// tcPayloadParamClass returns the class of a parameter visible at pos, from
// the innermost enclosing function outwards: a closure sees the parameters of
// the functions it is written inside.
func tcPayloadParamClass(name string, pos token.Pos, scopes []tcPayloadScope) (tcPayloadAttrClass, bool) {
	best := -1
	for i, scope := range scopes {
		if pos < scope.start || pos >= scope.end {
			continue
		}
		if _, ok := scope.class[name]; !ok {
			continue
		}
		if best < 0 || scope.end-scope.start < scopes[best].end-scopes[best].start {
			best = i
		}
	}
	if best < 0 {
		return tcPayloadAttrUnknown, false
	}
	return scopes[best].class[name], true
}

// tcPayloadTrackAttrDecl records one var declaration's static class: the
// spelled type when there is one, otherwise the right-hand side's class.
// Anything the scan cannot resolve stays unknown.
func tcPayloadTrackAttrDecl(scopes []tcPayloadScope, attrs *tcPayloadScoped[tcPayloadAttrClass], spec *ast.ValueSpec, slogName string, types tcPayloadTypeIndex) {
	for i, name := range spec.Names {
		if name.Name == "_" {
			continue
		}
		class := tcPayloadAttrUnknown
		if spec.Type != nil {
			class = tcPayloadAttrClassType(spec.Type, slogName, types)
		} else if len(spec.Values) > 0 {
			var value ast.Expr
			if len(spec.Values) == 1 {
				value = spec.Values[0]
			} else if i < len(spec.Values) {
				value = spec.Values[i]
			}
			class = tcPayloadAttrClassOf(value, spec.Pos(), scopes, attrs, slogName, types)
		}
		attrs.declare(name.Name, class)
	}
}

// tcPayloadTrackAttrAssignment records a short variable declaration's class.
// Only := can introduce a name; a plain = keeps the static type.
func tcPayloadTrackAttrAssignment(scopes []tcPayloadScope, attrs *tcPayloadScoped[tcPayloadAttrClass], name string, rhs ast.Expr, pos token.Pos, slogName string, types tcPayloadTypeIndex) {
	if name == "_" || rhs == nil {
		return
	}
	attrs.declare(name, tcPayloadAttrClassOf(rhs, pos, scopes, attrs, slogName, types))
}

// tcPayloadTrackedFields fails unless expr is a key/value slice this scan
// already reads: a tracked identifier, or a []any literal (judged in place).
func tcPayloadTrackedFields(t *testing.T, fset *token.FileSet, expr ast.Expr, fields *tcPayloadScoped[bool]) {
	t.Helper()
	switch src := expr.(type) {
	case *ast.Ident:
		if tracked, found := fields.lookup(src.Name); found && tracked {
			return
		}
	case *ast.CompositeLit:
		if tcPayloadFieldsLiteral(src) {
			return
		}
	}
	t.Fatalf("%s: spread argument is not a tracked key/value slice", fset.Position(expr.Pos()))
}

// tcPayloadSlogImport returns the file's local name for "log/slog". The scan
// follows the import so an aliased logger package cannot drop the attribute
// constructors out of the vocabulary.
func tcPayloadSlogImport(file *ast.File) string {
	for _, imp := range file.Imports {
		if imp.Path.Value != `"log/slog"` {
			continue
		}
		if imp.Name != nil {
			return imp.Name.Name
		}
		return "slog"
	}
	return ""
}

// tcPayloadSlogPkg reports a selector on the file's slog package (the
// attribute constructors below share their names with unrelated types,
// time.Duration for one).
func tcPayloadSlogPkg(sel *ast.SelectorExpr, slogName string) bool {
	if slogName == "" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == slogName
}

// tcPayloadAttrExpr reports an expression that denotes a slog.Attr: a slog
// attribute constructor call or a slog.Attr literal. slog reads such an
// element as one argument carrying its own key.
func tcPayloadAttrExpr(expr ast.Expr, slogName string) bool {
	switch node := expr.(type) {
	case *ast.CallExpr:
		sel, ok := node.Fun.(*ast.SelectorExpr)
		return ok && tcPayloadSlogPkg(sel, slogName) && tcPayloadAttrCtors[sel.Sel.Name]
	case *ast.CompositeLit:
		return tcPayloadAttrLiteral(node, slogName)
	}
	return false
}

// tcPayloadAttrLiteral reports a slog.Attr composite literal.
func tcPayloadAttrLiteral(lit *ast.CompositeLit, slogName string) bool {
	sel, ok := lit.Type.(*ast.SelectorExpr)
	return ok && tcPayloadSlogPkg(sel, slogName) && sel.Sel.Name == "Attr"
}

// tcPayloadFieldsLiteral reports a []any composite literal, the slice shape a
// log call spreads as fields.
func tcPayloadFieldsLiteral(expr ast.Expr) bool {
	literal, ok := expr.(*ast.CompositeLit)
	if !ok {
		return false
	}
	array, ok := literal.Type.(*ast.ArrayType)
	if !ok || array.Len != nil {
		return false
	}
	elem, ok := array.Elt.(*ast.Ident)
	return ok && elem.Name == "any"
}

// tcPayloadAttrKeySlots declares the declarations that may pass an existing
// attribute's own Key into a key position. The redaction rewrite rebuilds an
// attribute under the key it already carries, so that position introduces no
// key of its own. The exemption is a declaration, not a shape: any other .Key
// selector at a key position is a dynamic key this scan cannot see.
var tcPayloadAttrKeySlots = map[string]bool{
	"observability.redactAttr.a": true,
}

// tcPayloadRecordKey records the key at a key position. A key that is not a
// literal is a census failure, never a skip: the scan cannot read it, so it
// fails at that position instead of leaving the vocabulary silently incomplete.
// Three shapes are accepted without a literal, each re-emitting a key judged
// elsewhere: a parameter declared in tcPayloadForwardedParams, the .Key of a
// declaration registered in tcPayloadAttrKeySlots, and nothing else. Failing
// closed is this scan's own rule, not evidence that every emit path was seen
// (ADR-055 §6).
func tcPayloadRecordKey(t *testing.T, fset *token.FileSet, pos token.Pos, scopes []tcPayloadScope, seen, used map[string]bool, expr ast.Expr) {
	t.Helper()
	if value, ok := tcPayloadLiteral(expr); ok {
		seen[value] = true
		return
	}
	if sel, ok := expr.(*ast.SelectorExpr); ok && sel.Sel.Name == "Key" {
		symbol, declared := tcPayloadForwardedParam(sel.X, expr.Pos(), scopes)
		if !declared || !tcPayloadAttrKeySlots[symbol] {
			t.Fatalf("%s: .Key of %s reaches a log key position; only a registered attribute rewrite may re-emit a key it does not own",
				fset.Position(expr.Pos()), tcPayloadExprName(sel.X))
		}
		used[symbol] = true
		return
	}
	if symbol, ok := tcPayloadForwardedParam(expr, pos, scopes); ok {
		if !tcPayloadForwardedParams[symbol] {
			t.Fatalf("%s: parameter %s reaches a log key position; its call sites are outside this scan — pass a literal or declare the exemption",
				fset.Position(expr.Pos()), symbol)
		}
		used[symbol] = true
		return
	}
	t.Fatalf("%s: log attribute key is not a string literal", fset.Position(expr.Pos()))
}

// tcPayloadLiteral reads a string literal expression.
func tcPayloadLiteral(expr ast.Expr) (string, bool) {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return value, true
}

// tcPayloadLogLines returns the log lines carrying marker, for the failure
// message.
func tcPayloadLogLines(logs, marker string) string {
	var hits []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, marker) {
			hits = append(hits, line)
		}
	}
	return strings.Join(hits, "\n")
}
