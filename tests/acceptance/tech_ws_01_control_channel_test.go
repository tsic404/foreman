// TC-tech-ws-01 (AC-17) is the executable form of the WS control channel:
// one connection per declared runtime (gauge 1 for a one-runtime declaration),
// the "claim is push-driven" discriminator, the Job-side S-30 handshake with
// no fallback noise, the FOREMAN_WS_ENABLED=false polling fallback, and the
// §1.5 handshake-403 face (one error line, no backoff, no park).
package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// ws01Declare registers the single runtime the WS scenarios run with: one
// declaration yields one C-20 connection, which is the gauge reading AC-17
// names ("连接 gauge 为 1").
func ws01Declare(t *testing.T, h *harness) {
	t.Helper()
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp"}
	h.declare(registry.Discovery{
		CLIVersion: "0.6.1",
		Runtimes: []registry.DiscoveredRuntime{
			{Name: "foreman-omp", Type: "omp", Version: "0.6.1"},
		},
	})
}

// ws01CaptureDefaultLogs routes the components that take slog.Default() (the
// subscriber and the S-30 handler) into a buffer for the scenario's assertions.
func ws01CaptureDefaultLogs(t *testing.T) *syncBuffer {
	t.Helper()
	logs := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return logs
}

// ws01Events parses the JSON lines carrying msg from a raw buffer.
func ws01Events(logs *syncBuffer, msg string) []map[string]any {
	var found []map[string]any
	for _, line := range bytes.Split(logs.Bytes(), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			continue
		}
		if fields["msg"] == msg {
			found = append(found, fields)
		}
	}
	return found
}

// TestWS01SubscriptionAndPushDrivenClaim pins the two server-side halves of
// AC-17: the connection gauge, and the claim that can only be explained by the
// push (both polling intervals are 300s, so the next tick is far outside the
// judgement window).
func TestWS01SubscriptionAndPushDrivenClaim(t *testing.T) {
	// Step 2's windows (AC-17): the trigger lands within 60s of startup, and
	// the claim it drives lands within 30s of the trigger.
	const (
		ws01TriggerWindow = 60 * time.Second
		ws01ClaimWindow   = 30 * time.Second
	)

	wsLogs := ws01CaptureDefaultLogs(t)
	h := newHarnessWithWS(t, false)
	ws01Declare(t, h)

	cfg := h.cfg
	cfg.WSEnabled = true
	cfg.ClaimInterval = 300 * time.Second
	cfg.ClaimIntervalIdle = 300 * time.Second
	claimLoop := proxy.NewClaimLoop(cfg, h.client, h.sched)
	subscriber := proxy.NewWSSubscriber(h.client, claimLoop.Wake, h.metrics)

	startedAt := time.Now()
	triggerDeadline := startedAt.Add(ws01TriggerWindow)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = claimLoop.Run(ctx) }()
	go func() { _ = subscriber.Run(ctx) }()

	// 步骤 1: the subscription is up. One runtime ⇒ one dial ⇒ gauge 1. The
	// gauge and the §9.1 line are written in that order, so the wait must cover
	// the line itself — reading the buffer right after the gauge races the log
	// write. The assertions below still pin the line's fields.
	waitFor(t, "server-side WS connection", 10*time.Second, func() bool {
		return len(ws01Events(wsLogs, "ws.connected")) >= 1 &&
			h.metrics.wsConnectedCount("server") == 1 && h.upstream.wsDialCount() == 1
	})
	connected := ws01Events(wsLogs, "ws.connected")
	if len(connected) != 1 {
		t.Fatalf("ws.connected events = %d, want 1: %v", len(connected), connected)
	}
	if connected[0]["side"] != "server" || connected[0]["runtime_id"] != "rid-omp" {
		t.Errorf("ws.connected = %v, want side=server runtime rid-omp", connected[0])
	}

	// The startup poll is the claim loop's immediate first iteration. Waiting
	// it out leaves the push as the only possible cause of the next claim. The
	// wait spends step 2's trigger window, so a poll that misses the 60s
	// precondition fails here instead of at a comparison nothing can reach.
	waitFor(t, "startup poll within the trigger window", time.Until(triggerDeadline), func() bool {
		return len(h.upstream.calls("POST", "/api/daemon/tasks/claim")) == 1
	})
	if got := h.reg.Inflight(); got != 0 {
		t.Fatalf("in-flight entries before the trigger = %d, want 0", got)
	}
	// foreman_inflight_jobs is Registry.Inflight() (§9.2), published on a claim
	// round; no round has run, so the gauge has no reading and the registry
	// above carries the precondition.

	h.upstream.enqueueTask("task-ws")
	h.upstream.pushTaskAvailable(t, "rid-omp", "task-ws")

	// The criterion is ≤30s with the push as the only possible cause (both
	// polling intervals are 300s): the wait bound is that window, so a claim
	// the push did not drive fails here.
	waitFor(t, "push-driven claim within 30s of the push", ws01ClaimWindow, func() bool {
		return len(h.events("task.claimed")) == 1
	})
	if got := len(h.upstream.calls("POST", "/api/daemon/tasks/claim")); got < 2 {
		t.Fatalf("claim calls = %d, want the startup poll plus the push-driven round", got)
	}
	claimed := h.events("task.claimed")[0]
	if claimed["task_id"] != "task-ws" {
		t.Errorf("task.claimed = %v, want task-ws", claimed)
	}
	if !h.jobs.jobCreated("fm-task-ws") {
		t.Fatal("the push-driven claim created no Job")
	}
	if got := h.metrics.wsConnectedCount("server"); got != 1 {
		t.Errorf("foreman_ws_connected{side=server} = %d, want 1", got)
	}
}

// TestWS01JobSideHandshakeAccepted pins the S-30 face: the daemon's handshake
// is accepted (101), the gauge counts the Job connection, and a heartbeat
// round trip proves the accepted channel is the working path — the daemon has
// no fallback to log. No warning or error line is emitted on that path.
func TestWS01JobSideHandshakeAccepted(t *testing.T) {
	wsLogs := ws01CaptureDefaultLogs(t)
	h := newHarnessWithWS(t, false)

	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-job")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	entry, ok := h.reg.Get("task-job")
	if !ok {
		t.Fatal("task-job entry missing")
	}
	token := issueJobToken(t, h, "task-job")

	wsURL := "ws" + strings.TrimPrefix(h.jobsHTTP.URL, "http") +
		"/api/daemon/ws?runtime_ids=" + url.QueryEscape(entry.JobRuntimeID)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}},
	})
	if err != nil {
		t.Fatalf("S-30 handshake: %v (status %v)", err, resp)
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "done") }()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("S-30 handshake status = %d, want 101", resp.StatusCode)
	}

	// The two faces the S-30 acceptance names are independent observables: the
	// gauge counts the connection, and §9.1's ws.connected line records it.
	// The handler writes the gauge before the log line, so waiting on the gauge
	// and then reading the buffer races that write — the wait covers the line
	// itself, and the assertions below still pin its fields.
	waitFor(t, "job-side ws.connected line", 5*time.Second, func() bool {
		return len(ws01Events(wsLogs, "ws.connected")) >= 1
	})
	if got := h.metrics.wsConnectedCount("job"); got != 1 {
		t.Errorf("foreman_ws_connected{side=job} = %d, want 1", got)
	}
	connected := ws01Events(wsLogs, "ws.connected")
	jobSide := false
	for _, event := range connected {
		if event["side"] == "job" && event["task_id"] == "task-job" {
			jobSide = true
		}
	}
	if !jobSide {
		t.Fatalf("ws.connected events = %v, want one for side=job task-job", connected)
	}

	heartbeat, err := json.Marshal(map[string]any{
		"type":    "daemon:heartbeat",
		"payload": map[string]string{"runtime_id": entry.JobRuntimeID},
	})
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, heartbeat); err != nil {
		t.Fatalf("write heartbeat: %v", err)
	}
	_, raw, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read heartbeat ack: %v", err)
	}
	var frame struct {
		Type    string `json:"type"`
		Payload struct {
			RuntimeID string `json:"runtime_id"`
			Status    string `json:"status"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("heartbeat ack is not JSON: %v (%s)", err, raw)
	}
	if frame.Type != "daemon:heartbeat_ack" || frame.Payload.RuntimeID != entry.JobRuntimeID || frame.Payload.Status != "ok" {
		t.Fatalf("heartbeat ack = %s, want the pinned runtime_id+status body", raw)
	}

	// No fallback noise: the accepted path emits no warning or error line.
	for _, line := range bytes.Split(wsLogs.Bytes(), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			continue
		}
		if level, _ := event["level"].(string); level == "WARN" || level == "ERROR" {
			t.Errorf("WS path logged %v", event)
		}
	}
}

// TestWS01DisabledFallsBackToPolling pins the degradation switch: with
// FOREMAN_WS_ENABLED=false the route is absent, no connection is attempted,
// and the task still closes through polling alone.
func TestWS01DisabledFallsBackToPolling(t *testing.T) {
	h := newHarness(t, false)
	ws01Declare(t, h)

	cfg := h.cfg
	cfg.ClaimInterval = 20 * time.Millisecond
	cfg.ClaimIntervalIdle = 20 * time.Millisecond
	claimLoop := proxy.NewClaimLoop(cfg, h.client, h.sched)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = claimLoop.Run(ctx) }()

	h.upstream.enqueueTask("task-poll")
	waitFor(t, "polling claim", 5*time.Second, func() bool {
		return len(h.events("task.claimed")) == 1
	})
	if !h.jobs.jobCreated("fm-task-poll") {
		t.Fatal("the polling claim created no Job")
	}
	// The switch removes the route: a daemon asking for it with its own valid
	// token gets a plain 404, never a 5xx it must retry.
	token := issueJobToken(t, h, "task-poll")
	if code, body := h.call("GET", "/api/daemon/ws?runtime_ids=rid-omp", token, nil); code != http.StatusNotFound {
		t.Fatalf("WS route with FOREMAN_WS_ENABLED=false = %d (%s), want 404", code, body)
	}
	if got := h.upstream.wsDialCount(); got != 0 {
		t.Errorf("WS dials with the switch off = %d, want 0", got)
	}
	if got := h.metrics.wsConnectedCount("server"); got != 0 {
		t.Errorf("foreman_ws_connected{side=server} = %d, want 0", got)
	}
	if got := h.metrics.wsConnectedCount("job"); got != 0 {
		t.Errorf("foreman_ws_connected{side=job} = %d, want 0", got)
	}
}

// TestWS01Handshake403ReportsConfigError pins the §1.5 face PR #100 added: a
// 403 handshake is the credential/identity mismatch, so the subscriber reports
// it once and stops. No backoff retry (one dial total, still one after the 1s
// first step) and no park (Run returns instead of holding the stream for 30s).
func TestWS01Handshake403ReportsConfigError(t *testing.T) {
	wsLogs := ws01CaptureDefaultLogs(t)
	h := newHarnessWithWS(t, false)
	ws01Declare(t, h)
	h.upstream.setWSStatus(http.StatusForbidden)

	subscriber := proxy.NewWSSubscriber(h.client, func() {}, h.metrics)
	done := make(chan error, 1)
	go func() { done <- subscriber.Run(t.Context()) }()

	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a 403 handshake must stop the subscriber; a retry loop or a park would keep it running")
	}
	if runErr == nil || !strings.Contains(runErr.Error(), "403") {
		t.Fatalf("Run error = %v, want the 403 config error the supervisor escalates", runErr)
	}
	if got := h.upstream.wsDialCount(); got != 1 {
		t.Fatalf("handshake dials = %d, want exactly 1 (no blind retry)", got)
	}
	// The first §1.5 backoff step is 1s: surviving it with the dial count
	// unchanged proves the stop is a stop, not a scheduled retry.
	time.Sleep(1100 * time.Millisecond)
	if got := h.upstream.wsDialCount(); got != 1 {
		t.Fatalf("handshake dials after the first backoff window = %d, want 1", got)
	}

	events := ws01Events(wsLogs, "ws handshake rejected: 403 config error (credential does not match daemon_id/workspace_id)")
	if len(events) != 1 {
		t.Fatalf("403 config-error lines = %d, want exactly 1: %v", len(events), events)
	}
	event := events[0]
	if event["level"] != "ERROR" {
		t.Errorf("403 line level = %v, want ERROR", event["level"])
	}
	if event["status"] != float64(http.StatusForbidden) {
		t.Errorf("403 line status = %v, want 403", event["status"])
	}
	if event["runtime_id"] != "rid-omp" {
		t.Errorf("403 line runtime_id = %v, want rid-omp", event["runtime_id"])
	}
}
