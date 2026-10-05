package proxy

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/registry"
)

// wsWriteTimeout bounds a single frame write so a dead peer cannot wedge
// the read loop or a NotifyTaskAvailable caller.
const wsWriteTimeout = 10 * time.Second

// WSHandler is the Job-side WS endpoint (S30, ADR-010). It authenticates the
// handshake with the Job Token (§1.2 rule 1 plus the query runtime_ids
// ownership check), acks every daemon:heartbeat frame with the pinned
// payload, pushes daemon:task_available when the Job's task is deliverable,
// and closes on terminal/teardown.
type WSHandler struct {
	issuer  *auth.Issuer
	reg     *registry.Registry
	metrics Metrics
	log     *slog.Logger

	mu    sync.Mutex
	conns map[string]*jobConn // daemonID → live connection
}

// jobConn is one Job daemon's WS connection; writes are serialized.
type jobConn struct {
	daemonID string
	conn     *websocket.Conn
	writeMu  sync.Mutex
}

// NewWSHandler builds the handler. It is only mounted when
// FOREMAN_WS_ENABLED=true (the route is absent otherwise, §5.1).
func NewWSHandler(issuer *auth.Issuer, reg *registry.Registry, metrics Metrics) *WSHandler {
	return &WSHandler{
		issuer:  issuer,
		reg:     reg,
		metrics: orNoopMetrics(metrics),
		log:     slog.Default().With("component", "ws-handler"),
		conns:   make(map[string]*jobConn),
	}
}

// ServeHTTP implements GET /api/daemon/ws (S30).
func (h *WSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token, ok := bearerToken(r)
	if !ok || !auth.HasJobTokenPrefix(token) {
		// The WS endpoint exists only on the local daemon face; other
		// credentials are not proxied (§1.2 rule 1).
		writeError(w, http.StatusUnauthorized, "job token required")
		return
	}
	claims, err := h.issuer.Verify(token, auth.RequestScope{})
	if err != nil {
		h.metrics.AuthFailure(auth.Reason(err))
		h.log.Warn("ws auth failed", "remote_addr", r.RemoteAddr, "reason", auth.Reason(err))
		writeError(w, auth.HTTPStatus(err), "unauthorized")
		return
	}
	entry, ok := h.reg.ByDaemon(claims.JobName)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	runtimeIDs := r.URL.Query()["runtime_ids"]
	if len(runtimeIDs) == 0 {
		writeError(w, http.StatusBadRequest, "runtime_ids or user identity required")
		return
	}
	for _, rid := range runtimeIDs {
		if rid != entry.JobRuntimeID {
			writeError(w, http.StatusForbidden, "runtime does not belong to this job")
			return
		}
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	jc := &jobConn{daemonID: claims.JobName, conn: conn}
	h.mu.Lock()
	if old := h.conns[claims.JobName]; old != nil {
		// A daemon reconnect supersedes its previous connection.
		_ = old.conn.Close(websocket.StatusNormalClosure, "replaced")
	}
	h.conns[claims.JobName] = jc
	h.mu.Unlock()
	h.metrics.WSConnect(WSSideJob)
	h.log.Info("ws.connected",
		"side", WSSideJob, "task_id", entry.TaskID, "job_name", entry.JobName,
		"daemon_id", claims.JobName)

	// Catch-up push (proxy.md 请求路由 S30): the daemon registered before
	// connecting may have missed the task_available push; pushes are
	// idempotent because claim dedups by task_id.
	if entry.State == registry.StateDaemonRegistered && entry.DeliveredAt.IsZero() && len(entry.Payload) > 0 {
		jc.write(taskAvailableFrame(entry.TaskID))
	}

	h.readLoop(r.Context(), jc, entry.JobRuntimeID)

	h.mu.Lock()
	if h.conns[claims.JobName] == jc {
		delete(h.conns, claims.JobName)
	}
	h.mu.Unlock()
	_ = conn.Close(websocket.StatusNormalClosure, "closing")
	h.metrics.WSDisconnect(WSSideJob)
}

// readLoop answers daemon:heartbeat frames until the connection drops
// (§1.2 S30: every heartbeat frame gets an ack; the ack refreshes the
// heartbeat clock shared with S5).
func (h *WSHandler) readLoop(ctx context.Context, jc *jobConn, jobRuntimeID string) {
	for {
		_, data, err := jc.conn.Read(ctx)
		if err != nil {
			return
		}
		var frame wsFrame
		if err := json.Unmarshal(data, &frame); err != nil {
			h.log.Debug("ws frame undecodable", "daemon_id", jc.daemonID, "err", err)
			continue
		}
		if frame.Type != FrameDaemonHeartbeat {
			// The daemon only sends heartbeat frames on this channel;
			// anything else (e.g. rpc responses we never request) is ignored.
			h.log.Debug("ws frame ignored", "daemon_id", jc.daemonID, "type", frame.Type)
			continue
		}
		var payload struct {
			RuntimeID string `json:"runtime_id"`
		}
		_ = json.Unmarshal(frame.Payload, &payload)
		if payload.RuntimeID != "" && payload.RuntimeID != jobRuntimeID {
			// Unknown runtime (failure-handling scenario #12): the single
			// permitted runtime_gone ack, prompting re-registration.
			jc.write(heartbeatAckRuntimeGone(payload.RuntimeID))
			continue
		}
		h.reg.TouchHeartbeat(jobRuntimeID, time.Now())
		jc.write(heartbeatAckOK(jobRuntimeID))
	}
}

// write sends one frame with a bounded write timeout.
func (jc *jobConn) write(frame wsFrame) {
	ctx, cancel := context.WithTimeout(context.Background(), wsWriteTimeout)
	defer cancel()
	raw, err := json.Marshal(frame)
	if err != nil {
		return
	}
	jc.writeMu.Lock()
	defer jc.writeMu.Unlock()
	_ = jc.conn.Write(ctx, websocket.MessageText, raw)
}

// NotifyTaskAvailable pushes daemon:task_available to the Job's daemon when
// it is connected (S30); a disconnected daemon discovers the task by
// polling (S6) and by the on-connect catch-up push.
func (h *WSHandler) NotifyTaskAvailable(daemonID, taskID string) {
	h.mu.Lock()
	jc := h.conns[daemonID]
	h.mu.Unlock()
	if jc == nil {
		return
	}
	jc.write(taskAvailableFrame(taskID))
}

// CloseJob closes the Job's WS connection (terminal state or teardown,
// S30). The daemon falls back to HTTP polling until its Job is deleted.
func (h *WSHandler) CloseJob(daemonID string) {
	h.mu.Lock()
	jc := h.conns[daemonID]
	delete(h.conns, daemonID)
	h.mu.Unlock()
	if jc != nil {
		_ = jc.conn.Close(websocket.StatusNormalClosure, "job done")
	}
}
