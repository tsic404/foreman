package proxy

import (
	"encoding/json"
	"strings"
)

// WS frame types of the daemon control channel (ADR-010; the {type,payload}
// envelope and the frame set mirror the upstream daemon WS path). Foreman
// implements only the subset the daemon actually uses: heartbeat + ack,
// task_available push. server_capabilities is never sent (it would switch
// the daemon to WS-first claim, and daemon:rpc_request is not served here).
const (
	// FrameDaemonHeartbeat is sent by a daemon over its WS (S30 inbound).
	FrameDaemonHeartbeat = "daemon:heartbeat"
	// FrameDaemonHeartbeatAck answers every heartbeat frame (S30 outbound).
	FrameDaemonHeartbeatAck = "daemon:heartbeat_ack"
	// FrameDaemonTaskAvailable pushes task availability (both directions).
	FrameDaemonTaskAvailable = "daemon:task_available"
	// FrameDaemonRuntimeProfilesChanged is the C-20 invalidation hint: the
	// workspace's runtime profile list changed and must be re-fetched (§1.6).
	FrameDaemonRuntimeProfilesChanged = "daemon:runtime_profiles_changed"
)

// wsFrame is the {type,payload} envelope of the daemon WS channel. The
// workspace_id member covers upstream frames that put it at the envelope level;
// workspaceID() reads both shapes.
type wsFrame struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	WorkspaceID string          `json:"workspace_id"`
}

// workspaceID returns the workspace id the frame names, looking at the payload
// first and at the envelope second ("" when neither carries one).
func (f wsFrame) workspaceID() string {
	if len(f.Payload) > 0 {
		var payload struct {
			WorkspaceID string `json:"workspace_id"`
		}
		if err := json.Unmarshal(f.Payload, &payload); err == nil && payload.WorkspaceID != "" {
			return payload.WorkspaceID
		}
	}
	return f.WorkspaceID
}

// heartbeatAckOK is the pinned S-30 heartbeat ack payload (§2.2 S-30):
// exactly runtime_id + status; server_capabilities and runtime_gone are
// forbidden on the normal path.
func heartbeatAckOK(runtimeID string) wsFrame {
	payload, _ := json.Marshal(map[string]string{
		"runtime_id": runtimeID,
		"status":     "ok",
	})
	return wsFrame{Type: FrameDaemonHeartbeatAck, Payload: payload}
}

// heartbeatAckRuntimeGone is the single permitted runtime_gone signal:
// the answer to a heartbeat naming an unknown runtime (§1.4), which triggers
// the daemon's re-registration.
func heartbeatAckRuntimeGone(runtimeID string) wsFrame {
	payload, _ := json.Marshal(map[string]any{
		"runtime_id":   runtimeID,
		"status":       "runtime_gone",
		"runtime_gone": true,
	})
	return wsFrame{Type: FrameDaemonHeartbeatAck, Payload: payload}
}

// taskAvailableFrame pushes daemon:task_available (ADR-010).
func taskAvailableFrame(taskID string) wsFrame {
	payload, _ := json.Marshal(map[string]string{"task_id": taskID})
	return wsFrame{Type: FrameDaemonTaskAvailable, Payload: payload}
}

// wsURL converts the http(s) server base URL to ws(s) (C20 dial).
func wsURL(serverURL string) string {
	if strings.HasPrefix(serverURL, "https://") {
		return "wss://" + strings.TrimPrefix(serverURL, "https://")
	}
	return "ws://" + strings.TrimPrefix(serverURL, "http://")
}
