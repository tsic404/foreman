package injectproxy

import (
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxControlBody bounds a control-plane request body; the four keys of
// §4.2 are a few dozen bytes.
const maxControlBody = 1 << 20

// Control is the injection and record control plane (§4.2/§4.3). It listens
// on its own address so the deployment can keep this face internal to the
// test network while the data plane is the stable access point.
type Control struct {
	injector *Injector
	recorder *Recorder
	log      *slog.Logger
}

// NewControl builds the control plane.
func NewControl(injector *Injector, recorder *Recorder, log *slog.Logger) *Control {
	return &Control{injector: injector, recorder: recorder, log: log}
}

// Handler routes the control plane: the three injection operations of §4.2
// and the record face of §4.3.
func (c *Control) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /inject/state", c.handleState)
	mux.HandleFunc("POST /inject", c.handleInject)
	mux.HandleFunc("POST /inject/reset", c.handleReset)
	mux.HandleFunc("GET /record", c.handleRecord)
	// Rows reference their bodies by path (body_path / bytes_path); this
	// endpoint is the read counterpart of those paths, so the assertions that
	// diff a body are executable over the control plane alone. It reads only
	// inside the record directory.
	mux.HandleFunc("GET /record/blob", c.handleBlob)
	return mux
}

func (c *Control) handleState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, c.injector.State())
}

func (c *Control) handleInject(w http.ResponseWriter, r *http.Request) {
	var req Request
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxControlBody))
	// A typo'd key silently doing nothing is the one failure mode a test
	// harness cannot afford.
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid injection body: "+err.Error())
		return
	}
	state, err := c.injector.Set(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	c.log.Info("injection set", "mode", state.Mode, "path_regex", state.PathRegex, "status", state.Status, "until", state.Until)
	writeJSON(w, http.StatusOK, state)
}

func (c *Control) handleReset(w http.ResponseWriter, _ *http.Request) {
	state := c.injector.Reset()
	c.log.Info("injection reset")
	writeJSON(w, http.StatusOK, state)
}

func (c *Control) handleRecord(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	var since time.Time
	if raw := strings.TrimSpace(query.Get("since")); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "since must be an RFC3339 timestamp, got "+raw)
			return
		}
		since = parsed
	}
	limit := 0
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "limit must be a non-negative integer, got "+raw)
			return
		}
		limit = parsed
	}

	rows, err := c.recorder.Read(since, limit)
	if err != nil {
		c.log.Error("record read failed", "err", err)
		writeError(w, http.StatusInternalServerError, "record read failed")
		return
	}
	if rows == nil {
		rows = []record{} // an empty face reads as [] rather than null
	}
	writeJSON(w, http.StatusOK, rows)
}

func (c *Control) handleBlob(w http.ResponseWriter, r *http.Request) {
	file, err := c.recorder.OpenBlob(r.URL.Query().Get("path"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			writeError(w, http.StatusNotFound, "no such record blob")
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer file.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "", time.Time{}, file)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
