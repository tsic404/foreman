package observability

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/tsic404/foreman/internal/registry"
)

// Log tail bounds for /foreman/tasks/{id}/logs (observability.md §端点).
const (
	defaultLogTail = 200
	maxLogTail     = 2000
)

// TaskIndex is the registry read view the ops endpoints need; satisfied by
// *registry.Registry.
type TaskIndex interface {
	Get(taskID string) (registry.TaskEntry, bool)
	List() []registry.TaskEntry
	Inflight() int
}

// OpsConfig carries the static or late-bound values the ops endpoints
// report. RuntimeID resolves the real runtime id at request time — it is
// empty until the proxy's first register. Logger defaults to slog.Default.
type OpsConfig struct {
	Version   string
	JobImage  string
	RuntimeID func() string
	StartedAt time.Time
	Logger    *slog.Logger
}

// NewOpsHandler mounts the cluster-internal ops surface (contract §1.2
// S25): /metrics plus /foreman/*. No Job Token check — these endpoints are
// reachable only inside the cluster and never exposed via Ingress. A nil
// logs reader keeps the route but answers 404 (optional endpoint).
func NewOpsHandler(metrics *Metrics, index TaskIndex, logs PodLogReader, cfg OpsConfig) http.Handler {
	if cfg.StartedAt.IsZero() {
		cfg.StartedAt = time.Now()
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	log = log.With("component", "observability")
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /foreman/healthz", func(w http.ResponseWriter, _ *http.Request) {
		runtimeID := ""
		if cfg.RuntimeID != nil {
			runtimeID = cfg.RuntimeID()
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":     "ok",
			"inflight":   index.Inflight(),
			"runtime_id": runtimeID,
			"uptime_s":   int64(time.Since(cfg.StartedAt).Seconds()),
		})
	})
	mux.HandleFunc("GET /foreman/tasks", func(w http.ResponseWriter, _ *http.Request) {
		// TaskEntry.Payload is json:"-" — the list never leaks payloads (F3).
		entries := index.List()
		if entries == nil {
			entries = []registry.TaskEntry{}
		}
		writeJSON(w, http.StatusOK, entries)
	})
	mux.HandleFunc("GET /foreman/tasks/{taskID}", func(w http.ResponseWriter, r *http.Request) {
		e, ok := index.Get(r.PathValue("taskID"))
		if !ok {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		writeJSON(w, http.StatusOK, e)
	})
	mux.HandleFunc("GET /foreman/tasks/{taskID}/logs", func(w http.ResponseWriter, r *http.Request) {
		serveJobLogs(w, r, index, logs, log)
	})
	mux.HandleFunc("GET /foreman/tasks/{taskID}/payload-preview", func(w http.ResponseWriter, r *http.Request) {
		e, ok := index.Get(r.PathValue("taskID"))
		if !ok {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		// The payload lives in process memory only; after a restart there is
		// nothing to preview (contract §3.2).
		if len(e.Payload) == 0 {
			writeError(w, http.StatusNotFound, "payload preview unavailable")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(e.Payload)
	})
	mux.HandleFunc("GET /foreman/version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"version":   cfg.Version,
			"job_image": cfg.JobImage,
		})
	})
	return mux
}

// serveJobLogs proxies the newest pod's log tail for the task's Job.
func serveJobLogs(w http.ResponseWriter, r *http.Request, index TaskIndex, logs PodLogReader, log *slog.Logger) {
	e, ok := index.Get(r.PathValue("taskID"))
	if !ok {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	tail, err := parseTail(r.URL.Query().Get("tail"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if logs == nil {
		writeError(w, http.StatusNotFound, ErrJobLogsUnavailable.Error())
		return
	}
	data, err := logs.ReadLogs(r.Context(), e.JobNamespace, e.JobName, tail)
	switch {
	case errors.Is(err, ErrJobLogsUnavailable):
		writeError(w, http.StatusNotFound, ErrJobLogsUnavailable.Error())
		return
	case apierrors.IsForbidden(err):
		// observability.md §错误处理: pods/log RBAC denied is 403 + an error
		// log; the task chain itself is unaffected.
		log.ErrorContext(r.Context(), "read job logs forbidden",
			"task_id", e.TaskID, "job_name", e.JobName, "err", err)
		writeError(w, http.StatusForbidden, "job logs forbidden")
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, "read job logs")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// parseTail applies the contract bounds: default 200, clamp to 2000.
func parseTail(raw string) (int64, error) {
	if raw == "" {
		return defaultLogTail, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 1 {
		return 0, errors.New("tail must be a positive integer")
	}
	if n > maxLogTail {
		n = maxLogTail
	}
	return n, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError uses the contract §1.2 error shape {"error":"<message>"}.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
