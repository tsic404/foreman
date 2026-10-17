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
	// Settled returns the retained terminal record of an already-reclaimed
	// task (contract §3.1, ADR-032): the single-task endpoint answers with it
	// inside the retention window.
	Settled(taskID string) (registry.TaskEntry, bool)
	List() []registry.TaskEntry
	Inflight() int
}

// OpsConfig carries the static or late-bound values the ops endpoints
// report. RuntimeIDs resolves the declared real runtime ids at request time —
// empty until C1 answered. RegisterSource resolves the startup discovery view
// (S31); a nil one keeps the route and answers 404. Logger defaults to
// slog.Default.
type OpsConfig struct {
	Version        string
	JobImage       string
	RuntimeIDs     func() []string
	RegisterSource func() any
	StartedAt      time.Time
	Logger         *slog.Logger
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
		runtimeIDs := []string{}
		if cfg.RuntimeIDs != nil {
			if ids := cfg.RuntimeIDs(); ids != nil {
				runtimeIDs = ids
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":      "ok",
			"inflight":    index.Inflight(),
			"runtime_ids": runtimeIDs,
			"uptime_s":    int64(time.Since(cfg.StartedAt).Seconds()),
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
		taskID := r.PathValue("taskID")
		e, ok := index.Get(taskID)
		if !ok {
			// A reclaimed task keeps answering with its terminal record until
			// the retention window expires (observability.md §端点, ADR-032):
			// the convergence instant is no longer the end of the evidence
			// window. Beyond it, a reclaimed task and one that never existed
			// are again indistinguishable.
			e, ok = index.Settled(taskID)
		}
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
			// A reclaimed entry's terminal record is still answerable, but the
			// payload is not part of it: the record drops the body at the
			// verdict (registry.settledRecord), and this window has always
			// been the live entry alone (observability.md §端点).
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		// The served body is the entry's delivered payload — written by the S6
		// delivery — and the window is delivered_at until reclaim. An entry
		// that is not delivered yet holds the claim-time payload, which must
		// never be exposed as the delivery body (observability.md §端点).
		if e.DeliveredAt.IsZero() || len(e.Payload) == 0 {
			writeError(w, http.StatusNotFound, "payload unavailable")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(e.Payload)
	})
	// S31 is read-only by design: it reports the startup discovery, the C1
	// declaration and the catalog slots, and can never trigger a discovery or
	// a refresh (ADR-025).
	mux.HandleFunc("GET /foreman/register-source", func(w http.ResponseWriter, _ *http.Request) {
		if cfg.RegisterSource == nil {
			writeError(w, http.StatusNotFound, "register source unavailable")
			return
		}
		writeJSON(w, http.StatusOK, cfg.RegisterSource())
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
