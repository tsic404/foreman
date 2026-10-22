package observability

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/tsic404/foreman/internal/registry"
)

// Log tail bounds for /foreman/tasks/{id}/logs (§9.3).
const (
	defaultLogTail = 200
	maxLogTail     = 2000
)

// TaskIndex is the registry read view the ops endpoints need; satisfied by
// *registry.Registry.
type TaskIndex interface {
	// GetInWorkspace returns one live row; the workspace dimension is part of
	// the projection's partition (§4.1). A shared-state index reads the row
	// from the adjudication face, so every instance answers the same (§9.3
	// 多实例口径).
	GetInWorkspace(workspaceID, taskID string) (registry.TaskEntry, bool)
	// SettledInWorkspace returns the retained terminal record of an
	// already-reclaimed task (§4.3, ADR-032): the single-task endpoint answers
	// with it inside the retention window.
	SettledInWorkspace(workspaceID, taskID string) (registry.TaskEntry, bool)
	// ListInWorkspace lists the workspace's live rows ("" = every row, the
	// legacy single-workspace shape).
	ListInWorkspace(workspaceID string) []registry.TaskEntry
	// Inflight is the shared two-level counter's cluster value in stage two
	// and the local index count otherwise (§10 两级在途上限).
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
	// Health is the liveness view of the process's critical loops; nil keeps
	// the endpoint's always-ok shape (a handler mounted without loops, as in
	// the ops tests).
	Health *Health
	// WorkspaceIDs is the deployment's configured workspace set (§0,
	// ADR-047). A multi-workspace deployment must be asked with the
	// workspace dimension; a single workspace is its default. Empty keeps
	// the pre-ADR-047 single-workspace shape.
	WorkspaceIDs []string
	StartedAt    time.Time
	Logger       *slog.Logger
}

// NewOpsHandler mounts the cluster-internal ops surface (§2.2 S-25): /metrics
// plus /foreman/*. No Job Token check — these endpoints are
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
		body := map[string]any{
			"status":      "ok",
			"inflight":    index.Inflight(),
			"runtime_ids": runtimeIDs,
			"uptime_s":    int64(time.Since(cfg.StartedAt).Seconds()),
		}
		// §9.3: this endpoint feeds the liveness probe. A critical loop that
		// stopped while the process kept serving is exactly the half-dead
		// state the probe exists to end, so it must not answer ok (§1.5: a
		// config error is logged and alarmed, not silently absorbed).
		status := http.StatusOK
		if stopped := cfg.Health.Stopped(); len(stopped) > 0 {
			status = http.StatusServiceUnavailable
			body["status"] = "failed"
			body["stopped_components"] = stopped
		}
		writeJSON(w, status, body)
	})
	mux.HandleFunc("GET /foreman/tasks", func(w http.ResponseWriter, r *http.Request) {
		// TaskEntry.Payload is json:"-" — the list never leaks payloads (F3).
		workspaceID, ok := workspaceScope(r, cfg.WorkspaceIDs)
		if !ok {
			// §4.1: the list is partitioned by workspace; a deployment with
			// more than one workspace has no default dimension, and answering
			// every workspace's entries would cross the partition.
			writeError(w, http.StatusBadRequest, "workspace is required")
			return
		}
		entries := index.ListInWorkspace(workspaceID)
		if entries == nil {
			entries = []registry.TaskEntry{}
		}
		writeJSON(w, http.StatusOK, entries)
	})
	mux.HandleFunc("GET /foreman/tasks/{taskID}", func(w http.ResponseWriter, r *http.Request) {
		workspaceID, scoped := workspaceScope(r, cfg.WorkspaceIDs)
		if !scoped {
			writeError(w, http.StatusBadRequest, "workspace is required")
			return
		}
		taskID := r.PathValue("taskID")
		e, ok := index.GetInWorkspace(workspaceID, taskID)
		if !ok {
			// A reclaimed task keeps answering with its terminal record until
			// the retention window expires (§9.3, ADR-032):
			// the convergence instant is no longer the end of the evidence
			// window. Beyond it, a reclaimed task and one that never existed
			// are again indistinguishable.
			e, ok = index.SettledInWorkspace(workspaceID, taskID)
		}
		if !ok {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		if workspaceID != "" && e.WorkspaceID != workspaceID {
			// A cross-workspace task id is indistinguishable from one that
			// never existed (§4.1: 跨 workspace 一律 404).
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		writeJSON(w, http.StatusOK, e)
	})
	mux.HandleFunc("GET /foreman/tasks/{taskID}/logs", func(w http.ResponseWriter, r *http.Request) {
		workspaceID, scoped := workspaceScope(r, cfg.WorkspaceIDs)
		if !scoped {
			writeError(w, http.StatusBadRequest, "workspace is required")
			return
		}
		if !workspaceVisible(index, r.PathValue("taskID"), workspaceID) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		serveJobLogs(w, r, index, workspaceID, logs, log)
	})
	mux.HandleFunc("GET /foreman/tasks/{taskID}/payload-preview", func(w http.ResponseWriter, r *http.Request) {
		workspaceID, scoped := workspaceScope(r, cfg.WorkspaceIDs)
		if !scoped {
			writeError(w, http.StatusBadRequest, "workspace is required")
			return
		}
		e, ok := index.GetInWorkspace(workspaceID, r.PathValue("taskID"))
		if ok && !workspaceVisible(index, e.TaskID, workspaceID) {
			ok = false
		}
		if !ok {
			// A reclaimed entry's terminal record is still answerable, but the
			// payload is not part of it: the record drops the body at the
			// verdict (registry.settledRecord), and this window has always
			// been the live entry alone (§9.3).
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		// The served body is the entry's delivered payload — written by the S6
		// delivery — and the window is delivered_at until reclaim. An entry
		// that is not delivered yet holds the claim-time payload, which must
		// never be exposed as the delivery body (§9.3).
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
func serveJobLogs(w http.ResponseWriter, r *http.Request, index TaskIndex, workspaceID string, logs PodLogReader, log *slog.Logger) {
	e, ok := index.GetInWorkspace(workspaceID, r.PathValue("taskID"))
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
		// pods/log RBAC denied is 403 + an error log; the task chain itself
		// is unaffected.
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

// workspaceScope resolves the workspace dimension of an ops request (§4.1).
// An explicit workspace must be one the deployment serves. Without one, a
// single configured workspace is the dimension; a multi-workspace deployment
// has none (the caller must say which), and an empty set keeps the
// pre-ADR-047 single-workspace shape.
func workspaceScope(r *http.Request, configured []string) (string, bool) {
	workspaceID := strings.TrimSpace(r.URL.Query().Get("workspace"))
	if workspaceID != "" {
		if len(configured) > 0 && !slices.Contains(configured, workspaceID) {
			return "", false
		}
		return workspaceID, true
	}
	switch len(configured) {
	case 0:
		return "", true
	case 1:
		return configured[0], true
	default:
		return "", false
	}
}

// workspaceVisible applies the projection's workspace partition to the
// task-addressed ops routes: an entry of another workspace is invisible (404),
// exactly like a task that never existed.
func workspaceVisible(index TaskIndex, taskID, workspaceID string) bool {
	if workspaceID == "" {
		return true
	}
	e, ok := index.GetInWorkspace(workspaceID, taskID)
	if !ok {
		e, ok = index.SettledInWorkspace(workspaceID, taskID)
	}
	return ok && e.WorkspaceID == workspaceID
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes the ops surface's error shape {"error":"<message>"}.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
