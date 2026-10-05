// Package recovery implements the failure-compensation side of Foreman:
// the durable pending terminal-report queue here, and (in later slices) the
// restart/failure reconciler. Design: docs/05-modules/failure-handling.md.
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/scheduler"
)

// PendingReportTTL caps how long a queued terminal report is retried before
// it is dropped with an error log (failure-handling 场景 #10: 上限 24h).
const PendingReportTTL = 24 * time.Hour

// ReportSender re-delivers a queued report; satisfied by proxy.Client.
type ReportSender interface {
	Forward(ctx context.Context, ep scheduler.Endpoint, taskID string, body []byte) (int, []byte, error)
}

// pendingReport is one queued terminal report (contract §4). The file is
// the queue: a report exists iff its file exists, so a crash never loses it.
type pendingReport struct {
	TaskID     string          `json:"task_id"`
	Endpoint   string          `json:"endpoint"`
	Body       json.RawMessage `json:"body"`
	EnqueuedAt time.Time       `json:"enqueued_at"`
	Attempts   int             `json:"attempts"`
}

var taskIDFileChars = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

// PendingReports is the durable terminal-report queue
// (FOREMAN_STATE_ROOT/pending-reports/<taskid>.json, contract §4).
type PendingReports struct {
	dir string
	now func() time.Time
	log *slog.Logger

	mu       sync.Mutex
	onChange func(int) // foreman_pending_reports gauge hook
}

// PendingReportsOption customizes a PendingReports queue.
type PendingReportsOption func(*PendingReports)

// WithPendingReportsClock overrides the clock (tests).
func WithPendingReportsClock(now func() time.Time) PendingReportsOption {
	return func(p *PendingReports) { p.now = now }
}

// WithPendingReportsGauge wires the foreman_pending_reports gauge hook.
func WithPendingReportsGauge(onChange func(int)) PendingReportsOption {
	return func(p *PendingReports) { p.onChange = onChange }
}

// WithPendingReportsLogger overrides the logger (tests).
func WithPendingReportsLogger(l *slog.Logger) PendingReportsOption {
	return func(p *PendingReports) { p.log = l.With("component", "pending-reports") }
}

// NewPendingReports opens (creating if needed) the queue directory.
func NewPendingReports(dir string, opts ...PendingReportsOption) (*PendingReports, error) {
	if dir == "" {
		return nil, errors.New("pending-reports directory is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create pending-reports dir: %w", err)
	}
	p := &PendingReports{
		dir: dir,
		now: time.Now,
		log: slog.Default().With("component", "pending-reports"),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

// Enqueue lands a terminal report on disk (atomic tmp+rename; a report is
// queued only once the file is durable — failure-handling 顺序规则 5).
// Re-enqueueing the same task replaces the body but keeps the original
// enqueue time, so the TTL clocks the first failure.
func (p *PendingReports) Enqueue(taskID string, ep scheduler.Endpoint, body []byte) error {
	if !taskIDFileChars.MatchString(taskID) {
		return fmt.Errorf("task id %q is not a safe file name", taskID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	rep := pendingReport{
		TaskID:     taskID,
		Endpoint:   string(ep),
		Body:       body,
		EnqueuedAt: p.now(),
	}
	if raw, err := os.ReadFile(p.path(taskID)); err == nil {
		var old pendingReport
		if json.Unmarshal(raw, &old) == nil && !old.EnqueuedAt.IsZero() {
			rep.EnqueuedAt = old.EnqueuedAt
			rep.Attempts = old.Attempts
		}
	}
	if err := p.writeLocked(rep); err != nil {
		return err
	}
	p.log.Error("task.forward_queued",
		"task_id", taskID, "endpoint", rep.Endpoint)
	p.notifyLocked()
	return nil
}

// Drain retries every queued report once through sender. A report leaves
// the queue when the server accepts it (2xx) or settles it as moot
// (404/409: server-side already terminal or deleted). Reports older than
// PendingReportTTL are dropped with an error log (failure-handling 场景
// #10). It returns the number of reports still queued.
//
// The lock is never held across the network: the queue is snapshotted,
// forwarded, and settled entry-by-entry, so a slow upstream cannot block
// Enqueue (the daemon's terminal-report path) or Len (metrics).
func (p *PendingReports) Drain(ctx context.Context, sender ReportSender) (int, error) {
	p.mu.Lock()
	queue := p.listLocked()
	p.mu.Unlock()

	var firstErr error
	for _, rep := range queue {
		if p.now().Sub(rep.EnqueuedAt) > PendingReportTTL {
			p.log.Error("task.forward_dropped",
				"task_id", rep.TaskID, "endpoint", rep.Endpoint,
				"attempts", rep.Attempts, "reason", "pending report TTL exceeded")
			p.mu.Lock()
			p.removeLocked(rep.TaskID)
			p.mu.Unlock()
			continue
		}
		code, _, err := sender.Forward(ctx, scheduler.Endpoint(rep.Endpoint), rep.TaskID, rep.Body)
		delivered := err == nil && ((code >= 200 && code < 300) || code == 404 || code == 409)
		p.mu.Lock()
		if delivered {
			p.log.Info("task.forward_delivered",
				"task_id", rep.TaskID, "endpoint", rep.Endpoint, "attempts", rep.Attempts+1)
			p.removeLocked(rep.TaskID)
		} else if cur, ok := p.readLocked(rep.TaskID); ok {
			// Merge with a concurrent re-enqueue: bump the attempt counter
			// on the file as it stands now, never clobbering a newer body.
			cur.Attempts++
			if werr := p.writeLocked(cur); werr != nil {
				p.log.Error("pending report update failed", "task_id", rep.TaskID, "err", werr)
			}
		}
		p.mu.Unlock()
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if err == nil && !delivered && firstErr == nil {
			firstErr = fmt.Errorf("task %s: server returned %d", rep.TaskID, code)
		}
	}
	p.mu.Lock()
	remaining := len(p.listLocked())
	p.mu.Unlock()
	p.notify(remaining)
	return remaining, firstErr
}

// Len reports the queued report count (foreman_pending_reports).
func (p *PendingReports) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.listLocked())
}

func (p *PendingReports) path(taskID string) string {
	return filepath.Join(p.dir, taskID+".json")
}

// listLocked reads the queue oldest-first so long-stuck reports retry
// first. Malformed files are quarantined to <name>.bad (once, not re-logged
// every round) and excluded from the count.
func (p *PendingReports) listLocked() []pendingReport {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		p.log.Error("read pending-reports dir failed", "err", err)
		return nil
	}
	out := make([]pendingReport, 0, len(entries))
	for _, ent := range entries {
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".json" {
			continue
		}
		rep, ok := p.readFileLocked(ent.Name())
		if !ok {
			continue
		}
		out = append(out, rep)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EnqueuedAt.Before(out[j].EnqueuedAt) })
	return out
}

// readLocked reads one queue file by task id.
func (p *PendingReports) readLocked(taskID string) (pendingReport, bool) {
	return p.readFileLocked(taskID + ".json")
}

// readFileLocked parses one queue file; malformed files are quarantined.
func (p *PendingReports) readFileLocked(name string) (pendingReport, bool) {
	raw, err := os.ReadFile(filepath.Join(p.dir, name))
	if err != nil {
		return pendingReport{}, false
	}
	var rep pendingReport
	if err := json.Unmarshal(raw, &rep); err != nil || rep.TaskID == "" || rep.Endpoint == "" {
		p.log.Error("quarantining malformed pending report", "file", name)
		if rerr := os.Rename(filepath.Join(p.dir, name), filepath.Join(p.dir, name+".bad")); rerr != nil {
			p.log.Error("quarantine failed", "file", name, "err", rerr)
		}
		return pendingReport{}, false
	}
	return rep, true
}

func (p *PendingReports) writeLocked(rep pendingReport) error {
	raw, err := json.Marshal(rep)
	if err != nil {
		return fmt.Errorf("encode pending report: %w", err)
	}
	tmp := p.path(rep.TaskID) + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write pending report: %w", err)
	}
	if err := os.Rename(tmp, p.path(rep.TaskID)); err != nil {
		return fmt.Errorf("commit pending report: %w", err)
	}
	return nil
}

func (p *PendingReports) removeLocked(taskID string) {
	if err := os.Remove(p.path(taskID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		p.log.Error("remove pending report failed", "task_id", taskID, "err", err)
	}
}

func (p *PendingReports) notifyLocked() {
	p.notify(len(p.listLocked()))
}

// notify reports the queue depth to the gauge hook.
func (p *PendingReports) notify(n int) {
	if p.onChange != nil {
		p.onChange(n)
	}
}
