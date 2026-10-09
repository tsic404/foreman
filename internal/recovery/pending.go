// Package recovery implements the failure-compensation and recovery side of
// Foreman: the durable pending terminal-report queue, the restart/failure
// reconciler over the Job/Pod facts and the server's C13 truth, the K8s
// object watcher, the heartbeat-suspect signal and the cancel-ack timeout.
// Design: docs/05-modules/failure-handling.md, contracts §3.4/§4.
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
	"strings"
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
// the queue: a report is durable iff its file exists (staged until it lands).
type pendingReport struct {
	TaskID     string          `json:"task_id"`
	Endpoint   string          `json:"endpoint"`
	Body       json.RawMessage `json:"body"`
	EnqueuedAt time.Time       `json:"enqueued_at"`
	Attempts   int             `json:"attempts"`
}

var taskIDFileChars = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

// validTaskID gates every task id that may name a queue file, so a task id
// like ".." or "a/b" can never turn into a path outside the queue directory.
func validTaskID(taskID string) bool { return taskIDFileChars.MatchString(taskID) }

// PendingReports is the durable terminal-report queue
// (FOREMAN_STATE_ROOT/pending-reports/<taskid>.json, contract §4).
type PendingReports struct {
	dir string
	now func() time.Time
	log *slog.Logger

	mu       sync.Mutex
	onChange func(int) // foreman_pending_reports gauge hook

	// staged holds the reports that are not durable yet: a write that failed
	// (failure-handling 错误处理表「待发队列目录不可写：记 error + gauge 不
	// 增长；退避后重试」) or a task id that can never name a queue file. It is
	// the retry handle of that error row: every Drain retries the write,
	// Queued counts the report as undelivered, and the gauge stays at the
	// durable depth. A crash inside the window loses the report — the disk
	// cannot take it either way.
	staged map[string]pendingReport
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
		dir:    dir,
		now:    time.Now,
		log:    slog.Default().With("component", "pending-reports"),
		staged: make(map[string]pendingReport),
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
//
// A write that does not land keeps the report staged in memory instead of
// dropping it: Drain retries the write (错误处理表「退避后重试」) and Queued
// reports it as undelivered, so the task's objects stay until it landed. The
// returned error is that error row's "记 error" half — never a reason to
// discard the report.
func (p *PendingReports) Enqueue(taskID string, ep scheduler.Endpoint, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	rep := pendingReport{
		TaskID:     taskID,
		Endpoint:   string(ep),
		Body:       body,
		EnqueuedAt: p.now(),
	}
	p.inheritClockLocked(&rep)
	if !validTaskID(taskID) {
		// The id can never name a queue file: keep the report staged so the
		// drain delivers it straight from memory instead of losing it.
		p.staged[taskID] = rep
		return fmt.Errorf("task id %q is not a safe file name", taskID)
	}
	if err := p.writeLocked(rep); err != nil {
		p.staged[taskID] = rep
		return err
	}
	delete(p.staged, taskID)
	p.log.Error("task.forward_queued",
		"task_id", taskID, "endpoint", rep.Endpoint)
	p.notifyLocked()
	return nil
}

// inheritClockLocked keeps the enqueue time and attempt count of a report
// the queue already knows (staged in memory or on disk), so the TTL bounds
// the first failure rather than every re-enqueue.
func (p *PendingReports) inheritClockLocked(rep *pendingReport) {
	if prev, ok := p.staged[rep.TaskID]; ok && !prev.EnqueuedAt.IsZero() {
		rep.EnqueuedAt, rep.Attempts = prev.EnqueuedAt, prev.Attempts
		return
	}
	if !validTaskID(rep.TaskID) {
		return
	}
	raw, err := os.ReadFile(p.path(rep.TaskID))
	if err != nil {
		return
	}
	var old pendingReport
	if json.Unmarshal(raw, &old) == nil && !old.EnqueuedAt.IsZero() {
		rep.EnqueuedAt, rep.Attempts = old.EnqueuedAt, old.Attempts
	}
}

// Drain retries the staged writes and then every queued report once through
// sender. A report leaves the queue when the server accepts it (2xx) or
// settles it as moot (404/409: server-side already terminal or deleted).
// Reports older than PendingReportTTL are dropped with an error log
// (failure-handling 场景 #10). It returns the number of reports still
// undelivered: the durable entries plus the ones a failed write left staged.
//
// A report whose task id cannot name a queue file is delivered straight from
// staging: it can never become durable, and dropping it would lose a
// terminal callback (contract §4).
//
// The lock is never held across the network: the queue is snapshotted,
// forwarded, and settled entry-by-entry, so a slow upstream cannot block
// Enqueue (the daemon's terminal-report path) or Len (metrics).
func (p *PendingReports) Drain(ctx context.Context, sender ReportSender) (int, error) {
	p.mu.Lock()
	p.flushStagedLocked()
	queue := append(p.listLocked(), p.unstorableStagedLocked()...)
	p.mu.Unlock()

	var firstErr error
	for _, rep := range queue {
		if p.now().Sub(rep.EnqueuedAt) > PendingReportTTL {
			p.log.Error("task.forward_dropped",
				"task_id", rep.TaskID, "endpoint", rep.Endpoint,
				"attempts", rep.Attempts, "reason", "pending report TTL exceeded")
			p.mu.Lock()
			p.dropLocked(rep.TaskID)
			p.mu.Unlock()
			continue
		}
		code, _, err := sender.Forward(ctx, scheduler.Endpoint(rep.Endpoint), rep.TaskID, rep.Body)
		delivered := err == nil && ((code >= 200 && code < 300) || code == 404 || code == 409)
		p.mu.Lock()
		if delivered {
			p.log.Info("task.forward_delivered",
				"task_id", rep.TaskID, "endpoint", rep.Endpoint, "attempts", rep.Attempts+1)
			p.dropLocked(rep.TaskID)
		} else {
			p.bumpAttemptsLocked(rep.TaskID)
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
	durable := len(p.listLocked())
	remaining := durable + len(p.staged)
	p.mu.Unlock()
	p.notify(durable)
	return remaining, firstErr
}

// flushStagedLocked retries the disk write of every staged report
// (错误处理表「退避后重试」). A report whose write lands leaves the staging
// area — listLocked then carries it, so it counts as queued only once the
// file exists (顺序规则 5) — while a write that keeps failing stays staged
// for the next round. A report past the TTL is dropped, and one whose id can
// never name a file stays staged for the direct delivery.
func (p *PendingReports) flushStagedLocked() {
	for taskID, rep := range p.staged {
		if p.now().Sub(rep.EnqueuedAt) > PendingReportTTL {
			p.log.Error("task.forward_dropped",
				"task_id", taskID, "endpoint", rep.Endpoint,
				"attempts", rep.Attempts, "reason", "pending report TTL exceeded")
			delete(p.staged, taskID)
			continue
		}
		if !validTaskID(taskID) {
			continue
		}
		if err := p.writeLocked(rep); err != nil {
			p.log.Error("pending report write failed",
				"task_id", taskID, "endpoint", rep.Endpoint, "err", err)
			continue
		}
		p.log.Info("task.forward_queued",
			"task_id", taskID, "endpoint", rep.Endpoint, "retried_write", true)
		delete(p.staged, taskID)
	}
}

// unstorableStagedLocked lists the staged reports whose task id can never
// name a queue file; the delivery loop sends them straight from memory.
func (p *PendingReports) unstorableStagedLocked() []pendingReport {
	var out []pendingReport
	for _, rep := range p.staged {
		if !validTaskID(rep.TaskID) {
			out = append(out, rep)
		}
	}
	return out
}

// bumpAttemptsLocked records one more delivery attempt: a durable report is
// rewritten so its count survives a restart, a staged one is bumped in
// memory.
func (p *PendingReports) bumpAttemptsLocked(taskID string) {
	if validTaskID(taskID) {
		if cur, ok := p.readLocked(taskID); ok {
			// Merge with a concurrent re-enqueue: bump the attempt counter
			// on the file as it stands now, never clobbering a newer body.
			cur.Attempts++
			if err := p.writeLocked(cur); err != nil {
				p.log.Error("pending report update failed", "task_id", taskID, "err", err)
			}
			return
		}
	}
	if rep, ok := p.staged[taskID]; ok {
		rep.Attempts++
		p.staged[taskID] = rep
	}
}

// dropLocked removes a report from the queue, wherever it sits: the durable
// file, the staging area, or both.
func (p *PendingReports) dropLocked(taskID string) {
	if validTaskID(taskID) {
		p.removeLocked(taskID)
	}
	delete(p.staged, taskID)
}

// Len reports the durable queue depth — the foreman_pending_reports gauge.
// A report counts once its write succeeded; one a failed write left staged
// does not grow the gauge (错误处理表).
func (p *PendingReports) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.listLocked())
}

// Queued reports whether taskID still has an undelivered report: one whose
// file exists, or one a failed write left staged. The scheduler gates the
// object cleanup of a terminal entry on it, so the Job/Secret go only once
// the report landed (contract §4).
//
// A task id that cannot name a queue file is answered without touching the
// filesystem: such a report is never a durable queue entry (Drain delivers
// it straight from staging), so it gates no objects either.
func (p *PendingReports) Queued(taskID string) bool {
	if !validTaskID(taskID) {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.readLocked(taskID); ok {
		return true
	}
	_, ok := p.staged[taskID]
	return ok
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

// readFileLocked parses one queue file; malformed files are quarantined. A
// name that cannot be a queue file is ignored outright, so no caller can
// make it read or rename anything outside the queue directory.
func (p *PendingReports) readFileLocked(name string) (pendingReport, bool) {
	taskID, ok := strings.CutSuffix(name, ".json")
	if !ok || !validTaskID(taskID) {
		return pendingReport{}, false
	}
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
