package scheduler

import (
	"bytes"
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
)

// PendingReportTTL caps how long a queued terminal report is retried before
// it is dropped with an error log (§10: 上限 24h).
const PendingReportTTL = 24 * time.Hour

// MaxPendingReports is the queue's retention bound on held entries (§4.6,
// ADR-046 decision 6). It acts at enqueue: once the queue holds this many
// entries, a report for a task it does not already hold is refused and
// dropped with an error (the alarm face). Entries already queued — the ones
// carried on the cluster carrier included — are never removed for the bound;
// the bound is one order above the design's ≤10² working scale (ADR-046), so
// a routine outage never reaches it.
const MaxPendingReports = 1024

// ErrPendingQueueFull reports a report the retention bound refused: it was
// not enqueued, and the error is the alarm face (§4.6, ADR-046 decision 6).
var ErrPendingQueueFull = errors.New("pending-report queue at its retention bound")

// EnvStateRoot is FOREMAN_STATE_ROOT: the node-local state root the queue
// lives under (the same root the GC module reclaims).
const EnvStateRoot = "FOREMAN_STATE_ROOT"

// DefaultStateRoot is the state root applied when FOREMAN_STATE_ROOT is unset.
const DefaultStateRoot = "/var/lib/foreman"

// PendingReportsDir resolves the durable queue directory:
// FOREMAN_STATE_ROOT/pending-reports (contract §10).
func PendingReportsDir(getenv func(string) string) string {
	if getenv == nil {
		getenv = os.Getenv
	}
	root := strings.TrimSpace(getenv(EnvStateRoot))
	if root == "" {
		root = DefaultStateRoot
	}
	return filepath.Join(root, "pending-reports")
}

// ReportSender re-delivers a queued terminal report through the scheduler's
// single terminal face; satisfied by *Scheduler (DeliverQueued). A replay is
// not an inbound report: it never enters the attribution gate, and it sends
// the instance's decided body when one is registered (ADR-031).
type ReportSender interface {
	DeliverQueued(ctx context.Context, ep Endpoint, taskID, workspaceID string, body []byte, failureReason string) (int, []byte, error)
}

// pendingReport is one queued terminal report (contract §10). Its disk file is
// one face of the queue, not the queue itself: until a face holds the entry it
// lives staged in memory, and an id the disk cannot name stays queued on that
// copy alone.
type pendingReport struct {
	TaskID   string `json:"task_id"`
	JobName  string `json:"job_name"`
	DaemonID string `json:"daemon_id"`
	// WorkspaceID is the credential the replay must use when the entry is
	// gone: a queued report can outlive its credential, and a multi-workspace
	// deployment has no single client to fall back to (§10, ADR-047).
	WorkspaceID string          `json:"workspace_id,omitempty"`
	Endpoint    string          `json:"endpoint"`
	Body        json.RawMessage `json:"body"`
	EnqueuedAt  time.Time       `json:"enqueued_at"`
	Attempts    int             `json:"attempts"`
	// FailureReason is the verdict's attribution when the queued report is a
	// failure Foreman judged itself (§4.1). It rides the queue entry because
	// the projection is rebuilt from it after a restart; the body is never
	// the source (an inbound failure body carries the same key).
	FailureReason string `json:"failure_reason,omitempty"`
}

var taskIDFileChars = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

// validTaskID gates every task id that may name a queue file, so a task id
// like ".." or "a/b" can never turn into a path outside the queue directory.
func validTaskID(taskID string) bool { return taskIDFileChars.MatchString(taskID) }

// PendingReports is the durable terminal-report queue
// (FOREMAN_STATE_ROOT/pending-reports/<taskid>.json, contract §10). Its host
// is the scheduler (ADR-031): every enqueue carries the body that was actually
// forwarded, and the recovery loop drains the queue through the Settler seam,
// so there is one queue behind every terminal-report path.
type PendingReports struct {
	dir      string
	now      func() time.Time
	log      *slog.Logger
	carriers CarrierStore

	mu       sync.Mutex
	onChange func(int) // foreman_pending_reports gauge hook

	// carrierLocks serializes the carrier write and delete of one task id. A
	// carrier is one entry's cross-process face (ADR-046), and PutCarrier
	// reads an existing object (AlreadyExists) as its landed write: a delete
	// issued on "no entry holds this task" must not land between that write
	// and the membership check, or the entry keeps arriving as carrier-backed
	// with no object behind it and a process exit loses it (§10). The key is
	// the task id because the invariant is per task: distinct tasks own
	// distinct objects, so a slow cluster call holds up only its own task.
	// This lock is always taken before mu, never the other way round; mu is
	// never held across these calls, so Len never waits on the network and
	// Enqueue waits only for its own carrier write.
	carrierLocks *keyedMutex

	// maxEntries is the retention bound on held entries (§4.6, ADR-046
	// decision 6); Enqueue refuses a new entry once the queue holds this many.
	maxEntries int

	// staged holds the reports the disk has not taken yet. A failed disk write
	// is retried on every Drain; a crash loses it only if neither face landed.
	// A task id that cannot name a queue file keeps its carrier as the
	// durability face and its in-memory copy as the queue itself: Drain
	// retries that carrier write and delivers from staging, and the entry
	// reads as queued and counts on the gauge until it leaves.
	staged map[string]pendingReport

	// quarantineTried remembers malformed file names whose isolation was
	// already attempted: a rename that keeps failing (permissions, a taken
	// .bad name) is not retried and re-logged every Drain.
	quarantineTried map[string]bool

	// carrierBacked holds the task ids whose report has landed on its
	// cluster carrier (ADR-046). The carrier, not the node-local disk, is the
	// cross-process persistence face: a restored report is carried here even
	// when the disk write fails, so it stays queued and is replayed.
	carrierBacked map[string]bool

	// restored is set once the carrier list completed (successfully or not
	// configured). A failed list leaves it false: the queue is not rebuilt,
	// and no settlement may read it as empty (§4.3). Without a carrier store
	// the disk is the whole persistence face, so there is nothing to rebuild.
	restored bool
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

// WithPendingReportsCarrier wires the cluster carrier store (ADR-046). Nil
// keeps the node-local queue only — the shape tests and degraded builds use.
func WithPendingReportsCarrier(c CarrierStore) PendingReportsOption {
	return func(p *PendingReports) { p.carriers = c }
}

// WithPendingReportsMaxEntries overrides the retention bound on held entries
// (§4.6, ADR-046 decision 6). A non-positive value keeps the default: the
// contract's bound is what makes the queue finite, so there is no "off".
func WithPendingReportsMaxEntries(n int) PendingReportsOption {
	return func(p *PendingReports) {
		if n > 0 {
			p.maxEntries = n
		}
	}
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
		dir:             dir,
		now:             time.Now,
		log:             slog.Default().With("component", "pending-reports"),
		staged:          make(map[string]pendingReport),
		quarantineTried: make(map[string]bool),
		carrierBacked:   make(map[string]bool),
		carrierLocks:    newKeyedMutex(),
		maxEntries:      MaxPendingReports,
	}
	for _, opt := range opts {
		opt(p)
	}
	// Without a carrier store the node-local queue is complete at open: no
	// cross-process list can still add entries.
	p.restored = p.carriers == nil
	return p, nil
}

// Enqueue lands a terminal report on disk (atomic tmp+rename) and on its
// cluster carrier (ADR-046). The disk copy is the in-process queue; the
// carrier is what survives a process exit, so a report is only durable once
// its carrier write landed.
// Re-enqueueing the same task replaces the body but keeps the original
// enqueue time, so the TTL clocks the first failure.
//
// A write that does not land keeps the report staged in memory instead of
// dropping it: Drain retries the write (错误处理表「退避后重试」) and Queued
// reports it as undelivered, so the task's objects stay until it landed. The
// returned error is that error row's "记 error" half — never a reason to
// discard the report. It names a lost durability face, so a task id the disk
// cannot name is only an error when no carrier can hold it either.
//
// The retention bound (§4.6, ADR-046 decision 6) acts here, on new entries
// only: once the queue holds maxEntries reports, one for a task it does not
// already hold is refused with ErrPendingQueueFull and dropped with an error
// row (the alarm face). A re-enqueue replaces a body and never grows the
// queue, so it is not a new entry; entries already queued — carrier-backed
// ones included — are never removed to make room. Capacity the process cannot
// see — an unlistable queue directory, a carrier backlog the rebuild has not
// landed — reads as reached, never as room (unknown is not empty, §4.3).
func (p *PendingReports) Enqueue(ctx context.Context, taskID, jobName, daemonID, workspaceID string, ep Endpoint, body []byte, failureReason string) error {
	p.mu.Lock()
	// A report the queue already holds — staged, carrier-backed, or on disk —
	// is a replacement (§4.6): it never grows the queue, so it is admitted
	// without judging capacity. The disk face is the directory listing when
	// that can be read; when it cannot, the task's own file is still
	// probeable by name, so an already-queued report stays a replacement
	// instead of drawing a depth-unknown drop.
	_, staged := p.staged[taskID]
	if held := p.carrierBacked[taskID] || staged; !held {
		diskIDs, diskErr := p.diskIDsLocked()
		if diskErr == nil {
			_, held = diskIDs[taskID]
		} else if validTaskID(taskID) {
			if _, statErr := os.Stat(p.path(taskID)); statErr == nil {
				held = true
			}
		}
		if !held {
			if reason, cause := p.refusalLocked(diskIDs, diskErr); reason != "" {
				p.mu.Unlock()
				fields := []any{
					"task_id", taskID, "job_name", jobName, "daemon_id", daemonID,
					"endpoint", string(ep), "bound", p.maxEntries, "reason", reason,
				}
				if cause != nil {
					fields = append(fields, "err", cause)
				}
				p.log.Error("task.forward_dropped", fields...)
				return fmt.Errorf("%w: %s", ErrPendingQueueFull, reason)
			}
		}
	}
	rep := pendingReport{
		TaskID:      taskID,
		JobName:     jobName,
		DaemonID:    daemonID,
		WorkspaceID: workspaceID,
		Endpoint:    string(ep),
		// The caller's buffer is not retained: a queued report outlives the
		// request that carried it (staged, then replayed), so a buffer the
		// caller reuses must not rewrite a queued body.
		Body:          json.RawMessage(bytes.Clone(body)),
		EnqueuedAt:    p.now(),
		FailureReason: failureReason,
	}
	p.inheritClockLocked(&rep)
	var diskErr error
	if !validTaskID(taskID) {
		// The id can never name a queue file: keep the report staged so the
		// drain delivers it straight from memory instead of losing it. Its
		// carrier is the only durability face left; without one, the file
		// name is the loss face and the caller sees it.
		p.staged[taskID] = rep
		if p.carriers == nil {
			diskErr = fmt.Errorf("task id %q is not a safe file name", taskID)
		}
	} else if err := p.writeLocked(rep); err != nil {
		diskErr = err
		p.staged[taskID] = rep
	} else {
		delete(p.staged, taskID)
		// Every terminal report lands here before its forward (write-ahead),
		// so this is the queue's trace line, not an error signal: the failed
		// forward is what the error face reports (§9.1).
		p.log.Debug("task.forward_queued",
			"task_id", taskID, "job_name", rep.JobName, "daemon_id", rep.DaemonID,
			"endpoint", rep.Endpoint)
	}
	p.notifyLocked()
	p.mu.Unlock()

	// The carrier is the durability face (ADR-046): a report whose carrier
	// write fails stays in process memory and is retried on the next drain.
	carrierErr := p.persistCarrier(ctx, rep)
	if diskErr != nil {
		return diskErr
	}
	return carrierErr
}

// persistCarrier lands one report on its cluster carrier. A report that
// settles while the write is in flight leaves an orphan carrier behind — the
// post-write check deletes it instead of resurrecting a settled entry.
//
// The gate is the carrier's own identity, not the queue file's: a task id the
// file-name grammar rejects ("." or "_" in it) is a legal label value and keeps
// its cross-process face. An id the label grammar cannot carry has no carrier
// — that is an error face and the entry stays in process memory for retry
// (§10, ADR-046 decision 2), never a silent skip.
func (p *PendingReports) persistCarrier(ctx context.Context, rep pendingReport) error {
	if p.carriers == nil {
		return nil
	}
	if !carrierTaskIDValid(rep.TaskID) {
		err := fmt.Errorf("%w: %q", ErrCarrierTaskIDInvalid, rep.TaskID)
		p.log.Error("task.forward_undelivered",
			"task_id", rep.TaskID, "job_name", rep.JobName, "daemon_id", rep.DaemonID,
			"workspace_id", rep.WorkspaceID,
			"endpoint", rep.Endpoint,
			"reason", "pending-report carrier cannot carry the task id", "err", err)
		return err
	}
	carrier := Carrier{
		TaskID:        rep.TaskID,
		JobName:       rep.JobName,
		DaemonID:      rep.DaemonID,
		WorkspaceID:   rep.WorkspaceID,
		Endpoint:      rep.Endpoint,
		Body:          rep.Body,
		EnqueuedAt:    rep.EnqueuedAt,
		Attempts:      rep.Attempts,
		FailureReason: rep.FailureReason,
	}
	if n := carrier.sizeBytes(); n > MaxCarrierBytes {
		// Registered boundary: the report is kept in process memory and the
		// error face reports the loss on process exit (ADR-046 decision 6).
		err := fmt.Errorf("%w: %d bytes > %d", ErrCarrierPayloadTooLarge, n, MaxCarrierBytes)
		p.log.Error("task.forward_undelivered",
			"task_id", rep.TaskID, "job_name", rep.JobName, "daemon_id", rep.DaemonID,
			"workspace_id", rep.WorkspaceID,
			"endpoint", rep.Endpoint,
			"reason", "pending report exceeds the carrier object bound", "err", err)
		return err
	}

	// The write, the membership check that follows it and every carrier
	// delete for this task take the task's carrier lock: a delete for a task
	// that reads as settled must not remove the object this report's session
	// just adopted (AlreadyExists is a landed write here).
	unlock := p.carrierLocks.Lock(rep.TaskID)
	defer unlock()
	if err := p.carriers.PutCarrier(ctx, carrier); err != nil {
		if errors.Is(err, ErrCarrierPayloadTooLarge) {
			// Registered boundary: the report is kept in process memory and the
			// error face reports the loss on process exit (ADR-046 decision 6).
			p.log.Error("task.forward_undelivered",
				"task_id", rep.TaskID, "job_name", rep.JobName, "daemon_id", rep.DaemonID,
				"workspace_id", rep.WorkspaceID,
				"endpoint", rep.Endpoint,
				"reason", "pending report exceeds the carrier object bound", "err", err)
			return err
		}
		p.log.Error("task.forward_undelivered",
			"task_id", rep.TaskID, "job_name", rep.JobName, "daemon_id", rep.DaemonID,
			"workspace_id", rep.WorkspaceID,
			"endpoint", rep.Endpoint,
			"reason", "pending-report carrier write failed", "err", err)
		return err
	}

	p.mu.Lock()
	if !p.entryPresentLocked(rep.TaskID) {
		// The direct forward settled the report while the carrier write was
		// in flight: the carrier must not outlive the queue entry.
		p.mu.Unlock()
		p.deleteCarrierLocked(ctx, rep.TaskID)
		return nil
	}
	p.carrierBacked[rep.TaskID] = true
	p.notifyLocked()
	p.mu.Unlock()
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

// Drain retries the staged disk writes and the missing carrier writes, then
// sends every queued report once through sender. A report leaves the queue
// when the server accepts it (2xx) or settles it as moot (404/409: server-side
// already terminal or deleted). Reports older than PendingReportTTL are
// dropped with an error log (§10). It returns the number of reports still
// undelivered: the disk entries, the carrier-backed ones, and the ones a
// failed write left staged.
//
// A report whose task id cannot name a queue file has the carrier as its only
// durability face: a landed carrier keeps it across processes, and only one
// that cannot land is delivered straight from staging — dropping it would
// lose a terminal callback (contract §10).
//
// The lock is never held across the network: the queue is snapshotted,
// forwarded, and settled entry-by-entry, so a slow upstream cannot block
// Enqueue (the daemon's terminal-report path) or Len (metrics).
func (p *PendingReports) Drain(ctx context.Context, sender ReportSender) (int, error) {
	p.mu.Lock()
	expired := p.flushStagedLocked()
	retryCarriers := p.unbackedLocked()
	p.mu.Unlock()
	// A report whose carrier write failed is retried every drain
	// (ADR-046 decision 2): until it lands, a process exit loses it.
	for _, rep := range retryCarriers {
		_ = p.persistCarrier(ctx, rep)
	}
	for _, taskID := range expired {
		p.deleteCarrier(ctx, taskID)
	}

	p.mu.Lock()
	queue := p.drainQueueLocked()
	p.mu.Unlock()

	var firstErr error
	for _, rep := range queue {
		if p.now().Sub(rep.EnqueuedAt) > PendingReportTTL {
			p.log.Error("task.forward_dropped",
				"task_id", rep.TaskID, "job_name", rep.JobName, "daemon_id", rep.DaemonID,
				"endpoint", rep.Endpoint,
				"attempts", rep.Attempts, "reason", "pending report TTL exceeded")
			p.mu.Lock()
			deleteCarrier := p.dropSettledLocked(rep)
			p.mu.Unlock()
			if deleteCarrier {
				p.deleteCarrier(ctx, rep.TaskID)
			}
			continue
		}
		code, _, err := sender.DeliverQueued(ctx, Endpoint(rep.Endpoint), rep.TaskID, rep.WorkspaceID, rep.Body, rep.FailureReason)
		delivered := err == nil && terminalReportDelivered(code)
		p.mu.Lock()
		var deleteCarrier bool
		if delivered {
			p.log.Info("task.forward_delivered",
				"task_id", rep.TaskID, "job_name", rep.JobName, "daemon_id", rep.DaemonID,
				"endpoint", rep.Endpoint, "attempts", rep.Attempts+1)
			// Drop only the snapshot that was just delivered: a concurrent
			// Enqueue may have replaced the body (the decided body, a late
			// replay) while the forward was in flight, and deleting it by task
			// id would lose a report §10 forbids dropping. The carrier goes
			// only when no queue entry remains.
			deleteCarrier = p.dropSettledLocked(rep)
		} else {
			p.bumpAttemptsLocked(rep.TaskID)
		}
		p.mu.Unlock()
		if deleteCarrier {
			p.deleteCarrier(ctx, rep.TaskID)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if err == nil && !delivered && firstErr == nil {
			firstErr = fmt.Errorf("task %s: server returned %d", rep.TaskID, code)
		}
	}
	p.mu.Lock()
	remaining := p.undeliveredLocked()
	gauge := p.lenLocked()
	p.mu.Unlock()
	p.notify(gauge)
	return remaining, firstErr
}

// drainQueueLocked snapshots the reports this round delivers: the disk
// entries plus the staged ones that can never reach the disk or that are
// already durable on their carrier. A storable, carrier-less staged report
// waits for its write — the disk is its only durability face.
func (p *PendingReports) drainQueueLocked() []pendingReport {
	seen := make(map[string]struct{})
	queue := make([]pendingReport, 0)
	for _, rep := range p.listLocked() {
		seen[rep.TaskID] = struct{}{}
		queue = append(queue, rep)
	}
	for taskID, rep := range p.staged {
		if _, ok := seen[taskID]; ok {
			continue
		}
		if !validTaskID(taskID) || p.carrierBacked[taskID] {
			queue = append(queue, rep)
		}
	}
	return queue
}

// unbackedLocked lists the queued reports whose carrier write has not
// landed yet, so Drain can retry it.
func (p *PendingReports) unbackedLocked() []pendingReport {
	if p.carriers == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var out []pendingReport
	for _, rep := range p.listLocked() {
		seen[rep.TaskID] = struct{}{}
		if !p.carrierBacked[rep.TaskID] {
			out = append(out, rep)
		}
	}
	for taskID, rep := range p.staged {
		if _, ok := seen[taskID]; ok {
			continue
		}
		if !p.carrierBacked[taskID] {
			// A staged report whose id cannot name a queue file still has
			// the carrier face — that is the only durability it can get.
			out = append(out, rep)
		}
	}
	return out
}

// flushStagedLocked retries the disk write of every staged report
// (错误处理表「退避后重试」). A report whose write lands leaves the staging
// area — listLocked then carries it — while a write that keeps failing stays
// staged for the next round. A report past the TTL is dropped, and one whose
// id can never name a file stays staged for the direct delivery.
//
// It returns the TTL-dropped task ids whose carrier must be deleted: the
// caller holds no lock then, because the delete is a network call.
func (p *PendingReports) flushStagedLocked() []string {
	var expired []string
	for taskID, rep := range p.staged {
		if p.now().Sub(rep.EnqueuedAt) > PendingReportTTL {
			p.log.Error("task.forward_dropped",
				"task_id", taskID, "job_name", rep.JobName, "daemon_id", rep.DaemonID,
				"endpoint", rep.Endpoint,
				"attempts", rep.Attempts, "reason", "pending report TTL exceeded")
			if p.carrierBacked[taskID] {
				expired = append(expired, taskID)
			}
			delete(p.staged, taskID)
			delete(p.carrierBacked, taskID)
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
			"task_id", taskID, "job_name", rep.JobName, "daemon_id", rep.DaemonID,
			"endpoint", rep.Endpoint, "retried_write", true)
		delete(p.staged, taskID)
	}
	return expired
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

// dropIfUnchangedLocked removes the queue record only when it still holds the
// snapshot that was just delivered (or TTL-dropped). A concurrent Enqueue may
// have replaced the body while the forward was in flight; the newer record then
// stays queued for the next drain instead of being deleted by task id.
func (p *PendingReports) dropIfUnchangedLocked(sent pendingReport) {
	if validTaskID(sent.TaskID) {
		if cur, ok := p.readLocked(sent.TaskID); ok && !samePendingReport(cur, sent) {
			return
		}
		p.removeLocked(sent.TaskID)
	}
	if cur, ok := p.staged[sent.TaskID]; ok && samePendingReport(cur, sent) {
		delete(p.staged, sent.TaskID)
	}
}

// samePendingReport reports whether a queue record still holds the snapshot
// sent. The endpoint and the body are one enqueue's identity; the task id is
// not, because a concurrent enqueue for the same task replaces both.
func samePendingReport(sent, cur pendingReport) bool {
	return cur.Endpoint == sent.Endpoint && sameReportBody(cur.Body, sent.Body)
}

// sameReportBody compares two bodies as the queue encodes them. The disk face
// stores a pendingReport through json.Marshal, which compacts the body and
// escapes <, > and &, so a caller's raw bytes only match the encoded record by
// luck: a body carrying whitespace or those characters would never compare
// equal and a rollback would silently keep the entry. The fast path is the
// queue's own copy against itself; the encoding runs only when the bytes
// differ. A body that does not encode has no canonical form, and the two then
// match only verbatim — never dropped on a guess (§10).
func sameReportBody(a, b []byte) bool {
	if bytes.Equal(a, b) {
		return true
	}
	encodedA, err := json.Marshal(json.RawMessage(a))
	if err != nil {
		return false
	}
	encodedB, err := json.Marshal(json.RawMessage(b))
	if err != nil {
		return false
	}
	return bytes.Equal(encodedA, encodedB)
}

// dropSettledLocked removes the snapshot a drain just settled and reports
// whether its carrier must be deleted. The carrier goes only when no queue
// entry remains: a concurrent re-enqueue keeps its own body on the carrier
// and settles later.
func (p *PendingReports) dropSettledLocked(sent pendingReport) (deleteCarrier bool) {
	p.dropIfUnchangedLocked(sent)
	if p.entryPresentLocked(sent.TaskID) {
		return false
	}
	wasBacked := p.carrierBacked[sent.TaskID]
	delete(p.carrierBacked, sent.TaskID)
	return wasBacked
}

// entryPresentLocked reports whether a queue entry (disk or staged) still
// holds taskID. The carrier alone is not an entry — it is one entry's
// durability face.
func (p *PendingReports) entryPresentLocked(taskID string) bool {
	if validTaskID(taskID) {
		if _, ok := p.readLocked(taskID); ok {
			return true
		}
	}
	_, ok := p.staged[taskID]
	return ok
}

// diskIDsLocked is the queue's disk face as task ids, read from the directory's
// file names only: the capacity bound is an entry count, and a report body can
// be MiB-sized, so the admission gate never opens or decodes one. A directory
// that cannot be listed is an error — unknown, never an empty queue (§4.3).
func (p *PendingReports) diskIDsLocked() (map[string]struct{}, error) {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return nil, fmt.Errorf("list pending-reports dir: %w", err)
	}
	ids := make(map[string]struct{}, len(entries))
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		taskID, ok := strings.CutSuffix(ent.Name(), ".json")
		if !ok || !validTaskID(taskID) {
			continue
		}
		ids[taskID] = struct{}{}
	}
	return ids, nil
}

// heldCountLocked counts the entries the queue holds, deduplicated by task id
// across its faces: the disk files (by name), the staged reports and the
// carrier-backed ones.
func (p *PendingReports) heldCountLocked(diskIDs map[string]struct{}) int {
	n := len(diskIDs)
	for taskID := range p.staged {
		if _, ok := diskIDs[taskID]; !ok {
			n++
		}
	}
	for taskID := range p.carrierBacked {
		if _, ok := diskIDs[taskID]; ok {
			continue
		}
		if _, ok := p.staged[taskID]; ok {
			continue
		}
		n++
	}
	return n
}

// refusalLocked answers the admission gate for a new entry: the reason it may
// not join the queue, and the cause when the reason is a face it could not
// read. An empty reason means admitted.
//
// The unknown faces read as reached (unknown is not empty, §4.3): a directory
// that cannot be listed hides entries the process already holds, and a carrier
// backlog the rebuild has not landed hides the previous process's entries.
// Admitting against either is exactly what would let the queue grow past its
// bound. Refusing does not weaken §10's write-ahead: the direct forward still
// runs, the write-ahead ordering is unchanged, and the refusal itself is
// §4.6's registered alarm-face drop.
func (p *PendingReports) refusalLocked(diskIDs map[string]struct{}, diskErr error) (string, error) {
	switch {
	case diskErr != nil:
		return "pending report queue depth unknown: queue directory cannot be listed", diskErr
	case p.carriers != nil && !p.restored:
		return "pending report queue depth unknown: carrier backlog not rebuilt", nil
	case p.heldCountLocked(diskIDs) >= p.maxEntries:
		return "pending report queue bound reached", nil
	}
	return "", nil
}

// dropLocked removes a report from the queue, wherever it sits: the durable
// file, the staging area, or both. The carrier is the caller's (it needs the
// network), see Drop.
func (p *PendingReports) dropLocked(taskID string) {
	if validTaskID(taskID) {
		p.removeLocked(taskID)
	}
	delete(p.staged, taskID)
	delete(p.carrierBacked, taskID)
}

// queuedKnownLocked answers Queued from the entries the process actually
// holds: the carrier-backed ids, the staged reports and the disk files. A
// staged report is an entry the process holds whichever durability face it can
// still reach — an id the disk cannot name does not make its report delivered
// (Drain delivers it straight from staging).
func (p *PendingReports) queuedKnownLocked(taskID string) bool {
	if p.carrierBacked[taskID] {
		return true
	}
	if _, ok := p.staged[taskID]; ok {
		return true
	}
	if !validTaskID(taskID) {
		return false
	}
	_, ok := p.readLocked(taskID)
	return ok
}

// queuedLocked is the settlement gate: while the carrier list has not
// completed, the queue's state is unknown, and "unknown" must never read as
// "empty" (§4.3). A task may hold a queued report the rebuild has not seen
// yet, so every task reads as queued until the list lands.
func (p *PendingReports) queuedLocked(taskID string) bool {
	if !p.restored {
		return true
	}
	return p.queuedKnownLocked(taskID)
}

// lenLocked is the queue depth across its faces: the disk entries, the
// carrier-backed ones and the staged reports the disk can never take — an id
// the file grammar rejects has no disk face to wait for, so the in-memory copy
// is its queue face until its carrier lands (deduplicated by task id). A
// staged report a later write can still land is a write in flight and stays
// off the gauge until it lands.
func (p *PendingReports) lenLocked() int {
	seen := make(map[string]struct{})
	for _, rep := range p.listLocked() {
		seen[rep.TaskID] = struct{}{}
	}
	for taskID := range p.carrierBacked {
		seen[taskID] = struct{}{}
	}
	for taskID := range p.staged {
		if !validTaskID(taskID) {
			seen[taskID] = struct{}{}
		}
	}
	return len(seen)
}

// undeliveredLocked is the drain's report count: the durable entries plus
// the staged ones a failed write left in memory — every report the queue
// holds, whichever face holds it.
func (p *PendingReports) undeliveredLocked() int {
	seen := make(map[string]struct{})
	for _, rep := range p.listLocked() {
		seen[rep.TaskID] = struct{}{}
	}
	for taskID := range p.carrierBacked {
		seen[taskID] = struct{}{}
	}
	for taskID := range p.staged {
		seen[taskID] = struct{}{}
	}
	return len(seen)
}

// Len reports the queue depth — the foreman_pending_reports gauge. A report
// counts once a durability face holds it (the disk write or the carrier
// write); a report only reachable through staging counts once the disk can no
// longer take it, because that staging copy is then the queue itself
// (lenLocked).
func (p *PendingReports) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lenLocked()
}

// Queued reports whether taskID still has an undelivered report: one whose
// file exists, one a failed write left staged, or one carried on the cluster
// carrier. The scheduler gates the verdict-time Job deletion of a terminal
// entry on it, so the Job goes only once the report landed (contract §10) —
// the credential and the entry follow their own drain window (ADR-017).
//
// While the carrier list has not completed, the queue's state is unknown and
// every task answers true: a failed or unfinished rebuild must not let a
// settlement read the queue as empty (§4.3). Rebuilt reports whether the
// list completed; HasQueuedEntry is the known-membership face for callers
// deciding whether to write, not whether to settle.
//
// A task id that cannot name a queue file is answered from the in-memory
// entries only: the node-local copy can never hold it, so its staged report
// and its carrier are the two faces that make it queued.
func (p *PendingReports) Queued(taskID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.queuedLocked(taskID)
}

// Rebuilt reports whether the carrier list completed, so the queue's state is
// authoritative. A queue without a carrier store is complete at open (the
// disk is its whole persistence face). Settlement paths check it before
// acting: an uncompleted rebuild is not an empty queue (§4.3).
func (p *PendingReports) Rebuilt() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.restored
}

// HasQueuedEntry reports whether a queue entry the process actually holds
// exists for taskID. Unlike Queued it does not treat an uncompleted carrier
// list as "queued": it answers a write decision (does this report still need
// a write-ahead entry?), where an unknown list must not suppress the write.
func (p *PendingReports) HasQueuedEntry(taskID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.queuedKnownLocked(taskID)
}

// Drop forgets the report queued for taskID: the direct forward it was
// written ahead of settled it (accepted, moot, or permanently refused), so no
// replay is owed. A no-op when nothing is queued. The gauge follows the queue
// depth — every entry the queue holds, whichever face holds it; a carrier is
// deleted with the entry (ADR-046 decision 3).
func (p *PendingReports) Drop(ctx context.Context, taskID string) {
	p.mu.Lock()
	wasBacked := p.carrierBacked[taskID]
	p.dropLocked(taskID)
	p.notifyLocked()
	p.mu.Unlock()
	if wasBacked {
		// The sweep is the retry handle: a settled carrier whose delete
		// failed is removed on the next round (§6.5).
		p.deleteCarrier(ctx, taskID)
	}
}

// DropIfUnchanged forgets the report a caller wrote ahead of a verdict that
// then failed to land, and only while the queue still holds that same
// snapshot. A concurrent enqueue for the same task — the attribution gate's
// compensation, a late report — has replaced the endpoint and body, and that
// newer entry keeps its queue home and its carrier (§10): dropping it by task
// id would leave its own forward without a write-ahead entry.
func (p *PendingReports) DropIfUnchanged(ctx context.Context, taskID string, ep Endpoint, body []byte) {
	sent := pendingReport{TaskID: taskID, Endpoint: string(ep), Body: body}
	p.mu.Lock()
	// The faces are compared one by one: a newer body on the disk never hides
	// this snapshot in staging, and the staged copy is only the latest write
	// that did not land.
	if validTaskID(taskID) {
		if cur, ok := p.readLocked(taskID); ok && samePendingReport(sent, cur) {
			p.removeLocked(taskID)
		}
	}
	if cur, ok := p.staged[taskID]; ok && samePendingReport(sent, cur) {
		delete(p.staged, taskID)
	}
	wasBacked := false
	if !p.entryPresentLocked(taskID) {
		wasBacked = p.carrierBacked[taskID]
		delete(p.carrierBacked, taskID)
	}
	p.notifyLocked()
	p.mu.Unlock()
	if wasBacked {
		p.deleteCarrier(ctx, taskID)
	}
}

// deleteCarrier removes one carrier on behalf of a settled entry. The
// membership check and the delete share the task's carrier lock with the
// carrier writes: a duplicate terminal report can enqueue the same task while
// the delete is pending, and its carrier write answers AlreadyExists for this
// very object. Removing it then would leave a live entry carrier-backed with
// no carrier, which is the loss AC-16 forbids on process exit.
func (p *PendingReports) deleteCarrier(ctx context.Context, taskID string) {
	unlock := p.carrierLocks.Lock(taskID)
	defer unlock()
	p.deleteCarrierLocked(ctx, taskID)
}

// deleteCarrierLocked is deleteCarrier with the task's carrier lock held.
func (p *PendingReports) deleteCarrierLocked(ctx context.Context, taskID string) {
	if p.carriers == nil {
		return
	}
	p.mu.Lock()
	present := p.entryPresentLocked(taskID)
	p.mu.Unlock()
	if present {
		return
	}
	if err := p.carriers.DeleteCarrier(ctx, carrierSecretName(taskID)); err != nil {
		p.log.Error("pending carrier delete failed", "task_id", taskID, "err", err)
	}
}

// SweepCarrier deletes one listed carrier object when no queue entry holds
// its task — the sweep's only criterion (§6.5: carriers are swept by
// settlement, never by the retention bound or by Job absence). The membership
// check and the delete take the task's carrier lock with the carrier writes,
// so an enqueue that lands in between cannot adopt the object this sweep
// removes. An uncompleted carrier list reads every task as queued (§4.3), so a
// sweep before the rebuild deletes nothing. It reports whether the object
// went. The delete targets the listed name, not a name derived from the task
// id: the name form is not a selection surface (§6.5, ADR-046 decision 1).
func (p *PendingReports) SweepCarrier(ctx context.Context, taskID, name string) (bool, error) {
	if p.carriers == nil {
		return false, nil
	}
	unlock := p.carrierLocks.Lock(taskID)
	defer unlock()
	p.mu.Lock()
	queued := p.queuedLocked(taskID)
	p.mu.Unlock()
	if queued {
		return false, nil
	}
	if err := p.carriers.DeleteCarrier(ctx, name); err != nil {
		return false, err
	}
	return true, nil
}

// Restore rebuilds the queue from its carriers (§4.3). It is the same
// recovery as the index rebuild: a restored report keeps its enqueue time and
// body, so a replay sends exactly the body that was enqueued. A list failure
// leaves the queue unrestored — the caller must not read it as empty — and a
// later call retries.
func (p *PendingReports) Restore(ctx context.Context) error {
	p.mu.Lock()
	if p.restored || p.carriers == nil {
		p.restored = true
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	carriers, err := p.carriers.ListCarriers(ctx)
	if err != nil {
		return fmt.Errorf("restore pending reports: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	queued := 0
	for _, c := range carriers {
		rep := pendingReport{
			TaskID:        c.TaskID,
			JobName:       c.JobName,
			DaemonID:      c.DaemonID,
			WorkspaceID:   c.WorkspaceID,
			Endpoint:      c.Endpoint,
			Body:          c.Body,
			EnqueuedAt:    c.EnqueuedAt,
			Attempts:      c.Attempts,
			FailureReason: c.FailureReason,
		}
		if rep.TaskID == "" || rep.Endpoint == "" {
			p.log.Error("pending carrier malformed",
				"secret", c.Name, "task_id", rep.TaskID, "endpoint", rep.Endpoint)
			continue
		}
		if rep.EnqueuedAt.IsZero() {
			// A carrier without a clock reading is still a real report; the
			// TTL starts now rather than dropping it unread.
			rep.EnqueuedAt = p.now()
		}
		already := p.queuedKnownLocked(rep.TaskID)
		p.carrierBacked[rep.TaskID] = true
		if !already {
			// The disk (a container restart keeps the emptyDir) is the
			// in-process copy; it wins when both exist, because it was
			// written first and a re-enqueue keeps the original clock.
			p.staged[rep.TaskID] = rep
			queued++
		}
	}
	p.restored = true
	p.notifyLocked()
	p.log.Info("pending.rebuilt", "carriers", len(carriers), "queued", queued)
	return nil
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
		if p.quarantineTried[name] {
			// A previous isolation attempt failed; retrying it every Drain
			// would only repeat the two error lines.
			return pendingReport{}, false
		}
		p.quarantineTried[name] = true
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
	p.notify(p.lenLocked())
}

// notify reports the queue depth to the gauge hook.
func (p *PendingReports) notify(n int) {
	if p.onChange != nil {
		p.onChange(n)
	}
}
