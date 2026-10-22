package registry

import (
	"context"
	"time"
)

// EntryPatch carries the in-place field updates the stage-two shared row merge
// applies. Only the fields a single mutation owns travel here: the merge never
// writes a gate field (state, start fact, delivery accounting, terminal
// verdict), so a late merge can never revert a decision another instance made
// (02-contracts.md §4.2 跨实例单写, §5).
type EntryPatch struct {
	LastStatusSeen *string
	LeaseKeepUntil *time.Time
	NodeName       *string
	Result         *string
	FailureReason  *string
	TokenHash      *string
}

// SharedSessionNode is one soft-affinity row read from the shared store: the
// raw body plus the lifetime the store still holds on it (zero when the row
// has no expiry). The lifetime comes from the store's clock — the reader turns
// it into its own clock once instead of comparing two hosts' timestamps.
type SharedSessionNode struct {
	Raw       []byte
	Remaining time.Duration
}

// SharedStore is the stage-two face the registry consumes when the configured
// shared state implements it (internal/state's Redis store does; the phase-one
// test fakes and the no-op store do not). Capability detection keeps a
// deployment without the coordination primitives on the stage-one path.
type SharedStore interface {
	// Shared reports that the coordination primitives are available.
	Shared() bool
	// SetEntry writes the full row (claim time; the caller holds the task
	// write lock).
	SetEntry(ctx context.Context, ws, taskID string, e TaskEntry) error
	// MergeEntry applies the patch to the stored row in place.
	MergeEntry(ctx context.Context, ws, taskID string, p EntryPatch) error
	// ConfirmStart writes the once-only start fact; advance moves the state to
	// running (report path) or leaves it untouched (adoption path).
	ConfirmStart(ctx context.Context, ws, taskID string, at time.Time, advance bool) (TaskEntry, bool, error)
	// CommitJobCreated records the apiserver creation timestamp.
	CommitJobCreated(ctx context.Context, ws, taskID string, at time.Time) (TaskEntry, bool, error)
	// CommitDelivering records one delivery commit (the stage-one direct path;
	// stage two delivers through the queue pickup).
	CommitDelivering(ctx context.Context, ws, taskID string, at time.Time, payload []byte, window time.Duration) (TaskEntry, bool, error)
	// CommitDaemonRegistered records the S-4 registration.
	CommitDaemonRegistered(ctx context.Context, ws, taskID string, at time.Time) (TaskEntry, bool, error)
	// CommitTerminal writes the terminal verdict, the settled record and drops
	// the queue row in one atomic step; first=false when another instance
	// already settled the task.
	CommitTerminal(ctx context.Context, ws, taskID string, result Result, reason string, completedAt, drainDeadline time.Time) (TaskEntry, bool, error)
	// CommitDecidedReport records the instance's decided terminal body once.
	CommitDecidedReport(ctx context.Context, ws, taskID string, body []byte) (TaskEntry, bool, error)
	// SettleNoResult clears the verdict of a terminal row whose report the
	// server answered with "task gone" (§4.4).
	SettleNoResult(ctx context.Context, ws, taskID string, expected Result) (TaskEntry, bool, error)
	// DeleteEntry removes the live row and its queue marker.
	DeleteEntry(ctx context.Context, ws, taskID string) error
	// GetEntry reads one live row (partitioned lookup).
	GetEntry(ctx context.Context, ws, taskID string) (TaskEntry, bool, error)
	// GetEntryByJob resolves a Job name to its live row through the shared
	// index.
	GetEntryByJob(ctx context.Context, jobName string) (TaskEntry, bool, error)
	// DropQueue removes the task's queue marker.
	DropQueue(ctx context.Context, ws, taskID string) error
	// ReleaseEntry returns an entry's in-flight slot (both levels;
	// floor-guarded). Pending reservations are left untouched.
	ReleaseEntry(ctx context.Context, ws string) error
	// ListEntries lists every live row (takeover resync).
	ListEntries(ctx context.Context) ([]TaskEntry, error)
	// ListSettled lists every settled record (takeover resync).
	ListSettled(ctx context.Context) ([]TaskEntry, error)
	// ListRevoked lists the revocation set (takeover resync).
	ListRevoked(ctx context.Context) (map[string]time.Time, error)
	// PutSessionNode stores one soft-affinity row (§4.5 共享索引) with its
	// retention as the store-side TTL.
	PutSessionNode(ctx context.Context, ws, key string, raw []byte, ttl time.Duration) error
	// GetSessionNode reads one soft-affinity row and its remaining store-side
	// lifetime (zero = no expiry).
	GetSessionNode(ctx context.Context, ws, key string) (SharedSessionNode, bool, error)
	// Slots reads one in-flight counter (empty ws = cluster level).
	Slots(ctx context.Context, ws string) (int64, error)
}
