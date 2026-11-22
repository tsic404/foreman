// Package remotemaint owns the remote shared state's safe reclamation:
// worktrees, Nix roots and bare caches are candidates only after their TTL
// and quiet period, and every deletion re-checks ownership, generation,
// slots, sessions and references under the domain lock. It also confirms an
// isolated slot only after the remote session's close is proven.
// 02-contracts.md V2-C03/C07, 04-architecture.md §8.
package remotemaint

import (
	"context"
	"fmt"
	"time"

	"github.com/tsic404/foreman/internal/environment"
)

// Kind is one remote object class.
type Kind string

const (
	KindWorktree Kind = "worktree"
	KindBareRepo Kind = "bare-repo"
	KindNixRoot  Kind = "nix-root"
)

// Object is one remote shared-state entry as the inventory reports it.
// Generation is the owning tree's stable identity; Owner is the attempt that
// created the object. Neither the path nor the address is an ownership key.
type Object struct {
	ID            string
	Kind          Kind
	EnvironmentID string
	SourceID      string
	WorkspaceID   string
	Generation    string
	Owner         string
	AttemptID     string
	Path          string
	// Sessions, NixRoots and SharedByAttempts are the object's complete
	// association set. ReferencesComplete must be true for empty slices to
	// mean "no reference": an empty field on an incomplete row is unknown.
	Sessions           []string
	NixRoots           []string
	SharedByAttempts   []string
	ReferencesComplete bool
	LastUsed           time.Time
	Bytes              int64
}

// Inventory is the remote fixed-state client. Delete is only called with an
// object whose full identity was re-read under the domain lock.
type Inventory interface {
	List(ctx context.Context, environmentID string) ([]Object, error)
	Get(ctx context.Context, environmentID, objectID string) (Object, bool, error)
	Delete(ctx context.Context, object Object) error
}

// Sessions answers the remote session facts. Closed is the evidence a slot
// needs before it can return to the free pool.
type Sessions interface {
	Open(ctx context.Context, environmentID string) ([]string, error)
	Closed(ctx context.Context, environmentID, sessionID string) (bool, error)
}

// References answers the Nix roots still referenced by live attempts.
type References interface {
	NixRoots(ctx context.Context, environmentID string) ([]string, error)
}

// Environments is the slot ledger the maintenance path re-reads in-lock and
// confirms. *environment.Service satisfies it.
type Environments interface {
	List(ctx context.Context, filter environment.Filter, grant environment.Grant) ([]environment.EnvironmentView, error)
	Get(ctx context.Context, id environment.ID, grant environment.Grant) (environment.EnvironmentView, error)
	Slots(ctx context.Context, id environment.ID, grant environment.Grant) ([]environment.SlotView, error)
	ReleaseSlot(ctx context.Context, id environment.ID, slotID string, cleanupConfirmed bool) (environment.Slot, error)
}

// Locker serializes writers and reclaimers of one environment/source/workspace
// domain (02-contracts.md V2-C07).
type Locker interface {
	Lock(ctx context.Context, key string) (unlock func(), err error)
}

// AuditAction is one stable maintenance event name.
type AuditAction string

const (
	AuditReclaim AuditAction = "remote.reclaim"
	AuditSkip    AuditAction = "remote.skip"
	AuditFailed  AuditAction = "remote.failed"
	AuditConfirm AuditAction = "remote.slot.confirm"
)

// AuditEntry is one maintenance decision. It never carries a Secret value.
type AuditEntry struct {
	At            time.Time
	Action        AuditAction
	EnvironmentID string
	ObjectID      string
	Kind          Kind
	SourceID      string
	WorkspaceID   string
	Reason        string
}

// Auditor receives every skip and reclaim decision.
type Auditor interface {
	Record(ctx context.Context, entry AuditEntry)
}

// Report is one round's outcome.
type Report struct {
	Reclaimed      int
	Skipped        int
	SlotsConfirmed int
	Entries        []AuditEntry
}

func (r *Report) record(ctx context.Context, auditor Auditor, entry AuditEntry) {
	if r.Entries == nil {
		r.Entries = []AuditEntry{}
	}
	r.Entries = append(r.Entries, entry)
	if auditor != nil {
		auditor.Record(ctx, entry)
	}
}

// ErrUnreadable marks a remote fact the re-check could not read. It is a skip
// with an audit reason, never a licence to delete.
type ErrUnreadable struct {
	Fact string
	Err  error
}

func (e *ErrUnreadable) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("remote fact %s is unreadable: %v", e.Fact, e.Err)
}

func (e *ErrUnreadable) Unwrap() error { return e.Err }
