// The remote reclamation race: a candidate that became stale is re-read under
// the domain lock before deletion; any re-appearing Nix root, open session,
// slot, owner/generation/path disagreement or unreadable fact skips it. An
// isolated slot returns only after the remote close is proven.
// 02-contracts V2-C07, 04-architecture §8, V2-AC09/AC11, TC09/TC11.
package acceptance

import (
	"context"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/remotemaint"
)

func TestV2RemoteMaintenanceRechecksBeforeDelete(t *testing.T) {
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	old := now.Add(-2 * time.Hour)
	cases := []struct {
		name        string
		mutate      func(*v2MaintenanceFixture)
		wantReason  string
		wantDeleted bool
	}{
		{
			name:        "clean stale object",
			mutate:      func(*v2MaintenanceFixture) {},
			wantDeleted: true,
		},
		{
			name: "nix root reappears before delete",
			mutate: func(f *v2MaintenanceFixture) {
				f.references.roots = []string{"/nix/store/root-1"}
			},
			wantReason: "nix_root_referenced",
		},
		{
			name: "session is still open",
			mutate: func(f *v2MaintenanceFixture) {
				f.sessions.open = []string{"session-1"}
			},
			wantReason: "session_open",
		},
		{
			name: "slot still names the attempt",
			mutate: func(f *v2MaintenanceFixture) {
				f.ledger.slots = []environment.SlotView{{ID: "task-v2-1", State: "active", SessionID: "session-1"}}
			},
			wantReason: "slot_present",
		},
		{
			name: "tree generation changed",
			mutate: func(f *v2MaintenanceFixture) {
				f.ledger.view.Verification.TreeID = "tree-2"
			},
			wantReason: "generation_mismatch",
		},
		{
			name: "object belongs to another environment",
			mutate: func(f *v2MaintenanceFixture) {
				f.object.EnvironmentID = "build-b"
			},
			wantReason: "environment_mismatch",
		},
		{
			name: "owner is missing",
			mutate: func(f *v2MaintenanceFixture) {
				f.object.AttemptID = ""
			},
			wantReason: "owner_missing",
		},
		{
			name: "path is outside the authorized root",
			mutate: func(f *v2MaintenanceFixture) {
				f.object.Path = "/srv/other/worktrees/task-v2-1"
			},
			wantReason: "path_outside_authorized_root",
		},
		{
			name: "object changed after the candidate scan",
			mutate: func(f *v2MaintenanceFixture) {
				changed := f.object
				changed.Generation = "tree-2"
				f.inventory.getOverride = &changed
			},
			wantReason: "object_changed",
		},
		{
			name: "nix roots changed under the lock",
			mutate: func(f *v2MaintenanceFixture) {
				changed := f.object
				changed.NixRoots = []string{"/nix/store/root-2"}
				f.inventory.getOverride = &changed
			},
			wantReason: "object_changed",
		},
		{
			name: "object vanished after the candidate scan",
			mutate: func(f *v2MaintenanceFixture) {
				f.inventory.getMissing = true
			},
			wantReason: "object_vanished",
		},
		{
			name: "association metadata is incomplete",
			mutate: func(f *v2MaintenanceFixture) {
				f.object.ReferencesComplete = false
				f.object.Sessions = nil
				f.object.NixRoots = nil
			},
			wantReason: "references_incomplete",
		},
		{
			name: "another attempt references the shared bare repo",
			mutate: func(f *v2MaintenanceFixture) {
				f.object.Kind = remotemaint.KindBareRepo
				f.object.SharedByAttempts = []string{"task-v2-2"}
			},
			wantReason: "shared_reference_present",
		},
		{
			name: "nix roots cannot be read",
			mutate: func(f *v2MaintenanceFixture) {
				f.references = nil
			},
			wantReason: "nix_roots_unreadable",
		},
		{
			name: "session facts cannot be read",
			mutate: func(f *v2MaintenanceFixture) {
				f.sessions = nil
			},
			wantReason: "sessions_unreadable",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newV2MaintenanceFixture(now, old)
			tc.mutate(fixture)
			if len(fixture.inventory.objects) > 0 {
				fixture.inventory.objects[0] = fixture.object
			}
			service, err := remotemaint.New(fixture.config())
			if err != nil {
				t.Fatalf("remotemaint.New: %v", err)
			}
			report, err := service.RunOnce(context.Background())
			if err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if tc.wantDeleted {
				if len(fixture.inventory.deleted) != 1 {
					t.Fatalf("deleted = %v, want the clean stale object", fixture.inventory.deleted)
				}
				if report.Reclaimed != 1 {
					t.Fatalf("reclaimed = %d, want 1", report.Reclaimed)
				}
				if !hasAudit(report, remotemaint.AuditReclaim, "") {
					t.Fatalf("audit = %+v, want a reclaim record", report.Entries)
				}
				return
			}
			if len(fixture.inventory.deleted) != 0 {
				t.Fatalf("deleted = %v, want no deletion", fixture.inventory.deleted)
			}
			if !hasAudit(report, remotemaint.AuditSkip, tc.wantReason) {
				t.Fatalf("audit = %+v, want a skip with reason %q", report.Entries, tc.wantReason)
			}
		})
	}
}

func TestV2RemoteMaintenanceConfirmsIsolatedSlotOnlyAfterClose(t *testing.T) {
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	fixture := newV2MaintenanceFixture(now, now.Add(-48*time.Hour))
	fixture.ledger.slots = []environment.SlotView{{
		ID: "task-v2-1", State: string(environment.SlotIsolated), SessionID: "session-1",
	}}
	fixture.sessions.closed = map[string]bool{"session-1": false}
	service, err := remotemaint.New(fixture.config())
	if err != nil {
		t.Fatalf("remotemaint.New: %v", err)
	}

	report, err := service.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.SlotsConfirmed != 0 || len(fixture.ledger.releases) != 0 {
		t.Fatalf("confirmed=%d releases=%v, want the isolated slot kept while its session is open", report.SlotsConfirmed, fixture.ledger.releases)
	}
	if !hasAudit(report, remotemaint.AuditSkip, "session_still_open") {
		t.Fatalf("audit = %+v, want the still-open session recorded", report.Entries)
	}

	fixture.sessions.closed["session-1"] = true
	report, err = service.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce after close: %v", err)
	}
	if report.SlotsConfirmed != 1 {
		t.Fatalf("confirmed = %d, want the remote close submitted", report.SlotsConfirmed)
	}
	if len(fixture.ledger.releases) != 1 || !fixture.ledger.releases[0].confirmed {
		t.Fatalf("releases = %+v, want one confirmed remote close", fixture.ledger.releases)
	}
	if len(fixture.ledger.slots) != 1 {
		t.Fatalf("slots = %+v, want the reservation kept until the Pod-close fact", fixture.ledger.slots)
	}
	if !hasAudit(report, remotemaint.AuditConfirm, "remote_close_confirmed") {
		t.Fatalf("audit = %+v, want the confirmed close recorded", report.Entries)
	}
}

type v2MaintenanceFixture struct {
	ledger     *v2EnvironmentLedger
	inventory  *v2Inventory
	sessions   *v2Sessions
	references *v2References
	auditor    *remotemaint.MemoryAuditor
	object     remotemaint.Object
	now        time.Time
}

func newV2MaintenanceFixture(now, lastUsed time.Time) *v2MaintenanceFixture {
	fixture := &v2MaintenanceFixture{
		ledger: &v2EnvironmentLedger{
			view: environment.EnvironmentView{
				ID: "build-a", Endpoint: "build-a.example.test:7443",
				StateRoot: "/srv/rsbx/build-a",
				Verification: environment.VerificationView{
					State: string(environment.VerificationOK), TreeID: "tree-1",
				},
			},
		},
		inventory:  &v2Inventory{},
		sessions:   &v2Sessions{closed: map[string]bool{}},
		references: &v2References{},
		auditor:    remotemaint.NewMemoryAuditor(),
		now:        now,
	}
	fixture.object = remotemaint.Object{
		ID: "wt-1", Kind: remotemaint.KindWorktree, EnvironmentID: "build-a",
		SourceID: "src-a", WorkspaceID: "ws-1", Generation: "tree-1",
		Owner: "agent-1", AttemptID: "task-v2-1",
		Path:               "/srv/rsbx/build-a/worktrees/task-v2-1",
		Sessions:           []string{"session-1"},
		NixRoots:           []string{"/nix/store/root-1"},
		ReferencesComplete: true,
		LastUsed:           lastUsed,
	}
	fixture.inventory.objects = []remotemaint.Object{fixture.object}
	return fixture
}

func (f *v2MaintenanceFixture) config() remotemaint.Config {
	cfg := remotemaint.Config{
		Environments: f.ledger, Inventory: f.inventory,
		Audit: f.auditor, Now: func() time.Time { return f.now },
		TTL: time.Hour, Quiet: 30 * time.Minute,
	}
	if f.sessions != nil {
		cfg.Sessions = f.sessions
	}
	if f.references != nil {
		cfg.References = f.references
	}
	return cfg
}

func hasAudit(report remotemaint.Report, action remotemaint.AuditAction, reason string) bool {
	for _, entry := range report.Entries {
		if entry.Action != action {
			continue
		}
		if reason == "" || entry.Reason == reason {
			return true
		}
	}
	return false
}

type v2EnvironmentLedger struct {
	view     environment.EnvironmentView
	slots    []environment.SlotView
	releases []v2SlotRelease
}

type v2SlotRelease struct {
	id        string
	confirmed bool
}

func (l *v2EnvironmentLedger) List(context.Context, environment.Filter, environment.Grant) ([]environment.EnvironmentView, error) {
	return []environment.EnvironmentView{l.view}, nil
}

func (l *v2EnvironmentLedger) Get(_ context.Context, id environment.ID, _ environment.Grant) (environment.EnvironmentView, error) {
	if string(id) != l.view.ID {
		return environment.EnvironmentView{}, environment.ErrNotFound
	}
	return l.view, nil
}

func (l *v2EnvironmentLedger) Slots(context.Context, environment.ID, environment.Grant) ([]environment.SlotView, error) {
	return append([]environment.SlotView(nil), l.slots...), nil
}

func (l *v2EnvironmentLedger) ConfirmRemoteClose(_ context.Context, _ environment.ID, slotID string) (environment.Slot, error) {
	l.releases = append(l.releases, v2SlotRelease{id: slotID, confirmed: true})
	return environment.Slot{ID: slotID, RemoteClosed: true}, nil
}

type v2Inventory struct {
	objects     []remotemaint.Object
	deleted     []string
	getOverride *remotemaint.Object
	getMissing  bool
}

func (i *v2Inventory) List(context.Context, string) ([]remotemaint.Object, error) {
	return append([]remotemaint.Object(nil), i.objects...), nil
}

func (i *v2Inventory) Get(_ context.Context, _, objectID string) (remotemaint.Object, bool, error) {
	if i.getMissing {
		return remotemaint.Object{}, false, nil
	}
	if i.getOverride != nil {
		return *i.getOverride, true, nil
	}
	for _, object := range i.objects {
		if object.ID == objectID {
			return object, true, nil
		}
	}
	return remotemaint.Object{}, false, nil
}

func (i *v2Inventory) Delete(_ context.Context, object remotemaint.Object) error {
	i.deleted = append(i.deleted, object.ID)
	return nil
}

type v2Sessions struct {
	open   []string
	closed map[string]bool
	err    error
}

func (s *v2Sessions) Open(context.Context, string) ([]string, error) {
	return append([]string(nil), s.open...), s.err
}

func (s *v2Sessions) Closed(_ context.Context, _, sessionID string) (bool, error) {
	return s.closed[sessionID], s.err
}

type v2References struct {
	roots []string
	err   error
}

func (r *v2References) NixRoots(context.Context, string) ([]string, error) {
	return append([]string(nil), r.roots...), r.err
}
