package remotemaint

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/environment"
)

// Config wires one maintenance round.
type Config struct {
	Environments Environments
	Inventory    Inventory
	Sessions     Sessions
	References   References
	Locks        Locker
	Audit        Auditor
	Now          func() time.Time
	TTL          time.Duration
	Quiet        time.Duration
	Logger       *slog.Logger
}

// Service runs the remote maintenance rules.
type Service struct {
	cfg Config
}

// New validates the round's inputs. An unwired inventory has nothing to
// reclaim; an unwired ledger cannot confirm a slot.
func New(cfg Config) (*Service, error) {
	if cfg.Environments == nil {
		return nil, errors.New("remotemaint: the environment ledger is required")
	}
	if cfg.Inventory == nil {
		return nil, errors.New("remotemaint: the remote inventory is required")
	}
	if cfg.Locks == nil {
		cfg.Locks = NewKeyedLocker()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.TTL <= 0 || cfg.Quiet <= 0 {
		return nil, errors.New("remotemaint: TTL and quiet period must be positive")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Service{cfg: cfg}, nil
}

// RunOnce scans every registered environment: first confirmed slot closures,
// then TTL/quiet-period object candidates. A candidate's whole identity is
// re-read under the domain lock and any inconsistency skips it.
func (s *Service) RunOnce(ctx context.Context) (Report, error) {
	var report Report
	envs, err := s.cfg.Environments.List(ctx, environment.Filter{}, environment.Grant{DeploymentWide: true})
	if err != nil {
		return report, fmt.Errorf("list environments: %w", err)
	}
	sort.Slice(envs, func(i, j int) bool { return envs[i].ID < envs[j].ID })
	var errs []error
	for _, view := range envs {
		if ctx.Err() != nil {
			break
		}
		if err := s.confirmSlots(ctx, view.ID, &report); err != nil {
			errs = append(errs, err)
		}
		if err := s.reclaimObjects(ctx, view.ID, &report); err != nil {
			errs = append(errs, err)
		}
	}
	return report, errors.Join(errs...)
}

func (s *Service) confirmSlots(ctx context.Context, environmentID string, report *Report) error {
	if s.cfg.Sessions == nil {
		return nil
	}
	slots, err := s.cfg.Environments.Slots(ctx, environment.ID(environmentID), environment.Grant{DeploymentWide: true})
	if err != nil {
		return fmt.Errorf("read slots of %s: %w", environmentID, err)
	}
	for _, slot := range slots {
		if slot.State != string(environment.SlotIsolated) {
			continue
		}
		key := domainKey(environmentID, "", "")
		unlock, err := s.cfg.Locks.Lock(ctx, key)
		if err != nil {
			return fmt.Errorf("lock %s: %w", key, err)
		}
		s.confirmOneSlot(ctx, environmentID, slot, report)
		unlock()
	}
	return nil
}

func (s *Service) confirmOneSlot(ctx context.Context, environmentID string, candidate environment.SlotView, report *Report) {
	view, err := s.cfg.Environments.Get(ctx, environment.ID(environmentID), environment.Grant{DeploymentWide: true})
	if err != nil {
		s.skip(ctx, report, AuditEntry{EnvironmentID: environmentID, ObjectID: candidate.ID, Reason: "environment_unreadable"})
		return
	}
	slots, err := s.cfg.Environments.Slots(ctx, environment.ID(environmentID), environment.Grant{DeploymentWide: true})
	if err != nil {
		s.skip(ctx, report, AuditEntry{EnvironmentID: environmentID, ObjectID: candidate.ID, Reason: "slots_unreadable"})
		return
	}
	current, ok := findSlot(slots, candidate.ID)
	if !ok || current.State != string(environment.SlotIsolated) {
		s.skip(ctx, report, AuditEntry{EnvironmentID: environmentID, ObjectID: candidate.ID, Reason: "slot_state_changed"})
		return
	}
	if current.SessionID == "" {
		s.skip(ctx, report, AuditEntry{EnvironmentID: environmentID, ObjectID: candidate.ID, Reason: "session_identity_missing"})
		return
	}
	closed, err := s.cfg.Sessions.Closed(ctx, view.ID, current.SessionID)
	if err != nil {
		s.skip(ctx, report, AuditEntry{EnvironmentID: environmentID, ObjectID: candidate.ID, Reason: "session_unreadable"})
		return
	}
	if !closed {
		s.skip(ctx, report, AuditEntry{EnvironmentID: environmentID, ObjectID: candidate.ID, Reason: "session_still_open"})
		return
	}
	if _, err := s.cfg.Environments.ReleaseSlot(ctx, environment.ID(environmentID), current.ID, true); err != nil {
		s.fail(ctx, report, AuditEntry{EnvironmentID: environmentID, ObjectID: candidate.ID, Reason: err.Error()})
		return
	}
	report.SlotsConfirmed++
	report.record(ctx, s.cfg.Audit, AuditEntry{
		At: s.now(), Action: AuditConfirm, EnvironmentID: environmentID,
		ObjectID: candidate.ID, Reason: "remote_close_confirmed",
	})
}

func (s *Service) reclaimObjects(ctx context.Context, environmentID string, report *Report) error {
	objects, err := s.cfg.Inventory.List(ctx, environmentID)
	if err != nil {
		return fmt.Errorf("list remote objects of %s: %w", environmentID, err)
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].ID < objects[j].ID })
	for _, object := range objects {
		if ctx.Err() != nil {
			break
		}
		if !s.isCandidate(object) {
			continue
		}
		key := domainKey(environmentID, object.SourceID, object.WorkspaceID)
		unlock, err := s.cfg.Locks.Lock(ctx, key)
		if err != nil {
			return fmt.Errorf("lock %s: %w", key, err)
		}
		s.reclaimOne(ctx, environmentID, object, report)
		unlock()
	}
	return nil
}

func (s *Service) reclaimOne(ctx context.Context, environmentID string, candidate Object, report *Report) {
	view, err := s.cfg.Environments.Get(ctx, environment.ID(environmentID), environment.Grant{DeploymentWide: true})
	if err != nil {
		s.skipObject(ctx, report, candidate, "environment_unreadable")
		return
	}
	if view.StateRoot == "" {
		s.skipObject(ctx, report, candidate, "state_root_unreadable")
		return
	}
	current, ok, err := s.cfg.Inventory.Get(ctx, environmentID, candidate.ID)
	if err != nil {
		s.skipObject(ctx, report, candidate, "object_unreadable")
		return
	}
	if !ok {
		s.skipObject(ctx, report, candidate, "object_vanished")
		return
	}
	if !sameIdentity(candidate, current) {
		s.skipObject(ctx, report, candidate, "object_changed")
		return
	}
	if ok, reason := s.recheck(ctx, view, current); !ok {
		s.skipObject(ctx, report, candidate, reason)
		return
	}
	if err := s.cfg.Inventory.Delete(ctx, current); err != nil {
		s.fail(ctx, report, AuditEntry{
			EnvironmentID: environmentID, ObjectID: current.ID, Kind: current.Kind,
			SourceID: current.SourceID, WorkspaceID: current.WorkspaceID, Reason: err.Error(),
		})
		return
	}
	report.Reclaimed++
	report.record(ctx, s.cfg.Audit, AuditEntry{
		At: s.now(), Action: AuditReclaim, EnvironmentID: environmentID,
		ObjectID: current.ID, Kind: current.Kind, SourceID: current.SourceID,
		WorkspaceID: current.WorkspaceID, Reason: "ttl_and_quiet_period",
	})
}

// recheck applies the in-lock ownership, generation, reference and closure
// gates. The returned reason is the first failing gate, for the audit row.
func (s *Service) recheck(ctx context.Context, view environment.EnvironmentView, object Object) (bool, string) {
	if object.EnvironmentID != view.ID {
		return false, "environment_mismatch"
	}
	if object.Generation == "" || view.Verification.TreeID == "" || object.Generation != view.Verification.TreeID {
		return false, "generation_mismatch"
	}
	if object.Owner == "" || object.AttemptID == "" {
		return false, "owner_missing"
	}
	if !withinRoot(view.StateRoot, object.Path) {
		return false, "path_outside_authorized_root"
	}
	if !s.isCandidate(object) {
		return false, "reference_reappeared"
	}
	if !object.ReferencesComplete {
		return false, "references_incomplete"
	}
	slots, err := s.cfg.Environments.Slots(ctx, environment.ID(view.ID), environment.Grant{DeploymentWide: true})
	if err != nil {
		return false, "slots_unreadable"
	}
	for _, slot := range slots {
		if slot.ID == object.AttemptID ||
			(object.AttemptID == "" && slot.ID == object.ID) {
			return false, "slot_present"
		}
		for _, session := range object.Sessions {
			if session != "" && slot.SessionID == session {
				return false, "slot_present"
			}
		}
	}
	for _, attemptID := range object.SharedByAttempts {
		if attemptID == "" || attemptID == object.AttemptID {
			continue
		}
		return false, "shared_reference_present"
	}
	if s.cfg.Sessions == nil {
		return false, "sessions_unreadable"
	}
	open, err := s.cfg.Sessions.Open(ctx, view.ID)
	if err != nil {
		return false, "sessions_unreadable"
	}
	for _, session := range object.Sessions {
		if contains(open, session) {
			return false, "session_open"
		}
	}
	if s.cfg.References == nil {
		return false, "nix_roots_unreadable"
	}
	roots, err := s.cfg.References.NixRoots(ctx, view.ID)
	if err != nil {
		return false, "nix_roots_unreadable"
	}
	for _, root := range object.NixRoots {
		if contains(roots, root) {
			return false, "nix_root_referenced"
		}
	}
	return true, ""
}

func (s *Service) isCandidate(object Object) bool {
	if object.LastUsed.IsZero() {
		return false
	}
	age := s.now().Sub(object.LastUsed)
	return age >= s.cfg.Quiet && age >= s.cfg.TTL
}

func (s *Service) skipObject(ctx context.Context, report *Report, object Object, reason string) {
	s.skip(ctx, report, AuditEntry{
		EnvironmentID: object.EnvironmentID, ObjectID: object.ID, Kind: object.Kind,
		SourceID: object.SourceID, WorkspaceID: object.WorkspaceID, Reason: reason,
	})
}

func (s *Service) skip(ctx context.Context, report *Report, entry AuditEntry) {
	entry.At = s.now()
	entry.Action = AuditSkip
	report.Skipped++
	report.record(ctx, s.cfg.Audit, entry)
}

func (s *Service) fail(ctx context.Context, report *Report, entry AuditEntry) {
	entry.At = s.now()
	entry.Action = AuditFailed
	report.record(ctx, s.cfg.Audit, entry)
}

func (s *Service) now() time.Time { return s.cfg.Now() }

func sameIdentity(a, b Object) bool {
	return a.ID == b.ID && a.Kind == b.Kind &&
		a.EnvironmentID == b.EnvironmentID &&
		a.SourceID == b.SourceID && a.WorkspaceID == b.WorkspaceID &&
		a.Generation == b.Generation && a.Owner == b.Owner &&
		a.AttemptID == b.AttemptID &&
		a.ReferencesComplete == b.ReferencesComplete &&
		sameStringSet(a.Sessions, b.Sessions) &&
		sameStringSet(a.NixRoots, b.NixRoots) &&
		sameStringSet(a.SharedByAttempts, b.SharedByAttempts) &&
		a.Path == b.Path && !b.LastUsed.After(a.LastUsed)
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, value := range a {
		seen[value]++
	}
	for _, value := range b {
		seen[value]--
		if seen[value] < 0 {
			return false
		}
	}
	return true
}

func findSlot(slots []environment.SlotView, id string) (environment.SlotView, bool) {
	for _, slot := range slots {
		if slot.ID == id {
			return slot, true
		}
	}
	return environment.SlotView{}, false
}

func domainKey(environmentID, sourceID, workspaceID string) string {
	return environmentID + "\x00" + sourceID + "\x00" + workspaceID
}

func withinRoot(root, path string) bool {
	if root == "" || path == "" {
		return false
	}
	cleanRoot := filepath.Clean(root)
	cleanPath := filepath.Clean(path)
	rel, err := filepath.Rel(cleanRoot, cleanPath)
	if err != nil || rel == "." || rel == ".." {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
