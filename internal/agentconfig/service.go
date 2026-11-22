package agentconfig

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
)

// Config is the configuration service's setup.
type Config struct {
	Store  Store
	Clock  func() time.Time
	Logger *slog.Logger
}

// Service owns the revision counter and the projection shape. The content is
// validated in Write and never retained.
type Service struct {
	store Store
	now   func() time.Time
	log   *slog.Logger
	mu    sync.Mutex
}

// New builds the service.
func New(cfg Config) (*Service, error) {
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore()
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Service{store: cfg.Store, now: cfg.Clock, log: cfg.Logger.With("component", "agentconfig")}, nil
}

// Write validates one document and records the next revision. Only the digest,
// size, reference and file name are stored.
func (s *Service) Write(ctx context.Context, scope Scope, request WriteRequest, actor string) (View, error) {
	validatedScope, err := ValidateScope(scope)
	if err != nil {
		return View{}, err
	}
	validated, canonical, err := ValidateWrite(request)
	if err != nil {
		return View{}, err
	}
	sum := sha256.Sum256(canonical)
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, ok, err := s.store.GetRevision(ctx, validatedScope)
	if err != nil {
		return View{}, err
	}
	revision := uint64(1)
	if ok {
		revision = previous.Revision + 1
	}
	now := s.now()
	stored := Revision{
		Scope: validatedScope, Revision: revision, SecretRef: validated.SecretRef,
		FileName: validated.FileName, Digest: hex.EncodeToString(sum[:]),
		Size: int64(len(canonical)), UpdatedAt: now, UpdatedBy: actor,
	}
	if err := s.store.PutRevision(ctx, stored); err != nil {
		return View{}, err
	}
	if err := s.store.AppendAudit(ctx, AuditEntry{
		At: now, Actor: actor, Action: AuditWrite, ServerID: validatedScope.ServerID,
		WorkspaceID: validatedScope.WorkspaceID, Backend: validatedScope.Backend,
		Profile: validatedScope.Profile, Revision: revision, Result: AuditOK,
		Detail: "content validated and discarded",
	}); err != nil {
		s.log.Warn("agentconfig.audit_failed", "err", err.Error())
	}
	return viewOf(stored), nil
}

// Get returns one scope's current revision.
func (s *Service) Get(ctx context.Context, scope Scope) (View, bool, error) {
	validated, err := ValidateScope(scope)
	if err != nil {
		return View{}, false, err
	}
	revision, ok, err := s.store.GetRevision(ctx, validated)
	if err != nil || !ok {
		return View{}, ok, err
	}
	return viewOf(revision), true, nil
}

// List returns the revisions matching the optional filters.
func (s *Service) List(ctx context.Context, serverID, workspaceID string) ([]View, error) {
	revisions, err := s.store.ListRevisions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]View, 0, len(revisions))
	for _, revision := range revisions {
		if serverID != "" && revision.Scope.ServerID != serverID {
			continue
		}
		if workspaceID != "" && revision.Scope.WorkspaceID != workspaceID {
			continue
		}
		out = append(out, viewOf(revision))
	}
	return out, nil
}

// Resolve answers one scope's current revision. found=false means no
// configuration was written; err != nil means the lookup failed and the
// answer is unknown rather than an empty configuration.
func (s *Service) Resolve(ctx context.Context, scope Scope) (Revision, bool, error) {
	validated, err := ValidateScope(scope)
	if err != nil {
		return Revision{}, false, err
	}
	revision, ok, err := s.store.GetRevision(ctx, validated)
	if err != nil {
		return Revision{}, false, err
	}
	return revision, ok, nil
}

// Revision answers the current revision of one scope. The discovery cache key
// uses this value; ok=false means the lookup failed or the scope is invalid.
// A scope with no written configuration is a known zero, not an unknown.
func (s *Service) Revision(ctx context.Context, serverID, workspaceID, backend, profile string) (uint64, bool) {
	revision, found, err := s.Resolve(ctx, Scope{
		ServerID: serverID, WorkspaceID: workspaceID, Backend: backend, Profile: profile,
	})
	if err != nil {
		return 0, false
	}
	if !found {
		return 0, true
	}
	return revision.Revision, true
}

// Bind freezes one attempt to the current revision of a scope. The claim
// calls it so the first per-attempt projection returns the same revision the
// claim snapshot recorded, even if the scope rotates before the projection.
func (s *Service) Bind(ctx context.Context, scope Scope, attemptID string, revision uint64) (AttemptBinding, error) {
	validated, err := ValidateScope(scope)
	if err != nil {
		return AttemptBinding{}, err
	}
	attemptID = strings.TrimSpace(attemptID)
	if !idPattern.MatchString(attemptID) {
		return AttemptBinding{}, errors.New("attempt_id must be a bare identifier")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok, err := s.store.GetRevision(ctx, validated)
	if err != nil {
		return AttemptBinding{}, err
	}
	if !ok || stored.Revision != revision {
		return AttemptBinding{}, errors.New("the configuration revision changed before the attempt was bound")
	}
	binding, bound, err := s.store.GetAttempt(ctx, attemptID)
	if err != nil {
		return AttemptBinding{}, err
	}
	if bound {
		if binding.Scope.Key() != validated.Key() || binding.Revision.Revision != revision {
			return AttemptBinding{}, errors.New("attempt is already bound to another configuration revision")
		}
		return binding, nil
	}
	binding = AttemptBinding{AttemptID: attemptID, Scope: validated, Revision: stored}
	if err := s.store.PutAttempt(ctx, binding); err != nil {
		return AttemptBinding{}, err
	}
	return binding, nil
}

// Project builds the per-attempt read-only mount. It never creates a Secret:
// the deployment's projection mechanism consumes this description elsewhere.
func (s *Service) Project(ctx context.Context, scope Scope, attemptID string) (Projection, error) {
	attemptID = strings.TrimSpace(attemptID)
	if !idPattern.MatchString(attemptID) {
		return Projection{}, errors.New("attempt_id must be a bare identifier")
	}
	validated, err := ValidateScope(scope)
	if err != nil {
		return Projection{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, bound, err := s.store.GetAttempt(ctx, attemptID)
	if err != nil {
		return Projection{}, err
	}
	if bound {
		if binding.Scope.Key() != validated.Key() {
			return Projection{}, errors.New("attempt is already bound to another configuration scope")
		}
		return projectionOf(attemptID, binding.Revision), nil
	}
	revision, ok, err := s.store.GetRevision(ctx, validated)
	if err != nil {
		return Projection{}, err
	}
	if !ok {
		return Projection{}, ErrNotFound
	}
	binding = AttemptBinding{AttemptID: attemptID, Scope: validated, Revision: revision}
	if err := s.store.PutAttempt(ctx, binding); err != nil {
		return Projection{}, err
	}
	return projectionOf(attemptID, revision), nil
}

func projectionOf(attemptID string, revision Revision) Projection {
	slug := attemptSlug(attemptID)
	return Projection{
		AttemptID: attemptID, SecretRef: revision.SecretRef,
		SecretName: SecretNameForAttempt(attemptID),
		FileName:   revision.FileName, Revision: revision.Revision,
		Digest: revision.Digest, MountPath: MountDir + "/" + revision.FileName,
		VolumeName: "agent-config-" + slug, ReadOnly: true,
	}
}

// SecretNameForAttempt is the fixed Job's projection volume name. The claim
// freezes it before the Job exists, so the render and the projection agree.
func SecretNameForAttempt(attemptID string) string {
	return "fm-" + attemptSlug(attemptID) + "-agent-config"
}

// Audit returns the rows newest first.
func (s *Service) Audit(ctx context.Context) ([]AuditEntry, error) {
	entries, err := s.store.ListAudit(ctx)
	if err != nil {
		return nil, err
	}
	slices.Reverse(entries)
	return entries, nil
}

func viewOf(revision Revision) View {
	return View{
		ServerID: revision.Scope.ServerID, WorkspaceID: revision.Scope.WorkspaceID,
		Backend: revision.Scope.Backend, Profile: revision.Scope.Profile,
		Revision: revision.Revision, SecretRef: revision.SecretRef,
		FileName: revision.FileName, Digest: revision.Digest, Size: revision.Size,
		UpdatedAt: stamp(revision.UpdatedAt), UpdatedBy: revision.UpdatedBy,
	}
}

func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// attemptSlug keeps a derived Secret or volume name inside the DNS label
// budget while staying deterministic for the same attempt.
func attemptSlug(attemptID string) string {
	if len(attemptID) <= 40 {
		return attemptID
	}
	sum := sha256.Sum256([]byte(attemptID))
	return attemptID[:32] + "-" + hex.EncodeToString(sum[:4])
}
