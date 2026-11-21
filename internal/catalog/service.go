package catalog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
)

// Errors the management face answers with. They are deliberately coarse: the
// detail a caller needs is the outcome, not the internal step.
var (
	ErrSourceExists   = errors.New("server is already registered")
	ErrSourceNotFound = errors.New("server is not registered")
	ErrSyncInFlight   = errors.New("a sync of this source is already running")
	// ErrSourceURLExists refuses a second Server ID for one canonical source
	// URL. A run's source is its claiming client's fixed URL, so two ids for
	// one URL would make that run's attribution a coin toss — and a later
	// registration could blank an attempt that is already running
	// (04-architecture §2 运行绑定使用当时快照).
	ErrSourceURLExists = errors.New("this source URL is already registered under another server id")
)

// Config is the catalog service's configuration.
type Config struct {
	// Store persists sources, snapshots and audit rows. Defaults to an empty
	// MemoryStore.
	Store Store
	// Upstream is the directory/binding adapter. Required.
	Upstream Upstream
	// Credentials resolves a source's credential reference. Required when
	// Upstream is nil is false; the production adapter needs it.
	Credentials Credentials
	// SyncTimeout bounds one source's complete sync.
	SyncTimeout time.Duration
	// TickInterval is how often the automatic loop looks for a due source.
	TickInterval time.Duration
	// ManagedRuntimeIDs answers the runtimes this Foreman registered in one
	// source and workspace. It receives the source record so the implementation
	// never has to re-enter the catalog to learn the source URL. Nil classifies
	// every bound agent as unmanaged, which is the truthful answer for a source
	// this deployment does not serve.
	ManagedRuntimeIDs func(Source, string) []string
	// RunIndex is the coordinator's in-flight attempt face. Nil reports the
	// run facts as unavailable.
	RunIndex RunIndex
	// EnvironmentCount is the registered remote environment count over the
	// caller's grant. Nil means the environment registry is not wired (the
	// environment lifecycle is a separate delivery).
	EnvironmentCount func(context.Context, Grant) int
	// Clock is the service's single clock seam.
	Clock func() time.Time
	// Logger defaults to slog.Default.
	Logger *slog.Logger
}

// DefaultSyncTimeout bounds one source's complete sync.
const DefaultSyncTimeout = 2 * time.Minute

// DefaultTickInterval is the automatic loop's wake-up cadence.
const DefaultTickInterval = time.Second

// Service owns the source registry, the sync engine, the binding write-back and
// the overview read model. Every mutation runs under one mutex: syncs are
// infrequent and the state transitions are small, so serializing them keeps the
// generation and binding projections single-writer.
type Service struct {
	cfg      Config
	store    Store
	upstream Upstream
	now      func() time.Time
	log      *slog.Logger

	mu      sync.Mutex
	syncing map[ServerID]bool

	// bindingEpoch orders read-back-confirmed binding projections against a
	// sync that started before them. confirmed carries the value each
	// confirmation wrote, so a sync that read an older upstream answer can be
	// overlaid with the newer confirmed truth instead of reverting it.
	bindingEpoch map[ServerID]uint64
	confirmed    map[ServerID]map[string]confirmedBinding
}

// confirmedBinding is one read-back-confirmed binding value and the epoch it
// was projected at.
type confirmedBinding struct {
	workspaceID  string
	agentID      string
	runtimeID    string
	runtimeBound bool
	management   Management
	epoch        uint64
}

// New builds the service.
func New(cfg Config) (*Service, error) {
	if cfg.Upstream == nil {
		return nil, errors.New("catalog: an upstream adapter is required")
	}
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore()
	}
	if cfg.SyncTimeout <= 0 {
		cfg.SyncTimeout = DefaultSyncTimeout
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = DefaultTickInterval
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Service{
		cfg:      cfg,
		store:    cfg.Store,
		upstream: cfg.Upstream,
		now:      cfg.Clock,
		log:      cfg.Logger.With("component", "catalog"),
		syncing:  map[ServerID]bool{},

		bindingEpoch: map[ServerID]uint64{},
		confirmed:    map[ServerID]map[string]confirmedBinding{},
	}, nil
}

// Register stores a new source and verifies its connection. A source that
// cannot be reached is still registered, with the failure on its face: the
// operator can repair the credential and retry, and the overview never counts
// it as connected (04-architecture.md §2).
func (s *Service) Register(ctx context.Context, reg Registration, actor string) (SourceView, error) {
	src, err := ValidateRegistration(reg)
	if err != nil {
		return SourceView{}, err
	}
	if s.cfg.Credentials == nil {
		return SourceView{}, errors.New("catalog: no credential resolver is wired")
	}
	if _, err := s.cfg.Credentials.Token(src.CredentialRef); err != nil {
		return SourceView{}, errors.New("credential_ref does not resolve to a readable credential")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists, err := s.store.GetSource(ctx, src.ID); err != nil {
		return SourceView{}, err
	} else if exists {
		return SourceView{}, ErrSourceExists
	}
	existing, err := s.store.ListSources(ctx)
	if err != nil {
		return SourceView{}, err
	}
	for _, candidate := range existing {
		if canonicalSourceURL(candidate.URL) != canonicalSourceURL(src.URL) {
			continue
		}
		return SourceView{}, fmt.Errorf("%w (%s)", ErrSourceURLExists, candidate.ID)
	}
	now := s.now()
	src.CreatedAt = now
	src.UpdatedAt = now
	src.Connection = ConnectionUnverified
	src.Sync = SyncNever
	if src.AutoSync {
		src.NextSyncAt = now
	}
	if err := s.store.PutSource(ctx, src); err != nil {
		return SourceView{}, err
	}
	src = s.probeLocked(ctx, src)
	s.appendAuditLocked(ctx, AuditEntry{
		At: now, Actor: actor, Action: AuditRegisterServer,
		ServerID: string(src.ID), Result: AuditOK, Detail: string(src.Connection),
	})
	view, _, err := s.sourceViewLocked(ctx, src, Grant{})
	return view, err
}

// probeLocked verifies the connection face without touching the directory.
func (s *Service) probeLocked(ctx context.Context, src Source) Source {
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.SyncTimeout)
	defer cancel()
	_, err := s.upstream.ListWorkspaces(callCtx, src)
	src.LastAttemptAt = s.now()
	switch {
	case err == nil:
		src.Connection = ConnectionOK
		src.LastError = ""
	case IsUnauthorized(err):
		// A revoked credential blocks use immediately, before any cache can
		// answer for it (04-architecture.md §3.1).
		src.Connection = ConnectionUnauthorized
		src.LastError = err.Error()
	default:
		src.Connection = ConnectionFailed
		src.LastError = err.Error()
	}
	src.UpdatedAt = s.now()
	_ = s.store.PutSource(ctx, src)
	return src
}

// Get returns one source's view.
func (s *Service) Get(ctx context.Context, id ServerID) (SourceView, bool, error) {
	return s.getForGrant(ctx, id, Grant{})
}

// GetForGrant returns one source's view cropped to the caller's grant. A
// source the grant reaches through neither its synced workspaces nor its
// configured scope is answered as absent (§8 跨域检查在服务端).
func (s *Service) GetForGrant(ctx context.Context, id ServerID, grant Grant) (SourceView, bool, error) {
	return s.getForGrant(ctx, id, grant)
}

func (s *Service) getForGrant(ctx context.Context, id ServerID, grant Grant) (SourceView, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok, err := s.store.GetSource(ctx, id)
	if err != nil || !ok {
		return SourceView{}, false, err
	}
	view, visible, err := s.sourceViewLocked(ctx, src, grant)
	return view, visible, err
}

// List returns every registered source.
func (s *Service) List(ctx context.Context) ([]SourceView, error) {
	return s.listForGrant(ctx, Grant{})
}

// ListForGrant returns the sources the grant reaches, cropped to it.
func (s *Service) ListForGrant(ctx context.Context, grant Grant) ([]SourceView, error) {
	return s.listForGrant(ctx, grant)
}

func (s *Service) listForGrant(ctx context.Context, grant Grant) ([]SourceView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sources, err := s.store.ListSources(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(sources, func(a, b Source) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		}
		return 0
	})
	views := make([]SourceView, 0, len(sources))
	for _, src := range sources {
		view, visible, err := s.sourceViewLocked(ctx, src, grant)
		if err != nil {
			return nil, err
		}
		if !visible {
			continue
		}
		views = append(views, view)
	}
	return views, nil
}

// canonicalSourceURL is the comparison form of a source URL: one spelling per
// upstream, so two registrations for the same server cannot both claim a run's
// attribution. It only normalises the shape a registration already guarantees
// (absolute http(s), no userinfo) — a case-sensitive host is left alone
// because a path-carrying deployment is a different origin there.
func canonicalSourceURL(raw string) string {
	return strings.TrimRight(strings.TrimSpace(raw), "/")
}

// SourceIDForURL resolves the registered source with one exact upstream URL.
// Runtime attribution uses it together with the claiming client's fixed
// workspace→URL identity: the directory is never consulted, so adding another
// source with the same workspace id or deleting the workspace upstream cannot
// change where an existing attempt is shown. Two registrations sharing a URL
// are ambiguous and answer unknown (fail closed).
func (s *Service) SourceIDForURL(ctx context.Context, serverURL string) (ServerID, bool) {
	serverURL = canonicalSourceURL(serverURL)
	if serverURL == "" {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sources, err := s.store.ListSources(ctx)
	if err != nil {
		return "", false
	}
	var match ServerID
	found := false
	for _, src := range sources {
		if canonicalSourceURL(src.URL) != serverURL {
			continue
		}
		if found {
			return "", false
		}
		match, found = src.ID, true
	}
	return match, found
}

// GrantForScopes resolves a credential's scopes into a grant. A scope that
// names only a workspace is bound to the single registered source that holds
// it; zero or several holders refuse it. The ambiguity refusal is the point:
// a workspace id is only unique inside one upstream, so silently granting
// every holder is the cross-server leak the pair key exists to close.
func (s *Service) GrantForScopes(ctx context.Context, scopes []Scope) Grant {
	if scopes == nil {
		return Grant{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sources, err := s.store.ListSources(ctx)
	if err != nil {
		return Grant{Scopes: []Scope{}}
	}
	out := make([]Scope, 0, len(scopes))
	seen := map[string]bool{}
	add := func(serverID ServerID, workspaceID string) {
		key := string(serverID) + "\x00" + workspaceID
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, Scope{ServerID: serverID, WorkspaceID: workspaceID})
	}
	for _, scope := range scopes {
		if scope.WorkspaceID == "" {
			continue
		}
		if scope.ServerID != "" {
			add(scope.ServerID, scope.WorkspaceID)
			continue
		}
		src, holders := ServerID(""), 0
		for _, candidate := range sources {
			snap, ok, err := s.store.GetSnapshot(ctx, candidate.ID)
			if err != nil || !ok {
				continue
			}
			if slices.ContainsFunc(snap.Workspaces, func(workspace Workspace) bool {
				return workspace.ID == scope.WorkspaceID
			}) {
				src, holders = candidate.ID, holders+1
			}
		}
		if holders != 1 {
			s.log.Warn("catalog.grant_scope_ambiguous",
				"workspace_id", scope.WorkspaceID, "sources", holders)
			continue
		}
		add(src, scope.WorkspaceID)
	}
	return Grant{Scopes: out}
}

// ClaimOwned reports whether the exact C3 request that returned the task
// carried its real runtime. That runtime is the identity the upstream confirmed
// the task to before the claim response existed, so it is the confirmation
// boundary (02-contracts V2-C02 已确认归原 runtime).
//
// The agent's current binding and the live declaration are deliberately not
// consulted. A claim already in flight still belongs to the runtime that
// received it; reading the directory here would let a later edit — or the sync
// that publishes it — revoke a confirmation the upstream never revoked.
// Admission for *future* dispatches belongs upstream, which only hands out
// tasks for runtimes the request declared.
//
// known=false covers a caller with no request evidence (a direct claim outside
// the loop). known=true, owned=false is the refusing answer: the request names
// a runtime it did not carry. No Job may be created for it, and the caller must
// surface that instead of dropping the claim.
func (s *Service) ClaimOwned(_ context.Context, workspaceID, agentID, realRuntimeID string, claimedRuntimeIDs []string) (owned, known bool) {
	if len(claimedRuntimeIDs) == 0 || realRuntimeID == "" {
		return false, false
	}
	if !slices.Contains(claimedRuntimeIDs, realRuntimeID) {
		return false, true
	}
	return true, true
}

// UpdateSettings changes the automatic sync settings. It never revokes an
// existing binding or disables a manual sync.
func (s *Service) UpdateSettings(ctx context.Context, id ServerID, settings Settings, actor string) (SourceView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok, err := s.store.GetSource(ctx, id)
	if err != nil {
		return SourceView{}, err
	}
	if !ok {
		return SourceView{}, ErrSourceNotFound
	}
	now := s.now()
	if settings.AutoSync != nil {
		src.AutoSync = *settings.AutoSync
		if src.AutoSync && src.NextSyncAt.IsZero() {
			src.NextSyncAt = now
		}
	}
	if settings.SyncPeriodSeconds != nil {
		period := time.Duration(*settings.SyncPeriodSeconds) * time.Second
		if period < MinSyncPeriod || period > MaxSyncPeriod {
			return SourceView{}, fmt.Errorf("sync_period_s must be between %d and %d", int(MinSyncPeriod.Seconds()), int(MaxSyncPeriod.Seconds()))
		}
		src.SyncPeriod = period
	}
	if settings.RetryBudget != nil {
		if *settings.RetryBudget < 1 || *settings.RetryBudget > MaxRetryBudget {
			return SourceView{}, fmt.Errorf("retry_budget must be between 1 and %d", MaxRetryBudget)
		}
		src.RetryBudget = *settings.RetryBudget
	}
	src.UpdatedAt = now
	if err := s.store.PutSource(ctx, src); err != nil {
		return SourceView{}, err
	}
	s.appendAuditLocked(ctx, AuditEntry{
		At: now, Actor: actor, Action: AuditUpdateServer,
		ServerID: string(src.ID), Result: AuditOK,
	})
	view, _, err := s.sourceViewLocked(ctx, src, Grant{})
	return view, err
}

// Run is the automatic sync loop: it wakes on the tick interval and syncs every
// due source that opted into automatic sync. A source's failure never stops
// another's loop (04-architecture.md §3.1 每来源独立).
func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.SyncDue(ctx)
		}
	}
}

// SyncDue syncs every due source with automatic sync on. It is exported so a
// test can advance the clock instead of waiting for the ticker.
func (s *Service) SyncDue(ctx context.Context) {
	sources, err := s.store.ListSources(ctx)
	if err != nil {
		s.log.Error("catalog.sources_unreadable", "err", err.Error())
		return
	}
	now := s.now()
	for _, src := range sources {
		if !src.AutoSync || src.NextSyncAt.After(now) {
			continue
		}
		if _, err := s.Sync(ctx, src.ID); err != nil {
			// The failure is recorded on the source; it never aborts the loop
			// or another source's sync.
			s.log.Warn("catalog.sync_failed", "server_id", string(src.ID), "err", err.Error())
		}
	}
}

// sourceViewLocked builds one source's management view and whether the grant
// reaches it. The caller holds the service mutex. Counts are cropped, never
// computed globally and then hidden: a workspace-bound credential must not
// learn how many agents another workspace holds (§2).
func (s *Service) sourceViewLocked(ctx context.Context, src Source, grant Grant) (SourceView, bool, error) {
	writeBack := s.writeBackCapability(src, false)
	view := sourceView(src, false, writeBack)
	snap, ok, err := s.store.GetSnapshot(ctx, src.ID)
	if err != nil {
		return SourceView{}, false, err
	}
	visible := sourceVisible(src, snap, ok, grant)
	distinct := map[string]bool{}
	foreman := map[string]bool{}
	agents, managed := 0, 0
	if ok {
		for _, workspace := range snap.Workspaces {
			if !grant.Allows(src.ID, workspace.ID) {
				continue
			}
			distinct[workspace.ID] = true
			visible = true
		}
		for _, agent := range snap.Agents {
			if !grant.Allows(src.ID, agent.WorkspaceID) {
				continue
			}
			agents++
			if managementFor(agent, s.managedSet(src, agent.WorkspaceID)) != ManagedForeman {
				continue
			}
			managed++
			foreman[agent.WorkspaceID] = true
		}
	}
	view.Workspaces = len(distinct)
	view.WorkspacesWithForemanAgents = len(foreman)
	view.Agents = agents
	view.ManagedAgents = managed
	view.Stale = src.Connection != ConnectionOK || src.Sync != SyncOK
	view.WriteBack = s.writeBackCapability(src, ok)
	return view, visible, nil
}

// appendAuditLocked writes one audit row. The caller holds the mutex.
func (s *Service) appendAuditLocked(ctx context.Context, entry AuditEntry) {
	if err := s.store.AppendAudit(ctx, entry); err != nil {
		s.log.Warn("catalog.audit_failed", "action", entry.Action, "err", err.Error())
	}
}

// nextSyncAt is the next automatic attempt: the normal cadence until the retry
// budget is spent, then an exponential backoff bounded by one day.
func nextSyncAt(src Source, now time.Time) time.Time {
	if !src.AutoSync {
		return time.Time{}
	}
	period := src.SyncPeriod
	if period <= 0 {
		period = DefaultSyncPeriod
	}
	extra := src.FailureCount - src.RetryBudget
	if extra > 0 {
		backoff := period
		for i := 0; i < extra && backoff < 24*time.Hour; i++ {
			backoff *= 2
		}
		if backoff > 24*time.Hour {
			backoff = 24 * time.Hour
		}
		period = backoff
	}
	return now.Add(period)
}

// countManagement counts agents of one management class.
func countManagement(agents []Agent, class Management) int {
	count := 0
	for _, agent := range agents {
		if agent.Management == class {
			count++
		}
	}
	return count
}
