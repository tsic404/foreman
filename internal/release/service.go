package release

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
)

// VersionSource supplies candidate versions for an automatic check. A
// definition without a manifest URL uses no source; the management surface can
// still register a manual version list.
type VersionSource interface {
	Fetch(ctx context.Context, definition Definition) ([]Candidate, error)
}

// Config is the release service's configuration.
type Config struct {
	Store Store
	// Verifier judges one artifact. A zero Verifier fails every release
	// closed, which is the safe answer for a process with no fetcher.
	Verifier Verifier
	// Artifacts is the verified-byte cache consumers read. It defaults to
	// Verifier.Cache.
	Artifacts ArtifactCache
	// VersionSource defaults to the definition's manifest URL.
	VersionSource VersionSource
	// Directory is the synced directory the impact preview reverses through.
	// Nil makes every preview unknown, never "zero affected".
	Directory Directory
	// Runs is the coordinator's in-flight attempt face. Nil makes every
	// preview unknown for the same reason.
	Runs RunIndex
	// TickInterval is how often the automatic loop looks for a due check.
	TickInterval time.Duration
	Clock        func() time.Time
	Logger       *slog.Logger
}

// DefaultTickInterval is the automatic loop's wake-up cadence.
const DefaultTickInterval = time.Second

// Service owns the definitions, the release pointers and the impact preview.
// Every mutation runs under one mutex: releases are infrequent and the
// pointer switch must be one atomic step.
type Service struct {
	cfg   Config
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
	if cfg.Artifacts == nil {
		cfg.Artifacts = cfg.Verifier.Cache
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
		cfg:   cfg,
		store: cfg.Store,
		now:   cfg.Clock,
		log:   cfg.Logger.With("component", "release"),
	}, nil
}

// RegisterDefinition stores a new CLI release line. The release domain must
// already be a registered source: a definition without a directory could
// never preview its impact.
func (s *Service) RegisterDefinition(ctx context.Context, definition Definition, actor string) (DefinitionView, error) {
	definition, err := ValidateDefinition(definition)
	if err != nil {
		return DefinitionView{}, err
	}
	if s.cfg.Directory != nil {
		if _, ok := s.cfg.Directory.ReadSource(ctx, definition.ServerID); !ok {
			return DefinitionView{}, fmt.Errorf("release domain %q is not a registered source", definition.ServerID)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists, err := s.store.GetDefinition(ctx, definition.ID); err != nil {
		return DefinitionView{}, err
	} else if exists {
		return DefinitionView{}, errors.New("cli_id is already registered")
	}
	now := s.now()
	definition.CreatedAt = now
	definition.UpdatedAt = now
	if definition.AutoCheck {
		definition.NextCheckAt = now
	}
	if err := s.store.PutDefinition(ctx, definition); err != nil {
		return DefinitionView{}, err
	}
	s.auditLocked(ctx, AuditEntry{
		At: now, Actor: actor, Action: AuditRegisterDefinition,
		CLIID: definition.ID, ServerID: definition.ServerID, Result: AuditOK,
	})
	return definitionView(definition), nil
}

// UpdateSettings changes the independent check and auto-release switches.
func (s *Service) UpdateSettings(ctx context.Context, cliID string, settings Settings, actor string) (DefinitionView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	definition, ok, err := s.store.GetDefinition(ctx, cliID)
	if err != nil {
		return DefinitionView{}, err
	}
	if !ok {
		return DefinitionView{}, ErrNotFound
	}
	autoCheckChanged := settings.AutoCheck != nil && *settings.AutoCheck != definition.AutoCheck
	periodChanged := false
	if settings.AutoCheck != nil {
		definition.AutoCheck = *settings.AutoCheck
	}
	if settings.CheckPeriodSeconds != nil {
		period := time.Duration(*settings.CheckPeriodSeconds) * time.Second
		if period < MinCheckPeriod {
			return DefinitionView{}, fmt.Errorf("check period must be at least %s", MinCheckPeriod)
		}
		periodChanged = definition.CheckPeriodSeconds != *settings.CheckPeriodSeconds
		definition.CheckPeriodSeconds = *settings.CheckPeriodSeconds
	}
	if settings.AutoRelease != nil {
		definition.AutoRelease = *settings.AutoRelease
	}
	now := s.now()
	definition.UpdatedAt = now
	if definition.AutoCheck {
		// Changing only the auto-release switch must not postpone a check
		// that is already due.
		if autoCheckChanged || periodChanged || definition.NextCheckAt.IsZero() {
			definition.NextCheckAt = now.Add(checkPeriod(definition))
		}
	} else {
		definition.NextCheckAt = time.Time{}
	}
	if err := s.store.PutDefinition(ctx, definition); err != nil {
		return DefinitionView{}, err
	}
	s.auditLocked(ctx, AuditEntry{
		At: now, Actor: actor, Action: AuditUpdateSettings,
		CLIID: definition.ID, ServerID: definition.ServerID, Result: AuditOK,
	})
	return definitionView(definition), nil
}

// RegisterVersion adds one manually supplied candidate. An automatic check
// replaces the feed's candidates but keeps these.
func (s *Service) RegisterVersion(ctx context.Context, cliID string, candidate Candidate, actor string) (DefinitionView, error) {
	if strings.TrimSpace(candidate.Version) == "" {
		return DefinitionView{}, errors.New("version is required")
	}
	if len(candidate.Artifacts) == 0 {
		return DefinitionView{}, errors.New("at least one platform artifact is required")
	}
	if err := validateArtifactSet(candidate.Artifacts); err != nil {
		return DefinitionView{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	definition, ok, err := s.store.GetDefinition(ctx, cliID)
	if err != nil {
		return DefinitionView{}, err
	}
	if !ok {
		return DefinitionView{}, ErrNotFound
	}
	now := s.now()
	candidate.Manual = true
	candidate.AddedAt = now
	upsertCandidate(&definition, candidate)
	definition.CheckGeneration++
	definition.UpdatedAt = now
	if err := s.store.PutDefinition(ctx, definition); err != nil {
		return DefinitionView{}, err
	}
	s.auditLocked(ctx, AuditEntry{
		At: now, Actor: actor, Action: AuditRegisterVersion,
		CLIID: definition.ID, ServerID: definition.ServerID, Version: candidate.Version, Result: AuditOK,
	})
	return definitionView(definition), nil
}

// Check runs one version check. It is available even when AutoCheck is off;
// the switch only decides whether the automatic loop runs it.
func (s *Service) Check(ctx context.Context, cliID, actor string) (CheckReport, error) {
	s.mu.Lock()
	definition, ok, err := s.store.GetDefinition(ctx, cliID)
	s.mu.Unlock()
	if err != nil {
		return CheckReport{}, err
	}
	if !ok {
		return CheckReport{}, ErrNotFound
	}
	now := s.now()
	report := CheckReport{CLIID: cliID, CheckedAt: now, Generation: definition.CheckGeneration}
	source := s.cfg.VersionSource
	if source == nil && definition.ManifestURL == "" {
		report.Result = "no_source"
		report.Detail = "no version feed is configured; register a manual version list"
		s.mu.Lock()
		current, found, err := s.store.GetDefinition(ctx, cliID)
		if err == nil && found {
			current.LastCheckAt = now
			current.NextCheckAt = nextCheck(current, now)
			current.LastCheckError = ""
			current.UpdatedAt = now
			err = s.store.PutDefinition(ctx, current)
		}
		s.mu.Unlock()
		if err != nil {
			return report, err
		}
		return report, nil
	}
	if source == nil {
		source = manifestSource{verifier: s.cfg.Verifier}
	}
	candidates, fetchErr := source.Fetch(ctx, definition)
	if fetchErr != nil {
		s.mu.Lock()
		current, found, getErr := s.store.GetDefinition(ctx, cliID)
		if getErr == nil && found {
			current.LastCheckAt = now
			current.NextCheckAt = nextCheck(current, now)
			current.LastCheckError = fetchErr.Error()
			current.UpdatedAt = now
			getErr = s.store.PutDefinition(ctx, current)
			s.auditLocked(ctx, AuditEntry{
				At: now, Actor: actor, Action: AuditCheck, CLIID: cliID,
				ServerID: current.ServerID, Result: AuditFailed, Detail: current.LastCheckError,
			})
		}
		s.mu.Unlock()
		if getErr != nil {
			return report, getErr
		}
		report.Result = "failed"
		report.Detail = fetchErr.Error()
		report.Generation = definition.CheckGeneration
		return report, nil
	}
	candidates, normalizeErr := normalizeCandidates(candidates, now)
	if normalizeErr != nil {
		s.mu.Lock()
		current, found, getErr := s.store.GetDefinition(ctx, cliID)
		if getErr == nil && found {
			current.LastCheckAt = now
			current.NextCheckAt = nextCheck(current, now)
			current.LastCheckError = normalizeErr.Error()
			current.UpdatedAt = now
			getErr = s.store.PutDefinition(ctx, current)
			s.auditLocked(ctx, AuditEntry{
				At: now, Actor: actor, Action: AuditCheck, CLIID: cliID,
				ServerID: current.ServerID, Result: AuditFailed, Detail: normalizeErr.Error(),
			})
		}
		s.mu.Unlock()
		if getErr != nil {
			return report, getErr
		}
		report.Result = "failed"
		report.Detail = normalizeErr.Error()
		return report, nil
	}

	s.mu.Lock()
	current, found, getErr := s.store.GetDefinition(ctx, cliID)
	if getErr != nil {
		s.mu.Unlock()
		return report, getErr
	}
	if !found {
		s.mu.Unlock()
		return report, ErrNotFound
	}
	if current.CheckGeneration != definition.CheckGeneration {
		// Another check moved the candidate set while this one was reading
		// the feed. Its answer is stale; it cannot overwrite the newer set.
		s.mu.Unlock()
		report.Result = "superseded"
		report.Detail = "another check advanced the candidate set"
		return report, nil
	}
	mergeCandidates(&current, candidates)
	current.CheckGeneration++
	current.LastCheckAt = now
	current.NextCheckAt = nextCheck(current, now)
	current.LastCheckError = ""
	current.UpdatedAt = now
	if err := s.store.PutDefinition(ctx, current); err != nil {
		s.mu.Unlock()
		return report, err
	}
	s.auditLocked(ctx, AuditEntry{
		At: now, Actor: actor, Action: AuditCheck, CLIID: cliID,
		ServerID: current.ServerID, Result: AuditOK,
		Detail: fmt.Sprintf("candidates=%d", len(current.Candidates)),
	})
	report.Generation = current.CheckGeneration
	report.Candidates = len(current.Candidates)
	report.Result = "ok"
	autoRelease := current.AutoRelease
	highest := highestCandidate(current.Candidates)
	currentVersion := currentVersion(current)
	s.mu.Unlock()

	// Auto-release is an upgrade path: it never republishes the current
	// version and never silently downgrades to a lower candidate.
	if autoRelease && highest != "" && (currentVersion == "" || versionLess(currentVersion, highest)) {
		report.AutoRelease = true
		outcome, publishErr := s.Publish(ctx, cliID, highest, report.Generation, generationOf(current.Current), actor)
		if publishErr != nil {
			return report, publishErr
		}
		report.Publish = &outcome
	}
	return report, nil
}

// Publish verifies every required platform and atomically switches the
// pointer. A failed verification keeps the current pointer. A rollback is a
// publish of another version already verified, so in-flight attempts keep
// their frozen artifacts.
func (s *Service) Publish(ctx context.Context, cliID, version string, expectedGeneration, expectedReleaseGeneration uint64, actor string) (PublishOutcome, error) {
	now := s.now()
	version = strings.TrimSpace(version)
	outcome := PublishOutcome{CLIID: cliID, Version: version, At: now}
	s.mu.Lock()
	definition, ok, err := s.store.GetDefinition(ctx, cliID)
	if err != nil {
		s.mu.Unlock()
		return outcome, err
	}
	if !ok {
		s.mu.Unlock()
		return outcome, ErrNotFound
	}
	if expectedGeneration != definition.CheckGeneration {
		outcome.Result = PublishConflict
		outcome.Generation = definition.CheckGeneration
		outcome.Reasons = []string{ErrGenerationStale.Error()}
		outcome.KeptVersion = currentVersion(definition)
		s.mu.Unlock()
		return outcome, nil
	}
	if expectedReleaseGeneration != generationOf(definition.Current) {
		outcome.Result = PublishConflict
		outcome.Generation = definition.CheckGeneration
		outcome.Reasons = []string{ErrReleaseStale.Error()}
		outcome.KeptVersion = currentVersion(definition)
		s.mu.Unlock()
		return outcome, nil
	}
	if version == "" {
		outcome.Result = PublishFailed
		outcome.KeptVersion = currentVersion(definition)
		outcome.Reasons = []string{"version is required"}
		s.mu.Unlock()
		return outcome, nil
	}
	if definition.MinimumVersion != "" && versionLess(version, definition.MinimumVersion) {
		outcome.Result = PublishFailed
		outcome.KeptVersion = currentVersion(definition)
		outcome.Reasons = []string{fmt.Sprintf("version %s is below the minimum %s", version, definition.MinimumVersion)}
		s.auditLocked(ctx, AuditEntry{At: now, Actor: actor, Action: AuditPublish, CLIID: cliID, ServerID: definition.ServerID, Version: version, Result: AuditRejected, Detail: outcome.Reasons[0]})
		s.mu.Unlock()
		return outcome, nil
	}
	candidate, hasCandidate := findCandidate(definition, version)
	verified, hasVerified := definition.Verified[version]
	var artifacts []Artifact
	var checks []Verification
	switch {
	case hasVerified:
		if hasCandidate && !sameArtifacts(candidate.Artifacts, verified.Artifacts) {
			outcome.Result = PublishFailed
			outcome.KeptVersion = currentVersion(definition)
			outcome.Reasons = []string{ErrVersionImmutable.Error()}
			s.mu.Unlock()
			return outcome, nil
		}
		artifacts = append([]Artifact(nil), verified.Artifacts...)
		checks = append([]Verification(nil), verified.Checks...)
	case hasCandidate:
		artifacts = append([]Artifact(nil), candidate.Artifacts...)
	default:
		s.mu.Unlock()
		return outcome, ErrVersionNotFound
	}
	required := append([]Platform(nil), definition.RequiredPlatforms...)
	s.mu.Unlock()

	// A verified version is immutable, but the required platform set is part
	// of the release contract: a version that no longer covers every required
	// platform cannot be republished as a rollback.
	reasons := make([]string, 0)
	if err := validateArtifactSet(artifacts); err != nil {
		reasons = append(reasons, err.Error())
	}
	for _, gap := range platformGaps(artifacts, required) {
		reasons = append(reasons, "no artifact for "+gap)
	}
	if len(reasons) > 0 {
		outcome.Result = PublishFailed
		outcome.KeptVersion = currentVersion(definition)
		outcome.Reasons = reasons
		s.mu.Lock()
		s.auditLocked(ctx, AuditEntry{At: now, Actor: actor, Action: AuditPublish, CLIID: cliID, ServerID: definition.ServerID, Version: version, OldVersion: outcome.KeptVersion, Result: AuditFailed, Detail: strings.Join(reasons, "; ")})
		s.mu.Unlock()
		return outcome, nil
	}

	if !hasVerified {
		covered := map[string]Verification{}
		for _, platform := range required {
			artifact, found := artifactFor(artifacts, platform)
			if !found {
				// The gap was already reported above; keep the verification
				// list aligned with the required platform order.
				covered[platform.Key()] = Verification{Platform: platform, Reason: "no artifact"}
				continue
			}
			verification := s.cfg.Verifier.Verify(ctx, definition.ServerID, artifact)
			covered[platform.Key()] = verification
			if !verification.OK {
				reasons = append(reasons, fmt.Sprintf("%s: %s", platform.Key(), verification.Reason))
			}
		}
		checks = make([]Verification, 0, len(covered))
		for _, platform := range required {
			checks = append(checks, covered[platform.Key()])
		}
		if len(reasons) > 0 {
			outcome.Result = PublishFailed
			outcome.KeptVersion = currentVersion(definition)
			outcome.Reasons = reasons
			s.mu.Lock()
			s.auditLocked(ctx, AuditEntry{At: now, Actor: actor, Action: AuditPublish, CLIID: cliID, ServerID: definition.ServerID, Version: version, OldVersion: outcome.KeptVersion, Result: AuditFailed, Detail: strings.Join(reasons, "; ")})
			s.mu.Unlock()
			return outcome, nil
		}
	}

	s.mu.Lock()
	snapshot, found, getErr := s.store.GetDefinition(ctx, cliID)
	s.mu.Unlock()
	if getErr != nil {
		return outcome, getErr
	}
	if !found {
		return outcome, ErrNotFound
	}
	if snapshot.CheckGeneration != expectedGeneration {
		outcome.Result = PublishConflict
		outcome.Generation = snapshot.CheckGeneration
		outcome.Reasons = []string{ErrGenerationStale.Error()}
		outcome.KeptVersion = currentVersion(snapshot)
		return outcome, nil
	}

	impact, impactErr := s.impactFor(ctx, snapshot, version)
	if impactErr != nil && !errors.Is(impactErr, ErrGenerationStale) {
		s.log.Warn("release.impact_unavailable", "name", cliID, "err", impactErr.Error())
	}

	s.mu.Lock()
	current, found, getErr := s.store.GetDefinition(ctx, cliID)
	if getErr != nil {
		s.mu.Unlock()
		return outcome, getErr
	}
	if !found {
		s.mu.Unlock()
		return outcome, ErrNotFound
	}
	if current.CheckGeneration != expectedGeneration {
		outcome.Result = PublishConflict
		outcome.Generation = current.CheckGeneration
		outcome.Reasons = []string{ErrGenerationStale.Error()}
		outcome.KeptVersion = currentVersion(current)
		s.mu.Unlock()
		return outcome, nil
	}
	if expectedReleaseGeneration != generationOf(current.Current) &&
		!(current.Current != nil && current.Current.Version == version &&
			sameArtifacts(current.Current.Artifacts, artifacts)) {
		outcome.Result = PublishConflict
		outcome.Generation = current.CheckGeneration
		outcome.Reasons = []string{ErrReleaseStale.Error()}
		outcome.KeptVersion = currentVersion(current)
		s.mu.Unlock()
		return outcome, nil
	}
	if current.Current != nil && current.Current.Version == version &&
		sameArtifacts(current.Current.Artifacts, artifacts) {
		// A repeated request converges on the pointer it already has.
		s.auditLocked(ctx, AuditEntry{
			At: now, Actor: actor, Action: AuditPublish, CLIID: cliID, ServerID: current.ServerID,
			Version: version, OldVersion: version, Result: AuditOK, Detail: "already current",
			Known: impact.Known, Agents: impact.AffectedAgents,
			Workspaces: len(impact.Workspaces), InFlight: len(impact.InFlight),
		})
		existing := *current.Current
		s.mu.Unlock()
		outcome.Result = PublishPublished
		outcome.Generation = existing.Generation
		outcome.Published = &existing
		return outcome, nil
	}
	previous := currentVersion(current)
	generation := uint64(1)
	if current.Current != nil {
		generation = current.Current.Generation + 1
	}
	published := &Release{
		Version: version, Generation: generation, PublishedAt: now,
		Artifacts: append([]Artifact(nil), artifacts...), Actor: actor,
	}
	current.Current = published
	current.Releases = append(current.Releases, *published)
	if len(current.Releases) > MaxReleases {
		current.Releases = append([]Release(nil), current.Releases[len(current.Releases)-MaxReleases:]...)
	}
	if !hasVerified {
		if current.Verified == nil {
			current.Verified = map[string]VerifiedVersion{}
		}
		current.Verified[version] = VerifiedVersion{
			Artifacts: append([]Artifact(nil), artifacts...),
			Checks:    append([]Verification(nil), checks...),
			At:        now,
		}
	}
	current.UpdatedAt = now
	if err := s.store.PutDefinition(ctx, current); err != nil {
		s.mu.Unlock()
		return outcome, err
	}
	s.auditLocked(ctx, AuditEntry{
		At: now, Actor: actor, Action: AuditPublish, CLIID: cliID, ServerID: current.ServerID,
		Version: version, OldVersion: previous, Result: AuditOK,
		Known: impact.Known, Agents: impact.AffectedAgents,
		Workspaces: len(impact.Workspaces), InFlight: len(impact.InFlight),
	})
	s.mu.Unlock()

	outcome.Result = PublishPublished
	outcome.Generation = generation
	outcome.Published = published
	return outcome, nil
}

// Impact answers what the next start resolves for one candidate version. It
// is a read-only snapshot and never changes the pointer.
func (s *Service) Impact(ctx context.Context, request ImpactRequest) (ImpactView, error) {
	s.mu.Lock()
	definition, ok, err := s.store.GetDefinition(ctx, request.CLIID)
	s.mu.Unlock()
	if err != nil {
		return ImpactView{}, err
	}
	if !ok {
		return ImpactView{}, ErrNotFound
	}
	if request.ExpectedGeneration != 0 && request.ExpectedGeneration != definition.CheckGeneration {
		return ImpactView{}, ErrGenerationStale
	}
	if request.ExpectedReleaseGeneration != 0 &&
		request.ExpectedReleaseGeneration != generationOf(definition.Current) {
		return ImpactView{}, ErrReleaseStale
	}
	return s.impactFor(ctx, definition, request.Version)
}

func (s *Service) impactFor(ctx context.Context, definition Definition, version string) (ImpactView, error) {
	if version == "" {
		version = currentVersion(definition)
	}
	view := ImpactView{
		CLIID: definition.ID, ServerID: definition.ServerID, Kind: string(definition.Kind),
		Adapter: definition.Adapter, Candidate: version, CurrentVersion: currentVersion(definition),
		ReleaseGeneration: generationOf(definition.Current),
		Workspaces:        []ImpactWorkspace{}, Agents: []ImpactAgent{}, InFlight: []ImpactRun{},
	}
	unknown := func(reason string) {
		view.Known = false
		view.AffectedAgents = nil
		view.Unknown = append(view.Unknown, reason)
	}
	if s.cfg.Directory == nil {
		unknown("the source directory is not wired")
		return view, nil
	}
	source, ok := s.cfg.Directory.ReadSource(ctx, definition.ServerID)
	if !ok {
		unknown("the release domain is not a registered source")
		return view, nil
	}
	if !source.Synced {
		unknown("the source directory has not completed a sync")
		return view, nil
	}
	if source.Stale {
		reason := "the source directory is stale"
		if source.Detail != "" {
			reason += ": " + source.Detail
		}
		unknown(reason)
		return view, nil
	}

	workspaceNames := map[string]string{}
	for _, workspace := range source.Workspaces {
		workspaceNames[workspace.ID] = workspace.Name
	}
	seenWorkspaces := map[string]bool{}
	agents := make([]ImpactAgent, 0)
	for _, agent := range source.Agents {
		if !agent.Managed {
			continue
		}
		if definition.Kind == KindAgent && !strings.EqualFold(agent.Adapter, definition.Adapter) {
			continue
		}
		name := workspaceNames[agent.WorkspaceID]
		if name == "" {
			name = agent.WorkspaceName
		}
		agents = append(agents, ImpactAgent{
			WorkspaceID: agent.WorkspaceID, WorkspaceName: name,
			AgentID: agent.AgentID, AgentName: agent.AgentName,
			RuntimeID: agent.RuntimeID, Adapter: agent.Adapter,
			CurrentVersion: currentVersion(definition),
		})
		if !seenWorkspaces[agent.WorkspaceID] {
			seenWorkspaces[agent.WorkspaceID] = true
			view.Workspaces = append(view.Workspaces, ImpactWorkspace{WorkspaceID: agent.WorkspaceID, Name: name})
		}
	}
	view.Agents = agents
	count := len(agents)
	view.AffectedAgents = &count
	view.Known = true

	if s.cfg.Runs == nil {
		unknown("in-flight run facts are not wired")
		return view, nil
	}
	for _, run := range s.cfg.Runs.Running(ctx) {
		if run.ServerID == "" {
			unknown("an in-flight attempt has no registered release domain")
			continue
		}
		if run.ServerID != definition.ServerID {
			continue
		}
		if definition.Kind == KindAgent {
			switch {
			case run.Adapter == "":
				// The adapter was not frozen with the claim; the attempt may
				// belong to this line, so it is unknown rather than omitted.
				unknown("an in-flight attempt has no frozen adapter")
			case !strings.EqualFold(run.Adapter, definition.Adapter):
				continue
			}
		}
		frozenVersion, digests, known := run.MulticaVersion, run.MulticaDigests, run.CLIKnown
		if definition.Kind == KindAgent {
			frozenVersion, digests, known = run.AgentVersion, run.AgentDigests, run.AgentKnown
		}
		if known && frozenVersion == version {
			// This attempt already runs the candidate; it is not an old run.
			continue
		}
		view.InFlight = append(view.InFlight, ImpactRun{
			TaskID: run.TaskID, WorkspaceID: run.WorkspaceID,
			AgentID: run.AgentID, AgentName: run.AgentName,
			Version: frozenVersion, VersionKnown: known, Digests: digests,
			ConfigRevision: run.ConfigRevision, ConfigRevisionKnown: run.ConfigKnown,
			StartedAt: stamp(run.StartedAt),
		})
	}
	return view, nil
}

// List returns every definition.
func (s *Service) List(ctx context.Context) ([]DefinitionView, error) {
	definitions, err := s.store.ListDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]DefinitionView, 0, len(definitions))
	for _, definition := range definitions {
		out = append(out, definitionView(definition))
	}
	return out, nil
}

// Get returns one definition.
func (s *Service) Get(ctx context.Context, id string) (DefinitionView, bool, error) {
	definition, ok, err := s.store.GetDefinition(ctx, id)
	if err != nil || !ok {
		return DefinitionView{}, ok, err
	}
	return definitionView(definition), true, nil
}

// Adapter returns the adapter and current artifact digest an Agent CLI
// definition declares. It is the discovery service's lookup seam.
func (s *Service) Adapter(ctx context.Context, serverID, cliID string) (adapter, digest string, ok bool) {
	return s.AdapterForPlatform(ctx, serverID, cliID, "")
}

// AdapterForPlatform answers the adapter and the current artifact digest for
// one platform. A discovery cache key includes the platform, so the digest
// must be the payload that platform would actually receive.
func (s *Service) AdapterForPlatform(ctx context.Context, serverID, cliID, platform string) (adapter, digest string, ok bool) {
	definition, found, err := s.store.GetDefinition(ctx, cliID)
	if err != nil || !found || definition.ServerID != serverID {
		return "", "", false
	}
	if definition.Current != nil && len(definition.Current.Artifacts) > 0 {
		if platform == "" {
			// A missing platform is not a wildcard: the caller must name the
			// payload platform, or resolve the binding before asking.
			return definition.Adapter, "", true
		} else {
			for _, artifact := range definition.Current.Artifacts {
				if artifact.Platform.Key() == strings.ToLower(platform) {
					digest = artifact.Digest
					break
				}
			}
		}
	}
	return definition.Adapter, digest, true
}

// CurrentVersion answers the published version of one release line. An empty
// answer means no pointer exists, never a guessed builtin version.
func (s *Service) CurrentVersion(ctx context.Context, serverID string, kind Kind, adapter string) (string, bool) {
	definitions, err := s.store.ListDefinitions(ctx)
	if err != nil {
		return "", false
	}
	version := ""
	for _, definition := range definitions {
		if definition.ServerID != serverID || definition.Kind != kind {
			continue
		}
		if kind == KindAgent && !strings.EqualFold(definition.Adapter, adapter) {
			continue
		}
		current := currentVersion(definition)
		if current == "" {
			continue
		}
		if version == "" || versionLess(version, current) {
			version = current
		}
	}
	return version, version != ""
}

// CurrentRelease answers the published version and its verified artifact
// digests per platform for one release line. It is the claim-time snapshot:
// the run keeps this pointer even after a later publish moves the definition.
func (s *Service) CurrentRelease(ctx context.Context, serverID string, kind Kind, adapter string) (string, map[string]string, bool) {
	definitions, err := s.store.ListDefinitions(ctx)
	if err != nil {
		return "", nil, false
	}
	var selected *Definition
	for i := range definitions {
		definition := &definitions[i]
		if definition.ServerID != serverID || definition.Kind != kind {
			continue
		}
		if kind == KindAgent && !strings.EqualFold(definition.Adapter, adapter) {
			continue
		}
		version := currentVersion(*definition)
		if version == "" {
			continue
		}
		if selected == nil || versionLess(currentVersion(*selected), version) {
			selected = definition
		}
	}
	if selected == nil || selected.Current == nil {
		return "", nil, false
	}
	digests := make(map[string]string, len(selected.Current.Artifacts))
	for _, artifact := range selected.Current.Artifacts {
		digests[artifact.Platform.Key()] = artifact.Digest
	}
	return selected.Current.Version, digests, true
}

// CurrentDigest answers the published artifact digest one adapter would
// receive on a platform.
func (s *Service) CurrentDigest(ctx context.Context, serverID, adapter, platform string) (string, bool) {
	definitions, err := s.store.ListDefinitions(ctx)
	if err != nil {
		return "", false
	}
	var selected *Definition
	for i := range definitions {
		definition := &definitions[i]
		if definition.ServerID != serverID || definition.Kind != KindAgent ||
			!strings.EqualFold(definition.Adapter, adapter) || definition.Current == nil {
			continue
		}
		if selected == nil || versionLess(currentVersion(*selected), currentVersion(*definition)) {
			selected = definition
		}
	}
	if selected == nil {
		return "", false
	}
	for _, artifact := range selected.Current.Artifacts {
		if strings.EqualFold(artifact.Platform.Key(), platform) {
			return artifact.Digest, true
		}
	}
	return "", false
}

// VerifiedArtifact returns the bytes of one published or retained release
// digest. A cache miss refetches the same digest from the registered source;
// a digest mismatch or unavailable source is an explicit failure.
func (s *Service) VerifiedArtifact(ctx context.Context, cliID, digest string) ([]byte, error) {
	digest = strings.ToLower(strings.TrimSpace(digest))
	if !digestPattern.MatchString(digest) {
		return nil, ErrArtifactNotFound
	}
	definition, found, err := s.store.GetDefinition(ctx, cliID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}
	artifact, ok := artifactByDigest(definition, digest)
	if !ok {
		return nil, ErrArtifactNotFound
	}
	if s.cfg.Artifacts != nil {
		if body, hit, err := s.cfg.Artifacts.Get(ctx, definition.ServerID, digest); err != nil {
			return nil, err
		} else if hit {
			return body, nil
		}
	}
	verification, body := s.cfg.Verifier.VerifyBytes(ctx, definition.ServerID, artifact)
	if !verification.OK {
		return nil, fmt.Errorf("verified artifact %s is unavailable: %s", digest, verification.Reason)
	}
	return body, nil
}

// VerifiedArtifactByDomain returns bytes for a digest published anywhere in
// one release domain. It is the discovery executor's read seam.
func (s *Service) VerifiedArtifactByDomain(ctx context.Context, serverID, digest string) ([]byte, error) {
	definitions, err := s.store.ListDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	for _, definition := range definitions {
		if definition.ServerID != serverID {
			continue
		}
		if _, ok := artifactByDigest(definition, digest); ok {
			return s.VerifiedArtifact(ctx, definition.ID, digest)
		}
	}
	return nil, ErrArtifactNotFound
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

// Run is the automatic check loop. A check failure is recorded on the
// definition; it never stops another definition from being checked.
func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.checkDue(ctx)
		}
	}
}

func (s *Service) checkDue(ctx context.Context) {
	definitions, err := s.store.ListDefinitions(ctx)
	if err != nil {
		s.log.Warn("release.definitions_unreadable", "err", err.Error())
		return
	}
	now := s.now()
	for _, definition := range definitions {
		if !definition.AutoCheck {
			continue
		}
		if !definition.NextCheckAt.IsZero() && definition.NextCheckAt.After(now) {
			continue
		}
		if _, err := s.Check(ctx, definition.ID, "automatic"); err != nil {
			s.log.Warn("release.check_failed", "name", definition.ID, "err", err.Error())
		}
	}
}

func (s *Service) auditLocked(ctx context.Context, entry AuditEntry) {
	if err := s.store.AppendAudit(ctx, entry); err != nil {
		s.log.Warn("release.audit_failed", "action", entry.Action, "err", err.Error())
	}
}

func nextCheck(definition Definition, now time.Time) time.Time {
	if !definition.AutoCheck {
		return time.Time{}
	}
	return now.Add(checkPeriod(definition))
}

func checkPeriod(definition Definition) time.Duration {
	if definition.CheckPeriodSeconds <= 0 {
		return DefaultCheckPeriod
	}
	return time.Duration(definition.CheckPeriodSeconds) * time.Second
}

func currentVersion(definition Definition) string {
	if definition.Current == nil {
		return ""
	}
	return definition.Current.Version
}

func definitionView(definition Definition) DefinitionView {
	platforms := make([]string, 0, len(definition.RequiredPlatforms))
	for _, platform := range definition.RequiredPlatforms {
		platforms = append(platforms, platform.Key())
	}
	candidates := make([]string, 0, len(definition.Candidates))
	for _, candidate := range definition.Candidates {
		candidates = append(candidates, candidate.Version)
	}
	releases := append([]Release{}, definition.Releases...)
	return DefinitionView{
		CLIID: definition.ID, Kind: string(definition.Kind), Adapter: definition.Adapter,
		ServerID: definition.ServerID, RequiredPlatforms: platforms,
		MinimumVersion: definition.MinimumVersion, CurrentVersion: currentVersion(definition),
		CurrentGeneration: generationOf(definition.Current),
		AutoCheck:         definition.AutoCheck, CheckPeriodSeconds: definition.CheckPeriodSeconds,
		AutoRelease: definition.AutoRelease, CheckGeneration: definition.CheckGeneration,
		Candidates: candidates, Releases: releases,
		LastCheckAt: stamp(definition.LastCheckAt), NextCheckAt: stamp(definition.NextCheckAt),
		LastCheckError: definition.LastCheckError, UpdatedAt: stamp(definition.UpdatedAt),
	}
}

func generationOf(release *Release) uint64 {
	if release == nil {
		return 0
	}
	return release.Generation
}

func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func findCandidate(definition Definition, version string) (Candidate, bool) {
	for _, candidate := range definition.Candidates {
		if candidate.Version == version {
			return candidate, true
		}
	}
	return Candidate{}, false
}

func artifactFor(artifacts []Artifact, platform Platform) (Artifact, bool) {
	for _, artifact := range artifacts {
		if artifact.Platform.Key() == platform.Key() {
			return artifact, true
		}
	}
	return Artifact{}, false
}

func artifactByDigest(definition Definition, digest string) (Artifact, bool) {
	// The display history is bounded, but the verified set is the retention
	// record: a digest that was verified and is still referenced by a frozen
	// attempt must remain readable after its release leaves the window.
	for _, verified := range definition.Verified {
		for _, artifact := range verified.Artifacts {
			if strings.EqualFold(artifact.Digest, digest) {
				return artifact, true
			}
		}
	}
	releases := append([]Release(nil), definition.Releases...)
	if definition.Current != nil {
		releases = append(releases, *definition.Current)
	}
	for _, release := range releases {
		for _, artifact := range release.Artifacts {
			if strings.EqualFold(artifact.Digest, digest) {
				return artifact, true
			}
		}
	}
	return Artifact{}, false
}

// ArtifactFor answers one platform's entry in an artifact set.
func ArtifactFor(artifacts []Artifact, platform string) (Artifact, bool) {
	key := strings.ToLower(strings.TrimSpace(platform))
	for _, artifact := range artifacts {
		if strings.EqualFold(artifact.Platform.Key(), key) {
			return artifact, true
		}
	}
	return Artifact{}, false
}

// VerifiedAgentRelease is one Agent CLI definition whose current release is
// covered by a successful verification record for every required platform.
// It is the registration eligibility seam: a Current pointer alone is not
// proof that the published payload passed verification.
type VerifiedAgentRelease struct {
	CLIID      string
	Adapter    string
	Version    string
	Artifacts  []Artifact
	VerifiedAt time.Time
}

// VerifiedAgentReleases lists the current release of each Agent definition in
// one domain that still has a matching successful verification record.
func (s *Service) VerifiedAgentReleases(ctx context.Context, serverID string) ([]VerifiedAgentRelease, error) {
	definitions, err := s.store.ListDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]VerifiedAgentRelease, 0)
	for _, definition := range definitions {
		if definition.Kind != KindAgent || definition.ServerID != serverID || definition.Current == nil {
			continue
		}
		version := definition.Current.Version
		if version == "" {
			continue
		}
		verified, ok := definition.Verified[version]
		if !ok || !sameArtifacts(verified.Artifacts, definition.Current.Artifacts) {
			continue
		}
		if !verificationCovers(definition.RequiredPlatforms, definition.Current.Artifacts, verified.Checks) {
			continue
		}
		out = append(out, VerifiedAgentRelease{
			CLIID: definition.ID, Adapter: definition.Adapter, Version: version,
			Artifacts:  append([]Artifact(nil), definition.Current.Artifacts...),
			VerifiedAt: verified.At,
		})
	}
	return out, nil
}

func verificationCovers(required []Platform, artifacts []Artifact, checks []Verification) bool {
	if len(artifacts) == 0 {
		return false
	}
	for _, platform := range required {
		artifact, ok := artifactFor(artifacts, platform)
		if !ok {
			return false
		}
		covered := false
		for _, check := range checks {
			if check.Platform.Key() == platform.Key() && check.OK &&
				strings.EqualFold(check.Digest, artifact.Digest) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func platformGaps(artifacts []Artifact, required []Platform) []string {
	gaps := make([]string, 0)
	for _, platform := range required {
		if _, ok := artifactFor(artifacts, platform); !ok {
			gaps = append(gaps, platform.Key())
		}
	}
	return gaps
}

func sameArtifacts(a, b []Artifact) bool {
	if len(a) != len(b) {
		return false
	}
	index := make(map[string]Artifact, len(b))
	for _, artifact := range b {
		index[artifact.Platform.Key()] = artifact
	}
	for _, artifact := range a {
		other, ok := index[artifact.Platform.Key()]
		if !ok || !strings.EqualFold(artifact.Digest, other.Digest) || artifact.Size != other.Size {
			return false
		}
	}
	return true
}

func upsertCandidate(definition *Definition, candidate Candidate) {
	for i := range definition.Candidates {
		if definition.Candidates[i].Version == candidate.Version {
			definition.Candidates[i] = candidate
			return
		}
	}
	definition.Candidates = append(definition.Candidates, candidate)
	if len(definition.Candidates) > MaxCandidates {
		definition.Candidates = append([]Candidate(nil), definition.Candidates[len(definition.Candidates)-MaxCandidates:]...)
	}
}

func mergeCandidates(definition *Definition, feed []Candidate) {
	kept := make([]Candidate, 0, len(definition.Candidates))
	for _, candidate := range definition.Candidates {
		if candidate.Manual {
			kept = append(kept, candidate)
		}
	}
	for _, candidate := range feed {
		candidate.Manual = false
		replaced := false
		for i := range kept {
			if kept[i].Version == candidate.Version {
				// A manual entry is operator input: a feed with the same
				// version does not overwrite its artifacts.
				if !kept[i].Manual {
					kept[i] = candidate
				}
				replaced = true
				break
			}
		}
		if !replaced {
			kept = append(kept, candidate)
		}
	}
	if len(kept) > MaxCandidates {
		kept = append([]Candidate(nil), kept[len(kept)-MaxCandidates:]...)
	}
	definition.Candidates = kept
}

func normalizeCandidates(candidates []Candidate, now time.Time) ([]Candidate, error) {
	out := make([]Candidate, 0, len(candidates))
	seen := map[string]bool{}
	for _, candidate := range candidates {
		version := strings.TrimSpace(candidate.Version)
		if version == "" {
			return nil, errors.New("version feed carries a candidate without a version")
		}
		if seen[version] {
			return nil, fmt.Errorf("version feed declares %s twice", version)
		}
		if len(candidate.Artifacts) == 0 {
			return nil, fmt.Errorf("version %s carries no artifacts", version)
		}
		if err := validateArtifactSet(candidate.Artifacts); err != nil {
			return nil, fmt.Errorf("version %s: %w", version, err)
		}
		for i := range candidate.Artifacts {
			candidate.Artifacts[i].Digest = strings.ToLower(candidate.Artifacts[i].Digest)
			candidate.Artifacts[i].Platform.OS = strings.ToLower(candidate.Artifacts[i].Platform.OS)
			candidate.Artifacts[i].Platform.Arch = strings.ToLower(candidate.Artifacts[i].Platform.Arch)
		}
		candidate.Version = version
		candidate.AddedAt = now
		candidate.Manual = false
		out = append(out, candidate)
		seen[version] = true
	}
	return out, nil
}

func highestCandidate(candidates []Candidate) string {
	best := ""
	for _, candidate := range candidates {
		if best == "" || versionLess(best, candidate.Version) {
			best = candidate.Version
		}
	}
	return best
}

// manifestSource reads the definition's trusted version manifest. The
// manifest is JSON: either {"versions":[...]} or a bare array of candidates.
type manifestSource struct {
	verifier Verifier
}

// Fetch downloads and decodes one manifest.
func (m manifestSource) Fetch(ctx context.Context, definition Definition) ([]Candidate, error) {
	if strings.TrimSpace(definition.ManifestURL) == "" {
		return nil, errors.New("no version feed is configured")
	}
	if m.verifier.Fetcher == nil {
		return nil, errors.New("no manifest fetcher is wired")
	}
	raw, err := m.verifier.Fetcher.Fetch(ctx, definition.ManifestURL, 4<<20)
	if err != nil {
		return nil, fmt.Errorf("read version manifest: %w", err)
	}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || (trimmed[0] != '{' && trimmed[0] != '[') {
		return nil, errors.New("version manifest is not the expected JSON")
	}
	var document struct {
		Versions *[]Candidate `json:"versions"`
	}
	if err := json.Unmarshal(raw, &document); err == nil && document.Versions != nil {
		return *document.Versions, nil
	}
	var list []Candidate
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, errors.New("version manifest is not the expected JSON")
	}
	return list, nil
}
