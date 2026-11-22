package environment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
)

// Config is the environment service's configuration.
type Config struct {
	// Store persists registrations and tombstones. Defaults to an empty
	// MemoryStore.
	Store Store
	// Probe verifies one target. Required.
	Probe Probe
	// AllowedTrustDomains bounds the data-residency domains a target may
	// declare. Empty means the deployment has not approved any domain, so every
	// registration is refused until an allowlist is configured.
	AllowedTrustDomains []string
	// RuleReferences answers the published and drafted rules that still name
	// an environment. Nil means the rule manager is not wired; retirement then
	// has no rule gate.
	RuleReferences RuleReferenceSource
	// ProbeTimeout bounds one verification attempt.
	ProbeTimeout time.Duration
	// Clock is the service's single clock seam.
	Clock func() time.Time
	// Logger defaults to slog.Default.
	Logger *slog.Logger
}

// Service owns the environment registry, the verification lifecycle and the
// slot ledger. Every read and mutation runs through one store transaction, so
// the decision and the commit use the same snapshot; probes run before the
// transaction and the record revision is re-checked before applying an answer.
type Service struct {
	cfg   Config
	store TransactionalStore
	now   func() time.Time
	log   *slog.Logger
}

// New builds the service.
func New(cfg Config) (*Service, error) {
	if cfg.Probe == nil {
		return nil, errors.New("environment: a probe is required")
	}
	domains, err := normalizeTrustDomains(cfg.AllowedTrustDomains)
	if err != nil {
		return nil, err
	}
	cfg.AllowedTrustDomains = domains
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore()
	}
	store, ok := cfg.Store.(TransactionalStore)
	if !ok {
		store = &serialStore{Store: cfg.Store}
		cfg.Store = store
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = DefaultProbeTimeout
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Service{
		cfg:   cfg,
		store: store,
		now:   cfg.Clock,
		log:   cfg.Logger.With("component", "environment"),
	}, nil
}

// RuleReferenceIndex answers the concrete index this service was configured
// with. The rule manager fills it so a retired environment cannot be deleted
// out from under a drafted or published rule. Nil means the deployment did not
// wire a rule manager.
func (s *Service) RuleReferenceIndex() *MemoryRuleReferences {
	refs, _ := s.cfg.RuleReferences.(*MemoryRuleReferences)
	return refs
}

// storeConflictRetries bounds how many times a transaction is re-run after the
// durable store reports that another writer committed first.
const storeConflictRetries = 3

// errProbeStale means the record changed while its probe was running, so the
// probe answer no longer describes the stored target. Verify re-probes instead
// of applying it.
var errProbeStale = errors.New("environment changed during the probe")

// transact runs one read-decide-write sequence on one committed snapshot.
func (s *Service) transact(ctx context.Context, fn func(context.Context, Store) error) error {
	return s.store.Transact(ctx, fn)
}

// snapshotEnvironment reads one registration through a transaction so the
// target used for a probe is the committed one, not a cached copy.
func (s *Service) snapshotEnvironment(ctx context.Context, id ID) (Environment, bool, error) {
	var env Environment
	var ok bool
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		var err error
		env, ok, err = tx.GetEnvironment(ctx, id)
		return err
	})
	return env, ok, err
}

// ledgerEpoch names one immutable ledger generation. The carrier version is
// the primary fence: it lives on the carrier object, advances on every commit
// and is untouched by restoring a document, so a same-object backup restore
// still rejects old evidence. The in-document identity and counter are
// secondary checks that also catch a carrier swapped under the store.
type ledgerEpoch struct {
	carrier    string
	instance   string
	generation uint64
}

// ledgerEpochOf reads the epoch of the transaction's snapshot.
func ledgerEpochOf(ctx context.Context, tx Store) (ledgerEpoch, error) {
	carrier, err := tx.CarrierVersion(ctx)
	if err != nil {
		return ledgerEpoch{}, err
	}
	instance, err := tx.LedgerID(ctx)
	if err != nil {
		return ledgerEpoch{}, err
	}
	generation, err := tx.Generation(ctx)
	if err != nil {
		return ledgerEpoch{}, err
	}
	return ledgerEpoch{carrier: carrier, instance: instance, generation: generation}, nil
}

// snapshotLedgerEpoch reads the committed ledger epoch through one
// transaction, so the answer names exactly one committed carrier state.
func (s *Service) snapshotLedgerEpoch(ctx context.Context) (ledgerEpoch, error) {
	var epoch ledgerEpoch
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		var err error
		epoch, err = ledgerEpochOf(ctx, tx)
		return err
	})
	return epoch, err
}

// Register stores one target and verifies it. A target whose verification
// fails is still stored, with the failure on its face, so the operator can
// repair the configuration and verify again; it is never a scheduling
// candidate until a verification succeeds.
func (s *Service) Register(ctx context.Context, registration Registration, actor string) (EnvironmentView, error) {
	id, target, expectedIdentity, err := normalizeRegistration(registration)
	if err != nil {
		return EnvironmentView{}, err
	}
	if !s.trustDomainAllowed(target.TrustDomain) {
		return EnvironmentView{}, invalid("trust domain %q is not an allowed data-residency domain", target.TrustDomain)
	}

	// The probe runs before the transaction: a slow target must not hold the
	// ledger. Its answer is bound to the ledger epoch it was observed against,
	// so any change that commits meanwhile, including a restored document,
	// sends the answer back for a fresh probe instead of letting it decide a
	// ledger it never saw.
	for attempt := 0; ; attempt++ {
		epoch, err := s.snapshotLedgerEpoch(ctx)
		if err != nil {
			return EnvironmentView{}, err
		}
		result, probeErr := s.probe(ctx, target)
		now := s.now()

		var view EnvironmentView
		err = s.transact(ctx, func(ctx context.Context, tx Store) error {
			environments, err := tx.ListEnvironments(ctx)
			if err != nil {
				return err
			}
			committed, err := ledgerEpochOf(ctx, tx)
			if err != nil {
				return err
			}
			if committed != epoch {
				return errProbeStale
			}
			if tomb, ok, err := tx.GetTombstone(ctx, id); err != nil {
				return err
			} else if ok {
				return fmt.Errorf("%w (retired %s)", ErrRetired, stamp(tomb.RetiredAt))
			}
			if _, ok, err := tx.GetEnvironment(ctx, id); err != nil {
				return err
			} else if ok {
				return ErrExists
			}

			env := Environment{
				ID:               id,
				Target:           target,
				ExpectedIdentity: expectedIdentity,
				Phase:            PhasePending,
				Slots:            map[string]Slot{},
				Cleanups:         map[string]Cleanup{},
				CreatedAt:        now,
				UpdatedAt:        now,
			}

			if probeErr != nil {
				env.Verification = failedVerification(env.Verification, probeErr, now)
				if err := tx.PutEnvironment(ctx, env); err != nil {
					return err
				}
				s.appendAudit(ctx, tx, AuditRegister, id, actor, AuditFailed, probeErr.Error())
				view = s.view(ctx, tx, env)
				return nil
			}

			// A successful probe still has to pass the target checks. The observed
			// facts are kept even on failure so the operator can see what the target
			// actually reported. A tree id from a failed attempt is an observation,
			// not ownership: only a successful verification freezes TreeID.
			verification, validationErr := verificationFromProbe(target, expectedIdentity, result)
			if validationErr != nil {
				env.Verification = failedVerification(env.Verification, validationErr, now)
				env.Verification.ObservedTreeID = strings.TrimSpace(result.TreeID)
				env.Verification.Identity = strings.TrimSpace(result.Identity)
				env.Verification.Capacity = result.Capacity
				env.Verification.Version = strings.TrimSpace(result.Version)
				if observed, err := normalizeCapabilities(result.Capabilities); err == nil {
					env.Verification.Capabilities = observed
				}
			} else {
				env.Verification = verification
				env.Verification.ObservedTreeID = env.Verification.TreeID
				env.Verification.VerifiedAt = now
				env.Verification.LastAttemptAt = now
				env.Phase = PhaseActive
				env.Registered = true
			}

			treeID := strings.TrimSpace(result.TreeID)
			if treeID != "" {
				for _, other := range environments {
					if other.Verification.TreeID == treeID {
						return fmt.Errorf("%w (%s)", ErrTreeExists, other.ID)
					}
				}
			}

			var superseded []Environment
			if validationErr == nil {
				superseded = s.supersededRegistrations(environments, env)
			}
			batch := append([]Environment{env}, superseded...)
			if err := tx.PutEnvironments(ctx, batch); err != nil {
				return err
			}
			for _, other := range superseded {
				s.appendAudit(ctx, tx, AuditSupersede, other.ID, actor, AuditFailed, "superseded by "+string(env.ID))
			}
			if validationErr != nil {
				s.appendAudit(ctx, tx, AuditRegister, id, actor, AuditFailed, validationErr.Error())
			} else {
				s.appendAudit(ctx, tx, AuditRegister, id, actor, AuditOK, "verified")
			}
			view = s.view(ctx, tx, env)
			return nil
		})
		if errors.Is(err, errProbeStale) && attempt < storeConflictRetries {
			continue
		}
		if errors.Is(err, errProbeStale) {
			return EnvironmentView{}, ErrConflict
		}
		if err != nil {
			return EnvironmentView{}, err
		}
		return view, nil
	}
}

// supersededRegistrations marks every other registration that points at
// the same verified endpoint but a different persistent tree as no longer
// matching its target. The state root alone is not an ownership key: two hosts
// commonly use the same path, and they are different trees. The old ID is not
// silently re-pointed at the new tree.
func (s *Service) supersededRegistrations(environments []Environment, replacement Environment) []Environment {
	if replacement.Verification.TreeID == "" {
		return nil
	}
	var superseded []Environment
	for _, other := range environments {
		if other.ID == replacement.ID || other.Verification.TreeID == "" {
			continue
		}
		if other.Verification.TreeID == replacement.Verification.TreeID {
			continue
		}
		if other.Target.Endpoint != replacement.Target.Endpoint {
			continue
		}
		other.Verification.State = VerificationFailed
		other.Verification.LastAttemptAt = s.now()
		other.Verification.Error = "the persistent tree at this endpoint changed; this environment no longer matches the target; register a new environment id"
		other.Revision++
		other.UpdatedAt = s.now()
		superseded = append(superseded, other)
	}
	return superseded
}

// List answers the registrations the grant reaches.
func (s *Service) List(ctx context.Context, filter Filter, grant Grant) ([]EnvironmentView, error) {
	var views []EnvironmentView
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		environments, err := tx.ListEnvironments(ctx)
		if err != nil {
			return err
		}
		views = make([]EnvironmentView, 0, len(environments))
		for _, env := range environments {
			if !grant.AllowsEnvironment(env) {
				continue
			}
			if filter.TrustDomain != "" && env.Target.TrustDomain != filter.TrustDomain {
				continue
			}
			if filter.Source != "" && !env.allowsSource(filter.Source) {
				continue
			}
			views = append(views, s.view(ctx, tx, env))
		}
		sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
		return nil
	})
	if err != nil {
		return nil, err
	}
	return views, nil
}

// Get answers one registration.
func (s *Service) Get(ctx context.Context, id ID, grant Grant) (EnvironmentView, error) {
	var view EnvironmentView
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok || !grant.AllowsEnvironment(env) {
			return ErrNotFound
		}
		view = s.view(ctx, tx, env)
		return nil
	})
	if err != nil {
		return EnvironmentView{}, err
	}
	return view, nil
}

// Update edits one target's configuration. An identity-affecting edit marks
// the verification stale; a policy-only edit leaves it valid.
func (s *Service) Update(ctx context.Context, id ID, patch Patch, actor string) (EnvironmentView, error) {
	var view EnvironmentView
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}

		identityChanged := false
		policyChanged := false
		if patch.Endpoint != nil {
			value := strings.TrimSpace(*patch.Endpoint)
			if err := validateEndpoint(value); err != nil {
				return err
			}
			if value != env.Target.Endpoint {
				env.Target.Endpoint = value
				identityChanged = true
			}
		}
		if patch.TLSRef != nil {
			value := strings.TrimSpace(*patch.TLSRef)
			if !tlsRefPattern.MatchString(value) {
				return invalid("TLS reference must be a ref/<name> token")
			}
			if value != env.Target.TLSRef {
				env.Target.TLSRef = value
				identityChanged = true
			}
		}
		if patch.StateRoot != nil {
			value := strings.TrimSpace(*patch.StateRoot)
			if err := validateStateRoot(value); err != nil {
				return err
			}
			if value != env.Target.StateRoot {
				return fmt.Errorf("%w: the state root identifies the physical tree", ErrTreeChanged)
			}
		}
		if patch.TrustDomain != nil {
			value := strings.TrimSpace(*patch.TrustDomain)
			if !domainPattern.MatchString(value) {
				return invalid("trust domain must be 2-32 lowercase letters, digits or hyphens and start with a letter")
			}
			if value != env.Target.TrustDomain {
				if !s.trustDomainAllowed(value) {
					return invalid("trust domain %q is not an allowed data-residency domain", value)
				}
				env.Target.TrustDomain = value
				policyChanged = true
			}
		}
		if patch.AllowedSources != nil {
			values, err := normalizeSources(*patch.AllowedSources)
			if err != nil {
				return err
			}
			if !equalStrings(values, env.Target.AllowedSources) {
				env.Target.AllowedSources = values
				policyChanged = true
			}
		}
		if patch.Capabilities != nil {
			values, err := normalizeCapabilities(*patch.Capabilities)
			if err != nil {
				return err
			}
			if !equalStrings(values, env.Target.Capabilities) {
				env.Target.Capabilities = values
				identityChanged = true
			}
		}
		if patch.Concurrency != nil {
			if *patch.Concurrency < 1 || *patch.Concurrency > MaxConcurrency {
				return invalid("concurrency must be between 1 and %d", MaxConcurrency)
			}
			if *patch.Concurrency != env.Target.Concurrency {
				env.Target.Concurrency = *patch.Concurrency
				// Lowering the limit, or raising it inside the capacity the last
				// verification observed, cannot change which target is behind the
				// registration. A limit above the observed capacity needs a new
				// verification.
				if env.Verification.State != VerificationOK || *patch.Concurrency > env.Verification.Capacity {
					identityChanged = true
				} else {
					policyChanged = true
				}
			}
		}
		if patch.ExpectedIdentity != nil {
			value := strings.TrimSpace(*patch.ExpectedIdentity)
			if err := validateOptionalText("expected identity", value, 256); err != nil {
				return err
			}
			if value != env.ExpectedIdentity {
				env.ExpectedIdentity = value
				identityChanged = true
			}
		}
		if !identityChanged && !policyChanged {
			view = s.view(ctx, tx, env)
			return nil
		}
		if identityChanged && env.usedSlots() > 0 {
			relocation := patch.Endpoint != nil || patch.TLSRef != nil || patch.StateRoot != nil
			if relocation {
				return fmt.Errorf("%w: cannot change the endpoint, TLS reference or state root while slots are live or isolated", ErrSlotsPresent)
			}
		}
		if identityChanged {
			env.Verification.State = VerificationStale
			env.Verification.Error = "configuration changed; verify the target again"
			env.Verification.VerifiedAt = time.Time{}
		}
		env.Revision++
		env.UpdatedAt = s.now()
		if err := tx.PutEnvironment(ctx, env); err != nil {
			return err
		}
		s.appendAudit(ctx, tx, AuditUpdate, id, actor, AuditOK, fmt.Sprintf("revision %d", env.Revision))
		view = s.view(ctx, tx, env)
		return nil
	})
	if err != nil {
		return EnvironmentView{}, err
	}
	return view, nil
}

// Verify runs one verification attempt against the target. A failed attempt is
// a completed answer, not a transport error: the view carries the failure.
func (s *Service) Verify(ctx context.Context, id ID, actor string) (EnvironmentView, error) {
	for attempt := 0; ; attempt++ {
		// The target, its revision and the ledger epoch must come from one
		// committed snapshot: reading them separately could probe one target
		// while validating against a carrier that already points at another.
		var env Environment
		var ok bool
		var epoch ledgerEpoch
		err := s.transact(ctx, func(ctx context.Context, tx Store) error {
			var err error
			env, ok, err = tx.GetEnvironment(ctx, id)
			if err != nil || !ok {
				return err
			}
			epoch, err = ledgerEpochOf(ctx, tx)
			return err
		})
		if err != nil {
			return EnvironmentView{}, err
		}
		if !ok {
			return EnvironmentView{}, ErrNotFound
		}
		if !s.trustDomainAllowed(env.Target.TrustDomain) {
			return EnvironmentView{}, fmt.Errorf("%w: %s", ErrTrustDomainDenied, env.Target.TrustDomain)
		}
		revision := env.Revision
		result, probeErr := s.probe(ctx, env.Target)
		now := s.now()

		var view EnvironmentView
		err = s.transact(ctx, func(ctx context.Context, tx Store) error {
			current, ok, err := tx.GetEnvironment(ctx, id)
			if err != nil {
				return err
			}
			if !ok {
				return ErrNotFound
			}
			if current.Revision != revision {
				return errProbeStale
			}
			environments, err := tx.ListEnvironments(ctx)
			if err != nil {
				return err
			}
			committed, err := ledgerEpochOf(ctx, tx)
			if err != nil {
				return err
			}
			if committed != epoch {
				return errProbeStale
			}

			verificationErr := probeErr
			if verificationErr == nil {
				if current.Verification.TreeID != "" && strings.TrimSpace(result.TreeID) != current.Verification.TreeID {
					verificationErr = ErrTreeChanged
				}
			}
			if verificationErr == nil {
				for _, other := range environments {
					if other.ID != current.ID && other.Verification.TreeID != "" && other.Verification.TreeID == strings.TrimSpace(result.TreeID) {
						verificationErr = fmt.Errorf("%w (%s)", ErrTreeExists, other.ID)
						break
					}
				}
			}
			if verificationErr == nil {
				verification, err := verificationFromProbe(current.Target, current.ExpectedIdentity, result)
				if err != nil {
					verificationErr = err
				} else {
					verification.VerifiedAt = now
					verification.LastAttemptAt = now
					verification.ObservedTreeID = verification.TreeID
					current.Verification = verification
					if current.Phase == PhasePending {
						current.Phase = PhaseActive
					}
					current.Registered = true
				}
			}
			if verificationErr != nil {
				current.Verification = failedVerification(current.Verification, verificationErr, now)
				if observed := strings.TrimSpace(result.TreeID); observed != "" {
					current.Verification.ObservedTreeID = observed
				}
			}
			current.Revision++
			current.UpdatedAt = now
			var superseded []Environment
			if verificationErr == nil {
				superseded = s.supersededRegistrations(environments, current)
			}
			batch := append([]Environment{current}, superseded...)
			if err := tx.PutEnvironments(ctx, batch); err != nil {
				return err
			}
			for _, other := range superseded {
				s.appendAudit(ctx, tx, AuditSupersede, other.ID, actor, AuditFailed, "superseded by "+string(current.ID))
			}
			if verificationErr != nil {
				s.appendAudit(ctx, tx, AuditVerify, id, actor, AuditFailed, verificationErr.Error())
			} else {
				s.appendAudit(ctx, tx, AuditVerify, id, actor, AuditOK, "verified")
			}
			view = s.view(ctx, tx, current)
			return nil
		})
		if errors.Is(err, errProbeStale) && attempt < storeConflictRetries {
			continue
		}
		if errors.Is(err, errProbeStale) {
			return EnvironmentView{}, ErrConflict
		}
		if err != nil {
			return EnvironmentView{}, err
		}
		return view, nil
	}
}

// Drain atomically refuses new slots without evicting existing sessions.
func (s *Service) Drain(ctx context.Context, id ID, actor string) (EnvironmentView, error) {
	var view EnvironmentView
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}
		if !env.Registered {
			return fmt.Errorf("%w: only a registered environment can be drained", ErrNotVerified)
		}
		if env.Phase == PhaseDraining {
			view = s.view(ctx, tx, env)
			return nil
		}
		env.Phase = PhaseDraining
		env.Revision++
		env.UpdatedAt = s.now()
		if err := tx.PutEnvironment(ctx, env); err != nil {
			return err
		}
		s.appendAudit(ctx, tx, AuditDrain, id, actor, AuditOK, fmt.Sprintf("%d live slot(s)", env.activeSlots()))
		view = s.view(ctx, tx, env)
		return nil
	})
	if err != nil {
		return EnvironmentView{}, err
	}
	return view, nil
}

// Resume returns a verified environment to the active pool.
func (s *Service) Resume(ctx context.Context, id ID, actor string) (EnvironmentView, error) {
	var view EnvironmentView
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}
		if env.Verification.State != VerificationOK {
			return fmt.Errorf("%w: verify the target again before resuming", ErrNotVerified)
		}
		if !s.trustDomainAllowed(env.Target.TrustDomain) {
			return fmt.Errorf("%w: %s", ErrTrustDomainDenied, env.Target.TrustDomain)
		}
		if env.Phase == PhaseActive {
			view = s.view(ctx, tx, env)
			return nil
		}
		env.Phase = PhaseActive
		env.Revision++
		env.UpdatedAt = s.now()
		if err := tx.PutEnvironment(ctx, env); err != nil {
			return err
		}
		s.appendAudit(ctx, tx, AuditResume, id, actor, AuditOK, "active")
		view = s.view(ctx, tx, env)
		return nil
	})
	if err != nil {
		return EnvironmentView{}, err
	}
	return view, nil
}

// Retire removes one registration and leaves a tombstone. It never touches
// remote data: the service has no remote delete operation and the probe is
// read-only.
func (s *Service) Retire(ctx context.Context, id ID, actor string) (Retirement, error) {
	var retirement Retirement
	var blocked *BlockedError
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}
		blockers := s.retirementBlockers(ctx, env)
		if len(blockers) > 0 {
			s.appendAudit(ctx, tx, AuditRetire, id, actor, AuditFailed, strings.Join(blockers, "; "))
			blocked = &BlockedError{Reasons: blockers, Err: ErrRetireBlocked}
			return nil
		}
		now := s.now()
		tomb := Tombstone{
			ID:             env.ID,
			Endpoint:       env.Target.Endpoint,
			StateRoot:      env.Target.StateRoot,
			TreeID:         env.Verification.TreeID,
			TrustDomain:    env.Target.TrustDomain,
			AllowedSources: append([]string{}, env.Target.AllowedSources...),
			RetiredAt:      now,
			Actor:          actor,
		}
		if err := tx.RetireEnvironment(ctx, id, tomb); err != nil {
			return err
		}
		s.appendAudit(ctx, tx, AuditRetire, id, actor, AuditOK, "registration removed; remote data untouched")
		retirement = Retirement{ID: id, RetiredAt: now, Tombstone: tomb}
		return nil
	})
	if err != nil {
		return Retirement{}, err
	}
	if blocked != nil {
		return Retirement{}, blocked
	}
	return retirement, nil
}

// Retired answers the tombstones the grant reaches.
func (s *Service) Retired(ctx context.Context, grant Grant) ([]TombstoneView, error) {
	var views []TombstoneView
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		tombstones, err := tx.ListTombstones(ctx)
		if err != nil {
			return err
		}
		views = make([]TombstoneView, 0, len(tombstones))
		for _, tomb := range tombstones {
			if !grant.AllowsTombstone(tomb) {
				continue
			}
			views = append(views, tombstoneView(tomb))
		}
		sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
		return nil
	})
	if err != nil {
		return nil, err
	}
	return views, nil
}

// Slots answers one environment's slot ledger.
func (s *Service) Slots(ctx context.Context, id ID, grant Grant) ([]SlotView, error) {
	var views []SlotView
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok || !grant.AllowsEnvironment(env) {
			return ErrNotFound
		}
		views = make([]SlotView, 0, len(env.Slots))
		for _, slot := range env.Slots {
			views = append(views, slotView(slot))
		}
		sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
		return nil
	})
	if err != nil {
		return nil, err
	}
	return views, nil
}

// Cleanups answers one environment's pending non-slot cleanups.
func (s *Service) Cleanups(ctx context.Context, id ID, grant Grant) ([]CleanupView, error) {
	var views []CleanupView
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok || !grant.AllowsEnvironment(env) {
			return ErrNotFound
		}
		views = make([]CleanupView, 0, len(env.Cleanups))
		for _, cleanup := range env.Cleanups {
			views = append(views, cleanupView(cleanup))
		}
		sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
		return nil
	})
	if err != nil {
		return nil, err
	}
	return views, nil
}

// AcquireSlot reserves one idle slot atomically. A pending, unverified,
// draining or full environment refuses the reservation. The new reservation
// never carries a remote session: a Multica resume path is an affinity key,
// and only a session the Job actually opened may be recorded by BindSlot.
func (s *Service) AcquireSlot(ctx context.Context, id ID, slotID string) (Slot, error) {
	if err := validateSlotID(slotID); err != nil {
		return Slot{}, err
	}
	var slot Slot
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}
		if !s.trustDomainAllowed(env.Target.TrustDomain) {
			return fmt.Errorf("%w: %s", ErrTrustDomainDenied, env.Target.TrustDomain)
		}
		switch {
		case env.Phase == PhaseDraining:
			return ErrDraining
		case env.Phase != PhaseActive || env.Verification.State != VerificationOK:
			return ErrNotVerified
		case env.usedSlots() >= env.Target.Concurrency:
			return ErrFull
		}
		if _, exists := env.Slots[slotID]; exists {
			return ErrSlotExists
		}
		slot = Slot{ID: slotID, State: SlotActive, AcquiredAt: s.now()}
		env.Slots[slotID] = slot
		env.Revision++
		env.UpdatedAt = s.now()
		if err := tx.PutEnvironment(ctx, env); err != nil {
			return err
		}
		s.appendAudit(ctx, tx, AuditSlotAcquire, id, "scheduler", AuditOK, slotID)
		return nil
	})
	if err != nil {
		return Slot{}, err
	}
	return slot, nil
}

// SlotState answers one reservation's committed facts. A missing slot is
// found=false, not an unreadable ledger.
func (s *Service) SlotState(ctx context.Context, id ID, slotID string) (Slot, bool, error) {
	var slot Slot
	var found bool
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}
		slot, found = env.Slots[slotID]
		return nil
	})
	if err != nil {
		return Slot{}, false, err
	}
	return slot, found, nil
}

// BeginSlot atomically claims the execution right for one idle reservation and
// opens the unknown ready window. A second execution of the same attempt gets
// ErrSlotClaimed; a retry of the same claim is idempotent. A legacy opening
// record without a claim is fenced by the first caller.
func (s *Service) BeginSlot(ctx context.Context, id ID, slotID, claimID string) (Slot, error) {
	if err := validateSlotID(slotID); err != nil {
		return Slot{}, err
	}
	if err := validateClaimID(claimID); err != nil {
		return Slot{}, err
	}
	var result Slot
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, slot, err := s.slotForTransition(ctx, tx, id, slotID)
		if err != nil {
			return err
		}
		if slot.State == SlotIsolated {
			return fmt.Errorf("%w: %s", ErrSlotIsolated, slotID)
		}
		switch {
		case slot.Phase == SlotPhaseIdle && slot.ClaimID == "":
			slot.ClaimID = claimID
			slot.Phase = SlotPhaseOpening
			slot.Started = false
		case slot.Phase == SlotPhaseOpening && slot.ClaimID == claimID:
			// A lost begin answer is safe to retry for the same execution.
		case slot.Phase == SlotPhaseOpening && slot.ClaimID == "":
			// A legacy unknown window had no owner; the first execution fences it.
			slot.ClaimID = claimID
		default:
			return fmt.Errorf("%w: %s", ErrSlotClaimed, slotID)
		}
		slot.LastError = ""
		result, err = s.putSlotTransition(ctx, tx, id, env, slot, "begin")
		return err
	})
	if err != nil {
		return Slot{}, err
	}
	return result, nil
}

// TakeOverSlot fences a reservation that may already own a remote session.
// Recovery is the only caller. The observed claim is a compare-and-swap token:
// two rebuilt Jobs naming the same prior claim race for one winner, and the
// loser cannot bind, commit or release the slot.
func (s *Service) TakeOverSlot(ctx context.Context, id ID, slotID, claimID, expectedClaimID string) (Slot, error) {
	if err := validateSlotID(slotID); err != nil {
		return Slot{}, err
	}
	if err := validateClaimID(claimID); err != nil {
		return Slot{}, err
	}
	if err := validateOptionalText("expected claim id", expectedClaimID, 128); err != nil {
		return Slot{}, err
	}
	var result Slot
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, slot, err := s.slotForTransition(ctx, tx, id, slotID)
		if err != nil {
			return err
		}
		if slot.ClaimID != expectedClaimID {
			return fmt.Errorf("%w: %s", ErrSlotClaimed, slotID)
		}
		if slot.Phase == SlotPhaseIdle && slot.SessionID == "" && slot.State != SlotIsolated {
			return fmt.Errorf("%w: %s has nothing to recover", ErrSlotPhase, slotID)
		}
		slot.ClaimID = claimID
		slot.LastError = ""
		result, err = s.putSlotTransition(ctx, tx, id, env, slot, "takeover")
		return err
	})
	if err != nil {
		return Slot{}, err
	}
	return result, nil
}

// BindSlot pins the exact remote session this claim opened. Another execution
// cannot replace the session, and the same claim cannot swap it either.
func (s *Service) BindSlot(ctx context.Context, id ID, slotID, claimID, sessionID string) (Slot, error) {
	if err := validateSlotID(slotID); err != nil {
		return Slot{}, err
	}
	if err := validateClaimID(claimID); err != nil {
		return Slot{}, err
	}
	if strings.TrimSpace(sessionID) == "" {
		return Slot{}, errors.New("a connected slot needs a session id")
	}
	if err := validateOptionalText("session id", sessionID, 256); err != nil {
		return Slot{}, err
	}
	var result Slot
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, slot, err := s.slotForTransition(ctx, tx, id, slotID)
		if err != nil {
			return err
		}
		if slot.ClaimID != claimID {
			return fmt.Errorf("%w: %s", ErrSlotClaimed, slotID)
		}
		if slot.State == SlotIsolated {
			return fmt.Errorf("%w: %s", ErrSlotIsolated, slotID)
		}
		switch slot.Phase {
		case SlotPhaseConnected:
			if slot.SessionID == sessionID {
				result = slot
				return nil
			}
			return fmt.Errorf("%w: %s already names %q", ErrSlotSessionBound, slotID, slot.SessionID)
		case SlotPhaseOpening:
			slot.Phase = SlotPhaseConnected
			slot.SessionID = sessionID
			slot.Started = false
		default:
			return fmt.Errorf("%w: cannot bind from %q", ErrSlotPhase, slot.Phase)
		}
		slot.LastError = ""
		result, err = s.putSlotTransition(ctx, tx, id, env, slot, "connect")
		return err
	})
	if err != nil {
		return Slot{}, err
	}
	return result, nil
}

// MarkSlotStarted commits dispatch for the claim that bound the session.
// Replaying the same claim is idempotent; any other execution is refused.
func (s *Service) MarkSlotStarted(ctx context.Context, id ID, slotID, claimID string) (Slot, error) {
	if err := validateSlotID(slotID); err != nil {
		return Slot{}, err
	}
	if err := validateClaimID(claimID); err != nil {
		return Slot{}, err
	}
	var result Slot
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, slot, err := s.slotForTransition(ctx, tx, id, slotID)
		if err != nil {
			return err
		}
		if slot.ClaimID != claimID {
			return fmt.Errorf("%w: %s", ErrSlotClaimed, slotID)
		}
		if slot.State == SlotIsolated {
			return fmt.Errorf("%w: %s", ErrSlotIsolated, slotID)
		}
		switch slot.Phase {
		case SlotPhaseConnected:
			slot.Phase = SlotPhaseStarted
			slot.Started = true
		case SlotPhaseStarted:
			result = slot
			return nil
		default:
			return fmt.Errorf("%w: cannot start from %q", ErrSlotPhase, slot.Phase)
		}
		slot.LastError = ""
		result, err = s.putSlotTransition(ctx, tx, id, env, slot, "started")
		return err
	})
	if err != nil {
		return Slot{}, err
	}
	return result, nil
}

// slotForTransition reads the environment and one reservation for a Job-side
// mutation.
func (s *Service) slotForTransition(ctx context.Context, tx Store, id ID, slotID string) (Environment, Slot, error) {
	env, ok, err := tx.GetEnvironment(ctx, id)
	if err != nil {
		return Environment{}, Slot{}, err
	}
	if !ok {
		return Environment{}, Slot{}, ErrNotFound
	}
	slot, ok := env.Slots[slotID]
	if !ok {
		return Environment{}, Slot{}, ErrSlotNotFound
	}
	return env, slot, nil
}

// putSlotTransition commits one already-decided Job-side slot change.
func (s *Service) putSlotTransition(ctx context.Context, tx Store, id ID, env Environment, slot Slot, detail string) (Slot, error) {
	env.Slots[slot.ID] = slot
	env.Revision++
	env.UpdatedAt = s.now()
	if err := tx.PutEnvironment(ctx, env); err != nil {
		return Slot{}, err
	}
	s.appendAudit(ctx, tx, AuditSlotAdvance, id, "job", AuditOK, detail)
	return slot, nil
}

// ReleaseSlot releases one slot. A release whose remote cleanup is not
// confirmed leaves the slot isolated instead of freeing capacity.
func (s *Service) ReleaseSlot(ctx context.Context, id ID, slotID string, cleanupConfirmed bool) (Slot, error) {
	return s.releaseSlot(ctx, id, slotID, "", cleanupConfirmed)
}

// ReleaseSlotByClaim releases a Job-side reservation only for the execution
// that owns it. A stale instance that lost the Begin/TakeOver race cannot free
// the winner's slot or mark it isolated.
func (s *Service) ReleaseSlotByClaim(ctx context.Context, id ID, slotID, claimID string, cleanupConfirmed bool) (Slot, error) {
	if err := validateClaimID(claimID); err != nil {
		return Slot{}, err
	}
	return s.releaseSlot(ctx, id, slotID, claimID, cleanupConfirmed)
}

func (s *Service) releaseSlot(ctx context.Context, id ID, slotID, claimID string, cleanupConfirmed bool) (Slot, error) {
	var result Slot
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}
		slot, ok := env.Slots[slotID]
		if !ok {
			return ErrSlotNotFound
		}
		if claimID != "" && slot.ClaimID != claimID {
			return fmt.Errorf("%w: %s", ErrSlotClaimed, slotID)
		}
		now := s.now()
		actor := "scheduler"
		if claimID != "" {
			actor = "job"
		}
		switch {
		case slot.State == SlotActive && cleanupConfirmed:
			slot.ReleasedAt = now
			delete(env.Slots, slotID)
			s.appendAudit(ctx, tx, AuditSlotRelease, id, actor, AuditOK, slotID)
		case slot.State == SlotActive:
			slot.State = SlotIsolated
			slot.ReleasedAt = now
			slot.LastError = "remote cleanup is not confirmed"
			env.Slots[slotID] = slot
			s.appendAudit(ctx, tx, AuditSlotIsolate, id, actor, AuditFailed, slotID)
		case slot.State == SlotIsolated && cleanupConfirmed:
			slot.ReleasedAt = now
			delete(env.Slots, slotID)
			s.appendAudit(ctx, tx, AuditCleanupConfirm, id, actor, AuditOK, slotID)
		default:
			result = slot
			return nil
		}
		env.Revision++
		env.UpdatedAt = now
		if err := tx.PutEnvironment(ctx, env); err != nil {
			return err
		}
		result = slot
		return nil
	})
	if err != nil {
		return Slot{}, err
	}
	return result, nil
}

// ConfirmCleanup clears one isolated slot after an operator verified the
// remote session is closed.
func (s *Service) ConfirmCleanup(ctx context.Context, id ID, slotID string, actor string) (EnvironmentView, error) {
	var view EnvironmentView
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}
		slot, ok := env.Slots[slotID]
		if !ok {
			return ErrSlotNotFound
		}
		if slot.State != SlotIsolated {
			return ErrSlotActive
		}
		delete(env.Slots, slotID)
		env.Revision++
		env.UpdatedAt = s.now()
		if err := tx.PutEnvironment(ctx, env); err != nil {
			return err
		}
		s.appendAudit(ctx, tx, AuditCleanupConfirm, id, actor, AuditOK, slotID)
		view = s.view(ctx, tx, env)
		return nil
	})
	if err != nil {
		return EnvironmentView{}, err
	}
	return view, nil
}

// AddPendingCleanup records one non-slot cleanup that is not confirmed yet.
func (s *Service) AddPendingCleanup(ctx context.Context, id ID, cleanupID, ref string) error {
	if err := validateSlotID(cleanupID); err != nil {
		return fmt.Errorf("cleanup id: %w", err)
	}
	if err := validateOptionalText("cleanup reference", ref, 256); err != nil {
		return err
	}
	return s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}
		if _, exists := env.Cleanups[cleanupID]; exists {
			return ErrCleanupExists
		}
		env.Cleanups[cleanupID] = Cleanup{ID: cleanupID, Ref: ref, CreatedAt: s.now()}
		env.Revision++
		env.UpdatedAt = s.now()
		if err := tx.PutEnvironment(ctx, env); err != nil {
			return err
		}
		s.appendAudit(ctx, tx, AuditCleanupPending, id, "maintenance", AuditFailed, cleanupID)
		return nil
	})
}

// ConfirmPendingCleanup clears one non-slot cleanup.
func (s *Service) ConfirmPendingCleanup(ctx context.Context, id ID, cleanupID string, actor string) error {
	return s.transact(ctx, func(ctx context.Context, tx Store) error {
		env, ok, err := tx.GetEnvironment(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}
		if _, exists := env.Cleanups[cleanupID]; !exists {
			return ErrCleanupNotFound
		}
		delete(env.Cleanups, cleanupID)
		env.Revision++
		env.UpdatedAt = s.now()
		if err := tx.PutEnvironment(ctx, env); err != nil {
			return err
		}
		s.appendAudit(ctx, tx, AuditCleanupResolved, id, actor, AuditOK, cleanupID)
		return nil
	})
}

// Candidates answers the environments the scheduler may select. Selection uses
// only registered environment facts: trust domain, source, capabilities and
// free capacity.
func (s *Service) Candidates(ctx context.Context, filter CandidateFilter) []Candidate {
	var candidates []Candidate
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		environments, err := tx.ListEnvironments(ctx)
		if err != nil {
			return err
		}
		candidates = make([]Candidate, 0, len(environments))
		for _, env := range environments {
			if !s.trustDomainAllowed(env.Target.TrustDomain) {
				continue
			}
			if !env.isSchedulable() {
				continue
			}
			if filter.TrustDomain != "" && env.Target.TrustDomain != filter.TrustDomain {
				continue
			}
			if filter.Source != "" && !env.allowsSource(filter.Source) {
				continue
			}
			if !hasAllCapabilities(env.Target.Capabilities, filter.Capabilities) {
				continue
			}
			free := env.Target.Concurrency - env.usedSlots()
			if free <= 0 {
				continue
			}
			candidates = append(candidates, Candidate{
				ID:           env.ID,
				TrustDomain:  env.Target.TrustDomain,
				Capabilities: append([]string{}, env.Target.Capabilities...),
				Concurrency:  env.Target.Concurrency,
				FreeSlots:    free,
			})
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].FreeSlots != candidates[j].FreeSlots {
				return candidates[i].FreeSlots > candidates[j].FreeSlots
			}
			return candidates[i].ID < candidates[j].ID
		})
		return nil
	})
	if err != nil {
		s.log.Warn("environment.candidates_failed", "err", err.Error())
		return nil
	}
	return candidates
}

// CountRegistered answers the deployment-wide environment count.
func (s *Service) CountRegistered(ctx context.Context) int {
	return s.CountRegisteredForGrant(ctx, Grant{DeploymentWide: true})
}

// CountRegisteredForGrant answers the overview's environment count over the
// environments the grant reaches. A never-verified target is not an
// environment; a registration whose trust domain the deployment no longer
// allows is not available. The stable id is the counting key, so overlapping
// scopes cannot double count.
func (s *Service) CountRegisteredForGrant(ctx context.Context, grant Grant) int {
	count := 0
	err := s.transact(ctx, func(ctx context.Context, tx Store) error {
		environments, err := tx.ListEnvironments(ctx)
		if err != nil {
			return err
		}
		seen := map[ID]bool{}
		for _, env := range environments {
			if seen[env.ID] || !env.Registered || !grant.AllowsEnvironment(env) {
				continue
			}
			if !s.trustDomainAllowed(env.Target.TrustDomain) {
				continue
			}
			seen[env.ID] = true
			count++
		}
		return nil
	})
	if err != nil {
		return 0
	}
	return count
}

// Audit answers the audit rows, newest first.
func (s *Service) Audit(ctx context.Context) ([]AuditEntryView, error) {
	entries, err := s.store.ListAudit(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].At.Equal(entries[j].At) {
			return entries[i].At.After(entries[j].At)
		}
		return entries[i].Action > entries[j].Action
	})
	views := make([]AuditEntryView, 0, len(entries))
	for _, entry := range entries {
		views = append(views, auditEntryView(entry))
	}
	return views, nil
}

// probe runs one verification attempt under the configured timeout.
func (s *Service) probe(ctx context.Context, target Target) (ProbeResult, error) {
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.ProbeTimeout)
	defer cancel()
	return s.cfg.Probe.Inspect(callCtx, target)
}

func (s *Service) trustDomainAllowed(domain string) bool {
	if len(s.cfg.AllowedTrustDomains) == 0 {
		return false
	}
	for _, allowed := range s.cfg.AllowedTrustDomains {
		if allowed == domain {
			return true
		}
	}
	return false
}

// verificationFromProbe validates the observed facts and returns the stored
// verification. It never mutates the environment.
func verificationFromProbe(target Target, expectedIdentity string, result ProbeResult) (Verification, error) {
	treeID := strings.TrimSpace(result.TreeID)
	if treeID == "" {
		return Verification{}, errors.New("probe did not report a persistent tree identity")
	}
	if err := validateOptionalText("tree identity", treeID, 256); err != nil {
		return Verification{}, err
	}
	identity := strings.TrimSpace(result.Identity)
	if identity == "" {
		return Verification{}, errors.New("probe did not report an identity")
	}
	if err := validateOptionalText("identity", identity, 256); err != nil {
		return Verification{}, err
	}
	if expectedIdentity != "" && identity != expectedIdentity {
		return Verification{}, errors.New("the target identity does not match the expected identity")
	}
	observed, err := normalizeCapabilities(result.Capabilities)
	if err != nil {
		return Verification{}, fmt.Errorf("probe capabilities: %w", err)
	}
	if result.Capacity <= 0 {
		return Verification{}, errors.New("probe did not report a positive capacity")
	}
	if target.Concurrency > result.Capacity {
		return Verification{}, fmt.Errorf("configured concurrency %d exceeds the target capacity %d", target.Concurrency, result.Capacity)
	}
	if missing := missingCapabilities(target.Capabilities, observed); len(missing) > 0 {
		return Verification{}, fmt.Errorf("target does not report required capabilities: %s", strings.Join(missing, ", "))
	}
	version := strings.TrimSpace(result.Version)
	if err := validateOptionalText("version", version, 128); err != nil {
		return Verification{}, err
	}
	return Verification{
		State:        VerificationOK,
		TreeID:       treeID,
		Identity:     identity,
		Capabilities: observed,
		Capacity:     result.Capacity,
		Version:      version,
	}, nil
}

// failedVerification records one failure without discarding the last known
// target facts.
func failedVerification(previous Verification, err error, now time.Time) Verification {
	previous.State = VerificationFailed
	previous.LastAttemptAt = now
	previous.Error = err.Error()
	return previous
}

// retirementBlockers answers every reason a retirement would be refused.
func (s *Service) retirementBlockers(ctx context.Context, env Environment) []string {
	var blockers []string
	if active := env.activeSlots(); active > 0 {
		blockers = append(blockers, fmt.Sprintf("%d active slot(s)", active))
	}
	if isolated := env.isolatedSlots(); isolated > 0 {
		blockers = append(blockers, fmt.Sprintf("%d isolated slot(s) with unconfirmed cleanup", isolated))
	}
	if cleanups := len(env.Cleanups); cleanups > 0 {
		blockers = append(blockers, fmt.Sprintf("%d unconfirmed cleanup(s)", cleanups))
	}
	if s.cfg.RuleReferences != nil {
		refs, err := s.cfg.RuleReferences.ReferencingRules(ctx, env.ID)
		switch {
		case err != nil:
			blockers = append(blockers, "rule references cannot be read")
		case len(refs) > 0:
			blockers = append(blockers, fmt.Sprintf("%d rule(s) still reference this environment: %s", len(refs), strings.Join(refs, ", ")))
		}
	}
	return blockers
}

func (s *Service) view(ctx context.Context, _ Store, env Environment) EnvironmentView {
	return environmentView(env, s.retirementBlockers(ctx, env), s.trustDomainAllowed(env.Target.TrustDomain))
}

func (s *Service) appendAudit(ctx context.Context, tx Store, action string, id ID, actor string, result AuditResult, detail string) {
	if err := tx.AppendAudit(ctx, AuditEntry{
		At:            s.now(),
		Actor:         actor,
		Action:        action,
		EnvironmentID: string(id),
		Result:        result,
		Detail:        detail,
	}); err != nil {
		s.log.Warn("environment.audit_failed", "action", action, "environment_id", id, "err", err.Error())
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
