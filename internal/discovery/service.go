package discovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// AdapterRegistration wires one adapter. Trust is never granted here: only a
// successful probe does that, so an unverified backend answers unsupported.
type AdapterRegistration struct {
	Adapter Adapter
	Reason  string
}

// BoundAdapter needs the request's verified CLI bytes and read-only
// configuration. A trusted command surface alone is not enough to run it.
type BoundAdapter interface {
	RequiresBoundInputs() bool
}

// ArtifactReader returns verified CLI bytes for one release domain and digest.
type ArtifactReader interface {
	VerifiedArtifactByDomain(ctx context.Context, serverID, digest string) ([]byte, error)
}

// ConfigProjector materializes the exact revision for one attempt into dir.
// ok=false means the prototype has no readable content, which is
// unsupported/needs configuration, never a reason to run a global binary.
type ConfigProjector interface {
	ProjectFile(ctx context.Context, serverID, workspaceID, backend, profile, attemptID, dir string) (path string, revision uint64, ok bool, err error)
}

// Config is the discovery service's setup.
type Config struct {
	Store Store
	// Adapters are the backend commands this deployment knows.
	Adapters []AdapterRegistration
	// Timeout bounds one adapter execution.
	Timeout time.Duration
	// CacheTTL bounds reuse of one environment generation's successful result.
	CacheTTL time.Duration
	Clock    func() time.Time
	Logger   *slog.Logger
	// Artifacts and Configs are required by a BoundAdapter.
	Artifacts ArtifactReader
	Configs   ConfigProjector
}

// Defaults.
const (
	DefaultTimeout  = 20 * time.Second
	DefaultCacheTTL = 5 * time.Minute
)

type adapterState struct {
	adapter    Adapter
	trusted    bool
	reason     string
	verifiedAt time.Time
}

type adapterSnapshot struct {
	adapter    Adapter
	trusted    bool
	reason     string
	verifiedAt time.Time
	kinds      []Kind
}

// Service owns the request queue, the environment generations and the adapter
// trust table.
type Service struct {
	store     Store
	now       func() time.Time
	log       *slog.Logger
	timeout   time.Duration
	cacheTTL  time.Duration
	seq       atomic.Uint64
	artifacts ArtifactReader
	configs   ConfigProjector

	mu       sync.Mutex
	adapters map[string]*adapterState
	inflight map[string]chan struct{}
	envGen   map[string]uint64
	pending  map[string]pendingCompletion
}

// pendingCompletion is a completed result whose durable write failed. It is
// kept in memory so GET or a replay can converge once the store recovers,
// without executing the adapter again.
type pendingCompletion struct {
	request Request
	envKey  string
	latest  Latest
	advance bool
}

// New builds the service and fails any request a previous process left
// running: a restart cannot prove the command did not have a side effect, so
// the request is not blindly re-executed.
func New(cfg Config) (*Service, error) {
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore()
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = DefaultCacheTTL
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	service := &Service{
		store: cfg.Store, now: cfg.Clock,
		log:     cfg.Logger.With("component", "discovery"),
		timeout: cfg.Timeout, cacheTTL: cfg.CacheTTL,
		artifacts: cfg.Artifacts, configs: cfg.Configs,
		adapters: map[string]*adapterState{}, inflight: map[string]chan struct{}{},
		envGen: map[string]uint64{}, pending: map[string]pendingCompletion{},
	}
	for _, registration := range cfg.Adapters {
		if registration.Adapter == nil {
			continue
		}
		service.adapters[registration.Adapter.Name()] = &adapterState{
			adapter: registration.Adapter, reason: registration.Reason,
		}
	}
	service.recover(context.Background())
	return service, nil
}

func (s *Service) recover(ctx context.Context) {
	requests, err := s.store.ListRequests(ctx)
	if err != nil {
		s.log.Warn("discovery.recovery_failed", "err", err.Error())
		return
	}
	for _, request := range requests {
		if request.State != "queued" && request.State != "running" {
			continue
		}
		now := s.now()
		request.State = "completed"
		request.CompletedAt = &now
		request.Result = &Result{
			Class: ClassFailed, Models: []Model{}, Capabilities: []Capability{},
			Detail: "process restarted before the request completed", CompletedAt: now,
		}
		if err := s.store.PutRequest(ctx, request); err != nil {
			s.log.Warn("discovery.recovery_write_failed", "request_id", request.ID, "err", err.Error())
		}
	}
}

// VerifyAdapter runs one adapter's probe and records the trust verdict.
func (s *Service) VerifyAdapter(ctx context.Context, name string) (AdapterStatus, error) {
	s.mu.Lock()
	state, ok := s.adapters[name]
	if !ok {
		s.mu.Unlock()
		return AdapterStatus{}, ErrAdapterNotFound
	}
	snapshot := snapshotOf(state)
	s.mu.Unlock()
	probe, canProbe := snapshot.adapter.(ProbingAdapter)
	verifiedAt := s.now()
	trusted := false
	reason := snapshot.reason
	if !canProbe {
		if reason == "" {
			reason = "adapter has no verification probe; declared unsupported"
		}
	} else {
		probeCtx, cancel := context.WithTimeout(ctx, s.timeout)
		err := probe.Probe(probeCtx)
		cancel()
		if err != nil {
			reason = err.Error()
		} else {
			trusted, reason = true, ""
		}
	}
	s.mu.Lock()
	state.trusted = trusted
	state.reason = reason
	state.verifiedAt = verifiedAt
	status := statusOf(state)
	s.mu.Unlock()
	return status, nil
}

// VerifyAll probes every registered adapter.
func (s *Service) VerifyAll(ctx context.Context) []AdapterStatus {
	s.mu.Lock()
	names := make([]string, 0, len(s.adapters))
	for name := range s.adapters {
		names = append(names, name)
	}
	s.mu.Unlock()
	slices.Sort(names)
	out := make([]AdapterStatus, 0, len(names))
	for _, name := range names {
		status, err := s.VerifyAdapter(ctx, name)
		if err != nil {
			continue
		}
		out = append(out, status)
	}
	return out
}

// VerifyBoundAdapter probes one exact verified CLI payload and requires the
// payload's reported version to equal the candidate's exact version. It does
// not change the global trust table: it is the registration check that a
// global binary probe cannot provide for a different published digest.
func (s *Service) VerifyBoundAdapter(ctx context.Context, name, binaryPath, expectedVersion string) error {
	s.mu.Lock()
	state, ok := s.adapters[name]
	if !ok {
		s.mu.Unlock()
		return ErrAdapterNotFound
	}
	adapter := state.adapter
	s.mu.Unlock()
	bound, ok := adapter.(BoundProbingAdapter)
	if !ok {
		return errors.New("adapter does not support a bound probe")
	}
	probeCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	actual, err := bound.ProbeBound(probeCtx, binaryPath)
	if err != nil {
		return err
	}
	actual = strings.TrimSpace(actual)
	if actual == "" {
		return errors.New("bound CLI did not report a version")
	}
	if actual != strings.TrimSpace(expectedVersion) {
		return fmt.Errorf("bound CLI reports version %q, candidate declares %q", actual, expectedVersion)
	}
	return nil
}

// Adapters returns the trust table.
func (s *Service) Adapters() []AdapterStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AdapterStatus, 0, len(s.adapters))
	for _, state := range s.adapters {
		out = append(out, statusOf(state))
	}
	slices.SortFunc(out, func(a, b AdapterStatus) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// Submit records and executes one discovery request. A repeated upstream
// request returns the first request's result; a concurrent duplicate waits for
// that execution instead of starting a second one.
func (s *Service) Submit(ctx context.Context, input SubmitInput) (Request, error) {
	validated, err := ValidateInput(input)
	if err != nil {
		return Request{}, err
	}
	now := s.now()
	envKey := envKeyOf(validated)
	fingerprint := fingerprintOf(validated)

	s.mu.Lock()
	if validated.UpstreamRequestID != "" {
		if existing, found := s.findUpstreamLocked(ctx, validated.ServerID, validated.UpstreamRequestID); found {
			existing, err = s.flushPendingLocked(ctx, existing)
			if err != nil {
				s.mu.Unlock()
				return Request{}, err
			}
			request, err := s.waitLocked(ctx, existing)
			if err != nil {
				return Request{}, err
			}
			return s.authorizedReplay(request), nil
		}
	}
	latest, hasLatest, err := s.store.GetLatest(ctx, envKey)
	if err != nil {
		s.mu.Unlock()
		return Request{}, err
	}
	state := s.adapters[validated.Adapter]
	trusted := state != nil && state.trusted
	if hasLatest && trusted && latest.Fingerprint == fingerprint && cacheable(latest.Result.Class) &&
		now.Sub(latest.CompletedAt) < s.cacheTTL {
		completed := now
		request := Request{
			ID:                fmt.Sprintf("dsc-%d-%d", now.UnixNano(), s.seq.Add(1)),
			UpstreamRequestID: validated.UpstreamRequestID,
			ServerID:          validated.ServerID, WorkspaceID: validated.WorkspaceID,
			RuntimeID: validated.RuntimeID, Profile: validated.Profile, Kind: validated.Kind,
			Adapter: validated.Adapter, CLIDigest: validated.CLIDigest,
			ConfigRevision: validated.ConfigRevision, Platform: validated.Platform,
			Generation: latest.Generation, State: "completed",
			CachedFrom: latest.RequestID, CreatedAt: now, CompletedAt: &completed,
			Result: resultCopy(latest.Result),
		}
		if err := s.store.PutRequest(ctx, request); err != nil {
			s.mu.Unlock()
			return Request{}, err
		}
		s.mu.Unlock()
		return request, nil
	}
	generation := s.envGen[envKey]
	if generation == 0 && hasLatest {
		generation = latest.Generation
	}
	// Every new execution gets its own generation. A cache hit returned above
	// without executing; a refresh after the TTL executes again and must not
	// let a slower older attempt overwrite its result.
	generation++
	s.envGen[envKey] = generation
	request := Request{
		ID:                fmt.Sprintf("dsc-%d-%d", now.UnixNano(), s.seq.Add(1)),
		UpstreamRequestID: validated.UpstreamRequestID,
		ServerID:          validated.ServerID, WorkspaceID: validated.WorkspaceID,
		RuntimeID: validated.RuntimeID, Profile: validated.Profile, Kind: validated.Kind,
		Adapter: validated.Adapter, CLIDigest: validated.CLIDigest,
		ConfigRevision: validated.ConfigRevision, Platform: validated.Platform,
		Generation: generation, State: "queued", CreatedAt: now,
	}
	if !validated.Deadline.IsZero() {
		deadline := validated.Deadline
		request.Deadline = &deadline
	}
	if err := s.store.PutRequest(ctx, request); err != nil {
		s.mu.Unlock()
		return Request{}, err
	}
	done := make(chan struct{})
	s.inflight[request.ID] = done
	request.State = "running"
	if err := s.store.PutRequest(ctx, request); err != nil {
		delete(s.inflight, request.ID)
		close(done)
		s.mu.Unlock()
		return Request{}, err
	}
	s.mu.Unlock()

	if err := s.execute(ctx, request, fingerprint, done); err != nil {
		return Request{}, err
	}
	return s.Get(ctx, request.ID)
}

func (s *Service) waitLocked(ctx context.Context, existing Request) (Request, error) {
	if existing.State == "completed" {
		s.mu.Unlock()
		return existing, nil
	}
	done, running := s.inflight[existing.ID]
	if !running {
		s.mu.Unlock()
		return existing, nil
	}
	s.mu.Unlock()
	select {
	case <-done:
		return s.Get(ctx, existing.ID)
	case <-ctx.Done():
		return Request{}, ctx.Err()
	}
}

func (s *Service) findUpstreamLocked(ctx context.Context, serverID, upstreamID string) (Request, bool) {
	requests, err := s.store.ListRequests(ctx)
	if err != nil {
		return Request{}, false
	}
	for _, request := range requests {
		if request.ServerID == serverID && request.UpstreamRequestID == upstreamID {
			return request, true
		}
	}
	return Request{}, false
}

func (s *Service) execute(ctx context.Context, request Request, fingerprint string, done chan struct{}) error {
	runCtx := context.WithoutCancel(ctx)
	timeout := s.timeout
	if request.Deadline != nil {
		remaining := time.Until(*request.Deadline)
		if remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}
	runCtx, cancel := context.WithTimeout(runCtx, timeout)
	defer cancel()

	s.mu.Lock()
	state := snapshotOf(s.adapters[request.Adapter])
	s.mu.Unlock()
	var outcome Outcome
	var execErr error
	switch {
	case state.adapter == nil:
		outcome = Outcome{Class: ClassUnsupported, Detail: "adapter is not registered"}
	case !state.trusted:
		detail := state.reason
		if detail == "" {
			detail = "adapter is not verified"
		}
		outcome = Outcome{Class: ClassUnsupported, Detail: detail}
	case !slices.Contains(state.kinds, request.Kind):
		outcome = Outcome{Class: ClassUnsupported, Detail: "adapter does not support " + string(request.Kind)}
	default:
		dir, err := os.MkdirTemp("", "foreman-discovery-")
		if err != nil {
			execErr = errors.New("private discovery directory is unavailable")
			break
		}
		_ = os.Chmod(dir, 0o700)
		binaryPath, configPath, reason, prepErr := s.boundInputs(ctx, request, state.adapter, dir)
		if prepErr != nil {
			execErr = prepErr
			_ = os.RemoveAll(dir)
			break
		}
		if reason != "" {
			outcome = Outcome{Class: ClassUnsupported, Detail: reason}
			_ = os.RemoveAll(dir)
			break
		}
		outcome, execErr = state.adapter.Enumerate(runCtx, Call{
			ServerID: request.ServerID, WorkspaceID: request.WorkspaceID,
			RuntimeID: request.RuntimeID, Profile: request.Profile, Kind: request.Kind,
			Platform: request.Platform, CLIDigest: request.CLIDigest,
			ConfigRevision: request.ConfigRevision, Dir: dir,
			BinaryPath: binaryPath, ConfigPath: configPath,
		})
		_ = os.RemoveAll(dir)
	}
	return s.complete(request.ID, fingerprint, outcome, execErr, done)
}

func (s *Service) boundInputs(ctx context.Context, request Request, adapter Adapter, dir string) (string, string, string, error) {
	bound, ok := adapter.(BoundAdapter)
	if !ok || !bound.RequiresBoundInputs() {
		return "", "", "", nil
	}
	if request.CLIDigest == "" || s.artifacts == nil {
		return "", "", "verified CLI artifact is not available", nil
	}
	body, err := s.artifacts.VerifiedArtifactByDomain(ctx, request.ServerID, request.CLIDigest)
	if err != nil {
		return "", "", "verified CLI artifact is unavailable: " + err.Error(), nil
	}
	binaryPath := dir + "/agent-cli"
	if err := os.WriteFile(binaryPath, body, 0o500); err != nil {
		return "", "", "", errors.New("bound CLI could not be materialized")
	}
	if request.ConfigRevision == 0 || s.configs == nil {
		return "", "", "Agent configuration is not available", nil
	}
	configPath, revision, found, err := s.configs.ProjectFile(
		ctx, request.ServerID, request.WorkspaceID, request.Adapter,
		request.Profile, request.ID, dir)
	if err != nil {
		return "", "", "", err
	}
	if !found || revision != request.ConfigRevision {
		return "", "", "Agent configuration revision is not available", nil
	}
	return binaryPath, configPath, "", nil
}

func (s *Service) complete(requestID, fingerprint string, outcome Outcome, execErr error, done chan struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	request, found, err := s.store.GetRequest(context.Background(), requestID)
	if err != nil || !found {
		delete(s.inflight, requestID)
		close(done)
		if err != nil {
			return fmt.Errorf("read the running discovery request: %w", err)
		}
		return ErrNotFound
	}
	now := s.now()
	result := Result{
		Class: outcome.Class, Provider: outcome.Provider,
		Models:       append([]Model{}, outcome.Models...),
		Capabilities: append([]Capability{}, outcome.Capabilities...),
		Detail:       outcome.Detail, CompletedAt: now,
	}
	if execErr != nil {
		result.Class = ClassFailed
		result.Models = []Model{}
		result.Capabilities = []Capability{}
		result.Detail = "adapter execution failed: " + execErr.Error()
	}
	if result.Class == "" {
		if len(result.Models) > 0 || len(result.Capabilities) > 0 {
			result.Class = ClassNonEmpty
		} else {
			result.Class = ClassEmpty
		}
	}
	request.State = "completed"
	request.Result = &result
	request.CompletedAt = &now
	envKey := envKeyOf(SubmitInput{
		ServerID: request.ServerID, WorkspaceID: request.WorkspaceID,
		RuntimeID: request.RuntimeID, Profile: request.Profile, Kind: request.Kind,
	})
	latest, hasLatest, latestErr := s.store.GetLatest(context.Background(), envKey)
	if latestErr != nil {
		s.log.Warn("discovery.latest_read_failed", "request_id", requestID, "err", latestErr.Error())
	}
	if hasLatest && request.Generation < latest.Generation {
		request.Superseded = true
	}
	// A failed latest read leaves the ordering unknown. Writing then could
	// overwrite a newer generation, so keep the old current result instead.
	advance := latestErr == nil && (!hasLatest || request.Generation >= latest.Generation)
	latestValue := Latest{
		RequestID: request.ID, Generation: request.Generation,
		Fingerprint: fingerprint, Adapter: request.Adapter,
		CLIDigest: request.CLIDigest, ConfigRevision: request.ConfigRevision,
		Platform: request.Platform, Result: result, CompletedAt: now,
	}
	if err := s.store.PutRequest(context.Background(), request); err != nil {
		s.log.Warn("discovery.request_write_failed", "request_id", requestID, "err", err.Error())
		// The request record and the current result move together: a failed
		// completion write must not publish a current success while the
		// request still reads running. Keep the completed answer in memory so
		// GET or a replay can persist it after the store recovers.
		s.pending[requestID] = pendingCompletion{
			request: request, envKey: envKey, latest: latestValue, advance: advance,
		}
		delete(s.inflight, requestID)
		close(done)
		return fmt.Errorf("persist the completed discovery request: %w", err)
	}
	if advance {
		if err := s.store.PutLatest(context.Background(), envKey, latestValue); err != nil {
			s.log.Warn("discovery.latest_write_failed", "request_id", requestID, "err", err.Error())
		}
	}
	delete(s.pending, requestID)
	delete(s.inflight, requestID)
	close(done)
	return nil
}

// Get reads one request.
func (s *Service) Get(ctx context.Context, id string) (Request, error) {
	s.mu.Lock()
	if _, ok := s.pending[id]; ok {
		if _, err := s.flushPendingLocked(ctx, Request{ID: id}); err != nil {
			s.mu.Unlock()
			return Request{}, fmt.Errorf("the completed discovery result is not durable: %w", err)
		}
	}
	s.mu.Unlock()
	request, ok, err := s.store.GetRequest(ctx, id)
	if err != nil {
		return Request{}, err
	}
	if !ok {
		return Request{}, ErrNotFound
	}
	return request, nil
}

// flushPendingLocked retries a failed completion write. The caller holds
// s.mu. A missing pending entry returns the request as the store sees it.
func (s *Service) flushPendingLocked(ctx context.Context, existing Request) (Request, error) {
	pending, ok := s.pending[existing.ID]
	if !ok {
		return existing, nil
	}
	if err := s.store.PutRequest(ctx, pending.request); err != nil {
		return existing, fmt.Errorf("the completed discovery result is not durable: %w", err)
	}
	if pending.advance {
		latest, hasLatest, err := s.store.GetLatest(ctx, pending.envKey)
		if err != nil {
			return existing, fmt.Errorf("read the current discovery result: %w", err)
		}
		if !hasLatest || pending.request.Generation >= latest.Generation {
			if err := s.store.PutLatest(ctx, pending.envKey, pending.latest); err != nil {
				s.log.Warn("discovery.latest_write_failed", "request_id", pending.request.ID, "err", err.Error())
			}
		}
	}
	delete(s.pending, pending.request.ID)
	return pending.request, nil
}

// authorizedReplay re-checks the adapter's current trust after a repeated
// upstream request has waited. The original request association is kept and
// the adapter is not executed again, but a revoked adapter cannot hand back a
// usable old success.
func (s *Service) authorizedReplay(request Request) Request {
	s.mu.Lock()
	state := s.adapters[request.Adapter]
	trusted := state != nil && state.trusted
	reason := ""
	if state != nil {
		reason = state.reason
	}
	s.mu.Unlock()
	if trusted {
		return request
	}
	if reason == "" {
		reason = "the adapter is no longer trusted"
	}
	now := s.now()
	request.State = "completed"
	request.Result = &Result{
		Class: ClassUnsupported, Models: []Model{}, Capabilities: []Capability{},
		Detail: reason, CompletedAt: now,
	}
	return request
}

// List reads the requests matching the optional filters, newest first.
func (s *Service) List(ctx context.Context, serverID, workspaceID string) ([]Request, error) {
	requests, err := s.store.ListRequests(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Request, 0, len(requests))
	for _, request := range requests {
		if serverID != "" && request.ServerID != serverID {
			continue
		}
		if workspaceID != "" && request.WorkspaceID != workspaceID {
			continue
		}
		out = append(out, request)
	}
	slices.Reverse(out)
	return out, nil
}

// Models reads one environment's current result and revalidates it against
// the caller's current CLI, configuration, platform and adapter trust. A stale
// or superseded result stays history, never the current catalog.
func (s *Service) Models(ctx context.Context, query ModelQuery) (ModelCatalog, error) {
	envKey := envKeyOf(SubmitInput{
		ServerID: query.ServerID, WorkspaceID: query.WorkspaceID, RuntimeID: query.RuntimeID,
		Profile: query.Profile, Kind: KindModels,
	})
	latest, ok, err := s.store.GetLatest(ctx, envKey)
	if err != nil {
		return ModelCatalog{}, err
	}
	view := ModelCatalog{
		ServerID: query.ServerID, WorkspaceID: query.WorkspaceID, RuntimeID: query.RuntimeID,
		Profile: query.Profile, Models: []Model{},
	}
	if !ok {
		return view, nil
	}
	view.Class = latest.Result.Class
	view.Models = append(view.Models, latest.Result.Models...)
	view.RequestID = latest.RequestID
	view.Generation = latest.Generation
	view.CompletedAt = latest.CompletedAt.UTC().Format(time.RFC3339)
	view.Detail = latest.Result.Detail
	if !cacheable(latest.Result.Class) {
		return view, nil
	}
	if s.now().Sub(latest.CompletedAt) >= s.cacheTTL {
		view.Detail = "the cached model result has expired"
		return view, nil
	}
	state := s.adapterSnapshot(latest.Adapter)
	if state.adapter == nil || !state.trusted {
		view.Detail = state.reason
		if view.Detail == "" {
			view.Detail = "the adapter is no longer trusted"
		}
		return view, nil
	}
	// Every generation component must still match. A missing adapter,
	// digest, platform or configuration answer is unknown, never a wildcard:
	// the current-catalog endpoint must not become a history read that keeps
	// answering available=true after a rotation.
	if query.Adapter == "" || query.CLIDigest == "" || query.Platform == "" || !query.ConfigKnown {
		view.Detail = "the current environment binding is unknown"
		return view, nil
	}
	if !strings.EqualFold(query.Adapter, latest.Adapter) {
		view.Detail = "the adapter changed"
		return view, nil
	}
	if !strings.EqualFold(query.CLIDigest, latest.CLIDigest) {
		view.Detail = "the CLI digest changed"
		return view, nil
	}
	if query.ConfigRevision != latest.ConfigRevision {
		view.Detail = "the configuration revision changed"
		return view, nil
	}
	if !strings.EqualFold(query.Platform, latest.Platform) {
		view.Detail = "the platform changed"
		return view, nil
	}
	view.Available = true
	return view, nil
}

func statusOf(state *adapterState) AdapterStatus {
	snapshot := snapshotOf(state)
	kinds := make([]string, 0, len(snapshot.kinds))
	for _, kind := range snapshot.kinds {
		kinds = append(kinds, string(kind))
	}
	status := AdapterStatus{
		Name: snapshot.adapter.Name(), Kinds: kinds,
		Trusted: snapshot.trusted, Reason: snapshot.reason,
	}
	if !snapshot.verifiedAt.IsZero() {
		status.VerifiedAt = snapshot.verifiedAt.UTC().Format(time.RFC3339)
	}
	return status
}

func (s *Service) adapterSnapshot(name string) adapterSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return snapshotOf(s.adapters[name])
}

func snapshotOf(state *adapterState) adapterSnapshot {
	if state == nil || state.adapter == nil {
		return adapterSnapshot{}
	}
	return adapterSnapshot{
		adapter: state.adapter, trusted: state.trusted, reason: state.reason,
		verifiedAt: state.verifiedAt, kinds: append([]Kind(nil), state.adapter.Kinds()...),
	}
}

func resultCopy(result Result) *Result {
	copied := result
	copied.Models = append([]Model(nil), result.Models...)
	copied.Capabilities = append([]Capability(nil), result.Capabilities...)
	return &copied
}

func envKeyOf(input SubmitInput) string {
	return strings.Join([]string{
		input.ServerID, input.WorkspaceID, input.RuntimeID, input.Profile, string(input.Kind),
	}, "\x00")
}

func fingerprintOf(input SubmitInput) string {
	return strings.Join([]string{
		input.Adapter, input.CLIDigest, fmt.Sprint(input.ConfigRevision), input.Platform,
	}, "\x00")
}

func cacheable(class ResultClass) bool {
	return class == ClassNonEmpty || class == ClassEmpty
}
