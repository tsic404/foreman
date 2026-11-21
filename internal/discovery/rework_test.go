package discovery

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeArtifacts struct {
	body []byte
	err  error
}

func (f fakeArtifacts) VerifiedArtifactByDomain(_ context.Context, _ string, digest string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	if digest == "" {
		return nil, errors.New("empty digest")
	}
	return f.body, nil
}

type fakeConfigs struct {
	body     []byte
	revision uint64
	ok       bool
}

func (f fakeConfigs) ProjectFile(_ context.Context, _, _, _, _, _, dir string) (string, uint64, bool, error) {
	if !f.ok {
		return "", 0, false, nil
	}
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, f.body, 0o400); err != nil {
		return "", 0, false, err
	}
	return path, f.revision, true, nil
}

type boundAdapter struct {
	name     string
	kinds    []Kind
	calls    atomic.Int32
	captured Call
}

func (a *boundAdapter) Name() string                { return a.name }
func (a *boundAdapter) Kinds() []Kind               { return a.kinds }
func (a *boundAdapter) Probe(context.Context) error { return nil }
func (a *boundAdapter) RequiresBoundInputs() bool   { return true }
func (a *boundAdapter) Enumerate(_ context.Context, call Call) (Outcome, error) {
	a.calls.Add(1)
	a.captured = call
	binary, err := os.ReadFile(call.BinaryPath)
	if err != nil {
		return Outcome{}, err
	}
	config, err := os.ReadFile(call.ConfigPath)
	if err != nil {
		return Outcome{}, err
	}
	if string(binary) != "verified-binary" || string(config) != `{"provider":"omp"}` {
		return Outcome{}, errors.New("bound inputs do not carry the expected bytes")
	}
	return Outcome{Models: []Model{{ID: "m1", Provider: "omp"}}}, nil
}

// TestBoundExecutionUsesVerifiedCLIAndConfig is MODEL-09's bound-input
// clause: the executor materializes the request's verified CLI and the exact
// configuration revision, and never falls back to a global binary.
func TestBoundExecutionUsesVerifiedCLIAndConfig(t *testing.T) {
	adapter := &boundAdapter{name: "omp", kinds: []Kind{KindModels}}
	service, err := New(Config{
		Store: NewMemoryStore(), Adapters: []AdapterRegistration{{Adapter: adapter}},
		Artifacts: fakeArtifacts{body: []byte("verified-binary")},
		Configs:   fakeConfigs{body: []byte(`{"provider":"omp"}`), revision: 7, ok: true},
		Clock:     func() time.Time { return testNow }, Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	request, err := service.Submit(context.Background(), SubmitInput{
		ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
		Profile: "primary", Kind: KindModels, Adapter: "omp",
		CLIDigest: "d", ConfigRevision: 7, Platform: "linux/amd64",
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if request.Result == nil || request.Result.Class != ClassNonEmpty {
		t.Fatalf("result = %+v, want a non-empty success", request.Result)
	}
	if adapter.calls.Load() != 1 {
		t.Fatalf("adapter calls = %d, want 1", adapter.calls.Load())
	}
	if adapter.captured.CLIDigest != "d" || adapter.captured.ConfigRevision != 7 {
		t.Fatalf("captured call = %+v, want the request's digest and revision", adapter.captured)
	}
}

// TestBoundExecutionRefusesMissingInputs keeps an unavailable verified CLI or
// configuration from running the global command.
func TestBoundExecutionRefusesMissingInputs(t *testing.T) {
	adapter := &boundAdapter{name: "omp", kinds: []Kind{KindModels}}
	service, err := New(Config{
		Store: NewMemoryStore(), Adapters: []AdapterRegistration{{Adapter: adapter}},
		Artifacts: fakeArtifacts{body: []byte("verified-binary")},
		Configs:   fakeConfigs{body: []byte(`{"provider":"omp"}`), revision: 7, ok: true},
		Clock:     func() time.Time { return testNow }, Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	for name, input := range map[string]SubmitInput{
		"missing CLI": {
			ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
			Kind: KindModels, Adapter: "omp", ConfigRevision: 7,
		},
		"missing config": {
			ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
			Kind: KindModels, Adapter: "omp", CLIDigest: "d",
		},
	} {
		t.Run(name, func(t *testing.T) {
			request, err := service.Submit(context.Background(), input)
			if err != nil {
				t.Fatalf("Submit: %v", err)
			}
			if request.Result == nil || request.Result.Class != ClassUnsupported {
				t.Fatalf("result = %+v, want unsupported", request.Result)
			}
		})
	}
	if adapter.calls.Load() != 0 {
		t.Fatalf("adapter ran %d times without bound inputs", adapter.calls.Load())
	}
}

// TestCacheHitPersistsNewUpstreamRequest: B's cache hit is a durable
// association, so replaying B after the TTL or a restart never executes the
// command a second time.
func TestCacheHitPersistsNewUpstreamRequest(t *testing.T) {
	clock := &advancingClock{now: testNow}
	adapter := &fakeAdapter{name: "omp", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		return Outcome{Models: []Model{{ID: "m1", Provider: "omp"}}}, nil
	}}
	service, err := New(Config{
		Store: NewMemoryStore(), Adapters: []AdapterRegistration{{Adapter: adapter}},
		Clock: clock.Now, Timeout: 5 * time.Second, CacheTTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	first := input("omp", 1)
	first.UpstreamRequestID = "upstream-a"
	a, err := service.Submit(context.Background(), first)
	if err != nil {
		t.Fatalf("Submit A: %v", err)
	}
	clock.Advance(30 * time.Second)
	second := input("omp", 1)
	second.UpstreamRequestID = "upstream-b"
	b, err := service.Submit(context.Background(), second)
	if err != nil {
		t.Fatalf("Submit B: %v", err)
	}
	if b.ID == a.ID || b.CachedFrom != a.ID {
		t.Fatalf("B = %+v, want a persisted association to A %s", b, a.ID)
	}
	if adapter.calls.Load() != 1 {
		t.Fatalf("adapter calls = %d, want the cache hit to reuse A", adapter.calls.Load())
	}
	clock.Advance(2 * time.Minute)
	replayed, err := service.Submit(context.Background(), second)
	if err != nil {
		t.Fatalf("replay B: %v", err)
	}
	if replayed.ID != b.ID {
		t.Fatalf("replay B = %s, want the persisted request %s", replayed.ID, b.ID)
	}
	if adapter.calls.Load() != 1 {
		t.Fatalf("adapter calls after replay = %d, want 1", adapter.calls.Load())
	}
}

// TestSubmitAfterTrustRevocationDoesNotReplayTheCache: a revoked adapter's
// cached result is history. A new request must answer unsupported without
// running the adapter again, not hand back the old non-empty list.
func TestSubmitAfterTrustRevocationDoesNotReplayTheCache(t *testing.T) {
	service, _, adapter := catalogFixture(t)
	adapter.probeErr = errors.New("credential revoked")
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	request, err := service.Submit(context.Background(), SubmitInput{
		ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
		Kind: KindModels, Adapter: "omp", CLIDigest: "d",
		ConfigRevision: 1, Platform: "linux/amd64",
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if request.Result == nil || request.Result.Class != ClassUnsupported {
		t.Fatalf("result = %+v, want unsupported after revocation", request.Result)
	}
	if adapter.calls.Load() != 1 {
		t.Fatalf("adapter calls = %d, want only the pre-revocation execution", adapter.calls.Load())
	}
}

// TestReplayOfTheSameUpstreamRequestAfterRevocationIsUnsupported: a completed
// upstream request keeps its association, but once the adapter is revoked the
// same ID cannot be replayed as a usable non-empty/empty success and must not
// execute again.
func TestReplayOfTheSameUpstreamRequestAfterRevocationIsUnsupported(t *testing.T) {
	service, _, adapter := catalogFixture(t)
	original := input("omp", 1)
	original.Platform = "linux/amd64"
	original.UpstreamRequestID = "upstream-replayed"
	first, err := service.Submit(context.Background(), original)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if first.Result == nil || first.Result.Class != ClassNonEmpty {
		t.Fatalf("first result = %+v, want a non-empty success", first.Result)
	}
	adapter.probeErr = errors.New("credential revoked")
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	replayed, err := service.Submit(context.Background(), original)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed.ID != first.ID || replayed.UpstreamRequestID != original.UpstreamRequestID {
		t.Fatalf("replay = %+v, want the original association %s", replayed, first.ID)
	}
	if replayed.Result == nil || replayed.Result.Class != ClassUnsupported {
		t.Fatalf("replay result = %+v, want unsupported after revocation", replayed.Result)
	}
	if len(replayed.Result.Models) != 0 || len(replayed.Result.Capabilities) != 0 {
		t.Fatalf("replay result = %+v, want no old model list", replayed.Result)
	}
	if adapter.calls.Load() != 1 {
		t.Fatalf("adapter calls = %d, want no replay execution", adapter.calls.Load())
	}
}

func catalogFixture(t *testing.T) (*Service, *advancingClock, *fakeAdapter) {
	t.Helper()
	clock := &advancingClock{now: testNow}
	adapter := &fakeAdapter{name: "omp", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		return Outcome{Models: []Model{{ID: "m1", Provider: "omp"}}}, nil
	}}
	service, err := New(Config{
		Store: NewMemoryStore(), Adapters: []AdapterRegistration{{Adapter: adapter}},
		Clock: clock.Now, Timeout: 5 * time.Second, CacheTTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	base := input("omp", 1)
	base.Platform = "linux/amd64"
	if _, err := service.Submit(context.Background(), base); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	return service, clock, adapter
}

func query() ModelQuery {
	return ModelQuery{
		ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
		Adapter: "omp", CLIDigest: "d", ConfigRevision: 1, Platform: "linux/amd64",
		ConfigKnown: true,
	}
}

// TestModelCatalogInvalidatesOnEveryGenerationComponent: TTL, CLI rotation,
// configuration rotation, platform change and trust revocation each make the
// old result unavailable instead of continuing to answer available=true.
func TestModelCatalogInvalidatesOnEveryGenerationComponent(t *testing.T) {
	t.Run("ttl", func(t *testing.T) {
		service, clock, _ := catalogFixture(t)
		clock.Advance(2 * time.Minute)
		view, err := service.Models(context.Background(), query())
		if err != nil {
			t.Fatalf("Models: %v", err)
		}
		if view.Available || !strings.Contains(view.Detail, "expired") {
			t.Fatalf("view = %+v, want an expired unavailable catalog", view)
		}
	})
	t.Run("cli digest", func(t *testing.T) {
		service, _, _ := catalogFixture(t)
		q := query()
		q.CLIDigest = "other"
		view, err := service.Models(context.Background(), q)
		if err != nil || view.Available {
			t.Fatalf("view = %+v, %v; want unavailable", view, err)
		}
	})
	t.Run("config revision", func(t *testing.T) {
		service, _, _ := catalogFixture(t)
		for _, revision := range []uint64{0, 2} {
			q := query()
			q.ConfigRevision = revision
			view, err := service.Models(context.Background(), q)
			if err != nil || view.Available {
				t.Fatalf("revision %d view = %+v, %v; want unavailable", revision, view, err)
			}
		}
	})
	t.Run("platform", func(t *testing.T) {
		service, _, _ := catalogFixture(t)
		q := query()
		q.Platform = "linux/arm64"
		view, err := service.Models(context.Background(), q)
		if err != nil || view.Available {
			t.Fatalf("view = %+v, %v; want unavailable", view, err)
		}
	})
	t.Run("adapter", func(t *testing.T) {
		service, _, _ := catalogFixture(t)
		q := query()
		q.Adapter = "codex"
		view, err := service.Models(context.Background(), q)
		if err != nil || view.Available {
			t.Fatalf("view = %+v, %v; want unavailable", view, err)
		}
	})
	t.Run("trust revoked", func(t *testing.T) {
		service, _, adapter := catalogFixture(t)
		adapter.probeErr = errors.New("credential revoked")
		if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
			t.Fatalf("VerifyAdapter: %v", err)
		}
		view, err := service.Models(context.Background(), query())
		if err != nil || view.Available {
			t.Fatalf("view = %+v, %v; want unavailable after revocation", view, err)
		}
	})
}

// TestDiscoveryFileStoreWriteFailureKeepsLatest: a failed request write does
// not replace the visible current result.
func TestDiscoveryFileStoreWriteFailureKeepsLatest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "discovery.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	adapter := &fakeAdapter{name: "omp", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		return Outcome{Models: []Model{{ID: "m1", Provider: "omp"}}}, nil
	}}
	service, err := New(Config{
		Store: store, Adapters: []AdapterRegistration{{Adapter: adapter}},
		Clock: func() time.Time { return testNow }, Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	if _, err := service.Submit(context.Background(), input("omp", 1)); err != nil {
		t.Fatalf("first Submit: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove registry: %v", err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("mkdir registry path: %v", err)
	}
	if _, err := service.Submit(context.Background(), input("omp", 2)); err == nil {
		t.Fatal("Submit succeeded although the registry rename could not land")
	}
	q := query()
	view, err := service.Models(context.Background(), q)
	if err != nil {
		t.Fatalf("Models: %v", err)
	}
	if !view.Available || len(view.Models) != 1 || view.Models[0].ID != "m1" {
		t.Fatalf("view = %+v, want the old current result", view)
	}
}

type failingCompletionStore struct {
	*MemoryStore
	fail atomic.Bool
}

func (s *failingCompletionStore) PutRequest(ctx context.Context, request Request) error {
	if request.State == "completed" && s.fail.Load() {
		return errors.New("simulated completion write failure")
	}
	return s.MemoryStore.PutRequest(ctx, request)
}

// TestCompletedWriteFailureDoesNotAdvanceLatestOrStrandTheRequest: a failed
// completed-request write must not publish a contradictory current success.
// Once the store recovers, GET and a replay converge on the same result
// without executing the adapter again.
func TestCompletedWriteFailureDoesNotAdvanceLatestOrStrandTheRequest(t *testing.T) {
	store := &failingCompletionStore{MemoryStore: NewMemoryStore()}
	adapter := &fakeAdapter{name: "omp", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		return Outcome{Models: []Model{{ID: "m1", Provider: "omp"}}}, nil
	}}
	service, err := New(Config{
		Store: store, Adapters: []AdapterRegistration{{Adapter: adapter}},
		Clock: func() time.Time { return testNow }, Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	first := input("omp", 1)
	first.Platform = "linux/amd64"
	first.UpstreamRequestID = "upstream-a"
	if _, err := service.Submit(context.Background(), first); err != nil {
		t.Fatalf("first Submit: %v", err)
	}
	store.fail.Store(true)
	second := input("omp", 2)
	second.Platform = "linux/amd64"
	second.UpstreamRequestID = "upstream-b"
	if _, err := service.Submit(context.Background(), second); err == nil {
		t.Fatal("Submit succeeded although the completion write failed")
	}
	requests, err := service.List(context.Background(), "", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var secondID string
	for _, request := range requests {
		if request.UpstreamRequestID == "upstream-b" {
			secondID = request.ID
		}
	}
	if secondID == "" {
		t.Fatal("the failed completion left no recoverable request")
	}
	// While the store is still failing, GET reports an explicit error rather
	// than returning a running request with a success already visible.
	if _, err := service.Get(context.Background(), secondID); err == nil {
		t.Fatal("GET hid the incomplete durable write")
	}
	oldView, err := service.Models(context.Background(), ModelQuery{
		ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
		Adapter: "omp", CLIDigest: "d", ConfigRevision: 1,
		Platform: "linux/amd64", ConfigKnown: true,
	})
	if err != nil {
		t.Fatalf("Models: %v", err)
	}
	if !oldView.Available || oldView.RequestID == secondID {
		t.Fatalf("current result = %+v, want the first generation, not the failed completion", oldView)
	}
	store.fail.Store(false)
	recovered, err := service.Get(context.Background(), secondID)
	if err != nil {
		t.Fatalf("GET after recovery: %v", err)
	}
	if recovered.State != "completed" || recovered.Result == nil || recovered.Result.Class != ClassNonEmpty {
		t.Fatalf("recovered request = %+v, want the persisted completed result", recovered)
	}
	replayed, err := service.Submit(context.Background(), second)
	if err != nil {
		t.Fatalf("replay after recovery: %v", err)
	}
	if replayed.ID != secondID || replayed.Result == nil || replayed.Result.Class != ClassNonEmpty {
		t.Fatalf("replay = %+v, want the original completed association %s", replayed, secondID)
	}
	if adapter.calls.Load() != 2 {
		t.Fatalf("adapter calls = %d, want one execution per request", adapter.calls.Load())
	}
	newView, err := service.Models(context.Background(), ModelQuery{
		ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
		Adapter: "omp", CLIDigest: "d", ConfigRevision: 2,
		Platform: "linux/amd64", ConfigKnown: true,
	})
	if err != nil {
		t.Fatalf("Models after recovery: %v", err)
	}
	if !newView.Available || newView.RequestID != secondID {
		t.Fatalf("current result after recovery = %+v, want the recovered generation", newView)
	}
}

type toggleTrustAdapter struct {
	flip  atomic.Int32
	calls atomic.Int32
}

func (a *toggleTrustAdapter) Name() string  { return "omp" }
func (a *toggleTrustAdapter) Kinds() []Kind { return []Kind{KindModels} }
func (a *toggleTrustAdapter) Probe(context.Context) error {
	if a.flip.Add(1)%2 == 0 {
		return nil
	}
	return errors.New("verification failed")
}
func (a *toggleTrustAdapter) Enumerate(context.Context, Call) (Outcome, error) {
	a.calls.Add(1)
	return Outcome{Models: []Model{{ID: "m1", Provider: "omp"}}}, nil
}

// TestConcurrentVerifyAndSubmitUseOneTrustSnapshot is the race-detector
// target: execution reads one immutable trust snapshot while verification
// replaces it.
func TestConcurrentVerifyAndSubmitUseOneTrustSnapshot(t *testing.T) {
	adapter := &toggleTrustAdapter{}
	service, err := New(Config{
		Store: NewMemoryStore(), Adapters: []AdapterRegistration{{Adapter: adapter}},
		Clock: func() time.Time { return testNow }, Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = service.VerifyAdapter(context.Background(), "omp")
		}()
		go func() {
			defer wg.Done()
			_, _ = service.Submit(context.Background(), input("omp", uint64(i+1)))
		}()
	}
	wg.Wait()
}

// TestOMPBoundProbeUsesTheGivenPayload: registration must probe the exact
// verified binary path, not the adapter's global command.
func TestOMPBoundProbeUsesTheGivenPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent-cli")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then echo 'omp 1.0.0'; exit 0; fi\n" +
		"if [ \"$1\" = \"models\" ] && [ \"$2\" = \"ls\" ] && [ \"$3\" = \"--help\" ]; then echo '--json --no-extensions'; exit 0; fi\n" +
		"exit 1\n"
	if err := os.WriteFile(path, []byte(script), 0o500); err != nil {
		t.Fatalf("write probe script: %v", err)
	}
	adapter := NewOMPAdapter("/does/not/exist")
	version, err := adapter.ProbeBound(context.Background(), path)
	if err != nil {
		t.Fatalf("ProbeBound: %v", err)
	}
	if version != "1.0.0" {
		t.Fatalf("ProbeBound version = %q, want 1.0.0", version)
	}
	if _, err := adapter.ProbeBound(context.Background(), ""); err == nil {
		t.Fatal("ProbeBound accepted an empty payload path")
	}
}

// TestOMPBoundProbeRejectsAnUnparseableVersion: a non-empty version line is
// not enough; the payload must report an exact version in the accepted form.
func TestOMPBoundProbeRejectsAnUnparseableVersion(t *testing.T) {
	for _, output := range []string{"omp/not-a-version", "omp/", "omp version 1.0.0", "garbage"} {
		t.Run(output, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "agent-cli")
			script := "#!/bin/sh\n" +
				"if [ \"$1\" = \"--version\" ]; then printf '%s\\n' '" + output + "'; exit 0; fi\n" +
				"if [ \"$1\" = \"models\" ] && [ \"$2\" = \"ls\" ] && [ \"$3\" = \"--help\" ]; then echo '--json --no-extensions'; exit 0; fi\n" +
				"exit 1\n"
			if err := os.WriteFile(path, []byte(script), 0o500); err != nil {
				t.Fatalf("write probe script: %v", err)
			}
			adapter := NewOMPAdapter("/does/not/exist")
			if _, err := adapter.ProbeBound(context.Background(), path); err == nil {
				t.Fatalf("ProbeBound accepted version output %q", output)
			}
		})
	}
}
