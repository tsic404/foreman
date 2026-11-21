package discovery

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testNow = time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)

type fakeAdapter struct {
	name      string
	kinds     []Kind
	probeErr  error
	enumerate func(ctx context.Context, call Call) (Outcome, error)
	calls     atomic.Int32
}

func (a *fakeAdapter) Name() string                { return a.name }
func (a *fakeAdapter) Kinds() []Kind               { return a.kinds }
func (a *fakeAdapter) Probe(context.Context) error { return a.probeErr }
func (a *fakeAdapter) Enumerate(ctx context.Context, call Call) (Outcome, error) {
	a.calls.Add(1)
	return a.enumerate(ctx, call)
}

func newService(t *testing.T, adapters ...AdapterRegistration) *Service {
	t.Helper()
	service, err := New(Config{
		Store: NewMemoryStore(), Adapters: adapters,
		Clock: func() time.Time { return testNow }, Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return service
}

func input(adapter string, revision uint64) SubmitInput {
	return SubmitInput{
		ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
		Kind: KindModels, Adapter: adapter, CLIDigest: "d", ConfigRevision: revision,
		Platform: "linux/amd64",
	}
}

// TestFourResultClassesStayDistinct is V2-C04's core: a non-empty list, a
// legal empty list, an unsupported adapter and an execution failure each keep
// their own class.
func TestFourResultClassesStayDistinct(t *testing.T) {
	nonEmpty := &fakeAdapter{name: "nonempty", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		return Outcome{Models: []Model{{ID: "m1", Provider: "p"}}}, nil
	}}
	empty := &fakeAdapter{name: "empty", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		return Outcome{Models: []Model{}}, nil
	}}
	failed := &fakeAdapter{name: "failed", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		return Outcome{}, errors.New("bad JSON")
	}}
	unverified := &fakeAdapter{name: "unverified", kinds: []Kind{KindModels}, probeErr: errors.New("protocol unverified"), enumerate: func(context.Context, Call) (Outcome, error) {
		t.Fatal("an unverified adapter was executed")
		return Outcome{}, nil
	}}
	service := newService(t,
		AdapterRegistration{Adapter: nonEmpty},
		AdapterRegistration{Adapter: empty},
		AdapterRegistration{Adapter: failed},
		AdapterRegistration{Adapter: unverified, Reason: "protocol unverified"},
	)
	for _, name := range []string{"nonempty", "empty", "failed"} {
		if _, err := service.VerifyAdapter(context.Background(), name); err != nil {
			t.Fatalf("VerifyAdapter %s: %v", name, err)
		}
	}
	if status, err := service.VerifyAdapter(context.Background(), "unverified"); err != nil || status.Trusted {
		t.Fatalf("unverified status = %+v, %v; want untrusted", status, err)
	}
	cases := []struct {
		adapter string
		class   ResultClass
	}{
		{"nonempty", ClassNonEmpty},
		{"empty", ClassEmpty},
		{"unverified", ClassUnsupported},
		{"failed", ClassFailed},
	}
	for i, tc := range cases {
		request, err := service.Submit(context.Background(), input(tc.adapter, uint64(i+1)))
		if err != nil {
			t.Fatalf("Submit %s: %v", tc.adapter, err)
		}
		if request.Result == nil || request.Result.Class != tc.class {
			t.Fatalf("%s result = %+v, want %s", tc.adapter, request.Result, tc.class)
		}
	}
	if unverified.calls.Load() != 0 {
		t.Fatalf("the unverified adapter ran %d times", unverified.calls.Load())
	}
}

// TestDuplicateUpstreamRequestExecutesOnce is MODEL-09's dedupe clause: the
// same upstream request ID returns the first result and runs the adapter once.
func TestDuplicateUpstreamRequestExecutesOnce(t *testing.T) {
	adapter := &fakeAdapter{name: "omp", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		return Outcome{Models: []Model{{ID: "m1"}}}, nil
	}}
	service := newService(t, AdapterRegistration{Adapter: adapter})
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	first := input("omp", 1)
	first.UpstreamRequestID = "upstream-1"
	request, err := service.Submit(context.Background(), first)
	if err != nil {
		t.Fatalf("first Submit: %v", err)
	}
	second, err := service.Submit(context.Background(), first)
	if err != nil {
		t.Fatalf("second Submit: %v", err)
	}
	if request.ID != second.ID {
		t.Fatalf("duplicate IDs = %s and %s, want one request", request.ID, second.ID)
	}
	if adapter.calls.Load() != 1 {
		t.Fatalf("adapter ran %d times, want 1", adapter.calls.Load())
	}
}

// TestConcurrentDuplicateWaitsForTheOneExecution is the concurrent form of the
// same clause.
func TestConcurrentDuplicateWaitsForTheOneExecution(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	adapter := &fakeAdapter{name: "omp", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		once.Do(func() { close(started) })
		<-release
		return Outcome{Models: []Model{{ID: "m1"}}}, nil
	}}
	service := newService(t, AdapterRegistration{Adapter: adapter})
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	first := input("omp", 1)
	first.UpstreamRequestID = "upstream-1"
	var wg sync.WaitGroup
	wg.Add(2)
	var firstRequest Request
	var secondRequest Request
	go func() {
		defer wg.Done()
		firstRequest, _ = service.Submit(context.Background(), first)
	}()
	<-started
	go func() {
		defer wg.Done()
		secondRequest, _ = service.Submit(context.Background(), first)
	}()
	close(release)
	wg.Wait()
	if secondRequest.ID != firstRequest.ID {
		t.Fatalf("duplicate IDs = %s and %s", secondRequest.ID, firstRequest.ID)
	}
	if adapter.calls.Load() != 1 {
		t.Fatalf("adapter ran %d times, want 1", adapter.calls.Load())
	}
}

// TestOldGenerationDoesNotOverwriteNew is the generation-ordering clause: a
// request from the previous configuration that finishes last does not replace
// the newer result.
func TestOldGenerationDoesNotOverwriteNew(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	adapter := &fakeAdapter{name: "omp", kinds: []Kind{KindModels}, enumerate: func(_ context.Context, call Call) (Outcome, error) {
		if call.ConfigRevision == 1 {
			once.Do(func() { close(started) })
			<-release
			return Outcome{Models: []Model{{ID: "old"}}}, nil
		}
		return Outcome{Models: []Model{{ID: "new"}}}, nil
	}}
	service := newService(t, AdapterRegistration{Adapter: adapter})
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var oldRequest Request
	go func() {
		defer wg.Done()
		oldRequest, _ = service.Submit(context.Background(), input("omp", 1))
	}()
	<-started
	newRequest, err := service.Submit(context.Background(), input("omp", 2))
	if err != nil {
		t.Fatalf("new Submit: %v", err)
	}
	if newRequest.Generation != 2 {
		t.Fatalf("new generation = %d, want 2", newRequest.Generation)
	}
	close(release)
	wg.Wait()
	if !oldRequest.Superseded {
		t.Fatalf("old request = %+v, want superseded", oldRequest)
	}
	models, err := service.Models(context.Background(), ModelQuery{
		ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
		Adapter: "omp", CLIDigest: "d", ConfigRevision: 2,
		Platform: "linux/amd64", ConfigKnown: true,
	})
	if err != nil {
		t.Fatalf("Models: %v", err)
	}
	if !models.Available || len(models.Models) != 1 || models.Models[0].ID != "new" {
		t.Fatalf("models = %+v, want the new generation", models)
	}
	if models.Generation != 2 {
		t.Fatalf("generation = %d, want 2", models.Generation)
	}
}

// TestUnsupportedIsNotAnEmptySuccess: a missing adapter is unsupported and
// the catalog is not available.
func TestUnsupportedIsNotAnEmptySuccess(t *testing.T) {
	service := newService(t)
	request, err := service.Submit(context.Background(), input("missing", 1))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if request.Result == nil || request.Result.Class != ClassUnsupported {
		t.Fatalf("result = %+v, want unsupported", request.Result)
	}
	models, err := service.Models(context.Background(), ModelQuery{
		ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
	})
	if err != nil {
		t.Fatalf("Models: %v", err)
	}
	if models.Available || models.Class != ClassUnsupported {
		t.Fatalf("catalog = %+v, want unavailable/unsupported", models)
	}
}

func TestAdapterTrustRequiresAProbe(t *testing.T) {
	adapter := &fakeAdapter{name: "codex", kinds: []Kind{KindModels}, probeErr: errors.New("protocol not verified")}
	service := newService(t, AdapterRegistration{Adapter: adapter, Reason: "protocol not verified"})
	status, err := service.VerifyAdapter(context.Background(), "codex")
	if err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	if status.Trusted || !strings.Contains(status.Reason, "protocol not verified") {
		t.Fatalf("status = %+v, want untrusted with the probe reason", status)
	}
	if _, err := service.VerifyAdapter(context.Background(), "missing"); !errors.Is(err, ErrAdapterNotFound) {
		t.Fatalf("missing adapter error = %v", err)
	}
}

// TestSameFingerprintRefreshKeepsGenerationOrdering: a repeated refresh with
// the same adapter/CLI/config/platform is a new execution, and an older
// attempt that finishes later cannot replace it.
func TestSameFingerprintRefreshKeepsGenerationOrdering(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var once sync.Once
	adapter := &fakeAdapter{name: "omp", kinds: []Kind{KindModels}, enumerate: func(context.Context, Call) (Outcome, error) {
		if calls.Add(1) == 1 {
			once.Do(func() { close(started) })
			<-release
			return Outcome{Models: []Model{{ID: "old"}}}, nil
		}
		return Outcome{Models: []Model{{ID: "new"}}}, nil
	}}
	clock := &advancingClock{now: testNow}
	service, err := New(Config{
		Store: NewMemoryStore(), Adapters: []AdapterRegistration{{Adapter: adapter}},
		Clock: clock.Now, Timeout: 5 * time.Second, CacheTTL: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := service.VerifyAdapter(context.Background(), "omp"); err != nil {
		t.Fatalf("VerifyAdapter: %v", err)
	}
	firstInput := input("omp", 1)
	firstInput.UpstreamRequestID = "old"
	var wg sync.WaitGroup
	wg.Add(1)
	var oldRequest Request
	go func() {
		defer wg.Done()
		oldRequest, _ = service.Submit(context.Background(), firstInput)
	}()
	<-started
	clock.Advance(time.Second)
	secondInput := input("omp", 1)
	secondInput.UpstreamRequestID = "new"
	newRequest, err := service.Submit(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("new Submit: %v", err)
	}
	if newRequest.Generation != 2 {
		t.Fatalf("new generation = %d, want 2", newRequest.Generation)
	}
	close(release)
	wg.Wait()
	if !oldRequest.Superseded {
		t.Fatalf("old request = %+v, want superseded", oldRequest)
	}
	models, err := service.Models(context.Background(), ModelQuery{
		ServerID: "atlas", WorkspaceID: "core", RuntimeID: "rt-omp",
		Adapter: "omp", CLIDigest: "d", ConfigRevision: 1,
		Platform: "linux/amd64", ConfigKnown: true,
	})
	if err != nil {
		t.Fatalf("Models: %v", err)
	}
	if !models.Available || len(models.Models) != 1 || models.Models[0].ID != "new" || models.Generation != 2 {
		t.Fatalf("models = %+v, want the newer same-config refresh", models)
	}
}

// TestOMPModelListRejectsMalformedShapes keeps a null or an object without a
// models array from being parsed as a legal empty list.
func TestOMPModelListRejectsMalformedShapes(t *testing.T) {
	for _, raw := range []string{
		"null", "{}", `{"models":null}`, `{"models":"x"}`, `{"models":{}}`, "not json", "",
		`[null]`, `[{}]`, `[{"id":" "}]`, `[{"id":"m"}]`,
		`[{"id":"m","provider":"p"},{}]`,
		`{"provider":"p","models":[null]}`,
		`{"provider":"p","models":[{}]}`,
	} {
		if _, _, err := parseModelList([]byte(raw)); err == nil {
			t.Errorf("parseModelList(%q) succeeded, want an error", raw)
		}
	}
	for _, raw := range []string{"[]", `{"provider":"p","models":[]}`} {
		models, _, err := parseModelList([]byte(raw))
		if err != nil {
			t.Errorf("parseModelList(%q): %v", raw, err)
		}
		if len(models) != 0 {
			t.Errorf("parseModelList(%q) = %+v, want an empty list", raw, models)
		}
	}
	for _, raw := range []string{
		`{"provider":"p","models":[{"id":"m1","provider":"p"}]}`,
		`[{"id":"m1","provider":"p"}]`,
	} {
		models, provider, err := parseModelList([]byte(raw))
		if err != nil || len(models) != 1 || models[0].ID != "m1" || models[0].Provider != "p" {
			t.Fatalf("parseModelList(%s) = %+v, %q, %v", raw, models, provider, err)
		}
	}
}

type advancingClock struct {
	now time.Time
}

func (c *advancingClock) Now() time.Time { return c.now }

func (c *advancingClock) Advance(d time.Duration) { c.now = c.now.Add(d) }
