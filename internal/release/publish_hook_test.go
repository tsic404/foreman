package release

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// singlePlatformDefinition registers an Agent definition whose only required
// platform keeps the publish fixtures small.
func singlePlatformDefinition(t *testing.T, service *Service) {
	t.Helper()
	if _, err := service.RegisterDefinition(context.Background(), Definition{
		ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
		RequiredPlatforms: []Platform{{OS: "linux", Arch: "amd64"}},
	}, "test"); err != nil {
		t.Fatalf("RegisterDefinition: %v", err)
	}
}

// TestPublishHookNamesTheDomainAfterThePointerSwitches: the control plane
// refreshes an unserved source's declaration on publication, so the hook must
// run after the pointer moved and name the release domain. It must also run
// outside the service mutex, because it re-reads the domain it just wrote.
func TestPublishHookNamesTheDomainAfterThePointerSwitches(t *testing.T) {
	amd64 := elfFor("amd64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), fakeRuns{})
	singlePlatformDefinition(t, service)
	addVersion(t, service, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
	})

	var domain atomic.Value
	var seenVersion atomic.Value
	var hookBlocked atomic.Bool
	service.SetPublishHook(func(serverID string) {
		domain.Store(serverID)
		done := make(chan struct{})
		go func() {
			defer close(done)
			view, _, err := service.Get(context.Background(), "omp")
			if err == nil {
				seenVersion.Store(view.CurrentVersion)
			}
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			hookBlocked.Store(true)
		}
	})

	view, _, err := service.Get(context.Background(), "omp")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	outcome, err := service.Publish(context.Background(), "omp", "1.0.0", view.CheckGeneration, view.CurrentGeneration, "test")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if outcome.Result != PublishPublished {
		t.Fatalf("publish = %+v, want published", outcome)
	}
	if hookBlocked.Load() {
		t.Fatal("the publish hook re-read the service while it was still holding the lock: the control plane would stall on every publish")
	}
	if got, _ := domain.Load().(string); got != "atlas" {
		t.Fatalf("hook domain = %q, want atlas", got)
	}
	if got, _ := seenVersion.Load().(string); got != "1.0.0" {
		t.Fatalf("hook saw current version = %q, want the published 1.0.0", got)
	}
}

// TestPublishHookDoesNotRunOnAConflict: a publish that kept the old pointer
// changed nothing, so there is nothing to re-declare.
func TestPublishHookDoesNotRunOnAConflict(t *testing.T) {
	amd64 := elfFor("amd64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), fakeRuns{})
	singlePlatformDefinition(t, service)
	addVersion(t, service, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
	})
	var calls atomic.Int32
	service.SetPublishHook(func(string) { calls.Add(1) })

	view, _, err := service.Get(context.Background(), "omp")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	outcome, err := service.Publish(context.Background(), "omp", "1.0.0", view.CheckGeneration, 7, "test")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if outcome.Result == PublishPublished {
		t.Fatalf("publish = %+v, want a stale-release conflict", outcome)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("hook calls = %d, want none for a publish that kept the pointer", got)
	}
}

// TestPublishHookRunsOnAConvergedRepublish: the pointer already names the
// requested version, but the declaration it implies may never have landed. A
// repeat publication is the operator's retry, so it must reach the hook
// instead of succeeding as an unrecoverable no-op.
func TestPublishHookRunsOnAConvergedRepublish(t *testing.T) {
	amd64 := elfFor("amd64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), fakeRuns{})
	singlePlatformDefinition(t, service)
	addVersion(t, service, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
	})
	view, _, err := service.Get(context.Background(), "omp")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if outcome := publish(t, service, "1.0.0", view.CheckGeneration); outcome.Result != PublishPublished {
		t.Fatalf("first publish = %+v, want published", outcome)
	}

	var domain atomic.Value
	service.SetPublishHook(func(serverID string) { domain.Store(serverID) })
	publish(t, service, "1.0.0", view.CheckGeneration)
	if got, _ := domain.Load().(string); got != "atlas" {
		t.Fatalf("converged republish hook domain = %q, want atlas", got)
	}
}
