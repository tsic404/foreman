package release

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testNow = time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)

type fakeFetcher struct {
	bodies map[string][]byte
	calls  int
}

func (f *fakeFetcher) Fetch(_ context.Context, rawURL string, _ int64) ([]byte, error) {
	f.calls++
	body, ok := f.bodies[rawURL]
	if !ok {
		return nil, fmt.Errorf("no fixture for %s", rawURL)
	}
	return body, nil
}

type fakeDirectory struct {
	source SourceDirectory
	ok     bool
}

func (d fakeDirectory) ReadSource(context.Context, string) (SourceDirectory, bool) {
	return d.source, d.ok
}

type fakeRuns struct {
	runs []RunFact
}

func (r fakeRuns) Running(context.Context) []RunFact { return r.runs }

func elfFor(arch string) []byte {
	body := make([]byte, 64)
	copy(body, []byte{0x7f, 'E', 'L', 'F', 2, 1, 1})
	machine := uint16(62)
	if arch == "arm64" {
		machine = 183
	}
	binary.LittleEndian.PutUint16(body[18:20], machine)
	return body
}

func digestOf(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func artifactForTest(platform Platform, url string, body []byte) Artifact {
	return Artifact{Platform: platform, URL: url, Digest: digestOf(body), Size: int64(len(body))}
}

func newTestService(t *testing.T, fetcher Fetcher, directory Directory, runs RunIndex) *Service {
	t.Helper()
	return newTestServiceAt(t, fetcher, directory, runs, func() time.Time { return testNow })
}

func newTestServiceAt(t *testing.T, fetcher Fetcher, directory Directory, runs RunIndex, clock func() time.Time) *Service {
	t.Helper()
	cache := NewMemoryArtifactCache()
	service, err := New(Config{
		Store:        NewMemoryStore(),
		Verifier:     Verifier{Fetcher: fetcher, Cache: cache},
		Artifacts:    cache,
		Directory:    directory,
		Runs:         runs,
		Clock:        clock,
		TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return service
}

func registerTestDefinition(t *testing.T, service *Service) DefinitionView {
	t.Helper()
	view, err := service.RegisterDefinition(context.Background(), Definition{
		ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
		RequiredPlatforms: []Platform{
			{OS: "linux", Arch: "amd64"},
			{OS: "linux", Arch: "arm64"},
		},
	}, "test")
	if err != nil {
		t.Fatalf("RegisterDefinition: %v", err)
	}
	return view
}

func addVersion(t *testing.T, service *Service, version string, artifacts []Artifact) DefinitionView {
	t.Helper()
	view, err := service.RegisterVersion(context.Background(), "omp", Candidate{Version: version, Artifacts: artifacts}, "test")
	if err != nil {
		t.Fatalf("RegisterVersion %s: %v", version, err)
	}
	return view
}

func publish(t *testing.T, service *Service, version string, generation uint64) PublishOutcome {
	t.Helper()
	view, _, err := service.Get(context.Background(), "omp")
	if err != nil {
		t.Fatalf("Get before Publish: %v", err)
	}
	outcome, err := service.Publish(context.Background(), "omp", version, generation, view.CurrentGeneration, "test")
	if err != nil {
		t.Fatalf("Publish %s: %v", version, err)
	}
	return outcome
}

func syncedDirectory() fakeDirectory {
	return fakeDirectory{
		ok: true,
		source: SourceDirectory{
			ServerID: "atlas", Synced: true,
			Workspaces: []DirectoryWorkspace{{ID: "core", Name: "core"}},
			Agents: []DirectoryAgent{
				{WorkspaceID: "core", AgentID: "agent-omp", AgentName: "omp dev", RuntimeID: "rt-omp", Adapter: "omp", Managed: true},
				{WorkspaceID: "core", AgentID: "agent-claude", AgentName: "claude dev", RuntimeID: "rt-claude", Adapter: "claude", Managed: true},
				{WorkspaceID: "core", AgentID: "agent-outside", AgentName: "outside", RuntimeID: "rt-outside", Adapter: "omp", Managed: false},
			},
		},
	}
}

// TestPublishRejectsWrongArchitectureAndKeepsPointer is CLI-06's platform
// negative: a file whose digest matches but whose ELF machine is amd64 cannot
// be published as the arm64 artifact, and the previous pointer stays current.
func TestPublishRejectsWrongArchitectureAndKeepsPointer(t *testing.T) {
	amd64 := elfFor("amd64")
	arm64 := elfFor("arm64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
		"https://mirror.test/omp-1-arm64": arm64,
		"https://mirror.test/omp-2-amd64": amd64,
		"https://mirror.test/omp-2-arm64": amd64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), nil)
	registerTestDefinition(t, service)
	addVersion(t, service, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
		artifactForTest(Platform{OS: "linux", Arch: "arm64"}, "https://mirror.test/omp-1-arm64", arm64),
	})
	if outcome := publish(t, service, "1.0.0", 1); outcome.Result != PublishPublished {
		t.Fatalf("first publish = %+v, want published", outcome)
	}

	addVersion(t, service, "2.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-2-amd64", amd64),
		artifactForTest(Platform{OS: "linux", Arch: "arm64"}, "https://mirror.test/omp-2-arm64", amd64),
	})
	outcome := publish(t, service, "2.0.0", 2)
	if outcome.Result != PublishFailed {
		t.Fatalf("bad-platform publish = %+v, want failed", outcome)
	}
	if !strings.Contains(strings.Join(outcome.Reasons, "; "), "artifact is x86-64, not aarch64") {
		t.Fatalf("reasons = %v, want the architecture mismatch", outcome.Reasons)
	}
	view, _, err := service.Get(context.Background(), "omp")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if view.CurrentVersion != "1.0.0" {
		t.Fatalf("current = %q, want the old pointer 1.0.0", view.CurrentVersion)
	}
}

// TestPublishRequiresEveryRequiredPlatform is CLI-06's missing-platform
// negative: a version with only amd64 is never promoted on a two-platform
// release line.
func TestPublishRequiresEveryRequiredPlatform(t *testing.T) {
	amd64 := elfFor("amd64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
		"https://mirror.test/omp-2-amd64": amd64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), nil)
	registerTestDefinition(t, service)
	addVersion(t, service, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
	})
	outcome := publish(t, service, "1.0.0", 1)
	if outcome.Result != PublishFailed {
		t.Fatalf("missing-platform publish = %+v, want failed", outcome)
	}
	if !strings.Contains(strings.Join(outcome.Reasons, "; "), "no artifact for linux/arm64") {
		t.Fatalf("reasons = %v, want the missing platform", outcome.Reasons)
	}
	view, _, _ := service.Get(context.Background(), "omp")
	if view.CurrentVersion != "" {
		t.Fatalf("current = %q, want no pointer", view.CurrentVersion)
	}
}

// TestVerifiedReleaseServesAnOlderFrozenPublish covers the CLI download face's
// authorization: an attempt that froze the previous release must still find
// its exact version and platform after a newer release becomes current
// (V2-C08/C09).
func TestVerifiedReleaseServesAnOlderFrozenPublish(t *testing.T) {
	amd64 := elfFor("amd64")
	arm64 := elfFor("arm64")
	amd64b := append(elfFor("amd64"), 0x01)
	arm64b := append(elfFor("arm64"), 0x01)
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
		"https://mirror.test/omp-1-arm64": arm64,
		"https://mirror.test/omp-2-amd64": amd64b,
		"https://mirror.test/omp-2-arm64": arm64b,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), nil)
	registerTestDefinition(t, service)
	addVersion(t, service, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
		artifactForTest(Platform{OS: "linux", Arch: "arm64"}, "https://mirror.test/omp-1-arm64", arm64),
	})
	if outcome := publish(t, service, "1.0.0", 1); outcome.Result != PublishPublished {
		t.Fatalf("publish 1.0.0 = %+v", outcome)
	}
	addVersion(t, service, "2.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-2-amd64", amd64b),
		artifactForTest(Platform{OS: "linux", Arch: "arm64"}, "https://mirror.test/omp-2-arm64", arm64b),
	})
	if outcome := publish(t, service, "2.0.0", 2); outcome.Result != PublishPublished {
		t.Fatalf("publish 2.0.0 = %+v", outcome)
	}

	if got, ok := service.VerifiedRelease(context.Background(), "atlas", KindAgent, "omp", "1.0.0", "linux/amd64"); !ok || got != digestOf(amd64) {
		t.Fatalf("older frozen release = (%q, %v), want %q", got, ok, digestOf(amd64))
	}
	for _, tc := range []struct {
		name, server, adapter, version, platform string
	}{
		{name: "unknown version", server: "atlas", adapter: "omp", version: "0.9.0", platform: "linux/amd64"},
		{name: "unknown platform", server: "atlas", adapter: "omp", version: "1.0.0", platform: "linux/riscv64"},
		{name: "other source", server: "other", adapter: "omp", version: "1.0.0", platform: "linux/amd64"},
		{name: "other adapter", server: "atlas", adapter: "claude", version: "1.0.0", platform: "linux/amd64"},
	} {
		if got, ok := service.VerifiedRelease(context.Background(), tc.server, KindAgent, tc.adapter, tc.version, tc.platform); ok {
			t.Errorf("%s: VerifiedRelease = %q, want no answer", tc.name, got)
		}
	}
}

// TestManualReleaseWorksWhileCheckingIsOff is the switch independence clause:
// the automatic check may be disabled, and a manual release of a registered
// candidate still publishes.
func TestManualReleaseWorksWhileCheckingIsOff(t *testing.T) {
	amd64 := elfFor("amd64")
	arm64 := elfFor("arm64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
		"https://mirror.test/omp-1-arm64": arm64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), nil)
	registerTestDefinition(t, service)
	addVersion(t, service, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
		artifactForTest(Platform{OS: "linux", Arch: "arm64"}, "https://mirror.test/omp-1-arm64", arm64),
	})
	view, err := service.UpdateSettings(context.Background(), "omp", Settings{AutoCheck: ptr(false), AutoRelease: ptr(false)}, "test")
	if err != nil {
		t.Fatalf("UpdateSettings: %v", err)
	}
	if view.AutoCheck || view.AutoRelease {
		t.Fatalf("settings = %+v, want both off", view)
	}
	if outcome := publish(t, service, "1.0.0", view.CheckGeneration); outcome.Result != PublishPublished {
		t.Fatalf("manual publish = %+v, want published", outcome)
	}
}

// TestAutoReleaseIsASeparateSwitch makes the other direction observable: a
// check with auto-release on publishes without a second manual call.
func TestAutoReleaseIsASeparateSwitch(t *testing.T) {
	amd64 := elfFor("amd64")
	arm64 := elfFor("arm64")
	manifest := fmt.Sprintf(`{"versions":[{"version":"1.0.0","artifacts":[
		{"platform":{"os":"linux","arch":"amd64"},"url":"https://mirror.test/omp-1-amd64","sha256":"%s","size":%d},
		{"platform":{"os":"linux","arch":"arm64"},"url":"https://mirror.test/omp-1-arm64","sha256":"%s","size":%d}
	]}]}`, digestOf(amd64), len(amd64), digestOf(arm64), len(arm64))
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/manifest.json": []byte(manifest),
		"https://mirror.test/omp-1-amd64":   amd64,
		"https://mirror.test/omp-1-arm64":   arm64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), nil)
	_, err := service.RegisterDefinition(context.Background(), Definition{
		ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
		RequiredPlatforms: []Platform{{OS: "linux", Arch: "amd64"}, {OS: "linux", Arch: "arm64"}},
		ManifestURL:       "https://mirror.test/manifest.json",
		AutoRelease:       true,
	}, "test")
	if err != nil {
		t.Fatalf("RegisterDefinition: %v", err)
	}
	report, err := service.Check(context.Background(), "omp", "test")
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if report.Result != "ok" || report.Publish == nil || report.Publish.Result != PublishPublished {
		t.Fatalf("check report = %+v, want a published auto release", report)
	}
	view, _, _ := service.Get(context.Background(), "omp")
	if view.CurrentVersion != "1.0.0" {
		t.Fatalf("current = %q, want 1.0.0", view.CurrentVersion)
	}
}

// TestRollbackPublishesAnotherVerifiedVersion: a rollback is a publish of the
// old exact version, and the in-flight preview still names the old runs.
func TestRollbackPublishesAnotherVerifiedVersion(t *testing.T) {
	amd64 := elfFor("amd64")
	arm64 := elfFor("arm64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
		"https://mirror.test/omp-1-arm64": arm64,
		"https://mirror.test/omp-2-amd64": amd64,
		"https://mirror.test/omp-2-arm64": arm64,
	}}
	clock := &stepClock{now: testNow}
	runs := fakeRuns{runs: []RunFact{{
		TaskID: "task-1", ServerID: "atlas", WorkspaceID: "core",
		AgentID: "agent-omp", AgentName: "omp dev", Adapter: "omp",
		AgentVersion: "1.0.0", AgentKnown: true,
		ConfigRevision: 3, ConfigKnown: true,
		StartedAt: testNow.Add(30 * time.Second),
	}}}
	service := newTestServiceAt(t, fetcher, syncedDirectory(), runs, clock.Now)
	registerTestDefinition(t, service)
	addVersion(t, service, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
		artifactForTest(Platform{OS: "linux", Arch: "arm64"}, "https://mirror.test/omp-1-arm64", arm64),
	})
	if outcome := publish(t, service, "1.0.0", 1); outcome.Result != PublishPublished {
		t.Fatalf("publish 1.0.0 = %+v", outcome)
	}
	clock.Advance(time.Minute)
	addVersion(t, service, "2.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-2-amd64", amd64),
		artifactForTest(Platform{OS: "linux", Arch: "arm64"}, "https://mirror.test/omp-2-arm64", arm64),
	})
	if outcome := publish(t, service, "2.0.0", 2); outcome.Result != PublishPublished {
		t.Fatalf("publish 2.0.0 = %+v", outcome)
	}
	clock.Advance(time.Minute)
	// The run started after 1.0.0 and before 2.0.0: the rollback preview must
	// keep showing it on 1.0.0.
	rollback := publish(t, service, "1.0.0", 2)
	if rollback.Result != PublishPublished {
		t.Fatalf("rollback = %+v, want published", rollback)
	}
	impact, err := service.Impact(context.Background(), ImpactRequest{CLIID: "omp", Version: "2.0.0", ExpectedGeneration: 2})
	if err != nil {
		t.Fatalf("Impact: %v", err)
	}
	if len(impact.InFlight) != 1 || impact.InFlight[0].Version != "1.0.0" ||
		!impact.InFlight[0].VersionKnown || impact.InFlight[0].ConfigRevision != 3 ||
		!impact.InFlight[0].ConfigRevisionKnown {
		t.Fatalf("in-flight = %+v, want the run frozen at 1.0.0", impact.InFlight)
	}
}

// TestImpactNamesNextStartAgentsAndUnknownScope is V2-C05's core: an Agent CLI
// preview names only managed Agents bound to that adapter, and a stale source
// is unknown rather than zero.
func TestImpactNamesNextStartAgentsAndUnknownScope(t *testing.T) {
	amd64 := elfFor("amd64")
	arm64 := elfFor("arm64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
		"https://mirror.test/omp-1-arm64": arm64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), fakeRuns{})
	registerTestDefinition(t, service)
	addVersion(t, service, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
		artifactForTest(Platform{OS: "linux", Arch: "arm64"}, "https://mirror.test/omp-1-arm64", arm64),
	})
	impact, err := service.Impact(context.Background(), ImpactRequest{CLIID: "omp", Version: "1.0.0", ExpectedGeneration: 1})
	if err != nil {
		t.Fatalf("Impact: %v", err)
	}
	if !impact.Known || impact.AffectedAgents == nil || *impact.AffectedAgents != 1 {
		t.Fatalf("impact = %+v, want exactly the managed omp agent", impact)
	}
	if len(impact.Agents) != 1 || impact.Agents[0].AgentID != "agent-omp" {
		t.Fatalf("agents = %+v, want only agent-omp", impact.Agents)
	}

	stale := syncedDirectory()
	stale.source.Stale = true
	stale.source.Detail = "credential revoked"
	staleService := newTestService(t, fetcher, stale, fakeRuns{})
	registerTestDefinition(t, staleService)
	addVersion(t, staleService, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
		artifactForTest(Platform{OS: "linux", Arch: "arm64"}, "https://mirror.test/omp-1-arm64", arm64),
	})
	unknown, err := staleService.Impact(context.Background(), ImpactRequest{CLIID: "omp", Version: "1.0.0", ExpectedGeneration: 1})
	if err != nil {
		t.Fatalf("stale Impact: %v", err)
	}
	if unknown.Known || unknown.AffectedAgents != nil {
		t.Fatalf("stale impact = %+v, want unknown with a null count", unknown)
	}
	if len(unknown.Unknown) == 0 {
		t.Fatal("stale impact carries no unknown reason")
	}
}

// TestMulticaCLIImpactCoversEveryManagedBackend: the Multica CLI line affects
// every managed runtime, while an Agent CLI line is adapter-scoped.
func TestMulticaCLIImpactCoversEveryManagedBackend(t *testing.T) {
	service := newTestService(t, &fakeFetcher{}, syncedDirectory(), fakeRuns{})
	if _, err := service.RegisterDefinition(context.Background(), Definition{
		ID: "multica", Kind: KindMultica, ServerID: "atlas",
		RequiredPlatforms: []Platform{{OS: "linux", Arch: "amd64"}},
	}, "test"); err != nil {
		t.Fatalf("RegisterDefinition: %v", err)
	}
	impact, err := service.Impact(context.Background(), ImpactRequest{CLIID: "multica"})
	if err != nil {
		t.Fatalf("Impact: %v", err)
	}
	if impact.AffectedAgents == nil || *impact.AffectedAgents != 2 {
		t.Fatalf("impact = %+v, want both managed agents", impact)
	}
}

func TestImpactRefusesAMovedCandidateSet(t *testing.T) {
	service := newTestService(t, &fakeFetcher{}, syncedDirectory(), fakeRuns{})
	registerTestDefinition(t, service)
	if _, err := service.Impact(context.Background(), ImpactRequest{CLIID: "omp", Version: "1.0.0", ExpectedGeneration: 9}); !errors.Is(err, ErrGenerationStale) {
		t.Fatalf("Impact error = %v, want ErrGenerationStale", err)
	}
}

func TestDefinitionViewCarriesNoArtifactBytes(t *testing.T) {
	service := newTestService(t, &fakeFetcher{}, syncedDirectory(), fakeRuns{})
	view := registerTestDefinition(t, service)
	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(raw), "secret") || strings.Contains(string(raw), "content") {
		t.Fatalf("definition view carries unexpected fields: %s", raw)
	}
}

// TestArtifactValidationRefusesUntrustedURLsAndDuplicatePlatforms is the
// controlled-source clause: a registration may not smuggle credentials in a
// URL, use another scheme, or declare two payloads for one platform.
func TestArtifactValidationRefusesUntrustedURLsAndDuplicatePlatforms(t *testing.T) {
	body := elfFor("amd64")
	digest := digestOf(body)
	valid := Artifact{
		Platform: Platform{OS: "linux", Arch: "amd64"},
		URL:      "https://mirror.test/omp",
		Digest:   digest,
		Size:     int64(len(body)),
	}
	service := newTestService(t, &fakeFetcher{}, syncedDirectory(), fakeRuns{})
	registerTestDefinition(t, service)
	for name, artifact := range map[string]Artifact{
		"userinfo": {
			Platform: valid.Platform, URL: "https://ops:not-a-real-secret@mirror.test/omp",
			Digest: digest, Size: valid.Size,
		},
		"scheme": {
			Platform: valid.Platform, URL: "ftp://mirror.test/omp",
			Digest: digest, Size: valid.Size,
		},
		"relative": {
			Platform: valid.Platform, URL: "/omp",
			Digest: digest, Size: valid.Size,
		},
	} {
		if _, err := service.RegisterVersion(context.Background(), "omp", Candidate{
			Version: "1.0.0", Artifacts: []Artifact{artifact},
		}, "test"); err == nil {
			t.Errorf("%s artifact was accepted", name)
		}
	}
	if _, err := service.RegisterVersion(context.Background(), "omp", Candidate{
		Version: "1.0.0", Artifacts: []Artifact{valid, valid},
	}, "test"); err == nil {
		t.Fatal("a duplicate platform artifact was accepted")
	}
}

// TestPublishRefusesAVerifiedVersionThatMissesARequiredPlatform covers the
// rollback path: the bytes were verified once, but a release line that now
// requires another platform must not publish that snapshot.
func TestPublishRefusesAVerifiedVersionThatMissesARequiredPlatform(t *testing.T) {
	body := elfFor("amd64")
	store := NewMemoryStore()
	if err := store.PutDefinition(context.Background(), Definition{
		ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
		RequiredPlatforms: []Platform{{OS: "linux", Arch: "amd64"}, {OS: "linux", Arch: "arm64"}},
		CheckGeneration:   1,
		Verified: map[string]VerifiedVersion{
			"1.0.0": {
				Artifacts: []Artifact{artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", body)},
			},
		},
	}); err != nil {
		t.Fatalf("PutDefinition: %v", err)
	}
	service, err := New(Config{
		Store: store, Directory: syncedDirectory(), Runs: fakeRuns{},
		Clock: func() time.Time { return testNow }, TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	outcome, err := service.Publish(context.Background(), "omp", "1.0.0", 1, 0, "test")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if outcome.Result != PublishFailed {
		t.Fatalf("outcome = %+v, want failed", outcome)
	}
	if !strings.Contains(strings.Join(outcome.Reasons, "; "), "no artifact for linux/arm64") {
		t.Fatalf("reasons = %v, want the missing arm64 artifact", outcome.Reasons)
	}
	view, _, _ := service.Get(context.Background(), "omp")
	if view.CurrentVersion != "" {
		t.Fatalf("current = %q, want no pointer", view.CurrentVersion)
	}
}

// TestAutoReleaseNeverRepublishesOrDowngrades keeps the automatic path an
// upgrade: a feed that still lists the current version, or lists only older
// versions, does not move the pointer.
func TestAutoReleaseNeverRepublishesOrDowngrades(t *testing.T) {
	amd64 := elfFor("amd64")
	arm64 := elfFor("arm64")
	manifestFor := func(version string) []byte {
		return []byte(fmt.Sprintf(`{"versions":[{"version":%q,"artifacts":[
			{"platform":{"os":"linux","arch":"amd64"},"url":"https://mirror.test/omp-amd64","sha256":"%s","size":%d},
			{"platform":{"os":"linux","arch":"arm64"},"url":"https://mirror.test/omp-arm64","sha256":"%s","size":%d}
		]}]}`, version, digestOf(amd64), len(amd64), digestOf(arm64), len(arm64)))
	}
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/manifest.json": manifestFor("1.0.0"),
		"https://mirror.test/omp-amd64":     amd64,
		"https://mirror.test/omp-arm64":     arm64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), fakeRuns{})
	if _, err := service.RegisterDefinition(context.Background(), Definition{
		ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
		RequiredPlatforms: []Platform{{OS: "linux", Arch: "amd64"}, {OS: "linux", Arch: "arm64"}},
		ManifestURL:       "https://mirror.test/manifest.json",
		AutoRelease:       true,
	}, "test"); err != nil {
		t.Fatalf("RegisterDefinition: %v", err)
	}
	first, err := service.Check(context.Background(), "omp", "test")
	if err != nil || first.Publish == nil || first.Publish.Result != PublishPublished {
		t.Fatalf("first check = %+v, %v; want published", first, err)
	}
	second, err := service.Check(context.Background(), "omp", "test")
	if err != nil {
		t.Fatalf("second Check: %v", err)
	}
	if second.Publish != nil || second.AutoRelease {
		t.Fatalf("second check = %+v, want no republish", second)
	}
	fetcher.bodies["https://mirror.test/manifest.json"] = manifestFor("0.9.0")
	third, err := service.Check(context.Background(), "omp", "test")
	if err != nil {
		t.Fatalf("third Check: %v", err)
	}
	if third.Publish != nil || third.AutoRelease {
		t.Fatalf("downgrade check = %+v, want no publish", third)
	}
	view, _, _ := service.Get(context.Background(), "omp")
	if view.CurrentVersion != "1.0.0" || len(view.Releases) != 1 {
		t.Fatalf("view = %+v, want one release at 1.0.0", view)
	}
}

// TestCheckPeriodSurvivesARestart keeps the operator's cadence in the
// persisted definition rather than only in process memory.
func TestCheckPeriodSurvivesARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cli-releases.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	service, err := New(Config{Store: store, Directory: syncedDirectory(), Clock: func() time.Time { return testNow }, TickInterval: time.Hour})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := service.RegisterDefinition(context.Background(), Definition{
		ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
		RequiredPlatforms:  []Platform{{OS: "linux", Arch: "amd64"}},
		AutoCheck:          true,
		CheckPeriodSeconds: 45,
	}, "test"); err != nil {
		t.Fatalf("RegisterDefinition: %v", err)
	}
	reloaded, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	restarted, err := New(Config{Store: reloaded, Directory: syncedDirectory(), Clock: func() time.Time { return testNow }, TickInterval: time.Hour})
	if err != nil {
		t.Fatalf("New restarted: %v", err)
	}
	view, ok, err := restarted.Get(context.Background(), "omp")
	if err != nil || !ok {
		t.Fatalf("Get: %v, %v", err, ok)
	}
	if view.CheckPeriodSeconds != 45 {
		t.Fatalf("check period = %d, want 45", view.CheckPeriodSeconds)
	}
}

// TestRepeatedPublishConverges is CLI-06's repeat-request clause: publishing
// the same verified version again keeps the same pointer and generation.
func TestRepeatedPublishConverges(t *testing.T) {
	amd64 := elfFor("amd64")
	arm64 := elfFor("arm64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
		"https://mirror.test/omp-1-arm64": arm64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), fakeRuns{})
	registerTestDefinition(t, service)
	addVersion(t, service, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
		artifactForTest(Platform{OS: "linux", Arch: "arm64"}, "https://mirror.test/omp-1-arm64", arm64),
	})
	first := publish(t, service, "1.0.0", 1)
	if first.Result != PublishPublished {
		t.Fatalf("first publish = %+v", first)
	}
	calls := fetcher.calls
	second := publish(t, service, "1.0.0", 1)
	if second.Result != PublishPublished || second.Generation != first.Generation {
		t.Fatalf("repeat publish = %+v, want the same pointer", second)
	}
	if fetcher.calls != calls {
		t.Fatalf("repeat publish refetched %d artifacts", fetcher.calls-calls)
	}
	view, _, _ := service.Get(context.Background(), "omp")
	if len(view.Releases) != 1 {
		t.Fatalf("releases = %d, want one", len(view.Releases))
	}
}

// TestManifestRejectsNullAndMissingVersions keeps a malformed feed from
// becoming a successful empty candidate set.
func TestManifestRejectsNullAndMissingVersions(t *testing.T) {
	for name, body := range map[string]string{
		"null":            "null",
		"object":          "{}",
		"null-versions":   `{"versions":null}`,
		"missing-version": `{"versions":[{"artifacts":[]}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			fetcher := &fakeFetcher{bodies: map[string][]byte{
				"https://mirror.test/manifest.json": []byte(body),
			}}
			service := newTestService(t, fetcher, syncedDirectory(), fakeRuns{})
			if _, err := service.RegisterDefinition(context.Background(), Definition{
				ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
				RequiredPlatforms: []Platform{{OS: "linux", Arch: "amd64"}},
				ManifestURL:       "https://mirror.test/manifest.json",
			}, "test"); err != nil {
				t.Fatalf("RegisterDefinition: %v", err)
			}
			report, err := service.Check(context.Background(), "omp", "test")
			if err != nil {
				t.Fatalf("Check: %v", err)
			}
			if report.Result != "failed" {
				t.Fatalf("report = %+v, want failed", report)
			}
		})
	}
}

func ptr[T any](value T) *T { return &value }

type stepClock struct {
	now time.Time
}

func (c *stepClock) Now() time.Time { return c.now }

func (c *stepClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

// TestPublishFromOnePreviewAllowsOneWinner is the release-generation CAS:
// two targets submitted against the same preview cannot both move the pointer.
func TestPublishFromOnePreviewAllowsOneWinner(t *testing.T) {
	amd64 := elfFor("amd64")
	arm64 := elfFor("arm64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
		"https://mirror.test/omp-1-arm64": arm64,
		"https://mirror.test/omp-2-amd64": amd64,
		"https://mirror.test/omp-2-arm64": arm64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), fakeRuns{})
	registerTestDefinition(t, service)
	addVersion(t, service, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
		artifactForTest(Platform{OS: "linux", Arch: "arm64"}, "https://mirror.test/omp-1-arm64", arm64),
	})
	addVersion(t, service, "2.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-2-amd64", amd64),
		artifactForTest(Platform{OS: "linux", Arch: "arm64"}, "https://mirror.test/omp-2-arm64", arm64),
	})
	view, _, err := service.Get(context.Background(), "omp")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	first, err := service.Publish(context.Background(), "omp", "1.0.0", view.CheckGeneration, 0, "test")
	if err != nil || first.Result != PublishPublished {
		t.Fatalf("first publish = %+v, %v", first, err)
	}
	second, err := service.Publish(context.Background(), "omp", "2.0.0", view.CheckGeneration, 0, "test")
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if second.Result != PublishConflict || second.KeptVersion != "1.0.0" {
		t.Fatalf("second publish = %+v, want conflict keeping 1.0.0", second)
	}
	current, _, _ := service.Get(context.Background(), "omp")
	if current.CurrentVersion != "1.0.0" {
		t.Fatalf("current = %q, want 1.0.0", current.CurrentVersion)
	}
}

// TestPublishRejectsAMovedReleasePointer covers a preview that outlived a
// different successful publish: the pointer generation, not only the
// candidate set, must still match.
func TestPublishRejectsAMovedReleasePointer(t *testing.T) {
	amd64 := elfFor("amd64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
		"https://mirror.test/omp-2-amd64": amd64,
		"https://mirror.test/omp-3-amd64": amd64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), fakeRuns{})
	if _, err := service.RegisterDefinition(context.Background(), Definition{
		ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
		RequiredPlatforms: []Platform{{OS: "linux", Arch: "amd64"}},
	}, "test"); err != nil {
		t.Fatalf("RegisterDefinition: %v", err)
	}
	for _, version := range []string{"1.0.0", "2.0.0", "3.0.0"} {
		addVersion(t, service, version, []Artifact{
			artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-"+version[:1]+"-amd64", amd64),
		})
	}
	view, _, _ := service.Get(context.Background(), "omp")
	first, err := service.Publish(context.Background(), "omp", "1.0.0", view.CheckGeneration, 0, "test")
	if err != nil || first.Result != PublishPublished {
		t.Fatalf("first publish = %+v, %v", first, err)
	}
	// This caller previewed release generation 0; another publish moved it.
	second, err := service.Publish(context.Background(), "omp", "2.0.0", view.CheckGeneration, 0, "test")
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if second.Result != PublishConflict || !containsReason(second.Reasons, ErrReleaseStale.Error()) {
		t.Fatalf("second publish = %+v, want release-stale conflict", second)
	}
	if current, _, _ := service.Get(context.Background(), "omp"); current.CurrentVersion != "1.0.0" {
		t.Fatalf("current = %q, want 1.0.0", current.CurrentVersion)
	}
}

func containsReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if strings.Contains(reason, want) {
			return true
		}
	}
	return false
}

// TestImpactUsesFrozenRunFactsNotHistory: a run's version and revision come
// from the claim's frozen facts, even when the release history has moved or
// the run started in a later publish window.
func TestImpactUsesFrozenRunFactsNotHistory(t *testing.T) {
	runs := fakeRuns{runs: []RunFact{{
		TaskID: "task-1", ServerID: "atlas", WorkspaceID: "core",
		AgentID: "agent-omp", AgentName: "omp dev", Adapter: "omp",
		AgentVersion: "1.0.0", AgentKnown: true,
		ConfigRevision: 4, ConfigKnown: true,
		StartedAt: testNow.Add(2 * time.Hour),
	}}}
	service := newTestService(t, &fakeFetcher{}, syncedDirectory(), runs)
	registerTestDefinition(t, service)
	impact, err := service.Impact(context.Background(), ImpactRequest{CLIID: "omp", Version: "9.9.9"})
	if err != nil {
		t.Fatalf("Impact: %v", err)
	}
	if len(impact.InFlight) != 1 {
		t.Fatalf("in-flight = %+v, want the frozen run", impact.InFlight)
	}
	run := impact.InFlight[0]
	if !run.VersionKnown || run.Version != "1.0.0" {
		t.Fatalf("run version = %+v, want the frozen 1.0.0", run)
	}
	if !run.ConfigRevisionKnown || run.ConfigRevision != 4 {
		t.Fatalf("run config = %+v, want the frozen revision 4", run)
	}
}

// TestImpactKeepsUnknownRunFactsVisible: a run whose claim recorded no
// release facts is still listed, with an unknown version/revision instead of
// being omitted or rendered as "no release".
func TestImpactKeepsUnknownRunFactsVisible(t *testing.T) {
	runs := fakeRuns{runs: []RunFact{{
		TaskID: "task-1", ServerID: "atlas", WorkspaceID: "core",
		AgentID: "agent-omp", AgentName: "omp dev", Adapter: "omp",
		StartedAt: testNow,
	}}}
	service := newTestService(t, &fakeFetcher{}, syncedDirectory(), runs)
	registerTestDefinition(t, service)
	impact, err := service.Impact(context.Background(), ImpactRequest{CLIID: "omp", Version: "2.0.0"})
	if err != nil {
		t.Fatalf("Impact: %v", err)
	}
	if len(impact.InFlight) != 1 {
		t.Fatalf("in-flight = %+v, want the unknown run kept visible", impact.InFlight)
	}
	if impact.InFlight[0].VersionKnown || impact.InFlight[0].ConfigRevisionKnown {
		t.Fatalf("run = %+v, want both facts unknown", impact.InFlight[0])
	}
}

// TestFileStoreWriteFailureKeepsPointer makes the atomicity promise
// observable: when the rename cannot land, the in-memory pointer and the
// document on disk both stay on the old release.
func TestFileStoreWriteFailureKeepsPointer(t *testing.T) {
	amd64 := elfFor("amd64")
	dir := t.TempDir()
	path := filepath.Join(dir, "releases.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	cache := NewMemoryArtifactCache()
	service, err := New(Config{
		Store:     store,
		Verifier:  Verifier{Fetcher: &fakeFetcher{bodies: map[string][]byte{"https://mirror.test/omp-1-amd64": amd64, "https://mirror.test/omp-2-amd64": amd64}}, Cache: cache},
		Artifacts: cache, Directory: syncedDirectory(), Runs: fakeRuns{},
		Clock: func() time.Time { return testNow }, TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := service.RegisterDefinition(context.Background(), Definition{
		ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
		RequiredPlatforms: []Platform{{OS: "linux", Arch: "amd64"}},
	}, "test"); err != nil {
		t.Fatalf("RegisterDefinition: %v", err)
	}
	for _, version := range []string{"1.0.0", "2.0.0"} {
		addVersion(t, service, version, []Artifact{
			artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-"+version[:1]+"-amd64", amd64),
		})
	}
	view, _, _ := service.Get(context.Background(), "omp")
	if outcome := publish(t, service, "1.0.0", view.CheckGeneration); outcome.Result != PublishPublished {
		t.Fatalf("first publish = %+v", outcome)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read registry: %v", err)
	}
	// A directory at the target path makes the final rename fail even for a
	// privileged test runner.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove registry: %v", err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("mkdir registry path: %v", err)
	}
	if _, err := service.Publish(context.Background(), "omp", "2.0.0", view.CheckGeneration, 1, "test"); err == nil {
		t.Fatal("publish succeeded although the registry rename could not land")
	}
	if current, _, _ := service.Get(context.Background(), "omp"); current.CurrentVersion != "1.0.0" {
		t.Fatalf("in-memory current = %q, want 1.0.0", current.CurrentVersion)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove blocking directory: %v", err)
	}
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("restore registry: %v", err)
	}
	reloaded, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	definition, ok, err := reloaded.GetDefinition(context.Background(), "omp")
	if err != nil || !ok {
		t.Fatalf("reloaded definition = %+v, %v", definition, err)
	}
	if definition.Current == nil || definition.Current.Version != "1.0.0" {
		t.Fatalf("persisted current = %+v, want 1.0.0", definition.Current)
	}
}

// TestVerifiedArtifactServedFromCacheAfterSourceRemoval: a published payload
// remains retrievable by its frozen digest after the upstream no longer
// serves it.
func TestVerifiedArtifactServedFromCacheAfterSourceRemoval(t *testing.T) {
	amd64 := elfFor("amd64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{"https://mirror.test/omp-1-amd64": amd64}}
	service := newTestService(t, fetcher, syncedDirectory(), fakeRuns{})
	if _, err := service.RegisterDefinition(context.Background(), Definition{
		ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
		RequiredPlatforms: []Platform{{OS: "linux", Arch: "amd64"}},
	}, "test"); err != nil {
		t.Fatalf("RegisterDefinition: %v", err)
	}
	addVersion(t, service, "1.0.0", []Artifact{
		artifactForTest(Platform{OS: "linux", Arch: "amd64"}, "https://mirror.test/omp-1-amd64", amd64),
	})
	view, _, _ := service.Get(context.Background(), "omp")
	if outcome := publish(t, service, "1.0.0", view.CheckGeneration); outcome.Result != PublishPublished {
		t.Fatalf("publish = %+v", outcome)
	}
	digest := digestOf(amd64)
	delete(fetcher.bodies, "https://mirror.test/omp-1-amd64")
	body, err := service.VerifiedArtifact(context.Background(), "omp", digest)
	if err != nil {
		t.Fatalf("VerifiedArtifact after source removal: %v", err)
	}
	if string(body) != string(amd64) {
		t.Fatal("cache returned different bytes than the verified payload")
	}
	byDomain, err := service.VerifiedArtifactByDomain(context.Background(), "atlas", digest)
	if err != nil || string(byDomain) != string(amd64) {
		t.Fatalf("VerifiedArtifactByDomain = %d bytes, %v", len(byDomain), err)
	}
}

// TestFailedVerificationNeverCaches: only a payload that passed digest, size
// and executable checks becomes visible to consumers.
func TestFailedVerificationNeverCaches(t *testing.T) {
	amd64 := elfFor("amd64")
	cache := NewMemoryArtifactCache()
	verifier := Verifier{Fetcher: &fakeFetcher{bodies: map[string][]byte{"https://mirror.test/a": amd64}}, Cache: cache}
	wrongDigest := Artifact{
		Platform: Platform{OS: "linux", Arch: "amd64"},
		URL:      "https://mirror.test/a", Digest: strings.Repeat("0", 64),
	}
	if verification := verifier.Verify(context.Background(), "atlas", wrongDigest); verification.OK {
		t.Fatal("a digest mismatch was accepted")
	}
	if _, hit, err := cache.Get(context.Background(), "atlas", wrongDigest.Digest); err != nil || hit {
		t.Fatalf("failed verification became visible: hit=%v err=%v", hit, err)
	}
	wrongArch := Artifact{
		Platform: Platform{OS: "linux", Arch: "arm64"},
		URL:      "https://mirror.test/a", Digest: digestOf(amd64), Size: int64(len(amd64)),
	}
	if verification := verifier.Verify(context.Background(), "atlas", wrongArch); verification.OK {
		t.Fatal("an amd64 payload registered as arm64 was accepted")
	}
	if _, hit, err := cache.Get(context.Background(), "atlas", wrongArch.Digest); err != nil || hit {
		t.Fatalf("wrong-architecture verification became visible: hit=%v err=%v", hit, err)
	}
}

// TestDamagedCacheEntryIsRefetchedBySameDigest: a damaged cache file is
// removed as a miss and the same digest is fetched again; a corrupt entry can
// never replace the registered payload.
func TestDamagedCacheEntryIsRefetchedBySameDigest(t *testing.T) {
	amd64 := elfFor("amd64")
	cache, err := NewFileArtifactCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileArtifactCache: %v", err)
	}
	digest := digestOf(amd64)
	fetcher := &fakeFetcher{bodies: map[string][]byte{"https://mirror.test/a": amd64}}
	service, err := New(Config{
		Store: NewMemoryStore(), Verifier: Verifier{Fetcher: fetcher, Cache: cache},
		Artifacts: cache, Directory: syncedDirectory(), Runs: fakeRuns{},
		Clock: func() time.Time { return testNow }, TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := service.RegisterDefinition(context.Background(), Definition{
		ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
		RequiredPlatforms: []Platform{{OS: "linux", Arch: "amd64"}},
	}, "test"); err != nil {
		t.Fatalf("RegisterDefinition: %v", err)
	}
	addVersion(t, service, "1.0.0", []Artifact{{
		Platform: Platform{OS: "linux", Arch: "amd64"},
		URL:      "https://mirror.test/a", Digest: digest, Size: int64(len(amd64)),
	}})
	view, _, _ := service.Get(context.Background(), "omp")
	if outcome := publish(t, service, "1.0.0", view.CheckGeneration); outcome.Result != PublishPublished {
		t.Fatalf("publish = %+v", outcome)
	}
	callsAfterPublish := fetcher.calls
	path := filepath.Join(cache.root, domainKey("atlas"), digest)
	if err := os.WriteFile(path, []byte("damaged"), 0o600); err != nil {
		t.Fatalf("damage cache entry: %v", err)
	}
	body, err := service.VerifiedArtifact(context.Background(), "omp", digest)
	if err != nil || string(body) != string(amd64) {
		t.Fatalf("refetch = %d bytes, %v; want the same digest", len(body), err)
	}
	if fetcher.calls <= callsAfterPublish {
		t.Fatal("the damaged entry was not refetched")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the refetched payload was not restored at its digest path: %v", err)
	}
}

// TestVerifiedArtifactSurvivesReleaseHistoryWindow: the bounded display
// history is not the asset-authorisation set. A digest still in the verified
// set stays readable after more than MaxReleases different payloads publish.
func TestVerifiedArtifactSurvivesReleaseHistoryWindow(t *testing.T) {
	first := elfFor("amd64")
	firstURL := "https://mirror.test/omp-window-0"
	fetcher := &fakeFetcher{bodies: map[string][]byte{firstURL: first}}
	cache := NewMemoryArtifactCache()
	service, err := New(Config{
		Store: NewMemoryStore(), Verifier: Verifier{Fetcher: fetcher, Cache: cache},
		Artifacts: cache, Directory: syncedDirectory(), Runs: fakeRuns{},
		Clock: func() time.Time { return testNow }, TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := service.RegisterDefinition(context.Background(), Definition{
		ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
		RequiredPlatforms: []Platform{{OS: "linux", Arch: "amd64"}},
	}, "test"); err != nil {
		t.Fatalf("RegisterDefinition: %v", err)
	}
	versions := make([]string, 0, MaxReleases+2)
	for i := 0; i < MaxReleases+2; i++ {
		body := append([]byte(nil), first...)
		body[20] = byte(i)
		url := fmt.Sprintf("https://mirror.test/omp-window-%d", i)
		fetcher.bodies[url] = body
		version := fmt.Sprintf("1.0.%d", i)
		addVersion(t, service, version, []Artifact{
			artifactForTest(Platform{OS: "linux", Arch: "amd64"}, url, body),
		})
		view, _, err := service.Get(context.Background(), "omp")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		outcome, err := service.Publish(context.Background(), "omp", version, view.CheckGeneration, view.CurrentGeneration, "test")
		if err != nil || outcome.Result != PublishPublished {
			t.Fatalf("publish %s = %+v, %v", version, outcome, err)
		}
		versions = append(versions, version)
	}
	view, _, err := service.Get(context.Background(), "omp")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(view.Releases) != MaxReleases {
		t.Fatalf("release history = %d, want the %d-entry window", len(view.Releases), MaxReleases)
	}
	for _, release := range view.Releases {
		if release.Version == versions[0] {
			t.Fatalf("version %s is still in the display window", versions[0])
		}
	}
	digest := digestOf(first)
	delete(fetcher.bodies, firstURL)
	body, err := service.VerifiedArtifact(context.Background(), "omp", digest)
	if err != nil || string(body) != string(first) {
		t.Fatalf("VerifiedArtifact after the window = %d bytes, %v", len(body), err)
	}
	byDomain, err := service.VerifiedArtifactByDomain(context.Background(), "atlas", digest)
	if err != nil || string(byDomain) != string(first) {
		t.Fatalf("VerifiedArtifactByDomain after the window = %d bytes, %v", len(byDomain), err)
	}
	if _, err := service.VerifiedArtifactByDomain(context.Background(), "harbor", digest); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("unrelated domain error = %v, want ErrArtifactNotFound", err)
	}
	if _, err := service.VerifiedArtifact(context.Background(), "omp", strings.Repeat("0", 64)); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("unverified digest error = %v, want ErrArtifactNotFound", err)
	}
}

// TestRetainedDigestRefetchAfterWindowUsesTheSameDigest: a damaged cache entry
// outside the display window is refetched by its own digest; removing the
// source makes the read fail explicitly instead of falling back elsewhere.
func TestRetainedDigestRefetchAfterWindowUsesTheSameDigest(t *testing.T) {
	first := elfFor("amd64")
	firstURL := "https://mirror.test/omp-retained-0"
	dir := t.TempDir()
	cache, err := NewFileArtifactCache(dir)
	if err != nil {
		t.Fatalf("NewFileArtifactCache: %v", err)
	}
	fetcher := &fakeFetcher{bodies: map[string][]byte{firstURL: first}}
	service, err := New(Config{
		Store: NewMemoryStore(), Verifier: Verifier{Fetcher: fetcher, Cache: cache},
		Artifacts: cache, Directory: syncedDirectory(), Runs: fakeRuns{},
		Clock: func() time.Time { return testNow }, TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := service.RegisterDefinition(context.Background(), Definition{
		ID: "omp", Kind: KindAgent, Adapter: "omp", ServerID: "atlas",
		RequiredPlatforms: []Platform{{OS: "linux", Arch: "amd64"}},
	}, "test"); err != nil {
		t.Fatalf("RegisterDefinition: %v", err)
	}
	for i := 0; i < MaxReleases+1; i++ {
		body := append([]byte(nil), first...)
		body[20] = byte(i)
		url := fmt.Sprintf("https://mirror.test/omp-retained-%d", i)
		fetcher.bodies[url] = body
		version := fmt.Sprintf("2.0.%d", i)
		addVersion(t, service, version, []Artifact{
			artifactForTest(Platform{OS: "linux", Arch: "amd64"}, url, body),
		})
		view, _, err := service.Get(context.Background(), "omp")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		outcome, err := service.Publish(context.Background(), "omp", version, view.CheckGeneration, view.CurrentGeneration, "test")
		if err != nil || outcome.Result != PublishPublished {
			t.Fatalf("publish %s = %+v, %v", version, outcome, err)
		}
	}
	digest := digestOf(first)
	path := filepath.Join(dir, domainKey("atlas"), digest)
	if err := os.WriteFile(path, []byte("damaged"), 0o600); err != nil {
		t.Fatalf("damage cache entry: %v", err)
	}
	callsBefore := fetcher.calls
	body, err := service.VerifiedArtifact(context.Background(), "omp", digest)
	if err != nil || string(body) != string(first) {
		t.Fatalf("refetch after window = %d bytes, %v", len(body), err)
	}
	if fetcher.calls != callsBefore+1 {
		t.Fatalf("refetch calls = %d, want one same-digest fetch", fetcher.calls-callsBefore)
	}
	delete(fetcher.bodies, firstURL)
	if err := os.WriteFile(path, []byte("damaged"), 0o600); err != nil {
		t.Fatalf("damage cache entry again: %v", err)
	}
	if _, err := service.VerifiedArtifact(context.Background(), "omp", digest); err == nil {
		t.Fatal("a damaged retained digest without its source was served")
	}
}
