package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/release"
)

type fakeCLIVerifier struct {
	claims auth.Claims
	err    error
}

func (f fakeCLIVerifier) Verify(string, auth.RequestScope) (auth.Claims, error) {
	return f.claims, f.err
}

type fakePublishedRelease struct {
	kind     release.Kind
	adapter  string
	version  string
	platform string
	digest   string
}

type fakeCLIReleases struct {
	mu        sync.Mutex
	server    string
	version   string
	digests   map[string]string
	published []fakePublishedRelease
	body      []byte
	err       error
	block     chan struct{}
	reads     int
}

func (f *fakeCLIReleases) VerifiedRelease(_ context.Context, serverID string, kind release.Kind, adapter, version, platform string) (string, bool) {
	if serverID != f.server {
		return "", false
	}
	if kind == release.KindMultica && version == f.version {
		if digest, ok := f.digests[platform]; ok {
			return digest, true
		}
	}
	for _, published := range f.published {
		if published.kind != kind {
			continue
		}
		if kind == release.KindAgent && !strings.EqualFold(published.adapter, adapter) {
			continue
		}
		if published.version != version || !strings.EqualFold(published.platform, platform) {
			continue
		}
		return published.digest, true
	}
	return "", false
}

func (f *fakeCLIReleases) VerifiedArtifactByDomain(_ context.Context, serverID, digest string) ([]byte, error) {
	f.mu.Lock()
	f.reads++
	f.mu.Unlock()
	if f.block != nil {
		<-f.block
	}
	if serverID != f.server {
		return nil, errors.New("unknown artifact")
	}
	known := digest == f.digests["linux/amd64"]
	for _, published := range f.published {
		if published.digest == digest {
			known = true
		}
	}
	if !known {
		return nil, errors.New("unknown artifact")
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.body, nil
}

type fakeAttemptBindings struct {
	binding attempt.Binding
	found   bool
}

func (f fakeAttemptBindings) FrozenAttempt(sourceID, workspaceID, taskID string) (attempt.Binding, bool) {
	if !f.found || f.binding.TaskID != taskID ||
		f.binding.SourceID != sourceID || f.binding.WorkspaceID != workspaceID {
		return attempt.Binding{}, false
	}
	return f.binding, true
}

func frozenAttemptBinding(multicaDigest string) attempt.Binding {
	return attempt.Binding{
		TaskID: "task-1", SourceID: "source-1", WorkspaceID: "ws-1",
		CLIs: []attempt.CLI{
			{Kind: attempt.KindMultica, Version: "0.11.1", Platform: "linux/amd64", Digest: multicaDigest},
			{Kind: attempt.KindAgent, Backend: "codex", Version: "1.2.3", Platform: "linux/amd64",
				Digest: "sha256:" + strings.Repeat("c", 64)},
		},
	}
}

func TestCLIDistributionServesOnlyThePublishedFrozenArtifact(t *testing.T) {
	body := []byte("verified-bytes")
	digest := strings.Repeat("a", 64)
	source := &fakeCLIReleases{
		server: "source-1", version: "0.11.1",
		digests: map[string]string{"linux/amd64": digest}, body: body,
	}
	handler := &CLIDistribution{
		Verifier: fakeCLIVerifier{claims: auth.Claims{TaskID: "task-1", SourceID: "source-1", WorkspaceID: "ws-1"}},
		Releases: source,
		Attempts: fakeAttemptBindings{binding: frozenAttemptBinding("sha256:" + digest), found: true},
	}
	request := httptest.NewRequest(http.MethodGet,
		"/cli/multica/0.11.1/linux/amd64?digest=sha256:"+digest, nil)
	request.SetPathValue("kind", "multica")
	request.SetPathValue("version", "0.11.1")
	request.SetPathValue("os", "linux")
	request.SetPathValue("arch", "amd64")
	request.Header.Set("Authorization", "Bearer fmj_attempt")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if response.Body.String() != string(body) {
		t.Fatalf("body = %q, want the verified bytes", response.Body.String())
	}
	if got := response.Header().Get("X-CLI-Digest"); got != "sha256:"+digest {
		t.Fatalf("digest header = %q", got)
	}
}

func TestCLIDistributionRefusesUnfrozenRequests(t *testing.T) {
	digest := strings.Repeat("a", 64)
	base := func() (*CLIDistribution, *http.Request) {
		source := &fakeCLIReleases{
			server: "source-1", version: "0.11.1",
			digests: map[string]string{"linux/amd64": digest}, body: []byte("bytes"),
		}
		handler := &CLIDistribution{
			Verifier: fakeCLIVerifier{claims: auth.Claims{TaskID: "task-1", SourceID: "source-1", WorkspaceID: "ws-1"}},
			Releases: source,
			Attempts: fakeAttemptBindings{binding: frozenAttemptBinding("sha256:" + digest), found: true},
		}
		request := httptest.NewRequest(http.MethodGet,
			"/cli/multica/0.11.1/linux/amd64?digest=sha256:"+digest, nil)
		request.SetPathValue("kind", "multica")
		request.SetPathValue("version", "0.11.1")
		request.SetPathValue("os", "linux")
		request.SetPathValue("arch", "amd64")
		request.Header.Set("Authorization", "Bearer fmj_attempt")
		return handler, request
	}

	for _, tc := range []struct {
		name   string
		mutate func(*CLIDistribution, *http.Request)
		status int
	}{
		{name: "missing token", mutate: func(_ *CLIDistribution, r *http.Request) {
			r.Header.Del("Authorization")
		}, status: http.StatusUnauthorized},
		{name: "probe token", mutate: func(h *CLIDistribution, _ *http.Request) {
			h.Verifier = fakeCLIVerifier{claims: auth.Claims{SourceID: "source-1", Role: auth.RoleCatalogProbe}}
		}, status: http.StatusForbidden},
		{name: "another source", mutate: func(h *CLIDistribution, _ *http.Request) {
			h.Verifier = fakeCLIVerifier{claims: auth.Claims{TaskID: "task-1", SourceID: "source-2"}}
		}, status: http.StatusForbidden},
		{name: "neighbour version", mutate: func(_ *CLIDistribution, r *http.Request) {
			r.SetPathValue("version", "0.11.2")
			r.URL.RawQuery = "digest=sha256:" + digest
		}, status: http.StatusForbidden},
		{name: "neighbour digest", mutate: func(_ *CLIDistribution, r *http.Request) {
			r.URL.RawQuery = "digest=sha256:" + strings.Repeat("b", 64)
		}, status: http.StatusForbidden},
		{name: "another platform", mutate: func(_ *CLIDistribution, r *http.Request) {
			r.SetPathValue("arch", "arm64")
		}, status: http.StatusForbidden},
		{name: "unknown kind", mutate: func(_ *CLIDistribution, r *http.Request) {
			r.SetPathValue("kind", "omp")
		}, status: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, request := base()
			tc.mutate(handler, request)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("status = %d, want %d: %s", response.Code, tc.status, response.Body.String())
			}
			if source, ok := handler.Releases.(*fakeCLIReleases); ok {
				if source.reads != 0 {
					t.Fatalf("the artifact cache was read %d times for a refused request", source.reads)
				}
			}
		})
	}
}

func TestCLIDistributionRefusesPublishedArtifactsOutsideTheAttemptTuple(t *testing.T) {
	frozenMultica := strings.Repeat("a", 64)
	frozenAgent := strings.Repeat("b", 64)
	newerMultica := strings.Repeat("c", 64)
	otherAgent := strings.Repeat("d", 64)
	source := &fakeCLIReleases{
		server: "source-1", version: "0.11.1",
		digests: map[string]string{"linux/amd64": frozenMultica}, body: []byte("bytes"),
		published: []fakePublishedRelease{
			{kind: release.KindMultica, version: "0.11.1", platform: "linux/amd64", digest: frozenMultica},
			{kind: release.KindMultica, version: "0.11.2", platform: "linux/amd64", digest: newerMultica},
			{kind: release.KindAgent, adapter: "codex", version: "1.2.3", platform: "linux/amd64", digest: frozenAgent},
			{kind: release.KindAgent, adapter: "omp", version: "1.2.3", platform: "linux/amd64", digest: otherAgent},
		},
	}
	handler := &CLIDistribution{
		Verifier: fakeCLIVerifier{claims: auth.Claims{TaskID: "task-1", SourceID: "source-1", WorkspaceID: "ws-1"}},
		Releases: source,
		Attempts: fakeAttemptBindings{binding: attempt.Binding{
			TaskID: "task-1", SourceID: "source-1", WorkspaceID: "ws-1",
			CLIs: []attempt.CLI{
				{Kind: attempt.KindMultica, Version: "0.11.1", Platform: "linux/amd64", Digest: "sha256:" + frozenMultica},
				{Kind: attempt.KindAgent, Backend: "codex", Version: "1.2.3", Platform: "linux/amd64", Digest: "sha256:" + frozenAgent},
			},
		}, found: true},
	}
	for _, tc := range []struct {
		name    string
		kind    string
		version string
		backend string
		digest  string
	}{
		{name: "another published version", kind: "multica", version: "0.11.2", digest: newerMultica},
		{name: "another published backend", kind: "agent", version: "1.2.3", backend: "omp", digest: otherAgent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet,
				"/cli/"+tc.kind+"/"+tc.version+"/linux/amd64?digest=sha256:"+tc.digest+"&backend="+tc.backend, nil)
			request.SetPathValue("kind", tc.kind)
			request.SetPathValue("version", tc.version)
			request.SetPathValue("os", "linux")
			request.SetPathValue("arch", "amd64")
			request.Header.Set("Authorization", "Bearer fmj_attempt")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want 403: %s", response.Code, response.Body.String())
			}
			if !strings.Contains(response.Body.String(), "frozen CLI tuple") {
				t.Fatalf("body = %q, want the attempt tuple named", response.Body.String())
			}
			if source.reads != 0 {
				t.Fatalf("the published artifact was read %d times for an unauthorized tuple", source.reads)
			}
		})
	}
}

func TestCLIDownloadSaturationDoesNotBlockControlRequests(t *testing.T) {
	digest := strings.Repeat("a", 64)
	block := make(chan struct{})
	source := &fakeCLIReleases{
		server: "source-1", version: "0.11.1",
		digests: map[string]string{"linux/amd64": digest},
		body:    []byte("bytes"), block: block,
	}
	handler := &CLIDistribution{
		Verifier: fakeCLIVerifier{claims: auth.Claims{TaskID: "task-1", SourceID: "source-1", WorkspaceID: "ws-1"}},
		Releases: source,
		Attempts: fakeAttemptBindings{binding: frozenAttemptBinding("sha256:" + digest), found: true},
		Limiter:  NewDownloadLimiter(1),
	}
	request := httptest.NewRequest(http.MethodGet,
		"/cli/multica/0.11.1/linux/amd64?digest=sha256:"+digest, nil)
	request.SetPathValue("kind", "multica")
	request.SetPathValue("version", "0.11.1")
	request.SetPathValue("os", "linux")
	request.SetPathValue("arch", "amd64")
	request.Header.Set("Authorization", "Bearer fmj_attempt")

	first := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(first, request)
		close(done)
	}()
	// Wait until the first download owns the only slot.
	deadline := time.Now().Add(2 * time.Second)
	for {
		source.mu.Lock()
		reads := source.reads
		source.mu.Unlock()
		if reads == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the first download never started")
		}
		time.Sleep(time.Millisecond)
	}

	// A second download is refused on the download budget...
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, request)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second download status = %d, want 429", second.Code)
	}
	// ...while a control request on another handler still answers.
	control := httptest.NewRecorder()
	http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}).ServeHTTP(control, httptest.NewRequest(http.MethodPost, "/api/daemon/heartbeat", nil))
	if control.Code != http.StatusOK {
		t.Fatalf("control request status = %d, want 200 during a saturated download", control.Code)
	}
	close(block)
	<-done
	if first.Code != http.StatusOK {
		t.Fatalf("the first download status = %d, want 200", first.Code)
	}
}
