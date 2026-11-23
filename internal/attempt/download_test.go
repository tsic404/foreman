package attempt

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func frozenDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func executableBody(marker string) []byte {
	return append([]byte("#!"+marker+"\n"), []byte("echo "+marker+"\n")...)
}

func testBinding(proxy string, multica, agent []byte) Binding {
	return Binding{
		AttemptID: "attempt-1", TaskID: "task-1", SourceID: "source-1",
		WorkspaceID: "ws-1", AgentID: "agent-1", DaemonID: "fm-task-1",
		JobRuntimeID: "runtime-1", EnvironmentID: "env-1",
		Endpoint: "env.example.test:7443", TLSRef: "ref/env-1",
		StateRoot: "/srv/rsbx/env-1", EnvironmentGen: "generation-1",
		SlotID: "slot-1", Image: "ghcr.io/tsic404/rsbx",
		ImageDigest: "sha256:" + strings.Repeat("a", 64),
		Platform:    "linux/amd64", ProxyURL: proxy,
		CLIs: []CLI{
			{Kind: KindMultica, Version: "0.11.1", Platform: "linux/amd64",
				Digest: frozenDigest(multica), DownloadURL: proxy + "/cli/multica/0.11.1/linux/amd64"},
			{Kind: KindAgent, Backend: "codex", Version: "1.2.3", Platform: "linux/amd64",
				Digest: frozenDigest(agent), DownloadURL: proxy + "/cli/agent/1.2.3/linux/amd64?backend=codex"},
		},
	}
}

func TestBindingValidateRequiresFrozenCarrierInputs(t *testing.T) {
	multica, agent := executableBody("multica"), executableBody("codex")
	valid := testBinding("http://foreman.example.test:8080", multica, agent)
	if err := valid.Validate(); err != nil {
		t.Fatalf("the frozen binding was refused: %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*Binding)
		want   string
	}{
		{name: "missing image", mutate: func(b *Binding) { b.Image = "" }, want: "image is required"},
		{name: "missing image digest", mutate: func(b *Binding) { b.ImageDigest = "" }, want: "image digest is required"},
		{name: "tagged image", mutate: func(b *Binding) { b.Image = "ghcr.io/tsic404/rsbx:latest" }, want: "without a tag"},
		{name: "missing platform", mutate: func(b *Binding) { b.Platform = "" }, want: "platform is required"},
		{name: "bad platform", mutate: func(b *Binding) { b.Platform = "linux" }, want: "not os/arch"},
		{name: "missing proxy", mutate: func(b *Binding) { b.ProxyURL = "" }, want: "proxy url is required"},
		{name: "relative proxy", mutate: func(b *Binding) { b.ProxyURL = "/proxy" }, want: "not an absolute address"},
		{name: "cli platform mismatch", mutate: func(b *Binding) { b.CLIs[0].Platform = "linux/arm64" }, want: "not the client platform"},
		{name: "movable CLI version", mutate: func(b *Binding) { b.CLIs[1].Version = "latest" }, want: "movable tag"},
		{name: "bad CLI digest", mutate: func(b *Binding) { b.CLIs[0].Digest = "sha256:short" }, want: "not a sha256 digest"},
		{name: "direct source URL", mutate: func(b *Binding) {
			b.CLIs[0].DownloadURL = "https://github.com/multica-ai/multica/releases/download/v0.11.1/multica"
		}, want: "not on the Foreman proxy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := valid
			binding.CLIs = append([]CLI(nil), valid.CLIs...)
			tc.mutate(&binding)
			err := binding.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestProxyCacheFetchesOnlyFromTheFrozenProxy(t *testing.T) {
	multica, agent := executableBody("multica"), executableBody("codex")
	var hits atomic.Int32
	var authorization []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		authorization = append(authorization, r.Header.Get("Authorization"))
		switch {
		case strings.HasPrefix(r.URL.Path, "/cli/multica/"):
			_, _ = w.Write(multica)
		case strings.HasPrefix(r.URL.Path, "/cli/agent/"):
			_, _ = w.Write(agent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	binding := testBinding(server.URL, multica, agent)
	dir := t.TempDir()
	cache := &ProxyCache{BaseURL: server.URL, Token: "fmj_attempt", Dir: dir, MaxBytes: DefaultCLIBudgetBytes}
	for _, cli := range binding.CLIs {
		artifact, err := cache.Ensure(context.Background(), cli)
		if err != nil {
			t.Fatalf("Ensure(%s): %v", cli.Kind, err)
		}
		if want := filepath.Join(dir, cli.Kind); artifact.Path != want {
			t.Fatalf("%s artifact path = %q, want the kind-named %q the adapters read", cli.Kind, artifact.Path, want)
		}
		body, err := os.ReadFile(artifact.Path)
		if err != nil {
			t.Fatalf("read %s artifact: %v", cli.Kind, err)
		}
		if len(body) == 0 || artifact.Digest != cli.Digest {
			t.Fatalf("%s artifact = %+v, want the frozen bytes", cli.Kind, artifact)
		}
		info, err := os.Stat(artifact.Path)
		if err != nil || info.Mode().Perm()&0o111 == 0 {
			t.Fatalf("%s artifact is not executable: %v", cli.Kind, err)
		}
	}
	if hits.Load() != 2 {
		t.Fatalf("proxy requests = %d, want exactly one per CLI", hits.Load())
	}
	for _, got := range authorization {
		if got != "Bearer fmj_attempt" {
			t.Fatalf("authorization header = %q, want the attempt token", got)
		}
	}
}

func TestProxyCacheReusesVerifiedBytesWithinTheAttempt(t *testing.T) {
	multica, agent := executableBody("multica"), executableBody("codex")
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if strings.Contains(r.URL.Path, "multica") {
			_, _ = w.Write(multica)
			return
		}
		_, _ = w.Write(agent)
	}))
	defer server.Close()

	binding := testBinding(server.URL, multica, agent)
	cache := &ProxyCache{BaseURL: server.URL, Dir: t.TempDir(), MaxBytes: DefaultCLIBudgetBytes}
	for i := 0; i < 2; i++ {
		if _, err := cache.Ensure(context.Background(), binding.CLIs[0]); err != nil {
			t.Fatalf("Ensure: %v", err)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("proxy requests = %d, want the verified bytes reused", hits.Load())
	}
}

func TestProxyCacheRefetchesCorruptedBytes(t *testing.T) {
	multica, agent := executableBody("multica"), executableBody("codex")
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write(multica)
	}))
	defer server.Close()

	binding := testBinding(server.URL, multica, agent)
	cache := &ProxyCache{BaseURL: server.URL, Dir: t.TempDir(), MaxBytes: DefaultCLIBudgetBytes}
	artifact, err := cache.Ensure(context.Background(), binding.CLIs[0])
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if err := os.WriteFile(artifact.Path, []byte("corrupted"), 0o755); err != nil {
		t.Fatalf("corrupt the cache: %v", err)
	}
	again, err := cache.Ensure(context.Background(), binding.CLIs[0])
	if err != nil {
		t.Fatalf("Ensure after corruption: %v", err)
	}
	if again.Digest != binding.CLIs[0].Digest || hits.Load() != 2 {
		t.Fatalf("requests = %d, artifact = %+v: the corrupted bytes were not refetched", hits.Load(), again)
	}
}

func TestProxyCacheRefusesAForeignDownloadAddress(t *testing.T) {
	multica, agent := executableBody("multica"), executableBody("codex")
	var foreignHits atomic.Int32
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		foreignHits.Add(1)
		_, _ = w.Write(multica)
	}))
	defer foreign.Close()

	binding := testBinding("http://proxy.example.test:8080", multica, agent)
	binding.CLIs[0].DownloadURL = foreign.URL + "/cli/multica/0.11.1/linux/amd64"
	cache := &ProxyCache{BaseURL: "http://proxy.example.test:8080", Dir: t.TempDir(), MaxBytes: DefaultCLIBudgetBytes}
	_, err := cache.Ensure(context.Background(), binding.CLIs[0])
	var download *DownloadError
	if !errors.As(err, &download) || download.Stage != "authorization" {
		t.Fatalf("Ensure error = %v, want an authorization refusal", err)
	}
	if foreignHits.Load() != 0 {
		t.Fatalf("a foreign source was contacted %d times", foreignHits.Load())
	}
}

func TestProxyCacheRefusesRedirectsWithoutContactingTheTarget(t *testing.T) {
	multica, agent := executableBody("multica"), executableBody("codex")
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		_, _ = w.Write(multica)
	}))
	defer target.Close()

	var proxyHits atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyHits.Add(1)
		http.Redirect(w, r, target.URL+"/cli/multica/0.11.1/linux/amd64", http.StatusFound)
	}))
	defer proxy.Close()

	binding := testBinding(proxy.URL, multica, agent)
	dir := t.TempDir()
	cache := &ProxyCache{
		BaseURL: proxy.URL, Dir: dir, MaxBytes: DefaultCLIBudgetBytes,
		Attempts: 3, RetryDelay: time.Millisecond,
	}
	_, err := cache.Ensure(context.Background(), binding.CLIs[0])
	var download *DownloadError
	if !errors.As(err, &download) || download.Stage != "download" {
		t.Fatalf("Ensure error = %v, want a preparation failure at the download stage", err)
	}
	if !strings.Contains(err.Error(), "302") || !strings.Contains(strings.ToLower(err.Error()), "redirect") {
		t.Fatalf("Ensure error = %v, want the redirect answer named", err)
	}
	if targetHits.Load() != 0 {
		t.Fatalf("the redirect target was contacted %d times", targetHits.Load())
	}
	if proxyHits.Load() != 1 {
		t.Fatalf("proxy requests = %d, want one non-retried attempt", proxyHits.Load())
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatalf("read the CLI dir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("bytes were kept after a redirect: %v", entries)
	}
}

func TestProxyCacheRefusesDigestMismatchWithoutKeepingBytes(t *testing.T) {
	multica, agent := executableBody("multica"), executableBody("codex")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(executableBody("tampered"))
	}))
	defer server.Close()

	binding := testBinding(server.URL, multica, agent)
	dir := t.TempDir()
	cache := &ProxyCache{BaseURL: server.URL, Dir: dir, MaxBytes: DefaultCLIBudgetBytes}
	_, err := cache.Ensure(context.Background(), binding.CLIs[0])
	var download *DownloadError
	if !errors.As(err, &download) || download.Stage != "verify" {
		t.Fatalf("Ensure error = %v, want a digest refusal", err)
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatalf("read the CLI dir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("a mismatched artifact was kept: %v", entries)
	}
}

// TestProxyCacheFailedDownloadStillSpendsTheBudget pins the retry path: bytes
// read before a verification failure count against the attempt, so a later
// helper on the same attempt gets the reduced remainder and cannot refetch.
func TestProxyCacheFailedDownloadStillSpendsTheBudget(t *testing.T) {
	served := executableBody("served")
	expected := executableBody("expected")
	other := executableBody("other")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/cli/multica/"):
			_, _ = w.Write(served)
		case strings.HasPrefix(r.URL.Path, "/cli/agent/"):
			_, _ = w.Write(other)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	binding := testBinding(server.URL, served, other)
	// The frozen digest names bytes the server does not serve: the download
	// completes, then fails verification. Those bytes are still spent.
	binding.CLIs[0].Digest = frozenDigest(expected)
	dir := t.TempDir()
	first := &ProxyCache{BaseURL: server.URL, Dir: dir, MaxBytes: int64(len(served))}
	if _, err := first.Ensure(context.Background(), binding.CLIs[0]); err == nil {
		t.Fatal("Ensure accepted a digest mismatch")
	}
	if first.Consumed() == 0 {
		t.Fatal("a failed verification did not count the bytes it read")
	}

	// A fresh helper on the same attempt sees the exhausted remainder, not a
	// new default budget.
	second := &ProxyCache{BaseURL: server.URL, Dir: dir, MaxBytes: 0}
	_, err := second.Ensure(context.Background(), binding.CLIs[1])
	var download *DownloadError
	if !errors.As(err, &download) || download.Stage != "budget" {
		t.Fatalf("Ensure after a failed download = %v, want a budget refusal", err)
	}
}

func TestProxyCacheRefusesNonExecutableBytes(t *testing.T) {
	multica, agent := executableBody("multica"), executableBody("codex")
	plain := []byte("not a program")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(plain)
	}))
	defer server.Close()

	binding := testBinding(server.URL, multica, agent)
	binding.CLIs[0].Digest = frozenDigest(plain)
	cache := &ProxyCache{BaseURL: server.URL, Dir: t.TempDir(), MaxBytes: DefaultCLIBudgetBytes}
	_, err := cache.Ensure(context.Background(), binding.CLIs[0])
	var download *DownloadError
	if !errors.As(err, &download) || download.Stage != "verify" {
		t.Fatalf("Ensure error = %v, want an executable-entry refusal", err)
	}
}

func TestProxyCacheBudgetIsSharedAcrossTheAttempt(t *testing.T) {
	multica, agent := executableBody("multica"), executableBody("codex")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "multica") {
			_, _ = w.Write(multica)
			return
		}
		_, _ = w.Write(agent)
	}))
	defer server.Close()

	binding := testBinding(server.URL, multica, agent)
	cache := &ProxyCache{BaseURL: server.URL, Dir: t.TempDir(), MaxBytes: int64(len(multica) + 1)}
	if _, err := cache.Ensure(context.Background(), binding.CLIs[0]); err != nil {
		t.Fatalf("the first artifact fits the budget: %v", err)
	}
	_, err := cache.Ensure(context.Background(), binding.CLIs[1])
	var download *DownloadError
	if !errors.As(err, &download) || download.Stage != "budget" {
		t.Fatalf("Ensure error = %v, want the shared budget refusal", err)
	}
}

func TestProxyCacheNamesTheFrozenArtifactOnFailure(t *testing.T) {
	multica, agent := executableBody("multica"), executableBody("codex")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "denied", http.StatusForbidden)
	}))
	defer server.Close()

	binding := testBinding(server.URL, multica, agent)
	cli := binding.CLIs[1]
	cache := &ProxyCache{BaseURL: server.URL, Dir: t.TempDir(), MaxBytes: DefaultCLIBudgetBytes}
	_, err := cache.Ensure(context.Background(), cli)
	var download *DownloadError
	if !errors.As(err, &download) {
		t.Fatalf("Ensure error = %v, want a named preparation failure", err)
	}
	for _, want := range []string{cli.Kind, cli.Version, cli.Platform, cli.Digest, "403"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
}

func TestProxyCacheRetriesOnlyTheSameFrozenArtifact(t *testing.T) {
	multica, agent := executableBody("multica"), executableBody("codex")
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			http.Error(w, "temporary", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(multica)
	}))
	defer server.Close()

	binding := testBinding(server.URL, multica, agent)
	cache := &ProxyCache{BaseURL: server.URL, Dir: t.TempDir(), MaxBytes: DefaultCLIBudgetBytes, Attempts: 2, RetryDelay: time.Millisecond}
	artifact, err := cache.Ensure(context.Background(), binding.CLIs[0])
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if hits.Load() != 2 || artifact.Digest != binding.CLIs[0].Digest {
		t.Fatalf("requests = %d, artifact = %+v, want the same frozen artifact retried", hits.Load(), artifact)
	}
}
