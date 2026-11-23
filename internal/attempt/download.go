package attempt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ProxyCache fetches one frozen CLI artifact from the Foreman proxy and keeps
// the verified bytes for the rest of this attempt. A new Job starts with an
// empty directory, so every attempt performs its own proxy fetch (V2-C09).
// There is no direct-source or preinstalled-CLI path: the only address this
// type accepts is the attempt's frozen proxy origin.
type ProxyCache struct {
	// Client defaults to a client with a bounded per-request timeout.
	Client *http.Client
	// BaseURL is the frozen Foreman proxy origin.
	BaseURL string
	// Token is the attempt's Job token. It never travels in a URL.
	Token string
	// Dir holds the verified bytes for this attempt.
	Dir string
	// MaxBytes bounds all downloads of this attempt, retries included.
	MaxBytes int64
	// Attempts is the number of same-artifact attempts on a transient failure.
	// Zero means one.
	Attempts int
	// RetryDelay is the first backoff between transient attempts.
	RetryDelay time.Duration

	mu   sync.Mutex
	used int64
}

// DownloadError names the failed preparation step. Its fields are exactly what
// V2-C11 requires the Job log to show; the reason carries no credential.
type DownloadError struct {
	Kind     string
	Backend  string
	Version  string
	Platform string
	Digest   string
	Stage    string
	Reason   string
}

func (e *DownloadError) Error() string {
	return fmt.Sprintf("%s CLI %s/%s %s preparation failed at %s: %s",
		e.Kind, e.Version, e.Platform, e.Digest, e.Stage, e.Reason)
}

// Ensure makes exactly one frozen artifact available at the kind-named path
// the deployment adapters read (<dir>/multica, <dir>/agent). A cached file is
// reused only when its bytes still match the frozen digest.
func (c *ProxyCache) Ensure(ctx context.Context, cli CLI) (Artifact, error) {
	if err := c.validate(cli); err != nil {
		return Artifact{}, err
	}
	path := c.artifactPath(cli)
	if artifact, ok := c.cached(path, cli); ok {
		return artifact, nil
	}
	body, err := c.fetch(ctx, cli, path)
	if err != nil {
		return Artifact{}, err
	}
	return artifactFor(cli, path, body), nil
}

func (c *ProxyCache) validate(cli CLI) error {
	proxy, err := url.Parse(strings.TrimSpace(c.BaseURL))
	if err != nil || proxy.Scheme == "" || proxy.Host == "" {
		return &DownloadError{Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
			Platform: cli.Platform, Digest: cli.Digest, Stage: "proxy", Reason: "the Foreman proxy address is not absolute"}
	}
	download, err := url.Parse(strings.TrimSpace(cli.DownloadURL))
	if err != nil || download.Scheme == "" || download.Host == "" {
		return &DownloadError{Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
			Platform: cli.Platform, Digest: cli.Digest, Stage: "address", Reason: "the frozen download address is not absolute"}
	}
	if download.Scheme != proxy.Scheme || !strings.EqualFold(download.Host, proxy.Host) {
		return &DownloadError{Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
			Platform: cli.Platform, Digest: cli.Digest, Stage: "authorization", Reason: "the download address is not on the Foreman proxy"}
	}
	if !ValidDigest(cli.Digest) {
		return &DownloadError{Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
			Platform: cli.Platform, Digest: cli.Digest, Stage: "input", Reason: "the frozen digest is not a sha256 digest"}
	}
	if strings.TrimSpace(c.Dir) == "" {
		return &DownloadError{Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
			Platform: cli.Platform, Digest: cli.Digest, Stage: "prepare", Reason: "the CLI directory is not configured"}
	}
	return nil
}

func (c *ProxyCache) artifactPath(cli CLI) string {
	return filepath.Join(c.Dir, sanitizeName(cli.Kind))
}

func sanitizeName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func (c *ProxyCache) cached(path string, cli CLI) (Artifact, bool) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return Artifact{}, false
	}
	body, err := os.ReadFile(path)
	if err != nil || DigestHex(digestOf(body)) != DigestHex(cli.Digest) || !looksExecutable(body) {
		_ = os.Remove(path)
		return Artifact{}, false
	}
	return artifactFor(cli, path, body), true
}

func (c *ProxyCache) fetch(ctx context.Context, cli CLI, path string) ([]byte, error) {
	if err := os.MkdirAll(c.Dir, 0o755); err != nil {
		return nil, c.failure(cli, "prepare", "create the CLI directory: "+err.Error())
	}
	attempts := c.Attempts
	if attempts <= 0 {
		attempts = 1
	}
	delay := c.RetryDelay
	if delay <= 0 {
		delay = 500 * time.Millisecond
	}
	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		body, transient, err := c.once(ctx, cli)
		if err == nil {
			if err := writeVerified(path, body, cli); err != nil {
				return nil, err
			}
			return body, nil
		}
		last = err
		if !transient || ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		if delay < 4*time.Second {
			delay *= 2
		}
	}
	return nil, last
}

func (c *ProxyCache) once(ctx context.Context, cli CLI) ([]byte, bool, error) {
	client := c.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	} else {
		clone := *client
		client = &clone
	}
	// A followed redirect would leave the frozen Foreman proxy for a host the
	// attempt never authorized. Refusing every hop keeps the fetch on the
	// address Binding.Validate accepted (V2-C09/C11).
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, cli.DownloadURL, nil)
	if err != nil {
		return nil, false, c.failure(cli, "request", "build the download request: "+err.Error())
	}
	request.Header.Set("Accept", "application/octet-stream")
	if strings.TrimSpace(c.Token) != "" {
		request.Header.Set("Authorization", "Bearer "+c.Token)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, true, c.failure(cli, "download", "the Foreman proxy is unreachable")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= http.StatusMultipleChoices && response.StatusCode < http.StatusBadRequest {
		return nil, false, c.failure(cli, "download",
			fmt.Sprintf("the Foreman proxy answered %d with a redirect; redirects are not followed", response.StatusCode))
	}
	if response.StatusCode != http.StatusOK {
		transient := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		return nil, transient, c.failure(cli, "download", fmt.Sprintf("the Foreman proxy answered %d", response.StatusCode))
	}
	body, err := readBounded(response.Body, cli, c)
	if err != nil {
		return nil, false, err
	}
	if got := DigestHex(digestOf(body)); got != DigestHex(cli.Digest) {
		return nil, false, c.failure(cli, "verify", fmt.Sprintf("the artifact digest %s is not the frozen %s", got, DigestHex(cli.Digest)))
	}
	return body, false, nil
}

func readBounded(body io.Reader, cli CLI, c *ProxyCache) ([]byte, error) {
	var out bytes.Buffer
	buffer := make([]byte, 64<<10)
	for {
		n, err := body.Read(buffer)
		if n > 0 {
			if budgetErr := c.consume(cli, int64(n)); budgetErr != nil {
				return nil, budgetErr
			}
			out.Write(buffer[:n])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, c.failure(cli, "download", "the artifact stream ended before it was complete")
		}
	}
	if out.Len() == 0 {
		return nil, c.failure(cli, "verify", "the artifact is empty")
	}
	return out.Bytes(), nil
}

func (c *ProxyCache) consume(cli CLI, n int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	limit := c.MaxBytes
	if limit <= 0 {
		limit = DefaultCLIBudgetBytes
	}
	if c.used+n > limit {
		return c.failure(cli, "budget", fmt.Sprintf("the attempt's CLI download budget of %d bytes is exhausted", limit))
	}
	c.used += n
	return nil
}

func writeVerified(path string, body []byte, cli CLI) error {
	if !looksExecutable(body) {
		return &DownloadError{Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
			Platform: cli.Platform, Digest: cli.Digest, Stage: "verify", Reason: "the artifact has no executable entry"}
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".cli-*")
	if err != nil {
		return &DownloadError{Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
			Platform: cli.Platform, Digest: cli.Digest, Stage: "prepare", Reason: "create the artifact file: " + err.Error()}
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if err := tmp.Chmod(0o755); err != nil {
		_ = tmp.Close()
		return &DownloadError{Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
			Platform: cli.Platform, Digest: cli.Digest, Stage: "prepare", Reason: "mark the artifact executable: " + err.Error()}
	}
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		return &DownloadError{Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
			Platform: cli.Platform, Digest: cli.Digest, Stage: "prepare", Reason: "write the artifact: " + err.Error()}
	}
	if err := tmp.Close(); err != nil {
		return &DownloadError{Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
			Platform: cli.Platform, Digest: cli.Digest, Stage: "prepare", Reason: "close the artifact: " + err.Error()}
	}
	if err := os.Rename(name, path); err != nil {
		return &DownloadError{Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
			Platform: cli.Platform, Digest: cli.Digest, Stage: "prepare", Reason: "publish the artifact: " + err.Error()}
	}
	return nil
}

func looksExecutable(body []byte) bool {
	if len(body) >= 4 && body[0] == 0x7f && body[1] == 'E' && body[2] == 'L' && body[3] == 'F' {
		return true
	}
	return len(body) >= 2 && body[0] == '#' && body[1] == '!'
}

func (c *ProxyCache) failure(cli CLI, stage, reason string) *DownloadError {
	return &DownloadError{
		Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
		Platform: cli.Platform, Digest: cli.Digest, Stage: stage, Reason: reason,
	}
}

func artifactFor(cli CLI, path string, body []byte) Artifact {
	return Artifact{
		Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
		Platform: cli.Platform, Digest: cli.Digest, Path: path,
	}
}

func digestOf(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}
