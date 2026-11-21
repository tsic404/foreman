package release

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Fetcher reads one artifact's bytes. The production implementation refuses
// any scheme but http(s), a URL with userinfo, and a body over the bound.
type Fetcher interface {
	Fetch(ctx context.Context, rawURL string, limit int64) ([]byte, error)
}

// HTTPFetcher is the controlled-source fetcher.
type HTTPFetcher struct {
	Client   *http.Client
	MaxBytes int64
}

// NewHTTPFetcher returns a fetcher with redirects capped and no userinfo
// accepted.
func NewHTTPFetcher() *HTTPFetcher {
	return &HTTPFetcher{
		Client: &http.Client{
			Timeout: 2 * time.Minute,
			CheckRedirect: func(request *http.Request, via []*http.Request) error {
				if len(via) >= 3 {
					return errors.New("artifact source redirects too many times")
				}
				if request.URL.Scheme != "http" && request.URL.Scheme != "https" {
					return errors.New("artifact source redirected to a non-http(s) URL")
				}
				if request.URL.User != nil {
					return errors.New("artifact source redirected to a URL with credentials")
				}
				return nil
			},
		},
		MaxBytes: MaxArtifactBytes,
	}
}

// Fetch downloads one artifact into memory.
func (f *HTTPFetcher) Fetch(ctx context.Context, rawURL string, limit int64) ([]byte, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Host == "" {
		return nil, errors.New("artifact url is not absolute")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.New("artifact url must be http(s)")
	}
	if parsed.User != nil {
		return nil, errors.New("artifact url must not carry credentials")
	}
	client := f.Client
	if client == nil {
		client = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("artifact source answered HTTP %d", response.StatusCode)
	}
	if limit <= 0 {
		limit = f.MaxBytes
	}
	if limit <= 0 {
		limit = MaxArtifactBytes
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("artifact exceeds the %d byte verification bound", limit)
	}
	return body, nil
}

// Verifier checks one artifact's digest, size and executable platform.
type Verifier struct {
	Fetcher  Fetcher
	MaxBytes int64
	// Cache retains bytes only after every verification check passed.
	Cache ArtifactCache
}

// Verify reads and judges one platform entry. A missing fetcher fails closed:
// an unverified artifact is never reported as verified.
func (v Verifier) Verify(ctx context.Context, domain string, artifact Artifact) Verification {
	verification, _ := v.VerifyBytes(ctx, domain, artifact)
	return verification
}

// VerifyBytes is Verify plus the verified payload, which is also the value
// retained in the cache. A cache write failure fails the verification: a
// release pointer must not name bytes no consumer can retrieve.
func (v Verifier) VerifyBytes(ctx context.Context, domain string, artifact Artifact) (Verification, []byte) {
	verification := Verification{Platform: artifact.Platform, VerifiedAt: time.Now().UTC()}
	if v.Fetcher == nil {
		verification.Reason = "no artifact fetcher is wired"
		return verification, nil
	}
	limit := v.MaxBytes
	if limit <= 0 {
		limit = MaxArtifactBytes
	}
	body, err := v.Fetcher.Fetch(ctx, artifact.URL, limit)
	if err != nil {
		verification.Reason = "download failed: " + err.Error()
		return verification, nil
	}
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	verification.Digest = digest
	verification.Size = int64(len(body))
	if !strings.EqualFold(digest, artifact.Digest) {
		verification.Reason = "sha256 does not match the registered digest"
		return verification, nil
	}
	if artifact.Size > 0 && int64(len(body)) != artifact.Size {
		verification.Reason = fmt.Sprintf("size is %d bytes, registered %d", len(body), artifact.Size)
		return verification, nil
	}
	machine, err := executableMachine(body, artifact.Platform)
	if err != nil {
		verification.Reason = err.Error()
		return verification, nil
	}
	verification.Machine = machine
	if v.Cache != nil {
		if err := v.Cache.Put(ctx, domain, digest, body); err != nil {
			verification.Reason = "verified artifact could not be cached: " + err.Error()
			return verification, nil
		}
	}
	verification.OK = true
	return verification, body
}

// executableMachine reads the platform header of the artifact. It accepts a
// Linux ELF and a Darwin Mach-O; any other container is refused rather than
// treated as "probably fine".
func executableMachine(body []byte, platform Platform) (string, error) {
	switch strings.ToLower(platform.OS) {
	case "linux":
		return elfMachine(body, platform.Arch)
	case "darwin":
		return machoMachine(body, platform.Arch)
	default:
		return "", fmt.Errorf("no platform inspector for %s", platform.OS)
	}
}

// elfMachine reads e_machine from an ELF header and compares it with the
// registered architecture. This is the check that keeps an amd64 file from
// being published as arm64.
func elfMachine(body []byte, arch string) (string, error) {
	if len(body) < 20 || body[0] != 0x7f || body[1] != 'E' || body[2] != 'L' || body[3] != 'F' {
		return "", errors.New("artifact is not an ELF executable")
	}
	var order binary.ByteOrder
	switch body[5] {
	case 1:
		order = binary.LittleEndian
	case 2:
		order = binary.BigEndian
	default:
		return "", errors.New("ELF data encoding is not readable")
	}
	if body[4] != 2 {
		return "", errors.New("artifact is not a 64-bit ELF executable")
	}
	machine := order.Uint16(body[18:20])
	name := elfMachineName(machine)
	if name == "" {
		return "", fmt.Errorf("ELF machine %d is not a supported architecture", machine)
	}
	want := map[string]string{"amd64": "x86-64", "arm64": "aarch64"}[strings.ToLower(arch)]
	if want == "" {
		return "", fmt.Errorf("architecture %q has no ELF mapping", arch)
	}
	if name != want {
		return name, fmt.Errorf("artifact is %s, not %s", name, want)
	}
	return name, nil
}

func elfMachineName(machine uint16) string {
	switch machine {
	case 62:
		return "x86-64"
	case 183:
		return "aarch64"
	case 3:
		return "x86"
	case 40:
		return "arm"
	default:
		return ""
	}
}

// machoMachine reads cputype from a little-endian 64-bit Mach-O header.
func machoMachine(body []byte, arch string) (string, error) {
	if len(body) < 8 {
		return "", errors.New("artifact is not a Mach-O executable")
	}
	if body[0] != 0xcf || body[1] != 0xfa || body[2] != 0xed || body[3] != 0xfe {
		return "", errors.New("artifact is not a 64-bit Mach-O executable")
	}
	machine := binary.LittleEndian.Uint32(body[4:8])
	name := ""
	switch machine {
	case 0x01000007:
		name = "x86-64"
	case 0x0100000c:
		name = "aarch64"
	default:
		return "", fmt.Errorf("Mach-O cpu type %#x is not a supported architecture", machine)
	}
	want := map[string]string{"amd64": "x86-64", "arm64": "aarch64"}[strings.ToLower(arch)]
	if want == "" {
		return "", fmt.Errorf("architecture %q has no Mach-O mapping", arch)
	}
	if name != want {
		return name, fmt.Errorf("artifact is %s, not %s", name, want)
	}
	return name, nil
}
