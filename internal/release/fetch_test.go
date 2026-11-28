package release

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/attempt"
)

// realAgentCLIArtifactBytes is the largest Agent CLI artifact seen in the
// test environment. Verification has to accept a payload this big, or the
// publish endpoint answers 422 before any Job can fetch it (V2-C20).
const realAgentCLIArtifactBytes = 304 << 20

// TestVerificationBoundCoversRealAgentCLIArtifacts holds the bound to the two
// numbers that decide whether it is usable: it must cover an artifact the
// deployment actually publishes, and it must not exceed the attempt budget
// that has to download the same bytes (V2-C11/C20). A bound above the budget
// would verify a release no Job can fetch.
func TestVerificationBoundCoversRealAgentCLIArtifacts(t *testing.T) {
	if MaxArtifactBytes < realAgentCLIArtifactBytes {
		t.Fatalf("verification bound = %d bytes, want at least %d: a real Agent CLI artifact would be refused", MaxArtifactBytes, realAgentCLIArtifactBytes)
	}
	if MaxArtifactBytes > attempt.DefaultCLIBudgetBytes {
		t.Fatalf("verification bound = %d bytes exceeds the attempt download budget %d: a publish could succeed for bytes no Job can fetch", MaxArtifactBytes, attempt.DefaultCLIBudgetBytes)
	}
}

// TestReadBoundedAcceptsAPayloadAboveTheOldBound is the regression the issue
// reports: the previous 64 MiB bound refused every real Agent CLI artifact.
func TestReadBoundedAcceptsAPayloadAboveTheOldBound(t *testing.T) {
	body := bytes.Repeat([]byte{0x1f}, 64<<20+1)
	got, err := readBounded(bytes.NewReader(body), int64(len(body)), MaxArtifactBytes)
	if err != nil {
		t.Fatalf("readBounded(%d bytes) = %v, want a verified payload", len(body), err)
	}
	if len(got) != len(body) {
		t.Fatalf("readBounded length = %d, want %d", len(got), len(body))
	}
}

// TestReadBoundedRejectsADeclaredOversizeBody: a source that declares a
// length over the bound is refused before its bytes are buffered.
func TestReadBoundedRejectsADeclaredOversizeBody(t *testing.T) {
	_, err := readBounded(bytes.NewReader(make([]byte, 11)), 11, 10)
	if err == nil || !strings.Contains(err.Error(), "verification bound") {
		t.Fatalf("readBounded error = %v, want the verification bound refusal", err)
	}
}

// TestReadBoundedRejectsAShortDeclaredBody: a body that ends before its
// declared length is a failed download, not a smaller artifact.
func TestReadBoundedRejectsAShortDeclaredBody(t *testing.T) {
	_, err := readBounded(bytes.NewReader(make([]byte, 5)), 10, 100)
	if err == nil || !strings.Contains(err.Error(), "declared length") {
		t.Fatalf("readBounded error = %v, want the short-body refusal", err)
	}
}

// TestReadBoundedRejectsAnUndeclaredOversizeBody: without a Content-Length the
// bound still applies, so a chunked source cannot stream past it.
func TestReadBoundedRejectsAnUndeclaredOversizeBody(t *testing.T) {
	_, err := readBounded(bytes.NewReader(make([]byte, 11)), -1, 10)
	if err == nil || !strings.Contains(err.Error(), "verification bound") {
		t.Fatalf("readBounded error = %v, want the verification bound refusal", err)
	}
}

// TestHTTPFetcherEnforcesTheBoundOnAChunkedBody drives the same rejection
// through the fetcher: a streamed response has no declared length, so the
// fallback path is what decides it.
func TestHTTPFetcherEnforcesTheBoundOnAChunkedBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, 2048))
		w.(http.Flusher).Flush()
	}))
	defer server.Close()

	_, err := NewHTTPFetcher().Fetch(context.Background(), server.URL, 1024)
	if err == nil || !strings.Contains(err.Error(), "verification bound") {
		t.Fatalf("Fetch error = %v, want the verification bound refusal", err)
	}
}

// TestHTTPFetcherAcceptsADeclaredLengthWithinTheBound: the fast path allocates
// exactly the declared length and succeeds.
func TestHTTPFetcherAcceptsADeclaredLengthWithinTheBound(t *testing.T) {
	body := bytes.Repeat([]byte{0x2a}, 4096)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	defer server.Close()

	got, err := NewHTTPFetcher().Fetch(context.Background(), server.URL, 4096)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("Fetch returned %d bytes, want the declared %d", len(got), len(body))
	}
}
