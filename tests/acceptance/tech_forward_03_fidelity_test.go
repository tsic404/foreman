// TC-tech-forward-03: 透传保真——非 JSON body 往返等价 + 响应面等价（直连对照）。
//
// The same request travels two ways: straight to the upstream stub, and through
// Foreman carrying an agent credential. The upstream's view of it (method,
// path, query, body, header set modulo the §2.4 exclusion face) and the
// caller's view of the answer (status, header set, bytes) must be
// indistinguishable.
//
// Payload size is part of the criterion. A body past any proxy-side buffer must
// travel whole in both directions, and an answer must start flowing before the
// upstream finishes writing it: a passthrough that buffers would cap what an
// agent can upload or download and would delay every large answer.
package acceptance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// newForwardCaller is the bare caller both paths use: no compression
// negotiation and no default user agent of its own, so the two runs send
// exactly the probe's header set.
func newForwardCaller(t *testing.T) *http.Client {
	t.Helper()
	tr := &http.Transport{DisableCompression: true}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr}
}

// forwardCall performs one probe request with an explicit header set.
func forwardCall(t *testing.T, client *http.Client, url string, header http.Header, body []byte) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	for k, vv := range header {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return resp.StatusCode, resp.Header, raw
}

// forwardExcludedRequestFields is the §2.4 exclusion face as the direct run
// sees it: hop-by-hop fields, the target authority, and the forwarded body's
// length are the proxy's own connection, so the two runs are compared without
// them.
var forwardExcludedRequestFields = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
	"Host":                true,
	"Content-Length":      true,
}

// forwardObservedFields applies the exclusion face to an observed header set,
// including the fields a Connection value names.
func forwardObservedFields(h http.Header) http.Header {
	named := map[string]bool{}
	for _, v := range h.Values("Connection") {
		for _, f := range strings.Split(v, ",") {
			if f = http.CanonicalHeaderKey(strings.TrimSpace(f)); f != "" {
				named[f] = true
			}
		}
	}
	out := make(http.Header, len(h))
	for k, vv := range h {
		if forwardExcludedRequestFields[k] || named[k] {
			continue
		}
		out[k] = append([]string(nil), vv...)
	}
	return out
}

// forwardAnswerFields drops the answer fields that describe the proxy's own
// connection (framing, Date) rather than the upstream's promise.
func forwardAnswerFields(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, vv := range h {
		switch k {
		case "Date", "Content-Length", "Transfer-Encoding", "Connection", "Keep-Alive", "Trailer":
			continue
		}
		out[k] = append([]string(nil), vv...)
	}
	return out
}

// forwardSameFields asserts two header sets are equal field by field, values
// and their order included.
func forwardSameFields(t *testing.T, label string, got, want http.Header) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s fields = %v, want %v", label, got, want)
	}
	for k, wantVals := range want {
		if gotVals := got.Values(k); !slices.Equal(gotVals, wantVals) {
			t.Errorf("%s %s = %v, want %v", label, k, gotVals, wantVals)
		}
	}
}

// The direct comparison: a non-JSON upload reaches the upstream exactly as it
// would have arrived without Foreman, and the non-JSON verdict comes back
// unchanged.
func TestTechForward03DirectAndProxiedAreIndistinguishable(t *testing.T) {
	h := newHarness(t, false)
	client := newForwardCaller(t)

	const boundary = "----tc03probe"
	body := []byte("--" + boundary + "\r\n" +
		"Content-Disposition: form-data; name=\"f\"; filename=\"p.bin\"\r\n" +
		"Content-Type: application/octet-stream\r\n\r\n\x00\x01\xfe\xff\r\n" +
		"--" + boundary + "--\r\n")
	header := http.Header{
		"Authorization": {"Bearer mat_tasktoken"},
		"Content-Type":  {"multipart/form-data; boundary=" + boundary},
		"Accept":        {"application/octet-stream"},
		"Cookie":        {"a=1", "b=2"},
		"User-Agent":    {"tc-forward-03/1.0"},
		"X-Probe-Multi": {"a", "b"},
	}
	const query = "b=2&a=1&z=%2F"

	directCode, directHeader, directBody := forwardCall(t, client,
		h.upstream.server.URL+"/api/forward-probe?"+query, header, body)
	proxiedCode, proxiedHeader, proxiedBody := forwardCall(t, client,
		h.jobsHTTP.URL+"/api/forward-probe?"+query, header, body)

	observations := h.upstream.forwardObservations()
	if len(observations) != 2 {
		t.Fatalf("upstream probes = %d, want the direct and the proxied run", len(observations))
	}
	direct, proxied := observations[0], observations[1]
	if direct.Method != proxied.Method || direct.Path != proxied.Path || direct.RawQuery != proxied.RawQuery {
		t.Errorf("upstream saw %s %s?%s proxied, %s %s?%s direct",
			proxied.Method, proxied.Path, proxied.RawQuery, direct.Method, direct.Path, direct.RawQuery)
	}
	wantSHA := sha256.Sum256(body)
	if proxied.BodyLen != int64(len(body)) || proxied.BodySHA != hex.EncodeToString(wantSHA[:]) {
		t.Errorf("upstream body proxied = %d bytes (%s), want the caller's %d bytes",
			proxied.BodyLen, proxied.BodySHA, len(body))
	}
	if direct.BodyLen != proxied.BodyLen || direct.BodySHA != proxied.BodySHA {
		t.Errorf("upstream body direct = %d (%s), proxied = %d (%s)",
			direct.BodyLen, direct.BodySHA, proxied.BodyLen, proxied.BodySHA)
	}
	forwardSameFields(t, "upstream request", forwardObservedFields(proxied.Header), forwardObservedFields(direct.Header))
	// And the forwarded set is exactly the caller's: the proxy added nothing.
	forwardSameFields(t, "forwarded header", forwardObservedFields(proxied.Header), header)

	if directCode != http.StatusCreated || proxiedCode != http.StatusCreated {
		t.Fatalf("answer = %d proxied, %d direct, want 201", proxiedCode, directCode)
	}
	if !bytes.Equal(proxiedBody, forwardProbeAnswer) || !bytes.Equal(directBody, proxiedBody) {
		t.Errorf("answer body proxied = %v, direct = %v, want %v", proxiedBody, directBody, forwardProbeAnswer)
	}
	forwardSameFields(t, "answer header", forwardAnswerFields(proxiedHeader), forwardAnswerFields(directHeader))
}

// Payloads past any proxy-side buffer: the upload reaches the upstream whole
// and the download reaches the caller whole. A cap would show up as a short
// body, a wrong status or a transport failure.
func TestTechForward03PayloadPastAnyProxyBuffer(t *testing.T) {
	h := newHarness(t, false)
	client := newForwardCaller(t)
	wantSHA := forwardDigest(t, forwardProbeBulkBytes)

	upload, err := http.NewRequest(http.MethodPost, h.jobsHTTP.URL+"/api/forward-probe?kind=bulk", &forwardPattern{})
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	upload.ContentLength = forwardProbeBulkBytes
	upload.Header.Set("Authorization", "Bearer mat_tasktoken")
	upload.Header.Set("Content-Type", "application/octet-stream")
	upload.Header.Set("User-Agent", "tc-forward-03/1.0")
	uploadResp, err := client.Do(upload)
	if err != nil {
		t.Fatalf("bulk upload: %v", err)
	}
	defer uploadResp.Body.Close()
	raw, err := io.ReadAll(uploadResp.Body)
	if err != nil {
		t.Fatalf("bulk upload answer: %v", err)
	}
	if uploadResp.StatusCode != http.StatusCreated || !bytes.Equal(raw, forwardProbeAnswer) {
		t.Fatalf("bulk upload = %d %v, want the upstream verdict", uploadResp.StatusCode, raw)
	}
	observations := h.upstream.forwardObservations()
	if len(observations) != 1 {
		t.Fatalf("upstream probes = %d, want the bulk upload", len(observations))
	}
	if observations[0].BodyLen != forwardProbeBulkBytes || observations[0].BodySHA != wantSHA {
		t.Errorf("upstream saw %d bytes (%s), want %d (%s)",
			observations[0].BodyLen, observations[0].BodySHA, forwardProbeBulkBytes, wantSHA)
	}

	download, err := http.NewRequest(http.MethodGet, h.jobsHTTP.URL+"/api/forward-probe-bulk", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	download.Header.Set("Authorization", "Bearer mat_tasktoken")
	download.Header.Set("User-Agent", "tc-forward-03/1.0")
	downloadResp, err := client.Do(download)
	if err != nil {
		t.Fatalf("bulk download: %v", err)
	}
	defer downloadResp.Body.Close()
	if downloadResp.StatusCode != http.StatusOK || downloadResp.ContentLength != forwardProbeBulkBytes {
		t.Errorf("bulk download = %d with length %d, want 200 with %d",
			downloadResp.StatusCode, downloadResp.ContentLength, forwardProbeBulkBytes)
	}
	sum := sha256.New()
	read, err := io.Copy(sum, downloadResp.Body)
	if err != nil {
		t.Fatalf("bulk download body: %v", err)
	}
	if read != forwardProbeBulkBytes || hex.EncodeToString(sum.Sum(nil)) != wantSHA {
		t.Errorf("bulk download body = %d bytes (%s), want %d (%s)",
			read, hex.EncodeToString(sum.Sum(nil)), forwardProbeBulkBytes, wantSHA)
	}
}

// The answer starts flowing before the upstream finishes writing it: the caller
// reads the upstream's first chunk while the rest is still gated. A buffered
// passthrough would deliver nothing until the gate opens.
func TestTechForward03AnswerFlowsBeforeTheUpstreamFinishes(t *testing.T) {
	h := newHarness(t, false)
	client := newForwardCaller(t)
	firstChunkOut, release := h.upstream.forwardStreamGate()
	defer release()

	req, err := http.NewRequest(http.MethodGet, h.jobsHTTP.URL+"/api/forward-probe-stream", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer mat_tasktoken")
	req.Header.Set("User-Agent", "tc-forward-03/1.0")
	// Do runs in its own goroutine: a passthrough that buffers the whole
	// answer never sends the head, so Do would otherwise block until the
	// upstream gate is released and the test would surface as a timeout
	// instead of the assertion below.
	type answer struct {
		resp *http.Response
		err  error
	}
	answered := make(chan answer, 1)
	go func() {
		resp, err := client.Do(req)
		answered <- answer{resp: resp, err: err}
	}()
	var resp *http.Response
	select {
	case got := <-answered:
		if got.err != nil {
			t.Fatalf("streamed probe: %v", got.err)
		}
		resp = got.resp
	case <-time.After(3 * time.Second):
		// Let the upstream finish so the blocked Do can return; the buffered
		// channel keeps the goroutine from leaking.
		release()
		t.Fatal("the caller saw no answer head while the upstream was still writing: the answer is buffered")
	}
	defer resp.Body.Close()
	select {
	case <-firstChunkOut:
	case <-time.After(3 * time.Second):
		t.Fatal("the upstream never wrote its first chunk")
	}

	const firstChunkBytes = 16 << 10
	head := make([]byte, firstChunkBytes)
	read := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(resp.Body, head)
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatalf("read the first chunk: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the caller saw nothing while the upstream was still writing: the answer is buffered")
	}
	if !bytes.Equal(head, bytes.Repeat([]byte("f"), firstChunkBytes)) {
		t.Errorf("first chunk = %d bytes, want the upstream's", len(head))
	}

	release()
	rest, err := io.ReadAll(resp.Body)
	if err != nil || string(rest) != "-last" {
		t.Errorf("rest of the answer = %q, %v; want -last", rest, err)
	}
}
