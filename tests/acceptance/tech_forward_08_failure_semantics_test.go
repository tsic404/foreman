// TC-tech-forward-08 (AC-90): S-29's failure semantics have two segments. A
// failure before the upstream's head (connect or transport) and an upstream 5xx
// answer 502 with the fixed error body; a break after the head has gone out
// reaches the caller as a truncated answer — upstream status, the bytes already
// produced, then a read error — and is never re-judged as 502. Neither shape
// retries: one caller request produces one upstream request.
package acceptance

import (
	"io"
	"net/http"
	"testing"
)

// forwardGet performs one GET probe and returns the answer head plus whatever
// body bytes were readable; a read error is the truncated form, so it is
// returned rather than fatal.
func forwardGet(t *testing.T, client *http.Client, url string, header http.Header) (int, []byte, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
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
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(resp.Body)
	return resp.StatusCode, raw, readErr
}

func TestTechForward08FailureSemantics(t *testing.T) {
	h := newHarness(t, false)
	header := http.Header{
		"Authorization": {"Bearer mat_tasktoken"},
		"User-Agent":    {"tc-forward-08/1.0"},
	}
	client := newForwardCaller(t)

	// No response head: the forward fails in the head phase, so the caller
	// gets the fixed 502 and the request was attempted once.
	code, body, err := forwardGet(t, client, h.jobsHTTP.URL+"/api/forward-probe-refused", header)
	if err != nil {
		t.Fatalf("head-phase failure answer: %v", err)
	}
	if code != http.StatusBadGateway || string(body) != `{"error":"upstream unavailable"}` {
		t.Errorf("head-phase failure = %d %s, want 502 with the fixed error body", code, body)
	}
	if calls := h.upstream.calls(http.MethodGet, "/api/forward-probe-refused"); len(calls) != 1 {
		t.Errorf("upstream calls for the head-phase failure = %d, want exactly one", len(calls))
	}

	// An upstream 5xx is the other head-phase failure: the caller gets 502, not
	// the upstream's own status or body.
	code, body, err = forwardGet(t, client, h.jobsHTTP.URL+"/api/forward-probe-5xx", header)
	if err != nil {
		t.Fatalf("upstream 5xx answer: %v", err)
	}
	if code != http.StatusBadGateway || string(body) != `{"error":"upstream unavailable"}` {
		t.Errorf("upstream 5xx = %d %s, want 502 with the fixed error body", code, body)
	}
	if calls := h.upstream.calls(http.MethodGet, "/api/forward-probe-5xx"); len(calls) != 1 {
		t.Errorf("upstream calls for the 5xx = %d, want exactly one", len(calls))
	}

	// The head is out and the body breaks: the caller sees the upstream's
	// status and the bytes it produced, then a read error. A complete-looking
	// answer or a 502 here would be the wrong shape.
	code, body, err = forwardGet(t, client, h.jobsHTTP.URL+"/api/forward-probe-truncated", header)
	if err == nil {
		t.Fatal("a truncated upstream body must surface as a read error, not a complete answer")
	}
	if code != http.StatusOK {
		t.Errorf("truncated answer status = %d, want the upstream's 200 (a 502 is a re-judgement)", code)
	}
	if string(body) != "short" {
		t.Errorf("truncated answer body = %q, want the bytes the upstream produced", body)
	}
	if calls := h.upstream.calls(http.MethodGet, "/api/forward-probe-truncated"); len(calls) != 1 {
		t.Errorf("upstream calls for the truncated answer = %d, want exactly one", len(calls))
	}
}
