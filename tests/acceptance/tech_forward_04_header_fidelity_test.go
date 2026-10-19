// TC-tech-forward-04 (AC-33): the forwarded header set is verbatim. The same
// request travels directly and through Foreman; the upstream's view and the
// caller's view must match field for field, with same-name multi-values kept
// in order, the §2.4 exclusion face (hop-by-hop fields and whatever a
// Connection value names) carried on neither side, and no field added by the
// proxy.
package acceptance

import (
	"net/http"
	"slices"
	"testing"
)

func TestForward04HeaderSetVerbatim(t *testing.T) {
	h := newHarness(t, false)
	client := newForwardCaller(t)

	header := http.Header{
		"Authorization": {"Bearer mat_tasktoken"},
		"Content-Type":  {"application/json"},
		"Accept":        {"application/json"},
		"Cookie":        {"a=1", "b=2"},
		"X-Probe-Multi": {"a", "b"},
		"X-Probe-Dup":   {"one", "two"},
		"User-Agent":    {"tc-forward-04/1.0"},
		// The exclusion face: a hop-by-hop field and the field it names.
		"Connection": {"X-Hop"},
		"X-Hop":      {"drop-me"},
	}
	body := []byte(`{"x_future_field":true}`)
	const query = "b=2&a=1"

	directCode, directHeader, directBody := forwardCall(t, client,
		h.upstream.server.URL+"/api/forward-probe?"+query, header, body)
	proxiedCode, proxiedHeader, proxiedBody := forwardCall(t, client,
		h.jobsHTTP.URL+"/api/forward-probe?"+query, header, body)

	observations := h.upstream.forwardObservations()
	if len(observations) != 2 {
		t.Fatalf("upstream probes = %d, want the direct and the proxied run", len(observations))
	}
	direct, proxied := observations[0], observations[1]

	// Request side: the caller's set minus the exclusion face, values and
	// order included, and nothing added.
	forwardSameFields(t, "upstream request", forwardObservedFields(proxied.Header), forwardObservedFields(direct.Header))
	forwardSameFields(t, "forwarded request", forwardObservedFields(proxied.Header), forwardObservedFields(header))
	for _, excluded := range []string{"Connection", "X-Hop"} {
		if _, present := proxied.Header[excluded]; present {
			t.Errorf("the exclusion field %s reached the upstream", excluded)
		}
	}
	if _, present := proxied.Header["Accept-Encoding"]; present {
		t.Errorf("the proxy invented Accept-Encoding: %v", proxied.Header["Accept-Encoding"])
	}
	if got := proxied.Header.Values("X-Probe-Multi"); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("X-Probe-Multi = %v, want [a b] in order", got)
	}
	if got := proxied.Header.Values("Cookie"); !slices.Equal(got, []string{"a=1", "b=2"}) {
		t.Errorf("Cookie = %v, want [a=1 b=2] in order", got)
	}

	// Response side: the upstream's status, header set (multi-values in
	// order) and body reach the caller unchanged.
	if directCode != http.StatusCreated || proxiedCode != http.StatusCreated {
		t.Fatalf("answer = %d proxied, %d direct, want 201", proxiedCode, directCode)
	}
	forwardSameFields(t, "answer header", forwardAnswerFields(proxiedHeader), forwardAnswerFields(directHeader))
	if got := proxiedHeader.Values("X-Probe-Multi"); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("answer X-Probe-Multi = %v, want [a b] in order", got)
	}
	if got := proxiedHeader.Values("Set-Cookie"); !slices.Equal(got, []string{"a=1", "b=2"}) {
		t.Errorf("answer Set-Cookie = %v, want [a=1 b=2] in order", got)
	}
	if got := proxiedHeader.Get("X-Probe-Verdict"); got != "kept" {
		t.Errorf("answer X-Probe-Verdict = %q, want kept", got)
	}
	if string(proxiedBody) != string(directBody) || string(proxiedBody) != string(forwardProbeAnswer) {
		t.Errorf("answer body = %v, want the upstream's %v", proxiedBody, forwardProbeAnswer)
	}

	// One caller request, one upstream request.
	if len(h.upstream.forwardObservations()) != 2 {
		t.Errorf("upstream probes after both runs = %d, want 2", len(h.upstream.forwardObservations()))
	}
}
