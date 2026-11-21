package release

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func testAdmin(t *testing.T, service *Service, wide bool, ok bool) *AdminHandler {
	t.Helper()
	handler, err := NewAdminHandler(AdminConfig{
		Service: service,
		Authorize: func(*http.Request) (bool, bool) {
			return wide, ok
		},
		Actor: func(*http.Request) string { return "test-ops" },
	})
	if err != nil {
		t.Fatalf("NewAdminHandler: %v", err)
	}
	return handler
}

func serveAdmin(handler http.Handler, method, target, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	handler.ServeHTTP(rec, request)
	return rec
}

// TestReleaseAdminMapsPublishOutcomes pins the status an operator sees: a
// changed candidate set is a conflict, a failed verification is unprocessable,
// and neither answers 200.
func TestReleaseAdminMapsPublishOutcomes(t *testing.T) {
	amd64 := elfFor("amd64")
	arm64 := elfFor("arm64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{
		"https://mirror.test/omp-1-amd64": amd64,
		"https://mirror.test/omp-1-arm64": amd64,
		"https://mirror.test/omp-2-amd64": amd64,
		"https://mirror.test/omp-2-arm64": arm64,
	}}
	service := newTestService(t, fetcher, syncedDirectory(), fakeRuns{})
	handler := testAdmin(t, service, true, true)

	register := serveAdmin(handler, http.MethodPost, "/admin/cli", `{
		"cli_id":"omp","kind":"agent","adapter":"omp","server_id":"atlas",
		"required_platforms":[{"os":"linux","arch":"amd64"},{"os":"linux","arch":"arm64"}],
		"auto_check":false,"auto_release":false
	}`)
	if register.Code != http.StatusCreated {
		t.Fatalf("register = %d %s", register.Code, register.Body.String())
	}
	version := serveAdmin(handler, http.MethodPost, "/admin/cli/omp/versions", `{
		"version":"1.0.0","artifacts":[
			{"platform":{"os":"linux","arch":"amd64"},"url":"https://mirror.test/omp-1-amd64","sha256":"`+digestOf(amd64)+`","size":`+strconv.Itoa(len(amd64))+`},
			{"platform":{"os":"linux","arch":"arm64"},"url":"https://mirror.test/omp-1-arm64","sha256":"`+digestOf(amd64)+`","size":`+strconv.Itoa(len(amd64))+`}
		]}`)
	if version.Code != http.StatusCreated {
		t.Fatalf("version = %d %s", version.Code, version.Body.String())
	}
	failed := serveAdmin(handler, http.MethodPost, "/admin/cli/omp/release", `{"version":"1.0.0","expected_generation":1}`)
	if failed.Code != http.StatusUnprocessableEntity {
		t.Fatalf("bad-platform release = %d %s, want 422", failed.Code, failed.Body.String())
	}
	var outcome PublishOutcome
	if err := json.Unmarshal(failed.Body.Bytes(), &outcome); err != nil {
		t.Fatalf("decode outcome: %v", err)
	}
	if outcome.Result != PublishFailed || outcome.KeptVersion != "" {
		t.Fatalf("outcome = %+v, want failed with no pointer", outcome)
	}

	conflict := serveAdmin(handler, http.MethodPost, "/admin/cli/omp/release", `{"version":"1.0.0","expected_generation":99}`)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("stale release = %d %s, want 409", conflict.Code, conflict.Body.String())
	}
	staleImpact := serveAdmin(handler, http.MethodGet, "/admin/cli/omp/impact?version=1.0.0&generation=99", "")
	if staleImpact.Code != http.StatusConflict {
		t.Fatalf("stale impact = %d %s, want 409", staleImpact.Code, staleImpact.Body.String())
	}
	audit := serveAdmin(handler, http.MethodGet, "/admin/cli-audit", "")
	if audit.Code != http.StatusOK {
		t.Fatalf("audit = %d %s, want 200", audit.Code, audit.Body.String())
	}
}

// TestReleaseAdminRequiresAWideCredential keeps a workspace-bound credential
// out of a release domain that spans every workspace of one source.
func TestReleaseAdminRequiresAWideCredential(t *testing.T) {
	service := newTestService(t, &fakeFetcher{}, syncedDirectory(), fakeRuns{})
	for name, handler := range map[string]*AdminHandler{
		"unauthenticated": testAdmin(t, service, false, false),
		"scoped":          testAdmin(t, service, false, true),
	} {
		rec := serveAdmin(handler, http.MethodGet, "/admin/cli", "")
		want := http.StatusForbidden
		if name == "unauthenticated" {
			want = http.StatusUnauthorized
		}
		if rec.Code != want {
			t.Errorf("%s = %d, want %d", name, rec.Code, want)
		}
	}
}

// TestReleaseAdminServesVerifiedArtifactBytes keeps the cache consumable over
// the management surface: the published digest is addressable, and a digest
// that was never published is a 404.
func TestReleaseAdminServesVerifiedArtifactBytes(t *testing.T) {
	amd64 := elfFor("amd64")
	fetcher := &fakeFetcher{bodies: map[string][]byte{"https://mirror.test/omp-1-amd64": amd64}}
	service := newTestService(t, fetcher, syncedDirectory(), fakeRuns{})
	handler := testAdmin(t, service, true, true)
	register := serveAdmin(handler, http.MethodPost, "/admin/cli", `{
		"cli_id":"omp","kind":"agent","adapter":"omp","server_id":"atlas",
		"required_platforms":[{"os":"linux","arch":"amd64"}],
		"auto_check":false,"auto_release":false
	}`)
	if register.Code != http.StatusCreated {
		t.Fatalf("register = %d %s", register.Code, register.Body.String())
	}
	version := serveAdmin(handler, http.MethodPost, "/admin/cli/omp/versions", `{
		"version":"1.0.0","artifacts":[
			{"platform":{"os":"linux","arch":"amd64"},"url":"https://mirror.test/omp-1-amd64","sha256":"`+digestOf(amd64)+`","size":`+strconv.Itoa(len(amd64))+`}
		]}`)
	if version.Code != http.StatusCreated {
		t.Fatalf("version = %d %s", version.Code, version.Body.String())
	}
	published := serveAdmin(handler, http.MethodPost, "/admin/cli/omp/release", `{"version":"1.0.0","expected_generation":1,"expected_release_generation":0}`)
	if published.Code != http.StatusOK {
		t.Fatalf("publish = %d %s", published.Code, published.Body.String())
	}
	digest := digestOf(amd64)
	artifact := serveAdmin(handler, http.MethodGet, "/admin/cli/omp/artifacts/"+digest, "")
	if artifact.Code != http.StatusOK || string(artifact.Body.Bytes()) != string(amd64) {
		t.Fatalf("artifact = %d, %d bytes", artifact.Code, artifact.Body.Len())
	}
	missing := serveAdmin(handler, http.MethodGet, "/admin/cli/omp/artifacts/"+strings.Repeat("0", 64), "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing artifact = %d, want 404", missing.Code)
	}
}
