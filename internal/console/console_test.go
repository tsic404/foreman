package console_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/console"
)

// serve drives one request through the console handler.
func serve(t *testing.T, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	console.Handler().ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

// TestConsoleServesTheShellAndItsModules pins the entry a browser opens: the
// document at /admin/ and every file it references under /admin/assets/,
// with the type and the hardening headers each needs.
func TestConsoleServesTheShellAndItsModules(t *testing.T) {
	page := serve(t, http.MethodGet, "/admin/")
	if page.Code != http.StatusOK {
		t.Fatalf("GET /admin/ = %d, want 200", page.Code)
	}
	if got := page.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Errorf("the console document answers %q, want text/html", got)
	}
	if policy := page.Header().Get("Content-Security-Policy"); !strings.Contains(policy, "frame-ancestors 'none'") || !strings.Contains(policy, "default-src 'none'") {
		t.Errorf("the console document carries no usable policy: %q", policy)
	}
	body := page.Body.String()
	for _, want := range []string{"assets/console.css", "assets/app.mjs", "Content-Security-Policy"} {
		if !strings.Contains(body, want) {
			t.Errorf("the document does not reference %q", want)
		}
	}
	if strings.Contains(body, "Bearer ey") {
		t.Error("the served document carries a credential")
	}

	files := map[string]string{
		"/admin/assets/app.mjs":        "text/javascript",
		"/admin/assets/console.css":    "text/css",
		"/admin/assets/lib/ui.mjs":     "text/javascript",
		"/admin/assets/lib/agents.mjs": "text/javascript",
		"/admin/assets/lib/cli.mjs":    "text/javascript",
		"/admin/assets/lib/rules.mjs":  "text/javascript",
	}
	for path, kind := range files {
		rec := serve(t, http.MethodGet, path)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", path, rec.Code)
		}
		if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, kind) {
			t.Errorf("GET %s answers %q, want %s", path, got, kind)
		}
		if rec.Body.Len() == 0 {
			t.Errorf("GET %s answers an empty file", path)
		}
	}
}

// TestConsoleRefusesTheDocumentAtTheAssetFace pins the one cache rule the
// shell needs: the document names the files it loads, so a stale copy would
// point at a module set that an upgrade replaced.
func TestConsoleRefusesTheDocumentAtTheAssetFace(t *testing.T) {
	page := serve(t, http.MethodGet, "/admin/")
	if got := page.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("the console document answers Cache-Control %q, want no-store", got)
	}
	if got := serve(t, http.MethodGet, "/admin/assets/console.css").Header().Get("Cache-Control"); got == "no-store" {
		t.Error("a static file is served no-store; every load would refetch the whole module set")
	}
}

// TestConsoleAnswersOnlyItsOwnPaths pins the boundary with the Management
// surface beside it: the shell answers its document and its files, and a path
// outside them is not found rather than answered with the document — an API
// route must never be shadowed by the console.
func TestConsoleAnswersOnlyItsOwnPaths(t *testing.T) {
	for _, target := range []string{
		"/admin/servers",
		"/admin/overview",
		"/admin/assets/",
		"/admin/assets/nothing.mjs",
		"/admin/assets/app.go",
	} {
		if code := serve(t, http.MethodGet, target).Code; code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", target, code)
		}
	}
	// A traversal is refused by the mux's own path cleaning: it redirects to
	// the cleaned path, which is not a route.
	traversal := serve(t, http.MethodGet, "/admin/assets/../console.go")
	if traversal.Code == http.StatusOK || strings.Contains(traversal.Body.String(), "package console") {
		t.Errorf("GET /admin/assets/../console.go = %d and answered a file outside the console", traversal.Code)
	}
	if code := serve(t, http.MethodPost, "/admin/").Code; code != http.StatusMethodNotAllowed {
		t.Errorf("POST /admin/ = %d, want 405", code)
	}
	redirect := serve(t, http.MethodGet, "/admin")
	if redirect.Code != http.StatusPermanentRedirect || redirect.Header().Get("Location") != "/admin/" {
		t.Errorf("GET /admin = %d %q, want the document location", redirect.Code, redirect.Header().Get("Location"))
	}
}
