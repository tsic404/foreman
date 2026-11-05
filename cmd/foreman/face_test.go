package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/observability"
)

// TestJobFaceMuxExposesOnlyTheJobFaceAndLiveness pins the listener split: the
// ops data routes are not mounted on the Job face, so a Job pod dialing its
// own server address with its Job Token reaches no projection, payload preview,
// log or register-source answer (§2.2 S-25). The one ops-shaped route the face
// keeps is the liveness probe input, and it answers the verdict alone.
func TestJobFaceMuxExposesOnlyTheJobFaceAndLiveness(t *testing.T) {
	fake := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Job-Face", "1")
		w.WriteHeader(http.StatusOK)
	})
	mux := newJobFaceMux(fake, observability.NewHealth(), time.Now())

	for _, path := range []string{"/api/daemon/workspaces", "/api/daemon/register", "/api/tokens/current/renew"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK || rec.Header().Get("X-Job-Face") != "1" {
			t.Errorf("%s = %d, want the Job daemon face", path, rec.Code)
		}
	}
	for _, path := range []string{
		"/foreman/tasks",
		"/foreman/tasks/t-1",
		"/foreman/tasks/t-1/logs",
		"/foreman/tasks/t-1/payload-preview",
		"/foreman/register-source",
		"/foreman/version",
		"/metrics",
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		// A Job Token is not an ops credential; even presented here it must
		// not find a route.
		req.Header.Set("Authorization", "Bearer fmj_00000000000000000000000000000000")
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s on the Job face = %d, want 404", path, rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/foreman/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/foreman/healthz = %d, want 200", rec.Code)
	}
	for _, field := range []string{"inflight", "runtime_ids", "task_id", "workspace_id"} {
		if strings.Contains(rec.Body.String(), field) {
			t.Errorf("the Job face's liveness answer carries %q: %s", field, rec.Body)
		}
	}
}
