package main

import (
	"testing"

	"github.com/tsic404/foreman/internal/state"
)

// TestStateAuthenticatedMatchesTheAppliedCredential pins the two faces of the
// §10 安全 signal: authenticated must read the credential state.New actually
// applies (the dedicated password wins, else the URL's own data-node
// credential — userinfo for a single endpoint, the password query parameter
// for a sentinel URL), and the shipped REPLACE_ME placeholder must read as
// unauthenticated — an unreplaced manifest would otherwise authenticate with a
// value published in the repository and raise no alarm.
func TestStateAuthenticatedMatchesTheAppliedCredential(t *testing.T) {
	cases := []struct {
		name string
		cfg  state.Config
		want bool
	}{
		{"no credential", state.Config{URL: "redis://redis:6379"}, false},
		{"empty password", state.Config{URL: "redis://redis:6379", Password: ""}, false},
		{"real password", state.Config{URL: "redis://redis:6379", Password: "s3cret"}, true},
		{"shipped placeholder", state.Config{URL: "redis://redis:6379", Password: "REPLACE_ME"}, false},
		{"url userinfo password", state.Config{URL: "redis://:s3cret@redis:6379"}, true},
		{"url userinfo placeholder", state.Config{URL: "redis://:REPLACE_ME@redis:6379"}, false},
		{"url userinfo username-only ACL", state.Config{URL: "redis://foreman@redis:6379"}, true},
		{"password overrides url userinfo", state.Config{URL: "redis://:s3cret@redis:6379", Password: "REPLACE_ME"}, false},
		{"malformed url without password", state.Config{URL: "not-a-url"}, false},
		// The sentinel form takes its data-node credential from the password
		// query parameter; the URL's userinfo authenticates to the sentinels
		// only, so it is not the store's credential.
		{"sentinel data-node password", state.Config{URL: "redis://sentinel-a:26379?master_name=foreman&password=s3cret"}, true},
		{"sentinel data-node placeholder", state.Config{URL: "redis://sentinel-a:26379?master_name=foreman&password=REPLACE_ME"}, false},
		{"sentinel sentinel-only userinfo", state.Config{URL: "redis://:sentinel-secret@sentinel-a:26379?master_name=foreman"}, false},
		{"password overrides sentinel url", state.Config{URL: "redis://sentinel-a:26379?master_name=foreman&password=REPLACE_ME", Password: "s3cret"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stateAuthenticated(tc.cfg); got != tc.want {
				t.Errorf("stateAuthenticated(%+v) = %v, want %v", tc.cfg, got, tc.want)
			}
		})
	}
}
