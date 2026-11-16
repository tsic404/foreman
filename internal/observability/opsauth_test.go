package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func envOf(pairs map[string]string) func(string) string {
	return func(k string) string { return pairs[k] }
}

// TestLoadOpsCredentialsWideToken covers the deployment-wide credential: it
// carries no workspace list, which the readers interpret as "every served
// workspace".
func TestLoadOpsCredentialsWideToken(t *testing.T) {
	creds, err := LoadOpsCredentials(envOf(map[string]string{EnvOpsToken: "wide-secret"}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(creds) != 1 || creds[0].Token != "wide-secret" || creds[0].Workspaces != nil {
		t.Fatalf("creds = %+v, want one wide credential", creds)
	}
}

// TestLoadOpsCredentialsWorkspaceBound parses the <workspace>=<token> pairs
// into one credential each, with exactly that workspace granted.
func TestLoadOpsCredentialsWorkspaceBound(t *testing.T) {
	creds, err := LoadOpsCredentials(envOf(map[string]string{
		EnvOpsWorkspaceTokens: "ws-1=tok-a, ws-2=tok-b",
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(creds) != 2 {
		t.Fatalf("creds = %+v, want 2", creds)
	}
	if creds[0].Token != "tok-a" || len(creds[0].Workspaces) != 1 || creds[0].Workspaces[0] != "ws-1" {
		t.Fatalf("first credential = %+v", creds[0])
	}
	if creds[1].Token != "tok-b" || len(creds[1].Workspaces) != 1 || creds[1].Workspaces[0] != "ws-2" {
		t.Fatalf("second credential = %+v", creds[1])
	}
}

// TestLoadOpsCredentialsRejectsBadConfig is the fail-fast contract: a
// configuration mistake must refuse startup, never yield a quietly reduced
// credential set.
func TestLoadOpsCredentialsRejectsBadConfig(t *testing.T) {
	cases := map[string]map[string]string{
		"placeholder wide token": {EnvOpsToken: "REPLACE_ME"},
		"placeholder pair token": {EnvOpsWorkspaceTokens: "ws-1=REPLACE_ME"},
		"missing separator":      {EnvOpsWorkspaceTokens: "ws-1"},
		"empty token":            {EnvOpsWorkspaceTokens: "ws-1="},
		"empty workspace":        {EnvOpsWorkspaceTokens: "=tok-a"},
		"token bound twice":      {EnvOpsWorkspaceTokens: "ws-1=tok-a", EnvOpsToken: "tok-a"},
		"workspace bound twice":  {EnvOpsWorkspaceTokens: "ws-1=tok-a,ws-1=tok-b"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadOpsCredentials(envOf(env)); err == nil {
				t.Fatalf("accepted %v; a bad credential config must fail fast", env)
			}
		})
	}
}

// TestLoadOpsCredentialsErrorDoesNotLeakToken: the error reaches the container
// log, so it must name the entry's position, never echo the credential. The
// malformed shapes that carry a secret are the ones missing a separator — the
// whole entry is then the token.
func TestLoadOpsCredentialsErrorDoesNotLeakToken(t *testing.T) {
	const secret = "super-secret-token-value"
	for _, raw := range []string{secret, "ws-1" + secret, "=" + secret} {
		_, err := LoadOpsCredentials(envOf(map[string]string{EnvOpsWorkspaceTokens: raw}))
		if err == nil {
			t.Fatalf("accepted %q; expected a config error", raw)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks the credential %q: %v", raw, err)
		}
	}
}

// TestValidateOpsCredentials covers the deployment-mismatch guard: a bound
// credential naming a workspace this process cannot serve can only ever 404.
func TestValidateOpsCredentials(t *testing.T) {
	bound := []OpsCredential{{Token: "tok", Workspaces: []string{"ws-1"}}}

	if err := ValidateOpsCredentials(bound, []string{"ws-1", "ws-2"}); err != nil {
		t.Fatalf("served workspace rejected: %v", err)
	}
	if err := ValidateOpsCredentials(bound, []string{"ws-2"}); err == nil {
		t.Fatal("accepted a credential bound to an unserved workspace")
	}
	// An empty served set means discovery has not run; the check cannot judge.
	if err := ValidateOpsCredentials(bound, nil); err != nil {
		t.Fatalf("empty served set rejected: %v", err)
	}
	// A wide credential names no workspace and must always pass.
	if err := ValidateOpsCredentials([]OpsCredential{{Token: "wide"}}, []string{"ws-1"}); err != nil {
		t.Fatalf("wide credential rejected: %v", err)
	}
}

// TestOpsAuthAdmitsAndBindsIdentity: a valid credential reaches the handler
// and its workspace grant is on the request context.
func TestOpsAuthAdmitsAndBindsIdentity(t *testing.T) {
	a := newOpsAuth([]OpsCredential{{Token: "tok", Workspaces: []string{"ws-1"}}})

	var got opsIdentity
	var reached bool
	h := func(w http.ResponseWriter, r *http.Request) {
		reached = true
		got, _ = opsIdentityFrom(r.Context())
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	a.serve(h, rec, req)

	if !reached {
		t.Fatalf("admitted request did not reach the handler: %d", rec.Code)
	}
	if len(got.workspaces) != 1 || got.workspaces[0] != "ws-1" {
		t.Fatalf("identity = %+v, want ws-1 grant", got)
	}
}

// TestOpsAuthRejects covers every unauthenticated shape: no header, a wrong
// token, a non-bearer scheme, and an empty deployment credential set. All must
// answer 401 and never reach the handler.
func TestOpsAuthRejects(t *testing.T) {
	auth := newOpsAuth([]OpsCredential{{Token: "tok"}})
	cases := map[string]struct {
		auth   *opsAuth
		header string
	}{
		"no header":         {auth, ""},
		"wrong token":       {auth, "Bearer nope"},
		"basic scheme":      {auth, "Basic tok"},
		"bearer no value":   {auth, "Bearer "},
		"no credentials":    {newOpsAuth(nil), "Bearer tok"},
		"prefix only":       {auth, "Bearer"},
		"token is a jobtok": {auth, "Bearer job-token"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			reached := false
			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			if c.header != "" {
				req.Header.Set("Authorization", c.header)
			}
			rec := httptest.NewRecorder()
			c.auth.serve(func(http.ResponseWriter, *http.Request) { reached = true }, rec, req)

			if reached {
				t.Fatal("unauthorized request reached the handler")
			}
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
			if got := rec.Header().Get("WWW-Authenticate"); !strings.Contains(got, "Bearer") {
				t.Fatalf("WWW-Authenticate = %q, want a Bearer challenge", got)
			}
		})
	}
}

// TestBearerTokenSchemeIsCaseInsensitive: RFC 7235 makes the scheme name
// case-insensitive, and a proxy in front may normalise it either way.
func TestBearerTokenSchemeIsCaseInsensitive(t *testing.T) {
	for _, header := range []string{"Bearer abc", "bearer abc", "BEARER abc"} {
		got, ok := bearerToken(header)
		if !ok || got != "abc" {
			t.Fatalf("bearerToken(%q) = %q,%v; want abc,true", header, got, ok)
		}
	}
	for _, header := range []string{"", "Bearer", "Basic abc", "Token abc"} {
		if _, ok := bearerToken(header); ok {
			t.Fatalf("bearerToken(%q) admitted a non-bearer header", header)
		}
	}
}

// TestOpsIdentityAbsentFailsClosed: a handler reached without the credential
// face must not be granted every workspace.
func TestOpsIdentityAbsentFailsClosed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/foreman/tasks", nil)
	if identity, ok := opsIdentityFrom(req.Context()); ok {
		t.Fatalf("absent identity admitted with %+v; it must fail closed", identity)
	}
}
