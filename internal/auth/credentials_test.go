package auth

import (
	"strings"
	"testing"
)

func TestLoadServerToken(t *testing.T) {
	cases := map[string]struct {
		value   string
		wantErr bool
	}{
		"daemon token":      {"mdt_abc123", false},
		"pat fallback":      {"mul_xyz789", false},
		"trailing newline":  {"mdt_abc123\n", false},
		"missing":           {"", true},
		"wrong prefix":      {"fmj_abc", true},
		"task token prefix": {"mat_abc", true},
	}
	for name, tc := range cases {
		token, err := LoadServerToken(envFrom(map[string]string{EnvServerToken: tc.value}))
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr = %v", name, err, tc.wantErr)
			continue
		}
		if err == nil && strings.TrimSpace(token) != token {
			t.Errorf("%s: token %q not trimmed", name, token)
		}
	}
}

func TestContainsServerCredential(t *testing.T) {
	cases := map[string]bool{
		"mdt_abc":               true,
		"prefix mul_xyz suffix": true,
		"fmj_abc.def":           false,
		"mat_abc":               false,
		"no tokens here":        false,
		"":                      false,
	}
	for input, want := range cases {
		if got := ContainsServerCredential(input); got != want {
			t.Errorf("ContainsServerCredential(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestHasJobTokenPrefix(t *testing.T) {
	if !HasJobTokenPrefix("fmj_abc.def") {
		t.Error("fmj_ token not recognized")
	}
	if HasJobTokenPrefix("mdt_abc") {
		t.Error("mdt_ token misclassified as Job Token")
	}
}

func TestRedactToken(t *testing.T) {
	raw := "mdt_supersecretvalue"
	redacted := RedactToken(raw)
	if strings.Contains(redacted, "supersecretvalue") {
		t.Fatalf("redacted form %q leaks the token", redacted)
	}
	if !strings.HasPrefix(redacted, "mdt_") || len(redacted) != len("mdt_")+8 {
		t.Fatalf("redacted = %q, want prefix plus 8 hex chars", redacted)
	}
	if RedactToken("mul_othersecret") == redacted {
		t.Fatal("distinct tokens must redact to distinct digests")
	}
	if got := RedactToken("nounderscore"); strings.Contains(got, "nounderscore") {
		t.Fatalf("redacted = %q, leaks a token without prefix separator", got)
	}
}
