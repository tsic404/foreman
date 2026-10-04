package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// serverCredentialPattern matches mdt_/mul_ token shapes for the F3 scan.
var serverCredentialPattern = regexp.MustCompile(`m(dt|ul)_`)

// LoadServerToken reads Foreman's own server credential from env
// (Secret-mounted MULTICA_TOKEN): an mdt_ daemon token, or a mul_ PAT as
// fallback. The value must stay in memory only: never persisted by Foreman,
// never injected into a Job.
func LoadServerToken(getenv func(string) string) (string, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	token := strings.TrimSpace(getenv(EnvServerToken))
	if token == "" {
		return "", fmt.Errorf("%s is required", EnvServerToken)
	}
	if !strings.HasPrefix(token, "mdt_") && !strings.HasPrefix(token, "mul_") {
		return "", fmt.Errorf("%s must be an mdt_ or mul_ token", EnvServerToken)
	}
	return token, nil
}

// ContainsServerCredential reports whether s contains an mdt_/mul_ token
// shape; jobbuilder uses it to keep server credentials out of Job specs,
// Secrets and annotations (F3).
func ContainsServerCredential(s string) bool {
	return serverCredentialPattern.MatchString(s)
}

// RedactToken renders a token safe for logs: prefix plus the first 8 hex
// chars of its SHA-256 digest. Never log raw tokens.
func RedactToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	digest := hex.EncodeToString(sum[:])[:8]
	prefix, _, found := strings.Cut(token, "_")
	if !found || prefix == "" {
		return "sha256:" + digest
	}
	return prefix + "_" + digest
}
