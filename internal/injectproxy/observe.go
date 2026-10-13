package injectproxy

import "strings"

// Observation mapping: the small part of a message the record face is allowed
// to keep. Everything here feeds §4.3 rows and §4.2 matching — never the
// relayed bytes.

// credentialPrefixes are the credential faces of the multica protocol
// (03-contracts.md §1.1): the server credentials Foreman authenticates with
// (mdt_/mul_), the task-level token an agent's call carries (mat_) and the
// Job Token (fmj_). §4.3 lets the record face keep exactly this much of a
// credential — the token itself never leaves authPrefix.
var credentialPrefixes = []string{"mdt_", "mul_", "mat_", "fmj_"}

// prefixOther is the record face's bucket for every credential it does not
// recognize: a malformed Authorization header, a probe credential, or none at
// all (the record face classifies by credential, not by caller identity).
const prefixOther = "other"

// authPrefix reduces the Authorization header to a credential prefix. The
// token value is compared, never copied out.
func authPrefix(h *messageHead) string {
	value := h.first("Authorization")
	if i := strings.IndexByte(value, ' '); i >= 0 {
		value = strings.TrimSpace(value[i+1:])
	}
	for _, prefix := range credentialPrefixes {
		if strings.HasPrefix(value, prefix) {
			return prefix
		}
	}
	return prefixOther
}

// directionFor classifies a request by its credential face: Foreman's own
// credential is F→S, everything else is a transparent A→S request.
func directionFor(prefix string) string {
	if prefix == "mdt_" || prefix == "mul_" {
		return directionForemanToServer
	}
	return directionAgentToServer
}

// recordedHeaderNames is the header allowlist of the record face. It carries
// the capability string forward-01 asserts verbatim (§4.3's example row);
// nothing else is recorded, because a free-form header map is where
// credentials, cookies and tokens would leak.
var recordedHeaderNames = []string{"X-Client-Capabilities"}

// recordHeaders extracts the allowlisted headers, keyed by their canonical
// name.
func recordHeaders(h *messageHead) map[string]string {
	var out map[string]string
	for _, field := range h.headers {
		for _, name := range recordedHeaderNames {
			if !strings.EqualFold(field.name, name) {
				continue
			}
			if out == nil {
				out = map[string]string{}
			}
			out[name] = field.value
		}
	}
	return out
}
