package auth

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

// TokenPrefix marks a Job Token; the router uses it to split local
// verification from server passthrough (S29).
const TokenPrefix = "fmj_"

// tokenSegmentEncoding is the strict raw-url decoder for both token segments.
// Strict rejects a non-zero trailing padding bit, so a segment has exactly one
// accepted spelling: without it a rewritten final character decodes to the
// same signature bytes and passes the HMAC compare while hashing differently,
// and the revocation lookup would then miss the digest the reclaim wrote
// (§6.1).
var tokenSegmentEncoding = base64.RawURLEncoding.Strict()

// HasJobTokenPrefix reports whether token carries the Job Token prefix.
func HasJobTokenPrefix(token string) bool {
	return strings.HasPrefix(token, TokenPrefix)
}

// tokenPayload is the signed content of a Job Token. Field order matters:
// encoding/json marshals struct fields in declaration order, keeping the
// wire format stable. An empty TaskID is omitted so a probe payload
// {"j":..,"r":"catalog-probe","w":..,"e":..} carries no "t" key at all
// (a task token always has one, so its shape is unchanged).
type tokenPayload struct {
	JobName     string `json:"j"`
	TaskID      string `json:"t,omitempty"`
	Role        string `json:"r,omitempty"`
	WorkspaceID string `json:"w"`
	Expiry      int64  `json:"e"`
}

// valid reports whether the payload carries a usable identity: both token
// kinds need job/workspace/expiry, and exactly one of the task and role faces
// must identify the Job (a probe token has no "t").
func (p tokenPayload) valid() bool {
	return p.JobName != "" && p.WorkspaceID != "" && p.Expiry > 0 &&
		(p.TaskID != "" || p.Role != "")
}

// encodePayload marshals p and base64url-encodes it (RawURLEncoding, no
// padding).
func encodePayload(p tokenPayload) (string, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// canonicalToken returns the wire form of token rebuilt from the decoded bytes
// of its two segments, or token itself when it is not a well-formed Job Token.
// base64url spells the same bytes several ways (the trailing padding bits are
// not part of the value), so a digest over the received spelling is not a key
// the writer and the reader can both compute. Verify rejects every
// non-canonical spelling, which keeps this function the identity on accepted
// tokens.
func canonicalToken(token string) string {
	if !HasJobTokenPrefix(token) {
		return token
	}
	payloadSegment, sigSegment, found := strings.Cut(strings.TrimPrefix(token, TokenPrefix), ".")
	if !found {
		return token
	}
	payload, err := base64.RawURLEncoding.DecodeString(payloadSegment)
	if err != nil {
		return token
	}
	signature, err := base64.RawURLEncoding.DecodeString(sigSegment)
	if err != nil {
		return token
	}
	return TokenPrefix + base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(signature)
}

// parseToken splits token into payload and signature segments and decodes
// them. Any format violation maps to ErrMalformedToken.
func parseToken(token string) (payload tokenPayload, payloadSegment string, signature []byte, err error) {
	if !HasJobTokenPrefix(token) {
		return tokenPayload{}, "", nil, ErrMalformedToken
	}
	payloadSegment, sigSegment, found := strings.Cut(strings.TrimPrefix(token, TokenPrefix), ".")
	if !found || payloadSegment == "" || sigSegment == "" {
		return tokenPayload{}, "", nil, ErrMalformedToken
	}
	raw, decErr := tokenSegmentEncoding.DecodeString(payloadSegment)
	if decErr != nil {
		return tokenPayload{}, "", nil, ErrMalformedToken
	}
	if jsonErr := json.Unmarshal(raw, &payload); jsonErr != nil || !payload.valid() {
		return tokenPayload{}, "", nil, ErrMalformedToken
	}
	signature, sigErr := tokenSegmentEncoding.DecodeString(sigSegment)
	if sigErr != nil {
		return tokenPayload{}, "", nil, ErrMalformedToken
	}
	return payload, payloadSegment, signature, nil
}
