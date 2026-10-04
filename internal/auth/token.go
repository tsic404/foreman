package auth

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

// TokenPrefix marks a Job Token; the router uses it to split local
// verification from server passthrough (S29).
const TokenPrefix = "fmj_"

// HasJobTokenPrefix reports whether token carries the Job Token prefix.
func HasJobTokenPrefix(token string) bool {
	return strings.HasPrefix(token, TokenPrefix)
}

// tokenPayload is the signed content of a Job Token. Field order matters:
// encoding/json marshals struct fields in declaration order, keeping the
// wire format stable.
type tokenPayload struct {
	JobName     string `json:"j"`
	TaskID      string `json:"t"`
	WorkspaceID string `json:"w"`
	Expiry      int64  `json:"e"`
}

func (p tokenPayload) valid() bool {
	return p.JobName != "" && p.TaskID != "" && p.WorkspaceID != "" && p.Expiry > 0
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
	raw, decErr := base64.RawURLEncoding.DecodeString(payloadSegment)
	if decErr != nil {
		return tokenPayload{}, "", nil, ErrMalformedToken
	}
	if jsonErr := json.Unmarshal(raw, &payload); jsonErr != nil || !payload.valid() {
		return tokenPayload{}, "", nil, ErrMalformedToken
	}
	signature, sigErr := base64.RawURLEncoding.DecodeString(sigSegment)
	if sigErr != nil {
		return tokenPayload{}, "", nil, ErrMalformedToken
	}
	return payload, payloadSegment, signature, nil
}
