package auth

import (
	"errors"
	"net/http"
)

// Verification failures returned by Issuer.Verify, in the fixed check order
// of the auth module design. HTTPStatus maps them to the wire status;
// Reason maps them to a metrics label.
var (
	ErrMalformedToken = errors.New("malformed job token")
	ErrBadSignature   = errors.New("job token signature mismatch")
	ErrExpired        = errors.New("job token expired")
	ErrRevoked        = errors.New("job token revoked")
	ErrUnknownJob     = errors.New("unknown or terminal job")
	ErrScopeMismatch  = errors.New("token scope does not match request resource")
)

// ErrKeyTooShort rejects an HMAC key below MinKeyBytes.
var ErrKeyTooShort = errors.New("job token HMAC key too short")

// MinKeyBytes is the minimum decoded length of the Job Token HMAC key.
const MinKeyBytes = 32

// HTTPStatus maps a Verify error to the response status: scope mismatch is
// 403, every other verification failure is 401.
func HTTPStatus(err error) int {
	switch {
	case errors.Is(err, ErrScopeMismatch):
		return http.StatusForbidden
	case errors.Is(err, ErrMalformedToken), errors.Is(err, ErrBadSignature),
		errors.Is(err, ErrExpired), errors.Is(err, ErrRevoked), errors.Is(err, ErrUnknownJob):
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}

// Reason returns the metrics label for a Verify error. The vocabulary is
// closed (§9.2:
// malformed|signature|expired|scope|unknown_job|revoked) — a seventh value
// would split the counter into an off-contract series. ErrRevoked is the
// same-name reuse face (the entry came back with a predecessor's token);
// every other rejection of a live entry maps to unknown_job. The probe
// branch reuses the same labels: a missing session is unknown_job and a hash
// mismatch is revoked.
func Reason(err error) string {
	switch {
	case errors.Is(err, ErrMalformedToken):
		return "malformed"
	case errors.Is(err, ErrBadSignature):
		return "signature"
	case errors.Is(err, ErrExpired):
		return "expired"
	case errors.Is(err, ErrScopeMismatch):
		return "scope"
	case errors.Is(err, ErrRevoked):
		return "revoked"
	default:
		return "unknown_job"
	}
}
