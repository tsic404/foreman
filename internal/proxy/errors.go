package proxy

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// StatusError is a non-2xx server response. Callers branch on Code per §1.4 /
// §1.5: at runtime 401/403 back off like any other credential failure, and a
// 404 with a "runtime not found" body triggers re-registration. Only the
// startup self-check judges a credential error configuration (see
// IsCredentialConfigError).
type StatusError struct {
	Code int
	Body []byte
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("server returned %d: %s", e.Code, truncate(e.Body, 200))
}

// WorkspaceMismatchError reports C17 answering a workspace set that does not
// equal the configured workspace: the credential is bound to another
// workspace, which is a configuration error, not a transient upstream
// failure (§1.1, §1.5, AC-86). Visible carries the answer's workspace ids so
// the startup log can name both sides without a second request.
type WorkspaceMismatchError struct {
	Configured string
	Visible    []string
}

func (e *WorkspaceMismatchError) Error() string {
	visible := strings.Join(e.Visible, ",")
	if visible == "" {
		visible = "none"
	}
	return fmt.Sprintf("the server token's workspace set is not exactly %s (visible: %s)", e.Configured, visible)
}

// IsCredentialConfigError reports the credential defects the startup
// self-check refuses to start on: the C-17 answer is not exactly the
// configured workspace, or the server rejects the credential outright (§1.1).
// Only the startup path treats this as fatal — at runtime §1.5 keeps 401/403
// and 404 inside the §1.4 backoff, never judged configuration (AC-86).
func IsCredentialConfigError(err error) bool {
	var mismatch *WorkspaceMismatchError
	if errors.As(err, &mismatch) {
		return true
	}
	var status *StatusError
	return errors.As(err, &status) && (status.Code == http.StatusUnauthorized || status.Code == http.StatusForbidden)
}

// DiscoveryAnswerError reports a C-17 answer Foreman cannot use: an entry
// carries a workspace id but no display name. C-17 is the display-name
// authority for the policy's partition keys (§8.5), and "" must not enter the
// cache as a name — it reads as "the authority has not answered yet", which
// would let a genuinely absent key boot with a warning instead of refusing
// startup. The defect is deterministic, so it refuses startup rather than
// retrying like a transient upstream failure (§1.1, AC-96).
type DiscoveryAnswerError struct {
	// WorkspaceID names the answer entry that carried no display name.
	WorkspaceID string
}

func (e *DiscoveryAnswerError) Error() string {
	return fmt.Sprintf("C-17 answer has no display name for workspace %s", e.WorkspaceID)
}

// IsStartupFatal reports the self-check outcomes that refuse startup: a
// credential bound elsewhere or rejected outright, and a C-17 answer that
// cannot serve as the discovery and display-name authority. Transient
// upstream failures are never in this class — the loops converge instead
// (§1.1, AC-86).
func IsStartupFatal(err error) bool {
	var answer *DiscoveryAnswerError
	return errors.As(err, &answer) || IsCredentialConfigError(err)
}

// ErrTerminalUndelivered reports a terminal forward (C9–C11) whose retry
// budget (4s/8s/16s/32s/64s) is exhausted; the caller must enqueue the
// report to PendingReports (§10: terminal callbacks are never dropped).
var ErrTerminalUndelivered = errors.New("terminal report undelivered after retry budget")

// terminalRetryWaits is the §10 retry budget for C9–C11: six attempts,
// 124s of total waiting.
var terminalRetryWaits = []time.Duration{0, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, 64 * time.Second}

func truncate(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[:n]
}
