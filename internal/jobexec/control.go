package jobexec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ControlPath is the Job-face attempt projection the reference adapters read.
// It is Foreman's own face, outside the upstream daemon API, so it never joins
// the daemon-face census.
const ControlPath = "/foreman/attempt"

// Control answers the attempt's own facts over the Job face. Every answer is
// scoped to the Job token: the server resolves the attempt from the token and
// never trusts an id in the request.
type Control struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

// NewControl builds the client with a bounded request timeout.
func NewControl(baseURL, token string) *Control {
	return &Control{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Token:   token,
		Client:  &http.Client{Timeout: 30 * time.Second},
	}
}

// RegistrationFact is what the upstream confirmed when the daemon registered.
type RegistrationFact struct {
	DaemonID     string `json:"daemon_id"`
	RuntimeID    string `json:"runtime_id"`
	Version      string `json:"version"`
	AgentVersion string `json:"agent_version,omitempty"`
	ObservedAt   string `json:"observed_at,omitempty"`
}

// TaskFact is the delivery and execution state Foreman recorded.
type TaskFact struct {
	State  string `json:"state"`
	Result string `json:"result"`
	// Sent is the server-side delivery commit; Delivered is proven arrival.
	Sent                bool   `json:"sent"`
	SentAt              string `json:"sent_at,omitempty"`
	Delivered           bool   `json:"delivered"`
	Started             bool   `json:"started"`
	StartedAt           string `json:"started_at,omitempty"`
	ClaimsAfterDelivery int    `json:"claims_after_delivery,omitempty"`
}

// SlotFact is the shared ledger's view of the attempt's reservation.
type SlotFact struct {
	Exists    bool   `json:"exists"`
	State     string `json:"state,omitempty"`
	Phase     string `json:"phase,omitempty"`
	ClaimID   string `json:"claim_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	Started   bool   `json:"started,omitempty"`
}

// EnvironmentFact is a fresh observation of the attempt's environment identity.
// Empty fields mean the deployment could not observe it, which the ready check
// treats as a refusal rather than a ready.
type EnvironmentFact struct {
	TreeID   string `json:"tree_id"`
	Identity string `json:"identity"`
	State    string `json:"state"`
}

// AttemptActive reports a task whose payload may still be in flight.
const (
	TaskStateTerminal = "terminal"
)

// AttemptFact names the reservation the token is scoped to.
type AttemptFact struct {
	TaskID        string `json:"task_id"`
	EnvironmentID string `json:"environment_id"`
	SlotID        string `json:"slot_id"`
}

// Snapshot is the projection's answer.
type Snapshot struct {
	Attempt      AttemptFact       `json:"attempt"`
	Registration *RegistrationFact `json:"registration"`
	Task         TaskFact          `json:"task"`
	Slot         SlotFact          `json:"slot"`
}

// Snapshot reads the attempt's facts.
func (c *Control) Snapshot(ctx context.Context) (Snapshot, error) {
	var snapshot Snapshot
	status, body, err := c.do(ctx, http.MethodGet, ControlPath, nil)
	if err != nil {
		return Snapshot{}, err
	}
	if status != http.StatusOK {
		return Snapshot{}, responseError(status, body)
	}
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return Snapshot{}, fmt.Errorf("decode the attempt projection: %w", err)
	}
	return snapshot, nil
}

// ObserveEnvironment runs the deployment's read-only environment probe and
// answers its current facts. It is separate from Snapshot so the dispatch loop
// never triggers a probe on every poll.
func (c *Control) ObserveEnvironment(ctx context.Context) (EnvironmentFact, error) {
	var fact EnvironmentFact
	status, body, err := c.do(ctx, http.MethodGet, ControlPath+"/environment", nil)
	if err != nil {
		return EnvironmentFact{}, err
	}
	if status != http.StatusOK {
		return EnvironmentFact{}, responseError(status, body)
	}
	if err := json.Unmarshal(body, &fact); err != nil {
		return EnvironmentFact{}, fmt.Errorf("decode the environment observation: %w", err)
	}
	return fact, nil
}

// SlotRequest is one ledger transition. The ids it carries are the attempt's
// own claim and session; the environment and slot come from the token's frozen
// binding, never from this body.
type SlotRequest struct {
	ClaimID         string `json:"claim_id,omitempty"`
	ExpectedClaimID string `json:"expected_claim_id,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
	Confirmed       bool   `json:"confirmed,omitempty"`
}

// Slot commits one ledger phase and answers the resulting slot.
func (c *Control) Slot(ctx context.Context, phase string, request SlotRequest) (SlotFact, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return SlotFact{}, fmt.Errorf("encode the slot request: %w", err)
	}
	status, answer, err := c.do(ctx, http.MethodPost, ControlPath+"/slots/"+url.PathEscape(phase), body)
	if err != nil {
		return SlotFact{}, err
	}
	if status != http.StatusOK {
		return SlotFact{}, responseError(status, answer)
	}
	var decoded struct {
		Slot SlotFact `json:"slot"`
	}
	if err := json.Unmarshal(answer, &decoded); err != nil {
		return SlotFact{}, fmt.Errorf("decode the slot answer: %w", err)
	}
	return decoded.Slot, nil
}

func (c *Control) do(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, reader)
	if err != nil {
		return 0, nil, fmt.Errorf("build the Job-face request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.Token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.Client.Do(request)
	if err != nil {
		return 0, nil, fmt.Errorf("reach the Job face: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	answer, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return 0, nil, fmt.Errorf("read the Job-face answer: %w", err)
	}
	return response.StatusCode, answer, nil
}

func responseError(status int, body []byte) error {
	detail := strings.TrimSpace(string(body))
	if detail == "" {
		return fmt.Errorf("the Job face answered %d", status)
	}
	return fmt.Errorf("the Job face answered %d: %s", status, detail)
}
