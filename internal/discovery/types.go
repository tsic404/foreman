// Package discovery runs the restricted model and capability discovery
// adapters (04-architecture.md §7; 02-contracts.md V2-C04). It never creates a
// probe Job or Pod and never consumes a business remote slot.
package discovery

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"
)

// Kind is the discovery request's subject.
type Kind string

const (
	KindModels       Kind = "models"
	KindCapabilities Kind = "capabilities"
)

// ResultClass is the four-way result the design requires. An empty list is a
// success; an unsupported adapter is not an empty list.
type ResultClass string

const (
	ClassNonEmpty    ResultClass = "succeeded_nonempty"
	ClassEmpty       ResultClass = "succeeded_empty"
	ClassUnsupported ResultClass = "unsupported"
	ClassFailed      ResultClass = "failed"
)

// Model is one normalized model identifier.
type Model struct {
	ID       string `json:"id"`
	Provider string `json:"provider,omitempty"`
	Version  string `json:"version,omitempty"`
}

// Capability is one normalized capability name.
type Capability struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

// Outcome is one adapter execution's answer.
type Outcome struct {
	Class        ResultClass  `json:"class,omitempty"`
	Models       []Model      `json:"models,omitempty"`
	Capabilities []Capability `json:"capabilities,omitempty"`
	Provider     string       `json:"provider,omitempty"`
	Detail       string       `json:"detail,omitempty"`
}

// Call is one restricted execution's input. Dir is a private directory the
// executor owns; the adapter must not write outside it.
type Call struct {
	ServerID       string
	WorkspaceID    string
	RuntimeID      string
	Profile        string
	Kind           Kind
	Platform       string
	CLIDigest      string
	ConfigRevision uint64
	Dir            string
	// BinaryPath is the bound, verified CLI written into the private
	// directory. A bound adapter must not fall back to a global binary.
	BinaryPath string
	// ConfigPath is the read-only configuration file for this source/profile.
	ConfigPath string
}

// Adapter is one backend's restricted discovery command.
type Adapter interface {
	Name() string
	Kinds() []Kind
	Enumerate(ctx context.Context, call Call) (Outcome, error)
}

// ProbingAdapter can prove its command surface before it is trusted.
type ProbingAdapter interface {
	Probe(ctx context.Context) error
}

// BoundProbingAdapter can prove the command surface of one exact, verified
// CLI payload and report the version that payload declares. Registration
// uses it so a global binary's probe cannot vouch for a different published
// digest or version.
type BoundProbingAdapter interface {
	ProbeBound(ctx context.Context, binaryPath string) (string, error)
}

// AdapterStatus is the management projection of one adapter's trust.
type AdapterStatus struct {
	Name       string   `json:"name"`
	Kinds      []string `json:"kinds"`
	Trusted    bool     `json:"trusted"`
	Reason     string   `json:"reason,omitempty"`
	VerifiedAt string   `json:"verified_at,omitempty"`
}

// Request is one persisted discovery request. Result is set once the request
// completes.
type Request struct {
	ID                string  `json:"request_id"`
	UpstreamRequestID string  `json:"upstream_request_id,omitempty"`
	ServerID          string  `json:"server_id"`
	WorkspaceID       string  `json:"workspace_id"`
	RuntimeID         string  `json:"runtime_id"`
	Profile           string  `json:"profile,omitempty"`
	Kind              Kind    `json:"kind"`
	Adapter           string  `json:"adapter"`
	CLIDigest         string  `json:"cli_digest,omitempty"`
	ConfigRevision    uint64  `json:"config_revision,omitempty"`
	Platform          string  `json:"platform,omitempty"`
	Generation        uint64  `json:"generation"`
	State             string  `json:"state"`
	Result            *Result `json:"result,omitempty"`
	Superseded        bool    `json:"superseded"`
	// CachedFrom is the request whose immutable result this request reports.
	// A cache hit still persists the new upstream request ID.
	CachedFrom  string     `json:"cached_from,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	Deadline    *time.Time `json:"deadline,omitempty"`
}

// Result is one completed execution.
type Result struct {
	Class        ResultClass  `json:"class"`
	Provider     string       `json:"provider,omitempty"`
	Models       []Model      `json:"models"`
	Capabilities []Capability `json:"capabilities"`
	Detail       string       `json:"detail,omitempty"`
	CompletedAt  time.Time    `json:"completed_at"`
}

// SubmitInput is one management request.
type SubmitInput struct {
	UpstreamRequestID string
	ServerID          string
	WorkspaceID       string
	RuntimeID         string
	Profile           string
	Kind              Kind
	Adapter           string
	CLIDigest         string
	ConfigRevision    uint64
	Platform          string
	Deadline          time.Time
}

// Latest is the environment's current result. Fingerprint lets a duplicate
// request reuse the same generation without executing again.
type Latest struct {
	RequestID      string    `json:"request_id"`
	Generation     uint64    `json:"generation"`
	Fingerprint    string    `json:"fingerprint"`
	Adapter        string    `json:"adapter"`
	CLIDigest      string    `json:"cli_digest,omitempty"`
	ConfigRevision uint64    `json:"config_revision,omitempty"`
	Platform       string    `json:"platform,omitempty"`
	Result         Result    `json:"result"`
	CompletedAt    time.Time `json:"completed_at"`
}

// ModelQuery is one current-catalog read. The caller supplies the current CLI
// and configuration facts so a cached result cannot outlive them.
type ModelQuery struct {
	ServerID       string
	WorkspaceID    string
	RuntimeID      string
	Profile        string
	Adapter        string
	CLIDigest      string
	ConfigRevision uint64
	// ConfigKnown separates a known "no configuration" (revision zero) from
	// a lookup that failed. An unknown component is never a wildcard.
	ConfigKnown bool
	Platform    string
}

// ModelCatalog is the management view of one environment's current result.
// Available=false with Class=unsupported is not an empty catalog.
type ModelCatalog struct {
	ServerID    string      `json:"server_id"`
	WorkspaceID string      `json:"workspace_id"`
	RuntimeID   string      `json:"runtime_id"`
	Profile     string      `json:"profile,omitempty"`
	Available   bool        `json:"available"`
	Class       ResultClass `json:"class,omitempty"`
	Models      []Model     `json:"models"`
	RequestID   string      `json:"request_id,omitempty"`
	Generation  uint64      `json:"generation"`
	CompletedAt string      `json:"completed_at,omitempty"`
	Detail      string      `json:"detail,omitempty"`
}

// Errors the surface answers with.
var (
	ErrNotFound        = errors.New("discovery request is not registered")
	ErrAdapterNotFound = errors.New("adapter is not registered")
)

var (
	idPattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	platformPattern = regexp.MustCompile(`^[a-z0-9]+/[a-z0-9]+$`)
)

// ValidateInput checks one request.
func ValidateInput(input SubmitInput) (SubmitInput, error) {
	input.UpstreamRequestID = strings.TrimSpace(input.UpstreamRequestID)
	input.ServerID = strings.TrimSpace(input.ServerID)
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.RuntimeID = strings.TrimSpace(input.RuntimeID)
	input.Profile = strings.TrimSpace(input.Profile)
	input.Adapter = strings.TrimSpace(input.Adapter)
	input.CLIDigest = strings.TrimSpace(input.CLIDigest)
	input.Platform = strings.TrimSpace(input.Platform)
	if !idPattern.MatchString(input.ServerID) {
		return SubmitInput{}, errors.New("server_id must be a bare identifier")
	}
	if !idPattern.MatchString(input.WorkspaceID) {
		return SubmitInput{}, errors.New("workspace_id must be a bare identifier")
	}
	if !idPattern.MatchString(input.RuntimeID) {
		return SubmitInput{}, errors.New("runtime_id must be a bare identifier")
	}
	if input.Profile != "" && !idPattern.MatchString(input.Profile) {
		return SubmitInput{}, errors.New("profile must be a bare identifier")
	}
	if input.Adapter != "" && !idPattern.MatchString(input.Adapter) {
		return SubmitInput{}, errors.New("adapter must be a bare identifier")
	}
	if input.Platform != "" && !platformPattern.MatchString(strings.ToLower(input.Platform)) {
		return SubmitInput{}, errors.New("platform must be os/arch")
	}
	input.Platform = strings.ToLower(input.Platform)
	if input.Kind != KindModels && input.Kind != KindCapabilities {
		return SubmitInput{}, errors.New("kind must be models or capabilities")
	}
	return input, nil
}
