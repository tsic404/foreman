// Package agentconfig owns the Agent configuration revisions and the
// per-attempt read-only projection (04-architecture.md §3.5; 02-contracts.md
// V2-C04). This is the prototype face: it validates the submitted JSON,
// records the revision and never stores the content or creates a Secret.
package agentconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// MaxConfigBytes bounds one submitted configuration document.
const MaxConfigBytes = 32 << 10

// FileName is the only file a projection mounts.
const FileName = "config.json"

// MountDir is the per-attempt read-only directory.
const MountDir = "/run/foreman/agent-config"

// Scope names one configuration generation. Profile is the upstream profile
// or account identifier when one backend has more than one.
type Scope struct {
	ServerID    string `json:"server_id"`
	WorkspaceID string `json:"workspace_id"`
	Backend     string `json:"backend"`
	Profile     string `json:"profile,omitempty"`
}

// Key is the stored identity of one scope.
func (s Scope) Key() string {
	return s.ServerID + "\x00" + s.WorkspaceID + "\x00" + s.Backend + "\x00" + s.Profile
}

// Revision is one stored configuration revision. Content is deliberately
// absent: the digest and size prove what was validated without retaining it.
type Revision struct {
	Scope     Scope     `json:"scope"`
	Revision  uint64    `json:"revision"`
	SecretRef string    `json:"secret_ref"`
	FileName  string    `json:"file_name"`
	Digest    string    `json:"sha256"`
	Size      int64     `json:"size"`
	UpdatedAt time.Time `json:"updated_at"`
	UpdatedBy string    `json:"updated_by"`
}

// WriteRequest is the PUT body. Config is the virtual JSON document; it is
// validated and discarded.
type WriteRequest struct {
	Profile   string          `json:"profile"`
	FileName  string          `json:"file_name"`
	SecretRef string          `json:"secret_ref"`
	Config    json.RawMessage `json:"config"`
}

// View is one management row. It carries the reference and the revision, never
// the content.
type View struct {
	ServerID    string `json:"server_id"`
	WorkspaceID string `json:"workspace_id"`
	Backend     string `json:"backend"`
	Profile     string `json:"profile,omitempty"`
	Revision    uint64 `json:"revision"`
	SecretRef   string `json:"secret_ref"`
	FileName    string `json:"file_name"`
	Digest      string `json:"sha256"`
	Size        int64  `json:"size"`
	UpdatedAt   string `json:"updated_at,omitempty"`
	UpdatedBy   string `json:"updated_by,omitempty"`
}

// Projection is the per-attempt mount description. The prototype never
// creates the Secret; the shape fixes the path, the revision and read-only.
type Projection struct {
	AttemptID  string `json:"attempt_id"`
	SecretRef  string `json:"secret_ref"`
	SecretName string `json:"secret_name"`
	FileName   string `json:"file_name"`
	Revision   uint64 `json:"revision"`
	Digest     string `json:"sha256"`
	MountPath  string `json:"mount_path"`
	VolumeName string `json:"volume_name"`
	ReadOnly   bool   `json:"read_only"`
}

// AttemptBinding freezes the exact configuration revision an attempt first
// projected. A later rotation is visible only to a new attempt.
type AttemptBinding struct {
	AttemptID string   `json:"attempt_id"`
	Scope     Scope    `json:"scope"`
	Revision  Revision `json:"revision"`
}

// AuditEntry records one configuration write without content.
type AuditEntry struct {
	At          time.Time `json:"at"`
	Actor       string    `json:"actor"`
	Action      string    `json:"action"`
	ServerID    string    `json:"server_id"`
	WorkspaceID string    `json:"workspace_id"`
	Backend     string    `json:"backend"`
	Profile     string    `json:"profile,omitempty"`
	Revision    uint64    `json:"revision"`
	Result      string    `json:"result"`
	Detail      string    `json:"detail,omitempty"`
}

// Errors the management face answers with.
var (
	ErrNotFound = errors.New("agent configuration is not registered")
)

var (
	idPattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	fileNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}\.json$`)
)

// ValidateScope checks the identity fields.
func ValidateScope(scope Scope) (Scope, error) {
	scope.ServerID = strings.TrimSpace(scope.ServerID)
	scope.WorkspaceID = strings.TrimSpace(scope.WorkspaceID)
	scope.Backend = strings.TrimSpace(scope.Backend)
	scope.Profile = strings.TrimSpace(scope.Profile)
	for name, value := range map[string]string{
		"server_id": scope.ServerID, "workspace_id": scope.WorkspaceID, "backend": scope.Backend,
	} {
		if !idPattern.MatchString(value) {
			return Scope{}, fmt.Errorf("%s must be 1-64 characters of letters, digits, '.', '_' or '-'", name)
		}
	}
	if scope.Profile != "" && !idPattern.MatchString(scope.Profile) {
		return Scope{}, errors.New("profile must be a bare identifier")
	}
	return scope, nil
}

// ValidateWrite checks one submitted document. It requires a JSON object and
// an explicit file name and Secret reference; it never echoes the content.
func ValidateWrite(request WriteRequest) (WriteRequest, []byte, error) {
	request.FileName = strings.TrimSpace(request.FileName)
	request.SecretRef = strings.TrimSpace(request.SecretRef)
	if request.FileName == "" {
		request.FileName = FileName
	}
	if !fileNamePattern.MatchString(request.FileName) {
		return WriteRequest{}, nil, errors.New("file_name must be a single .json file name")
	}
	if !idPattern.MatchString(request.SecretRef) {
		return WriteRequest{}, nil, errors.New("secret_ref must be a bare reference, not a secret value")
	}
	if len(request.Config) == 0 {
		return WriteRequest{}, nil, errors.New("config is required")
	}
	if len(request.Config) > MaxConfigBytes {
		return WriteRequest{}, nil, fmt.Errorf("config exceeds %d bytes", MaxConfigBytes)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(request.Config, &object); err != nil || object == nil {
		return WriteRequest{}, nil, errors.New("config must be a JSON object")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return WriteRequest{}, nil, errors.New("config must be a JSON object")
	}
	return request, canonical, nil
}
