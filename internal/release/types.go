// Package release owns the CLI artifact registry, the release pointer and the
// pre-release impact snapshot (04-architecture.md §3.5, §5; 02-contracts.md
// V2-C04, V2-C05). It stores artifact metadata, never artifact bytes; the bytes
// are fetched only long enough to verify a digest and a platform header.
package release

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Kind separates the two release lines: the Multica CLI every managed runtime
// consumes, and an Agent CLI a backend adapter consumes.
type Kind string

const (
	KindMultica Kind = "multica"
	KindAgent   Kind = "agent"

	// DefaultCheckPeriod is the automatic check cadence a definition gets when
	// it does not name one.
	DefaultCheckPeriod = 6 * time.Hour
	// MinCheckPeriod keeps a version feed from being read every few seconds.
	MinCheckPeriod = 30 * time.Second
	// MaxCandidates bounds one definition's remembered candidate set.
	MaxCandidates = 32
	// MaxReleases bounds one definition's release history.
	MaxReleases = 32
	// MaxArtifactBytes bounds one verification download.
	MaxArtifactBytes = 64 << 20
)

// Platform is one artifact's target. Libc is optional because not every
// distribution declares one.
type Platform struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
	Libc string `json:"libc,omitempty"`
}

// Key is the stable identity of one platform entry.
func (p Platform) Key() string {
	key := strings.ToLower(p.OS) + "/" + strings.ToLower(p.Arch)
	if p.Libc != "" {
		key += "/" + strings.ToLower(p.Libc)
	}
	return key
}

// Artifact is one platform's download entry. Digest is the lowercase hex
// SHA-256 the verifier compares; Size 0 means "not declared".
type Artifact struct {
	Platform Platform `json:"platform"`
	URL      string   `json:"url"`
	Digest   string   `json:"sha256"`
	Size     int64    `json:"size,omitempty"`
}

// Candidate is one version offered by a version feed or entered manually.
type Candidate struct {
	Version   string     `json:"version"`
	Artifacts []Artifact `json:"artifacts"`
	// Manual marks a version entered through the management surface. An
	// automatic check replaces the feed's candidates but keeps these.
	Manual  bool      `json:"manual,omitempty"`
	AddedAt time.Time `json:"added_at"`
}

// Verification is one platform's verdict. Machine is the executable header's
// architecture, kept so an operator can see why a file was refused.
type Verification struct {
	Platform   Platform  `json:"platform"`
	OK         bool      `json:"ok"`
	Reason     string    `json:"reason,omitempty"`
	Digest     string    `json:"sha256,omitempty"`
	Size       int64     `json:"size,omitempty"`
	Machine    string    `json:"machine,omitempty"`
	VerifiedAt time.Time `json:"verified_at"`
}

// Release is one pointer value. The artifact list is the verified snapshot,
// so a later candidate cannot rewrite what an in-flight run resolved.
type Release struct {
	Version     string     `json:"version"`
	Generation  uint64     `json:"generation"`
	PublishedAt time.Time  `json:"published_at"`
	Artifacts   []Artifact `json:"artifacts"`
	Actor       string     `json:"actor"`
}

// Definition is one CLI release line. ServerID is the release domain: a CLI
// definition never spans two sources.
type Definition struct {
	ID                 string      `json:"cli_id"`
	Kind               Kind        `json:"kind"`
	Adapter            string      `json:"adapter,omitempty"`
	ServerID           string      `json:"server_id"`
	RequiredPlatforms  []Platform  `json:"required_platforms"`
	MinimumVersion     string      `json:"minimum_version,omitempty"`
	ManifestURL        string      `json:"manifest_url,omitempty"`
	AutoCheck          bool        `json:"auto_check"`
	CheckPeriodSeconds int         `json:"check_period_s"`
	AutoRelease        bool        `json:"auto_release"`
	Candidates         []Candidate `json:"candidates,omitempty"`
	Releases           []Release   `json:"releases,omitempty"`
	Current            *Release    `json:"current,omitempty"`
	// Verified carries versions whose artifacts were verified before; a
	// rollback publishes one of these without refetching the bytes.
	Verified map[string]VerifiedVersion `json:"verified,omitempty"`
	// CheckGeneration is the candidate-set generation a preview was taken
	// against. A publish must state the same value, or the preview is stale.
	CheckGeneration uint64    `json:"check_generation"`
	LastCheckAt     time.Time `json:"last_check_at,omitempty"`
	NextCheckAt     time.Time `json:"next_check_at,omitempty"`
	LastCheckError  string    `json:"last_check_error,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// VerifiedVersion is the immutable artifact set a version was verified with.
// Publishing the same version with different digests is refused: a version is
// one payload, not a mutable label.
type VerifiedVersion struct {
	Artifacts []Artifact     `json:"artifacts"`
	Checks    []Verification `json:"checks"`
	At        time.Time      `json:"at"`
}

// Settings is the mutable part of a definition. The two switches are
// independent: checking may be off while a manual release still works.
type Settings struct {
	AutoCheck          *bool `json:"auto_check"`
	CheckPeriodSeconds *int  `json:"check_period_s"`
	AutoRelease        *bool `json:"auto_release"`
}

// CheckReport is one check attempt's observable result.
type CheckReport struct {
	CLIID       string          `json:"cli_id"`
	Result      string          `json:"result"`
	Candidates  int             `json:"candidates"`
	Generation  uint64          `json:"check_generation"`
	AutoRelease bool            `json:"auto_release"`
	Publish     *PublishOutcome `json:"publish,omitempty"`
	Detail      string          `json:"detail,omitempty"`
	CheckedAt   time.Time       `json:"checked_at"`
}

// PublishResult is the release attempt's conclusion.
type PublishResult string

const (
	PublishPublished PublishResult = "published"
	PublishFailed    PublishResult = "failed"
	PublishConflict  PublishResult = "conflict"
)

// PublishOutcome is what a release request answers. A failed attempt names the
// pointer that stayed current.
type PublishOutcome struct {
	CLIID       string        `json:"cli_id"`
	Result      PublishResult `json:"result"`
	Version     string        `json:"version"`
	KeptVersion string        `json:"kept_version,omitempty"`
	Generation  uint64        `json:"generation,omitempty"`
	Published   *Release      `json:"published,omitempty"`
	Reasons     []string      `json:"reasons,omitempty"`
	At          time.Time     `json:"at"`
}

// DefinitionView is one management row.
type DefinitionView struct {
	CLIID              string    `json:"cli_id"`
	Kind               string    `json:"kind"`
	Adapter            string    `json:"adapter,omitempty"`
	ServerID           string    `json:"server_id"`
	RequiredPlatforms  []string  `json:"required_platforms"`
	MinimumVersion     string    `json:"minimum_version,omitempty"`
	CurrentVersion     string    `json:"current_version,omitempty"`
	CurrentGeneration  uint64    `json:"current_generation"`
	AutoCheck          bool      `json:"auto_check"`
	CheckPeriodSeconds int       `json:"check_period_s"`
	AutoRelease        bool      `json:"auto_release"`
	CheckGeneration    uint64    `json:"check_generation"`
	Candidates         []string  `json:"candidates"`
	Releases           []Release `json:"releases"`
	LastCheckAt        string    `json:"last_check_at,omitempty"`
	NextCheckAt        string    `json:"next_check_at,omitempty"`
	LastCheckError     string    `json:"last_check_error,omitempty"`
	UpdatedAt          string    `json:"updated_at,omitempty"`
}

// ImpactRequest asks what a candidate changes at the next start. The
// generation is the check generation the caller saw; the service refuses a
// preview over a candidate set that moved.
type ImpactRequest struct {
	CLIID              string
	Version            string
	ExpectedGeneration uint64
	// ExpectedReleaseGeneration is the release pointer the caller previewed.
	// The publish must still match it or the preview is stale.
	ExpectedReleaseGeneration uint64
}

// ImpactWorkspace is one workspace that will resolve the candidate next.
type ImpactWorkspace struct {
	WorkspaceID string `json:"workspace_id"`
	Name        string `json:"name"`
}

// ImpactAgent is one managed Agent that will resolve the candidate next.
type ImpactAgent struct {
	WorkspaceID    string `json:"workspace_id"`
	WorkspaceName  string `json:"workspace_name"`
	AgentID        string `json:"agent_id"`
	AgentName      string `json:"agent_name"`
	RuntimeID      string `json:"runtime_id"`
	Adapter        string `json:"adapter,omitempty"`
	CurrentVersion string `json:"current_version,omitempty"`
}

// ImpactRun is one in-flight attempt that keeps the version it started with.
// VersionKnown=false means the claim did not record a verified release fact;
// the preview must show unknown rather than "no release".
type ImpactRun struct {
	TaskID              string            `json:"task_id"`
	WorkspaceID         string            `json:"workspace_id"`
	AgentID             string            `json:"agent_id"`
	AgentName           string            `json:"agent_name"`
	Version             string            `json:"version,omitempty"`
	VersionKnown        bool              `json:"version_known"`
	Digests             map[string]string `json:"digests,omitempty"`
	ConfigRevision      uint64            `json:"config_revision,omitempty"`
	ConfigRevisionKnown bool              `json:"config_revision_known"`
	StartedAt           string            `json:"started_at,omitempty"`
}

// ImpactView is the read-only preview. Known=false means the reverse lookup
// could not be trusted: AffectedAgents is then null, never a displayed zero.
type ImpactView struct {
	CLIID          string `json:"cli_id"`
	ServerID       string `json:"server_id"`
	Kind           string `json:"kind"`
	Adapter        string `json:"adapter,omitempty"`
	Candidate      string `json:"candidate_version"`
	CurrentVersion string `json:"current_version,omitempty"`
	// ReleaseGeneration is the pointer generation the preview observed.
	ReleaseGeneration uint64            `json:"release_generation"`
	Known             bool              `json:"known"`
	AffectedAgents    *int              `json:"affected_agents"`
	Unknown           []string          `json:"unknown,omitempty"`
	Workspaces        []ImpactWorkspace `json:"workspaces"`
	Agents            []ImpactAgent     `json:"agents"`
	InFlight          []ImpactRun       `json:"in_flight"`
}

// SourceDirectory is the reverse-lookup input for one source. It is the
// synced directory, not the live upstream: a stale read must be visible as
// unknown.
type SourceDirectory struct {
	ServerID   string
	Synced     bool
	Stale      bool
	Detail     string
	Workspaces []DirectoryWorkspace
	Agents     []DirectoryAgent
}

// DirectoryWorkspace is one workspace row.
type DirectoryWorkspace struct {
	ID   string
	Name string
}

// DirectoryAgent is one Agent row with the backend its runtime declares.
type DirectoryAgent struct {
	WorkspaceID   string
	WorkspaceName string
	AgentID       string
	AgentName     string
	RuntimeID     string
	Adapter       string
	Managed       bool
}

// Directory reads one source's synced rows.
type Directory interface {
	ReadSource(ctx context.Context, serverID string) (SourceDirectory, bool)
}

// RunFact is one in-flight attempt. Adapter is the backend provider the claim
// was dispatched under; empty means the row predates the dispatch record.
// The version/digest/revision fields are the facts frozen at claim time, not
// values inferred from the release history when the preview is read.
type RunFact struct {
	TaskID         string
	ServerID       string
	WorkspaceID    string
	AgentID        string
	AgentName      string
	Adapter        string
	MulticaVersion string
	MulticaDigests map[string]string
	AgentVersion   string
	AgentDigests   map[string]string
	CLIKnown       bool
	AgentKnown     bool
	ConfigRevision uint64
	ConfigKnown    bool
	StartedAt      time.Time
}

// RunIndex reads the coordinator's in-flight attempts.
type RunIndex interface {
	Running(ctx context.Context) []RunFact
}

// Errors the management face answers with.
var (
	ErrNotFound         = errors.New("CLI definition is not registered")
	ErrVersionNotFound  = errors.New("candidate version is not registered")
	ErrArtifactNotFound = errors.New("verified artifact is not published")
	ErrGenerationStale  = errors.New("candidate set changed after the preview")
	ErrReleaseStale     = errors.New("release pointer changed after the preview")
	ErrVersionImmutable = errors.New("version is already verified with different artifacts")
)

var (
	cliIDPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	adapterPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	digestPattern  = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

// Validation helpers.

// ValidatePlatform checks the platform spellings the verifier can decide.
func ValidatePlatform(p Platform) error {
	switch strings.ToLower(p.OS) {
	case "linux", "darwin":
	default:
		return fmt.Errorf("platform os %q is not supported", p.OS)
	}
	switch strings.ToLower(p.Arch) {
	case "amd64", "arm64":
	default:
		return fmt.Errorf("platform arch %q is not supported", p.Arch)
	}
	return nil
}

// ValidateArtifact checks one download entry.
func ValidateArtifact(a Artifact) error {
	if err := ValidatePlatform(a.Platform); err != nil {
		return err
	}
	if !digestPattern.MatchString(strings.ToLower(a.Digest)) {
		return errors.New("sha256 must be 64 lowercase hex characters")
	}
	if err := validateHTTPURL(a.URL); err != nil {
		return err
	}
	if a.Size < 0 {
		return errors.New("artifact size must not be negative")
	}
	return nil
}

// validateHTTPURL admits only an absolute http(s) URL without embedded
// credentials. The fetcher repeats this check because a registration body is
// not the only way an artifact URL can reach it.
func validateHTTPURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" {
		return errors.New("url must be absolute")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("url must be http(s)")
	}
	if parsed.User != nil {
		return errors.New("url must not carry credentials")
	}
	return nil
}

// validateArtifactSet refuses two entries for one platform: a release line
// must name one payload per platform, not an ambiguous pair.
func validateArtifactSet(artifacts []Artifact) error {
	seen := map[string]bool{}
	for _, artifact := range artifacts {
		if err := ValidateArtifact(artifact); err != nil {
			return err
		}
		key := artifact.Platform.Key()
		if seen[key] {
			return fmt.Errorf("artifact for %s is declared twice", key)
		}
		seen[key] = true
	}
	return nil
}

// ValidateDefinition checks the registration body.
func ValidateDefinition(d Definition) (Definition, error) {
	id := strings.TrimSpace(d.ID)
	if !cliIDPattern.MatchString(id) {
		return Definition{}, errors.New("cli_id must be 1-64 characters of letters, digits, '.', '_' or '-'")
	}
	if d.Kind != KindMultica && d.Kind != KindAgent {
		return Definition{}, errors.New("kind must be multica or agent")
	}
	if strings.TrimSpace(d.ServerID) == "" {
		return Definition{}, errors.New("server_id is required")
	}
	adapter := strings.TrimSpace(d.Adapter)
	if d.Kind == KindAgent {
		if adapter == "" || !adapterPattern.MatchString(adapter) {
			return Definition{}, errors.New("an agent CLI needs a bare adapter name")
		}
	}
	if len(d.RequiredPlatforms) == 0 {
		return Definition{}, errors.New("at least one required platform is needed")
	}
	seen := map[string]bool{}
	platforms := make([]Platform, 0, len(d.RequiredPlatforms))
	for _, platform := range d.RequiredPlatforms {
		if err := ValidatePlatform(platform); err != nil {
			return Definition{}, err
		}
		key := platform.Key()
		if seen[key] {
			return Definition{}, fmt.Errorf("required platform %s is declared twice", key)
		}
		seen[key] = true
		platforms = append(platforms, Platform{OS: strings.ToLower(platform.OS), Arch: strings.ToLower(platform.Arch), Libc: strings.ToLower(platform.Libc)})
	}
	if d.ManifestURL != "" {
		if err := validateHTTPURL(d.ManifestURL); err != nil {
			return Definition{}, fmt.Errorf("manifest_url: %w", err)
		}
	}
	if d.CheckPeriodSeconds <= 0 {
		d.CheckPeriodSeconds = int(DefaultCheckPeriod / time.Second)
	}
	if time.Duration(d.CheckPeriodSeconds)*time.Second < MinCheckPeriod {
		return Definition{}, fmt.Errorf("check period must be at least %s", MinCheckPeriod)
	}
	return Definition{
		ID:                 id,
		Kind:               d.Kind,
		Adapter:            adapter,
		ServerID:           strings.TrimSpace(d.ServerID),
		RequiredPlatforms:  platforms,
		MinimumVersion:     strings.TrimSpace(d.MinimumVersion),
		ManifestURL:        strings.TrimSpace(d.ManifestURL),
		AutoCheck:          d.AutoCheck,
		CheckPeriodSeconds: d.CheckPeriodSeconds,
		AutoRelease:        d.AutoRelease,
	}, nil
}

// versionLess compares dotted numeric versions. A non-numeric segment falls
// back to a string comparison, which is enough for the minimum-version gate.
func versionLess(a, b string) bool {
	as := strings.Split(strings.TrimPrefix(a, "v"), ".")
	bs := strings.Split(strings.TrimPrefix(b, "v"), ".")
	for i := 0; i < len(as) || i < len(bs); i++ {
		var av, bv string
		if i < len(as) {
			av = as[i]
		}
		if i < len(bs) {
			bv = bs[i]
		}
		ai, aerr := parseDigits(av)
		bi, berr := parseDigits(bv)
		if aerr == nil && berr == nil {
			if ai != bi {
				return ai < bi
			}
			continue
		}
		if av != bv {
			return av < bv
		}
	}
	return false
}

func parseDigits(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	value := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, errors.New("not numeric")
		}
		value = value*10 + int(r-'0')
	}
	return value, nil
}
