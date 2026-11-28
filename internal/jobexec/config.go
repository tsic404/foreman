package jobexec

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tsic404/foreman/internal/attempt"
)

// Environment keys the adapters read. The render sets the paths; the two
// timeouts and the rsbx entry are the reference adapter's own knobs, so a
// deployment that keeps the default adapters can tune them without rebuilding.
const (
	EnvControlURL      = "FOREMAN_JOB_CONTROL_URL"
	EnvProxyURL        = "FOREMAN_JOB_PROXY_URL"
	EnvTokenFile       = "FOREMAN_JOB_TOKEN_FILE"
	EnvCLIDir          = "FOREMAN_JOB_CLI_DIR"
	EnvCLIBudget       = "FOREMAN_JOB_CLI_BUDGET_BYTES"
	EnvSessionDir      = "FOREMAN_JOB_SESSION_DIR"
	EnvRsbxBinary      = "FOREMAN_JOB_RSBX_BIN"
	EnvRsbxEntry       = "FOREMAN_JOB_RSBX_ENTRY"
	EnvDaemonProfile   = "FOREMAN_JOB_DAEMON_PROFILE"
	EnvDeliveryTimeout = "FOREMAN_JOB_DELIVERY_TIMEOUT"
	EnvRegisterTimeout = "FOREMAN_JOB_REGISTRATION_TIMEOUT"
	EnvSessionSettle   = "FOREMAN_JOB_RSBX_SETTLE"
	EnvSessionStop     = "FOREMAN_JOB_RSBX_STOP_GRACE"
	EnvReadyTimeout    = "FOREMAN_JOB_READY_TIMEOUT"
	// EnvTLSDir names the directory the frozen TLS references resolve under.
	// Unset means the deployment authenticates the carrier through its own
	// channel (an ssh host key or a unix socket's permissions) and the
	// reference is a deployer-side input; set means the reference must resolve
	// to a parseable certificate bundle before any session opens.
	EnvTLSDir = "FOREMAN_JOB_TLS_DIR"

	// Default paths mirror the fixed render's. They are the reference
	// deployment's layout, not a contract: a custom adapter set may use its
	// own, and the render passes the paths explicitly anyway.
	defaultTokenFile  = "/run/foreman/cred/config.json"
	defaultCLIDir     = "/run/foreman/clis"
	defaultStateFile  = "/run/foreman/state/attempt-state.json"
	defaultRsbxBinary = "/usr/local/bin/rsbx"
	defaultHome       = "/run/foreman/home"
	defaultProfile    = "foreman-job"
	defaultTLSDir     = "/run/foreman/tls"

	defaultDeliveryTimeout  = 10 * time.Minute
	defaultRegisterTimeout  = 5 * time.Minute
	defaultSessionSettle    = 5 * time.Second
	defaultSessionStopGrace = 20 * time.Second
	defaultReadyTimeout     = 30 * time.Second
)

// Settings is the adapter process's resolved configuration.
type Settings struct {
	ControlURL string
	// ProxyURL is the frozen Foreman proxy origin the CLI artifacts come
	// from. It defaults to ControlURL: the reference render serves both faces
	// from one listener.
	ProxyURL   string
	Token      string
	CLIDir     string
	CLIBudget  int64
	SessionDir string
	RsbxBinary string
	// Home is the Job container's HOME: the daemon resolves its profile
	// under <Home>/.multica/profiles/<profile>, inside the per-Job volume.
	Home string
	// RsbxEntry overrides the session entry; empty runs the official
	// Multica CLI daemon from CLIDir.
	RsbxEntry         []string
	DaemonProfile     string
	DeliveryTimeout   time.Duration
	RegistrationGrace time.Duration
	SessionSettle     time.Duration
	SessionStopGrace  time.Duration
	ReadyTimeout      time.Duration
	TLSDir            string
}

// LoadSettings resolves the adapter configuration from the process
// environment. The Job credential and the per-attempt directories are
// required; the control address is resolved lazily so the CLI adapter keeps
// working in a deployment that only exposes the download face.
func LoadSettings() (Settings, error) {
	tokenFile := strings.TrimSpace(os.Getenv(EnvTokenFile))
	if tokenFile == "" {
		tokenFile = defaultTokenFile
	}
	token, err := readJobToken(tokenFile)
	if err != nil {
		return Settings{}, err
	}
	controlURL := strings.TrimSpace(os.Getenv(EnvControlURL))
	if controlURL == "" {
		controlURL = strings.TrimSpace(os.Getenv("MULTICA_SERVER_URL"))
	}
	settings := Settings{
		ControlURL:        strings.TrimRight(controlURL, "/"),
		ProxyURL:          strings.TrimRight(strings.TrimSpace(os.Getenv(EnvProxyURL)), "/"),
		Token:             token,
		CLIDir:            strings.TrimSpace(os.Getenv(EnvCLIDir)),
		CLIBudget:         attempt.DefaultCLIBudgetBytes,
		RsbxBinary:        strings.TrimSpace(os.Getenv(EnvRsbxBinary)),
		DaemonProfile:     strings.TrimSpace(os.Getenv(EnvDaemonProfile)),
		DeliveryTimeout:   defaultDeliveryTimeout,
		RegistrationGrace: defaultRegisterTimeout,
		SessionSettle:     defaultSessionSettle,
		SessionStopGrace:  defaultSessionStopGrace,
		ReadyTimeout:      defaultReadyTimeout,
		TLSDir:            defaultTLSDir,
		Home:              strings.TrimSpace(os.Getenv("HOME")),
	}
	if settings.ProxyURL == "" {
		settings.ProxyURL = settings.ControlURL
	}
	if settings.Home == "" {
		settings.Home = defaultHome
	}
	if settings.CLIDir == "" {
		settings.CLIDir = defaultCLIDir
	}
	settings.SessionDir = strings.TrimSpace(os.Getenv(EnvSessionDir))
	if settings.SessionDir == "" {
		store := strings.TrimSpace(os.Getenv("FOREMAN_JOB_STORE_FILE"))
		if store == "" {
			store = defaultStateFile
		}
		settings.SessionDir = filepath.Join(filepath.Dir(store), "session")
	}
	if settings.RsbxBinary == "" {
		settings.RsbxBinary = defaultRsbxBinary
	}
	if settings.DaemonProfile == "" {
		settings.DaemonProfile = defaultProfile
	}
	if value := strings.TrimSpace(os.Getenv(EnvTLSDir)); value != "" {
		settings.TLSDir = value
	}
	if raw := strings.TrimSpace(os.Getenv(EnvCLIBudget)); raw != "" {
		value, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil || value <= 0 {
			return Settings{}, fmt.Errorf("%s must be a positive byte count, got %q", EnvCLIBudget, raw)
		}
		settings.CLIBudget = value
	}
	if raw := strings.TrimSpace(os.Getenv(EnvDeliveryTimeout)); raw != "" {
		value, parseErr := time.ParseDuration(raw)
		if parseErr != nil || value <= 0 {
			return Settings{}, fmt.Errorf("%s must be a positive duration, got %q", EnvDeliveryTimeout, raw)
		}
		settings.DeliveryTimeout = value
	}
	if raw := strings.TrimSpace(os.Getenv(EnvRegisterTimeout)); raw != "" {
		value, parseErr := time.ParseDuration(raw)
		if parseErr != nil || value <= 0 {
			return Settings{}, fmt.Errorf("%s must be a positive duration, got %q", EnvRegisterTimeout, raw)
		}
		settings.RegistrationGrace = value
	}
	if raw := strings.TrimSpace(os.Getenv(EnvSessionSettle)); raw != "" {
		value, parseErr := time.ParseDuration(raw)
		if parseErr != nil || value < 0 {
			return Settings{}, fmt.Errorf("%s must be a non-negative duration, got %q", EnvSessionSettle, raw)
		}
		settings.SessionSettle = value
	}
	if raw := strings.TrimSpace(os.Getenv(EnvSessionStop)); raw != "" {
		value, parseErr := time.ParseDuration(raw)
		if parseErr != nil || value <= 0 {
			return Settings{}, fmt.Errorf("%s must be a positive duration, got %q", EnvSessionStop, raw)
		}
		settings.SessionStopGrace = value
	}
	if raw := strings.TrimSpace(os.Getenv(EnvReadyTimeout)); raw != "" {
		value, parseErr := time.ParseDuration(raw)
		if parseErr != nil || value <= 0 {
			return Settings{}, fmt.Errorf("%s must be a positive duration, got %q", EnvReadyTimeout, raw)
		}
		settings.ReadyTimeout = value
	}
	if raw := strings.TrimSpace(os.Getenv(EnvRsbxEntry)); raw != "" {
		var entry []string
		if err := json.Unmarshal([]byte(raw), &entry); err != nil || len(entry) == 0 {
			return Settings{}, fmt.Errorf("%s must be a non-empty JSON string array, got %q", EnvRsbxEntry, raw)
		}
		settings.RsbxEntry = entry
	}
	return settings, nil
}

// Control builds the projection client for the control-facing adapters. An
// adapter that cannot reach Foreman cannot answer any of the four
// capabilities truthfully, so the absence of an address is an error rather
// than a guess.
func (s Settings) Control() (*Control, error) {
	if strings.TrimSpace(s.ControlURL) == "" {
		return nil, fmt.Errorf("%s (or MULTICA_SERVER_URL) is required: the adapter answers through the Job face", EnvControlURL)
	}
	return NewControl(s.ControlURL, s.Token), nil
}

// Entry is the session's entry program: the deployment's override, or the
// official Multica CLI daemon the render downloaded into CLIDir.
func (s Settings) Entry() []string {
	if len(s.RsbxEntry) > 0 {
		return append([]string(nil), s.RsbxEntry...)
	}
	return []string{
		filepath.Join(s.CLIDir, "multica"), "daemon", "start",
		"--foreground", "--profile", s.DaemonProfile,
	}
}

// AgentPath is the selected Agent CLI the daemon starts. The reference
// deployment downloads exactly one Agent artifact; a backend that names its
// own launcher variable keeps working through the profile it was given.
func (s Settings) AgentPath() string { return filepath.Join(s.CLIDir, "agent") }

// ProfileDir is where the reference daemon resolves the per-Job profile. The
// token lives in the Job's own volume, so the profile state dies with the
// Pod and is never reused across tasks.
func (s Settings) ProfileDir() string {
	return filepath.Join(s.Home, ".multica", "profiles", s.DaemonProfile)
}

func readJobToken(path string) (string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read the Job credential: %w", err)
	}
	var credential struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(body, &credential); err != nil {
		return "", fmt.Errorf("decode the Job credential: %w", err)
	}
	if strings.TrimSpace(credential.Token) == "" {
		return "", errors.New("the Job credential carries no token")
	}
	return strings.TrimSpace(credential.Token), nil
}
