// Command foreman-job is the fixed foreman-job's entry point. It runs the
// three-stage attempt contract inside the Job container: prepare the frozen
// Multica and Agent CLIs, wait for the frozen rsbx session and verify the
// daemon registration, then dispatch once and settle the remote close.
// 02-contracts.md V2-C03/C04, V2-C08–C12.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/tsic404/foreman/internal/attempt"
)

const (
	defaultSpecFile   = "/run/foreman/cred/attempt.json"
	defaultStoreFile  = "/run/foreman/state/attempt-state.json"
	defaultCLIDir     = "/run/foreman/clis"
	defaultTokenFile  = "/run/foreman/cred/config.json"
	defaultRuntimeBin = "/run/foreman/runtime"
	envSpecFile       = "FOREMAN_JOB_SPEC_FILE"
	envStoreFile      = "FOREMAN_JOB_STORE_FILE"
	envCLIDir         = "FOREMAN_JOB_CLI_DIR"
	envTokenFile      = "FOREMAN_JOB_TOKEN_FILE"
	envProxyURL       = "FOREMAN_JOB_PROXY_URL"
	envCLIBudget      = "FOREMAN_JOB_CLI_BUDGET_BYTES"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := run(ctx, log); err != nil {
		log.Error("attempt.failed", "err", err.Error())
		switch {
		case errors.Is(err, attempt.ErrRefused):
			os.Exit(2)
		case errors.Is(err, attempt.ErrInterrupted):
			os.Exit(3)
		default:
			os.Exit(1)
		}
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	specPath := envOrDefault(envSpecFile, defaultSpecFile)
	body, err := os.ReadFile(specPath)
	if err != nil {
		return fmt.Errorf("read the attempt spec: %w", err)
	}
	var binding attempt.Binding
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&binding); err != nil {
		return fmt.Errorf("decode the attempt spec: %w", err)
	}

	store, err := attempt.NewFileStore(envOrDefault(envStoreFile, defaultStoreFile))
	if err != nil {
		return err
	}
	cache, err := cliCache()
	if err != nil {
		return err
	}
	supervisor := &attempt.Supervisor{
		Cache:     cache,
		Remote:    attempt.ExecRemote{Path: helperPath(attempt.EnvRSBXHelper)},
		Registrar: attempt.ExecRegistrar{Path: helperPath(attempt.EnvRegisterHelper)},
		Runner:    attempt.ExecRunner{Path: helperPath(attempt.EnvRunnerHelper)},
		Slots:     attempt.ExecSlots{Path: helperPath(attempt.EnvSlotHelper)},
		Store:     store,
		Logger:    log,
	}
	outcome, err := supervisor.Run(ctx, binding)
	encoded, encodeErr := json.Marshal(outcome)
	if encodeErr == nil {
		log.Info("attempt.outcome", "outcome", string(encoded))
	}
	return err
}

func helperPath(key string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value != "" {
		return value
	}
	switch key {
	case attempt.EnvCLIHelper:
		return defaultRuntimeBin + "/foreman-job-cli"
	case attempt.EnvRSBXHelper:
		return defaultRuntimeBin + "/rsbx-remote"
	case attempt.EnvRegisterHelper:
		return defaultRuntimeBin + "/foreman-job-register"
	case attempt.EnvRunnerHelper:
		return defaultRuntimeBin + "/foreman-job-runner"
	case attempt.EnvSlotHelper:
		return defaultRuntimeBin + "/foreman-job-slots"
	default:
		return ""
	}
}

// cliCache prefers the deployment's CLI adapter when one is shipped beside the
// supervisor. The reference image always ships it, so the fallback exists for
// a deployment that deliberately runs a supervisor without adapters: it then
// fetches the artifact itself, with the same frozen reference and the same
// digest check. The two paths answer the same observable facts.
func cliCache() (attempt.Cache, error) {
	path := helperPath(attempt.EnvCLIHelper)
	if info, err := os.Stat(path); err == nil && !info.IsDir() && info.Mode().Perm()&0o111 != 0 {
		return attempt.ExecCache{Path: path}, nil
	}
	return proxyCache()
}

// proxyCache is the only CLI source the Job has. It reads the attempt's Job
// token from the credential Secret, never a deployment-wide credential, and
// keeps every download inside the frozen Foreman proxy origin.
func proxyCache() (*attempt.ProxyCache, error) {
	proxyURL := strings.TrimSpace(os.Getenv(envProxyURL))
	if proxyURL == "" {
		return nil, fmt.Errorf("%s is required: the Job has no direct-source or preinstalled-CLI fallback", envProxyURL)
	}
	token, err := readJobToken(envOrDefault(envTokenFile, defaultTokenFile))
	if err != nil {
		return nil, err
	}
	budget := attempt.DefaultCLIBudgetBytes
	if raw := strings.TrimSpace(os.Getenv(envCLIBudget)); raw != "" {
		value, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil || value <= 0 {
			return nil, fmt.Errorf("%s must be a positive byte count, got %q", envCLIBudget, raw)
		}
		budget = value
	}
	return &attempt.ProxyCache{
		BaseURL: proxyURL, Token: token,
		Dir:      envOrDefault(envCLIDir, defaultCLIDir),
		MaxBytes: budget, Attempts: 2,
	}, nil
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
	return credential.Token, nil
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
