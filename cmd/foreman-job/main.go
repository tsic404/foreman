// Command foreman-job is the fixed foreman-job's entry point. It runs the
// three-stage attempt contract inside the Job container: prepare the frozen
// Multica and Agent CLIs, wait for the frozen rsbx session and verify the
// daemon registration, then dispatch once and settle the remote close.
// 02-contracts.md V2-C03/C04, 04-architecture.md §6.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/tsic404/foreman/internal/attempt"
)

const (
	defaultSpecFile  = "/run/foreman/cred/attempt.json"
	defaultStoreFile = "/run/foreman/state/attempt-state.json"
	envSpecFile      = "FOREMAN_JOB_SPEC_FILE"
	envStoreFile     = "FOREMAN_JOB_STORE_FILE"
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
	supervisor := &attempt.Supervisor{
		Cache:     attempt.ExecCache{Path: helperPath(attempt.EnvCLIHelper)},
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
		return "/usr/local/bin/foreman-job-cli"
	case attempt.EnvRSBXHelper:
		return "/usr/local/bin/rsbx-remote"
	case attempt.EnvRegisterHelper:
		return "/usr/local/bin/foreman-job-register"
	case attempt.EnvRunnerHelper:
		return "/usr/local/bin/foreman-job-runner"
	case attempt.EnvSlotHelper:
		return "/usr/local/bin/foreman-job-slots"
	default:
		return ""
	}
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
