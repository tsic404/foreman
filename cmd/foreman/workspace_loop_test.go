package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
)

// TestIsolateWorkspaceLoop pins AC-86's runtime face: every failure — a
// 401/403 or C-17 mismatch included — stays inside its workspace and is
// retried. Only the startup self-check fails fast; ending the process here
// would stop the other workspaces' heartbeats, claims and subscriptions
// (§1.4, §1.5).
func TestIsolateWorkspaceLoop(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	transient := errors.New("upstream timeout")

	t.Run("transient failure retries then succeeds", func(t *testing.T) {
		calls := 0
		err := isolateWorkspaceLoop(context.Background(), "claim", log, time.Millisecond, func(context.Context) error {
			calls++
			if calls < 3 {
				return transient
			}
			return nil
		})
		if err != nil || calls != 3 {
			t.Fatalf("loop = %v after %d calls, want nil after the third", err, calls)
		}
	})

	for name, runtimeErr := range map[string]error{
		"credential rejected": &proxy.StatusError{Code: 403},
		"workspace mismatch":  &proxy.WorkspaceMismatchError{Configured: "ws-a", Visible: []string{"ws-b"}},
	} {
		t.Run(name+" is retried in isolation", func(t *testing.T) {
			calls := 0
			err := isolateWorkspaceLoop(context.Background(), "claim", log, time.Millisecond, func(context.Context) error {
				calls++
				if calls < 2 {
					return runtimeErr
				}
				return nil
			})
			if err != nil || calls != 2 {
				t.Fatalf("loop = %v after %d calls, want a retry that then succeeds", err, calls)
			}
		})
	}
}
