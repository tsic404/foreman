// TC-tech-cache-04 implements AC-22 on the production collector: the
// node-local agent state rule deletes session/transcript stores older than
// FOREMAN_AGENT_STATE_TTL and memory stores older than
// FOREMAN_AGENT_MEMORY_TTL, judged per entry by its own newest mtime; fresh
// entries and a 35-day memory entry (inside the 90-day memory window) survive,
// and a zero TTL turns each rule off. The design case's sample tree runs in a
// temp root here; the DaemonSet wiring (kubectl set env on foreman-gc) is the
// environment face.
package acceptance

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/gc"
)

// tcCache04Files is the sample tree of the design case, relative to the state
// root: the five state stores each carry an old and a fresh entry, hermes-state
// carries the three memory samples (95d / 35d / fresh), and the codex store
// carries one old entry.
var tcCache04Files = []string{
	"home/.multica/profiles/foreman-job/hermes-sessions/a/x/old/state.db",
	"home/.multica/profiles/foreman-job/hermes-sessions/a/x/new/state.db",
	"home/.multica/profiles/foreman-job/dsh-sessions/a/old/s.jsonl",
	"home/.multica/profiles/foreman-job/dsh-sessions/a/new/s.jsonl",
	"home/.multica/profiles/foreman-job/reasonix-state/a/old/s.jsonl",
	"home/.multica/profiles/foreman-job/reasonix-state/a/new/s.jsonl",
	"home/.multica/pi-sessions/old.jsonl",
	"home/.multica/pi-sessions/new.jsonl",
	"home/.codex/multica-sessions/a/old/s.jsonl",
	"home/.multica/profiles/foreman-job/hermes-state/a/x/old/mem.md",
	"home/.multica/profiles/foreman-job/hermes-state/b/y/new/mem.md",
	"home/.multica/profiles/foreman-job/hermes-state/c/z/mid/mem.md",
}

// tcCache04Age pushes one subtree's entries back by age, deepest first, so the
// store's newest mtime (the rule's judgement basis) is the aged value rather
// than the directory creation moment.
func tcCache04Age(t *testing.T, path string, age time.Duration) {
	t.Helper()
	when := time.Now().Add(-age)
	var paths []string
	err := filepath.Walk(path, func(entry string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		paths = append(paths, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", path, err)
	}
	for i := len(paths) - 1; i >= 0; i-- {
		if err := os.Chtimes(paths[i], when, when); err != nil {
			t.Fatalf("age %s: %v", paths[i], err)
		}
	}
}

// tcCache04Build creates the sample tree and ages the old entries.
func tcCache04Build(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, name := range tcCache04Files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte("sample\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	const day = 24 * time.Hour
	for _, name := range []string{
		"home/.multica/profiles/foreman-job/hermes-sessions/a/x/old",
		"home/.multica/profiles/foreman-job/dsh-sessions/a/old",
		"home/.multica/profiles/foreman-job/reasonix-state/a/old",
		"home/.multica/pi-sessions/old.jsonl",
		"home/.codex/multica-sessions/a/old",
	} {
		tcCache04Age(t, filepath.Join(root, filepath.FromSlash(name)), 35*day)
	}
	tcCache04Age(t, filepath.Join(root, "home/.multica/profiles/foreman-job/hermes-state/a"), 95*day)
	tcCache04Age(t, filepath.Join(root, "home/.multica/profiles/foreman-job/hermes-state/c"), 35*day)
	return root
}

func tcCache04Config(root string, stateTTL, memoryTTL time.Duration) gc.Config {
	return gc.Config{
		StateRoot:      root,
		CacheTTL:       720 * time.Hour,
		TaskDirTTL:     168 * time.Hour,
		CacheMaxBytes:  gc.DefaultCacheMaxBytes,
		Quiesce:        0,
		Interval:       time.Hour,
		AgentStateTTL:  stateTTL,
		AgentMemoryTTL: memoryTTL,
	}
}

func TestCache04AgentStateAndMemoryReclaim(t *testing.T) {
	root := tcCache04Build(t)
	collector := gc.New(tcCache04Config(root, gc.DefaultAgentStateTTL, gc.DefaultAgentMemoryTTL),
		slog.New(slog.NewJSONHandler(io.Discard, nil)))
	stats, err := collector.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	gone := []string{
		"home/.multica/profiles/foreman-job/hermes-sessions/a/x/old/state.db",
		"home/.multica/profiles/foreman-job/dsh-sessions/a/old/s.jsonl",
		"home/.multica/profiles/foreman-job/reasonix-state/a/old/s.jsonl",
		"home/.multica/pi-sessions/old.jsonl",
		"home/.codex/multica-sessions/a/old/s.jsonl",
		"home/.multica/profiles/foreman-job/hermes-state/a/x/old/mem.md",
	}
	for _, name := range gone {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(name))); !os.IsNotExist(err) {
			t.Errorf("%s still exists, want it reclaimed (expired entry)", name)
		}
	}
	kept := []string{
		"home/.multica/profiles/foreman-job/hermes-sessions/a/x/new/state.db",
		"home/.multica/profiles/foreman-job/dsh-sessions/a/new/s.jsonl",
		"home/.multica/profiles/foreman-job/reasonix-state/a/new/s.jsonl",
		"home/.multica/pi-sessions/new.jsonl",
		"home/.multica/profiles/foreman-job/hermes-state/b/y/new/mem.md",
		// 35 days: inside the 90-day memory window, outside the 14-day state
		// window — deleting it would mean the memory rule borrowed the state
		// TTL.
		"home/.multica/profiles/foreman-job/hermes-state/c/z/mid/mem.md",
	}
	for _, name := range kept {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(name))); err != nil {
			t.Errorf("%s was removed, want it kept: %v", name, err)
		}
	}
	if stats.AgentState != 5 || stats.AgentMemory != 1 {
		t.Errorf("stats = agent_state %d / agent_memory %d, want 5 / 1", stats.AgentState, stats.AgentMemory)
	}

	// The zero-TTL face: both rules are off and the same tree survives a
	// round untouched.
	zeroRoot := tcCache04Build(t)
	zero := gc.New(tcCache04Config(zeroRoot, 0, 0), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	zeroStats, err := zero.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce with zero TTLs: %v", err)
	}
	if zeroStats.AgentState != 0 || zeroStats.AgentMemory != 0 {
		t.Errorf("zero-TTL stats = %d / %d, want no reclaims", zeroStats.AgentState, zeroStats.AgentMemory)
	}
	for _, name := range gone {
		if _, err := os.Stat(filepath.Join(zeroRoot, filepath.FromSlash(name))); err != nil {
			t.Errorf("%s was removed with both TTLs at zero: %v", name, err)
		}
	}
}
