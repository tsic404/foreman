// TC-tech-multiws-04 implements AC-84: node-local state is judged per
// workspace — a cleanup round removes only expired entries and never crosses
// into another workspace's tree.
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

func ageTree(t *testing.T, root string, at time.Time) {
	t.Helper()
	if err := filepath.Walk(root, func(path string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return os.Chtimes(path, at, at)
	}); err != nil {
		t.Fatalf("age %s: %v", root, err)
	}
}

func TestTechMultiWS04NodeStatePartitioned(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-2 * time.Hour)
	fresh := time.Now()

	write := func(rel string, at time.Time) {
		t.Helper()
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatalf("chtimes %s: %v", rel, err)
		}
	}
	// The directory's own mtime is what the task-dir / repo rules read.
	touchDir := func(rel string, at time.Time) {
		t.Helper()
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatalf("chtimes %s: %v", rel, err)
		}
	}

	write("workspaces/ws-a/task-old/file", old)
	touchDir("workspaces/ws-a/task-old", old)
	write("workspaces/ws-a/task-fresh/file", fresh)
	touchDir("workspaces/ws-a/task-fresh", fresh)
	write("workspaces/ws-b/task-old/file", old)
	touchDir("workspaces/ws-b/task-old", old)
	write("workspaces/ws-b/task-fresh/file", fresh)
	touchDir("workspaces/ws-b/task-fresh", fresh)

	write("workspaces/.repos/ws-a/repo.git/objects/pack", old)
	ageTree(t, filepath.Join(root, "workspaces/.repos/ws-a/repo.git"), old)
	write("workspaces/.repos/ws-b/repo.git/objects/pack", fresh)
	touchDir("workspaces/.repos/ws-b/repo.git", fresh)

	// Workspace-partitioned home roots (§8.8): each workspace's sessions are
	// judged independently.
	write("home/ws-a/.multica/profiles/p/hermes-sessions/old/file", old)
	touchDir("home/ws-a/.multica/profiles/p/hermes-sessions/old", old)
	write("home/ws-b/.multica/profiles/p/hermes-sessions/fresh/file", fresh)
	touchDir("home/ws-b/.multica/profiles/p/hermes-sessions/fresh", fresh)

	collector := gc.New(gc.Config{
		StateRoot:     root,
		Node:          "node-1",
		CacheTTL:      time.Hour,
		TaskDirTTL:    time.Hour,
		CacheMaxBytes: 1 << 30,
		Quiesce:       0,
		AgentStateTTL: time.Hour,
		Interval:      time.Hour,
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))

	stats, err := collector.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.TaskDirs < 2 {
		t.Errorf("task dirs removed = %d, want the expired entry of each workspace", stats.TaskDirs)
	}
	if stats.BareRepos < 1 {
		t.Errorf("bare repos removed = %d, want A's expired cache", stats.BareRepos)
	}
	if stats.AgentState < 1 {
		t.Errorf("agent states removed = %d, want A's expired session store", stats.AgentState)
	}

	exists := func(rel string) bool {
		_, err := os.Stat(filepath.Join(root, rel))
		return err == nil
	}
	for _, gone := range []string{
		"workspaces/ws-a/task-old",
		"workspaces/ws-b/task-old",
		"workspaces/.repos/ws-a/repo.git",
		"home/ws-a/.multica/profiles/p/hermes-sessions/old",
	} {
		if exists(gone) {
			t.Errorf("%s survived despite exceeding its TTL", gone)
		}
	}
	for _, kept := range []string{
		"workspaces/ws-a/task-fresh",
		"workspaces/ws-b/task-fresh",
		"workspaces/.repos/ws-b/repo.git",
		"home/ws-b/.multica/profiles/p/hermes-sessions/fresh",
	} {
		if !exists(kept) {
			t.Errorf("%s was removed by another workspace's expired tree", kept)
		}
	}
}
