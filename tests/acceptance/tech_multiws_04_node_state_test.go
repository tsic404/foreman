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

// pathExists reports whether path is present.
func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// writeAt creates one file under root and stamps it with at.
func writeAt(t *testing.T, root, rel string, at time.Time) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
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

// TestTechMultiWS04SymlinkDoesNotCrossWorkspaces: a reclaim must not leave
// the workspace it was judging. ws-a's session store is a link into ws-b's
// task directory, so the state rule would judge ws-b's tree by its own
// retention and remove it while ws-b's rule keeps it. The link target
// survives in both TTL states.
func TestTechMultiWS04SymlinkDoesNotCrossWorkspaces(t *testing.T) {
	for _, tc := range []struct {
		name string
		age  time.Duration
	}{
		{"stale target", 400 * time.Hour},
		{"fresh target", time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeAt(t, root, "workspaces/ws-b/task-old/work/file.txt", time.Now().Add(-tc.age))
			ageTree(t, filepath.Join(root, "workspaces/ws-b/task-old"), time.Now().Add(-tc.age))

			link := filepath.Join(root, "home/ws-a/.multica/profiles/p/hermes-sessions")
			if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
				t.Fatalf("mkdir link parent: %v", err)
			}
			target, err := filepath.Rel(filepath.Dir(link), filepath.Join(root, "workspaces/ws-b/task-old"))
			if err != nil {
				t.Fatalf("relative link target: %v", err)
			}
			if err := os.Symlink(target, link); err != nil {
				t.Fatalf("symlink: %v", err)
			}

			collector := gc.New(gc.Config{
				StateRoot:     root,
				Node:          "node-1",
				TaskDirTTL:    720 * time.Hour,
				AgentStateTTL: 336 * time.Hour,
				Quiesce:       0,
				Interval:      time.Hour,
			}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
			stats, err := collector.RunOnce(t.Context())
			if err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !pathExists(filepath.Join(root, "workspaces/ws-b/task-old/work/file.txt")) {
				t.Fatal("ws-b's tree was reclaimed through ws-a's link")
			}
			if stats.AgentState != 0 {
				t.Errorf("agent states removed = %d, want nothing reclaimed through the link", stats.AgentState)
			}
		})
	}
}

// TestTechMultiWS04TaskRootsLinkDoesNotCrossWorkspaces: the orphan task-root
// area carries no workspace segment of its own, so a link on it is the
// shortest way out of its unit. A round that judges the area must not remove
// an entry of the workspace the link resolves to.
func TestTechMultiWS04TaskRootsLinkDoesNotCrossWorkspaces(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-400 * time.Hour)
	// The target is ws-b's partitioned home: no other rule reclaims it, so
	// only a task-root deletion through the link could remove it.
	writeAt(t, root, "home/ws-b/roots/hash-old/work/file.txt", old)
	ageTree(t, filepath.Join(root, "home/ws-b/roots"), old)

	link := filepath.Join(root, "workspaces/.task_roots")
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatalf("mkdir link parent: %v", err)
	}
	if err := os.Symlink(filepath.Join("..", "home", "ws-b", "roots"), link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	collector := gc.New(gc.Config{
		StateRoot:  root,
		Node:       "node-1",
		TaskDirTTL: time.Hour,
		Quiesce:    0,
		Interval:   time.Hour,
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	stats, err := collector.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !pathExists(filepath.Join(root, "home/ws-b/roots/hash-old/work/file.txt")) {
		t.Fatal("ws-b's tree was reclaimed through .task_roots")
	}
	if stats.TaskRoots != 0 {
		t.Errorf("task roots removed = %d, want nothing reclaimed through the link", stats.TaskRoots)
	}
}

// TestTechMultiWS04QuiesceIsPerWorkspace: ws-a is writing its task directory
// inside the quiesce window, ws-b is quiet. The round skips every rule of
// ws-a — its expired cache goes with its task directory — and reclaims ws-b's
// expired entries in the same pass. A write in one workspace stalls neither
// the other workspace nor the rest of its own rules.
func TestTechMultiWS04QuiesceIsPerWorkspace(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-1000 * time.Hour)
	for _, ws := range []string{"ws-a", "ws-b"} {
		writeAt(t, root, "workspaces/"+ws+"/task-old/work/file.txt", old)
		writeAt(t, root, "workspaces/.repos/"+ws+"/repo.git/HEAD", old)
	}
	ageTree(t, root, old)
	// ws-a keeps writing: its whole workspace stays inside the window.
	writeAt(t, root, "workspaces/ws-a/task-live/work/file.txt", time.Now())

	collector := gc.New(gc.Config{
		StateRoot:     root,
		Node:          "node-1",
		CacheTTL:      720 * time.Hour,
		TaskDirTTL:    168 * time.Hour,
		CacheMaxBytes: 1 << 30,
		AgentStateTTL: gc.DefaultAgentStateTTL,
		Quiesce:       30 * time.Minute,
		Interval:      time.Hour,
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	stats, err := collector.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !stats.Skipped {
		t.Errorf("stats = %+v, want the busy workspace reported as skipped", stats)
	}
	for _, kept := range []string{
		"workspaces/ws-a/task-old",
		"workspaces/.repos/ws-a/repo.git",
	} {
		if !pathExists(filepath.Join(root, kept)) {
			t.Errorf("%s was reclaimed inside the quiesce window", kept)
		}
	}
	for _, gone := range []string{
		"workspaces/ws-b/task-old",
		"workspaces/.repos/ws-b/repo.git",
	} {
		if pathExists(filepath.Join(root, gone)) {
			t.Errorf("%s survived; ws-a's write stalled the round", gone)
		}
	}
	if stats.CacheMeasured {
		t.Error("CacheMeasured = true, want the skipped workspace read as unmeasured")
	}
}
