package gc

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// envFrom turns a map into a getenv func.
func envFrom(kv map[string]string) func(string) string {
	return func(key string) string { return kv[key] }
}

func testConfig(root string) Config {
	return Config{
		StateRoot:      root,
		CacheTTL:       720 * time.Hour,
		TaskDirTTL:     168 * time.Hour,
		CacheMaxBytes:  DefaultCacheMaxBytes,
		Quiesce:        30 * time.Minute,
		Interval:       time.Hour,
		AgentStateTTL:  DefaultAgentStateTTL,
		AgentMemoryTTL: DefaultAgentMemoryTTL,

		SkillCacheTTL:      DefaultSkillCacheTTL,
		SkillCacheMaxBytes: DefaultSkillCacheMaxBytes,
	}
}

func newTestCollector(t *testing.T, cfg Config) *Collector {
	t.Helper()
	// Discard log output: the rounds are asserted through Stats and the
	// resulting file tree, not through log scraping.
	return New(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

// makeTree creates files (relative to root) and returns root.
func makeTree(t *testing.T, root string, files map[string]string) string {
	t.Helper()
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	return root
}

// ageTree pushes every entry in the tree back by age, newest mtime included.
func ageTree(t *testing.T, root string, age time.Duration) {
	t.Helper()
	ts := time.Now().Add(-age)
	err := filepath.WalkDir(root, func(path string, _ os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Chtimes(path, ts, ts)
	})
	if err != nil {
		t.Fatalf("chtimes %s: %v", root, err)
	}
}

func agePath(t *testing.T, path string, age time.Duration) {
	t.Helper()
	ts := time.Now().Add(-age)
	if err := os.Chtimes(path, ts, ts); err != nil {
		t.Fatalf("chtimes %s: %v", path, err)
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// growPack simulates a fetch landing in a bare cache after the scan: it bumps
// the pack's mtime and appends extra bytes, so the pre-delete re-measure sees
// both a newer usedAt and a larger size.
func growPack(t *testing.T, cacheDir string, extra int) {
	t.Helper()
	pack := filepath.Join(cacheDir, "pack")
	now := time.Now()
	if err := os.Chtimes(pack, now, now); err != nil {
		t.Fatalf("chtimes %s: %v", pack, err)
	}
	f, err := os.OpenFile(pack, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", pack, err)
	}
	if _, err := f.Write([]byte(strings.Repeat("x", extra))); err != nil {
		f.Close()
		t.Fatalf("append %s: %v", pack, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", pack, err)
	}
}

func TestRunOnceTTL(t *testing.T) {
	root := t.TempDir()
	makeTree(t, root, map[string]string{
		"workspaces/ws-1/task-old/workdir/file.txt":                   "old",
		"workspaces/ws-1/task-new/workdir/file.txt":                   "new",
		"workspaces/ws-2/task-old/workdir/file.txt":                   "old",
		"workspaces/.repos/ws-1/repo-old.git/HEAD":                    "old",
		"workspaces/.repos/ws-1/repo-new.git/HEAD":                    "new",
		"workspaces/.task_roots/hash-old/work/file.txt":               "old",
		"workspaces/.task_roots/hash-new/work/file.txt":               "new",
		"workspaces/.skill-cache/v1/ws-1/src/id/hash-old/bundle.json": "old",
		"workspaces/.skill-cache/v1/ws-1/src/id/hash-new/bundle.json": "new",
		"home/.multica/pi-sessions/session-old.jsonl":                 "ancient",
		"home/.multica/pi-sessions/session-new.jsonl":                 "new",
		"workspaces/ws-1/task-old/sub/deep/file.txt":                  "old",
		"workspaces/ws-1/task-new/sub/deep/file.txt":                  "new",
		"workspaces/.repos/ws-1/repo-old.git/objects/pack":            "old",
	})
	// Everything ancient first, then re-freshen the parts that must survive.
	ageTree(t, root, 1000*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/ws-1/task-new"), 1*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/ws-1/task-new/workdir/file.txt"), 1*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/ws-1/task-new/sub/deep/file.txt"), 1*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/repo-new.git"), 1*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/repo-new.git/HEAD"), 1*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.task_roots/hash-new"), 1*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.task_roots/hash-new/work/file.txt"), 1*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.skill-cache/v1/ws-1/src/id/hash-new"), 1*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.skill-cache/v1/ws-1/src/id/hash-new/bundle.json"), 1*time.Hour)
	agePath(t, filepath.Join(root, "home/.multica/pi-sessions/session-new.jsonl"), 1*time.Hour)

	c := newTestCollector(t, testConfig(root))
	st, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if st.Skipped {
		t.Fatal("round skipped; quiesce should not trigger for a cold tree")
	}
	if st.TaskDirs != 2 || st.TaskRoots != 1 || st.BareRepos != 1 || st.SkillCache != 1 || st.AgentState != 1 || st.AgentMemory != 0 {
		t.Fatalf("stats = %+v, want 2 task dirs, 1 task root, 1 bare repo, 1 skill bundle, 1 agent-state file", st)
	}
	for _, gone := range []string{
		"workspaces/ws-1/task-old",
		"workspaces/ws-2/task-old",
		"workspaces/.repos/ws-1/repo-old.git",
		"workspaces/.task_roots/hash-old",
		"workspaces/.skill-cache/v1/ws-1/src/id/hash-old",
		"home/.multica/pi-sessions/session-old.jsonl",
	} {
		if exists(filepath.Join(root, gone)) {
			t.Errorf("%s survived, want it reclaimed", gone)
		}
	}
	for _, kept := range []string{
		"workspaces/ws-1/task-new",
		"workspaces/.repos/ws-1/repo-new.git",
		"workspaces/.task_roots/hash-new",
		"workspaces/.skill-cache/v1/ws-1/src/id/hash-new/bundle.json",
		"home/.multica/pi-sessions/session-new.jsonl",
	} {
		if !exists(filepath.Join(root, kept)) {
			t.Errorf("%s was reclaimed, want it kept", kept)
		}
	}
}

// TestRunOnceSkillCacheLimit pins the per-workspace size cap: the cache is
// trimmed least-recently-used first, and one workspace's excess never evicts
// another workspace's entries.
func TestRunOnceSkillCacheLimit(t *testing.T) {
	root := t.TempDir()
	makeTree(t, root, map[string]string{
		"workspaces/.skill-cache/v1/ws-1/src/id-a/hash-old/bundle.json":    strings.Repeat("a", 4096),
		"workspaces/.skill-cache/v1/ws-1/src/id-a/hash-mid/bundle.json":    strings.Repeat("b", 4096),
		"workspaces/.skill-cache/v1/ws-1/src/id-b/hash-new/bundle.json":    strings.Repeat("c", 4096),
		"workspaces/.skill-cache/v1/ws-2/src/id-c/hash-oldest/bundle.json": strings.Repeat("d", 4096),
	})
	for _, entry := range []struct {
		path string
		age  time.Duration
	}{
		{"workspaces/.skill-cache/v1/ws-1/src/id-a/hash-old", 20 * 24 * time.Hour},
		{"workspaces/.skill-cache/v1/ws-1/src/id-a/hash-mid", 10 * 24 * time.Hour},
		{"workspaces/.skill-cache/v1/ws-1/src/id-b/hash-new", time.Hour},
		{"workspaces/.skill-cache/v1/ws-2/src/id-c/hash-oldest", 25 * 24 * time.Hour},
	} {
		dir := filepath.Join(root, filepath.FromSlash(entry.path))
		agePath(t, dir, entry.age)
		agePath(t, filepath.Join(dir, "bundle.json"), entry.age)
	}

	cfg := testConfig(root)
	cfg.Quiesce = 0               // the tree's parent dirs are fresh; keep the round
	cfg.SkillCacheMaxBytes = 8192 // two bundles; ws-1 holds three
	st, err := newTestCollector(t, cfg).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if st.SkillCache != 1 {
		t.Fatalf("SkillCache = %d, want 1 (ws-1's least recently used entry)", st.SkillCache)
	}
	if exists(filepath.Join(root, "workspaces/.skill-cache/v1/ws-1/src/id-a/hash-old")) {
		t.Error("ws-1's least recently used entry survived the size cap")
	}
	for _, kept := range []string{
		"workspaces/.skill-cache/v1/ws-1/src/id-a/hash-mid",
		"workspaces/.skill-cache/v1/ws-1/src/id-b/hash-new",
		// ws-2 is under its own cap: its entry is the oldest of all, but a
		// capacity pass never reaches across workspaces.
		"workspaces/.skill-cache/v1/ws-2/src/id-c/hash-oldest",
	} {
		if !exists(filepath.Join(root, kept)) {
			t.Errorf("%s was reclaimed, want it kept", kept)
		}
	}
}

func TestRunOnceQuiesce(t *testing.T) {
	root := t.TempDir()
	makeTree(t, root, map[string]string{
		"workspaces/ws-1/task-old/workdir/file.txt": "old",
	})
	ageTree(t, root, 1000*time.Hour)
	// A recent write inside the workspace's tree skips that workspace for the
	// round; the rest of the node is still cleaned.
	agePath(t, filepath.Join(root, "workspaces/ws-1/task-old/workdir/file.txt"), time.Minute)

	c := newTestCollector(t, testConfig(root))
	st, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !st.Skipped {
		t.Fatalf("stats = %+v, want a skipped round", st)
	}
	if !exists(filepath.Join(root, "workspaces/ws-1/task-old")) {
		t.Fatal("task dir was reclaimed during a quiesced round")
	}
}

func TestRunOnceQuiesceDisabled(t *testing.T) {
	root := t.TempDir()
	makeTree(t, root, map[string]string{
		"workspaces/ws-1/task-old/workdir/file.txt": "old",
	})
	ageTree(t, root, 1000*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/ws-1/task-old/workdir/file.txt"), time.Minute)

	cfg := testConfig(root)
	cfg.Quiesce = 0
	c := newTestCollector(t, cfg)
	st, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if st.Skipped {
		t.Fatalf("stats = %+v, want the round to run with the quiesce window off", st)
	}
}

// The .repos cap reclaims the least recently used bare cache first.
func TestRunOnceCacheLimitLRU(t *testing.T) {
	root := t.TempDir()
	makeTree(t, root, map[string]string{
		"workspaces/.repos/ws-1/old.git/pack":   "0123456789", // 10 bytes
		"workspaces/.repos/ws-1/mid.git/pack":   "0123456789", // 10 bytes
		"workspaces/.repos/ws-1/young.git/pack": "0123456789", // 10 bytes
	})
	ageTree(t, root, 10*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/mid.git"), 5*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/mid.git/pack"), 5*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/young.git"), 2*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/young.git/pack"), 2*time.Hour)

	cfg := testConfig(root)
	cfg.CacheMaxBytes = 25 // 30 bytes of cache, 5 over the cap
	c := newTestCollector(t, cfg)
	st, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if st.BareRepos != 1 {
		t.Fatalf("stats = %+v, want exactly one bare repo reclaimed", st)
	}
	if exists(filepath.Join(root, "workspaces/.repos/ws-1/old.git")) {
		t.Error("old.git (least recently used) survived, want it reclaimed first")
	}
	for _, kept := range []string{"mid.git", "young.git"} {
		if !exists(filepath.Join(root, "workspaces/.repos/ws-1", kept)) {
			t.Errorf("%s was reclaimed, want it kept", kept)
		}
	}
	if st.CacheBytes != 20 {
		t.Errorf("cache_bytes = %d, want 20", st.CacheBytes)
	}
}

// A cache written after the TTL scan (an in-flight fetch/clone) must not be
// the LRU victim: the eviction pass re-reads its mtime and size, skips it for
// that round, and keeps evicting next-oldest caches until the true total fits.
// This is the degradation face of AC-12 / TC-tech-cache-01: an in-flight cache
// is never deleted and the reported remaining bytes stay truthful.
func TestRunOnceCacheLimitSkipsRefreshedCache(t *testing.T) {
	root := t.TempDir()
	makeTree(t, root, map[string]string{
		"workspaces/.repos/ws-1/old.git/pack":   "0123456789", // 10 bytes
		"workspaces/.repos/ws-1/mid.git/pack":   "0123456789", // 10 bytes
		"workspaces/.repos/ws-1/young.git/pack": "0123456789", // 10 bytes
	})
	ageTree(t, root, 10*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/mid.git"), 5*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/mid.git/pack"), 5*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/young.git"), 2*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/young.git/pack"), 2*time.Hour)

	cfg := testConfig(root)
	cfg.CacheMaxBytes = 25 // the scan sees 30 bytes, 5 over the cap
	c := newTestCollector(t, cfg)
	oldPath := filepath.Join(root, "workspaces/.repos/ws-1/old.git")
	// The hook stands in for a fetch landing between the scan and the delete.
	c.remeasure = func(ctx context.Context, path string) (time.Time, int64, error) {
		if path == oldPath {
			growPack(t, oldPath, 9) // 10 → 19 bytes
		}
		return measure(ctx, path)
	}

	st, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if st.BareRepos != 2 {
		t.Fatalf("stats = %+v, want two bare repos reclaimed", st)
	}
	if !exists(oldPath) {
		t.Error("old.git was reclaimed although it was refreshed after the scan")
	}
	if exists(filepath.Join(root, "workspaces/.repos/ws-1/mid.git")) {
		t.Error("mid.git survived, want the next-oldest cache reclaimed")
	}
	if exists(filepath.Join(root, "workspaces/.repos/ws-1/young.git")) {
		t.Error("young.git survived, want the corrected total to keep the pass evicting")
	}
	if st.CacheBytes != 19 {
		t.Errorf("cache_bytes = %d, want the actual 19 bytes left behind", st.CacheBytes)
	}
}

// A refreshed cache is skipped even when it is the only candidate: the round
// must terminate with the actual remaining bytes, and the next round re-judges
// the cache from a fresh scan.
func TestRunOnceCacheLimitRefreshedSoleCache(t *testing.T) {
	root := t.TempDir()
	makeTree(t, root, map[string]string{
		"workspaces/.repos/ws-1/only.git/pack": "012345678901234567890123456789", // 30 bytes
	})
	ageTree(t, root, 10*time.Hour)

	cfg := testConfig(root)
	cfg.CacheMaxBytes = 25
	c := newTestCollector(t, cfg)
	onlyPath := filepath.Join(root, "workspaces/.repos/ws-1/only.git")
	c.remeasure = func(ctx context.Context, path string) (time.Time, int64, error) {
		if path == onlyPath {
			growPack(t, onlyPath, 9) // 30 → 39 bytes
		}
		return measure(ctx, path)
	}

	st, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if st.BareRepos != 0 {
		t.Fatalf("stats = %+v, want the refreshed cache kept this round", st)
	}
	if !exists(onlyPath) {
		t.Error("only.git was reclaimed although it was refreshed after the scan")
	}
	if st.CacheBytes != 39 {
		t.Errorf("cache_bytes = %d, want the actual 39 bytes left behind", st.CacheBytes)
	}
}

// The corrected total must keep the pass evicting even when the refreshed
// cache alone stays over the cap: every other victim goes, and the round ends
// reporting the true remaining bytes instead of exiting early on the stale
// scan total.
func TestRunOnceCacheLimitRefreshedCacheDrainsOtherVictims(t *testing.T) {
	root := t.TempDir()
	makeTree(t, root, map[string]string{
		"workspaces/.repos/ws-1/old.git/pack":   "0123456789", // 10 bytes
		"workspaces/.repos/ws-1/mid.git/pack":   "0123456789", // 10 bytes
		"workspaces/.repos/ws-1/young.git/pack": "0123456789", // 10 bytes
	})
	ageTree(t, root, 10*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/mid.git"), 5*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/mid.git/pack"), 5*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/young.git"), 2*time.Hour)
	agePath(t, filepath.Join(root, "workspaces/.repos/ws-1/young.git/pack"), 2*time.Hour)

	cfg := testConfig(root)
	cfg.CacheMaxBytes = 25 // the scan sees 30 bytes, 5 over the cap
	c := newTestCollector(t, cfg)
	oldPath := filepath.Join(root, "workspaces/.repos/ws-1/old.git")
	c.remeasure = func(ctx context.Context, path string) (time.Time, int64, error) {
		if path == oldPath {
			growPack(t, oldPath, 40) // 10 → 50 bytes, over the cap on its own
		}
		return measure(ctx, path)
	}

	st, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if st.BareRepos != 2 {
		t.Fatalf("stats = %+v, want both other caches reclaimed", st)
	}
	if !exists(oldPath) {
		t.Error("old.git was reclaimed although it was refreshed after the scan")
	}
	for _, gone := range []string{"mid.git", "young.git"} {
		if exists(filepath.Join(root, "workspaces/.repos/ws-1", gone)) {
			t.Errorf("%s survived, want the corrected total to keep evicting", gone)
		}
	}
	if st.CacheBytes != 50 {
		t.Errorf("cache_bytes = %d, want the actual 50 bytes left behind", st.CacheBytes)
	}
}

func TestRunOnceMissingStateRoot(t *testing.T) {
	cfg := testConfig(filepath.Join(t.TempDir(), "absent"))
	st, err := newTestCollector(t, cfg).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if st.Skipped || st.TaskDirs != 0 || st.BareRepos != 0 {
		t.Fatalf("stats = %+v, want a no-op round", st)
	}
}

// The node-local agent state rules judge store directories by their newest
// mtime and files by their own: stale entries go, fresh ones stay, and the
// state and memory TTLs stay distinct.
func TestRunOnceAgentStateTTL(t *testing.T) {
	root := t.TempDir()
	const profile = "home/.multica/profiles/foreman-job"
	makeTree(t, root, map[string]string{
		// Session stores nest <agent>/<profile>/<store>; the store holding the
		// fresh file stays as a whole.
		profile + "/hermes-sessions/agent-1/default/issue-old/state.db": "old",
		profile + "/hermes-sessions/agent-1/default/issue-new/state.db": "new",
		profile + "/dsh-sessions/agent-1/old/s.jsonl":                   "old",
		profile + "/dsh-sessions/agent-1/new/s.jsonl":                   "new",
		profile + "/reasonix-state/agent-1/old/s.jsonl":                 "old",
		profile + "/reasonix-state/agent-1/new/s.jsonl":                 "new",
		// Memory follows the memory TTL only: `old` is stale for both, `young`
		// outlived the state TTL but not the memory TTL.
		profile + "/hermes-state/agent-1/default/old/MEMORY.md":   "old",
		profile + "/hermes-state/agent-1/default/young/MEMORY.md": "young",
		// pi-sessions and the codex stores are judged file by file.
		"home/.multica/pi-sessions/session-old.jsonl":                       "old",
		"home/.multica/pi-sessions/session-new.jsonl":                       "new",
		"home/.codex/multica-sessions/p_ns/agent-1/issue/rollout-old.jsonl": "old",
		"home/.codex/multica-sessions/p_ns/agent-1/issue/rollout-new.jsonl": "new",
		// A profile whose whole store面 is stale keeps its root directory.
		"home/.multica/profiles/other/hermes-sessions/agent-1/default/issue-old/state.db": "old",
	})
	ageTree(t, root, 100*24*time.Hour)
	// 35 days: stale for the 14-day state TTL, still fresh for the 90-day
	// memory TTL.
	for _, path := range []string{
		profile + "/hermes-state/agent-1/default/young",
		profile + "/hermes-state/agent-1/default/young/MEMORY.md",
	} {
		agePath(t, filepath.Join(root, path), 35*24*time.Hour)
	}
	for _, path := range []string{
		profile + "/hermes-sessions/agent-1/default/issue-new/state.db",
		profile + "/dsh-sessions/agent-1/new/s.jsonl",
		profile + "/reasonix-state/agent-1/new/s.jsonl",
		"home/.multica/pi-sessions/session-new.jsonl",
		"home/.codex/multica-sessions/p_ns/agent-1/issue/rollout-new.jsonl",
	} {
		agePath(t, filepath.Join(root, path), time.Hour)
	}

	c := newTestCollector(t, testConfig(root))
	st, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if st.AgentState != 6 {
		t.Fatalf("stats = %+v, want 6 agent-state entries reclaimed", st)
	}
	if st.AgentMemory != 1 {
		t.Fatalf("stats = %+v, want 1 agent-memory entry reclaimed", st)
	}
	for _, gone := range []string{
		profile + "/hermes-sessions/agent-1/default/issue-old",
		profile + "/dsh-sessions/agent-1/old",
		profile + "/reasonix-state/agent-1/old",
		profile + "/hermes-state/agent-1/default/old",
		"home/.multica/pi-sessions/session-old.jsonl",
		"home/.codex/multica-sessions/p_ns/agent-1/issue/rollout-old.jsonl",
		"home/.multica/profiles/other/hermes-sessions/agent-1/default/issue-old",
	} {
		if exists(filepath.Join(root, gone)) {
			t.Errorf("%s survived, want it reclaimed", gone)
		}
	}
	for _, kept := range []string{
		profile + "/hermes-sessions/agent-1/default/issue-new/state.db",
		profile + "/dsh-sessions/agent-1/new/s.jsonl",
		profile + "/reasonix-state/agent-1/new/s.jsonl",
		profile + "/hermes-state/agent-1/default/young/MEMORY.md",
		"home/.multica/pi-sessions/session-new.jsonl",
		"home/.codex/multica-sessions/p_ns/agent-1/issue/rollout-new.jsonl",
		// The profile roots survive an emptied store面 (prepare's three-segment
		// self-heal must not see them as never initialized).
		profile,
		"home/.multica/profiles/other",
	} {
		if !exists(filepath.Join(root, kept)) {
			t.Errorf("%s was reclaimed, want it kept", kept)
		}
	}
}

// A session shell this rule emptied is reaped on a later round: files are
// judged by their own mtime, and the directory that held them must not stay
// behind forever. The shell waits out the TTL after its last file left, so a
// directory another process is about to write is never raced.
func TestRunOnceReapsEmptiedSessionShell(t *testing.T) {
	root := t.TempDir()
	const shell = "home/.codex/multica-sessions/p_ns/agent-1/issue"
	makeTree(t, root, map[string]string{shell + "/rollout-old.jsonl": "old"})
	ageTree(t, root, 100*24*time.Hour)

	c := newTestCollector(t, testConfig(root))
	if _, err := c.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// The round that reclaimed the file leaves the shell: removing the file
	// stamped the directory with that round's clock, so it is not yet stale.
	if !exists(filepath.Join(root, shell)) {
		t.Fatalf("%s disappeared in the round that reclaimed its file", shell)
	}

	// Once the shell's own mtime is past the TTL, a later round reaps it. The
	// store roots above it stay (prepare's self-heal owns them).
	c.now = func() time.Time { return time.Now().Add(400 * time.Hour) }
	if _, err := c.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if exists(filepath.Join(root, shell)) {
		t.Errorf("%s survived a round after its last file left, want the empty shell reaped", shell)
	}
	if !exists(filepath.Join(root, "home/.codex/multica-sessions")) {
		t.Error("the store root was reaped, want only the emptied shell removed")
	}
}

// A zero agent-state or agent-memory TTL turns that rule off entirely.
func TestRunOnceAgentTTLDisabled(t *testing.T) {
	root := t.TempDir()
	const profile = "home/.multica/profiles/foreman-job"
	makeTree(t, root, map[string]string{
		profile + "/hermes-sessions/agent-1/default/issue-old/state.db": "old",
		profile + "/hermes-state/agent-1/default/old/MEMORY.md":         "old",
		"home/.multica/pi-sessions/session-old.jsonl":                   "old",
		"home/.codex/multica-sessions/p_ns/agent-1/issue/rollout.jsonl": "old",
	})
	ageTree(t, root, 400*24*time.Hour)

	cfg := testConfig(root)
	cfg.AgentStateTTL = 0
	cfg.AgentMemoryTTL = 0
	st, err := newTestCollector(t, cfg).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if st.AgentState != 0 || st.AgentMemory != 0 {
		t.Fatalf("stats = %+v, want both agent rules off", st)
	}
	for _, kept := range []string{
		profile + "/hermes-sessions/agent-1/default/issue-old/state.db",
		profile + "/hermes-state/agent-1/default/old/MEMORY.md",
		"home/.multica/pi-sessions/session-old.jsonl",
		"home/.codex/multica-sessions/p_ns/agent-1/issue/rollout.jsonl",
	} {
		if !exists(filepath.Join(root, kept)) {
			t.Errorf("%s was reclaimed with the agent rules off", kept)
		}
	}
}

// A recent write under /state/home skips the home unit for the round: a
// running Job's stores are never raced, and the workspaces tree is still
// judged on its own.
func TestRunOnceQuiesceAgentState(t *testing.T) {
	root := t.TempDir()
	const profile = "home/.multica/profiles/foreman-job"
	makeTree(t, root, map[string]string{
		profile + "/hermes-sessions/agent-1/default/issue-old/state.db": "old",
		"home/.multica/pi-sessions/session-old.jsonl":                   "old",
	})
	ageTree(t, root, 100*24*time.Hour)
	// The live task keeps writing its store.
	makeTree(t, root, map[string]string{
		profile + "/hermes-sessions/agent-1/default/issue-live/state.db": "live",
	})

	st, err := newTestCollector(t, testConfig(root)).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !st.Skipped {
		t.Fatalf("stats = %+v, want a skipped round", st)
	}
	for _, kept := range []string{
		profile + "/hermes-sessions/agent-1/default/issue-old",
		"home/.multica/pi-sessions/session-old.jsonl",
	} {
		if !exists(filepath.Join(root, kept)) {
			t.Errorf("%s was reclaimed during a quiesced round", kept)
		}
	}
}

// A symbolic link on a fixed path segment must not redirect a reclaim into
// another workspace's tree (§8.8: 不跨 workspace 清删). ws-a's session store
// links at ws-b's task directory: the state rule would judge that tree by its
// own retention and remove it, while the task rule keeps it. Nothing is
// reclaimed through the link, in either TTL state.
func TestRunOnceSymlinkNotFollowed(t *testing.T) {
	for _, tc := range []struct {
		name string
		age  time.Duration
	}{
		{"stale target", 400 * time.Hour},
		{"fresh target", time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			makeTree(t, root, map[string]string{
				"workspaces/ws-b/task-old/work/file.txt": "x",
			})
			ageTree(t, filepath.Join(root, "workspaces/ws-b/task-old"), tc.age)

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

			cfg := testConfig(root)
			cfg.TaskDirTTL = 720 * time.Hour
			cfg.AgentStateTTL = 336 * time.Hour
			cfg.Quiesce = 0
			st, err := newTestCollector(t, cfg).RunOnce(context.Background())
			if err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !exists(filepath.Join(root, "workspaces/ws-b/task-old/work/file.txt")) {
				t.Fatal("ws-b's tree was reclaimed through ws-a's link")
			}
			if st.AgentState != 0 {
				t.Errorf("agent_state = %d, want nothing reclaimed through the link", st.AgentState)
			}
		})
	}
}

// The quiesce window is judged per workspace: a write in one workspace must
// not stall another workspace's reclamation (§8.8).
func TestRunOnceQuiescePerWorkspace(t *testing.T) {
	root := t.TempDir()
	makeTree(t, root, map[string]string{
		"workspaces/ws-busy/task-old/work/file.txt":  "old",
		"workspaces/ws-quiet/task-old/work/file.txt": "old",
	})
	ageTree(t, root, 400*time.Hour)
	// The busy workspace keeps writing inside the window.
	makeTree(t, root, map[string]string{
		"workspaces/ws-busy/task-live/work/file.txt": "live",
	})

	st, err := newTestCollector(t, testConfig(root)).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !st.Skipped {
		t.Errorf("stats = %+v, want the busy workspace reported as skipped", st)
	}
	if !exists(filepath.Join(root, "workspaces/ws-busy/task-old")) {
		t.Error("the busy workspace's task dir was reclaimed inside the window")
	}
	if exists(filepath.Join(root, "workspaces/ws-quiet/task-old")) {
		t.Error("the quiet workspace's expired task dir survived the round")
	}
}

// One workspace's write keeps its whole set of rules out of the round, not
// just the tree that was written: a task writing its working directory also
// keeps a bare cache it is reading — reads do not refresh mtime — from being
// reclaimed underneath it (§8.8).
func TestRunOnceQuiesceCoversEveryTreeOfTheWorkspace(t *testing.T) {
	root := t.TempDir()
	makeTree(t, root, map[string]string{
		"workspaces/ws-a/task-old/work/file.txt":   "old",
		"workspaces/.repos/ws-a/repo-old.git/HEAD": "old",
		"workspaces/ws-b/task-old/work/file.txt":   "old",
		"workspaces/.repos/ws-b/repo-old.git/HEAD": "old",
	})
	ageTree(t, root, 1000*time.Hour)
	// The running task writes its working directory only.
	makeTree(t, root, map[string]string{
		"workspaces/ws-a/task-live/work/file.txt": "live",
	})

	st, err := newTestCollector(t, testConfig(root)).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if st.BareRepos != 1 {
		t.Errorf("BareRepos = %d, want only ws-b's expired cache reclaimed", st.BareRepos)
	}
	if !exists(filepath.Join(root, "workspaces/.repos/ws-a/repo-old.git")) {
		t.Error("ws-a's cache was reclaimed although ws-a is inside the window")
	}
	if exists(filepath.Join(root, "workspaces/.repos/ws-b/repo-old.git")) {
		t.Error("ws-b's expired cache survived; ws-a's write stalled the round")
	}
	if st.CacheMeasured {
		t.Error("CacheMeasured = true, want the skipped workspace read as unmeasured")
	}
}

// removeWithin resolves every component inside the root, so a link that
// leaves it fails the removal instead of acting on its target. The pre-check
// in delete names the skip; this is what holds if the link appears after it.
func TestRemoveWithinRefusesEscapingLink(t *testing.T) {
	parent := t.TempDir()
	unit := filepath.Join(parent, "ws-a")
	victim := filepath.Join(parent, "ws-b")
	makeTree(t, victim, map[string]string{"store/state.db": "x"})
	if err := os.MkdirAll(filepath.Join(unit, "profiles"), 0o755); err != nil {
		t.Fatalf("mkdir unit: %v", err)
	}
	if err := os.Symlink(victim, filepath.Join(unit, "profiles", "sessions")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	if err := removeWithin(unit, "profiles/sessions/store"); err == nil {
		t.Fatal("removeWithin removed a path whose component leaves the root")
	}
	if !exists(filepath.Join(victim, "store/state.db")) {
		t.Fatal("the link target was removed through the root boundary")
	}

	makeTree(t, unit, map[string]string{"own/state.db": "x"})
	if err := removeWithin(unit, "own"); err != nil {
		t.Fatalf("removeWithin inside the root: %v", err)
	}
	if exists(filepath.Join(unit, "own")) {
		t.Error("own/ survived, want it removed")
	}
}

// removeWithin refuses a unit that is itself a symbolic link: the opened root
// follows the final component, so only the fd's identity check keeps the
// removal from landing in the linked tree.
func TestRemoveWithinRefusesLinkedUnit(t *testing.T) {
	parent := t.TempDir()
	victim := filepath.Join(parent, "ws-b")
	makeTree(t, victim, map[string]string{"hash/state.db": "x"})
	unit := filepath.Join(parent, "ws-a")
	if err := os.Symlink(victim, unit); err != nil {
		t.Fatalf("symlink unit: %v", err)
	}

	err := removeWithin(unit, "hash")
	if err == nil {
		t.Fatal("removeWithin removed through a linked unit")
	}
	if !isSymlinkSkip(err) {
		t.Fatalf("err = %v, want the removal reported as a symlink skip", err)
	}
	if !exists(filepath.Join(victim, "hash/state.db")) {
		t.Fatal("the linked tree was removed through the unit")
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.StateRoot != DefaultStateRoot || cfg.CacheTTL != DefaultCacheTTL ||
		cfg.TaskDirTTL != DefaultTaskDirTTL || cfg.CacheMaxBytes != DefaultCacheMaxBytes ||
		cfg.Quiesce != DefaultGCQuiesce || cfg.Interval != DefaultGCInterval ||
		cfg.AgentStateTTL != DefaultAgentStateTTL || cfg.AgentMemoryTTL != DefaultAgentMemoryTTL ||
		cfg.SkillCacheTTL != DefaultSkillCacheTTL || cfg.SkillCacheMaxBytes != DefaultSkillCacheMaxBytes {
		t.Fatalf("defaults = %+v", cfg)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{
		EnvStateRoot:          "/state",
		EnvCacheTTL:           "24h",
		EnvTaskDirTTL:         "48h",
		EnvCacheMaxBytes:      "512Mi",
		EnvGCQuiesce:          "5m",
		EnvGCInterval:         "30m",
		EnvAgentStateTTL:      "12h",
		EnvAgentMemoryTTL:     "0",
		EnvSkillCacheTTL:      "36h",
		EnvSkillCacheMaxBytes: "2Gi",
		EnvNodeName:           "node-a",
		"FOREMAN_LOG_LEVEL":   "debug",
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.StateRoot != "/state" || cfg.Node != "node-a" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.CacheTTL != 24*time.Hour || cfg.TaskDirTTL != 48*time.Hour ||
		cfg.Quiesce != 5*time.Minute || cfg.Interval != 30*time.Minute {
		t.Fatalf("durations = %+v", cfg)
	}
	// "0" is the contract's rule-off value, not a parse error.
	if cfg.AgentStateTTL != 12*time.Hour || cfg.AgentMemoryTTL != 0 {
		t.Fatalf("agent ttls = state %s, memory %s", cfg.AgentStateTTL, cfg.AgentMemoryTTL)
	}
	if cfg.CacheMaxBytes != 512<<20 {
		t.Fatalf("CacheMaxBytes = %d, want %d", cfg.CacheMaxBytes, 512<<20)
	}
	if cfg.SkillCacheTTL != 36*time.Hour || cfg.SkillCacheMaxBytes != 2<<30 {
		t.Fatalf("skill cache = ttl %s, max %d", cfg.SkillCacheTTL, cfg.SkillCacheMaxBytes)
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	cases := map[string]map[string]string{
		"bad ttl":     {EnvCacheTTL: "abc"},
		"zero ttl":    {EnvTaskDirTTL: "0s"},
		"bad size":    {EnvCacheMaxBytes: "50 Gib"},
		"neg size":    {EnvCacheMaxBytes: "-1"},
		"bad quiesce": {EnvGCQuiesce: "soon"},
		"bad period":  {EnvGCInterval: "-5m"},
		"bad level":   {"FOREMAN_LOG_LEVEL": "trace"},
		// The agent TTLs accept 0 (rule off) but never a negative duration.
		"neg agent state":  {EnvAgentStateTTL: "-1h"},
		"bad agent memory": {EnvAgentMemoryTTL: "1d"},
		"neg agent memory": {EnvAgentMemoryTTL: "-336h"},
		// The skill bundle cache takes a live retention and a byte quantity.
		"zero skill ttl": {EnvSkillCacheTTL: "0s"},
		"bad skill size": {EnvSkillCacheMaxBytes: "lots"},
	}
	for name, env := range cases {
		if _, err := LoadConfig(envFrom(env)); err == nil {
			t.Errorf("%s: LoadConfig accepted %v", name, env)
		}
	}
}
