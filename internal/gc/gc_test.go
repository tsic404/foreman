package gc

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// envFrom turns a map into a getenv func.
func envFrom(kv map[string]string) func(string) string {
	return func(key string) string { return kv[key] }
}

func testConfig(root string) Config {
	return Config{
		StateRoot:     root,
		CacheTTL:      720 * time.Hour,
		TaskDirTTL:    168 * time.Hour,
		CacheMaxBytes: DefaultCacheMaxBytes,
		Quiesce:       30 * time.Minute,
		Interval:      time.Hour,
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

func TestRunOnceTTL(t *testing.T) {
	root := t.TempDir()
	makeTree(t, root, map[string]string{
		"workspaces/ws-1/task-old/workdir/file.txt":        "old",
		"workspaces/ws-1/task-new/workdir/file.txt":        "new",
		"workspaces/ws-2/task-old/workdir/file.txt":        "old",
		"workspaces/.repos/ws-1/repo-old.git/HEAD":         "old",
		"workspaces/.repos/ws-1/repo-new.git/HEAD":         "new",
		"workspaces/.task_roots/hash-old/work/file.txt":    "old",
		"workspaces/.task_roots/hash-new/work/file.txt":    "new",
		"workspaces/.skill-cache/ws-1/entry/blob.txt":      "ancient",
		"home/.multica/pi-sessions/session-old.jsonl":      "ancient",
		"workspaces/ws-1/task-old/sub/deep/file.txt":       "old",
		"workspaces/ws-1/task-new/sub/deep/file.txt":       "new",
		"workspaces/.repos/ws-1/repo-old.git/objects/pack": "old",
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

	c := newTestCollector(t, testConfig(root))
	st, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if st.Skipped {
		t.Fatal("round skipped; quiesce should not trigger for a cold tree")
	}
	if st.TaskDirs != 2 || st.TaskRoots != 1 || st.BareRepos != 1 {
		t.Fatalf("stats = %+v, want 2 task dirs, 1 task root, 1 bare repo", st)
	}
	for _, gone := range []string{
		"workspaces/ws-1/task-old",
		"workspaces/ws-2/task-old",
		"workspaces/.repos/ws-1/repo-old.git",
		"workspaces/.task_roots/hash-old",
	} {
		if exists(filepath.Join(root, gone)) {
			t.Errorf("%s survived, want it reclaimed", gone)
		}
	}
	for _, kept := range []string{
		"workspaces/ws-1/task-new",
		"workspaces/.repos/ws-1/repo-new.git",
		"workspaces/.task_roots/hash-new",
		// .skill-cache and home have no rule in the design: untouched.
		"workspaces/.skill-cache/ws-1/entry/blob.txt",
		"home/.multica/pi-sessions/session-old.jsonl",
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
	// A single recent write anywhere under the state root skips the round.
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

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.StateRoot != DefaultStateRoot || cfg.CacheTTL != DefaultCacheTTL ||
		cfg.TaskDirTTL != DefaultTaskDirTTL || cfg.CacheMaxBytes != DefaultCacheMaxBytes ||
		cfg.Quiesce != DefaultGCQuiesce || cfg.Interval != DefaultGCInterval {
		t.Fatalf("defaults = %+v", cfg)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := LoadConfig(envFrom(map[string]string{
		EnvStateRoot:        "/state",
		EnvCacheTTL:         "24h",
		EnvTaskDirTTL:       "48h",
		EnvCacheMaxBytes:    "512Mi",
		EnvGCQuiesce:        "5m",
		EnvGCInterval:       "30m",
		EnvNodeName:         "node-a",
		"FOREMAN_LOG_LEVEL": "debug",
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
	if cfg.CacheMaxBytes != 512<<20 {
		t.Fatalf("CacheMaxBytes = %d, want %d", cfg.CacheMaxBytes, 512<<20)
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
	}
	for name, env := range cases {
		if _, err := LoadConfig(envFrom(env)); err == nil {
			t.Errorf("%s: LoadConfig accepted %v", name, env)
		}
	}
}
