// TC-tech-cache-08 implements AC-100 on the production collector: the
// node-side skill bundle cache the upstream daemon keeps under
// workspaces/.skill-cache is reclaimed by retention and by a per-workspace
// size cap. Expired entries go, fresh entries stay, and a capacity pass in
// one workspace never touches another workspace's entries — even when that
// other workspace holds the oldest entry on the node.
package acceptance

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/gc"
)

// tcCache08Bundle is one cache entry: the upstream daemon's bundle payload
// plus the directory that carries it.
type tcCache08Bundle struct {
	path string
	age  time.Duration
}

// tcCache08Entries is the scenario's tree. Workspace "a" holds two expired
// entries and three live ones; workspace "b" holds one entry that is older
// than everything in "a" yet inside retention.
var tcCache08Entries = []tcCache08Bundle{
	{"workspaces/.skill-cache/v1/ws-a/src/skill-1/hash-expired-1", 30 * 24 * time.Hour},
	{"workspaces/.skill-cache/v1/ws-a/src/skill-2/hash-expired-2", 40 * 24 * time.Hour},
	{"workspaces/.skill-cache/v1/ws-a/src/skill-1/hash-old", 20 * 24 * time.Hour},
	{"workspaces/.skill-cache/v1/ws-a/src/skill-1/hash-mid", 10 * 24 * time.Hour},
	{"workspaces/.skill-cache/v1/ws-a/src/skill-2/hash-new", time.Hour},
	{"workspaces/.skill-cache/v1/ws-b/src/skill-3/hash-oldest", 25 * 24 * time.Hour},
}

// tcCache08Build creates the sample tree and ages every entry, its bundle
// file included: the rule judges an entry by its newest write.
func tcCache08Build(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, entry := range tcCache08Entries {
		dir := filepath.Join(root, filepath.FromSlash(entry.path))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		bundle := filepath.Join(dir, "bundle.json")
		if err := os.WriteFile(bundle, []byte(strings.Repeat("b", 1024)), 0o644); err != nil {
			t.Fatalf("write %s: %v", bundle, err)
		}
		when := time.Now().Add(-entry.age)
		for _, path := range []string{bundle, dir} {
			if err := os.Chtimes(path, when, when); err != nil {
				t.Fatalf("age %s: %v", path, err)
			}
		}
	}
	return root
}

func TestCache08SkillCacheReclaim(t *testing.T) {
	root := tcCache08Build(t)
	cfg := gc.Config{
		StateRoot:     root,
		CacheTTL:      720 * time.Hour,
		TaskDirTTL:    168 * time.Hour,
		CacheMaxBytes: gc.DefaultCacheMaxBytes,
		Quiesce:       0,
		Interval:      time.Hour,
		AgentStateTTL: gc.DefaultAgentStateTTL,
		// Two live entries of 1 KiB fit; workspace "a" holds three, so one
		// eviction is due after the two expired ones go.
		SkillCacheTTL:      gc.DefaultSkillCacheTTL,
		SkillCacheMaxBytes: 2 * 1024,
	}
	stats, err := gc.New(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil))).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	gone := []string{
		"workspaces/.skill-cache/v1/ws-a/src/skill-1/hash-expired-1",
		"workspaces/.skill-cache/v1/ws-a/src/skill-2/hash-expired-2",
		"workspaces/.skill-cache/v1/ws-a/src/skill-1/hash-old",
	}
	for _, name := range gone {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(name))); !os.IsNotExist(err) {
			t.Errorf("%s survived, want it reclaimed", name)
		}
	}
	kept := []string{
		"workspaces/.skill-cache/v1/ws-a/src/skill-1/hash-mid",
		"workspaces/.skill-cache/v1/ws-a/src/skill-2/hash-new",
		// The capacity pass is per workspace: ws-b's entry is the oldest on
		// the node, yet ws-b stays under its own cap.
		"workspaces/.skill-cache/v1/ws-b/src/skill-3/hash-oldest",
	}
	for _, name := range kept {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(name))); err != nil {
			t.Errorf("%s was reclaimed: %v", name, err)
		}
	}
	if stats.SkillCache != len(gone) {
		t.Errorf("SkillCache = %d, want %d", stats.SkillCache, len(gone))
	}
}
