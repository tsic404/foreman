package gc

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// State-root layout (ADR-005 / 05-modules/job-template.md §节点清理).
const (
	workspacesDir = "workspaces"
	reposDir      = ".repos"
	taskRootsDir  = ".task_roots"
	skillCacheDir = ".skill-cache"
)

// Node-local agent state layout (05-modules/job-template.md §节点清理).
// It lives on the `home` hostPath beside the workspaces tree, so every rule
// below is covered by the same quiesce probe and the same state-root guard.
const (
	homeDir     = "home"
	multicaDir  = ".multica"
	codexDir    = ".codex"
	profilesDir = "profiles"

	piSessionsDir    = "pi-sessions"
	codexSessionsDir = "multica-sessions"

	hermesSessionsDir = "hermes-sessions"
	dshSessionsDir    = "dsh-sessions"
	reasonixStateDir  = "reasonix-state"
	hermesStateDir    = "hermes-state"
)

// Stats is one round's outcome; byte counts are FreedBytes (reclaimed) and
// CacheBytes (the .repos total left behind).
type Stats struct {
	Skipped     bool  `json:"skipped"`
	TaskDirs    int   `json:"task_dirs"`
	TaskRoots   int   `json:"task_roots"`
	BareRepos   int   `json:"bare_repos"`
	AgentState  int   `json:"agent_state"`
	AgentMemory int   `json:"agent_memory"`
	FreedBytes  int64 `json:"freed_bytes"`
	CacheBytes  int64 `json:"cache_bytes"`
}

// Collector runs the cleanup rules over one state root. It is safe for a
// single goroutine (the DaemonSet runs one per node).
type Collector struct {
	cfg Config
	log *slog.Logger
	now func() time.Time
}

// New builds a Collector; a nil logger falls back to slog.Default.
func New(cfg Config, log *slog.Logger) *Collector {
	if log == nil {
		log = slog.Default()
	}
	return &Collector{
		cfg: cfg,
		log: log,
		now: time.Now,
	}
}

// Run runs one round immediately, then one per FOREMAN_GC_INTERVAL until
// ctx is done. Round failures are logged, never fatal: a bad round must not
// stop the node's collector.
func (c *Collector) Run(ctx context.Context) error {
	for {
		if _, err := c.RunOnce(ctx); err != nil && ctx.Err() == nil {
			c.log.Error("cache.gc.failed", "node", c.cfg.Node, "err", err.Error())
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(c.cfg.Interval):
		}
	}
}

// RunOnce applies the rules in order: quiesce check, TTL reclamation of
// task directories / bare caches / orphan task roots, the .repos size cap by
// LRU, then the node-local agent state and memory rules. Every deletion is
// best-effort: one failing path must not abort the round.
func (c *Collector) RunOnce(ctx context.Context) (Stats, error) {
	var st Stats
	root, err := filepath.Abs(c.cfg.StateRoot)
	if err != nil {
		return st, fmt.Errorf("resolve state root %q: %w", c.cfg.StateRoot, err)
	}
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			c.log.Debug("cache.gc.skipped", "node", c.cfg.Node, "reason", "state root missing", "path", root)
			return st, nil
		}
		return st, fmt.Errorf("stat state root %q: %w", root, err)
	}

	skipped, err := c.quiesced(ctx, root)
	if err != nil {
		return st, err
	}
	if skipped {
		st.Skipped = true
		c.report(st)
		return st, nil
	}

	var errs []error
	workspaces := filepath.Join(root, workspacesDir)

	// Task directories: <root>/workspaces/<workspace>/<task-dir>.
	if n, freed, err := c.expireChildren(ctx, root, workspaces, c.cfg.TaskDirTTL); err != nil {
		errs = append(errs, err)
	} else {
		st.TaskDirs, st.FreedBytes = n, st.FreedBytes+freed
	}

	// Bare caches: <root>/workspaces/.repos/<workspace>/<repo>.git.
	caches, freed, err := c.expireRepos(ctx, root, filepath.Join(workspaces, reposDir))
	st.BareRepos, st.FreedBytes = caches.expired, st.FreedBytes+freed
	if err != nil {
		errs = append(errs, err)
	}

	// Orphan task roots: <root>/workspaces/.task_roots/<hash>.
	if n, freed, err := c.expireDirs(ctx, root, filepath.Join(workspaces, taskRootsDir), c.cfg.TaskDirTTL); err != nil {
		errs = append(errs, err)
	} else {
		st.TaskRoots, st.FreedBytes = n, st.FreedBytes+freed
	}

	// .repos size cap: reclaim oldest-first until the total fits.
	reclaimed, freed, remaining, err := c.enforceCacheLimit(ctx, root, caches.entries)
	st.BareRepos += reclaimed
	st.FreedBytes += freed
	st.CacheBytes = remaining
	if err != nil {
		errs = append(errs, err)
	}

	// Node-local agent state: profile-relative stores, pi-sessions and the
	// codex session stores (<root>/home), then hermes memory.
	states, stateFreed, err := c.expireAgentState(ctx, root)
	st.AgentState, st.FreedBytes = states, st.FreedBytes+stateFreed
	if err != nil {
		errs = append(errs, err)
	}
	c.log.Info("gc.agent_state", "node", c.cfg.Node, "removed_count", states, "freed_bytes", stateFreed)

	memories, memoryFreed, err := c.expireAgentMemory(ctx, root)
	st.AgentMemory, st.FreedBytes = memories, st.FreedBytes+memoryFreed
	if err != nil {
		errs = append(errs, err)
	}
	c.log.Info("gc.agent_memory", "node", c.cfg.Node, "removed_count", memories, "freed_bytes", memoryFreed)

	c.report(st)
	return st, errors.Join(errs...)
}

// quiesced reports whether the state root was written within the quiesce
// window; such a round is skipped so a running Job's directories are never
// raced (05-modules/job-template.md §节点清理).
func (c *Collector) quiesced(ctx context.Context, root string) (bool, error) {
	if c.cfg.Quiesce <= 0 {
		return false, nil
	}
	newest, err := newestMtime(ctx, root, c.now().Add(-c.cfg.Quiesce))
	if err != nil {
		return false, fmt.Errorf("scan state root %q: %w", root, err)
	}
	if newest.IsZero() {
		return false, nil
	}
	return c.now().Sub(newest) < c.cfg.Quiesce, nil
}

// expireChildren deletes every <workspaces>/<workspace>/<child> older than
// ttl, skipping the hidden bookkeeping directories (.repos, .skill-cache,
// .task_roots).
func (c *Collector) expireChildren(ctx context.Context, root, workspaces string, ttl time.Duration) (int, int64, error) {
	spaces, err := readDirs(workspaces)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("scan %q: %w", workspaces, err)
	}
	var (
		deleted int
		freed   int64
		errs    []error
	)
	for _, space := range spaces {
		if ctx.Err() != nil {
			break
		}
		if isBookkeeping(space.Name()) {
			continue
		}
		children, err := readDirs(filepath.Join(workspaces, space.Name()))
		if err != nil {
			errs = append(errs, fmt.Errorf("scan %q: %w", space.Name(), err))
			continue
		}
		for _, child := range children {
			path := filepath.Join(workspaces, space.Name(), child.Name())
			n, ok := c.expire(ctx, root, path, child.Name(), ttl)
			if !ok {
				continue
			}
			deleted++
			freed += n
		}
	}
	return deleted, freed, errors.Join(errs...)
}

// expireDirs deletes every direct child of dir older than ttl.
func (c *Collector) expireDirs(ctx context.Context, root, dir string, ttl time.Duration) (int, int64, error) {
	entries, err := readDirs(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("scan %q: %w", dir, err)
	}
	var (
		deleted int
		freed   int64
	)
	for _, e := range entries {
		if ctx.Err() != nil {
			break
		}
		n, ok := c.expire(ctx, root, filepath.Join(dir, e.Name()), e.Name(), ttl)
		if !ok {
			continue
		}
		deleted++
		freed += n
	}
	return deleted, freed, nil
}

// cache is a bare repository measured during the TTL scan and reused for
// the LRU pass.
type cache struct {
	path   string
	usedAt time.Time
	bytes  int64
	gone   bool
}

type cacheScan struct {
	entries []cache
	expired int
}

// expireRepos reclaims bare caches by TTL and returns the survivors for the
// size cap (<root>/workspaces/.repos/<workspace>/<repo>.git).
func (c *Collector) expireRepos(ctx context.Context, root, repos string) (cacheScan, int64, error) {
	var (
		scan  cacheScan
		freed int64
		errs  []error
	)
	spaces, err := readDirs(repos)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return scan, 0, nil
		}
		return scan, 0, fmt.Errorf("scan %q: %w", repos, err)
	}
	for _, space := range spaces {
		if ctx.Err() != nil {
			break
		}
		reposOfSpace, err := readDirs(filepath.Join(repos, space.Name()))
		if err != nil {
			errs = append(errs, fmt.Errorf("scan %q: %w", space.Name(), err))
			continue
		}
		for _, repo := range reposOfSpace {
			if ctx.Err() != nil {
				break
			}
			path := filepath.Join(repos, space.Name(), repo.Name())
			usedAt, bytes, err := measure(ctx, path)
			if err != nil {
				errs = append(errs, fmt.Errorf("measure %q: %w", path, err))
				continue
			}
			if c.expired(usedAt, c.cfg.CacheTTL) {
				n, err := c.delete(root, path, repo.Name(), "bare-cache")
				if err != nil {
					errs = append(errs, err)
					continue
				}
				freed += n
				scan.expired++
				scan.entries = append(scan.entries, cache{path: path, usedAt: usedAt, bytes: bytes, gone: true})
				continue
			}
			scan.entries = append(scan.entries, cache{path: path, usedAt: usedAt, bytes: bytes})
		}
	}
	return scan, freed, errors.Join(errs...)
}

// enforceCacheLimit deletes the least recently used bare caches until the
// total size of .repos is within FOREMAN_CACHE_MAX_BYTES.
func (c *Collector) enforceCacheLimit(ctx context.Context, root string, entries []cache) (int, int64, int64, error) {
	var total int64
	live := make([]cache, 0, len(entries))
	for _, e := range entries {
		if e.gone {
			continue
		}
		total += e.bytes
		live = append(live, e)
	}
	var (
		deleted int
		freed   int64
		errs    []error
	)
	for total > c.cfg.CacheMaxBytes {
		oldest := -1
		for i, e := range live {
			if e.gone {
				continue
			}
			if oldest < 0 || e.usedAt.Before(live[oldest].usedAt) {
				oldest = i
			}
		}
		if oldest < 0 {
			break
		}
		if ctx.Err() != nil {
			break
		}
		e := &live[oldest]
		n, err := c.delete(root, e.path, filepath.Base(e.path), "cache-lru")
		if err != nil {
			errs = append(errs, err)
			e.gone = true // do not retry the same path in this round
			continue
		}
		e.gone = true
		deleted++
		freed += n
		total -= n
	}
	return deleted, freed, total, errors.Join(errs...)
}

// expireAgentState reclaims node-local agent sessions and transcripts: the
// profile-relative store roots under
// <root>/home/.multica/profiles/*/{hermes-sessions,dsh-sessions,reasonix-state},
// the <root>/home/.multica/pi-sessions/*.jsonl files and the files under
// <root>/home/.codex/multica-sessions. A zero TTL turns the rule off.
func (c *Collector) expireAgentState(ctx context.Context, root string) (int, int64, error) {
	if c.cfg.AgentStateTTL <= 0 {
		return 0, 0, nil
	}
	ttl := c.cfg.AgentStateTTL
	var (
		removed int
		freed   int64
		errs    []error
	)
	profiles, err := readDirs(filepath.Join(root, homeDir, multicaDir, profilesDir))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, fmt.Errorf("scan profiles under %q: %w", root, err))
	}
	for _, profile := range profiles {
		for _, store := range []string{hermesSessionsDir, dshSessionsDir, reasonixStateDir} {
			dir := filepath.Join(root, homeDir, multicaDir, profilesDir, profile.Name(), store)
			n, f, err := c.expireStores(ctx, root, dir, ttl)
			removed, freed = removed+n, freed+f
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	// pi-sessions is a flat *.jsonl directory; the codex session stores hold
	// their files at any depth beneath the store namespace.
	pi := filepath.Join(root, homeDir, multicaDir, piSessionsDir)
	n, f, err := c.expireFiles(ctx, root, pi, ".jsonl", ttl, false)
	removed, freed = removed+n, freed+f
	if err != nil {
		errs = append(errs, err)
	}
	codex := filepath.Join(root, homeDir, codexDir, codexSessionsDir)
	n, f, err = c.expireFiles(ctx, root, codex, "", ttl, true)
	removed, freed = removed+n, freed+f
	if err != nil {
		errs = append(errs, err)
	}
	return removed, freed, errors.Join(errs...)
}

// expireAgentMemory reclaims node-local agent memory (hermes-state) with
// FOREMAN_AGENT_MEMORY_TTL; a zero TTL turns the rule off.
func (c *Collector) expireAgentMemory(ctx context.Context, root string) (int, int64, error) {
	if c.cfg.AgentMemoryTTL <= 0 {
		return 0, 0, nil
	}
	var (
		removed int
		freed   int64
		errs    []error
	)
	profiles, err := readDirs(filepath.Join(root, homeDir, multicaDir, profilesDir))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, fmt.Errorf("scan profiles under %q: %w", root, err))
	}
	for _, profile := range profiles {
		dir := filepath.Join(root, homeDir, multicaDir, profilesDir, profile.Name(), hermesStateDir)
		n, f, err := c.expireStores(ctx, root, dir, c.cfg.AgentMemoryTTL)
		removed, freed = removed+n, freed+f
		if err != nil {
			errs = append(errs, err)
		}
	}
	return removed, freed, errors.Join(errs...)
}

// expireStores reclaims stale store directories under dir. Stores nest under
// container directories upstream (<agent>/<profile>/<store>), so a directory
// holding regular files is one store entry — removed whole, never split —
// while a directory holding only subdirectories is a container and is
// descended into. The store roots and the profile roots themselves are never
// entries, so a profile emptied by the rule keeps its root.
func (c *Collector) expireStores(ctx context.Context, root, dir string, ttl time.Duration) (int, int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("scan %q: %w", dir, err)
	}
	return c.expireStoreChildren(ctx, root, dir, entries, ttl)
}

// expireStoreChildren applies the store-entry rule to one already-read
// directory level.
func (c *Collector) expireStoreChildren(ctx context.Context, root, dir string, entries []fs.DirEntry, ttl time.Duration) (int, int64, error) {
	var (
		removed int
		freed   int64
		errs    []error
	)
	for _, e := range entries {
		if ctx.Err() != nil {
			break
		}
		if !e.IsDir() {
			continue // stores are directories; stray files at a container level stay
		}
		store := filepath.Join(dir, e.Name())
		children, err := os.ReadDir(store)
		if err != nil {
			errs = append(errs, fmt.Errorf("scan %q: %w", store, err))
			continue
		}
		if isContainer(children) {
			n, f, err := c.expireStoreChildren(ctx, root, store, children, ttl)
			removed, freed = removed+n, freed+f
			if err != nil {
				errs = append(errs, err)
			}
			continue
		}
		n, ok := c.expire(ctx, root, store, e.Name(), ttl)
		if !ok {
			continue
		}
		removed++
		freed += n
	}
	return removed, freed, errors.Join(errs...)
}

// isContainer reports whether entries are only subdirectories, i.e. the
// directory nests stores further down instead of being one. An empty
// directory is not a container: a store shell left behind is still judged by
// its own mtime.
func isContainer(entries []fs.DirEntry) bool {
	if len(entries) == 0 {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() {
			return false
		}
	}
	return true
}

// expireFiles deletes regular files under dir whose own mtime is older than
// ttl. ext narrows the rule to one suffix ("" = every file) and deep also
// walks subdirectories.
func (c *Collector) expireFiles(ctx context.Context, root, dir, ext string, ttl time.Duration, deep bool) (int, int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("scan %q: %w", dir, err)
	}
	var (
		removed int
		freed   int64
		errs    []error
	)
	for _, e := range entries {
		if ctx.Err() != nil {
			break
		}
		path := filepath.Join(dir, e.Name())
		if e.IsDir() {
			if !deep {
				continue
			}
			n, f, err := c.expireFiles(ctx, root, path, ext, ttl, deep)
			removed, freed = removed+n, freed+f
			if err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if !e.Type().IsRegular() || (ext != "" && filepath.Ext(e.Name()) != ext) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			errs = append(errs, fmt.Errorf("stat %q: %w", path, err))
			continue
		}
		if !c.expired(info.ModTime(), ttl) {
			continue
		}
		n, err := c.delete(root, path, e.Name(), "agent-state")
		if err != nil {
			errs = append(errs, err)
			continue
		}
		removed++
		freed += n
	}
	return removed, freed, errors.Join(errs...)
}

// expire deletes path when its newest write is older than ttl, reporting
// whether it was removed.
func (c *Collector) expire(ctx context.Context, root, path, name string, ttl time.Duration) (int64, bool) {
	usedAt, _, err := measure(ctx, path)
	if err != nil {
		c.log.Warn("cache.gc.measure_failed", "node", c.cfg.Node, "path", path, "err", err.Error())
		return 0, false
	}
	if !c.expired(usedAt, ttl) {
		return 0, false
	}
	n, err := c.delete(root, path, name, "ttl")
	if err != nil {
		c.log.Warn("cache.gc.delete_failed", "node", c.cfg.Node, "path", path, "err", err.Error())
		return 0, false
	}
	return n, true
}

func (c *Collector) expired(usedAt time.Time, ttl time.Duration) bool {
	if usedAt.IsZero() {
		return false
	}
	return c.now().Sub(usedAt) > ttl
}

// delete removes path, refusing anything outside root.
func (c *Collector) delete(root, path, name, reason string) (int64, error) {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return 0, fmt.Errorf("refusing to delete %q outside state root %q", path, root)
	}
	bytes := dirBytes(path)
	if err := os.RemoveAll(path); err != nil {
		return 0, fmt.Errorf("remove %q: %w", path, err)
	}
	c.log.Debug("cache.gc.delete", "node", c.cfg.Node, "path", path, "name", name, "reason", reason, "freed_bytes", bytes)
	return bytes, nil
}

func (c *Collector) report(st Stats) {
	c.log.Info("cache.gc",
		"node", c.cfg.Node,
		"skipped", st.Skipped,
		"freed_bytes", st.FreedBytes,
		"task_dirs", st.TaskDirs,
		"bare_repos", st.BareRepos,
	)
}

// isBookkeeping reports whether name is one of the state root's hidden
// bookkeeping directories, which the task-directory scan skips.
func isBookkeeping(name string) bool {
	switch name {
	case reposDir, taskRootsDir, skillCacheDir:
		return true
	default:
		return false
	}
}

func readDirs(path string) ([]fs.DirEntry, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	dirs := entries[:0]
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e)
		}
	}
	return dirs, nil
}

// measure returns the newest mtime and the total size of the tree at path
// (symlinks are not followed).
func measure(ctx context.Context, path string) (time.Time, int64, error) {
	var (
		newest time.Time
		bytes  int64
	)
	err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.ModTime().After(newest) {
			newest = info.ModTime()
		}
		if info.Mode().IsRegular() {
			bytes += info.Size()
		}
		return nil
	})
	if err != nil {
		return time.Time{}, 0, err
	}
	return newest, bytes, nil
}

// newestMtime returns the newest mtime in the tree at root, stopping as
// soon as a write newer than floor is seen (the quiesce probe).
func newestMtime(ctx context.Context, root string, floor time.Time) (time.Time, error) {
	var newest time.Time
	stop := errors.New("gc: quiesce hit")
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.ModTime().After(newest) {
			newest = info.ModTime()
		}
		if !floor.IsZero() && newest.After(floor) {
			return stop
		}
		return nil
	})
	if err != nil && !errors.Is(err, stop) {
		return time.Time{}, err
	}
	return newest, nil
}

// dirBytes sums the regular-file sizes of the tree at path.
func dirBytes(path string) int64 {
	var bytes int64
	_ = filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if info.Mode().IsRegular() {
			bytes += info.Size()
		}
		return nil
	})
	return bytes
}
