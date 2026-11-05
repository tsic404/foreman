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

// State-root layout (ADR-005 / §8.8).
const (
	workspacesDir = "workspaces"
	reposDir      = ".repos"
	taskRootsDir  = ".task_roots"
	skillCacheDir = ".skill-cache"
)

// Skill-bundle cache layout the upstream daemon writes (anchored in
// docs/upstream-anchors.md): <workspaces>/.skill-cache/<version>/<workspace>/
// <source>/<id>/<hash>/bundle.json. A directory holding the bundle file is one
// entry; the depth bound keeps a foreign tree from turning a round into an
// unbounded crawl.
const (
	skillBundleFile    = "bundle.json"
	skillEntryMaxDepth = 4
)

// Node-local agent state layout (§8.8).
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
// CacheBytes (the .repos total left behind). Skipped reports that at least one
// unit was left untouched because its tree was written inside the quiesce
// window; the rest of the round still runs (§8.8 judges the window per
// workspace). CacheMeasured tells whether the total came from a scan of the
// whole cache tree: a skipped or interrupted scan leaves it false, so its zero
// value is never read as an empty cache.
type Stats struct {
	Skipped       bool  `json:"skipped"`
	TaskDirs      int   `json:"task_dirs"`
	TaskRoots     int   `json:"task_roots"`
	BareRepos     int   `json:"bare_repos"`
	SkillCache    int   `json:"skill_cache"`
	AgentState    int   `json:"agent_state"`
	AgentMemory   int   `json:"agent_memory"`
	FreedBytes    int64 `json:"freed_bytes"`
	CacheBytes    int64 `json:"cache_bytes"`
	CacheMeasured bool  `json:"cache_measured"`
}

// Collector runs the cleanup rules over one state root. It is safe for a
// single goroutine (the DaemonSet runs one per node).
type Collector struct {
	cfg Config
	log *slog.Logger
	now func() time.Time
	// remeasure re-reads a bare cache right before the LRU pass deletes it.
	// It is a field so a test can simulate a fetch landing after the scan.
	remeasure func(context.Context, string) (time.Time, int64, error)
	// skippedUnits counts this round's units left untouched by the quiesce
	// window; RunOnce folds it into Stats.Skipped.
	skippedUnits int
	// busyByWorkspace is this round's quiesce verdict per workspace id. §8.8
	// skips a workspace's whole set of rules, so a task writing its
	// directories also keeps the bare caches it reads — reads do not refresh
	// mtime — from being reclaimed underneath it.
	busyByWorkspace map[string]bool
}

// New builds a Collector; a nil logger falls back to slog.Default.
func New(cfg Config, log *slog.Logger) *Collector {
	if log == nil {
		log = slog.Default()
	}
	return &Collector{
		cfg:       cfg,
		log:       log,
		now:       time.Now,
		remeasure: measure,
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

// RunOnce applies the rules in order: TTL reclamation of task directories /
// bare caches / orphan task roots, the .repos size cap by LRU, the per-
// workspace skill bundle cache rules (TTL and size cap), then the node-local
// agent state and memory rules. The quiesce window is probed once per
// workspace and shared by all of its rules, so one busy workspace never
// stalls another. Every deletion is best-effort: one failing path must not
// abort the round.
func (c *Collector) RunOnce(ctx context.Context) (Stats, error) {
	var st Stats
	c.skippedUnits = 0
	c.busyByWorkspace = nil
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

	var errs []error
	busy, err := c.probeWorkspaces(ctx, root)
	if err != nil {
		errs = append(errs, err)
	}
	c.busyByWorkspace = busy
	workspaces := filepath.Join(root, workspacesDir)

	// Task directories: <root>/workspaces/<workspace>/<task-dir>.
	if n, freed, err := c.expireChildren(ctx, root, workspaces, c.cfg.TaskDirTTL); err != nil {
		errs = append(errs, err)
	} else {
		st.TaskDirs, st.FreedBytes = n, st.FreedBytes+freed
	}

	// Bare caches: <root>/workspaces/.repos/<workspace>/<repo>.git.
	caches, freed, unscanned, scanErr := c.expireRepos(ctx, root, filepath.Join(workspaces, reposDir))
	st.BareRepos, st.FreedBytes = caches.expired, st.FreedBytes+freed
	if scanErr != nil {
		errs = append(errs, scanErr)
	}
	// The total is only a measurement when the whole tree was walked: a
	// failed or cancelled scan, and a cache unit skipped for the quiesce
	// window, leave entries out — so the count would read low rather than
	// unknown.
	scanComplete := scanErr == nil && ctx.Err() == nil && unscanned == 0

	// Orphan task roots: <root>/workspaces/.task_roots/<hash>. The unit is
	// not partitioned by workspace, so it passes the literal-path gate on its
	// own; a link here belongs to no workspace and is never walked.
	taskRoots := filepath.Join(workspaces, taskRootsDir)
	if literal, err := c.literalPath(root, taskRoots); err != nil {
		errs = append(errs, err)
	} else if literal {
		if n, freed, err := c.expireDirs(ctx, taskRoots, c.cfg.TaskDirTTL); err != nil {
			errs = append(errs, err)
		} else {
			st.TaskRoots, st.FreedBytes = n, st.FreedBytes+freed
		}
	}

	// .repos size cap: reclaim oldest-first until the total fits.
	reclaimed, freed, remaining, err := c.enforceCacheLimit(ctx, caches.entries)
	st.BareRepos += reclaimed
	st.FreedBytes += freed
	st.CacheBytes = remaining
	st.CacheMeasured = scanComplete
	if err != nil {
		errs = append(errs, err)
	}

	// Node-side skill bundle cache: the upstream daemon stores resolved
	// bundles under <root>/workspaces/.skill-cache; the design leaves their
	// reclamation to this component, with capacity and retention judged per
	// workspace (§8.8).
	bundles, bundleFreed, err := c.expireSkillCache(ctx, root)
	st.SkillCache, st.FreedBytes = bundles, st.FreedBytes+bundleFreed
	if err != nil {
		errs = append(errs, err)
	}

	// Node-local agent state: profile-relative stores, pi-sessions and the
	// codex session stores (<root>/home), then hermes memory. Both rules
	// judge the same home roots, so the round picks them once.
	homes, err := c.homeRootsForRound(ctx, root)
	if err != nil {
		errs = append(errs, err)
	}
	states, stateFreed, err := c.expireAgentState(ctx, homes)
	st.AgentState, st.FreedBytes = states, st.FreedBytes+stateFreed
	if err != nil {
		errs = append(errs, err)
	}
	c.log.Info("gc.agent_state", "node", c.cfg.Node, "removed_count", states, "freed_bytes", stateFreed)

	memories, memoryFreed, err := c.expireAgentMemory(ctx, homes)
	st.AgentMemory, st.FreedBytes = memories, st.FreedBytes+memoryFreed
	if err != nil {
		errs = append(errs, err)
	}
	c.log.Info("gc.agent_memory", "node", c.cfg.Node, "removed_count", memories, "freed_bytes", memoryFreed)

	st.Skipped = c.skippedUnits > 0
	c.report(st)
	return st, errors.Join(errs...)
}

// busy reports whether the tree at path was written within the quiesce
// window.
func (c *Collector) busy(ctx context.Context, path string) (bool, error) {
	if c.cfg.Quiesce <= 0 {
		return false, nil
	}
	newest, err := newestMtime(ctx, path, c.now().Add(-c.cfg.Quiesce))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("scan %q: %w", path, err)
	}
	return !newest.IsZero() && c.now().Sub(newest) < c.cfg.Quiesce, nil
}

// probeWorkspaces reports, per workspace id, whether any of that workspace's
// state trees was written inside the quiesce window. §8.8 skips a workspace's
// whole set of rules, not one tree of it: a task writing its task directory
// also keeps its bare caches — read without refreshing mtime — from being
// reclaimed in the same round. A tree that cannot be read counts as busy, so
// an unreadable one is never cleaned.
func (c *Collector) probeWorkspaces(ctx context.Context, root string) (map[string]bool, error) {
	if c.cfg.Quiesce <= 0 {
		return nil, nil
	}
	trees, err := workspaceTrees(root)
	busy := make(map[string]bool, len(trees))
	var errs []error
	errs = append(errs, err)
	for id, paths := range trees {
		for _, path := range paths {
			written, probeErr := c.busy(ctx, path)
			if probeErr != nil {
				errs = append(errs, probeErr)
				written = true
			}
			if written {
				busy[id] = true
				break
			}
		}
	}
	return busy, errors.Join(errs...)
}

// workspaceTrees lists each workspace's state trees under root, keyed by
// workspace id: its task directory, bare caches, skill bundle caches and
// partitioned home root.
func workspaceTrees(root string) (map[string][]string, error) {
	trees := make(map[string][]string)
	var errs []error
	collect := func(dir string, skip func(string) bool) {
		entries, err := readDirs(dir)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, fmt.Errorf("scan %q: %w", dir, err))
			}
			return
		}
		for _, entry := range entries {
			if skip != nil && skip(entry.Name()) {
				continue
			}
			id := entry.Name()
			trees[id] = append(trees[id], filepath.Join(dir, id))
		}
	}
	workspaces := filepath.Join(root, workspacesDir)
	collect(workspaces, isBookkeeping)
	collect(filepath.Join(workspaces, reposDir), nil)
	collect(filepath.Join(root, homeDir), hiddenEntry)
	versions, err := readDirs(filepath.Join(workspaces, skillCacheDir))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, fmt.Errorf("scan %q: %w", filepath.Join(workspaces, skillCacheDir), err))
	}
	for _, version := range versions {
		collect(filepath.Join(workspaces, skillCacheDir, version.Name()), nil)
	}
	return trees, errors.Join(errs...)
}

// hiddenEntry reports whether name is a dot entry, which the walk of a home
// root skips.
func hiddenEntry(name string) bool { return strings.HasPrefix(name, ".") }

// workspaceBusy reports whether workspace id is inside the quiesce window.
func (c *Collector) workspaceBusy(id string) bool {
	return c.busyByWorkspace[id]
}

// skipUnit applies the gates a workspace unit must pass before a rule walks
// it. A unit reached through a symbolic link is left alone: the link
// redirects the walk into another workspace's tree (§8.8: 不跨 workspace
// 清删). A unit of a busy workspace is deferred to a later round.
func (c *Collector) skipUnit(ctx context.Context, root, unit, workspace string) (bool, error) {
	literal, err := c.literalPath(root, unit)
	if err != nil {
		return true, err
	}
	if !literal {
		return true, nil
	}
	if c.workspaceBusy(workspace) {
		c.skippedUnits++
		return true, nil
	}
	return false, nil
}

// literalPath reports whether path is reached from root without a symbolic
// link on any component, logging the skip when one is found: the link would
// redirect the walk and a later delete would act on its target, possibly
// another workspace's tree (§8.8: 不跨 workspace 清删).
func (c *Collector) literalPath(root, path string) (bool, error) {
	link, err := linkedSegment(root, path)
	if err != nil {
		return false, fmt.Errorf("inspect %q: %w", path, err)
	}
	if link == "" {
		return true, nil
	}
	c.log.Warn("cache.gc.symlink_skipped", "node", c.cfg.Node, "path", path, "segment", link)
	return false, nil
}

// linkedSegment returns the first component of path below root that is a
// symbolic link, or "" when the path is literal all the way down. A missing
// component ends the walk: there is nothing left to follow.
func linkedSegment(root, path string) (string, error) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", err
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", nil // outside the root; delete reports that on its own
	}
	current := root
	for _, segment := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, segment)
		info, err := os.Lstat(current)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return "", nil
			}
			return "", err
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			return current, nil
		}
	}
	return "", nil
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
		unit := filepath.Join(workspaces, space.Name())
		skip, err := c.skipUnit(ctx, root, unit, space.Name())
		if err != nil {
			errs = append(errs, fmt.Errorf("scan %q: %w", space.Name(), err))
		}
		if skip {
			continue
		}
		children, err := readDirs(unit)
		if err != nil {
			errs = append(errs, fmt.Errorf("scan %q: %w", space.Name(), err))
			continue
		}
		for _, child := range children {
			path := filepath.Join(unit, child.Name())
			n, ok := c.expire(ctx, unit, path, child.Name(), ttl)
			if !ok {
				continue
			}
			deleted++
			freed += n
		}
	}
	return deleted, freed, errors.Join(errs...)
}

// expireDirs deletes every direct child of dir older than ttl. dir is also
// the root every deletion stays within.
func (c *Collector) expireDirs(ctx context.Context, dir string, ttl time.Duration) (int, int64, error) {
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
		n, ok := c.expire(ctx, dir, filepath.Join(dir, e.Name()), e.Name(), ttl)
		if !ok {
			continue
		}
		deleted++
		freed += n
	}
	return deleted, freed, nil
}

// cacheEntry is one reclaimable cache tree (a bare repository or a skill
// bundle entry) measured during the scan and reused for the LRU pass. unit is
// the tree the entry belongs to and the root its deletion stays within.
type cacheEntry struct {
	path   string
	unit   string
	usedAt time.Time
	bytes  int64
	gone   bool
}

type cacheScan struct {
	entries []cacheEntry
	expired int
}

// expireRepos reclaims bare caches by TTL and returns the survivors for the
// size cap (<root>/workspaces/.repos/<workspace>/<repo>.git). The skipped
// count covers the workspace units this round did not walk, so the caller can
// mark the byte total unmeasured.
func (c *Collector) expireRepos(ctx context.Context, root, repos string) (cacheScan, int64, int, error) {
	var (
		scan      cacheScan
		freed     int64
		unscanned int
		errs      []error
	)
	spaces, err := readDirs(repos)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return scan, 0, 0, nil
		}
		return scan, 0, 0, fmt.Errorf("scan %q: %w", repos, err)
	}
	for _, space := range spaces {
		if ctx.Err() != nil {
			break
		}
		unit := filepath.Join(repos, space.Name())
		skip, err := c.skipUnit(ctx, root, unit, space.Name())
		if err != nil {
			errs = append(errs, fmt.Errorf("scan %q: %w", space.Name(), err))
		}
		if skip {
			unscanned++
			continue
		}
		reposOfSpace, err := readDirs(unit)
		if err != nil {
			errs = append(errs, fmt.Errorf("scan %q: %w", space.Name(), err))
			continue
		}
		for _, repo := range reposOfSpace {
			if ctx.Err() != nil {
				break
			}
			path := filepath.Join(unit, repo.Name())
			usedAt, bytes, err := measure(ctx, path)
			if err != nil {
				errs = append(errs, fmt.Errorf("measure %q: %w", path, err))
				continue
			}
			if c.expired(usedAt, c.cfg.CacheTTL) {
				n, err := c.delete(unit, path, repo.Name(), "bare-cache")
				if isSymlinkSkip(err) {
					scan.entries = append(scan.entries, cacheEntry{path: path, unit: unit, usedAt: usedAt, bytes: bytes})
					continue
				}
				if err != nil {
					errs = append(errs, err)
					continue
				}
				freed += n
				scan.expired++
				scan.entries = append(scan.entries, cacheEntry{path: path, unit: unit, usedAt: usedAt, bytes: bytes, gone: true})
				continue
			}
			scan.entries = append(scan.entries, cacheEntry{path: path, unit: unit, usedAt: usedAt, bytes: bytes})
		}
	}
	return scan, freed, unscanned, errors.Join(errs...)
}

// enforceCacheLimit deletes the least recently used bare caches until the
// total size of .repos is within FOREMAN_CACHE_MAX_BYTES.
func (c *Collector) enforceCacheLimit(ctx context.Context, entries []cacheEntry) (int, int64, int64, error) {
	live := make([]cacheEntry, 0, len(entries))
	for _, e := range entries {
		if !e.gone {
			live = append(live, e)
		}
	}
	return c.reclaimToLimit(ctx, live, c.cfg.CacheMaxBytes, "cache-lru")
}

// reclaimToLimit deletes the least recently used entries until their total
// size fits limit. The scan's mtime is only a snapshot: an entry may be
// re-written after the scan, so each victim is re-measured right before
// deletion and an entry written since the scan is skipped for this round (it
// is no longer the least recently used). That skip is not airtight: a write
// landing between re-measure and delete, or one that leaves the mtime at the
// scanned value (strict After below), still has its entry deleted this round —
// an accepted residual window, not eliminable while deletion stays lock-free.
func (c *Collector) reclaimToLimit(ctx context.Context, live []cacheEntry, limit int64, reason string) (int, int64, int64, error) {
	var total int64
	for _, e := range live {
		total += e.bytes
	}
	var (
		deleted int
		freed   int64
		errs    []error
	)
	for total > limit {
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
		// TTL expiry above keeps its scan-time semantics: its 168h/720h
		// windows make the scan→delete race practically unreachable.
		usedAt, bytes, err := c.remeasure(ctx, e.path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				total -= e.bytes // already gone, not part of the remaining total
			} else {
				errs = append(errs, fmt.Errorf("re-measure %q: %w", e.path, err))
			}
			e.gone = true // do not retry the same path in this round
			continue
		}
		if usedAt.After(e.usedAt) {
			total += bytes - e.bytes
			e.usedAt, e.bytes = usedAt, bytes
			e.gone = true // in flight: skip it, evict the next oldest instead
			c.log.Debug("cache.gc.lru_skip", "node", c.cfg.Node, "path", e.path, "reason", "cache refreshed since scan")
			continue
		}
		n, err := c.delete(e.unit, e.path, filepath.Base(e.path), reason)
		if err != nil {
			if !isSymlinkSkip(err) {
				errs = append(errs, err)
			}
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

// expireSkillCache reclaims the node-side skill bundle cache the upstream
// daemon keeps under <root>/workspaces/.skill-cache (§8.8): expired entries
// first, then each workspace's total trimmed to the size cap, least recently
// used first. Capacity is judged per workspace, so one workspace's growth
// never evicts another workspace's entries.
func (c *Collector) expireSkillCache(ctx context.Context, root string) (int, int64, error) {
	base := filepath.Join(root, workspacesDir, skillCacheDir)
	versions, err := readDirs(base)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("scan %q: %w", base, err)
	}
	var (
		removed int
		freed   int64
		errs    []error
		live    = make(map[string][]cacheEntry)
	)
	for _, version := range versions {
		spaces, err := readDirs(filepath.Join(base, version.Name()))
		if err != nil {
			errs = append(errs, fmt.Errorf("scan %q: %w", version.Name(), err))
			continue
		}
		for _, space := range spaces {
			if ctx.Err() != nil {
				return removed, freed, errors.Join(errs...)
			}
			unit := filepath.Join(base, version.Name(), space.Name())
			skip, err := c.skipUnit(ctx, root, unit, space.Name())
			if err != nil {
				errs = append(errs, fmt.Errorf("scan %q: %w", space.Name(), err))
			}
			if skip {
				continue
			}
			entries, err := c.scanSkillEntries(ctx, unit)
			if err != nil {
				errs = append(errs, fmt.Errorf("scan %q: %w", space.Name(), err))
				continue
			}
			for _, e := range entries {
				if c.expired(e.usedAt, c.cfg.SkillCacheTTL) {
					n, err := c.delete(unit, e.path, filepath.Base(e.path), "skill-cache")
					if err != nil {
						if !isSymlinkSkip(err) {
							errs = append(errs, err)
						}
						continue
					}
					removed++
					freed += n
					continue
				}
				// Aggregate across cache versions: the cap is one
				// workspace's total, not one version's.
				live[space.Name()] = append(live[space.Name()], e)
			}
		}
	}
	for _, entries := range live {
		n, f, _, err := c.reclaimToLimit(ctx, entries, c.cfg.SkillCacheMaxBytes, "skill-cache-lru")
		removed, freed = removed+n, freed+f
		if err != nil {
			errs = append(errs, err)
		}
	}
	return removed, freed, errors.Join(errs...)
}

// scanSkillEntries lists one workspace's skill bundle entries: a directory
// holding bundle.json is one entry and is not descended into, any other
// directory is a container on the way to one.
func (c *Collector) scanSkillEntries(ctx context.Context, spaceRoot string) ([]cacheEntry, error) {
	var entries []cacheEntry
	err := filepath.WalkDir(spaceRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(spaceRoot, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if depth := len(strings.Split(rel, string(filepath.Separator))); depth > skillEntryMaxDepth {
			return fs.SkipDir
		}
		bundle, err := os.Stat(filepath.Join(path, skillBundleFile))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !bundle.Mode().IsRegular() {
			return nil
		}
		usedAt, bytes, err := measure(ctx, path)
		if err != nil {
			return err
		}
		entries = append(entries, cacheEntry{path: path, unit: spaceRoot, usedAt: usedAt, bytes: bytes})
		return fs.SkipDir
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// expireAgentState reclaims node-local agent sessions and transcripts: the
// profile-relative store roots under
// <root>/home/.multica/profiles/*/{hermes-sessions,dsh-sessions,reasonix-state},
// the <root>/home/.multica/pi-sessions/*.jsonl files and the files under
// <root>/home/.codex/multica-sessions. A zero TTL turns the rule off.
func (c *Collector) expireAgentState(ctx context.Context, homes []agentTrees) (int, int64, error) {
	if c.cfg.AgentStateTTL <= 0 {
		return 0, 0, nil
	}
	ttl := c.cfg.AgentStateTTL
	var (
		removed int
		freed   int64
		errs    []error
	)
	for _, home := range homes {
		if home.multica != "" {
			profiles, err := readDirs(filepath.Join(home.multica, profilesDir))
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, fmt.Errorf("scan profiles under %q: %w", home.multica, err))
			}
			for _, profile := range profiles {
				for _, store := range []string{hermesSessionsDir, dshSessionsDir, reasonixStateDir} {
					dir := filepath.Join(home.multica, profilesDir, profile.Name(), store)
					n, f, err := c.expireStores(ctx, home.multica, dir, ttl)
					removed, freed = removed+n, freed+f
					if err != nil {
						errs = append(errs, err)
					}
				}
			}
			// pi-sessions is a flat *.jsonl directory.
			n, f, err := c.expireFiles(ctx, home.multica, filepath.Join(home.multica, piSessionsDir), ".jsonl", ttl, false)
			removed, freed = removed+n, freed+f
			if err != nil {
				errs = append(errs, err)
			}
		}
		if home.codex != "" {
			// The codex session stores hold their files at any depth beneath
			// the store namespace.
			codex := filepath.Join(home.codex, codexSessionsDir)
			n, f, err := c.expireFiles(ctx, home.codex, codex, "", ttl, true)
			removed, freed = removed+n, freed+f
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	return removed, freed, errors.Join(errs...)
}

// homeRoot is one home root to walk: the legacy shared root (ws == "") and
// every workspace-partitioned root the template wrote (<root>/home/<ws>,
// expressed through the workspace placeholder, §8.8).
type homeRoot struct {
	path string
	ws   string
}

// homeRoots lists the home roots a round may walk. Each root is judged
// independently, so a round never reaches across workspaces.
func homeRoots(root string) []homeRoot {
	home := filepath.Join(root, homeDir)
	roots := []homeRoot{{path: home}}
	entries, err := readDirs(home)
	if err != nil {
		return roots
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		roots = append(roots, homeRoot{path: filepath.Join(home, name), ws: name})
	}
	return roots
}

// agentTrees is one home root this round may reclaim. multica and codex are
// the two state trees the rules below it walk, and the roots their deletions
// stay within; "" means the tree is absent or reached through a symbolic link,
// which the rules never follow.
type agentTrees struct {
	multica string
	codex   string
}

// homeRootsForRound drops the home roots this round must not touch — reached
// through a symbolic link, or inside the quiesce window — and settles the two
// state trees of the surviving roots. A partitioned root follows its
// workspace's verdict; the legacy shared root has no workspace and is probed
// on its own.
func (c *Collector) homeRootsForRound(ctx context.Context, root string) ([]agentTrees, error) {
	var (
		trees []agentTrees
		errs  []error
	)
	for _, home := range homeRoots(root) {
		if literal, err := c.literalPath(root, home.path); err != nil {
			errs = append(errs, err)
			continue
		} else if !literal {
			continue
		}
		busy := c.workspaceBusy(home.ws)
		if home.ws == "" {
			written, err := c.busy(ctx, home.path)
			if err != nil {
				errs = append(errs, err)
			}
			busy = written
		}
		if busy {
			c.skippedUnits++
			continue
		}
		var t agentTrees
		if literal, err := c.literalPath(root, filepath.Join(home.path, multicaDir)); err != nil {
			errs = append(errs, err)
		} else if literal {
			t.multica = filepath.Join(home.path, multicaDir)
		}
		if literal, err := c.literalPath(root, filepath.Join(home.path, codexDir)); err != nil {
			errs = append(errs, err)
		} else if literal {
			t.codex = filepath.Join(home.path, codexDir)
		}
		trees = append(trees, t)
	}
	return trees, errors.Join(errs...)
}

// expireAgentMemory reclaims node-local agent memory (hermes-state) with
// FOREMAN_AGENT_MEMORY_TTL; a zero TTL turns the rule off.
func (c *Collector) expireAgentMemory(ctx context.Context, homes []agentTrees) (int, int64, error) {
	if c.cfg.AgentMemoryTTL <= 0 {
		return 0, 0, nil
	}
	var (
		removed int
		freed   int64
		errs    []error
	)
	for _, home := range homes {
		if home.multica == "" {
			continue
		}
		profiles, err := readDirs(filepath.Join(home.multica, profilesDir))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("scan profiles under %q: %w", home.multica, err))
		}
		for _, profile := range profiles {
			dir := filepath.Join(home.multica, profilesDir, profile.Name(), hermesStateDir)
			n, f, err := c.expireStores(ctx, home.multica, dir, c.cfg.AgentMemoryTTL)
			removed, freed = removed+n, freed+f
			if err != nil {
				errs = append(errs, err)
			}
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
func (c *Collector) expireStores(ctx context.Context, unit, dir string, ttl time.Duration) (int, int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("scan %q: %w", dir, err)
	}
	return c.expireStoreChildren(ctx, unit, dir, entries, ttl)
}

// expireStoreChildren applies the store-entry rule to one already-read
// directory level.
func (c *Collector) expireStoreChildren(ctx context.Context, unit, dir string, entries []fs.DirEntry, ttl time.Duration) (int, int64, error) {
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
			n, f, err := c.expireStoreChildren(ctx, unit, store, children, ttl)
			removed, freed = removed+n, freed+f
			if err != nil {
				errs = append(errs, err)
			}
			continue
		}
		n, ok := c.expire(ctx, unit, store, e.Name(), ttl)
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
func (c *Collector) expireFiles(ctx context.Context, unit, dir, ext string, ttl time.Duration, deep bool) (int, int64, error) {
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
			n, f, err := c.expireFiles(ctx, unit, path, ext, ttl, deep)
			removed, freed = removed+n, freed+f
			if err != nil {
				errs = append(errs, err)
			}
			c.reapEmptyDir(ctx, unit, path, ttl)
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
		n, err := c.delete(unit, path, e.Name(), "agent-state")
		if err != nil {
			if !isSymlinkSkip(err) {
				errs = append(errs, err)
			}
			continue
		}
		removed++
		freed += n
	}
	return removed, freed, errors.Join(errs...)
}

// reapEmptyDir removes dir when this round left it empty and its own mtime is
// past ttl. The mtime gate keeps a directory another process just created or
// wrote: only shells the rule can prove stale are removed.
func (c *Collector) reapEmptyDir(ctx context.Context, unit, dir string, ttl time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		return false
	}
	info, err := os.Stat(dir)
	if err != nil || !c.expired(info.ModTime(), ttl) {
		return false
	}
	if _, err := c.delete(unit, dir, filepath.Base(dir), "agent-state"); err != nil {
		if !isSymlinkSkip(err) {
			c.log.Warn("cache.gc.delete_failed", "node", c.cfg.Node, "path", dir, "err", err.Error())
		}
		return false
	}
	return true
}

// expire deletes path when its newest write is older than ttl, reporting
// whether it was removed. unit is the tree the path belongs to and the root
// the removal stays within.
func (c *Collector) expire(ctx context.Context, unit, path, name string, ttl time.Duration) (int64, bool) {
	usedAt, _, err := measure(ctx, path)
	if err != nil {
		c.log.Warn("cache.gc.measure_failed", "node", c.cfg.Node, "path", path, "err", err.Error())
		return 0, false
	}
	if !c.expired(usedAt, ttl) {
		return 0, false
	}
	n, err := c.delete(unit, path, name, "ttl")
	if err != nil {
		if !isSymlinkSkip(err) {
			c.log.Warn("cache.gc.delete_failed", "node", c.cfg.Node, "path", path, "err", err.Error())
		}
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

// errSymlinkSegment marks a path whose deletion was refused because a
// component of the path is a symbolic link.
var errSymlinkSegment = errors.New("symbolic link on the path")

// isSymlinkSkip reports whether err is a refusal to follow a symbolic link,
// which is a designed skip rather than a failed deletion.
func isSymlinkSkip(err error) bool {
	return errors.Is(err, errSymlinkSegment)
}

// delete removes path, which must stay inside unit — the state tree of the
// workspace that owns it. The removal goes through os.Root, whose fd is
// checked against unit's own lstat first, so a unit that is a link or was
// replaced after the scan is refused rather than handed another workspace's
// tree (§8.8: 不跨 workspace 清删). The literal-path pre-check only names the
// skip for the log; the confinement is the root's.
func (c *Collector) delete(unit, path, name, reason string) (int64, error) {
	rel, err := filepath.Rel(unit, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return 0, fmt.Errorf("refusing to delete %q outside %q", path, unit)
	}
	if link, err := linkedSegment(unit, path); err != nil {
		return 0, fmt.Errorf("inspect %q: %w", path, err)
	} else if link != "" {
		c.log.Warn("cache.gc.symlink_skipped", "node", c.cfg.Node, "path", path, "segment", link, "reason", reason)
		return 0, fmt.Errorf("%w: %q", errSymlinkSegment, link)
	}
	bytes := dirBytes(path)
	if err := removeWithin(unit, rel); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil // the tree is already gone
		}
		return 0, fmt.Errorf("remove %q: %w", path, err)
	}
	c.log.Debug("cache.gc.delete", "node", c.cfg.Node, "path", path, "name", name, "reason", reason, "freed_bytes", bytes)
	return bytes, nil
}

// removeWithin removes rel below unit. Every component is resolved inside the
// opened root, and the root is only used when its fd is the directory unit
// names: os.OpenRoot follows a link on the final component, so without the
// identity check a linked unit would hand the removal another tree.
func removeWithin(unit, rel string) error {
	root, err := os.OpenRoot(unit)
	if err != nil {
		return err
	}
	defer root.Close()
	opened, err := root.Stat(".")
	if err != nil {
		return err
	}
	named, err := os.Lstat(unit)
	if err != nil {
		return err
	}
	if !os.SameFile(opened, named) {
		return fmt.Errorf("%w: %q", errSymlinkSegment, unit)
	}
	return root.RemoveAll(rel)
}

func (c *Collector) report(st Stats) {
	c.log.Info("cache.gc",
		"node", c.cfg.Node,
		"skipped", st.Skipped,
		"freed_bytes", st.FreedBytes,
		"task_dirs", st.TaskDirs,
		"bare_repos", st.BareRepos,
		"skill_cache", st.SkillCache,
		"agent_state", st.AgentState,
		"agent_memory", st.AgentMemory,
		"cache_bytes", st.CacheBytes,
		"cache_measured", st.CacheMeasured,
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
