// TC-tech-cache-07 implements AC-79: every node-cleanup round's event carries
// the bare-cache bytes left behind together with a measurement state, so the
// cache cap's occupancy is observable in production and a quiesce-skipped
// round is never read as an empty cache. The collector writes the event; the
// DaemonSet's log stream is the environment face.
package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/gc"
)

// tcCache07Event is the cache.gc line's observation face. Pointers keep
// "absent" distinct from "zero" — that difference is what the measurement
// state exists for.
type tcCache07Event struct {
	Msg           string `json:"msg"`
	Skipped       *bool  `json:"skipped"`
	CacheBytes    *int64 `json:"cache_bytes"`
	CacheMeasured *bool  `json:"cache_measured"`
}

// tcCache07Config is the baseline round configuration; tests override the
// field their scenario is about.
func tcCache07Config(root string, maxBytes int64) gc.Config {
	return gc.Config{
		StateRoot:     root,
		CacheTTL:      720 * time.Hour,
		TaskDirTTL:    168 * time.Hour,
		CacheMaxBytes: maxBytes,
		Quiesce:       30 * time.Minute,
		Interval:      time.Hour,
	}
}

// tcCache07Run runs one round and returns the cache.gc event it wrote. The
// round error is left to the caller: an interrupted scan is one of the
// states under test, not a test failure.
func tcCache07Run(t *testing.T, ctx context.Context, cfg gc.Config) (gc.Stats, tcCache07Event, error) {
	t.Helper()
	var buf bytes.Buffer
	collector := gc.New(cfg, slog.New(slog.NewJSONHandler(&buf, nil)))
	st, err := collector.RunOnce(ctx)

	var event tcCache07Event
	found := false
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var rec tcCache07Event
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		if rec.Msg == "cache.gc" {
			event, found = rec, true
		}
	}
	if !found {
		t.Fatalf("no cache.gc event in %q", buf.String())
	}
	return st, event, err
}

// tcCache07Round runs one round over root and returns the cache.gc event it
// wrote, failing the test when the round reports an error.
func tcCache07Round(t *testing.T, root string, maxBytes int64) (gc.Stats, tcCache07Event) {
	t.Helper()
	st, event, err := tcCache07Run(t, context.Background(), tcCache07Config(root, maxBytes))
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	return st, event
}

// tcCache07Write creates files relative to root.
func tcCache07Write(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
}

// tcCache07Age pushes every entry under path back by age.
func tcCache07Age(t *testing.T, path string, age time.Duration) {
	t.Helper()
	when := time.Now().Add(-age)
	err := filepath.Walk(path, func(entry string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return os.Chtimes(entry, when, when)
	})
	if err != nil {
		t.Fatalf("chtimes %s: %v", path, err)
	}
}

// tcCache07TreeBytes is the on-disk size of a tree (regular files only, the
// same measure the collector uses).
func tcCache07TreeBytes(t *testing.T, path string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(path, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", path, err)
	}
	return total
}

// TestCache07ReportsBytesAfterLRU: a round that reclaims the least recently
// used cache reports the bytes left behind, and that number equals the tree
// on disk after the reclaim — not the pre-reclaim scan total.
func TestCache07ReportsBytesAfterLRU(t *testing.T) {
	root := t.TempDir()
	tcCache07Write(t, root, map[string]string{
		"workspaces/.repos/ws-1/old.git/HEAD":   "0123456789", // 10 bytes, least recently used
		"workspaces/.repos/ws-1/young.git/HEAD": "abcdefghij", // 10 bytes
	})
	tcCache07Age(t, root, 100*time.Hour)
	tcCache07Age(t, filepath.Join(root, "workspaces/.repos/ws-1/young.git"), time.Hour)

	st, event := tcCache07Round(t, root, 10)
	if st.Skipped {
		t.Fatal("round skipped; the tree is outside the quiesce window")
	}
	if event.CacheMeasured == nil || !*event.CacheMeasured {
		t.Fatalf("cache_measured = %v, want true", event.CacheMeasured)
	}
	if event.CacheBytes == nil {
		t.Fatal("cache_bytes is absent, want the remaining bytes")
	}
	if _, err := os.Stat(filepath.Join(root, "workspaces/.repos/ws-1/old.git")); !os.IsNotExist(err) {
		t.Fatalf("old.git stat err = %v, want the LRU cache reclaimed", err)
	}
	onDisk := tcCache07TreeBytes(t, filepath.Join(root, "workspaces/.repos"))
	if *event.CacheBytes != onDisk {
		t.Errorf("cache_bytes = %d, want the on-disk total %d", *event.CacheBytes, onDisk)
	}
	if st.CacheBytes != *event.CacheBytes || !st.CacheMeasured {
		t.Errorf("Stats = %+v, want it to carry the reported measurement", st)
	}
}

// TestCache07EmptyCacheIsMeasuredZero: an empty bare-cache tree is a
// measurement, and it says zero.
func TestCache07EmptyCacheIsMeasuredZero(t *testing.T) {
	root := t.TempDir()
	tcCache07Age(t, root, 2*time.Hour) // outside the quiesce window

	_, event := tcCache07Round(t, root, gc.DefaultCacheMaxBytes)
	if event.CacheMeasured == nil || !*event.CacheMeasured {
		t.Fatalf("cache_measured = %v, want true for a complete scan", event.CacheMeasured)
	}
	if event.CacheBytes == nil || *event.CacheBytes != 0 {
		t.Fatalf("cache_bytes = %v, want a measured 0", event.CacheBytes)
	}
}

// TestCache07SkippedRoundIsNotMeasured: a quiesce-skipped round reports the
// byte field with the measurement state false, so a consumer cannot read its
// zero as an empty cache.
func TestCache07SkippedRoundIsNotMeasured(t *testing.T) {
	root := t.TempDir()
	tcCache07Write(t, root, map[string]string{
		"workspaces/.repos/ws-1/a.git/HEAD": "0123456789",
	})
	// The tree is fresh: inside the quiesce window, so the round must skip.

	st, event := tcCache07Round(t, root, gc.DefaultCacheMaxBytes)
	if !st.Skipped {
		t.Fatal("round did not skip; a fresh tree must trigger the quiesce window")
	}
	if event.Skipped == nil || !*event.Skipped {
		t.Fatalf("skipped = %v, want true", event.Skipped)
	}
	if event.CacheMeasured == nil || *event.CacheMeasured {
		t.Fatalf("cache_measured = %v, want false for a skipped round", event.CacheMeasured)
	}
	if event.CacheBytes == nil {
		t.Fatal("cache_bytes is absent; the field must accompany the measurement state")
	}
}

// TestCache07IncompleteScanIsNotMeasured: a cache tree that cannot be fully
// walked leaves the byte total short of the truth, so the round must report
// the measurement state false and that partial count must not be read as a
// measurement.
func TestCache07IncompleteScanIsNotMeasured(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("an unreadable cache entry cannot fail a root-owned walk")
	}
	root := t.TempDir()
	tcCache07Write(t, root, map[string]string{
		"workspaces/.repos/ws-1/good.git/HEAD":           "0123456789",            // 10 bytes, readable
		"workspaces/.repos/ws-1/broken.git/HEAD":         "0123456789",            // 10 bytes, readable
		"workspaces/.repos/ws-1/broken.git/objects/pack": strings.Repeat("x", 20), // 20 bytes behind the blocked directory
	})
	blocked := filepath.Join(root, "workspaces/.repos/ws-1/broken.git/objects")
	if err := os.Chmod(blocked, 0); err != nil {
		t.Fatalf("chmod %s: %v", blocked, err)
	}
	t.Cleanup(func() { _ = os.Chmod(blocked, 0o755) })

	// The quiesce probe walks the same tree and would fail first, so it is
	// off: the scenario under test is the cache scan, not the probe.
	cfg := tcCache07Config(root, gc.DefaultCacheMaxBytes)
	cfg.Quiesce = 0
	st, event, err := tcCache07Run(t, context.Background(), cfg)
	if err == nil {
		t.Fatal("RunOnce reported no error; an unreadable cache entry must surface as a scan failure")
	}
	if event.CacheMeasured == nil || *event.CacheMeasured {
		t.Fatalf("cache_measured = %v, want false when the scan could not finish", event.CacheMeasured)
	}
	if event.CacheBytes == nil {
		t.Fatal("cache_bytes is absent; the partial total must still accompany the measurement state")
	}
	// The tree holds 40 bytes, 20 of them readable; a repo whose walk fails
	// is dropped whole instead of counted in part, so only good.git's 10
	// bytes remain — read as a complete measurement, that is a quarter of
	// the occupancy.
	if *event.CacheBytes != 10 {
		t.Errorf("cache_bytes = %d, want 10 (only the measurable repo; the failing one is dropped whole)", *event.CacheBytes)
	}
	if st.CacheMeasured || st.CacheBytes != *event.CacheBytes {
		t.Errorf("Stats = %+v, want the same unmeasured partial total", st)
	}
}

// TestCache07CancelledScanIsNotMeasured: a round whose context is already
// cancelled stops between cache entries without reporting an error, so the
// byte total it produced is a lower bound, not a measurement.
func TestCache07CancelledScanIsNotMeasured(t *testing.T) {
	root := t.TempDir()
	tcCache07Write(t, root, map[string]string{
		"workspaces/.repos/ws-1/a.git/HEAD": "0123456789",
		"workspaces/.repos/ws-1/b.git/HEAD": "abcdefghij",
	})
	// The quiesce probe is not the scenario: the round must reach the scan.
	cfg := tcCache07Config(root, gc.DefaultCacheMaxBytes)
	cfg.Quiesce = 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	st, event, err := tcCache07Run(t, ctx, cfg)
	if err != nil {
		t.Fatalf("RunOnce: %v; cancellation between entries is silent, so only the measurement state can flag it", err)
	}
	if st.Skipped {
		t.Fatal("round skipped; a cancelled scan must still report the round")
	}
	if event.Skipped == nil || *event.Skipped {
		t.Fatalf("skipped = %v, want false", event.Skipped)
	}
	if event.CacheMeasured == nil || *event.CacheMeasured {
		t.Fatalf("cache_measured = %v, want false when the scan was cut short", event.CacheMeasured)
	}
	if event.CacheBytes == nil || *event.CacheBytes != 0 {
		t.Fatalf("cache_bytes = %v, want the unmeasured 0 the interrupted walk reached", event.CacheBytes)
	}
	if onDisk := tcCache07TreeBytes(t, filepath.Join(root, "workspaces/.repos")); onDisk != 20 {
		t.Fatalf("on-disk tree = %d bytes, want the 20 the cancelled scan never measured", onDisk)
	}
	if st.CacheMeasured || st.CacheBytes != *event.CacheBytes {
		t.Errorf("Stats = %+v, want the same unmeasured total", st)
	}
}
